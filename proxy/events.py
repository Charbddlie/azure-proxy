"""The proxy's own account of what it did, as records rather than prose.

Everything in here is already said out loud in the log. The reason it is also
kept as structured records is that the log cannot be read by anything but a
person: "estimating 62% foreign load on this deployment" is a sentence, and the
question a dashboard needs to answer — *did the estimate of what other tenants
hold move, on which deployment, and by how much* — cannot be got back out of it
without parsing English. Parsing English out of one's own log is how a
monitoring surface starts silently lying the first time someone rewords a
message.

So the rule is: one call site, two outputs. `server._ev` writes the human line
and the record from the same arguments, so a record can never describe an event
the log did not, and rewording a message cannot break a reader.

What is NOT in here, ever: request and response bodies. `_setup_logging` is
forbidden from recording prompts at any log level, and a ring buffer served over
HTTP is a strictly worse place to put them than a file with the operator's
umask on it. The fields below are names, numbers and statuses.
"""

# The event kinds, and what each one is FOR. Anything the dashboard filters or
# colours by has to be one of these, so keeping the set closed is what stops a
# typo'd kind from becoming an event that no filter ever matches.
KINDS = (
    "route_refresh", # a new discovery generation was loaded without restarting
    "boot",         # startup narration: identity, endpoints, mode
    "request",      # a request arrived; face + model, before any route is picked
    "response",     # a request finished; the route that answered and the status
    "capacity",     # a successful request raised the persisted safe-RPM maximum
    "throttle",     # 429, or the 200-plus-retry-after that means the same thing
    "demote",       # 5xx or transport error: park the route, but not a quota fact
    "foreign",      # the estimate of what OTHER tenants hold moved
    "failover",     # left one route for the next in the chain
    "held",         # a pinned session queued for its own endpoint instead
    "pin",          # a conversation was bound to a route, or lost its binding
    "inherited",    # a thread with no pin of its own was placed on the endpoint
                    # its session's state lives on — a subagent's first turn
    "unpinned",     # a request carrying encrypted state had nothing to place it
                    # by, so the balancer chose and it may well be refused
    "upstream_error",  # the upstream failed the turn inside a 200, in an SSE
                    # `error` event, or named a refusal in a buffered body
    "stripped",     # a turn went out without the encrypted reasoning it came
                    # with, because it could not be kept on the deployment that
                    # can read it
    "timeout",      # upstream took longer than the request timeout
    "exhausted",    # every route in the chain failed; the caller got a 503
    "image_tool",   # a Responses turn asked the model to draw, so the attempt
                    # list was held to endpoints with an image deployment to name
    "token",        # the Azure credential was refreshed, or could not be
)

# Kinds that mean something needs looking at. The dashboard's "problems only"
# filter is this set, and it is defined here rather than there so that adding a
# kind forces a decision about which half it belongs to.
#
# `stripped` is in it and `pin` is not, which is the distinction worth keeping:
# pinning a session is the mechanism working, and putting it here would bury
# the filter under one line per conversation. A strip is the mechanism having
# already failed — the turn survived, but it answered without its own reasoning,
# and a run of them means affinity is not holding.
#
# `inherited` is out for the same reason as `pin`: placing a subagent on its
# parent's endpoint is the mechanism working, once per spawn. `unpinned` is in,
# because it is the one case where the proxy knowingly sends ciphertext
# somewhere that may refuse it. `upstream_error` is in and belongs there most of
# all: a turn the upstream failed inside a 200 used to leave no trace here at
# all, which is why the 2026-08-25 outage could not be read out of proxy.log.
PROBLEM_KINDS = frozenset(
    {"throttle", "demote", "failover", "timeout", "exhausted", "stripped",
     "unpinned", "upstream_error"})

LEVELS = ("debug", "info", "warning", "error")
LEVEL_RANK = {name: index for index, name in enumerate(LEVELS)}
EVENT_LEVELS = {
    "route_refresh": "info",
    "request": "debug",
    "boot": "info", "response": "info", "capacity": "info", "foreign": "info",
    "held": "info", "pin": "info", "inherited": "info", "image_tool": "info", "token": "info",
    "throttle": "warning", "demote": "warning", "failover": "warning",
    "stripped": "warning", "unpinned": "warning",
    "timeout": "error", "exhausted": "error", "upstream_error": "error",
}


def event_level(kind: str, level: str = "info", fields=None) -> str:
    """Canonical severity for new observations and older dashboard snapshots.

    Rate limiting is a recoverable upstream refusal. A timeout means an I/O
    deadline expired, so its severity is ERROR independently of quota state.
    Explicit warnings/errors on otherwise routine events remain visible.
    """
    fields = fields or {}
    level = str(level or "info").lower()
    level = {"warn": "warning", "critical": "error", "fatal": "error"}.get(level, level)
    level = level if level in LEVEL_RANK else "info"
    base = EVENT_LEVELS.get(kind, level)
    if kind == "request" and LEVEL_RANK[level] < LEVEL_RANK["warning"]:
        return "debug"
    if kind == "upstream_error" and fields.get("error_code") in (
            "rate_limit_exceeded", "too_many_requests"):
        return "warning"
    if kind == "response":
        try:
            status = int(fields.get("status", 0))
        except (TypeError, ValueError):
            status = 0
        if fields.get("broke") or status >= 500:
            base = "error"
        elif status >= 400:
            base = "warning"
    if kind == "token" and fields.get("ok") is False:
        remaining = fields.get("expires_in_seconds", 0)
        base = "warning" if isinstance(remaining, (int, float)) and remaining > 0 else "error"
    return max((base, level), key=LEVEL_RANK.__getitem__)


def normalized_event(event: dict) -> dict:
    return dict(event, level=event_level(event.get("kind", ""), event.get("level", "info"), event))


def event_feed(feed, since=0, limit=400, kind=None, initial=False, now=None):
    """A complete cursor interval, with explicit history and unread-loss metadata."""
    import collections
    import time
    from .retention import RETENTION_SECONDS
    now = time.time() if now is None else now
    cursor = feed.get("next", 0)
    expired = [e["seq"] for e in feed.get("events", []) if e["at"] <= now - RETENTION_SECONDS]
    expiry = max([feed.get("retention_through", 0)] + expired)
    held = [e for e in feed.get("events", []) if e["at"] > now - RETENTION_SECONDS]
    fresh = [e for e in held if initial or e["seq"] > since]
    reasons = []
    if not initial and cursor >= since:
        lost = max(0, cursor - since - len(fresh))
        retention = min(lost, max(0, min(cursor, expiry) - since))
        if retention:
            reasons.append(dict(reason="retention", count=retention))
        if lost > retention:
            reasons.append(dict(reason="buffer_overwrite", count=lost - retention))
    kinds = (PROBLEM_KINDS if kind == "problems" else
             frozenset(k.strip() for k in kind.split(",")) if kind else None)
    if kinds is not None:
        fresh = [e for e in fresh if e["kind"] in kinds]
    skipped = max(0, len(fresh) - limit) if limit > 0 else 0
    if skipped:
        fresh = fresh[-limit:]
        if not initial:
            reasons.append(dict(reason="limit", count=skipped))
    count = sum(item["count"] for item in reasons)
    return dict(events=fresh, next=cursor, dropped=bool(count),
                stream_id=feed.get("stream_id"), initial=initial,
                history=dict(truncated=initial and (cursor > len(fresh)), loaded=len(fresh)),
                gap=dict(count=count, reasons=reasons, since=since, through=cursor) if count else None,
                counts=dict(collections.Counter(e["kind"] for e in held)))
