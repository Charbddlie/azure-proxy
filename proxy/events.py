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

import collections
import itertools
import threading
import time
from typing import Dict, List, Optional, Tuple

# The event kinds, and what each one is FOR. Anything the dashboard filters or
# colours by has to be one of these, so keeping the set closed is what stops a
# typo'd kind from becoming an event that no filter ever matches.
KINDS = (
    "boot",         # startup narration: identity, endpoints, mode
    "request",      # a request arrived; face + model, before any route is picked
    "response",     # a request finished; the route that answered and the status
    "throttle",     # 429, or the 200-plus-retry-after that means the same thing
    "demote",       # 5xx or transport error: park the route, but not a quota fact
    "foreign",      # the estimate of what OTHER tenants hold moved
    "failover",     # left one route for the next in the chain
    "held",         # a pinned session queued for its own endpoint instead
    "pin",          # a conversation was bound to a route, or lost its binding
    "timeout",      # upstream took longer than the request timeout
    "exhausted",    # every route in the chain failed; the caller got a 503
    "token",        # the Azure credential was refreshed, or could not be
)

# Kinds that mean something needs looking at. The dashboard's "problems only"
# filter is this set, and it is defined here rather than there so that adding a
# kind forces a decision about which half it belongs to.
PROBLEM_KINDS = frozenset(
    {"throttle", "demote", "failover", "timeout", "exhausted"})

LEVELS = ("debug", "info", "warning", "error")


class EventLog:
    """A bounded ring of recent events, with a cursor readers can resume from.

    Bounded because this runs for weeks: an unbounded list of every request is
    a memory leak with a dashboard attached. The consequence of the bound is
    that a slow reader can fall off the back, and `since` says so rather than
    handing back a shorter list that looks continuous — a gap the reader knows
    about is a gap it can label, and one it does not know about is a lie.

    Sequence numbers are global and never reused, so a reader's cursor stays
    meaningful across a resize and cannot be confused by the ring wrapping.
    They are NOT persisted: a restart resets them to zero, which readers detect
    as `next` going backwards.
    """

    def __init__(self, capacity: int = 2000):
        self._events: collections.deque = collections.deque(maxlen=capacity)
        self._seq = itertools.count(1)
        # deque.append is atomic under the GIL, but `since` walks the deque and
        # the server touches this from several request tasks at once. The lock
        # costs nothing at this volume and removes a whole class of question.
        self._lock = threading.Lock()

    def record(self, kind: str, message: str, level: str = "info",
               **fields) -> None:
        """Append one event. Never raises: this sits on the request path.

        `route` is accepted as the Route object or its string form and split
        into endpoint/deployment, because every consumer wants them separately
        — the source view groups by endpoint, the model view by deployment —
        and doing the split at each call site is how the two halves drift.
        """
        try:
            route = fields.pop("route", None)
            if route is not None:
                text = str(route)
                fields.setdefault("route", text)
                if "endpoint" not in fields and "/" in text:
                    endpoint, _, deployment = text.partition("/")
                    fields.setdefault("endpoint", endpoint)
                    fields.setdefault("deployment", deployment)
            event = {"seq": next(self._seq), "at": time.time(),
                     "kind": kind, "level": level, "message": message}
            event.update(fields)
            with self._lock:
                self._events.append(event)
        except Exception:       # pragma: no cover - defensive
            pass

    def since(self, seq: int = 0, limit: int = 500,
              kinds: Optional[frozenset] = None) -> Tuple[List[dict], int, bool]:
        """(events after `seq`, the next cursor, whether anything was missed).

        The cursor is advanced past everything examined, not just past what is
        returned — a reader filtering for `throttle` must not be handed the same
        thousand `request` events again on every poll.
        """
        with self._lock:
            events = list(self._events)
        if not events:
            return [], seq, False
        # Anything older than the ring's oldest entry is gone for good. A reader
        # at seq 0 is starting fresh rather than resuming, so it has missed
        # nothing by definition.
        dropped = seq > 0 and events[0]["seq"] > seq + 1
        fresh = [e for e in events if e["seq"] > seq]
        cursor = fresh[-1]["seq"] if fresh else seq
        if kinds is not None:
            fresh = [e for e in fresh if e["kind"] in kinds]
        if limit and len(fresh) > limit:
            fresh = fresh[-limit:]
        return fresh, cursor, dropped

    def counts(self) -> Dict[str, int]:
        """How many of each kind are currently held. For the dashboard header."""
        with self._lock:
            events = list(self._events)
        out: Dict[str, int] = {}
        for e in events:
            out[e["kind"]] = out.get(e["kind"], 0) + 1
        return out
