"""Serving process: execute cached mappings and preserve live conversations.

Routing publishes the target and fallback order independently. This process
owns HTTP/SSE connections, Azure credentials, session bindings and protocol
compatibility. It emits raw observations to a background journal writer.

    python -m proxy
"""

import asyncio
import collections
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from typing import Callable, Dict, List, Optional, Tuple

import httpx
from azure.identity import AzureCliCredential
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .events import PROBLEM_KINDS, EventLog
from .config import Config, Route, _as_number
from .bridge import (Attempt, ConfigView, ServingBridge, SnapshotMiddleware,
                     Telemetry)
from .config import TABLES
from .process import ProcessClaim

ROOT = os.environ.get(
    "AZURE_PROXY_HOME",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SETTINGS = os.path.join(ROOT, "settings")
RUNTIME = os.path.join(ROOT, "runtime")

log = logging.getLogger("azure-proxy")

# The structured half of the log. Every `_ev` call writes both, from the same
# arguments — see proxy/events.py for why the dashboard cannot just read the
# text. Sized for roughly an hour of a busy run at the volume `request` and
# `response` events arrive.
events = EventLog(capacity=2000)

_EV_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO,
              "warning": logging.WARNING, "error": logging.ERROR}


def _ev(kind: str, level: str, msg: str, *args, **fields) -> None:
    """Say something, once, in both registers.

    `msg`/`args` are the ordinary %-style logging pair, so the human line is
    unchanged from what it was before the record existed; `fields` is what the
    record carries beyond the sentence. Formatting is done eagerly for the
    record — logging would have skipped it below the level threshold, but a
    record that only exists at debug level is a dashboard that goes blank when
    someone quietens the log.
    """
    log.log(_EV_LEVELS.get(level, logging.INFO), msg, *args)
    try:
        message = msg % args if args else msg
    except Exception:           # pragma: no cover - defensive
        message = msg
    events.record(kind, message, level=level, **fields)
    if "bridge" in globals():
        fields = {key: str(value) if isinstance(value, Route) else value
                  for key, value in fields.items()}
        bridge.record("event", event_kind=kind, message=message,
                      level=level, fields=fields)

# This box is a shared account with a single ~/.azure, so the proxy keeps its
# own credential directory and every operator action has to go through the
# wrapper that points at it. Bare `az` touches someone else's login.
LOGIN_HINT = "on the proxy host run: ./az.sh login --use-device-code"

# Headers we must not relay upstream: hop-by-hop, or ours to set. content-type
# and content-length belong in the second group because the body is
# re-serialised here — and forwarding the client's content-type does not
# override ours, it arrives alongside it as `application/json,application/json`,
# which the Responses API rejects outright.
STRIP_REQUEST_HEADERS = {
    "host", "content-length", "content-type", "connection", "keep-alive",
    "transfer-encoding", "upgrade", "proxy-authorization", "te", "trailer",
    "authorization", "api-key",
}
STRIP_RESPONSE_HEADERS = {
    "content-length", "content-encoding", "connection", "keep-alive",
    "transfer-encoding", "upgrade",
}

# The three ways a request's attempt order can be decided. See
# `routing.balance` in settings/policy.yaml for what each one buys.
BALANCE_MODES = ("strict_priority", "priority_threshold", "capacity")

# The names these modes had before there were three of them. Kept working
# because they are what an operator who read the old README will type, and
# because a config that used to mean something specific should not silently
# start meaning something else.
BALANCE_ALIASES = {"priority": "strict_priority", "weighted": "capacity"}

# The ways a request can arrive, as far as the ledger is concerned: the two text
# faces times streamed or not, plus the image face. Quota does not care about the
# distinction — one deployment ceiling serves all of them — but an operator does,
# because they fail differently. Streamed Responses calls are the ones Azure
# refuses with a 200 plus retry-after (see THROTTLE_HEADER), and they are the
# ones session affinity pins. A load figure that cannot say which of them it is
# made of cannot tell you which mechanism you are watching.
#
# The image face has no streamed twin here. gpt-image-* can stream partial
# images, and such a call is relayed like any other stream, but it is one
# request against a per-minute request ceiling either way, and the ledger has
# nothing to gain from splitting it.
#
# The order is the display order, and the index is what goes in the ledger.
# tui/bars.py and tui/theme.py carry the same list and must stay in step.
FACES = ("chat", "chat_stream", "responses", "responses_stream", "image")

# The paths the four handlers register under, which are also what `face` is
# throughout: the string a log line shows and the thing face_code reads.
CHAT_FACE = "/v1/chat/completions"
RESPONSES_FACE = "/v1/responses"
IMAGE_FACES = {"generations": "/v1/images/generations",
               "edits": "/v1/images/edits"}

# The Responses API can draw pictures itself, through `tools: [{"type":
# "image_generation"}]`, but Azure will not choose a deployment for it:
#
#   {"error": {"message": "imagegen deployment must be provided through
#              header: x-ms-oai-image-generation-deployment", ...}}
#
# So a caller has to know a deployment name, on the endpoint their model
# happened to be balanced onto — two things the proxy exists to hide. It fills
# the header in per attempt instead, and keeps the attempt on an endpoint that
# has an image deployment to name. A header the caller set is left alone.
IMAGE_TOOL_HEADER = "x-ms-oai-image-generation-deployment"


def face_code(face: str, stream: bool) -> int:
    """The FACES index for one request. `face` is the request path."""
    if face.startswith("/v1/images/"):
        return 4
    base = 2 if face.endswith("responses") else 0
    return base + (1 if stream else 0)


# --------------------------------------------------------------------------
# Session affinity
# --------------------------------------------------------------------------
#
# Some state does not travel between endpoints, and the proxy spent three
# revisions believing otherwise. The README used to argue that because codex
# sends `store: false` and resends its whole `input` every turn, the endpoints
# were interchangeable and a request could go anywhere. That is wrong, and the
# way it is wrong is expensive:
#
#   codex sends `include: ["reasoning.encrypted_content"]`. Azure returns the
#   model's reasoning as an ENCRYPTED blob, and codex hands that blob back on
#   the next turn. The key belongs to the resource that produced it. Send the
#   blob to a different endpoint and it answers
#
#     The encrypted content for item rs_… could not be verified.
#     Reason: Encrypted content could not be decrypted.
#
#   which kills the trial. Measured: switching balance to `capacity` took the
#   first three codex trials to NonZeroAgentExitCode, 0 passed.
#
# So `store: false` means "do not keep this server-side", not "this request is
# self-contained". The encrypted blob IS cross-turn state; it just happens to be
# carried by the client instead of the server. Every mechanism that moves a
# request between endpoints breaks it: capacity sampling, threshold spillover,
# and — the one that stings — the in-band throttle failover added to fix the
# rate limiting.
#
# Hence: a conversation that carries endpoint-bound state is pinned to the
# deployment that produced it, and never moved.
#
# The pin names the ROUTE — endpoint and deployment — not the endpoint. One
# resource can serve a model from two deployments (a second SKU is a second
# quota), and whether Azure's encrypted content is scoped to the resource or to
# the deployment is not something we have evidence for. Pinning to the exact
# deployment cannot be wrong; pinning to the endpoint would be a guess, and the
# failure mode of guessing wrong is a killed trial.
#
# What identifies a conversation, measured off real codex 0.149 traffic through
# harbor (2026-08-20). Four independent fields agree exactly, and stay constant
# across every turn of a trial:
#
#   session-id: 01a0216b-cf47-7542-95ad-7d524fbb1582      (header)
#   thread-id:  01a0216b-cf47-7542-95ad-7d524fbb1582      (header)
#   prompt_cache_key: 01a0216b-…                          (body)
#   client_metadata.session_id: 01a0216b-…                (body)
#
# The header is preferred because it costs nothing to read, with the body fields
# behind it so a client that sends one but not the other still works.
#
# What makes a request sticky is `include: ["reasoning.encrypted_content"]`,
# which codex sends from its FIRST turn — before there is any state to protect.
# That is exactly right: pinning has to happen on the turn that CREATES the
# blob, not the one that returns it, or the blob is already on the wrong
# endpoint by the time anyone notices. It also means callers that do not ask for
# encrypted reasoning — the chat face, mini-swe-agent — are never pinned and
# keep the full run of the balancer.
#
# `previous_response_id` and `store: true` qualify too, for the same reason in
# a different costume: one dereferences an object that lives on a single
# endpoint, the other mints it. No caller here uses them today — codex sends
# `store: false` and resends the whole transcript — but the failure if one ever
# did would be a 404 on someone else's endpoint, which reads like an outage
# rather than a routing mistake. Cheap to cover now, expensive to diagnose later.
#
# Prompt caching is deliberately NOT a trigger. It is per-endpoint too, and
# splitting a conversation costs real money — 89% of terminus-2's 11.4M input
# tokens were cache hits — but a cache miss returns the right answer. Pinning
# for it would trade a correctness mechanism for an economic one and quietly
# reduce the balancer to strict priority, since almost every request would
# qualify.


class SessionAffinity:
    """Remembers which deployment owns a conversation's encrypted state.

    Pinning happens on the first response a sticky session gets, not on the
    first request: until something has actually been produced there is no state
    to be bound to, so the opening turn is free to fail over and to be placed by
    the balancer like any other. That is what keeps affinity from collapsing
    into "every session on endpoint one" — sessions are distributed as they
    start, and only then held.

    Two books, because a client session is not one conversation. `_pins` binds
    (conversation, model) to an exact route, which is what each thread's own
    reasoning needs. `_family` binds the conversation alone to an ENDPOINT, and
    is what a thread consults when it has no pin of its own — the case that
    matters being a subagent, which opens a new thread on a new model already
    holding ciphertext its parent minted. See `family`.
    """

    def __init__(self, config: Config):
        self.cfg = config
        # session key -> [route, last used]. Ordered so the oldest entry is
        # cheap to evict; a benchmark opens a bounded number of sessions but a
        # long-lived proxy should not grow without limit.
        self._pins: "collections.OrderedDict[str, List]" = \
            collections.OrderedDict()
        # family key -> [endpoint name, last used]. The coarser book: see
        # `family` below. Same eviction rules, same bound.
        self._family: "collections.OrderedDict[str, List]" = \
            collections.OrderedDict()
        # Cumulative, never reset: "did the fallback fire at all during my run"
        # is the question this answers, and it should be one read of /healthz
        # rather than a grep over a log that runs to tens of megabytes.
        self._bound = {}
        self._catalog = {}
        self._stripped = 0
        self._inherited = 0
        self._rejected = 0
        # family -> when its ciphertext was last found to be unplaceable. See
        # `tainted`.
        self._refused: "collections.OrderedDict[str, float]" = \
            collections.OrderedDict()

    def note_stripped(self, family: Optional[str] = None) -> None:
        self._stripped += 1
        self._taint(family)

    def note_inherited(self) -> None:
        self._inherited += 1

    def note_rejected(self, family: Optional[str] = None) -> None:
        """An endpoint refused this family's ciphertext outright."""
        self._rejected += 1
        self._taint(family)

    def _taint(self, family: Optional[str]) -> None:
        if not family:
            return
        self._refused[family] = time.time()
        self._refused.move_to_end(family)
        while len(self._refused) > self.cfg.affinity_max:
            self._refused.popitem(last=False)

    def tainted(self, family: Optional[str]) -> bool:
        """Does this family still hold ciphertext nothing here can place?

        Sticky, not one-shot, and that is the whole of what it is for. The
        proxy strips a REQUEST; it cannot strip the client's transcript, and
        codex rebuilds `input` from that transcript every turn. So a session
        that was stripped once goes on resending the same unreadable items on
        every subsequent turn — and the turn after the strip is pinned again,
        looks readable, and is refused. Measured 2026-08-25: a session that
        survived a stripped turn died on the next one with

            <- 400 /v1/responses ... invalid_encrypted_content

        Once tainted, the family keeps being stripped until its entry expires.
        That costs it reasoning continuity for the rest of the hour, which is
        the same trade `off_route: strip` already makes and the same direction:
        a turn that has forgotten how it got here is a cost the run absorbs, a
        refused turn ends it. Over-stripping a session that could have
        recovered is the price, and it is small — a session only gets here
        after it has already been moved off the endpoint that minted its state,
        and the pin follows it to the new one rather than back.
        """
        if not family:
            return False
        at = self._refused.get(family)
        if at is None:
            return False
        if time.time() - at > self.cfg.affinity_ttl:
            self._refused.pop(family, None)
            return False
        return True

    # -- identification ---------------------------------------------------
    def sticky(self, body: dict) -> bool:
        """Does this request carry (or create) endpoint-bound state?

        Three ways to qualify. The first is configurable because it is a list
        of `include` markers that Azure may extend; the other two are not,
        because they are not heuristics — they are what the Responses API
        means by stateful, and letting someone switch them off would only
        enable a configuration that is known to be broken.
        """
        include = body.get("include")
        if isinstance(include, list) and any(
                str(i) in self.cfg.affinity_markers for i in include):
            return True                 # encrypted reasoning: decryptable only
                                        # by the resource that produced it

        # References a response object that exists upstream, on one endpoint.
        # Anywhere else it is a 404.
        if body.get("previous_response_id"):
            return True

        # Creates that object. Nothing fails on this turn — which is the trap:
        # the damage shows up on the next one, by which time the state is
        # already on an endpoint nobody chose deliberately. Same reason the
        # encrypted-reasoning pin fires on the turn that mints the blob.
        if body.get("store") is True:
            return True

        return False

    def session_type(self, body: dict) -> str:
        """A compact operator-facing classification of a sticky session."""
        items = body.get("input")
        if isinstance(items, list) and any(
                isinstance(item, dict) and item.get("type") == "agent_message"
                for item in items):
            return "subagent"
        metadata = body.get("client_metadata")
        cli = metadata.get("cli") if isinstance(metadata, dict) else None
        if isinstance(cli, str) and cli.strip():
            return cli.strip().lower()
        if body.get("previous_response_id") or body.get("store") is True:
            return "stateful"
        return "reasoning"

    def _conversation(self, request: Request, body: dict) -> Optional[str]:
        """The conversation id this request belongs to, whatever carries it."""
        for spec in self.cfg.affinity_keys:
            where, _, name = spec.partition(":")
            if where == "header":
                value = request.headers.get(name)
            else:
                value = body
                for part in name.split("."):
                    value = value.get(part) if isinstance(value, dict) else None
            if isinstance(value, str) and value.strip():
                return "{}={}".format(spec, value.strip())
        return None

    def key(self, request: Request, body: dict) -> Optional[str]:
        """The pin slot for this request: who is asking, and for which model.

        The model belongs in the key because a pin binds one *deployment's*
        encrypted state, and one conversation id can cover more than one model
        — a session whose main model is gpt-5.6-sol may also send a turn for
        gpt-5.6-terra, and those two blobs are minted by two different
        deployments. Keying on the conversation alone gives them one slot to
        share, so each turn overwrites the other's pin and both sessions end up
        routed to a deployment that cannot decrypt what they carry.
        """
        conversation = self._conversation(request, body)
        if conversation is None:
            return None
        model = body.get("model")
        model = model.strip() if isinstance(model, str) else ""
        return "{}\0model={}".format(conversation, model)

    def family(self, request: Request, body: dict) -> Optional[str]:
        """The slot for everything one client session has going at once.

        The same key without the model, and it exists because a codex session
        is not one conversation. When the master calls
        `collaboration.spawn_agent`, the subagent opens its OWN thread — a
        different `thread-id`, the SAME `session-id` — on a different model,
        and codex seeds that thread with an `agent_message` item it inherited
        from the parent. Measured on 2026-08-25, that item carries the parent's
        ciphertext:

            {"type": "agent_message",
             "content": [{...},
                         {"type": "encrypted_content",
                          "encrypted_content": "gAAAAABqjaoo…"}]}

        The blob was minted by the master's endpoint. `key` gives the subagent
        a slot of its own — correctly, because its own reasoning is minted
        wherever it lands — but that slot is empty on the subagent's first
        turn, so the balancer placed it anywhere and the parent's blob went to
        an endpoint that could not read it:

            event: error   {"code": "invalid_encrypted_content",
                            "message": "Encrypted function output content
                                        could not be decrypted or decoded."}
            event: response.failed

        which codex reports as `stream disconnected before completion` and the
        worker's first turn dies. Intermittent, because a balancer that
        happened to pick the master's endpoint produced a working subagent —
        with three endpoints serving the model, roughly one spawn in three.

        So the family pin holds a whole session's threads on ONE endpoint,
        while `key` still binds each thread+model to its exact deployment. The
        endpoint is the useful unit here: the parent's blob has to survive a
        move to a *different deployment* on the same resource (sol -> terra),
        which the per-route pin cannot express and which the subagent case
        requires by construction.
        """
        return self._conversation(request, body)

    # -- the map ----------------------------------------------------------
    def _expire(self, now: float) -> None:
        ttl = self.cfg.affinity_ttl
        for book in (self._pins, self._family):
            while book:
                _key, entry = next(iter(book.items()))
                if now - entry[1] <= ttl:
                    break
                book.popitem(last=False)
            while len(book) > self.cfg.affinity_max:
                book.popitem(last=False)

        for key in list(self._bound):
            if key not in self._pins:
                self._bound.pop(key, None)
        for key in list(self._catalog):
            if key not in self._family:
                self._catalog.pop(key, None)

    def candidates(self, request, body, table, active):
        """Retain the family catalog for bound sessions as deployments drain."""
        family = self.family(request, body)
        self._expire(time.time())
        catalog = self._catalog.get(family, {}).get("tables", {})
        retained = catalog.get(table, {}).get(body.get("model"), [])
        # Active order is supplied by routing. Retired routes are only
        # accessible through this family's retained catalog.
        return list(active) + [r for r in retained if str(r) not in {str(a) for a in active}]

    def home(self, family: Optional[str],
             routes: List[Route]) -> Tuple[Optional[str], List[Route]]:
        """The endpoint this session's state lives on, and the way in.

        Used when the exact (conversation, model) slot is empty but the family
        one is not — which is precisely a subagent's first turn. The answer is
        an endpoint name and the subset of `routes` that sits on it, so the
        balancer still chooses between that endpoint's deployments and
        failover inside the resource still works.

        An empty subset is not an error and does not clear the entry: the
        session's endpoint simply does not serve this model. The caller strips
        the ciphertext and routes wherever it likes, which costs the turn its
        inherited context and nothing else.
        """
        if not family:
            return None, []
        now = time.time()
        self._expire(now)
        entry = self._family.get(family)
        if entry is None:
            return None, []
        entry[1] = now
        self._family.move_to_end(family)
        return entry[0], [r for r in routes if r.endpoint == entry[0]]

    def pinned(self, key: Optional[str],
               routes: List[Route]) -> Optional[Route]:
        """Use the retained descriptor while a bound deployment drains.

        Publishing a new model table leaves existing bindings intact. Expiry
        and explicit successful re-binding still follow the affinity policy.
        """
        if not key:
            return None
        now = time.time()
        self._expire(now)
        entry = self._pins.get(key)
        if entry is None:
            return None
        route = self._bound.get(key)
        if route is not None:
            entry[1] = now
            self._pins.move_to_end(key)
            return route
        for route in routes:
            if str(route) == entry[0]:
                entry[1] = now
                self._pins.move_to_end(key)
                return route
        _ev("pin", "warning",
            "session pinned to %s, which no longer serves this model; "
            "routing without the pin", entry[0], route=entry[0], dropped=True)
        return None

    def pin(self, key: Optional[str], route: Route,
            family: Optional[str] = None,
            session_type: str = "reasoning") -> None:
        """Record where this conversation's state now lives.

        Both books are written on every successful sticky response. A move is
        logged rather than being made silently: a pin that changes route is how
        a session loses state it is still carrying, and the previous outage
        could not be read out of proxy.log because this branch said nothing.
        """
        now = time.time()
        if key:
            self._bound[key] = route
            entry = self._pins.get(key)
            if entry is None:
                _ev("pin", "info", "session pinned to %s (%d live)", route,
                    len(self._pins) + 1, route=route, live=len(self._pins) + 1)
                self._pins[key] = [str(route), now, session_type]
            else:
                if entry[0] != str(route):
                    _ev("pin", "warning",
                        "session moved from %s to %s; state minted on the old "
                        "one is no longer readable", entry[0], route,
                        route=route, from_route=entry[0], moved=True)
                entry[0], entry[1] = str(route), now
                if len(entry) < 3:
                    entry.append(session_type)
                else:
                    entry[2] = session_type
            self._pins.move_to_end(key)

        if family:
            entry = self._family.get(family)
            if entry is None:
                self._family[family] = [route.endpoint, now]
            else:
                if entry[0] != route.endpoint:
                    _ev("pin", "warning",
                        "session's endpoint moved from %s to %s; threads "
                        "spawned before this carry state it cannot read",
                        entry[0], route.endpoint,
                        route=route, from_endpoint=entry[0], moved=True)
                entry[0], entry[1] = route.endpoint, now
            self._family.move_to_end(family)

        if family:
            catalog = self._catalog.get(family)
            if not catalog or catalog["endpoint"] != route.endpoint:
                catalog = self._catalog[family] = {"endpoint": route.endpoint, "tables": {},
                                                   "images": {}}
            deployment = getattr(self.cfg, "image_deployments", {}).get(route.endpoint)
            if deployment:
                catalog["images"][route.endpoint] = deployment
            for table in TABLES:
                retained = catalog["tables"].setdefault(table, {})
                for model, candidates in getattr(self.cfg, table, {}).items():
                    local = [r for r in candidates if r.endpoint == route.endpoint]
                    if local:
                        previous = retained.get(model, [])
                        retained[model] = local + [r for r in previous
                                                 if str(r) not in {str(a) for a in local}]
        self._expire(now)

    def report(self) -> dict:
        now = time.time()
        self._expire(now)
        # Per route, and per endpoint underneath it. The endpoint total is the
        # one to read for "is affinity pushing everything at one resource"; the
        # route breakdown is what says which deployment is carrying it.
        routes: Dict[str, int] = {}
        counts: Dict[str, int] = {}
        models: Dict[str, dict] = {}
        for key, entry in self._pins.items():
            route, _ts = entry[:2]
            session_type = entry[2] if len(entry) > 2 else "reasoning"
            routes[route] = routes.get(route, 0) + 1
            endpoint = route.split("/", 1)[0]
            counts[endpoint] = counts.get(endpoint, 0) + 1
            marker = "\0model="
            model = key.rpartition(marker)[2] if marker in key else ""
            if model:
                bucket = models.setdefault(model, {"total": 0, "types": {}})
                bucket["total"] += 1
                types = bucket["types"]
                types[session_type] = types.get(session_type, 0) + 1
        return {"enabled": self.cfg.affinity_enabled,
                "on_conflict": self.cfg.affinity_on_conflict,
                "off_route": self.cfg.affinity_off_route,
                "stripped_turns": self._stripped,
                # How often a thread was placed on its session's endpoint
                # rather than by the balancer — the subagent case. Read next to
                # stripped_turns: inherited means the parent's state survived,
                # stripped means it did not.
                "inherited_turns": self._inherited,
                # Turns an endpoint actually refused. Zero is the number to
                # expect; anything else says routing let ciphertext reach an
                # endpoint that could not read it, and the log says which.
                "rejected_turns": self._rejected,
                "live_sessions": len(self._pins),
                "live_families": len(self._family),
                "sessions_per_route": routes,
                "sessions_per_endpoint": counts,
                "sessions_per_model": models}


class TokenUnavailable(Exception):
    """No usable credential, and refreshing did not produce one."""


class TokenCache:
    """Holds the Azure AD token, refreshing it before it expires.

    `az account get-access-token` returns whatever is in the CLI's cache, so the
    remaining lifetime of a freshly fetched token is not predictable — it has
    been observed at under ten minutes as well as near an hour. Three rules keep
    that from causing trouble:

      * A token stays usable until it genuinely expires (less a small slack).
        The refresh margin only decides when to *start* trying, not when to stop
        trusting what we hold.
      * The margin is clamped to half the token's actual lifetime. Without this,
        a token shorter-lived than the configured margin would be considered
        due-for-refresh the moment it arrived, and every request would pay for
        an `az` subprocess.
      * Refresh attempts are rate limited, so a broken `az` cannot turn into one
        subprocess spawn per request.

    AZURE_PROXY_STATIC_TOKEN short-circuits everything. The test suite sets it so
    the proxy can run against fake upstreams without an Azure login.
    """

    SAFETY_SLACK = 30.0         # stop trusting a token this long before expiry
    MIN_RETRY_INTERVAL = 10.0   # never spawn `az` more often than this

    def __init__(self, scope: str, margin: float, fetch=None):
        self._scope = scope
        self._margin = float(margin)
        self._static = os.environ.get("AZURE_PROXY_STATIC_TOKEN")
        self._credential = None
        if not self._static and fetch is None:
            self._credential = AzureCliCredential()
        self._fetch = fetch or self._fetch_via_cli

        self._token: Optional[str] = None
        self._expires_at = 0.0
        self._refresh_at = 0.0
        self._last_attempt = 0.0
        self._last_error: Optional[str] = None
        self._lock = asyncio.Lock()

    def _fetch_via_cli(self):
        token = self._credential.get_token(self._scope)
        return token.token, float(token.expires_on)

    # -- state ------------------------------------------------------------
    def _usable(self, now: float) -> bool:
        return bool(self._token) and now < self._expires_at - self.SAFETY_SLACK

    def expires_in(self) -> float:
        return max(0.0, self._expires_at - time.time())

    def status(self) -> dict:
        if self._static:
            return {"have_token": True, "expires_in_seconds": 3600, "last_error": None}
        return {"have_token": bool(self._token),
                "expires_in_seconds": round(self.expires_in(), 1),
                "last_error": self._last_error}

    # -- refresh ----------------------------------------------------------
    async def _refresh(self, now: float) -> bool:
        if now - self._last_attempt < self.MIN_RETRY_INTERVAL:
            return False
        self._last_attempt = now
        had_error = self._last_error
        try:
            token, expires_at = await asyncio.to_thread(self._fetch)
        except Exception as e:
            self._last_error = "{}: {}".format(type(e).__name__, e)
            # Only log the transition. The background refresher runs every 30s,
            # and a persistent failure must not fill the log with one line per
            # tick.
            if self._last_error != had_error:
                if self._usable(time.time()):
                    _ev("token", "warning",
                        "token refresh failed: %s (still holding a "
                        "token good for %.1fm)",
                        self._last_error, self.expires_in() / 60,
                        ok=False, expires_in_seconds=round(self.expires_in(), 1),
                        error=self._last_error)
                else:
                    _ev("token", "error", "token refresh failed: %s — %s",
                        self._last_error, LOGIN_HINT,
                        ok=False, expires_in_seconds=0.0,
                        error=self._last_error)
            return False

        self._token = token
        self._expires_at = expires_at
        lifetime = max(0.0, expires_at - now)
        # Clamp: never treat a token as due for refresh the moment it arrives.
        margin = min(self._margin, lifetime / 2)
        self._refresh_at = expires_at - margin
        self._last_error = None
        _ev("token", "info", "token refreshed, expires in %.1fm", lifetime / 60,
            ok=True, expires_in_seconds=round(lifetime, 1))
        return True

    async def get(self) -> str:
        if self._static:
            return self._static
        now = time.time()
        if self._usable(now) and now < self._refresh_at:
            return self._token

        async with self._lock:
            now = time.time()
            if self._usable(now) and now < self._refresh_at:
                return self._token
            await self._refresh(now)
            if self._usable(time.time()):
                # Either the refresh worked, or it failed but what we already
                # hold is still good. Both are fine; only the second leaves
                # _last_error set, which /healthz reports.
                return self._token
            raise TokenUnavailable(self._last_error or "no token available")

    async def run_background_refresh(self, interval: float = 30.0):
        """Keep the token warm so no request pays for the `az` subprocess."""
        if self._static:
            return
        while True:
            try:
                await asyncio.sleep(interval)
                now = time.time()
                if not self._usable(now) or now >= self._refresh_at:
                    async with self._lock:
                        await self._refresh(time.time())
            except asyncio.CancelledError:
                raise
            except Exception:
                pass        # a background refresh failure is retried next tick


def _setup_logging(level: str):
    """One logger, on stdout, which start.sh appends to proxy.log.

    Bodies are never logged at any level: prompts are user data, and a proxy
    that quietly archives them would be a worse problem than anything it helps
    debug.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    log.handlers[:] = [handler]
    log.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    log.propagate = False


PIDFILE = os.path.join(ROOT, ".proxy.pid")


_claim = None


def write_pidfile() -> None:
    global _claim
    _claim = ProcessClaim(ROOT, "serving")


def clear_pidfile() -> None:
    global _claim
    if _claim:
        _claim.close()
        _claim = None


cfg = ConfigView(Config(load_routes=False))
_setup_logging(cfg.log_level)
tokens = TokenCache(cfg.scope, cfg.refresh_margin)
# One token cache per distinct data-plane scope. `tokens` is the default-scope
# cache and stays the common path; an endpoint that pins its own scope (the
# AI-Foundry project path uses ai.azure.com) gets its own cache here, and a
# route names its scope so _forward can mint the matching token per attempt.
TOKEN_CACHES: Dict[str, TokenCache] = {cfg.scope: tokens}
for _scope in cfg.scopes:
    TOKEN_CACHES.setdefault(_scope, TokenCache(_scope, cfg.refresh_margin))


def token_cache_for(scope: Optional[str]) -> TokenCache:
    """The cache for a route's scope; the default cache when it names none."""
    scope = scope or cfg.scope
    if scope not in TOKEN_CACHES:
        cache = TOKEN_CACHES[scope] = TokenCache(scope, cfg.refresh_margin)
        if client is not None:
            _refreshers.append(asyncio.create_task(cache.run_background_refresh()))
    return TOKEN_CACHES[scope]


bridge = ServingBridge(ROOT, cfg)
telemetry = Telemetry(bridge)
affinity = SessionAffinity(cfg)
app = FastAPI(title="azure-proxy", docs_url=None, redoc_url=None)
client: Optional[httpx.AsyncClient] = None
_refreshers: List[asyncio.Task] = []
STARTED_AT = time.time()
bridge.sessions = affinity.report
app.add_middleware(SnapshotMiddleware, config=cfg)


def _install_scopes(candidate):
    for scope in candidate.scopes:
        token_cache_for(scope)


bridge.on_config = _install_scopes


def _az_account() -> Optional[str]:
    """Who the proxy's own credential directory is logged in as.

    Worth one subprocess at boot: on a shared machine the single most likely
    cause of a sudden wall of 401s is that the identity changed, and that is
    invisible unless something says so out loud.
    """
    if os.environ.get("AZURE_PROXY_STATIC_TOKEN"):
        return None
    try:
        out = subprocess.check_output(
            ["az", "account", "show", "--query", "user.name", "-o", "tsv"],
            stderr=subprocess.DEVNULL, timeout=20)
        return out.decode().strip() or None
    except Exception:
        return None


def _log_startup():
    log.info("listening on http://%s:%s", cfg.host, cfg.port)
    log.info("az config dir: %s", cfg.az_config_dir or "~/.azure (shared!)")
    # The one deviation from passthrough, so say it out loud once. If a run
    # produces something inexplicable, this line is what tells you the proxy
    # had its hands on the request body at all.
    log.info("responses compat rewrites: %s",
             "on" if cfg.responses_compat else "off (raw Azure validation)")
    # The other thing that decides where a request lands. Anything but strict
    # priority makes the answer to "which endpoint served this?" depend on state
    # rather than on config, so it has to be visible at the top of the log
    # rather than inferred from the traffic.
    if cfg.balance_configured != cfg.balance:
        if cfg.balance_configured in BALANCE_ALIASES:
            log.info("routing.balance=%r is the old name for %s",
                     cfg.balance_configured, cfg.balance)
        else:
            log.warning("routing.balance=%r is not a known mode; using %s",
                        cfg.balance_configured, cfg.balance)
    log.info("routing balance: %s", cfg.balance)
    if cfg.balance == "priority_threshold":
        log.info("spill threshold: %.0f%% of each route's own quota, "
                 "measured over %.0fs of this proxy's own traffic",
                 cfg.spill_threshold * 100, cfg.load_window)
    if cfg.balance != "strict_priority" and cfg.static_weights:
        log.info("cold-start weights: %s",
                 ", ".join("{}={}".format(k, v)
                           for k, v in sorted(cfg.static_weights.items())))

    account = _az_account()
    if account is None:
        pass                    # static token, or `az` could not answer
    elif cfg.expected_account and account != cfg.expected_account:
        log.warning("az account is %s but policy.yaml expects %s — requests "
                    "will 401 if this principal lacks the data actions. %s",
                    account, cfg.expected_account, LOGIN_HINT)
    else:
        log.info("az account: %s", account)

    status = tokens.status()
    if status["have_token"]:
        log.info("token good for %.1fm", status["expires_in_seconds"] / 60)

    for name, chat, responses, image in cfg.endpoints:
        log.info("endpoint %-28s chat=%-22s responses=%-14s image=%s",
                 name, chat, responses, image)
    # One record for the whole of the above rather than fifteen. The startup
    # narration is a dozen lines because a person reading a log wants them
    # separately; an event stream wants the one line that says the proxy came
    # up, and everything those dozen lines established is already in /healthz.
    _ev("boot", "info",
        "%d models on /v1/chat/completions, %d on /v1/responses, "
        "%d on /v1/images/*, probed at %s",
        len(cfg.routes), len(cfg.responses_routes), len(cfg.image_routes),
        cfg.generated_at,
        chat_models=len(cfg.routes), responses_models=len(cfg.responses_routes),
        image_models=len(cfg.image_routes),
        balance=cfg.balance, endpoints=[e[0] for e in cfg.endpoints],
        account=account, probed_at=cfg.generated_at)


@app.on_event("startup")
async def _startup():
    global client
    await bridge.start()
    client = httpx.AsyncClient(timeout=httpx.Timeout(cfg.timeout, connect=15.0))
    # Prime every scope's token at boot rather than on the first request, so a
    # bad login fails loudly here. One background refresher per cache.
    for cache in TOKEN_CACHES.values():
        await cache.get()
    _refreshers[:] = [asyncio.create_task(cache.run_background_refresh())
                      for cache in TOKEN_CACHES.values()]
    _log_startup()


@app.on_event("shutdown")
async def _shutdown():
    for task in _refreshers:
        task.cancel()
    if client:
        await client.aclose()
    await bridge.stop()
    # Uvicorn may re-raise SIGTERM after its graceful shutdown has completed.
    # Release ownership during lifespan shutdown as well as through atexit.
    clear_pidfile()


@app.get("/healthz")
async def healthz():
    token = tokens.status()
    # Healthy means every scope in use has a live token, not just the default:
    # a dead ai.azure.com login would take the AI-Foundry routes down while the
    # default scope still looked fine.
    per_scope = {scope: cache.status() for scope, cache in TOKEN_CACHES.items()}
    ok = all(s["have_token"] and s["expires_in_seconds"] > 0
             for s in per_scope.values())
    return {"ok": ok,
            "role": "serving",
            "pid": os.getpid(),
            "routing": bridge.status(),
            "models": len(cfg.routes),
            "responses_models": len(cfg.responses_routes),
            "image_models": len(cfg.image_routes),
            "probed_at": cfg.generated_at,
            "az_config_dir": cfg.az_config_dir,
            "responses_compat": cfg.responses_compat,
            "balance": cfg.balance,
            "spill_threshold": cfg.spill_threshold,
            # For the dashboard header: where to say the proxy is listening,
            # how long it has been up, and over how long a window the load
            # figures on /routes are averaged. All three are already knowable
            # from the config file and the process table; having them here is
            # what lets a reader attached over HTTP alone say them.
            "host": cfg.host,
            "port": cfg.port,
            "started_at": STARTED_AT,
            "uptime_seconds": round(time.time() - STARTED_AT, 1),
            "load_window_seconds": cfg.load_window,
            "session_affinity": affinity.report(),
            "token": token,
            "tokens": per_scope}


@app.get("/routes")
async def routes_report():
    report = dict((bridge.snapshot or {}).get("report", {}))
    report["session_affinity"] = affinity.report()
    report["routing"] = bridge.status()
    report["stats_stale"] = (not report["routing"]["ok"]
                             or bool(report["routing"]["telemetry_dropped"]))
    return report


@app.get("/events")
async def events_feed(since: int = 0, limit: int = 500,
                      kind: Optional[str] = None):
    feed = (bridge.snapshot or {}).get("events", {})
    held = feed.get("events", [])
    fresh = [event for event in held if event["seq"] > since]
    cursor = feed.get("next", 0)
    dropped = bool(since > 0 and held and held[0]["seq"] > since + 1)
    kinds = (PROBLEM_KINDS if kind == "problems" else
             frozenset(k.strip() for k in kind.split(",")) if kind else None)
    if kinds is not None:
        fresh = [event for event in fresh if event["kind"] in kinds]
    if limit > 0 and len(fresh) > limit:
        fresh = fresh[-limit:]
        dropped = True
    return {"events": fresh, "next": cursor, "dropped": dropped,
            "counts": feed.get("counts", {}), "stats_stale": not bridge.status()["ok"]}


@app.get("/v1/models")
async def list_models():
    # The union of every face, not the chat one. A model can be served on
    # /v1/responses and nowhere else — the pro and codex deployments are — and
    # the gpt-image-* deployments are on the images faces and nowhere at all
    # otherwise. Listing only what chat can reach would leave them
    # undiscoverable.
    tables = (("chat", cfg.routes), ("responses", cfg.responses_routes),
              ("image", cfg.image_routes), ("image_edits", cfg.image_edit_routes))
    names = sorted(set(cfg.routes) | set(cfg.responses_routes)
                   | set(cfg.image_routes))
    return {
        "object": "list",
        "data": [
            {
                "id": name,
                "object": "model",
                "owned_by": "azure",
                # Not part of the OpenAI schema; harmless to clients and the
                # fastest way to see a model's failover depth and which of the
                # faces it can be reached on.
                "routes": [r.endpoint for r in
                           (cfg.routes.get(name)
                            or cfg.responses_routes.get(name)
                            or cfg.image_routes.get(name) or [])],
                "faces": [f for f, table in tables if name in table],
            }
            for name in names
        ],
    }


def _upstream_headers(incoming, token: str,
                      content_type: str = "application/json") -> dict:
    """Ours first, then the caller's, minus the ones that are not theirs to set.

    `content_type` is a parameter because /v1/images/edits is multipart: its
    body is relayed byte for byte, so the boundary the client chose has to
    travel with it. It still cannot come from `incoming` — content-type is in
    STRIP_REQUEST_HEADERS precisely so that a forwarded one cannot arrive
    alongside ours as `application/json,application/json`.
    """
    headers = {"Authorization": "Bearer " + token,
               "Content-Type": content_type}
    if cfg.forward_headers:
        for k, v in incoming.items():
            if k.lower() not in STRIP_REQUEST_HEADERS:
                headers.setdefault(k, v)
    return headers


def _relay_headers(resp: httpx.Response, route: Route) -> dict:
    headers = {k: v for k, v in resp.headers.items()
               if k.lower() not in STRIP_RESPONSE_HEADERS}
    headers["x-azure-proxy-route"] = str(route)
    return headers


def _relay(resp: httpx.Response, route: Route, request_bytes: int = 0,
           entry: Optional[Attempt] = None,
           counted: bool = False) -> Response:
    body = resp.content
    limited = counted
    if resp.status_code == 200 and not counted:
        reason = _inband_error(body)
        if reason:
            # The buffered face has never been seen to do this — the live
            # sampling on 2026-08-20 found it only on streamed calls — but the
            # check costs one substring search on a body already in memory, and
            # if it ever does happen the alternative is scoring a throttled
            # deployment as healthy.
            _ev("throttle", "warning",
                "!! %s returned 200 carrying %s; demoting", route, reason,
                route=route, inband=True, reason=reason, buffered=True)
            telemetry.demote(route, "429", resp.headers.get("retry-after"),
                         entry)
            limited = True
    telemetry.settle(route, entry, request_bytes, _read_total_tokens(body))
    if resp.status_code < 400 and not limited:
        telemetry.note_success(route, entry)
    return Response(content=body, status_code=resp.status_code,
                    headers=_relay_headers(resp, route),
                    media_type=resp.headers.get("content-type"))


# Azure refuses a streaming Responses call with HTTP 200, not 429. Captured off
# the live service on 2026-08-20 by pushing endpoint-a past its TPM ceiling
# (/tmp capture, 5 of 8 concurrent requests refused):
#
#   HTTP/1.1 200 OK
#   retry-after: 4
#   x-ratelimit-remaining-tokens: -27689        <- negative
#   x-ratelimit-reset-tokens: 64
#
#   event: response.created      ... {"status":"in_progress", ...}
#   event: error                 ... {"code":"rate_limit_exceeded", "message":
#                                     "Your requests to gpt-5.6-sol for
#                                      gpt-5.6-sol in swedencentral have
#                                      exceeded rate limit."}
#   event: response.failed       ... {"status":"failed", ...}
#
# The refusal is in the HEADERS. `retry-after` was present on all five refused
# responses and on none of the three that succeeded, which makes it a clean
# discriminator available at exactly the moment a 429's status line would be —
# before a single body byte, at zero latency, and independent of how big the
# stream or its preamble turns out to be. That last part is what matters: the
# first attempt at this scanned the body instead, and a codex-shaped
# response.created is larger than any sane scan window, so the marker sat past
# the end of it and 184 refusals in one run were logged as successes.
#
# Negative x-ratelimit-remaining-tokens is NOT a substitute: one of the three
# successful responses reported -27978 and streamed to completion anyway. The
# counter goes negative when the window is oversubscribed; `retry-after` is
# Azure actually declining to serve this request.
THROTTLE_HEADER = "retry-after"

# Kept as a backstop, and as the only thing that can catch a refusal that
# arrives without the header. `retry_on_codes` in policy.yaml is what to edit if
# Azure introduces another code that means "come back later".
INBAND_RATE_LIMIT = b"rate_limit_exceeded"

# Azure refuses a request the same way it throttles one: HTTP 200, then an
# `error` event and `response.failed`. A proxy that only records status lines
# writes that down as a healthy 200 — which is why, during the 2026-08-25
# subagent outage, every client saw
#
#   stream disconnected before completion: Encrypted function output content
#   could not be decrypted or decoded.
#
# and `grep -i "could not be decrypted" proxy.log` returned nothing at all. The
# refusal is not in the head either: hold_bytes is 16KB and a codex-shaped
# response.created runs to 46KB, so the probe is long finished by the time the
# error event goes past. It has to be read off the relay, which is the only
# place that sees every byte.
#
# Read, logged, and otherwise left alone: the stream still belongs to the
# caller and is forwarded unchanged.
_STREAM_ERROR_EVENT = re.compile(rb"(?:\A|\n)event:[ \t]*error\r?\n")
_STREAM_ERROR_CODE = re.compile(rb'"code"\s*:\s*"([A-Za-z0-9_.-]{3,64})"')
_STREAM_ERROR_MESSAGE = re.compile(rb'"message"\s*:\s*"((?:[^"\\]|\\.){0,240})"')

# How much of what follows an `error` event to keep in order to read its code
# and message out. The event itself is a few hundred bytes; the rest is the
# `response.failed` that follows it.
ERROR_TAIL_BYTES = 4096

# The event that says a stream is a whole answer. Its absence is the only thing
# that separates "the turn is done" from "the connection stopped" — measured
# 2026-08-25, an upstream ended a stream mid-answer with no error event at all,
# a fully-formed function call and nothing after it, and the caller reported it
# as "stream disconnected before completion" with no reason available anywhere.
STREAM_COMPLETED = b"response.completed"

# What Azure says when ciphertext reaches an endpoint that cannot read it. Two
# messages, one code: "The encrypted content for item rs_… could not be
# verified" for a reasoning item on the buffered face, and "Encrypted function
# output content could not be decrypted or decoded" for the `agent_message` a
# subagent inherits from its parent, in-stream.
ENCRYPTED_REJECTED = b"invalid_encrypted_content"

# Events that can precede the real answer without being any of it. A stream that
# has produced only these has told the caller nothing, which is what makes it
# safe to throw away and start again somewhere else; anything outside this set
# means generation is under way and the stream is now the caller's. Listing the
# preamble rather than the content is the safe direction to be wrong in: an
# event type nobody here has heard of counts as content and ends the probe,
# which costs at most a missed failover, never a spliced stream.
STREAM_PREAMBLE_EVENTS = (b"response.created", b"response.in_progress",
                          b"response.queued")

# Long enough to bridge any marker this file looks for across a chunk boundary.
OVERLAP_BYTES = 48

_TOTAL_TOKENS = re.compile(rb'"total_tokens"\s*:\s*(\d+)')
_EVENT_SPLIT = re.compile(rb"\r?\n\r?\n")
_COMPLETED_EVENT = re.compile(
    rb'(?:^|\n)(?:event:[ \t]*response\.completed[ \t]*\r?(?:\n|$)'
    rb'|data:[ \t]*\{[ \t]*"type"[ \t]*:[ \t]*"response\.completed")')


def _throttled_200(resp: httpx.Response) -> bool:
    """Is this 200 actually a refusal? See THROTTLE_HEADER."""
    return resp.status_code == 200 and THROTTLE_HEADER in resp.headers


def _inband_error(blob: bytes) -> Optional[str]:
    """The retryable in-band error code in `blob`, if there is one."""
    for marker in cfg.stream_retry_markers:
        if marker in blob:
            return marker.decode("ascii", "replace")
    return None


def _has_content_event(blob: bytes) -> bool:
    """Has a complete SSE event gone past that is not part of the preamble?

    Only complete events count — the trailing fragment is dropped, because half
    an event says nothing about which type it will turn out to be.
    """
    events = _EVENT_SPLIT.split(blob)[:-1]
    return any(event.strip() and not any(p in event
                                         for p in STREAM_PREAMBLE_EVENTS)
               for event in events)


def _read_total_tokens(blob: bytes) -> Optional[int]:
    """What the response says it cost, read off the bytes without parsing them.

    A byte scan rather than json.loads: the same look-but-do-not-touch rule the
    in-band rate limit check follows, and the only one available on the
    streaming path where re-serialising would destroy the encrypted reasoning
    codex depends on. The last match wins — usage is stated once, at the end.
    """
    match = None
    for match in _TOTAL_TOKENS.finditer(blob):
        pass
    return int(match.group(1)) if match else None


class _StreamWatch:
    """Watches a stream go past without altering, delaying or reframing it.

    Answers three questions from raw bytes: is this 200 actually a throttle,
    what did it cost, and did the upstream fail the turn on its way through.
    Carries a few bytes of overlap between chunks so no marker can hide on a
    chunk boundary.

    The scan is NOT bounded to the head of the stream any more, and that was the
    bug that made the first version of this useless. It used to stop after 16KB
    on the theory that a refusal always arrives early. A refusal does arrive
    early in *event order* — but a codex-shaped `response.created` echoes the
    whole request back and is itself larger than 16KB, so the marker landed past
    the end of the window and 184 refusals in a single run were counted as
    successes. A bytes.find per chunk costs nothing next to the HTTP it is
    already doing; guessing where the interesting part is costs correctness.
    """

    __slots__ = ("_overlap", "rate_limited", "total_tokens", "_error_tail",
                 "completed", "_completion_tail", "_completion_pending")

    def __init__(self):
        self._overlap = b""
        self.rate_limited = False
        self.total_tokens: Optional[int] = None
        # Did the upstream ever say it had finished? A stream that stops
        # without this is a turn the client reports as "disconnected before
        # completion", whether or not anything explained why.
        self.completed = False
        self._completion_tail = b""
        self._completion_pending = False
        # The bytes that followed an `error` event, capped at ERROR_TAIL_BYTES.
        # None until one goes past. Accumulating a bounded tail is what makes
        # the code readable whichever chunk boundary it lands on; the marker
        # itself is short enough for the overlap to bridge.
        self._error_tail: Optional[bytearray] = None

    def feed(self, chunk: bytes) -> bool:
        """Returns True on the chunk that first reveals a rate limit."""
        window = self._overlap + chunk
        found = False
        if not self.rate_limited and _inband_error(window):
            self.rate_limited = found = True
        total = _read_total_tokens(window)
        if total is not None:
            self.total_tokens = total
        if not self.completed:
            # Confirm a terminal event only after its blank-line delimiter.
            # Keep a bounded tail of the current event so large response bodies
            # and markers split across network chunks need no full buffering.
            parts = _EVENT_SPLIT.split(self._completion_tail + chunk)
            for event in parts[:-1]:
                if self._completion_pending or _COMPLETED_EVENT.search(event):
                    self.completed = True
                self._completion_pending = False
            tail = parts[-1]
            self._completion_pending |= bool(_COMPLETED_EVENT.search(tail))
            self._completion_tail = tail[-OVERLAP_BYTES:]
        if self._error_tail is None:
            match = _STREAM_ERROR_EVENT.search(window)
            if match:
                self._error_tail = bytearray(window[match.end():])
        elif len(self._error_tail) < ERROR_TAIL_BYTES:
            self._error_tail.extend(chunk)
        self._overlap = window[-OVERLAP_BYTES:]
        return found

    def upstream_error(self) -> Optional[Tuple[str, str]]:
        """(code, message) of the in-band error this stream carried, if any."""
        if self._error_tail is None:
            return None
        window = bytes(self._error_tail[:ERROR_TAIL_BYTES])
        code = _STREAM_ERROR_CODE.search(window)
        message = _STREAM_ERROR_MESSAGE.search(window)
        return (code.group(1).decode("ascii", "replace") if code else "unknown",
                message.group(1).decode("utf-8", "replace") if message else "")


_captured = [0]

# Anything that looks like an id the model minted and the client is handing
# back. `rs_` is a reasoning item; the encrypted blob rides inside one.
_STATE_ID = re.compile(r'"id"\s*:\s*"((?:rs|msg|fc|rsp)_[^"]{4,})"')


def _capture_request(face: str, request: Request, body: dict) -> None:
    """Record the SHAPE of one request, for diagnosis only.

    Metadata, never content: header names and values, which top-level keys were
    present, which item types are in `input`, and the ids the client is handing
    back. No prompt text, no tool descriptions, no message content — enough to
    answer "what can this proxy key a session on?" and nothing else.

    Gated on the same server.capture_dir switch as _capture_stream, and subject
    to the same limit.
    """
    if not cfg.capture_dir or _captured[0] >= cfg.capture_limit:
        return
    try:
        items = body.get("input")
        items = items if isinstance(items, list) else []
        blob = json.dumps(body)
        shape = {
            "face": face,
            "at": time.strftime("%H:%M:%S"),
            "headers": {k: v for k, v in request.headers.items()
                        if k.lower() not in ("authorization", "api-key")},
            "body_keys": sorted(body),
            "model": body.get("model"),
            "stream": body.get("stream"),
            "store": body.get("store"),
            "include": body.get("include"),
            "prompt_cache_key": body.get("prompt_cache_key"),
            "previous_response_id": body.get("previous_response_id"),
            "client_metadata": body.get("client_metadata"),
            "input_len": len(items),
            "input_types": [i.get("type") for i in items
                            if isinstance(i, dict)][:40],
            "state_ids": sorted(set(_STATE_ID.findall(blob)))[:20],
            "has_encrypted": "encrypted_content" in blob,
            "body_bytes": len(blob),
        }
        os.makedirs(cfg.capture_dir, mode=0o700, exist_ok=True)
        _captured[0] += 1
        path = os.path.join(cfg.capture_dir,
                            "req-{:03d}.json".format(_captured[0]))
        with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                  "w") as f:
            json.dump(shape, f, indent=2, sort_keys=True)
    except Exception as e:                  # diagnostics never break traffic
        log.warning("request capture failed: %s", e)


def _capture_stream(route: Route, resp: httpx.Response, blob: bytes) -> None:
    """Write one raw upstream stream to disk, for diagnosis only.

    OFF unless `server.capture_dir` is set, and it must be left off: these files
    contain prompts, which are user data, and the whole reason this proxy does
    not log bodies is that one quietly archiving them is worse than anything it
    helps debug.

    It exists because the alternative is guessing. The in-band throttle was
    mis-diagnosed twice from log lines alone; five minutes with the actual bytes
    on disk settled it. Turn it on, reproduce, read, turn it off, delete the
    files. `capture_mode: suspicious` keeps only streams that never reached
    response.completed, which is both the interesting set and the small one.
    """
    if not cfg.capture_dir or _captured[0] >= cfg.capture_limit:
        return
    healthy = b"response.completed" in blob
    if cfg.capture_mode != "all" and healthy:
        return
    try:
        os.makedirs(cfg.capture_dir, mode=0o700, exist_ok=True)
        _captured[0] += 1
        path = os.path.join(cfg.capture_dir, "{}-{:03d}-{}.sse".format(
            "ok" if healthy else "bad", _captured[0],
            str(route).replace("/", "_")))
        with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                  "wb") as f:
            f.write(("### route=%s status=%s bytes=%d completed=%s\n"
                     "### headers=%r\n\n" % (
                         route, resp.status_code, len(blob), healthy,
                         dict(resp.headers))).encode())
            f.write(blob)
        log.warning("captured %s stream to %s",
                    "healthy" if healthy else "bad", path)
    except Exception as e:                  # diagnostics never break traffic
        log.warning("capture failed: %s", e)


class _StreamHead:
    """What was read from a stream before deciding whether to keep it.

    `pending` is a read that was still outstanding when the probe stopped
    waiting. It is handed on rather than cancelled: cancelling a read in flight
    would leave the connection in a state httpx cannot resume from, and the
    point of the probe is to be able to walk away *without* damage.
    """

    __slots__ = ("aiter", "chunks", "pending", "retry_reason", "error",
                 "timed_out")

    def __init__(self, aiter):
        self.aiter = aiter
        self.chunks: List[bytes] = []
        self.pending = None
        self.retry_reason: Optional[str] = None
        self.error: Optional[Exception] = None
        self.timed_out = False


async def _probe_stream_head(resp: httpx.Response) -> _StreamHead:
    """Hold the first bytes of a stream back long enough to see what it is.

    "The stream has started" and "the caller has content" are not the same
    event, and the gap between them is the only place a throttled 200 can still
    be undone. So the head of the body is read here, into memory, before any of
    it is handed downstream. If it turns out to be an in-band error the whole
    thing is dropped and the request goes to another endpoint with the caller
    none the wiser; if it turns out to be a real answer every byte is released
    in the chunks it arrived in, and nothing is ever inspected again in a way
    that could change it.

    The window is bounded three ways, because this is a streaming interface and
    latency is the reason it exists:

      * it ends the moment a complete non-preamble event arrives, which is what
        a healthy stream produces first. That is the common case and it costs
        nothing.
      * it ends after `stream_probe.hold_seconds`. A throttle is generated
        before any model work happens and arrives in the same burst as
        response.created; a real answer that has not started after half a second
        is not going to be helped by holding it longer.
      * it ends after `stream_probe.hold_bytes`, so a stream that opens with a
        wall of preamble cannot be buffered without limit.

    Whichever ends it, the held bytes are released unchanged and in order.
    """
    head = _StreamHead(resp.aiter_bytes().__aiter__())
    if cfg.probe_seconds <= 0 or cfg.probe_bytes <= 0:
        return head

    loop = asyncio.get_event_loop()
    deadline = loop.time() + cfg.probe_seconds
    buffered = b""
    while len(buffered) < cfg.probe_bytes:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        task = asyncio.ensure_future(head.aiter.__anext__())
        done, _ = await asyncio.wait({task}, timeout=remaining)
        if not done:
            head.pending = task
            break
        try:
            chunk = task.result()
        except StopAsyncIteration:
            break                       # upstream finished inside the window
        except httpx.TimeoutException as e:
            # Cannot happen at the default settings — the probe gives up long
            # before the request timeout — but a timeout is never retried, so
            # say which one this is rather than letting it look like a reset.
            head.error, head.timed_out = e, True
            break
        except httpx.HTTPError as e:
            head.error = e
            break
        head.chunks.append(chunk)
        buffered += chunk

        head.retry_reason = _inband_error(buffered)
        if head.retry_reason or _has_content_event(buffered):
            break
    return head


def _relay_stream(resp: httpx.Response, route: Route, face: str, started: float,
                  head: _StreamHead, request_bytes: int = 0,
                  entry: Optional[Attempt] = None,
                  counted: bool = False,
                  model: Optional[str] = None,
                  family: Optional[str] = None) -> StreamingResponse:
    """Forward the upstream body byte for byte as it arrives.

    Nothing here parses SSE, reframes it, or waits for it. Passing the bytes
    through unexamined is what keeps fields the proxy has never heard of —
    encrypted reasoning among them — intact on the way back. The bytes the probe
    already read go out first, in the chunks they arrived in.

    The scan looks but does not touch. Its answers only reach the quota tracker
    and the log, which is the whole point: past this line the stream belongs to
    the caller and cannot be retried, so the only thing left to influence is
    where the *next* request goes. `counted` says a rate limit was already
    charged during the probe, so a stream relayed anyway on the last route is
    not counted twice.

    A stream that ends in an `error` event is a failed turn wearing a 200, and
    it is written down as one. `family` is the client session it belongs to: an
    `invalid_encrypted_content` refusal is remembered against it, so every
    later turn of that session goes out without the ciphertext this one was
    refused for, whatever the routing decides.
    """
    if entry:
        entry.streaming = True

    async def body():
        sent = 0
        # Whether the upstream iterator ran out, as opposed to this generator
        # being closed early. Only the first case says anything about the
        # upstream: a client that hangs up mid-turn also lands in `finally`,
        # and warning about that would blame the wrong end.
        drained = False
        success_recorded = False
        watch = _StreamWatch()
        watch.rate_limited = counted
        # Only accumulated when diagnostics are on; otherwise it stays empty and
        # the stream is forwarded without ever being held in memory.
        keep = bool(cfg.capture_dir)
        seen = bytearray() if keep else None

        def record_success() -> None:
            nonlocal success_recorded
            if not success_recorded and not watch.rate_limited \
                    and not watch.upstream_error():
                telemetry.settle(route, entry, request_bytes, watch.total_tokens)
                telemetry.note_success(route, entry)
                success_recorded = True

        def inspect(chunk: bytes) -> None:
            if keep:
                seen.extend(chunk)
            if watch.feed(chunk):
                _ev("throttle", "warning",
                    "!! %s returned 200 carrying %s after %d bytes; "
                    "too late to retry, demoting for the next request",
                    route, INBAND_RATE_LIMIT.decode(), sent,
                    route=route, face=face, model=model, inband=True,
                    too_late=True, bytes=sent)
                telemetry.demote(route, "429", resp.headers.get("retry-after"),
                             entry)
            if watch.completed:
                # Clients may close immediately after receiving this event.
                # Record it before yielding, while cleanup and EOF are pending.
                record_success()

        try:
            for chunk in head.chunks:
                inspect(chunk)
                sent += len(chunk)
                yield chunk
            if head.pending is not None:
                # The read the probe stopped waiting for. Awaited, never
                # cancelled, so the connection stays usable.
                try:
                    chunk = await head.pending
                except StopAsyncIteration:
                    chunk = b""
                if chunk:
                    inspect(chunk)
                    sent += len(chunk)
                    yield chunk
            async for chunk in head.aiter:
                inspect(chunk)
                sent += len(chunk)
                yield chunk
            drained = True
        except httpx.HTTPError as e:
            # No failover once the client has bytes: a second attempt would
            # splice two streams together in its parser. The break is passed on
            # as a break, and the reason goes here.
            _ev("response", "warning",
                "%s stream broke after %d bytes on %s: %s",
                face, sent, route, e,
                route=route, face=face, model=model, bytes=sent,
                broke=True, error=str(e),
                seconds=round(time.monotonic() - started, 1))
            raise
        finally:
            if drained and face != RESPONSES_FACE:
                record_success()
            if not success_recorded:
                telemetry.settle(route, entry, request_bytes, watch.total_tokens)
            telemetry.finish(entry)
            await resp.aclose()
            if keep:
                _capture_stream(route, resp, bytes(seen))
            _note_upstream_error(watch.upstream_error(), route, face, model,
                                 family, sent)
            if drained and not watch.completed and not watch.upstream_error():
                # The upstream stopped mid-answer and said nothing about why.
                # Unretryable — the caller already has the bytes, and splicing
                # a second attempt onto them would corrupt its parser — but it
                # must not be filed as a clean 200 either, which is what the
                # line below on its own would do.
                _ev("upstream_error", "warning",
                    "!! %s stopped after %d bytes without response.completed "
                    "and without an error; the caller sees a turn that ended "
                    "early", route, sent,
                    route=route, face=face, model=model,
                    error_code="truncated", bytes=sent, in_stream=True)
            _ev("response", "info",
                "<- %s stream ended %s %d bytes route=%s %.1fs",
                face, resp.status_code, sent, route,
                time.monotonic() - started,
                route=route, face=face, model=model, stream=True,
                status=resp.status_code, bytes=sent,
                seconds=round(time.monotonic() - started, 1))

    return StreamingResponse(body(), status_code=resp.status_code,
                             headers=_relay_headers(resp, route),
                             media_type=resp.headers.get("content-type"))


def _note_upstream_error(failure: Optional[Tuple[str, str]], route: Route,
                         face: str, model: Optional[str],
                         family: Optional[str], sent: int) -> None:
    """Write down an upstream refusal that arrived inside a 200.

    Rate limits are left alone: they have their own line, from the code that
    also demotes the route, and saying it twice would only make the log harder
    to read. Everything else lands here, which until now was nowhere.
    """
    if failure is None:
        return
    code, message = failure
    if code.encode() in cfg.stream_retry_markers:
        return
    # `server_error` arrives with no `message` at all — type, code, and a
    # headers object. Naming the code is the whole of what it has to say.
    _ev("upstream_error", "warning",
        "!! %s answered 200 and then failed the turn: %s%s (after %d bytes)",
        route, code, ": " + message if message else "", sent,
        route=route, face=face, model=model, error_code=code,
        error=message, bytes=sent, in_stream=True)
    if code == ENCRYPTED_REJECTED.decode() and cfg.affinity_enabled:
        affinity.note_rejected(family)


def _error(status: int, message: str, code: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": {"message": message, "type": "azure_proxy_error",
                           "code": code}})


# --------------------------------------------------------------------------
# Responses-face compatibility
# --------------------------------------------------------------------------
#
# The only place the proxy edits a request body. Azure's /responses validator
# is stricter than the one behind api.openai.com, and codex 0.148 trips it on
# every single turn — both trials of a one-task run died in
# NonZeroAgentExitCodeError before this existed. The two rewrites below were
# found by replaying one captured codex request against the backend field by
# field (2026-08-19): with them, that same request returns 200 and streams
# normally. See request.responses_compat in settings/policy.yaml.
#
# Both are shape fixes. Neither changes the prompt, the sampling parameters or
# what any tool does, which is what keeps benchmark numbers comparable.

# Azure rejects description:"" with `empty_string`; OpenAI accepts it. The
# value only has to be non-empty, so make it obviously a placeholder — it can
# show up in a captured trajectory and should not read like a real description.
EMPTY_DESCRIPTION_FILLER = "(no description)"

# codex hangs this off every input message: {turn_id, create_time}. Azure
# accepts the key but rejects the create_time inside it with
# `unknown_parameter`, so the whole thing goes. It is the client's bookkeeping
# for a stateful conversation store; this proxy has no session state, so
# nothing downstream can want it.
CLIENT_METADATA_KEY = "internal_chat_message_metadata_passthrough"


def _fill_empty_tool_descriptions(tools, stats: Dict[str, int]) -> None:
    """Replace every empty `description` in a tool list, however deep.

    codex wraps its real tools in a `type: "namespace"` tool named `functions`
    whose own description is blank; the tools that do the work hang underneath
    it in its own `tools` list, described properly. Recursing costs nothing and
    means a second level of nesting cannot reintroduce the 400.
    """
    if not isinstance(tools, list):
        return
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if tool.get("description") == "":
            tool["description"] = EMPTY_DESCRIPTION_FILLER
            stats["descriptions"] += 1
        _fill_empty_tool_descriptions(tool.get("tools"), stats)


def _apply_responses_compat(body: dict) -> Dict[str, int]:
    """Make `body` acceptable to Azure's /responses validator, in place.

    Mutating is safe: the dict came from this request's own JSON and is not
    shared with anything else. Returns what changed, for the log.
    """
    stats = {"descriptions": 0, "client_metadata": 0}

    for item in body.get("input") or []:
        if not isinstance(item, dict):
            continue
        if item.pop(CLIENT_METADATA_KEY, None) is not None:
            stats["client_metadata"] += 1
        # Where codex actually puts them — the 400 named input[0].tools[0].
        _fill_empty_tool_descriptions(item.get("tools"), stats)

    # The spec's own location for tools. Nothing has been seen to send a blank
    # description here, but the validator checks it just the same.
    _fill_empty_tool_descriptions(body.get("tools"), stats)

    return stats


def _strip_encrypted_reasoning(body: dict) -> Tuple[dict, int]:
    """`body` without the ciphertext only one endpoint can read.

    Azure encrypts with a key belonging to the resource that produced it, so
    handing it to any other endpoint earns `invalid_encrypted_content` — a 400
    on the buffered face, and on the streaming face a 200 that carries

        event: error   {"code": "invalid_encrypted_content", …}
        event: response.failed

    which codex reports as a failed turn either way. Affinity exists to make
    sure that never happens; this is what to do on the turns where it could not
    be kept, so that losing a pin costs the turn its context instead of costing
    the run.

    Two places carry it, and both have been seen live:

      * a top-level `reasoning` item with `encrypted_content` — the model's own
        chain of thought, on every codex turn after the first.
      * an `encrypted_content` entry inside another item's `content` list —
        which is how codex 0.148 hands a subagent the message its parent wrote
        for it. Missing this one is what let a stripped subagent turn fail
        anyway: the item is an `agent_message`, not a `reasoning`, so the
        type test above walked straight past it.

    So the rule is the field, not the item type. An item whose only content was
    ciphertext is dropped rather than sent with an empty `content`, which the
    validator rejects.

    Returns a copy down to every dict it modifies, because the caller's `body`
    is reused by the other attempts and `input` is a list it shares.
    """
    items = body.get("input")
    if not isinstance(items, list):
        return body, 0

    dropped = 0
    kept: List = []
    for item in items:
        if not isinstance(item, dict):
            kept.append(item)
            continue

        if item.get("type") == "reasoning" and item.get("encrypted_content"):
            dropped += 1
            continue

        content = item.get("content")
        if isinstance(content, list) and any(
                isinstance(c, dict) and c.get("encrypted_content")
                for c in content):
            clean = [c for c in content
                     if not (isinstance(c, dict) and c.get("encrypted_content"))]
            dropped += len(content) - len(clean)
            if not clean:
                continue
            item = dict(item)
            item["content"] = clean
        elif item.get("encrypted_content"):
            # Some other item type carrying it at the top. Take the field and
            # leave the item: it is the ciphertext that is unreadable, not the
            # tool call or message wrapped around it.
            dropped += 1
            item = {k: v for k, v in item.items() if k != "encrypted_content"}

        kept.append(item)

    if not dropped:
        return body, 0
    out = dict(body)
    out["input"] = kept
    return out, dropped


def _carries_encrypted(body: dict) -> bool:
    """Is there endpoint-bound ciphertext in this request's `input`?

    Same two places `_strip_encrypted_reasoning` looks, asked as a question.
    Used only to decide whether a routing decision is worth warning about:
    `include: ["reasoning.encrypted_content"]` makes a request sticky from its
    first turn, before there is anything to protect, so "sticky" on its own
    says nothing about whether this particular turn is at risk.
    """
    for item in body.get("input") or []:
        if not isinstance(item, dict):
            continue
        if item.get("encrypted_content"):
            return True
        content = item.get("content")
        if isinstance(content, list) and any(
                isinstance(c, dict) and c.get("encrypted_content")
                for c in content):
            return True
    return False


async def _body_and_model(request: Request):
    """(body, model, None) or (None, None, error response)."""
    try:
        body = await request.json()
    except Exception:
        return None, None, _error(400, "request body is not valid JSON",
                                  "invalid_json")
    requested = body.get("model")
    if not requested:
        return None, None, _error(400, "`model` is required", "missing_model")
    return body, requested, None


# --------------------------------------------------------------------------
# multipart, read but not parsed
# --------------------------------------------------------------------------
#
# /v1/images/edits arrives as multipart/form-data carrying the image, and
# usually a mask beside it. The proxy needs exactly one thing out of it — the
# value of the `model` field, so it knows which deployment to send it to — and
# then relays the body byte for byte, because Azure takes the deployment from
# the URL path and ignores a `model` that disagrees with it.
#
# So the body is scanned, not parsed. A full parser would mean a dependency
# (starlette's form() needs python-multipart, which is not in requirements.txt)
# and a re-encode of several megabytes of PNG to change nothing. The scan below
# reads one small text field out of a structure whose framing is fixed by
# RFC 7578, and gets no further into the body than the end of that field.
_MULTIPART_FIELD = re.compile(
    br'name="model"'            # the field, in the Content-Disposition line
    br'(?:;[^\r\n]*)?\r\n'      # anything else on that line: filename, etc.
    br'(?:[^\r\n]+\r\n)*'       # the part's remaining headers, if any
    br'\r\n'                    # the blank line that ends them
    br'([^\r\n]*)')             # the value, up to the CRLF before the boundary


def _multipart_model(blob: bytes) -> Optional[str]:
    """The `model` form field's value, or None if the body does not carry one."""
    found = _MULTIPART_FIELD.search(blob)
    if not found:
        return None
    try:
        return found.group(1).decode("utf-8").strip() or None
    except UnicodeDecodeError:
        return None


async def _forward(request: Request, body: dict, routes: List[Route],
                   target_of: Callable[[Route], str], requested: str,
                   face: str, raw: Optional[bytes] = None,
                   content_type: str = "application/json",
                   image_tool: bool = False) -> Response:
    """Try each route in turn until one answers. Shared by every face.

    `raw` is the body to send when it must not be re-serialised — the multipart
    of an /v1/images/edits call. `body` is then whatever could be read out of it
    for the log and the routing decision, not something that will be sent.
    """
    # The image face is not a variation on the text ones, it is a different
    # shape of failure: one deployment per model, a per-minute request ceiling
    # in the low tens, and calls that take 10-25 seconds. See Config's image
    # block for why that changes the attempt list and the wait between attempts.
    is_image = face.startswith("/v1/images/")
    image_deployments = _image_deployments(request, body)
    stream = bool(body.get("stream"))
    ledger_face = face_code(face, stream)
    _ev("request", "info", "-> %s model=%s stream=%s", face, requested, stream,
        face=face, model=requested, stream=stream,
        ledger_face=FACES[ledger_face])
    if cfg.capture_dir:
        _capture_request(face, request, body)

    try:
        token = await tokens.get()
    except TokenUnavailable as e:
        # The `az` login has lapsed. Say so instead of returning a traceback:
        # this is an operator action, not something the caller can retry into.
        _ev("token", "error", "%s model=%s rejected: no usable credential (%s)",
            face, requested, e,
            face=face, model=requested, ok=False, error=str(e))
        return _error(
            503,
            "Azure credentials unavailable ({}). {}".format(e, LOGIN_HINT),
            "credentials_unavailable")

    headers = _upstream_headers(request.headers, token, content_type)

    # Where this request is allowed to go. A conversation carrying encrypted
    # reasoning is bound to the endpoint that produced it (see SessionAffinity),
    # so once pinned its attempt list is that ONE endpoint, tried repeatedly,
    # rather than a walk across the others. Moving it would trade a slow request
    # for a guaranteed decryption failure.
    session = affinity.key(request, body) if cfg.affinity_enabled else None
    family = affinity.family(request, body) if cfg.affinity_enabled else None
    sticky = bool(session) and not is_image and affinity.sticky(body)
    session_type = affinity.session_type(body) if sticky else ""
    pinned = affinity.pinned(session, routes) if sticky else None
    # This family has been found carrying ciphertext that could not be placed,
    # so it goes out stripped wherever it goes — the client resends the same
    # unreadable items every turn, so one stripped retry is not enough. No
    # event of its own: the per-route `stripped` warning below fires whenever
    # this actually removes something, and saying it twice per turn for an hour
    # would drown the log this is meant to make readable.
    tainted = affinity.tainted(family) if sticky else False
    # The endpoint that owns this session's state, and the routes onto it.
    # Consulted only when the exact slot is empty, which for codex means the
    # first turn of a thread — including a subagent's, which arrives already
    # carrying ciphertext its parent minted. See SessionAffinity.family.
    home, local = affinity.home(family, routes) if sticky and pinned is None \
        else (None, [])
    if pinned is not None and cfg.affinity_on_conflict == "wait":
        attempts = [pinned] * max(1, 1 + cfg.affinity_attempts)
    elif pinned is not None:
        # `switch` keeps the old behaviour: try the pin first, then fail over
        # like anything else. Faster, and wrong for codex.
        attempts = [pinned] + [r for r in routes if str(r) != str(pinned)]
        attempts = attempts[:cfg.max_attempts]
    elif local:
        # This thread has no pin of its own but its session does. Balance
        # inside that endpoint — a second deployment there is a second quota,
        # and the ciphertext is readable across both — then let the rest of the
        # world follow as a fallback, stripped, so an endpoint that has gone
        # down costs the turn its inherited context rather than the turn.
        affinity.note_inherited()
        rest = [r for r in routes if r.endpoint != home]
        attempts = (list(local) + list(rest))[:cfg.max_attempts]
        _ev("inherited", "info",
            "%s model=%s has no pin of its own; its session's state is on %s, "
            "so it goes there (%d route(s))",
            face, requested, home, len(local),
            face=face, model=requested, endpoint=home, routes=len(local))
    elif is_image:
        # Cycled, not truncated. Most image models have one deployment, so a
        # permutation of the routes is a list of length one and the first 429
        # would be the caller's answer. Repeating the same route is right here
        # for the reason repeating it on a text face would be wrong: the refusal
        # is a per-minute request ceiling that refills on a clock, not a sign
        # that this destination is unwell.
        ordered = list(routes)
        attempts = [ordered[i % len(ordered)]
                    for i in range(max(1, cfg.image_attempts))]
    else:
        # Balancing decides the order; max_attempts still decides the depth, and
        # the list is a permutation of `routes`, so no endpoint is tried twice.
        attempts = list(routes)[:cfg.max_attempts]
        if sticky and family and _carries_encrypted(body):
            # Nothing knows where this ciphertext came from — a proxy restart,
            # an expired pin, or a client that opened a thread the proxy never
            # saw the parent of. It may well be rejected, so say so here rather
            # than leaving the next occurrence to be reconstructed from the
            # client's error message.
            _ev("unpinned", "warning",
                "%s model=%s carries encrypted state but has no pin and no "
                "session endpoint; routing it to %s on the balancer's word",
                face, requested, attempts[0] if attempts else "nowhere",
                face=face, model=requested,
                route=str(attempts[0]) if attempts else None)
    held = pinned is not None and cfg.affinity_on_conflict == "wait"
    if held:
        _ev("held", "info", "%s model=%s held on pinned %s (%d attempts)",
            face, requested, pinned, len(attempts),
            route=pinned, face=face, model=requested, attempts=len(attempts))

    def pause(next_route, hint):
        """How long to wait before the next attempt, and what to say about it.

        A held session is not backing off from a busy endpoint, it is queueing
        for the only one that can serve it, so Azure's own Retry-After is the
        right number rather than a blind exponential. Capped, because a pinned
        endpoint that never recovers must eventually give the error back to the
        caller rather than hang.

        An image retry is the same argument from the other direction: it is
        usually queueing for the only deployment that serves the model, and
        Azure states how long the ceiling has left to refill.
        """
        if held or is_image:
            cap = cfg.affinity_max_wait if held else cfg.image_max_wait
            wait = _as_number(hint) or cfg.backoff_initial
            wait = min(max(wait, 0.5), cap)
            return wait, "waiting for {}".format(next_route)
        return (delay + random.random() * cfg.backoff_jitter,
                "retrying {}".format(next_route))
    delay = cfg.backoff_initial
    last_error: Optional[str] = None
    retry_after_hint: Optional[str] = None
    started = time.monotonic()
    # The size of the body the caller sent, which is all the proxy knows about
    # what this request will cost before the answer comes back. See
    # QuotaTracker.estimate_tokens.
    #
    # Zero on the image face, which is not the same as unknown: it turns the
    # dispatch charge into a nominal one token and stops `settle` from learning
    # a tokens-per-byte ratio. Both are deliberate. An edits call's body is
    # megabytes of PNG that bear no relation to what it will be billed, and an
    # image deployment publishes a request ceiling and no token one — so the
    # ledger figure that matters for it is the request count, which is charged
    # by the entry existing at all.
    request_bytes = 0.0 if is_image else (
        _as_number(request.headers.get("content-length")) or 0)

    for i, route in enumerate(attempts):
        is_last = i == len(attempts) - 1
        # A route that pins a non-default scope needs its own token (the base
        # `headers` holds the default-scope one). Resolved here, before the route
        # is charged below, so a lapsed scope skips cleanly without leaving an
        # uncharged-then-unsettled entry in the ledger. Warm from the background
        # refresher, so this is a lookup rather than an `az` spawn.
        route_token = None
        if route.scope and route.scope != cfg.scope:
            try:
                route_token = await token_cache_for(route.scope).get()
            except TokenUnavailable as e:
                last_error = "no {} credential: {}".format(route.scope, e)
                _ev("token", "warning",
                    "%s model=%s skipping %s: no token for scope %s (%s)",
                    face, requested, route, route.scope, e,
                    route=route, face=face, model=requested, ok=False,
                    scope=route.scope, error=str(e))
                continue
        payload = dict(body)
        # Off the endpoint that minted it, the ciphertext in this body is
        # unreadable and Azure rejects the whole call. Two routes can read it:
        # the exact pin, and — when there is no pin yet — any deployment on the
        # endpoint this session's state lives on, which is what carries a
        # subagent's inherited `agent_message` across from its parent's model.
        # Anywhere else it has to come off, or the turn dies instead of merely
        # forgetting how it got here.
        readable = route is pinned or (pinned is None and home is not None
                                       and route.endpoint == home)
        if sticky and (tainted or not readable) \
                and cfg.affinity_off_route == "strip":
            payload, dropped = _strip_encrypted_reasoning(payload)
            if dropped:
                # Taints the family as it strips: the items are still in the
                # client's transcript and will be back next turn, on a route
                # that by then looks readable. See SessionAffinity.tainted.
                affinity.note_stripped(family)
                _ev("stripped", "warning",
                    "%s model=%s is off the endpoint that minted its state; "
                    "dropped %d encrypted item(s) so %s can answer it",
                    face, requested, dropped, route,
                    route=route, face=face, model=requested, stripped=dropped)
        payload["model"] = route.deployment
        entry = telemetry.charge(route, request_bytes, ledger_face, model=requested)
        try:
            log.debug("%s model=%s attempt %d/%d on %s",
                      face, requested, i + 1, len(attempts), route)

            # The Responses API's built-in image_generation tool needs to be told
            # which deployment to draw with, per attempt, because the answer is a
            # property of the endpoint the attempt landed on. See the handler for
            # why the attempt list is already restricted to endpoints that have one.
            attempt_headers = headers
            # A route may authenticate against a non-default scope (the AI-Foundry
            # project endpoint uses ai.azure.com); its token was resolved at the top
            # of the loop, before this route was charged, so a lapsed scope skips
            # the route rather than settling a charge it never sent.
            if route_token is not None:
                attempt_headers = dict(attempt_headers,
                                       Authorization="Bearer " + route_token)
            if image_tool:
                deployment = image_deployments.get(route.endpoint)
                if deployment:
                    attempt_headers = dict(attempt_headers,
                                           **{IMAGE_TOOL_HEADER: deployment})

            try:
                # stream=True returns as soon as the response headers are in, which
                # is what makes the retry decision possible before any body has been
                # handed to the client. A buffered reply just reads it straight back.
                #
                # `content=` rather than `json=` when the caller's body must survive
                # unchanged: an images/edits multipart, whose boundary is already in
                # the Content-Type header that came with it.
                upstream = client.build_request(
                    "POST", target_of(route), headers=attempt_headers,
                    **({"content": raw} if raw is not None else {"json": payload}))
                resp = await client.send(upstream, stream=True)
            except httpx.TimeoutException:
                # A slow reasoning call and a hung one are indistinguishable here.
                # Retrying would pay twice for the same prompt while the original
                # may still be running, so the caller decides instead. For the same
                # reason this does not demote the route: slow is not broken, and a
                # long reasoning turn must not cost an endpoint its share.
                telemetry.note_timeout(route, entry)
                if not cfg.retry_on_timeout:
                    _ev("timeout", "warning",
                        "<- 504 %s model=%s timed out after %ss on %s",
                        face, requested, cfg.timeout, route,
                        route=route, face=face, model=requested,
                        seconds=cfg.timeout, status=504)
                    return _error(504, "upstream timed out after {}s on {}".format(
                        cfg.timeout, route), "upstream_timeout")
                last_error = "timeout on {}".format(route)
            except httpx.HTTPError as e:
                telemetry.failed(route, "transport", entry)
                if not cfg.retry_on_transport_error:
                    _ev("response", "warning",
                        "<- 502 %s model=%s transport error on %s: %s",
                        face, requested, route, e,
                        route=route, face=face, model=requested,
                        status=502, error=str(e))
                    return _error(502, "transport error on {}: {}".format(route, e),
                                  "upstream_transport_error")
                last_error = "transport error on {}: {}".format(route, e)
            else:
                # Record before deciding what to do with the response: a 429 on the
                # last route is still a 429, and the demotion it earns is what keeps
                # the *next* request off this endpoint.
                telemetry.observed(route, resp.status_code, resp.headers, entry)
                retry_after_hint = resp.headers.get("retry-after")

                # Azure refuses streamed Responses calls with a 200 that carries
                # retry-after (see THROTTLE_HEADER). Treating that as retryable
                # here, off the headers, is what makes it behave exactly like a 429:
                # decided before any body is read, so the failover window does not
                # depend on how large the stream's preamble happens to be.
                throttled = _throttled_200(resp)
                retryable = resp.status_code in cfg.retry_on_status or throttled
                if retryable:
                    if throttled:
                        _ev("throttle", "warning",
                            "!! %s answered 200 + %s: %s throttled",
                            route, THROTTLE_HEADER,
                            resp.headers.get(THROTTLE_HEADER),
                            route=route, face=face, model=requested, inband=True,
                            header=resp.headers.get(THROTTLE_HEADER))
                    telemetry.demote(route, "429" if throttled else str(resp.status_code),
                                 resp.headers.get("retry-after"),
                                 entry)

                if retryable and not is_last:
                    await resp.aread()
                    await resp.aclose()
                    last_error = "{} from {}".format(
                        "200+throttled" if throttled else resp.status_code, route)
                elif stream and resp.status_code == 200:
                    # A 200 is not yet an answer on this face. Read the head of the
                    # stream before releasing any of it — see _probe_stream_head.
                    # This is now a backstop: the header check above catches the
                    # refusal Azure has actually been seen to send. It stays because
                    # it is the only thing that can catch one sent without the
                    # header, and it costs nothing when there is nothing to find.
                    head = await _probe_stream_head(resp)

                    if head.timed_out:
                        await resp.aclose()
                        telemetry.note_timeout(
                            route, entry)
                        _ev("timeout", "warning",
                            "<- 504 %s model=%s timed out on %s",
                            face, requested, route,
                            route=route, face=face, model=requested,
                            status=504, in_stream=True)
                        return _error(504, "upstream timed out after {}s on {}".format(
                            cfg.timeout, route), "upstream_timeout")

                    if head.retry_reason or head.error is not None:
                        reason = head.retry_reason or "transport"
                        # Nothing has reached the caller, so this is still a
                        # pre-first-byte failure and the ordinary failover rules
                        # apply. That is the entire point of having probed.
                        #
                        # Unless the header check above already charged for it: one
                        # refusal, one demotion. Otherwise a throttle that shows up
                        # in both places would park the route twice as hard as one
                        # that only shows up in the headers.
                        if not throttled:
                            telemetry.demote(route,
                                         "429" if head.retry_reason else reason,
                                         resp.headers.get("retry-after"),
                                         entry)
                        if not is_last:
                            await resp.aclose()
                            last_error = "in-stream {} from {}".format(reason, route)
                            wait, how = pause(attempts[i + 1],
                                              resp.headers.get("retry-after"))
                            _ev("failover", "warning",
                                "!! %s, %s in %.1fs", last_error, how, wait,
                                route=route, face=face, model=requested,
                                to_route=str(attempts[i + 1]), reason=reason,
                                wait_seconds=round(wait, 2), in_stream=True)
                            await asyncio.sleep(wait)
                            delay *= cfg.backoff_multiplier
                            continue
                        if head.error is not None:
                            # A connection that died during the probe has no body to
                            # hand over, so with nowhere left to fail over to this
                            # is the no-response case.
                            await resp.aclose()
                            last_error = "in-stream {} from {}".format(reason, route)
                            break
                        # Last route, but there is still a stream to give. Relay it:
                        # a throttled stream carrying Azure's own error event is more
                        # use to the caller than a 503 this proxy invented, and it is
                        # what a direct call would have got.
                        _ev("throttle", "warning",
                            "%s model=%s: in-stream %s on the last route "
                            "%s; relaying it", face, requested, reason, route,
                            route=route, face=face, model=requested,
                            reason=reason, in_stream=True, last_route=True)

                    if sticky:
                        affinity.pin(session, route, family, session_type)
                    return _relay_stream(resp, route, face, started, head,
                                         request_bytes, entry,
                                         counted=bool(head.retry_reason) or throttled,
                                         model=requested, family=family)
                else:
                    # Everything else is the caller's answer, including a 4xx from
                    # the deployment: parameters are forwarded untouched, so a 400
                    # about an unsupported parameter is the real, useful result.
                    #
                    # A retryable status on the LAST route is relayed too, rather
                    # than replaced with a synthetic 503. A real 429 carries
                    # Retry-After and the upstream's own message, which is what the
                    # client's backoff needs; the error below is reserved for the
                    # case where no route produced a response at all.
                    await resp.aread()
                    await resp.aclose()
                    _ev("response", "info",
                        "<- %s %s model=%s route=%s %.1fs", resp.status_code,
                        face, requested, route, time.monotonic() - started,
                        route=route, face=face, model=requested, stream=False,
                        status=resp.status_code,
                        seconds=round(time.monotonic() - started, 1))
                    # The buffered face's shape of the same refusal: a 400 whose
                    # body names invalid_encrypted_content. Same treatment — say so
                    # in the log, and make the client's retry go out stripped.
                    if sticky and ENCRYPTED_REJECTED in resp.content:
                        _ev("upstream_error", "warning",
                            "!! %s refused %s model=%s for the encrypted state it "
                            "carried (%s)", route, face, requested,
                            ENCRYPTED_REJECTED.decode(),
                            route=route, face=face, model=requested,
                            error_code=ENCRYPTED_REJECTED.decode(),
                            status=resp.status_code)
                        affinity.note_rejected(family)
                    if sticky and resp.status_code < 400:
                        affinity.pin(session, route, family, session_type)
                    return _relay(resp, route, request_bytes, entry,
                                  counted=retryable)

            if is_last:
                break
            wait, how = pause(attempts[i + 1], retry_after_hint)
            _ev("failover", "warning", "!! %s, %s in %.1fs", last_error, how, wait,
                route=route, face=face, model=requested,
                to_route=str(attempts[i + 1]), reason=last_error,
                wait_seconds=round(wait, 2))
            await asyncio.sleep(wait)
            delay *= cfg.backoff_multiplier
        finally:
            if not entry.streaming:
                telemetry.finish(entry)

    _ev("exhausted", "error",
        "<- 503 %s model=%s: all %d attempt(s) failed; last: %s",
        face, requested, len(attempts), last_error,
        face=face, model=requested, attempts=len(attempts),
        status=503, error=last_error,
        tried=[str(r) for r in attempts])
    return _error(503, "all {} attempt(s) for {!r} failed; last: {}".format(
        len(attempts), requested, last_error), "all_routes_failed")


def _other_faces(requested: str, *tables) -> List[str]:
    """The faces, by name, that can reach `requested` — for a 404 to point at.

    A name that exists on another face is not a typo, and saying "unknown
    model" sends the caller hunting for one. gpt-5-pro really is served only on
    /v1/responses; gpt-image-2 really is served only on the images faces.
    """
    return [name for name, table in tables if requested in table]


def _image_404(requested: str, table: Dict[str, List[Route]], face: str,
               code: str) -> JSONResponse:
    """The images faces' version of the 404 the text faces send."""
    elsewhere = _other_faces(requested,
                             ("/v1/chat/completions", cfg.routes),
                             ("/v1/responses", cfg.responses_routes))
    if elsewhere:
        return _error(
            404,
            "model {!r} is not an image model (it is reachable on {}). Models "
            "on {}: {}".format(requested, ", ".join(elsewhere), face,
                               ", ".join(sorted(table)) or "none"),
            code)
    return _error(
        404,
        "unknown model {!r}; available on {}: {}".format(
            requested, face, ", ".join(sorted(table)) or "none"),
        "model_not_found")


def _wants_image_tool(body: dict) -> bool:
    """Does this Responses call ask the model to generate an image itself?"""
    tools = body.get("tools")
    if not isinstance(tools, list):
        return False
    return any(isinstance(t, dict) and t.get("type") == "image_generation"
               for t in tools)


def _routes_for(request, body, table):
    active = getattr(cfg, table).get(body.get("model"), [])
    if cfg.affinity_enabled and affinity.sticky(body):
        return affinity.candidates(request, body, table, active)
    return active


def _image_deployments(request, body):
    deployments = dict(cfg.image_deployments)
    if cfg.affinity_enabled and affinity.sticky(body):
        family = affinity.family(request, body)
        deployments.update(affinity._catalog.get(family, {}).get("images", {}))
    return deployments


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body, requested, err = await _body_and_model(request)
    if err is not None:
        return err

    routes = _routes_for(request, body, "routes")
    if not routes:
        elsewhere = _other_faces(requested,
                                 ("/v1/responses", cfg.responses_routes),
                                 ("/v1/images/generations", cfg.image_routes))
        if elsewhere:
            # The mirror of the case below, and now a real one: gpt-5-pro and
            # the codex models answer chat/completions with a flat 400 and are
            # served only on /v1/responses, and the gpt-image-* deployments
            # have no text face at all. The name is right and the model works —
            # telling the caller it does not exist would send them hunting for a
            # typo that is not there.
            return _error(
                404,
                "model {!r} has no chat/completions route (it is reachable on "
                "{}). Models with a chat face: {}".format(
                    requested, ", ".join(elsewhere),
                    ", ".join(sorted(cfg.routes)) or "none"),
                "no_chat_route")
        return _error(
            404,
            "unknown model {!r}; available: {}".format(
                requested, ", ".join(sorted(cfg.routes))),
            "model_not_found")

    return await _forward(request, body, routes, Route.chat_target, requested,
                          CHAT_FACE)


@app.post("/v1/responses")
async def responses(request: Request):
    body, requested, err = await _body_and_model(request)
    if err is not None:
        return err

    routes = _routes_for(request, body, "responses_routes")
    if not routes:
        elsewhere = _other_faces(requested,
                                 ("/v1/chat/completions", cfg.routes),
                                 ("/v1/images/generations", cfg.image_routes))
        if elsewhere:
            # Worth distinguishing from an unknown model: the name is right and
            # the model works, just not on this face. Sending "unknown model"
            # would send someone hunting for a typo that is not there.
            return _error(
                404,
                "model {!r} has no Responses API route (it is reachable on "
                "{}). Models with a responses face: {}".format(
                    requested, ", ".join(elsewhere),
                    ", ".join(sorted(cfg.responses_routes)) or "none"),
                "no_responses_route")
        return _error(
            404,
            "unknown model {!r}; available: {}".format(
                requested, ", ".join(sorted(cfg.routes))),
            "model_not_found")

    if cfg.responses_compat:
        changed = _apply_responses_compat(body)
        if any(changed.values()):
            # debug, not info: codex trips both of these on every turn, and an
            # info line here would double the volume of a 89-task run's log.
            # That the rewrites are on at all is said once, at startup.
            log.debug("/v1/responses compat: filled %d empty tool "
                      "description(s), dropped %d %s",
                      changed["descriptions"], changed["client_metadata"],
                      CLIENT_METADATA_KEY)

    # A turn that asks the model to draw needs an image deployment named in a
    # header, and the name is only valid on the endpoint it lives on. So the
    # attempt list is narrowed to endpoints that have one before the balancer
    # sees it, and _forward fills the header in per attempt. Without this the
    # call is a coin flip: it works when the balancer happens to pick the
    # resource that holds gpt-image-*, and 400s when it does not.
    image_tool = (_wants_image_tool(body)
                  and IMAGE_TOOL_HEADER not in request.headers)
    if image_tool:
        image_deployments = _image_deployments(request, body)
        with_images = [r for r in routes if r.endpoint in image_deployments]
        if not with_images:
            _ev("image_tool", "warning",
                "%s model=%s asks for the image_generation tool but no endpoint "
                "serving it has an image deployment; forwarding as-is",
                RESPONSES_FACE, requested,
                face=RESPONSES_FACE, model=requested, ok=False)
            image_tool = False
        else:
            if len(with_images) < len(routes):
                _ev("image_tool", "info",
                    "%s model=%s asks for the image_generation tool; holding it "
                    "to the %d of %d route(s) with an image deployment",
                    RESPONSES_FACE, requested, len(with_images), len(routes),
                    face=RESPONSES_FACE, model=requested,
                    routes=len(with_images), of=len(routes))
            routes = with_images

    return await _forward(request, body, routes, Route.responses_target,
                          requested, RESPONSES_FACE, image_tool=image_tool)


# --------------------------------------------------------------------------
# The images faces
# --------------------------------------------------------------------------


@app.post("/v1/images/generations")
async def images_generations(request: Request):
    body, requested, err = await _body_and_model(request)
    if err is not None:
        return err

    routes = cfg.image_routes.get(requested)
    if not routes:
        return _image_404(requested, cfg.image_routes, "images/generations",
                          "no_image_route")

    return await _forward(request, body, routes,
                          lambda r: r.image_target("generations"),
                          requested, IMAGE_FACES["generations"])


@app.post("/v1/images/edits")
async def images_edits(request: Request):
    """Multipart in, multipart out — the body is relayed exactly as it arrived.

    Only the `model` field is read, and only to decide where to send it. Azure
    takes the deployment from the URL path, so nothing in the body has to be
    rewritten and several megabytes of image do not have to be re-encoded to
    change a string Azure will ignore. See _multipart_model.
    """
    content_type = request.headers.get("content-type", "")
    if not content_type.lower().startswith("multipart/form-data"):
        return _error(
            400,
            "/v1/images/edits takes multipart/form-data, not {!r}".format(
                content_type or "nothing"),
            "not_multipart")

    blob = await request.body()
    requested = _multipart_model(blob)
    if not requested:
        return _error(400, "`model` is required", "missing_model")

    routes = cfg.image_edit_routes.get(requested)
    if not routes:
        if requested in cfg.image_routes:
            # A real distinction rather than a shade of the same 404: the model
            # is on the images face and generates perfectly well, it just has no
            # imageEdits capability. Sending "unknown model" for that would be a
            # lie the caller cannot act on.
            return _error(
                404,
                "model {!r} generates images but does not serve /images/edits. "
                "Models that do: {}".format(
                    requested, ", ".join(sorted(cfg.image_edit_routes)) or "none"),
                "no_image_edits_route")
        return _image_404(requested, cfg.image_edit_routes, "images/edits",
                          "no_image_route")

    return await _forward(request, {"model": requested}, routes,
                          lambda r: r.image_target("edits"),
                          requested, IMAGE_FACES["edits"],
                          raw=blob, content_type=content_type)
