"""Local OpenAI-compatible proxy over Azure OpenAI.

Exposes two unauthenticated faces on loopback, /v1/chat/completions and
/v1/responses. Picks an endpoint for the requested model, attaches Azure
credentials, rewrites `model` to that endpoint's deployment name, and forwards
everything else untouched. Retries on 429/5xx/transport errors by moving to the
next endpoint that serves the model.

Which endpoint goes first is `routing.balance`, in three flavours:
`strict_priority` walks the probe's order, `priority_threshold` walks it until
the head route is carrying more than its share and then moves down, and
`capacity` ignores priority entirely and samples in proportion to quota. All
three walk the same chain on failure — balancing reorders the attempts, it does
not change what counts as a failure or how many are allowed.

The two faces are routed separately: Azure gates the Responses API behind its
own data action and does not serve it on older api-versions, so a model
reachable through chat/completions may have no responses route at all. The
probe decides which is which.

    python -m proxy
    uvicorn proxy.server:app --host 127.0.0.1 --port 8787
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
from typing import Callable, Deque, Dict, List, Optional, Tuple

import httpx
import yaml
from azure.identity import AzureCliCredential
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .events import PROBLEM_KINDS, EventLog

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

# The four ways a request can arrive, as far as the ledger is concerned: two
# faces times streamed or not. Quota does not care about the distinction — one
# deployment ceiling serves all four — but an operator does, because the four
# fail differently. Streamed Responses calls are the ones Azure refuses with a
# 200 plus retry-after (see THROTTLE_HEADER), and they are the ones session
# affinity pins. A load figure that cannot say which of the four it is made of
# cannot tell you which of those mechanisms you are watching.
#
# The order is the display order, and the index is what goes in the ledger.
FACES = ("chat", "chat_stream", "responses", "responses_stream")


def face_code(face: str, stream: bool) -> int:
    """The FACES index for one request. `face` is the request path."""
    base = 2 if face.endswith("responses") else 0
    return base + (1 if stream else 0)


def _route_sort_key(route: "Route"):
    """Failover order for one model's routes: priority, then size, then name."""
    capacity = route.capacity_tokens or route.capacity_requests or 0
    return (route.priority, -capacity, route.deployment)


class Route:
    """One (endpoint, deployment) pair that can serve a model.

    A model can have more than one of these on the SAME endpoint: a second
    deployment of the same model, bought under a different SKU, is a second
    quota and a genuinely separate destination. Everything downstream keys on
    the pair, never on the endpoint alone.
    """

    __slots__ = ("endpoint", "url", "api_version", "deployment",
                 "limit_param", "priority", "responses_path",
                 "model_version", "capacity_requests", "capacity_tokens")

    def __init__(self, endpoint, url, api_version, deployment, limit_param,
                 priority, responses_path=None, model_version=None,
                 capacity_requests=None, capacity_tokens=None):
        self.endpoint = endpoint
        self.url = url
        self.api_version = api_version
        self.deployment = deployment
        self.limit_param = limit_param
        self.priority = priority
        # None when this endpoint does not serve the Responses API. Which of
        # the two URL shapes it wants is settled by the probe.
        self.responses_path = responses_path
        # Which vintage of the model this deployment serves. Carried for
        # reporting; the proxy does not route on it.
        self.model_version = model_version
        # The quota ARM says this deployment holds, in the units the runtime
        # later reads off x-ratelimit-limit-*. None when the probe could not ask
        # ARM — an older runtime/, or an endpoint discovered by guesswork.
        self.capacity_requests = capacity_requests
        self.capacity_tokens = capacity_tokens

    def chat_target(self) -> str:
        return "{}openai/deployments/{}/chat/completions?api-version={}".format(
            self.url, self.deployment, self.api_version)

    def responses_target(self) -> str:
        # No deployment in the path here: the Responses API takes it from the
        # body's `model`, which _forward has already rewritten.
        return self.url + self.responses_path

    def __repr__(self):
        return "{}/{}".format(self.endpoint, self.deployment)


class Config:
    def __init__(self):
        with open(os.path.join(SETTINGS, "policy.yaml")) as f:
            policy = yaml.safe_load(f)
        with open(os.path.join(RUNTIME, "sources.json")) as f:
            sources = json.load(f)
        with open(os.path.join(RUNTIME, "models.json")) as f:
            models = json.load(f)

        self.policy = policy
        self.host = policy["server"]["host"]
        self.port = policy["server"]["port"]
        self.log_level = policy["server"].get("log_level", "info")

        # Diagnostics. Off unless a directory is named, and it has to stay that
        # way: these files contain prompts. See _capture_stream.
        self.capture_dir = policy["server"].get("capture_dir") or None
        self.capture_mode = policy["server"].get("capture_mode", "suspicious")
        self.capture_limit = int(policy["server"].get("capture_limit", 40))

        r = policy["routing"]
        self.retry_on_status = set(r["retry_on_status"])
        self.retry_on_transport_error = r["retry_on_transport_error"]
        self.retry_on_timeout = r["retry_on_timeout"]
        self.timeout = r["request_timeout_seconds"]
        self.max_attempts = r["max_attempts_per_request"]
        self.backoff_initial = r["backoff_initial_seconds"]
        self.backoff_multiplier = r["backoff_multiplier"]
        self.backoff_jitter = r["backoff_jitter_seconds"]

        # How the attempt order is chosen. Unknown values fall back to the
        # conservative one rather than refusing to boot: a typo here should not
        # take the proxy down, and strict priority is what it did before any of
        # this existed.
        self.balance_configured = r.get("balance", "priority_threshold")
        self.balance = BALANCE_ALIASES.get(self.balance_configured,
                                           self.balance_configured)
        if self.balance not in BALANCE_MODES:
            self.balance = "strict_priority"

        b = r.get("balancing") or {}
        self.static_weights = b.get("static_weights") or {}
        self.headroom_high_water = float(b.get("headroom_high_water", 0.5))
        self.weight_floor = float(b.get("weight_floor", 0.05))
        self.observation_ttl = float(b.get("observation_ttl_seconds", 120))
        self.demote_multiplier = float(b.get("demote_multiplier", 0.25))
        self.demote_seconds = float(b.get("demote_seconds", 30))
        self.demote_halflife = float(
            b.get("demote_recovery_halflife_seconds", 30))
        self.spill_threshold = float(b.get("spill_threshold", 0.70))
        self.load_window = float(b.get("load_window_seconds", 60))
        self.chars_per_token = float(b.get("assumed_chars_per_token", 4)) or 4.0

        f = b.get("foreign_load") or {}
        self.foreign_enabled = bool(f.get("enabled", True))
        self.foreign_reclaim = float(f.get("reclaim_per_minute", 0.1))

        s = r.get("stream_probe") or {}
        self.probe_seconds = float(s.get("hold_seconds", 0.5))
        self.probe_bytes = int(s.get("hold_bytes", 16384))
        # Encoded once, at boot: this list is consulted per chunk of every
        # stream, and str.encode() on the hot path for a constant is waste.
        self.stream_retry_markers = [
            str(code).encode() for code in
            (s.get("retry_on_codes") or ["rate_limit_exceeded"])]

        a = r.get("session_affinity") or {}
        self.affinity_enabled = bool(a.get("enabled", True))
        self.affinity_keys = list(a.get("keys") or [
            "header:session-id", "body:prompt_cache_key",
            "body:client_metadata.session_id", "header:x-session-id"])
        self.affinity_markers = list(a.get("sticky_include") or
                                     ["reasoning.encrypted_content"])
        self.affinity_ttl = float(a.get("ttl_seconds", 3600))
        self.affinity_max = int(a.get("max_sessions", 4096))
        self.affinity_on_conflict = a.get("on_conflict", "wait")
        if self.affinity_on_conflict not in ("wait", "switch"):
            self.affinity_on_conflict = "wait"
        self.affinity_attempts = int(a.get("wait_attempts", 4))
        self.affinity_max_wait = float(a.get("max_wait_seconds", 30))

        self.forward_headers = policy["request"]["forward_headers"]
        self.responses_compat = policy["request"].get("responses_compat", True)
        self.scope = policy["auth"]["scope"]
        self.refresh_margin = policy["auth"]["refresh_margin_seconds"]
        self.expected_account = policy["auth"].get("expected_account")

        # Give the Azure CLI its own directory before anything shells out to it.
        # AzureCliCredential spawns `az` with a copy of os.environ, so setting
        # it here is enough for the credential, the background refresher and the
        # startup account check alike.
        self.az_config_dir = policy["auth"].get("az_config_dir")
        if self.az_config_dir:
            self.az_config_dir = os.path.expanduser(self.az_config_dir)
            # A relative path is relative to the repo, not to whatever directory
            # the proxy happened to be started from. That is what lets the
            # credential directory travel with the checkout: the whole tree can
            # be moved to another path, or handed to another account, without a
            # setting that points back at where it used to live.
            if not os.path.isabs(self.az_config_dir):
                self.az_config_dir = os.path.join(ROOT, self.az_config_dir)
            os.environ["AZURE_CONFIG_DIR"] = self.az_config_dir

        # An endpoint counts as usable if either face is up. They are gated
        # separately by Azure and they fail separately: a resource whose chat
        # face is refused can still serve the Responses API, and dropping it
        # entirely would take working routes down with the broken one.
        meta = {e["name"]: e for e in sources["endpoints"]
                if "ok" in (e["status"], e.get("responses_status"))}
        self.endpoints = [(e["name"], e["status"], e.get("responses_status", "?"))
                          for e in sources["endpoints"]]
        self.routes: Dict[str, List[Route]] = {}
        self.responses_routes: Dict[str, List[Route]] = {}
        for name, spec in models["models"].items():
            built, responses = [], []
            for hop in spec["routes"]:
                ep = meta.get(hop["endpoint"])
                if ep is None:
                    continue        # endpoint went unhealthy since the last probe
                route = Route(
                    endpoint=hop["endpoint"],
                    url=ep["url"],
                    api_version=ep["api_version"],
                    deployment=hop["deployment"],
                    limit_param=hop["limit_param"],
                    priority=hop.get("priority", ep.get("priority", 0)),
                    responses_path=ep.get("responses_path"),
                    model_version=hop.get("model_version"),
                    capacity_requests=hop.get("capacity_requests"),
                    capacity_tokens=hop.get("capacity_tokens"),
                )
                # Absent `faces` means a runtime/ written before the Responses
                # API existed here. Default it to chat only, so a stale probe
                # leaves the old face working and merely reports no routes on
                # the new one.
                faces = hop.get("faces", ["chat"])
                # Not every deployment has a chat face. gpt-5-pro and the codex
                # models answer chat/completions with a flat 400 and serve the
                # Responses API only, so listing them as chat routes would offer
                # a destination that cannot work.
                if "chat" in faces:
                    built.append(route)
                if "responses" in faces and route.responses_path:
                    responses.append(route)
            # Candidate order in endpoints.yaml is failover priority, and
            # capacity breaks ties within one endpoint — a model served twice by
            # the same resource should be reached for at its larger deployment
            # first. Sorted here rather than trusted from the file, which is the
            # same key probe/probe.py:route_sort_key writes it in.
            built.sort(key=_route_sort_key)
            responses.sort(key=_route_sort_key)
            if built:
                self.routes[name] = built
            if responses:
                self.responses_routes[name] = responses

        self.generated_at = models.get("_generated_at")


# --------------------------------------------------------------------------
# Quota tracking and route selection
# --------------------------------------------------------------------------
#
# Azure returns its rate limit state on every response:
#
#   x-ratelimit-limit-requests / -tokens          the ceiling
#   x-ratelimit-remaining-requests / -tokens      "what is left in the window"
#   x-ratelimit-renewalperiod-requests / -tokens  window length, seconds — 60
#   x-ratelimit-reset-requests / -tokens          always 0 in every sample taken
#
# The ceiling is the useful part. It is a stable property of the deployment, it
# arrives on traffic the proxy is already carrying, and it is what both the
# weights and the load fractions are built from. Measured 2026-08-20 for
# gpt-5.6-sol: 333 RPM / 333k TPM on endpoint-a, 1000 / 1M on endpoint-b, 499 /
# 499k on endpoint-c — so the ratio TPM:RPM is 1000:1 on all
# three, and the ceilings differ by 3x between endpoints.
#
# `remaining` is NOT a minute's worth of budget, whatever renewalperiod=60
# implies, and this matters enough to record how it was established. Measured
# against an idle endpoint-b/gpt-4.1-mini (2000 RPM / 2M TPM) on 2026-08-20:
#
#   * 16 requests fired concurrently — an instantaneous rate of ~960 RPM, i.e.
#     ~48% of the stated ceiling — moved remaining-requests from 2000 to 1989.
#     A 60-second window would have had to read 1984 and stay there.
#   * Three seconds later it was back at 1999, and it stayed there for the next
#     35 seconds of sampling. Nothing decayed over a minute; it refilled almost
#     immediately.
#   * remaining-tokens behaved the same way and is charged actual usage, not the
#     max_completion_tokens reservation: 16 requests with a 4000-token ceiling
#     each moved it by 27 tokens total, matching the ~19 tokens each actually
#     used.
#   * Responses within one burst disagree with each other (1999, 1996, 1995,
#     1990, 1989 ...), so it is not even a consistent snapshot.
#
# So `1 - remaining/limit` understates real load by close to two orders of
# magnitude: it read 0.55% at a moment when the true minute-equivalent load was
# ~48%. It is a sub-second bucket quoted against a per-minute ceiling. It cannot
# carry a threshold, and it is left where it was — a brake that may only
# *reduce* a weight, and only below half, which the measurement says will
# essentially never fire. It is kept because it costs nothing and the one time
# it does fire it is telling the truth about an instantaneous burst.
#
# What carries the threshold instead is the proxy's own ledger: every request it
# dispatches is recorded against the (endpoint, deployment) it went to, with a
# timestamp and a token cost, and load is what is still inside the window
# divided by the measured ceiling. The trade is explicit and worth stating:
#
#   + it is exact for our own traffic, it needs no response to update, and it
#     is available at dispatch — which matters, because at concurrency 30 with
#     minute-long reasoning turns a signal that only updates on completion lags
#     by an entire request.
#   + "no traffic" reads as zero load rather than as unknown, so a route that
#     has been quiet is preferred rather than starved.
#   - it is blind to anyone else spending the same deployment's quota. Nothing
#     can fix that from here; what covers it is the reactive half of the system,
#     which is the 429 (and in-band 429) demotion below.
#
# State is per (endpoint, deployment), not per endpoint: quota on Azure is
# granted to a deployment — x-ratelimit-key comes back as the deployment name —
# and one endpoint serving two models has two separate buckets that have nothing
# to do with each other.


def _as_number(value) -> Optional[float]:
    """Header value -> float, or None. Azure has been consistent about sending
    plain integers here, but a weight calculation is not the place to find out
    what happens the day it is not."""
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


class RouteState:
    """Observed quota and recent failures for one (endpoint, deployment)."""

    __slots__ = ("key", "limit_requests", "limit_tokens", "remaining_requests",
                 "remaining_tokens", "renewal_seconds", "observed_at",
                 "penalty", "penalty_until", "penalty_at", "attempts", "ok",
                 "rate_limited", "errors", "last_status", "sent",
                 "tokens_per_char", "token_samples",
                 "foreign", "foreign_at", "foreign_hold_until",
                 "foreign_samples", "foreign_our_load")

    def __init__(self, key: str):
        self.key = key
        self.limit_requests: Optional[float] = None
        self.limit_tokens: Optional[float] = None
        self.remaining_requests: Optional[float] = None
        self.remaining_tokens: Optional[float] = None
        self.renewal_seconds: Optional[float] = None
        self.observed_at: Optional[float] = None
        # 1.0 is "no penalty". A failure knocks it down; time brings it back.
        self.penalty = 1.0
        self.penalty_until = 0.0        # parked until here (Retry-After)
        self.penalty_at = 0.0           # when the penalty was last recomputed
        self.attempts = 0
        self.ok = 0
        self.rate_limited = 0
        self.errors = 0
        self.last_status: Optional[str] = None

        # The proxy's own ledger: one [dispatched_at, tokens, face] entry per
        # request sent here, oldest first, pruned to the load window. A list
        # rather than a tuple because the token cost starts as an estimate and
        # is overwritten with the real figure if the response turns out to state
        # one. `face` is an index into FACES and is carried for reporting only —
        # nothing routes on it, because one ceiling serves all four faces.
        self.sent: Deque[List[float]] = collections.deque()
        # Tokens per byte of request body, learned from responses that report
        # usage. Seeded from policy.yaml and then corrected, because the true
        # ratio depends on the model as much as on the prompt: a reasoning turn
        # bills thinking tokens that were never in the request at all.
        self.tokens_per_char: Optional[float] = None
        self.token_samples = 0

        # What everyone ELSE is estimated to be taking from this deployment, as
        # a fraction of its ceiling. Only ever learned at the moment of a
        # throttle — see QuotaTracker.note_foreign — and decayed away in
        # between, because nothing reports it and silence is not evidence that
        # it is still there.
        self.foreign = 0.0
        self.foreign_at = 0.0
        self.foreign_hold_until = 0.0   # reclaim paused until Retry-After ends
        self.foreign_samples = 0
        self.foreign_our_load: Optional[float] = None   # our share last time

    # -- ledger -----------------------------------------------------------
    def prune(self, now: float, window: float) -> None:
        sent = self.sent
        cutoff = now - window
        while sent and sent[0][0] < cutoff:
            sent.popleft()

    def in_window(self, now: float, window: float) -> Tuple[int, float]:
        """(requests, tokens) dispatched here inside the window."""
        self.prune(now, window)
        return len(self.sent), sum(e[1] for e in self.sent)

    def in_window_by_face(self, now: float,
                          window: float) -> List[Tuple[int, float]]:
        """The same pair, split four ways by FACES. Same order as FACES.

        Only ever used for reporting: routing acts on the totals, because a
        deployment's quota is not divided by face — one ceiling serves all four,
        and spending it through /v1/responses leaves exactly as little for
        /v1/chat/completions as spending it the other way round.
        """
        self.prune(now, window)
        out = [[0, 0.0] for _ in FACES]
        for entry in self.sent:
            slot = out[entry[2] if len(entry) > 2 else 0]
            slot[0] += 1
            slot[1] += entry[1]
        return [(int(r), t) for r, t in out]


class QuotaTracker:
    """Chooses the attempt order, and remembers why.

    Three modes, all of which produce a full permutation of the model's routes
    so the failover chain stays intact and no endpoint can be tried twice:

    `strict_priority` returns the probe's order untouched.

    `capacity` samples the list without replacement in proportion to weight.
    Sampling rather than "send to whoever has the most headroom left": every
    in-flight request would compute the same answer from the same shared state
    and pile onto the same endpoint, and the correction only arrives after the
    responses do. Sampling has no such feedback delay — it needs no in-flight
    accounting, and over a run the split converges on the weights.

    `priority_threshold` walks the priority order and heads for the first route
    that is not already carrying more than `spill_threshold` of its own quota.
    The equilibrium is worth being explicit about, because it is the whole
    design: load is measured over a sliding window, so once the top route is
    pinned at the threshold each individual request tips it over, goes to the
    next route instead, and lets the top route fall back under. The split is
    therefore per-request rather than in blocks, the top route stabilises at
    exactly the threshold, and the overflow — and only the overflow — moves
    down the chain. If every route is over its threshold there is no overflow
    destination left, so it falls back to `capacity`: spreading the excess in
    proportion to size is better than putting all of it back on route one.
    """

    def __init__(self, config: Config):
        self.cfg = config
        self.states: Dict[str, RouteState] = {}

    # -- state ------------------------------------------------------------
    def state(self, route: Route) -> RouteState:
        """The quota bucket for this route, keyed by endpoint AND deployment.

        Not by endpoint. Azure grants quota to a deployment — x-ratelimit-key
        comes back as the deployment name — so two deployments of the same model
        on one resource have two ceilings, two loads and two throttles. Folding
        them together would have one route's 429 park the other, and would spend
        one ceiling's worth of budget against the sum of two.
        """
        key = str(route)
        st = self.states.get(key)
        if st is None:
            st = self.states[key] = RouteState(key)
        return st

    def observed(self, route: Route, status: int, headers) -> None:
        """Fold one upstream response into the route's state."""
        st = self.state(route)
        st.last_status = str(status)
        if status < 400:
            st.ok += 1

        limit_r = _as_number(headers.get("x-ratelimit-limit-requests"))
        limit_t = _as_number(headers.get("x-ratelimit-limit-tokens"))
        if limit_r:
            st.limit_requests = limit_r
        if limit_t:
            st.limit_tokens = limit_t
        remaining_r = _as_number(headers.get("x-ratelimit-remaining-requests"))
        remaining_t = _as_number(headers.get("x-ratelimit-remaining-tokens"))
        if remaining_r is not None or remaining_t is not None:
            st.remaining_requests = remaining_r
            st.remaining_tokens = remaining_t
            st.renewal_seconds = _as_number(
                headers.get("x-ratelimit-renewalperiod-requests")
                or headers.get("x-ratelimit-renewalperiod-tokens"))
            st.observed_at = time.time()

    # -- the proxy's own ledger -------------------------------------------
    def estimate_tokens(self, route: Route, request_bytes: int) -> float:
        """What this request is expected to cost the deployment.

        Charged at dispatch, because that is when Azure charges it and because
        waiting for the answer would make the load signal lag by the length of
        a request — which, for a reasoning turn, is most of the window.

        The request's own size is the only thing available at that moment, so
        the estimate is bytes x a learned tokens-per-byte ratio. The seed value
        is a plain characters-per-token guess; every response that states its
        usage corrects it (see `settle`). Being wrong here is survivable: the
        ratio is per-route and converges within a handful of requests, and the
        threshold it feeds is a soft one.
        """
        st = self.state(route)
        ratio = st.tokens_per_char
        if ratio is None:
            ratio = 1.0 / self.cfg.chars_per_token
        return max(1.0, request_bytes * ratio)

    def charge(self, route: Route, tokens: float,
               face: int = 0) -> List[float]:
        """Record a dispatch. Returns the ledger entry, for `settle`."""
        now = time.time()
        st = self.state(route)
        st.prune(now, self.cfg.load_window)
        entry = [now, float(tokens), face]
        st.sent.append(entry)
        return entry

    def settle(self, route: Route, entry: Optional[List[float]],
               request_bytes: int, total_tokens: Optional[int]) -> None:
        """Replace an estimate with what the response said it actually cost.

        Two things come out of this. The ledger entry is corrected, which only
        matters if the request was short enough to still be inside the window.
        The tokens-per-byte ratio is updated, which is the part that pays: it is
        what every *subsequent* estimate on this route is built from, and it is
        how a reasoning model's thinking tokens — invisible in the request —
        get accounted for at all.
        """
        if not total_tokens or total_tokens <= 0:
            return
        st = self.state(route)
        if entry is not None:
            entry[1] = float(total_tokens)
        if request_bytes <= 0:
            return
        sample = total_tokens / float(request_bytes)
        if st.tokens_per_char is None:
            st.tokens_per_char = sample
        else:
            # Exponential, alpha 0.3: fast enough to follow a change of model or
            # of prompt shape within a few requests, slow enough that one
            # unusually long reasoning turn does not redefine the route.
            st.tokens_per_char = 0.7 * st.tokens_per_char + 0.3 * sample
        st.token_samples += 1

    def load(self, st: RouteState, now: float) -> Optional[float]:
        """How much of this route's own quota the proxy is currently using.

        A fraction of *its own* ceiling, never an absolute figure: the three
        endpoints in service differ by 3x in quota, so a shared number would
        mean "spill at 70% of the smallest one" for two of them.

        Both dimensions are computed and the larger wins — whichever ceiling is
        reached first is the one that will produce the 429. In practice that is
        tokens: the live run on 2026-08-20 sat at ~27% of endpoint-a's TPM while
        using ~8% of its RPM, so TPM binds about 3x sooner.

        Returns None when nothing is known about the ceiling, and None means
        *unknown*, not *full*. The caller treats it as "not busy" on purpose: a
        route that has never answered has to be tried before it can report a
        ceiling, and a rule that read silence as saturation would make sure it
        never got the chance.
        """
        requests, tokens = st.in_window(now, self.cfg.load_window)
        fractions = []
        if st.limit_requests:
            fractions.append(requests / st.limit_requests)
        if st.limit_tokens:
            fractions.append(tokens / st.limit_tokens)
        if not fractions:
            return None
        return max(fractions)

    def load_dimension(self, st: RouteState, now: float) -> Optional[str]:
        """Which ceiling `load` is currently measuring against.

        Reported so that a reader knows what the load figure IS. "38% of RPM"
        and "38% of TPM" are the same number about two different walls, and
        which wall it is decides what to do about it — an RPM-bound route wants
        fewer, larger requests, a TPM-bound one wants the opposite.
        """
        requests, tokens = st.in_window(now, self.cfg.load_window)
        by_requests = (requests / st.limit_requests) if st.limit_requests else None
        by_tokens = (tokens / st.limit_tokens) if st.limit_tokens else None
        if by_requests is None and by_tokens is None:
            return None
        if by_tokens is None:
            return "requests"
        if by_requests is None:
            return "tokens"
        return "tokens" if by_tokens >= by_requests else "requests"

    def load_by_face(self, st: RouteState,
                     now: float) -> Optional[Dict[str, float]]:
        """`load`, split into the four FACES. The four sum to `load` exactly.

        Split along whichever dimension `load` itself used, not along a fixed
        one. Splitting tokens while `load` reported requests would produce four
        segments that add up to a different number than the total beside them,
        which in a stacked bar is not a rounding quibble — it is a bar whose
        segments do not fill it.
        """
        dimension = self.load_dimension(st, now)
        if dimension is None:
            return None
        index = 0 if dimension == "requests" else 1
        ceiling = (st.limit_requests if dimension == "requests"
                   else st.limit_tokens)
        per_face = st.in_window_by_face(now, self.cfg.load_window)
        return {name: pair[index] / ceiling
                for name, pair in zip(FACES, per_face)}

    # -- everyone else ----------------------------------------------------
    #
    # The ledger above counts only what this proxy sent. Quota is granted per
    # deployment and shared with whoever else holds credentials for it — on this
    # box alone there are eight other proxies under a different account pointed
    # at the same endpoints, and two of them could not be read to find out which
    # deployments they use. A model that assumes the proxy is alone will keep
    # walking into a wall it cannot see.
    #
    # There is exactly one moment when the other tenants become observable: the
    # instant Azure refuses. A throttle means total consumption reached the
    # ceiling, and our own share of that total is a number we already have, so
    #
    #     foreign = clamp(1 - our_load_at_throttle, 0, 1)
    #
    # A throttle at our_load 0.9 says almost all of it was us; a throttle at 0.05
    # says someone else is using 95% of the deployment and we were merely the
    # request that arrived last. Nothing else in the response carries this:
    # x-ratelimit-remaining-* is measured to be a sub-second bucket quoted
    # against a per-minute ceiling (it read 0.55% consumed at ~48% true load),
    # so it cannot size a foreign share either. Deliberately not used — a signal
    # already proven wrong by 90x does not deserve a second try.
    #
    # SYNTHETIC PROBES ARE USELESS HERE and the reason is worth writing down,
    # because "just send a test request" is the obvious idea. Throttling fires on
    # AGGREGATE consumption, so a single small request succeeds whether the other
    # tenant is using 90% or 0%. A probe can only distinguish the two by being
    # large enough to hit the ceiling, which spends real quota and disturbs the
    # traffic it is trying to measure. The only informative probe is real
    # traffic. This is TCP's problem exactly: available capacity is discoverable
    # only by using it.
    #
    # So the control law is AIMD, for the same reason TCP uses it:
    #
    #   Multiplicative decrease — on a throttle, jump the estimate straight up
    #     to the observation (and never down: max() with what is already held,
    #     so a throttle that happens to catch us at high load cannot erase what
    #     an earlier one revealed).
    #   Additive increase — between throttles, give the capacity back LINEARLY,
    #     at foreign_reclaim_per_minute.
    #
    # Linear, not an exponential halflife, and this is not a stylistic
    # preference. An exponential gives back capacity fastest in the moments just
    # after a throttle — precisely when the deployment is known to be contended
    # and caution is worth most — and then trails off slowly for a long time
    # afterwards, when the estimate is stale and worth the least. It is backwards
    # in both halves. Linear reclaim is constant and gentle throughout.
    #
    # AIMD is also the only increase/decrease pairing that converges to a fair
    # split between independent controllers that cannot see each other (AIAD and
    # MIMD do not). That is not theoretical tidiness here: several of the other
    # proxies on this box are plausibly doing something adaptive too, and this is
    # what keeps two of them from settling into a permanently unfair share.
    #
    # Reclaim does not start until Retry-After has elapsed. Azure states how long
    # this particular congestion is expected to last, so there is no reason to
    # guess at that timescale.

    def foreign_load(self, st: RouteState, now: float) -> float:
        """The current estimate of what other tenants are taking.

        Reclaimed linearly since the last throttle, after that throttle's
        Retry-After has passed.
        """
        if not self.cfg.foreign_enabled or st.foreign <= 0.0:
            return 0.0
        rate = self.cfg.foreign_reclaim        # per minute
        if rate <= 0:
            return st.foreign                  # 0 disables reclaim entirely
        start = max(st.foreign_at, st.foreign_hold_until)
        if now <= start:
            return st.foreign
        return max(0.0, st.foreign - (now - start) / 60.0 * rate)

    def total_load(self, st: RouteState, now: float) -> Optional[float]:
        """Our load plus everyone else's. None only if the ceiling is unknown.

        This — not `load` — is what both routing modes act on. A route that is
        60% consumed by someone else has 40% left to offer, whatever our own
        ledger says about it.
        """
        ours = self.load(st, now)
        if ours is None:
            return None
        return ours + self.foreign_load(st, now)

    def note_foreign(self, route: Route, retry_after=None) -> Optional[dict]:
        """Fold one throttle into the estimate of other tenants' share.

        The multiplicative-decrease half of AIMD: the estimate only ever jumps
        UP here. Taking max() with what is already held matters because our own
        load fluctuates — a throttle that arrives while we happen to be at 0.9
        would otherwise wipe out an earlier throttle's finding that someone else
        holds 0.8, and we would spend the next several minutes relearning it.

        Returns what moved, or None if nothing did — the caller folds it into
        its own event, because "we backed off" and "we revised our estimate of
        who else is here" are separately worth knowing and only this function
        can tell the second one from the first.
        """
        if not self.cfg.foreign_enabled:
            return None
        now = time.time()
        st = self.state(route)
        ours = self.load(st, now)
        if ours is None:
            return None         # no ceiling known: cannot express a share
        observed = min(1.0, max(0.0, 1.0 - ours))
        held = self.foreign_load(st, now)
        st.foreign = max(held, observed)
        st.foreign_at = now
        # Reclaim is paused until Azure says the congestion should be over.
        park = _as_number(retry_after)
        st.foreign_hold_until = now + max(0.0, park or 0.0)
        st.foreign_samples += 1
        st.foreign_our_load = ours
        moved = {"our_load": round(ours, 4),
                 "foreign_before": round(held, 4),
                 "foreign_after": round(st.foreign, 4),
                 "foreign_samples": st.foreign_samples,
                 "foreign_hold_seconds": round(max(0.0, park or 0.0), 1)}
        if observed > 0.15 and observed > held + 0.1:
            # Worth a line: this is the proxy asserting that someone else is on
            # the deployment, which is a claim about the world outside it and
            # the only place that claim is ever made.
            _ev("foreign", "info",
                "%s throttled at our load %.2f: estimating %.0f%% foreign "
                "load on this deployment", route, ours, observed * 100,
                route=route, **moved)
        return moved

    def demote(self, route: Route, reason: str,
               retry_after=None) -> None:
        """Temporarily send less traffic here.

        Not just "skip it for this one request" — a 429 says the deployment is
        at its ceiling right now, which is a fact about the next few seconds,
        not about one caller. Retry-After, when Azure sends it, is a better
        answer than any constant, so it wins.

        This coexists with the foreign-load estimate rather than being replaced
        by it, and the two are deliberately NOT the same mechanism:

          * the penalty and parking window here are an evasive manoeuvre on the
            scale of seconds — get off this deployment until Retry-After has
            passed. It applies to 5xx and transport errors too, which say
            nothing at all about quota.
          * foreign load is a standing estimate of how much of the deployment
            is not ours to use, on the scale of minutes. It changes the size the
            route is believed to be, not whether it is currently answering.

        Stacking them is correct: a throttled route should be both avoided right
        now (parking) and treated as smaller from now on (foreign). Folding
        either into the other would lose one of the two timescales — and it is
        the slow one that keeps the proxy from re-learning the same wall every
        thirty seconds.
        """
        now = time.time()
        st = self.state(route)
        moved = None
        if reason == "429":
            st.rate_limited += 1
            # Only rate limits carry information about other tenants. A 500 or a
            # connection reset means the endpoint is unwell, not that its quota
            # is spoken for, and inferring a foreign share from one would
            # permanently shrink a route for being briefly broken.
            moved = self.note_foreign(route, retry_after)
        else:
            st.errors += 1
        st.last_status = reason

        self._decay(st, now)
        st.penalty = max(self.cfg.weight_floor,
                         st.penalty * self.cfg.demote_multiplier)
        park = _as_number(retry_after)
        if park is None or park <= 0:
            park = self.cfg.demote_seconds
        st.penalty_until = max(st.penalty_until, now + park)
        st.penalty_at = now
        # Two kinds, because they are two different claims. A throttle says the
        # deployment is at ITS ceiling, which is partly a statement about
        # everyone else on it; anything else says this endpoint is unwell, which
        # is a statement about nobody but itself.
        _ev("throttle" if reason == "429" else "demote", "info",
            "demoted %s on %s: weight x%.2f for %.0fs",
            route, reason, st.penalty, park,
            route=route, reason=reason, penalty=round(st.penalty, 4),
            park_seconds=round(park, 1),
            foreign_updated=moved is not None, **(moved or {}))

    def failed(self, route: Route, reason: str) -> None:
        self.demote(route, reason)

    def _decay(self, st: RouteState, now: float) -> None:
        """Walk a penalty back towards 1.0 as time passes.

        Recovery starts only once the Retry-After window is over, so a route
        Azure has explicitly asked us to leave alone is left alone for exactly
        as long as it asked.
        """
        if st.penalty >= 1.0:
            return
        start = max(st.penalty_until, st.penalty_at)
        if now <= start:
            return
        halflife = self.cfg.demote_halflife
        if halflife <= 0:
            st.penalty = 1.0
        else:
            st.penalty = min(1.0, st.penalty * 2 ** ((now - start) / halflife))
        st.penalty_at = now

    # -- weights ----------------------------------------------------------
    def measured_capacity(self, st: RouteState) -> Optional[float]:
        """What Azure said this deployment's ceiling is, or None.

        Tokens first: TPM is the constraint that binds. The live run on
        2026-08-20 sat at ~27% of endpoint-a's TPM ceiling while using ~8% of its
        RPM one, so a request-count weight would be tracking the limit that is
        not the limit. The two are not reliably proportional across deployments,
        so which one is used has to be decided rather than assumed.
        """
        return st.limit_tokens or st.limit_requests or None

    def static_prior(self, route: Route) -> float:
        """The cold-start guess for a route Azure has not described yet.

        The probe's ARM figure first, and it is barely a guess: `rateLimits` is
        quoted in the same units as x-ratelimit-limit-*, per deployment, so a
        route starts out knowing its real ceiling and the first response merely
        confirms it. Tokens before requests, matching measured_capacity, so the
        prior and the measurement are on one scale and the conversion in
        weights() is an identity for these routes.

        The per-endpoint table in policy.yaml is what is left for a route ARM
        could not describe: an endpoint with no coordinates, no ARM permission,
        or a runtime/ written before any of this. It is a worse answer by
        construction — quota is granted per deployment, and one endpoint's
        deployments do not share a number.
        """
        capacity = route.capacity_tokens or route.capacity_requests
        if capacity:
            return float(capacity)
        static = self.cfg.static_weights.get(route.endpoint)
        try:
            static = float(static)
        except (TypeError, ValueError):
            return 1.0
        return static if static > 0 else 1.0

    def headroom(self, st: RouteState, now: float) -> Optional[float]:
        """Fraction of the window still unspent, or None if we cannot say.

        None and 1.0 are different answers and the caller treats them the same
        on purpose: an observation older than the TTL means *unknown*, not
        *empty*, and a route must not be starved for having gone quiet.
        """
        if st.observed_at is None:
            return None
        if now - st.observed_at > self.cfg.observation_ttl:
            return None
        fractions = []
        if st.limit_tokens and st.remaining_tokens is not None:
            fractions.append(st.remaining_tokens / st.limit_tokens)
        if st.limit_requests and st.remaining_requests is not None:
            fractions.append(st.remaining_requests / st.limit_requests)
        if not fractions:
            return None
        return max(0.0, min(1.0, min(fractions)))

    def effective_penalty(self, st: RouteState, now: float) -> float:
        """The multiplier a route's weight actually gets right now.

        Inside the parking window it is the floor, not the accumulated penalty:
        Retry-After is Azure saying how long this deployment will keep refusing,
        and there is nothing to be gained by arguing with it. The floor rather
        than zero so the route stays reachable — it is the last route on some
        model's chain, and it has to be able to report a fresh quota header to
        climb back out.
        """
        if now < st.penalty_until:
            return self.cfg.weight_floor
        return max(self.cfg.weight_floor, min(1.0, st.penalty))

    def weights(self, routes: List[Route],
                now: Optional[float] = None) -> List[float]:
        """Weights for one model's routes, on one scale.

        The whole set has to be computed together, because a measured route and
        an unmeasured one are not quoted in the same units. Azure reports TPM in
        the hundreds of thousands; the static table in policy.yaml is a handful
        of RPM figures someone typed. Scoring them against each other directly
        is a trap that closes immediately: the first response to arrive gives
        one route a ceiling of 333000 while every route still unmeasured sits at
        333, the sampler never picks any of them again, and they never get a
        chance to report a ceiling of their own. Strict priority with extra
        steps — which is exactly what this was supposed to replace.

        So the priors are converted into measured units first, using the routes
        where both numbers are known. When nothing has been measured yet the
        priors are already mutually consistent and are used as they are; when
        the priors are equal or absent, an unmeasured route inherits the mean
        measured capacity. That last case is the important one, because it is
        the default: a route nobody has numbers for is assumed average, tried,
        and thereby measured.
        """
        now = time.time() if now is None else now
        states = [self.state(r) for r in routes]
        for st in states:
            self._decay(st, now)

        measured = [self.measured_capacity(st) for st in states]
        priors = [self.static_prior(r) for r in routes]

        both = [(m, p) for m, p in zip(measured, priors) if m]
        if both:
            scale = (sum(m for m, _ in both) / len(both)
                     / (sum(p for _, p in both) / len(both)))
        else:
            scale = 1.0

        out = []
        for st, cap, prior in zip(states, measured, priors):
            capacity = cap if cap else prior * scale

            # Everything below only ever REDUCES a route, and the reductions are
            # collected into one factor rather than applied one at a time. That
            # matters because they stack: a route that is fully loaded AND
            # parked AND low on headroom would otherwise be floored three times
            # over and end up at 0.05^3 of its size — 1 request in 8000, which
            # is starvation with extra steps.
            #
            # Flooring the PRODUCT is what makes weight_floor mean what
            # policy.yaml says it means, and it is what keeps the control loop
            # closed. A route believed to be full still receives 5% of its
            # capacity in real traffic, and those requests are the probe: they
            # cost nothing extra, they are indistinguishable from ordinary work,
            # and if the other tenant has gone away they simply succeed and the
            # reclaim above starts giving the route back. Without a floor here
            # the estimate could only ever go up, because the only thing that
            # can lower it is traffic we would no longer be sending.
            reduction = 1.0

            # Sample by what is actually LEFT, not by how big the deployment is
            # on paper. A route 60% consumed by other tenants has 40% to offer,
            # and weighting it at its full ceiling is how the proxy used to keep
            # pushing into a wall it could not see.
            #
            # Our own load is in here too, which makes this a closed loop rather
            # than a static split: sending to a route lowers its weight, so
            # traffic settles where every route carries the same FRACTION of its
            # own ceiling. That is the fixed point of weight_i = L_i(1 - f_i)
            # under proportional sampling, and it is the right definition of
            # balanced. It does not oscillate: the load it reads is a 60s
            # sliding average and the sampler is stochastic, so there is no
            # synchronised herd to swing.
            total = self.total_load(st, now)
            if total is not None:
                reduction *= max(0.0, 1.0 - total)

            head = self.headroom(st, now)
            high = self.cfg.headroom_high_water
            if head is not None and high > 0 and head < high:
                # Flat above the high-water mark, linear below it. Anything
                # smoother would be reacting to noise: `remaining` reads
                # near-full nearly always, so the only part of its range that
                # carries information is the bottom.
                reduction *= head / high

            reduction *= self.effective_penalty(st, now)
            out.append(capacity * max(self.cfg.weight_floor, reduction))
        return out

    # -- selection --------------------------------------------------------
    def order(self, routes: List[Route]) -> List[Route]:
        """The order to try `routes` in. Never drops or duplicates one."""
        if len(routes) < 2 or self.cfg.balance == "strict_priority":
            return list(routes)
        if self.cfg.balance == "capacity":
            return self._sample(routes)
        return self._priority_threshold(routes)

    def _priority_threshold(self, routes: List[Route]) -> List[Route]:
        """Priority order, but step over a route that is already busy."""
        now = time.time()
        threshold = self.cfg.spill_threshold
        for i, route in enumerate(routes):
            st = self.state(route)
            self._decay(st, now)
            if now < st.penalty_until:
                # Azure asked us to stay off this one. Being under threshold
                # does not override that — the ledger only knows about our own
                # traffic, and a Retry-After is the deployment telling us about
                # everyone's.
                continue
            load = self.total_load(st, now)
            if load is None or load < threshold:
                # Head is this route; the rest of the chain stays in priority
                # order, so failover remains the same predictable walk.
                return [route] + routes[:i] + routes[i + 1:]
        # Nothing is under its threshold. There is no "next" endpoint left to
        # overflow into, so spread by capacity rather than handing all of it
        # back to route one.
        return self._sample(routes)

    def _sample(self, routes: List[Route]) -> List[Route]:
        """Draw the whole list without replacement, in proportion to weight.

        Without replacement is what keeps the failover chain intact: the first
        draw is where the request goes, the rest are the order it falls back
        through, and no endpoint appears twice.
        """
        pool = list(routes)
        weights = self.weights(pool)
        chosen: List[Route] = []
        while pool:
            total = sum(weights)
            if total <= 0:
                # Every candidate is parked. Priority order is as good an answer
                # as any, and better than none: something has to be tried.
                chosen.extend(pool)
                break
            target = random.random() * total
            index = len(pool) - 1
            for i, w in enumerate(weights):
                target -= w
                if target <= 0:
                    index = i
                    break
            chosen.append(pool.pop(index))
            weights.pop(index)
        return chosen

    def note_attempt(self, route: Route) -> None:
        self.state(route).attempts += 1

    # -- reporting --------------------------------------------------------
    def report(self, routes_by_model: Dict[str, List[Route]]) -> dict:
        now = time.time()
        seen: Dict[str, dict] = {}
        models = {}
        for model, routes in sorted(routes_by_model.items()):
            weights = self.weights(routes, now)
            total = sum(weights) or 1.0
            models[model] = [
                {"route": str(r), "weight": round(w, 1),
                 "share": round(w / total, 4)}
                for r, w in zip(routes, weights)
            ]
            for r, weight in zip(routes, weights):
                st = self.state(r)
                requests, tokens = st.in_window(now, self.cfg.load_window)
                by_face = st.in_window_by_face(now, self.cfg.load_window)
                load_by_face = self.load_by_face(st, now)
                seen[st.key] = {
                    "endpoint": r.endpoint,
                    "deployment": r.deployment,
                    "model_version": r.model_version,
                    "priority": r.priority,
                    "weight": round(weight, 1),
                    # What ARM says this deployment was granted, from the probe.
                    # Reported next to the measured pair below because the two
                    # are the same quantity from two sources: a disagreement
                    # means the quota moved since the last probe.
                    "capacity_requests": r.capacity_requests,
                    "capacity_tokens": r.capacity_tokens,
                    "limit_requests": st.limit_requests,
                    "limit_tokens": st.limit_tokens,
                    # What THIS proxy has sent inside the load window, which is
                    # what the threshold mode acts on. Not the same thing as
                    # remaining_* below, and deliberately reported next to it:
                    # when the two disagree, someone else is on this deployment.
                    "sent_requests_in_window": requests,
                    "sent_tokens_in_window": round(tokens),
                    # The same two figures split four ways by FACES, and our
                    # load split the same way. The split is along whichever
                    # ceiling `load` is measuring against — named here rather
                    # than left to be guessed, because the four fractions are
                    # only meaningful next to the dimension they are fractions
                    # of, and they are built to sum to `our_load` exactly so a
                    # stacked bar drawn from them fills to the right place.
                    "sent_by_face": {
                        name: {"requests": pair[0], "tokens": round(pair[1])}
                        for name, pair in zip(FACES, by_face)},
                    "load_dimension": self.load_dimension(st, now),
                    # Six places, not four. A handful of requests against
                    # a 150k RPM ceiling is 2e-5 of it, which rounds to a flat
                    # zero at four — and a zero here is indistinguishable from
                    # "nothing was sent", which is the one thing this field
                    # exists to tell apart. The totals above keep four places:
                    # they are read as percentages, where the extra digits are
                    # noise.
                    "our_load_by_face": (
                        None if load_by_face is None else
                        {k: round(v, 6) for k, v in load_by_face.items()}),
                    # `load` is the old name for `our_load` and is kept so an
                    # existing dashboard or eyeball does not silently start
                    # reading nothing.
                    "load": (lambda l: None if l is None else round(l, 4))(
                        self.load(st, now)),
                    # Split out rather than folded together: a high total is
                    # actionable in completely different ways depending on
                    # whether it is us or someone else, and the age says whether
                    # the foreign figure is a fresh observation or one that has
                    # nearly been reclaimed away.
                    "our_load": (lambda l: None if l is None else round(l, 4))(
                        self.load(st, now)),
                    "foreign_load": round(self.foreign_load(st, now), 4),
                    "total_load": (lambda l: None if l is None else round(l, 4))(
                        self.total_load(st, now)),
                    "foreign_observed_age_seconds": (
                        None if not st.foreign_samples
                        else round(now - st.foreign_at, 1)),
                    "foreign_our_load_at_throttle": st.foreign_our_load,
                    "foreign_reclaim_per_minute": self.cfg.foreign_reclaim,
                    "foreign_reclaim_starts_in": max(
                        0.0, round(st.foreign_hold_until - now, 1)),
                    "foreign_samples": st.foreign_samples,
                    "spill_threshold": self.cfg.spill_threshold,
                    "tokens_per_request_estimate": (
                        None if st.tokens_per_char is None else
                        round(st.tokens_per_char, 4)),
                    "token_samples": st.token_samples,
                    "remaining_requests": st.remaining_requests,
                    "remaining_tokens": st.remaining_tokens,
                    "renewal_seconds": st.renewal_seconds,
                    "observed_age_seconds": (
                        None if st.observed_at is None
                        else round(now - st.observed_at, 1)),
                    "headroom": (lambda h: None if h is None else round(h, 4))(
                        self.headroom(st, now)),
                    # The multiplier actually in force, which inside a parking
                    # window is the floor rather than the accumulated penalty.
                    "penalty": round(self.effective_penalty(st, now), 4),
                    "penalty_recovering_to": round(st.penalty, 4),
                    "parked_for_seconds": max(
                        0.0, round(st.penalty_until - now, 1)),
                    "attempts": st.attempts,
                    "ok": st.ok,
                    "rate_limited": st.rate_limited,
                    "errors": st.errors,
                    "last_status": st.last_status,
                }
        return {"balance": self.cfg.balance,
                "load_window_seconds": self.cfg.load_window,
                # The canonical order of the four faces, so a reader stacking
                # them does not have to hard-code it and drift.
                "faces": list(FACES),
                "routes": seen, "models": models}


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
    """

    def __init__(self, config: Config):
        self.cfg = config
        # session key -> [endpoint name, last used]. Ordered so the oldest entry
        # is cheap to evict; a benchmark opens a bounded number of sessions but
        # a long-lived proxy should not grow without limit.
        self._pins: "collections.OrderedDict[str, List]" = \
            collections.OrderedDict()

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

    def key(self, request: Request, body: dict) -> Optional[str]:
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

    # -- the map ----------------------------------------------------------
    def _expire(self, now: float) -> None:
        ttl = self.cfg.affinity_ttl
        while self._pins:
            key, entry = next(iter(self._pins.items()))
            if now - entry[1] <= ttl:
                break
            self._pins.popitem(last=False)
        while len(self._pins) > self.cfg.affinity_max:
            self._pins.popitem(last=False)

    def pinned(self, key: Optional[str],
               routes: List[Route]) -> Optional[Route]:
        """The route this session is bound to, if it is still usable.

        A pin naming a route that no longer serves the model is dropped rather
        than approximated — the sibling deployment on the same endpoint is not
        the same destination, and neither is another endpoint. The state is lost
        either way, and refusing to route at all would be worse than routing
        somewhere it might work.
        """
        if not key:
            return None
        now = time.time()
        self._expire(now)
        entry = self._pins.get(key)
        if entry is None:
            return None
        for route in routes:
            if str(route) == entry[0]:
                entry[1] = now
                self._pins.move_to_end(key)
                return route
        _ev("pin", "warning",
            "session pinned to %s, which no longer serves this model; "
            "dropping the pin", entry[0], route=entry[0], dropped=True)
        self._pins.pop(key, None)
        return None

    def pin(self, key: Optional[str], route: Route) -> None:
        if not key:
            return
        entry = self._pins.get(key)
        if entry is None:
            _ev("pin", "info", "session pinned to %s (%d live)", route,
                len(self._pins) + 1, route=route, live=len(self._pins) + 1)
            self._pins[key] = [str(route), time.time()]
        else:
            entry[0], entry[1] = str(route), time.time()
        self._pins.move_to_end(key)
        self._expire(time.time())

    def report(self) -> dict:
        now = time.time()
        self._expire(now)
        # Per route, and per endpoint underneath it. The endpoint total is the
        # one to read for "is affinity pushing everything at one resource"; the
        # route breakdown is what says which deployment is carrying it.
        routes: Dict[str, int] = {}
        counts: Dict[str, int] = {}
        for route, _ts in self._pins.values():
            routes[route] = routes.get(route, 0) + 1
            endpoint = route.split("/", 1)[0]
            counts[endpoint] = counts.get(endpoint, 0) + 1
        return {"enabled": self.cfg.affinity_enabled,
                "on_conflict": self.cfg.affinity_on_conflict,
                "live_sessions": len(self._pins),
                "sessions_per_route": routes,
                "sessions_per_endpoint": counts}


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


def write_pidfile() -> None:
    """Claim the pidfile.

    Written by the process itself rather than by start.sh, which used `$!`.
    The difference that matters is the other end: the process can drop the file
    on its way out, so a clean shutdown does not leave a pid behind for stop.sh
    to have to recognise as debris.

    Under ROOT, not under the working directory. A pidfile identifies one
    INSTALLATION's running proxy, and resolving it against cwd made it identify
    "whatever directory someone happened to launch from" instead — which the
    test suite discovered the hard way: it spawns proxies with AZURE_PROXY_HOME
    pointed at a temporary tree but inherits the caller's cwd, so running the
    tests from the repo overwrote the live service's pidfile and left it
    orphaned, answering on its port with nothing able to stop it.
    """
    with open(PIDFILE, "w") as f:
        f.write("{}\n".format(os.getpid()))


def clear_pidfile() -> None:
    """Drop the claim. Safe to call twice, and on a file someone else removed."""
    try:
        os.unlink(PIDFILE)
    except OSError:
        pass


cfg = Config()
_setup_logging(cfg.log_level)
tokens = TokenCache(cfg.scope, cfg.refresh_margin)
quota = QuotaTracker(cfg)
affinity = SessionAffinity(cfg)
app = FastAPI(title="azure-proxy", docs_url=None, redoc_url=None)
client: Optional[httpx.AsyncClient] = None
_refresher: Optional[asyncio.Task] = None
STARTED_AT = time.time()


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

    for name, chat, responses in cfg.endpoints:
        log.info("endpoint %-28s chat=%-22s responses=%s", name, chat, responses)
    # One record for the whole of the above rather than fifteen. The startup
    # narration is a dozen lines because a person reading a log wants them
    # separately; an event stream wants the one line that says the proxy came
    # up, and everything those dozen lines established is already in /healthz.
    _ev("boot", "info",
        "%d models on /v1/chat/completions, %d on /v1/responses, "
        "probed at %s",
        len(cfg.routes), len(cfg.responses_routes), cfg.generated_at,
        chat_models=len(cfg.routes), responses_models=len(cfg.responses_routes),
        balance=cfg.balance, endpoints=[e[0] for e in cfg.endpoints],
        account=account, probed_at=cfg.generated_at)


@app.on_event("startup")
async def _startup():
    global client, _refresher
    client = httpx.AsyncClient(timeout=httpx.Timeout(cfg.timeout, connect=15.0))
    await tokens.get()      # fail loudly at boot rather than on the first request
    _refresher = asyncio.create_task(tokens.run_background_refresh())
    _log_startup()


@app.on_event("shutdown")
async def _shutdown():
    if _refresher:
        _refresher.cancel()
    if client:
        await client.aclose()


@app.get("/healthz")
async def healthz():
    token = tokens.status()
    ok = token["have_token"] and token["expires_in_seconds"] > 0
    return {"ok": ok,
            "models": len(cfg.routes),
            "responses_models": len(cfg.responses_routes),
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
            "token": token}


@app.get("/routes")
async def routes_report():
    """Why each request went where it went.

    Routing is the one thing here whose behaviour cannot be read off the config
    — it depends on quota headers Azure sent minutes ago, on how much of the
    window the proxy has already spent, and on 429s that have since decayed
    away. When the split looks wrong, this is the only place that can say
    whether the weights, the load fractions, the headroom brake or a demotion is
    responsible. Numbers only; no request or response content passes through it.

    `sent_*_in_window` next to `remaining_*` is the pairing worth reading: the
    first is this proxy's own ledger, the second is Azure's. When they tell
    different stories, someone else is spending the same deployment's quota.
    """
    # Both faces draw from the same (endpoint, deployment) buckets, so union
    # them rather than reporting one model twice under two headings.
    merged: Dict[str, List[Route]] = dict(cfg.routes)
    for name, routes in cfg.responses_routes.items():
        merged.setdefault(name, routes)
    report = quota.report(merged)
    # Which endpoint a request lands on is no longer decided by the balancer
    # alone: a pinned session overrides it entirely. Reporting the pins next to
    # the weights is what makes an "unbalanced" split explainable rather than
    # mysterious.
    report["session_affinity"] = affinity.report()
    # Which faces each model can actually be reached on. The dashboard's model
    # view needs it to explain why a model with three sources shows traffic on
    # only one of the four bar segments — a Responses-only model cannot produce
    # a chat segment, and that is configuration, not an anomaly.
    report["model_faces"] = {
        name: [f for f, table in (("chat", cfg.routes),
                                  ("responses", cfg.responses_routes))
               if name in table]
        for name in merged}
    report["endpoints"] = [
        {"name": name, "chat": chat, "responses": responses}
        for name, chat, responses in cfg.endpoints]
    return report


@app.get("/events")
async def events_feed(since: int = 0, limit: int = 500,
                      kind: Optional[str] = None):
    """What the proxy has been doing, as records rather than prose.

    The same events the log narrates — see proxy/events.py for why they are
    also kept structured. Poll with the `next` cursor from the previous reply;
    `dropped` is true when the ring turned over faster than the reader read it,
    so a gap can be labelled instead of silently closed up.

    `kind` is a comma-separated filter, or the word `problems` for the set that
    means something needs looking at. Numbers, names and statuses only — no
    request or response content reaches this, the same rule the log follows.
    """
    if kind == "problems":
        kinds = PROBLEM_KINDS
    elif kind:
        kinds = frozenset(k.strip() for k in kind.split(",") if k.strip())
    else:
        kinds = None
    found, cursor, dropped = events.since(since, limit=limit, kinds=kinds)
    return {"events": found, "next": cursor, "dropped": dropped,
            "counts": events.counts()}


@app.get("/v1/models")
async def list_models():
    # The union of the two faces, not the chat one. A model can be served on
    # /v1/responses and nowhere else — the pro and codex deployments are — and
    # listing only what chat can reach would leave them undiscoverable.
    names = sorted(set(cfg.routes) | set(cfg.responses_routes))
    return {
        "object": "list",
        "data": [
            {
                "id": name,
                "object": "model",
                "owned_by": "azure",
                # Not part of the OpenAI schema; harmless to clients and the
                # fastest way to see a model's failover depth and which of the
                # two faces it can be reached on.
                "routes": [r.endpoint for r in
                           (cfg.routes.get(name)
                            or cfg.responses_routes.get(name) or [])],
                "faces": ([f for f, table in (("chat", cfg.routes),
                                              ("responses",
                                               cfg.responses_routes))
                           if name in table]),
            }
            for name in names
        ],
    }


def _upstream_headers(incoming, token: str) -> dict:
    headers = {"Authorization": "Bearer " + token,
               "Content-Type": "application/json"}
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
           entry: Optional[List[float]] = None,
           counted: bool = False) -> Response:
    body = resp.content
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
            quota.demote(route, "429", resp.headers.get("retry-after"))
    quota.settle(route, entry, request_bytes, _read_total_tokens(body))
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

    Answers two questions from raw bytes: is this 200 actually a throttle, and
    what did it cost. Carries a few bytes of overlap between chunks so neither
    marker can hide on a chunk boundary.

    The scan is NOT bounded to the head of the stream any more, and that was the
    bug that made the first version of this useless. It used to stop after 16KB
    on the theory that a refusal always arrives early. A refusal does arrive
    early in *event order* — but a codex-shaped `response.created` echoes the
    whole request back and is itself larger than 16KB, so the marker landed past
    the end of the window and 184 refusals in a single run were counted as
    successes. A bytes.find per chunk costs nothing next to the HTTP it is
    already doing; guessing where the interesting part is costs correctness.
    """

    __slots__ = ("_overlap", "rate_limited", "total_tokens")

    def __init__(self):
        self._overlap = b""
        self.rate_limited = False
        self.total_tokens: Optional[int] = None

    def feed(self, chunk: bytes) -> bool:
        """Returns True on the chunk that first reveals a rate limit."""
        window = self._overlap + chunk
        found = False
        if not self.rate_limited and _inband_error(window):
            self.rate_limited = found = True
        total = _read_total_tokens(window)
        if total is not None:
            self.total_tokens = total
        self._overlap = window[-OVERLAP_BYTES:]
        return found


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
                  entry: Optional[List[float]] = None,
                  counted: bool = False,
                  model: Optional[str] = None) -> StreamingResponse:
    """Forward the upstream body byte for byte as it arrives.

    Nothing here parses SSE, reframes it, or waits for it. Passing the bytes
    through unexamined is what keeps fields the proxy has never heard of —
    encrypted reasoning among them — intact on the way back. The bytes the probe
    already read go out first, in the chunks they arrived in.

    The scan looks but does not touch. Its answers only reach the quota tracker,
    which is the whole point: past this line the stream belongs to the caller
    and cannot be retried, so the only thing left to influence is where the
    *next* request goes. `counted` says a rate limit was already charged during
    the probe, so a stream relayed anyway on the last route is not counted twice.
    """
    async def body():
        sent = 0
        watch = _StreamWatch()
        watch.rate_limited = counted
        # Only accumulated when diagnostics are on; otherwise it stays empty and
        # the stream is forwarded without ever being held in memory.
        keep = bool(cfg.capture_dir)
        seen = bytearray() if keep else None

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
                quota.demote(route, "429", resp.headers.get("retry-after"))

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
            await resp.aclose()
            quota.settle(route, entry, request_bytes, watch.total_tokens)
            if keep:
                _capture_stream(route, resp, bytes(seen))
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


async def _forward(request: Request, body: dict, routes: List[Route],
                   target_of: Callable[[Route], str], requested: str,
                   face: str) -> Response:
    """Try each route in turn until one answers. Shared by both faces."""
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

    headers = _upstream_headers(request.headers, token)

    # Where this request is allowed to go. A conversation carrying encrypted
    # reasoning is bound to the endpoint that produced it (see SessionAffinity),
    # so once pinned its attempt list is that ONE endpoint, tried repeatedly,
    # rather than a walk across the others. Moving it would trade a slow request
    # for a guaranteed decryption failure.
    session = affinity.key(request, body) if cfg.affinity_enabled else None
    sticky = bool(session) and affinity.sticky(body)
    pinned = affinity.pinned(session, routes) if sticky else None
    if pinned is not None and cfg.affinity_on_conflict == "wait":
        attempts = [pinned] * max(1, 1 + cfg.affinity_attempts)
    elif pinned is not None:
        # `switch` keeps the old behaviour: try the pin first, then fail over
        # like anything else. Faster, and wrong for codex.
        attempts = [pinned] + [r for r in routes if r is not pinned]
        attempts = attempts[:cfg.max_attempts]
    else:
        # Balancing decides the order; max_attempts still decides the depth, and
        # the list is a permutation of `routes`, so no endpoint is tried twice.
        attempts = quota.order(routes)[:cfg.max_attempts]
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
        """
        if held:
            wait = _as_number(hint) or cfg.backoff_initial
            wait = min(max(wait, 0.5), cfg.affinity_max_wait)
            return wait, "waiting {:.1f}s for pinned {}".format(wait, next_route)
        return (delay + random.random() * cfg.backoff_jitter,
                "retrying {}".format(next_route))
    delay = cfg.backoff_initial
    last_error: Optional[str] = None
    retry_after_hint: Optional[str] = None
    started = time.monotonic()
    # The size of the body the caller sent, which is all the proxy knows about
    # what this request will cost before the answer comes back. See
    # QuotaTracker.estimate_tokens.
    request_bytes = _as_number(request.headers.get("content-length")) or 0

    for i, route in enumerate(attempts):
        is_last = i == len(attempts) - 1
        payload = dict(body)
        payload["model"] = route.deployment
        quota.note_attempt(route)
        entry = quota.charge(route, quota.estimate_tokens(route, request_bytes),
                             ledger_face)
        log.debug("%s model=%s attempt %d/%d on %s",
                  face, requested, i + 1, len(attempts), route)

        try:
            # stream=True returns as soon as the response headers are in, which
            # is what makes the retry decision possible before any body has been
            # handed to the client. A buffered reply just reads it straight back.
            upstream = client.build_request("POST", target_of(route),
                                            json=payload, headers=headers)
            resp = await client.send(upstream, stream=True)
        except httpx.TimeoutException:
            # A slow reasoning call and a hung one are indistinguishable here.
            # Retrying would pay twice for the same prompt while the original
            # may still be running, so the caller decides instead. For the same
            # reason this does not demote the route: slow is not broken, and a
            # long reasoning turn must not cost an endpoint its share.
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
            quota.failed(route, "transport")
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
            quota.observed(route, resp.status_code, resp.headers)
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
                quota.demote(route, "429" if throttled else str(resp.status_code),
                             resp.headers.get("retry-after"))

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
                        quota.demote(route,
                                     "429" if head.retry_reason else reason,
                                     resp.headers.get("retry-after"))
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
                    affinity.pin(session, route)
                return _relay_stream(resp, route, face, started, head,
                                     request_bytes, entry,
                                     counted=bool(head.retry_reason) or throttled,
                                     model=requested)
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
                if sticky and resp.status_code < 400:
                    affinity.pin(session, route)
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

    _ev("exhausted", "error",
        "<- 503 %s model=%s: all %d route(s) failed; last: %s",
        face, requested, len(attempts), last_error,
        face=face, model=requested, attempts=len(attempts),
        status=503, error=last_error,
        tried=[str(r) for r in attempts])
    return _error(503, "all {} route(s) for {!r} failed; last: {}".format(
        len(attempts), requested, last_error), "all_routes_failed")


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body, requested, err = await _body_and_model(request)
    if err is not None:
        return err

    routes = cfg.routes.get(requested)
    if not routes:
        if requested in cfg.responses_routes:
            # The mirror of the case below, and now a real one: gpt-5-pro and
            # the codex models answer chat/completions with a flat 400 and are
            # served only on /v1/responses. The name is right and the model
            # works — telling the caller it does not exist would send them
            # hunting for a typo that is not there.
            return _error(
                404,
                "model {!r} has no chat/completions route (it is reachable on "
                "/v1/responses). Models with a chat face: {}".format(
                    requested, ", ".join(sorted(cfg.routes)) or "none"),
                "no_chat_route")
        return _error(
            404,
            "unknown model {!r}; available: {}".format(
                requested, ", ".join(sorted(cfg.routes))),
            "model_not_found")

    return await _forward(request, body, routes, Route.chat_target, requested,
                          "/v1/chat/completions")


@app.post("/v1/responses")
async def responses(request: Request):
    body, requested, err = await _body_and_model(request)
    if err is not None:
        return err

    routes = cfg.responses_routes.get(requested)
    if not routes:
        if requested in cfg.routes:
            # Worth distinguishing from an unknown model: the name is right and
            # the model works, just not on this face. Sending "unknown model"
            # would send someone hunting for a typo that is not there.
            return _error(
                404,
                "model {!r} has no Responses API route (it is reachable on "
                "/v1/chat/completions). Models with a responses face: {}".format(
                    requested,
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

    return await _forward(request, body, routes, Route.responses_target,
                          requested, "/v1/responses")
