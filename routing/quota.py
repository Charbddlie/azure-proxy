"""Quota estimation and target selection, owned by the routing process."""

import collections
import json
import logging
import math
import os
import random
import time
from typing import Deque, Dict, List, Optional, Tuple

from proxy.config import Config, Route, FACES, _as_number
from proxy.events import event_level

log = logging.getLogger("azure-proxy.routing")
DEFAULT_TPM = 1_000_000.0


def _ev(kind, level, msg, *args, **fields):
    level = event_level(kind, level, fields)
    log.log(getattr(logging, level.upper(), logging.INFO), msg, *args)


class RouteState:
    """Observed quota and recent failures for one (endpoint, deployment)."""

    __slots__ = ("key", "limit_requests", "limit_tokens", "remaining_requests",
                 "remaining_tokens", "renewal_seconds", "observed_at",
                 "penalty", "penalty_until", "penalty_at", "attempts", "ok",
                 "rate_limited", "errors", "last_status", "sent",
                 "tokens_per_char", "token_samples",
                 "safe_rpm", "other_rpm", "rpm_samples",
                 "last_dispatch_rpm", "last_throttle_rpm",
                 "timeouts", "last_timeout_rpm",
                 "foreign", "foreign_at", "foreign_hold_until",
                 "foreign_samples", "foreign_our_load", "foreign_seen")

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

        # Learned capacity. A successful request proves the dispatch rate at
        # that instant was safe, so the largest such observation is a lower
        # bound on the route's capacity. It only grows and is persisted by the
        # tracker. A throttle below that bound exposes concurrent traffic from
        # other users: safe_rpm - our_rpm.
        self.safe_rpm = 0.0
        self.other_rpm = 0.0
        self.rpm_samples = 0
        self.last_dispatch_rpm = 0.0
        self.last_throttle_rpm: Optional[float] = None
        self.timeouts = 0
        self.last_timeout_rpm: Optional[float] = None

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
        # Monotonic history, retained in routing checkpoints across restarts.
        self.foreign_seen = False

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
    so the failover chain stays intact and no deployment can be tried twice:

    `strict_priority` returns the probe's order untouched.

    `capacity` samples deployments with no history of positive others usage
    uniformly first, followed by previously contended deployments weighted by
    available capacity. Both groups are sampled without replacement.

    `priority_threshold` walks the priority order and heads for the first route
    that is not already carrying more than `spill_threshold` of its own quota.
    The equilibrium is worth being explicit about, because it is the whole
    design: load is measured over a sliding window, so once the top route is
    pinned at the threshold each individual request tips it over, goes to the
    next route instead, and lets the top route fall back under. The split is
    therefore per-request rather than in blocks, the top route stabilises at
    exactly the threshold, and the overflow — and only the overflow — moves
    down the chain. If every route is over its threshold there is no overflow
    destination left, so it falls back to available-capacity-weighted sampling.
    """

    def __init__(self, config: Config, clock=None, emit=None, persist_capacity=True):
        self.cfg = config
        self.clock = clock or time.time
        self.emit = emit or _ev
        self.persist_capacity = persist_capacity
        self.rpm_window = max(1.0, float(getattr(config, "rpm_window", 60.0)))
        self.capacity_state_file = getattr(config, "capacity_state_file", None)
        self.states: Dict[str, RouteState] = {}
        self.image_routes = {
            str(route)
            for table in ("image_routes", "image_edit_routes")
            for routes in getattr(config, table, {}).values()
            for route in routes}
        self._legacy_qps_capacity = False
        self._saved_capacity = self._load_capacity()
        if self._legacy_qps_capacity:
            self._save_capacity()

    def _load_capacity(self) -> Dict[str, float]:
        """Load monotonic safe-RPM observations from the previous process."""
        if not self.capacity_state_file:
            return {}
        try:
            with open(self.capacity_state_file) as f:
                doc = json.load(f)
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return {}
        values = doc.get("routes") if isinstance(doc, dict) else None
        if not isinstance(values, dict):
            return {}
        # Older files used QPM for the same per-minute unit; their values
        # carry over unchanged. Only the earlier QPS format needs scaling.
        self._legacy_qps_capacity = (
            "qps_window_seconds" in doc
            and "rpm_window_seconds" not in doc
            and "qpm_window_seconds" not in doc)
        scale = 60.0 if self._legacy_qps_capacity else 1.0
        out = {}
        for key, value in values.items():
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            if value > 0 and math.isfinite(value):
                out[str(key)] = value * scale
        return out

    def _save_capacity(self) -> None:
        """Atomically save every learned maximum as soon as one increases."""
        if not self.persist_capacity:
            return
        path = self.capacity_state_file
        if not path:
            return
        directory = os.path.dirname(path)
        try:
            os.makedirs(directory, exist_ok=True)
            temporary = "{}.{}.tmp".format(path, os.getpid())
            with open(temporary, "w") as f:
                json.dump({"updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                       time.gmtime()),
                           "rpm_window_seconds": self.rpm_window,
                           "routes": dict(sorted(self._saved_capacity.items()))},
                          f, indent=2, sort_keys=True)
                f.write("\n")
            os.replace(temporary, path)
        except OSError as e:
            log.warning("could not save learned capacity to %s: %s", path, e)

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
            st.safe_rpm = self._saved_capacity.get(key, 0.0)
        return st

    def observed(self, route: Route, status: int, headers) -> None:
        """Fold one upstream response into the route's state."""
        st = self.state(route)
        st.last_status = str(status)

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
            st.observed_at = self.clock()

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
        now = self.clock()
        st = self.state(route)
        st.prune(now, max(self.cfg.load_window, self.rpm_window))
        cutoff = now - self.rpm_window
        recent = sum(1 for item in st.sent if item[0] >= cutoff)
        dispatch_rpm = (recent + 1) * 60.0 / self.rpm_window
        entry = [now, float(tokens), face, dispatch_rpm]
        st.sent.append(entry)
        st.last_dispatch_rpm = dispatch_rpm
        return entry

    def rpm(self, st: RouteState, now: float) -> float:
        """Requests per minute over the configured rolling window."""
        cutoff = now - self.rpm_window
        return sum(1 for item in st.sent if item[0] >= cutoff) \
            * 60.0 / self.rpm_window

    def rpm_by_face(self, st: RouteState, now: float) -> Dict[str, float]:
        cutoff = now - self.rpm_window
        counts = [0] * len(FACES)
        for item in st.sent:
            if item[0] >= cutoff:
                counts[item[2] if len(item) > 2 else 0] += 1
        return {name: count * 60.0 / self.rpm_window
                for name, count in zip(FACES, counts)}

    def note_success(self, route: Route,
                     entry: Optional[List[float]]) -> None:
        """Raise the saved capacity to the largest proven-safe dispatch RPM."""
        st = self.state(route)
        observed = (float(entry[3]) if entry is not None and len(entry) > 3
                    else self.rpm(st, self.clock()))
        if observed <= 0:
            return
        st.ok += 1
        st.rpm_samples += 1
        if observed > st.safe_rpm:
            before = st.safe_rpm
            st.safe_rpm = observed
            self._saved_capacity[st.key] = observed
            self._save_capacity()
            self.emit("capacity", "info",
                "%s raised learned capacity from %.2f to %.2f RPM",
                route, before, observed, route=route,
                capacity_before=round(before, 4),
                capacity_after=round(observed, 4))
        # A success at this rate proves a previous `others` observation can no
        # longer make the route exceed its learned capacity.
        st.other_rpm = min(st.other_rpm,
                           max(0.0, st.safe_rpm - observed))

    def note_timeout(self, route: Route,
                     observed_rpm: Optional[float] = None) -> None:
        """Record a request that produced no complete upstream response."""
        st = self.state(route)
        st.timeouts += 1
        st.last_status = "timeout"
        st.last_timeout_rpm = (observed_rpm if observed_rpm is not None
                               else st.last_dispatch_rpm or None)

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
        Retry-After has passed. This remains an internal routing signal; the
        dashboard reports the separate safe-RPM observation.
        """
        if not self.cfg.foreign_enabled or st.foreign <= 0.0:
            return 0.0
        rate = self.cfg.foreign_reclaim
        if rate <= 0:
            return st.foreign
        start = max(st.foreign_at, st.foreign_hold_until)
        if now <= start:
            return st.foreign
        return max(0.0, st.foreign - (now - start) / 60.0 * rate)

    def total_load(self, st: RouteState, now: float) -> Optional[float]:
        """Our load plus everyone else's. None only if the ceiling is unknown.

        priority_threshold uses this combined load to decide when to spill.
        Capacity sampling subtracts foreign and current usage in capacity units.
        """
        ours = self.load(st, now)
        if ours is None:
            return None
        return ours + self.foreign_load(st, now)

    def note_foreign(self, route: Route, retry_after=None,
                     observed_rpm: Optional[float] = None) -> Optional[dict]:
        """At a throttle, capacity minus our RPM is concurrent outside RPM."""
        now = self.clock()
        st = self.state(route)
        our_rpm = (observed_rpm if observed_rpm is not None
                   else st.last_dispatch_rpm or self.rpm(st, now))
        st.last_throttle_rpm = our_rpm
        before_rpm = st.other_rpm
        outside_rpm = max(0.0, st.safe_rpm - our_rpm)
        st.other_rpm = outside_rpm

        ours = self.load(st, now)
        held = self.foreign_load(st, now)
        if self.cfg.foreign_enabled and ours is not None:
            observed = min(1.0, max(0.0, 1.0 - ours))
            st.foreign = max(held, observed)
        st.foreign_seen = st.foreign_seen or st.foreign > 0.0
        st.foreign_at = now
        park = _as_number(retry_after)
        st.foreign_hold_until = now + max(0.0, park or 0.0)
        st.foreign_samples += 1
        st.foreign_our_load = ours
        moved = {"our_load": (round(ours, 4) if ours is not None else None),
                 "our_rpm": round(our_rpm, 4),
                 "capacity_rpm": (round(st.safe_rpm, 4)
                                  if st.safe_rpm > 0 else None),
                 "other_rpm": round(outside_rpm, 4),
                 "foreign_before": round(held, 4),
                 "foreign_after": round(st.foreign, 4),
                 "foreign_samples": st.foreign_samples,
                 "foreign_hold_seconds": round(max(0.0, park or 0.0), 1)}
        if outside_rpm > 0:
            self.emit("foreign", "info",
                "%s throttled at %.2f RPM below its %.2f RPM learned maximum; "
                "the %.2f RPM difference is others",
                route, our_rpm, st.safe_rpm, outside_rpm,
                route=route, **moved)
        return moved if ours is not None or st.safe_rpm > 0 else None

    def demote(self, route: Route, reason: str, retry_after=None,
               observed_rpm: Optional[float] = None) -> None:
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
        now = self.clock()
        st = self.state(route)
        moved = None
        if reason == "429":
            st.rate_limited += 1
            # Only rate limits carry information about other tenants. A 500 or a
            # connection reset means the endpoint is unwell, not that its quota
            # is spoken for, and inferring a foreign share from one would
            # permanently shrink a route for being briefly broken.
            moved = self.note_foreign(route, retry_after, observed_rpm)
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
        self.emit("throttle" if reason == "429" else "demote", "info",
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
        elif st.penalty > 0:
            # Clamp in log space before exponentiation, including long replay gaps.
            exponent = math.log2(st.penalty) + (now - start) / halflife
            st.penalty = 2 ** min(0.0, exponent)
        st.penalty_at = now

    # -- weights ----------------------------------------------------------
    def capacity_estimates(self, routes: List[Route]) -> List[float]:
        """Per-model ceilings: observed, declared, known mean, then 1M TPM.

        Image deployments use a separate RPM scale with a 1 RPM cold start.
        Imputed capacities stay out of RouteState so only actual observations
        and declarations contribute to subsequent means.
        """
        known = {}
        units = {}
        for route in routes:
            key = str(route)
            unit = units[key] = "requests" if key in self.image_routes else "tokens"
            state = self.state(route)
            for value in (getattr(state, "limit_" + unit),
                          getattr(route, "capacity_" + unit)):
                value = _as_number(value)
                if value is not None and value > 0:
                    known[key] = value
                    break
        defaults = {"tokens": DEFAULT_TPM, "requests": 1.0}
        for unit in defaults:
            values = [cap for key, cap in known.items() if units[key] == unit]
            if values:
                defaults[unit] = math.fsum(values) / len(values)
        return [known.get(str(route), defaults[units[str(route)]]) for route in routes]

    def usage_rate(self, route: Route, now: float) -> float:
        """Our rolling dispatch usage in TPM (RPM for image deployments)."""
        st = self.state(route)
        if str(route) in self.image_routes:
            return self.rpm(st, now)
        cutoff = now - self.cfg.load_window
        return sum(entry[1] for entry in st.sent if entry[0] >= cutoff) \
            * 60.0 / self.cfg.load_window

    def headroom(self, st: RouteState, now: float) -> Optional[float]:
        """Diagnostic remaining fraction; expired observations are unknown."""
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
        """Sample capacity available to us, with failure backoff and one floor."""
        now = self.clock() if now is None else now
        out = []
        for route, capacity in zip(routes, self.capacity_estimates(routes)):
            st = self.state(route)
            self._decay(st, now)
            other = capacity * self.foreign_load(st, now)
            available = max(0.0, capacity - other - self.usage_rate(route, now))
            # Apply the floor once, after total usage and temporary failures.
            out.append(max(capacity * self.cfg.weight_floor,
                           available * self.effective_penalty(st, now)))
        return out

    # -- selection --------------------------------------------------------
    def selection_parameters(self, routes: List[Route], now=None):
        """Routes whose others usage has always been zero are explored uniformly."""
        now = self.clock() if now is None else now
        weights = self.weights(routes, now)
        if self.cfg.balance != "capacity":
            return [(0, weight) for weight in weights]
        return [(0, 1.0) if not self.state(route).foreign_seen
                else (1, weight) for route, weight in zip(routes, weights)]

    def order(self, routes: List[Route]) -> List[Route]:
        """The order to try `routes` in. Never drops or duplicates one."""
        if len(routes) < 2 or self.cfg.balance == "strict_priority":
            return list(routes)
        if self.cfg.balance == "capacity":
            return self._sample(routes)
        return self._priority_threshold(routes)

    def _priority_threshold(self, routes: List[Route]) -> List[Route]:
        """Priority order, but step over a route that is already busy."""
        now = self.clock()
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
        """Draw each selection group without replacement, in priority order."""
        pool = list(routes)
        parameters = self.selection_parameters(pool)
        chosen: List[Route] = []
        while pool:
            priority = min(p for p, _ in parameters)
            weights = [w if p == priority else 0.0 for p, w in parameters]
            total = sum(weights)
            if total <= 0:
                index = next(i for i, (p, _) in enumerate(parameters) if p == priority)
                chosen.append(pool.pop(index))
                parameters.pop(index)
                continue
            target = random.random() * total
            index = max(i for i, w in enumerate(weights) if w > 0)
            for i, w in enumerate(weights):
                if w > 0 and target < w:
                    index = i
                    break
                target -= w
            chosen.append(pool.pop(index))
            parameters.pop(index)
        return chosen

    def note_attempt(self, route: Route) -> None:
        self.state(route).attempts += 1

    # -- reporting --------------------------------------------------------
    def report(self, routes_by_model: Dict[str, List[Route]]) -> dict:
        now = self.clock()
        seen: Dict[str, dict] = {}
        models = {}
        for model, routes in sorted(routes_by_model.items()):
            weights = self.weights(routes, now)
            capacities = self.capacity_estimates(routes)
            parameters = self.selection_parameters(routes, now)
            first_priority = min((p for p, _ in parameters), default=0)
            total = sum(w for p, w in parameters if p == first_priority) or 1.0
            models[model] = [
                {"route": str(r), "weight": round(w, 1),
                 "selection_priority": priority, "selection_weight": selection_weight,
                 "share": (round(selection_weight / total, 4)
                           if priority == first_priority else 0.0)}
                for r, w, (priority, selection_weight) in zip(routes, weights, parameters)
            ]
            for r, weight, capacity, (priority, selection_weight) in zip(
                    routes, weights, capacities, parameters):
                st = self.state(r)
                tpm = capacity if str(r) not in self.image_routes else None
                other_tpm = tpm * self.foreign_load(st, now) if tpm is not None else None
                our_tpm = self.usage_rate(r, now) if tpm is not None else None
                requests, tokens = st.in_window(now, self.cfg.load_window)
                by_face = st.in_window_by_face(now, self.cfg.load_window)
                load_by_face = self.load_by_face(st, now)
                current_rpm = self.rpm(st, now)
                rpm_by_face = self.rpm_by_face(st, now)
                seen[st.key] = {
                    "endpoint": r.endpoint,
                    "deployment": r.deployment,
                    "model_version": r.model_version,
                    "priority": r.priority,
                    "weight": round(weight, 1),
                    "selection_priority": priority,
                    "selection_weight": selection_weight,
                    "estimated_capacity_tpm": round(tpm, 1) if tpm is not None else None,
                    "other_tpm": round(other_tpm, 1) if other_tpm is not None else None,
                    "our_tpm": round(our_tpm, 1) if our_tpm is not None else None,
                    "available_tpm": (round(max(0.0, tpm - other_tpm - our_tpm), 1)
                                      if tpm is not None else None),
                    # What ARM says this deployment was granted, from the probe.
                    # Reported next to the measured pair below because the two
                    # are the same quantity from two sources: a disagreement
                    # means the quota moved since the last probe.
                    "capacity_requests": r.capacity_requests,
                    "capacity_tokens": r.capacity_tokens,
                    # Runtime capacity is learned from traffic. The largest RPM
                    # that completed without a limit is monotonic and persisted;
                    # a later throttle below it exposes the difference as other
                    # users' concurrent RPM.
                    "current_rpm": round(current_rpm, 4),
                    "capacity_rpm": (round(st.safe_rpm, 4)
                                     if st.safe_rpm > 0 else None),
                    "other_rpm": round(st.other_rpm, 4),
                    "rpm_window_seconds": self.rpm_window,
                    "rpm_samples": st.rpm_samples,
                    "last_throttle_rpm": (round(st.last_throttle_rpm, 4)
                                          if st.last_throttle_rpm is not None
                                          else None),
                    "timeouts": st.timeouts,
                    "last_timeout_rpm": (round(st.last_timeout_rpm, 4)
                                         if st.last_timeout_rpm is not None
                                         else None),
                    "rpm_by_face": {k: round(v, 4)
                                    for k, v in rpm_by_face.items()},
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
                    "foreign_seen": st.foreign_seen,
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
                "rpm_window_seconds": self.rpm_window,
                # The canonical order of the four faces, so a reader stacking
                # them does not have to hard-code it and drift.
                "faces": list(FACES),
                "routes": seen, "models": models}
