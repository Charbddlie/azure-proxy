"""One poll's worth of JSON, regrouped into the two ways it gets looked at.

/routes is keyed by `endpoint/deployment` — the pair Azure grants quota to, and
therefore the only key the balancer can work in. But neither question an
operator actually asks is shaped like that:

  *by source*  — this resource is one bucket of money and one blast radius.
                 Which of its deployments are being used, and how full is each?
  *by model*   — a caller asks for `gpt-5.4`. How many places can serve that,
                 how big is each, and is any of them in trouble?

Both are the same rows sorted differently, so both are built here and neither
board does its own arithmetic. When the two views disagreed in an earlier cut
it was because each was summing the routes it happened to have to hand.
"""

from typing import Dict, List, Optional

import re
import time


def _rpm_fields(data: dict) -> dict:
    """Read older proxy snapshots using the current per-minute field names."""
    out = dict(data)
    for key in data:
        if "qpm" in key:
            out.setdefault(key.replace("qpm", "rpm"), data[key])
            out.pop(key)
    if isinstance(out.get("message"), str):
        out["message"] = out["message"].replace("QPM", "RPM")
    return out


class RouteView:
    """One (endpoint, deployment) row, with the model it serves attached.

    /routes reports the route table and the model table separately — the route
    entries do not name their model, because the balancer never needs to know.
    The dashboard does, in both directions, so the join happens once here.
    """

    __slots__ = ("key", "model", "share", "data")

    def __init__(self, key: str, model: Optional[str], share: Optional[float],
                 data: dict):
        self.key = key
        self.model = model
        self.share = share
        self.data = _rpm_fields(data)

    def __getattr__(self, name):
        # Everything /routes reports is readable as an attribute, so adding a
        # field to the report does not also require adding it here. Guarded
        # against `data` itself being the miss, which would recurse forever.
        if name == "data":
            raise AttributeError(name)
        try:
            return self.data[name]
        except KeyError:
            raise AttributeError(name)

    @property
    def endpoint(self) -> str:
        return self.data.get("endpoint") or self.key.partition("/")[0]

    @property
    def deployment(self) -> str:
        return self.data.get("deployment") or self.key.partition("/")[2]

    @property
    def total_load(self) -> Optional[float]:
        return self.data.get("total_load")

    @property
    def our_load(self) -> Optional[float]:
        return self.data.get("our_load")

    @property
    def foreign_load(self) -> float:
        return self.data.get("foreign_load") or 0.0

    @property
    def current_rpm(self) -> float:
        return self.data.get("current_rpm") or 0.0

    @property
    def capacity_rpm(self) -> Optional[float]:
        return self.data.get("capacity_rpm")

    @property
    def other_rpm(self) -> float:
        return self.data.get("other_rpm") or 0.0

    @property
    def rpm_load(self) -> Optional[float]:
        if not self.capacity_rpm:
            return None
        return (self.current_rpm + self.other_rpm) / self.capacity_rpm

    @property
    def rpm_other_load(self) -> float:
        if not self.capacity_rpm:
            return 0.0
        return self.other_rpm / self.capacity_rpm

    @property
    def rpm_load_by_face(self) -> Optional[Dict[str, float]]:
        if not self.capacity_rpm:
            return None
        values = self.data.get("rpm_by_face") or {}
        return {name: (values.get(name) or 0.0) / self.capacity_rpm
                for name in values}

    @property
    def by_face(self) -> Optional[Dict[str, float]]:
        return self.data.get("our_load_by_face")

    @property
    def busy(self) -> bool:
        """Has anything been sent here inside the load window.

        Deliberately the ledger and not `our_load`: a route with no known
        ceiling has `our_load` None however much traffic it took, and calling
        that idle is exactly the mistake the unknown-ceiling rule exists to
        prevent.
        """
        return bool(self.data.get("sent_requests_in_window"))

    @property
    def sort_load(self) -> float:
        """Load for ordering. Unknown sorts below idle, not above."""
        value = self.data.get("total_load")
        return -1.0 if value is None else value

    @property
    def busy_load(self) -> float:
        """Current RPM for activity ordering."""
        return self.current_rpm

    @property
    def released(self) -> Optional[str]:
        """When Azure says this deployment's model version shipped.

        `model_version` off the probe — a real date from ARM rather than
        anything inferred from the name. The proxy does not route on it; it is
        carried for exactly this kind of reporting.
        """
        return self.data.get("model_version")

    @property
    def recency_key(self) -> tuple:
        """Newest first, then strongest, then by name so ties do not shuffle."""
        name = self.model or self.deployment
        return (release_key(self.released), tier_of(name), name)


class Group:
    """A source or a model: a title, a set of routes, and their totals."""

    def __init__(self, name: str, routes: List[RouteView], kind: str,
                 faces: Optional[List[str]] = None):
        self.name = name
        self.routes = routes
        self.kind = kind                # "source" | "model"
        self.faces = faces or []

    @property
    def priority(self) -> Optional[int]:
        values = [r.data.get("priority") for r in self.routes
                  if r.data.get("priority") is not None]
        return min(values) if values else None

    @property
    def active(self) -> int:
        return sum(1 for r in self.routes if r.busy)

    @property
    def capacity_tokens(self) -> float:
        return sum(r.data.get("capacity_tokens") or 0.0 for r in self.routes)

    @property
    def capacity_requests(self) -> float:
        return sum(r.data.get("capacity_requests") or 0.0 for r in self.routes)

    @property
    def capacity_rpm(self) -> float:
        return sum(r.capacity_rpm or 0.0 for r in self.routes)

    @property
    def sent_requests(self) -> int:
        return sum(r.data.get("sent_requests_in_window") or 0
                   for r in self.routes)

    @property
    def sent_tokens(self) -> float:
        return sum(r.data.get("sent_tokens_in_window") or 0.0
                   for r in self.routes)

    @property
    def peak_load(self) -> float:
        """The busiest route in the group. What decides the group's colour.

        The peak rather than the mean, because a group is in trouble when ANY
        of its routes is: averaging a saturated deployment against three idle
        ones produces a comfortable number about an uncomfortable situation.
        """
        return max([r.sort_load for r in self.routes] or [-1.0])

    @property
    def peak_busy(self) -> float:
        """The same peak, for ordering. See RouteView.busy_load."""
        return max([r.busy_load for r in self.routes] or [0.0])

    @property
    def peak_rpm_load(self) -> float:
        values = [r.rpm_load for r in self.routes if r.rpm_load is not None]
        return max(values or [-1.0])

    @property
    def released(self) -> Optional[str]:
        """The newest release date among this group's routes.

        The newest rather than the oldest: gpt-4o is served from two
        deployments on two different model versions, and what matters for
        placing it on screen is the freshest thing behind the name.
        """
        dates = [r.released for r in self.routes if r.released]
        return max(dates) if dates else None

    @property
    def recency_key(self) -> tuple:
        """How the group is ordered once activity has had its say.

        A source is not a model and has no release date, so it falls back to
        priority — which is the failover order, and the order an operator
        expects to read three otherwise-equal endpoints in.
        """
        if self.kind == "source":
            return (self.priority if self.priority is not None else 99,
                    self.name)
        return (release_key(self.released), tier_of(self.name), self.name)


SORTS = ("activity", "name", "capacity")

# How many distinct model versions stay visible when idle, counted WITHIN each
# family. The newest three text models — 5.6, 5.5, 5.4 as of 2026-08 — and the
# newest three image models, with everything behind them hidden unless it is
# actually carrying traffic.
#
# Per family, because the families number independently: gpt-image-2 is the
# newest image model there is, and ranking its `2` against gpt-5.6's `5.6` on
# one list buries the current image model under three generations of text. The
# image face is also the quietest one on the proxy — 2 RPM on gpt-image-2 —
# so an activity rule alone would keep it off screen almost always.
#
# A COUNT of versions rather than a fixed floor, because a floor rots: pinning
# it at 5.4 keeps 5.4 on screen forever, and every new release would need an
# edit here to take effect. Counting slides on its own — when 5.7 lands, 5.4
# drops off the bottom, and when gpt-image-3 lands it takes its own slot.
#
# And versions rather than release dates, even though the dates are exact and
# right there in the data. They are too closely spaced to cut on: gpt-5.3-codex
# is 2026-02-24 and gpt-5.4 is 2026-03-05, so separating them needs a window
# accurate to nine days, which the next release would invalidate. Dates order
# the list; version numbers decide what is old. Two questions, two signals.
KEEP_RECENT_VERSIONS = 3

# `gpt-<family>-<major>[.<minor>]`, family optional: gpt-5.6-sol is ("", 5, 6)
# and gpt-image-1.5 is ("image", 1, 5). The family word has to be letters only,
# so gpt-4o still parses as the plain family at version 4.
_VERSION = re.compile(r"^gpt-(?:([a-z]+)-)?(\d+)(?:\.(\d+))?")

# Strength within one release, weakest number = strongest model. Only ever a
# tiebreak: several models ship on the same day (gpt-5.6-luna/sol/terra all on
# 2026-07-09) and the date alone cannot order them.
_TIERS = (("-codex-max", 1), ("-max", 1), ("-pro", 0), ("-codex", 2),
          ("-mini", 4), ("-nano", 5))
_TIER_DEFAULT = 3               # the plain model, between codex and mini


def version_of(name: str) -> Optional[tuple]:
    """(family, major, minor) parsed out of a model name, or None for a family
    we cannot rank — the o-series, or anything that is not `gpt-…`.

    The family is part of the key so that versions are only ever compared
    against their own line: gpt-image-2 is the second image model, not an
    ancient text one.
    """
    match = _VERSION.match(name or "")
    if not match:
        return None
    return (match.group(1) or "", int(match.group(2)),
            int(match.group(3) or 0))


def tier_of(name: str) -> int:
    for suffix, rank in _TIERS:
        if (name or "").endswith(suffix):
            return rank
    return _TIER_DEFAULT


def release_key(released: Optional[str]) -> float:
    """Sort key for a release date, newest first.

    An unknown date sorts LAST, not first. A model the probe could not date is
    not thereby the newest one; treating it as such would put the least-known
    thing at the top of the screen.
    """
    if not released:
        return 0.0
    digits = "".join(c for c in released[:10] if c.isdigit())
    return -float(digits) if digits else 0.0


class Snapshot:
    """The regrouped view of one poll. Cheap enough to rebuild every redraw."""

    def __init__(self, raw: dict):
        self.error = raw.get("error")
        self.age = raw.get("age")
        self.health_error = raw.get("health_error", self.error)
        self.health_age = raw.get("health_age", self.age)
        self.gap = raw.get("gap")
        self.dropped = bool(self.gap)
        self.missed_events = raw.get("missed_events", 0)
        self.unknown_gaps = raw.get("unknown_gaps", 0)
        self.history_notice = raw.get("history_notice")
        self.health: dict = raw.get("health") or {}
        self.events: List[dict] = [_rpm_fields(e)
                                   for e in (raw.get("events") or [])]
        routes_doc: dict = _rpm_fields(raw.get("routes") or {})
        self.routing = self.health.get("routing") or {}
        heartbeat_age = self.routing.get("heartbeat_age_seconds")
        routing_fetch_age = raw.get("local_state_age") if self.health.get("local_status") else self.health_age
        self.heartbeat_age = (max(0, heartbeat_age + (routing_fetch_age or 0))
                              if heartbeat_age is not None else None)
        updated_at = routes_doc.get("updated_at")
        self.stats_age = (max(0, time.time() - updated_at)
                          if updated_at is not None else None)
        report_routing = routes_doc.get("routing") or {}
        if (updated_at is not None and report_routing.get("heartbeat") is not None
                and report_routing.get("heartbeat_age_seconds") is not None):
            # Use the server's relative age when attaching from a different clock.
            self.stats_age = max(0, report_routing["heartbeat_age_seconds"]
                                 + report_routing["heartbeat"] - updated_at
                                 + (self.age or 0))
        self.stats_stale = (bool(routes_doc.get("stats_stale"))
                            or bool(self.routing and not self.routing.get("ok"))
                            or bool(self.routing.get("telemetry_dropped"))
                            or (self.stats_age is not None and self.stats_age >= 3))

        self.balance = routes_doc.get("balance") or self.health.get("balance")
        self.load_window = routes_doc.get("load_window_seconds")
        self.rpm_window = routes_doc.get("rpm_window_seconds")
        self.affinity = (routes_doc.get("session_affinity")
                         or self.health.get("session_affinity") or {})
        self.faces = routes_doc.get("faces") or []
        self.ok = bool(routes_doc) or bool(self.health)

        table: dict = routes_doc.get("routes") or {}
        models: dict = routes_doc.get("models") or {}
        model_faces: dict = routes_doc.get("model_faces") or {}

        # Invert model -> [route] once. A deployment serves exactly one model,
        # so the inverse is a function; building it per board would be building
        # it twice.
        owner: Dict[str, tuple] = {}
        for model, entries in models.items():
            for entry in entries:
                owner[entry.get("route")] = (model, entry.get("share"))

        self._views: Dict[str, RouteView] = {}
        for key, data in table.items():
            model, share = owner.get(key, (None, None))
            self._views[key] = RouteView(key, model, share, data)

        by_source: Dict[str, List[RouteView]] = {}
        for view in self._views.values():
            by_source.setdefault(view.endpoint, []).append(view)
        self.sources = [Group(name, views, "source")
                        for name, views in by_source.items()]

        self.models = []
        for model, entries in models.items():
            views = [self._views[e["route"]] for e in entries
                     if e.get("route") in self._views]
            self.models.append(
                Group(model, views, "model", model_faces.get(model)))

        # The versions that stay on screen when nothing is using them. Computed
        # from what this proxy actually serves rather than from a list written
        # down somewhere, so a newly probed model needs no code change to show
        # up and push an old one off. Counted per family, so the image models
        # get their own three slots instead of competing with the text ones.
        present: Dict[str, set] = {}
        for group in self.models:
            version = version_of(group.name)
            if version:
                present.setdefault(version[0], set()).add(version)
        self._recent = set()
        for versions in present.values():
            self._recent.update(
                sorted(versions, reverse=True)[:KEEP_RECENT_VERSIONS])

    def model_sessions(self, model: str) -> dict:
        return ((self.affinity.get("sessions_per_model") or {}).get(model)
                or {"total": 0, "types": {}})

    def pinned(self, model: str, endpoint: Optional[str] = None) -> Optional[int]:
        """Unique families for a model, optionally restricted to one endpoint."""
        entry = (self.affinity.get("sessions_per_model") or {}).get(model) or {}
        if endpoint is None:
            return entry.get("total", 0 if self.affinity.get("model_tracking") else None)
        if "endpoints" in entry:
            return entry["endpoints"].get(endpoint, 0)
        return 0 if self.affinity.get("model_tracking") else None

    # -- what is worth showing --------------------------------------------
    def is_legacy(self, name: str) -> bool:
        """Is this an old model, by version rather than by date.

        Old means "old within its own family": the newest few gpt-<n> releases
        stay, and so do the newest few gpt-image-<n>, because a caller asking
        for an image model has no use at all for the text list.

        A family with no version at all — the o-series — counts as legacy.
        That is what was asked for, and it is the only honest answer available:
        nothing in the name says where `o3` sits relative to `gpt-5.4`. It also
        means an unfamiliar family would be hidden while idle, which is why the
        count of what was hidden is always on screen and `a` always brings it
        back. A dashboard may leave things out; it may not do so quietly.
        """
        version = version_of(name)
        if version is None:
            return True
        return version not in self._recent

    def visible_groups(self, groups: List[Group],
                       show_all: bool = False) -> List[Group]:
        """Drop old models that nobody is using. Traffic always wins.

        `busy` and not `our_load`, deliberately: a route with no measured
        ceiling reports no load however much went through it, and hiding a
        model that is being hammered because its ceiling is unknown would be
        the worst possible time to hide it.
        """
        if show_all:
            return list(groups)
        return [g for g in groups
                if g.kind != "model" or g.active or not self.is_legacy(g.name)]

    def visible_routes(self, routes: List[RouteView],
                       show_all: bool = False) -> List[RouteView]:
        """The same rule applied to the rows inside a source card."""
        if show_all:
            return list(routes)
        return [r for r in routes
                if r.busy or not self.is_legacy(r.model or r.deployment)]

    def hidden_models(self) -> int:
        """How many models the rule is currently holding back. For the footer."""
        return sum(1 for g in self.models
                   if not g.active and self.is_legacy(g.name))

    def sorted_groups(self, groups: List[Group], mode: str) -> List[Group]:
        """Order for display. `activity` puts what is moving at the top.

        Alphabetical is offered too, and matters more than it sounds: under
        `activity` a card moves as traffic shifts, which is the right default
        for watching and the wrong one for finding a name you already know.
        """
        if mode == "name":
            return sorted(groups, key=lambda g: g.name)
        if mode == "capacity":
            return sorted(groups,
                          key=lambda g: (-g.capacity_rpm, g.name))
        # Activity first — this is a dashboard, and the thing that is moving is
        # the thing to look at. `recency_key` settles everything below that:
        # newest release, then strongest variant, then name. Which matters more
        # than it sounds, because on a quiet proxy every model ties on activity
        # and the whole board is decided here.
        return sorted(groups,
                      key=lambda g: (-g.active, -g.peak_busy) + g.recency_key)

    @staticmethod
    def sorted_routes(routes: List[RouteView]) -> List[RouteView]:
        """Within a card: busiest first, then newest — the same rule as above,
        so a model does not sit high on one board and low on the other."""
        return sorted(routes, key=lambda r: (-r.busy_load,) + r.recency_key)
