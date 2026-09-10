"""Replay observations, calculate targets, and atomically publish a generation."""

import collections
import copy
import logging
import os
import time
import uuid

from proxy.bridge import SNAPSHOT_FIELDS, SNAPSHOT_VERSION, decode_snapshot
from proxy.config import BALANCE_ALIASES, Config, Route, TABLES
from proxy.events import event_level, normalized_event
from proxy.state import SCHEMA_VERSION, Store
from proxy.retention import CLEANUP_INTERVAL, RETENTION_SECONDS, prune_checkpoint, recent
from .quota import DEFAULT_RPM, QuotaTracker, RouteState


# These fields also describe deployments in v1 telemetry and checkpoints.
ROUTE_FIELDS = (
    "endpoint", "url", "api_version", "deployment", "limit_param", "priority",
    "responses_path", "model_version", "capacity_requests", "capacity_tokens",
    "image_edits", "scope",
)


def route_record(route):
    return {key: getattr(route, key) for key in ROUTE_FIELDS}


def route_from_record(record):
    return Route(**{key: record[key] for key in ROUTE_FIELDS if key in record})


def target_record(route):
    """Resolve URL conventions here; serving executes the published addresses."""
    targets = {
        "/v1/chat/completions": route.chat_target(),
        "/v1/images/generations": route.image_target("generations"),
        "/v1/images/edits": route.image_target("edits"),
    }
    if route.responses_path:
        targets["/v1/responses"] = route.responses_target()
    return dict(endpoint=route.endpoint, deployment=route.deployment, scope=route.scope,
                targets=targets, routing_data=route_record(route))


class Engine:
    def __init__(self, root, config=None):
        self.root = root
        self.config = config or Config(root=root)
        cfg = self.config
        log = logging.getLogger(__name__)
        if cfg.balance_configured != cfg.balance:
            level = logging.INFO if cfg.balance_configured in BALANCE_ALIASES else logging.WARNING
            log.log(level, "routing.balance=%r resolved to %s", cfg.balance_configured, cfg.balance)
        log.info("routing balance=%s spill_threshold=%s rpm_window=%ss cold-start RPM=%s",
                 cfg.balance, cfg.spill_threshold, cfg.rpm_window, DEFAULT_RPM)
        self.store = Store(root)
        self.instance = uuid.uuid4().hex
        self.now = time.time()
        self.cursor = 0
        self.pending = {}
        self.producers = {}
        self.catalog = {}
        self.retired_routes = set()
        self.route_refresh = dict(enabled=self.config.route_refresh_enabled)
        self.sessions = {}
        self.events = collections.deque(maxlen=2000)
        self.event_seq = 0
        self.stream_id = uuid.uuid4().hex
        self.retention_through = 0
        self.quota = QuotaTracker(self.config, clock=lambda: self.now,
                                 emit=self.event, persist_capacity=False)
        try:
            snapshot = self.store.get("snapshot")
            self.revision = snapshot["revision"] if snapshot else 0
            saved = self.store.get("checkpoint")
            if saved:
                self.restore(saved)
            active = {str(route) for name in TABLES
                      for routes in getattr(self.config, name).values() for route in routes}
            self.retired_routes = (self.retired_routes | (set(self.catalog) - active)) - active
            self.catalog = {key: value for key, value in self.catalog.items() if key in active}
        except Exception:
            self.store.close()
            raise
        self.mirrored_capacity = None
        self.last_publication = None
        self.published_at = 0.0

    def event(self, kind, level, message, *args, **fields):
        level = event_level(kind, level, fields)
        self.event_seq += 1
        route = fields.get("route")
        if route is not None:
            fields["route"] = str(route)
            endpoint, _, deployment = str(route).partition("/")
            fields.setdefault("endpoint", endpoint)
            fields.setdefault("deployment", deployment)
        self.events.append(dict(fields, seq=self.event_seq, at=self.now,
                                kind=kind, level=level,
                                message=message % args if args else message))

    def restore(self, saved):
        if saved.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("incompatible routing checkpoint")
        prune_checkpoint(saved, time.time() - RETENTION_SECONDS + CLEANUP_INTERVAL)
        self.cursor = saved["cursor"]
        self.sessions = saved["sessions"]
        self.events.extend(normalized_event(event) for event in saved["events"])
        self.event_seq = saved["event_seq"]
        self.stream_id = saved.get("stream_id", self.stream_id)
        self.retention_through = saved.get("retention_through", 0)
        self.catalog = saved.get("catalog", {})
        self.retired_routes = set(saved.get("retired_routes", []))
        self.quota._saved_capacity = saved["capacity"]
        entries = {}
        for key, record in saved["states"].items():
            state = RouteState(key)
            for name, value in record.items():
                setattr(state, name, collections.deque(value) if name == "sent" else value)
            # Explicit database values, including an initialized False, take precedence.
            if "foreign_seen" not in record:
                state.foreign_seen = state.other_rpm > 0.0
            self.quota.states[key] = state
            entries.update((item[4], item) for item in state.sent if len(item) > 4)
        self.pending = saved["pending"]
        self.producers = saved.get("producers", {})
        for key, record in self.pending.items():
            record["entry"] = entries.get(key, record["entry"])

    def checkpoint(self):
        return dict(schema_version=SCHEMA_VERSION, cursor=self.cursor,
                    retention_epoch=int(self.now // CLEANUP_INTERVAL),
                    capacity=dict(self.quota._saved_capacity), pending=self.pending,
                    catalog=self.catalog, sessions=self.sessions, producers=self.producers,
                    retired_routes=sorted(self.retired_routes),
                    events=list(self.events), event_seq=self.event_seq,
                    stream_id=self.stream_id, retention_through=self.retention_through,
                    states={key: {name: list(value) if isinstance(value, collections.deque)
                                  else value for name in RouteState.__slots__
                                  for value in [getattr(state, name)]}
                            for key, state in self.quota.states.items()})

    def reload_routes(self, discovery):
        """Validate and swap discovery only, retaining in-flight accounting."""
        if discovery["sources"].get("_generated_at") != discovery["models"].get("_generated_at"):
            raise ValueError("discovery files belong to different generations")
        loaded = Config(root=self.root, discovery=discovery)
        candidate = copy.copy(self.config)
        for name in (*TABLES, "endpoints", "scopes", "endpoint_identities",
                     "image_deployments", "generated_at", "discovery"):
            setattr(candidate, name, getattr(loaded, name))
        tables = {name: {model: [target_record(route) for route in routes]
                         for model, routes in getattr(candidate, name).items()} for name in TABLES}
        fields = {name: sorted(value) if isinstance(value, set) else value
                  for name in SNAPSHOT_FIELDS for value in [getattr(candidate, name)]}
        fields["endpoint_identities"] = candidate.endpoint_identities
        decode_snapshot(dict(schema_version=SNAPSHOT_VERSION, revision=self.revision + 1,
                             config=fields, tables=tables, report={}, events={},
                             routing=dict(heartbeat=self.now, processed_seq=self.cursor,
                                          pid=os.getpid(), ready=True)), candidate)
        def keys(config):
            return {str(route) for name in TABLES
                    for routes in getattr(config, name).values() for route in routes}
        old, new = keys(self.config), keys(candidate)
        from .discovery import persist_discovery
        persist_discovery(self.root, discovery)
        self.retired_routes = (self.retired_routes | (old - new)) - new
        self.catalog = {key: value for key, value in self.catalog.items() if key not in self.retired_routes}
        self.config = candidate
        self.quota.cfg = candidate
        self.last_publication = None
        self.event("route_refresh", "info", "routes refreshed: %s added, %s removed, %s total",
                   len(new - old), len(old - new), len(new),
                   added=len(new - old), removed=len(old - new), total=len(new))
        logging.getLogger(__name__).info("routes refreshed: %s added, %s removed, %s total",
                                         len(new - old), len(old - new), len(new))

    def consume(self, event):
        self.now = event["at"]
        kind = event["kind"]
        producer = event.get("producer", "legacy")
        if event.get("producer_pid"):
            self.producers[producer] = dict(pid=event["producer_pid"], at=self.now)
        if kind == "producer_heartbeat":
            return
        if kind == "event":
            self.event(event["event_kind"], event["level"], event["message"],
                       **event.get("fields", {}))
            return
        if kind == "sessions":
            self.sessions = event["summary"]
            return
        if kind in ("producer_started", "producer_stopped"):
            # A single serving owner emits lifecycle records after all its
            # final observations. An abrupt serving exit leaves unknown
            # completions; a new owner retires those entries without claiming
            # upstream success or timeout.
            self.pending = {key: value for key, value in self.pending.items()
                            if value.get("producer", "legacy") != producer}
            if kind == "producer_stopped":
                self.producers.pop(producer, None)
            return
        attempt = event.get("attempt")
        if kind == "finish":
            self.pending.pop(attempt, None)
            return
        route = route_from_record(event["route"])
        record = self.pending.get(attempt)
        entry = record["entry"] if record else None
        if kind == "dispatch":
            if attempt in self.pending:
                return
            self.quota.note_attempt(route)
            entry = self.quota.charge(
                route, self.quota.estimate_tokens(route, event["request_bytes"]), event["face"])
            entry.append(attempt)
            self.pending[attempt] = dict(entry=entry, route=event["route"], producer=producer)
            self.catalog[str(route)] = dict(route=event["route"], model=event.get("model", ""))
        elif kind == "observed":
            self.quota.observed(route, event["status"], event["headers"])
        elif kind == "demote":
            self.quota.demote(route, event["reason"], event.get("retry_after"),
                              entry[3] if entry else None)
        elif kind == "timeout":
            self.quota.note_timeout(route, entry[3] if entry else None)
        elif kind == "usage":
            self.quota.settle(route, entry, event["request_bytes"], event["total_tokens"])
        elif kind == "success" and record and not record.get("success"):
            self.quota.note_success(route, entry)
            record["success"] = True

    def weighted_targets(self, routes):
        ordered = self.quota.order(routes)
        return [(route, weight, priority) for route, (priority, weight)
                in zip(ordered, self.quota.selection_parameters(ordered))]

    def snapshot(self, ready=True):
        for producer, state in list(self.producers.items()):
            if self.now - state["at"] < 3:
                continue
            try:
                with open("/proc/{}/stat".format(state["pid"])) as file:
                    dead = file.read().rpartition(")")[2].split()[0] == "Z"
            except FileNotFoundError:
                dead = True
            if dead:
                self.producers.pop(producer, None)
                self.pending = {key: value for key, value in self.pending.items()
                                if value.get("producer") != producer}
        cutoff = self.now - RETENTION_SECONDS + CLEANUP_INTERVAL
        self.retention_through = max([self.retention_through] +
                                    [e["seq"] for e in self.events if e["at"] <= cutoff])
        self.events = collections.deque(recent(self.events, cutoff), maxlen=2000)
        self.pending = {key: value for key, value in self.pending.items()
                        if value.get("entry") and value["entry"][0] > cutoff}
        for state in self.quota.states.values():
            state.sent = collections.deque(entry for entry in state.sent if entry[0] > cutoff)
        cfg = self.config
        tables = {name: {model: [dict(target_record(route), selection_weight=weight,
                                    selection_priority=priority)
                                for route, weight, priority in self.weighted_targets(routes)]
                         for model, routes in getattr(cfg, name).items()} for name in TABLES}
        merged = {}
        for name in TABLES:
            for model, routes in getattr(cfg, name).items():
                current = merged.setdefault(model, [])
                seen = {str(route) for route in current}
                current.extend(route for route in routes if str(route) not in seen)
        for item in self.catalog.values():
            model = item["model"]
            route = route_from_record(item["route"])
            if model and str(route) not in self.retired_routes and str(route) not in {str(r) for r in merged.get(model, [])}:
                merged.setdefault(model, []).append(route)
        report = self.quota.report(merged)
        report.update(session_affinity=self.sessions,
                      route_refresh=self.route_refresh,
                      spill_threshold=cfg.spill_threshold, probed_at=cfg.generated_at,
                      model_faces={model: [face for face, name in zip(
                          ("chat", "responses", "image", "image_edits"), TABLES)
                          if model in getattr(cfg, name)] for model in merged},
                      endpoints=[dict(name=name, chat=chat, responses=responses, image=image)
                                 for name, chat, responses, image in cfg.endpoints],
                      image_deployments=cfg.image_deployments,
                      updated_at=self.now)
        config = {name: sorted(value) if isinstance(value, set) else value
                  for name in SNAPSHOT_FIELDS for value in [getattr(cfg, name)]}
        config["endpoint_identities"] = getattr(cfg, "endpoint_identities", {})
        return dict(schema_version=SNAPSHOT_VERSION, revision=self.revision + 1,
                    config=config, tables=tables, report=report,
                    routing=dict(pid=os.getpid(), instance_id=self.instance,
                                 heartbeat=self.now, processed_seq=self.cursor, ready=ready,
                                 route_refresh=self.route_refresh),
                    events=dict(events=list(self.events), next=self.event_seq,
                                stream_id=self.stream_id, retention_through=self.retention_through,
                                counts=dict(collections.Counter(e["kind"] for e in self.events))))

    def step(self, ready=True):
        # Catch up to a fixed waterline before publishing; concurrent arrivals
        # belong to the next generation and cannot starve publication.
        waterline = self.store.highwater()
        cutoff = time.time() - RETENTION_SECONDS + CLEANUP_INTERVAL
        if (ready and self.last_publication is not None and self.cursor == waterline
                and time.monotonic() - self.published_at < 1.0):
            return self.last_publication
        replaying = waterline - self.cursor >= 10000
        progress_at = time.monotonic()
        log = logging.getLogger(__name__)
        if replaying:
            log.info("routing replay starting: seq %s -> %s", self.cursor, waterline)
        while self.cursor < waterline:
            batch = self.store.read_events(self.cursor, through=waterline)
            if not batch:
                retention = self.store.get("retention") or {}
                if retention.get("through", 0) >= waterline:
                    self.cursor = waterline
                    break
                raise RuntimeError("telemetry gap before routing waterline")
            for seq, event in batch:
                if event["at"] > cutoff:
                    self.consume(event)
                self.cursor = seq
            if replaying and time.monotonic() - progress_at >= 5:
                log.info("routing replay progress: seq %s / %s", self.cursor, waterline)
                progress_at = time.monotonic()
        if replaying:
            log.info("routing replay complete: seq %s", self.cursor)
        self.now = time.time()
        snapshot = self.snapshot(ready)
        decode_snapshot(snapshot, self.config)
        self.store.publish(self.checkpoint(), snapshot)
        self.revision = snapshot["revision"]
        self.last_publication = snapshot
        self.published_at = time.monotonic()
        if self.mirrored_capacity != self.quota._saved_capacity:
            self.quota.persist_capacity = True
            try:
                self.quota._save_capacity()
            finally:
                self.quota.persist_capacity = False
            self.mirrored_capacity = dict(self.quota._saved_capacity)
        return snapshot

    def close(self):
        self.store.close()
