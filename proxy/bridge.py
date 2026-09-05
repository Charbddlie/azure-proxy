"""In-memory mapping cache and raw observations for the serving process."""

import asyncio
import collections
import contextvars
import copy
import itertools
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass

from .config import Route, TABLES
from .state import SCHEMA_VERSION, Store

log = logging.getLogger("azure-proxy")
INTERVAL = max(0.001, float(os.environ.get("AZURE_PROXY_SYNC_INTERVAL", "0.1")))
MAX_PENDING = 100000
SNAPSHOT_FIELDS = (
    "balance", "spill_threshold", "load_window", "rpm_window", "generated_at",
    "endpoints", "image_deployments", "scopes",
)


def route_record(route):
    return {key: getattr(route, key) for key in Route.__slots__}


class ConfigView:
    """One immutable config generation per ASGI request, including its stream."""
    def __init__(self, initial):
        self.current = initial
        self.context = contextvars.ContextVar("serving_config", default=None)

    def __getattr__(self, name):
        return getattr(self.context.get() or self.current, name)


class SnapshotMiddleware:
    def __init__(self, app, config):
        self.app, self.config = app, config

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        token = self.config.context.set(self.config.current)
        try:
            await self.app(scope, receive, send)
        finally:
            self.config.context.reset(token)


def decode_snapshot(snapshot, base):
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("incompatible routing snapshot")
    if not isinstance(snapshot.get("revision"), int) or snapshot["revision"] < 1:
        raise ValueError("invalid routing revision")
    candidate = copy.copy(base)
    for key in SNAPSHOT_FIELDS:
        setattr(candidate, key, snapshot["config"][key])
    if candidate.balance not in ("strict_priority", "priority_threshold", "capacity"):
        raise ValueError("invalid routing balance mode")
    for name in ("load_window", "rpm_window", "spill_threshold"):
        value = getattr(candidate, name)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError("invalid routing window/threshold")
    if not isinstance(candidate.scopes, list) or not all(
            isinstance(scope, str) and scope for scope in candidate.scopes):
        raise ValueError("invalid credential scopes")
    if not isinstance(candidate.image_deployments, dict) or not all(
            isinstance(key, str) and isinstance(value, str) and value
            for key, value in candidate.image_deployments.items()):
        raise ValueError("invalid image deployment map")
    candidate.scopes = set(candidate.scopes)
    for table in TABLES:
        models = snapshot["tables"][table]
        if not isinstance(models, dict):
            raise ValueError("invalid model table")
        built = {}
        for model, records in models.items():
            if not isinstance(model, str) or not isinstance(records, list) or not records:
                raise ValueError("invalid model mapping")
            routes = []
            for record in records:
                route = Route(**record)
                if not all(isinstance(v, str) and v for v in
                           (route.endpoint, route.url, route.deployment)):
                    raise ValueError("incomplete route descriptor")
                if not route.url.startswith(("http://", "https://")):
                    raise ValueError("invalid upstream URL")
                if route.scope is not None and route.scope not in candidate.scopes:
                    raise ValueError("route scope missing from snapshot scopes")
                if table == "responses_routes" and not route.responses_path:
                    raise ValueError("missing responses path")
                routes.append(route)
            if len({str(route) for route in routes}) != len(routes):
                raise ValueError("duplicate deployment in model mapping")
            built[model] = routes
        setattr(candidate, table, built)
    if not any(getattr(candidate, name) for name in TABLES):
        raise ValueError("routing snapshot has no usable routes")
    if not isinstance(snapshot.get("report"), dict) or not isinstance(snapshot.get("events"), dict):
        raise ValueError("missing routing reports")
    routing = snapshot["routing"]
    if (not isinstance(routing["heartbeat"], (int, float))
            or not math.isfinite(routing["heartbeat"])
            or not isinstance(routing.get("processed_seq"), int)
            or not isinstance(routing.get("pid"), int)
            or not isinstance(routing.get("ready"), bool)):
        raise ValueError("invalid routing heartbeat")
    return candidate


class ServingBridge:
    def __init__(self, root, config):
        self.root, self.config = root, config
        self.producer = uuid.uuid4().hex
        self.pending = collections.deque()
        self.local_seq = 0
        self.committed_seq = 0
        self.last_event_seq = 0
        self.dropped = 0
        self.error = None
        self.snapshot = None
        self.store = None
        self.task = None
        self.sessions = None
        self.on_config = None
        self.stopping = False

    def record(self, kind, **data):
        self.local_seq += 1
        if len(self.pending) >= MAX_PENDING:
            self.dropped += 1
            if self.dropped == 1:
                log.error("telemetry queue full; serving continues with a statistics gap")
            return
        self.pending.append(dict(data, kind=kind, at=time.time(), local_seq=self.local_seq))

    async def start(self):
        self.store = await asyncio.to_thread(Store, self.root)
        try:
            snapshot = await asyncio.to_thread(self.store.get, "snapshot")
            if snapshot is None:
                raise RuntimeError("no routing snapshot; start routing first")
            self.install(snapshot)
        except Exception:
            await asyncio.to_thread(self.store.close)
            self.store = None
            raise
        self.record("producer_started", producer=self.producer)
        self.task = asyncio.create_task(self.run())

    def install(self, snapshot):
        if self.snapshot and snapshot["revision"] <= self.snapshot["revision"]:
            return
        candidate = decode_snapshot(snapshot, self.config.current)
        if self.on_config:
            self.on_config(candidate)
        self.config.current = candidate
        self.snapshot = snapshot

    async def flush(self):
        batch = list(itertools.islice(self.pending, 2000))
        if batch:
            local, global_seq = await asyncio.to_thread(self.store.append, self.producer, batch)
            self.committed_seq, self.last_event_seq = local, global_seq
            while self.pending and self.pending[0]["local_seq"] <= local:
                self.pending.popleft()

    async def run(self):
        last_sessions = None
        while not self.stopping:
            try:
                if self.sessions:
                    summary = self.sessions()
                    if summary != last_sessions:
                        self.record("sessions", summary=summary)
                        last_sessions = summary
                await self.flush()
                snapshot = await asyncio.to_thread(self.store.get, "snapshot")
                if snapshot:
                    self.install(snapshot)
                self.error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = str(exc)
                if error != self.error:
                    log.error("routing exchange unavailable: %s; using cached mapping", error)
                self.error = error
            await asyncio.sleep(INTERVAL)

    async def stop(self):
        if self.task:
            self.stopping = True
            await self.task
        if self.store:
            self.record("producer_stopped", producer=self.producer)
            try:
                while self.pending:
                    await self.flush()
            except Exception:
                log.exception("could not flush final telemetry; statistics gap")
            await asyncio.to_thread(self.store.close)

    def status(self):
        routing = dict((self.snapshot or {}).get("routing", {}))
        age = max(0, time.time() - routing.get("heartbeat", 0))
        routing.update(ok=bool(routing.get("ready")) and age < 3 and not self.error,
                       heartbeat_age_seconds=round(age, 3),
                       mapping_version=(self.snapshot or {}).get("revision"),
                       exchange_error=self.error,
                       telemetry_pending=len(self.pending),
                       telemetry_dropped=self.dropped,
                       emitted_seq=self.local_seq, committed_seq=self.committed_seq,
                       last_event_seq=self.last_event_seq,
                       backlog=max(0, self.last_event_seq - routing.get("processed_seq", 0)))
        return routing


@dataclass
class Attempt:
    id: str
    request_bytes: float
    streaming: bool = False
    finished: bool = False
    success: bool = False
    usage: object = None


class Telemetry:
    """Only captures observations; all quota arithmetic lives in routing."""
    def __init__(self, bridge):
        self.bridge = bridge

    def charge(self, route, request_bytes, face=0, model=""):
        entry = Attempt(uuid.uuid4().hex, request_bytes)
        self.bridge.record("dispatch", attempt=entry.id, route=route_record(route),
                           request_bytes=request_bytes, face=face, model=model)
        return entry

    def observed(self, route, status, headers, entry=None):
        self.bridge.record("observed", route=route_record(route),
                           attempt=entry.id if entry else None, status=status,
                           headers={k: v for k, v in headers.items()
                                    if k.lower() in {
                                        "retry-after", "x-ratelimit-limit-requests",
                                        "x-ratelimit-limit-tokens", "x-ratelimit-remaining-requests",
                                        "x-ratelimit-remaining-tokens", "x-ratelimit-reset-requests",
                                        "x-ratelimit-reset-tokens", "x-ratelimit-renewalperiod-requests",
                                        "x-ratelimit-renewalperiod-tokens"}})

    def demote(self, route, reason, retry_after=None, entry=None):
        self.bridge.record("demote", route=route_record(route), reason=reason,
                           retry_after=retry_after, attempt=entry.id if entry else None)

    def failed(self, route, reason, entry=None):
        self.demote(route, reason, entry=entry)

    def note_timeout(self, route, entry=None):
        self.bridge.record("timeout", route=route_record(route),
                           attempt=entry.id if entry else None)

    def settle(self, route, entry, request_bytes, total_tokens):
        if entry and total_tokens and entry.usage != total_tokens:
            entry.usage = total_tokens
            self.bridge.record("usage", route=route_record(route), attempt=entry.id,
                               request_bytes=request_bytes, total_tokens=total_tokens)

    def note_success(self, route, entry):
        if entry and not entry.success:
            entry.success = True
            self.bridge.record("success", route=route_record(route), attempt=entry.id)

    def finish(self, entry):
        if entry and not entry.finished:
            entry.finished = True
            self.bridge.record("finish", attempt=entry.id)
