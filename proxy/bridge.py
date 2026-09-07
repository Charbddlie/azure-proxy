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
from typing import Optional
from urllib.parse import urlsplit

from .config import TABLES
from .state import Store

log = logging.getLogger("azure-proxy")
INTERVAL = max(0.001, float(os.environ.get("AZURE_PROXY_SYNC_INTERVAL", "0.1")))
MAX_PENDING = 100000
SNAPSHOT_VERSION = 2
SNAPSHOT_FIELDS = ("image_deployments", "scopes")
TABLE_PATHS = dict(zip(TABLES, (
    "/v1/chat/completions", "/v1/responses",
    "/v1/images/generations", "/v1/images/edits")))


@dataclass(frozen=True)
class Target:
    """Installed deployment; nested data stays read-only for its lifetime."""
    endpoint: str
    deployment: str
    scope: Optional[str]
    targets: dict
    routing_data: dict
    selection_weight: Optional[float] = None

    def __repr__(self):
        return "{}/{}".format(self.endpoint, self.deployment)


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
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != SNAPSHOT_VERSION:
        raise ValueError("incompatible routing snapshot; start matching routing first")
    if type(snapshot.get("revision")) is not int or snapshot["revision"] < 1:
        raise ValueError("invalid routing revision")
    candidate = copy.copy(base)
    candidate.endpoint_identities = snapshot["config"].get("endpoint_identities", {})
    if not isinstance(candidate.endpoint_identities, dict) or not all(
            isinstance(k, str) and isinstance(v, str) and len(v) == 64
            for k, v in candidate.endpoint_identities.items()):
        raise ValueError("invalid endpoint identity registry")
    for key in SNAPSHOT_FIELDS:
        setattr(candidate, key, snapshot["config"][key])
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
                route = Target(endpoint=record["endpoint"], deployment=record["deployment"],
                               scope=record.get("scope"), targets=record["targets"],
                               routing_data=record["routing_data"],
                               selection_weight=record.get("selection_weight"))
                if not all(isinstance(v, str) and v for v in
                           (route.endpoint, route.deployment)):
                    raise ValueError("incomplete route descriptor")
                if not isinstance(route.targets, dict) or TABLE_PATHS[table] not in route.targets:
                    raise ValueError("missing upstream target")
                for path, url in route.targets.items():
                    if not isinstance(path, str) or not isinstance(url, str):
                        raise ValueError("invalid upstream URL")
                    parsed = urlsplit(url)
                    if parsed.scheme not in ("http", "https") or not parsed.hostname:
                        raise ValueError("invalid upstream URL")
                if route.scope is not None and (
                        not isinstance(route.scope, str) or route.scope not in candidate.scopes):
                    raise ValueError("route scope missing from snapshot scopes")
                if not isinstance(route.routing_data, dict):
                    raise ValueError("invalid routing metadata")
                if route.selection_weight is not None and (
                        not isinstance(route.selection_weight, (int, float))
                        or not math.isfinite(route.selection_weight) or route.selection_weight < 0):
                    raise ValueError("invalid deployment selection weight")
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
    candidate.routing_report = snapshot["report"]
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
        self.pending.append(dict(data, kind=kind, at=time.time(), local_seq=self.local_seq,
                                 producer=self.producer, producer_pid=os.getpid()))

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
        heartbeat_at = 0
        while not self.stopping:
            try:
                if time.monotonic() - heartbeat_at >= 1:
                    self.record("producer_heartbeat")
                    heartbeat_at = time.monotonic()
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
        self.bridge.record("dispatch", attempt=entry.id, route=route.routing_data,
                           request_bytes=request_bytes, face=face, model=model)
        return entry

    def observed(self, route, status, headers, entry=None):
        self.bridge.record("observed", route=route.routing_data,
                           attempt=entry.id if entry else None, status=status,
                           headers={k.lower(): v for k, v in headers.items()
                                    if k.lower() == "retry-after"
                                    or k.lower().startswith("x-ratelimit-")})

    def demote(self, route, reason, retry_after=None, entry=None):
        self.bridge.record("demote", route=route.routing_data, reason=reason,
                           retry_after=retry_after, attempt=entry.id if entry else None)

    def failed(self, route, reason, entry=None):
        self.demote(route, reason, entry=entry)

    def note_timeout(self, route, entry=None):
        self.bridge.record("timeout", route=route.routing_data,
                           attempt=entry.id if entry else None)

    def settle(self, route, entry, request_bytes, total_tokens):
        if entry and total_tokens and entry.usage != total_tokens:
            entry.usage = total_tokens
            self.bridge.record("usage", route=route.routing_data, attempt=entry.id,
                               request_bytes=request_bytes, total_tokens=total_tokens)

    def note_success(self, route, entry):
        if entry and not entry.success:
            entry.success = True
            self.bridge.record("success", route=route.routing_data, attempt=entry.id)

    def finish(self, entry):
        if entry and not entry.finished:
            entry.finished = True
            self.bridge.record("finish", attempt=entry.id)
