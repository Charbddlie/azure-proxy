"""Durable family -> resource bindings. No request content is stored here."""

import asyncio
import collections
import hashlib
import json
import math
import os
import random
import sqlite3
import threading
import time
from urllib.parse import urlsplit, parse_qsl

from .bridge import Target
from .config import TABLES, endpoint_identity


class AffinityError(Exception):
    def __init__(self, code, message, status=409):
        super().__init__(message)
        self.code, self.status = code, status


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def resource(route, default_scope):
    return endpoint_identity(route.routing_data["url"], route.scope or default_scope)


def carries_encrypted(value):
    if isinstance(value, dict):
        return bool(value.get("encrypted_content")) or any(
            carries_encrypted(v) for v in value.values() if isinstance(v, (dict, list)))
    if isinstance(value, list):
        return any(carries_encrypted(v) for v in value)
    return False


def weighted_order(routes):
    """Sample the earliest published priority group, then its failover groups."""
    pool, chosen = list(routes), []
    while pool:
        weights = [r.selection_weight for r in pool]
        if all(w is None for w in weights):
            return chosen + pool   # compatibility with the previous publisher
        weights = [w if w is not None else 1.0 for w in weights]
        priority = min(r.selection_priority for r in pool)
        weights = [w if r.selection_priority == priority else 0.0
                   for r, w in zip(pool, weights)]
        if not sum(weights):
            index = next(i for i, r in enumerate(pool) if r.selection_priority == priority)
            chosen.append(pool.pop(index))
            continue
        index = random.choices(range(len(pool)), weights=weights)[0]
        chosen.append(pool.pop(index))
    return chosen


class AffinityStore:
    def __init__(self, root, ttl=172800, clock=time.time, active_window=300):
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("session affinity TTL must be positive and finite")
        if not math.isfinite(active_window) or active_window <= 0:
            raise ValueError("session affinity active window must be positive and finite")
        self.ttl, self.clock = ttl, clock
        self.active_window = active_window
        self.lock = threading.RLock()
        self.dirty = {}
        self.catalog_cache = collections.OrderedDict()
        self.error = None
        directory = os.path.join(root, "runtime")
        os.makedirs(directory, mode=0o700, exist_ok=True)
        path = os.path.join(directory, "affinity.sqlite3")
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.fchmod(fd, 0o600)
        os.close(fd)
        self.db = sqlite3.connect(path, timeout=1, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            self.db.close()
            raise ValueError("unsupported affinity database version")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS descriptors (
                id TEXT PRIMARY KEY, identity TEXT NOT NULL, catalog TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS bindings (
                family TEXT PRIMARY KEY, endpoint TEXT NOT NULL,
                descriptor TEXT NOT NULL REFERENCES descriptors(id),
                activity REAL NOT NULL, expires REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS bindings_expiry ON bindings(expires);
            CREATE INDEX IF NOT EXISTS bindings_activity ON bindings(activity);
            CREATE INDEX IF NOT EXISTS bindings_descriptor ON bindings(descriptor);
            CREATE TABLE IF NOT EXISTS binding_models (
                family TEXT NOT NULL REFERENCES bindings(family), model TEXT NOT NULL,
                PRIMARY KEY(family, model));
            PRAGMA user_version=1;
        """)
        self.last_flush = self.clock()

    def _get(self, family):
        row = self.db.execute(
            "SELECT b.endpoint,d.identity,d.catalog,b.expires,d.id FROM bindings b "
            "JOIN descriptors d ON d.id=b.descriptor WHERE b.family=?", (family,)).fetchone()
        if row and row[3] > self.clock():
            catalog = self.catalog_cache.get(row[4])
            if catalog is None:
                catalog = json.loads(row[2])
                self.catalog_cache[row[4]] = catalog
            self.catalog_cache.move_to_end(row[4])
            if len(self.catalog_cache) > 128:
                self.catalog_cache.popitem(last=False)
            return dict(endpoint=row[0], identity=row[1], catalog=catalog, expires=row[3])
        return None

    def get(self, family):
        with self.lock:
            self.flush_due()
            return self._get(family)

    def bind(self, family, endpoint, identity, catalog):
        """The unique family row is committed before returning the winner."""
        encoded = json.dumps(catalog, sort_keys=True, separators=(",", ":"), allow_nan=False)
        descriptor = digest(identity + "\0" + encoded)
        with self.lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            winner = self._get(family)
            if winner:
                return winner
            now = self.clock()
            self.db.execute("DELETE FROM binding_models WHERE family=?", (family,))
            self.db.execute("INSERT OR IGNORE INTO descriptors VALUES(?,?,?)",
                            (descriptor, identity, encoded))
            self.db.execute("INSERT OR REPLACE INTO bindings VALUES(?,?,?,?,?)",
                            (family, endpoint, descriptor, now, now + self.ttl))
        self.error = None
        return dict(endpoint=endpoint, identity=identity, catalog=catalog, expires=now + self.ttl, created=True)

    def inherit(self, family, parent):
        """Atomically give a fork its parent's endpoint and retained catalog."""
        with self.lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            source = self._get(parent)
            if not source:
                return None
            winner = self._get(family)
            if winner:
                if (winner["endpoint"], winner["identity"]) != (source["endpoint"], source["identity"]):
                    raise AffinityError("affinity_parent_conflict", "fork and parent have different endpoint bindings")
                return winner
            now = self.clock()
            self.db.execute("DELETE FROM binding_models WHERE family=?", (family,))
            self.db.execute("INSERT OR REPLACE INTO bindings "
                            "SELECT ?,endpoint,descriptor,?,? FROM bindings WHERE family=?",
                            (family, now, now + self.ttl, parent))
        self.error = None
        return dict(source, expires=now + self.ttl, created=True)

    def note_model(self, family, model):
        """Remember each model used by an unexpired endpoint-bound family once."""
        with self.lock:
            if self.db.execute("SELECT 1 FROM binding_models WHERE family=? AND model=?",
                               (family, model)).fetchone():
                return
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO binding_models "
                                "SELECT family,? FROM bindings WHERE family=? AND expires>?",
                                (model, family, self.clock()))

    def touch(self, family, expires):
        with self.lock:
            now = self.clock()
            self.dirty[family] = now
            if expires - now <= min(2.0, self.ttl / 2):
                self.flush()
            else:
                self.flush_due()

    def flush_due(self):
        if self.clock() - self.last_flush >= min(1.0, self.ttl / 4, self.active_window / 4):
            self.flush()

    def flush(self):
        with self.lock:
            if self.dirty:
                with self.db:
                    self.db.executemany(
                        "UPDATE bindings SET activity=MAX(activity,?), expires=MAX(expires,?) "
                        "WHERE family=? AND expires>?",
                        [(at, at + self.ttl, family, at) for family, at in self.dirty.items()])
                self.dirty.clear()
                self.error = None
            self.last_flush = self.clock()

    def cleanup(self):
        with self.lock, self.db:
            self.db.execute("DELETE FROM binding_models WHERE family IN "
                            "(SELECT family FROM bindings WHERE expires<=? LIMIT 500)",
                            (self.clock(),))
            self.db.execute("DELETE FROM bindings WHERE family IN "
                            "(SELECT family FROM bindings WHERE expires<=? LIMIT 500)",
                            (self.clock(),))
            self.db.execute("DELETE FROM descriptors WHERE id IN (SELECT id FROM descriptors d "
                            "WHERE NOT EXISTS (SELECT 1 FROM bindings b WHERE b.descriptor=d.id) LIMIT 50)")

    def scopes(self):
        with self.lock:
            catalogs = [json.loads(row[0]) for row in self.db.execute(
                "SELECT catalog FROM descriptors d WHERE EXISTS "
                "(SELECT 1 FROM bindings b WHERE b.descriptor=d.id AND b.expires>?)", (self.clock(),))]
            return {r["scope"] for catalog in catalogs for models in catalog.get("tables", {}).values()
                    for routes in models.values() for r in routes if r.get("scope")}

    def report(self):
        with self.lock:
            self.flush_due()
            now = self.clock()
            cutoff = now - self.active_window
            counts = dict(self.db.execute("SELECT endpoint,COUNT(*) FROM bindings "
                                         "WHERE expires>? AND activity>=? GROUP BY endpoint", (now, cutoff)))
            models = {}
            for model, endpoint, count in self.db.execute(
                    "SELECT m.model,b.endpoint,COUNT(*) FROM binding_models m "
                    "JOIN bindings b ON b.family=m.family WHERE b.expires>? AND b.activity>=? "
                    "GROUP BY m.model,b.endpoint", (now, cutoff)):
                entry = models.setdefault(model, dict(total=0, endpoints={}))
                entry["total"] += count
                entry["endpoints"][endpoint] = count
            unattributed = self.db.execute(
                "SELECT COUNT(*) FROM bindings b WHERE b.expires>? AND b.activity>=? AND NOT EXISTS "
                "(SELECT 1 FROM binding_models m WHERE m.family=b.family)", (now, cutoff)).fetchone()[0]
            retained = self.db.execute("SELECT COUNT(*) FROM bindings WHERE expires>?", (now,)).fetchone()[0]
            return dict(enabled=True, mode="endpoint", ttl_seconds=self.ttl,
                        active_window_seconds=self.active_window, retained_sessions=retained,
                        live_sessions=sum(counts.values()), sessions_per_endpoint=counts,
                        sessions_per_model=models, model_tracking=True,
                        unattributed_sessions=unattributed,
                        persistence=dict(ok=self.error is None, error=self.error,
                                         pending=len(self.dirty), last_flush=self.last_flush))

    def close(self):
        with self.lock:
            try:
                self.flush()
            finally:
                self.db.close()


class SessionAffinity:
    def __init__(self, config, root):
        self.cfg, self.root = config, root
        self.store = None
        self.active = collections.Counter()
        self.task = None
        self.emit = None
        self.last_report = dict(enabled=True, mode="endpoint", live_sessions=0,
                                persistence=dict(ok=False, error="not started"))

    def family(self, request, body):
        for spec in self.cfg.affinity_keys:
            where, _, name = spec.partition(":")
            value = request.headers.get(name) if where == "header" else body
            if where != "header":
                for part in name.split("."):
                    value = value.get(part) if isinstance(value, dict) else None
            if isinstance(value, str) and value.strip():
                # The carrier, model and thread are deliberately absent.
                return digest(value.strip())
        return None

    def sticky(self, body):
        return (carries_encrypted(body) or bool(body.get("previous_response_id"))
                or body.get("store") is True
                or "reasoning.encrypted_content" in (body.get("include") or []))

    @staticmethod
    def parent_family(request, body=None):
        """Read fork ancestry from Codex's header or its provider-forwarded body copy."""
        client_metadata = (body or {}).get("client_metadata")
        client_metadata = client_metadata if isinstance(client_metadata, dict) else {}
        raw = request.headers.get("x-codex-turn-metadata")
        raw = raw or client_metadata.get("x-codex-turn-metadata")
        if not isinstance(raw, str) or not raw or len(raw) > 16384:
            return None
        try:
            metadata = json.loads(raw)
        except (TypeError, ValueError):
            return None
        parent = metadata.get("forked_from_thread_id") if isinstance(metadata, dict) else None
        return digest(parent.strip()) if isinstance(parent, str) and parent.strip() else None

    async def start(self):
        self.store = await asyncio.to_thread(AffinityStore, self.root, self.cfg.affinity_ttl,
                                            active_window=getattr(self.cfg, "affinity_active_window", 300))
        self.last_report = await asyncio.to_thread(self.store.report)
        self.task = asyncio.create_task(self.run())

    def catalog(self, endpoint, identity):
        from dataclasses import asdict
        from .config import Route
        tables = {}
        for name in TABLES:
            tables[name] = {}
            for model, routes in getattr(self.cfg, name).items():
                local = [asdict(r) for r in routes if r.endpoint == endpoint
                         and resource(r, self.cfg.scope) == identity]
                for record in local:
                    record["routing_data"] = {key: value for key, value in record["routing_data"].items()
                                              if key in Route.__slots__}
                    for address in record["targets"].values():
                        url = urlsplit(address)
                        if (url.username or url.password or url.fragment
                                or any(key != "api-version" for key, _ in parse_qsl(url.query))):
                            raise ValueError("credential-bearing target cannot be persisted")
                if local:
                    tables[name][model] = local
        return dict(tables=tables, images={endpoint: self.cfg.image_deployments[endpoint]}
                    if endpoint in self.cfg.image_deployments else {})

    def resolve(self, family, body, table, parent=None):
        routes = list(getattr(self.cfg, table).get(body["model"], []))
        required = self.sticky(body)
        if not family:
            if required:
                raise AffinityError("session_id_required", "stateful request requires a session/family ID", 400)
            return routes, None
        binding = self.store.get(family)
        if not binding and parent and parent != family:
            binding = self.store.inherit(family, parent) or binding
        if not binding:
            if carries_encrypted(body) or body.get("previous_response_id"):
                raise AffinityError("affinity_missing", "state has no unexpired endpoint binding")
            if not required or not routes:
                return routes, None
            # Endpoint order comes from routing; deployment selection occurs below.
            route = routes[0]
            identity = resource(route, self.cfg.scope)
            binding = self.store.bind(family, route.endpoint, identity,
                                      self.catalog(route.endpoint, identity))
        self.store.touch(family, binding["expires"])
        endpoint, identity = binding["endpoint"], binding["identity"]
        published_identity = getattr(self.cfg, "endpoint_identities", {}).get(endpoint)
        if published_identity and published_identity != identity:
            raise AffinityError("endpoint_identity_conflict", "bound endpoint now names a different resource")
        for name in TABLES:
            for candidates in getattr(self.cfg, name).values():
                if any(r.endpoint == endpoint and resource(r, self.cfg.scope) != identity
                       for r in candidates):
                    raise AffinityError("endpoint_identity_conflict", "bound endpoint now names a different resource")
        local = [r for r in routes if r.endpoint == endpoint and resource(r, self.cfg.scope) == identity]
        if not local:
            local = [Target(**r) for r in binding["catalog"]["tables"].get(table, {}).get(body["model"], [])]
        if not local:
            raise AffinityError("bound_model_unavailable", "bound endpoint does not support the requested model", 404)
        return weighted_order(local), binding

    async def prepare(self, request, body, table):
        family = self.family(request, body)
        try:
            routes, binding = await asyncio.to_thread(self.resolve, family, body, table,
                                                     self.parent_family(request, body))
        except (sqlite3.Error, OSError) as exc:
            self.store.error = type(exc).__name__
            raise AffinityError("affinity_store_unavailable", "endpoint binding could not be persisted", 503) from exc
        except ValueError as exc:
            raise AffinityError("affinity_descriptor_invalid", "endpoint descriptor cannot be safely persisted", 503) from exc
        request.state.affinity_family = family if binding else None
        request.state.affinity_binding = binding
        if binding:
            try:
                await asyncio.to_thread(self.store.note_model, family, body["model"])
            except (sqlite3.Error, OSError) as exc:
                # Display statistics can catch up on a later request.
                self.store.error = type(exc).__name__
            self.active[family] += 1
            if binding.get("created") and self.emit:
                self.emit("pin", "info", "family bound to endpoint %s", binding["endpoint"],
                          endpoint=binding["endpoint"], binding_mode="endpoint")
        return routes

    def release(self, request):
        family = getattr(request.state, "affinity_family", None)
        if family and self.active[family]:
            self.active[family] -= 1
            if not self.active[family]:
                del self.active[family]

    async def run(self):
        cleanup_at = time.monotonic()
        while True:
            try:
                for family in list(self.active):
                    binding = await asyncio.to_thread(self.store.get, family)
                    if binding:
                        await asyncio.to_thread(self.store.touch, family, binding["expires"])
                await asyncio.to_thread(self.store.flush_due)
                if time.monotonic() >= cleanup_at:
                    await asyncio.to_thread(self.store.cleanup)
                    cleanup_at = time.monotonic() + 60
                self.last_report = await asyncio.to_thread(self.store.report)
            except (sqlite3.Error, OSError) as exc:
                self.store.error = type(exc).__name__
                self.last_report["persistence"] = dict(ok=False, error=self.store.error)
            await asyncio.sleep(min(0.5, self.cfg.affinity_ttl / 4, self.store.active_window / 4))

    def report(self):
        return self.last_report

    async def status(self):
        try:
            self.last_report = await asyncio.to_thread(self.store.report)
        except (sqlite3.Error, OSError) as exc:
            self.store.error = type(exc).__name__
            self.last_report = dict(self.last_report, persistence=dict(ok=False, error=self.store.error))
        return self.last_report

    async def stop(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.store:
            await asyncio.to_thread(self.store.close)
