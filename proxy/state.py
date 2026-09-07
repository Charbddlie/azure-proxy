"""Local, versioned exchange between serving and routing.

SQLite connections are used by background workers. Forwarding uses memory.
The producer watermark makes retries safe even after acknowledged rows are
compacted; the routing checkpoint and publication commit in one transaction.
"""

import fcntl
import json
import os
import sqlite3
import collections
import time

from .retention import CLEANUP_INTERVAL, RETENTION_SECONDS, prune_checkpoint, recent

SCHEMA_VERSION = 1
COMPACT_BATCH = 2000


def is_busy(error):
    """Recognize transient SQLite lock errors across supported Python versions."""
    code = getattr(error, "sqlite_errorcode", None)
    if code is not None:
        return code & 0xff in (5, 6)  # SQLITE_BUSY / SQLITE_LOCKED, including extended codes.
    return str(error).split(":", 1)[0] in (
        "database is locked", "database table is locked", "database schema is locked")


def encode(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False)


class InstanceLock:
    def __init__(self, root, role):
        os.makedirs(os.path.join(root, "runtime"), exist_ok=True)
        self.file = open(os.path.join(root, "runtime", role + ".lock"), "a+")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError("{} is already running for {}".format(role, root))

    def close(self):
        self.file.close()


class Store:
    def __init__(self, root):
        directory = os.path.join(root, "runtime")
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, "control.sqlite3")
        # The database contains deployment metadata, never access tokens.
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        self.db = sqlite3.connect(path, timeout=1, check_same_thread=False)
        try:
            self._initialize()
        except Exception:
            self.db.close()
            raise
        self._checkpoint_cursor = None
        self._event_seq = None
        self._event_key = None
        self._checkpoint_epoch = None

    def _initialize(self):
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
            self.db.execute("PRAGMA auto_vacuum=INCREMENTAL")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise ValueError("unsupported control database version {}".format(version))
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS telemetry (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                producer TEXT NOT NULL,
                local_seq INTEGER NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(producer, local_seq)
            );
            CREATE TABLE IF NOT EXISTS producers (
                id TEXT PRIMARY KEY, local_seq INTEGER NOT NULL,
                global_seq INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS state (
                name TEXT PRIMARY KEY, payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS telemetry_at ON telemetry (
                COALESCE(json_extract(payload, '$.at'), 0)
            );
            PRAGMA user_version=1;
        """)

    def get(self, name):
        if name in ("snapshot", "checkpoint"):
            rows = dict(self.db.execute("SELECT name,payload FROM state WHERE name IN (?, 'events')",
                                        (name,)))
            if name not in rows:
                return None
            value = json.loads(rows[name])
            feed = json.loads(rows["events"]) if "events" in rows else None
            if feed is not None:
                value["events"] = feed if name == "snapshot" else feed["events"]
            return value
        row = self.db.execute("SELECT payload FROM state WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else None

    def append(self, producer, batch):
        with self.db:
            row = self.db.execute(
                "SELECT local_seq, global_seq FROM producers WHERE id=?",
                (producer,)).fetchone()
            local, global_seq = row or (0, 0)
            for event in batch:
                if event["local_seq"] <= local:
                    continue
                cursor = self.db.execute(
                    "INSERT INTO telemetry(producer,local_seq,payload) VALUES(?,?,?)",
                    (producer, event["local_seq"], encode(event)))
                local, global_seq = event["local_seq"], cursor.lastrowid
            self.db.execute("INSERT OR REPLACE INTO producers VALUES(?,?,?)",
                            (producer, local, global_seq))
        return local, global_seq

    def highwater(self):
        return self.db.execute(
            "SELECT COALESCE(MAX(global_seq),0) FROM producers").fetchone()[0]

    def read_events(self, after, limit=2000, through=None):
        bound = " AND seq<=?" if through is not None else ""
        args = (after, through, limit) if through is not None else (after, limit)
        return [(seq, dict(json.loads(payload), producer=producer)) for seq, producer, payload in self.db.execute(
            "SELECT seq,producer,payload FROM telemetry WHERE seq>?" + bound + " ORDER BY seq LIMIT ?",
            args)]

    def expire(self, now=None):
        """Delete one bounded batch and atomically record the discarded sequence range."""
        now = time.time() if now is None else now
        cutoff = now - RETENTION_SECONDS + CLEANUP_INTERVAL
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            rows = self.db.execute(
                "SELECT seq FROM telemetry WHERE COALESCE(json_extract(payload, '$.at'), 0)<=? "
                "ORDER BY COALESCE(json_extract(payload, '$.at'), 0) LIMIT ?",
                (cutoff, COMPACT_BATCH)).fetchall()
            retention = self.get("retention") or {"through": 0, "deleted": 0}
            if rows:
                retention["through"] = max(retention["through"], max(row[0] for row in rows))
                retention["deleted"] += len(rows)
                self.db.executemany("DELETE FROM telemetry WHERE seq=?", rows)
            retention.update(cleaned_at=now, hours=24)
            self.db.execute("INSERT OR REPLACE INTO state VALUES('retention',?)", (encode(retention),))
            for name, payload in self.db.execute(
                    "SELECT name,payload FROM state WHERE name IN ('events','checkpoint','snapshot')"
                    ).fetchall():
                value = json.loads(payload)
                if name == "checkpoint":
                    prune_checkpoint(value, cutoff)
                feed = value if name == "events" else value.get("events") if name == "snapshot" else None
                if isinstance(feed, dict):
                    feed["retention_through"] = max([feed.get("retention_through", 0)] +
                        [e["seq"] for e in feed.get("events", []) if e["at"] <= cutoff])
                    feed["events"] = recent(feed.get("events", []), cutoff)
                    feed["counts"] = dict(collections.Counter(e["kind"] for e in feed["events"]))
                updated = encode(value)
                if updated != payload:
                    self.db.execute("UPDATE state SET payload=? WHERE name=?", (updated, name))
        return len(rows)

    def reclaim(self, pages=256):
        """Release a bounded number of free pages; WAL checkpoints remain non-blocking."""
        if self.db.execute("PRAGMA auto_vacuum").fetchone()[0] == 2:
            self.db.execute("PRAGMA incremental_vacuum({})".format(int(pages))).fetchall()
        return self.db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()

    def publish(self, checkpoint, snapshot):
        # Event history is shared by snapshot/checkpoint. An idle heartbeat
        # writes only the compact publication, rather than rewriting the ring.
        publication = {key: value for key, value in snapshot.items() if key != "events"}
        records = [("snapshot", encode(publication))]
        feed = snapshot["events"]
        event_key = (feed.get("stream_id"), feed.get("retention_through"), feed["next"], len(feed["events"]),
                     feed["events"][0].get("seq") if feed["events"] else None)
        if (self._checkpoint_cursor != checkpoint["cursor"]
                or self._event_key != event_key
                or self._checkpoint_epoch != checkpoint.get("retention_epoch")):
            saved = {key: value for key, value in checkpoint.items() if key != "events"}
            records.append(("checkpoint", encode(saved)))
        if self._event_key != event_key:
            records.append(("events", encode(snapshot["events"])))
        with self.db:
            self.db.executemany("INSERT OR REPLACE INTO state VALUES(?,?)", records)
            # Bound the write lock when recovering a large backlog. Remaining
            # checkpointed rows are safe to compact in later publications.
            self.db.execute("DELETE FROM telemetry WHERE seq IN "
                            "(SELECT seq FROM telemetry WHERE seq<=? ORDER BY seq LIMIT ?)",
                            (checkpoint["cursor"], COMPACT_BATCH))
        self._checkpoint_cursor = checkpoint["cursor"]
        self._event_seq = snapshot["events"]["next"]
        self._event_key = event_key
        self._checkpoint_epoch = checkpoint.get("retention_epoch")

    def close(self):
        self.db.close()
