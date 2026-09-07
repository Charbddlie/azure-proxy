"""Read local process status independently of serving, without creating state."""

import json
from pathlib import Path
import sqlite3
import time


def _alive(pid):
    if not isinstance(pid, int) or pid <= 1:
        return False
    try:
        with open("/proc/{}/stat".format(pid)) as file:
            return file.read().rpartition(")")[2].split()[0] != "Z"
    except (OSError, IndexError):
        return False


def read_status(root):
    runtime = Path(root) / "runtime"
    result = {}
    try:
        database = sqlite3.connect((runtime / "control.sqlite3").as_uri() + "?mode=ro", uri=True, timeout=.2)
        try:
            row = database.execute("SELECT payload FROM state WHERE name='snapshot'").fetchone()
            emitted = database.execute("SELECT COALESCE(MAX(global_seq),0) FROM producers").fetchone()[0]
        finally:
            database.close()
        if row:
            snapshot = json.loads(row[0])
            if not isinstance(snapshot, dict):
                raise ValueError("invalid snapshot")
            routing = dict(snapshot.get("routing") or {})
            age = max(0, time.time() - routing.get("heartbeat", 0))
            alive = _alive(routing.get("pid"))
            routing.update(ok=bool(routing.get("ready")) and alive and age < 3,
                           heartbeat_age_seconds=age, local_observation=True,
                           backlog=max(0, emitted - routing.get("processed_seq", 0)))
            if not alive:
                routing["ready"] = False
            result["routing"] = routing
    except (sqlite3.Error, OSError, ValueError, TypeError):
        pass
    try:
        with open(runtime / "supervisor.json") as file:
            supervisor = json.load(file)
        if not isinstance(supervisor, dict):
            raise ValueError("invalid supervisor status")
        supervisor["ok"] = _alive(supervisor.get("pid")) and time.time() - supervisor.get("heartbeat", 0) < 3
        result["supervisor"] = supervisor
    except (OSError, ValueError, TypeError):
        pass
    return result or None
