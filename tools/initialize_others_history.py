"""One-time, backed-up history initialization by learned RPM capacity."""

import argparse
import json
import os
import sqlite3
import time
import uuid

from proxy.config import ROOT
from proxy.state import InstanceLock, encode


def initialize(db, threshold=100, apply=False):
    """Change only explicit history flags; preserve measurements and cursors."""
    with db:
        if apply:
            db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT payload FROM state WHERE name='checkpoint'").fetchone()
        if row is None:
            raise ValueError("routing checkpoint is missing")
        saved = json.loads(row[0])
        changed = {key: dict(foreign_seen=state["foreign_seen"], safe_rpm=state["safe_rpm"])
                   for key, state in saved["states"].items()
                   if state.get("foreign_seen") and 0 < state.get("safe_rpm", 0) < threshold}
        backup = None
        if apply and changed:
            backup = "others_history_backup/" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]
            db.execute("INSERT INTO state(name,payload) VALUES(?,?)", (
                backup, encode(dict(at=time.time(), threshold_rpm=threshold,
                                    cursor=saved["cursor"], routes=changed))))
            for key in changed:
                saved["states"][key]["foreign_seen"] = False
            db.execute("UPDATE state SET payload=? WHERE name='checkpoint'", (encode(saved),))
    return dict(applied=apply, threshold_rpm=threshold, changed=changed, backup=backup)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write after routing is stopped")
    args = parser.parse_args()
    lock = InstanceLock(ROOT, "routing") if args.apply else None
    try:
        mode = "rw" if args.apply else "ro"
        with sqlite3.connect("file:" + os.path.join(ROOT, "runtime", "control.sqlite3") + "?mode=" + mode,
                             uri=True, timeout=5) as db:
            print(json.dumps(initialize(db, apply=args.apply), ensure_ascii=False, indent=2))
    finally:
        if lock is not None:
            lock.close()


if __name__ == "__main__":
    main()
