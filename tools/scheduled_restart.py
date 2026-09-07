"""Restart at the next 01:00 Asia/Shanghai; one-shot unless --daily is given."""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from proxy.manage import health, live_pid
from proxy.state import InstanceLock


def next_run(now, hour=1, minute=0):
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    target = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return target if target > local else target + timedelta(days=1)


def restart(timeout, legacy_migration=False):
    def manage(action, role):
        subprocess.run([sys.executable, "-m", "proxy.manage", action, role,
                        "--timeout", str(timeout)], cwd=ROOT, check=True)
    before = health()
    if not before:
        raise RuntimeError("serving health check failed; scheduled restart cancelled")
    legacy = not before.get("supervisor", {}).get("active")
    if legacy and not legacy_migration:
        raise RuntimeError("legacy serving needs --legacy-migration after old sessions finish")
    # Publish the compatible weights/producer protocol before serving changes.
    manage("restart", "routing")
    if legacy:
        print("First migration: draining in-flight requests; legacy memory bindings cannot be restored.", flush=True)
        manage("stop", "serving")
        manage("start", "serving")
    else:
        manage("restart", "serving")
    after = health()
    if not after or not after.get("ok") or not after.get("routing", {}).get("ok"):
        raise RuntimeError("post-restart health verification failed")
    print(json.dumps(dict(result="healthy", supervisor=after.get("supervisor"),
                          affinity_store=after.get("affinity_store"))), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily", action="store_true", help="repeat each day; default is one run")
    parser.add_argument("--now", action="store_true", help="run immediately (for an external scheduler)")
    parser.add_argument("--legacy-migration", action="store_true",
                        help="allow first stop/start after old sessions have finished")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    root = os.environ.get("AZURE_PROXY_HOME", str(ROOT))
    lock = InstanceLock(root, "scheduled-restart")
    try:
        while True:
            target = datetime.now(ZoneInfo("Asia/Shanghai")) if args.now else next_run(datetime.now().astimezone())
            print("Scheduled restart: {}".format(target.isoformat()), flush=True)
            if args.dry_run:
                return
            while time.time() < target.timestamp():
                time.sleep(max(0, min(30, target.timestamp() - time.time())))
            restart(args.timeout, args.legacy_migration)
            if not args.daily:
                return
            args.now = False
    finally:
        lock.close()


if __name__ == "__main__":
    main()
