"""Manage serving and routing independently. restart defaults to routing."""

import argparse
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request

from .config import Config, ROOT


def pidfile(role):
    return os.path.join(ROOT, ".proxy.pid" if role == "serving" else ".routing.pid")


def live_pid(role):
    try:
        with open(pidfile(role)) as file:
            pid = int(file.read().strip())
        if pid <= 1:
            raise RuntimeError("invalid pidfile for " + role)
        with open("/proc/{}/stat".format(pid)) as file:
            state = file.read().rpartition(")")[2].split()[0]
        if state == "Z":
            return None
        with open("/proc/{}/cmdline".format(pid), "rb") as file:
            args = file.read().split(b"\0")
        expected = b"proxy" if role == "serving" else b"routing"
        if not any(args[i:i + 2] == [b"-m", expected] for i in range(len(args) - 1)):
            raise RuntimeError("pidfile points to an unrelated process; refusing to signal it")
        with open("/proc/{}/environ".format(pid), "rb") as file:
            env = dict(item.split(b"=", 1) for item in file.read().split(b"\0") if b"=" in item)
        process_root = env.get(b"AZURE_PROXY_HOME")
        process_root = os.fsdecode(process_root) if process_root else os.readlink("/proc/{}/cwd".format(pid))
        if os.path.realpath(process_root) != os.path.realpath(ROOT):
            raise RuntimeError("pidfile belongs to another installation")
        return pid
    except (FileNotFoundError, ProcessLookupError):
        return None
    except ValueError:
        raise RuntimeError("invalid pidfile for " + role)


def health():
    cfg = Config(load_routes=False)
    host = "127.0.0.1" if cfg.host == "0.0.0.0" else cfg.host
    try:
        with urllib.request.urlopen("http://{}:{}/healthz".format(host, cfg.port), timeout=1) as reply:
            return json.load(reply)
    except (OSError, ValueError):
        return None


def publication():
    path = os.path.join(ROOT, "runtime", "control.sqlite3")
    try:
        db = sqlite3.connect("file:" + path + "?mode=ro", uri=True, timeout=1)
        try:
            row = db.execute("SELECT payload FROM state WHERE name='snapshot'").fetchone()
            return json.loads(row[0]) if row else None
        finally:
            db.close()
    except (sqlite3.Error, ValueError):
        return None


def refuse_online(role):
    pid = live_pid(role)
    if pid:
        raise RuntimeError("{} already running (pid {})".format(role, pid))
    if role == "serving" and health():
        raise RuntimeError("the configured serving endpoint is already answering")


def start(role, timeout):
    refuse_online(role)
    module = "proxy" if role == "serving" else "routing"
    log_path = os.path.join(ROOT, "proxy.log" if role == "serving" else "routing.log")
    with open(log_path, "ab", buffering=0) as log:
        process = subprocess.Popen([sys.executable, "-m", module], stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                   env=dict(os.environ, AZURE_PROXY_MANAGED_LOG="1"),
                                   cwd=os.path.dirname(os.path.dirname(__file__)))
    deadline = time.monotonic() + timeout
    accepted_revision = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("{} failed to start; see {}".format(role, log_path))
        if role == "serving":
            reply = health()
            ready = reply and reply.get("supervisor", {}).get("pid", reply.get("pid")) == process.pid
        else:
            snapshot = publication()
            ready = (snapshot and snapshot["routing"].get("ready")
                     and snapshot["routing"]["pid"] == process.pid)
            if ready:
                if accepted_revision is None:
                    accepted_revision = snapshot["revision"]
                reply = health()
                if reply:
                    state = reply.get("routing", {})
                    ready = (state.get("pid") == process.pid and state.get("ok")
                             and state.get("mapping_version", 0) >= accepted_revision)
        if ready:
            print("started {} (pid {})".format(role, process.pid))
            return
        time.sleep(0.1)
    raise RuntimeError("{} started but readiness/serving acknowledgement timed out; see {}".format(
        role, log_path))


def stop(role, timeout, force=False):
    pid = live_pid(role)
    if pid is None:
        print(role + " is not running")
        return
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = live_pid(role)
        if current is None:
            print("stopped {} (pid {})".format(role, pid))
            return
        if current != pid:
            raise RuntimeError("{} owner changed during shutdown".format(role))
        time.sleep(0.1)
    if not force:
        raise RuntimeError("{} is still draining; retry later or explicitly use --force".format(role))
    # Revalidate ownership immediately before escalation.
    if live_pid(role) == pid:
        os.kill(pid, signal.SIGKILL)
        print("force-stopped {} (pid {})".format(role, pid))
        for _ in range(50):
            if live_pid(role) is None:
                return
            time.sleep(0.1)
        raise RuntimeError("{} has not exited after SIGKILL".format(role))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "restart", "status"))
    parser.add_argument("role", nargs="?", choices=("serving", "routing", "all"))
    parser.add_argument("--force", action="store_true", help="allow forced shutdown after drain timeout")
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args(argv)
    role = args.role or ("routing" if args.action == "restart" else "all")
    roles = ["routing", "serving"] if role == "all" else [role]
    try:
        if args.action == "status":
            for name in roles:
                print("{}: {}".format(name, live_pid(name) or "stopped"))
            return
        if args.action == "start":
            for name in roles:
                refuse_online(name)
        if args.action == "restart" and "serving" in roles:
            from .supervisor import control_path
            if live_pid("serving") and not os.path.exists(control_path()):
                raise RuntimeError("legacy serving requires a scheduled first migration; wait for old sessions, then stop/start")
        if args.action in ("stop", "restart"):
            for name in reversed(roles):
                if args.action != "restart" or name != "serving":
                    stop(name, args.timeout, args.force)
        if args.action in ("start", "restart"):
            for name in roles:
                if args.action == "restart" and name == "serving" and live_pid(name):
                    with socket.socket(socket.AF_UNIX) as channel:
                        channel.settimeout(args.timeout + 10)
                        channel.connect(control_path())
                        channel.sendall(json.dumps(dict(action="restart", timeout=args.timeout)).encode() + b"\n")
                        with channel.makefile("rb") as reader:
                            result = json.loads(reader.readline())
                    if not result.get("ok"):
                        raise RuntimeError(result.get("error", "rolling restart failed"))
                    print("rolled serving: " + json.dumps(result))
                else:
                    start(name, args.timeout)
    except (RuntimeError, OSError, ValueError) as exc:
        parser.exit(1, str(exc) + "\n")


if __name__ == "__main__":
    main()
