"""Background discovery and atomic publication of a coherent route-file pair."""

import json
import logging
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time


def _read(path):
    with open(path) as file:
        return json.load(file)


def _atomic_json(path, value):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=os.path.dirname(path),
                                         prefix=".discovery-", delete=False) as file:
            temporary = file.name
            json.dump(value, file, ensure_ascii=False, allow_nan=False, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def persist_discovery(root, discovery, directory=None):
    """The bundle is authoritative; sources/models remain readable mirrors."""
    directory = directory or os.path.join(root, "runtime")
    os.makedirs(directory, exist_ok=True)
    bundle = os.path.join(directory, "discovery.json")
    try:
        previous = _read(bundle)
    except FileNotFoundError:
        try:
            previous = {key: _read(os.path.join(directory, key + ".json"))
                        for key in ("sources", "models")}
        except FileNotFoundError:
            previous = None
        if previous is not None:
            _atomic_json(bundle, previous)
    if previous is not None and previous != discovery:
        _atomic_json(os.path.join(directory, "discovery.previous.json"), previous)
    for key in ("sources", "models"):
        _atomic_json(os.path.join(directory, key + ".json"), discovery[key])
    _atomic_json(bundle, discovery)


class RouteRefresher:
    def __init__(self, root, interval=3600, timeout=900):
        self.root, self.interval, self.timeout = root, interval, timeout
        self.stop = threading.Event()
        self.results = queue.Queue(maxsize=1)
        self.lock = threading.Lock()
        self.state = dict(enabled=True, interval_seconds=interval, running=False,
                          last_started_at=None, last_success_at=None, last_error=None,
                          next_refresh_at=time.time())
        self.thread = threading.Thread(target=self.run, name="route-refresh", daemon=True)

    def start(self):
        self.thread.start()

    def status(self):
        with self.lock:
            return dict(self.state)

    def update(self, **fields):
        with self.lock:
            self.state.update(fields)

    def take(self):
        try:
            return self.results.get_nowait()
        except queue.Empty:
            return None

    def accepted(self):
        self.update(last_success_at=time.time(), last_error=None)

    def rejected(self, error):
        self.update(last_error=str(error))
        logging.getLogger(__name__).error("route refresh failed; keeping current routes: %s", error)

    def probe(self):
        runtime = os.path.join(self.root, "runtime")
        os.makedirs(runtime, exist_ok=True)
        script = os.path.join(os.path.dirname(os.path.dirname(__file__)), "probe", "probe.py")
        with tempfile.TemporaryDirectory(prefix="route-refresh-", dir=runtime) as staging:
            process = subprocess.Popen(
                [sys.executable, "-u", script, "--output-dir", staging, "--strict"],
                cwd=self.root, env=dict(os.environ, AZURE_PROXY_HOME=self.root),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + self.timeout
            try:
                while True:
                    if self.stop.is_set():
                        raise InterruptedError("route refresh stopped")
                    if time.monotonic() >= deadline:
                        raise TimeoutError("route discovery exceeded {} seconds".format(self.timeout))
                    try:
                        output, _ = process.communicate(timeout=.5)
                        break
                    except subprocess.TimeoutExpired:
                        continue
                if process.returncode:
                    raise RuntimeError(output.decode("utf-8", "replace")[-2000:])
                return _read(os.path.join(staging, "discovery.json"))
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.communicate()

    def run(self):
        log = logging.getLogger(__name__)
        while not self.stop.is_set():
            started = time.time()
            due = time.monotonic() + self.interval
            self.update(running=True, last_started_at=started,
                        next_refresh_at=started + self.interval)
            log.info("route refresh starting")
            try:
                discovery = self.probe()
                self.results.put_nowait(discovery)
            except InterruptedError:
                return
            except Exception as error:
                self.rejected(error)
            finally:
                self.update(running=False)
            if self.stop.wait(max(0, due - time.monotonic())):
                return

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
