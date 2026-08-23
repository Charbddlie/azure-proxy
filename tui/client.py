"""Polling the proxy over its own HTTP surface.

Loopback GETs against /healthz, /routes and /events, on a thread, so a slow or
dead proxy stalls the poll rather than the redraw. The dashboard keeps painting
the last good snapshot with its age shown; freezing the screen would be a worse
answer to "is it still alive" than an old screen that says how old it is.

urllib rather than httpx: this is three GETs a second against localhost, and
the dashboard is also expected to run attached to a proxy in a different
environment, where having no dependency of its own is worth more than the
convenience.

Nothing here writes. Every endpoint it touches is a reader, and /events and
/routes are documented as numbers-only — no request or response content passes
through either, so the dashboard cannot become a place prompts end up.
"""

import json
import threading
import time
import urllib.error
import urllib.request
from typing import Optional


class Poller:
    """Fetches in the background; `snapshot()` returns the latest, never blocks.

    The event cursor lives here rather than in the boards because it is a
    property of the connection: on reconnect, or when the proxy restarts and its
    sequence numbers go back to zero, the cursor has to be reset in step with
    the socket, and only this object sees both.
    """

    def __init__(self, base_url: str, interval: float = 1.0,
                 event_limit: int = 400, timeout: float = 4.0):
        self.base_url = base_url.rstrip("/")
        self.interval = interval
        self.event_limit = event_limit
        self.timeout = timeout

        self._lock = threading.Lock()
        self._health: Optional[dict] = None
        self._routes: Optional[dict] = None
        self._events: list = []
        self._cursor = 0
        self._dropped = False
        self._fetched_at = 0.0
        self._error: Optional[str] = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle --------------------------------------------------------
    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="poll",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def refresh_now(self) -> None:
        """Skip the rest of the interval. Bound to `r`."""
        self._wake.set()

    # -- reading ----------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return {"health": self._health, "routes": self._routes,
                    "events": list(self._events), "dropped": self._dropped,
                    "fetched_at": self._fetched_at, "error": self._error,
                    "age": (time.time() - self._fetched_at
                            if self._fetched_at else None)}

    # -- polling ----------------------------------------------------------
    def _get(self, path: str) -> dict:
        request = urllib.request.Request(
            self.base_url + path, headers={"accept": "application/json"})
        with urllib.request.urlopen(request, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                health = self._get("/healthz")
                routes = self._get("/routes")
                feed = self._get("/events?since={}&limit={}".format(
                    self._cursor, self.event_limit))
            except Exception as e:
                with self._lock:
                    self._error = "{}: {}".format(type(e).__name__, e)
            else:
                with self._lock:
                    self._health, self._routes = health, routes
                    cursor = int(feed.get("next", self._cursor) or 0)
                    if cursor < self._cursor:
                        # The proxy restarted: sequence numbers begin again at
                        # one, so anything held is from a process that no longer
                        # exists. Keeping it would interleave two runs' events
                        # under one timeline.
                        self._events = []
                        self._dropped = False
                    self._cursor = cursor
                    self._events.extend(feed.get("events") or [])
                    # Bounded independently of the server's ring: the dashboard
                    # only ever displays a screenful, and an attached session
                    # left running for a week should not grow.
                    if len(self._events) > 4000:
                        del self._events[:-4000]
                    self._dropped = self._dropped or bool(feed.get("dropped"))
                    self._fetched_at = time.time()
                    self._error = None
            self._wake.wait(self.interval)
            self._wake.clear()
