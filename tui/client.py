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

from proxy.retention import RETENTION_SECONDS, recent


class Poller:
    """Fetches in the background; `snapshot()` returns the latest, never blocks.

    The event cursor lives here rather than in the boards because it is a
    property of the connection: on reconnect, or when the proxy restarts and its
    sequence numbers go back to zero, the cursor has to be reset in step with
    the socket, and only this object sees both.
    """

    def __init__(self, base_url: str, interval: float = 1.0,
                 event_limit: int = 400, timeout: float = 4.0, local_root=None):
        self.base_url = base_url.rstrip("/")
        self.interval = interval
        self.event_limit = event_limit
        self.timeout = timeout
        self.local_root = local_root
        self._local_fetched_at = 0

        self._lock = threading.Lock()
        self._health: Optional[dict] = None
        self._routes: Optional[dict] = None
        self._events: list = []
        self._cursor = 0
        self._initial_loaded = False
        self._stream_id = None
        self._gap = None
        self._missed = 0
        self._unknown_gaps = 0
        self._history = None
        self._fetched_at = 0.0
        self._error: Optional[str] = None
        self._health_fetched_at = 0.0
        self._health_error: Optional[str] = None
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

    def acknowledge_gap(self):
        with self._lock:
            self._gap = None

    def _accept_feed(self, feed):
        """Called under the poll lock; a stale generation never rewinds a cursor."""
        stream_id = feed.get("stream_id")
        cursor = int(feed.get("next", self._cursor) or 0)
        if stream_id and self._stream_id and stream_id != self._stream_id:
            self._events = []
            self._cursor = 0
            self._initial_loaded = False
            self._gap = self._history = None
            self._missed = self._unknown_gaps = 0
            self._stream_id = stream_id
            return False  # Fetch latest history with an explicit initial request.
        if cursor < self._cursor:
            if stream_id:
                return False
            # Legacy protocol cannot distinguish a restart from a stale snapshot.
            # Confirm the lower waterline on a subsequent poll before resetting.
            if getattr(self, "_legacy_lower", None) != cursor:
                self._legacy_lower = cursor
                return False
            self._events, self._cursor, self._initial_loaded = [], 0, False
            self._gap = self._history = None
            self._missed = self._unknown_gaps = 0
        self._legacy_lower = None
        self._stream_id = stream_id or self._stream_id
        if not self._initial_loaded:
            if (feed.get("history") or {}).get("truncated") or feed.get("dropped"):
                self._history = "已载入最近 {} 条历史".format(len(feed.get("events") or []))
        else:
            gap = feed.get("gap")
            if gap or feed.get("dropped"):
                count = gap.get("count") if gap else None
                reasons = [item["reason"] for item in gap.get("reasons", [])] if gap else ["legacy_unknown"]
                self._gap = dict(count=count, reasons=reasons, at=time.time(),
                                 deadline=time.monotonic() + 30)
                if count is None:
                    self._unknown_gaps += 1
                else:
                    self._missed += count
        self._events.extend(e for e in (feed.get("events") or []) if e["seq"] > self._cursor)
        self._cursor = cursor
        self._initial_loaded = True
        del self._events[:-4000]
        return True

    # -- reading ----------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            now = time.time()
            self._events = recent(self._events, now - RETENTION_SECONDS)
            return {"health": self._health, "routes": self._routes,
                    "events": list(self._events),
                    "gap": (dict(self._gap) if self._gap and time.monotonic() < self._gap["deadline"] else None),
                    "missed_events": self._missed, "unknown_gaps": self._unknown_gaps,
                    "history_notice": self._history,
                    "fetched_at": self._fetched_at, "error": self._error,
                    "age": (max(0, now - self._fetched_at)
                            if self._fetched_at else None),
                    "health_error": self._health_error,
                    "local_state_age": max(0, now - self._local_fetched_at) if self._local_fetched_at else None,
                    "health_age": (max(0, now - self._health_fetched_at)
                                   if self._health_fetched_at else None)}

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
            except Exception as e:
                local = None
                if self.local_root:
                    from .local_status import read_status
                    local = read_status(self.local_root)
                with self._lock:
                    self._health_error = self._error = "{}: {}".format(type(e).__name__, e)
                    if local:
                        self._health = dict(self._health or {}, **local, local_status=True)
                        self._local_fetched_at = time.time()
                self._wake.wait(self.interval)
                self._wake.clear()
                continue
            # Serving reachability is independent of the statistics endpoints.
            with self._lock:
                self._health = health
                self._health_fetched_at = time.time()
                self._health_error = None
            try:
                routes = self._get("/routes")
                feed = self._get("/events?since={}&limit={}&initial={}".format(
                    self._cursor, self.event_limit, str(not self._initial_loaded).lower()))
            except Exception as e:
                with self._lock:
                    self._error = "{}: {}".format(type(e).__name__, e)
            else:
                with self._lock:
                    if self._accept_feed(feed):
                        self._routes = routes
                        self._fetched_at = time.time()
                        self._error = None
                    else:
                        self._error = "event snapshot changed or stale; retrying"
            self._wake.wait(self.interval)
            self._wake.clear()
