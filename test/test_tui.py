"""Dashboard process status, freshness, and independent health polling."""

import copy
import io
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from rich.console import Console

from tui.app import Dashboard, _footer, _processes
from tui.client import Poller
from tui.snapshot import Snapshot


def healthy():
    return {
        "health": {
            "pid": 101, "ok": True, "host": "127.0.0.1", "port": 8811,
            "uptime_seconds": 120, "token": {"have_token": True, "expires_in_seconds": 600},
            "routing": {"pid": 202, "ready": True, "ok": True,
                        "heartbeat_age_seconds": 0.2, "backlog": 0},
        },
        "routes": {"updated_at": 999.5, "stats_stale": False},
        "age": 0.1, "health_age": 0.1, "health_error": None,
    }


def render(renderable, width=120):
    output = io.StringIO()
    Console(file=output, width=width, color_system=None).print(renderable)
    return output.getvalue()


class ProcessStatusTests(unittest.TestCase):
    def snapshot(self, raw):
        with patch("tui.snapshot.time.time", return_value=1000):
            return Snapshot(raw)

    def test_both_processes_online_with_pids(self):
        text = render(_processes(self.snapshot(healthy())))
        self.assertIn("serving online  PID 101", text)
        self.assertIn("routing online  PID 202", text)
        self.assertIn("heartbeat 0s ago", text)

    def test_process_and_persistence_rows_use_requested_chinese_labels(self):
        raw = healthy()
        raw["health"]["routing"].update(telemetry_pending=0, telemetry_dropped=0)
        raw["health"].update(supervisor=dict(ok=True, pid=1059128, active=1059139, draining=[]),
                              affinity_store=dict(ok=True, pending=0))
        for width in (60, 80, 120, 180):
            text = render(_processes(self.snapshot(raw)), width)
            self.assertIn("统计记录  待处理 0  待写入 0  已丢失 0", text)
            self.assertIn("管理进程  在线  PID 1059128", text)
            self.assertIn("接流进程  PID 1059139  等待旧请求结束的进程 0", text)
            self.assertIn("会话绑定  持久化正常  待写入 0", text)

    def test_bad_credentials_do_not_mean_serving_is_offline(self):
        raw = healthy()
        raw["health"]["ok"] = False
        raw["health"]["token"] = {"have_token": False}
        self.assertIn("serving online", render(_processes(self.snapshot(raw))))

    def test_missing_heartbeat_preserves_last_pid_and_shows_backlog(self):
        raw = healthy()
        raw["health"]["routing"].update(ok=False, heartbeat_age_seconds=88000, backlog=3900000)
        raw["routes"].update(updated_at=1000 - 88000, stats_stale=True)
        snapshot = self.snapshot(raw)
        text = render(_processes(snapshot))
        self.assertIn("serving online", text)
        self.assertIn("routing no heartbeat  last PID 202", text)
        self.assertIn("heartbeat 24h ago", text)
        self.assertIn("待处理 3,900,000", text)
        footer = render(_footer(Dashboard(None, Console()), snapshot, 0))
        self.assertIn("fetch 0s ago", footer)
        self.assertIn("stats 24h ago  stale", footer)

    def test_stopped_routing(self):
        raw = healthy()
        raw["health"]["routing"].update(ready=False, ok=False)
        self.assertIn("routing stopped  last PID 202", render(_processes(self.snapshot(raw))))

    def test_exchange_failure_and_dropped_telemetry_are_visible(self):
        raw = healthy()
        raw["health"]["routing"].update(ok=False, exchange_error="database is locked",
                                         telemetry_dropped=12)
        snapshot = self.snapshot(raw)
        text = render(_processes(snapshot))
        self.assertIn("routing degraded", text)
        self.assertIn("已丢失 12", text)
        self.assertTrue(snapshot.stats_stale)

    def test_serving_unreachable_makes_routing_unknown(self):
        raw = healthy()
        raw.update(error="ConnectionRefusedError", health_error="ConnectionRefusedError")
        text = render(_processes(self.snapshot(raw)))
        self.assertIn("serving unreachable  last PID 101", text)
        self.assertIn("routing unknown  last PID 202", text)
        self.assertNotIn("routing online", text)

    def test_initial_and_legacy_health_have_unknown_routing(self):
        for raw, serving in (({}, "connecting"), ({"health": {"ok": True}}, "online")):
            with self.subTest(raw=raw):
                text = render(_processes(self.snapshot(raw)))
                self.assertIn("serving " + serving, text)
                self.assertIn("routing unknown", text)

    def test_ages_advance_between_polls_and_expire_online_status(self):
        raw = healthy()
        raw["health_age"] = 3
        snapshot = self.snapshot(raw)
        self.assertAlmostEqual(snapshot.heartbeat_age, 3.2)
        self.assertIn("routing no heartbeat", render(_processes(snapshot)))
        raw["health_age"] = 6
        text = render(_processes(self.snapshot(raw)))
        self.assertIn("serving unconfirmed", text)
        self.assertIn("routing unknown", text)
        with patch("tui.snapshot.time.time", return_value=1010):
            self.assertEqual(Snapshot(raw).stats_age, 10.5)

    def test_recovery_replaces_pids_and_clears_staleness(self):
        raw = healthy()
        raw["health"]["pid"] = 303
        raw["health"]["routing"]["pid"] = 404
        snapshot = self.snapshot(raw)
        text = render(_processes(snapshot))
        self.assertIn("serving online  PID 303", text)
        self.assertIn("routing online  PID 404", text)
        self.assertFalse(snapshot.stats_stale)

    def test_statistics_age_uses_remote_clock_when_available(self):
        raw = healthy()
        raw["routes"].update(updated_at=2000, routing={
            "heartbeat": 2000, "heartbeat_age_seconds": 10})
        snapshot = self.snapshot(raw)
        self.assertAlmostEqual(snapshot.stats_age, 10.1)
        self.assertTrue(snapshot.stats_stale)

    def test_process_rows_and_footer_fit_narrow_terminal(self):
        raw = healthy()
        raw["routes"]["routes"] = {
            "endpoint-{}/deployment".format(i): {"endpoint": "endpoint-{}".format(i)}
            for i in range(20)}
        raw["routes"]["models"] = {
            "model-{}".format(i): [{"route": "endpoint-{}/deployment".format(i)}]
            for i in range(20)}
        raw["events"] = [dict(seq=i, at=999, kind="timeout", level="error", message="timeout")
                         for i in range(50)]
        for width in (60, 80, 120, 180):
            for board in (0, 1, 2):
                with self.subTest(width=width, board=board):
                    console = Console(file=io.StringIO(), width=width, height=24)
                    dashboard = Dashboard(SimpleNamespace(snapshot=lambda: raw), console)
                    dashboard.board = board
                    dashboard.show_all = True
                    with patch("tui.snapshot.time.time", return_value=1000):
                        frame = dashboard.render()
                    lines = console.render_lines(frame, console.options.update(height=None))
                    self.assertLessEqual(len(lines), 24)
                    text = render(frame, width)
                    self.assertIn("serving online", text)
                    self.assertIn("routing online", text)
                    self.assertIn("fetch 0s ago", text)


class PollerStatusTests(unittest.TestCase):
    def poll_once(self, poller, responses):
        def get(path):
            value = responses.get(path, responses.get(path.split("&initial=")[0]))
            if isinstance(value, Exception) or path.startswith("/events"):
                poller.stop()
            if isinstance(value, Exception):
                raise value
            return copy.deepcopy(value)
        poller._stop.clear()
        with patch.object(poller, "_get", side_effect=get), patch("tui.client.time.time", return_value=1000):
            poller._loop()

    def test_statistics_failure_keeps_successful_health_and_last_statistics(self):
        poller = Poller("http://unused")
        responses = {"/healthz": healthy()["health"], "/routes": healthy()["routes"],
                     "/events?since=0&limit=400": {"next": 0, "events": []}}
        self.poll_once(poller, responses)
        for path in ("/routes", "/events?since=0&limit=400"):
            with self.subTest(path=path):
                failed = dict(responses, **{path: OSError("statistics unavailable")})
                self.poll_once(poller, failed)
                raw = poller.snapshot()
                self.assertIsNone(raw["health_error"])
                self.assertIn("statistics unavailable", raw["error"])
                self.assertEqual(raw["routes"], healthy()["routes"])
                raw["health_age"] = 0
                self.assertIn("serving online", render(_processes(Snapshot(raw))))
        self.poll_once(poller, {"/healthz": OSError("connection refused")})
        self.assertIn("connection refused", poller.snapshot()["health_error"])
        self.poll_once(poller, responses)
        self.assertIsNone(poller.snapshot()["health_error"])
        self.assertIsNone(poller.snapshot()["error"])


class LocalStatusTests(unittest.TestCase):
    def test_unavailable_local_state_is_read_only(self):
        import os
        import tempfile
        from tui.local_status import read_status
        with tempfile.TemporaryDirectory() as root:
            self.assertIsNone(read_status(root))
            self.assertEqual(os.listdir(root), [])

    def test_fresh_local_routing_status_is_independent_of_http_fetch_age(self):
        raw = healthy()
        raw["health"]["local_status"] = True
        raw.update(health_age=100, health_error="connection refused", local_state_age=.1)
        snapshot = Snapshot(raw)
        self.assertAlmostEqual(snapshot.heartbeat_age, .3)
        output = render(_processes(snapshot))
        self.assertIn("serving unreachable", output)
        self.assertIn("routing online", output)

    def test_polling_continues_without_manual_refresh(self):
        poller = Poller("http://unused", interval=0.01)
        third_poll = threading.Event()
        calls = []
        def get(path):
            calls.append(path)
            if path == "/healthz":
                return healthy()["health"]
            if path == "/routes":
                return healthy()["routes"]
            if calls.count("/healthz") >= 3:
                third_poll.set()
            return {"next": 0, "events": []}
        with patch.object(poller, "_get", side_effect=get):
            poller.start()
            try:
                self.assertTrue(third_poll.wait(2))
            finally:
                poller.stop()
                poller._thread.join(2)
        self.assertGreaterEqual(calls.count("/healthz"), 3)
        self.assertIsNone(poller.snapshot()["error"])


if __name__ == "__main__":
    unittest.main()
