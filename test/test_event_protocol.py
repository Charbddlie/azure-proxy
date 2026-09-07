"""History truncation, incremental loss, stale generations and TUI acknowledgements."""

import io
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from rich.console import Console
from proxy.events import event_feed
from tui.app import Dashboard
from tui.client import Poller
from test_tui import healthy, render


def feed(start, stop, stream_id="stable", at=None):
    at = time.time() if at is None else at
    return dict(stream_id=stream_id, next=stop, events=[
        dict(seq=i, at=at, kind="response", message="ok") for i in range(start, stop + 1)])


class EventProtocolTests(unittest.TestCase):
    def test_first_2000_to_400_is_history_not_gap(self):
        result = event_feed(feed(1, 2000), initial=True)
        self.assertEqual(len(result["events"]), 400)
        self.assertFalse(result["dropped"])
        self.assertIsNone(result["gap"])
        poller = Poller("unused")
        poller._accept_feed(result)
        self.assertEqual(poller.snapshot()["history_notice"], "已载入最近 400 条历史")
        self.assertEqual(poller.snapshot()["missed_events"], 0)

    def test_initial_empty_then_burst_is_incremental(self):
        poller = Poller("unused")
        poller._accept_feed(event_feed(feed(1, 0), initial=True))
        self.assertTrue(poller._initial_loaded)
        poller._accept_feed(event_feed(feed(1, 2000), since=0))
        self.assertEqual(poller.snapshot()["gap"]["count"], 1600)
        self.assertEqual(poller.snapshot()["gap"]["reasons"], ["limit"])

    def test_overwrite_and_limit_are_counted_once(self):
        result = event_feed(feed(1001, 3000), since=500)
        self.assertEqual(result["gap"]["count"], 2100)
        self.assertEqual(result["gap"]["reasons"], [dict(reason="buffer_overwrite", count=500),
                                                     dict(reason="limit", count=1600)])
        filtered = event_feed(feed(1001, 3000), since=1000, kind="timeout")
        self.assertIsNone(filtered["gap"])
        self.assertEqual(filtered["next"], 3000)

    def test_retention_when_entire_unread_stream_expires(self):
        result = event_feed(feed(1, 2000, at=time.time() - 86401), since=500)
        self.assertEqual(result["gap"]["count"], 1500)
        self.assertEqual(result["gap"]["reasons"], [dict(reason="retention", count=1500)])

    def test_same_stream_stale_snapshot_does_not_rewind_or_duplicate(self):
        poller = Poller("unused")
        poller._accept_feed(event_feed(feed(1, 200), initial=True))
        self.assertFalse(poller._accept_feed(event_feed(feed(1, 100), since=200)))
        self.assertEqual(poller._cursor, 200)
        self.assertEqual(len(poller._events), 200)
        poller._accept_feed(event_feed(feed(1, 200), since=200))
        self.assertEqual(len(poller._events), 200)

    def test_rebuild_resets_history_alerts_cursor_and_counters(self):
        poller = Poller("unused")
        poller._accept_feed(event_feed(feed(1, 0), initial=True))
        poller._accept_feed(event_feed(feed(1, 2000), since=0))
        self.assertFalse(poller._accept_feed(event_feed(feed(1, 10, "new"), since=2000)))
        self.assertFalse(poller._initial_loaded)
        self.assertEqual(poller._cursor, 0)
        self.assertEqual(poller._events, [])
        self.assertIsNone(poller.snapshot()["gap"])
        self.assertEqual(poller.snapshot()["missed_events"], 0)
        poller._accept_feed(event_feed(feed(1, 10, "new"), initial=True))
        self.assertEqual(poller._cursor, 10)

    def test_30_second_timeout_manual_confirmation_and_new_gap(self):
        poller = Poller("unused")
        with patch("tui.client.time.monotonic", return_value=100):
            poller._accept_feed(event_feed(feed(1, 0), initial=True))
            poller._accept_feed(event_feed(feed(1, 2000), since=0))
        with patch("tui.client.time.monotonic", return_value=129):
            self.assertIsNotNone(poller.snapshot()["gap"])
        with patch("tui.client.time.monotonic", return_value=130):
            self.assertIsNone(poller.snapshot()["gap"])
            poller._accept_feed(event_feed(feed(2001, 3000), since=2000))
            self.assertIsNotNone(poller.snapshot()["gap"])
            Dashboard(poller, Console()).key("r")
            self.assertIsNotNone(poller.snapshot()["gap"])
            Dashboard(poller, Console()).key("c")
            self.assertIsNone(poller.snapshot()["gap"])
            self.assertEqual(poller.snapshot()["missed_events"], 2200)

    def test_legacy_first_truncation_suppressed_later_loss_unknown(self):
        poller = Poller("unused")
        poller._accept_feed(dict(events=[], next=2000, dropped=True))
        self.assertIsNone(poller.snapshot()["gap"])
        poller._accept_feed(dict(events=[], next=4000, dropped=True))
        self.assertIsNone(poller.snapshot()["gap"]["count"])
        self.assertEqual(poller.snapshot()["unknown_gaps"], 1)

    def test_local_cache_eviction_does_not_count_as_loss(self):
        poller = Poller("unused")
        poller._accept_feed(event_feed(feed(1, 0), initial=True))
        for i in range(0, 6000, 200):
            poller._accept_feed(event_feed(feed(i + 1, i + 200), since=i))
        self.assertEqual(len(poller._events), 4000)
        self.assertEqual(poller.snapshot()["missed_events"], 0)

    def test_alert_status_and_footer_fit_all_widths(self):
        raw = healthy()
        raw.update(gap=dict(count=1600, reasons=["limit"]), missed_events=1600)
        raw["health"].update(supervisor=dict(ok=True, pid=123, active=101, draining=[99]),
                              affinity_store=dict(ok=True, pending=2))
        raw["events"] = feed(1, 100)["events"]
        for width in (60, 80, 120, 180):
            for board in (0, 1, 2):
                console = Console(file=io.StringIO(), width=width, height=24)
                dash = Dashboard(SimpleNamespace(snapshot=lambda: raw), console)
                dash.board = board
                frame = dash.render()
                self.assertLessEqual(len(console.render_lines(frame, console.options.update(height=None))), 24)
                text = render(frame, width)
                for label in ("serving online", "管理进程", "持久化正常", "轮询期间漏读", "确认提示"):
                    self.assertIn(label, text)


if __name__ == "__main__":
    unittest.main()
