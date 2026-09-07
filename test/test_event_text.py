"""Event names and explanations are Chinese; severity codes stay unchanged."""

import copy
import io
import unittest

from rich.console import Console
from tui.boards import EVENT_FILTERS, render_events, _event_change
from tui.event_text import KIND_LABELS, kind_label, message


class EventTextTests(unittest.TestCase):
    def test_distinguishes_retry_destinations(self):
        for target, label in (("source/a", "原地重试"), ("source/b", "换部署重试"), ("other/b", "换源重试")):
            event = dict(kind="failover", route="source/a", to_route=target,
                         reason="200+throttled from source/a", wait_seconds=11)
            before = copy.deepcopy(event)
            self.assertEqual(kind_label(event), label)
            self.assertIn("11 秒后", message(event))
            self.assertIn("上游限流", message(event))
            self.assertEqual(event, before)
        self.assertEqual(_event_change(dict(route="source/a", to_route="source/a"), 26).plain, "同一部署")

    def test_limit_and_lower_weight_are_different_actions(self):
        limit = dict(kind="throttle", inband=True, header="10")
        lower = dict(kind="throttle", reason="429", penalty=.05, park_seconds=10)
        self.assertEqual(kind_label(limit), "上游限流")
        self.assertIn("HTTP 200", message(limit))
        self.assertIn("等待 10 秒", message(limit))
        self.assertEqual(kind_label(lower), "降低权重")
        self.assertIn("5%", message(lower))
        self.assertIn("10 秒", message(lower))

    def test_failed_and_streamed_responses_do_not_claim_success(self):
        event = dict(kind="response", status=429, seconds=12.1)
        self.assertIn("向客户端返回上游限流", message(event))
        event.update(status=200, stream=True, bytes=100)
        self.assertIn("数据流结束", message(event))
        self.assertNotIn("成功", message(event))
        self.assertIn("读取上游响应超时", message(dict(kind="timeout", timeout_type="ReadTimeout",
                                                       timeout_seconds=900, seconds=900.7)))

    def test_current_kinds_have_chinese_messages_and_legacy_has_no_special_case(self):
        for kind in KIND_LABELS:
            text = message(dict(kind=kind))
            self.assertTrue(any("\u4e00" <= c <= "\u9fff" for c in text), (kind, text))
        self.assertNotIn("stripped", KIND_LABELS)
        self.assertNotIn("unpinned", KIND_LABELS)

    def test_severity_labels_and_filters_stay_english(self):
        self.assertEqual(EVENT_FILTERS, ("DEBUG+", "INFO+", "WARNING+", "ERROR"))
        events = [dict(kind="throttle", level="warning", inband=True, header="10", seq=1),
                  dict(kind="timeout", level="error", timeout_type="ReadTimeout", timeout_seconds=900, seq=2)]
        for width in (60, 80, 120, 180):
            output = io.StringIO()
            console = Console(file=output, width=width, color_system=None)
            table, _ = render_events(events, width, 10, 0, 0, False)
            console.print(table)
            text = output.getvalue()
            self.assertIn("上游限流", text)
            self.assertIn("请求超时", text)
            self.assertNotIn("throttle", text)
            self.assertNotIn("ReadTimeout", text)
            if width >= 90:
                self.assertIn("WARN", text)
                self.assertIn("ERROR", text)


if __name__ == "__main__":
    unittest.main()
