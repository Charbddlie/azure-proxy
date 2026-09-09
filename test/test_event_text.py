"""Event names and explanations are Chinese; severity codes stay unchanged."""

import copy
import io
import unittest

from rich.console import Console
from rich.cells import cell_len
from tui.boards import EVENT_FILTERS, render_events, _event_change, _event_context
from tui.event_text import KIND_LABELS, kind_label, message


class EventTextTests(unittest.TestCase):
    def test_empty_event_view_has_no_keyboard_instructions(self):
        empty, count = render_events([], 120, 10, 0, 1, False)
        self.assertEqual(count, 0)
        self.assertEqual(empty.plain, "没有匹配事件")

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

    def test_limit_description_is_independent_of_demotion_fields(self):
        limit = dict(kind="throttle", inband=True, header="10")
        lower = dict(kind="throttle", reason="429", penalty=.05, park_seconds=10)
        self.assertEqual(kind_label(limit), "上游限流")
        self.assertIn("HTTP 200", message(limit))
        self.assertIn("等待 10 秒", message(limit))
        self.assertEqual(kind_label(lower), "上游限流")
        self.assertEqual(message(lower), "上游限流")
        self.assertEqual(_event_change(lower, 100).plain, "")

    def test_demotion_events_preserve_failure_reason_without_weight_claims(self):
        for reason, expected in (("503", "上游返回 HTTP 503"),
                                 ("transport: ConnectError", "上游连接异常"),
                                 (None, "上游请求异常")):
            event = dict(kind="demote", reason=reason, penalty=.25, park_seconds=30)
            self.assertEqual(kind_label(event), "上游异常")
            self.assertEqual(message(event), expected)
            self.assertEqual(_event_change(event, 100).plain, "")

    def test_demotion_metadata_preserves_warning_events_and_foreign_usage(self):
        events = [dict(kind="throttle", level="warning", reason="429", seq=1,
                       penalty=.25, park_seconds=30, foreign_updated=True,
                       other_rpm=12, capacity_rpm=100),
                  dict(kind="demote", level="warning", reason="503", seq=2,
                       penalty=.25, park_seconds=30)]
        for width in (80, 120, 180):
            output = io.StringIO()
            table, count = render_events(events, width, 10, 0, 2, True)
            Console(file=output, width=width, color_system=None).print(table)
            text = output.getvalue()
            self.assertEqual(count, 2)
            self.assertIn("上游限流", text)
            self.assertIn("上游异常", text)
            for removed in ("降权", "降低权重", "权重系数", "×0.25"):
                self.assertNotIn(removed, text)
        self.assertEqual(_event_change(events[0], 100).plain, "外部 12.0 RPM / 最大 100.0")

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

    def test_request_context_replaces_repeated_model_and_target_columns(self):
        target = "yifanyang-foundry-eastus2/gpt-6-astra-2026-07-09"
        event = dict(kind="failover", route="old-source/gpt-6-astra", to_route=target,
                     model="gpt-6-astra", reason="429", wait_seconds=1)
        for width in (120, 160, 180):
            with self.subTest(width=width):
                output = io.StringIO()
                table, _ = render_events([event], width, 5, 0, 0, False)
                Console(file=output, width=width, color_system=None).print(table)
                text = output.getvalue()
                self.assertEqual(len(table.columns), 6)
                self.assertIn("→ " + target, text)
                self.assertEqual(text.count("gpt-6-astra"), 1)
                self.assertNotIn("old-source", text)
                self.assertLess(text.index(target), text.index("上游限流"))
                self.assertEqual(len(text.splitlines()), 1)
                self.assertLessEqual(cell_len(text.rstrip("\n")), width)

    def test_context_preserves_same_route_and_non_retry_information(self):
        cases = [
            (dict(kind="failover", route="source/gpt-5.5", to_route="source/gpt-5.5"),
             "↻ source/gpt-5.5"),
            (dict(kind="response", route="source/gpt-5.5", model="gpt-5.5-alias"),
             "source/gpt-5.5"),
            (dict(kind="request", model="gpt-5.5"), "gpt-5.5"),
            (dict(kind="pin", endpoint="source", model="gpt-5.5"), "source"),
            (dict(kind="demote", route="source/gpt-5.5", park_seconds=18, penalty=.25),
             "source/gpt-5.5"),
            (dict(kind="foreign", route="source/gpt-5.5", other_rpm=12, capacity_rpm=100),
             "source/gpt-5.5 · 外部 12.0 RPM / 最大 100.0"),
            (dict(kind="boot"), ""),
        ]
        for event, expected in cases:
            with self.subTest(event=event):
                before = copy.deepcopy(event)
                self.assertEqual(_event_context(event, 100).plain, expected)
                self.assertEqual(event, before)

    def test_context_width_is_capped_and_remains_visible_on_narrow_screens(self):
        event = dict(kind="failover", route="old/gpt-5.5", to_route="new/gpt-5.5",
                     reason="429", wait_seconds=1)
        widths = []
        for width in (60, 80, 120, 180):
            output = io.StringIO()
            table, _ = render_events([event], width, 5, 0, 0, False)
            Console(file=output, width=width, color_system=None).print(table)
            widths.append(table.columns[4].width)
            self.assertIn("→ new/gpt-5.5", output.getvalue())
            other, _ = render_events([dict(event, to_route="x" * 150)], width, 5, 0, 0, False)
            self.assertEqual(other.columns[4].width, table.columns[4].width)
        self.assertEqual(widths, sorted(widths))
        self.assertEqual(widths[-1], 52)
        long_context = dict(event, to_route="source/" + "模型" * 100)
        output = io.StringIO()
        table, _ = render_events([long_context], 120, 5, 0, 0, False)
        Console(file=output, width=120, color_system=None).print(table)
        self.assertIn("…", output.getvalue())
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertLessEqual(cell_len(output.getvalue().rstrip("\n")), 120)

    def test_wide_terminals_keep_the_explanation_close_to_the_route(self):
        event = dict(kind="response", route="gpt4v-scus/gpt-5.5", status=200, seconds=2)
        starts = []
        for width in (120, 180, 240):
            output = io.StringIO()
            table, _ = render_events([event], width, 5, 0, 0, False)
            Console(file=output, width=width, color_system=None).print(table)
            line = output.getvalue().splitlines()[0]
            starts.append(cell_len(line[:line.index("请求返回")]))
            self.assertEqual(table.columns[4].width, 52)
        self.assertEqual(len(set(starts)), 1)
        self.assertLess(starts[0], 84)


if __name__ == "__main__":
    unittest.main()
