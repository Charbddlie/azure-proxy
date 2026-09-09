"""Dashboard process status, freshness, and independent health polling."""

import copy
import io
import re
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, mock_open, patch

from rich.console import Console
from rich.cells import cell_len

from tui import theme
from tui.app import (INPUT_HZ, Dashboard, _InputReader, _footer, _mouse_tracking,
                     _processes, _proxy_status, _status_line, _tabs, _top_bar)
from tui.bars import FACES, capacity_bar, rpm_capacity, legend
from tui.boards import (BOARDS, MAX_CARD, render_groups, _source_card, _model_card, _row_pins,
                        _pinned_value, _route_rows, _usage_number, _card_widths, _share_number)
from tui.client import Poller
from tui.layout import truncate
from tui.snapshot import Snapshot, RouteView


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
        top = render(_top_bar(snapshot))
        self.assertIn("fetch 0s ago", top)
        self.assertIn("stats 24h ago  stale", top)

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
            for board in range(len(BOARDS)):
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
                    self.assertTrue(text.splitlines()[0].startswith("azure-proxy  127.0.0.1:8811"))
                    self.assertIn("serving  routing", "\n".join(text.splitlines()[:2]))
                    self.assertIn("fetch 0s ago", text.splitlines()[0])
                    self.assertEqual("serving online" in text, board == 3)
                    self.assertEqual("routing online" in text, board == 3)
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


class InteractionTests(unittest.TestCase):
    def dashboard(self, width=120, height=24):
        raw = healthy()
        raw["routes"]["routes"] = {
            "source/deployment-{:02d}".format(i): {"endpoint": "source"}
            for i in range(30)}
        raw["routes"]["models"] = {
            "gpt-5.6-{:02d}".format(i): [{"route": "source/deployment-{:02d}".format(i)}]
            for i in range(30)}
        poller = Mock(snapshot=lambda: raw)
        console = Console(file=io.StringIO(), width=width, height=height)
        dash = Dashboard(poller, console)
        dash.show_all = True
        return dash, raw

    def test_tabs_use_rendered_cell_coordinates_even_when_wrapped(self):
        for width in (24, 60, 120, 180):
            with self.subTest(width=width):
                dash, raw = self.dashboard(width=width, height=40)
                raw["history_notice"] = "历史提示 " * 30
                dash.render()
                self.assertEqual({region[3] for region in dash._tab_regions}, set(range(len(BOARDS))))
                dash.offset = [1, 2, 3, 4]
                for y, start, end, board in dash._tab_regions:
                    for x in (start, end - 1):
                        dash.key("\x1b[<0;{};{}M".format(x, y))
                        self.assertEqual(dash.board, board)
                self.assertEqual(dash.offset, [1, 2, 3, 4])
                previous = dash.board
                dash.key("\x1b[<0;1;1M")
                self.assertEqual(dash.board, previous)
                y, x, _, board = dash._tab_regions[0]
                for button, final in ((0, "m"), (2, "M"), (32, "M")):
                    dash.key("\x1b[<{};{};{}{}".format(button, x, y, final))
                    self.assertEqual(dash.board, previous)

    def test_wheel_scrolls_two_lines_and_reuses_cards(self):
        dash, _ = self.dashboard()
        with patch("tui.app.render_groups", wraps=render_groups) as build:
            dash.render()
            full_lines = dash._card_cache[0][1]
            self.assertGreater(len(full_lines), dash._body_height)
            dash.key("\x1b[<65;4;15M")
            frame = dash.render()
            self.assertEqual(dash.offset[0], 2)
            self.assertEqual(build.call_count, 1)
            lines = dash.console.render_lines(frame, dash.console.options.update(height=None))
            body_start = len(lines) - dash._body_height - len(
                dash.console.render_lines(_footer(dash, Snapshot(dash.poller.snapshot()),
                                                 dash._extent[0])))
            self.assertEqual(lines[body_start], full_lines[2])
            dash.key("\x1b[<64;4;15M")
            self.assertEqual(dash.offset[0], 0)
            dash._scroll(10000)
            self.assertEqual(dash.offset[0], len(full_lines) - dash._body_height)
            dash.render()
            self.assertEqual(build.call_count, 1)

    def test_cache_invalidates_on_data_layout_and_visibility_changes(self):
        dash, raw = self.dashboard()
        with patch("tui.app.render_groups", wraps=render_groups) as build:
            dash.render()
            raw["routes"] = copy.deepcopy(raw["routes"])
            dash.render()
            self.assertEqual(build.call_count, 1)
            for change in (lambda: raw.update(routes=dict(raw["routes"], updated_at=1001)),
                           lambda: setattr(dash.console, "width", 100),
                           lambda: setattr(dash.console, "height", 30),
                           lambda: dash._activate_button("toggle_legacy")):
                before = build.call_count
                change()
                dash.render()
                self.assertGreater(build.call_count, before)

    def test_scroll_clamps_after_data_shrinks_and_events_fill_last_page(self):
        dash, raw = self.dashboard()
        dash.render()
        dash._scroll(10000)
        raw["routes"] = dict(raw["routes"], routes={}, models={})
        dash.render()
        self.assertEqual(dash.offset[0], 0)
        raw["events"] = [dict(seq=i, kind="timeout", at=999) for i in range(50)]
        dash.board = 2
        dash.render()
        dash._scroll(10000)
        self.assertEqual(dash.offset[2], 50 - dash._body_height)
        raw["events"] = raw["events"][:2]
        dash.render()
        self.assertEqual(dash.offset[2], 0)

    def test_removed_keys_do_nothing_and_ctrl_c_quits(self):
        for board in range(len(BOARDS)):
            dash, raw = self.dashboard()
            raw["gap"] = dict(count=1, reasons=["limit"])
            dash.board = board
            dash.render()
            dash.offset, dash._extent = [5] * len(BOARDS), [100] * len(BOARDS)
            before = (dash.board, list(dash.offset), dash.filter, dash.kind_filter, dash.show_all, dash.quit)
            for key in ("\x1b[A", "\x1b[B", "\x1b[C", "\x1b[D", "\x1b[5~", "\x1b[6~",
                        "\x1b[H", "\x1b[F", "\t", " ", "h", "j", "k", "l", "g", "G",
                        "r", "q", "Q", "s", "a", "f", "t", "c"):
                dash.key(key)
                self.assertEqual((dash.board, dash.offset, dash.filter, dash.kind_filter, dash.show_all, dash.quit), before)
            dash.poller.refresh_now.assert_not_called()
            dash.poller.acknowledge_gap.assert_not_called()
            dash.key("\x03")
            self.assertTrue(dash.quit)

    def test_confirmation_only_on_events_with_pending_notice(self):
        for width in (60, 120, 180):
            for board in range(len(BOARDS)):
                for pending in (False, True):
                    with self.subTest(width=width, board=board, pending=pending):
                        dash, raw = self.dashboard(width=width)
                        dash.board = board
                        raw["gap"] = dict(count=1, reasons=["limit"]) if pending else None
                        dash.render()
                        footer = render(_footer(dash, Snapshot(raw), dash._extent[board]), width)
                        self.assertEqual("确认提示" in footer, board == 2 and pending)
                        for removed in ("↑", "↓", "←", "→", "刷新", "退出",
                                        "点击标签切换", "滚轮滚动"):
                            self.assertNotIn(removed, footer)
                        dash._activate_button("acknowledge_gap")
                        self.assertEqual(dash.poller.acknowledge_gap.call_count,
                                         int(board == 2 and pending))
                        dash._activate_button("acknowledge_gap")
                        self.assertLessEqual(dash.poller.acknowledge_gap.call_count, 1)

    def test_input_refresh_is_immediate_and_bursts_are_coalesced(self):
        dash, _ = self.dashboard()
        dash._extent[0] = 100
        now = [0.0]
        events = iter([(0.001, ["\x1b[<65;4;15M"]),
                       (0.005, ["\x1b[<65;4;15M"]),
                       (None, []), (0.04, ["\x03"])])
        updates = []

        def read(timeout):
            at, keys = next(events)
            now[0] = now[0] + timeout if at is None else at
            return keys

        with patch("tui.app.time.monotonic", side_effect=lambda: now[0]), \
                patch.object(_InputReader, "read", side_effect=read), \
                patch.object(dash, "render", return_value="frame"), \
                patch("tui.app.Live") as live:
            live.return_value.__enter__.return_value.update.side_effect = (
                lambda *args, **kwargs: updates.append((now[0], kwargs)))
            dash.run()
            self.assertFalse(live.call_args.kwargs["auto_refresh"])
        self.assertEqual(dash.offset[0], 4)
        self.assertEqual(len(updates), 2)
        self.assertEqual(updates[0], (0.001, {"refresh": True}))
        self.assertAlmostEqual(updates[1][0], 0.001 + 1 / INPUT_HZ)

    def test_render_time_is_included_in_the_frame_budget(self):
        dash, _ = self.dashboard()
        dash._extent[0] = 100
        now, starts = [0.0], []
        events = iter([(0.001, ["\x1b[<65;4;15M"]), (0.013, ["\x1b[<65;4;15M"]),
                       (None, []), (0.05, ["\x03"])])

        def read(timeout):
            at, keys = next(events)
            now[0] = now[0] + timeout if at is None else at
            return keys

        def render_frame():
            starts.append(now[0])
            if len(starts) > 1:
                now[0] += .01
            return "frame"

        with patch("tui.app.time.monotonic", side_effect=lambda: now[0]), \
                patch.object(_InputReader, "read", side_effect=read), \
                patch.object(dash, "render", side_effect=render_frame), patch("tui.app.Live"):
            dash.run()
        self.assertEqual(len(starts), 3)
        self.assertAlmostEqual(starts[2] - starts[1], 1 / INPUT_HZ)

    def test_event_scroll_only_renders_new_rows_and_reuses_unchanged_payloads(self):
        from tui.boards import render_events
        dash, raw = self.dashboard()
        dash.board = 2
        raw["events"] = [dict(seq=i, at=999, kind="response", status=200, seconds=1) for i in range(100)]
        with patch("tui.app.render_events", wraps=render_events) as build:
            dash.render()
            first = build.call_count
            self.assertEqual(first, dash._body_height)
            dash.key("\x1b[<65;4;15M")
            dash.render()
            self.assertEqual(build.call_count, first + dash.scroll_lines)
            dash.key("\x1b[<64;4;15M")
            raw["events"] = copy.deepcopy(raw["events"])
            dash.render()
            self.assertEqual(build.call_count, first + dash.scroll_lines)
            raw["events"][-1]["status"] = 201
            frame = dash.render()
            self.assertEqual(build.call_count, first + dash.scroll_lines + 1)
            lines = dash.console.render_lines(frame, dash.console.options.update(height=None))
            footer_height = len(dash.console.render_lines(_footer(dash, Snapshot(raw), dash._extent[2])))
            body_start = len(lines) - dash._body_height - footer_height
            expected, _ = render_events(raw["events"], dash.console.width, dash._body_height,
                                        0, dash.filter, False, dash.kind_filter)
            self.assertEqual(lines[body_start:body_start + dash._body_height],
                             dash.console.render_lines(expected, dash.console.options.update(height=dash._body_height)))
            before_resize = build.call_count
            dash.console.width = 140
            dash.render()
            self.assertGreater(build.call_count, before_resize)

    def test_event_row_cache_has_a_bound(self):
        dash, raw = self.dashboard()
        dash.board = 2
        raw["events"] = [dict(seq=i, at=999, kind="response", status=200) for i in range(100)]
        with patch("tui.app.EVENT_ROW_CACHE_SIZE", 4):
            dash.render()
            self.assertEqual(len(dash._event_rows), 4)


class TerminalInputTests(unittest.TestCase):
    def test_fragmented_and_combined_sequences(self):
        sequences = ["\x1b[<65;123;45M", "\x1b[<0;22;6M", "\x1b[<0;22;6m",
                     "\x1b[A", "\x1b[6~", "\x1bOA", "f", "\x03"]
        burst = "".join(sequences)
        self.assertEqual(_InputReader().feed(burst), sequences)
        reader = _InputReader()
        self.assertEqual([key for byte in burst for key in reader.feed(byte)], sequences)
        self.assertEqual(reader.buffer, "")

    def test_every_split_of_mouse_report_preserves_next_key(self):
        report = "\x1b[<0;22;6m"
        for split in range(len(report)):
            reader = _InputReader()
            self.assertEqual(reader.feed(report[:split]), [])
            self.assertEqual(reader.feed(report[split:] + "c"), [report, "c"])

    def test_mouse_modes_are_restored_on_error(self):
        output = io.StringIO()
        console = Console(file=output, force_terminal=True)
        with patch("tui.app.sys.stdin.isatty", return_value=True):
            with self.assertRaises(RuntimeError):
                with _mouse_tracking(console):
                    raise RuntimeError("render failed")
        self.assertEqual(output.getvalue(),
                         "\x1b[?1000h\x1b[?1006h\x1b[?1000l\x1b[?1006l")

    def test_redirected_output_does_not_enable_mouse_reporting(self):
        output = io.StringIO()
        with _mouse_tracking(Console(file=output)):
            pass
        self.assertEqual(output.getvalue(), "")


class CapacityBarTests(unittest.TestCase):
    def test_faces_restore_original_glyphs_and_shared_green(self):
        console = Console(file=io.StringIO())
        key = legend()
        expected = dict(chat="█", chat_stream="▓", responses="▒", responses_stream="▚", image="▞")
        self.assertEqual(theme.FACE_GLYPH, expected)
        style = console.get_style(theme.OURS)
        for face in FACES:
            glyph = expected[face]
            self.assertEqual(cell_len(glyph), 1)
            bar = capacity_bar(10, {face: 1})
            self.assertEqual(bar.plain, glyph * 10)
            self.assertEqual(bar.cell_len, 10)
            self.assertEqual(bar.get_style_at_offset(console, 0), style)
            self.assertIsNone(bar.get_style_at_offset(console, 0).bgcolor)
            offset = key.plain.index(theme.FACE_LABEL[face]) - 1
            self.assertEqual(key.get_style_at_offset(console, offset), style)
            self.assertIsNone(key.get_style_at_offset(console, offset + 1).bgcolor)
        self.assertEqual(len(set(theme.FACE_GLYPH.values())), len(FACES))
        self.assertNotIn(theme.FOREIGN_GLYPH, theme.FACE_GLYPH.values())

    def test_faces_do_not_change_the_terminal_background(self):
        console = Console(file=io.StringIO())
        for face in FACES:
            bar = capacity_bar(10, {face: .2}, .2)
            self.assertEqual(bar.plain, theme.FACE_GLYPH[face] * 2 + "·" * 6 + "░░")
            self.assertEqual(bar.get_style_at_offset(console, 1), console.get_style(theme.OURS))
            for offset in range(10):
                self.assertIsNone(bar.get_style_at_offset(console, offset).bgcolor)

    def test_streaming_block_emits_only_the_original_green(self):
        output = io.StringIO()
        console = Console(file=output, width=20, force_terminal=True,
                          color_system="truecolor", no_color=False)
        console.print(capacity_bar(4, {"chat_stream": 1}))
        ansi = output.getvalue()
        self.assertIn("38;2;127;160;138", ansi)
        self.assertNotIn("48;2;", ansi)
        self.assertIn("▓" * 4, ansi)
        self.assertIn("\x1b[0m", ansi)

    def test_others_recedes_from_left_and_keeps_right_edge(self):
        for foreign, free in ((.6, 2), (.4, 4), (.2, 6), (0, 8)):
            bar = capacity_bar(10, {"chat": .2}, foreign)
            self.assertEqual(bar.plain, "█" * 2 + "·" * free + "░" * round(foreign * 10))

    def test_width_and_segments_survive_edge_cases(self):
        for width in (0, 1, 2, 7, 20, 80):
            for faces, foreign in (({}, 0), ({}, .5), ({}, 1),
                                   ({"chat": .2}, -.5), ({"chat": 2}, 3),
                                   ({face: .001 for face in FACES}, .001)):
                with self.subTest(width=width, faces=faces, foreign=foreign):
                    bar = capacity_bar(width, faces, foreign)
                    self.assertEqual(bar.cell_len, width)
                    if theme.FOREIGN_GLYPH in bar.plain:
                        self.assertTrue(bar.plain.endswith(theme.FOREIGN_GLYPH))
                    if theme.FREE_GLYPH in bar.plain and theme.FOREIGN_GLYPH in bar.plain:
                        self.assertLess(bar.plain.rfind(theme.FREE_GLYPH),
                                        bar.plain.find(theme.FOREIGN_GLYPH))
            self.assertEqual(capacity_bar(width, None, .5).plain, theme.UNKNOWN_GLYPH * width)


class CompactStatusTests(unittest.TestCase):
    def test_top_row_uses_shared_process_status_colours(self):
        raw = healthy()
        for changes, colours in (({}, (theme.OK, theme.OK)),
                                 ({"health_error": "connection refused"}, (theme.CRIT, theme.WARN)),
                                 ({"health_age": 6}, (theme.WARN, theme.WARN))):
            snapshot = Snapshot(dict(raw, **changes))
            line = _status_line(snapshot)
            self.assertEqual(line.plain, "serving  routing")
            self.assertEqual(tuple(span.style for span in line.spans), colours)
        raw["health"]["routing"]["heartbeat_age_seconds"] = 10
        self.assertEqual(_status_line(Snapshot(raw)).spans[-1].style, theme.CRIT)

    def test_proxy_tab_holds_details_and_scrolls_independently(self):
        raw = healthy()
        raw["health"].update(supervisor=dict(ok=True, pid=303, active=101, draining=[]),
                              affinity_store=dict(ok=True, pending=0))
        raw["routes"]["session_affinity"] = dict(live_sessions=99)
        raw["history_notice"] = "历史提示 " * 40
        console = Console(file=io.StringIO(), width=80, height=14)
        dash = Dashboard(Mock(snapshot=lambda: raw), console)
        text = render(dash.render(), 80)
        self.assertTrue(text.splitlines()[0].startswith("azure-proxy  127.0.0.1:8811"))
        self.assertIn("stats", text.splitlines()[0])
        self.assertEqual(text.splitlines()[1].strip(), "")
        self.assertIn("proxy 状态", text.splitlines()[2])
        for detail in ("PID", "token", "管理进程", "历史提示", "endpoint-bound"):
            self.assertNotIn(detail, text)
        self.assertNotIn("endpoint-bound", _tabs(0, Snapshot(raw), False).plain)
        dash.board = 3
        text = render(dash.render(), 80)
        for detail in ("serving online", "routing online", "PID 101", "token", "管理进程"):
            self.assertIn(detail, text)
        self.assertGreater(dash._extent[3], 1)
        dash.key("\x1b[<65;5;8M")
        self.assertEqual(dash.offset, [0, 0, 0, 2])
        self.assertNotIn("activity", render(_footer(dash, Snapshot(raw), dash._extent[3]), 80))

    def test_buttons_are_clickable_at_bottom_left_and_freshness_at_top_right(self):
        for width in (60, 80, 120, 180):
            for board, actions in ((0, {"toggle_legacy"}), (1, {"toggle_legacy"}),
                                   (2, {"cycle_level", "cycle_kind", "acknowledge_gap"}), (3, set())):
                with self.subTest(width=width, board=board):
                    raw = healthy()
                    raw["gap"] = dict(count=2, reasons=["limit"])
                    poller = Mock(snapshot=lambda: raw)
                    console = Console(file=io.StringIO(), width=width, height=24)
                    dash = Dashboard(poller, console)
                    dash.board = board
                    frame = dash.render()
                    text = render(frame, width)
                    self.assertIn("fetch", text.splitlines()[0])
                    self.assertIn("stats", text.splitlines()[0])
                    self.assertNotIn("fetch", render(_footer(dash, Snapshot(raw), 1), width))
                    self.assertNotIn("排序", text)
                    self.assertEqual({r[3] for r in dash._button_regions}, actions)
                    self.assertFalse(text.splitlines()[-1].strip())
                    if actions:
                        self.assertEqual({r[0] for r in dash._button_regions}, {23})
                        self.assertEqual(min(r[1] for r in dash._button_regions), 1)
                    if board in (0, 1):
                        footer = text.splitlines()[-2]
                        self.assertNotIn("current", footer)
                        self.assertTrue(footer.startswith("[显示旧模型"))
                        self.assertIn("█chat", footer)
                        self.assertGreater(footer.index("█chat"), footer.index("]"))
                        self.assertEqual(cell_len(footer.rstrip()), width)
                    old_board, old_all = dash.board, dash.show_all
                    dash.key("\x1b[<0;{};24M".format(width - 3))
                    self.assertEqual((dash.board, dash.show_all), (old_board, old_all))
                    for action in actions:
                        y, x, _, _ = next(r for r in dash._button_regions if r[3] == action)
                        before = (dash.show_all, dash.filter, dash.kind_filter)
                        dash.key("\x1b[<0;{};{}M".format(x, y))
                        if action == "toggle_legacy":
                            self.assertNotEqual(dash.show_all, before[0])
                        elif action == "cycle_level":
                            self.assertNotEqual(dash.filter, before[1])
                        elif action == "cycle_kind":
                            self.assertNotEqual(dash.kind_filter, before[2])
                        else:
                            poller.acknowledge_gap.assert_called_once()

    def test_header_has_one_blank_row_before_clickable_tabs(self):
        raw = healthy()
        for width in (60, 80, 120, 180):
            console = Console(file=io.StringIO(), width=width, height=24)
            dash = Dashboard(Mock(snapshot=lambda: raw), console)
            dash.board = 2
            lines = render(dash.render(), width).splitlines()
            top_height = len(console.render_lines(_top_bar(Snapshot(raw), width),
                                                 console.options.update(height=None)))
            self.assertEqual(lines[top_height].strip(), "")
            self.assertEqual(min(r[0] for r in dash._tab_regions), top_height + 2)
            dash.key("\x1b[<0;1;{}M".format(top_height + 1))
            self.assertEqual(dash.board, 2)
            y, x, _, _ = next(r for r in dash._tab_regions if r[3] == 0)
            dash.key("\x1b[<0;{};{}M".format(x, y))
            self.assertEqual(dash.board, 0)


class SourceLayoutTests(unittest.TestCase):
    def test_card_widths_are_capped_and_fit_narrow_terminals(self):
        routes = {"source-{}/deployment".format(i): {} for i in range(6)}
        snapshot = Snapshot(dict(routes=dict(routes=routes)))
        self.assertEqual(_card_widths(300, snapshot, True), [74, 74, 73, 73])
        self.assertEqual(_card_widths(237, snapshot, True), [78, 78, 77])
        self.assertEqual(_card_widths(2000, snapshot, True), [93] * 6)
        self.assertEqual(_card_widths(137, snapshot, True), [68, 67])
        self.assertEqual(_card_widths(60, snapshot, True), [60])
        self.assertEqual(_card_widths(40, snapshot, True), [40])
        self.assertEqual(_card_widths(0, snapshot, True), [])
        self.assertEqual(_card_widths(300, Snapshot({}), True), [])

    def test_columns_are_added_as_soon_as_the_width_cap_is_exceeded(self):
        routes = {"yifanyang-foundry-img-polandcentral-{}/deployment".format(i): {}
                  for i in range(6)}
        snapshot = Snapshot(dict(routes=dict(routes=routes)))
        for width, expected in ((93, [93]), (94, [46, 46]),
                                (160, [79, 79]), (188, [93, 93]),
                                (189, [62, 62, 61]), (200, [66, 65, 65]),
                                (250, [82, 82, 82]), (283, [93, 93, 93]),
                                (284, [70, 70, 69, 69])):
            with self.subTest(width=width):
                self.assertEqual(_card_widths(width, snapshot, True), expected)

    def test_resizing_preserves_width_cap_and_monotonic_column_count(self):
        for count in (1, 2, 6):
            for name in ("source", "yifanyang-foundry-img-polandcentral", "源" * 50):
                routes = {"{}-{}/deployment".format(name, i): {} for i in range(count)}
                snapshot = Snapshot(dict(routes=dict(routes=routes)))
                previous_columns = 0
                for width in range(1, 1001):
                    with self.subTest(count=count, name=name, width=width):
                        widths = _card_widths(width, snapshot, True)
                        self.assertLessEqual(max(widths), MAX_CARD)
                        self.assertGreaterEqual(min(widths), 1)
                        self.assertLessEqual(max(widths) - min(widths), 1)
                        self.assertEqual(sum(widths) + 2 * (len(widths) - 1),
                                         min(width, count * MAX_CARD + 2 * (count - 1)))
                        self.assertLessEqual(len(widths), count)
                        self.assertGreaterEqual(len(widths), previous_columns)
                        previous_columns = len(widths)

    def test_rendered_cards_keep_the_width_cap_on_both_boards(self):
        routes = {"source-{}/deployment".format(i): {} for i in range(6)}
        models = {"model-{}".format(i): [dict(route=key)]
                  for i, key in enumerate(routes)}
        snapshot = Snapshot(dict(routes=dict(routes=routes, models=models)))
        for width in (94, 137, 160, 188, 189, 200, 250, 282, 283, 284, 340, 1000):
            for kind, groups in (("source", snapshot.sources), ("model", snapshot.models)):
                with self.subTest(width=width, kind=kind):
                    board, _ = render_groups(groups, width, 30, 0, "activity", snapshot, kind, True)
                    text = render(board, width)
                    borders = re.findall(r"╭[^╭╮\n]*╮", text)
                    self.assertEqual(len(borders), len(groups))
                    self.assertTrue(all(cell_len(border) <= MAX_CARD for border in borders))
                    self.assertTrue(all(cell_len(line) <= width for line in text.splitlines()))
                    self.assertEqual(max(cell_len(line.rstrip()) for line in text.splitlines()),
                                     min(width, len(groups) * MAX_CARD + 2 * (len(groups) - 1)))

    def test_model_cards_share_source_column_widths_even_with_more_models(self):
        endpoints = ("long-source-foundry-eastus2", "another-source-foundry-westus")
        routes, models = {}, {}
        for i in range(8):
            model = "gpt-5.6-{}".format(i)
            models[model] = []
            for endpoint in endpoints:
                key = "{}/deployment-{}".format(endpoint, i)
                routes[key] = dict(endpoint=endpoint)
                models[model].append(dict(route=key))
        snapshot = Snapshot(dict(routes=dict(routes=routes, models=models)))
        for width in (60, 120, 160, 240, 360):
            sources, _ = render_groups(snapshot.sources, width, 30, 0, "activity", snapshot, "source", True)
            with patch("tui.boards._model_card", wraps=_model_card) as cards:
                render_groups(snapshot.models, width, 30, 0, "activity", snapshot, "model", True)
            self.assertEqual([call.args[1] for call in cards.call_args_list],
                             [sources.widths[i % len(sources.widths)] for i in range(len(snapshot.models))])
            if width == 160:
                self.assertEqual(sources.widths, [79, 79])

    def test_long_source_stands_alone_and_short_sources_stack(self):
        routes, models = {}, {}
        for source, count in (("long", 12), ("short-a", 4), ("short-b", 3)):
            for i in range(count):
                key = "{}/deployment-{}".format(source, i)
                routes[key] = dict(endpoint=source, capacity_rpm=10)
                models.setdefault("gpt-5.6-{}".format(i), []).append(dict(route=key))
        snapshot = Snapshot(dict(routes=dict(routes=routes, models=models)))
        for width in (160, 180):
            console = Console(file=io.StringIO(), width=width, height=40)
            board, _ = render_groups(snapshot.sources, width, 36, 0, "activity", snapshot, "source", True)
            lines = console.render_lines(board, console.options.update(height=None))
            text = render(board, width)
            rows = text.splitlines()
            positions = {name: next((i, row.index(name)) for i, row in enumerate(rows) if name in row)
                         for name in ("long", "short-a", "short-b")}
            self.assertEqual(positions["long"][0], 0)
            self.assertGreater(positions["short-a"][1], positions["long"][1])
            self.assertEqual(positions["short-a"][1], positions["short-b"][1])
            self.assertGreater(positions["short-b"][0], positions["short-a"][0])
            self.assertEqual(len(lines), 28)


class PinnedCardTests(unittest.TestCase):
    def test_status_labels_the_configured_active_window(self):
        snapshot = self.snapshot()
        snapshot.affinity.update(active_window_seconds=300, retained_sessions=99)
        text = render(_proxy_status(snapshot), 180)
        self.assertIn("活跃会话绑定  9 pinned", text)
        self.assertIn("近 5 分钟，含进行中的请求", text)
        self.assertNotIn("99 pinned", text)
        snapshot.affinity["active_window_seconds"] = 30
        self.assertIn("近 30 秒", render(_proxy_status(snapshot), 180))

    def snapshot(self):
        return Snapshot({"routes": {
            "routes": {key: dict(capacity_rpm=10, model_version="2026-07-09") for key in (
                "alpha/sol-a", "alpha/sol-b", "alpha/terra", "beta/sol")},
            "models": {"gpt-5.6-sol": [dict(route=key) for key in (
                "alpha/sol-a", "alpha/sol-b", "beta/sol")],
                "gpt-5.6-terra": [dict(route="alpha/terra")]},
            "model_faces": {"gpt-5.6-sol": ["chat", "responses", "image"]},
            "session_affinity": dict(mode="endpoint", model_tracking=True,
                live_sessions=9, sessions_per_endpoint=dict(alpha=7, beta=2),
                sessions_per_model={"gpt-5.6-sol": dict(total=5, endpoints=dict(alpha=3, beta=2)),
                                    "gpt-5.6-terra": dict(total=4, endpoints=dict(alpha=4))})}})

    def test_each_deployment_shows_shared_pins_without_inflating_card_totals(self):
        snapshot = self.snapshot()
        source = next(g for g in snapshot.sources if g.name == "alpha")
        model = next(g for g in snapshot.models if g.name == "gpt-5.6-sol")
        self.assertEqual(_row_pins(source.routes, snapshot),
                         {"alpha/sol-a": 3, "alpha/sol-b": 3, "alpha/terra": 4})
        self.assertEqual(_row_pins(model.routes, snapshot, model.name),
                         {"alpha/sol-a": 3, "alpha/sol-b": 3, "beta/sol": 2})
        for width in (46, 80, 120):
            for detail in (False, True):
                with self.subTest(width=width, detail=detail):
                    source_text = render(_source_card(source, width, detail, 7, snapshot, True), width)
                    model_text = render(_model_card(model, width, detail, snapshot), width)
                    self.assertIn("7 pinned", source_text.splitlines()[1])
                    self.assertIn("5 pinned", model_text.splitlines()[1])
                    self.assertRegex(source_text, r"sol-a\s+3\s")
                    self.assertRegex(source_text, r"sol-b\s+3\s")
                    self.assertRegex(source_text, r"gpt-5.6-terra\s+4\s")
                    self.assertRegex(model_text, r"alpha·sol-a\s+3\s")
                    self.assertRegex(model_text, r"alpha·sol-b\s+3\s")
                    self.assertRegex(model_text, r"beta\s+2\s")
                    for text in (source_text, model_text):
                        self.assertEqual(text.count("MAX RPM: "), 1)
                        self.assertNotIn("MAX RPM:", text.splitlines()[0])
                        self.assertIn("MAX RPM:", text.splitlines()[1])
                        self.assertNotIn("pinned", text.splitlines()[0])
                        self.assertNotIn("pinned", "\n".join(text.splitlines()[2:]))
                        self.assertNotIn("/3", text)
                        for hidden in ("deployments", "个源", "在用", "chat", "responses", "2026-07-09"):
                            self.assertNotIn(hidden, text)

    def test_missing_breakdown_is_unknown_and_complete_zero_is_zero(self):
        snapshot = self.snapshot()
        self.assertEqual(snapshot.pinned("gpt-5.6-sol", "alpha"), 3)
        self.assertEqual(snapshot.pinned("gpt-5.6-sol", "absent"), 0)
        self.assertEqual(snapshot.pinned("absent"), 0)
        snapshot.affinity = dict(mode="endpoint", sessions_per_endpoint=dict(alpha=7))
        self.assertIsNone(snapshot.pinned("gpt-5.6-sol", "alpha"))
        self.assertIsNone(snapshot.pinned("gpt-5.6-sol"))
        card = _model_card(snapshot.models[0], 80, False, snapshot)
        text = render(card, 80)
        self.assertIn("· pinned", text.splitlines()[1])
        self.assertRegex(text, r"alpha·sol-a\s+·\s")
        self.assertNotIn("?", text)

    def test_maximum_label_distinguishes_the_changing_value(self):
        for value in (2.5, 20, 12000):
            label = rpm_capacity(value)
            self.assertTrue(label.plain.startswith("MAX RPM: "))
            self.assertEqual(label.style, theme.DIM)
            self.assertEqual(label.spans[-1].style, theme.ACCENT)
        self.assertEqual(rpm_capacity(None).plain, "MAX RPM: —")
        pinned = _pinned_value(12)
        self.assertEqual(pinned.plain, "12")
        self.assertNotEqual(pinned.style, rpm_capacity(10).spans[-1].style)
        pinned_rgb = tuple(int(theme.PINNED[i:i + 2], 16) for i in (1, 3, 5))
        rpm_rgb = tuple(int(theme.ACCENT[i:i + 2], 16) for i in (1, 3, 5))
        self.assertTrue(all(p > r for p, r in zip(pinned_rgb, rpm_rgb)))
        self.assertGreater(pinned_rgb[2], pinned_rgb[0])

    def test_cards_omit_demotion_state(self):
        snapshot = self.snapshot()
        snapshot._views["alpha/sol-a"].data["penalty"] = .5
        snapshot._views["alpha/sol-b"].data["parked_for_seconds"] = 30
        for width in (46, 64, 80, 93):
            for card in (_source_card(snapshot.sources[0], width, False, 7, snapshot, True),
                         _model_card(snapshot.models[0], width, False, snapshot)):
                text = render(card, width)
                self.assertNotIn("降权", text)
                self.assertNotIn("demoted", text)
                self.assertIn("pinned", text.splitlines()[1])
                self.assertTrue(all(cell_len(line) == width for line in text.splitlines()))

    def test_zero_and_missing_pins_use_dots_in_every_row_and_summary(self):
        snapshot = self.snapshot()
        for count in (0, None):
            self.assertEqual(_pinned_value(count).plain, "·")
            snapshot.affinity = dict(model_tracking=True, sessions_per_model={}) if count == 0 else {}
            for card in (_source_card(snapshot.sources[0], 80, False, count, snapshot, True),
                         _model_card(snapshot.models[0], 80, False, snapshot)):
                text = render(card, 80)
                self.assertIn("· pinned", text.splitlines()[1])
                self.assertNotIn("0 pinned", text)
                self.assertNotIn("?", text)
                self.assertRegex(text, r"sol-a\s+·\s")
                self.assertRegex(text, r"sol-b\s+·\s")

    def test_rows_without_a_pinned_mapping_still_show_placeholders(self):
        snapshot = self.snapshot()
        routes = snapshot.models[0].routes
        table = _route_rows(routes, 80, {r.key: r.key for r in routes}, {}, False)
        output = io.StringIO()
        console = Console(file=output, width=80, color_system=None)
        lines = console.render_lines(table, console.options.update(height=None))
        for line in lines[::2]:
            pins = [segment.text for segment in line if segment.style and
                    segment.style.color == console.get_style(theme.PINNED).color]
            self.assertEqual("".join(pins).strip(), "·")

    def test_card_and_row_rpm_values_have_the_same_right_edge(self):
        snapshot = self.snapshot()
        for width in (46, 80, 120):
            for card in (_source_card(snapshot.sources[0], width, False, 7, snapshot, True),
                         _model_card(snapshot.models[0], width, False, snapshot)):
                lines = render(card, width).splitlines()
                self.assertEqual(card.title_align, "center")
                self.assertNotIn("MAX RPM:", lines[0])
                maximum_edge = cell_len(lines[1].rstrip(" │"))
                self.assertIn("MAX RPM:", lines[1])
                self.assertTrue(all(cell_len(line.rstrip(" │")) == maximum_edge
                                    for line in lines[2:-1] if line.strip(" │")))

    def test_last_entry_has_one_blank_line_before_the_bottom_border(self):
        snapshot = self.snapshot()
        for width in (46, 80, 120):
            for card in (_source_card(snapshot.sources[0], width, False, 7, snapshot, True),
                         _model_card(snapshot.models[0], width, False, snapshot)):
                lines = render(card, width).splitlines()
                self.assertEqual(lines[-2], "│" + " " * (width - 2) + "│")
                self.assertTrue(lines[-3].strip(" │"))
                self.assertTrue(lines[-1].startswith("╰"))

    def test_every_card_content_row_has_two_cells_of_side_padding(self):
        snapshot = self.snapshot()
        for width in (46, 80, 120):
            for card in (_source_card(snapshot.sources[0], width, False, 7, snapshot, True),
                         _model_card(snapshot.models[0], width, False, snapshot)):
                for line in render(card, width).splitlines()[1:-1]:
                    self.assertTrue(line.startswith("│  "), line)
                    self.assertTrue(line.endswith("  │"), line)
                    self.assertEqual(cell_len(line), width)

    def test_pinned_hidden_and_maximum_share_the_row_below_title(self):
        snapshot = self.snapshot()
        source = snapshot.sources[0]
        old = copy.deepcopy(source.routes[0])
        old.model, old.key = "gpt-4", "alpha/old"
        source.routes.append(old)
        for width in (46, 80, 120):
            card = _source_card(source, width, False, 7, snapshot, False)
            lines = render(card, width).splitlines()
            for value in ("7 pinned", "隐藏 1 旧", "MAX RPM: 40.0"):
                self.assertIn(value, lines[1])
                self.assertNotIn(value, lines[0])
            self.assertEqual(card.title_align, "center")

    def test_pinned_and_usage_columns_align_in_a_single_row(self):
        names = ("a", "long-model-name", "模型")
        pins = dict(zip(names, (7, 42, 300)))
        routes = [RouteView(name, name, None, dict(capacity_rpm=100, current_rpm=23,
                    other_rpm=47, rpm_by_face=dict(chat=23))) for name in names]
        for width in (42, 64, 96):
            body = _route_rows(routes, width, {name: name for name in names}, pins, True)
            lines = render(body, width).splitlines()
            self.assertEqual(len(body.columns), 6)
            self.assertEqual(len(lines), 5)
            self.assertTrue(all(not line.strip() for line in lines[1::2]))
            pin_edges, bar_edges = [], []
            for row, route in enumerate(Snapshot.sorted_routes(routes)):
                main = lines[row * 2]
                count = re.search(r"\b{}\b".format(pins[route.key]), main)
                self.assertIsNotNone(count)
                pin_edges.append(cell_len(main[:count.end()]))
                bar = re.search(r"[█▓▒▚▞░·]+", main)
                self.assertIsNotNone(bar)
                bar_edges.append((cell_len(main[:bar.start()]), cell_len(main[:bar.end()])))
                self.assertTrue(main[:bar.start()].endswith("23 "))
                self.assertTrue(main[bar.end():].startswith(" 47"))
                self.assertNotIn("ours", main)
                self.assertNotIn("others", main)
                self.assertLessEqual(cell_len(main), width)
            self.assertEqual(len(set(pin_edges)), 1)
            self.assertEqual(len(set(bar_edges)), 1)

    def test_single_row_usage_handles_unknown_ceiling_and_large_values(self):
        for capacity in (None, 100):
            for ours, others in ((0, 0), (12.5, 7.5), (12345, 999999), (999.5, 987.6)):
                route = RouteView("source/model", "model", None,
                                  dict(capacity_rpm=capacity, current_rpm=ours, other_rpm=others))
                for width in (42, 64, 96):
                    for detail in (False, True):
                        body = _route_rows([route], width, {route.key: "model"}, {}, detail)
                        lines = render(body, width).splitlines()
                        self.assertEqual(len(lines), 1)
                        self.assertLessEqual(cell_len(lines[0]), width)
                        self.assertNotIn("尚无完成样本", lines[0])
                        self.assertNotIn("ours", lines[0])
                        self.assertNotIn("others", lines[0])
                        self.assertIn(_usage_number(ours), lines[0])
                        self.assertIn(_usage_number(others), lines[0])


class RouteProbabilityTests(unittest.TestCase):
    def test_probability_colour_is_independent_of_demotion(self):
        route = RouteView("source/deploy", "model", .46, {})
        console = Console(file=io.StringIO(), width=64)
        for state in ({}, dict(penalty=.25), dict(parked_for_seconds=30)):
            route.data = state
            table = _route_rows([route], 64, {route.key: "deploy"}, {}, False, show_share=True)
            segments = console.render_lines(table, console.options.update(height=None))[0]
            probability = next(segment for segment in segments if segment.text == ".46")
            self.assertEqual(probability.style.color, console.get_style(theme.ACCENT).color)

    def test_compact_probability_format(self):
        for value, expected in ((None, "·"), (0, "0"), (1, "1"), (.5, ".5"),
                                (.25, ".25"), (.46, ".46"), (1 / 3, ".33"),
                                (2 / 3, ".67"), (.4567, ".46"), (.004, "0")):
            with self.subTest(value=value):
                self.assertEqual(_share_number(value), expected)

    def test_model_card_uses_reported_probability_as_first_column(self):
        keys = ("alpha/deploy-a", "beta/deploy-b", "gamma/deploy-c")
        snapshot = Snapshot({"routes": {
            "routes": {key: dict(capacity_rpm=100, current_rpm=rpm)
                       for key, rpm in zip(keys, (7, 23, 1))},
            "models": {"model": [dict(route=key, share=share)
                                  for key, share in zip(keys, (.4567, .5433, 0))]}}})
        for width in (40, 46, 64, 93):
            for detail in (False, True):
                with self.subTest(width=width, detail=detail):
                    text = render(_model_card(snapshot.models[0], width, detail, snapshot), width)
                    self.assertTrue(text.splitlines()[1].startswith("│  route prob.  "))
                    self.assertEqual(text.count("route prob."), 1)
                    for probability, endpoint in ((".46", "alpha"), (".54", "beta"), ("0", "gamma")):
                        self.assertRegex(text, r"(?m)^│\s+{}\s+{}\s".format(
                            re.escape(probability), endpoint))
                    self.assertNotIn("0.46", text)
                    self.assertTrue(all(cell_len(line) == width for line in text.splitlines()))
        source = render(_source_card(snapshot.sources[0], 64, False, None, snapshot, True), 64)
        self.assertRegex(source, r"(?m)^│\s+model\s")
        self.assertNotIn(".46", source)
        self.assertNotIn("route prob.", source)

    def test_probability_column_preserves_row_spacing_and_alignment(self):
        routes = [RouteView(key, "model", share, dict(capacity_rpm=100, current_rpm=23,
                           other_rpm=47, rpm_by_face=dict(chat=23)))
                  for key, share in (("a/deploy", .46), ("long-name/deploy", 1), ("模型/deploy", None))]
        for width in (34, 42, 64, 96):
            table = _route_rows(routes, width, {r.key: r.endpoint for r in routes},
                                {r.key: 7 for r in routes}, False, show_share=True)
            lines = render(table, width).splitlines()
            self.assertEqual(len(table.columns), 7)
            self.assertEqual(len(lines), 5)
            self.assertTrue(all(not line.strip() for line in lines[1::2]))
            edges = []
            for line, route in zip(lines[::2], Snapshot.sorted_routes(routes)):
                self.assertTrue(line.lstrip().startswith(_share_number(route.share) + " "))
                pin = re.search(r"\b7\b", line)
                edges.append(cell_len(line[:pin.end()]))
                self.assertIn("23", line)
                self.assertIn("47", line)
                self.assertLessEqual(cell_len(line), width)
            self.assertEqual(len(set(edges)), 1)

    def test_middle_ellipsis_keeps_both_ends_and_fits_terminal_cells(self):
        for text, width, expected in (("abcdef", 6, "abcdef"), ("abcdef", 5, "ab…ef"),
                                      ("abcdef", 4, "ab…f"), ("abcdef", 1, "…"),
                                      ("abcdef", 0, ""), ("abcdef", -1, ""),
                                      ("模型部署名称", 7, "模…名称")):
            with self.subTest(text=text, width=width):
                self.assertEqual(truncate(text, width, middle=True), expected)
        self.assertEqual(truncate("abcdef", 4), "abc…")
        for name in ("yifanyang-foundry-resource·gpt-5.6-sol-deploy-west",
                     "模型部署名称" * 10):
            for width in range(1, 80):
                self.assertLessEqual(cell_len(truncate(name, width, middle=True)), width)

    def test_long_deployment_labels_use_middle_ellipsis(self):
        endpoint = "yifanyang-foundry-resource"
        keys = [endpoint + "/gpt-5.6-sol-deploy-" + suffix for suffix in ("east", "west")]
        snapshot = Snapshot({"routes": {
            "routes": {key: dict(capacity_rpm=100) for key in keys},
            "models": {"model": [dict(route=key, share=.5) for key in keys]}}})
        for width in (46, 64, 93):
            text = render(_model_card(snapshot.models[0], width, False, snapshot), width)
            for suffix in ("east", "west"):
                self.assertRegex(text, r"\.5\s+yifa\S*…\S*{}\s".format(suffix))
            self.assertTrue(all(cell_len(line) == width for line in text.splitlines()))


class ScrollConfigurationTests(unittest.TestCase):
    def test_default_and_configured_sensitivity_apply_to_both_directions(self):
        for kwargs, step in (({}, 2), ({"scroll_lines": 5}, 5)):
            dash = Dashboard(None, Console(file=io.StringIO()), **kwargs)
            dash._extent[0] = 100
            dash.key("\x1b[<65;10;10M")
            self.assertEqual(dash.offset[0], step)
            dash.key("\x1b[<64;10;10M")
            self.assertEqual(dash.offset[0], 0)
            dash.offset[0] = 98
            dash.key("\x1b[<65;10;10M")
            self.assertEqual(dash.offset[0], 99)

    def test_dashboard_rejects_invalid_scroll_steps(self):
        for value in (0, -1, True, 1.5, "2", None):
            with self.assertRaises(ValueError):
                Dashboard(None, Console(file=io.StringIO()), scroll_lines=value)

    def test_policy_scroll_configuration_and_defaults(self):
        import argparse
        from tui.__main__ import _default_scroll_lines
        for policy, expected in (("{}", 2), ("tui: null", 2),
                                 ("tui:\n  scroll_lines: 4", 4),
                                 ("tui:\n  scroll_lines: '3'", 3)):
            with patch("builtins.open", mock_open(read_data=policy)):
                self.assertEqual(_default_scroll_lines(), expected)
        for policy in ("tui:\n  scroll_lines: 0", "tui:\n  scroll_lines: -1",
                       "tui:\n  scroll_lines: 1.5", "tui:\n  scroll_lines: true", "tui: []"):
            with patch("builtins.open", mock_open(read_data=policy)):
                with self.assertRaises(argparse.ArgumentTypeError):
                    _default_scroll_lines()
        with patch("builtins.open", side_effect=FileNotFoundError):
            self.assertEqual(_default_scroll_lines(), 2)

    def test_command_line_override_takes_priority(self):
        from tui.__main__ import main
        with patch("tui.__main__._default_url", return_value="http://127.0.0.1:8811"), \
                patch("tui.__main__._default_scroll_lines", return_value=3) as default, \
                patch("tui.__main__.run") as run:
            self.assertEqual(main([]), 0)
            self.assertEqual(run.call_args.kwargs["scroll_lines"], 3)
            default.reset_mock()
            self.assertEqual(main(["--scroll-lines", "6"]), 0)
            self.assertEqual(run.call_args.kwargs["scroll_lines"], 6)
            default.assert_not_called()

    def test_invalid_command_line_step_fails_before_starting_the_dashboard(self):
        from tui.__main__ import main
        with patch("tui.__main__.run") as run, patch("sys.stderr", io.StringIO()):
            for value in ("0", "-1", "1.5", "bad"):
                with self.assertRaises(SystemExit) as error:
                    main(["--scroll-lines", value])
                self.assertEqual(error.exception.code, 2)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
