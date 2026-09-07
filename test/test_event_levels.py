"""Severity policy, terminal filters, and timeout/rate-limit distinctions."""

import asyncio
import io
import logging
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from run_tests import (Behaviour, DEPLOYMENT, FakeAzure, MODEL, Proxy, ROOT,
                       _inline_telemetry, ask, serving_target)

sys.path.insert(0, ROOT)
import httpx
from rich.console import Console
from proxy.bridge import ServingBridge
from proxy.events import EVENT_LEVELS, KINDS, event_level
from routing.engine import Engine
from tui.app import Dashboard, _tabs
from tui.boards import (DEFAULT_EVENT_FILTER, EVENT_FILTERS, _event_message,
                        filter_events, render_events)
from tui.snapshot import Snapshot
from tui import theme


EVENTS = [
    dict(kind="request", level="info", message="request-debug", seq=1),
    dict(kind="capacity", level="info", message="learned-capacity", seq=2),
    dict(kind="foreign", level="info", message="outside-load", seq=3),
    dict(kind="throttle", level="info", message="endpoint-ratelimit", seq=4),
    dict(kind="timeout", level="warning", message="read-timeout", seq=5),
    dict(kind="response", level="info", status=200, message="completed-request", seq=6),
    dict(kind="exhausted", level="error", message="no-routes-left", seq=7),
]


class EventLevelTests(unittest.TestCase):
    def test_every_event_kind_has_a_default_level(self):
        self.assertEqual(set(EVENT_LEVELS), set(KINDS))
        self.assertEqual(event_level("throttle", "info"), "warning")
        self.assertEqual(event_level("timeout", "warning"), "error")
        self.assertEqual(event_level("request", "info"), "debug")

    def test_status_and_credential_failures_adjust_severity(self):
        for status, expected in ((200, "info"), (400, "warning"), (429, "warning"),
                                 (500, "error"), (504, "error")):
            self.assertEqual(event_level("response", "info", {"status": status}), expected)
        self.assertEqual(event_level("response", "warning", {"broke": True}), "error")
        self.assertEqual(event_level("token", "info", {"ok": False}), "error")
        self.assertEqual(event_level("token", "warning", {"ok": False, "expires_in_seconds": 60}),
                         "warning")
        self.assertEqual(event_level("upstream_error", "warning", {"error_code": "rate_limit_exceeded"}),
                         "warning")
        self.assertEqual(event_level("upstream_error", "warning", {"error_code": "truncated"}), "error")

    def test_default_threshold_filters_legacy_events_without_mutating_them(self):
        selected = filter_events(EVENTS, DEFAULT_EVENT_FILTER)
        self.assertEqual([e["seq"] for e in selected], [2, 3, 4, 5, 6, 7])
        self.assertEqual(EVENTS[3]["level"], "info")
        self.assertEqual(EVENTS[4]["level"], "warning")
        self.assertEqual(len(filter_events(EVENTS, 0)), len(EVENTS))
        self.assertEqual([e["seq"] for e in filter_events(EVENTS, 3)], [5, 7])
        self.assertEqual([e["seq"] for e in filter_events(EVENTS, 1, 2)], [3])
        self.assertEqual([e["seq"] for e in filter_events(EVENTS, DEFAULT_EVENT_FILTER, 2)], [3])

    def test_buttons_cycle_levels_and_types_independently(self):
        dashboard = Dashboard(None, Console(file=io.StringIO()))
        dashboard.board = 2
        self.assertEqual(EVENT_FILTERS[dashboard.filter], "INFO+")
        dashboard.offset[2] = 99
        dashboard._activate_button("cycle_level")
        self.assertEqual(EVENT_FILTERS[dashboard.filter], "WARNING+")
        self.assertEqual(dashboard.offset[2], 0)
        for _ in range(3):
            dashboard._activate_button("cycle_level")
        self.assertEqual(dashboard.filter, DEFAULT_EVENT_FILTER)
        dashboard._activate_button("cycle_kind")
        self.assertEqual(dashboard.kind_filter, 1)
        self.assertEqual(dashboard.filter, DEFAULT_EVENT_FILTER)

    def test_render_exposes_levels_and_filtered_count_at_different_widths(self):
        self.assertEqual(_event_message(EVENTS[3]).style, theme.WARN)
        self.assertEqual(_event_message(EVENTS[4]).style, theme.CRIT)
        for width in (60, 120):
            output = io.StringIO()
            console = Console(file=output, width=width, color_system=None)
            table, count = render_events(EVENTS, width, 10, 0, DEFAULT_EVENT_FILTER, False)
            console.print(table)
            text = output.getvalue()
            self.assertEqual(count, 6)
            self.assertIn("请求超时", text)
            self.assertNotIn("learned-capacity", text)
            if width == 120:
                self.assertIn("WARN", text)
                self.assertIn("ERROR", text)
        self.assertIn("6/7", _tabs(2, Snapshot({"events": EVENTS}), False).plain)

    def test_server_records_and_logs_the_same_canonical_level(self):
        from proxy import server
        bridge = ServingBridge("unused", None)
        with patch.object(server, "bridge", bridge), patch.object(server.log, "log") as log:
            server._ev("timeout", "warning", "test I/O timeout")
        log.assert_called_once_with(logging.ERROR, "test I/O timeout")
        self.assertEqual(len(bridge.pending), 1)
        event = bridge.pending[0]
        self.assertEqual(event["level"], "error")
        self.assertEqual(event["event_kind"], "timeout")
        engine = Engine.__new__(Engine)
        engine.event = Mock()
        engine.producers = {}
        engine.consume(event)
        engine.event.assert_called_once_with("timeout", "error", "test I/O timeout")

    def test_rate_refusals_are_warning_and_do_not_increment_timeouts(self):
        for status in (429, 200):
            a = FakeAzure("alpha", [Behaviour(status=status, headers={"retry-after": "1"})]).start()
            try:
                p = Proxy([("alpha", a.url)], max_attempts=1)
                try:
                    self.assertEqual(ask(p)[0], status)
                    events = p.get("/events")[1]["events"]
                    limited = [e for e in events if e["kind"] == "throttle"]
                    self.assertTrue(limited)
                    self.assertTrue(all(e["level"] == "warning" for e in limited))
                    route = p.get("/routes")[1]["routes"]["alpha/" + DEPLOYMENT]
                    self.assertEqual((route["rate_limited"], route["timeouts"]), (1, 0))
                finally:
                    p.close()
            finally:
                a.stop()

    def test_read_timeout_is_error_and_does_not_increment_ratelimits(self):
        a = FakeAzure("alpha", [Behaviour(delay=0.2)]).start()
        try:
            p = Proxy([("alpha", a.url)], timeout=0.05, max_attempts=1)
            try:
                self.assertEqual(ask(p)[0], 504)
                events = p.get("/events")[1]["events"]
                timeouts = [e for e in events if e["kind"] == "timeout"]
                self.assertEqual(len(timeouts), 1)
                self.assertEqual(timeouts[0]["level"], "error")
                self.assertEqual(timeouts[0]["timeout_type"], "ReadTimeout")
                self.assertEqual(timeouts[0]["timeout_seconds"], 0.05)
                route = p.get("/routes")[1]["routes"]["alpha/" + DEPLOYMENT]
                self.assertEqual((route["rate_limited"], route["timeouts"]), (0, 1))
            finally:
                p.close()
        finally:
            a.stop()

    def test_stream_timeout_after_content_is_recorded_as_error(self):
        from proxy import server
        async def run():
            config = SimpleNamespace(timeout=0.1, capture_dir=None, stream_retry_markers=[],
                                     capacity_state_file=None, rpm_window=60, load_window=60)
            observer, engine = _inline_telemetry(config)
            route = serving_target("alpha", "http://localhost/", "v", DEPLOYMENT, "max_tokens", 0)
            entry = observer.charge(route, 10, 3)
            async def chunks():
                yield b'event: response.output_text.delta\ndata: {"delta":"hi"}\n\n'
                raise httpx.ReadTimeout("stalled stream")
            head = server._StreamHead(chunks())
            resp = httpx.Response(200, headers={"content-type": "text/event-stream"})
            with patch.multiple(server, cfg=config, telemetry=observer), patch.object(server, "_ev") as ev:
                reply = server._relay_stream(resp, route, server.RESPONSES_FACE,
                                             time.monotonic(), head, 10, entry)
                with self.assertRaises(httpx.ReadTimeout):
                    async for _ in reply.body_iterator:
                        pass
                calls = [call for call in ev.call_args_list if call.args[0] == "timeout"]
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0].args[1], "error")
                state = engine.quota.state(route)
                self.assertEqual((state.timeouts, state.rate_limited), (1, 0))
        asyncio.run(run())

    def test_buffered_body_timeout_is_classified_before_first_byte(self):
        from proxy import server
        from starlette.requests import Request
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'{"unfinished":'
                raise httpx.ReadTimeout("stalled response body")
        async def run():
            config = server.Config(load_routes=False)
            config.capacity_state_file = None
            config.affinity_enabled = False
            config.timeout = 0.1
            observer, engine = _inline_telemetry(config)
            route = serving_target("alpha", "http://localhost/", "v", DEPLOYMENT, "max_tokens", 0)
            response = httpx.Response(200, stream=Stream())
            client = SimpleNamespace(build_request=Mock(return_value=httpx.Request("POST", "http://localhost")),
                                     send=AsyncMock(return_value=response))
            tokens = SimpleNamespace(get=AsyncMock(return_value="test-token"))
            request = Request({"type": "http", "headers": []})
            with patch.multiple(server, cfg=config, telemetry=observer, client=client, tokens=tokens), \
                    patch.object(server, "_ev") as ev:
                reply = await server._forward(request, {"model": MODEL}, [route],
                                              MODEL, server.CHAT_FACE)
                self.assertEqual(reply.status_code, 504)
                self.assertEqual(engine.quota.state(route).timeouts, 1)
                self.assertEqual(engine.quota.state(route).rate_limited, 0)
                self.assertTrue(response.is_closed)
                self.assertTrue(any(call.args[:2] == ("timeout", "error") for call in ev.call_args_list))
        asyncio.run(run())

    def test_connect_timeout_reports_its_own_limit(self):
        from proxy import server
        with patch.object(server, "cfg", SimpleNamespace(timeout=900)), \
                patch.object(server, "telemetry") as telemetry, patch.object(server, "_ev") as ev:
            limit = server._record_timeout("route", None, "chat", MODEL,
                                            httpx.ConnectTimeout("connect"), time.monotonic())
        self.assertEqual(limit, 15)
        self.assertEqual(ev.call_args.kwargs["timeout_type"], "ConnectTimeout")
        self.assertEqual(ev.call_args.kwargs["timeout_seconds"], 15)
        telemetry.note_timeout.assert_called_once()

    def test_probe_hold_deadline_is_not_a_request_timeout(self):
        from proxy import server
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                await asyncio.sleep(0.03)
                yield b"healthy data"
        async def run():
            resp = httpx.Response(200, stream=Stream())
            with patch.object(server, "cfg", SimpleNamespace(probe_seconds=0.005, probe_bytes=1024)):
                head = await server._probe_stream_head(resp)
                self.assertFalse(head.timed_out)
                self.assertIsNotNone(head.pending)
                self.assertEqual(await head.pending, b"healthy data")
                await head.aiter.aclose()
                await resp.aclose()
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main(verbosity=2)
