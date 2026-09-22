"""Upstream connections are reclaimed at every downstream disconnect boundary."""

import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
from starlette.requests import ClientDisconnect

from run_tests import (Behaviour, DEPLOYMENT, FakeAzure, MODEL, Proxy, ask,
                       serving_target, sse)
from proxy import server


class ConnectionPoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.azure = FakeAzure("alpha", [
            Behaviour(events=sse("alpha", extra=2), event_delay=.05),
            Behaviour(),
        ]).start()
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=1),
            timeout=httpx.Timeout(1, pool=.05))
        self.route = serving_target("alpha", self.azure.url, "v", DEPLOYMENT,
                                    "max_completion_tokens", 0)
        self.config = SimpleNamespace(
            probe_seconds=.01, probe_bytes=16384, capture_dir=None,
            stream_retry_markers=[server.INBAND_RATE_LIMIT], timeout=1)
        self.observer = Mock()
        self.patches = patch.multiple(server, cfg=self.config, telemetry=self.observer)
        self.patches.start()
        self.responses = []
        self.heads = []

    async def asyncTearDown(self):
        for head in self.heads:
            if head.pending is not None:
                head.pending.cancel()
                await asyncio.gather(head.pending, return_exceptions=True)
        for response in self.responses:
            await response.aclose()
        await self.client.aclose()
        self.patches.stop()
        await asyncio.to_thread(self.azure.stop)

    async def relay(self):
        response = await self.client.send(self.client.build_request(
            "POST", self.azure.url + "openai/v1/responses", json={"stream": True}),
            stream=True)
        self.responses.append(response)
        head = await server._probe_stream_head(response)
        self.heads.append(head)
        entry = SimpleNamespace(streaming=False)
        reply = server._relay_stream(response, self.route, server.RESPONSES_FACE,
                                     time.monotonic(), head, entry=entry)
        return reply, head, entry

    async def assert_reusable(self, head):
        response = await self.client.post(self.azure.url + "openai/v1/responses", json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client._transport._pool._requests, [])
        self.assertTrue(head.pending is None or head.pending.done(),
                        "a disconnected response left an upstream read running")

    async def test_disconnect_before_body_starts_releases_pool_slot(self):
        reply, head, entry = await self.relay()

        async def send(message):
            raise OSError("downstream closed before response headers")

        async def receive():
            await asyncio.Event().wait()

        with self.assertRaises(ClientDisconnect):
            await reply({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        await self.assert_reusable(head)
        self.observer.finish.assert_called_once_with(entry)

    async def test_disconnect_during_body_send_releases_pool_slot(self):
        reply, head, entry = await self.relay()

        async def send(message):
            if message["type"] == "http.response.body":
                raise OSError("downstream closed while writing a chunk")

        async def receive():
            await asyncio.Event().wait()

        with self.assertRaises(ClientDisconnect):
            await reply({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        await self.assert_reusable(head)
        self.observer.finish.assert_called_once_with(entry)

    async def test_disconnect_cancellation_releases_pool_slot(self):
        reply, head, entry = await self.relay()
        sent = asyncio.Event()

        async def send(message):
            if message["type"] == "http.response.body":
                sent.set()
                await asyncio.Event().wait()

        async def receive():
            await sent.wait()
            return {"type": "http.disconnect"}

        await reply({"type": "http", "asgi": {"spec_version": "2.3"}}, receive, send)
        await self.assert_reusable(head)
        self.observer.finish.assert_called_once_with(entry)

    async def test_repeated_disconnects_keep_one_slot_pool_usable(self):
        for _ in range(12):
            self.azure.reset()
            await self.test_disconnect_before_body_starts_releases_pool_slot()
            self.observer.reset_mock()

    async def test_disconnect_while_awaiting_probe_read_releases_pool_slot(self):
        reply, head, entry = await self.relay()
        sent = asyncio.Event()

        async def send(message):
            if message["type"] == "http.response.body":
                sent.set()

        async def receive():
            await sent.wait()
            # Let the relay begin awaiting the probe's detached read.
            await asyncio.sleep(.005)
            return {"type": "http.disconnect"}

        await reply({"type": "http", "asgi": {"spec_version": "2.3"}}, receive, send)
        await self.assert_reusable(head)
        self.observer.finish.assert_called_once_with(entry)

    async def test_normal_stream_finishes_once_and_reuses_pool(self):
        reply, head, entry = await self.relay()
        chunks = []

        async def send(message):
            chunks.append(message.get("body", b""))

        async def receive():
            await asyncio.Event().wait()

        await reply({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        self.assertIn(b"response.completed", b"".join(chunks))
        await self.assert_reusable(head)
        self.observer.finish.assert_called_once_with(entry)

    async def test_cancelled_probe_stops_read_and_releases_pool(self):
        response = await self.client.send(self.client.build_request(
            "POST", self.azure.url + "openai/v1/responses", json={}), stream=True)
        self.responses.append(response)
        self.config.probe_seconds = 10
        waiting = asyncio.Event()
        original_wait = asyncio.wait

        async def wait(*args, **kwargs):
            waiting.set()
            return await original_wait(*args, **kwargs)

        with patch.object(server.asyncio, "wait", side_effect=wait), \
                patch.object(server, "_close_upstream", wraps=server._close_upstream) as close:
            task = asyncio.create_task(server._probe_stream_head(response))
            await waiting.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            pending = close.call_args.args[1]
            self.assertTrue(pending.done())
        self.assertTrue(response.is_closed)
        await self.assert_reusable(SimpleNamespace(pending=pending))

    async def test_exception_after_headers_releases_pool_slot(self):
        self.config.max_attempts = 1
        self.config.backoff_initial = .01
        self.config.scope = ""
        self.config.forward_headers = False
        entry = SimpleNamespace(streaming=False)
        self.observer.charge.return_value = entry
        self.observer.observed.side_effect = RuntimeError("observation failed")
        request = SimpleNamespace(headers={}, state=SimpleNamespace())
        with patch.object(server, "client", self.client), \
                patch.object(server, "tokens", SimpleNamespace(get=AsyncMock(return_value="test"))), \
                patch.object(server, "_image_deployments", return_value={}):
            with self.assertRaisesRegex(RuntimeError, "observation failed"):
                await server._forward(request, {"stream": False}, [self.route],
                                      "test-model", server.CHAT_FACE)
        await self.assert_reusable(SimpleNamespace(pending=None))
        self.observer.finish.assert_called_once_with(entry)


class ConnectionPoolIntegrationTests(unittest.TestCase):
    def test_real_disconnects_beyond_pool_capacity_leave_proxy_usable(self):
        count = 110
        azure = FakeAzure("alpha", [
            Behaviour(events=sse("alpha", extra=1), event_delay=.03)
            for _ in range(count)
        ] + [Behaviour()]).start()
        proxy = None
        try:
            proxy = Proxy([("alpha", azure.url)], timeout=1, max_attempts=1,
                          probe_seconds=.005)
            with httpx.Client(timeout=5) as client:
                for index in range(count):
                    with client.stream("POST", proxy.url("/v1/responses"), json={
                            "model": MODEL, "input": "test", "stream": True}) as response:
                        if response.status_code != 200:
                            self.fail("disconnect {}: {} {}".format(
                                index, response.read(), proxy.get("/events")[1]["events"][-5:]))
                        self.assertIn(b"response.created", next(response.iter_bytes()))
            self.assertEqual(ask(proxy)[0], 200)
            self.assertEqual(azure.hits, count + 1)
            events = proxy.get("/events")[1]["events"]
            self.assertFalse([e for e in events if e.get("timeout_type") == "PoolTimeout"])
        finally:
            if proxy:
                proxy.close()
            azure.stop()


if __name__ == "__main__":
    unittest.main()
