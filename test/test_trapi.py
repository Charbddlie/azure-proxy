"""TRAPI provider isolation and independent terminal status."""
import sys
import urllib.error
import unittest
from unittest.mock import AsyncMock, patch

from run_tests import Behaviour, FakeAzure, MODEL, Proxy, ROOT, ask, multipart, who

sys.path.insert(0, ROOT)
import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from proxy import trapi_embeddings


class TrapiTuiTests(unittest.TestCase):
    def test_trapi_tui_reads_do_not_enable_upstream_or_change_azure_routing(self):
        sys.path.insert(0, ROOT)
        from tui.client import Poller
        from tui.snapshot import Snapshot

        a = FakeAzure("alpha", [Behaviour(), Behaviour(status=500)]).start()
        try:
            p = Proxy([("alpha", a.url)], max_attempts=1,
                      trapi_url="https://trapi.invalid/v1/embeddings")
            try:
                poller = Poller(p.url(""))
                poller._poll_trapi()
                raw = poller.snapshot()
                assert raw["trapi"]["requests"] == 0
                assert not raw["trapi"]["client_initialized"]
                initial_token = dict(raw["trapi"]["token"])
                assert raw["trapi"]["enabled"]
                assert "api://trapi/.default" not in p.get("/healthz")[1]["tokens"]

                raw["routes"] = p.get("/routes")[1]
                snapshot = Snapshot(raw)
                assert [g.name for g in snapshot.sources] == ["alpha"]
                assert [g.name for g in snapshot.models] == [MODEL]
                assert snapshot.trapi["model"] == "trapi/text-embedding-ada-002_2"

                # Both ordinary success and exhausted Azure failure stay on Azure.
                status, body, _ = ask(p)
                assert status == 200 and who(body) == "alpha", body
                status, body, _ = ask(p)
                assert status == 500, (status, body)

                for path in ("/v1/chat/completions", "/v1/responses",
                             "/v1/images/generations"):
                    status, body, _ = p.post({
                        "model": "trapi/text-embedding-ada-002_2", "input": "hi"},
                        path=path)
                    assert status == 404, (path, status, body)
                blob, content_type = multipart(model="trapi/text-embedding-ada-002_2")
                status, body, _ = p.post_raw("/v1/images/edits", blob, content_type)
                assert status == 404, (status, body)
                for model in ("text-embedding-ada-002_2", "trapi/unknown"):
                    status, body, _ = p.post({"model": model, "input": "hi"},
                                              path="/v1/embeddings")
                    assert status == 404, (model, status, body)

                poller._poll_trapi()
                state = poller.snapshot()["trapi"]
                assert state["requests"] == 0 and not state["client_initialized"]
                assert state["token"] == initial_token
                assert a.hits == 2
            finally:
                p.close()
        finally:
            a.stop()


    def test_trapi_status_failure_does_not_hide_azure_status(self):
        sys.path.insert(0, ROOT)
        from tui.client import Poller

        for failure in (urllib.error.HTTPError("http://proxy/embeddings/status",
                                              404, "Not Found", {}, None),
                        TimeoutError("status timed out")):
            poller = Poller("http://unused")

            def get(path):
                if path == "/healthz":
                    return {"ok": True}
                if path == "/routes":
                    return {"routes": {"alpha/deployment": {"endpoint": "alpha"}}}
                if path.startswith("/events?"):
                    poller._stop.set()  # finish after one complete poll
                    return {"events": [], "next": 0}
                raise failure

            poller._get = get
            poller._wake.set()
            poller._loop()
            raw = poller.snapshot()
            assert raw["error"] is None and raw["health"]["ok"]
            assert "alpha/deployment" in raw["routes"]["routes"]
            assert raw["trapi_error"] and raw["trapi"] is None

            poller._get = lambda _path: {"model": "trapi/example", "requests": 3}
            poller._poll_trapi()
            assert poller.snapshot()["trapi_error"] is None
            poller._get = get
            poller._poll_trapi()
            raw = poller.snapshot()
            assert raw["error"] is None and raw["trapi_error"]
            assert raw["trapi"]["requests"] == 3  # last known values stay visible


    def test_trapi_dashboard_is_a_separate_scrollable_board(self):
        sys.path.insert(0, ROOT)
        from io import StringIO
        from types import SimpleNamespace
        from rich.cells import cell_len
        from rich.console import Console
        from tui.app import Dashboard
        from tui.boards import BOARDS, render_trapi
        from tui.snapshot import Snapshot

        base = {"health": {"host": "127.0.0.1", "port": 8811},
                "trapi": {"model": "trapi/text-embedding-ada-002_2", "requests": 7,
                          "errors": 1, "last_status": 429,
                          "token": {"have_token": True, "expires_in_seconds": 1800}}}
        for width in (40, 80, 140):
            stream = StringIO()
            console = Console(file=stream, width=width, height=30, color_system=None)
            dash = Dashboard(SimpleNamespace(snapshot=lambda: base), console)
            assert dash.board == 0
            for _ in range(len(BOARDS) - 1):
                dash.key("\x1b[C")
            assert BOARDS[dash.board] == "trapi"
            console.print(dash.render())
            text = stream.getvalue()
            assert "HTTP 429" in text and "累计请求" in text, text
            assert "Azure token" not in text and "RPM" not in text, text
            if width >= 80:
                assert base["trapi"]["model"] in text
            dash.key("\x1b[B")
            assert dash.offset[dash.board] == min(1, dash._extent[dash.board] - 1)
            dash.key("g")
            assert dash.offset[dash.board] == 0
            dash.key("\x1b[C")
            assert dash.board == 0

            for raw in (base, {"trapi_error": "此代理未启用 TRAPI"}, {},
                        dict(base, trapi_error="TRAPI 状态读取失败")):
                body, extent = render_trapi(Snapshot(raw), width, 4, 0)
                stream = StringIO()
                Console(file=stream, width=width, color_system=None).print(body)
                lines = stream.getvalue().splitlines()
                assert len(lines) <= 4 and extent >= 1
                assert all(cell_len(line) <= width for line in lines)
                full, _ = render_trapi(Snapshot(raw), width, 1000, 0)
                full_stream = StringIO()
                Console(file=full_stream, width=width, color_system=None).print(full)
                full_lines = full_stream.getvalue().splitlines()
                assert lines == full_lines[:4]
                tail, _ = render_trapi(Snapshot(raw), width, 4, extent - 1)
                tail_stream = StringIO()
                Console(file=tail_stream, width=width, color_system=None).print(tail)
                assert tail_stream.getvalue().splitlines() == full_lines[-4:]


class TrapiProviderTests(unittest.TestCase):
    def test_missing_endpoint_is_disabled_and_does_not_authenticate(self):
        app = FastAPI()
        token = AsyncMock(return_value="test-token")
        trapi_embeddings.install(app, token, lambda: {}, lambda *a, **k: None)
        with TestClient(app) as client:
            status = client.get("/embeddings/status").json()
            self.assertFalse(status["enabled"])
            self.assertFalse(status["client_initialized"])
            response = client.post("/v1/embeddings", json={
                "model": trapi_embeddings.MODEL, "input": "hello"})
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["error"]["type"], "provider_not_configured")
            token.assert_not_awaited()

    def test_configured_embedding_preserves_response_and_closes_its_client(self):
        app = FastAPI()
        token = AsyncMock(return_value="test-token")
        upstream = AsyncMock()
        upstream.post.side_effect = [
            httpx.Response(200, json={"model": "text-embedding-ada-002",
                                    "data": [{"embedding": [0.1, 0.2]}]}),
            httpx.Response(429, json={"error": {"message": "limited"}},
                           headers={"Retry-After": "7"})]
        url = "https://trapi.invalid/custom/v1/embeddings"
        trapi_embeddings.install(app, token, lambda: {}, lambda *a, **k: None, url=url)
        with patch.object(trapi_embeddings.httpx, "AsyncClient", return_value=upstream) as factory:
            with TestClient(app) as client:
                self.assertFalse(client.get("/embeddings/status").json()["client_initialized"])
                factory.assert_not_called()
                token.assert_not_awaited()
                for name in ("text-embedding-ada-002_2", "trapi/unknown"):
                    rejected = client.post("/v1/embeddings", json={"model": name, "input": "hi"})
                    self.assertEqual(rejected.status_code, 404)
                token.assert_not_awaited()
                payload = {"model": trapi_embeddings.MODEL, "input": ["hello"], "encoding_format": "float"}
                response = client.post("/v1/embeddings", json=payload)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["data"][0]["embedding"], [0.1, 0.2])
                self.assertEqual(upstream.post.call_args.args, (url,))
                forwarded = upstream.post.call_args.kwargs
                self.assertEqual(forwarded["json"], dict(payload, model=trapi_embeddings.DEPLOYMENT))
                self.assertEqual(forwarded["headers"]["Authorization"], "Bearer test-token")
                response = client.post("/v1/embeddings", json=payload)
                self.assertEqual(response.status_code, 429)
                self.assertEqual(response.headers["retry-after"], "7")
                status = client.get("/embeddings/status").json()
                self.assertEqual((status["requests"], status["errors"], status["last_status"]), (2, 1, 429))
            upstream.aclose.assert_awaited_once()

    def test_auth_failure_does_not_create_upstream_client(self):
        app = FastAPI()
        token = AsyncMock(side_effect=RuntimeError("credential unavailable"))
        trapi_embeddings.install(app, token, lambda: {}, lambda *a, **k: None,
                                 url="https://trapi.invalid/v1/embeddings")
        with patch.object(trapi_embeddings.httpx, "AsyncClient") as factory:
            with TestClient(app) as client:
                response = client.post("/v1/embeddings", json={
                    "model": trapi_embeddings.MODEL, "input": "hello"})
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json()["error"]["type"], "authentication_unavailable")
                factory.assert_not_called()

    def test_default_model_catalog_does_not_enable_trapi(self):
        a = FakeAzure("alpha").start()
        try:
            p = Proxy([("alpha", a.url)])
            try:
                self.assertEqual([m["id"] for m in p.get("/v1/models")[1]["data"]], [MODEL])
                self.assertFalse(p.get("/embeddings/status")[1]["enabled"])
            finally:
                p.close()
        finally:
            a.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
