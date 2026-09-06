"""An explicit TRAPI namespace, separate from Azure text-model routing.

Authentication uses the proxy's existing credential/token-cache implementation.
No unprefixed aliases or cross-provider fallbacks are accepted. Authentication
and a dedicated HTTP pool are lazy: ordinary Azure requests and proxy startup
never contact TRAPI. Upstream status and Retry-After are preserved.
"""
import time

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response

DEPLOYMENT = "text-embedding-ada-002_2"
MODEL = "trapi/" + DEPLOYMENT
SCOPE = "api://trapi/.default"
STRIP_HEADERS = {"content-length", "content-encoding", "transfer-encoding",
                 "connection", "keep-alive", "upgrade", "trailer"}


def model_entry():
    return {"id": MODEL, "object": "model", "owned_by": "trapi",
            "routes": ["trapi-embeddings"], "faces": ["embeddings"]}


def install(app, token_getter, token_status, event, *, url=None):
    # Endpoint configuration belongs to the deployment, never to source control.
    url = url.strip() if isinstance(url, str) else ""
    stats = {"requests": 0, "errors": 0, "last_status": None}
    client = None

    def get_client():
        nonlocal client
        if client is None:
            client = httpx.AsyncClient(timeout=httpx.Timeout(120, connect=15.0))
        return client

    async def close():
        if client is not None:
            await client.aclose()

    app.router.add_event_handler("shutdown", close)

    def failure(status, message, kind):
        return JSONResponse(status_code=status, content={"error": {
            "message": message, "type": kind}})

    @app.get("/embeddings/status")
    async def status():
        return {"enabled": bool(url), "model": MODEL, "upstream_model": DEPLOYMENT,
                "upstream": url or None,
                "client_initialized": client is not None, "token": token_status(), **stats}

    @app.post("/v1/embeddings")
    async def embeddings(request: Request):
        try:
            payload = await request.json()
        except ValueError:
            return failure(400, "Expected a JSON request", "invalid_request_error")
        if not isinstance(payload, dict) or "input" not in payload:
            return failure(400, "An input field is required", "invalid_request_error")
        if payload.get("model") != MODEL:
            return failure(404, "Embedding model must be " + MODEL, "model_not_found")
        if not url:
            return failure(503, "TRAPI embeddings endpoint is not configured",
                           "provider_not_configured")
        stats["requests"] += 1
        started = time.monotonic()
        try:
            try:
                token = await token_getter()
            except Exception:
                response = failure(503, "TRAPI authentication is unavailable", "authentication_unavailable")
            else:
                upstream = await get_client().post(
                    url, json={**payload, "model": DEPLOYMENT},
                    headers={"Authorization": "Bearer " + token,
                             "Content-Type": "application/json"})
                response = Response(
                    content=upstream.content, status_code=upstream.status_code,
                    headers={key: value for key, value in upstream.headers.items()
                             if key.lower() not in STRIP_HEADERS})
        except httpx.TimeoutException:
            response = failure(504, "TRAPI embedding request timed out", "upstream_timeout")
        except httpx.RequestError:
            response = failure(502, "TRAPI embedding transport failed", "upstream_connection_error")
        stats["last_status"] = response.status_code
        stats["errors"] += int(response.status_code >= 400)
        level = ("info" if response.status_code < 400 else
                 "warning" if response.status_code < 500 else "error")
        event("embedding", level,
              "TRAPI embeddings status=%s duration=%.2fs", response.status_code,
              time.monotonic() - started, model=MODEL, status=response.status_code)
        return response
