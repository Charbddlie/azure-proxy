"""A stand-in for an Azure OpenAI endpoint.

Lets the test suite provoke failures the real service will not produce on
demand — 429 storms, 500s, hangs, streams that die halfway — and inspect exactly
what the proxy sent upstream. Requests are recorded so tests can assert on the
rewritten `model`, the injected Authorization header, and forwarded client
headers.

Each instance is one fake endpoint, serving both faces: it answers on whatever
path it is given and shapes its default reply from that path. Behaviour is a
list of scripted responses consumed in order; the last entry repeats once the
list runs out.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List, Optional
import socket
import time


class Behaviour:
    """One scripted reply.

    status: HTTP status to return.
    delay:  seconds to sleep before replying, for provoking client timeouts.
    body:   response JSON; a default is synthesised from the path when omitted.
    headers: extra response headers. Azure hangs its rate limit state here, and
            the proxy's weights are built from it, so a test that wants to
            exercise balancing sends them with ratelimit().

    The remaining three make a streaming reply instead of a buffered one:

    events:      SSE events, sent one chunk each with a flush between them.
    event_delay: seconds between events, so a test can observe the gap.
    cut_after:   drop the connection after this many events, without the
                 terminating chunk. That is a stream dying mid-flight, which is
                 the one failure the proxy must NOT retry.
    """

    def __init__(self, status: int = 200, delay: float = 0.0,
                 body: Optional[dict] = None,
                 events: Optional[List[dict]] = None,
                 event_delay: float = 0.0, cut_after: Optional[int] = None,
                 headers: Optional[dict] = None):
        self.status = status
        self.delay = delay
        self.body = body
        self.events = events
        self.event_delay = event_delay
        self.cut_after = cut_after
        self.headers = headers or {}


def ratelimit(limit_requests=None, remaining_requests=None,
              limit_tokens=None, remaining_tokens=None, renewal=60) -> dict:
    """The x-ratelimit-* block Azure returns on every response.

    remaining defaults to the limit, which is what the real service almost
    always reports: measured against a live endpoint these counters read
    near-full on nearly every sample, so "full" is the realistic baseline and a
    test that wants a squeezed endpoint has to ask for one.
    """
    headers = {}
    if limit_requests is not None:
        headers["x-ratelimit-limit-requests"] = str(limit_requests)
        headers["x-ratelimit-remaining-requests"] = str(
            limit_requests if remaining_requests is None else remaining_requests)
        headers["x-ratelimit-renewalperiod-requests"] = str(renewal)
    if limit_tokens is not None:
        headers["x-ratelimit-limit-tokens"] = str(limit_tokens)
        headers["x-ratelimit-remaining-tokens"] = str(
            limit_tokens if remaining_tokens is None else remaining_tokens)
        headers["x-ratelimit-renewalperiod-tokens"] = str(renewal)
    return headers


def sse(name: str, extra: int = 0, total_tokens: int = 2) -> List[dict]:
    """A minimal well-formed Responses stream: created, then completed.

    Two events is the whole minimum: a real client (codex 0.148) accepts a
    stream carrying nothing else and still computes its token counts.

    The usage block on the final event is what the proxy reads to find out what
    the request actually cost — by scanning the bytes as they go past, since it
    cannot parse a stream it is forwarding verbatim.
    """
    events = [{"type": "response.created",
               "response": {"id": "resp_fake", "status": "in_progress"}}]
    for i in range(extra):
        events.append({"type": "response.output_text.delta",
                       "delta": "chunk{}".format(i)})
    events.append({"type": "response.completed",
                   "response": {"id": "resp_fake", "status": "completed",
                                "from": name,
                                "usage": {"input_tokens": 1, "output_tokens": 1,
                                          "total_tokens": total_tokens}}})
    return events


def sse_rate_limited(name: str, after: int = 0, preamble_padding: int = 0,
                     deployment: str = "gpt-5.6-sol",
                     region: str = "swedencentral") -> List[dict]:
    """A 200 that is really a 429, in the shape Azure actually sends one.

    Captured off endpoint-a on 2026-08-20 by pushing it past its TPM ceiling:
    HTTP 200, `retry-after` in the headers, then response.created, an `error`
    event, and response.failed. The message wording is verbatim from that
    capture. See THROTTLE_HEADER in proxy/server.py — the headers are the part
    that matters, and ratelimit_throttled() below produces them.

    `after` inserts that many real output deltas before the error. That is the
    other half of the behaviour under test and it is not the same case: with
    `after=0` nothing the caller can use has been produced yet, so the proxy is
    free to drop the whole stream and try another endpoint; with `after>0` the
    caller is already parsing output, and splicing a second stream onto it is
    the one thing the proxy must never do.

    `preamble_padding` bloats response.created, which is the failure that got
    through to production. A real codex request echoes back far more than 16KB
    in that first event, so a proxy that goes looking for the error inside a
    fixed-size window never reaches it.
    """
    created = {"type": "response.created",
               "response": {"id": "resp_fake", "status": "in_progress"}}
    if preamble_padding:
        created["response"]["instructions"] = "x" * preamble_padding
    events = [created]
    for i in range(after):
        events.append({"type": "response.output_text.delta",
                       "delta": "chunk{}".format(i)})
    message = ("Your requests to {} for {} in {} have exceeded rate limit."
               .format(deployment, deployment, region))
    events.append(
        {"type": "error",
         "error": {"type": "too_many_requests", "code": "rate_limit_exceeded",
                   "headers": {"x-ms-fe-error": "true"},
                   "message": message, "param": None},
         "sequence_number": 1})
    events.append(
        {"type": "response.failed",
         "response": {"id": "resp_fake", "status": "failed",
                      "error": {"code": "rate_limit_exceeded",
                                "message": message},
                      "from": name}})
    return events


def ratelimit_throttled(retry_after=4, limit_tokens=333000,
                        remaining_tokens=-27689, limit_requests=None,
                        remaining_requests=None) -> dict:
    """The headers Azure sends with a throttled 200.

    `retry-after` on a 200 is the whole signal — it appeared on every refused
    response in the capture and on none of the successful ones. The negative
    remaining-tokens is reproduced because it is real, and because it must NOT
    be what the proxy keys on: a response that streamed to completion in the
    same capture reported -27978.
    """
    headers = ratelimit(limit_tokens=limit_tokens,
                        remaining_tokens=remaining_tokens,
                        limit_requests=limit_requests,
                        remaining_requests=remaining_requests)
    headers["Retry-After"] = str(retry_after)
    return headers


def is_responses(path: str) -> bool:
    """Which face was this request for? The path is the only difference."""
    return "responses" in path


def is_images(path: str) -> bool:
    return "/images/" in path


def join_duplicates(items) -> dict:
    """Collapse repeated headers the way a real HTTP server reports them.

    A plain dict comprehension would keep only the last of a repeated header and
    hide the fact that it was sent twice. That is not a hypothetical: sending
    Content-Type twice reaches Azure as `application/json,application/json`, and
    the Responses API rejects it. What the test sees has to match what the
    service sees, or the test certifies a bug as fixed.
    """
    joined = {}
    for key, value in items:
        key = key.lower()
        joined[key] = joined[key] + "," + value if key in joined else value
    return joined


class FakeAzure:
    def __init__(self, name: str, behaviours: Optional[List[Behaviour]] = None,
                 headers: Optional[dict] = None, total_tokens: int = 2):
        """headers: sent with every reply, under whatever a Behaviour adds.

        Rate limit state is a property of the deployment, not of one reply, so
        the common case is to set it once here rather than on each Behaviour.

        total_tokens: what every default reply claims to have cost. The proxy
        charges an estimate at dispatch and corrects it from this, so a test
        that wants to drive a route's token load to a particular fraction of its
        ceiling sets this rather than trying to size a prompt.
        """
        self.name = name
        self.behaviours = behaviours or [Behaviour()]
        self.headers = headers or {}
        self.total_tokens = total_tokens
        self.requests: List[dict] = []
        self._index = 0
        self._lock = threading.Lock()

        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass                            # keep test output readable

            def do_POST(self):
                length = int(self.headers.get("content-length", 0))
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    body = json.loads(raw)
                except Exception:
                    body = {"_unparsed": raw.decode("utf-8", "replace")}

                with fake._lock:
                    fake.requests.append({
                        "path": self.path,
                        "headers": join_duplicates(self.headers.items()),
                        "body": body,
                        # The bytes as they arrived. /v1/images/edits relays a
                        # multipart body without re-encoding it, and the only
                        # way to assert that is to compare what was sent with
                        # what turned up.
                        "raw": raw,
                    })
                    behaviour = fake.behaviours[min(fake._index,
                                                    len(fake.behaviours) - 1)]
                    fake._index += 1

                if behaviour.delay:
                    time.sleep(behaviour.delay)

                if behaviour.events is not None and behaviour.status == 200:
                    self._stream(behaviour)
                else:
                    self._buffered(behaviour, body)

            # -- replies ---------------------------------------------------
            def _default_body(self, body):
                if is_images(self.path):
                    # The images faces answer with base64, and with a usage
                    # block that counts image tokens. `from` is where the test
                    # reads which upstream served it, since there is no text
                    # field to hide a name in.
                    return {
                        "created": 1788000000,
                        "from": fake.name,
                        "data": [{"b64_json": "aW1hZ2U="}],
                        "usage": {"input_tokens": 1, "output_tokens": 1,
                                  "total_tokens": fake.total_tokens},
                    }
                if is_responses(self.path):
                    return {
                        "id": "resp_fake",
                        "object": "response",
                        "status": "completed",
                        "model": body.get("model", "unknown"),
                        "output": [{
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text",
                                         "text": "from:" + fake.name}],
                        }],
                        "usage": {"input_tokens": 1, "output_tokens": 1,
                                  "total_tokens": fake.total_tokens},
                    }
                return {
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "model": body.get("model", "unknown"),
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant",
                                    "content": "from:" + fake.name},
                        "finish_reason": "stop",
                    }],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1,
                              "total_tokens": fake.total_tokens},
                }

            def _extra_headers(self, behaviour):
                """Instance defaults, with the Behaviour's own on top."""
                merged = dict(fake.headers)
                merged.update(behaviour.headers)
                return merged

            def _buffered(self, behaviour, body):
                payload = behaviour.body
                if payload is None:
                    if behaviour.status == 200:
                        payload = self._default_body(body)
                    else:
                        payload = {"error": {
                            "message": "fake {} from {}".format(behaviour.status,
                                                                fake.name),
                            "code": "fake_error"}}

                data = json.dumps(payload).encode()
                try:
                    self.send_response(behaviour.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    for key, value in self._extra_headers(behaviour).items():
                        self.send_header(key, str(value))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    # The proxy hung up first — expected in the timeout test,
                    # where it stops waiting before this reply is sent.
                    pass

            def _stream(self, behaviour):
                """Chunked SSE, flushed per event so arrival order is testable."""
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Transfer-Encoding", "chunked")
                    for key, value in self._extra_headers(behaviour).items():
                        self.send_header(key, str(value))
                    self.end_headers()
                    self.wfile.flush()

                    for i, event in enumerate(behaviour.events):
                        if behaviour.cut_after is not None and i >= behaviour.cut_after:
                            # No terminating chunk: the client sees a truncated
                            # body, not a tidy end of stream.
                            self.close_connection = True
                            self.connection.close()
                            return
                        payload = "event: {}\ndata: {}\n\n".format(
                            event.get("type", "message"),
                            json.dumps(event)).encode()
                        self.wfile.write(b"%x\r\n" % len(payload)
                                         + payload + b"\r\n")
                        self.wfile.flush()
                        if behaviour.event_delay:
                            time.sleep(behaviour.event_delay)

                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)

    @property
    def url(self) -> str:
        return "http://127.0.0.1:{}/".format(self.port)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._server.shutdown()
        self._server.server_close()

    def reset(self):
        with self._lock:
            self.requests.clear()
            self._index = 0

    @property
    def hits(self) -> int:
        return len(self.requests)


def dead_url() -> str:
    """A URL nothing is listening on, for provoking connection errors."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return "http://127.0.0.1:{}/".format(port)
