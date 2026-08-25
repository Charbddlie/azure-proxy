#!/usr/bin/env python3
"""Tests for the behaviour the proxy adds on top of Azure.

Everything here runs against fake upstreams (test/fake_azure.py), so no Azure
credentials are needed, no quota is spent, and failures the real service will
not produce on demand — 429 storms, hangs, dead hosts — can be provoked exactly.

Each test writes a throwaway settings/ + runtime/ tree, starts a real proxy
process against it, and asserts on what the client saw and what the upstreams
received.

    python test/run_tests.py
    python test/run_tests.py failover_on_429      # single test by name
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fake_azure import (Behaviour, FakeAzure, dead_url, ratelimit,  # noqa: E402
                        ratelimit_throttled, sse, sse_rate_limited)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The checkout's own interpreter, found relative to this file rather than by an
# absolute path — so a moved or copied tree tests itself rather than whatever
# happens to still be at the old location.
PYTHON = os.environ.get(
    "AZURE_PROXY_PYTHON", os.path.join(ROOT, ".venv", "bin", "python"))

MODEL = "test-model"
DEPLOYMENT = "test-deployment"      # deliberately different, to catch rewriting
RESPONSES_PATH = "openai/v1/responses"


# --------------------------------------------------------------------------
# harness
# --------------------------------------------------------------------------

class Proxy:
    """A real proxy process wired to a throwaway config tree."""

    def __init__(self, endpoints, timeout=30, max_attempts=4, port=0,
                 responses=True, responses_compat=True,
                 balance="priority_threshold", static_weights=None,
                 demote_seconds=30, demote_halflife=30, observation_ttl=120,
                 spill_threshold=0.70, load_window=60, probe_seconds=0.5,
                 affinity=True, affinity_on_conflict="wait",
                 affinity_attempts=4, affinity_max_wait=30,
                 affinity_off_route="strip",
                 foreign=True, foreign_reclaim=0.1, deployments=None,
                 faces=None):
        """endpoints: list of (name, url). Order is failover priority.

        deployments: {endpoint name: [{"name": …, "capacity_tokens": …}, …]},
        for the case one endpoint serves the model from more than one
        deployment — a second SKU is a second quota, and the two are separate
        routes. Omitted, every endpoint gets one deployment with no declared
        capacity, which is what a runtime/ from before ARM discovery looks like.

        faces: {endpoint name: ["chat"] | ["responses"] | both}, for a
        deployment that serves one face and not the other. Azure gates the two
        separately and some models really are Responses-only.

        responses: whether the fake endpoints serve the Responses API. Set it
        False to model a deployment that only answers on chat/completions —
        Azure gates the two faces separately, so that combination is real.

        responses_compat: the /v1/responses body rewrites. On by default,
        matching settings/policy.yaml.

        balance: strict_priority | priority_threshold | capacity. Defaults to
        priority_threshold, matching settings/policy.yaml — so every test
        written before balancing existed now runs against the shipping default,
        and passes for the right reason: at one request per test nothing is
        anywhere near its threshold, so the top route takes all of it.

        The demote/observation/window knobs exist so the recovery and load tests
        can run in seconds instead of minutes. Their production values are in
        settings/policy.yaml.
        """
        self.home = tempfile.mkdtemp(prefix="azure-proxy-test-")
        self.port = port or _free_port()
        os.makedirs(os.path.join(self.home, "settings"))
        os.makedirs(os.path.join(self.home, "runtime"))

        weights = "".join(
            "      {}: {}\n".format(name, value)
            for name, value in sorted((static_weights or {}).items()))

        with open(os.path.join(self.home, "settings", "policy.yaml"), "w") as f:
            f.write(
                "server:\n"
                "  host: 127.0.0.1\n"
                "  port: {port}\n"
                "routing:\n"
                "  retry_on_status: [429, 500, 502, 503, 504]\n"
                "  retry_on_transport_error: true\n"
                "  retry_on_timeout: false\n"
                "  request_timeout_seconds: {timeout}\n"
                "  max_attempts_per_request: {attempts}\n"
                "  backoff_initial_seconds: 0.05\n"
                "  backoff_multiplier: 1.5\n"
                "  backoff_jitter_seconds: 0.01\n"
                "  balance: {balance}\n"
                "  stream_probe:\n"
                "    hold_seconds: {probe}\n"
                "    hold_bytes: 16384\n"
                "    retry_on_codes: [rate_limit_exceeded]\n"
                "  session_affinity:\n"
                "    enabled: {affinity}\n"
                "    on_conflict: {conflict}\n"
                "    wait_attempts: {waits}\n"
                "    max_wait_seconds: {maxwait}\n"
                "    off_route: {offroute}\n"
                "    ttl_seconds: 3600\n"
                "    max_sessions: 4096\n"
                "    sticky_include: [reasoning.encrypted_content]\n"
                "    keys: [header:session-id, body:prompt_cache_key,\n"
                "           body:client_metadata.session_id,\n"
                "           header:x-session-id]\n"
                "  balancing:\n"
                "    headroom_high_water: 0.5\n"
                "    weight_floor: 0.05\n"
                "    observation_ttl_seconds: {ttl}\n"
                "    demote_multiplier: 0.25\n"
                "    demote_seconds: {demote}\n"
                "    demote_recovery_halflife_seconds: {halflife}\n"
                "    spill_threshold: {spill}\n"
                "    load_window_seconds: {window}\n"
                "    assumed_chars_per_token: 4\n"
                "    foreign_load:\n"
                "      enabled: {foreign}\n"
                "      reclaim_per_minute: {reclaim}\n"
                "    static_weights:\n{weights}"
                "request:\n"
                "  mode: passthrough\n"
                "  responses_compat: {compat}\n"
                "  forward_headers: true\n"
                "auth:\n"
                "  scope: https://cognitiveservices.azure.com/.default\n"
                "  expected_account: test\n"
                "  refresh_margin_seconds: 300\n".format(
                    port=self.port, timeout=timeout, attempts=max_attempts,
                    balance=balance, ttl=observation_ttl,
                    demote=demote_seconds, halflife=demote_halflife,
                    spill=spill_threshold, window=load_window,
                    probe=probe_seconds,
                    foreign="true" if foreign else "false",
                    reclaim=foreign_reclaim,
                    affinity="true" if affinity else "false",
                    conflict=affinity_on_conflict, waits=affinity_attempts,
                    maxwait=affinity_max_wait, offroute=affinity_off_route,
                    weights=weights or "      {}\n",
                    compat="true" if responses_compat else "false"))

        header = {"_generated_by": "test", "_generated_at": "test"}
        both = ["chat", "responses"] if responses else ["chat"]
        endpoint_extra = ({"responses_status": "ok",
                           "responses_path": RESPONSES_PATH} if responses
                          else {"responses_status": "unsupported"})

        def plan(name):
            """This endpoint's deployments, as the probe would have written them.

            The default — one deployment, no capacity_* at all — is the shape a
            runtime/ written before ARM discovery has, so every test that does
            not opt in is also the regression test for still booting on it.
            """
            return (deployments or {}).get(name) or [{"name": DEPLOYMENT}]

        def faces_for(name):
            return [f for f in ((faces or {}).get(name) or both) if f in both]

        def route(name, i, spec):
            hop = {"endpoint": name, "deployment": spec["name"], "priority": i,
                   "limit_param": "max_completion_tokens",
                   "faces": faces_for(name)}
            for field in ("capacity_requests", "capacity_tokens",
                          "model_version"):
                if spec.get(field) is not None:
                    hop[field] = spec[field]
            return hop

        sources = dict(header, endpoints=[
            dict({"name": name, "url": url, "api_version": "test-version",
                  "auth": "cli", "priority": i, "status": "ok",
                  "deployments": [
                      dict(spec, limit_param="max_completion_tokens",
                           faces=faces_for(name))
                      for spec in plan(name)]}, **endpoint_extra)
            for i, (name, url) in enumerate(endpoints)])
        models = dict(header, models={MODEL: {"routes": [
            route(name, i, spec)
            for i, (name, _url) in enumerate(endpoints)
            for spec in plan(name)]}})
        _dump(os.path.join(self.home, "runtime", "sources.json"), sources)
        _dump(os.path.join(self.home, "runtime", "models.json"), models)

        env = dict(os.environ,
                   AZURE_PROXY_HOME=self.home,
                   AZURE_PROXY_STATIC_TOKEN="test-token-abc",
                   PYTHONPATH=ROOT)
        self.log = open(os.path.join(self.home, "proxy.log"), "w+")
        self.proc = subprocess.Popen([PYTHON, "-m", "proxy"], env=env,
                                     stdout=self.log, stderr=subprocess.STDOUT)
        self._await_health()

    def _await_health(self):
        for _ in range(80):
            if self.proc.poll() is not None:
                self.log.seek(0)
                raise RuntimeError("proxy exited on startup:\n" + self.log.read())
            try:
                urllib.request.urlopen(self.url("/healthz"), timeout=1).read()
                return
            except Exception:
                time.sleep(0.25)
        raise RuntimeError("proxy never became healthy")

    def url(self, path):
        return "http://127.0.0.1:{}{}".format(self.port, path)

    def post(self, body, headers=None, timeout=60,
             path="/v1/chat/completions"):
        req = urllib.request.Request(
            self.url(path),
            data=json.dumps(body).encode(),
            headers=dict({"Content-Type": "application/json"}, **(headers or {})),
            method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read()), dict(r.headers)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"_raw": raw.decode("utf-8", "replace")}
            return e.code, parsed, dict(e.headers)

    def get(self, path):
        with urllib.request.urlopen(self.url(path), timeout=10) as r:
            return r.status, json.loads(r.read())

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.log.close()
        shutil.rmtree(self.home, ignore_errors=True)


def _dump(path, doc):
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)


def _free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def ask(proxy, headers=None, timeout=60, **kw):
    return proxy.post(dict({"model": MODEL,
                            "messages": [{"role": "user", "content": "hi"}],
                            "max_completion_tokens": 16}, **kw),
                      headers=headers, timeout=timeout)


def who(body):
    """Which fake upstream answered."""
    return body["choices"][0]["message"]["content"].replace("from:", "")


# responses face
#
# The real shape codex 0.148.0 sends, captured off a logging server rather than
# read out of documentation. Nothing here is optional decoration: every field is
# something a passthrough proxy could silently drop, and the encrypted reasoning
# in `include` is what keeps a stateless multi-turn session coherent.
# --------------------------------------------------------------------------

CODEX_BODY = {
    "model": MODEL,
    "store": False,
    "stream": True,
    "include": ["reasoning.encrypted_content"],
    "reasoning": {"effort": "low", "context": "all_turns"},
    "text": {"verbosity": "low"},
    "tool_choice": "auto",
    "parallel_tool_calls": False,
    "prompt_cache_key": "session-uuid-42",
    "client_metadata": {"cli": "codex", "version": "0.148.0"},
    "input": [
        {"type": "message", "role": "developer",
         "content": [{"type": "input_text", "text": "the system prompt"}]},
        # codex does not put its tools in the top-level `tools` array.
        {"type": "additional_tools", "name": "shell",
         "parameters": {"type": "object", "properties": {}}},
        {"type": "message", "role": "user",
         "content": [{"type": "input_text", "text": "hi"}]},
    ],
}


# The two things in a real codex request that Azure rejects and OpenAI does
# not, in the shape they arrive in (captured from a live run, 2026-08-19):
# a `namespace` wrapper tool whose own description is blank, with the tools
# that do the work underneath it, and codex's private per-message bookkeeping.
# Two blank descriptions, one nested, so a rewrite that only walks the top of
# the tool tree cannot pass this.
CODEX_STRICT_BODY = {
    "model": MODEL,
    "stream": False,
    "input": [
        {"type": "message", "role": "user",
         "content": [{"type": "input_text", "text": "hi"}],
         "internal_chat_message_metadata_passthrough": {
             "turn_id": "turn-1", "create_time": 1755600000.0},
         "tools": [
             {"type": "namespace", "name": "functions", "description": "",
              "tools": [
                  {"type": "function", "name": "exec",
                   "description": "run a shell command"},
                  {"type": "function", "name": "wait", "description": ""},
              ]},
         ]},
    ],
}

# Mirrors EMPTY_DESCRIPTION_FILLER in proxy/server.py. Duplicated on purpose:
# the value reaches the model and shows up in captured trajectories, so
# changing it should have to be done in two places deliberately.
FILLER = "(no description)"


def strict_body():
    return json.loads(json.dumps(CODEX_STRICT_BODY))    # deep copy


def responses_body(**kw):
    body = json.loads(json.dumps(CODEX_BODY))       # deep copy
    body.update(kw)
    return body


def ask_responses(proxy, stream=False, timeout=60, **kw):
    return proxy.post(responses_body(stream=stream, **kw), timeout=timeout,
                      path="/v1/responses")


def who_responses(body):
    return body["output"][0]["content"][0]["text"].replace("from:", "")


def stream_responses(proxy, body=None, timeout=30):
    """Post a streaming request, recording when each chunk actually arrived.

    stdlib HTTP would buffer here, which is exactly what these tests are trying
    to detect, so this one client uses httpx — already a dependency of the proxy.
    Returns (status, headers, [(arrival_time, chunk)], error_or_None).
    """
    import httpx

    chunks = []
    body = body if body is not None else responses_body(stream=True)
    try:
        with httpx.stream("POST", proxy.url("/v1/responses"), json=body,
                          timeout=timeout) as r:
            status, headers = r.status_code, dict(r.headers)
            try:
                for chunk in r.iter_raw():
                    chunks.append((time.time(), chunk))
            except httpx.HTTPError as e:
                return status, headers, chunks, e
    except httpx.HTTPError as e:
        return None, {}, chunks, e
    return status, headers, chunks, None


def stream_text(chunks):
    return b"".join(c for _t, c in chunks).decode("utf-8", "replace")


# --------------------------------------------------------------------------
# tests
# --------------------------------------------------------------------------

def test_priority_is_config_order():
    """The first endpoint in the config wins — with no name special-cased.

    Run the same two healthy endpoints in both orders; the answer must follow
    the config, not the endpoint's name or URL.
    """
    a = FakeAzure("alpha").start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, _ = ask(p)
            assert status == 200, status
            assert who(body) == "alpha", who(body)
            assert b.hits == 0, "second endpoint should not have been touched"
        finally:
            p.close()

        a.reset(); b.reset()
        p = Proxy([("beta", b.url), ("alpha", a.url)])       # reversed
        try:
            status, body, _ = ask(p)
            assert status == 200, status
            assert who(body) == "beta", who(body)
            assert a.hits == 0
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_failover_on_429():
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, headers = ask(p)
            assert status == 200, (status, body)
            assert who(body) == "beta", who(body)
            assert a.hits == 1
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_failover_on_500():
    a = FakeAzure("alpha", [Behaviour(status=500)]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, _ = ask(p)
            assert status == 200, (status, body)
            assert who(body) == "beta"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_failover_on_dead_host():
    """A connection error is a failover trigger, not an error to return."""
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("dead", dead_url()), ("beta", b.url)])
        try:
            status, body, _ = ask(p)
            assert status == 200, (status, body)
            assert who(body) == "beta"
        finally:
            p.close()
    finally:
        b.stop()


def test_failover_walks_the_whole_chain():
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta", [Behaviour(status=503)]).start()
    c = FakeAzure("gamma").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url), ("gamma", c.url)])
        try:
            status, body, _ = ask(p)
            assert status == 200, (status, body)
            assert who(body) == "gamma"
            assert (a.hits, b.hits, c.hits) == (1, 1, 1)
        finally:
            p.close()
    finally:
        a.stop(); b.stop(); c.stop()


def test_no_failover_on_4xx():
    """A 400 is the caller's answer, not a reason to try elsewhere.

    Parameters are forwarded untouched, so an upstream complaint about an
    unsupported parameter is the real result — retrying would just produce the
    same 400 from a different endpoint while hiding the first one.
    """
    a = FakeAzure("alpha", [Behaviour(status=400)]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, _ = ask(p)
            assert status == 400, (status, body)
            assert "fake 400" in body["error"]["message"], body
            assert b.hits == 0, "must not fail over on a 4xx"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_no_failover_on_timeout():
    """A slow reasoning call and a hung one are indistinguishable from here."""
    a = FakeAzure("alpha", [Behaviour(delay=3.0)]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], timeout=1)
        try:
            status, body, _ = ask(p)
            assert status == 504, (status, body)
            assert body["error"]["code"] == "upstream_timeout", body
            assert b.hits == 0, "must not retry a timeout elsewhere"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_last_upstream_error_is_relayed_not_synthesised():
    """When every route is rate limited, the caller gets the real 429.

    Synthesising a 503 here would discard Retry-After and the upstream's own
    message, which is exactly what a client's backoff logic needs. The proxy
    only invents a status when no route produced a response at all.
    """
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta", [Behaviour(status=429)]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, headers = ask(p)
            assert status == 429, (status, body)
            assert "from beta" in body["error"]["message"], body
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
            assert (a.hits, b.hits) == (1, 1)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_all_routes_unreachable_gives_synthetic_503():
    """With no response from anywhere, there is nothing to relay."""
    p = Proxy([("dead1", dead_url()), ("dead2", dead_url())])
    try:
        status, body, _ = ask(p)
        assert status == 503, (status, body)
        assert body["error"]["code"] == "all_routes_failed", body
    finally:
        p.close()


def test_max_attempts_caps_the_chain():
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta", [Behaviour(status=429)]).start()
    c = FakeAzure("gamma").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url), ("gamma", c.url)],
                  max_attempts=2)
        try:
            status, _body, _ = ask(p)
            assert status == 429, status
            assert c.hits == 0, "third route is beyond max_attempts_per_request"
        finally:
            p.close()
    finally:
        a.stop(); b.stop(); c.stop()


def test_model_is_rewritten_to_deployment_name():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            status, _body, _ = ask(p)
            assert status == 200
            sent = a.requests[0]
            assert sent["body"]["model"] == DEPLOYMENT, sent["body"]["model"]
            assert DEPLOYMENT in sent["path"], sent["path"]
            assert "test-version" in sent["path"], sent["path"]
        finally:
            p.close()
    finally:
        a.stop()


def test_parameters_pass_through_untouched():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask(p, temperature=0.5, reasoning_effort="low",
                seed=7, some_future_param={"x": 1})
            sent = a.requests[0]["body"]
            assert sent["temperature"] == 0.5, sent
            assert sent["reasoning_effort"] == "low", sent
            assert sent["seed"] == 7, sent
            assert sent["some_future_param"] == {"x": 1}, sent
        finally:
            p.close()
    finally:
        a.stop()


def test_credentials_are_injected_and_client_headers_forwarded():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask(p, headers={"X-Session-ID": "sess-42",
                            "Authorization": "Bearer client-supplied"})
            sent = a.requests[0]["headers"]
            # Harbor sends X-Session-ID; some routers key affinity on it.
            assert sent.get("x-session-id") == "sess-42", sent
            # The caller's credentials must never reach Azure.
            assert sent.get("authorization") == "Bearer test-token-abc", sent
        finally:
            p.close()
    finally:
        a.stop()


def test_unknown_model_is_rejected_locally():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            status, body, _ = p.post({"model": "nope",
                                      "messages": [{"role": "user", "content": "hi"}]})
            assert status == 404, (status, body)
            assert body["error"]["code"] == "model_not_found", body
            assert MODEL in body["error"]["message"], body
            assert a.hits == 0
        finally:
            p.close()
    finally:
        a.stop()


def test_models_endpoint_lists_routes_in_priority_order():
    a = FakeAzure("alpha").start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body = p.get("/v1/models")
            assert status == 200
            entry = next(m for m in body["data"] if m["id"] == MODEL)
            assert entry["routes"] == ["alpha", "beta"], entry["routes"]
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


# --------------------------------------------------------------------------
# balancing
#
# The `capacity` assertions are statistical, so they are written with room to be
# unlucky: each one is several standard deviations wide at the sample size used.
# A run that trips one is a broken weight calculation, not a bad draw.
#
# The `priority_threshold` ones are not statistical at all — the mode is
# deterministic given the ledger, and the exact spill point is the thing worth
# pinning, so they assert on it.
# --------------------------------------------------------------------------

def spread(proxy, n):
    """Send n requests, return {endpoint name: how many it answered}."""
    counts = {}
    for _ in range(n):
        status, body, _ = ask(proxy)
        assert status == 200, (status, body)
        name = who(body)
        counts[name] = counts.get(name, 0) + 1
    return counts


def test_priority_threshold_keeps_a_quiet_run_on_the_top_route():
    """Below the threshold the default mode is strict priority.

    This is half of what makes it a safe default: a benchmark that fits inside
    one endpoint's quota runs entirely on the preferred endpoint, and nothing
    about the run becomes non-deterministic in exchange for a spillover that was
    never needed. 30 requests against a 1000 RPM ceiling is 3% of it.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=1000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            counts = spread(p, 30)
            assert counts == {"alpha": 30}, counts
            assert b.hits == 0, "nothing was busy; nothing should have spilled"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_priority_threshold_spills_over_at_the_threshold():
    """And the other half: it starts spreading BEFORE anything is full.

    alpha's ceiling is 10 requests per window and the threshold is 70%, so the
    spill point is exact and worth asserting exactly: requests 1-7 fill alpha to
    7/10, and from the 8th on its load is no longer below 0.70, so everything
    after that goes to beta. Nothing here waits for a 429 — alpha never refuses
    anything, and it still stops receiving traffic.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=10)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            counts = spread(p, 20)
            assert counts.get("alpha") == 7, counts
            assert counts.get("beta") == 13, counts

            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["sent_requests_in_window"] == 7, alpha
            assert abs(alpha["load"] - 0.7) < 1e-6, alpha
            assert alpha["rate_limited"] == 0, "no 429 was needed"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_priority_threshold_uses_whichever_ceiling_binds_first():
    """Both dimensions are measured; the larger fraction decides.

    alpha has plenty of requests left (1000) and very few tokens (1000, at 200 a
    request). TPM is what binds — as it does in reality, where the live run sat
    at ~27% of TPM and ~8% of RPM — so the spill has to be driven by the token
    ledger even though the request count says there is room for a hundred times
    more traffic.

    Four requests fit under 70% of 1000 tokens; the fifth would not, so it goes
    to beta.
    """
    a = FakeAzure("alpha", total_tokens=200,
                  headers=ratelimit(limit_requests=1000,
                                    limit_tokens=1000)).start()
    b = FakeAzure("beta", total_tokens=200,
                  headers=ratelimit(limit_requests=1000,
                                    limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            counts = spread(p, 12)
            assert 3 <= counts.get("alpha", 0) <= 5, counts
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            # The request dimension is nowhere near the threshold, so if this
            # spilled at all it was the tokens that did it.
            assert alpha["sent_requests_in_window"] / 1000.0 < 0.02, alpha
            assert alpha["load"] >= 0.6, alpha
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_unmeasured_route_is_not_treated_as_saturated():
    """"Unknown" must not mean "full", or a quiet route is starved forever.

    alpha never sends x-ratelimit-*, so its load cannot be computed at all. The
    only safe reading is "not busy": a route has to be tried before it can
    report a ceiling, and a rule that read silence as saturation would make sure
    it never got the chance. Priority order therefore stands.
    """
    a = FakeAzure("alpha").start()              # no x-ratelimit-* ever
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            counts = spread(p, 40)
            assert counts == {"alpha": 40}, counts

            _s, report = p.get("/routes")
            assert report["routes"]["alpha/" + DEPLOYMENT]["load"] is None
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_priority_threshold_steps_over_a_parked_route():
    """A Retry-After outranks an idle ledger.

    The ledger only knows about this proxy's own traffic. When Azure says a
    deployment is at its ceiling, it is describing everyone's, so a route inside
    its parking window is skipped as the head of the chain however little the
    proxy itself has sent there.
    """
    a = FakeAzure("alpha", [Behaviour(status=429, headers={"Retry-After": "30"}),
                            Behaviour()],
                  headers=ratelimit(limit_requests=1000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], demote_seconds=30)
        try:
            status, body, _ = ask(p)            # provokes the 429, fails over
            assert status == 200 and who(body) == "beta", (status, body)

            counts = spread(p, 20)
            # alpha's own load is ~2%, and it is still skipped.
            assert counts == {"beta": 20}, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_strict_priority_ignores_load_entirely():
    """The kill switch. Priority means priority, whatever the ledger says.

    alpha is driven to 200% of its stated ceiling and still takes every request.
    This is how a run in flight gets put back onto behaviour that can be read
    off the config without reasoning about windows or thresholds.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=10)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority")
        try:
            counts = spread(p, 20)
            assert counts == {"alpha": 20}, counts
            assert b.hits == 0
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_capacity_mode_ignores_priority_from_the_first_request():
    """The third mode: no priority at all, just size.

    Distinct from priority_threshold, which would have put all 40 of these on
    alpha — nothing here is anywhere near a threshold. beta is nine times the
    size, so it should take most of them while alpha keeps a real share.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=100000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=900000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1})
        try:
            spread(p, 10)                       # let both report a ceiling
            counts = spread(p, 200)
            share = counts.get("beta", 0) / 200.0
            assert 0.80 < share < 0.98, counts
            assert counts.get("alpha", 0) > 0, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_old_balance_names_still_work():
    """`priority` and `weighted` are what the previous README told people to set.

    A config that used to mean something specific must not silently start
    meaning something else, so the old names map onto the modes they named and
    /healthz reports the canonical one.
    """
    a = FakeAzure("alpha").start()
    try:
        for old, new in (("priority", "strict_priority"),
                         ("weighted", "capacity")):
            p = Proxy([("alpha", a.url)], balance=old)
            try:
                _status, health = p.get("/healthz")
                assert health["balance"] == new, (old, health)
            finally:
                p.close()
    finally:
        a.stop()


def test_an_unknown_balance_mode_does_not_stop_the_proxy():
    """A typo in policy.yaml costs a warning, not a proxy that will not boot."""
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)], balance="nonsense")
        try:
            _status, health = p.get("/healthz")
            assert health["balance"] == "strict_priority", health
            status, _body, _ = ask(p)
            assert status == 200, status
        finally:
            p.close()
    finally:
        a.stop()


def test_token_cost_is_learned_from_the_response():
    """The ledger's token figure comes from usage, not from a fixed guess.

    A request is charged an estimate at dispatch, because that is when Azure
    charges it and the answer is not back yet. What the response says it
    actually cost then corrects both the entry and the per-route ratio every
    later estimate is built from — which is the only way a reasoning model's
    thinking tokens, absent from the request entirely, are ever accounted for.
    """
    a = FakeAzure("alpha", total_tokens=5000,
                  headers=ratelimit(limit_requests=1000,
                                    limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            spread(p, 4)
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["token_samples"] == 4, alpha
            # Four settled requests at 5000 each. The first was dispatched on
            # the seed guess and corrected, so the total is exact.
            assert alpha["sent_tokens_in_window"] == 20000, alpha
        finally:
            p.close()
    finally:
        a.stop()


def test_streamed_token_cost_is_read_off_the_bytes():
    """Including on the streaming path, where nothing may be parsed.

    usage arrives inside the final SSE event of a stream the proxy is forwarding
    verbatim. It is picked up by scanning the bytes as they go past — the same
    look-but-do-not-touch technique as the in-band rate limit check — so the
    ledger works for codex, which never makes a non-streaming call.
    """
    a = FakeAzure("alpha", [Behaviour(events=sse("alpha", extra=2,
                                                 total_tokens=1234))],
                  headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            _status, _headers, chunks, err = stream_responses(p)
            assert err is None, err
            assert "response.completed" in stream_text(chunks)
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["sent_tokens_in_window"] == 1234, alpha
            assert alpha["token_samples"] == 1, alpha
        finally:
            p.close()
    finally:
        a.stop()


def test_load_falls_out_of_the_window():
    """The ledger is a sliding window, not a running total.

    Without expiry a route would be permanently over its threshold after one
    busy minute and never come back.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=10)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], load_window=3)
        try:
            counts = spread(p, 10)
            assert counts.get("beta", 0) > 0, "should have spilled"
            time.sleep(3.5)
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["sent_requests_in_window"] == 0, alpha
            assert alpha["load"] == 0, alpha
            # And traffic comes back to it.
            status, body, _ = ask(p)
            assert who(body) == "alpha", (status, body)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_weighted_split_follows_observed_quota():
    """Weights come from x-ratelimit-limit-*, not from the config order.

    Both endpoints start on the same static weight, so the only thing that can
    pull the split away from 50/50 is what Azure said about their ceilings. The
    small endpoint here is also the priority-1 one — under the old routing it
    would take all of it.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=100,
                                             limit_tokens=100000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=900,
                                            limit_tokens=900000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1})
        try:
            spread(p, 10)               # let both report their ceilings
            counts = spread(p, 300)
            share = counts.get("beta", 0) / 300.0
            # Expected 0.9; sd is 0.017, so this is ~6 sd of slack either way.
            assert 0.80 < share < 0.98, counts
            assert counts.get("alpha", 0) > 0, "the small route is not banned"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_cold_start_falls_back_to_static_weights():
    """No quota headers anywhere: the config table has to carry the split.

    A real endpoint reports its limits on the first response, but until then —
    and forever, if a deployment never sends the headers — the proxy has only
    what policy.yaml told it.
    """
    a = FakeAzure("alpha").start()          # no x-ratelimit-* at all
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 100, "beta": 900})
        try:
            counts = spread(p, 300)
            share = counts.get("beta", 0) / 300.0
            assert 0.80 < share < 0.98, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_429_demotes_the_route_and_it_recovers():
    """A 429 has to outlive the request that provoked it, then expire.

    Failing over is not enough on its own: without a demotion the very next
    request walks into the same wall. Without recovery, one unlucky minute
    costs an endpoint the rest of the run.

    The timings are picked so the two measurements land cleanly on either side
    of the parking window — the burst of 40 takes about a second, well inside
    Retry-After, and the sleep afterwards covers the window plus enough
    halflives to be back at full weight.
    """
    a = FakeAzure("alpha", [Behaviour(status=429, headers={"Retry-After": "2"}),
                            Behaviour()]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1},
                  demote_seconds=2, demote_halflife=0.5)
        try:
            # Provoke the 429. It fails over, and alpha is now parked.
            status, body, _ = ask(p)
            assert status == 200 and who(body) == "beta", (status, body)

            parked = spread(p, 40)
            # Parked means floored at 0.05 against beta's 1.0, i.e. ~4.8%.
            assert parked.get("alpha", 0) <= 8, parked

            # Retry-After was 2s and the weight doubles every 0.5s after that,
            # so the sleep leaves three halflives of slack.
            time.sleep(3.5)
            recovered = spread(p, 40)
            assert recovered.get("alpha", 0) >= 8, recovered
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_weighted_never_tries_the_same_endpoint_twice():
    """Reordering must not turn into resampling.

    The attempt list is a permutation of the routes, so however the draw comes
    out, one request touches each endpoint at most once — the same invariant
    priority mode has always had. Two of the three always fail, so every
    request has to walk past them to reach the one that answers; twenty
    requests is enough to see the order come out several different ways.
    """
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta", [Behaviour(status=503)]).start()
    c = FakeAzure("gamma").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url), ("gamma", c.url)],
                  balance="capacity",
                  static_weights={"alpha": 1, "beta": 1, "gamma": 1})
        try:
            orders = set()
            for _ in range(20):
                before = (a.hits, b.hits, c.hits)
                status, body, _ = ask(p)
                assert status == 200, (status, body)
                assert who(body) == "gamma", who(body)
                after = (a.hits, b.hits, c.hits)
                used = tuple(y - x for x, y in zip(before, after))
                assert max(used) <= 1, used
                assert used[2] == 1, used
                orders.add(used)
            # Not an assertion about randomness so much as a check that this
            # test is exercising more than one path: if the order never varied,
            # the invariant above would be trivially satisfied.
            assert len(orders) > 1, orders
        finally:
            p.close()
    finally:
        a.stop(); b.stop(); c.stop()


def test_low_remaining_quota_reduces_a_routes_share():
    """`remaining` is a brake, and only near the bottom of its range.

    Sampled against the live service these counters read near-full on almost
    every response, so the proxy ignores the top half outright. An endpoint
    genuinely down to a tenth of its window is the case that has to bite.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=100000,
                                             remaining_tokens=10000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=100000,
                                            remaining_tokens=100000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity")
        try:
            spread(p, 10)
            counts = spread(p, 200)
            # Equal ceilings, so priority order and static weights say 50/50.
            # alpha is at 0.1 headroom against a 0.5 high-water mark: 0.2 of
            # its capacity, i.e. an expected share of 1/6.
            share = counts.get("alpha", 0) / 200.0
            assert 0.05 < share < 0.32, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_balance_priority_is_unchanged_by_any_weight():
    """The kill switch. `priority` means priority, whatever the numbers say.

    This is the one that has to keep working: it is how a run in flight gets
    put back on known behaviour without reasoning about weights at all.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=1000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        # Everything points at beta: it has a thousand times the quota and the
        # static table agrees. Priority order still wins.
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="strict_priority",
                  static_weights={"alpha": 1, "beta": 1000})
        try:
            counts = spread(p, 30)
            assert counts == {"alpha": 30}, counts
            assert b.hits == 0, "priority mode must not touch the second route"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_an_unmeasured_route_is_not_starved_by_a_measured_one():
    """Measured and unmeasured routes have to be scored on one scale.

    This is the trap the first draft fell into. Azure quotes TPM in the
    hundreds of thousands and the static table in policy.yaml is a handful of
    RPM figures, so comparing them as raw numbers hands the whole run to
    whichever route happened to answer first: it comes back holding a weight of
    100000, everything else is still sitting on 1, and none of them is ever
    sampled again — so none of them ever reports a ceiling either. Strict
    priority with extra steps.

    Here alpha reports its quota and beta never does. They are the same size,
    so the split has to stay near even.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=100000)).start()
    b = FakeAzure("beta").start()            # never sends x-ratelimit-*
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity")
        try:
            counts = spread(p, 200)
            share = counts.get("beta", 0) / 200.0
            # Expected 0.5, sd 0.035. Anything under 0.3 is the bug, not luck.
            assert 0.35 < share < 0.65, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_static_weights_are_converted_into_measured_units():
    """The cold-start table is a prior about capacity, not a raw weight.

    One route has been measured and one has not, and the table says the
    unmeasured one is three times the size. That ratio is what has to survive
    the unit conversion — the measured route's own limit/prior pair is the
    exchange rate.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=100000)).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 100, "beta": 300})
        try:
            counts = spread(p, 200)
            # alpha measures 100000 against a prior of 100, so beta's prior of
            # 300 is worth 300000: an expected share of 0.75.
            share = counts.get("beta", 0) / 200.0
            assert 0.62 < share < 0.88, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_throttled_200_is_detected_from_the_headers():
    """The regression that matters: a refused 200 is spotted before the body.

    This is how Azure actually refuses a streamed Responses call — HTTP 200 with
    `retry-after` in the headers. Detecting it there rather than by reading the
    stream is what makes the detection independent of what the stream contains,
    and the previous version of this proxy failed in production precisely
    because it depended on that.
    """
    a = FakeAzure("alpha", [Behaviour(events=sse_rate_limited("alpha"),
                                      headers=ratelimit_throttled())]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, headers, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is None, err
            text = stream_text(chunks)
            assert '"from": "beta"' in text, text
            assert "rate_limit_exceeded" not in text, text
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
            assert (a.hits, b.hits) == (1, 1), (a.hits, b.hits)

            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            # Once, not twice: the header check and the body backstop must not
            # both charge for the same refusal.
            assert alpha["rate_limited"] == 1, alpha
            # Retry-After was 4, so the park is Azure's number, not the default.
            assert 0 < alpha["parked_for_seconds"] <= 4.5, alpha
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_throttled_200_is_caught_however_big_the_preamble():
    """The exact production failure, pinned.

    A codex-shaped response.created echoes the request back and runs well past
    any fixed scan window, so the error event that follows it lands outside.
    The first version of this proxy scanned 16KB and let 184 refusals through in
    one run while reporting them as successes. Here the preamble is 64KB — four
    times that window — and the refusal still has to be caught.
    """
    a = FakeAzure("alpha",
                  [Behaviour(events=sse_rate_limited("alpha",
                                                     preamble_padding=65536),
                             headers=ratelimit_throttled())]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, headers, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is None, err
            text = stream_text(chunks)
            assert '"from": "beta"' in text, text
            assert "rate_limit_exceeded" not in text, text
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_oversized_preamble_is_still_counted_without_the_header():
    """And the backstop has to survive it too, header or no header.

    Same 64KB preamble, but Azure sends no retry-after — the shape nobody has
    observed, which is the only reason the body scan still exists. The stream
    reaches the caller (the probe released at its byte bound long before the
    error), so this one cannot be retried; what it must not do is go
    unnoticed, because then the next request walks into the same wall.
    """
    a = FakeAzure("alpha",
                  [Behaviour(events=sse_rate_limited("alpha",
                                                     preamble_padding=65536)),
                   Behaviour()]).start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            _status, _headers, chunks, err = stream_responses(p)
            assert err is None, err
            assert "rate_limit_exceeded" in stream_text(chunks)
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["rate_limited"] == 1, alpha
            assert alpha["penalty"] < 1.0, alpha
        finally:
            p.close()
    finally:
        a.stop()


def test_negative_remaining_tokens_alone_is_not_a_refusal():
    """A counter in deficit is not the same as Azure declining.

    In the live capture one response reported x-ratelimit-remaining-tokens of
    -27978 and streamed to completion. Keying on the sign of that counter would
    have thrown away a perfectly good answer and failed the request over for
    nothing.
    """
    headers = ratelimit(limit_tokens=333000, remaining_tokens=-27978)
    a = FakeAzure("alpha", [Behaviour(events=sse("alpha", extra=2),
                                      headers=headers)]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, hdrs, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is None, err
            assert '"from": "alpha"' in stream_text(chunks)
            assert hdrs.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT
            assert b.hits == 0, "must not fail over on a negative counter alone"
            _s, report = p.get("/routes")
            assert report["routes"]["alpha/" + DEPLOYMENT]["rate_limited"] == 0
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_throttled_200_on_the_buffered_face_also_fails_over():
    """The non-streaming face reads the same header.

    Nothing has been seen to refuse a buffered call this way, but the check is
    one dict lookup and the cost of being wrong is a wasted benchmark run.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled())]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, headers = ask(p)
            assert status == 200, (status, body)
            assert who(body) == "beta", who(body)
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_in_stream_rate_limit_fails_over_before_any_content():
    """Azure's 200-wrapped 429, and the window in which it is still undoable.

    A throttled streaming Responses call comes back as HTTP 200 whose second
    event is `error / rate_limit_exceeded`. Nothing about the status line says
    so, so a proxy that only inspects `resp.status_code` scores it as a success,
    hands the caller a broken stream, and sends the retry straight back to the
    endpoint that is refusing it. That is the loop that produced 5 ApiRateLimit
    failures in 19 tasks with a 429 count of zero.

    The stream having *started* is not the same as the caller having anything.
    Here the error arrives before a single output byte, so nothing has been
    committed to and the request can go somewhere else — which it must do
    invisibly: the client sees one clean stream from beta and no trace of alpha.

    Default balance mode, deliberately: this is a correctness fix about reading
    the upstream's answer, not a balancing feature.
    """
    a = FakeAzure("alpha", [Behaviour(events=sse_rate_limited("alpha"))]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, headers, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is None, err
            text = stream_text(chunks)
            assert '"from": "beta"' in text, text
            assert "response.completed" in text, text
            # Not one byte of the throttled attempt may reach the caller: a
            # client parsing two spliced streams is the failure this whole
            # design is arranged around.
            assert "rate_limit_exceeded" not in text, text
            assert "response.failed" not in text, text
            # One stream, not two spliced together. Counting the SSE event line
            # rather than the phrase, which also appears inside the event's JSON.
            assert text.count("event: response.created") == 1, text
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
            assert (a.hits, b.hits) == (1, 1), (a.hits, b.hits)

            # And it is still counted, so the next request avoids alpha too.
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["rate_limited"] == 1, alpha
            assert alpha["penalty"] < 1.0, alpha
            assert alpha["parked_for_seconds"] > 0, alpha
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_in_stream_rate_limit_after_content_is_relayed_not_retried():
    """The other side of the same line, and the invariant that outranks it.

    Same 200-wrapped throttle, except that real output deltas came first. The
    caller is now parsing a stream, so it is theirs: the error is passed
    through as part of it, byte for byte, and the second endpoint is never
    touched. Counting it is all that is left to do.
    """
    a = FakeAzure("alpha",
                  [Behaviour(events=sse_rate_limited("alpha", after=3),
                             event_delay=0.05)]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, headers, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is None, err
            text = stream_text(chunks)
            # Relayed whole and unaltered, error events and all.
            assert "response.created" in text, text
            assert "chunk0" in text and "chunk2" in text, text
            assert "rate_limit_exceeded" in text, text
            assert "response.failed" in text, text
            assert headers.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT
            assert b.hits == 0, "must not fail over after the first byte"

            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            # Once, not twice: the probe and the relay must not both charge it.
            assert alpha["rate_limited"] == 1, alpha
            assert alpha["penalty"] < 1.0, alpha
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_in_stream_rate_limit_on_the_last_route_is_relayed():
    """With nowhere left to go, the caller gets Azure's own error.

    Inventing a 503 here would throw away the upstream's message and whatever
    Retry-After came with it — the same reasoning as for a real 429 on the last
    route. The single endpoint is tried exactly once, not once per marker.
    """
    a = FakeAzure("alpha", [Behaviour(events=sse_rate_limited("alpha"))]).start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            status, _headers, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is None, err
            text = stream_text(chunks)
            assert "rate_limit_exceeded" in text, text
            assert a.hits == 1, a.hits

            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["rate_limited"] == 1, alpha
        finally:
            p.close()
    finally:
        a.stop()


def test_stream_probe_does_not_delay_the_first_byte():
    """The probe may not turn a streaming interface into a buffering one.

    A stream that starts producing output immediately must reach the caller
    immediately — the probe releases as soon as a non-preamble event goes past,
    so the only thing that waits out the full hold window is a stream that has
    said nothing yet. Measured against a hold of 3s, which is six times the
    shipping default: anything that buffers would show up as a 3s first byte.
    """
    events = sse("alpha", extra=3)
    a = FakeAzure("alpha", [Behaviour(events=events, event_delay=0.3)]).start()
    try:
        p = Proxy([("alpha", a.url)], probe_seconds=3.0)
        try:
            started = time.time()
            _status, _headers, chunks, err = stream_responses(p)
            assert err is None, err
            assert chunks, "no chunks at all"
            first = chunks[0][0] - started
            assert first < 1.5, "first byte took {:.2f}s; probe is buffering".format(
                first)
        finally:
            p.close()
    finally:
        a.stop()


def test_in_stream_rate_limit_moves_the_next_request_elsewhere():
    """The whole point of counting it: the next request goes somewhere else."""
    # Alpha's first reply is throttled and the rest are ordinary, so the
    # demotion has to outlive the response that caused it — which is the claim
    # being tested, and it also lets the chat requests below get JSON back. The
    # error arrives after content, so this exercises the path where failover is
    # not available and the demotion is the only remedy.
    a = FakeAzure("alpha", [Behaviour(events=sse_rate_limited("alpha", after=2),
                                      event_delay=0.05),
                            Behaviour()]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1},
                  demote_seconds=30, demote_halflife=30)
        try:
            # Keep drawing until alpha serves one and gets itself demoted. Each
            # probe gets its own session id: CODEX_BODY asks for encrypted
            # reasoning, so without this they would all be one pinned
            # conversation and only ever reach whichever endpoint drew first —
            # which is affinity working correctly, and not what is under test
            # here.
            for i in range(12):
                stream_responses(p, body=responses_body(
                    stream=True, prompt_cache_key="probe-{}".format(i)))
                _s, report = p.get("/routes")
                if report["routes"]["alpha/" + DEPLOYMENT]["rate_limited"]:
                    break
            else:
                raise AssertionError("alpha never drew a request")

            counts = spread(p, 40)
            assert counts.get("alpha", 0) <= 8, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_a_clean_stream_is_not_mistaken_for_a_rate_limit():
    """The scan must not fire on ordinary traffic, or every route decays."""
    a = FakeAzure("alpha", [Behaviour(events=sse("alpha", extra=4))]).start()
    try:
        p = Proxy([("alpha", a.url)], balance="capacity")
        try:
            _status, _headers, chunks, err = stream_responses(p)
            assert err is None, err
            assert "response.completed" in stream_text(chunks)
            _s, report = p.get("/routes")
            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            assert alpha["rate_limited"] == 0, alpha
            assert alpha["penalty"] == 1.0, alpha
        finally:
            p.close()
    finally:
        a.stop()


def test_routes_endpoint_reports_what_it_observed():
    """The one place that can explain a split after the fact.

    Both routes are rate limited, so the request walks the whole chain and both
    ends of it get recorded — no assertion here depends on which one the
    sampler happened to draw first. Azure sends its x-ratelimit-* block on a
    429 as well as on a 200, which is the case worth pinning: a deployment that
    only ever refuses still tells you how big it is.
    """
    a = FakeAzure("alpha", [Behaviour(status=429, headers=dict(
        ratelimit(limit_requests=100, limit_tokens=100000,
                  remaining_tokens=0), **{"Retry-After": "30"}))]).start()
    b = FakeAzure("beta", [Behaviour(status=429, headers=dict(
        ratelimit(limit_requests=900, limit_tokens=900000,
                  remaining_tokens=0), **{"Retry-After": "30"}))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity")
        try:
            status, _body, _ = ask(p)
            assert status == 429, status
            _status, report = p.get("/routes")
            assert report["balance"] == "capacity", report

            alpha = report["routes"]["alpha/" + DEPLOYMENT]
            beta = report["routes"]["beta/" + DEPLOYMENT]
            for name, entry in (("alpha", alpha), ("beta", beta)):
                assert entry["attempts"] == 1, (name, entry)
                assert entry["rate_limited"] == 1, (name, entry)
                assert entry["penalty"] < 1.0, (name, entry)
                assert entry["parked_for_seconds"] > 20, (name, entry)
                assert entry["last_status"] == "429", (name, entry)

            assert alpha["limit_tokens"] == 100000, alpha
            assert beta["limit_tokens"] == 900000, beta
            assert beta["renewal_seconds"] == 60, beta
            # Equally penalised, equally empty, so the ceilings are all that is
            # left to tell them apart.
            assert beta["weight"] > alpha["weight"], (alpha, beta)

            shares = {r["route"]: r["share"] for r in report["models"][MODEL]}
            assert abs(sum(shares.values()) - 1.0) < 1e-6, shares
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_healthz_reports_the_balance_mode():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)], balance="capacity")
        try:
            _status, health = p.get("/healthz")
            assert health["balance"] == "capacity", health
        finally:
            p.close()
    finally:
        a.stop()


# --------------------------------------------------------------------------
# foreign load
#
# Quota is shared with whoever else holds credentials for the same deployment,
# and they are invisible from here except at one moment: when Azure refuses. A
# throttle says the total hit the ceiling, and our own share of that total is
# already known, so the remainder is theirs.
#
# These drive it through the real thing rather than calling the estimator: the
# fake reports a large ceiling and then throttles anyway, which is exactly the
# "we are barely using it and it is still full" case that cannot be produced on
# demand against the live service.
# --------------------------------------------------------------------------

def route_state(proxy, name="alpha"):
    _s, report = proxy.get("/routes")
    return report["routes"][name + "/" + DEPLOYMENT]


def test_foreign_load_is_estimated_from_a_throttle_at_low_load():
    """The headline case: barely any traffic from us, and still refused.

    alpha's ceiling is 1,000,000 tokens and this proxy has sent a handful of
    tiny requests, so our own load is ~0. Being throttled anyway can only mean
    someone else is consuming the deployment, and the estimate has to say so —
    a proxy that only counts its own traffic reads this as "plenty of room" and
    keeps aiming at it.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=1, limit_tokens=1000000, remaining_tokens=1000000)),
        Behaviour(headers=ratelimit(limit_tokens=1000000))]).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority")
        try:
            status, body, _ = ask(p)            # alpha throttles, beta serves
            assert status == 200, (status, body)

            st = route_state(p)
            assert st["our_load"] < 0.01, st
            # 1 - our_load, i.e. essentially all of it is someone else's.
            assert st["foreign_load"] > 0.98, st
            assert st["total_load"] > 0.98, st
            assert st["foreign_samples"] == 1, st
            assert st["foreign_our_load_at_throttle"] is not None, st
            assert st["foreign_observed_age_seconds"] < 5, st
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_a_throttle_we_caused_ourselves_is_not_blamed_on_others():
    """The other end of the same formula.

    A tiny ceiling that this proxy fills by itself must produce a foreign
    estimate near zero, or the proxy would permanently shrink a route for its
    own traffic and never use the quota it actually owns.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=10)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=10000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority")
        try:
            spread(p, 10)                       # fill alpha's own ledger
            assert route_state(p)["our_load"] >= 1.0, route_state(p)

            # Now it throttles, with our ledger already showing we are the cause.
            a.behaviours = [Behaviour(headers=ratelimit_throttled(
                retry_after=1, limit_tokens=None))]
            a._index = 0
            ask(p)
            st = route_state(p)
            assert st["foreign_load"] < 0.05, st
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def provoke_throttle(proxy, fake, name="alpha", tries=25):
    """Send until `fake` has served one request and revealed its foreign load.

    Capacity mode picks a route per request, so which endpoint sees the first
    call is not deterministic — the test has to keep asking rather than assume.
    """
    for _ in range(tries):
        status, body, _ = ask(proxy)
        assert status == 200, (status, body)
        if route_state(proxy, name)["rate_limited"]:
            return
    raise AssertionError("{} never drew a request".format(name))


def test_capacity_mode_samples_by_what_is_left_not_by_size():
    """A route someone else is using is smaller than its ceiling says.

    Both endpoints report the same 1M ceiling, so the nominal split is 50/50.
    alpha then reveals ~100% foreign load by throttling while we are idle, and
    the traffic has to move to beta — but not all of it, because alpha keeps
    its weight_floor share as a probe.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=1, limit_tokens=1000000, remaining_tokens=1000000))]
        + [Behaviour(headers=ratelimit(limit_tokens=1000000))] * 400).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1}, foreign_reclaim=0.0,
                  demote_seconds=1, demote_halflife=0.5)
        try:
            provoke_throttle(p, a)
            assert route_state(p)["foreign_load"] > 0.9, route_state(p)
            time.sleep(2)               # let the short-term parking expire

            before = a.hits
            counts = spread(p, 200)
            alpha_share = (a.hits - before) / 200.0
            # Nominally 50%. Believed full, so it should collapse towards the
            # floor — but NOT to zero, or nothing would ever discover that the
            # other tenant had left.
            assert alpha_share < 0.25, counts
            assert alpha_share > 0.0, "the probe channel was starved shut"
            assert counts.get("beta", 0) > 140, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_priority_threshold_spills_on_total_not_just_our_own():
    """Spillover has to count everyone's traffic, not only ours.

    alpha is priority 1 with room to spare by our own reckoning — one request
    against a 1000-request ceiling — but a throttle reveals the deployment is
    already full. It must stop being the head of the chain even though our own
    ledger says 0.1% load.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=1, limit_requests=1000, limit_tokens=1000000,
        remaining_tokens=1000000))]
        + [Behaviour(headers=ratelimit(limit_requests=1000))] * 100).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="priority_threshold", foreign_reclaim=0.0)
        try:
            ask(p)
            st = route_state(p)
            assert st["our_load"] < 0.02, st
            assert st["total_load"] > 0.7, st

            before = a.hits
            counts = spread(p, 30)
            assert counts.get("beta", 0) == 30, counts
            assert a.hits == before, "priority 1 was over threshold; no spill"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_foreign_load_is_reclaimed_linearly():
    """It has to come back down, or one bad minute costs the route forever.

    Nothing reports that a foreign job has ended, so the only way to find out is
    to stop believing the old estimate at a steady rate and let real traffic
    rediscover the capacity. At 6.0 per minute a full estimate is gone in ten
    seconds, which is the same law the shipping 0.1 uses, run fast enough to
    test.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=0, limit_tokens=1000000, remaining_tokens=1000000))]
        + [Behaviour(headers=ratelimit(limit_tokens=1000000))] * 50).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority", foreign_reclaim=6.0)
        try:
            ask(p)
            first = route_state(p)["foreign_load"]
            assert first > 0.9, first

            time.sleep(3)
            middle = route_state(p)["foreign_load"]
            # 3s at 6.0/min = 0.3 reclaimed. Linear, so this is predictable
            # rather than merely "smaller".
            assert 0.5 < middle < 0.85, (first, middle)

            time.sleep(8)
            assert route_state(p)["foreign_load"] == 0.0, route_state(p)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_reclaim_waits_for_retry_after():
    """Azure said how long the congestion lasts; reclaiming during it is noise."""
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=4, limit_tokens=1000000, remaining_tokens=-5))]
        + [Behaviour(headers=ratelimit(limit_tokens=1000000))] * 50).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority", foreign_reclaim=6.0)
        try:
            ask(p)
            st = route_state(p)
            assert st["foreign_reclaim_starts_in"] > 2.5, st
            held = st["foreign_load"]

            time.sleep(2)               # still inside Retry-After
            assert route_state(p)["foreign_load"] == held, route_state(p)

            time.sleep(4)               # past it, reclaim has started
            assert route_state(p)["foreign_load"] < held, route_state(p)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_a_persistent_foreign_load_settles_instead_of_relearning():
    """Steady outside pressure should produce a steady estimate.

    The multiplicative-decrease half takes max() with what is already held
    precisely so that a throttle arriving while our own load happens to be high
    cannot erase what an earlier one established. Without it the estimate
    sawtooths between the real value and nearly nothing, and the proxy spends
    every cycle relearning the same fact.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=0, limit_tokens=1000000, remaining_tokens=1000000))] * 200).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority", foreign_reclaim=0.5)
        try:
            seen = []
            for _ in range(6):
                provoke_throttle(p, a)
                seen.append(route_state(p)["foreign_load"])
                time.sleep(0.3)
            assert all(v > 0.9 for v in seen), seen
            # Stable, not sawtoothing: no sample collapses and rebuilds.
            assert max(seen) - min(seen) < 0.1, seen
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_capacity_returns_when_the_foreign_load_goes_away():
    """The loop has to close: reclaim, then real traffic proves it is free.

    alpha throttles once, loses almost all its weight, and then stops
    throttling — which is what a foreign job finishing looks like from here.
    After the estimate is reclaimed the split must return to even, because the
    probe traffic that kept flowing is what discovers it.
    """
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=0, limit_tokens=1000000, remaining_tokens=1000000))]
        + [Behaviour(headers=ratelimit(limit_tokens=1000000))] * 500).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1}, foreign_reclaim=6.0,
                  demote_seconds=1, demote_halflife=0.5)
        try:
            provoke_throttle(p, a)
            assert route_state(p)["foreign_load"] > 0.9, route_state(p)

            time.sleep(12)              # 6.0/min for 12s reclaims everything
            st = route_state(p)
            assert st["foreign_load"] == 0.0, st

            before = a.hits
            counts = spread(p, 200)
            share = (a.hits - before) / 200.0
            assert 0.3 < share < 0.7, (counts, share)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_foreign_load_can_be_switched_off():
    """Reverts to counting only our own traffic."""
    a = FakeAzure("alpha", [Behaviour(headers=ratelimit_throttled(
        retry_after=0, limit_tokens=1000000, remaining_tokens=1000000))]
        + [Behaviour(headers=ratelimit(limit_tokens=1000000))] * 400).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=1000000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1}, foreign=False,
                  demote_seconds=1, demote_halflife=0.5)
        try:
            provoke_throttle(p, a)
            st = route_state(p)
            assert st["foreign_load"] == 0.0, st
            assert st["total_load"] == st["our_load"], st

            time.sleep(2)               # let the demote parking expire
            before = a.hits
            counts = spread(p, 200)
            share = (a.hits - before) / 200.0
            # Equal ceilings and no foreign model: back to an even split.
            assert 0.3 < share < 0.7, (counts, share)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_foreign_load_is_per_deployment_not_per_endpoint():
    """Quota is granted to a deployment; x-ratelimit-key names one.

    Someone else saturating a different deployment on the same endpoint says
    nothing about ours, so the estimate must not be shared between them.
    """
    sys.path.insert(0, ROOT)
    import logging
    from proxy.server import QuotaTracker, Route
    logging.getLogger("azure-proxy").setLevel(logging.CRITICAL)

    class Cfg:
        balance = "capacity"
        weight_floor = 0.05
        headroom_high_water = 0.5
        observation_ttl = 120
        demote_multiplier = 0.25
        demote_seconds = 30
        demote_halflife = 30
        static_weights = {}
        spill_threshold = 0.7
        load_window = 60
        chars_per_token = 4
        foreign_enabled = True
        foreign_reclaim = 0.0

    q = QuotaTracker(Cfg())
    big = Route("alpha", "http://x/", "v", "gpt-5.6-sol",
                "max_completion_tokens", 0)
    other = Route("alpha", "http://x/", "v", "gpt-4o-mini",
                  "max_completion_tokens", 0)
    for r in (big, other):
        q.observed(r, 200, {"x-ratelimit-limit-tokens": "1000000"})

    q.demote(other, "429", "1")             # a different deployment throttles
    now = time.time()
    assert q.foreign_load(q.state(other), now) > 0.9
    assert q.foreign_load(q.state(big), now) == 0.0, "estimate leaked across"


# --------------------------------------------------------------------------
# session affinity
#
# The encrypted reasoning blob codex asks for is bound to the endpoint that
# produced it: hand it back to a different one and the answer is "Encrypted
# content could not be decrypted", which ends the trial. So a conversation that
# carries that state has to stay where it started, and everything that moves
# requests around — capacity sampling, threshold spillover, throttle failover —
# has to stop doing so for those requests only.
# --------------------------------------------------------------------------

CODEX_SESSION = "01a0216b-cf47-7542-95ad-7d524fbb1582"


def codex_turn(session=CODEX_SESSION, stream=False, **kw):
    """A request shaped like a real codex turn, with its session markers.

    The two that matter are `include: [reasoning.encrypted_content]`, which is
    what makes it sticky, and the session id, which is what it sticks by.
    """
    body = responses_body(stream=stream, **kw)
    body["include"] = ["reasoning.encrypted_content"]
    body["prompt_cache_key"] = session
    return body


def sticky_ask(proxy, session=CODEX_SESSION, header=True, **kw):
    headers = {"session-id": session} if header else None
    return proxy.post(codex_turn(session, **kw), headers=headers,
                      path="/v1/responses")


def test_encrypted_session_sticks_to_one_endpoint():
    """The whole point: every turn of a conversation lands on one endpoint.

    Capacity mode with two equal endpoints would otherwise split these roughly
    down the middle, and every request that moved would hand codex an encrypted
    blob the new endpoint cannot decrypt. Twenty turns, one endpoint.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1})
        try:
            seen = set()
            for _ in range(20):
                status, body, headers = sticky_ask(p)
                assert status == 200, (status, body)
                seen.add(headers.get("x-azure-proxy-route"))
            assert len(seen) == 1, "session was split across {}".format(seen)
            # And one of the two endpoints never saw it at all.
            assert 0 in (a.hits, b.hits), (a.hits, b.hits)

            _s, health = p.get("/healthz")
            aff = health["session_affinity"]
            assert aff["live_sessions"] == 1, aff
            assert sum(aff["sessions_per_endpoint"].values()) == 1, aff
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_different_sessions_still_spread():
    """Affinity pins a conversation, it does not pin the proxy.

    Each session is placed by the balancer when it starts and held afterwards,
    so load is still shared — just at session granularity instead of request
    granularity. Without this, affinity would collapse into strict priority.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1})
        try:
            routes = {}
            for i in range(40):
                session = "session-{}".format(i)
                _s, _b, headers = sticky_ask(p, session=session)
                routes[session] = headers.get("x-azure-proxy-route")
            used = set(routes.values())
            assert len(used) == 2, "sessions all went to {}".format(used)
            share = sum(1 for v in routes.values() if "alpha" in v) / 40.0
            # Expected 0.5, sd 0.079. Generous, but a broken implementation
            # pins everything to one endpoint and lands at 0.0 or 1.0.
            assert 0.2 < share < 0.8, routes
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_requests_without_encrypted_reasoning_are_never_pinned():
    """mini-swe-agent and the chat face must not pay for codex's problem.

    They send no `include`, so they carry no endpoint-bound state and keep the
    request-by-request balancing they had before affinity existed.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=100000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=900000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 9})
        try:
            # Same session id on every request, but no encrypted reasoning.
            counts = {}
            for _ in range(60):
                _s, body, _h = p.post(
                    dict({"model": MODEL,
                          "messages": [{"role": "user", "content": "hi"}]}),
                    headers={"session-id": CODEX_SESSION})
                name = who(body)
                counts[name] = counts.get(name, 0) + 1
            assert len(counts) == 2, "balancing was suppressed: {}".format(counts)
            assert counts.get("beta", 0) > counts.get("alpha", 0), counts

            _s, health = p.get("/healthz")
            assert health["session_affinity"]["live_sessions"] == 0, health
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_upstream_state_pins_even_without_encrypted_reasoning():
    """`previous_response_id` and `store: true` are endpoint-bound too.

    Nothing in this setup sends them today — codex uses `store: false` and
    resends the transcript — but a response object lives on exactly one
    endpoint. Dereferencing it anywhere else is a 404, and minting it without
    pinning puts it somewhere nobody chose. Both would surface as an upstream
    outage rather than a routing bug, which is the expensive kind of mistake.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=100000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=900000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 9})
        try:
            for label, extra in (("store", {"store": True}),
                                 ("previous", {"previous_response_id": "resp_1"})):
                session = "{}-{}".format(CODEX_SESSION, label)
                seen = set()
                for _ in range(25):
                    _s, body, _h = p.post(
                        dict({"model": MODEL, "input": "hi"}, **extra),
                        headers={"session-id": session},
                        path="/v1/responses")
                    seen.add(who_responses(body))
                assert len(seen) == 1, \
                    "{} was spread across {}".format(label, seen)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_pinned_session_waits_out_a_throttle_instead_of_moving():
    """The conflict, resolved the way correctness demands.

    alpha is pinned and then starts refusing with the 200+retry-after throttle.
    Failing over would be faster and would guarantee a decryption error on the
    next turn — a lost trial rather than a slow one. So the request waits and
    re-attempts alpha, and beta is never touched.
    """
    a = FakeAzure("alpha", [Behaviour(),                       # pins here
                            Behaviour(headers=ratelimit_throttled(retry_after=1)),
                            Behaviour()]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 0.0001},
                  affinity_attempts=4, affinity_max_wait=3)
        try:
            _s, _b, headers = sticky_ask(p)
            assert headers.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT

            # Second turn is throttled; it must come back from alpha anyway.
            status, body, headers = sticky_ask(p, timeout=60)
            assert status == 200, (status, body)
            assert headers.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT
            assert who_responses(body) == "alpha", body
            assert b.hits == 0, "a pinned session must not move on a throttle"
            assert a.hits == 3, a.hits
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_first_turn_of_a_sticky_session_can_still_fail_over():
    """Before anything is produced there is no state to protect.

    The opening turn has to be free to move, or a throttled endpoint would
    strand a conversation that had not even started. The pin follows the
    response, not the request.
    """
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)],
                  balance="strict_priority")
        try:
            status, body, headers = sticky_ask(p)
            assert status == 200, (status, body)
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT

            # ...and it is now pinned to where it actually succeeded.
            for _ in range(5):
                _s, _b, headers = sticky_ask(p)
                assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_affinity_survives_the_inband_throttle_failover():
    """The regression this feature exists to prevent.

    The in-band throttle failover added earlier moves a streaming request to
    another endpoint before the first byte. That is exactly right for a stateless
    caller and exactly wrong for codex, because the reply it salvages carries
    encrypted reasoning minted somewhere else. Once pinned, it must not fire.
    """
    a = FakeAzure("alpha", [Behaviour(events=sse("alpha")),         # pins here
                            Behaviour(events=sse_rate_limited("alpha"),
                                      headers=ratelimit_throttled(retry_after=1)),
                            Behaviour(events=sse("alpha"))]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 0.0001},
                  affinity_attempts=4, affinity_max_wait=3)
        try:
            body = codex_turn(stream=True)
            _st, headers, _c, err = stream_responses(p, body=body)
            assert err is None, err
            assert headers.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT

            _st, headers, chunks, err = stream_responses(p, body=body,
                                                         timeout=60)
            assert err is None, err
            assert headers.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT
            assert '"from": "alpha"' in stream_text(chunks)
            assert b.hits == 0, "throttle failover moved a pinned session"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_affinity_can_be_switched_off():
    """The escape hatch back to pure balancing."""
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1}, affinity=False)
        try:
            seen = set()
            for _ in range(40):
                _s, _b, headers = sticky_ask(p)
                seen.add(headers.get("x-azure-proxy-route"))
            assert len(seen) == 2, "affinity=false should still spread: {}".format(
                seen)
            _s, health = p.get("/healthz")
            assert health["session_affinity"]["enabled"] is False, health
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_session_id_falls_back_to_the_body_when_there_is_no_header():
    """codex sends both; something else may send only one.

    prompt_cache_key carried the same value as the session-id header in every
    captured request, so it is a usable fallback rather than a guess.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1})
        try:
            seen = set()
            for _ in range(20):
                _s, _b, headers = sticky_ask(p, header=False)   # body key only
                seen.add(headers.get("x-azure-proxy-route"))
            assert len(seen) == 1, "body key did not pin: {}".format(seen)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_a_pin_to_a_vanished_endpoint_is_not_honoured_or_destroyed():
    """A pin must not strand a session, and reading it must not erase it.

    If a re-probe drops the pinned endpoint, the encrypted state is lost
    whatever happens — so refusing to route at all would turn one broken turn
    into a broken run. The pin is not honoured and the request goes somewhere
    that works.

    It is not deleted, though. A lookup that declines to use an entry has no
    business destroying it: the next successful response overwrites the slot
    anyway, and deleting on read is how one unusable lookup becomes a session
    that has silently lost its binding and gets balanced onto a deployment that
    cannot decrypt what it carries. Driven against the map directly; a pin that
    corresponds to no route is awkward to stage over HTTP.
    """
    sys.path.insert(0, ROOT)
    import logging
    from proxy.server import Route, SessionAffinity
    # In-process, so the proxy's own logger would write into the test output.
    logging.getLogger("azure-proxy").setLevel(logging.CRITICAL)

    class Cfg:
        affinity_enabled = True
        affinity_keys = ["header:session-id", "body:prompt_cache_key"]
        affinity_markers = ["reasoning.encrypted_content"]
        affinity_ttl = 3600
        affinity_max = 16
        affinity_on_conflict = "wait"
        affinity_off_route = "strip"

    def route(name):
        return Route(name, "http://x/", "v", DEPLOYMENT, "max_completion_tokens",
                     0, "openai/v1/responses")

    aff = SessionAffinity(Cfg())
    gone, live = route("gone"), route("live")
    aff.pin("s1", gone)
    assert aff.pinned("s1", [gone, live]) is gone
    # The endpoint disappears from the model's route list.
    assert aff.pinned("s1", [live]) is None, "a dead pin must not be honoured"
    # The entry survives being declined, so nothing else can be unpinned by it.
    assert aff.report()["live_sessions"] == 1, aff.report()
    # And it is still the binding if the endpoint comes back.
    assert aff.pinned("s1", [gone, live]) is gone
    aff.pin("s1", live)
    assert aff.pinned("s1", [live]) is live


def test_two_models_in_one_session_do_not_share_a_pin():
    """One conversation id, two models, two bindings.

    A pin binds one *deployment's* encrypted reasoning, and a session id does
    not have to cover only one model. Measured on 2026-08-25: a gpt-5.6-sol
    session and a gpt-5.6-terra session held the same slot and took turns
    overwriting it, so each model's turns kept finding the other's route,
    declining it, and being balanced onto a deployment that could not decrypt
    what they carried. Every one of those turns was a 400
    `invalid_encrypted_content`, which codex reports as `turn.failed`.

    So the model belongs in the key. Two slots, no interference.
    """
    sys.path.insert(0, ROOT)
    import logging
    from proxy.server import Route, SessionAffinity
    logging.getLogger("azure-proxy").setLevel(logging.CRITICAL)

    class Cfg:
        affinity_enabled = True
        affinity_keys = ["header:session-id", "body:prompt_cache_key"]
        affinity_markers = ["reasoning.encrypted_content"]
        affinity_ttl = 3600
        affinity_max = 16
        affinity_on_conflict = "wait"
        affinity_off_route = "strip"

    class Req:
        def __init__(self, headers):
            self.headers = headers

    def route(endpoint, deployment):
        return Route(endpoint, "http://x/", "v", deployment,
                     "max_completion_tokens", 0, "openai/v1/responses")

    aff = SessionAffinity(Cfg())
    request = Req({"session-id": CODEX_SESSION})
    sol = {"model": "sol", "include": ["reasoning.encrypted_content"]}
    terra = {"model": "terra", "include": ["reasoning.encrypted_content"]}

    k_sol, k_terra = aff.key(request, sol), aff.key(request, terra)
    assert k_sol and k_terra
    assert k_sol != k_terra, "one slot for two models: {}".format(k_sol)

    a, b = route("alpha", "sol"), route("beta", "terra")
    aff.pin(k_sol, a)
    aff.pin(k_terra, b)
    # Neither model's route list contains the other's deployment, which is what
    # used to make each lookup discard the other's pin.
    assert aff.pinned(k_sol, [a]) is a
    assert aff.pinned(k_terra, [b]) is b
    assert aff.pinned(k_sol, [a]) is a, "terra's turn unpinned sol"
    assert aff.report()["live_sessions"] == 2, aff.report()


def codex_reasoning_item():
    """The item that only one deployment can read, as codex sends it back."""
    return {"type": "reasoning", "id": "rs_03a861b6bc394df2016a8d4bfefb3c81",
            "summary": [], "encrypted_content": "gAAAAABo" + "x" * 64}


def test_a_turn_off_its_pinned_deployment_keeps_its_reasoning():
    """On the route that minted it, the blob is forwarded untouched.

    This is the case the whole mechanism exists to produce, and it is here so
    that the stripping test below cannot pass by stripping everything.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  static_weights={"alpha": 1, "beta": 1})
        try:
            # Turn one carries no reasoning yet; it is what creates the pin.
            status, _body, _h = sticky_ask(p)
            assert status == 200, status
            # Turn two comes back with the blob, and is pinned, so it is on the
            # only deployment that can read it.
            body = codex_turn(CODEX_SESSION)
            body["input"] = list(body["input"]) + [codex_reasoning_item()]
            status, _body, headers = p.post(
                body, headers={"session-id": CODEX_SESSION},
                path="/v1/responses")
            assert status == 200, status

            served = a if a.hits else b
            sent = served.requests[-1]["body"]["input"]
            kept = [i for i in sent if i.get("type") == "reasoning"]
            assert len(kept) == 1, "the pinned deployment lost its reasoning"
            assert kept[0]["encrypted_content"], kept[0]
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_a_turn_off_its_pinned_deployment_loses_reasoning_not_the_run():
    """Unpinned, the blob comes off rather than killing the turn.

    A session whose pin has expired, or whose deployment a re-probe took away,
    still carries reasoning only its old deployment can decrypt. Forwarding it
    earns a 400 `invalid_encrypted_content`, which codex reports as
    `turn.failed` and does not retry — the run ends. Dropping the item costs
    the turn its reasoning context and nothing else.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            # A session the proxy has never seen, already carrying a blob.
            body = codex_turn("a-session-with-no-pin-here")
            body["input"] = list(body["input"]) + [codex_reasoning_item()]
            status, _body, _h = p.post(
                body, headers={"session-id": "a-session-with-no-pin-here"},
                path="/v1/responses")
            assert status == 200, status

            sent = a.requests[-1]["body"]["input"]
            assert not [i for i in sent if i.get("type") == "reasoning"], sent
            # Only the unreadable item goes; the conversation is intact.
            assert len(sent) == len(body["input"]) - 1, sent

            # A silent fallback is a fallback nobody notices has become the
            # normal case, so it has to be visible without reading the log: a
            # counter to check after a run, and a `problems` event during one.
            _s, health = p.get("/healthz")
            aff = health["session_affinity"]
            assert aff["stripped_turns"] == 1, aff
            assert aff["off_route"] == "strip", aff

            _s, seen = p.get("/events?kind=problems&limit=200")
            stripped = [e for e in seen["events"] if e["kind"] == "stripped"]
            assert len(stripped) == 1, seen["events"]
            assert stripped[0]["level"] == "warning", stripped[0]
            assert stripped[0]["stripped"] == 1, stripped[0]
            assert stripped[0]["endpoint"] == "alpha", stripped[0]
        finally:
            p.close()
    finally:
        a.stop()


def test_off_route_send_forwards_the_blob_untouched():
    """`off_route: send` is the escape hatch back to letting Azure refuse it."""
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url)], affinity_off_route="send")
        try:
            body = codex_turn("another-session-with-no-pin")
            body["input"] = list(body["input"]) + [codex_reasoning_item()]
            status, _body, _h = p.post(
                body, headers={"session-id": "another-session-with-no-pin"},
                path="/v1/responses")
            assert status == 200, status

            sent = a.requests[-1]["body"]["input"]
            kept = [i for i in sent if i.get("type") == "reasoning"]
            assert len(kept) == 1, sent
        finally:
            p.close()
    finally:
        a.stop()


def test_affinity_evicts_by_ttl_and_count():
    """The map is bounded in both directions, or a long-lived proxy leaks."""
    sys.path.insert(0, ROOT)
    import logging
    from proxy.server import Route, SessionAffinity
    # In-process, so the proxy's own logger would write into the test output.
    logging.getLogger("azure-proxy").setLevel(logging.CRITICAL)

    class Cfg:
        affinity_enabled = True
        affinity_keys = ["header:session-id"]
        affinity_markers = ["reasoning.encrypted_content"]
        affinity_ttl = 3600
        affinity_max = 5
        affinity_on_conflict = "wait"
        affinity_off_route = "strip"

    r = Route("alpha", "http://x/", "v", DEPLOYMENT, "max_completion_tokens", 0)
    aff = SessionAffinity(Cfg())
    for i in range(20):
        aff.pin("s{}".format(i), r)
    assert aff.report()["live_sessions"] <= 6, aff.report()
    # The most recent survives; the oldest is gone.
    assert aff.pinned("s19", [r]) is r
    assert aff.pinned("s0", [r]) is None

    Cfg.affinity_ttl = -1                   # everything is already stale
    assert aff.pinned("s19", [r]) is None
    assert aff.report()["live_sessions"] == 0


# --------------------------------------------------------------------------
# responses face
# --------------------------------------------------------------------------

def test_content_type_is_sent_once_on_both_faces():
    """Forwarding the client's content-type does not override ours — it joins it.

    httpx merges the two into `application/json,application/json`. chat/completions
    tolerates that; the Responses API answers 400 unsupported_content_type. The
    body is re-serialised here, so its framing headers are the proxy's to set.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask(p, headers={"Content-Type": "application/json"})
            ask_responses(p)
            for sent in a.requests:
                got = sent["headers"].get("content-type")
                assert got == "application/json", (sent["path"], got)
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_non_streaming_works():
    """mini-swe-agent goes through litellm.responses(), which does not stream."""
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            status, body, headers = ask_responses(p)
            assert status == 200, (status, body)
            assert body["object"] == "response", body
            assert who_responses(body) == "alpha", body
            assert headers.get("x-azure-proxy-route") == "alpha/" + DEPLOYMENT
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_target_has_no_deployment_in_the_path():
    """The Responses API takes the deployment from the body, not the URL.

    Getting this wrong is not a subtle failure — it is a 404 on every call —
    but it is the one structural difference from chat/completions, so it is
    worth pinning rather than assuming.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask_responses(p)
            sent = a.requests[0]
            assert sent["path"] == "/" + RESPONSES_PATH, sent["path"]
            assert DEPLOYMENT not in sent["path"], sent["path"]
            assert sent["body"]["model"] == DEPLOYMENT, sent["body"]
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_unknown_fields_pass_through_untouched():
    """Everything but `model` must reach Azure exactly as the client sent it.

    A whitelist here would look harmless and quietly break codex: dropping
    `include` costs it the encrypted reasoning it needs to stay coherent across
    turns, and its tools ride inside `input`, not in the top-level `tools`.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask_responses(p)
            sent = a.requests[0]["body"]
            expected = responses_body(stream=False)
            expected["model"] = DEPLOYMENT
            assert sent == expected, "upstream body differs:\n{}\n{}".format(
                json.dumps(sent, indent=2, sort_keys=True),
                json.dumps(expected, indent=2, sort_keys=True))
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_compat_fixes_what_azure_rejects():
    """The two rewrites, on the shape codex actually sends.

    Without them codex fails on every turn — both trials of a one-task run died
    in NonZeroAgentExitCodeError. Azure answers `empty_string` for the namespace
    tool's blank description and `unknown_parameter` for the create_time inside
    codex's private metadata; the real OpenAI backend accepts both.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            status, _body, _ = p.post(strict_body(), path="/v1/responses")
            assert status == 200, status

            item = a.requests[0]["body"]["input"][0]
            assert "internal_chat_message_metadata_passthrough" not in item, item

            namespace = item["tools"][0]
            assert namespace["description"] == FILLER, namespace
            # Nested, not just the top of the tool tree.
            assert namespace["tools"][1]["description"] == FILLER, namespace
            # A description that was already there is not overwritten.
            assert namespace["tools"][0]["description"] == "run a shell command"
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_compat_changes_nothing_else():
    """Scoped to those two fields. Anything wider would break comparability.

    The point of the proxy is that a benchmark number means what it says, so
    the diff against what the client sent has to be exactly the two documented
    rewrites plus the deployment name — no more.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            p.post(strict_body(), path="/v1/responses")
            sent = a.requests[0]["body"]

            expected = strict_body()
            expected["model"] = DEPLOYMENT
            item = expected["input"][0]
            del item["internal_chat_message_metadata_passthrough"]
            item["tools"][0]["description"] = FILLER
            item["tools"][0]["tools"][1]["description"] = FILLER

            assert sent == expected, "upstream body differs:\n{}\n{}".format(
                json.dumps(sent, indent=2, sort_keys=True),
                json.dumps(expected, indent=2, sort_keys=True))
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_compat_can_be_turned_off():
    """The escape hatch: off means you see exactly what Azure sees.

    Worth keeping working — the way these two rewrites were found in the first
    place was replaying an unmodified body and reading the backend's complaint.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)], responses_compat=False)
        try:
            p.post(strict_body(), path="/v1/responses")
            sent = a.requests[0]["body"]

            expected = strict_body()
            expected["model"] = DEPLOYMENT
            assert sent == expected, json.dumps(sent, indent=2, sort_keys=True)
        finally:
            p.close()
    finally:
        a.stop()


def test_chat_face_is_never_rewritten():
    """The rewrites belong to /v1/responses. chat/completions stays passthrough.

    Nothing on the chat face has ever needed them, and a blank tool description
    there is the caller's business.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask(p, tools=[{"type": "function",
                           "function": {"name": "x", "description": ""}}],
                internal_chat_message_metadata_passthrough={"turn_id": "t"})
            sent = a.requests[0]["body"]
            assert sent["tools"][0]["function"]["description"] == "", sent
            assert sent["internal_chat_message_metadata_passthrough"] == {
                "turn_id": "t"}, sent
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_failover_on_429():
    a = FakeAzure("alpha", [Behaviour(status=429)]).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, _ = ask_responses(p)
            assert status == 200, (status, body)
            assert who_responses(body) == "beta", body
            assert a.hits == 1 and b.hits == 1, (a.hits, b.hits)
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_responses_stream_fails_over_before_the_first_byte():
    """A retryable status arrives with the headers, before any body is sent.

    So a streaming request can still fail over — as long as it happens here and
    nowhere later.
    """
    a = FakeAzure("alpha", [Behaviour(status=500)]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, headers, chunks, err = stream_responses(p)
            assert err is None, err
            assert status == 200, status
            assert "text/event-stream" in headers.get("content-type", ""), headers
            assert headers.get("x-azure-proxy-route") == "beta/" + DEPLOYMENT
            text = stream_text(chunks)
            assert "response.completed" in text, text
            assert '"from": "beta"' in text, text
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_responses_stream_is_not_retried_once_it_has_started():
    """The one failure that must NOT fail over.

    Retrying after the client already holds bytes would splice a second stream
    onto the first inside its parser. The break is passed on as a break instead,
    and the second endpoint is never touched.
    """
    a = FakeAzure("alpha", [Behaviour(events=sse("alpha", extra=3),
                                      cut_after=2)]).start()
    b = FakeAzure("beta", [Behaviour(events=sse("beta"))]).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, _headers, chunks, err = stream_responses(p)
            assert status == 200, status
            assert err is not None, "a truncated stream should surface as an error"
            text = stream_text(chunks)
            assert "response.created" in text, text
            assert "response.completed" not in text, "stream should be truncated"
            assert b.hits == 0, "must not fail over after the first byte"
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_responses_stream_arrives_incrementally():
    """Events must reach the client as they are produced, not in one lump.

    A buffered relay passes every other test in this file and still makes codex
    sit in silence until generation finishes.
    """
    events = sse("alpha", extra=2)
    a = FakeAzure("alpha", [Behaviour(events=events, event_delay=0.4)]).start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            _status, _headers, chunks, err = stream_responses(p)
            assert err is None, err
            assert len(chunks) >= 2, "arrived as one lump: {}".format(len(chunks))
            spread = chunks[-1][0] - chunks[0][0]
            assert spread > 0.3, "chunks arrived {:.2f}s apart; buffered".format(
                spread)
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_route_absent_is_distinguished_from_unknown_model():
    """Azure grants the two faces separately, so this is a real state.

    Answering `model_not_found` would send someone hunting for a typo in a name
    that is correct and working on the other face.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)], responses=False)
        try:
            status, body, _ = ask_responses(p)
            assert status == 404, (status, body)
            assert body["error"]["code"] == "no_responses_route", body
            assert "chat/completions" in body["error"]["message"], body
            assert a.hits == 0, "should be rejected locally"

            # The same proxy still serves the model on the chat face.
            status, body, _ = ask(p)
            assert status == 200, (status, body)
        finally:
            p.close()
    finally:
        a.stop()


def test_responses_unknown_model_is_still_model_not_found():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            status, body, _ = ask_responses(p, model="nope")
            assert status == 404, (status, body)
            assert body["error"]["code"] == "model_not_found", body
            assert a.hits == 0
        finally:
            p.close()
    finally:
        a.stop()


def test_models_endpoint_reports_both_faces():
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            _status, body = p.get("/v1/models")
            entry = next(m for m in body["data"] if m["id"] == MODEL)
            assert entry["faces"] == ["chat", "responses"], entry
        finally:
            p.close()

        p = Proxy([("alpha", a.url)], responses=False)
        try:
            _status, body = p.get("/v1/models")
            entry = next(m for m in body["data"] if m["id"] == MODEL)
            assert entry["faces"] == ["chat"], entry
            _status, health = p.get("/healthz")
            assert health["responses_models"] == 0, health
        finally:
            p.close()
    finally:
        a.stop()


# --------------------------------------------------------------------------
# token refresh
#
# Driven directly against TokenCache with a scripted fetch. `az` hands back
# whatever is in its own cache, so the lifetime of a fresh token is not
# predictable — these pin down what happens at the awkward values.
# --------------------------------------------------------------------------

def _token_cache(fetch, margin=300):
    os.environ.pop("AZURE_PROXY_STATIC_TOKEN", None)
    sys.path.insert(0, ROOT)
    import logging
    from proxy.server import TokenCache
    # These run in-process, so the proxy's own logger would write its refresh
    # lines into the middle of the test output.
    logging.getLogger("azure-proxy").setLevel(logging.CRITICAL)
    return TokenCache("scope", margin, fetch=fetch)


def _run(coro):
    import asyncio
    return asyncio.new_event_loop().run_until_complete(coro)


def test_token_short_lived_does_not_refresh_every_call():
    """A token shorter-lived than the margin must not be refetched constantly.

    `az` regularly returns a token with only minutes left. With a fixed 5 minute
    margin and no clamping, such a token is 'due for refresh' the instant it
    arrives, and every single request pays for an `az` subprocess.
    """
    calls = []

    def fetch():
        calls.append(1)
        return "tok-{}".format(len(calls)), time.time() + 120    # 2 min < margin

    cache = _token_cache(fetch, margin=300)

    async def go():
        return [await cache.get() for _ in range(5)]

    got = _run(go())
    assert len(calls) == 1, "fetched {} times, expected 1".format(len(calls))
    assert set(got) == {"tok-1"}, got


def test_token_is_reused_until_the_refresh_point():
    calls = []

    def fetch():
        calls.append(1)
        return "tok-{}".format(len(calls)), time.time() + 3600

    cache = _token_cache(fetch, margin=300)

    async def go():
        return [await cache.get() for _ in range(10)]

    got = _run(go())
    assert len(calls) == 1, len(calls)
    assert set(got) == {"tok-1"}, got


def test_token_survives_a_failing_refresh_while_still_valid():
    """A transient `az` failure must not take the proxy down.

    Past the refresh point the cache starts trying to renew, but what it already
    holds stays valid for a while yet — that token should keep serving, with the
    failure visible on /healthz rather than turned into an error response.
    """
    state = {"n": 0}

    def fetch():
        state["n"] += 1
        if state["n"] == 1:
            return "tok-1", time.time() + 120
        raise RuntimeError("az exploded")

    cache = _token_cache(fetch, margin=300)

    async def go():
        first = await cache.get()
        # Stand in for time passing to the refresh point. Clamping puts that
        # point at half the token's life, so it cannot be reached by argument.
        cache._refresh_at = time.time() - 1
        cache.MIN_RETRY_INTERVAL = 0
        second = await cache.get()
        return first, second

    first, second = _run(go())
    assert first == "tok-1", first
    assert state["n"] == 2, "should have attempted a refresh"
    assert second == "tok-1", "should keep serving the still-valid token"
    assert cache.status()["last_error"] is not None, "failure should be visible"


def test_token_unavailable_is_raised_when_nothing_is_usable():
    from proxy.server import TokenUnavailable

    def fetch():
        raise RuntimeError("az not logged in")

    cache = _token_cache(fetch)

    async def go():
        try:
            await cache.get()
        except TokenUnavailable as e:
            return str(e)
        return None

    message = _run(go())
    assert message is not None, "expected TokenUnavailable"
    assert "az not logged in" in message, message


def test_token_refresh_attempts_are_rate_limited():
    """A broken `az` must not become one subprocess spawn per request."""
    calls = []

    def fetch():
        calls.append(1)
        raise RuntimeError("az broken")

    cache = _token_cache(fetch)

    async def go():
        from proxy.server import TokenUnavailable
        for _ in range(5):
            try:
                await cache.get()
            except TokenUnavailable:
                pass

    _run(go())
    assert len(calls) == 1, "attempted {} times, rate limit should allow 1".format(
        len(calls))


def test_expired_token_is_not_served():
    def fetch():
        return "tok-expired", time.time() - 1

    cache = _token_cache(fetch)

    async def go():
        from proxy.server import TokenUnavailable
        try:
            await cache.get()
        except TokenUnavailable:
            return "raised"
        return "served"

    assert _run(go()) == "raised", "an already-expired token must not be used"


# --------------------------------------------------------------------------
# ARM discovery, and what it buys the balancer
# --------------------------------------------------------------------------
#
# The fragments below are real ARM replies, trimmed of the fields nothing reads
# and otherwise verbatim (endpoint-a and endpoint-b, 2026-08-21). They are what
# makes these tests worth having: the mapping being asserted is not a made-up
# edge case, it is the one endpoint-b actually presents.

sys.path.insert(0, os.path.join(ROOT, "probe"))
import probe as probe_module  # noqa: E402


def arm(name, model, sku="GlobalStandard", capacity=1000, limits=None,
        state="Succeeded", capabilities=None, version="2026-01-01"):
    return {
        "name": name,
        "sku": {"name": sku, "capacity": capacity},
        "properties": {
            "provisioningState": state,
            "capabilities": (capabilities if capabilities is not None
                             else {"chatCompletion": "true",
                                   "responses": "true"}),
            "model": {"format": "OpenAI", "name": model, "version": version},
            "rateLimits": limits,
        },
    }


def test_arm_plan_maps_a_deployment_to_the_model_it_actually_serves():
    """A deployment's name is not a promise about what is behind it.

    endpoint-b has one called `gpt-4o-mini` serving gpt-4.1-mini. Probing by
    name cannot detect that — the deployment answers, and the reply says
    nothing about the model — so a proxy that keys on the deployment name
    advertises a model it does not have. ARM is the only place the truth is
    written down.
    """
    plan = probe_module.plan_deployments([
        arm("gpt-4o-mini", "gpt-4.1-mini", sku="Standard", capacity=450,
            version="2025-04-14"),
    ])
    assert len(plan) == 1, plan
    assert plan[0]["deployment"] == "gpt-4o-mini", plan
    assert plan[0]["model"] == "gpt-4.1-mini", plan
    assert plan[0]["model_version"] == "2025-04-14", plan


def test_arm_plan_keeps_two_deployments_of_one_model_apart():
    """A second deployment of a model is a second quota, not a duplicate."""
    plan = probe_module.plan_deployments([
        arm("gpt-5.5", "gpt-5.5", sku="DataZoneStandard", capacity=5000,
            limits=[{"key": "request", "count": 5000.0},
                    {"key": "token", "count": 5000000.0}]),
        arm("gpt-5.5-2", "gpt-5.5", sku="GlobalStandard", capacity=15000,
            limits=[{"key": "request", "count": 15000.0},
                    {"key": "token", "count": 15000000.0}]),
    ])
    assert [d["deployment"] for d in plan] == ["gpt-5.5", "gpt-5.5-2"], plan
    assert {d["model"] for d in plan} == {"gpt-5.5"}, plan
    assert [d["capacity_tokens"] for d in plan] == [5000000.0, 15000000.0], plan


def test_arm_plan_drops_what_this_proxy_cannot_serve():
    """Batch SKUs, image deployments, half-built ones. None are chat routes.

    All three would fail the data-plane probe anyway; excluding them here saves
    the request and keeps the reason in one readable place rather than in a
    status code.
    """
    plan = probe_module.plan_deployments([
        arm("gpt-4.1-batch", "gpt-4.1", sku="DataZoneBatch", capacity=30000),
        arm("o4-mini", "o4-mini", sku="GlobalBatch", capacity=200000),
        arm("gpt-image-2", "gpt-image-2", capacity=2,
            capabilities={"imageGenerations": "true", "imageEdits": "true"}),
        arm("dall-e-3", "dall-e-3", sku="Standard", capacity=1,
            capabilities={"imageGenerations": "true"}),
        arm("half-built", "gpt-5.5", state="Creating"),
        arm("gpt-5.6-sol", "gpt-5.6-sol"),
    ])
    assert [d["deployment"] for d in plan] == ["gpt-5.6-sol"], plan


def test_arm_capacity_prefers_rate_limits_and_falls_back_to_the_sku():
    """rateLimits is the real pair; sku.capacity is the older deployments' only
    number, and it means RPM with TPM a thousand times it."""
    stated, inferred = probe_module.plan_deployments([
        arm("stated", "a", capacity=999,
            limits=[{"key": "request", "count": 333.0},
                    {"key": "token", "count": 333000.0}]),
        arm("inferred", "b", capacity=450, limits=None),
    ])
    assert (stated["capacity_requests"], stated["capacity_tokens"]) == \
        (333.0, 333000.0), stated
    assert (inferred["capacity_requests"], inferred["capacity_tokens"]) == \
        (450.0, 450000.0), inferred


def test_arm_plan_orders_routes_by_capacity_within_an_endpoint():
    """Endpoint priority first, size second. Priority still decides."""
    ordered = probe_module.order_routes([
        {"priority": 1, "deployment": "huge", "capacity_tokens": 99000000},
        {"priority": 0, "deployment": "small", "capacity_tokens": 5000000},
        {"priority": 0, "deployment": "big", "capacity_tokens": 15000000},
    ])
    assert [r["deployment"] for r in ordered] == ["big", "small", "huge"], ordered


def deployment_hits(fake):
    """{deployment name: requests served}.

    The two faces say it in different places — chat puts the deployment in the
    path, responses in the body's `model` — which is exactly the rewrite the
    proxy is responsible for, so both are read here rather than assumed.
    """
    counts = {}
    for req in fake.requests:
        parts = req["path"].split("/")
        if "deployments" in parts:
            name = parts[parts.index("deployments") + 1]
        else:
            name = (req["body"] or {}).get("model")
        if name:
            counts[name] = counts.get(name, 0) + 1
    return counts


def test_the_larger_deployment_on_an_endpoint_is_reached_for_first():
    """One endpoint, two deployments of the model, and no reason to take the
    small one first. The probe writes them in this order and the server sorts
    on the same key rather than trusting the file."""
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)], balance="strict_priority",
                  deployments={"alpha": [
                      {"name": "small", "capacity_tokens": 5000000},
                      {"name": "big", "capacity_tokens": 15000000}]})
        try:
            status, _b, headers = ask(p)
            assert status == 200, status
            assert headers["x-azure-proxy-route"] == "alpha/big", headers

            _s, report = p.get("/routes")
            assert set(report["routes"]) == {"alpha/small", "alpha/big"}, report
            # Declared and measured are reported side by side; nothing has
            # measured anything here, so only the declared half is filled in.
            big = report["routes"]["alpha/big"]
            assert big["capacity_tokens"] == 15000000, big
            assert big["limit_tokens"] is None, big
        finally:
            p.close()
    finally:
        a.stop()


def test_declared_capacity_is_the_cold_start_prior():
    """Before any response has been seen, the split is ARM's numbers.

    Neither fake sends x-ratelimit-limit-*, so nothing is ever measured and the
    prior is all there is — which is the point: a fresh process should already
    be sending three times as much at a route that is three times the size,
    rather than discovering it. No static_weights are configured at all, so a
    fall-through to the per-endpoint table would split this 50/50.
    """
    a = FakeAzure("alpha").start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity",
                  deployments={
                      "alpha": [{"name": DEPLOYMENT,
                                 "capacity_tokens": 15000000}],
                      "beta": [{"name": DEPLOYMENT,
                                "capacity_tokens": 5000000}]})
        try:
            counts = spread(p, 200)
            share = counts.get("alpha", 0) / 200.0
            # Expected 0.75, sd 0.031. A prior that ignored capacity lands at
            # 0.5 and a strict-priority regression lands at 1.0.
            assert 0.65 < share < 0.85, counts
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_a_pinned_session_does_not_move_between_deployments():
    """Affinity pins the route, not the resource.

    Two deployments of one model on one endpoint are two destinations. Whether
    Azure's encrypted reasoning is scoped to the resource or to the deployment
    is not something this proxy has evidence for, so the pin is the narrow one:
    a session that started on `big` keeps going to `big`.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_tokens=500000)).start()
    try:
        p = Proxy([("alpha", a.url)], balance="capacity",
                  deployments={"alpha": [
                      {"name": "small", "capacity_tokens": 5000000},
                      {"name": "big", "capacity_tokens": 15000000}]})
        try:
            seen = set()
            for _ in range(20):
                status, body, headers = sticky_ask(p)
                assert status == 200, (status, body)
                seen.add(headers.get("x-azure-proxy-route"))
            assert len(seen) == 1, "session was split across {}".format(seen)

            hits = deployment_hits(a)
            assert len(hits) == 1, hits
            _s, health = p.get("/healthz")
            aff = health["session_affinity"]
            assert list(aff["sessions_per_route"]) == list(seen), aff
        finally:
            p.close()
    finally:
        a.stop()


def test_a_responses_only_model_is_listed_and_not_offered_on_chat():
    """Some deployments have no chat face at all.

    gpt-5-pro and the codex models answer chat/completions with a flat
    `400 The requested operation is unsupported.` and serve /v1/responses only
    (measured 2026-08-21 on endpoint-a and endpoint-b). Three things follow, and
    all three used to be wrong: they must not appear as chat routes, they must
    still be listed by /v1/models, and asking for one on the chat face must say
    which face it IS on rather than that it does not exist.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)], faces={"alpha": ["responses"]})
        try:
            _s, models = p.get("/v1/models")
            entry, = models["data"]
            assert entry["id"] == MODEL, entry
            assert entry["faces"] == ["responses"], entry

            status, body, _h = ask(p)
            assert status == 404, (status, body)
            assert body["error"]["code"] == "no_chat_route", body
            assert "/v1/responses" in body["error"]["message"], body
            assert a.hits == 0, "a chat request reached a responses-only route"

            status, body, _h = p.post(dict(CODEX_BODY, stream=False),
                                      path="/v1/responses")
            assert status == 200, (status, body)
        finally:
            p.close()
    finally:
        a.stop()


# --------------------------------------------------------------------------
# The ledger's face split, and the event feed
#
# Both exist for the dashboard rather than for routing, and both have one
# property that has to hold or the dashboard quietly lies: the four face
# fractions must sum to the load figure printed beside them, and an event must
# not claim the foreign estimate moved when it did not.
# --------------------------------------------------------------------------

def test_ledger_splits_load_by_face():
    """Four faces, one ceiling, and the split adds up to the whole.

    The stacked bar in the dashboard is drawn from `our_load_by_face` and
    labelled with `our_load`. If the two are computed against different
    dimensions — the split by tokens while the total binds on requests — the
    bar's segments do not fill it and the number beside it disagrees with the
    picture. So the invariant is exact equality, not approximate agreement.
    """
    a = FakeAzure("alpha",
                  # The last behaviour repeats, so the streamed leg comes last.
                  [Behaviour(), Behaviour(), Behaviour(),
                   Behaviour(events=sse("alpha"))],
                  headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url)], balance="strict_priority")
        try:
            ask(p)                                          # chat, buffered
            ask(p)
            ask_responses(p, stream=False)                  # responses
            stream_responses(p)                             # responses, streamed

            _s, report = p.get("/routes")
            route = report["routes"]["alpha/" + DEPLOYMENT]
            by_face = route["our_load_by_face"]
            sent = route["sent_by_face"]

            assert sent["chat"]["requests"] == 2, sent
            assert sent["responses"]["requests"] == 1, sent
            assert sent["responses_stream"]["requests"] == 1, sent
            assert sent["chat_stream"]["requests"] == 0, sent

            assert route["load_dimension"] == "requests", route
            assert abs(sum(by_face.values()) - route["our_load"]) < 1e-9, route
            # And the split is of the same quantity the totals report.
            assert (sum(v["requests"] for v in sent.values())
                    == route["sent_requests_in_window"]), route
        finally:
            p.close()
    finally:
        a.stop()


def test_ledger_face_split_survives_the_window_expiring():
    """Expired entries leave the per-face view as well as the total.

    `prune` drops from the front of one deque; the per-face figures are derived
    from that same deque rather than kept as their own counters, which is the
    only arrangement where the two cannot drift apart. This is the test that
    would fail if someone ever added counters.
    """
    a = FakeAzure("alpha", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url)], load_window=3,
                  balance="strict_priority")
        try:
            ask(p)
            ask_responses(p, stream=False)
            _s, report = p.get("/routes")
            route = report["routes"]["alpha/" + DEPLOYMENT]
            assert route["sent_by_face"]["chat"]["requests"] == 1, route

            time.sleep(3.5)
            _s, report = p.get("/routes")
            route = report["routes"]["alpha/" + DEPLOYMENT]
            assert route["sent_requests_in_window"] == 0, route
            assert all(v["requests"] == 0 for v in route["sent_by_face"].values()), \
                route
            assert sum(route["our_load_by_face"].values()) == 0, route
        finally:
            p.close()
    finally:
        a.stop()


def test_events_record_a_throttle_and_the_foreign_estimate_it_moved():
    """The one question the log cannot answer without being parsed.

    A 429 does two separable things: it parks the route for a few seconds, and
    it revises the standing estimate of how much of that deployment belongs to
    somebody else. In the log those are two lines with other traffic between
    them. Here they are one record, and `foreign_updated` is the field that
    says the second thing happened at all.
    """
    a = FakeAzure("alpha", [Behaviour(status=429)],
                  # The 429 has to carry a ceiling or there is no share to
                  # express: `foreign = 1 - our_load` needs a denominator, and
                  # `note_foreign` correctly declines to guess one.
                  headers=ratelimit(limit_requests=10)).start()
    b = FakeAzure("beta", headers=ratelimit(limit_requests=1000)).start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, body, _h = ask(p)
            assert status == 200 and who(body) == "beta", (status, body)

            _s, feed = p.get("/events")
            throttles = [e for e in feed["events"]
                         if e["kind"] == "throttle" and "demoted" in e["message"]]
            assert throttles, feed["events"]
            event = throttles[-1]
            assert event["endpoint"] == "alpha", event
            assert event["deployment"] == DEPLOYMENT, event
            assert event["reason"] == "429", event
            assert event["park_seconds"] > 0, event
            assert event["foreign_updated"] is True, event
            assert event["foreign_after"] > 0, event
            assert event["foreign_after"] >= event["foreign_before"], event

            # And a failover was recorded, naming where the traffic went.
            failovers = [e for e in feed["events"] if e["kind"] == "failover"]
            assert failovers, feed["events"]
            assert failovers[-1]["to_route"] == "beta/" + DEPLOYMENT, failovers
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_events_do_not_claim_a_foreign_update_on_a_5xx():
    """A broken endpoint says nothing about who else is using it.

    Demoting on a 500 is an evasive manoeuvre; inferring a foreign share from
    one would permanently shrink a route for being briefly ill. The record has
    to distinguish the two, because on the dashboard they are different colours
    and mean different things to do.
    """
    a = FakeAzure("alpha", [Behaviour(status=500)],
                  headers=ratelimit(limit_requests=10)).start()
    b = FakeAzure("beta").start()
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)])
        try:
            status, _body, _h = ask(p)
            assert status == 200, status

            _s, feed = p.get("/events")
            demotes = [e for e in feed["events"] if e["kind"] == "demote"]
            assert demotes, feed["events"]
            assert demotes[-1]["reason"] == "500", demotes[-1]
            assert demotes[-1]["foreign_updated"] is False, demotes[-1]
            assert not [e for e in feed["events"] if e["kind"] == "foreign"], \
                feed["events"]
        finally:
            p.close()
    finally:
        a.stop(); b.stop()


def test_events_cursor_only_returns_what_is_new():
    """A poller that asks twice must not be told the same thing twice.

    The cursor advances past everything examined, not just past what was
    returned — otherwise a reader filtering for throttles is handed the whole
    backlog of request/response events on every single poll.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask(p)
            _s, first = p.get("/events")
            assert first["events"], first
            assert first["next"] > 0, first
            assert first["dropped"] is False, first

            _s, again = p.get("/events?since={}".format(first["next"]))
            assert again["events"] == [], again
            assert again["next"] == first["next"], again

            ask(p)
            _s, third = p.get("/events?since={}".format(first["next"]))
            assert third["events"], third
            assert third["next"] > first["next"], third
            assert all(e["seq"] > first["next"] for e in third["events"]), third

            # Filtering advances the cursor past what it filtered out.
            _s, filtered = p.get(
                "/events?kind=throttle&since={}".format(first["next"]))
            assert filtered["events"] == [], filtered
            assert filtered["next"] >= third["next"], filtered
        finally:
            p.close()
    finally:
        a.stop()


def test_events_carry_no_request_content():
    """The same rule the log follows: names, numbers and statuses only.

    /events is served over the same unauthenticated loopback port as everything
    else here, so it is a strictly worse place for a prompt than a file with
    the operator's umask on it. The prompt in the test body is a distinctive
    string precisely so this can look for it.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            p.post({"model": MODEL,
                    "messages": [{"role": "user",
                                  "content": "SECRET-CANARY-9f3a"}],
                    "max_completion_tokens": 16})
            _s, feed = p.get("/events")
            blob = json.dumps(feed)
            assert "SECRET-CANARY" not in blob, blob
        finally:
            p.close()
    finally:
        a.stop()


def test_a_test_proxy_does_not_touch_the_real_pidfile():
    """A proxy under test claims its own tree's pidfile, not the caller's.

    The pidfile is written by the proxy process itself, and it used to be
    written relative to the working directory. The test suite spawns proxies
    with AZURE_PROXY_HOME pointed at a temporary tree but does not change cwd,
    so running the tests from the repo overwrote the live service's pidfile —
    ./stop.sh then found a pid that had exited, called it debris, and left the
    real proxy running on its port with nothing able to stop it.

    Asserted from the outside rather than by inspecting the constant, because
    the property that matters is where the file lands.
    """
    a = FakeAzure("alpha").start()
    try:
        p = Proxy([("alpha", a.url)])
        try:
            ask(p)
            here = os.path.join(os.getcwd(), ".proxy.pid")
            mine = os.path.join(p.home, ".proxy.pid")
            assert os.path.exists(mine), "no pidfile in the proxy's own tree"
            assert int(open(mine).read().strip()) == p.proc.pid, mine
            if os.path.exists(here) and os.path.abspath(here) != os.path.abspath(mine):
                assert int(open(here).read().strip()) != p.proc.pid, (
                    "the test proxy claimed the pidfile of the tree it was "
                    "launched from")
        finally:
            p.close()
    finally:
        a.stop()


# --------------------------------------------------------------------------

TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_")]


def main():
    wanted = sys.argv[1:]
    selected = [t for t in TESTS
                if not wanted or any(w in t.__name__ for w in wanted)]
    if not selected:
        sys.exit("no test matches {}".format(wanted))

    failures = []
    for t in selected:
        name = t.__name__[5:]
        sys.stdout.write("  {:<58}".format(name))
        sys.stdout.flush()
        started = time.time()
        try:
            t()
        except Exception:
            failures.append((name, traceback.format_exc()))
            print("FAIL  {:.1f}s".format(time.time() - started))
        else:
            print("ok    {:.1f}s".format(time.time() - started))

    print()
    if failures:
        for name, tb in failures:
            print("=" * 70)
            print(name)
            print(tb)
        print("{}/{} failed".format(len(failures), len(selected)))
        sys.exit(1)
    print("{}/{} passed".format(len(selected), len(selected)))


if __name__ == "__main__":
    main()
