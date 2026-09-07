"""Real HTTP/SSE rolling switches, failed warmup and producer ownership."""

import concurrent.futures
import json
import os
import signal
import socket
import subprocess
import time
import unittest
import urllib.request

from run_tests import CODEX_BODY, DEPLOYMENT, PYTHON, ROOT, Behaviour, sticky_ask, sse
from test_split import fixture, wait_for, wait_routing, config_at
from proxy.state import Store
from routing.engine import Engine, route_record


def roll(p):
    with socket.socket(socket.AF_UNIX) as channel:
        channel.settimeout(10)
        channel.connect(os.path.join(p.home, "runtime", "supervisor.sock"))
        channel.sendall(b'{"action":"restart","timeout":5}\n')
        with channel.makefile("rb") as reader:
            return json.loads(reader.readline())


class RollingTests(unittest.TestCase):
    def test_long_sse_survives_switch_and_new_requests_have_no_failures(self):
        with fixture([Behaviour(events=sse("alpha", extra=60), event_delay=.07), Behaviour()]) as (p, a, b):
            request = urllib.request.Request(p.url("/v1/responses"), data=json.dumps(CODEX_BODY).encode(),
                                             headers={"Content-Type": "application/json"})
            old = p.get_raw("/healthz")[1]["pid"]
            stream_id = p.get("/events")[1]["stream_id"]
            with urllib.request.urlopen(request, timeout=15) as response:
                first = response.readline()
                p.sync()
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    switch = pool.submit(roll, p)
                    statuses = []
                    while not switch.done():
                        statuses.append(p.get_raw("/healthz")[0])
                    result = switch.result()
                self.assertTrue(result["ok"], result)
                self.assertLess(result["switch_seconds"], 1)
                self.assertTrue(statuses)
                self.assertTrue(all(s == 200 for s in statuses))
                health = p.get_raw("/healthz")[1]
                self.assertNotEqual(health["pid"], old)
                self.assertIn(old, health["supervisor"]["draining"])
                self.assertFalse(roll(p)["ok"])
                self.assertEqual(sticky_ask(p)[0], 200)
                tail = first + response.read()
            self.assertIn(b"response.completed", tail)
            self.assertEqual(tail.count(b'"delta"'), 60)
            wait_for(lambda: not p.get_raw("/healthz")[1]["supervisor"]["draining"])
            self.assertEqual(p.get("/events")[1]["stream_id"], stream_id)
            report = p.get("/routes")[1]["routes"]["alpha/" + DEPLOYMENT]
            self.assertEqual(report["attempts"], 2)
            self.assertEqual(report["ok"], 2)
            self.assertEqual(p.get("/healthz")[1]["session_affinity"]["live_sessions"], 2)
            print("rolling acceptance: " + json.dumps(dict(result, new_request_failures=0,
                                                           health_requests=len(statuses))))

    def test_failed_warmup_preserves_old_active(self):
        with fixture() as (p, a, b):
            old = p.get_raw("/healthz")[1]["pid"]
            path = os.path.join(p.home, "settings", "policy.yaml")
            with open(path) as file:
                policy = file.read()
            with open(path, "w") as file:
                file.write(policy.replace("ttl_seconds: 172800", "ttl_seconds: 0"))
            result = roll(p)
            self.assertFalse(result["ok"], result)
            self.assertEqual(p.get_raw("/healthz")[1]["pid"], old)
            self.assertEqual(sticky_ask(p)[0], 200)

    def test_normal_and_crash_restarts_restore_bindings(self):
        with fixture() as (p, a, b):
            expected = sticky_ask(p)[2]["x-azure-proxy-route"]
            for crash in (False, True):
                if crash:
                    os.kill(p.get_raw("/healthz")[1]["pid"], signal.SIGKILL)
                    def inactive():
                        with open(os.path.join(p.home, "runtime", "supervisor.json")) as file:
                            return json.load(file)["active"] is None
                    wait_for(inactive)
                result = roll(p)
                self.assertTrue(result["ok"], result)
                self.assertEqual(sticky_ask(p)[2]["x-azure-proxy-route"], expected)
                wait_for(lambda: not p.get_raw("/healthz")[1]["supervisor"]["draining"])

    def test_producer_lifecycle_only_retires_owned_attempts(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            engine = Engine(p.home, config_at(p.home))
            try:
                route = route_record(engine.config.routes["test-model"][0])
                for owner in ("old", "new"):
                    engine.consume(dict(kind="dispatch", producer=owner, attempt=owner,
                                        route=route, request_bytes=10, face=0, at=time.time()))
                engine.consume(dict(kind="producer_started", producer="third", at=time.time()))
                self.assertEqual(set(engine.pending), {"old", "new"})
                engine.consume(dict(kind="producer_stopped", producer="old", at=time.time()))
                self.assertEqual(set(engine.pending), {"new"})
            finally:
                engine.close()


if __name__ == "__main__":
    unittest.main()
