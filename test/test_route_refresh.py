"""Hourly discovery, coherent files, and in-process route replacement."""

import copy
import json
import os
import tempfile
import signal
import unittest
from unittest.mock import Mock, patch

from probe.probe import discover
from proxy.config import Config
from routing.discovery import RouteRefresher, persist_discovery
from routing.engine import Engine, route_record
from test_split import fixture, config_at
from run_tests import MODEL


class RefreshWorkerTests(unittest.TestCase):
    def test_startup_refresh_and_hourly_schedule(self):
        worker = RouteRefresher("unused")
        worker.stop = Mock()
        worker.stop.is_set.return_value = False
        worker.stop.wait.side_effect = [False, True]
        worker.results = Mock()
        worker.probe = Mock(return_value={"generation": 1})
        with patch("routing.discovery.time.monotonic", return_value=10), \
                patch("routing.discovery.time.time", return_value=100):
            worker.run()
        self.assertEqual(worker.probe.call_count, 2)
        self.assertEqual(worker.stop.wait.call_args_list[0].args, (3600,))
        self.assertEqual(worker.status()["next_refresh_at"], 3700)
        self.assertFalse(worker.status()["running"])

    def test_failed_probe_retains_previous_success_and_publishes_nothing(self):
        worker = RouteRefresher("unused")
        worker.update(last_success_at=123)
        worker.stop = Mock()
        worker.stop.is_set.return_value = False
        worker.stop.wait.return_value = True
        worker.probe = Mock(side_effect=RuntimeError("network unavailable"))
        worker.run()
        self.assertIsNone(worker.take())
        self.assertEqual(worker.status()["last_success_at"], 123)
        self.assertIn("network unavailable", worker.status()["last_error"])

    def test_shutdown_terminates_only_the_owned_probe_process_group(self):
        with tempfile.TemporaryDirectory() as root:
            worker = RouteRefresher(root)
            worker.stop.set()
            process = Mock(pid=12345)
            process.poll.return_value = None
            process.communicate.return_value = (b"", None)
            with patch("routing.discovery.subprocess.Popen", return_value=process), \
                    patch("routing.discovery.os.killpg") as kill:
                with self.assertRaises(InterruptedError):
                    worker.probe()
                kill.assert_called_once_with(12345, signal.SIGTERM)

    def test_strict_discovery_distinguishes_failed_arm_from_empty_inventory(self):
        candidates = [dict(name="source", subscription="sub", resource_group="group")]
        with patch("probe.probe.arm_deployments", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "ARM discovery failed"):
                discover(candidates, [], "token", strict=True)
        with patch("probe.probe.arm_deployments", return_value=[]):
            self.assertEqual(discover(candidates, [], "token", strict=True), {"source": ([], "arm")})

    def test_bundle_remains_coherent_if_a_mirror_write_fails(self):
        with tempfile.TemporaryDirectory() as root:
            old = dict(sources={"old": 1}, models={"old": 2})
            new = dict(sources={"new": 1}, models={"new": 2})
            persist_discovery(root, old)
            from routing.discovery import _atomic_json
            def write(path, value):
                if path.endswith("models.json"):
                    raise OSError("disk full")
                return _atomic_json(path, value)
            with patch("routing.discovery._atomic_json", side_effect=write):
                with self.assertRaises(OSError):
                    persist_discovery(root, new)
            with open(os.path.join(root, "runtime", "discovery.json")) as file:
                self.assertEqual(json.load(file), old)
            with open(os.path.join(root, "runtime", "discovery.previous.json")) as file:
                self.assertEqual(json.load(file), old)


class DynamicReloadTests(unittest.TestCase):
    def test_reload_preserves_accounting_and_suppresses_retired_routes(self):
        with fixture() as (proxy, _a, _b):
            proxy.sync()
            proxy.stop_routing()
            engine = Engine(proxy.home, config_at(proxy.home))
            try:
                old = engine.config
                route = old.routes[MODEL][0]
                state = engine.quota.state(route)
                state.safe_rpm, state.foreign_seen = 123, True
                discovery = copy.deepcopy(old.discovery)
                for doc in discovery.values():
                    doc["_generated_at"] = "new-generation"
                records = discovery["models"]["models"][MODEL]["routes"]
                records[:] = [r for r in records if r["endpoint"] != route.endpoint]
                fresh = dict(records[0], deployment="new-deploy")
                records.append(fresh)
                engine.reload_routes(discovery)
                self.assertIsNot(engine.config, old)
                self.assertIn(route, old.routes[MODEL])
                self.assertIs(engine.quota.state(route), state)
                self.assertEqual(state.safe_rpm, 123)
                self.assertTrue(state.foreign_seen)
                # A late observation from an old request must not restore a retired destination.
                engine.consume(dict(kind="dispatch", at=engine.now, route=route_record(route),
                                    attempt="old-request", request_bytes=1, face=0, model=MODEL))
                report = engine.step()["report"]
                self.assertNotIn(str(route), report["routes"])
                self.assertIn(fresh["endpoint"] + "/new-deploy", report["routes"])
                self.assertIn("old-request", engine.pending)
                self.assertEqual(Config(root=proxy.home).generated_at, "new-generation")
                engine.close()
                engine = Engine(proxy.home, Config(root=proxy.home))
                self.assertIn(str(route), engine.retired_routes)
                self.assertNotIn(str(route), engine.step()["report"]["routes"])
            finally:
                engine.close()

    def test_invalid_candidate_keeps_live_config_and_runtime_files(self):
        with fixture() as (proxy, _a, _b):
            proxy.sync()
            proxy.stop_routing()
            engine = Engine(proxy.home, config_at(proxy.home))
            try:
                old = engine.config
                discovery = copy.deepcopy(old.discovery)
                discovery["sources"]["endpoints"][0]["url"] = "file:///invalid/"
                with self.assertRaises(ValueError):
                    engine.reload_routes(discovery)
                self.assertIs(engine.config, old)
                self.assertFalse(os.path.exists(os.path.join(proxy.home, "runtime", "discovery.json")))
            finally:
                engine.close()


if __name__ == "__main__":
    unittest.main()
