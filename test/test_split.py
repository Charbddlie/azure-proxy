"""Two-process acceptance tests. Uses temporary roots and local fake upstreams."""

import asyncio
import contextlib
import concurrent.futures
import copy
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
import unittest
import urllib.request
from types import SimpleNamespace
from unittest.mock import mock_open, patch

from run_tests import (Behaviour, CODEX_BODY, DEPLOYMENT, FakeAzure, MODEL,
                       PYTHON, ROOT, Proxy, ask, sse, who)

sys.path.insert(0, ROOT)
from proxy.bridge import (SNAPSHOT_VERSION, TABLE_PATHS, ConfigView, ServingBridge,
                          Target, Telemetry, decode_snapshot)
from proxy.config import Config, Route, TABLES
from proxy.state import Store
from routing.engine import Engine, route_from_record, route_record, target_record


@contextlib.contextmanager
def fixture(behaviour=None):
    a, b = FakeAzure("alpha", behaviour), FakeAzure("beta")
    a.start()
    b.start()
    p = None
    try:
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="strict_priority")
        yield p, a, b
    finally:
        if p:
            p.close()
        a.stop()
        b.stop()


def wait_for(check, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.025)
    raise AssertionError("condition did not become true")


def wait_routing(p):
    return wait_for(lambda: (lambda h: h["routing"]["ok"]
                            and h["routing"]["pid"] == p.routing_proc.pid)(
                                p.get_raw("/healthz")[1]))


def turn(p, session):
    return p.post(dict(CODEX_BODY, stream=False, prompt_cache_key=session),
                  path="/v1/responses")


def edit_models(p, edit):
    path = os.path.join(p.home, "runtime", "models.json")
    with open(path) as file:
        doc = json.load(file)
    edit(doc)
    with open(path, "w") as file:
        json.dump(doc, file)


def config_at(root):
    with patch.multiple("proxy.config", ROOT=root,
                        SETTINGS=os.path.join(root, "settings"),
                        RUNTIME=os.path.join(root, "runtime")):
        return Config()


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.route = Route("alpha", "http://localhost/", "v", DEPLOYMENT,
                           "max_tokens", 0, "openai/v1/responses")
        self.record = target_record(self.route)
        self.snapshot = dict(
            schema_version=SNAPSHOT_VERSION, revision=1,
            config=dict(scopes=["test-scope"], image_deployments={}),
            tables={name: {} for name in TABLES},
            report={}, events={}, routing=dict(
                heartbeat=time.time(), processed_seq=0, pid=1, ready=True))
        self.snapshot["tables"]["routes"][MODEL] = [self.record]

    def test_target_urls_match_existing_azure_and_foundry_conventions(self):
        for url, scope in (("https://example/", None),
                           ("https://example/api/projects/project/", "https://ai.azure.com/.default"),
                           ("https://example/", "https://ai.azure.com/.default")):
            with self.subTest(url=url, scope=scope):
                route = Route("alpha", url, "v", DEPLOYMENT, "max_tokens", 0,
                              "openai/v1/responses", scope=scope)
                record = target_record(route)
                self.assertEqual(record["targets"], {
                    "/v1/chat/completions": route.chat_target(),
                    "/v1/responses": route.responses_target(),
                    "/v1/images/generations": route.image_target("generations"),
                    "/v1/images/edits": route.image_target("edits")})
                self.assertEqual(record["routing_data"], route_record(route))
                snapshot = copy.deepcopy(self.snapshot)
                snapshot["config"]["scopes"] += [scope] if scope else []
                snapshot["tables"] = {table: {MODEL: [record]} for table in TABLES}
                installed = decode_snapshot(snapshot, SimpleNamespace())
                for table, face in TABLE_PATHS.items():
                    self.assertEqual(getattr(installed, table)[MODEL][0].targets[face],
                                     record["targets"][face])

    def test_optional_metadata_round_trips_without_serving_interpretation(self):
        metadata = {"future_algorithm": {"weights": [1, None, {"other": True}]}}
        self.record["routing_data"].update(metadata)
        self.record["future_optional_field"] = [1, 2]
        self.snapshot["config"]["future_optional_field"] = {"v": 2}
        self.snapshot["report"] = dict(balance="future-mode", metrics=metadata)
        cfg = decode_snapshot(self.snapshot, SimpleNamespace())
        target = cfg.routes[MODEL][0]
        self.assertIsInstance(target, Target)
        self.assertEqual(cfg.routing_report, self.snapshot["report"])
        bridge = ServingBridge("unused", None)
        telemetry = Telemetry(bridge)
        entry = telemetry.charge(target, 10)
        telemetry.observed(target, 200, {
            "X-RateLimit-New-Quota": "42", "retry-after": "2",
            "authorization": "secret", "set-cookie": "secret"}, entry)
        telemetry.demote(target, "429", entry=entry)
        telemetry.note_timeout(target, entry)
        telemetry.settle(target, entry, 10, 23)
        telemetry.note_success(target, entry)
        telemetry.finish(entry)
        for event in bridge.pending:
            if "route" in event:
                self.assertEqual(event["route"], self.record["routing_data"])
        self.assertEqual(bridge.pending[1]["headers"], {
            "x-ratelimit-new-quota": "42", "retry-after": "2"})
        self.assertEqual(route_record(route_from_record(self.record["routing_data"])),
                         route_record(self.route))
        self.assertNotIn("targets", bridge.pending[0]["route"])
        # Routing can replace its entire metadata shape without a serving update.
        self.record["routing_data"] = {"new_schema": [{"opaque": None}]}
        target = decode_snapshot(self.snapshot, SimpleNamespace()).routes[MODEL][0]
        telemetry.charge(target, 10)
        self.assertEqual(bridge.pending[-1]["route"], self.record["routing_data"])

    def test_invalid_targets_preserve_the_installed_generation(self):
        cfg = ConfigView(SimpleNamespace())
        bridge = ServingBridge("unused", cfg)
        bridge.install(self.snapshot)
        installed = cfg.current
        original = copy.deepcopy(self.snapshot)
        for field, value in (
                ("endpoint", ""), ("deployment", 3), ("scope", []),
                ("scope", "unpublished-scope"), ("targets", {}),
                ("targets", {"/v1/chat/completions": "file:///tmp/example"}),
                ("targets", {"/v1/chat/completions": "https://"}),
                ("routing_data", [])):
            with self.subTest(field=field, value=value):
                bad = copy.deepcopy(original)
                bad["revision"] = 2
                bad["tables"]["routes"][MODEL][0][field] = value
                with self.assertRaises(ValueError):
                    bridge.install(bad)
                self.assertIs(cfg.current, installed)
                self.assertIs(bridge.snapshot, self.snapshot)
        duplicate = copy.deepcopy(original)
        duplicate["revision"] = 2
        duplicate["tables"]["routes"][MODEL].append(copy.deepcopy(self.record))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            bridge.install(duplicate)
        for version in (1, 999):
            with self.assertRaisesRegex(ValueError, "incompatible"):
                bridge.install(dict(original, schema_version=version, revision=2))
        self.assertIs(cfg.current, installed)
        self.assertEqual(self.snapshot, original)

    def test_serving_configuration_skips_routing_owned_settings_and_files(self):
        policy = dict(
            server=dict(host="127.0.0.1", port=0),
            routing=dict(retry_on_status=[], retry_on_transport_error=True,
                         retry_on_timeout=False, request_timeout_seconds=30,
                         max_attempts_per_request=4, backoff_initial_seconds=1,
                         backoff_multiplier=2, backoff_jitter_seconds=0,
                         balance={"new": "shape"}, balancing="routing-owned"),
            request=dict(forward_headers=True),
            auth=dict(scope="test-scope", refresh_margin_seconds=300))
        with patch("builtins.open", mock_open(read_data=json.dumps(policy))) as opened:
            cfg = Config(load_routes=False)
        self.assertEqual(opened.call_count, 1)
        self.assertTrue(opened.call_args.args[0].endswith("policy.yaml"))
        self.assertFalse(hasattr(cfg, "balance"))
        self.assertFalse(hasattr(cfg, "load_window"))
        self.assertEqual(cfg.scopes, {"test-scope"})
        self.assertEqual(cfg.timeout, 30)
        policy["routing"]["balance"] = "strict_priority"
        for name, value in (("load_window_seconds", 0), ("spill_threshold", -1),
                            ("load_window_seconds", float("inf"))):
            with self.subTest(name=name, value=value):
                policy["routing"]["balancing"] = {name: value}
                with patch("builtins.open", mock_open(read_data=json.dumps(policy))):
                    self.assertEqual(Config(load_routes=False).timeout, 30)
                files = [io.StringIO(json.dumps(doc)) for doc in
                         (policy, {"endpoints": []}, {"models": {}})]
                with patch("builtins.open", side_effect=files):
                    with self.assertRaisesRegex(ValueError, "invalid routing window/threshold"):
                        Config()


class SplitTests(unittest.TestCase):
    def test_updated_urls_and_opaque_reports_arrive_without_serving_restart(self):
        with fixture() as (p, a, b):
            self.assertEqual(turn(p, "existing")[0], 200)
            old_path = a.requests[-1]["path"]
            p.stop_routing()
            with contextlib.closing(Store(p.home)) as store:
                snapshot = store.get("snapshot")
                for table in TABLES:
                    for records in snapshot["tables"][table].values():
                        for record in records:
                            record["routing_data"]["future"] = {"nested": [1, 2]}
                            record["targets"]["/v1/responses"] = b.url + "custom/responses"
                snapshot["report"].update(balance="future-mode", future_metric={"value": 17})
                snapshot["report"].pop("spill_threshold", None)
                snapshot["revision"] += 1
                with store.db:
                    store.db.execute("UPDATE state SET payload=? WHERE name='snapshot'",
                                     (json.dumps(snapshot),))
                wait_for(lambda: p.get_raw("/healthz")[1]["routing"]["mapping_version"]
                         == snapshot["revision"])
                self.assertEqual(turn(p, "existing")[0], 200)
                self.assertEqual(a.requests[-1]["path"], old_path)
                self.assertEqual(turn(p, "new")[0], 200)
                self.assertEqual(b.requests[-1]["path"], "/custom/responses")
                health = p.get_raw("/healthz")[1]
                self.assertEqual(health["supervisor"]["pid"], p.proc.pid)
                self.assertEqual(health["balance"], "future-mode")
                self.assertIsNone(health["spill_threshold"])
                self.assertEqual(p.get_raw("/routes")[1]["future_metric"], {"value": 17})
                def observations():
                    return [event for _, event in store.read_events(0, limit=10000)
                            if event["kind"] == "dispatch"
                            and "future" in event["route"]]
                events = wait_for(observations)
                self.assertEqual(events[-1]["route"]["future"], {"nested": [1, 2]})
            p.start_routing()
            wait_routing(p)
            self.assertEqual(p.get("/routes")[1]["routes"]["alpha/" + DEPLOYMENT]["ok"], 3)

    def test_v1_checkpoint_and_observations_recover_into_v2_snapshot(self):
        with fixture() as (p, a, b):
            p.proc.terminate()
            p.proc.wait(timeout=10)
            p.stop_routing()
            config = config_at(p.home)
            route = config.routes[MODEL][0]
            key = str(route)
            # Literal legacy descriptor and pending entry: independent of the v2 encoder.
            legacy_route = dict(endpoint="alpha", url=a.url, api_version="v",
                                deployment=DEPLOYMENT, limit_param="max_tokens", priority=0,
                                responses_path="openai/v1/responses", model_version=None,
                                capacity_requests=None, capacity_tokens=None,
                                image_edits=False, scope=None)
            pending = [time.time(), 1, 0, 1, "legacy-attempt"]
            with contextlib.closing(Store(p.home)) as store:
                checkpoint = dict(
                    schema_version=1, cursor=store.highwater(), capacity={key: 7.0},
                    pending={"legacy-attempt": dict(entry=pending, route=legacy_route)},
                    catalog={key: dict(route=legacy_route, model=MODEL)},
                    sessions={}, events=[], event_seq=0,
                    states={key: dict(sent=[pending], safe_rpm=7.0, attempts=1)})
                legacy_snapshot = dict(
                    schema_version=1, revision=10, config={}, tables={}, report={},
                    routing={}, events=dict(events=[], next=0, counts={}))
                store.publish(checkpoint, legacy_snapshot)
                batch = [dict(kind=kind, local_seq=index + 1, at=time.time(),
                              attempt="legacy-attempt", route=legacy_route,
                              request_bytes=10, total_tokens=42)
                         for index, kind in enumerate(("usage", "success", "finish"))]
                store.append("legacy-producer", batch)
                with contextlib.closing(Engine(p.home, config)) as engine:
                    snapshot = engine.step()
                    self.assertEqual(snapshot["schema_version"], SNAPSHOT_VERSION)
                    state = engine.quota.state(route)
                    self.assertEqual((state.attempts, state.ok, state.safe_rpm), (1, 1, 7))
                    self.assertEqual(state.token_samples, 1)
                    self.assertEqual(state.sent[0][1], 42)
                    self.assertFalse(engine.pending)
                self.assertEqual(store.get("checkpoint")["schema_version"], 1)
                self.assertEqual(store.db.execute("PRAGMA user_version").fetchone()[0], 1)
                store.append("legacy-producer", batch)
                with contextlib.closing(Engine(p.home, config)) as engine:
                    state = engine.step()["report"]["routes"][key]
                    self.assertEqual((state["attempts"], state["ok"], state["capacity_rpm"]),
                                     (1, 1, 7))

    def test_retired_family_catalog_preserves_a_child_models_encrypted_state(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            edit_models(p, lambda doc: doc["models"].update(
                {"child-model": copy.deepcopy(doc["models"][MODEL])}))
            p.start_routing()
            wait_routing(p)
            self.assertEqual(turn(p, "family")[2]["x-azure-proxy-route"], "alpha/" + DEPLOYMENT)
            p.stop_routing()
            def retire(doc):
                for model in doc["models"].values():
                    model["routes"] = [r for r in model["routes"] if r["endpoint"] == "beta"]
            edit_models(p, retire)
            p.start_routing()
            wait_routing(p)
            inherited = {"type": "reasoning", "encrypted_content": "parent-ciphertext"}
            body = dict(CODEX_BODY, model="child-model", stream=False,
                        prompt_cache_key="family", input=[inherited])
            response = p.post(body, path="/v1/responses")
            self.assertEqual(response[2]["x-azure-proxy-route"], "alpha/" + DEPLOYMENT)
            self.assertEqual(a.requests[-1]["body"]["input"], [inherited])

    def test_new_model_and_token_scope_arrive_without_serving_restart(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            def add_model(doc):
                doc["models"]["added-model"] = {"routes": [copy.deepcopy(
                    doc["models"][MODEL]["routes"][1])]}
            edit_models(p, add_model)
            path = os.path.join(p.home, "runtime", "sources.json")
            with open(path) as file:
                sources = json.load(file)
            sources["endpoints"][1]["scope"] = "https://ai.azure.com/.default"
            with open(path, "w") as file:
                json.dump(sources, file)
            p.start_routing()
            wait_routing(p)
            result = p.post(dict(CODEX_BODY, model="added-model", stream=False), path="/v1/responses")
            self.assertEqual(result[2]["x-azure-proxy-route"], "beta/" + DEPLOYMENT)
            health = p.get("/healthz")[1]
            self.assertEqual(health["supervisor"]["pid"], p.proc.pid)
            self.assertIn("https://ai.azure.com/.default", health["tokens"])

    def test_buffered_request_keeps_its_generation_during_remap(self):
        with fixture([Behaviour(delay=0.7)]) as (p, a, b):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(ask, p)
                wait_for(lambda: a.hits == 1)
                p.stop_routing()
                edit_models(p, lambda doc: doc["models"][MODEL]["routes"][1].update(priority=-1))
                p.start_routing()
                wait_routing(p)
                self.assertEqual(who(future.result(timeout=10)[1]), "alpha")
            self.assertEqual(who(ask(p)[1]), "beta")

    def test_serving_stop_waits_for_drain_without_forcing(self):
        with fixture([Behaviour(delay=1.5)]) as (p, a, b):
            def request():
                req = urllib.request.Request(p.url("/v1/chat/completions"),
                    data=json.dumps({"model": MODEL, "messages": []}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=10) as reply:
                    return json.load(reply)
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(request)
                wait_for(lambda: a.hits == 1)
                result = subprocess.run(["./stop.sh", "serving", "--timeout", "0.1"],
                                        cwd=ROOT, env=p.env, capture_output=True, timeout=5)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(b"still draining", result.stderr)
                self.assertIsNone(p.proc.poll())
                self.assertEqual(who(future.result(timeout=10)), "alpha")
            self.assertIn(p.proc.wait(timeout=10), (0, -15))
            self.assertFalse(os.path.exists(os.path.join(p.home, ".proxy.pid")))
            self.assertIsNone(p.routing_proc.poll())

    def test_mixed_results_during_downtime_are_replayed_once(self):
        with fixture([Behaviour(), Behaviour(status=429), Behaviour()]) as (p, a, b):
            self.assertEqual(ask(p)[0], 200)
            p.stop_routing(kill=True)
            self.assertEqual(who(ask(p)[1]), "beta")
            self.assertEqual(who(ask(p)[1]), "alpha")
            for _ in range(2):
                p.start_routing()
                wait_routing(p)
                report = p.get("/routes")[1]["routes"]
                alpha, beta = report["alpha/" + DEPLOYMENT], report["beta/" + DEPLOYMENT]
                self.assertEqual((alpha["attempts"], alpha["ok"], alpha["rate_limited"]), (3, 2, 1))
                self.assertEqual((beta["attempts"], beta["ok"]), (1, 1))
                p.stop_routing(kill=True)

    def test_bad_routing_config_keeps_existing_serving_available(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            with open(os.path.join(p.home, "settings", "policy.yaml"), "w") as file:
                file.write("routing: [invalid\n")
            result = subprocess.run([PYTHON, "-m", "routing"], cwd=ROOT, env=p.env,
                                    capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(os.path.exists(os.path.join(p.home, ".routing.pid")))
            self.assertEqual(who(ask(p)[1]), "alpha")

    def test_crash_around_publication_recovers_transactionally(self):
        with fixture() as (p, a, b):
            p.sync()
            p.stop_routing()
            config = config_at(p.home)
            route = config.routes[MODEL][0]
            store = Store(p.home)
            try:
                for index, committed in enumerate((False, True)):
                    attempt = "publish-{}".format(index)
                    start = index * 3 + 1
                    events = [dict(kind=kind, local_seq=start + offset,
                                   attempt=attempt, at=time.time(), route=route_record(route),
                                   request_bytes=10, face=0, model=MODEL)
                              for offset, kind in enumerate(("dispatch", "success", "finish"))]
                    store.append("publish-test", events)
                    engine = Engine(p.home, config)
                    publish = engine.store.publish
                    def fail(checkpoint, snapshot):
                        if committed:
                            publish(checkpoint, snapshot)
                        raise sqlite3.OperationalError("simulated crash")
                    with patch.object(engine.store, "publish", side_effect=fail):
                        with self.assertRaises(sqlite3.OperationalError):
                            engine.step()
                    engine.close()
                    restored = Engine(p.home, config)
                    report = restored.step()["report"]["routes"][str(route)]
                    restored.close()
                    self.assertEqual((report["attempts"], report["ok"]), (index + 1, index + 1))
            finally:
                store.close()

    def test_stream_survives_repeated_routing_kills_and_replays_once(self):
        with fixture([Behaviour(events=sse("alpha", extra=35), event_delay=0.07)]) as (p, a, b):
            request = urllib.request.Request(
                p.url("/v1/responses"), data=json.dumps(CODEX_BODY).encode(),
                headers={"Content-Type": "application/json"})
            pid = p.get("/healthz")[1]["pid"]
            with urllib.request.urlopen(request, timeout=15) as reply:
                first = reply.readline()
                self.assertTrue(first)
                p.sync()  # Persist dispatch while the response is still open.
                for _ in range(3):
                    p.stop_routing(kill=True)
                    p.start_routing()
                    wait_routing(p)
                body = first + reply.read()
            self.assertIn(b"response.completed", body)
            self.assertEqual(body.count(b'"delta"'), 35)
            self.assertEqual(p.get("/healthz")[1]["pid"], pid)
            report = p.get("/routes")[1]["routes"]["alpha/" + DEPLOYMENT]
            self.assertEqual((report["attempts"], report["ok"], report["rpm_samples"]), (1, 1, 1))
            self.assertEqual(a.hits, 1)
            self.assertEqual(b.hits, 0)

    def test_cached_target_and_retired_session_survive_mapping_update(self):
        with fixture() as (p, a, b):
            first = turn(p, "existing")
            self.assertEqual(first[2]["x-azure-proxy-route"], "alpha/" + DEPLOYMENT)
            p.stop_routing()
            wait_for(lambda: not p.get_raw("/healthz")[1]["routing"]["ok"])
            self.assertEqual(who(ask(p)[1]), "alpha")
            self.assertEqual(turn(p, "offline-new")[0], 200)
            # Retire alpha from new model assignments.
            edit_models(p, lambda doc: doc["models"][MODEL].update(
                routes=[r for r in doc["models"][MODEL]["routes"] if r["endpoint"] == "beta"]))
            p.start_routing()
            wait_routing(p)
            self.assertEqual(turn(p, "online-new")[2]["x-azure-proxy-route"], "beta/" + DEPLOYMENT)
            for session in ("existing", "offline-new"):
                self.assertEqual(turn(p, session)[2]["x-azure-proxy-route"], "alpha/" + DEPLOYMENT)
            report = p.get("/routes")[1]
            self.assertEqual(report["routes"]["alpha/" + DEPLOYMENT]["attempts"], 5)
            self.assertFalse(report["stats_stale"])

    def test_routing_stays_off_the_request_path_when_database_is_locked(self):
        with fixture() as (p, a, b):
            p.sync()
            db = sqlite3.connect(os.path.join(p.home, "runtime", "control.sqlite3"))
            db.execute("BEGIN IMMEDIATE")
            try:
                started = time.monotonic()
                request = urllib.request.Request(p.url("/v1/chat/completions"),
                    data=json.dumps({"model": MODEL, "messages": []}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=0.8) as reply:
                    self.assertEqual(who(json.load(reply)), "alpha")
                self.assertLess(time.monotonic() - started, 0.8)
            finally:
                db.rollback()
                db.close()
            p.sync()
            self.assertEqual(p.get("/routes")[1]["routes"]["alpha/" + DEPLOYMENT]["ok"], 1)

    def test_invalid_publication_preserves_the_last_mapping(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            store = Store(p.home)
            try:
                snapshot = store.get("snapshot")
                snapshot["revision"] += 10
                snapshot["schema_version"] = 999
                with store.db:
                    store.db.execute("UPDATE state SET payload=? WHERE name='snapshot'",
                                     (json.dumps(snapshot),))
                wait_for(lambda: p.get_raw("/healthz")[1]["routing"]["exchange_error"])
                self.assertEqual(who(ask(p)[1]), "alpha")
                self.assertTrue(p.get_raw("/healthz")[1]["ok"])
            finally:
                store.close()

    def test_restart_script_defaults_to_routing_and_rejects_duplicate(self):
        with fixture() as (p, a, b):
            duplicate = subprocess.run([PYTHON, "-m", "routing"], cwd=ROOT, env=p.env,
                                       capture_output=True, timeout=5)
            self.assertNotEqual(duplicate.returncode, 0)
            self.assertIn(b"already running", duplicate.stderr)
            old = p.routing_proc.pid
            try:
                result = subprocess.run(["./restart.sh"], cwd=ROOT, env=p.env,
                                        capture_output=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                health = p.get_raw("/healthz")[1]
                self.assertEqual(health["supervisor"]["pid"], p.proc.pid)
                self.assertNotEqual(health["routing"]["pid"], old)
                self.assertTrue(health["routing"]["ok"])
            finally:
                subprocess.run(["./stop.sh", "routing"], cwd=ROOT, env=p.env,
                               capture_output=True, timeout=15, check=True)

    def test_cold_serving_requires_a_snapshot(self):
        with fixture() as (p, a, b):
            p.proc.terminate()
            p.proc.wait(timeout=10)
            p.stop_routing()
            store = Store(p.home)
            with store.db:
                store.db.execute("DELETE FROM state WHERE name='snapshot'")
            store.close()
            result = subprocess.run([PYTHON, "-m", "proxy"], cwd=ROOT, env=p.env,
                                    capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"no routing snapshot", result.stdout + result.stderr)
            self.assertFalse(os.path.exists(os.path.join(p.home, ".proxy.pid")))

    def test_replay_uses_dispatch_time_and_recovers_outstanding_usage(self):
        with fixture() as (p, a, b):
            p.sync()
            p.stop_routing()
            config = config_at(p.home)
            route = config.routes[MODEL][0]
            store = Store(p.home)
            now = time.time()
            def event(seq, kind, attempt, at, **kw):
                return dict(kw, local_seq=seq, kind=kind, attempt=attempt, at=at,
                            route=route_record(route))
            try:
                store.append("replay-test", [
                    event(1, "dispatch", "long-one", now - 90, request_bytes=10, face=0, model=MODEL),
                    event(2, "dispatch", "long-two", now - 80, request_bytes=10, face=0, model=MODEL)])
                engine = Engine(p.home, config)
                engine.step()
                engine.close()
                store.append("replay-test", [
                    event(3, "usage", "long-two", now, request_bytes=10, total_tokens=42),
                    event(4, "success", "long-two", now),
                    event(5, "finish", "long-two", now)])
                engine = Engine(p.home, config)
                snapshot = engine.step()
                report = snapshot["report"]["routes"][str(route)]
                self.assertEqual(report["current_rpm"], 0)
                self.assertEqual(report["capacity_rpm"], 2)
                self.assertEqual(report["ok"], 1)
                self.assertEqual(report["token_samples"], 1)
                engine.close()
                # A producer retry after compaction must still be deduplicated.
                local, _ = store.append("replay-test", [event(4, "success", "long-two", now)])
                self.assertEqual(local, 5)
                engine = Engine(p.home, config)
                repeated = engine.step()["report"]["routes"][str(route)]
                engine.close()
                self.assertEqual(repeated["attempts"], 2)
                self.assertEqual(repeated["ok"], 1)
                self.assertEqual(repeated["capacity_rpm"], 2)
            finally:
                store.close()

    def test_telemetry_queue_is_bounded_and_marks_a_gap(self):
        with patch("proxy.bridge.MAX_PENDING", 2):
            bridge = ServingBridge("unused", None)
            for _ in range(3):
                bridge.record("test")
            self.assertEqual(len(bridge.pending), 2)
            self.assertEqual(bridge.status()["telemetry_dropped"], 1)

    def test_serving_import_has_no_routing_dependency(self):
        with fixture() as (p, a, b):
            result = subprocess.run([PYTHON, "-c",
                "import sys; import proxy.server; "
                "assert not any(m == 'routing' or m.startswith('routing.') for m in sys.modules)"],
                cwd=ROOT, env=p.env, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
