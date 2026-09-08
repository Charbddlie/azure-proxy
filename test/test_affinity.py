"""Endpoint affinity, atomic first dispatch, recovery and failure invariants."""

import concurrent.futures
import asyncio
import copy
import json
import multiprocessing
import os
import signal
import sqlite3
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from run_tests import (CODEX_BODY, MODEL, DEPLOYMENT, Proxy, FakeAzure,
                       Behaviour, codex_turn, sticky_ask, PYTHON)
from test_split import config_at, edit_models, fixture, wait_for, wait_routing
from proxy.affinity import (AffinityStore, AffinityError, SessionAffinity,
                            carries_encrypted, digest, resource, weighted_order)
from proxy.bridge import Target
from proxy.config import Config, TABLES
from routing.engine import Engine, target_record


def bind_in_process(root, endpoint, queue):
    store = AffinityStore(root)
    queue.put(store.bind(digest("family"), endpoint, endpoint, {"tables": {}})["endpoint"])
    store.close()


class StoreTests(unittest.TestCase):
    def test_active_window_filters_all_counts_without_expiring_bindings(self):
        with tempfile.TemporaryDirectory() as root:
            now = [1000]
            store = AffinityStore(root, clock=lambda: now[0], active_window=300)
            for family in ("idle", "unattributed"):
                store.bind(family, "alpha", "identity", {})
            store.note_model("idle", "sol")
            now[0] = 1300
            self.assertEqual(store.report()["live_sessions"], 2)
            now[0] += .01
            report = store.report()
            self.assertEqual(report["live_sessions"], 0)
            self.assertEqual(report["sessions_per_endpoint"], {})
            self.assertEqual(report["sessions_per_model"], {})
            self.assertEqual(report["unattributed_sessions"], 0)
            self.assertEqual(report["retained_sessions"], 2)
            self.assertEqual(report["active_window_seconds"], 300)
            binding = store.get("idle")
            self.assertIsNotNone(binding)
            store.touch("idle", binding["expires"])
            store.flush()
            report = store.report()
            self.assertEqual(report["live_sessions"], 1)
            self.assertEqual(report["sessions_per_endpoint"], dict(alpha=1))
            self.assertEqual(report["sessions_per_model"], dict(sol=dict(total=1, endpoints=dict(alpha=1))))
            store.close()
            store = AffinityStore(root, clock=lambda: now[0], active_window=600)
            self.assertEqual(store.report()["live_sessions"], 2)
            store.close()

    def test_invalid_active_window(self):
        with tempfile.TemporaryDirectory() as root:
            for window in (0, -1, float("nan"), float("inf")):
                with self.assertRaisesRegex(ValueError, "active window"):
                    AffinityStore(root, active_window=window)

    def test_fork_inherits_durable_descriptor_and_has_independent_activity(self):
        with tempfile.TemporaryDirectory() as root:
            now = [1000]
            store = AffinityStore(root, clock=lambda: now[0])
            source = store.bind("parent", "alpha", "identity", {"tables": {"old": []}})
            store.note_model("parent", "parent-model")
            now[0] += 301
            child = store.inherit("child", "parent")
            self.assertEqual(child["catalog"], source["catalog"])
            self.assertEqual(child["endpoint"], "alpha")
            self.assertTrue(child["created"])
            self.assertEqual(store.report()["live_sessions"], 1)
            self.assertEqual(store.report()["sessions_per_model"], {})
            self.assertEqual(store.report()["retained_sessions"], 2)
            store.close()
            store = AffinityStore(root, clock=lambda: now[0])
            self.assertEqual(store.get("child")["endpoint"], "alpha")
            self.assertEqual(store.inherit("grandchild", "child")["endpoint"], "alpha")
            self.assertEqual(store.db.execute("SELECT COUNT(*) FROM descriptors").fetchone()[0], 1)
            self.assertFalse(store.inherit("child", "parent").get("created", False))
            store.close()

    def test_missing_or_expired_parent_is_not_used_to_create_a_binding(self):
        with tempfile.TemporaryDirectory() as root:
            now = [1000]
            store = AffinityStore(root, ttl=10, clock=lambda: now[0])
            self.assertIsNone(store.inherit("child", "missing"))
            store.bind("parent", "alpha", "identity", {})
            now[0] += 11
            self.assertIsNone(store.inherit("child", "parent"))
            self.assertIsNone(store.get("child"))
            store.close()

    def test_bound_fork_no_longer_depends_on_parent_lifetime(self):
        affinity = SessionAffinity(SimpleNamespace(responses_routes={"model": []},
            endpoint_identities={}, scope="scope", routes={}, image_routes={}, image_edit_routes={}), "unused")
        target = Mock(endpoint="alpha", scope="scope", selection_weight=None,
                      routing_data={"url": "https://alpha.example/"})
        affinity.cfg.responses_routes["model"] = [target]
        affinity.store = Mock()
        binding = dict(endpoint="alpha", identity=resource(target, "scope"), expires=9999)
        affinity.store.get.return_value = binding
        routes, result = affinity.resolve("child", dict(model="model", input=[{"encrypted_content": "cipher"}]),
                                          "responses_routes", parent="parent")
        self.assertEqual(routes, [target])
        self.assertIs(result, binding)
        affinity.store.inherit.assert_not_called()

    def test_inheritance_cannot_move_an_existing_child(self):
        with tempfile.TemporaryDirectory() as root:
            store = AffinityStore(root)
            store.bind("parent", "alpha", "identity", {})
            store.bind("child", "beta", "other-identity", {})
            with self.assertRaises(AffinityError) as error:
                store.inherit("child", "parent")
            self.assertEqual(error.exception.code, "affinity_parent_conflict")
            self.assertEqual(store.get("child")["endpoint"], "beta")
            store.close()

    def test_parent_metadata_is_optional_and_validated(self):
        for raw in (None, "", "broken", "[]", "null", '{"forked_from_thread_id":42}',
                    '{"forked_from_thread_id":" "}', " " * 16385):
            self.assertIsNone(SessionAffinity.parent_family(SimpleNamespace(
                headers={"x-codex-turn-metadata": raw})))
        self.assertEqual(SessionAffinity.parent_family(SimpleNamespace(headers={
            "x-codex-turn-metadata": '{"forked_from_thread_id":" parent "}'})), digest("parent"))
        body = dict(client_metadata={"x-codex-turn-metadata": '{"forked_from_thread_id":"parent"}'})
        self.assertEqual(SessionAffinity.parent_family(SimpleNamespace(headers={}), body), digest("parent"))
        for value in (None, [], "bad", {"x-codex-turn-metadata": 42},
                      {"x-codex-turn-metadata": {"forked_from_thread_id": "parent"}}):
            self.assertIsNone(SessionAffinity.parent_family(SimpleNamespace(headers={}),
                                                           dict(client_metadata=value)))

    def test_model_counts_are_unique_per_family_endpoint_and_persist(self):
        with tempfile.TemporaryDirectory() as root:
            store = AffinityStore(root)
            for family, endpoint in (("one", "alpha"), ("two", "alpha"), ("three", "beta")):
                store.bind(digest(family), endpoint, endpoint, {})
            for family, model in (("one", "sol"), ("one", "sol"), ("one", "terra"),
                                  ("two", "terra"), ("three", "sol")):
                store.note_model(digest(family), model)
            store.note_model(digest("missing"), "sol")
            expected = dict(sol=dict(total=2, endpoints=dict(alpha=1, beta=1)),
                            terra=dict(total=2, endpoints=dict(alpha=2)))
            self.assertEqual(store.report()["live_sessions"], 3)
            self.assertEqual(store.report()["sessions_per_model"], expected)
            store.close()
            store = AffinityStore(root)
            self.assertEqual(store.report()["sessions_per_model"], expected)
            self.assertEqual(store.report()["unattributed_sessions"], 0)
            store.close()

    def test_expiry_rebinding_and_cleanup_retire_old_model_counts(self):
        with tempfile.TemporaryDirectory() as root:
            now = [1000]
            store = AffinityStore(root, ttl=10, clock=lambda: now[0])
            family = digest("family")
            store.bind(family, "alpha", "alpha", {})
            store.note_model(family, "sol")
            now[0] += 11
            self.assertEqual(store.report()["sessions_per_model"], {})
            store.bind(family, "beta", "beta", {})
            self.assertEqual(store.report()["sessions_per_model"], {})
            store.note_model(family, "terra")
            self.assertEqual(store.report()["sessions_per_model"],
                             dict(terra=dict(total=1, endpoints=dict(beta=1))))
            now[0] += 11
            store.cleanup()
            self.assertEqual(store.db.execute("SELECT COUNT(*) FROM binding_models").fetchone()[0], 0)
            store.close()

    def test_existing_binding_database_adds_model_tracking_without_guessing(self):
        with tempfile.TemporaryDirectory() as root:
            store = AffinityStore(root)
            family = digest("existing")
            store.bind(family, "alpha", "alpha", {"tables": {"routes": {"sol": [], "terra": []}}})
            store.db.execute("DROP TABLE binding_models")
            store.close()
            store = AffinityStore(root)
            self.assertEqual(store.get(family)["endpoint"], "alpha")
            self.assertEqual(store.report()["sessions_per_model"], {})
            self.assertEqual(store.report()["unattributed_sessions"], 1)
            store.note_model(family, "sol")
            self.assertEqual(store.report()["sessions_per_model"],
                             dict(sol=dict(total=1, endpoints=dict(alpha=1))))
            store.close()

    def test_model_statistics_write_failure_does_not_reject_bound_request(self):
        affinity = SessionAffinity(SimpleNamespace(affinity_keys=["header:session-id"]), "unused")
        affinity.store = Mock()
        affinity.store.note_model.side_effect = sqlite3.OperationalError("locked")
        binding = dict(endpoint="alpha")
        request = SimpleNamespace(headers={"session-id": "family"}, state=SimpleNamespace())
        with patch.object(affinity, "resolve", return_value=(["target"], binding)):
            routes = asyncio.run(affinity.prepare(request, dict(model="sol"), "routes"))
        self.assertEqual(routes, ["target"])
        self.assertEqual(request.state.affinity_binding, binding)
        self.assertEqual(affinity.store.error, "OperationalError")

    def test_concurrent_processes_commit_one_winner(self):
        with tempfile.TemporaryDirectory() as root:
            AffinityStore(root).close()
            context = multiprocessing.get_context("spawn")
            queue = context.Queue()
            processes = [context.Process(target=bind_in_process, args=(root, str(i), queue)) for i in range(8)]
            for process in processes:
                process.start()
            results = [queue.get(timeout=10) for _ in processes]
            for process in processes:
                process.join(10)
                self.assertEqual(process.exitcode, 0)
            self.assertEqual(len(set(results)), 1)

    def test_48_hour_renewal_expiry_reopen_and_permissions(self):
        with tempfile.TemporaryDirectory() as root:
            now = [1000.0]
            store = AffinityStore(root, clock=lambda: now[0])
            family = digest("private-session-name")
            binding = store.bind(family, "alpha", "identity", {"tables": {}})
            now[0] += 172799
            store.touch(family, binding["expires"])
            store.close()
            store = AffinityStore(root, clock=lambda: now[0])
            self.assertEqual(store.get(family)["expires"], now[0] + 172800)
            now[0] += 172800
            self.assertIsNone(store.get(family))
            store.cleanup()
            self.assertEqual(store.db.execute("SELECT COUNT(*) FROM bindings").fetchone()[0], 0)
            self.assertEqual(store.db.execute("SELECT COUNT(*) FROM descriptors").fetchone()[0], 0)
            store.close()
            path = os.path.join(root, "runtime", "affinity.sqlite3")
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            with open(path, "rb") as file:
                self.assertNotIn(b"private-session-name", file.read())

    def test_descriptor_dedup_and_no_memory_eviction(self):
        with tempfile.TemporaryDirectory() as root:
            store = AffinityStore(root)
            for i in range(4200):
                store.bind(digest(str(i)), "alpha", "identity", {"tables": {}})
            self.assertIsNotNone(store.get(digest("0")))
            self.assertEqual(store.db.execute("SELECT COUNT(*) FROM descriptors").fetchone()[0], 1)
            store.close()

    def test_invalid_ttl(self):
        with tempfile.TemporaryDirectory() as root:
            for ttl in (0, -1, float("nan"), float("inf")):
                with self.assertRaises(ValueError):
                    AffinityStore(root, ttl)

    def test_failed_commit_does_not_create_binding(self):
        with tempfile.TemporaryDirectory() as root:
            store = AffinityStore(root)
            store.db.execute("PRAGMA query_only=ON")
            with self.assertRaises(sqlite3.OperationalError):
                store.bind(digest("family"), "alpha", "identity", {})
            self.assertIsNone(store.get(digest("family")))
            store.close()

    def test_24_hour_log_cleanup_is_independent_of_48_hour_binding(self):
        from proxy.state import Store
        with tempfile.TemporaryDirectory() as root:
            affinity = AffinityStore(root)
            control = Store(root)
            family = digest("family")
            affinity.bind(family, "alpha", "identity", {})
            control.append("worker", [dict(kind="event", local_seq=1, at=time.time() - 90000)])
            self.assertEqual(control.expire(), 1)
            self.assertIsNotNone(affinity.get(family))
            control.close(); affinity.close()

    def test_descriptor_cache_eviction_does_not_evict_bindings(self):
        with tempfile.TemporaryDirectory() as root:
            store = AffinityStore(root)
            for i in range(150):
                key = digest(str(i))
                store.bind(key, "alpha", "identity", {"generation": i})
                store.get(key)
            self.assertEqual(len(store.catalog_cache), 128)
            self.assertIsNotNone(store.get(digest("0")))
            store.close()

    def test_keys_normalize_carriers_and_ignore_model_thread(self):
        config = SimpleNamespace(affinity_keys=["header:session-id", "body:prompt_cache_key",
                                                "body:client_metadata.session_id", "header:x-session-id"])
        affinity = SessionAffinity(config, "unused")
        parent = SimpleNamespace(headers={"session-id": " root ", "thread-id": "parent"})
        child = SimpleNamespace(headers={"thread-id": "child"})
        self.assertEqual(affinity.family(parent, {"model": "a", "prompt_cache_key": "other"}),
                         affinity.family(child, {"model": "b", "prompt_cache_key": "root"}))
        self.assertTrue(affinity.sticky({"input": [{"content": [{"encrypted_content": "cipher"}]}]}))


class AffinityIntegrationTests(unittest.TestCase):
    def test_fork_inheritance_is_model_and_endpoint_name_agnostic(self):
        a, b = FakeAzure("renamed-resource"), FakeAzure("future-backup")
        a.start()
        b.start()
        p = None
        try:
            p = Proxy([("renamed-resource", a.url), ("future-backup", b.url)])
            p.stop_routing()
            edit_models(p, lambda doc: doc["models"].update({
                name: copy.deepcopy(doc["models"][MODEL])
                for name in ("gpt-7-azure", "arbitrary-model-alias")}))
            p.start_routing()
            wait_routing(p)
            parent = dict(CODEX_BODY, prompt_cache_key="parent")
            self.assertEqual(p.post(parent, path="/v1/responses")[0], 200)
            for model in ("gpt-7-azure", "arbitrary-model-alias"):
                body = dict(CODEX_BODY, model=model, prompt_cache_key=model,
                            input=[{"encrypted_content": "parent-state"}],
                            client_metadata={"x-codex-turn-metadata":
                                             '{"forked_from_thread_id":"parent"}'})
                result = p.post(body, path="/v1/responses")
                self.assertEqual(result[0], 200, result)
                self.assertEqual(result[2]["x-azure-proxy-route"], "renamed-resource/" + DEPLOYMENT)
            self.assertEqual(b.hits, 0)
        finally:
            if p:
                p.close()
            a.stop()
            b.stop()

    def test_ephemeral_fork_inherits_parent_then_survives_restart_and_nested_forks(self):
        from test_rolling import roll
        with fixture() as (p, a, b):
            self.assertEqual(p.post(dict(CODEX_BODY, prompt_cache_key="parent"),
                                   path="/v1/responses")[0], 200)
            p.stop_routing()
            def retire(doc):
                for entry in doc["models"].values():
                    entry["routes"] = [route for route in entry["routes"] if route["endpoint"] == "beta"]
            edit_models(p, retire)
            p.start_routing()
            wait_routing(p)
            items = [{"type": "reasoning", "encrypted_content": "parent-cipher"}]
            for parent, child in (("parent", "side"), ("side", "grandchild")):
                body = dict(CODEX_BODY, stream=False, prompt_cache_key=child, input=items)
                headers = {"session-id": child, "thread-id": child, "x-codex-turn-metadata":
                           json.dumps(dict(forked_from_thread_id=parent, session_id=child))}
                if child == "grandchild":
                    body["client_metadata"] = {"x-codex-turn-metadata": headers["x-codex-turn-metadata"]}
                    headers = {}  # copilot-api's provider route forwards only the body copy.
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    results = list(pool.map(lambda _: p.post(body, headers=headers, path="/v1/responses"), range(4)))
                self.assertTrue(all(r[0] == 200 for r in results), results)
                self.assertTrue(all(r[2]["x-azure-proxy-route"] == "alpha/" + DEPLOYMENT for r in results))
                self.assertEqual(a.requests[-1]["body"]["input"], items)
                self.assertTrue(roll(p)["ok"])
                wait_for(lambda: not p.get_raw("/healthz")[1]["supervisor"]["draining"])
                self.assertEqual(p.post(body, path="/v1/responses")[0], 200)
            self.assertEqual(b.hits, 0)
            self.assertEqual(p.get("/healthz")[1]["session_affinity"]["retained_sessions"], 3)

    def test_unknown_fork_parent_fails_before_dispatch(self):
        with fixture() as (p, a, b):
            body = dict(CODEX_BODY, prompt_cache_key="side", input=[{"encrypted_content": "cipher"}])
            result = p.post(body, headers={"x-codex-turn-metadata":
                            '{"forked_from_thread_id":"unknown"}'}, path="/v1/responses")
            self.assertEqual(result[0], 409)
            self.assertEqual(result[1]["error"]["code"], "affinity_missing")
            self.assertEqual(a.hits + b.hits, 0)

    def test_active_window_setting_is_validated(self):
        with fixture() as (p, a, b):
            policy = config_at(p.home).policy
            self.assertEqual(config_at(p.home).affinity_active_window, 300)
            for window in (0, -1, float("nan"), float("inf")):
                policy["routing"]["session_affinity"]["active_window_seconds"] = window
                with patch("proxy.config.yaml.safe_load", return_value=policy):
                    with self.assertRaisesRegex(ValueError, "active window"):
                        Config(load_routes=False)
            policy["routing"]["session_affinity"]["active_window_seconds"] = 600
            with patch("proxy.config.yaml.safe_load", return_value=policy):
                self.assertEqual(Config(load_routes=False).affinity_active_window, 600)

    def test_first_dispatch_is_committed_and_concurrent_family_never_splits(self):
        a, b = FakeAzure("alpha", [Behaviour(delay=.2)]).start(), FakeAzure("beta").start()
        p = Proxy([("alpha", a.url), ("beta", b.url)], balance="capacity")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
                results = list(pool.map(lambda _: sticky_ask(p), range(24)))
            self.assertTrue(all(result[0] == 200 for result in results))
            self.assertEqual(len({r[2]["x-azure-proxy-route"].split("/")[0] for r in results}), 1)
            db = sqlite3.connect(os.path.join(p.home, "runtime", "affinity.sqlite3"))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM bindings").fetchone()[0], 1)
            db.close()
        finally:
            p.close(); a.stop(); b.stop()

    def test_missing_identifier_and_unknown_old_state_fail_before_dispatch(self):
        with fixture() as (p, a, b):
            for body, code in ((dict(model=MODEL, store=True), "session_id_required"),
                               (dict(model=MODEL, prompt_cache_key="new", previous_response_id="r"), "affinity_missing"),
                               (dict(model=MODEL, prompt_cache_key="new", input=[{"encrypted_content": "cipher"}]), "affinity_missing")):
                result = p.post(body, path="/v1/responses")
                self.assertEqual(result[1]["error"]["code"], code)
            self.assertEqual(a.hits + b.hits, 0)

    def test_cross_model_subagent_and_plain_followup_inherit_endpoint(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            edit_models(p, lambda doc: doc["models"].update(child=copy.deepcopy(doc["models"][MODEL])))
            p.start_routing(); wait_routing(p)
            self.assertEqual(sticky_ask(p)[0], 200)
            from run_tests import CODEX_SESSION
            items = [{"type": "agent_message", "content": [{"type": "text", "text": "instruction"},
                      {"type": "encrypted_content", "encrypted_content": "cipher"}]}]
            for body in (dict(model="child", input=items), dict(model="child", input="plain")):
                result = p.post(body, headers={"session-id": CODEX_SESSION, "thread-id": "child"}, path="/v1/responses")
                self.assertEqual(result[0], 200)
                self.assertEqual(result[2]["x-azure-proxy-route"], "alpha/" + DEPLOYMENT)
            self.assertEqual(a.requests[-2]["body"]["input"], items)
            self.assertEqual(b.hits, 0)
            for path in ("/healthz", "/routes"):
                report = p.get(path)[1]["session_affinity"]
                self.assertEqual(report["sessions_per_model"]["child"],
                                 dict(total=1, endpoints=dict(alpha=1)))
                self.assertEqual(report["sessions_per_model"][MODEL],
                                 dict(total=1, endpoints=dict(alpha=1)))
                self.assertEqual(report["live_sessions"], 1)

    def test_upstream_refusal_never_taints_or_mutates_state(self):
        failure = {"error": {"code": "invalid_encrypted_content", "message": "unchanged upstream error"}}
        with fixture([Behaviour(), Behaviour(status=400, body=failure), Behaviour()]) as (p, a, b):
            self.assertEqual(sticky_ask(p)[0], 200)
            body = codex_turn(input=[{"type": "reasoning", "encrypted_content": "cipher"},
                                     {"type": "message", "content": "tail"}])
            for expected in (400, 200):
                result = p.post(body, path="/v1/responses")
                self.assertEqual(result[0], expected)
                if expected == 400:
                    self.assertEqual(result[1], failure)
                self.assertEqual(a.requests[-1]["body"]["input"], body["input"])
            self.assertEqual(b.hits, 0)

    def test_model_unavailable_and_resource_identity_conflict_preserve_binding(self):
        with fixture() as (p, a, b):
            self.assertEqual(sticky_ask(p)[0], 200)
            body = codex_turn(model="absent")
            self.assertEqual(p.post(body, path="/v1/responses")[1]["error"]["code"], "bound_model_unavailable")
            p.stop_routing()
            path = os.path.join(p.home, "runtime", "sources.json")
            with open(path) as file:
                sources = json.load(file)
            sources["endpoints"][0]["url"] = b.url
            with open(path, "w") as file:
                json.dump(sources, file)
            p.start_routing(); wait_routing(p)
            self.assertEqual(sticky_ask(p)[1]["error"]["code"], "endpoint_identity_conflict")
            self.assertEqual(b.hits, 0)

    def test_database_lock_prevents_first_dispatch(self):
        with fixture() as (p, a, b):
            db = sqlite3.connect(os.path.join(p.home, "runtime", "affinity.sqlite3"))
            db.execute("BEGIN IMMEDIATE")
            try:
                result = sticky_ask(p)
                self.assertEqual(result[0], 503)
                self.assertEqual(result[1]["error"]["code"], "affinity_store_unavailable")
                self.assertEqual(a.hits + b.hits, 0)
            finally:
                db.rollback(); db.close()

    def test_retryable_refusals_preserve_all_encrypted_fields_and_order(self):
        with fixture([Behaviour(), Behaviour(status=429), Behaviour(status=500), Behaviour()]) as (p, a, b):
            self.assertEqual(sticky_ask(p)[0], 200)
            body = codex_turn(input=[{"type": "reasoning", "encrypted_content": "first"},
                                    {"type": "agent_message", "content": [
                                        {"type": "text", "text": "middle"},
                                        {"type": "encrypted_content", "encrypted_content": "last"}]}])
            self.assertEqual(p.post(body, path="/v1/responses")[0], 200)
            self.assertEqual(len(a.requests), 4)
            self.assertTrue(all(r["body"]["input"] == body["input"] for r in a.requests[1:]))
            self.assertEqual(b.hits, 0)

    def test_inflight_activity_renews_short_ttl(self):
        import asyncio
        with tempfile.TemporaryDirectory() as root:
            async def run():
                config = SimpleNamespace(affinity_ttl=.4, affinity_active_window=.2)
                affinity = SessionAffinity(config, root)
                await affinity.start()
                try:
                    key = digest("family")
                    affinity.store.bind(key, "alpha", "identity", {})
                    affinity.active[key] = 1
                    await asyncio.sleep(1.1)
                    self.assertIsNotNone(affinity.store.get(key))
                    self.assertEqual((await affinity.status())["live_sessions"], 1)
                    affinity.active.clear()
                    await asyncio.sleep(.6)
                    self.assertIsNone(affinity.store.get(key))
                finally:
                    await affinity.stop()
            asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
