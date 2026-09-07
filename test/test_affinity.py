"""Endpoint affinity, atomic first dispatch, recovery and failure invariants."""

import concurrent.futures
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

from run_tests import (CODEX_BODY, MODEL, DEPLOYMENT, Proxy, FakeAzure,
                       Behaviour, codex_turn, sticky_ask, PYTHON)
from test_split import config_at, edit_models, fixture, wait_for, wait_routing
from proxy.affinity import (AffinityStore, AffinityError, SessionAffinity,
                            carries_encrypted, digest, resource, weighted_order)
from proxy.bridge import Target
from proxy.config import TABLES
from routing.engine import Engine, target_record


def bind_in_process(root, endpoint, queue):
    store = AffinityStore(root)
    queue.put(store.bind(digest("family"), endpoint, endpoint, {"tables": {}})["endpoint"])
    store.close()


class StoreTests(unittest.TestCase):
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
                config = SimpleNamespace(affinity_ttl=.4)
                affinity = SessionAffinity(config, root)
                await affinity.start()
                try:
                    key = digest("family")
                    affinity.store.bind(key, "alpha", "identity", {})
                    affinity.active[key] = 1
                    await asyncio.sleep(1.1)
                    self.assertIsNotNone(affinity.store.get(key))
                    affinity.active.clear()
                    await asyncio.sleep(.6)
                    self.assertIsNone(affinity.store.get(key))
                finally:
                    await affinity.stop()
            asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
