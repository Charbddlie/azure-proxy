"""Routing survives transient SQLite locks and resumes without double counting."""

import json
import os
import sqlite3
import subprocess
import tempfile
import time
import unittest
import urllib.request
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_split import config_at, fixture, wait_for, wait_routing
from run_tests import DEPLOYMENT, MODEL, PYTHON, ask, who
from proxy.state import COMPACT_BATCH, Store, is_busy
from routing.__main__ import run
from routing.engine import Engine, route_record
from routing.quota import QuotaTracker, RouteState


class PenaltyRecoveryTests(unittest.TestCase):
    def test_recovery_handles_long_gaps_and_small_penalties(self):
        quota = QuotaTracker.__new__(QuotaTracker)
        quota.cfg = SimpleNamespace(demote_halflife=30)
        for penalty, seconds, expected in ((0.125, 30, 0.25), (0.125, 90, 1.0),
                                           (0.05, 86400, 1.0), (5e-324, 86400, 1.0),
                                           (0.0, 86400, 0.0)):
            with self.subTest(penalty=penalty, seconds=seconds):
                state = RouteState("test")
                state.penalty = penalty
                state.penalty_at = 100
                quota._decay(state, 100 + seconds)
                self.assertAlmostEqual(state.penalty, expected)
                self.assertEqual(state.penalty_at, 100 + seconds)

    def test_retry_after_is_respected_and_zero_halflife_recovers(self):
        quota = QuotaTracker.__new__(QuotaTracker)
        quota.cfg = SimpleNamespace(demote_halflife=30)
        state = RouteState("test")
        state.penalty = 0.25
        state.penalty_until = 200
        quota._decay(state, 200)
        self.assertEqual(state.penalty, 0.25)
        quota._decay(state, 230)
        self.assertAlmostEqual(state.penalty, 0.5)
        quota.cfg.demote_halflife = 0
        quota._decay(state, 231)
        self.assertEqual(state.penalty, 1.0)


class RetryLoopTests(unittest.TestCase):
    def test_busy_error_classification(self):
        for message in ("database is locked", "database table is locked: state",
                        "database schema is locked: main"):
            self.assertTrue(is_busy(sqlite3.OperationalError(message)))
        self.assertFalse(is_busy(sqlite3.OperationalError("disk I/O error")))
        self.assertFalse(is_busy(sqlite3.OperationalError("no such table: state")))
        error = sqlite3.OperationalError("extended busy")
        error.sqlite_errorcode = 517  # SQLITE_BUSY_SNAPSHOT
        self.assertTrue(is_busy(error))

    def test_busy_publications_retry_same_engine_and_log_recovery(self):
        engine = Mock()
        engine.step.side_effect = [sqlite3.OperationalError("database is locked"),
                                   sqlite3.OperationalError("database is locked"), {}, {}]
        stop = Mock()
        stop.is_set.side_effect = [False, False, False, True]
        with patch("routing.__main__.Engine", return_value=engine) as factory:
            with self.assertLogs("routing.__main__", level="INFO") as logs:
                run("unused", stop)
        factory.assert_called_once_with("unused")
        self.assertEqual(engine.step.call_count, 4)
        engine.step.assert_called_with(ready=False)
        engine.close.assert_called_once()
        self.assertEqual(len(logs.output), 2)
        self.assertIn("recovered", logs.output[-1])

    def test_busy_startup_retries_and_non_lock_errors_propagate(self):
        engine = Mock()
        stop = Mock()
        stop.is_set.side_effect = [False, False, True]
        with patch("routing.__main__.Engine", side_effect=[
                sqlite3.OperationalError("database is locked"), engine]) as factory:
            with self.assertLogs("routing.__main__"):
                run("unused", stop)
        self.assertEqual(factory.call_count, 2)
        engine.close.assert_called_once()

        engine = Mock()
        engine.step.side_effect = sqlite3.OperationalError("disk I/O error")
        stop = Mock()
        stop.is_set.return_value = False
        with patch("routing.__main__.Engine", return_value=engine):
            with self.assertRaisesRegex(sqlite3.OperationalError, "disk I/O"):
                run("unused", stop)
        engine.close.assert_called_once()
        stop.wait.assert_not_called()

    def test_shutdown_under_lock_releases_engine(self):
        engine = Mock()
        engine.step.side_effect = [{}, sqlite3.OperationalError("database is locked")]
        stop = Mock()
        stop.is_set.side_effect = [False, True]
        with patch("routing.__main__.Engine", return_value=engine):
            with self.assertLogs("routing.__main__") as logs:
                run("unused", stop)
        self.assertIn("heartbeat will expire", logs.output[-1])
        engine.close.assert_called_once()


class RecoveryTests(unittest.TestCase):
    def test_others_history_survives_zero_estimate_and_restart(self):
        with fixture() as (p, a, b):
            p.sync()
            p.stop_routing()
            config = config_at(p.home)
            config.balance = "capacity"
            config.foreign_enabled = True
            config.foreign_reclaim = 0.1
            engine = Engine(p.home, config)
            route = config.routes[MODEL][0]
            try:
                # Learned history outlives the diagnostic retention window.
                engine.now = time.time() - 2 * 86400
                engine.quota.observed(route, 200, {"x-ratelimit-limit-tokens": "1000000"})
                engine.quota.note_foreign(route)
                engine.now += 601
                engine.quota.charge(route, 1000000)
                engine.quota.note_foreign(route)
                self.assertEqual(engine.quota.state(route).foreign, 0)
                report = engine.step()["report"]["routes"][str(route)]
                self.assertTrue(report["foreign_seen"])
                self.assertEqual(report["selection_priority"], 1)
                engine.close()
                engine = Engine(p.home, config)
                snapshot = engine.step()
                report = snapshot["report"]["routes"][str(route)]
                self.assertEqual(report["other_tpm"], 0)
                self.assertTrue(report["foreign_seen"])
                self.assertEqual(report["selection_priority"], 1)
                targets = snapshot["tables"]["routes"][MODEL]
                self.assertEqual({t["endpoint"]: t["selection_priority"] for t in targets},
                                 {"alpha": 1, "beta": 0})
            finally:
                engine.close()

    def test_legacy_checkpoint_recovers_others_history_from_raw_estimate(self):
        with fixture() as (p, a, b):
            p.sync()
            p.stop_routing()
            config = config_at(p.home)
            config.balance = "capacity"
            config.foreign_enabled = True
            config.foreign_reclaim = 0.1
            engine = Engine(p.home, config)
            try:
                route = config.routes[MODEL][0]
                engine.quota.state(route)
                for foreign in (0.0, 0.25):
                    with self.subTest(foreign=foreign):
                        saved = engine.checkpoint()
                        state = saved["states"][str(route)]
                        state.pop("foreign_seen")
                        state.update(foreign=foreign, foreign_at=time.time() - 2 * 86400,
                                     foreign_hold_until=0)
                        engine.restore(saved)
                        restored = engine.quota.state(route)
                        self.assertEqual(engine.quota.foreign_load(restored, engine.now), 0)
                        self.assertEqual(restored.foreign_seen, foreign > 0)
                        self.assertEqual(engine.quota.selection_parameters([route])[0][0],
                                         int(foreign > 0))
                        self.assertEqual(engine.checkpoint()["states"][str(route)]["foreign_seen"],
                                         foreign > 0)
            finally:
                engine.close()

    def test_expired_demotions_are_excluded_from_replay(self):
        with fixture() as (p, a, b):
            p.sync()
            p.stop_routing()
            engine = Engine(p.home, config_at(p.home))
            writer = Store(p.home)
            try:
                route = engine.config.routes[MODEL][0]
                now = time.time()
                events = [dict(kind="demote", reason="429", retry_after=1,
                               local_seq=i + 1, at=at, route=route_record(route))
                          for i, at in enumerate((now - 86400, now))]
                writer.append("long-gap-test", events)
                report = engine.step()["report"]["routes"][str(route)]
                self.assertEqual(report["rate_limited"], 1)
                self.assertAlmostEqual(engine.quota.state(route).penalty,
                                       engine.config.demote_multiplier)
            finally:
                engine.close()
                writer.close()

    def test_publication_rollback_then_retry_preserves_exactly_once_counts(self):
        with fixture() as (p, a, b):
            p.sync()
            p.stop_routing()
            engine = Engine(p.home, config_at(p.home))
            writer = Store(p.home)
            try:
                before = engine.step()
                cursor = writer.get("checkpoint")["cursor"]
                route = engine.config.routes[MODEL][0]
                events = [dict(kind=kind, local_seq=i + 1, attempt="locked-attempt",
                               at=time.time(), route=route_record(route), request_bytes=10,
                               face=0, model=MODEL)
                          for i, kind in enumerate(("dispatch", "success", "finish"))]
                _, waterline = writer.append("lock-test", events)
                writer.db.execute("BEGIN IMMEDIATE")
                try:
                    with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                        engine.step()
                    self.assertEqual(engine.cursor, waterline)
                    self.assertEqual(writer.get("checkpoint")["cursor"], cursor)
                    self.assertEqual(writer.get("snapshot")["revision"], before["revision"])
                finally:
                    writer.db.rollback()
                for _ in range(2):
                    report = engine.step()["report"]["routes"][str(route)]
                    self.assertEqual((report["attempts"], report["ok"]), (1, 1))
                engine.close()
                engine = Engine(p.home, config_at(p.home))
                report = engine.step()["report"]["routes"][str(route)]
                self.assertEqual((report["attempts"], report["ok"]), (1, 1))
            finally:
                engine.close()
                writer.close()

    def test_running_process_survives_long_write_lock_and_resumes(self):
        with fixture() as (p, a, b):
            p.sync()
            pid = p.routing_proc.pid
            serving_pid = p.get_raw("/healthz")[1]["pid"]
            db = sqlite3.connect(os.path.join(p.home, "runtime", "control.sqlite3"))
            db.execute("BEGIN IMMEDIATE")
            try:
                request = urllib.request.Request(p.url("/v1/chat/completions"),
                    data=json.dumps({"model": MODEL, "messages": []}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=1) as reply:
                    self.assertEqual(who(json.load(reply)), "alpha")
                wait_for(lambda: not p.get_raw("/healthz")[1]["routing"]["ok"], timeout=6)
                self.assertIsNone(p.routing_proc.poll())
                self.assertEqual(p.get_raw("/healthz")[1]["pid"], serving_pid)
            finally:
                db.rollback()
                db.close()
            wait_routing(p)
            p.sync()
            self.assertEqual(p.routing_proc.pid, pid)
            report = p.get("/routes")[1]
            self.assertFalse(report["stats_stale"])
            route = report["routes"]["alpha/" + DEPLOYMENT]
            self.assertEqual((route["attempts"], route["ok"]), (1, 1))
            self.assertEqual(report["routing"]["telemetry_dropped"], 0)

    def test_startup_under_write_lock_recovers_in_same_process(self):
        with fixture() as (p, a, b):
            p.stop_routing()
            db = sqlite3.connect(os.path.join(p.home, "runtime", "control.sqlite3"))
            db.execute("BEGIN IMMEDIATE")
            try:
                p.routing_proc = subprocess.Popen([PYTHON, "-m", "routing"], env=p.env,
                    stdout=p.routing_log, stderr=subprocess.STDOUT)
                def retry_logged():
                    p.routing_log.seek(0)
                    return "routing database busy" in p.routing_log.read()
                wait_for(retry_logged, timeout=6)
                self.assertIsNone(p.routing_proc.poll())
            finally:
                db.rollback()
                db.close()
            wait_routing(p)
            self.assertIsNone(p.routing_proc.poll())
            self.assertEqual(ask(p)[0], 200)

    def test_compaction_is_bounded_and_continues_during_idle_publications(self):
        with tempfile.TemporaryDirectory(prefix="routing-compaction-test-") as root:
            store = Store(root)
            try:
                count = COMPACT_BATCH + 5
                events = [{"local_seq": i + 1} for i in range(count)]
                store.append("test", events)
                checkpoint = {"cursor": count, "events": []}
                snapshot = {"revision": 1, "events": {"next": 0, "events": []}}
                store.publish(checkpoint, snapshot)
                self.assertEqual(store.db.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0], 5)
                self.assertFalse(store.read_events(count))
                self.assertEqual(store.get("checkpoint")["cursor"], count)
                snapshot["revision"] = 2
                store.publish(checkpoint, snapshot)
                self.assertFalse(store.read_events(0))
                self.assertEqual(store.append("test", events), (count, count))
                self.assertFalse(store.read_events(0))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
