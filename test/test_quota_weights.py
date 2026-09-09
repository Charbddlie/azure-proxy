"""RPM-based selection, history, and per-endpoint fallback regressions."""

import collections
import random
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from proxy.affinity import weighted_order
from proxy.bridge import Target
from proxy.config import Route
from routing.quota import QuotaTracker


def route(endpoint, deployment="sol", tokens=None, requests=None):
    return Route(endpoint, "http://localhost/", "v", deployment, "max_tokens", 0,
                 capacity_tokens=tokens, capacity_requests=requests)


class CapacityWeightTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.cfg = SimpleNamespace(
            balance="capacity", weight_floor=0.05, observation_ttl=120,
            spill_threshold=0.7, demote_multiplier=0.25, demote_seconds=30,
            demote_halflife=30, load_window=60, rpm_window=60,
            foreign_enabled=True, foreign_reclaim=0.1,
            image_routes={}, image_edit_routes={})
        self.q = QuotaTracker(self.cfg, clock=lambda: self.now,
                              emit=lambda *args, **kwargs: None, persist_capacity=False)

    def learned(self, name, capacity, others=0, historical=False, tokens=None):
        r = route(name, tokens=tokens)
        st = self.q.state(r)
        st.safe_rpm, st.other_rpm = capacity, others
        st.foreign_seen = historical or others > 0
        return r

    def test_unknown_capacity_has_one_rpm_sampling_prior(self):
        routes = [route("a"), route("b", tokens=9e6, requests=9000)]
        self.assertEqual(self.q.capacity_estimates(routes), [1, 1])
        self.assertEqual(self.q.weights(routes), [1, 1])
        report = self.q.report({"sol": routes})["routes"]
        self.assertIsNone(report[str(routes[0])]["capacity_rpm"])
        self.assertEqual(report[str(routes[0])]["estimated_capacity_rpm"], 1)

    def test_learned_rpm_is_independent_of_declared_and_header_quotas(self):
        a, b = self.learned("a", 100, tokens=9e6), self.learned("b", 300, tokens=100)
        self.q.observed(a, 200, {"x-ratelimit-limit-tokens": "1",
                                "x-ratelimit-limit-requests": "900000",
                                "x-ratelimit-remaining-tokens": "-100"})
        self.assertEqual(self.q.weights([a, b]), [100, 300])
        self.q.state(a).safe_rpm = 200
        self.assertEqual(self.q.weights([b, a]), [300, 200])

    def test_available_rpm_deducts_our_requests_and_other_rpm(self):
        a, b = self.learned("a", 100, 60), self.learned("b", 300)
        for tokens in (1, 1e8, 50):
            self.q.charge(a, tokens)
        self.assertEqual(self.q.weights([a, b]), [37, 300])
        self.now += 61
        self.assertEqual(self.q.weights([a, b]), [40, 300])
        self.assertEqual(self.q.state(a).other_rpm, 60)

    def test_rpm_uses_its_own_window(self):
        a = self.learned("a", 100)
        self.q.rpm_window = 30
        self.q.charge(a, 1e8)
        self.assertEqual(self.q.weights([a]), [98])
        self.now += 31
        self.assertEqual(self.q.weights([a]), [100])

    def test_saturation_and_failure_keep_one_probe_floor(self):
        a = self.learned("a", 100, 100)
        self.assertEqual(self.q.weights([a]), [5])
        self.q.demote(a, "503", "30")
        self.assertEqual(self.q.weights([a]), [5])
        self.q.state(a).other_rpm = 20
        self.assertEqual(self.q.weights([a]), [5])
        self.now += 91
        self.assertEqual(self.q.weights([a]), [80])

    def test_reporting_a_shorter_token_window_preserves_rpm_history(self):
        a = self.learned("a", 100)
        self.cfg.load_window = 10
        self.q.charge(a, 1e9)
        self.now += 20
        for _ in range(2):
            report = self.q.report({"sol": [a]})["routes"][str(a)]
            self.assertEqual(report["sent_tokens_in_window"], 0)
            self.assertEqual(report["current_rpm"], 1)
            self.assertEqual(report["available_rpm"], 99)

    def test_text_and_image_routes_use_the_same_learned_rpm(self):
        a, b = route("a", "image", requests=9000), route("b", tokens=9e6)
        self.cfg.image_routes = {"image": [a]}
        self.q.state(a).safe_rpm = self.q.state(b).safe_rpm = 100
        for r in (a, b):
            self.q.charge(r, 1e8)
        self.assertEqual(self.q.weights([a, b]), [99, 99])

    def test_load_and_report_match_dashboard_rpm(self):
        a = self.learned("a", 100, 20, tokens=1)
        self.q.charge(a, 1e9, 0)
        self.q.charge(a, 1, 1)
        st = self.q.state(a)
        self.assertEqual(self.q.load(st, self.now), .02)
        self.assertEqual(self.q.foreign_load(st, self.now), .2)
        self.assertAlmostEqual(self.q.total_load(st, self.now), .22)
        self.assertEqual(self.q.load_dimension(st, self.now), "requests")
        self.assertAlmostEqual(sum(self.q.load_by_face(st, self.now).values()), .02)
        report = self.q.report({"sol": [a]})["routes"][str(a)]
        self.assertEqual((report["capacity_rpm"], report["other_rpm"],
                          report["current_rpm"], report["available_rpm"]), (100, 20, 2, 78))
        self.assertEqual((report["weight"], report["weight_unit"]), (78, "rpm"))
        self.assertFalse(any(key.endswith("_tpm") for key in report))

    def test_unknown_load_stays_unknown(self):
        st = self.q.state(route("a", tokens=9e6))
        self.assertIsNone(self.q.load(st, self.now))
        self.assertIsNone(self.q.load_by_face(st, self.now))
        self.assertEqual(self.q.foreign_load(st, self.now), 0)

    def test_history_is_set_only_by_positive_outside_rpm(self):
        a = route("a", tokens=1e6)
        st = self.q.state(a)
        st.foreign = .9  # old token-based diagnostic state
        self.q.observed(a, 200, {"x-ratelimit-limit-tokens": "1000000"})
        self.q.note_foreign(a, observed_rpm=1)
        self.assertFalse(st.foreign_seen)
        st.safe_rpm = 100
        self.q.note_foreign(a, observed_rpm=100)
        self.assertFalse(st.foreign_seen)
        self.q.note_foreign(a, observed_rpm=40)
        self.assertEqual(st.other_rpm, 60)
        self.assertTrue(st.foreign_seen)

    def test_time_alone_does_not_erase_observed_other_rpm(self):
        a = self.learned("a", 100, 60)
        self.now += 2 * 86400
        self.assertEqual(self.q.foreign_load(self.q.state(a), self.now), .6)
        self.assertEqual(self.q.selection_parameters([a]), [(2, 40)])

    def test_success_clears_current_others_and_keeps_history(self):
        a = self.learned("a", 100, 60)
        self.q.note_success(a, [self.now, 1, 0, 100])
        self.assertEqual(self.q.state(a).other_rpm, 0)
        self.assertTrue(self.q.state(a).foreign_seen)
        self.assertEqual(self.q.selection_parameters([a]), [(1, 1)])
        self.q.note_foreign(a, observed_rpm=20)
        self.assertEqual(self.q.selection_parameters([a]), [(2, 20)])

    def test_zero_others_throttle_keeps_history_clear(self):
        a = self.learned("a", 100)
        self.q.demote(a, "429", "30", observed_rpm=100)
        self.assertFalse(self.q.state(a).foreign_seen)
        self.assertEqual(self.q.selection_parameters([a]), [(0, 1)])

    def test_first_two_groups_are_uniform_despite_capacity_load_and_penalty(self):
        for historical, priority in ((False, 0), (True, 1)):
            self.setUp()
            routes = [self.learned(name, capacity, historical=historical)
                      for name, capacity in (("a", 1), ("b", 100), ("c", 9000))]
            self.q.charge(routes[0], 1e9)
            self.q.demote(routes[0], "503", "30")
            self.assertEqual(self.q.selection_parameters(routes), [(priority, 1)] * 3)
            self.assertEqual([r["share"] for r in self.q.report({"sol": routes})["models"]["sol"]],
                             [.3333] * 3)
            rng = random.Random(42)
            with patch("routing.quota.random.random", side_effect=rng.random):
                counts = collections.Counter(str(self.q.order(routes)[0]) for _ in range(1200))
            self.assertEqual(len(counts), 3)
            self.assertTrue(all(330 < n < 470 for n in counts.values()), counts)

    def test_three_groups_and_fallback_probabilities(self):
        contended = self.learned("a", 100, 25)
        clear = self.learned("b", 9000, historical=True)
        fresh = self.learned("c", 10)
        routes = [contended, clear, fresh]
        self.assertEqual(self.q.selection_parameters(routes), [(2, 75), (1, 1), (0, 1)])
        for random_value in (0, .5, .999):
            with patch("routing.quota.random.random", return_value=random_value):
                self.assertEqual(self.q.order(routes), [fresh, clear, contended])
        for candidates, expected in ((routes, [0, 0, 1]), (routes[:2], [0, 1]), (routes[:1], [1])):
            report = self.q.report({"sol": candidates})["models"]["sol"]
            self.assertEqual([r["share"] for r in report], expected)

    def test_third_group_weights_available_rpm(self):
        a, b = self.learned("a", 100, 25), self.learned("b", 200, 50)
        for _ in range(25):
            self.q.charge(a, 1e9)
        self.assertEqual(self.q.selection_parameters([a, b]), [(2, 50), (2, 150)])
        shares = self.q.report({"sol": [a, b]})["models"]["sol"]
        self.assertEqual([r["share"] for r in shares], [.25, .75])

    def test_legacy_foreign_switch_does_not_disable_rpm_observations(self):
        a = self.learned("a", 100, 50)
        self.cfg.foreign_enabled = False
        self.assertEqual(self.q.selection_parameters([a]), [(2, 50)])
        report = self.q.report({"sol": [a]})["routes"][str(a)]
        self.assertTrue(report["foreign_seen"])
        self.assertEqual(report["other_rpm"], 50)

    def test_serving_respects_all_three_groups_after_endpoint_filtering(self):
        targets = [Target("other", "zero", None, {}, {}, 1, 0),
                   Target("bound", "clear-small", None, {}, {}, 1, 1),
                   Target("bound", "clear-large", None, {}, {}, 1, 1),
                   Target("bound", "small", None, {}, {}, 100, 2),
                   Target("bound", "large", None, {}, {}, 900, 2)]
        self.assertEqual(weighted_order(targets)[0], targets[0])
        rng = random.Random(42)
        with patch("proxy.affinity.random.choices", side_effect=rng.choices):
            counts = collections.Counter(weighted_order(targets[1:])[0].deployment for _ in range(1000))
            self.assertEqual(set(counts), {"clear-small", "clear-large"})
            self.assertTrue(450 < counts["clear-large"] < 550, counts)
            counts = collections.Counter(weighted_order(targets[3:])[0].deployment for _ in range(1000))
        self.assertTrue(850 < counts["large"] < 950, counts)
        self.assertEqual({str(r) for r in weighted_order(targets)[1:]}, {str(r) for r in targets[1:]})
        self.assertEqual([r.selection_priority for r in weighted_order(targets)], [0, 1, 1, 2, 2])

    def test_empty_route_set(self):
        self.assertEqual(self.q.weights([]), [])
        self.assertEqual(self.q.selection_parameters([]), [])


if __name__ == "__main__":
    unittest.main()
