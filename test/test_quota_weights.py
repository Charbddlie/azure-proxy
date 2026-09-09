"""Deterministic capacity-weight regressions; no upstream requests."""

import unittest
import collections
import random
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
            headroom_high_water=0.5, spill_threshold=0.7,
            demote_multiplier=0.25, demote_seconds=30, demote_halflife=30,
            load_window=60, foreign_enabled=True, foreign_reclaim=0.1,
            static_weights={"a": 333, "b": 1000, "c": 499, "d": 850},
            image_routes={}, image_edit_routes={})
        self.q = QuotaTracker(self.cfg, clock=lambda: self.now,
                              emit=lambda *args, **kwargs: None,
                              persist_capacity=False)

    def observe(self, r, tokens):
        self.q.observed(r, 200, {"x-ratelimit-limit-tokens": str(tokens)})

    def test_all_unknown_start_at_one_million_tpm(self):
        routes = [route("a"), route("b"), route("d")]
        self.assertEqual(self.q.weights(routes), [1e6, 1e6, 1e6])

    def test_partial_knowledge_uses_mean_of_known_deployment_ceilings(self):
        routes = [route("a", tokens=333000), route("b", tokens=1000000),
                  route("c", tokens=499000), route("d"), route("d", "sol-dz")]
        for r in routes[:3]:
            self.observe(r, r.capacity_tokens)
        mean = (333000 + 1000000 + 499000) / 3
        self.assertEqual(self.q.weights(routes), [333000, 1000000, 499000, mean, mean])
        # Repeated estimation must keep imputed values out of the known set.
        self.observe(routes[0], 600000)
        mean = (600000 + 1000000 + 499000) / 3
        self.assertEqual(self.q.weights(routes), [600000, 1000000, 499000, mean, mean])
        self.assertIsNone(self.q.state(routes[3]).limit_tokens)
        self.observe(routes[3], 2000000)
        mean = (600000 + 1000000 + 499000 + 2000000) / 4
        self.assertEqual(self.q.weights(routes), [600000, 1000000, 499000, 2000000, mean])

    def test_declared_tpm_is_known_and_observed_tpm_takes_precedence(self):
        routes = [route("a", tokens=300000), route("b", tokens=900000), route("d")]
        self.assertEqual(self.q.weights(routes), [300000, 900000, 600000])
        self.observe(routes[0], 600000)
        self.assertEqual(self.q.weights(routes), [600000, 900000, 750000])

    def test_request_limits_are_not_tpm(self):
        a, b = route("a", requests=333), route("b")
        self.q.observed(a, 200, {"x-ratelimit-limit-requests": "333"})
        self.assertEqual(self.q.weights([a, b]), [1e6, 1e6])
        self.observe(b, 600000)
        self.assertEqual(self.q.weights([a, b]), [600000, 600000])

    def test_invalid_ceilings_are_excluded_from_mean(self):
        known = route("a", tokens=600000)
        for value in (0, -1, float("inf"), float("nan")):
            with self.subTest(value=value):
                unknown = route("b", tokens=value)
                self.q.state(unknown).limit_tokens = value
                self.assertEqual(self.q.weights([known, unknown]), [600000, 600000])

    def test_mean_is_local_to_the_model_and_is_independent_of_order(self):
        a, b, c = route("a", tokens=300000), route("b", tokens=900000), route("d")
        self.assertEqual(self.q.weights([c, b, a]), [600000, 900000, 300000])
        self.observe(route("unrelated", "other-model"), 90000000)
        self.assertEqual(self.q.weights([a, b, c]), [300000, 900000, 600000])

    def test_foreign_and_current_tpm_are_subtracted_per_deployment(self):
        a, b = route("a", tokens=1e6), route("a", "sol-dz", tokens=1e6)
        for r in (a, b):
            self.observe(r, 1e6)
        self.q.state(a).foreign = 0.6
        self.q.state(a).foreign_at = self.now
        self.q.charge(a, 200000)
        self.q.observed(a, 200, {"x-ratelimit-remaining-tokens": "-100"})
        self.assertEqual(self.q.weights([a, b]), [200000, 1e6])
        self.now += 120
        self.assertAlmostEqual(self.q.weights([a, b])[0], 600000)
        self.cfg.foreign_enabled = False
        self.assertEqual(self.q.weights([a, b]), [1e6, 1e6])

    def test_unknown_capacity_also_deducts_foreign_and_current_usage(self):
        a, b = route("a", tokens=600000), route("b")
        self.q.state(b).foreign = 0.25
        self.q.state(b).foreign_at = self.now
        self.q.charge(b, 90000)
        self.assertEqual(self.q.weights([a, b]), [600000, 360000])

    def test_current_tokens_are_normalized_to_tpm_and_expire(self):
        a, b = route("a", tokens=1e6), route("b", tokens=1e6)
        self.cfg.load_window = 30
        self.cfg.foreign_enabled = False
        self.q.charge(a, 100000)
        self.assertEqual(self.q.weights([a, b]), [800000, 1e6])
        self.now += 31
        self.assertEqual(self.q.weights([a, b]), [1e6, 1e6])

    def test_own_saturation_keeps_the_minimum_probe_weight(self):
        a, b = route("a", tokens=600000), route("b")
        self.q.charge(a, 900000)
        self.assertEqual(self.q.weights([a, b]), [30000, 600000])

    def test_parking_and_foreign_load_share_one_weight_floor(self):
        a, b = route("a", tokens=1e6), route("b", tokens=1e6)
        self.observe(a, 1e6)
        self.q.demote(a, "429", "30")
        self.assertEqual(self.q.weights([a, b]), [50000, 1e6])
        self.now += 630
        self.assertEqual(self.q.weights([a, b]), [1e6, 1e6])

    def test_image_capacity_stays_in_requests_per_minute(self):
        a, b = route("a", "image", requests=2), route("b", "image", requests=30)
        self.cfg.image_routes = {"image": [a, b]}
        q = QuotaTracker(self.cfg, clock=lambda: self.now, persist_capacity=False)
        self.assertEqual(q.weights([a, b]), [2, 30])
        q.observed(a, 200, {"x-ratelimit-limit-requests": "4"})
        self.assertEqual(q.weights([a, b]), [4, 30])
        q.charge(a, 1)
        self.assertEqual(q.weights([a, b]), [3, 30])

    def test_report_exposes_capacity_foreign_and_available_tpm(self):
        a, b = route("a", tokens=600000), route("b")
        self.q.state(a).foreign = 0.2
        self.q.state(a).foreign_at = self.now
        self.q.state(b).foreign = 0.25
        self.q.state(b).foreign_at = self.now
        self.q.charge(b, 100000)
        report = self.q.report({"sol": [a, b]})
        unknown = report["routes"][str(b)]
        self.assertEqual(unknown["estimated_capacity_tpm"], 600000)
        self.assertEqual(unknown["other_tpm"], 150000)
        self.assertEqual(unknown["our_tpm"], 100000)
        self.assertEqual(unknown["available_tpm"], 350000)
        self.assertEqual(unknown["weight"], 350000)
        self.assertEqual(report["models"]["sol"][1]["share"], round(35 / 83, 4))

    def test_zero_others_exploration_is_uniform_despite_capacity_and_own_load(self):
        routes = [route("a", tokens=1e5), route("b", tokens=1e6), route("c", tokens=9e6)]
        self.q.charge(routes[0], 2e5)
        self.q.demote(routes[0], "503", "30")
        self.assertEqual(self.q.selection_parameters(routes), [(0, 1.0)] * 3)
        rng = random.Random(42)
        with patch("routing.quota.random.random", side_effect=rng.random):
            counts = collections.Counter(str(self.q.order(routes)[0]) for _ in range(1200))
        self.assertTrue(all(330 < n < 470 for n in counts.values()), counts)
        self.assertEqual(len(counts), 3)

    def test_exploration_precedes_contended_routes_and_keeps_failover(self):
        routes = [route("a", tokens=1e6), route("b", tokens=1e5), route("c", tokens=9e6)]
        self.q.state(routes[0]).foreign = 0.1
        self.q.state(routes[0]).foreign_at = self.now
        params = self.q.selection_parameters(routes)
        self.assertEqual(params, [(1, 900000), (0, 1), (0, 1)])
        with patch("routing.quota.random.random", return_value=0.0):
            self.assertEqual(self.q.order(routes), [routes[1], routes[2], routes[0]])
        shares = self.q.report({"sol": routes})["models"]["sol"]
        self.assertEqual([r["share"] for r in shares], [0.0, 0.5, 0.5])

    def test_all_contended_routes_use_available_tpm_weights(self):
        routes = [route("a", tokens=1e6), route("b", tokens=2e6)]
        for r in routes:
            self.q.state(r).foreign = 0.25
            self.q.state(r).foreign_at = self.now
        self.q.charge(routes[0], 250000)
        self.assertEqual(self.q.selection_parameters(routes), [(1, 500000), (1, 1500000)])
        shares = self.q.report({"sol": routes})["models"]["sol"]
        self.assertEqual([r["share"] for r in shares], [0.25, 0.75])
        self.now += 150
        self.assertEqual(self.q.selection_parameters(routes), [(0, 1), (0, 1)])

    def test_serving_samples_groups_after_endpoint_filtering(self):
        targets = [Target("other", "zero", None, {}, {}, 1, 0),
                   Target("bound", "small", None, {}, {}, 100, 1),
                   Target("bound", "large", None, {}, {}, 900, 1)]
        self.assertEqual(weighted_order(targets)[0], targets[0])
        rng = random.Random(42)
        with patch("proxy.affinity.random.choices", side_effect=rng.choices):
            counts = collections.Counter(weighted_order(targets[1:])[0].deployment for _ in range(1000))
        self.assertTrue(850 < counts["large"] < 950, counts)
        self.assertEqual({str(r) for r in weighted_order(targets)[1:]},
                         {str(r) for r in targets[1:]})

    def test_empty_route_set(self):
        self.assertEqual(self.q.weights([]), [])


if __name__ == "__main__":
    unittest.main()
