#!/usr/bin/env python3
"""Unit tests for paired_stats.py statistical analysis functions.

Tests bootstrap ratio CI, sign test, Wilcoxon signed-rank test,
Holm-Bonferroni correction, and imputation behavior.
"""

import math
import sys
from pathlib import Path
import unittest

# Ensure CraConTest repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.paired_stats import (
    DEFAULT_IMPUTATION_UNIVERSE,
    boot_ratio,
    holm,
    impute_rank,
    metrics,
    safe_mean,
    sign_p_less,
    wilcoxon_p_less,
)


class TestImputeRank(unittest.TestCase):
    def test_numeric_value_returned_as_float(self):
        self.assertEqual(impute_rank(5, 100), 5.0)
        self.assertEqual(impute_rank(3.14, 50), 3.14)
        self.assertEqual(impute_rank(1.0, None), 1.0)

    def test_none_imputed_with_positive_universe(self):
        self.assertEqual(impute_rank(None, 250), 250.0)
        self.assertEqual(impute_rank(None, 42.0), 42.0)

    def test_none_imputed_with_fallback_default(self):
        self.assertEqual(impute_rank(None, None), float(DEFAULT_IMPUTATION_UNIVERSE))
        self.assertEqual(impute_rank(None, 0), float(DEFAULT_IMPUTATION_UNIVERSE))
        self.assertEqual(impute_rank(None, -5), float(DEFAULT_IMPUTATION_UNIVERSE))
        self.assertEqual(impute_rank(None, None, default_impute=500), 500.0)


class TestBootRatio(unittest.TestCase):
    def test_empty_or_mismatched_inputs(self):
        lo, hi = boot_ratio([], [])
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        lo, hi = boot_ratio([1, 2], [1])
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        lo, hi = boot_ratio(None, [1, 2])
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

    def test_constant_ratio(self):
        a = [4.0] * 20
        b = [2.0] * 20
        lo, hi = boot_ratio(a, b, n=500, seed=42)
        self.assertAlmostEqual(lo, 2.0, places=5)
        self.assertAlmostEqual(hi, 2.0, places=5)

    def test_ratio_with_imputation(self):
        a = [1.0, 2.0, None, 4.0]
        b = [2.0, 4.0, 6.0, 8.0]
        univs = [100, 100, 50, 100]
        lo, hi = boot_ratio(a, b, univs=univs, n=500, seed=42)
        self.assertFalse(math.isnan(lo))
        self.assertFalse(math.isnan(hi))
        self.assertLess(lo, hi)

    def test_zero_denominator_handled_safely(self):
        a = [1.0, 2.0]
        b = [0.0, 0.0]
        lo, hi = boot_ratio(a, b, n=100, seed=42)
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

    def test_custom_default_impute(self):
        a = [None, None]
        b = [10.0, 10.0]
        lo, hi = boot_ratio(a, b, default_impute=50.0, n=50, seed=42)
        self.assertAlmostEqual(lo, 5.0, places=4)
        self.assertAlmostEqual(hi, 5.0, places=4)


class TestSignTest(unittest.TestCase):
    def test_empty_input(self):
        p, w, l, t = sign_p_less([], [])
        self.assertEqual(p, 1.0)
        self.assertEqual((w, l, t), (0, 0, 0))

    def test_mismatched_lengths(self):
        p, w, l, t = sign_p_less([1, 2], [1])
        self.assertEqual(p, 1.0)
        self.assertEqual((w, l, t), (0, 0, 0))

        p, w, l, t = sign_p_less(None, [1, 2])
        self.assertEqual(p, 1.0)
        self.assertEqual((w, l, t), (0, 0, 0))

    def test_all_wins(self):
        x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        y = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        p, w, l, t = sign_p_less(x, y)
        self.assertEqual((w, l, t), (10, 0, 0))
        self.assertLess(p, 0.001)

    def test_all_losses(self):
        x = [2, 3, 4, 5, 6]
        y = [1, 2, 3, 4, 5]
        p, w, l, t = sign_p_less(x, y)
        self.assertEqual((w, l, t), (0, 5, 0))
        self.assertEqual(p, 1.0)

    def test_ties(self):
        x = [1, 2, 3]
        y = [1, 2, 3]
        p, w, l, t = sign_p_less(x, y)
        self.assertEqual((w, l, t), (0, 0, 3))
        self.assertEqual(p, 1.0)

    def test_none_semantics(self):
        # x discovered bug (rank 5), y failed to discover (None) -> Win for x
        p, w, l, t = sign_p_less([5], [None])
        self.assertEqual((w, l, t), (1, 0, 0))

        # x failed to discover (None), y discovered (rank 5) -> Loss for x
        p, w, l, t = sign_p_less([None], [5])
        self.assertEqual((w, l, t), (0, 1, 0))

        # Both failed to discover -> Tie
        p, w, l, t = sign_p_less([None], [None])
        self.assertEqual((w, l, t), (0, 0, 1))


class TestWilcoxonTest(unittest.TestCase):
    def test_empty_or_mismatched(self):
        self.assertTrue(math.isnan(wilcoxon_p_less([], [])))
        self.assertTrue(math.isnan(wilcoxon_p_less([1, 2], [1])))

    def test_all_zero_differences(self):
        # All differences are 0 (all ties) -> len(d) == 0 < 5 -> returns NaN
        self.assertTrue(math.isnan(wilcoxon_p_less([1.0] * 10, [1.0] * 10)))

    def test_insufficient_differences(self):
        # Fewer than 5 non-zero differences must return NaN
        self.assertTrue(math.isnan(wilcoxon_p_less([1, 2, 3], [2, 3, 4])))

    def test_significant_difference(self):
        x = [1.0, 2.0, 1.0, 3.0, 2.0, 1.0, 2.0, 1.0]
        y = [10.0, 12.0, 15.0, 11.0, 14.0, 13.0, 12.0, 10.0]
        p = wilcoxon_p_less(x, y)
        self.assertFalse(math.isnan(p))
        self.assertLess(p, 0.05)

    def test_imputation_with_universe(self):
        x = [1.0, 2.0, 1.0, 2.0, 1.0, None]
        y = [10.0, 12.0, 15.0, 11.0, 14.0, 100.0]
        univs = [100] * 6
        p = wilcoxon_p_less(x, y, univs=univs)
        self.assertFalse(math.isnan(p))

    def test_custom_default_impute(self):
        # Missing values with no universe fallback to default_impute
        x = [1.0, 2.0, 1.0, 2.0, 1.0, None]
        y = [10.0, 12.0, 15.0, 11.0, 14.0, 10.0]
        p = wilcoxon_p_less(x, y, default_impute=500)
        self.assertFalse(math.isnan(p))


class TestHolmCorrection(unittest.TestCase):
    def test_empty_input(self):
        self.assertEqual(holm([]), [])

    def test_single_value(self):
        self.assertEqual(holm([0.04]), [0.04])
        self.assertEqual(holm([0.8]), [0.8])

    def test_duplicate_pvalues(self):
        # Tied p-values: [0.02, 0.02] -> step-down multipliers: 2, 1
        # rank 0: 2 * 0.02 = 0.04 -> max(0, 0.04) = 0.04
        # rank 1: 1 * 0.02 = 0.02 -> max(0.04, 0.02) = 0.04
        self.assertEqual(holm([0.02, 0.02]), [0.04, 0.04])

    def test_known_calculation(self):
        # p-values: [0.01, 0.04, 0.03]
        # Sorted: 0.01 (idx 0), 0.03 (idx 2), 0.04 (idx 1)
        # Multipliers: 3, 2, 1
        # Adjusted:
        # rank 0: 3 * 0.01 = 0.03 -> max(0, 0.03) = 0.03
        # rank 1: 2 * 0.03 = 0.06 -> max(0.03, 0.06) = 0.06
        # rank 2: 1 * 0.04 = 0.04 -> max(0.06, 0.04) = 0.06
        # Original order: [0.03, 0.06, 0.06]
        res = holm([0.01, 0.04, 0.03])
        self.assertAlmostEqual(res[0], 0.03)
        self.assertAlmostEqual(res[1], 0.06)
        self.assertAlmostEqual(res[2], 0.06)

    def test_cap_at_one(self):
        res = holm([0.6, 0.7, 0.8])
        for p in res:
            self.assertLessEqual(p, 1.0)

    def test_monotonicity(self):
        pvals = [0.001, 0.01, 0.02, 0.05, 0.1]
        adj = holm(pvals)
        for i in range(len(pvals) - 1):
            self.assertLessEqual(adj[i], adj[i + 1])
        for raw, adjusted in zip(pvals, adj):
            self.assertGreaterEqual(adjusted, raw)


class TestMetricsAndSafeMean(unittest.TestCase):
    def test_safe_mean(self):
        self.assertTrue(math.isnan(safe_mean([])))
        self.assertTrue(math.isnan(safe_mean([None, None])))
        self.assertEqual(safe_mean([1.0, 2.0, 3.0]), 2.0)
        self.assertEqual(safe_mean([1.0, None, 3.0]), 2.0)

    def test_metrics(self):
        order = [("evt1", 100), ("evt2", 0), ("evt3", 50)]
        fail = {("evt1", 100): False, ("evt2", 0): True, ("evt3", 50): True}
        anchor = {"evt1": 1, "evt2": 2, "evt3": 3}
        k1, s25 = metrics(order, fail, anchor)
        self.assertEqual(k1, 2)
        self.assertEqual(s25, 2)


if __name__ == "__main__":
    unittest.main()
