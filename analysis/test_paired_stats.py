#!/usr/bin/env python3
"""Unit and integration tests for paired_stats.py statistical analysis functions.

Tests bootstrap ratio CI, sign test, Wilcoxon signed-rank test,
Holm-Bonferroni correction, imputation behavior, CSV/directory loading,
StateKey parity, and CLI invocation.
"""

import contextlib
import io
import json
import math
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

# Ensure CraConTest repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.paired_stats import (
    DEFAULT_IMPUTATION_UNIVERSE,
    _compute_state_keys,
    analyse,
    boot_ratio,
    holm,
    impute_rank,
    imputed_mean,
    is_failure_class,
    is_mutation,
    load_run,
    metrics,
    rank,
    report,
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

    def test_rejects_bool_nan_and_non_positive(self):
        for bad in (True, False, float("nan"), 0, -1, -0.5):
            with self.assertRaises(ValueError):
                impute_rank(bad, 100)


class TestBootRatio(unittest.TestCase):
    def test_empty_or_mismatched_inputs(self):
        lo, hi = boot_ratio([], [])
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        lo, hi = boot_ratio([1, 2], [1])
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        lo, hi = boot_ratio(None, [1, 2])
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        lo, hi = boot_ratio([2.0], [1.0])
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
        # Imputed a = [1, 2, 50, 4], b = [2, 4, 6, 8]; point ratio = 57/20 = 2.85.
        self.assertGreaterEqual(lo, 0.5)
        self.assertLessEqual(lo, 2.85)
        self.assertGreaterEqual(hi, 2.85)
        self.assertLessEqual(hi, 25.0)
        self.assertLess(lo, hi)

    def test_zero_and_partial_zero_denominators_return_nan(self):
        lo, hi = boot_ratio([1.0, 2.0], [0.0, 0.0], n=100, seed=42)
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        lo, hi = boot_ratio([1.0, 2.0], [0.0, 10.0], n=100, seed=42)
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

    def test_rejects_invalid_n_and_bool_and_handles_neg_inf(self):
        with self.assertRaises(ValueError):
            boot_ratio([2.0, 4.0], [1.0, 2.0], n=0)
        with self.assertRaises(ValueError):
            boot_ratio([2.0, 4.0], [1.0, 2.0], n=True)
        with self.assertRaises(ValueError):
            boot_ratio([True, 4.0], [1.0, 2.0], n=50)

        lo, hi = boot_ratio([-1.0, 2.0], [1.0, 2.0], n=50)
        self.assertTrue(math.isnan(lo) and math.isnan(hi))
        lo, hi = boot_ratio([float("inf"), 2.0], [1.0, 2.0], n=50)
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

    def test_custom_default_impute(self):
        a = [None, None]
        b = [10.0, 10.0]
        lo, hi = boot_ratio(a, b, default_impute=50.0, n=50, seed=42)
        self.assertAlmostEqual(lo, 5.0, places=4)
        self.assertAlmostEqual(hi, 5.0, places=4)


class TestSignTest(unittest.TestCase):
    def test_empty_and_mismatched_inputs_return_nan(self):
        p, w, l, t = sign_p_less([], [])
        self.assertTrue(math.isnan(p))
        self.assertEqual((w, l, t), (0, 0, 0))

        p, w, l, t = sign_p_less([1, 2], [1])
        self.assertTrue(math.isnan(p))
        self.assertEqual((w, l, t), (0, 0, 0))

        p, w, l, t = sign_p_less(None, [1, 2])
        self.assertTrue(math.isnan(p))
        self.assertEqual((w, l, t), (0, 0, 0))

    def test_all_wins(self):
        x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        y = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        p, w, l, t = sign_p_less(x, y)
        self.assertEqual((w, l, t), (10, 0, 0))
        self.assertAlmostEqual(p, 2.0 ** -10, places=9)

    def test_all_losses(self):
        x = [2, 3, 4, 5, 6]
        y = [1, 2, 3, 4, 5]
        p, w, l, t = sign_p_less(x, y)
        self.assertEqual((w, l, t), (0, 5, 0))
        self.assertEqual(p, 1.0)

    def test_ties_and_mixed_nan(self):
        x = [1, 2, 3]
        y = [1, 2, 3]
        p, w, l, t = sign_p_less(x, y)
        self.assertEqual((w, l, t), (0, 0, 3))
        self.assertEqual(p, 1.0)

        p2, w2, l2, t2 = sign_p_less([1, float("nan"), 3, 4], [2, 5, 3, 1])
        self.assertEqual((w2, l2, t2), (1, 1, 1))
        self.assertAlmostEqual(p2, 0.75, places=6)

    def test_rejects_bool_and_non_positive(self):
        with self.assertRaises(ValueError):
            sign_p_less([True], [2])
        with self.assertRaises(ValueError):
            sign_p_less([0], [2])
        with self.assertRaises(ValueError):
            sign_p_less([1], [-1])

    def test_none_semantics(self):
        p, w, l, t = sign_p_less([5], [None])
        self.assertEqual((w, l, t), (1, 0, 0))

        p, w, l, t = sign_p_less([None], [5])
        self.assertEqual((w, l, t), (0, 1, 0))

        p, w, l, t = sign_p_less([None], [None])
        self.assertEqual((w, l, t), (0, 0, 1))


class TestWilcoxonTest(unittest.TestCase):
    def test_empty_or_mismatched(self):
        self.assertTrue(math.isnan(wilcoxon_p_less([], [])))
        self.assertTrue(math.isnan(wilcoxon_p_less([1, 2], [1])))

    def test_all_zero_differences(self):
        self.assertTrue(math.isnan(wilcoxon_p_less([1.0] * 10, [1.0] * 10)))

    def test_insufficient_differences(self):
        self.assertTrue(math.isnan(wilcoxon_p_less([1, 2, 3], [2, 3, 4])))

    def test_significant_difference_and_ties(self):
        x = [1.0, 2.0, 1.0, 3.0, 2.0, 1.0, 2.0, 1.0]
        y = [10.0, 12.0, 15.0, 11.0, 14.0, 13.0, 12.0, 10.0]
        p = wilcoxon_p_less(x, y)
        self.assertFalse(math.isnan(p))
        self.assertLess(p, 0.01)

        # Equal-magnitude ties in |d| should produce exact tie-aware permutation p-value
        p_tied = wilcoxon_p_less([1, 1, 1, 1, 1, 1], [2, 2, 2, 3, 4, 5])
        self.assertAlmostEqual(p_tied, 2.0 ** -6, places=6)

        # Opposite direction must have p > 0.5
        p_rev = wilcoxon_p_less(y, x)
        self.assertGreater(p_rev, 0.99)

    def test_imputation_with_universe(self):
        # 5 strong wins (-9..-14) and 1 missing (100 - 100 = 0, dropped) -> 5 negative diffs -> p = 1/32
        x = [1.0, 2.0, 1.0, 2.0, 1.0, None]
        y = [10.0, 12.0, 15.0, 11.0, 14.0, 100.0]
        univs = [100] * 6
        p = wilcoxon_p_less(x, y, univs=univs)
        self.assertAlmostEqual(p, 1.0 / 32.0, places=6)

    def test_custom_default_impute_direction(self):
        # None -> 500 vs 10 creates +490 (largest |d|, rank 6), shifting exact p to 13/64 = 0.203125
        x = [1.0, 2.0, 1.0, 2.0, 1.0, None]
        y = [10.0, 12.0, 15.0, 11.0, 14.0, 10.0]
        p = wilcoxon_p_less(x, y, default_impute=500)
        self.assertAlmostEqual(p, 13.0 / 64.0, places=6)

    def test_one_sided_inf_returns_nan_and_bool_raises(self):
        self.assertTrue(math.isnan(wilcoxon_p_less([float("inf"), 1, 2, 3, 4, 5], [10] * 6)))
        with self.assertRaises(ValueError):
            wilcoxon_p_less([True, 2, 3, 4, 5], [10] * 5)


class TestHolmCorrection(unittest.TestCase):
    def test_empty_input(self):
        self.assertEqual(holm([]), [])

    def test_single_value(self):
        self.assertEqual(holm([0.04]), [0.04])
        self.assertEqual(holm([0.8]), [0.8])

    def test_duplicate_pvalues(self):
        self.assertEqual(holm([0.02, 0.02]), [0.04, 0.04])

    def test_known_calculation(self):
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

    def test_nan_none_inf_and_bounds_handling(self):
        res = holm([float('nan'), None, 0.01, 0.05])
        self.assertTrue(math.isnan(res[0]))
        self.assertTrue(math.isnan(res[1]))
        self.assertAlmostEqual(res[2], 0.02)
        self.assertAlmostEqual(res[3], 0.05)

        all_nan = holm([float('nan'), None])
        self.assertTrue(all(math.isnan(v) for v in all_nan))

        with self.assertRaises(ValueError):
            holm([-0.1, 0.05])
        with self.assertRaises(ValueError):
            holm([0.05, 1.5])
        with self.assertRaises(ValueError):
            holm([float('inf'), 0.05])


class TestMetricsAndHelpers(unittest.TestCase):
    def test_is_failure_class(self):
        for cls in ('violation', 'open_error', 'panic', 'hang'):
            self.assertTrue(is_failure_class(cls))
        for cls in ('valid', 'oracle_error', '', None):
            self.assertFalse(is_failure_class(cls))

    def test_safe_mean(self):
        self.assertTrue(math.isnan(safe_mean([])))
        self.assertTrue(math.isnan(safe_mean([None, None])))
        self.assertEqual(safe_mean([1.0, 2.0, 3.0]), 2.0)
        self.assertEqual(safe_mean([1.0, None, 3.0]), 2.0)

    def test_metrics_truncates_at_25_and_dedups_by_state_key(self):
        order = [(i, 0, 1) for i in range(1, 31)]
        fail = {(i, 0, 1): (i in (2, 3, 26)) for i in range(1, 31)}
        anchor = {i: i for i in range(1, 31)}
        k1, s25 = metrics(order, fail, anchor, state_keys={2: "skSame", 3: "skSame", 26: "skAfter25"})
        self.assertEqual(k1, 2)
        self.assertEqual(s25, 1)  # Event 26 is beyond [:25], and 2 & 3 share "skSame"

    def test_imputed_mean_matches_boot_estimand(self):
        seq = [2.0, None]
        self.assertAlmostEqual(imputed_mean(seq, univs=[10, 100]), 51.0)

    def test_is_mutation_all_kinds(self):
        for kind in ('Create', 'Link', 'Lock', 'MkdirAll', 'OpenReadWrite', 'Remove', 'RemoveAll', 'Rename', 'ReuseForWrite'):
            self.assertTrue(is_mutation({'kind': kind}))
        self.assertTrue(is_mutation({'kind': 'Sync'}))
        self.assertTrue(is_mutation({'kind': 'SyncData'}))
        self.assertFalse(is_mutation({'kind': 'Sync', 'err': 'io'}))
        self.assertFalse(is_mutation({'kind': 'SyncData', 'err': 'io'}))
        self.assertFalse(is_mutation({'kind': 'SyncTo', 'full_sync': False}))
        self.assertTrue(is_mutation({'kind': 'SyncTo', 'full_sync': True}))
        self.assertFalse(is_mutation({'kind': 'SyncTo', 'full_sync': True, 'err': 'io'}))
        self.assertTrue(is_mutation({'kind': 'Write', 'len': 4}))
        self.assertTrue(is_mutation({'kind': 'WriteAt', 'len': 1}))
        self.assertFalse(is_mutation({'kind': 'Write', 'len': 0}))
        self.assertFalse(is_mutation({'kind': 'Preallocate', 'size': 0}))
        self.assertTrue(is_mutation({'kind': 'Preallocate', 'size': 16}))
        self.assertFalse(is_mutation({'kind': 'Read'}))

    def test_compute_state_keys_dedups_barrier_return_to_clean_state(self):
        events = [
            {'id': 1, 'kind': 'Create', 'file': '000001.log', 'handle': 1},
            {'id': 2, 'kind': 'OpenDir', 'file': '', 'handle': 2, 'dir': True},
            {'id': 3, 'kind': 'Sync', 'file': '', 'handle': 2},
            {'id': 4, 'kind': 'Write', 'file': '000001.log', 'handle': 1, 'off': 0, 'len': 8},
            {'id': 5, 'kind': 'SyncData', 'file': '000001.log', 'handle': 1},
        ]
        sk = _compute_state_keys(events)
        self.assertIsNotNone(sk)
        assert sk is not None
        # Both cutoff 3 (after dir sync) and cutoff 5 (after data sync) have empty dirty/pending -> identical StateKey
        self.assertEqual(sk[3], sk[5])
        self.assertNotEqual(sk[3], sk[4])


class TestAnalyseAndReportIntegration(unittest.TestCase):
    def test_load_run_always_returns_3_tuple_and_handles_null_arrays(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, 'outcomes.json'), 'w', encoding='utf-8') as f:
                json.dump({'outcomes': None}, f)
            with open(os.path.join(tmp, 'trace.json'), 'w', encoding='utf-8') as f:
                json.dump({'events': None}, f)
            with open(os.path.join(tmp, 'graph.json'), 'w', encoding='utf-8') as f:
                json.dump({'cuts': {'1': {'state_key': 'sk1'}}}, f)
            with open(os.path.join(tmp, 'ranking_dcg.json'), 'w', encoding='utf-8') as f:
                json.dump({'candidates': None}, f)

            fail, anchor, state_keys = load_run(tmp)
            self.assertEqual(fail, {})
            self.assertEqual(anchor, {0: 0})
            self.assertEqual(state_keys, {1: 'sk1'})
            self.assertEqual(rank(tmp, 'dcg'), [])
            self.assertEqual(rank(tmp, 'missing'), [])

    def test_analyse_wide_csv_and_ingest_not_misdetected_as_pr(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "casestudy_ingest_wide.CSV")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write("ops,seed,universe_size,total_failures,k1_dcg,k1_dcg-pr,k1_r2\n")
                f.write("120,1,100,0,0,0,0\n")
                for s in range(2, 10):
                    f.write(f"120,{s},100,2,1,2,{s + 5}\n")
            res = analyse([], p)
            self.assertEqual(len(res), 1)
            setting, used, nd, dens, K_single, _, S_mean, _ = res[0]
            self.assertEqual(setting, "120")
            self.assertEqual(used, 8)
            self.assertEqual(nd, 9)
            self.assertEqual(K_single["dcg"], [1.0] * 8)
            self.assertEqual(K_single["dcg-pr"], [2.0] * 8)
            self.assertTrue(all(s is None for s in S_mean["dcg"]))
            self.assertAlmostEqual(dens[0], 0.02)

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                report([], p, "Ingest Wide Test")
            out = buf.getvalue()
            self.assertIn("DCG<R2", out)
            self.assertNotIn("DCG-PR<R2", out)

    def test_analyse_long_csv_and_parent_path_pr_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            nested = os.path.join(tmp, "checkpoint_parent")
            os.makedirs(nested)
            p = os.path.join(nested, "benchmark_per_seed.csv")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write("fault_id,seed,universe_size,total_failures,strategy_code,k1_single,s25_single,k1_failure_mean,s25_mean\n")
                for seed in range(1, 10):
                    for sc, k1 in (("dcg", 1), ("dcg-pr", 2), ("r2", 10), ("r1", 12), ("b0", 20), ("dcg-flag-only", 3), ("b2", 30)):
                        f.write(f"MUT-1,{seed},100,5,{sc},{k1},2,{k1}.0,2.0\n")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                report([], p, "Parent Folder Test")
            out = buf.getvalue()
            self.assertNotIn("DCG-PR<R2", out)
            self.assertIn("DCG<R2", out)

            buf_pr = io.StringIO()
            with contextlib.redirect_stdout(buf_pr):
                report([], p, "Forced PR Test", prefer_pr=True)
            self.assertIn("DCG-PR<R2", buf_pr.getvalue())

            buf_no_pr = io.StringIO()
            with contextlib.redirect_stdout(buf_no_pr):
                report([], p, "Disabled PR Test", prefer_pr=False)
            self.assertIn("DCG<R2", buf_no_pr.getvalue())

    def test_multi_setting_report_holm_ties_and_estimand_alignment(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "multi.csv")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write("fault_id,seed,universe_size,total_failures,strategy_code,k1_single,s25_single,k1_failure_mean,s25_mean\n")
                # Setting A: DCG beats R2 strongly on K_mean, ties on K_single
                for s in range(1, 10):
                    f.write(f"SET-A,{s},100,2,dcg,1,2,1.0,2.0\n")
                    f.write(f"SET-A,{s},100,2,r2,1,2,10.0,1.0\n")
                    f.write(f"SET-A,{s},100,2,b0,5,1,5.0,1.0\n")
                # Setting B: all ties between DCG and R2 (Wilcoxon returns NaN, must still count in Holm m=2!)
                for s in range(1, 10):
                    f.write(f"SET-B,{s},100,2,dcg,3,1,3.0,1.0\n")
                    f.write(f"SET-B,{s},100,2,r2,3,1,3.0,1.0\n")
                    f.write(f"SET-B,{s},100,2,b0,2,1,2.0,1.0\n")
                # Setting C: DCG misses failure on seed 1..5 -> imputed mean is much worse than R2!
                for s in range(1, 10):
                    k1_dcg = "" if s <= 5 else "1"
                    f.write(f"SET-C,{s},100,2,dcg,{k1_dcg},1,{k1_dcg},1.0\n")
                    f.write(f"SET-C,{s},100,2,r2,5,1,5.0,1.0\n")
                    f.write(f"SET-C,{s},100,2,b0,5,1,5.0,1.0\n")
                # Setting D: fewer than 8 failing seeds -> not testable
                for s in range(1, 4):
                    f.write(f"SET-D,{s},100,1,dcg,1,1,1.0,1.0\n")
                    f.write(f"SET-D,{s},100,1,r2,2,1,2.0,1.0\n")

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rows = report([], p, "Multi Setting Test")
            out = buf.getvalue()
            self.assertEqual(len(rows), 3)
            # SET-D reported as null
            self.assertIn("fewer than 8 seeds with any failure", out)
            # SET-C imputed DCG mean = (5*100 + 4*1)/9 = 56.00 > R2 5.00, so NOT survival-biased < 5.00
            self.assertIn("56.00", out)
            self.assertIn("WORSE than R2 on average k1: ['SET-C']", out)
            self.assertIn("WORSE than uniform random (B0) on average k1: ['SET-B', 'SET-C']", out)
            # Secondary table printed because K_single != K_mean on SET-A
            self.assertIn("Single-draw (k1_single) paired tests", out)
            # SET-A Wilcoxon Holm factor must be >= 2 (not 1!) because SET-B/SET-C remain in family m=3
            row_a = rows[0]
            adj_w = holm([1.0 if math.isnan(r.p_wilcoxon) else r.p_wilcoxon for r in rows])
            self.assertGreater(adj_w[0], row_a.p_wilcoxon * 1.5)

    def test_missing_strategy_rows_not_penalized_as_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "partial_rows.csv")
            with open(p, "w", encoding="utf-8", newline="") as f:
                f.write("fault_id,seed,universe_size,total_failures,strategy_code,k1_single,k1_failure_mean\n")
                for s in range(1, 10):
                    f.write(f"MUT-1,{s},100,2,dcg,1,1.0\n")
                    # r2 only recorded on first 8 seeds, missing on seed 9
                    if s <= 8:
                        f.write(f"MUT-1,{s},100,2,r2,5,5.0\n")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rows = report([], p, "Partial Missing Rows")
            out = buf.getvalue()
            self.assertIn("5.00", out)  # Not inflated by missing seed 9 imputed to 100
            self.assertEqual(rows[0].w, 8)  # 8 common paired seeds, not 9

    def test_csv_validation_rejects_bad_headers_duplicates_ragged_and_negatives(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Missing seed column
            p1 = os.path.join(tmp, "no_seed.csv")
            with open(p1, "w", encoding="utf-8") as f:
                f.write("fault_id,strategy_code,k1_single\nMUT-1,dcg,1\n")
            with self.assertRaises(ValueError):
                analyse([], p1)

            # Duplicate (fault_id, seed, strategy_code)
            p2 = os.path.join(tmp, "dup.csv")
            with open(p2, "w", encoding="utf-8") as f:
                f.write("fault_id,seed,total_failures,strategy_code,k1_single\nMUT-1,1,1,dcg,1\nMUT-1,1,1,dcg,2\n")
            with self.assertRaises(ValueError):
                analyse([], p2)

            # Ragged row
            p3 = os.path.join(tmp, "ragged.csv")
            with open(p3, "w", encoding="utf-8") as f:
                f.write("fault_id,seed,total_failures,strategy_code,k1_single\nMUT-1,1,1\n")
            with self.assertRaises(ValueError):
                analyse([], p3)

            # Negative universe_size
            p4 = os.path.join(tmp, "neg.csv")
            with open(p4, "w", encoding="utf-8") as f:
                f.write("fault_id,seed,universe_size,total_failures,strategy_code,k1_single\nMUT-1,1,-5,1,dcg,1\n")
            with self.assertRaises(ValueError):
                analyse([], p4)

            # Nonexistent requested setting
            p5 = os.path.join(tmp, "ok.csv")
            with open(p5, "w", encoding="utf-8") as f:
                f.write("fault_id,seed,universe_size,total_failures,strategy_code,k1_single\nMUT-1,1,10,1,dcg,1\n")
            with self.assertRaises(ValueError):
                analyse(["MUT-99"], p5)

    def test_directory_mode_flat_missing_trace_and_corrupt_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            for seed in range(1, 9):
                sd = os.path.join(tmp, "MUT-X", f"seed_{seed}")
                os.makedirs(sd)
                with open(os.path.join(sd, "trace.json"), "w", encoding="utf-8") as f:
                    json.dump({"events": [
                        {"id": 1, "kind": "OpenReadWrite"},
                        {"id": 2, "kind": "Lock"},
                        {"id": 3, "kind": "Write", "len": 0},
                    ]}, f)
                with open(os.path.join(sd, "outcomes.json"), "w", encoding="utf-8") as f:
                    json.dump({"outcomes": [
                        {"candidate": {"event": 1, "percent": 0, "seed": 1}, "class": "violation"},
                        {"candidate": {"event": 2, "percent": 0, "seed": 1}, "class": "valid"},
                    ]}, f)
                with open(os.path.join(sd, "ranking_dcg.json"), "w", encoding="utf-8") as f:
                    json.dump({"candidates": [{"event": 1, "percent": 0, "seed": 1}, {"event": 2, "percent": 0, "seed": 1}]}, f)
                with open(os.path.join(sd, "ranking_r2.json"), "w", encoding="utf-8") as f:
                    json.dump({"candidates": [{"event": 2, "percent": 0, "seed": 1}, {"event": 1, "percent": 0, "seed": 1}]}, f)

            # Add an incomplete seed dir missing trace.json and an m=0 seed dir
            incomplete = os.path.join(tmp, "MUT-X", "seed_incomplete")
            os.makedirs(incomplete)
            with open(os.path.join(incomplete, "outcomes.json"), "w", encoding="utf-8") as f:
                json.dump({"outcomes": []}, f)
            zero_m = os.path.join(tmp, "MUT-X", "seed_zero")
            os.makedirs(zero_m)
            with open(os.path.join(zero_m, "trace.json"), "w", encoding="utf-8") as f:
                json.dump({"events": []}, f)
            with open(os.path.join(zero_m, "outcomes.json"), "w", encoding="utf-8") as f:
                json.dump({"outcomes": [{"candidate": {"event": 0, "percent": 0, "seed": 1}, "class": "valid"}]}, f)

            res = analyse([], tmp, strategies=("dcg", "r2"), baselines_for_tests=("r2",))
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0][1], 8)
            self.assertEqual(res[0][2], 9)  # Excludes seed_incomplete missing trace.json!
            self.assertEqual(res[0][4]["dcg"], [1] * 8)
            self.assertEqual(res[0][4]["r2"], [2] * 8)
            self.assertEqual(res[0][6]["dcg"], [1] * 8)

            # Flat root directory mode
            flat_res = analyse([], os.path.join(tmp, "MUT-X", "seed_1"), strategies=("dcg", "r2"))
            self.assertEqual(flat_res[0][0], "seed_1")
            self.assertEqual(flat_res[0][1], 1)

            bad_rank = os.path.join(tmp, "MUT-X", "seed_1", "ranking_dcg.json")
            with open(bad_rank, "w", encoding="utf-8") as f:
                f.write("{corrupt")
            with self.assertRaises(json.JSONDecodeError):
                rank(os.path.join(tmp, "MUT-X", "seed_1"), "dcg")

            with open(os.path.join(tmp, "MUT-X", "seed_1", "trace.json"), "w", encoding="utf-8") as tf:
                tf.write("{bad")
            with self.assertRaises(json.JSONDecodeError):
                load_run(os.path.join(tmp, "MUT-X", "seed_1"))

    def test_edge_cases_and_cli_execution(self):
        lo, hi = boot_ratio([float("nan"), 2.0, 4.0], [1.0, 2.0, 2.0], n=200, seed=1)
        self.assertTrue(math.isnan(lo) and math.isnan(hi))

        p, w, l, t = sign_p_less([float("nan")] * 8, [1.0] * 8)
        self.assertTrue(math.isnan(p))
        self.assertEqual((w, l, t), (0, 0, 0))

        p_inf = wilcoxon_p_less([float("inf"), 1, 2, 3, 4, 5], [float("inf"), 10, 10, 10, 10, 10])
        self.assertFalse(math.isnan(p_inf))
        self.assertLess(p_inf, 0.05)
        self.assertTrue(math.isnan(wilcoxon_p_less([1, 2, 3, 4, 5, float("nan")], [10] * 6)))

        with self.assertRaises(ValueError):
            holm([True, 0.05])
        with self.assertRaises(ValueError):
            impute_rank(None, None, default_impute=0)
        with self.assertRaises(ValueError):
            report([], ".", "Bad Impute", default_impute=-1)

        with tempfile.TemporaryDirectory() as tmp:
            missing_dir = os.path.join(tmp, "does_not_exist")
            with self.assertRaises(FileNotFoundError):
                analyse(["MUT-1"], missing_dir)

            p_csv = os.path.join(tmp, "long_strategy.csv")
            with open(p_csv, "w", encoding="utf-8", newline="") as f:
                f.write("fault_id,seed,universe_size,total_failures,strategy,k1_single,k1_failure_mean\n")
                for s in range(1, 10):
                    f.write(f"MUT-1,{s},100,2,dcg,1,1.0\n")
                    f.write(f"MUT-1,{s},100,2,r2,10,10.0\n")
            res = analyse([], p_csv)
            self.assertEqual(res[0][4]["dcg"], [1.0] * 9)

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                report([], p_csv, "Absent PR Test", prefer_pr=True)
            out = buf.getvalue()
            self.assertIn("N/Ax", out)
            self.assertIn("WORSE than R2 on average k1: none", out)

            # CLI subprocess execution (success and clean invalid --default-impute error)
            script = str(Path(__file__).resolve().parent / "paired_stats.py")
            ok_proc = subprocess.run(
                [sys.executable, script, p_csv, "CLI Test", "--no-dcg-pr"],
                text=True,
                capture_output=True,
            )
            self.assertEqual(ok_proc.returncode, 0, msg=ok_proc.stderr)
            self.assertIn("CLI Test", ok_proc.stdout)

            bad_proc = subprocess.run(
                [sys.executable, script, p_csv, "Bad CLI", "--default-impute", "0"],
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(bad_proc.returncode, 0)
            self.assertNotIn("Traceback", bad_proc.stderr)


if __name__ == "__main__":
    unittest.main()
