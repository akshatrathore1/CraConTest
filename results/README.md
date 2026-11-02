# CraConTest Experimental Results Artifacts

This directory stores the reference experimental data, empirical evaluations, and statistical discovery tables produced by the CraConTest framework on CockroachDB Pebble (`v2.1.7`).

The artifacts in `results/` are organized into two distinct stages:

---

## Stage 1: Single-Run Reference Directories & Discovery Curves

Stage 1 contains full trace, execution history, candidate universe, crash exploration journal, outcome table, ranking permutations, and discovery curves for single-seed (`Seed=42`) workloads. These committed artifacts serve as the fixed regression baseline and are guarded by continuous integration tests (`TestGateG4` and `TestMainRunArtifactsMatchStoredTables` in `internal/exp`):

1. **Smoke Run Directories (`results/smoke_mut0`, `results/smoke_mut1`):**
   - Workload: 100 operations, Seed 42, 16 keys, 64-byte values (`experiments/configs/smoke.json`).
   - Faults: `MUT-0` (unfaulted null control) and `MUT-1` (dense WAL sync drop).
   - Artifacts: `run.json`, `trace.json`, `history.json`, `universe.json`, `journal.ndjson`, `outcomes.json`, `ranking_*.json`, `curves.csv`, `positions.csv`.
   - Discovery curve tables and plots: `discovery_mut0.csv`, `discovery_mut1.csv`, `plot_mut0.svg`, `plot_mut1.svg`.

2. **Main Dense-Fault Directories (`results/main_mut0` through `results/main_mut4`):**
   - Workload: 1,000 operations, Seed 42, 64 keys, 128-byte values (`experiments/configs/main.json`).
   - Faults: `MUT-0` (null control), `MUT-1` (dense WAL sync drop), `MUT-2` (dense manifest sync drop), `MUT-3` (dense directory sync drop), `MUT-4` (dense SST flush sync drop).
   - Failure densities: 28.0% to 49.6% across `MUT-1..4` (0 violations on `MUT-0`).
   - Artifacts: Full trace, candidate universe (~2,000 candidates), complete explore journal, outcomes, ranking permutations (`ranking_dcg.json`, `ranking_r2.json`, `ranking_b0.json`, etc.), `positions.csv`.
   - Discovery curve tables and plots: `discovery_main_mut0..4.csv`, `plot_main_mut0..4.svg`.

> **Note on Naming & Invariants:** Directory names `results/main_mut0..4` and `results/smoke_mut0..1` are canonical and must not be altered, ensuring `TestGateG4` and `TestMainRunArtifactsMatchStoredTables` continuously pass.

---

## Stage 2: Multi-Seed Benchmark & Holdout CSV Tables

Stage 2 stores aggregated 95%-confidence-interval summaries and unaggregated per-seed discovery tables across multi-seed benchmark evaluations (120 operations/seed, 32 deterministic seeds: `42, 100..900, 1001..1022`). These tables enable full statistical reproduction and hypothesis testing via `analysis/paired_stats.py` without requiring the repository to commit hundreds of megabytes of raw per-run trace and universe JSONs:

1. **In-Sample Multi-Seed Benchmark (`results/benchmark_summary.csv`, `results/benchmark_per_seed.csv`):**
   - Scope: 10 in-sample fault models (`MUT-0` through `MUT-9`, 320 runs total).
   - `benchmark_summary.csv`: Aggregated summary statistics across 32 seeds for each strategy (`dcg`, `dcg-flag-only`, `r2`, `r1`, `b6`, `b0`, `b7`, `b5`, `b4`, `b1`, `b2`, `b3`), including median and mean $k_1$ with 95% bootstrap confidence intervals, $S_{25}$, $S_{100}$, normalized AUC, and theoretical random expectation.
   - `benchmark_per_seed.csv`: Unaggregated per-seed, per-strategy discovery metrics across all 320 individual runs (`fault_id`, `seed`, `universe_size`, `total_failures`, `total_violations`, `strategy_code`, `k1_single`, `s25_single`, `s100_single`, etc.).

2. **Holdout & Out-of-Contract Benchmark (`results/holdout_summary.csv`, `results/holdout_per_seed.csv`):**
   - Scope: 17 holdout and out-of-contract fault settings across 32 seeds (544 runs total).
     - Parameterized WAL sync drops: `MUT-X1_k2`, `MUT-X1_k3`, `MUT-X1_k4`, `MUT-X1_k6`, `MUT-X1_k9`.
     - Parameterized MANIFEST sync drops: `MUT-X2_k1`, `MUT-X2_k3`, `MUT-X2_k4`, `MUT-X2_k5`.
     - Parameterized SST sync drops: `MUT-X3_k1`, `MUT-X3_k2`, `MUT-X3_k3`.
     - Stochastic sync drops: `MUT-X4_p04` (flaky 4% non-dir sync drop), `MUT-X5_p12` (flaky 12% dir sync drop).
     - Out-of-contract controls: `MUT-X6_k2`, `MUT-X6_k5`, `MUT-X6_k9` (silent WAL write drops; all `Sync()` calls succeed).
   - `holdout_summary.csv`: Aggregated summary statistics across 32 seeds for each strategy and fault model.
   - `holdout_per_seed.csv`: Unaggregated 32-seed per-seed discovery metrics enabling paired non-parametric statistical hypothesis testing (one-sided exact sign test and Wilcoxon signed-rank test with Holm-Bonferroni FWER adjustment) via `analysis/paired_stats.py`.

3. **Pebble #5825 Real-Bug A/B Benchmark (`results/realbug_pebble5825_summary.csv`, `results/realbug_pebble5825_per_seed.csv`):**
   - Scope: Real-world bug from CockroachDB Pebble issue #5825 evaluated across 32 deterministic seeds under partial retention policies (0%, 50%, 100%) with manifest rotation (`-max-manifest-size 1`).
     - `MUT-0` (unfaulted baseline): Parent directory sync is not forced upon file creation or rename. In partial-retention states (50%), file data survives while the unsynced parent directory entry is dropped, reproducing Pebble #5825 (69 total failures across 32 seeds).
     - `MUT-0-FIX5825` (causal fix control): Immediate parent directory sync is forced upon `Create`, `OpenReadWrite`, and `Rename`, making directory entries durable and causally eliminating the bug (0 failures across all 32 seeds).
   - `realbug_pebble5825_summary.csv`: Aggregated summary statistics across 32 seeds comparing standard DCG, DCG-PR (`dcg-pr`), DCG Flag-Only, R2, R1, and baseline search heuristics.
   - `realbug_pebble5825_per_seed.csv`: Unaggregated per-seed discovery metrics across 32 seeds. Demonstrates that while standard DCG prioritizes 0% retention first (median $k_1 = 310.50$, mean $k_1 = 288.53$), `dcg-pr` incorporates a post-hoc partial-retention prior tracking `UnsyncedDirEntryCount >= 2`, discovering the failure at median $k_1 = 2.50$ (mean $k_1 = 10.50$).

---

## Statistical Reproduction Commands

To reproduce the paired statistical comparisons from the per-seed CSV tables:

```bash
# In-Sample Benchmark (MUT-1..9)
python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9

# Holdout Suite (MUT-X1..X5)
python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12

# Out-of-Contract Control (MUT-X6)
python3 analysis/paired_stats.py results/holdout_per_seed.csv "Out-of-Contract Control" MUT-X6_k2 MUT-X6_k5 MUT-X6_k9

# Pebble #5825 Real-Bug A/B Benchmark (MUT-0)
python3 analysis/paired_stats.py results/realbug_pebble5825_per_seed.csv "Pebble #5825 Real-Bug (32 seeds)" MUT-0
```
