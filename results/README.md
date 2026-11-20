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

Stage 2 stores aggregated 95%-confidence-interval summaries and unaggregated per-seed discovery tables across multi-seed benchmark evaluations (120 operations/seed, 32 deterministic seeds: `42, 100..900, 1001..1022`). These tables enable full statistical reproduction and hypothesis testing via `analysis/paired_stats.py` without requiring the repository to commit hundreds of megabytes of raw per-run trace and universe JSONs.

Within each workload seed (`RunSingleSeedFault` in `internal/exp/benchmark.go`), `R1`, `R2`, and `DCG-FlagOnly` average metrics across 100 shuffles (`seed*1000 + i*31 + 17`), whereas `B0` records the per-seed median across 100 fixed shuffles (`i*31 + 17`) before computing the cross-seed mean and median across the 32 workload seeds (making `B0` a stricter, lower-$k_1$ baseline than the analytical mean expectation $E[K_1] = (N+1)/(m+1)$, e.g., $2.00$ vs. $2.73$ on `MUT-1`).

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

3. **Pebble #5825 Case Study Benchmark (`results/casestudy_pebble5825_summary.csv`, `results/casestudy_pebble5825_per_seed.csv`):**
   - Scope: Real-world bug from CockroachDB Pebble issue #5825 evaluated across 32 deterministic seeds under partial retention policies (0%, 50%, 100%) with manifest rotation (`-max-manifest-size 1`).
     - `MUT-0` (unfaulted baseline): Parent directory sync is not forced upon file creation or rename. In partial-retention states (50%), file data survives while the unsynced parent directory entry is dropped, reproducing Pebble #5825 (58 total failures across 25 of 32 failing seeds).
   - `BARRIER-5825` (Manifest-Rotation Barrier Control): Enforces a parent-directory sync before creating a new `marker.manifest.*` file. In the evaluated model, this makes the pending `MANIFEST-xxxxxx` entry durable before its marker; no failures are observed across the 32 evaluated seeds.
   - `casestudy_pebble5825_summary.csv`: Aggregated summary statistics across 32 seeds comparing standard DCG, DCG-PR (`dcg-pr`), DCG Flag-Only, R2, R1, and baseline search heuristics.
   - `casestudy_pebble5825_per_seed.csv`: Unaggregated per-seed discovery metrics across 32 seeds. Standard DCG prioritizes 0% retention first (median/mean $k_1=203.00/243.84$); post-hoc `dcg-pr` discovers the failure at 2.00/12.40, compared with accounting-corrected B0 178.73/192.72 and representative B5 62.00/57.68.

4. **Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race (`results/casestudy_checkpoint_summary.csv`, `results/casestudy_checkpoint_per_seed.csv`):**
   - Scope: Post-Freeze Evaluation 1 evaluates the checkpoint directory race present in Pebble v2.1.7 and later fixed upstream by `15b500fb`. The benchmark uses 32 deterministic seeds (`42, 100..900, 1001..1022`), retention policies 0%, 50%, and 100%, and `-checkpoint-at 60`.
   - `MUT-0` (unfaulted baseline): Pebble `db.Checkpoint("checkpoint")` writes checkpoint manifest and marker pointer files before `dir.Sync()`. Under 50% partial retention, reopening fails on 31 of 32 seeds (53 total failures).
   - `BARRIER-CKPT` (Checkpoint Directory Barrier Control): Adds immediate parent-directory syncs when writing checkpoint manifests and markers. No checkpoint recovery failures are observed across the 32 evaluated seeds.
   - `casestudy_checkpoint_summary.csv`: Accounting-corrected DCG-PR median/mean $k_1=245.00/156.39$ (AUC Failures 0.5010, AUC Bugs 0.6321), versus DCG 288.00/258.65, R2 280.41/271.55, and B0 262.26/238.53. The paired DCG-PR/R2 result is 22/9/0.
   - `casestudy_checkpoint_per_seed.csv`: Unaggregated per-seed discovery metrics across all 32 seeds.

5. **MUT-5 Universe-Scaling Benchmark (`results/scaling_mut5_summary.csv`, `results/scaling_mut5_per_seed.csv`):**
   - Scope: Scaling behavior of prioritization strategies on `MUT-5` (single WAL sync drop) across operations $\in \{120, 300, 600, 1200\}$ and 8 seeds ($\in \{42, 100, 200, 300, 400, 500, 600, 700\}$), generating candidate universes from ~338 to ~2,958 candidates.
   - `scaling_mut5_summary.csv`: Summary metrics across the 4 operation counts showing mean and median $k_1$ for DCG, R2, R1, B0, and B2 (LIFO), along with average planning and exploration times.
   - `scaling_mut5_per_seed.csv`: Unaggregated per-seed, per-ops run metrics (32 runs total) recording `ops, seed, universe_size, k1_dcg, k1_r2, k1_r1, k1_b0, k1_b2, plan_rank_ms, explore_total_ms`.
   - Key findings: DCG has $k_1 = 1.00$ at each of the four evaluated operation counts. Over the same measured range, B0's mean of per-seed rounded shuffle medians increases from 26.75 to 220.88 and B2 (LIFO) from 260.50 to 2,881.00. These finite measurements do not establish asymptotic growth rates. Planning and ranking time reaches 38.5 ms at approximately 3,000 candidates.

6. **Multi-Shape Null-Control Verification (`results/null_control_multishape.csv`):**
   - Scope: Extended empirical false-alarm verification across 12 distinct workload shapes (varying operations $\in [80, 300]$, key counts $\in [8, 64]$, value sizes $\in [32, 256]$ bytes, sync intervals $\in [4, 12]$, flush intervals $\in [25, 60]$, and compactions) and 9 seeds ($42, 100..800$) on `MUT-0` at 0%/100% retention policies (108 runs total).
   - Findings: No violations, open errors, panics, or hangs are observed across the 108 evaluated runs (>50,000 crash candidates). Because workloads are fixed and candidates from one trace are dependent, these counts do not estimate a population false-alarm probability.

7. **Pebble Subsystem Discovery Benchmark (`results/subsystem_discovery.csv`):**
   - Scope: Systematic empirical discovery across Pebble v2.1.7 subsystems performing multi-file and directory-marker mutations (Workload A: External SSTable Ingestion, Workload B: Live Format-Version Ratcheting, Workload C: Combined Lifecycle Stress) under unfaulted `MUT-0` across intermediate retention policies `[0, 50, 100]` and seeds `42, 100, 777` (5,415 candidate states evaluated).
   - `subsystem_discovery.csv`: 9 data rows recording workload, seed, ops, maintenance parameters (`ingest_every`, `ratchet_at`, `checkpoint_at`, `max_manifest_file_size`), policies, universe size, violations, durably committed violations, prefix violations, structural violations, open errors, panics, hangs, and git commit hash.
    - Findings & Empirical Crash-Consistency Evaluation:
       - **0 lost durably-committed keys** across all 9 runs. No durably-committed data loss is observed in the evaluated workloads and crash states.
     - **0 engine recovery panics or hangs**.
     - **Workload A (External SSTable Ingestion):** 0 lost committed keys. 40 prefix violations across seeds 42 (15) and 777 (25) reveal a cross-channel commit-order hole on direct L0 ingest: when `db.Ingest` takes the non-overlapping direct L0 shortcut (`asFlushable = false`), Pebble allocates sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1$ is lost from the unsynced `WAL`, creating a cross-channel sequence hole (whereas overlapping flushable ingest syncs the shared WAL at `commit.go:289` and preserves strict prefix order).
     - **Workload B (Live Format Ratcheting):** 0 violations, 0 panics, and 3 open errors under 50% retention at initial database creation cutoffs (0 on seed 42, 2 on seed 100, 1 on seed 777). `atomicfs.LocateMarker` deterministically selects the latest valid format marker iteration under both full and partial retention.
   - **Workload C (Combined Stress):** 0 lost committed keys, 0 panics. 15 open errors occur exclusively under 50% partial retention during manifest rotation and checkpointing prior to directory sync (5 on seed 42, 7 on seed 100, 3 on seed 777).
   - Reproduction & Validation Commands:
     - Read-only validation: `go test -v -run TestSubsystemDiscovery ./internal/exp`
     - CSV regeneration: `go test -v -run TestSubsystemDiscovery ./internal/exp -update`

8. **Direct L0 `db.Ingest` 32-Seed Benchmark (`results/casestudy_ingest_summary.csv`, `results/casestudy_ingest_per_seed.csv`):**
   - Scope: Post-Freeze Evaluation 2 (Cross-Channel Ordering — Direct L0 `db.Ingest` undocumented cross-channel ordering behavior where sequence numbers advance and MANIFEST is synced without syncing preceding un-flushed WAL writes under a global prefix oracle), evaluated across 32 seeds (`seed = 1..32`, 25 ops) under endpoint `[0, 100]` retention (>13,000 candidate crash states).
   - Findings: Under unfaulted `MUT-0` at endpoint (`0%/100%`) retention, **22 of 32 seeds** exhibit prefix violations (276 total failures; seeds `3, 5, 7, 8, 9, 16, 20, 23, 26, 29` have 0 failures because randomly generated ingest keys overlapped the active memtable and took the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`). Under `BARRIER-INGEST`, no prefix violations are observed across the 32 evaluated seeds. DCG/DCG-PR median/mean $k_1$ is 9.00/15.32, R2 is 26.77/27.00, and accounting-corrected B0 is 33.74/34.32; the paired DCG-PR/R2 sign test is not significant at 0.05 (13/6/3, $p=0.0835$).

9. **Multi-Distribution Workload Diversity Suite (`results/workload_diversity_summary.csv`, `results/workload_diversity_per_seed.csv`):**
   - Scope: Generalization evaluation across 5 standardized access distributions (`ycsb-a`, `ycsb-b`, `ycsb-f`, `overwrite-heavy`, `delete-heavy`) evaluated across 16 seeds (`seed = 1..16`, 25 ops) and 4 fault models (`MUT-0`, `MUT-1`, `MUT-5`, `MUT-9`).
   - Findings: No failures are observed on `MUT-0` across the 80 evaluated preset/seed trials. Across 14 failing settings, DCG has lower mean $k_1$ than corrected B0 on 14 and R2 on 13, with one `delete-heavy/MUT-1` regression. DCG $S_{25}$ is at least R2 on all 14 (9 strict, 5 ties). Corrected sparse examples are `ycsb-f/MUT-5` DCG/R2/B0 1.00/8.49/39.71 and `overwrite-heavy/MUT-5` 1.00/9.40/22.99.

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

# Pebble #5825 Case Study Benchmark (MUT-0)
python3 analysis/paired_stats.py results/casestudy_pebble5825_per_seed.csv "Pebble #5825 Case Study (32 seeds)" MUT-0

# Pebble Checkpoint Case Study Benchmark (MUT-0)
python3 analysis/paired_stats.py results/casestudy_checkpoint_per_seed.csv "Pebble Checkpoint Case Study (32 seeds)" MUT-0
```

---

## Provenance and Cross-Validation Contract

`results/` contains 23 CSV files in total. Each CSV's own schema/provenance is authoritative.

- **14 accounting-corrected CSVs carrying `git_commit = f9202dfca579`:** all Stage 2 benchmark, holdout, case-study, workload-diversity, null-control, and subsystem-discovery CSVs. B0 `*_mean` fields are true shuffle means under `v1.1.2-evaluation-freeze`.

- **All 14 Stage 2 CSVs carrying a `git_commit` column identify `f9202dfca579`**, the accounting-corrected `v1.1.2-evaluation-freeze`. Earlier artifacts remain reachable through Git history and their immutable tags; they are not mixed into current tables.

- **9 Fixed-Schema Regression CSVs Without a `git_commit` Column:**
  - **7 Stage 1 Single-Run Discovery Curve Tables:** `discovery_mut0.csv` and `discovery_mut1.csv` guarded byte-for-byte by `TestGateG4` (`internal/exp/gate_g4_test.go:20-95`); `discovery_main_mut0.csv` through `discovery_main_mut4.csv` guarded byte-for-byte by `TestMainRunArtifactsMatchStoredTables` (`internal/exp/main_artifacts_test.go:15-56`).
  - **2 Trace-Length Scaling Tables:** `scaling_mut5_per_seed.csv` validated read-only by `TestScalingMUT5` via `validateScalingCSV` (`internal/exp/scaling_test.go:33-92`), and both `scaling_mut5_per_seed.csv` and `scaling_mut5_summary.csv` regenerated together by `TestScalingMUT5 -update` (`internal/exp/scaling_test.go:154-225`).
