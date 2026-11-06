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

3. **Pebble #5825 Case Study Benchmark (`results/casestudy_pebble5825_summary.csv`, `results/casestudy_pebble5825_per_seed.csv`):**
   - Scope: Real-world bug from CockroachDB Pebble issue #5825 evaluated across 32 deterministic seeds under partial retention policies (0%, 50%, 100%) with manifest rotation (`-max-manifest-size 1`).
     - `MUT-0` (unfaulted baseline): Parent directory sync is not forced upon file creation or rename. In partial-retention states (50%), file data survives while the unsynced parent directory entry is dropped, reproducing Pebble #5825 (98 total failures across 31 failing seeds).
     - `BARRIER-5825` (Manifest-Rotation Barrier Control): Enforces a parent directory sync (`fsync("db")`) invoked *before* `fs.inner.Create` when creating any `marker.manifest.*` file. Because this executes before creating the new marker entry, the unsynced directory entry flushed to durability at that exact moment is the pending `MANIFEST-xxxxxx` entry (created earlier in `createManifest`), ensuring the target manifest is durable before the marker pointing to it exists. This causally eliminates the bug (0 failures across all 32 seeds).
   - `casestudy_pebble5825_summary.csv`: Aggregated summary statistics across 32 seeds comparing standard DCG, DCG-PR (`dcg-pr`), DCG Flag-Only, R2, R1, and baseline search heuristics.
   - `casestudy_pebble5825_per_seed.csv`: Unaggregated per-seed discovery metrics across 32 seeds. Demonstrates that while standard DCG prioritizes 0% retention first (median $k_1 = 205.00$ [203.00, 210.00], mean $k_1 = 253.45$), `dcg-pr` incorporates a post-hoc partial-retention prior tracking `UnsyncedDirEntryCount >= 2`, discovering the failure at median $k_1 = \mathbf{2.00}$ [1.00, 3.00] (mean $k_1 = 2.65$, AUC Failures = $0.8455$, AUC Bugs = $0.9212$), compared to uniform random B0 median $k_1 = 112.00$ (mean $136.23$) and state-representative B5 median $k_1 = 17.00$ (mean $45.26$).

4. **Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race (`15b500fb`) (`results/casestudy_checkpoint_summary.csv`, `results/casestudy_checkpoint_per_seed.csv`):**
   - Scope: Real-world holdout bug from CockroachDB Pebble commit `15b500fb` (`checkpoint.go` directory sync omission) evaluated across 32 deterministic seeds (`42, 100..900, 1001..1022`) under partial retention policies (0%, 50%, 100%) with `-checkpoint-at 60`.
   - `MUT-0` (unfaulted baseline): Pebble `db.Checkpoint("checkpoint")` writes checkpoint manifest and marker pointer files (`marker.manifest.*`) before `dir.Sync()`. Under 50% partial retention, unsynced directory entries are dropped while marker pointers survive, causing checkpoint recovery failure (`open_error`) on 30 of 32 seeds (67 total failures across seeds).
   - `BARRIER-CKPT` (Checkpoint Directory Barrier Control): Forces immediate parent directory sync when writing checkpoint manifests and markers inside `checkpoint`. Completely eliminates checkpoint recovery failures across all 32 seeds (0 checkpoint failures).
   - `casestudy_checkpoint_summary.csv`: Aggregated summary statistics across 32 seeds comparing standard DCG, DCG-PR (`dcg-pr`), DCG Flag-Only, R2, R1, and baseline search heuristics. Demonstrates that `dcg-pr` (prior frozen before evaluation) discovers checkpoint failures at median $k_1 = \mathbf{2.00}$ [1.00, 125.50] (mean $k_1 = 93.03$, AUC Failures = $0.5414$, AUC Bugs = $0.6739$), whereas 0%-retention-first DCG has median $k_1 = 215.00$ (mean $k_1 = 262.93$), R2 has median $k_1 = 323.56$ (mean $k_1 = 325.80$), and random B0 has median $k_1 = 162.50$ (mean $175.35$).
   - `casestudy_checkpoint_per_seed.csv`: Unaggregated per-seed discovery metrics across all 32 seeds.

5. **MUT-5 Universe-Scaling Benchmark (`results/scaling_mut5_summary.csv`, `results/scaling_mut5_per_seed.csv`):**
   - Scope: Scaling behavior of prioritization strategies on `MUT-5` (single WAL sync drop) across operations $\in \{120, 300, 600, 1200\}$ and 8 seeds ($\in \{42, 100, 200, 300, 400, 500, 600, 700\}$), generating candidate universes from ~338 to ~2,958 candidates.
   - `scaling_mut5_summary.csv`: Summary metrics across the 4 operation counts showing mean and median $k_1$ for DCG, R2, R1, B0, and B2 (LIFO), along with average planning and exploration times.
   - `scaling_mut5_per_seed.csv`: Unaggregated per-seed, per-ops run metrics (32 runs total) recording `ops, seed, universe_size, k1_dcg, k1_r2, k1_r1, k1_b0, k1_b2, plan_rank_ms, explore_total_ms`.
   - Key findings: DCG preserves constant $O(1)$ first-failure cost ($k_1 = 1.00$ across all ops levels), while B0 scales linearly from mean $k_1 = 25.75$ to $222.38$, and B2 (LIFO) scales linearly from mean $k_1 = 260.50$ to $2881.25$. Planning and ranking overhead remains sub-60ms (56.1ms) even at ~3,000 candidates.

6. **Multi-Shape Null-Control Verification (`results/null_control_multishape.csv`):**
   - Scope: Extended empirical false-alarm verification across 12 distinct workload shapes (varying operations $\in [80, 300]$, key counts $\in [8, 64]$, value sizes $\in [32, 256]$ bytes, sync intervals $\in [4, 12]$, flush intervals $\in [25, 60]$, and compactions) and 9 seeds ($42, 100..800$) on `MUT-0` at 0%/100% retention policies (108 runs total).
   - Findings: Exactly 0 violations, 0 open errors, 0 panics, and 0 hangs across all 108 runs (>50,000 crash candidates evaluated), verifying a 0.00% false-alarm rate across diverse workload topologies.

7. **Pebble Subsystem Discovery Benchmark (`results/subsystem_discovery.csv`):**
   - Scope: Systematic empirical discovery across Pebble v2.1.7 subsystems performing multi-file and directory-marker mutations (Workload A: External SSTable Ingestion, Workload B: Live Format-Version Ratcheting, Workload C: Combined Lifecycle Stress) under unfaulted `MUT-0` across intermediate retention policies `[0, 50, 100]` and seeds `42, 100, 777` (5,424 candidate states evaluated).
   - `subsystem_discovery.csv`: 9 data rows recording workload, seed, ops, maintenance parameters (`ingest_every`, `ratchet_at`, `checkpoint_at`, `max_manifest_file_size`), policies, universe size, violations, durably committed violations, prefix violations, structural violations, open errors, panics, hangs, and git commit hash.
   - Findings & Empirical Crash-Consistency Evaluation:
     - **0 lost durably-committed keys** across all 9 runs. Once `db.Ingest` returns `nil`, ingested keys are permanently durable and recovered 100% of the time, confirming 100% synchronous durability via `d.objProvider.Sync()` (`ingest.go:1490`) and `atomicfs.Marker.Move` (`marker.go:189`).
     - **0 engine recovery panics or hangs**.
     - **Workload A (External SSTable Ingestion):** 0 lost committed keys. 40 prefix violations across seeds 42 (15) and 777 (25) reveal a cross-channel commit-order hole on direct L0 ingest: when `db.Ingest` takes the non-overlapping direct L0 shortcut (`asFlushable = false`), Pebble allocates sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1$ is lost from the unsynced `WAL`, creating a cross-channel sequence hole (whereas overlapping flushable ingest syncs the shared WAL at `commit.go:289` and preserves strict prefix order).
     - **Workload B (Live Format Ratcheting):** 100% clean recovery and atomic transitions across all policies; 0 violations, 0 panics, and 4 transient unlinked-marker open errors under 50% retention (1 on seed 42, 2 on seed 100, 1 on seed 777). `atomicfs.LocateMarker` deterministically selects the latest valid format marker iteration under both full and partial retention.
     - **Workload C (Combined Stress):** 0 lost committed keys, 0 panics. 17 open errors occur exclusively under 50% partial retention during manifest rotation and checkpointing prior to directory sync (5 on seed 42, 8 on seed 100, 4 on seed 777).
   - Reproduction & Validation Commands:
     - Read-only validation: `go test -v -run TestSubsystemDiscovery ./internal/exp`
     - CSV regeneration: `go test -v -run TestSubsystemDiscovery ./internal/exp -update`

8. **Direct L0 `db.Ingest` 32-Seed Benchmark (`results/casestudy_ingest_summary.csv`, `results/casestudy_ingest_per_seed.csv`):**
   - Scope: Real-world bug from direct L0 external SSTable ingestion (`asFlushable = false`) where sequence numbers advance and MANIFEST is synced without syncing preceding un-flushed WAL writes, evaluated across 32 seeds (`seed = 1..32`, 25 ops) under endpoint `[0, 100]` retention (>13,000 candidate crash states).
   - Findings: Under unfaulted `MUT-0`, prefix violations manifest on 26 of 32 seeds under partial retention and 22 of 32 under endpoint retention. Under causal remedy `BARRIER-INGEST` (`db.LogData(nil, pebble.Sync)`), prefix violations drop to **0 across all 32 seeds**. Pre-frozen `DCG-PR` and `DCG` achieve median $k_1 = \mathbf{9.00}$ [9.00, 19.00] (mean $15.32$), compared to $27.88$ (mean $27.64$) for `R2`, $28.60$ (mean $30.61$) for `R1`, $28.18$ (mean $29.88$) for `DCG Flag-Only`, $81.00$ (mean $80.18$) for `sync-boundary`, and $21.50$ (mean $24.73$) for uniform random `B0`.

9. **Multi-Distribution Workload Diversity Suite (`results/workload_diversity_summary.csv`, `results/workload_diversity_per_seed.csv`):**
   - Scope: Generalization evaluation across 5 standardized access distributions (`ycsb-a`, `ycsb-b`, `ycsb-f`, `overwrite-heavy`, `delete-heavy`) evaluated across 16 seeds (`seed = 1..16`, 25 ops) and 4 fault models (`MUT-0`, `MUT-1`, `MUT-5`, `MUT-9`).
   - Findings: Exactly 0 false alarms on unfaulted control `MUT-0` across all 16 seeds for all 5 presets (80 complete trials). `DCG` achieves up to **$9.3\times$ speedup** over `R2` on `overwrite-heavy/MUT-5` (mean $k_1 = 1.00$ vs $9.30$, and **$20.5\times$** over `B0`'s $20.50$) and **$9.3\times$** on `ycsb-f/MUT-5` (mean $k_1 = 1.00$ vs $9.30$, and **$20.5\times$** over `B0`'s $20.50$), maintaining $k_1 = 1.00$ across all failing presets on `ycsb-a`, `ycsb-b`, `ycsb-f`, and `overwrite-heavy`.

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
