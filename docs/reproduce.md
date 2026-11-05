# Reproduction Runbook

This runbook lists the commands that regenerate every table, figure and acceptance check
in CraConTest, with the outputs and runtimes measured on Linux (go1.27.3, amd64).

**Determinism.** Exploring, ranking and evaluating a *stored* run directory is fully
deterministic: the same `trace.json` and `universe.json` always give the same outcomes,
rankings and curves. Recording is not bit-for-bit repeatable. Pebble schedules background
flushes and compactions itself, so two recordings of the same workload can differ by a few
events and therefore by a few candidates (for example 2,142, 2,144 and 2,150 candidates in
three MUT-0 recordings of the main workload). This is why crash states are always rebuilt
from a stored trace and never by re-running the engine. The committed run directories
under `results/` are the reference data for every figure and table.

---

## 1. Build

Prerequisites: Go as pinned in `go.mod`. On Windows set `CGO_ENABLED=0`; run the race
detector on Linux or WSL2.

```bash
go build -o cracontest ./cmd/cracontest
```

The CLI has five subcommands. Flags use a single dash:

| Command | Flags |
|---|---|
| `record` | `-spec <config.json> -out <run-dir> [-fault <MUT-0 … MUT-9, MUT-X1 … MUT-X6>] [-policies 0,100] [-ingest-every <N>] [-ratchet-at <N>]` |
| `plan` | `-run <run-dir> [-seed <ranking seed>]` |
| `explore` | `-run <run-dir> [-timeout <duration>]` |
| `evaluate` | `-run <run-dir> [-budgets 0,1,5,...] [-discovery <csv> -plot <svg>]` |
| `benchmark` | `[-seeds 42,100,...] [-faults MUT-1,...] [-ops 120] [-report <md>] [-csv <csv>] [-per-seed-csv <csv>] [-ingest-every <N>] [-ratchet-at <N>]` |

### Directory Structure for Reproduction Artifacts

```
CraConTest/
├── analysis/
│   ├── paired_stats.py        # Paired per-seed Wilcoxon & t-test analysis
│   └── requirements.txt       # Python dependencies (scipy>=1.11.0)
├── results/
│   ├── README.md              # Organization of Stage 1 & Stage 2 results artifacts
│   ├── benchmark_summary.csv  # Aggregated 32-seed benchmark metrics (MUT-0..9)
│   ├── benchmark_per_seed.csv # Unaggregated per-seed discovery metrics (MUT-0..9)
│   ├── holdout_summary.csv    # Aggregated 32-seed holdout metrics (MUT-X1..X6)
│   ├── holdout_per_seed.csv   # Unaggregated per-seed discovery metrics (MUT-X1..X6)
│   ├── realbug_pebble5825_summary.csv  # Aggregated 32-seed Pebble #5825 A/B metrics
│   ├── realbug_pebble5825_per_seed.csv # Unaggregated per-seed Pebble #5825 A/B metrics
│   ├── realbug_checkpoint_summary.csv  # Aggregated 32-seed Checkpoint A/B metrics
│   ├── realbug_checkpoint_per_seed.csv # Unaggregated per-seed Checkpoint A/B metrics
│   ├── scaling_mut5_summary.csv        # MUT-5 universe-scaling summary (120..1200 ops)
│   ├── scaling_mut5_per_seed.csv       # Per-seed scaling metrics
│   ├── null_control_multishape.csv     # 108-run multi-shape null-control verification
│   └── subsystem_discovery.csv         # Pebble subsystem empirical discovery (Workloads A, B, C)
├── docs/
│   ├── report.md              # Technical report
│   ├── reproduce.md           # Reproduction runbook
│   └── benchmark-report.md    # Multi-seed benchmark empirical report
└── paper/                     # Reserved for publication materials
```

---

## 2. Main Experiment (1,000-operation workload)

Configuration `experiments/configs/main.json`: seed 42, 1,000 operations, 64 keys, 128-byte
values, a synchronous write every 10th mutation, an explicit flush every 100 operations.
Operation mix: 50% set, 20% get, 10% delete, 10% two-key batch, 5% range delete, 5% scan.
Retention policies 0% and 100%.

```bash
for m in 0 1 2 3 4; do
  d=results/main_mut$m
  ./cracontest record   -spec experiments/configs/main.json -out $d -fault MUT-$m
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery results/discovery_main_mut$m.csv \
                                -plot      results/plot_main_mut$m.svg
done
```

| Run | Fault | Trace events | Candidates | Outcomes | Failure kinds |
|---|---|---|---|---|---|
| `main_mut0` | none | 2,510 | 2,150 | 2,150 valid | 0 |
| `main_mut1` | WAL sync dropped | 2,427 | 1,978 | 865 violations (`durably_committed`) | 15 |
| `main_mut2` | manifest sync dropped | 2,494 | 2,112 | 1,047 open errors (malformed manifest) | 1 |
| `main_mut3` | directory sync dropped | 2,480 | 2,048 | 999 violations (`durably_committed`) | 3 |
| `main_mut4` | flushed-table sync dropped | 2,500 | 2,124 | 594 open errors (table size mismatch) | 2 |

`record`, `plan` and `evaluate` each take well under a second; `explore` takes 5–15 s per
run. `evaluate` writes `curves.csv` and `positions.csv` into the run directory:
`positions.csv` gives, for each ranking and for random order over 1,000 fixed seeds, the
position of the first failure and the position at which every failure kind has appeared.
To recompute curves and positions from a stored run without re-exploring, run only
`evaluate`. If a run directory already holds a `journal.ndjson`, `explore` reuses its
finished outcomes; delete the journal, `outcomes.json` and `curves.csv` to explore again.

Figures: `results/plot_main_mut*.svg`. Tables: `results/discovery_main_mut*.csv` and
`results/main_mut*/positions.csv`.

## 3. Smoke Experiment (100-operation workload)

Configuration `experiments/configs/smoke.json`: seed 42, 100 operations, 16 keys, 64-byte
values, sync every 10th mutation, flush every 50 operations. Use it for a quick end-to-end check.

```bash
for f in mut0:MUT-0 mut1:MUT-1; do
  d=results/smoke_${f%%:*}
  ./cracontest record   -spec experiments/configs/smoke.json -out $d -fault ${f#*:}
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery results/discovery_${f%%:*}.csv \
                                -plot      results/plot_${f%%:*}.svg
done
```

| Run | Trace events | Candidates | Outcomes | Distinct signatures | Total time |
|---|---|---|---|---|---|
| `smoke_mut0` | 316 | 302 | 302 valid | 0 | ≈0.6 s |
| `smoke_mut1` | 305 | 280 | 192 valid, 88 violations | 6 | ≈0.5 s |

## 4. Multi-Seed Benchmark (32 Seeds, MUT-0 to MUT-9)

To evaluate ranking strategies across both dense and sparse failure distributions with statistical confidence intervals, the multi-seed benchmark evaluates 32 deterministic workload seeds across all 10 fault models:

```bash
./cracontest benchmark -report docs/benchmark-report.md -csv results/benchmark_summary.csv -per-seed-csv results/benchmark_per_seed.csv
```

To run the paired statistical comparison across seeds via python3:

```bash
python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9
```

The multi-seed benchmark runs 32 deterministic workload seeds (`42, 100..900, 1001..1022`; 120 operations, `SyncEvery = 8`, `FlushEvery = 40`) across `MUT-0` through `MUT-9` with automatic background compactions and table stats disabled (`DisableAutomaticCompactions: true`, `DisableTableStats: true`, and deterministic mid-workload compaction `CompactAt = 80` for `MUT-8`) so traces and candidate counts are 100% repeatable across runs and platforms.

### Benchmark Results Summary (32 Seeds)

#### Cost-to-First-Failure ($k_1$ Mean) & Distinct Failing States ($S_{25}$ / $S_{100}$ Mean)

| Fault Model | Fault Description | Failure Density | DCG $k_1$ | R2 $k_1$ | B0 $k_1$ | DCG $S_{25}$ / $S_{100}$ | R2 $S_{25}$ / $S_{100}$ | B0 $S_{25}$ / $S_{100}$ |
|---|---|---|---|---|---|---|---|---|
| **MUT-0** | Null Control (No Fault) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** | 1.44 | 2.00 | **23.94** / **23.97** | 17.12 / **23.97** | 6.12 / 14.61 |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** | 1.17 | 1.55 | **25.00** / **33.03** | 21.15 / **33.03** | 5.67 / 13.78 |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** | 1.44 | 2.00 | 24.00 / **27.00** | 17.26 / **27.00** | 5.00 / 11.22 |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** | 1.95 | 2.39 | **21.72** / **21.72** | 12.58 / **21.72** | 3.56 / 8.62 |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** | 13.01 | 25.97 | **2.00** / **2.00** | 1.30 / **2.00** | 0.48 / 1.00 |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** | 7.72 | 31.12 | **1.62** / **1.62** | 1.07 / **1.62** | 0.00 / 0.81 |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** | 3.56 | 4.44 | **12.88** / **12.88** | 6.72 / 12.82 | 2.00 / 5.05 |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** | 1.40 | 1.95 | **25.00** / **27.00** | 17.78 / **27.00** | 3.03 / 9.83 |

### 4.2 Holdout & Out-of-Contract Benchmark (32 Seeds, MUT-X1 to MUT-X6)

To verify that durability-contract priors generalize to unseen fault settings, the holdout suite evaluates 17 parameterized, delayed, and stochastic fault models across 32 deterministic seeds:

```bash
./cracontest benchmark \
  -faults MUT-X1_k2,MUT-X1_k3,MUT-X1_k4,MUT-X1_k6,MUT-X1_k9,MUT-X2_k1,MUT-X2_k3,MUT-X2_k4,MUT-X2_k5,MUT-X3_k1,MUT-X3_k2,MUT-X3_k3,MUT-X4_p04,MUT-X5_p12,MUT-X6_k2,MUT-X6_k5,MUT-X6_k9 \
  -csv results/holdout_summary.csv \
  -per-seed-csv results/holdout_per_seed.csv \
  -report ""
```

To run paired statistical comparisons on the holdout per-seed results:

```bash
# Holdout Faults (MUT-X1..X5)
python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12

# Out-of-Contract Negative Control (MUT-X6)
python3 analysis/paired_stats.py results/holdout_per_seed.csv "Out-of-Contract Control" MUT-X6_k2 MUT-X6_k5 MUT-X6_k9
```

### 4.3 Pebble #5825 Real-Bug A/B Benchmark (32 Seeds, MUT-0 vs BARRIER-5825)

To evaluate real-world crash inconsistency caused by unsynced parent directory entries under partial retention (CockroachDB Pebble #5825), the real-bug benchmark compares unfaulted `MUT-0` against the patched control `BARRIER-5825` (Manifest-Rotation Barrier Control) across 32 seeds under policies 0%, 50%, and 100% with manifest rotation (`-max-manifest-size 1`):

```bash
./cracontest benchmark \
  -faults MUT-0,BARRIER-5825 \
  -max-manifest-size 1 \
  -policies 0,50,100 \
  -csv results/realbug_pebble5825_summary.csv \
  -per-seed-csv results/realbug_pebble5825_per_seed.csv \
  -report ""
```

To run paired statistical comparisons on the Pebble #5825 real-bug per-seed results:

```bash
python3 analysis/paired_stats.py results/realbug_pebble5825_per_seed.csv "Pebble #5825 Real-Bug (32 seeds)" MUT-0
```

#### Results Summary:
- **Causal Verification:** Under `MUT-0`, 99 failures occur across 31 failing seeds (all manifesting exclusively at 50% partial-retention candidates). Under `BARRIER-5825`, which forces parent directory sync on `Create`, `OpenReadWrite`, and `Rename`, failures drop to **0 across all 32 seeds**, causally confirming the fix for Pebble #5825.
- **Post-Hoc Partial-Retention Prior (`dcg-pr`):** Standard `dcg` prioritizes 0%-retention candidates first, requiring a median $k_1$ of **205.00** [203.00, 210.00] (mean 253.52) to discover the partial-retention failure. `dcg-pr` incorporates a partial-retention prior tracking `UnsyncedDirEntryCount >= 2`, discovering the first failure at median $k_1 = \mathbf{2.00}$ [1.00, 3.00] (mean 2.65, AUC Failures = 0.8455, AUC Bugs = 0.9212), compared to uniform random B0 median $k_1 = 104.50$ (mean 134.63) and state-representative B5 median $k_1 = 17.00$ (mean 45.26).

### 4.4 Pebble Checkpoint Real-Bug Holdout A/B Benchmark (32 Seeds, MUT-0 vs BARRIER-CKPT)

To evaluate real-world crash recovery failure caused by unsynced parent directory entries in checkpoint creation (CockroachDB Pebble commit `15b500fb`), the checkpoint holdout benchmark evaluates unfaulted `MUT-0` against the causal fix control `BARRIER-CKPT` (Checkpoint Directory Barrier Control) across 32 seeds under partial retention (0%, 50%, 100%) with `-checkpoint-at 60`:

```bash
./cracontest benchmark \
  -faults MUT-0,BARRIER-CKPT \
  -checkpoint-at 60 \
  -policies 0,50,100 \
  -csv results/realbug_checkpoint_summary.csv \
  -per-seed-csv results/realbug_checkpoint_per_seed.csv \
  -report ""
```

To run paired statistical comparisons on the Pebble Checkpoint per-seed results:

```bash
python3 analysis/paired_stats.py results/realbug_checkpoint_per_seed.csv "Pebble Checkpoint Real-Bug (32 seeds)" MUT-0
```

#### Results Summary:
- **Causal Verification:** In Pebble `db.Checkpoint("checkpoint")`, checkpoint manifest and marker pointer files (`marker.manifest.*`) are written before `dir.Sync()`. Under 50% partial retention, unsynced directory entries are dropped while marker pointers persist, causing checkpoint recovery failure (`open_error`) on 30 of 32 seeds (67 total failures across seeds). Under `BARRIER-CKPT`, which forces parent directory sync when creating checkpoint manifest/marker files, checkpoint recovery failures drop to **0 across all 32 seeds**.
- **Pre-Freeze Prior Discovery (`dcg-pr`):** The partial-retention prior (`dcg-pr`) was frozen before evaluation (tag `v1.0-dcg-pr-freeze`). `dcg-pr` discovers the checkpoint failure at median $k_1 = \mathbf{2.00}$ [1.00, 125.50] (mean 93.17, AUC Failures = 0.5414, AUC Bugs = 0.6739), compared to 0%-retention-first DCG median $k_1 = 215.00$ (mean 262.97) and uniform random B0 median $k_1 = 168.75$ (mean 175.40).

### 4.5 MUT-5 Universe-Scaling Benchmark (ops ∈ {120, 300, 600, 1200})

To evaluate candidate universe scaling behavior on single-sync drop faults (`MUT-5`, single WAL sync drop), the scaling experiment evaluates 8 seeds across 4 operations scales:

```bash
go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update
```

Outputs: `results/scaling_mut5_per_seed.csv` and `results/scaling_mut5_summary.csv`.

> **Note:** `TestScalingMUT5` and `TestNullControlMultiShape` validate the committed CSVs in `results/` by default during `go test ./...` (read-only — no files are written). To regenerate the CSVs from scratch, pass the `-update` flag:
> ```
> go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update
> go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update
> ```

#### Results Summary:
- Candidate universes scale from ~338 (120 ops) to ~2,958 (1,200 ops).
- DCG maintains constant $O(1)$ discovery cost: median $k_1 = 1.00$, mean $k_1 = 1.00$ across all 4 scales.
- Random and heuristic baselines scale linearly with universe size: B0 mean $k_1$ scales from 25.75 to 222.38; B2 (LIFO) mean $k_1$ scales from 260.50 to 2,881.25; R2 mean $k_1$ scales from 11.62 to 83.50.
- Average planning/ranking runtime remains sub-60ms (56.1ms) even at ~3,000 candidates.

### 4.6 Multi-Shape Null-Control Verification (108 Runs, 0 False Alarms)

To establish zero false alarm rates across diverse workload geometries, the multi-shape null-control check tests 12 shapes across 9 seeds on `MUT-0` at 0%/100% policies:

```bash
go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update
```

Outputs: `results/null_control_multishape.csv`.
Result: Exactly 0 violations, 0 open errors, 0 panics, and 0 hangs across all 108 runs (>50,000 crash states explored).

### 4.7 Pebble Subsystem Discovery Benchmark (Workloads A, B, C under MUT-0)

To evaluate whether unfaulted Pebble v2.1.7 (`MUT-0`) exhibits crash-consistency vulnerabilities during multi-file and directory-marker mutations, the subsystem discovery suite executes Workload A (External SSTable Ingestion), Workload B (Live Format-Version Ratcheting), and Workload C (Combined Lifecycle Stress) across seeds `42, 100, 777` under intermediate retention policies `[0, 50, 100]`:

```bash
# Read-only validation of committed artifact:
go test -v -run TestSubsystemDiscovery ./internal/exp

# Full regeneration of discovery CSV:
go test -v -run TestSubsystemDiscovery ./internal/exp -update
```

Outputs: `results/subsystem_discovery.csv` (9 data rows).

To execute the CLI benchmark commands directly:
```bash
# Workload A: External SSTable Ingestion
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ingest-every 25 -policies 0,50,100

# Workload B: Live Format-Version Ratcheting
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ratchet-at 50 -policies 0,50,100

# Workload C: Combined Lifecycle Stress
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ingest-every 30 -ratchet-at 40 -checkpoint-at 60 -max-manifest-size 1 -policies 0,50,100
```

#### Results Summary:
- **0 lost durably-committed keys** across all 9 runs (5,409 candidate states), proving 100% synchronous durability via `d.objProvider.Sync()` (`ingest.go:1490`) and `atomicfs.Marker.Move` (`marker.go:189`).
- **0 engine recovery panics or hangs**.
- **Workload A (External SSTable Ingestion):** 0 lost committed keys. 40 prefix violations across seeds 42 (15) and 777 (25) reveal a cross-channel commit-order hole on direct L0 ingest: when `db.Ingest` takes the non-overlapping direct L0 shortcut (`asFlushable = false`), Pebble allocates sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1$ is lost from the unsynced `WAL`, creating a cross-channel sequence hole (whereas overlapping flushable ingest syncs the shared WAL at `commit.go:289` and preserves strict prefix order).
- **Workload B (Live Format-Version Ratcheting):** 100% clean recovery and atomic transitions across all policies; 0 violations, 0 panics, and 4 transient unlinked-marker open errors under 50% retention (1 on seed 42, 2 on seed 100, 1 on seed 777).
- **Workload C (Combined Lifecycle Stress):** 0 lost committed keys, 0 panics. 23 open errors occur exclusively under 50% partial retention during manifest rotation and checkpointing prior to directory sync (6 on seed 42, 10 on seed 100, 7 on seed 777).

## 5. Acceptance Tests

| Check | Command | Measured result | Runtime |
|---|---|---|---|
| G1 replay fidelity | `go test -v -run 'TestGateG1$' ./internal/replay` | 2,322 events, 20/20 seeded cutoffs byte-identical to the live 100% clone; every replayed state recovers | 0.1 s |
| G2 null control | `go test -v -run 'TestGateG2$' ./internal/workload` | every endpoint candidate of a fresh 1,000-op MUT-0 recording (≈2,140) valid; 0 violations, open errors, panics, hangs or oracle errors | 7 s |
| G3 graph labels | `go test -v -run 'TestGateG3$' ./internal/graph` | ≈2,510 events, ≈1,073 cutoffs, ≈173 distinct state keys, Semantic=1 on 96% of cutoffs, build time ≈12 ms | 0.05 s |
| G4 discovery curves | `go test -v -run 'TestGateG4$\|TestMainRunArtifacts' ./internal/exp` | curves recomputed from the stored smoke and main tables match the committed `results/discovery_*.csv`; nothing re-executed | 0.5 s |
| Real-disk recovery | `go test -v -run TestRealDisk ./internal/recovery` | 200 synced keys survive a clean close and an abrupt child-process exit on `vfs.Default` | 1.6 s |
| Subsystem discovery | `go test -v -run TestSubsystemDiscovery ./internal/exp` | 9 rows validated in `results/subsystem_discovery.csv`; 0 durably committed violations, 0 panics | <0.01 s |

G1, G2 and G3 record a fresh trace, so their counts vary by a few events between runs
(see Determinism above).

## 6. Full Suite

```bash
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...      # Linux or WSL2
```

All 13 packages pass. `go test` takes about 12 s and `go test -race` about 50 s on Linux.
