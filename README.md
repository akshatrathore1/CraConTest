# CraConTest

**CraConTest** is an impact-aware, graph-guided crash-consistency testing framework for persistent key-value storage engines, evaluated on [CockroachDB's Pebble](https://github.com/cockroachdb/pebble).

---

## Overview

Testing storage engines for crash consistency typically requires exploring huge state spaces. Every filesystem mutation, flush, or write-ahead log (WAL) entry represents a potential crash cutoff, compounded by partial retention of unsynced dirty data across files. Exhaustive exploration grows quickly with workload size, and uniform random selection may spend its budget on redundant crash states. CraConTest measures whether a ranking informed by persistence dependencies and durability contracts outperforms random under the same testing budget.

CraConTest treats crash-consistency testing as an explicit resource-bounded search problem. By capturing low-level Virtual File System (VFS) operations alongside storage engine semantic markers, CraConTest constructs a **persistence dependency graph** and extracts **POSIX durability-contract features**. It prioritizes candidate crash points based on structural contract violations, reachable uncommitted hazards, and persistence-state equivalence classes.

### Key Contributions & Capabilities

1. **Non-Invasive VFS Interception & Attribution (`vfsrec`):**
   - Intercepts and records all filesystem mutations, flushes, syncs (`Sync` and `SyncData`), and namespace lineage without modifying storage engine internals.
   - Correlates client-level transaction boundaries with physical persistence events.

2. **Persistence Dependency Graph & Durability-Contract Features (`graph`):**
   - Constructs a causal persistence DAG capturing file lifecycle (creation, deletion, replacements, hard links), intra-file sequential dependencies, and cross-file causal ordering via engine markers.
   - Computes exact topological reachability bitsets and partitions crash candidates into canonical persistence states (`StateKey`). `StateKey` represents a two-sided approximation: finer than the physical disk image on pending entries (since `keyPending` includes the creating `trace.Event` ID), and coarser than the client-level oracle (aggregating dirty byte spans without tracking client acknowledgment history or overwritten spans).
   - Extracts POSIX durability-contract features (`features.go`) in a single causal forward pass, tracking unpersisted WAL writes after client acknowledgment (`AckSyncMissing`), uncommitted SSTable data or directory entries referenced by committed `MANIFEST` edits (`committedInManifest` / `DanglingTableCount`), unsynced parent directory entries across `Create`, `ReuseForWrite`, `Rename`, `Link`, and `MkdirAll` (`UnsyncedDirectoryRename`), and unpersisted `MANIFEST` writes.

3. **Ranking Strategies & Baselines (`strategy`):**
   - **B0 (Uniform Random):** Seeded uniform random selection across the candidate universe.
   - **B1–B3 (Position Heuristics):** FIFO (`B1`), LIFO (`B2`), and deterministic Stride (`B3`).
   - **B4 (Combined Topological Reachability):** Greedy multi-term score (`Desc + CrossFile + Semantic + Novelty`) over downstream graph reachability.
   - **B5 (State-Representative):** First candidate per distinct `(StateKey, Retention)` equivalence class in trace order.
   - **B6 (Sync-Boundary):** Prioritizes cutoffs immediately preceding a `Sync`, `SyncData`, or `Rename`.
   - **R1 (0%-Retention-First Random):** Partitions the universe by retention policy, randomly sampling 0%-retention candidates before 100%-retention candidates.
   - **R2 (State-Representative Random):** Partitions 0%-retention candidates into canonical `StateKey` equivalence classes, selecting boundary-adjacent representatives within each `StateKey` class using graph features (`BeforeBoundary`, `BarrierProximity`, and canonical order `candidate.Less`) before shuffling classes, and tests redundant cutoffs afterward.
   - **RHG / B7 (Risk-Horizon Guided):** Ranks 0%-retention state representatives by uncommitted hazard reachability, barrier proximity, and semantic tier.
   - **DCG / B8 (Durability-Contract Guided) & DCG-FlagOnly (Ablation):** Trace-derived durability-contract analysis that prioritizes 0%-retention state representatives that exhibit structural POSIX durability-contract violations (`ContractScore > 0`) ahead of unflagged representatives, alongside a randomized flag-only ablation (`DCG-FlagOnly`) that isolates the effect of the contract indicators (POSIX durability-contract priors instantiated for Pebble). DCG-FlagOnly ties full DCG on nearly every setting, showing that the binary durability-contract flags combined with StateKey equivalence partitioning drive the speedup.
   - **DCG-PR (Durability-Contract Guided with Partial-Retention Prior):** A post-hoc variant of DCG that groups intermediate (`0 < Percent < 100`) candidates by `(StateKey, Percent)`, picks one canonical representative per group via `isDCGBetterRep` (preferring `UnsyncedDirEntryCount >= 2`), promotes only those selected representatives with `ContractFeatures.UnsyncedDirEntryCount >= 2` into Tier 1 ahead of the 0%-retention representative sweep, places remaining intermediate representatives after 0%-retention representatives, and places all intermediate non-representatives behind 0%-retention non-representatives. Achieves median $k_1 = 1.00$ on all 12 testable in-contract holdout fault settings, $k_1 = \mathbf{2.00}$ on Pebble issue `#5825`, and median $k_1 = \mathbf{245.00}$ (CI `[3.00, 306.00]`, mean `156.39`) on the checkpoint race later fixed upstream by `15b500fb`.

4. **Strict Ground-Truth Crash Oracle (`oracle`):**
   - Evaluates recovered database states against an append-only mutation history ledger (`history`).
   - Verifies durably committed operations, atomic batch commit atomicity, and prefix consistency.
   - Detects data loss, torn updates, resurrected keys, corruptions, and recovery panics or hangs.

5. **High-Throughput Replay & Evaluation Pipeline (`exp`):**
   - Replays crash states onto memory-backed crashable filesystems with sub-millisecond per-candidate execution.
   - Supports offline multi-strategy evaluation from cached outcomes without redundant execution.
   - Includes an automated multi-seed benchmark runner for evaluating ranking strategies across diverse fault models and workload distributions.

---

## Architecture

The testing workflow is organized into four decoupled phases:

```
Workload Spec ──> [ 1. RECORD ] ──> Trace & Mutation Ledger
                                           │
                                     [ 2. PLAN ] ──> Dependency Graph & Candidate Rankings
                                           │
                                   [ 3. EXPLORE ] ──> Replay, Recovery & Oracle Check
                                           │
                                   [ 4. EVALUATE ] ──> Discovery Curves & Performance Metrics
```

See [docs/architecture.md](docs/architecture.md) for full architectural details.

---

## Directory Structure

```
.
├── cmd/cracontest/          # Command-line interface (record, plan, explore, evaluate, benchmark)
├── analysis/                # Statistical comparison scripts and dependencies
│   ├── paired_stats.py      # Paired per-seed exact sign, Wilcoxon signed-rank, and bootstrap CI analysis
│   ├── test_paired_stats.py # Unit and integration tests for paired_stats.py
│   └── requirements.txt     # Python dependency pin (scipy, with numpy resolved transitively)
├── docs/                    # Technical documentation and guides
│   ├── architecture.md      # System architecture and data flow
│   ├── crash-model.md       # Crash model, durability barriers, and invariants
│   ├── vfs-design.md        # VFS interception and recording specification
│   ├── reproduce.md         # Step-by-step reproduction runbook
│   ├── report.md            # Technical report and experimental evaluation
│   └── benchmark-report.md  # Multi-seed benchmark empirical report
├── experiments/configs/     # Experiment configurations (main.json, smoke.json)
├── internal/
│   ├── candidate/           # Crash candidate universe and serialization
│   ├── exp/                 # Exploration runner, offline evaluator, and benchmark runner
│   ├── faults/              # Durability fault and barrier suite (MUT-0..9, holdouts MUT-X1..X6, BARRIER-5825, BARRIER-CKPT)
│   ├── graph/               # Persistence DAG, state view, and durability-contract features
│   ├── history/             # Mutation history tracking
│   ├── oracle/              # Crash consistency validation oracle
│   ├── recovery/            # Crash recovery harness and integrity verification
│   ├── replay/              # VFS trace replay engine
│   ├── strategy/            # Ranking strategies (B0–B6, R1, R2, RHG, DCG, DCG-FlagOnly, DCG-PR)
│   ├── trace/               # Event schema and wire validation
│   ├── vfsrec/              # Recording VFS filesystem wrapper
│   └── workload/            # Seeded YCSB-style workload drivers
├── paper/                   # Reserved for publication materials
├── results/                 # Experiment outputs, benchmark CSV, discovery curves, and plots
│   ├── benchmark_summary.csv          # Aggregated 32-seed benchmark metrics
│   ├── benchmark_per_seed.csv         # Unaggregated per-seed discovery metrics
│   ├── holdout_summary.csv            # Aggregated holdout fault metrics (MUT-X1..X6)
│   ├── holdout_per_seed.csv           # Per-seed holdout metrics
│   ├── casestudy_pebble5825_summary.csv # Pebble #5825 A/B benchmark summary
│   ├── casestudy_pebble5825_per_seed.csv
│   ├── casestudy_checkpoint_summary.csv # Checkpoint directory-race benchmark summary
│   ├── casestudy_checkpoint_per_seed.csv
│   ├── casestudy_ingest_summary.csv     # Direct L0 Ingest 32-seed A/B benchmark summary
│   ├── casestudy_ingest_per_seed.csv
│   ├── workload_diversity_summary.csv # 5-preset workload diversity summary (16 seeds)
│   ├── workload_diversity_per_seed.csv
│   ├── scaling_mut5_summary.csv       # MUT-5 universe-scaling summary (120..1200 ops)
│   ├── scaling_mut5_per_seed.csv      # Per-seed scaling metrics
│   ├── null_control_multishape.csv    # 108-run multi-shape null-control verification
│   └── subsystem_discovery.csv        # Pebble subsystem empirical discovery (Workloads A, B, C)
└── .github/workflows/       # Continuous integration workflow
```

---

## Quickstart

### Prerequisites

- Go 1.27+ (`go.mod` specifies Go 1.27.4).
- Python 3.12+ (for statistical analysis in `analysis/paired_stats.py`; pinned to `scipy==1.18.1` in `analysis/requirements.txt`, resolving `numpy` transitively, verified on CPython 3.12 and 3.15).
- OS: Linux (Ubuntu/ext4) or Windows (NTFS). On Windows, build and test with `$env:CGO_ENABLED="0"`.

### Building the CLI

```bash
# Linux / macOS
go build -o cracontest ./cmd/cracontest
```

```powershell
# Windows PowerShell
$env:CGO_ENABLED="0"; go build -o cracontest.exe ./cmd/cracontest
```

### Running the End-to-End Smoke Test

Run the full lifecycle on the seeded 100-operation smoke workload (about half a second):

```bash
# Record the workload with injected WAL sync suppression (MUT-1)
./cracontest record -spec experiments/configs/smoke.json -out /tmp/smoke_mut1 -fault MUT-1

# Build the dependency graph, the candidate universe and one ranking per strategy
./cracontest plan -run /tmp/smoke_mut1

# Replay, crash, recover and check every candidate once
./cracontest explore -run /tmp/smoke_mut1

# Evaluate discovery curves from the stored outcome table
./cracontest evaluate -run /tmp/smoke_mut1
```

On Windows PowerShell:

```powershell
.\cracontest.exe record -spec experiments\configs\smoke.json -out $env:TEMP\smoke_mut1 -fault MUT-1
.\cracontest.exe plan -run $env:TEMP\smoke_mut1
.\cracontest.exe explore -run $env:TEMP\smoke_mut1
.\cracontest.exe evaluate -run $env:TEMP\smoke_mut1
```

The curves are written to `/tmp/smoke_mut1/curves.csv` (or `$env:TEMP\smoke_mut1\curves.csv` on Windows). The main 1,000-operation
experiment uses `experiments/configs/main.json`; see [docs/reproduce.md](docs/reproduce.md).

### Running the Multi-Seed Benchmark

Run the full 32-seed in-sample benchmark across `MUT-1` through `MUT-9` (with `MUT-0` null-control verification), writing outputs to a scratch directory so tracked reference artifacts in `docs/` and `results/` are preserved:

```bash
./cracontest benchmark -report /tmp/benchmark-report.md -csv /tmp/benchmark_summary.csv -per-seed-csv /tmp/benchmark_per_seed.csv
```

```powershell
# Windows PowerShell
.\cracontest.exe benchmark -report $env:TEMP\benchmark-report.md -csv $env:TEMP\benchmark_summary.csv -per-seed-csv $env:TEMP\benchmark_per_seed.csv
```

Run the paired statistical comparison across the committed per-seed results:

```bash
python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9
```

```powershell
# Windows PowerShell
python analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9
```

---

## Experimental Findings

The experimental evaluation comprises both single-seed 1,000-operation runs and a 32-seed multi-fault benchmark on Pebble v2.1.7. Full analysis is available in [docs/report.md](docs/report.md) and [docs/benchmark-report.md](docs/benchmark-report.md); raw data is stored under `results/`.

### 1. Oracle Soundness (MUT-0)

Across all 2,150 candidates of the 1,000-operation main run, all 302 candidates of the 100-operation smoke run, and all 32 workload seeds of the multi-seed benchmark (>11,000 evaluated crash states), no failures were observed on unfaulted runs (`MUT-0`) at 0%/100% retention. The 108-run multi-shape null control likewise produced no failures over 12 workload shapes and >50,000 crash states. These are descriptive results for the evaluated non-ingest workloads and endpoint policies, not a population-level false-alarm guarantee; `db.Ingest` and 50% partial retention expose additional mechanisms discussed below.

### 2. Why Naive Topological Reachability (B4) Underperforms Random on Dense Faults (MUT-1 to MUT-4)

On dense faults where every barrier of a class is suppressed (28%–50% failure density), uniform random (`B0`) reaches a first failure in a median of 1–3 crashes, whereas naive topological reachability (`B4`) requires 13–61 crashes. Because downstream reachability (`Desc`) is monotonically highest at the beginning of a trace, `B4` spends its initial budget before the first synchronous client write is acknowledged (event 57 of 2,427 in `main_mut1`), where rollback to the initial state is legal. Our root-cause analysis identified two structural reasons for this counterintuitive behavior:
1. **Monotonic Downstream Reachability Bias:** In a directed acyclic persistence graph, topological depth correlates inversely with downstream reachability. The earliest mutations reach the greatest total number of subsequent vertices ($|\text{Desc}(v)|$ is monotonically highest at the start of a trace). By scoring by downstream reachability (`Desc`), `B4` systematically prioritized candidates near the beginning of execution.
2. **Pre-Acknowledgment Semantics:** Storage engine crash-consistency contracts strictly apply *after* a client write has been acknowledged. In the initial portion of a workload trace (prior to the first acknowledged synchronous write at event 57 of 2,427 in `main_mut1`), any crash state legally rolls back to the initial database state without violating durability. `B4` spent its first 50 selections exploring exclusively inside this pre-acknowledgment region, where no consistency violations could legally exist.

### 3. State-Equivalence Sampling (R2) vs. Durability-Contract Priors (DCG) Across Dense and Sparse Faults (32-Seed Benchmark, MUT-0 to MUT-9)

To evaluate ranking strategies across both dense and sparse failure distributions, we ran a 32-seed benchmark across 10 fault models (`MUT-0` through `MUT-9`), evaluating over 11,000 crash states. Candidate universes are identical across all ranking strategies within each `(seed, fault)` execution, but universe sizes vary across fault models because `faults.FS` drops specific sync barriers prior to VFS trace recording. `R2` (`State-Representative Random`) is CraConTest's internal `StateKey` equivalence-class baseline—not a re-implementation of Pathfinder's full C++/LLVM syscall-function subgraph isomorphism and DPOR model checker—so comparisons against `R2` measure an intra-framework difference. DCG's indicators inspect the same missing barriers targeted by the synthetic sync-omission faults; $k_1 = 1.00$ on those faults therefore reflects by-construction alignment plus `StateKey` deduplication, and should not be interpreted as fault-class-independent prediction.

#### Table 1: Cost-to-First-Failure ($k_1$ Mean [95% CI])

The authoritative accounting-corrected table is in [the technical report](docs/report.md#32-seed-benchmark-summary-tables) and is generated from [the summary CSV](results/benchmark_summary.csv). The active source is `v1.1.2-evaluation-freeze` (`f9202dfca579`).

#### Table 2: Failing State Discovery ($S_{25}$ Early Budget / $S_{100}$ Exhaustive Budget Mean)

The corrected early/full-budget discovery values are reported alongside Table 1 in [the technical report](docs/report.md) and [the generated benchmark report](docs/benchmark-report.md).

#### Analysis

1. **POSIX Durability-Contract Priors Across Inode Data and Parent-Directory Entries:**
   Under the project's POSIX VFS model, durable file persistence requires flushing dirty file data and, after namespace mutation, the parent directory entry. CraConTest instantiates these priors for Pebble's recorded VFS categories by tracking `parentDir`, `dirEntrySynced`, dirty byte spans, and directory-scoped sync events. MUT-9 is dense (41.4% density) at $k=1$ and sparse (0.8%–12.4%) at $k \in \{7, 9, 10, 12\}$; directory-entry tracking was added after inspecting MUT-9, so these settings evaluate within-family parameter changes rather than a wholly unseen durability mechanism.

2. **Component Attribution & DCG-FlagOnly Ablation:**
   Comparing full DCG against the **DCG-FlagOnly** ablation isolates the exact contribution of each mechanism:
   - **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and $12.88$ on MUT-8). On dense faults (`MUT-1`, `MUT-2`, `MUT-4`), sparse faults (`MUT-5`, `MUT-8`), and directory faults (`MUT-9`), DCG-FlagOnly achieves the identical optimal $k_1 = 1.00$ [1.00, 1.00] as full DCG, confirming that the contract indicators (POSIX durability-contract priors instantiated for Pebble) are the primary driver of first-failure cost reduction.
   - **Continuous Contract Severity Scoring (`ContractFeatures.ContractScore`):** Breaks ties within Tier 1 when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop, ~0.8% density), where full DCG achieves mean $k_1 = 1.00$ compared to $1.19$ for `DCG-FlagOnly`.
   - **Pass 1 Category Interleaving & Pass 2 Violation Prioritization:** Pass 1 Tier 2 interleaves uncontracted barrier-adjacent candidates across write categories (`categoryOrder`), while Pass 2 Tier 3A prioritizes remaining violating non-representatives by contract tier, barrier proximity, and `ContractScore` after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

3. **$S_{25}$ Early-Budget Discovery & The ~40-State Equivalence Ceiling:**
   On the evaluated 120-operation traces, full-budget discovery often approaches the available `StateKey` ceiling: DCG and R2 match at $S_{100}=23.97$ on `MUT-1`, $33.00$ on `MUT-2`, $27.00$ on `MUT-3`, $21.69$ on `MUT-4`, and $27.00$ on `MUT-9`. On `MUT-8`, DCG has median/mean $k_1=1.00/1.00$, compared with R2 $3.41/3.42$ and B0 $6.49/6.49$. Mean $S_{25}$ is DCG/R2/B0 $12.88/6.73/2.12$, and $S_{100}$ is $12.88/12.80/5.19$. Early-budget coverage therefore exposes differences that later saturation obscures. DCG/R2/B0 mean $S_{25}$ is $23.94/17.14/6.34$ on `MUT-1` and $25.00/17.75/3.31$ on `MUT-9`. These are failing-state coverage comparisons at a fixed candidate budget, not wall-clock throughput gains.

### 4. Parameterized & Stochastic Fault Generalization (MUT-X1..X5) and Out-of-Contract Control (MUT-X6)

The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) families were reevaluated without retuning under `v1.1.2-evaluation-freeze`. Across 12 analyzed in-contract settings, DCG records 246 wins, 1 loss, and 48 ties versus R2 (Holm-adjusted sign $p\le0.006$); the loss is on stochastic `MUT-X4_p04`. MUT-X6 remains a non-significant negative control (21/38/4, Holm $p=1.0$).

Subsequently, `internal/strategy/dcg.go` and `internal/graph/features.go` were first frozen at Git tag **`v1.0-dcg-pr-freeze` (`67bbe7d`, 2026-11-02)** for the original chronological record of Evaluations 1–3 (Checkpoint, Direct L0 Ingest, and Workload Diversity), following the development and calibration of `DCG-PR` on Pebble `#5825` (§5.1). Commit `3c492bd` ("Harden crash replay, trace validation, and experiment accounting", 2026-11-19) later modified `internal/graph/features.go` and `internal/strategy/dcg.go` (238 insertions, 41 deletions) prior to `v1.1.2-evaluation-freeze` (`f9202dfca579`) for implementation and accounting corrections. Consequently, the active `v1.1.2-evaluation-freeze` campaign is an accounting-corrected reevaluation of those known settings rather than a new 0-byte-diff blind holdout:
1. **Evaluation 1 (Intra-Class Subsystem Transfer — checkpoint race later fixed by `15b500fb`, §5.2)**: Evaluated across 32 seeds (`MUT-0` vs. `BARRIER-CKPT`).
2. **Evaluation 2 (Cross-Channel Ordering — Direct L0 `db.Ingest` Cross-Channel Commit-Order Hole, §5.3)**: Evaluated across 32 seeds (`MUT-0` vs. pre-ingest WAL synchronization `BARRIER-INGEST`).
3. **Evaluation 3 (Multi-Distribution Workload Suite, §8)**: 5 workload presets evaluated across 16 seeds and 4 fault models.

The revised campaign evaluates settings already known from the earlier studies; it is not a newly blind holdout. The silent-write-loss control includes regressions versus R2 and B0, so its non-significant paired tests do not establish equivalence. Together with the by-construction sync-omission results, these observations limit the claim to the evaluated contract-visible mechanisms.

### 5. Storage Engine Case Studies (Pebble #5825, Checkpoint Directory Race & Direct L0 Ingest)

CraConTest has been evaluated across two directory-sync crash bugs in Pebble v2.1.7 (issue #5825 and the checkpoint race later fixed by `15b500fb`) and one cross-channel commit-order prefix divergence in Direct L0 `db.Ingest`:

1. **Pebble #5825 (Manifest Rotation / Multi-Entry Directory Race) & `BARRIER-5825`:**
   - Under 50% partial retention during manifest rotation, an unsynced directory entry for the new `MANIFEST-XXXXXX` file coexists with an unsynced removal of the old manifest, resulting in 58 failures across 25 of 32 failing seeds under unpatched `MUT-0` (deterministic partial-retention sampler).
   - The `BARRIER-5825` intervention adds a parent-directory sync before marker creation; no failures are observed across the 32 evaluated seeds.
   - DCG-PR promotes candidates with coexisting unsynced directory entries (`UnsyncedDirEntryCount >= 2`), discovering the bug at median/mean $k_1=2.00/12.40$ versus DCG 203.00/243.84 and accounting-corrected B0 178.73/192.72.

2. **Pebble Checkpoint Directory Race & `BARRIER-CKPT`:**
   - In Pebble's checkpoint subsystem, during `db.Checkpoint(destDir)`, Pebble creates hard-linked SSTables, `MANIFEST`, and `marker.manifest.*` inside `destDir` before directory synchronization. Under 50% partial retention this yields 53 open failures across 31 of 32 MUT-0 seeds in the accounting-corrected reevaluation.
   - Case Study 2 belongs to the same structural bug class as Case Study 1 (`#5825`) (multi-entry directory races in Pebble's `atomicfs` marker protocol), evaluating **intra-class subsystem transfer** of `dcg-pr` (from manifest rotation in `"db/"` to `db.Checkpoint` in `"checkpoint/"`), not generalization to an unseen bug class.
   - The `BARRIER-CKPT` intervention (`internal/faults/faults.go:695-707`, inside `maybeSyncParentDir`) adds destination-directory synchronization; no checkpoint recovery failures are observed across the 32 evaluated seeds.
   - This is intra-class transfer within Pebble's marker protocol, not a blind unseen bug class. DCG-PR records median/mean $k_1=245.00/156.39$ versus DCG 288.00/258.65, R2 280.41/271.55, and corrected B0 262.26/238.53; DCG-PR/R2 paired W/L/T is 22/9/0.

3. **Direct L0 `db.Ingest` Cross-Channel Prefix Hole & `BARRIER-INGEST`:**
   - When external SSTables do not overlap active memtables, Pebble bypasses the WAL (`asFlushable = false`), allocating sequence number $s_2 > s_1$ and committing via `MANIFEST` sync (`vs.manifestFile.Sync()`) while earlier un-synced WAL writes at $s_1$ remain unpersisted. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1 < s_2$ is discarded from the unsynced WAL. Under endpoint (`0%/100%`) retention, **22 of 32 seeds** exhibit prefix violations (276 total failures; seeds `3, 5, 7, 8, 9, 16, 20, 23, 26, 29` have 0 failures because randomly generated ingest keys overlapped the active memtable and took the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`).
   - RocksDB's `db.h` notes that `IngestExternalFile` does not imply `FlushWAL(true)`, and under a per-operation synced-only durability oracle (which permits individual unacknowledged NoSync writes to be lost independently), dropping $s_1$ while retaining $s_2$ is permissible. Direct L0 `db.Ingest` is an **undocumented cross-channel ordering behavior between unsynced `NoSync` WAL writes and synced `MANIFEST` ingests under a global prefix ("no-hole") oracle (not a loss of synced data and not reported upstream as a Pebble bug, as Pebble's contract guarantees durability only for synced operations)**.
   - The `BARRIER-INGEST` intervention (`db.LogData(nil, pebble.Sync)`) flushes the active WAL before MANIFEST version edit; no prefix violations are observed across the 32 evaluated seeds ($S_{25}=0.00, S_{100}=0.00$).
   - DCG/DCG-PR median/mean $k_1$ is 9.00/15.32, R2 is 26.77/27.00, and corrected B0 is 33.74/34.32. The paired DCG-PR/R2 sign test is not significant at 0.05 (13/6/3, $p=0.0835$).

### 6. Candidate Universe Scaling (MUT-5, 120..1200 ops) & Multi-Shape Null Control (108 Runs)

Across four evaluated workload sizes (`MUT-5`, 120–1,200 operations per seed), candidate universes grow from approximately 338 to 2,958 states while `DCG` retains median and mean $k_1 = 1.00$; measured planning time at 1,200 ops averages 38.5 ms (max 42 ms). The baseline discovery positions increase over this range. The 108-run multi-shape null control produces no observed failures over >50,000 crash states, but its fixed workloads and dependent candidates do not support an independent-trial population bound.

### 7. Pebble Subsystem Audit & Empirical Crash-Consistency Evaluation (MUT-0)

To examine selected Pebble v2.1.7 subsystems that perform multi-file and directory-marker mutations under unfaulted conditions (`MUT-0`), we conducted a targeted source audit and empirical discovery campaign across ingestion, format ratcheting, and checkpoints:

1. **Source-Level Audit of Pebble v2.1.7 (`github.com/cockroachdb/pebble/v2`):**
   - **External SSTable Ingestion (`ingest.go`):** In `ingest.go:1490`, `d.objProvider.Sync()` unconditionally syncs `dataDir` prior to sequence number allocation and branching. Direct L0 ingest commits via MANIFEST sync (`vs.manifestFile.Sync()` at `version_set.go:649`) without WAL writes. Flushable ingest commits via synchronous WAL fsync (`commit.directWrite(b)` at `commit.go:289` / `ingest.go:1367`) without MANIFEST writes.
   - **Live Format-Major-Version Ratcheting (`format_major_version.go`):** `RatchetFormatMajorVersion` (`format_major_version.go:436–480`) calls `finalizeFormatVersUpgrade` (`line 486`), which transitions `marker.format-version` via `atomicfs.Marker.Move` (`line 495`). Zero MANIFEST edits are logged; directory sync occurs synchronously inside `Move`.
   - **AtomicFS Marker Dynamics (`vfs/atomicfs/marker.go`):** `Move` executes `Create(new)` -> `f.Close()` -> `Remove(old)` -> `dirFD.Sync()`. Under this project's partial-retention model (`deterministicPartialClone`), which defines a sorted entry-and-4KB-block Bernoulli partial-retention model (whose marginal retention rates are checked in `TestDeterministicPartialCloneStatisticalFidelity`) rather than calling Pebble's `MemFS.CrashClone`, `oldMarker` is retained 100% of the time, and `newMarker` 50% of the time; `LocateMarker` deterministically selects the latest valid iteration index.
   - **Checkpoint Subsystem Durability (`checkpoint.go`):** Performs directory syncs at lines 287 (OPTIONS and format marker), 596 (SSTable hard links, MANIFEST, and manifest marker), and 448 (WAL files). The partial-retention experiments evaluate intermediate states before these barriers complete.

2. **Workload Drivers & CLI Flags:**
   - Extended `internal/workload/workload.go` with `IngestEvery` (staged SSTable ingestion with disjoint key ranges) and `RatchetAt` (live format ratcheting). Exposed flags `-ingest-every` and `-ratchet-at` on CLI `record` and `benchmark`.
   - Ingested mutations are recorded with `Mutating: true` and `Sync: true` in the append-only history ledger, verified by the prefix oracle.

3. **Systematic Unfaulted Discovery Results (`results/subsystem_discovery.csv`):**
   - Dedicated discovery test `TestSubsystemDiscovery` evaluated Workload A (IngestEvery: 25), Workload B (RatchetAt: 50), and Workload C (Combined Stress: IngestEvery: 30, RatchetAt: 40, CheckpointAt: 60, MaxManifestFileSize: 1) across seeds `42, 100, 777` under `MUT-0` at retention policies `[0, 50, 100]` (5,415 candidate states).
   - **0 lost durably-committed keys** and **0 engine recovery panics** across all workloads and seeds, with no durably-committed data loss observed in these workloads.
   - **Workload A (External SSTable Ingestion):** 0 durably-committed violations and 3 partial-retention open errors (0 on Seed 42, 2 on Seed 100, 1 on Seed 777). 40 prefix violations across seeds 42 (15) and 777 (25) reveal a cross-channel commit-order hole on direct L0 ingest: when `db.Ingest` takes the non-overlapping Direct L0 shortcut (`asFlushable = false`), Pebble allocates sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1$ is lost from the unsynced `WAL`, creating a cross-channel sequence hole (whereas overlapping flushable ingest syncs the shared WAL at `commit.go:289` and preserves strict prefix order).
   - **Workload B (Live Format Ratcheting):** 0 data-loss violations and 0 panics across all policies; 3 open errors under 50% retention (0 on Seed 42, 2 on Seed 100, 1 on Seed 777) occurring exclusively during initial database creation prior to operation 1 (creating `"db/MANIFEST-000001"` and `"db/marker.manifest.000001"` prior to initial directory sync); `RatchetFormatMajorVersion` at operation 50 causes 0 open errors and 0 violations.
   - **Workload C (Combined Lifecycle Stress):** Partial-retention open errors (15 total: 5 on Seed 42, 7 on Seed 100, 3 on Seed 777) during manifest rotation and checkpointing prior to directory sync; zero errors under 0% and 100% retention.
   - **Verdict:** Across all 5,415 candidate states in Workloads A, B, and C under `MUT-0`, Pebble v2.1.7 exhibits **0 `durably_committed` data-loss violations and 0 recovery panics**, with no durably-committed data loss observed in these workloads. Simultaneously, the audit exposed two distinct recovery hazards under `MUT-0`: (1) an **input-dependent cross-channel `prefix` monotonicity hole** during non-overlapping Direct L0 `db.Ingest` (`40` prefix violations in Workload A, analyzed across 32 seeds in §5.3), and (2) **50%-retention intra-operation directory races (`open_error`)** when a crash occurs inside the multi-entry window between `MANIFEST-*`/`.sst` creation and `dirFD.Sync()` (21 total open errors).

### 8. Multi-Distribution Workload Diversity Suite

To evaluate robustness and generalization across non-uniform key-value access distributions, CraConTest provides 5 distinct workload presets configured via `-preset` (`internal/workload/workload.go`; the key counts below are the calibrated preset defaults when `KeyCount == 0`, as used by the diversity benchmark, whereas CLI `record`/`benchmark` inherit `DefaultConfig()` `KeyCount = 64` or `DefaultMultiSeedBenchmarkConfig()` `KeyCount = 16` unless `key_count: 0` is set in `-spec`):
1. `ycsb-a`: 50% update (`set`), 50% point read (`get`) over 8 hot keys (`keyCount = 8`) with periodic sync/flush.
2. `ycsb-b`: 75% point read (`get`), 25% update (`set`, 3 reads per write operation via `i%4 == 3`, adapted from YCSB-B for 25-operation traces so multiple sync cycles occur per run) over 12 keys (`keyCount = 12`) with periodic sync/flush.
3. `ycsb-f`: 50% standalone point read (`get`, even steps) and 50% read-modify-write two-key atomic batch transaction (odd steps: reading 2 keys via `Get` and updating 2 distinct keys atomically via `Batch`) over 8 keys (`keyCount = 8`) with periodic sync/flush.
4. `overwrite-heavy`: 100% overwrite (`set`) across 4 hot keys (`keyCount = 4`), driving deep per-key version chains and frequent memtable flush pressure.
5. `delete-heavy`: Equal 1/3 cycling (`i%3`) of point `set`, point `delete`, and `range_delete` (`DeleteRange`) over 8 keys (`keyCount = 8`) with periodic checkpoints (`checkpointAt = 10`).

Evaluated across 16 seeds (`seed = 1..16`, 25 ops) and 4 fault models (`MUT-0`, `MUT-1`, `MUT-5`, `MUT-9`) across 5 ranking strategies (`results/workload_diversity_summary.csv`):
- **Null Control Integrity:** Unfaulted `MUT-0` produces strictly **0 false alarms** across all 16 seeds for all 5 presets.
- **Sparse-Fault Candidate Cost:** Corrected mean $k_1$ for DCG/R2/B0 is 1.00/8.49/39.71 on `ycsb-f/MUT-5`, 1.00/9.40/22.99 on `overwrite-heavy/MUT-5`, and 1.00/8.27/27.11 on `ycsb-a/MUT-5`. These are candidate-count comparisons, not wall-clock speedups.
- **Delete-Heavy Dynamics:** DCG/R2/B0 mean $k_1$ is 3.00/2.37/6.82 on `MUT-1`, 20.54/35.59/57.16 on `MUT-5`, and 3.00/4.44/10.88 on `MUT-9`. The MUT-1 R2 regression is retained; DCG's $S_{25}=14.19$ exceeds R2/B0 10.09/3.46.
- **Early-Budget Discovery ($S_{25}$):** Across 14 failing settings, DCG has lower mean $k_1$ than corrected B0 on 14 and R2 on 13, with one `delete-heavy/MUT-1` regression. DCG $S_{25}$ is at least R2 on all 14 (9 strict improvements and 5 ties). Because these traces use 25 operations, $B=25$ approaches full state-representative coverage for several presets.

---

## Testing & CI

Run the full test suite and code linters:

```bash
# Linux / macOS
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
```

```powershell
# Windows PowerShell
$env:CGO_ENABLED="0"; go vet ./...; go test -count=1 ./...
```

Continuous integration runs automatically on GitHub Actions (`.github/workflows/ci.yml`).

---

## License

This project is licensed under the Apache 2.0 License.
