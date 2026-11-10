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
   - **RHG / B7 (Reachability-Hazard Guided):** Ranks 0%-retention state representatives by uncommitted hazard reachability, barrier proximity, and semantic tier.
   - **DCG / B8 (Durability-Contract Guided) & DCG-FlagOnly (Ablation):** Trace-derived durability-contract analysis that prioritizes 0%-retention state representatives that exhibit structural POSIX durability-contract violations (`ContractScore > 0`) ahead of unflagged representatives, alongside a randomized flag-only ablation (`DCG-FlagOnly`) that isolates the effect of the contract indicators (POSIX durability-contract priors instantiated for Pebble). DCG-FlagOnly ties full DCG on nearly every setting, showing that the binary durability-contract flags combined with StateKey equivalence partitioning drive the speedup.
   - **DCG-PR (Durability-Contract Guided with Partial-Retention Prior):** A post-hoc variant of DCG that inspects `ContractFeatures.UnsyncedDirEntryCount` and promotes 50%-retention candidates (with ≥ 2 unsynced directory entries coexisting in the same parent) into Tier 1, ahead of the standard 0%-retention sweep. Achieves median $k_1 = 1.00$ on all 12 testable in-contract holdout fault settings and median $k_1 = \mathbf{2.00}$ on Pebble `#5825` and median $k_1 = \mathbf{3.00}$ on checkpoint commit `15b500fb`.

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
│   ├── paired_stats.py      # Paired per-seed Wilcoxon & t-test statistical analysis
│   └── requirements.txt     # Python dependencies (scipy)
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
│   ├── faults/              # Seeded durability faults (MUT-0 to MUT-9)
│   ├── graph/               # Persistence DAG, state view, and durability-contract features
│   ├── history/             # Mutation history tracking
│   ├── oracle/              # Crash consistency validation oracle
│   ├── recovery/            # Crash recovery harness and integrity verification
│   ├── replay/              # VFS trace replay engine
│   ├── strategy/            # Ranking strategies (B0–B6, R1, R2, RHG, DCG, DCG-FlagOnly)
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
│   ├── casestudy_checkpoint_summary.csv # Checkpoint 15b500fb A/B benchmark summary
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

- Go 1.27+ (`go.mod` specifies Go 1.27.1).
- OS: Linux (Ubuntu/ext4) or Windows (NTFS). On Windows, build with `CGO_ENABLED=0`.

### Building the CLI

```bash
go build -o cracontest ./cmd/cracontest
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

The curves are written to `/tmp/smoke_mut1/curves.csv`. The main 1,000-operation
experiment uses `experiments/configs/main.json`; see [docs/reproduce.md](docs/reproduce.md).

### Running the Multi-Seed Benchmark

Run the full 32-seed benchmark across all fault models (`MUT-0` through `MUT-9`):

```bash
./cracontest benchmark -report docs/benchmark-report.md -csv results/benchmark_summary.csv -per-seed-csv results/benchmark_per_seed.csv
```

Run the paired statistical comparison across seeds via python3:

```bash
python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9
```

---

## Experimental Findings

The experimental evaluation comprises both single-seed 1,000-operation runs and a 32-seed multi-fault benchmark on Pebble v2.1.7. Full analysis is available in [docs/report.md](docs/report.md) and [docs/benchmark-report.md](docs/benchmark-report.md); raw data is stored under `results/`.

### 1. Oracle Soundness (MUT-0)

Across all 2,150 candidates of the 1,000-operation main run, all 302 candidates of the 100-operation smoke run, and all 32 workload seeds of the multi-seed benchmark (>11,000 evaluated crash states), zero false alarms were observed on unfaulted runs (`MUT-0`). Every crash candidate recovered to a state that strictly satisfied the durability prefix and atomicity invariants, with 0 false alarms across all 32 seeds at 0%/100% retention. Confirmed also across a 108-run Multi-Shape Null-Control suite spanning 12 workload geometries (>50,000 evaluated crash states, 0.00% false-alarm rate). We note explicitly that this 0.00% false-alarm rate is scoped strictly to non-ingest workloads evaluated under endpoint (0%/100%) retention; enabling `db.Ingest` under the global prefix oracle or enabling 50% partial retention surfaces cross-channel prefix divergence and `atomicfs` directory races.

### 2. Why Naive Topological Reachability (B4) Underperforms Random on Dense Faults (MUT-1 to MUT-4)

On dense faults where every barrier of a class is suppressed (28%–50% failure density), uniform random (`B0`) reaches a first failure in a median of 1–3 crashes, whereas naive topological reachability (`B4`) requires 13–61 crashes. Because downstream reachability (`Desc`) is monotonically highest at the beginning of a trace, `B4` spends its initial budget before the first synchronous client write is acknowledged (event 57 of 2,427 in `main_mut1`), where rollback to the initial state is legal. Our root-cause analysis identified two structural reasons for this counterintuitive behavior:
1. **Monotonic Downstream Reachability Bias:** In a directed acyclic persistence graph, topological depth correlates inversely with downstream reachability. The earliest mutations reach the greatest total number of subsequent vertices ($|\text{Desc}(v)|$ is monotonically highest at the start of a trace). By scoring by downstream reachability (`Desc`), `B4` systematically prioritized candidates near the beginning of execution.
2. **Pre-Acknowledgment Semantics:** Storage engine crash-consistency contracts strictly apply *after* a client write has been acknowledged. In the initial portion of a workload trace (prior to the first acknowledged synchronous write at event 57 of 2,427 in `main_mut1`), any crash state legally rolls back to the initial database state without violating durability. `B4` spent its first 50 selections exploring exclusively inside this pre-acknowledgment region, where no consistency violations could legally exist.

### 3. State-Equivalence Sampling (R2) vs. Durability-Contract Priors (DCG) Across Dense and Sparse Faults (32-Seed Benchmark, MUT-0 to MUT-9)

To evaluate ranking strategies across both dense and sparse failure distributions, we ran a 32-seed benchmark across 10 fault models (`MUT-0` through `MUT-9`), evaluating over 11,000 crash states. Candidate universes are identical across all ranking strategies within each `(seed, fault)` execution, but universe sizes vary across fault models because `faults.FS` drops specific sync barriers prior to VFS trace recording. Note that `R2` (`State-Representative Random`) is CraConTest's internal `StateKey` equivalence-class baseline—**not** a re-implementation of Pathfinder's full C++/LLVM syscall-function subgraph isomorphism and DPOR model checker—so comparisons against `R2` measure the intra-framework gain of adding POSIX durability-contract priors onto `StateKey` partitioning rather than a head-to-head comparison against the Pathfinder tool. Because `DCG`'s 0%-retention indicators inspect the VFS trace for missing `Sync`/`SyncData` barriers after client acknowledgment or metadata updates, achieving $k_1 = 1.00$ on synthetic sync-omission faults reflects exact alignment between trace-level durability-contract checking and sync-omission fault injection combined with `StateKey` equivalence deduplication.

#### Table 1: Cost-to-First-Failure ($k_1$ Mean [95% CI])

| Fault Model | Fault Description | Failure Density | DCG | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|---|---|
| **MUT-0** | Null Control (No Fault) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.44 [1.42, 1.46] | 1.37 [1.34, 1.39] | 5.00 [5.00, 5.00] | 17.00 [17.00, 17.00] | 2.00 [2.00, 2.00] |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.17 [1.16, 1.19] | 1.04 [1.04, 1.05] | **1.00** [1.00, 1.00] | 5.00 [5.00, 5.00] | 1.55 [1.38, 1.71] |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** [1.00, 1.00] | 1.03 [1.03, 1.04] | **1.00** [1.00, 1.00] | 1.44 [1.40, 1.48] | 1.17 [1.15, 1.19] | **1.00** [1.00, 1.00] | 9.00 [9.00, 9.00] | 2.00 [2.00, 2.00] |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.95 [1.90, 2.01] | 1.67 [1.61, 1.74] | **1.00** [1.00, 1.00] | 31.62 [31.24, 32.01] | 2.39 [2.22, 2.56] |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 13.01 [12.68, 13.34] | 17.04 [16.54, 17.53] | 262.25 [258.20, 266.30] | 17.00 [17.00, 17.00] | 25.97 [25.00, 26.94] |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** [1.00, 1.00] | 1.20 [1.17, 1.24] | 14.08 [13.50, 14.65] | 7.72 [7.46, 7.97] | 20.79 [19.89, 21.70] | 101.00 [97.37, 104.63] | 43.15 [42.38, 43.93] | 31.12 [29.02, 33.21] |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 3.58 [3.50, 3.66] | 3.26 [3.12, 3.40] | **1.00** [1.00, 1.00] | 55.25 [54.85, 55.65] | 4.41 [4.23, 4.58] |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.40 [1.36, 1.44] | 1.21 [1.18, 1.23] | **1.00** [1.00, 1.00] | 17.00 [17.00, 17.00] | 1.95 [1.90, 2.01] |

#### Table 2: Failing State Discovery ($S_{25}$ Early Budget / $S_{100}$ Exhaustive Budget Mean)

| Fault Model | DCG ($S_{25}$ / $S_{100}$) | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|
| **MUT-1** | **23.94** / **23.97** | **23.94** / **23.97** | 17.69 / **23.97** | 17.12 / **23.97** | 10.02 / 19.80 | 6.53 / 8.94 | 5.00 / 13.00 | 6.12 / 14.61 |
| **MUT-2** | **25.00** / **33.00** | **25.00** / **33.00** | 24.00 / **33.00** | 21.14 / **33.00** | 8.67 / 22.79 | 7.00 / 11.34 | 11.00 / 30.00 | 5.64 / 13.75 |
| **MUT-3** | 24.00 / **27.00** | **24.12** / **27.00** | 21.03 / **27.00** | 17.26 / **27.00** | 7.27 / 19.00 | 7.16 / 12.06 | 9.00 / 21.00 | 5.00 / 11.22 |
| **MUT-4** | **21.69** / **21.69** | **21.69** / **21.69** | 16.69 / **21.69** | 12.57 / **21.69** | 5.44 / 14.51 | 8.00 / 12.25 | 0.00 / 16.69 | 3.53 / 8.59 |
| **MUT-5** | **2.00** / **2.00** | **2.00** / **2.00** | 1.81 / **2.00** | 1.30 / **2.00** | 0.87 / 1.60 | 0.00 / 0.00 | 1.00 / 1.00 | 0.48 / 1.00 |
| **MUT-6** | **1.62** / **1.62** | **1.62** / **1.62** | 1.22 / **1.62** | 1.07 / **1.62** | 0.39 / 1.20 | 0.00 / 0.41 | 0.00 / 1.22 | 0.00 / 0.81 |
| **MUT-7** | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | **12.88** / **12.88** | **12.88** / **12.88** | 7.88 / **12.88** | 6.74 / 12.82 | 3.27 / 8.41 | 8.00 / 11.75 | 0.00 / 9.88 | 2.00 / 5.05 |
| **MUT-9** | **25.00** / **27.00** | **25.00** / **27.00** | 20.00 / **27.00** | 17.78 / **27.00** | 5.57 / 17.44 | 7.00 / 10.94 | 5.00 / 26.00 | 3.03 / 9.83 |

#### Analysis

1. **POSIX Durability-Contract Priors Across Inode Data and Parent-Directory Entries:**
   Under POSIX filesystem semantics, ensuring durable file persistence after crash requires two conditions: (1) flushing dirty file data (`fsync` / `fdatasync`), and (2) flushing the parent directory entry (`fsync(parentDir)`) after file creation, renaming, hard-linking, or directory creation (`Create`, `ReuseForWrite`, `Rename`, `Link`, `MkdirAll`). If a file is written and its data synced, but its parent directory entry remains unsynced, a crash at 0% retention causes the directory entry to vanish, resulting in lost files upon recovery. CraConTest implements universal POSIX durability priors without engine-specific heuristics: tracking `parentDir` and `dirEntrySynced` for every filesystem node, enforcing that durability holds if and only if `dirtyBytes == 0 && dirEntrySynced == true`, and scoping directory sync events strictly to the target directory path. MUT-9 is dense (41.4% density) at $k=1$ (initial directory sync) and sparse (0.8%–12.4%) at $k \in \{7, 9, 10, 12\}$ (flush/compaction directory syncs); directory-entry tracking was added after inspecting MUT-9, so MUT-9 evaluates parameterized $k$-generalization within the directory-sync family. On MUT-9 ($k=1$), DCG identifies the uncommitted directory entry prior, expanding state coverage to $S_{100} = 27.00$ (vs B0's $9.83$, a 2.7x expansion) and $S_{25} = 25.00$ (vs B0's $3.03$, an 8.2x early-budget advantage).

2. **Component Attribution & DCG-FlagOnly Ablation:**
   Comparing full DCG against the **DCG-FlagOnly** ablation isolates the exact contribution of each mechanism:
   - **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and $12.88$ on MUT-8). On dense faults (`MUT-1`, `MUT-2`, `MUT-4`), sparse faults (`MUT-5`, `MUT-8`), and directory faults (`MUT-9`), DCG-FlagOnly achieves the identical optimal $k_1 = 1.00$ [1.00, 1.00] as full DCG, confirming that the contract indicators (POSIX durability-contract priors instantiated for Pebble) are the primary driver of first-failure cost reduction.
   - **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop, ~0.8% density), where full DCG achieves mean $k_1 = 1.00$ compared to $1.20$ for `DCG-FlagOnly`.
   - **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

3. **$S_{25}$ Early-Budget Discovery & The ~40-State Equivalence Ceiling:**
   In a 120-operation workload, there are only ~35–40 distinct canonical `StateKey` persistence states under 0%-retention crashes. Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100 (reaching identical $S_{100} = 23.97$ on `MUT-1`, $33.00$ on `MUT-2`, $27.00$ on `MUT-3`, $21.69$ on `MUT-4`, and $27.00$ on `MUT-9`). Under MUT-8 (compaction-triggered SST sync drop, 15.2% density), DCG reaches the first failing state at median $k_1 = 1.0$ (mean 1.00) compared to R2's median $k_1 = 3.6$ (mean 3.58) and B0's median $k_1 = 4.0$ (mean 4.41), achieves early-budget $S_{25} = 12.88$ (vs R2's $6.74$ and B0's $2.00$), and matches R2 at full budget ($S_{100} = 12.88$ for DCG vs $12.82$ for R2, compared to $5.05$ for B0). Therefore, early-budget coverage $S_{25}$ ($B = 25$) is the critical discriminant metric for practical CI testing: DCG achieves near-full state discovery saturation within the first 25 tests (e.g. $S_{25} = 23.94$ vs R2's $17.12$ and B0's $6.12$ on `MUT-1`; $S_{25} = 12.88$ vs R2's $6.74$ and B0's $2.00$ on `MUT-8`; $S_{25} = 25.00$ vs R2's $17.78$ and B0's $3.03$ on `MUT-9`), yielding a 2x–8x throughput advantage over random selection.

### 4. Parameterized & Stochastic Fault Generalization (MUT-X1..X5) and Out-of-Contract Control (MUT-X6)

The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault families were committed to `internal/faults/faults.go` on `2026-11-01` (`c67a44a`, prior to tag `v1.0-dcg-pr-freeze`) to evaluate whether the four binary POSIX durability-contract flags generalize across arbitrary sync-drop indices $k$ and drop probabilities $p$ without re-tuning (`287 wins / 0 losses / 43 ties` vs. `R2`, Holm $p \le 0.002$).

Subsequently, `internal/strategy/dcg.go` and `internal/graph/features.go` were frozen at Git tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**, following the development and calibration of the partial-retention prior `DCG-PR` on Pebble `#5825` (§5.1). **Three strictly post-freeze evaluations** were designed and executed with 0 bytes of modification to the ranking or feature code:
1. **Post-Freeze Evaluation 1 (Intra-Class Subsystem Transfer — Checkpoint `15b500fb`, §5.2)**: Evaluated across 32 seeds (`MUT-0` vs. path-pattern barrier control `BARRIER-CKPT`).
2. **Post-Freeze Evaluation 2 (Cross-Channel Ordering — Direct L0 `db.Ingest` Cross-Channel Commit-Order Hole, §5.3)**: Evaluated across 32 seeds (`MUT-0` vs. path-pattern barrier control `BARRIER-INGEST`).
3. **Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §8)**: 5 workload presets evaluated across 16 seeds and 4 fault models.

Across all 12 testable in-contract holdout settings (`MUT-X1..X5`), `DCG`/`DCG-PR` achieves median $k_1 = 1.00$, winning 287/287 non-tied comparisons against `R2` (0 losses, Holm-adjusted sign-test $p \le 0.002$, mean speedup $2.0\times$–$19.9\times$ across 100-shuffle means ($2.2\times$–$19.1\times$ on single-seed rankings)). On the 3 out-of-contract settings (`MUT-X6`), performance degrades gracefully to the `R2` baseline (Holm-adjusted $p = 1.0$, not significant) — confirming the contract boundary is correctly defined.

### 5. Storage Engine Case Studies (Pebble #5825, Checkpoint 15b500fb & Direct L0 Ingest)

CraConTest has been evaluated across two real-world directory-sync crash bugs (#5825 and Checkpoint 15b500fb) and one cross-channel commit-order prefix divergence (Direct L0 db.Ingest) in CockroachDB's Pebble v2.1.7:

1. **Pebble #5825 (Manifest Rotation / Multi-Entry Directory Race) & `BARRIER-5825`:**
   - Under 50% partial retention during manifest rotation, an unsynced directory entry for the new `MANIFEST-XXXXXX` file coexists with an unsynced removal of the old manifest, resulting in 58 failures across 25 of 32 failing seeds under unpatched `MUT-0` (deterministic partial-retention sampler).
   - Causal barrier control `BARRIER-5825` injects a parent directory sync immediately following manifest rotation, eliminating all failures (0/32 seeds).
   - Pre-frozen `DCG-PR` promotes candidates with coexisting unsynced directory entries (`UnsyncedDirEntryCount >= 2`), discovering the bug at median $k_1 = \mathbf{2.00}$ (mean $12.40$, $S_{25} = 1.44$, $S_{100} = 1.44$) vs $211.00$ for 0%-first `DCG` (mean $301.60$) and $148.00$ for `B0` (mean $180.74$).

2. **Pebble Checkpoint Commit `15b500fb` (Checkpoint Directory Sync) & `BARRIER-CKPT`:**
   - In Pebble's checkpoint subsystem, during `db.Checkpoint(destDir)`, Pebble creates hard-linked SSTables, `MANIFEST`, and `marker.manifest.*` inside `destDir` before calling the directory sync inside `manifestMarker.Move` (`checkpoint.go:596`). When a crash occurs *inside that multi-entry window prior to `dirFD.Sync()`* under 50% partial retention, `marker.manifest.*` can survive while the newly created `MANIFEST-*` or hard-linked `.sst` entry in the same directory is dropped, causing reopening to fail with `open_error` (58 failures across 30 of 32 seeds under `MUT-0`).
   - Case Study 2 belongs to the same structural bug class as Case Study 1 (`#5825`) (multi-entry directory races in Pebble's `atomicfs` marker protocol), evaluating **intra-class subsystem transfer** of `dcg-pr` (from manifest rotation in `"db/"` to `db.Checkpoint` in `"checkpoint/"`), not generalization to an unseen bug class.
   - Causal barrier control `BARRIER-CKPT` (`internal/faults/faults.go:602-625`) forces destination directory synchronization across both `"db/"` and `"checkpoint/"`, reducing checkpoint recovery failures to strictly 0 across all 32 seeds.
   - Evaluated post-freeze on pre-frozen `DCG-PR` (intra-class subsystem transfer within Pebble's atomicfs marker protocol; because BARRIER-CKPT and dcg-pr share the same directory-entry mechanism as #5825, this tests subsystem transfer rather than a blind unseen bug class), `DCG-PR` discovers the first failure at median $k_1 = \mathbf{3.00}$ (mean $140.03$) vs $386.50$ for 0%-first `DCG` (mean $319.20$), $328.40$ for `R2` (mean $336.53$), and $172.00$ for `B0` (mean $190.73$).

3. **Direct L0 `db.Ingest` Cross-Channel Prefix Hole & `BARRIER-INGEST`:**
   - When external SSTables do not overlap active memtables, Pebble bypasses the WAL (`asFlushable = false`), allocating sequence number $s_2 > s_1$ and committing via `MANIFEST` sync (`vs.manifestFile.Sync()`) while earlier un-synced WAL writes at $s_1$ remain unpersisted. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1 < s_2$ is discarded from the unsynced WAL. Under endpoint (`0%/100%`) retention, **22 of 32 seeds** exhibit prefix violations (276 total failures; seeds `3, 5, 7, 8, 9, 16, 20, 23, 26, 29` have 0 failures because randomly generated ingest keys overlapped the active memtable and took the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`).
   - RocksDB's `db.h` notes that `IngestExternalFile` does not imply `FlushWAL(true)`, and under a per-operation synced-only durability oracle (which permits individual unacknowledged NoSync writes to be lost independently), dropping $s_1$ while retaining $s_2$ is permissible. Direct L0 `db.Ingest` is an **undocumented cross-channel ordering behavior between unsynced `NoSync` WAL writes and synced `MANIFEST` ingests under a global prefix ("no-hole") oracle (not a loss of synced data and not reported upstream as a Pebble bug, as Pebble's contract guarantees durability only for synced operations)**.
   - Causal remedy `BARRIER-INGEST` (`db.LogData(nil, pebble.Sync)`) flushes the active WAL before MANIFEST version edit, eliminating prefix violations to strictly 0 across all 32 seeds (**0 of 32 seeds** fail, $S_{25}=0.00, S_{100}=0.00$).
   - Pre-frozen `DCG-PR` achieves median $k_1 = \mathbf{9.00}$ (mean $15.32$) and `DCG` achieves $9.00$ (mean $15.32$), compared to $27.88$ (mean $27.64$) for `R2`, $28.60$ (mean $30.61$) for `R1`, $28.18$ (mean $29.88$) for `DCG Flag-Only`, $81.00$ (mean $80.18$) for `sync-boundary`, and $21.50$ (mean $24.73$) for `B0` across 276 total failures on 22 failing seeds.

### 6. Candidate Universe Scaling (MUT-5, 120..1200 ops) & Multi-Shape Null Control (108 Runs)

As the candidate universe grows from ~338 to ~2,958 states (`MUT-5`, 120–1200 ops/seed), `DCG` maintains $k_1 = 1.00$ (median) with $\le 63.0$ ms planning time, while `R2`, `R1`, and `B0` scale linearly in universe size. Across the 108-run multi-shape null-control suite (>50,000 crash states), the harness exhibits 0 false alarms (Rule-of-Three 95% upper bound $\le 2.78\%$ per run, $\le 0.006\%$ across $>50{,}000$ candidates).

### 7. Pebble Subsystem Audit & Empirical Crash-Consistency Evaluation (MUT-0)

To establish whether uninvestigated Pebble v2.1.7 subsystems performing multi-file and directory-marker mutations exhibit crash-consistency vulnerabilities under unfaulted conditions (`MUT-0`), we conducted an exhaustive source-level audit and empirical discovery campaign across three subsystems:

1. **Source-Level Audit of Pebble v2.1.7 (`github.com/cockroachdb/pebble/v2`):**
   - **External SSTable Ingestion (`ingest.go`):** In `ingest.go:1490`, `d.objProvider.Sync()` unconditionally syncs `dataDir` prior to sequence number allocation and branching. Direct L0 ingest commits via MANIFEST sync (`vs.manifestFile.Sync()` at `version_set.go:649`) without WAL writes. Flushable ingest commits via synchronous WAL fsync (`commit.directWrite(b)` at `commit.go:289` / `ingest.go:1367`) without MANIFEST writes.
   - **Live Format-Major-Version Ratcheting (`format_major_version.go`):** `RatchetFormatMajorVersion` (`format_major_version.go:436–480`) calls `finalizeFormatVersUpgrade` (`line 486`), which transitions `marker.format-version` via `atomicfs.Marker.Move` (`line 495`). Zero MANIFEST edits are logged; directory sync occurs synchronously inside `Move`.
   - **AtomicFS Marker Dynamics (`vfs/atomicfs/marker.go`):** `Move` executes `Create(new)` -> `f.Close()` -> `Remove(old)` -> `dirFD.Sync()`. Under this project's partial-retention model (`deterministicPartialClone`), which defines a sorted entry-and-4KB-block Bernoulli partial-retention model (whose marginal retention rates are checked in `TestDeterministicPartialCloneStatisticalFidelity`) rather than calling Pebble's `MemFS.CrashClone`, `oldMarker` is retained 100% of the time, and `newMarker` 50% of the time; `LocateMarker` deterministically selects the latest valid iteration index.
   - **Checkpoint Subsystem Durability (`checkpoint.go`):** Enforces three directory syncs at lines 287 (OPTIONS and format marker), 596 (SSTable hard links, MANIFEST, and manifest marker), and 448 (WAL files), guaranteeing atomic crash consistency.

2. **Workload Drivers & CLI Flags:**
   - Extended `internal/workload/workload.go` with `IngestEvery` (staged SSTable ingestion with disjoint key ranges) and `RatchetAt` (live format ratcheting). Exposed flags `-ingest-every` and `-ratchet-at` on CLI `record` and `benchmark`.
   - Ingested mutations are recorded with `Mutating: true` and `Sync: true` in the append-only history ledger, verified by the prefix oracle.

3. **Systematic Unfaulted Discovery Results (`results/subsystem_discovery.csv`):**
   - Dedicated discovery test `TestSubsystemDiscovery` evaluated Workload A (IngestEvery: 25), Workload B (RatchetAt: 50), and Workload C (Combined Stress: IngestEvery: 30, RatchetAt: 40, CheckpointAt: 60, MaxManifestFileSize: 1) across seeds `42, 100, 777` under `MUT-0` at retention policies `[0, 50, 100]` (5,415 candidate states).
   - **0 lost durably-committed keys** and **0 engine recovery panics** across all workloads and seeds, with no durably-committed data loss observed in these workloads.
   - **Workload A (External SSTable Ingestion):** 0 durably-committed violations and 3 partial-retention open errors (0 on Seed 42, 2 on Seed 100, 1 on Seed 777). 40 prefix violations across seeds 42 (15) and 777 (25) reveal a cross-channel commit-order hole on direct L0 ingest: when `db.Ingest` takes the non-overlapping Direct L0 shortcut (`asFlushable = false`), Pebble allocates sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1$ is lost from the unsynced `WAL`, creating a cross-channel sequence hole (whereas overlapping flushable ingest syncs the shared WAL at `commit.go:289` and preserves strict prefix order).
   - **Workload B (Live Format Ratcheting):** 0 data-loss violations and 0 panics across all policies; 3 open errors under 50% retention (0 on Seed 42, 2 on Seed 100, 1 on Seed 777) occurring exclusively during initial database creation prior to operation 1 (creating `"db/MANIFEST-000001"` and `"db/marker.manifest.000001"` prior to initial directory sync); `RatchetFormatMajorVersion` at operation 50 causes 0 open errors and 0 violations.
   - **Workload C (Combined Lifecycle Stress):** Partial-retention open errors (13 total: 4 on Seed 42, 7 on Seed 100, 2 on Seed 777) during manifest rotation and checkpointing prior to directory sync; zero errors under 0% and 100% retention.
   - **Verdict:** Across all 5,415 candidate states in Workloads A, B, and C under `MUT-0`, Pebble v2.1.7 exhibits **0 `durably_committed` data-loss violations and 0 recovery panics**, with no durably-committed data loss observed in these workloads. Simultaneously, the audit exposed two distinct recovery hazards under `MUT-0`: (1) an **input-dependent cross-channel `prefix` monotonicity hole** during non-overlapping Direct L0 `db.Ingest` (`40` prefix violations in Workload A, analyzed across 32 seeds in §5.3), and (2) **50%-retention intra-operation directory races (`open_error`)** when a crash occurs inside the multi-entry window between `MANIFEST-*`/`.sst` creation and `dirFD.Sync()` (19 total open errors).

### 8. Multi-Distribution Workload Diversity Suite

To evaluate robustness and generalization across non-uniform key-value access distributions, CraConTest provides 5 distinct workload presets configured via `-preset` (`internal/workload/workload.go`):
1. `ycsb-a`: 50% update (`set`), 50% point read (`get`) over 8 hot keys (`keyCount = 8`) with periodic sync/flush.
2. `ycsb-b`: 75% point read (`get`), 25% update (`set`, 3 reads per write operation via `i%4 == 3`, adapted from YCSB-B for 25-operation traces so multiple sync cycles occur per run) over 12 keys (`keyCount = 12`) with periodic sync/flush.
3. `ycsb-f`: 100% read-modify-write two-key atomic batch transactions (reading 2 keys via `Get` and updating 2 distinct keys atomically via `Batch` per transaction) over 8 keys (`keyCount = 8`) with periodic sync/flush.
4. `overwrite-heavy`: 100% overwrite (`set`) across 4 hot keys (`keyCount = 4`), driving deep per-key version chains and frequent memtable flush pressure.
5. `delete-heavy`: Equal 1/3 cycling (`i%3`) of point `set`, point `delete`, and `range_delete` (`DeleteRange`) over 8 keys (`keyCount = 8`) with periodic checkpoints (`checkpointAt = 10`).

Evaluated across 16 seeds (`seed = 1..16`, 25 ops) and 4 fault models (`MUT-0`, `MUT-1`, `MUT-5`, `MUT-9`) across 5 ranking strategies (`results/workload_diversity_summary.csv`):
- **Null Control Integrity:** Unfaulted `MUT-0` produces strictly **0 false alarms** across all 16 seeds for all 5 presets.
- **Graph Guidance Superiority:** `DCG` achieves a **$9.3\times$ speedup** over `R2` on both `ycsb-f/MUT-5` and `overwrite-heavy/MUT-5` (mean $k_1 = 1.00$ vs. $9.30$, and **$20.5\times$** over `B0`'s $20.50$), and an **$8.4\times$ speedup** over `R2` on `ycsb-a/MUT-5` ($1.00$ vs. $8.35$, and **$23.6\times$** over `B0`'s $23.55$). `DCG` achieves optimal mean $k_1 = 1.00$ across all failing fault models on `ycsb-a`, `ycsb-b`, `ycsb-f`, and `overwrite-heavy`.
- **Delete-Heavy Dynamics:** On `delete-heavy`, `DCG` achieves mean $k_1 = 3.00$ on `MUT-1` (beating `B0`'s $4.12$, while `R2` averages $2.49$, and expanding early-budget state discovery $S_{25}$ from `R2`'s $10.08$ and `B0`'s $3.59$ to **`14.19`**), $k_1 = 20.54$ on sparse `MUT-5` (beating `R2`'s $34.89$ by **$1.7\times$** and `B0`'s $50.92$ by **$2.5\times$**), and $k_1 = 3.00$ on `MUT-9` (beating both `R2`'s $4.30$ and `B0`'s $9.94$, and expanding $S_{25}$ from $5.16$ to **`7.44`**). The $k_1 = 3.00$ on `delete-heavy` occurs because early synced tombstones targeting non-existent keys in $S_L$ are logical no-ops upon rollback, making the 3rd flagged candidate the first observable key-value failure.
- **Early-Budget Discovery ($S_{25}$) & Empirical Generalization:** Across the 14 failing preset × fault settings, `DCG` outperforms uniform random (`B0`) on **all 14 settings** ($14/14$) and outperforms `R2` on $k_1$ in **13 of 14 settings** ($13/14$), with a single regression on `delete-heavy/MUT-1` (`DCG` mean $k_1 = 3.00$ vs. `R2`'s $2.49$, where early synced tombstones on absent keys are logical state no-ops). On early-budget state coverage ($S_{25}$), `DCG` **matches or exceeds** `R2` across all 14 failing settings (strictly higher on 12 settings, with ties on `ycsb-a/MUT-5` at $1.38$ vs. $1.38$ and `ycsb-b/MUT-1` at $7.00$ vs. $7.00$, and noting that because these preset traces use 25 operations, $B=25$ approaches the full equivalence-class universe for several presets).

---

## Testing & CI

Run the full test suite and code linters:

```bash
# Run Go vet
go vet ./...

# Run all unit and integration tests
go test -count=1 ./...

# Run race detector (Linux)
go test -race -count=1 ./...
```

Continuous integration runs automatically on GitHub Actions (`.github/workflows/ci.yml`).

---

## License

This project is licensed under the Apache 2.0 License.
