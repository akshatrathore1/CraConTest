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
   - Computes exact topological reachability bitsets and partitions crash candidates into canonical persistence states (`StateKey`).
   - Extracts POSIX durability-contract features (`features.go`) in a single causal forward pass, tracking unpersisted WAL writes after client acknowledgment (`AckSyncMissing`), uncommitted SSTable data or directory entries referenced by committed `MANIFEST` edits (`committedInManifest` / `DanglingTableCount`), unsynced parent directory entries across `Create`, `ReuseForWrite`, `Rename`, `Link`, and `MkdirAll` (`UnsyncedDirectoryRename`), and unpersisted `MANIFEST` writes.

3. **Ranking Strategies & Baselines (`strategy`):**
   - **B0 (Uniform Random):** Seeded uniform random selection across the candidate universe.
   - **B1–B3 (Position Heuristics):** FIFO (`B1`), LIFO (`B2`), and deterministic Stride (`B3`).
   - **B4 (Combined Topological Reachability):** Greedy multi-term score (`Desc + CrossFile + Semantic + Novelty`) over downstream graph reachability.
   - **B5 (State-Representative):** First candidate per distinct `(StateKey, Retention)` equivalence class in trace order.
   - **B6 (Sync-Boundary):** Prioritizes cutoffs immediately preceding a `Sync`, `SyncData`, or `Rename`.
   - **R1 (0%-Retention-First Random):** Partitions the universe by retention policy, randomly sampling 0%-retention candidates before 100%-retention candidates.
   - **R2 (State-Representative Random):** Partitions 0%-retention candidates into canonical `StateKey` equivalence classes and randomly samples one representative per distinct persistence state before testing redundant cutoffs.
   - **RHG / B7 (Reachability-Hazard Guided):** Ranks 0%-retention state representatives by uncommitted hazard reachability, barrier proximity, and semantic tier.
   - **DCG / B8 (Durability-Contract Guided) & DCG-FlagOnly (Ablation):** Prioritizes 0%-retention state representatives that exhibit structural POSIX durability-contract violations (`ContractScore > 0`) ahead of unflagged representatives, alongside a randomized flag-only ablation (`DCG-FlagOnly`) that isolates the effect of the binary contract indicator.

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
├── docs/                    # Technical documentation and guides
│   ├── architecture.md      # System architecture and data flow
│   ├── crash-model.md       # Crash model, durability barriers, and invariants
│   ├── vfs-design.md        # VFS interception and recording specification
│   ├── reproduce.md         # Step-by-step reproduction runbook
│   ├── report.md            # Technical report and experimental evaluation
│   └── benchmark-report.md    # Multi-seed benchmark empirical report
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
./cracontest benchmark -report docs/benchmark-report.md -csv results/benchmark_summary.csv
```

---

## Experimental Findings

The experimental evaluation comprises both single-seed 1,000-operation runs and a 32-seed multi-fault benchmark on Pebble v2.1.7. Full analysis is available in [docs/report.md](docs/report.md) and [docs/benchmark-report.md](docs/benchmark-report.md); raw data is stored under `results/`.

### 1. Oracle Soundness (MUT-0)

Across all 2,150 candidates of the 1,000-operation main run, all 302 candidates of the 100-operation smoke run, and all 32 workload seeds of the multi-seed benchmark (>11,000 evaluated crash states), zero false alarms were observed on unfaulted runs (`MUT-0`). Every crash candidate recovered to a state that strictly satisfied the durability prefix and atomicity invariants, demonstrating 100% oracle precision.

### 2. Why Naive Topological Reachability (B4) Underperforms Random on Dense Faults (MUT-1 to MUT-4)

On dense faults where every barrier of a class is suppressed (28%–50% failure density), uniform random (`B0`) reaches a first failure in a median of 1–3 crashes, whereas naive topological reachability (`B4`) requires 13–61 crashes. Because downstream reachability (`Desc`) is monotonically highest at the beginning of a trace, `B4` spends its initial budget before the first synchronous client write is acknowledged (event 57 of 2,427 in `main_mut1`), where rollback to the initial state is legal. Our root-cause analysis identified two structural reasons for this counterintuitive behavior:
1. **Monotonic Downstream Reachability Bias:** In a directed acyclic persistence graph, topological depth correlates inversely with downstream reachability. The earliest mutations reach the greatest total number of subsequent vertices ($|\text{Desc}(v)|$ is monotonically highest at the start of a trace). By scoring by downstream reachability (`Desc`), `B4` systematically prioritized candidates near the beginning of execution.
2. **Pre-Acknowledgment Semantics:** Storage engine crash-consistency contracts strictly apply *after* a client write has been acknowledged. In the initial portion of a workload trace (prior to the first acknowledged synchronous write at event 57 of 2,427 in `main_mut1`), any crash state legally rolls back to the initial database state without violating durability. `B4` spent its first 50 selections exploring exclusively inside this pre-acknowledgment region, where no consistency violations could legally exist.

### 3. State-Equivalence Sampling (R2) vs. Durability-Contract Priors (DCG) Across Dense and Sparse Faults (32-Seed Benchmark, MUT-0 to MUT-9)

To evaluate ranking strategies across both dense and sparse failure distributions, we ran a 32-seed benchmark across 10 fault models (`MUT-0` through `MUT-9`), evaluating over 11,000 crash states.

#### Table 1: Cost-to-First-Failure ($k_1$ Mean [95% CI])

| Fault Model | Fault Description | Failure Density | DCG | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|---|---|
| **MUT-0** | Null Control (No Fault) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.44 [1.42, 1.46] | 1.37 [1.34, 1.40] | 5.00 [5.00, 5.00] | 17.00 [17.00, 17.00] | 2.00 [2.00, 2.00] |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.17 [1.16, 1.19] | 1.04 [1.04, 1.05] | **1.00** [1.00, 1.00] | 5.00 [5.00, 5.00] | 1.58 [1.41, 1.74] |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** [1.00, 1.00] | 1.03 [1.03, 1.04] | **1.00** [1.00, 1.00] | 1.44 [1.40, 1.48] | 1.17 [1.15, 1.19] | **1.00** [1.00, 1.00] | 9.00 [9.00, 9.00] | 2.00 [2.00, 2.00] |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.95 [1.90, 2.01] | 1.67 [1.61, 1.74] | **1.00** [1.00, 1.00] | 31.62 [31.24, 32.01] | 2.39 [2.22, 2.56] |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 13.01 [12.68, 13.34] | 17.04 [16.54, 17.53] | 262.25 [258.20, 266.30] | 17.00 [17.00, 17.00] | 25.97 [25.00, 26.94] |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** [1.00, 1.00] | 1.20 [1.17, 1.24] | 14.08 [13.50, 14.65] | 7.72 [7.46, 7.97] | 20.79 [19.89, 21.70] | 101.00 [97.37, 104.63] | 43.15 [42.38, 43.93] | 31.12 [29.02, 33.21] |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 3.58 [3.50, 3.66] | 3.28 [3.15, 3.41] | **1.00** [1.00, 1.00] | 55.25 [54.85, 55.65] | 4.41 [4.23, 4.58] |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.40 [1.36, 1.44] | 1.21 [1.18, 1.23] | **1.00** [1.00, 1.00] | 17.00 [17.00, 17.00] | 1.95 [1.90, 2.01] |

#### Table 2: Failing State Discovery ($S_{25}$ Early Budget / $S_{100}$ Exhaustive Budget Mean)

| Fault Model | DCG ($S_{25}$ / $S_{100}$) | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|
| **MUT-1** | **23.97** / **24.00** | **23.97** / **24.00** | 17.69 / **24.00** | 17.13 / **24.00** | 10.04 / 19.83 | 6.53 / 8.94 | 5.00 / 13.00 | 6.12 / 14.61 |
| **MUT-2** | **25.00** / **33.03** | **25.00** / **33.03** | 24.00 / **33.03** | 21.15 / **33.03** | 8.68 / 22.81 | 7.03 / 11.38 | 11.00 / 30.00 | 5.64 / 13.75 |
| **MUT-3** | 24.00 / **27.00** | **24.12** / **27.00** | 21.03 / **27.00** | 17.26 / **27.00** | 7.27 / 19.00 | 7.16 / 12.06 | 9.00 / 21.00 | 5.00 / 11.22 |
| **MUT-4** | **21.69** / **21.69** | **21.69** / **21.69** | 16.69 / **21.69** | 12.57 / **21.69** | 5.44 / 14.51 | 8.00 / 12.25 | 0.00 / 16.69 | 3.53 / 8.59 |
| **MUT-5** | **2.00** / **2.00** | **2.00** / **2.00** | 1.81 / **2.00** | 1.30 / **2.00** | 0.87 / 1.60 | 0.00 / 0.00 | 1.00 / 1.00 | 0.48 / 1.00 |
| **MUT-6** | **1.62** / **1.62** | **1.62** / **1.62** | 1.22 / **1.62** | 1.07 / **1.62** | 0.39 / 1.20 | 0.00 / 0.41 | 0.00 / 1.22 | 0.00 / 0.81 |
| **MUT-7** | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | **12.88** / **12.88** | **12.88** / **12.88** | 7.88 / **12.88** | 6.74 / 12.82 | 3.27 / 8.41 | 8.00 / 11.75 | 0.00 / 9.88 | 2.00 / 5.08 |
| **MUT-9** | **25.00** / **27.00** | **25.00** / **27.00** | 20.00 / **27.00** | 17.78 / **27.00** | 5.57 / 17.44 | 7.00 / 10.94 | 5.00 / 26.00 | 3.03 / 9.83 |

#### Analysis

1. **POSIX Durability-Contract Priors Across Inode Data and Parent-Directory Entries:**
   Under POSIX filesystem semantics, ensuring durable file persistence after crash requires two conditions: (1) flushing dirty file data (`fsync` / `fdatasync`), and (2) flushing the parent directory entry (`fsync(parentDir)`) after file creation, renaming, hard-linking, or directory creation (`Create`, `ReuseForWrite`, `Rename`, `Link`, `MkdirAll`). If a file is written and its data synced, but its parent directory entry remains unsynced, a crash at 0% retention causes the directory entry to vanish, resulting in lost files upon recovery. CraConTest implements universal POSIX durability priors without engine-specific heuristics: tracking `parentDir` and `dirEntrySynced` for every filesystem node, enforcing that durability holds if and only if `dirtyBytes == 0 && dirEntrySynced == true`, and scoping directory sync events strictly to the target directory path. MUT-9 is dense (41.4% density) at $k=1$ (initial directory sync) and sparse (0.8%–12.4%) at $k \in \{7, 9, 10, 12\}$ (flush/compaction directory syncs); directory-entry tracking was added after inspecting MUT-9, so MUT-9 evaluates parameterized $k$-generalization within the directory-sync family. On MUT-9 ($k=1$), DCG identifies the uncommitted directory entry prior, expanding state coverage to $S_{100} = 27.00$ (vs B0's $9.83$, a 2.7x expansion) and $S_{25} = 25.00$ (vs B0's $3.03$, an 8.2x early-budget advantage).

2. **Component Attribution & DCG-FlagOnly Ablation:**
   Comparing full DCG against the **DCG-FlagOnly** ablation isolates the exact contribution of each mechanism:
   - **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and $12.88$ on MUT-8). On dense faults (`MUT-1`, `MUT-2`, `MUT-4`), sparse faults (`MUT-5`, `MUT-8`), and directory faults (`MUT-9`), DCG-FlagOnly achieves the identical optimal $k_1 = 1.00$ [1.00, 1.00] as full DCG, confirming that the binary contract violation flag is the primary driver of first-failure cost reduction.
   - **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop, ~0.8% density), where full DCG achieves mean $k_1 = 1.00$ compared to $1.20$ for `DCG-FlagOnly`.
   - **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

3. **$S_{25}$ Early-Budget Discovery & The ~40-State Equivalence Ceiling:**
   In a 120-operation workload, there are only ~35–40 distinct canonical `StateKey` persistence states under 0%-retention crashes. Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100 (reaching identical $S_{100} = 24.00$ on `MUT-1`, $33.03$ on `MUT-2`, $27.00$ on `MUT-3`, $21.69$ on `MUT-4`, and $27.00$ on `MUT-9`). Under MUT-8 (compaction-triggered SST sync drop, 15.2% density), DCG reaches the first failing state at median $k_1 = 1.0$ (mean 1.00) compared to R2's median $k_1 = 3.6$ (mean 3.58) and B0's median $k_1 = 4.0$ (mean 4.41), achieves early-budget $S_{25} = 12.88$ (vs R2's $6.74$ and B0's $2.00$), and matches R2 at full budget ($S_{100} = 12.88$ for DCG vs $12.82$ for R2, compared to $5.08$ for B0). Therefore, early-budget coverage $S_{25}$ ($B = 25$) is the critical discriminant metric for practical CI testing: DCG achieves near-full state discovery saturation within the first 25 tests (e.g. $S_{25} = 23.97$ vs R2's $17.13$ and B0's $6.12$ on `MUT-1`; $S_{25} = 12.88$ vs R2's $6.74$ and B0's $2.00$ on `MUT-8`; $S_{25} = 25.00$ vs R2's $17.78$ and B0's $3.03$ on `MUT-9`), yielding a 2x–8x throughput advantage over random selection.

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
