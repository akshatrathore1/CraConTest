# CraConTest

**CraConTest** is an impact-aware, graph-guided crash-consistency testing framework for persistent key-value storage engines, evaluated on [CockroachDB's Pebble](https://github.com/cockroachdb/pebble).

---

## Overview

Testing storage engines for crash consistency typically requires exploring huge state spaces. Every file system mutation, flush, or write-ahead log (WAL) entry represents a potential crash cutoff, compounded by partial retention of unsynced dirty data across files. Exhaustive exploration is intractable on realistic workloads, while uniform random testing often wastes budget evaluating non-critical or redundant crash states.

CraConTest treats crash-consistency testing as an explicit resource-bounded search problem. By capturing low-level Virtual File System (VFS) operations alongside storage engine semantic markers, CraConTest constructs a **persistence dependency graph**. It prioritizes candidate crash points based on structural downstream reachability, cross-file impact, semantic criticality, and state novelty.

### Key Contributions & Capabilities

1. **Non-Invasive VFS Interception & Attribution (`vfsrec`):**
   - Intercepts and records all file system mutations, flushes, syncs (`Sync` and `SyncData`), and namespace lineage without modifying storage engine internals.
   - Correlates client-level transaction boundaries with physical persistence events.

2. **Persistence Dependency Graph (`graph`):**
   - Tracks file lifecycle (creation, deletion, replacements, hard links).
   - Captures intra-file sequential dependencies and cross-file causal ordering via engine markers.
   - Computes exact reachability bitsets over topological vertex order.

3. **Impact-Aware Ranking Strategies (`strategy`):**
   - **B0 (Random Baseline):** Uniform random candidate selection across fixed seeds.
   - **B4 (Combined Heuristic):** Multi-objective scoring balancing downstream persistence reachability (`Desc`), affected file count (`CrossFile`), semantic criticality (`Semantic`), and novel state diversity (`NovelState`).
   - **B5 (Representative):** Partitioning crash states by equivalence classes (`StateKey`).
   - **B6 (Sync-Boundary):** Testing crash states immediately preceding or following durability barriers.

4. **Strict Ground-Truth Crash Oracle (`oracle`):**
   - Evaluates recovered database states against an append-only mutation history ledger (`history`).
   - Verifies durably committed operations, atomic batch commit atomicity, and prefix consistency.
   - Detects data loss, torn updates, resurrected keys, corruptions, and recovery panics or hangs.

5. **High-Throughput Replay & Evaluation Pipeline (`exp`):**
   - Replays crash states onto memory-backed crashable file systems with sub-millisecond per-candidate execution.
   - Supports offline multi-strategy evaluation from cached outcomes without redundant execution.

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
├── cmd/cracontest/          # Command-line interface (record, plan, explore, evaluate)
├── docs/                    # Technical documentation and guides
│   ├── architecture.md      # System architecture and data flow
│   ├── crash-model.md       # Crash model, durability barriers, and invariants
│   ├── vfs-design.md        # VFS interception and recording specification
│   ├── reproduce.md         # Step-by-step reproduction runbook
│   └── report.md            # Technical report and experimental evaluation
├── experiments/configs/     # Experiment configurations (e.g., smoke.json)
├── internal/
│   ├── candidate/           # Crash candidate universe and serialization
│   ├── exp/                 # Exploration runner, offline evaluator, and plotting
│   ├── faults/              # Fault injection harness (MUT-0, MUT-1)
│   ├── graph/               # Persistence DAG, reachability, and DOT export
│   ├── history/             # Mutation history tracking
│   ├── oracle/              # Crash consistency validation oracle
│   ├── recovery/            # Crash recovery harness and integrity verification
│   ├── replay/              # VFS trace replay engine
│   ├── strategy/            # Ranking strategies and score auditing
│   ├── trace/               # Event schema and wire validation
│   ├── vfsrec/              # Recording VFS filesystem wrapper
│   └── workload/            # Seeded YCSB-style workload drivers
├── results/                 # Experiment outputs, discovery curves, and plots
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

Run the full testing lifecycle on a seeded 1,000-operation workload:

```bash
# Record workload with injected WAL sync suppression (MUT-1)
./cracontest record --config experiments/configs/smoke.json --out results/smoke_mut1 --fault MUT-1

# Construct dependency graph and plan candidate ranking orders
./cracontest plan --run results/smoke_mut1

# Execute candidate replay and recovery exploration
./cracontest explore --run results/smoke_mut1

# Evaluate discovery curves across strategies
./cracontest evaluate --run results/smoke_mut1
```

Explore completed results in `results/smoke_mut1/curves.csv`.

---

## Experimental Findings

Evaluations on Pebble v2.1.7 across 280 candidate crash states for seeded WAL persistence faults (MUT-1) demonstrate:

- **Null Control Soundness (MUT-0):** Across 2,142 crash candidates (1,071 cutoffs × {0, 100}% retention), CraConTest reported **zero false positives**, zero recovery panics, and zero oracle errors (Gate G2).
- **Bug Discovery Speed (MUT-1):** Under the `MUT-1` fault (dropped WAL syncs), CraConTest discovered 88 consistency violations across 2 distinct bug signatures:
  - **B6 (Sync-Boundary)** discovered **both distinct bug signatures within budget 25** (5 failures found).
  - **B5 (Representative)** discovered bug 1 by budget 25 and bug 2 by budget 50.
  - **B0 (Random Baseline)** required a budget of 50+ to reliably encounter both signatures across random seeds.
- **Throughput:** Replay and oracle verification achieve **>1,000 crash candidate evaluations per second** in-memory.

For complete replication instructions and timing benchmarks, see [docs/reproduce.md](docs/reproduce.md).

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
