# CraConTest

**CraConTest** is an impact-aware, graph-guided crash-consistency testing framework for persistent key-value storage engines, evaluated on [CockroachDB's Pebble](https://github.com/cockroachdb/pebble).

---

## Overview

Testing storage engines for crash consistency typically requires exploring huge state spaces. Every file system mutation, flush, or write-ahead log (WAL) entry represents a potential crash cutoff, compounded by partial retention of unsynced dirty data across files. Exhaustive exploration grows quickly with workload size, and uniform random selection may spend its budget on redundant crash states. CraConTest measures whether a ranking informed by persistence dependencies does better than random under the same budget.

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
   - **B6 (Sync-Boundary):** Moving crash points immediately before a successful sync or rename to the front of the order (no candidate is removed).

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
├── experiments/configs/     # Experiment configurations (main.json, smoke.json)
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

---

## Experimental Findings

Pebble v2.1.7, seed 42, endpoint retention policies 0% and 100%. Full analysis in
[docs/report.md](docs/report.md); the data is under `results/`.

- **No false positives (MUT-0).** In the main 1,000-operation run, all 2,150 crash candidates
  recovered and matched a legal state: zero violations, open errors, panics, hangs or oracle
  errors. The 100-operation smoke run agrees (302 of 302 valid).
- **The seeded fault is detected (MUT-1, dropped WAL syncs).** 865 of 1,978 candidates
  (44%) lose acknowledged synchronous writes, forming 3 distinct signatures, all of the
  `durably_committed` kind. The smoke run shows 88 violations and 2 signatures.
- **Ranking does not beat random on this fault.** Because violations are dense, a random
  order finds the first one almost immediately (median position 2 over 1,000 random orders)
  and all three signatures by a median of 14 candidates. The sync-boundary order (B6) needs
  17 and 47, the representative order (B5) 25 and 101, and the combined graph score (B4)
  55 and 481. B4 favours the earliest cutoffs, which reach the most later events: its first
  50 candidates all lie before the first acknowledged synchronous write (event 57 of 2,427),
  where no acknowledged data can be lost. This is a negative result for the guided
  strategies under a dense fault; sparser faults are the next experiment.
- **Cost.** Replay, crash, recovery and oracle check take about 6.5 ms per candidate on the
  main workload (about 150 candidates per second) and about 1 ms on the smoke workload.

For commands, expected outputs and runtimes, see [docs/reproduce.md](docs/reproduce.md).

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
