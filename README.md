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
│   ├── faults/              # Seeded durability faults (MUT-0 to MUT-4)
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

Pebble v2.1.7, seed 42, endpoint retention policies 0% and 100%, main 1,000-operation
workload. Full analysis in [docs/report.md](docs/report.md); the data is under `results/`.

- **No false positives.** Without a fault (MUT-0) all 2,150 crash candidates recover to a
  legal state; the 100-operation smoke run agrees (302 of 302).
- **Every seeded fault is detected.** Dropping WAL syncs (MUT-1) or directory syncs (MUT-3)
  loses acknowledged writes in 44% and 49% of candidates; dropping manifest syncs (MUT-2)
  or flushed-table syncs (MUT-4) makes recovery refuse to open in 50% and 28% of
  candidates.
- **Random order reaches the first failure fastest.** Because these faults are dense, a
  random order hits a failure within a median of 1–3 crashes under every fault, while the
  sync-boundary (B6), representative (B5) and combined graph-score (B4) orders need 5–61.
- **State-aware orders cover more distinct violations.** Under MUT-1, B6 has found 13 of the
  15 violation signatures after 100 crashes (random: 7–11 over ten seeds), and B5 finds all
  15 after 189 crashes (random: median 853). Signatures and B5's groups both describe the
  pending state, so this advantage is partly by construction, and it does not appear for
  the faults with one to three failure kinds. B4 is the slowest order throughout.
- **Cost.** Replay, crash, recovery and oracle check take about 6.5 ms per candidate on the
  main workload and about 1 ms on the smoke workload.

The next experiments target faults that affect only a few crash points, where ranking can
matter most. For commands, expected outputs and runtimes, see
[docs/reproduce.md](docs/reproduce.md).

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
