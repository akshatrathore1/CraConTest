# Reproduction Runbook

This document details the exact commands, execution steps, expected outputs, and measured runtimes to reproduce all benchmark tables, figures, and verification gates in CraConTest.

All experiments are deterministic under fixed random seeds.

---

## 1. System Requirements & Build Setup

### Prerequisites
- Go 1.27+ (`go version` matching repository `go.mod`)
- OS: Linux (Ubuntu/ext4) or Windows (NTFS). On Windows, set `CGO_ENABLED=0`.

### Build
To build the `cracontest` CLI tool:
```bash
go build -o cracontest ./cmd/cracontest
```
Expected runtime: ~1.5s.

---

## 2. End-to-End Pipeline Reproduction

The complete testing lifecycle consists of four decoupled phases: `record`, `plan`, `explore`, and `evaluate`.

### Experiment 1: MUT-0 Null Control (Baseline Soundness)

Executes the fixed workload (1000 ops, seed 42) with MUT-0 (unfaulted baseline) to establish that CraConTest produces zero false positives.

```bash
# 1. Record workload trace and mutation ledger
./cracontest record --config experiments/configs/smoke.json --out results/smoke_mut0 --fault MUT-0

# 2. Build graph and generate candidate ranking orders
./cracontest plan --run results/smoke_mut0

# 3. Explore candidate crash states via replay and in-process recovery
./cracontest explore --run results/smoke_mut0

# 4. Evaluate outcomes across budgets and ranking strategies
./cracontest evaluate --run results/smoke_mut0
```

| Phase | Expected Output | Measured Runtime |
|---|---|---|
| `record` | `trace.json` (721 events), `history.json` (1000 ops) | ~350 ms |
| `plan` | `universe.json` (302 candidates), `ranking_b0/b4/b5/b6.json` | ~20 ms |
| `explore` | `journal.ndjson` (302 runs), `outcomes.json` (0 violations) | ~300 ms |
| `evaluate` | `curves.csv` (all strategies 0 failures at all budgets) | ~10 ms |
| **Total** | | **~680 ms** |

---

### Experiment 2: MUT-1 Injected Fault (Dropped WAL Sync)

Executes the identical workload with fault `MUT-1` (which silently suppresses WAL `Sync`/`SyncData` calls) to test failure detection efficiency across ranking strategies.

```bash
# 1. Record trace under MUT-1 fault injection
./cracontest record --config experiments/configs/smoke.json --out results/smoke_mut1 --fault MUT-1

# 2. Generate candidate crash universe and rankings
./cracontest plan --run results/smoke_mut1

# 3. Explore crash candidates
./cracontest explore --run results/smoke_mut1

# 4. Evaluate discovery curves
./cracontest evaluate --run results/smoke_mut1
```

| Phase | Expected Output | Measured Runtime |
|---|---|---|
| `record` | `trace.json` (670 events, 0 WAL syncs), `history.json` | ~340 ms |
| `plan` | `universe.json` (280 candidates), `ranking_b0/b4/b5/b6.json` | ~18 ms |
| `explore` | `journal.ndjson` (280 runs), `outcomes.json` (88 violations, 2 distinct bugs) | ~280 ms |
| `evaluate` | `curves.csv` | ~10 ms |
| **Total** | | **~650 ms** |

---

## 3. Generating Gate Artifacts, Discovery Curves, and Plots

### Gate G4 Discovery Curves and SVG Plots

To compute discovery curves across B0 (mean/min/max over 10 ranking seeds), B4 (combined), B5 (representative), and B6 (sync-boundary) and output standalone SVG charts and CSVs:

```bash
go test -v -run TestGenerateG4Artifacts ./internal/exp
```
Measured runtime: **~0.15s**.

Generated Artifacts:
- `results/discovery_mut0.csv`
- `results/discovery_mut1.csv`
- `results/plot_mut0.svg`
- `results/plot_mut1.svg`

### Gate G3 Dependency Graph Metrics

To construct the persistence dependency graph over the 1,000-operation workload trace and inspect vertex/edge counts, topological ordering, and semantic features:

```bash
go test -v -run TestGateG3 ./internal/graph
```
Measured runtime: **~0.01s** graph build time.
Expected metrics:
- Trace events: 2,512
- Candidate cutoffs: 1,074
- Graph build time: <10 ms
- Distinct StateKeys: 174
- Semantic category share: 96.28%

### Gate G2 Comprehensive Null Control (2,142 Candidates)

To verify the oracle on the full universe of 2,142 crash candidates (1,071 cutoffs × {0, 100}% retention) against MUT-0:

```bash
go test -v -run TestGateG2 ./internal/workload
```
Measured runtime: **~7.4s**.
Expected metrics:
- Candidates evaluated: 2,142
- Violations: 0
- Open errors: 0
- Panics: 0
- Hangs / Timeouts: 0
- Oracle errors: 0

---

## 4. Real-Disk Recovery Smoke Verification

To verify that clean database shutdown and abrupt child-process kill (without calling `Close()`) recover cleanly and pass structural integrity checks on real disk filesystems (`vfs.Default`):

```bash
go test -v -run TestRealDisk ./internal/recovery
```
Measured runtime: **~1.8s**.
Checks:
1. `TestRealDiskCleanClose`: 200 synced keys with mid-run flush survive clean close.
2. `TestRealDiskChildProcessKill`: Child process exits with code 3 during active synced writes; parent recovers and verifies all 200 keys and values match via `recovery.SafeOpen` + `recovery.CheckIntegrity`.

---

## 5. Full Test Suite & Race Detection

```bash
# Vet codebase
go vet ./...

# Run all package unit and integration tests
go test -count=1 ./...

# Run race detector (Linux)
go test -race -count=1 ./...
```
Expected runtime:
- `go vet ./...`: ~0.5s
- `go test -count=1 ./...`: ~12s
- `go test -race -count=1 ./...`: ~20s
All tests should pass with 0 errors across all 13 packages.
