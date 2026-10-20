# CraConTest — Architecture

This document describes the architecture of CraConTest: how the system is
organized, what each component does, how data flows between them, and how the
phases of execution compose into a complete crash-consistency testing pipeline.

## Overview

CraConTest treats crash-consistency testing as a search problem. Instead of
testing crash states at random, it builds a persistence dependency graph over
VFS events, ranks crash points by likely impact, and tests the highest-ranked
first. The system is structured as a four-phase pipeline that separates
recording from analysis and testing from evaluation.

## Phases

### Phase 1: RECORD (once per workload specification)

A seeded workload drives Pebble through a recording VFS wrapper that captures
every mutating filesystem call. The recording runs on `vfs.NewMem()` (not
`NewCrashableMem()`; the crashable filesystem is only needed in Phase 3).

**Inputs:**
- A workload specification: operation mix, sizes, base seed, retention
  policies, strategy list, crash budgets, and optional fault identifier.

**Outputs:**
- A **trace**: every mutating `vfs.FS` and `vfs.File` call with its payload,
  `DiskWriteCategory`, operation markers (`OpBegin`/`OpEnd` around each client
  API call), and engine event annotations (from `Options.EventListener`).
- A **history ledger**: for each client operation, the operation ID, kind, key,
  complete ordered mutation payloads, sync flag, and the trace event IDs
  where it began and was acknowledged. Hashes alone cannot reconstruct legal
  states or verify atomic batches.
- An optional **fault label**: which seeded fault was active, or none for the
  null control.

**Design details:**
- The recording VFS serializes all mutations under a single lock so trace order
  equals filesystem order.
- Write data is copied before recording (Pebble's `File.Write()` may modify
  the passed slice).
- Both `Sync()` and `SyncData()` are intercepted and recorded. Pebble routes
  WAL and SSTable syncs through `vfs.NewSyncingFile`, which dispatches via
  `File.SyncData()`. Manifest files and directory handles use `File.Sync()`
  directly.
- `File.SyncTo()` is recorded but provides no durability guarantee (it returns
  `(false, nil)` in MemFS).
- Small memtable sizes and explicit `Flush`/`Compact` calls ensure the trace
  contains table creation, manifest updates, and directory syncs, not just WAL
  writes.

### Phase 2: PLAN (offline, no engine execution)

The trace is analyzed offline to build a persistence dependency graph, enumerate
crash candidates, and rank them by strategy.

**Inputs:**
- The trace from Phase 1.

**Outputs:**
- A **persistence DAG**: same-file write ordering, sync barriers,
  rename/link/directory-sync edges and observed cross-file listener
  dependencies. Filename-only guesses do not constitute manifest parsing.
- A **persistence-state view**: for each event index *k*, which files hold
  unsynced data, which directory entries are pending, and how many persistence
  events are downstream.
- A **candidate universe** *U*: each candidate is a tuple
  *(event cutoff k, retention percentage, seed)*.
  - *k*: after one-based events `1..k`; zero is before the trace.
  - *retention percentage*: what share of unsynced data survives (maps to
    `CrashCloneCfg.UnsyncedDataPercent`).
  - *seed*: derived from the base seed, cutoff and percentage by the
    version-one SHA-256 encoding.
- **Rankings**: one ordered list of candidates per strategy.

The initial execution profile uses 0% and 100% retention. Pinned CrashClone
does not reproduce intermediate-retention images from a seed alone because
map traversal affects RNG consumption. The tuple can represent 0..100, but
intermediate policies require a separately validated sampler or saved image.
The complete tuple, not a truncated hash, identifies a candidate and outcome.

**Strategies** rank *U* using only trace-derived features. They cannot observe
outcomes, history, or recovery results. This constraint is enforced by Go
import rules: `internal/strategy` must not import `internal/oracle`,
`internal/exp`, `internal/recovery`, or `internal/replay`.

### Phase 3: EXPLORE (expensive, cached)

Each candidate in *U* is executed at most once. Results are cached in an
outcome table.

**Steps per candidate:**

1. **Replay**: apply trace events `events[:k]` to a fresh `vfs.NewCrashableMem()`.
   For increasing *k*, a single forward pass can serve multiple candidates.
   Replay must stop mutating the filesystem before calling `CrashClone()` —
   the v2.1.7 `MemFS.CrashClone` acquires an exclusive write lock
   (`cloneMu.Lock()`) that blocks all mutations.
  Preserve open resource handles and cursor/alias behavior. Do not close
  or synchronize those handles before the snapshot.

2. **Clone**: call `CrashClone(CrashCloneCfg{UnsyncedDataPercent: p, RNG: rng})`
   on the replayed filesystem to produce the post-crash state.

3. **Recover**: open Pebble on the cloned filesystem with:
   - An `Options.Logger` whose `Fatalf` panics instead of calling `os.Exit(1)`
     (Pebble's default), with `recover()` around the `Open()` call.
   - A timeout to detect hangs.
   - A journal: append "started" before each candidate and "finished" after.
     On restart, a started-but-not-finished candidate is recorded as a hang.

4. **Check**: run `DB.CheckLevels()`, confirm forward and backward iteration
   agree, dump all key-value pairs.

5. **Oracle**: compare the dump against the history ledger using the prefix
  (no-hole) rule. Legal states are whole-operation prefixes between the last
  acknowledged synchronous mutation and the latest begun mutation at the
  cutoff. In-flight batches are wholly present or absent. A later Sync also
  covers earlier NoSync mutations. Compare whole states before diagnosing
  divergence so overwrites, deletes and repeated states remain legal.
  Emit exactly one outcome class per candidate.

**Outputs:**
- An **outcome table**: one record per candidate with outcome class, violated
  rule, signature, and timings.
- A **journal**: started/finished markers for crash recovery of the explore
  process itself.

### Phase 4: EVALUATE (offline, no execution)

Rankings and the outcome table are joined to produce discovery curves,
time-to-first-bug metrics, overhead measurements, and recall/false-positive
counts against seeded faults.

**Inputs:**
- Rankings from Phase 2, outcome table from Phase 3, fault labels from Phase 1.

**Outputs:**
- Discovery curves (violations found vs. candidates tested).
- Time to first bug per strategy and budget.
- Overhead breakdown: ranking time, replay+clone time, recovery+check time.
- Recall (fraction of seeded faults detected) and false-positive count.
- Figures and tables (produced by Python analysis scripts).

## Cost Model

The cost per candidate breaks down into three components:

| Component | Dominant Factor | Measurement Point |
|-----------|----------------|-------------------|
| **Replay** | Filesystem size at event *k* (proportional to bytes written in events `0..k`) | Wall time per replayed event |
| **Clone** | Filesystem size (tree walk + data copy) | Wall time for `CrashClone()` |
| **Recovery** | WAL replay length + manifest complexity | Wall time for `pebble.Open()` + `CheckLevels()` |

**Cost estimation approach**: run a 1,000-operation workload specification,
measure each component at 20 evenly spaced event indices, extrapolate to
estimate the affordable universe size for the full experiment. This cost check
is a Week 2–3 milestone.

Trace size, selected cutoffs and total execution cost require measurement
with the recorder and replay active. Historical close-first clone timings
exclude replay and do not establish an experiment budget. If the full pass
is too expensive, all strategies use the same fixed stratified universe.

## Package Structure

```
CraConTest/
├── cmd/cracontest/       # CLI binary
├── internal/
│   ├── trace/            # Event schema, (de)serialization    [shared]
│   ├── vfsrec/           # Recording vfs.FS wrapper           [role a]
│   ├── replay/           # Trace replay into CrashableMem     [role a]
│   ├── recovery/         # Panic-safe Open, timeout, checks   [role a]
│   ├── faults/           # Seeded-bug VFS decorators          [role a]
│   ├── graph/            # Persistence DAG and state view     [role b]
│   ├── candidate/        # Candidate type and universe        [role b]
│   ├── strategy/         # Ranking strategies                 [role b]
│   ├── workload/         # Seeded workload generators         [role c]
│   ├── history/          # Client-visible operation ledger    [role c]
│   ├── oracle/           # Consistency rules and outcomes     [role c]
│   └── exp/              # Run specs, journal, evaluator      [role c]
├── analysis/             # Python scripts for statistics
├── experiments/configs/  # Workload specifications
├── results/              # Experiment outputs
├── docs/                 # This document and others
└── paper/                # Publication materials
```

### Import Rules

The following import constraints enforce information-flow invariants and are
verified by `go list -deps` in CI:

| Package | Must NOT import |
|---------|----------------|
| `strategy` | `oracle`, `exp`, `recovery`, `replay` |
| `graph` | `oracle`, `exp`, `recovery`, `replay` |
| `candidate` | `oracle`, `exp`, `recovery`, `replay` |
| `trace` | any internal package |
| `history` | any internal package |

The `strategy` package ranks candidates using only the persistence-state view
from `graph`. It never observes outcomes, recovery results, or timing data.

## Information Flow Constraints

1. Strategies see only the trace-derived graph view and candidate universe.
   They cannot access outcomes, history, recovery results, or timing data.
2. The engine runs only in RECORD and inside recovery. PLAN, replay, and
   EVALUATE need no engine.
3. Crash states are constructed from the trace only. The workload is never
   re-run to reach a crash point.
4. Each candidate is executed at most once per workload specification.
   EVALUATE reads cached outcomes.
5. The oracle reads the history ledger and recovered contents. It never reads
   rankings.
6. Seeded faults exist only in RECORD as filesystem decorators. A run without
   a fault is the null control (must yield zero violations).
7. Every stage records its own wall time separately.

## Data Flow Diagram

```
RECORD                 PLAN                  EXPLORE              EVALUATE
──────                 ────                  ───────              ────────

workload spec          trace ──► graph       universe ──► runner  rankings ──► evaluator
    │                            │               │                    │
    ▼                            ▼               ▼                   ▼
workload driver        persistence DAG   replay(0..k)         join rankings
    │                            │          into CrashableMem   with outcomes
    ▼                            ▼               │
Pebble ──► vfsrec     candidate universe   CrashClone ──► recovery ──► oracle
    │          │               │                                    │
    ▼          ▼               ▼                                    ▼
history    trace          strategies ──► rankings           outcome table
                               │
                               ▼
                         (import rule test:
                          no access to outcomes)
```

## CLI Commands

| Command | Phase | Description |
|---------|-------|-------------|
| `cracontest record` | RECORD | Run workload, produce trace and history |
| `cracontest plan` | PLAN | Build graph, enumerate candidates, rank |
| `cracontest explore` | EXPLORE | Execute candidates, fill outcome table |
| `cracontest evaluate` | EVALUATE | Join rankings with outcomes, compute metrics |
| `cracontest report` | — | Generate figures from evaluation results |

## Stored Artifacts

| Artifact | Location | Git-tracked |
|----------|----------|-------------|
| Workload specifications | `experiments/configs/` | Yes |
| Trace, history, outcomes | `results/raw/<run-id>/` | No (git-ignored) |
| Evaluation results | `results/processed/` | Yes |
| Figures | `results/plots/`, `paper/` | Yes |

Trace, history, universe, ranking and outcome documents use schema_version=1
and ordered non-null arrays. Outcomes persist the complete candidate tuple.
Each run records its configuration, seeds, weights, fault identity, Pebble
version, Go version and platform. Run directories are explicit identifiers;
canonical run-ID hashing remains a separate design task.

## Risks and Early Measurements

1. **Replay fidelity** (Gate G1, Week 3): at random event indices, the
   replayed 100%-retention state must exactly equal the live run's state
   (same files, same bytes).

2. **Cost per candidate** (Week 2–3): clone time + recovery time + check time
   on a 1,000-operation run determines the affordable universe size.

3. **Trace volume**: payloads are copied, so trace size grows with total bytes
   written. Measure before choosing workload sizes.

4. **Concurrency**: with concurrent writers, the commit order is determined by
   Pebble, not the harness. Start with single-writer workloads. Concurrent
   workloads require recovering commit order from WAL events before the prefix
   rule applies.

## Dependencies

- **Pebble v2.1.7**: `github.com/cockroachdb/pebble/v2` pinned at v2.1.7.
  Uses `vfs.FS`, `vfs.File`, `vfs.NewMem()`, `vfs.NewCrashableMem()`,
  `MemFS.CrashClone()`, `Options.EventListener`, `Options.Logger`,
  `DB.CheckLevels()`.

- **Go toolchain**: pinned version (see `go.mod`). Build and test:
  `go build ./...`, `go vet ./...`, `go test ./...`.

## Revision History

| Date | Section | Correction |
|---|---|---|
| 2026-10-20 | Pipeline and artifacts | Defined event-ID cutoffs, endpoint retention, full mutation history and full-tuple outcomes; removed uncertified cost and run-hash claims. |

- **Python**: for `analysis/` scripts (statistics, plotting). Not required for
  the Go pipeline.
