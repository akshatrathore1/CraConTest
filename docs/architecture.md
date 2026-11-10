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

A seeded workload drives Pebble through a recording VFS wrapper (`vfsrec.New`) that captures
every mutating filesystem call. `vfsrec.New` wraps any in-memory `vfs.FS` (`vfs.NewMem()` in recorder
unit tests or `vfs.NewCrashableMem()` in CLI `record` and `benchmark`). `CrashClone()` is never
invoked on the recording filesystem during Phase 1 RECORD—crash snapshots are constructed
exclusively in Phase 3 EXPLORE by replaying trace prefixes into a fresh `vfs.NewCrashableMem()`.

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
- In `appendSyncEventLocked`, the recorder enforces same-handle adjacency: any
  `Sync`, `SyncData`, or `SyncTo` is repositioned immediately adjacent to the
  preceding `Write`/`WriteAt` on the same handle, halting at any same-handle
  event, lock, or client operation boundary (`OpBegin`/`OpEnd`), preventing
  concurrent background `Close` or `EngineEvent` records from splitting write-sync
  pairs.
- `NoSync` client mutations (`set`, `delete`, `batch`, `range_delete`) wait on `walCond`
  (`WaitForWALWrite` in `internal/vfsrec/recorder.go:111-148`, with `NotifyDroppedWALWrite` for
  `MUT-X6`) so `LogWriter.flushLoop` emits the operation's `.log` `Write` before `OpEnd`.
- During ingest workloads, `inIngestSection` gates non-staging, non-marker background `Remove`
  calls (`internal/vfsrec/recorder.go:468-475`) on `ingestCond` until a deferred `EndIngestSection()`
  call broadcasts upon `db.Ingest` return, while `FlushBegin` (`internal/vfsrec/listener.go:102-138`)
  executes a 500-iteration `runtime.Gosched()` loop and, when outside `inIngestSection`, waits on
  `manifestCond` while `pendingManifestDeletions > 0` (`ManifestCreated` at `listener.go:151-158`
  when `dbManifestCount > 2` until `ManifestDeleted` at `listener.go:159-169`) so obsolete manifest
  cleanup from a prior manifest rotation completes before the next flush begins. All synchronization
  barriers (`WaitForWALWrite` on `walCond`, `FlushBegin` on `manifestCond`, and `drainBackgroundWork`)
  wait purely on condition-variable / counter predicates during normal execution and enforce a 5-second
  fail-fast safety bound that panics loudly (`panic(...)`) if a background goroutine ever stalls,
  ensuring traces never silently fall back to wall-clock timing. The 5-second fail-fast timers in
  `WaitForWALWrite` and `FlushBegin` set a mutex-protected `timedOut` boolean flag prior to `Broadcast()`
  (preventing any clock-skew lost wakeup); because `FlushBegin` runs inside Pebble's flush callback,
  its panic propagates unrecoverably through Pebble's background flush goroutine by design. This synchronization
  prevents asynchronous cleaner deletions of SST/WAL files from interleaving with `db.Ingest` so
  traces are reproducible, shifting background cleaner deletions to immediately after
  `EndIngestSection()` (under-representing mid-ingest background cleaner states and
  over-representing post-ingest states).
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
- **Durability-contract features (`ContractFeatures`)**: per-cutoff indicators
  (`AckSyncMissing`, `DanglingTableCount`—covering dangling tables and uncommitted manifest edits via `manifestDirtyUncommitted`—`UnsyncedDirectoryRename`,
  `HasClosedDirtyLog`, `UnsyncedDirEntryCount`, `ContractScore`) computed causally from trace prefixes
  (`internal/graph/features.go`) without inspecting recovery outcomes.
- **Persistence State Equivalence (`StateKey`)**: partitions crash candidates into
  canonical equivalence classes. `StateKey` represents a two-sided approximation:
  finer-grained than the physical disk image on pending entries (since `keyPending`
  records the creating `trace.Event` ID), and coarser than the consistency oracle
  (aggregating dirty byte spans without tracking client acknowledgment history or
  overwritten spans).
- A **candidate universe** *U*: each candidate is a tuple
  *(event cutoff k, retention percentage, seed)*.
  - *k*: after one-based events `1..k`; zero is before the trace.
  - *retention percentage*: what share of unsynced data survives (maps to
    `CrashCloneCfg.UnsyncedDataPercent`).
  - *seed*: derived from the base seed, cutoff and percentage by the
    version-one SHA-256 encoding.
  Within a given `(seed, fault)` execution, all ranking strategies evaluate an
  identical candidate universe. Across fault models, universe size varies because
  `faults.FS` intercepts and drops specific sync barriers prior to trace recording.
- **Rankings**: one ordered list of candidates per strategy.

To eliminate trace and clone nondeterminism and guarantee bit-for-bit reproducibility across all retention policies, `replay.State.Clone(percent, seed)` (`internal/replay/replay.go:89-110`) explicitly handles all three retention regimes:
- `percent == 0`: delegates directly to `MemFS.CrashClone` with `UnsyncedDataPercent: 0` (RNG-free).
- `percent == 100`: delegates directly to `MemFS.CrashClone` with `UnsyncedDataPercent: 100` and `rand.New(rand.NewPCG(0, seed))` (seed-invariant because every `IntN(100) < 100` check succeeds unconditionally).
- `1 <= percent <= 99`: constructs `base0` (`UnsyncedDataPercent: 0`) and `full100` (`UnsyncedDataPercent: 100` with `rand.New(rand.NewPCG(0, 0))`), then runs `deterministicPartialClone` using `rng := rand.New(rand.NewPCG(0, seed))`, ensuring every intermediate state is bounded between `base0` and `full100` (`TestReplayEndpointEquivalenceAndBoundaryContainment`). Note that while `0%` and `100%` bound every intermediate policy `p%`, two distinct intermediate percentages `p1 < p2` are not guaranteed to be bitwise subsets at every offset because retaining an unsynced directory at `p2` that was skipped at `p1` consumes additional RNG draws for its children.

In this model, `State.Clone` captures exact `fileSyncedBytes` snapshots at each file's latest `Sync`/`SyncData` (or `SyncTo` with `FullSync=true`). For intermediate retention policies (`1..99%`), for files whose directory entry is synced (`syncedSet[name] == true`), `deterministicPartialClone` reads `syncedBytes` from `base0` (`CrashClone(0)`), and consults `fileSyncedBytes` for unsynced directory entries. `deterministicPartialClone` partitions each file into 4KB blocks. Any 4KB block where the live file contents differ from the synced snapshot (`syncedBytes`) is sampled sequentially from the candidate's PCG PRNG stream (`rand.New(rand.NewPCG(0, seed))`) as directory entries are traversed in sorted lexicographical order (`sort.Strings`) and differing 4KB blocks are traversed in ascending offset order, deciding whether that block retains the unsynced live dirty bytes or reverts to the durable snapshot (`syncedBytes`). Unsynced directory entries are similarly sampled independently in sorted lexicographical order, as verified in `TestDeterministicPartialCloneStatisticalFidelity` and `TestReplayPartialCloneInPlaceOverwriteAfterSync`. Consequently, all retention policies `0..100%` (including `50%`) are **100% bit-for-bit seed-deterministic** (`TestReplayClonePartialRetentionSeedDeterminism` and `TestExplorePartialRetentionFiveRepeatZeroFlips`). The complete tuple, not a truncated hash, identifies a candidate and outcome.

**Strategies** rank *U* using only trace-derived features. They cannot observe
outcomes, history, or recovery results. The `internal/strategy` package implements
both topological/heuristic orderings (`B0`–`B6`), retention and equivalence-class
baselines (`R1`, `R2`), reachability-hazard ranking (`RHG / B7`), and
durability-contract guided ranking (`DCG / B8` and `DCG-FlagOnly`). This constraint
is enforced by Go import rules: `internal/strategy` must not import
`internal/oracle`, `internal/exp`, `internal/recovery`, or `internal/replay`.

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

2. **Clone**: call `State.Clone(p, seed)` (`internal/replay/replay.go:89-110`) on the
   replayed filesystem to produce the post-crash state across all three retention cases:
   - `p == 0`: delegates to `MemFS.CrashClone` with `UnsyncedDataPercent: 0` (RNG-free).
   - `p == 100`: delegates to `MemFS.CrashClone` with `UnsyncedDataPercent: 100` and `rand.New(rand.NewPCG(0, seed))` (seed-invariant because every `IntN(100) < 100` check succeeds unconditionally).
   - `1 <= p <= 99`: constructs `base0` (`UnsyncedDataPercent: 0`) and `full100` (`UnsyncedDataPercent: 100` with `rand.New(rand.NewPCG(0, 0))`), then runs `deterministicPartialClone` using `rng := rand.New(rand.NewPCG(0, seed))`, ensuring every intermediate state is bounded between `base0` and `full100` (`TestReplayEndpointEquivalenceAndBoundaryContainment`). Note that while `0%` and `100%` bound every intermediate policy `p%`, two distinct intermediate percentages `p1 < p2` are not guaranteed to be bitwise subsets at every offset because retaining an unsynced directory at `p2` that was skipped at `p1` consumes additional RNG draws for its children.

3. **Recover**: open Pebble on the cloned filesystem with:
   - An `Options.Logger` whose `Fatalf` panics instead of calling `os.Exit(1)`
     (Pebble's default), with `recover()` around the `Open()` call.
   - A timeout to detect hangs.
   - A journal: append "started" before each candidate and "finished" after.
     On restart, a started-but-not-finished candidate is recorded as a hang.

4. **Check**: run `DB.CheckLevels()`, confirm forward and backward iteration
   agree, dump all key-value pairs.

5. **Oracle**: compare the dump against the history ledger using the prefix
  (no-hole) rule. Legal states are whole-operation prefixes between the lower bound
  $L$ (the state after the last client operation that completed with `Sync: true`,
  or the initial state if no synchronous operation has completed) and the latest begun
  mutation at the cutoff. If recovered state is older than $L$, the oracle reports
  a `durably_committed` violation (Rule 1). Multi-key client batches must commit
  atomically: observing an interior state where only some mutations of a batch are
  present violates `atomic_batch` (Rule 2). A later Sync also covers earlier NoSync
  mutations. Compare whole states before diagnosing divergence so overwrites, deletes
  and repeated states remain legal. Emit exactly one outcome class per candidate.

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
- Discovery CSVs and SVG plots, written by `cracontest evaluate -discovery <csv> -plot <svg>`.
- Multi-seed benchmark metrics (`results/benchmark_summary.csv` and `results/benchmark_per_seed.csv`).
- Paired statistical analysis computed via `py -3.12 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9`.

## Cost Model

The cost per candidate breaks down into three components:

| Component | Dominant Factor | Measurement Point |
|-----------|----------------|-------------------|
| **Replay** | Filesystem size at event *k* (proportional to bytes written in events `0..k`) | Wall time per replayed event |
| **Clone** | Filesystem size (tree walk + data copy) | Wall time for `CrashClone()` |
| **Recovery** | WAL replay length + manifest complexity | Wall time for `pebble.Open()` + `CheckLevels()` |

**Measured cost** (Linux, amd64, 1,000-operation workload, see
`docs/reproduce.md`): the stored trace has about 2,500 events and 390 KB.
Replay plus clone at a single cutoff takes about 0.5 ms. A complete
exploration of the endpoint universe (2,150 candidates) takes about 14 s,
or about 6.5 ms per candidate including recovery, structural checks and the
oracle. Cost per candidate grows with trace length, because each crash state
replays a longer prefix. At this workload size every candidate is explored
once; for much larger workloads all strategies would share one fixed
stratified sample instead.

## Package Structure

```
CraConTest/
├── cmd/cracontest/       # CLI binary
├── internal/
│   ├── trace/            # Event schema, (de)serialization
│   ├── vfsrec/           # Recording vfs.FS wrapper
│   ├── replay/           # Trace replay into CrashableMem
│   ├── recovery/         # Panic-safe Open, timeout, checks
│   ├── faults/           # Seeded durability fault decorators (MUT-0 to MUT-9)
│   ├── graph/            # Persistence DAG, state view, and durability-contract features
│   ├── candidate/        # Candidate type and universe
│   ├── strategy/         # Ranking strategies (B0–B6, R1, R2, RHG, DCG, DCG-FlagOnly)
│   ├── workload/         # Seeded workload generators
│   ├── history/          # Client-visible operation ledger
│   ├── oracle/           # Consistency rules and outcomes
│   └── exp/              # Run specs, journal, evaluator, and multi-seed benchmark runner
├── analysis/             # Statistical comparison scripts and dependencies
│   ├── paired_stats.py   # Paired per-seed Wilcoxon & t-test analysis
│   └── requirements.txt  # Python dependencies (scipy)
├── experiments/configs/  # Workload specifications (main.json, smoke.json)
├── results/              # Stored run directories, discovery CSVs, benchmark_per_seed.csv, and plots
├── docs/                 # This document and others
└── paper/                # Publication materials (ADR 2026-10-27-a-paper-directory-policy: gitkeep only)
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
| `cracontest evaluate` | EVALUATE | Join rankings with outcomes, compute metrics; optionally write the discovery CSV and plot |
| `cracontest benchmark` | ALL | Run multi-seed evaluation across fault models (MUT-0..MUT-9) and write summary CSV and report |

## Stored Artifacts

| Artifact | Location | Git-tracked |
|----------|----------|-------------|
| Workload specifications | `experiments/configs/` | Yes |
| Run directory: `run.json`, `trace.json`, `history.json`, `universe.json`, `ranking_*.json`, `journal.ndjson`, `outcomes.json`, `curves.csv` | `results/<run-id>/` | Yes, for the runs the report uses |
| Discovery CSVs and plots | `results/discovery_*.csv`, `results/plot_*.svg` | Yes |
| Technical report | `docs/report.md` | Yes |
| Multi-seed benchmark summary and report | `results/benchmark_summary.csv`, `docs/benchmark-report.md` | Yes |

Trace, history, universe, ranking and outcome documents use schema_version=1
and ordered non-null arrays. Outcomes persist the complete candidate tuple.
Each run records its configuration, seeds, weights, fault identity, Pebble
version, Go version and platform. Run directories are explicit identifiers;
canonical run-ID hashing remains a separate design task.

## Validation and Open Risks

1. **Replay fidelity.** At 20 seeded cutoffs of a 1,000-operation recording, the
   replayed 100%-retention state equals the live state byte for byte, and every
   replayed state reopens (`TestGateG1`).
2. **Oracle soundness.** On unfaulted recordings every endpoint candidate is
   valid, so the oracle and recovery path raise no false alarms (`TestGateG2`).
3. **Trace volume.** Payloads are copied, so trace size grows with the bytes
   written: about 390 KB for the 1,000-operation workload. Larger workloads
   will need this measured again before choosing sizes.
4. **Concurrency.** With concurrent writers the commit order is decided by
   Pebble, not the harness. Current workloads use a single writer; concurrent
   workloads need the commit order recovered from WAL events before the
   prefix rule applies.

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
| 2026-10-22 | Cost, artifacts, validation | Replaced planned measurements with measured cost and validation results; artifact locations and CLI match the implementation. |
