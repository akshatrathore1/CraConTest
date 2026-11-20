# CraConTest Technical Report: Dependency-Guided Crash Testing on Pebble

**Date:** November 2026
**Subject:** Experimental evaluation of persistence-dependency-guided crash-point ranking on CockroachDB's Pebble key-value storage engine (v2.1.7).

---

## 1. Summary

Crash-consistency testing must choose which crash states to examine: every filesystem
event is a possible crash point, and each point has several possible post-crash states
depending on which unsynced data survives. CraConTest records one execution, rebuilds any
crash state from that recording, and evaluates whether rankings derived from persistence
dependencies and POSIX durability contracts find consistency failures with fewer crash
injections than random selection or simpler baselines.

Because `DCG`'s 0%-retention contract indicators (`AckSyncMissing`, `DanglingTableCount`—covering both unpersisted tables referenced by a committed manifest and uncommitted manifest edits via `manifestDirtyUncommitted`—`UnsyncedDirectoryRename`, and closed dirty log tracking via `HasClosedDirtyLog` / `DirtyLogBytes`) inspect the same missing `Sync`/`SyncData` barriers targeted by the synthetic sync-omission faults, $k_1 = 1.00$ on `MUT-1..9` and `MUT-X1..X5` reflects substantial by-construction alignment between the ranking features and injected faults, combined with `StateKey` equivalence deduplication. This alignment is an evaluation limitation, not evidence of fault-class-independent prediction. `ContractFeatures` in `internal/graph/features.go` defines these indicators alongside the partial-retention indicator `UnsyncedDirEntryCount`.

Crucially, as a central empirical finding, when evaluated on uninjected 50%-retention multi-entry directory races (`#5825` and `Checkpoint`), **frozen 0%-first `DCG` underperforms uniform random `B0`** (accounting-corrected medians $203.00$ vs. $178.73$ on #5825 and $288.00$ vs. $262.26$ on Checkpoint). The post-hoc partial-retention prior (`dcg-pr`, `UnsyncedDirEntryCount >= 2`) achieves very early discovery on #5825, but its corrected Checkpoint median is $245.00$.

Furthermore, **no failures were observed on `MUT-0` in the evaluated non-ingest workloads under endpoint (`0%/100%`) retention**: the 1,000-op TestGateG2 main run (internal/workload/gate_g2_test.go, DefaultConfig().Operations = 1000), the 32-seed MUT-0 benchmark, the 108-run TestNullControlMultiShape suite, and the 80-run TestWorkloadDiversityBenchmark MUT-0 suite. These are descriptive results for the tested configurations, not population-level false-alarm guarantees: candidates from the same trace are dependent, and the fixed workload shapes and seeds do not establish independent, identically distributed Bernoulli trials for a rule-of-three bound. db.Ingest workloads are evaluated separately in TestSubsystemDiscovery and TestCaseStudyIngestBenchmark. Enabling `db.Ingest` under the global prefix ("no-hole") oracle or enabling `50%` partial retention surfaces the cross-channel commit-order prefix divergence and `atomicfs` multi-entry directory races described in §9, §10, and §13.

Research question:

> *Can semantic persistence dependencies and durability-contract priors concentrate crash
> testing on fewer crash states while finding consistency failures faster than random selection?*

We address this question through an eight-stage empirical evaluation on CockroachDB's Pebble (v2.1.7):

1. **Stage 1 — Single-Seed 1,000-Operation Dense Faults & Topological Reachability Pathology (§2–§7):**
   - **Zero false alarms (MUT-0):** Every crash candidate recovered to a legal state (2,150 of 2,150 in the main run, 302 of 302 in the smoke run).
   - **Dense fault detection (MUT-1..MUT-4):** Every seeded fault was detected (28%–50% failure density), exposing lost acknowledged writes, malformed manifests, and corrupt tables.
   - **Topological reachability pathology:** Uniform random (`B0`) reached a first failure in a median of 1–3 crashes, whereas naive topological reachability (`B4`) required 13–61 crashes. Reachability is monotonically highest at the start of a trace, causing `B4` to expend its initial testing budget before the first synchronous write is acknowledged (event 57 of 2,427 in `main_mut1`), where rollback to the initial state is legal.
   - **Coverage advantage:** State-aware orderings (`B5`, `B6`) covered distinct violation signatures faster at moderate budgets than random selection on `MUT-1`.

2. **Stage 2 — 32-Seed In-Sample Benchmark (`MUT-0..9`) & `DCG-FlagOnly` Component Attribution (§8.1–§8.3):**
   - Across 32 deterministic seeds (>11,000 crash states), `DCG` achieves optimal $k_1 = 1.00$ [1.00, 1.00] across all active fault models (`MUT-1..6`, `MUT-8`, `MUT-9`), with 0 false alarms on `MUT-0`.
   - Across the 237 active seed-runs on `MUT-1..9`: `DCG` beats `R2` with `113 wins / 0 losses / 124 ties`, `DCG-FlagOnly` beats `R2` with `113 wins / 2 losses / 122 ties`, and `DCG` vs. `DCG-FlagOnly` ties on `235 of 237` active seed-runs (`2 wins / 0 losses / 235 ties`, differing on only 2 seeds of `MUT-3`), confirming that binary POSIX durability-contract priors combined with `StateKey` equivalence partitioning drive the speedup.

3. **Stage 3 — Parameterized & Stochastic Fault Generalization (`MUT-X1..X5`) and Out-of-Contract Control (`MUT-X6`) (§8.4):**
   - The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault families were committed to `internal/faults/faults.go` on `2026-11-01` (`c67a44a`, prior to tag `v1.0-dcg-pr-freeze`). The accounting-corrected `v1.1.2-evaluation-freeze` reevaluation records **246 wins / 1 loss / 48 ties** against `R2` on the 12 analyzed in-contract settings, with mean $k_1$ speedups of approximately $2.0\times\text{--}19.9\times$ and a single `MUT-X4_p04` loss; `MUT-X6` remains non-significant (Holm $p = 1.0$) and includes regressions.
   - Subsequently, `internal/strategy/dcg.go` and `internal/graph/features.go` were frozen at Git tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**, following the development and calibration of the partial-retention prior `DCG-PR` on Pebble `#5825` (§9). **Three strictly post-freeze evaluations** were designed and executed with 0 bytes of modification to the ranking or feature code: (1) Post-Freeze Evaluation 1 (Intra-Class Subsystem Transfer — checkpoint race later fixed upstream by `15b500fb`, §10), (2) Post-Freeze Evaluation 2 (Cross-Channel Ordering — Direct L0 `db.Ingest`, §13.5), and (3) Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §14).

4. **Stage 4 — Real-World Case Study 1: CockroachDB Pebble Issue `#5825` (`MUT-0` vs. `BARRIER-5825`) & Partial-Retention Prior (`DCG-PR`) (§9):**
   - Reproduces the failure reported in Pebble `#5825` under deterministic 50% partial retention with manifest rotation (58 failures across 25 of 32 seeds under `MUT-0`; no failures observed under the `BARRIER-5825` intervention). On the accounting-corrected freeze, `DCG-PR` reduces median $k_1$ from $203.00$ (0%-first `DCG`, mean $243.84$) and $178.73$ (`B0`, mean $192.72$) to **2.00** [2.00, 5.00] (mean **12.40** [0.00, 31.90], $S_{25} = 1.44$, $S_{100} = 1.44, AUC Failures = $0.6733$, AUC Bugs = $0.7562$).

5. **Stage 5 — Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race (fixed upstream by `15b500fb`) (`MUT-0` vs. `BARRIER-CKPT`) (§10):**
   - The accounting-corrected reevaluation reproduces 53 failures across 31 of 32 seeds under `MUT-0` and no failures under `BARRIER-CKPT`. Because `#5825` and Checkpoint share the marker directory-race mechanism, Case Study 2 evaluates intra-class subsystem transfer from `"db/"` to `"checkpoint/"`. `DCG-PR` records median $k_1 = \mathbf{245.00}$ [3.00, 306.00] (mean **156.39** [99.92, 212.86], $S_{25}=S_{100}=0.53$, AUC Failures = $0.5010$, AUC Bugs = $0.6321$), with 22 wins, 9 losses, and 0 ties against R2.

6. **Stage 6 — Candidate Universe Scaling (`120..1,200` ops) & Multi-Shape Null Control (`108` runs) (§11–§12):**
   - On `MUT-5`, as candidate universes grow from ~338 to ~2,958 states, `DCG` maintains $k_1 = 1.00$ at all four evaluated operation counts (planning time $\le 63.0\text{ ms}$), while the measured discovery costs of `R2`, `R1`, `B0`, and `LIFO` increase. This finite experiment does not establish asymptotic complexity. Across 108 unfaulted runs spanning 12 workload shapes (>50,000 crash states), there are **0 observed failures** (0 violations, 0 open errors, 0 panics, 0 hangs). Correlated candidates and fixed test configurations do not support an independent-trial confidence bound on a population false-alarm rate.

7. **Stage 7 — Pebble Subsystem Source Audit, Workload Extension & Empirical Crash-Consistency Evaluation under Unfaulted MUT-0 (§13.1–§13.4):**
   - Targeted source audit of Pebble v2.1.7 ingestion, format-version markers, atomic markers, and checkpoints (`ingest.go:1490`, `version_set.go:649`, `commit.go:289`, `format_major_version.go:486`, `vfs/atomicfs/marker.go:189`, and `checkpoint.go` lines 287, 596, 448).
   - Evaluated Workload A (Ingestion), Workload B (Format Ratcheting), and Workload C (Combined Stress) under `MUT-0` across policies `[0, 50, 100]` (5,415 candidate states in `results/subsystem_discovery.csv`). Across all 5,415 candidate states, Pebble v2.1.7 exhibits **0 `durably_committed` data-loss violations and 0 recovery panics**, with no durably-committed data loss observed in these workloads. Simultaneously, the audit exposed two distinct recovery hazards under `MUT-0`: (1) an **input-dependent cross-channel `prefix` monotonicity hole** during non-overlapping Direct L0 `db.Ingest` (`40` prefix violations in Workload A, analyzed across 32 seeds in §13.5), and (2) **50%-retention intra-operation directory races (`open_error`)** when a crash occurs inside the multi-entry window between `MANIFEST-*`/`.sst` creation and `dirFD.Sync()` (19 total open errors).

8. **Stage 8 — Direct L0 `db.Ingest` 32-Seed Case Study, Barrier Intervention (`BARRIER-INGEST`) & Multi-Distribution Workload Generalization (§13.5, §14):**
   - **Cross-Channel Prefix Hole:** Under unpatched `MUT-0` (`BarrierIngest: false`), direct L0 ingest bypasses WAL, incrementing sequence number $s_2 > s_1$ and committing via MANIFEST fsync while earlier un-synced WAL writes at $s_1$ remain unpersisted. Across 32 seeds, under endpoint (`0%/100%`) retention, **22 of 32 seeds** exhibit prefix violations (276 total failures; seeds `3, 5, 7, 8, 9, 16, 20, 23, 26, 29` have 0 failures because randomly generated ingest keys overlapped the active memtable and took the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`). `DCG-PR` and `DCG` achieve median/mean $k_1 = 9.00/15.32$, compared with R2 26.77/27.00 and accounting-corrected B0 33.74/34.32. The DCG-PR/R2 paired comparison is 13 wins, 6 losses, and 3 ties (sign $p=0.0835$), so the lower mean is descriptive rather than significant at 0.05.
   - **Barrier Intervention (`BARRIER-INGEST`):** Pre-ingest WAL flush via `db.LogData(nil, pebble.Sync)` commits a zero-byte sync record forcing `SyncData()` on the active WAL before MANIFEST version edit. Under `BARRIER-INGEST`, no prefix violations were observed across the 32 evaluated seeds ($S_{25}=0.00, S_{100}=0.00$), compared with 276 failures across 22 seeds without the intervention.
   - **Multi-Distribution Workload Generalization:** Across 5 presets, 16 seeds, and 4 fault models, unfaulted MUT-0 produces zero failures in all 80 trials. On the 14 failing preset/fault settings, DCG has lower mean $k_1$ than corrected B0 on 14/14 and R2 on 13/14, with a single regression on `delete-heavy/MUT-1` (3.00 vs. 2.37). DCG $S_{25}$ matches or exceeds R2 on 14/14 (strictly higher on 9, tied on 5). Representative sparse-fault means include `ycsb-f/MUT-5` DCG/R2/B0 1.00/8.49/39.71 and `overwrite-heavy/MUT-5` 1.00/9.40/22.99.

---

## 2. Method

### 2.1 System under test

For detailed architectural component breakdowns and the formal VFS crash consistency model, see [docs/architecture.md](architecture.md) and [docs/crash-model.md](crash-model.md).

- **Engine:** Pebble `v2.1.7` (`github.com/cockroachdb/pebble/v2`), unmodified [15].
- **Recording:** Pebble runs on `vfs.NewCrashableMem()` wrapped by a recording filesystem that logs
  every mutating call with its payload, write category and open-handle identity, together
  with client operation markers and engine listener events.
- **Crash states:** for a cutoff *k* the trace prefix `1..k` is replayed into
  `vfs.NewCrashableMem()` and cloned with a retention policy of 0% or 100% of the unsynced
  data. Pebble is reopened on the clone with a panic-safe logger and a timeout, the
  structure is checked (`CheckLevels`, forward/backward iteration), and the contents are
  dumped.
- **Oracle:** the recovered contents must equal one whole client state between the state
  after the last acknowledged synchronous write and the state after the last write begun
  before the cutoff. A match below that bound is `durably_committed`; a match with an
  interior state of a batch is `atomic_batch`; anything else is `prefix`. Open errors,
  panics and hangs are separate outcome classes.
- **Signatures:** a violation's signature hashes its rule, the kind of the first divergent
  operation, and the write categories of the files and directory entries pending at the
  cutoff. Violations with the same rule and operation therefore stay distinct when
  different kinds of files were exposed at the crash.
- **Equivalence Partitioning and StateKey:** For candidate deduplication, CraConTest computes canonical persistence states (`StateKey`). `StateKey` represents a two-sided approximation of storage engine state: it is finer-grained than the physical disk image on pending namespace entries because `keyPending` incorporates the creating `trace.Event` ID, but coarser than the client-level oracle because it tracks aggregate dirty byte intervals without distinguishing which specific client transaction acknowledged the writes or the full history of overwritten byte spans. Because `StateKey` hashes only VFS persistence state and does not encode client `OpBegin`/`OpEnd` boundaries, two cutoffs sharing a `StateKey` (such as before and after a client `OpEnd` with no intervening VFS mutation, as tested in `TestStateKeyIgnoresOpBeginAndOpEnd` in `internal/graph/graph_test.go`) yield identical 0%-retention disk images, but can be evaluated against different oracle `[L, U]` prefix bounds if a client operation was acknowledged between them. Therefore, `StateKey` representative selection (`R2`, `DCG`) is an equivalence reduction over the physical persistence state rather than an isomorphism over oracle verdicts.

### 2.2 Workloads

| Name | Config | Operations | Keys | Value size | Sync | Flush |
|---|---|---|---|---|---|---|
| main | `experiments/configs/main.json` | 1,000 | 64 | 128 B | every 10th mutation | every 100 ops |
| smoke | `experiments/configs/smoke.json` | 100 | 16 | 64 B | every 10th mutation | every 50 ops |

Operation mix for both: 50% set, 20% get, 10% delete, 10% two-key batch, 5% range delete,
5% iteration. One sequential client; base seed 42.

### 2.3 Faults

Each fault makes one class of durability barrier return success without syncing, before
the call reaches the recorder, so the trace shows the barrier as missing.

| Fault | Barrier dropped |
|---|---|
| MUT-0 | none (null control) |
| MUT-1 | `Sync`/`SyncData` on write-ahead log files (`pebble-wal`, created or reused) |
| MUT-2 | `Sync`/`SyncData` on manifest files (`pebble-manifest`) |
| MUT-3 | `Sync` on directory handles |
| MUT-4 | `Sync`/`SyncData` on tables written by memtable flushes (`pebble-memtable-flush`) |

The table above lists the Stage 1 dense faults. The full CraConTest evaluation framework encompasses 18 fault and control configurations, including single-sync and windowed faults (`MUT-5` through `MUT-9`, detailed in §8), holdout and out-of-contract faults (`MUT-X1` through `MUT-X6`, detailed in §8.4), and barrier controls (`BARRIER-5825`, `BARRIER-CKPT`, `BARRIER-INGEST`, detailed in §9, §10, and §13.5). See [docs/architecture.md](architecture.md) and `internal/faults/` for full definitions.

### 2.4 Candidate universe

Cutoffs are selected from the trace (the pretrace state, every namespace mutation, every
write with a positive length, every successful barrier, the event just before each
successful barrier or rename, and the end of every mutating client operation), each
combined with the 0% and 100% retention policies and one derived seed. Every candidate is
executed exactly once; strategies are compared offline against the stored outcome table
and never see outcomes.

### 2.5 Strategies

1. **B0 random:** a seeded shuffle of the universe. Budget curves average 10 fixed seeds
   (1, 2, 3, 4, 5, 42, 100, 777, 1337, 2026); discovery positions use 1,000 seeds
   (10000–10999).
2. **B4 combined:** greedy order by `Desc + CrossFile + Semantic + Novelty` (all weights 1),
   where `Desc` and `CrossFile` count later persistence events reachable in the dependency
   graph, `Semantic` marks events inside client writes, with a write category, or named by
   an engine event, and `Novelty` rewards a not-yet-selected persistence state.
3. **B5 representative:** the first candidate of each distinct (persistence state,
   retention) group, then the rest, both in trace order.
4. **B6 sync-boundary:** candidates immediately before a successful sync or rename first,
   then the rest, both in trace order.

### 2.6 Measures

- **Discovery curves:** failures and distinct violation signatures within the first *n*
  candidates of each ranking.
- **Discovery positions:** the position in a ranking of the first failure of any class,
  and the position at which every failure kind has appeared. A failure kind is a
  violation's signature, or an open error, panic or hang identified by its class and its
  error text with numbers removed. Positions count all failures, so they also cover faults
  that make recovery fail rather than lose data.

---

## 3. Acceptance Results

| Gate | Result |
|---|---|
| G1 replay fidelity | 1,000-op recording, 2,322 events (225 KB trace). At 20 seeded cutoffs the replayed 100%-retention state is byte-identical to the live state, and every replayed state reopens. Replay plus clone ≈0.5 ms per cutoff. |
| G2 null control | Every endpoint candidate of a fresh 1,000-op MUT-0 recording (≈2,140 candidates) is valid: 0 violations, open errors, panics, hangs or oracle errors (≈7 s). |
| G3 graph labels | On a 1,000-op trace (≈2,510 events, ≈1,073 cutoffs) every candidate has an operation label and a category set; ≈173 distinct persistence states; build time ≈12 ms. Binary `Semantic` saturated at 96.28% (1,034 / 1,074 cutoffs), providing minimal discrimination in Stage 1 and motivating the Stage 2 replacement by 5-tier `CutoffTier` (`SemanticTier`) and `ContractFeatures` while preserving v1 `Semantic` for B4 backward compatibility (§5, §6). |
| G4 discovery curves | Curves and positions for B0, B4, B5 and B6 are computed from the stored outcome tables, without re-executing any candidate. |

Counts in G1–G3 vary by a few events between recordings because Pebble schedules background
work itself; every reported experiment is computed from a stored trace.

---

## 4. Results

### 4.1 Outcomes per fault (main workload)

| Run | Events | Candidates | Failing | Failure class | Failure kinds |
|---|---|---|---|---|---|
| `main_mut0` | 2,510 | 2,150 | 0 (0%) | — | 0 |
| `main_mut1` WAL sync | 2,427 | 1,978 | 865 (44%) | `durably_committed` violation | 15 |
| `main_mut2` manifest sync | 2,494 | 2,112 | 1,047 (50%) | open error: malformed manifest | 1 |
| `main_mut3` directory sync | 2,480 | 2,048 | 999 (49%) | `durably_committed` violation | 3 |
| `main_mut4` flushed-table sync | 2,500 | 2,124 | 594 (28%) | open error: table size differs from manifest | 2 |

Under MUT-3 the crash loses directory entries that were never synced, so recovered data is
missing although the files themselves were synced. Under MUT-2 and MUT-4 Pebble refuses to
open, which is the correct reaction to a damaged manifest or table: these are durability
failures reported as open errors rather than silently wrong contents.

### 4.2 Discovery positions

Positions are 1-based; lower is better. Random: median over 1,000 seeds with the
10th–90th percentile. Data: `results/<run>/positions.csv`.

**Table 1. First failure of any class.**

| Run | Random | B6 sync-boundary | B5 representative | B4 combined |
|---|---|---|---|---|
| `main_mut1` | 2 [1–5] | 17 | 25 | 55 |
| `main_mut2` | 1 [1–4] | 5 | 13 | 13 |
| `main_mut3` | 2 [1–4] | 9 | 25 | 39 |
| `main_mut4` | 3 [1–7] | 39 | 51 | 61 |
| `smoke_mut1` | 2 [1–6] | 17 | 25 | 55 |

**Table 2. All failure kinds found.**

| Run (kinds) | Random | B6 sync-boundary | B5 representative | B4 combined |
|---|---|---|---|---|
| `main_mut1` (15) | 853 [431–1,450] | 1,643 | **189** | 1,703 |
| `main_mut2` (1) | 1 [1–4] | 5 | 13 | 13 |
| `main_mut3` (3) | 27 [9–64] | 49 | 77 | 49 |
| `main_mut4` (2) | 17 [5–60] | 63 | 81 | 65 |
| `smoke_mut1` (6) | 199 [103–266] | **31** | **59** | 197 |

### 4.3 Budget curves for MUT-1 (main run)

**Table 3. Distinct violation signatures within a budget of *n* crashes** (B0: mean over 10
seeds with [min, max]).

| Strategy | 10 | 25 | 50 | 100 | 200 |
|---|---|---|---|---|---|
| B0 random | 3.0 [1, 4] | 5.0 [3, 8] | 7.5 [6, 9] | 9.1 [7, 11] | 11.5 [9, 13] |
| B4 combined | 0 | 0 | 0 | 3 | 4 |
| B5 representative | 0 | 1 | 3 | 7 | **15** |
| B6 sync-boundary | 0 | 4 | **9** | **13** | 14 |

**Table 4. Violations within a budget of *n*.**

| Strategy | 10 | 25 | 50 | 100 | 200 |
|---|---|---|---|---|---|
| B0 random | 4.1 [1, 6] | 9.6 [7, 12] | 19.5 [16, 26] | 41.3 [38, 48] | 85.1 [67, 102] |
| B4 combined | 0 | 0 | 0 | 22 | 72 |
| B5 representative | 0 | 1 | 12 | 37 | 83 |
| B6 sync-boundary | 0 | 5 | 17 | 41 | 62 |

Figures: `results/plot_main_mut1.svg` and the other `results/plot_main_mut*.svg`.
Data: `results/discovery_main_mut*.csv`.

### 4.4 Cost

Replay, crash, recovery and oracle check take about 6.5 ms per candidate on the main
workload (2,150 candidates in 14 s) and about 1 ms on the smoke workload. Per-candidate
cost grows with trace length because each crash state replays a longer prefix.

---

## 5. Discussion

1. **Dense faults favour random order for the first failure.** Under every fault, failures
   begin early and then make up 28–50% of the remaining candidates, so a random draw finds
   one within a few crashes. No ordering can improve much on that, and every guided order
   spends its first candidates on early cutoffs or on a fixed group order.
2. **Why B4 is slowest.** The combined score weights downstream reachability. The events
   with the largest reach are the earliest ones, so B4 starts with cutoffs before the first
   acknowledged synchronous write (event 57 of 2,427 in `main_mut1`), where no
   acknowledged data can be lost. The dependency structure is correct, but reachability
   from an early event does not indicate that a crash there can violate a client guarantee.
3. **Coverage of distinct signatures.** B5 visits one candidate per distinct persistence
   state first, and B6 visits barrier-adjacent cutoffs first. Both spread their early
   budget across different pending-file situations, and violation signatures record the
   categories of pending files. That is why B5 and B6 collect distinct MUT-1 signatures
   faster than random, but it is also partly circular: a signature is defined by part of
   the state that B5 groups by. The result means that state diversity yields signature
   diversity. It does not show that these orderings find more distinct underlying bugs.
   With faults that have only one to three failure kinds (MUT-2 to MUT-4), random order
   covers them as fast as or faster than any guided order.
4. **The Semantic term saturates.** v1 binary `Semantic` is 1 on 96.28% of cutoffs (`1,034 / 1,074` cutoffs on the 1,000-op trace) because almost every event carries a write category, so it contributes little to B4's order. This saturation directly motivated replacing binary `Semantic` in Stage 2 with the 5-tier `CutoffTier` (`SemanticTier`: `TierManifestCritical`, `TierSyncBoundary`, `TierDirectoryMeta`, `TierDataWrite`, `TierBackground`) and explicit `ContractFeatures` in `internal/graph/features.go`, while keeping `features.Semantic` unchanged for Stage 1 `B4` backward compatibility.

---

## 6. Stage 1 Limitations

1. **One workload and one seed.** A single YCSB-A-like mix at 1,000 operations (plus a
   100-operation smoke run); no repeated workload seeds, so no confidence intervals beyond
   the spread of random orders.
2. **Dense faults only.** Each fault drops a whole class of barriers, so a large share of
   crash points fail. A fault that affects only a few crash points has not been evaluated.
3. **Signature definition.** Distinct-signature counts depend on the categories included
   in signatures, which overlap with the state used by B5 (§5).
4. **Endpoint retention only.** 0% and 100% retention; intermediate policies were not
   evaluated.
5. **In-process recovery and a VFS-level crash model.** Exploration runs in one process with
   a timeout, and crashes are simulated at the filesystem interface; device-level behaviour
   is out of scope.
6. **Binary Semantic saturation.** v1 binary `Semantic` saturated at 96.28% (`1,034 / 1,074` cutoffs), failing to discriminate critical metadata commits from background data writes until replaced by Stage 2's 5-tier `CutoffTier` and POSIX `ContractFeatures`.

---

## 7. Conclusion

The Stage 1 evaluation established that the crash testing pipeline replays traces deterministically and detects injected sync omissions with zero false alarms on the unfaulted control trace: crash states replay deterministically, unfaulted runs produce zero false alarms, and each seeded dense durability bug is exposed through lost acknowledged writes or refused recovery. However, ranking results on dense faults revealed clear structural limitations: uniform random reaches a first failure fastest under dense faults, while naive topological reachability scores are consistently slowest due to monotonic descendant bias in the pre-acknowledgment prefix. State-aware orders (B5, B6) accelerate the discovery of distinct violation contexts, but their first-failure efficiency remains bounded on dense mutations.

### 7.1 Scope of StateKey Equivalence Partitioning

To enable representative crash-candidate deduplication without executing redundant states, CraConTest partitions candidates by canonical persistence state (`StateKey`). It is essential to recognize that `StateKey` acts as a two-sided approximation:
- **Finer than the disk image on pending entries:** Because `keyPending` includes the `trace.Event` ID at which the namespace mutation occurred, two crash states with identical pending files created at different event points yield distinct `StateKey`s.
- **Coarser than the consistency oracle on dirty spans and client acknowledgment:** `StateKey` tracks dirty byte intervals and unsynced directory entries without distinguishing which client operation acknowledged the writes or the full history of overwritten byte spans. Because `StateKey` hashes only VFS persistence state and does not encode client `OpBegin`/`OpEnd` boundaries, two cutoffs sharing a `StateKey` (such as before and after a client `OpEnd` with no intervening VFS mutation, as tested in `TestStateKeyIgnoresOpBeginAndOpEnd` in `internal/graph/graph_test.go`) yield identical 0%-retention disk images, but can be evaluated against different oracle `[L, U]` prefix bounds if a client operation was acknowledged between them. Therefore, `StateKey` representative selection (`R2`, `DCG`) is an equivalence reduction over the physical persistence state rather than an isomorphism over oracle verdicts.

These findings directly motivated the Stage 2 investigation presented in Section 8: evaluating ranking strategies on sparse and lifecycle-triggered faults where most crash points are harmless (e.g. dropping a single $k$-th WAL sync or second manifest sync), evaluating across 32 deterministic workload seeds with 95% confidence intervals, and replacing blind graph reachability with explicit POSIX durability-contract priors (`internal/graph/features.go`).

---

## 8. Multi-Seed Benchmark, Root Causes, and Contract-Guided Evaluations

For complete per-seed metric dumps, individual benchmark logs, and paired statistical test scripts, refer to [docs/benchmark-report.md](benchmark-report.md) and `results/`.

### 8.1 Why Naive Topological Reachability (B4) Underperforms Random on Dense Faults

The Stage 1 evaluation established that the combined dependency heuristic (**B4**) underperformed uniform random selection (**B0**) on MUT-1, finding a first violation at median position 55 compared to B0's median position 2. Our root-cause investigation identified two fundamental structural defects in topological reachability heuristics:

1. **Monotonic Downstream Reachability Bias:** In any directed acyclic graph capturing execution dependencies, topological depth correlates inversely with downstream reachability. The earliest mutations reach the greatest total number of subsequent vertices ($|\text{Desc}(v)|$ is monotonically decreasing across the trace). By weighting `Desc`, `B4` systematically preferred the earliest crash candidates in the trace.
2. **Pre-Acknowledgment Semantics:** Storage engine crash-consistency contracts (e.g. POSIX `fsync` and WAL commit guarantees) strictly apply *after* a client write has been acknowledged. In the initial portion of a workload trace (prior to the first acknowledged synchronous write at event 57 of 2,427), any crash state legally recovers to the pre-workload state without violating durability. `B4` spent its first 50 selections exploring exclusively inside this pre-acknowledgment region, where no consistency violations could legally exist.
3. **Candidate Universe Uniformity Within Fault vs. Variation Across Faults:** Within each `(seed, fault)` execution, all ranking strategies evaluate an identical candidate universe. Across fault models, however, candidate universe sizes vary (e.g. 1,978 to 2,150 candidates on the 1,000-op run; ~338 to ~2,958 on scaling runs) because `faults.FS` intercepts and drops specific sync barriers before they reach `vfsrec.FS`, altering the number of recorded events and cutoffs in the trace.

### 8.2 Ranking Strategies and Baselines

To address these limitations, the expanded evaluation incorporates principled space partitioning and concrete durability contract invariants:

1. **Rigorous Baselines (R1 and R2):**
   - **R1 (0%-Retention-First):** Recognizes that storage consistency violations manifest primarily when dirty unpersisted bytes are dropped upon crash. `R1` partitions the candidate universe by retention policy and exhaustively tests 0% retention before testing 100% retention.
   - **R2 (State-Representative Random):** Partitions 0% retention candidates into equivalence classes by canonical persistence state key (`StateKey`), selecting boundary-adjacent representatives within each `StateKey` class using graph features (`BeforeBoundary`, `BarrierProximity`, and canonical order `candidate.Less`) before shuffling classes and evaluating redundant cutoffs afterward. Note that `R2` is CraConTest's internal `StateKey` equivalence-class baseline—**not** a re-implementation of Pathfinder's full C++/LLVM syscall-function subgraph isomorphism and DPOR model checker—so comparisons against `R2` measure the intra-framework gain of adding POSIX durability-contract priors onto `StateKey` partitioning rather than a head-to-head comparison against the Pathfinder tool. On sparse faults such as `MUT-5`, `R2` cuts search cost by half ($k_1 = 13.01$ vs `B0`'s $25.97$).
2. **RHG (Reachability-Hazard Guided / B7):**
   - Eliminates representative key divergence by maintaining strict deterministic grouping and ranks candidates by reachable uncommitted hazard surfaces, achieving normalized AUC values between $0.70$ and $0.82$.
3. **DCG (Durability-Contract Guided / B8) & DCG-FlagOnly (Ablation):**
   - Trace-derived durability-contract analysis that directly encodes the contract indicators as POSIX durability-contract priors instantiated for Pebble:
     - **Indicator 1 (`AckSyncMissing`):** Detects dropped WAL sync barriers following client-acknowledged transactions.
     - **Indicator 2 (`DanglingTableCount`):** Identifies dangling SST references where the manifest references an unpersisted table (gated on manifest commitment `fi.committedInManifest`) as well as uncommitted MANIFEST edits tracked via `manifestDirtyUncommitted` and `closedWhileDirty` setting `MaskDanglingTable`.
     - **Indicator 3 (`UnsyncedDirectoryRename`):** Flags directory hazards where critical namespace files (`CURRENT`, `MANIFEST`) or post-ack active WAL/options files cross directory boundaries without parent directory sync (`len(ackSyncOps) > 0`).
     - **Lifecycle / Partial Indicators (`HasClosedDirtyLog` & `UnsyncedDirEntryCount`):** Tracks WAL files closed while holding dirty unpersisted bytes (`HasClosedDirtyLog` / `DirtyLogBytes`) and multi-entry unsynced directory populations (`UnsyncedDirEntryCount`) for partial-retention races.
   - **Performance & Trace-Contract Alignment:** The 0%-retention indicators inspect the same missing barriers targeted by `MUT-1..6`, `MUT-8`, `MUT-9`, and `MUT-X1..X5`; $k_1 = 1.00$ therefore measures by-construction alignment plus `StateKey` equivalence deduplication. The `DCG-FlagOnly` ablation ties full `DCG` on nearly every setting, attributing most of the measured rank-position difference to the binary flags and partitioning. No failures are observed on the 32 evaluated `MUT-0` endpoint runs.

### 32-Seed Benchmark Summary Tables

> **Note on Per-Seed Shuffle Aggregation:** Within each workload seed (`RunSingleSeedFault` in `internal/exp/benchmark.go`), `R1`, `R2`, and `DCG-FlagOnly` average metrics across 100 shuffles (`seed*1000 + i*31 + 17`), whereas `B0` records the per-seed median across 100 fixed shuffles (`i*31 + 17`) before computing the cross-seed mean and median across the 32 workload seeds (making `B0` a stricter, lower-$k_1$ baseline than the analytical mean expectation $E[K_1] = (N+1)/(m+1)$, e.g., $2.00$ vs. $2.73$ on `MUT-1`). Note that while `benchmark.go` defaults to 100 shuffles (`B0PercentileCnt: 100`), `casestudy_ingest_test.go:429` and the 16-seed benchmark in `workload_diversity_test.go:303` configure `B0PercentileCnt: 50` (and the smoke check in `workload_diversity_test.go:50, 75` configures `B0PercentileCnt: 10`). Furthermore, `analysis/paired_stats.py` reports the 100-shuffle means (`k1_failure_mean`, matching the summary tables) for strategy mean columns and speedup ratios while computing paired sign/Wilcoxon tests and W/L/T tallies on the canonical single-seed rankings (`k1_single`).

Across 32 deterministic workload seeds (>11,000 evaluated crash candidates):

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

> **Note on MUT-6 $S_{25}$ Denominator:** In `benchmark_summary.csv` and the summary tables above, $S_{25}$ and $S_{100}$ are averaged across all 32 evaluated seeds ($52/32 = 1.62$). In `analysis/paired_stats.py`, paired statistical tests condition strictly on the 13 failing seeds with non-zero failure density ($m > 0$), reporting conditional mean $S_{25} = 4.00$ ($52/13$) for DCG (and $1.07$ vs. $2.64$ for R2). Both metrics describe the identical underlying run data.

### POSIX Durability Priors & DCG-FlagOnly Ablation

1. **General POSIX Durability-Contract Priors:**
   Under POSIX filesystem semantics, ensuring durable file persistence after crash requires two conditions: (1) flushing dirty file data (`fsync` / `fdatasync`), and (2) flushing the parent directory entry (`fsync(parentDir)`) after file creation, renaming, hard-linking, or directory creation (`Create`, `ReuseForWrite`, `Rename`, `Link`, `MkdirAll`). If a file is written and its data synced, but its parent directory entry remains unsynced, a crash at 0% retention causes the directory entry to vanish, resulting in lost files upon recovery. CraConTest implements POSIX durability-contract priors instantiated for Pebble's VFS categories: tracking `parentDir` and `dirEntrySynced` for every filesystem node, enforcing that durability holds if and only if `dirtyBytes == 0 && dirEntrySynced == true`, and scoping directory sync events strictly to the target directory path. MUT-9 is dense (41.4% density) at $k=1$ (initial directory sync) and sparse (0.8%–12.4%) at $k \in \{7, 9, 10, 12\}$ (flush/compaction directory syncs); directory-entry tracking was added after inspecting MUT-9, so MUT-9 evaluates parameterized $k$-generalization within the directory-sync family. On MUT-9 ($k=1$), DCG identifies the uncommitted directory entry prior, expanding state coverage to $S_{100} = 27.00$ (vs B0's $9.83$, a 2.7x expansion) and $S_{25} = 25.00$ (vs B0's $3.03$, an 8.2x early-budget advantage).

2. **Component Attribution & DCG-FlagOnly Ablation:**
   Comparing full DCG against the **DCG-FlagOnly** ablation isolates the exact contribution of each mechanism:
   - **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and $12.88$ on MUT-8). On dense faults (`MUT-1`, `MUT-2`, `MUT-4`), sparse faults (`MUT-5`, `MUT-8`), and directory faults (`MUT-9`), DCG-FlagOnly achieves the identical optimal $k_1 = 1.00$ [1.00, 1.00] as full DCG, confirming that the contract indicators (POSIX durability-contract priors instantiated for Pebble) are the primary driver of first-failure cost reduction.
   - **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop, ~0.8% density), where full DCG achieves mean $k_1 = 1.00$ compared to $1.20$ for `DCG-FlagOnly`.
   - **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

3. **$S_{25}$ Early-Budget Discovery, Crash Efficiency ($CE_{25}$), & The ~40-State Equivalence Ceiling:**
   In a 120-operation workload, there are only ~35–40 distinct canonical `StateKey` persistence states under 0%-retention crashes. Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100 (reaching identical $S_{100} = 23.97$ on `MUT-1`, $33.00$ on `MUT-2`, $27.00$ on `MUT-3`, $21.69$ on `MUT-4`, and $27.00$ on `MUT-9`). Disclosing our experimental design evolution: $k_1$ and early-budget coverage $S_{25}$ ($B = 25$) were specifically adopted after observing that $S_{100}$ saturates the available equivalence-class universe on 120-op traces, obscuring early discovery differences. To normalize early-budget discovery across budgets, we report **Crash Efficiency ($CE_{25} = S_{25} / 25$)**: DCG achieves near-optimal efficiency within 25 tests (e.g. on `MUT-1`, $S_{25} = 23.94 \implies CE_{25} = 0.958$ vs. R2's $17.12 \implies CE_{25} = 0.685$ and B0's $6.12 \implies CE_{25} = 0.245$; on `MUT-2`, $S_{25} = 25.00 \implies CE_{25} = 1.000$ vs. R2's $0.846$ and B0's $0.226$; on `MUT-8`, $S_{25} = 12.88 \implies CE_{25} = 0.515$ vs. R2's $0.270$ and B0's $0.080$; on `MUT-9`, $S_{25} = 25.00 \implies CE_{25} = 1.000$ vs. R2's $0.711$ and B0's $0.121$), demonstrating that $S_{25}$ and $CE_{25}$ serve as the critical discriminant metrics for practical CI testing with a 2x–8x throughput advantage over random selection.

### 8.4 Parameterized & Stochastic Fault Generalization (`MUT-X1..X5`) and Out-of-Contract Control (`MUT-X6`)

The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault families were committed to `internal/faults/faults.go` on `2026-11-01` (`c67a44a`, prior to tag `v1.0-dcg-pr-freeze`) to evaluate whether the four binary POSIX durability-contract flags generalize across arbitrary sync-drop indices $k$ and drop probabilities $p$ without re-tuning. The accounting-corrected evaluation at `v1.1.2-evaluation-freeze` reports **246 wins / 1 loss / 48 ties** against `R2` across the 12 analyzed in-contract settings, with mean $k_1$ speedups of approximately $2.0\times\text{--}19.9\times$ and Holm-adjusted sign-test $p \le 0.006$; the single loss occurs on stochastic `MUT-X4_p04`.

Subsequently, `internal/strategy/dcg.go` and `internal/graph/features.go` were frozen at Git tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**, and **three strictly post-freeze evaluations** were designed and executed with 0 bytes of modification to the ranking or feature code:
1. **Post-Freeze Evaluation 1 (Intra-Class Subsystem Transfer — checkpoint race later fixed upstream by `15b500fb`, §10)**: Evaluated across 32 seeds (`MUT-0` vs. `BARRIER-CKPT`).
2. **Post-Freeze Evaluation 2 (Cross-Channel Ordering — Direct L0 `db.Ingest` Cross-Channel Commit-Order Hole, §13.5)**: Evaluated across 32 seeds (`MUT-0` vs. `BARRIER-INGEST`).
3. **Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §14)**: 5 workload presets (`ycsb-a`, `ycsb-b`, `ycsb-f`, `overwrite-heavy`, `delete-heavy`) evaluated across 16 seeds and 4 fault models.

Because all parameterized and stochastic fault settings in Table 8.4 involve only 0%- and 100%-retention crash states (no partial-retention intermediates), `dcg-pr` produces an identical ranking to `dcg` on these settings; the table therefore reports `DCG / DCG-PR` as a single column.

| Fault | Fault Family | Seeds Fail (Density) | DCG / DCG-PR — Median $k_1$ [95% CI] | DCG / DCG-PR — Mean $k_1$ | Sync-Boundary (B6) — Median [Mean] | State-Representative (B5) — Median [Mean] | Uniform Random (B0) — Median [Mean] | R2 — Median [Mean] | R1 — Median [Mean] |
|---|---|---|---|---|---|---|---|---|---|
| MUT-X1\_k2 | Delayed WAL Sync ($k=2$) | 32/32 (2.6%) | **1.00** [1.00, 1.00] | **1.00** | 19.00 [19.00] | 27.00 [27.00] | 25.00 [24.14] | 13.09 [13.02] | 16.86 [16.94] |
| MUT-X1\_k3 | Delayed WAL Sync ($k=3$) | 31/32 (2.2%) | **1.00** [1.00, 1.00] | **1.00** | 21.00 [21.00] | 29.00 [29.00] | 30.00 [36.52] | 12.94 [13.01] | 18.95 [22.65] |
| MUT-X1\_k4 | Delayed WAL Sync ($k=4$) | 12/32 (1.1%) | **1.00** [1.00, 1.00] | **1.00** | 23.00 [23.67] | 31.00 [31.67] | 69.75 [62.21] | 13.25 [13.13] | 40.37 [40.50] |
| MUT-X1\_k6 | Delayed WAL Sync ($k=6$) | 32/32 (2.7%) | **1.00** [1.00, 1.00] | **1.00** | 35.00 [35.00] | 43.00 [43.00] | 26.50 [25.95] | 13.46 [13.38] | 16.88 [17.04] |
| MUT-X1\_k9 | Delayed WAL Sync ($k=9$) | 10/32 (1.6%) | **1.00** [1.00, 1.00] | **1.00** | 41.00 [43.40] | 49.00 [51.40] | 49.50 [59.95] | 12.55 [12.71] | 28.45 [34.04] |
| MUT-X2\_k1 | Delayed Manifest Sync ($k=1$) | 32/32 (17.3%) | **1.00** [1.00, 1.00] | **1.00** | 5.00 [5.00] | 13.00 [13.00] | 4.00 [4.14] | 2.55 [2.54] | 2.85 [2.94] |
| MUT-X2\_k3 | Delayed Manifest Sync ($k=3$) | 21/32 (2.1%) | **1.00** [1.00, 1.00] | **1.00** | 61.00 [61.10] | 69.00 [69.10] | 30.50 [31.86] | 7.83 [7.81] | 21.28 [21.16] |
| MUT-X2\_k4 | Delayed Manifest Sync ($k=4$) | 0/32 (0.0%) | 0.00 [0.00, 0.00] | 0.00 | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] |
| MUT-X2\_k5 | Delayed Manifest Sync ($k=5$) | 0/32 (0.0%) | 0.00 [0.00, 0.00] | 0.00 | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] |
| MUT-X3\_k1 | Delayed SST Sync ($k=1$) | 32/32 (30.0%) | **1.00** [1.00, 1.00] | **1.00** | 31.00 [31.62] | 41.00 [41.62] | 2.00 [2.28] | 2.05 [2.04] | 1.62 [1.64] |
| MUT-X3\_k2 | Delayed SST Sync ($k=2$) | 32/32 (15.4%) | **1.00** [1.00, 1.00] | **1.00** | 49.00 [49.25] | 59.00 [59.25] | 4.00 [4.28] | 3.74 [3.70] | 3.24 [3.21] |
| MUT-X3\_k3 | Delayed SST Sync ($k=3$) | 32/32 (0.9%) | **1.00** [1.00, 1.00] | **1.00** | 67.00 [67.00] | 77.00 [77.00] | 75.50 [74.34] | 19.46 [19.88] | 42.34 [42.84] |
| MUT-X4\_p04 | Stochastic File Sync ($p=0.04$) | 32/32 (2.6%) | **1.00** [1.00, 1.00] | **1.00** | 37.00 [37.00] | 45.00 [45.00] | 26.50 [25.62] | 13.32 [13.32] | 17.17 [17.50] |
| MUT-X5\_p12 | Stochastic Dir Sync ($p=0.12$) | 32/32 (0.9%) | **1.00** [1.00, 1.00] | **1.00** | 65.00 [65.00] | 75.00 [75.00] | 72.50 [74.28] | 19.30 [19.46] | 42.49 [42.16] |
| MUT-X6\_k2 | Silent WAL Write Loss ($k=2$) | 32/32 (14.6%) | 6.00 [4.00, 6.00] | 6.84 | 21.00 [21.00] | 25.00 [25.00] | 5.00 [5.33] | 5.21 [6.74] | 6.60 [7.10] |
| MUT-X6\_k5 | Silent WAL Write Loss ($k=5$) | 22/32 (12.3%) | 16.00 [13.00, 22.00] | 16.36 | 35.00 [34.50] | 39.00 [38.68] | 7.00 [8.02] | 12.81 [12.56] | 8.99 [10.15] |
| MUT-X6\_k9 | Silent WAL Write Loss ($k=9$) | 10/32 (11.7%) | 3.00 [3.00, 8.00] | 5.60 | 43.00 [45.40] | 47.00 [49.40] | 10.00 [8.70] | 6.75 [6.69] | 12.05 [10.40] |

Each holdout setting ran 32 deterministic seeds at 120 ops/seed (544 total seeds, 17 fault settings) from `v1.1.2-evaluation-freeze` (`f9202dfca579`). `MUT-X2_k4` and `MUT-X2_k5` produced zero failures under all strategies (fault density below the trace-length detection threshold); they are excluded from paired statistics. The detailed table immediately below is retained from the historical `v1.0-dcg-pr-freeze` evaluation for chronology; the authoritative revised values are in the regenerated `results/holdout_summary.csv` and `results/holdout_per_seed.csv`.

**Paired statistical summary** (`python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12`):

- **In-contract reevaluation (`MUT-X1..X5`, 12 testable settings, 295 failing seed-runs):** `DCG` / `DCG-PR` records **246 wins / 1 loss / 48 ties** versus `R2`; mean $k_1$ speedups range approximately **$2.0\times$–$19.9\times$**, with Holm-adjusted sign-test values up to $0.006$. The loss is on stochastic `MUT-X4_p04`; it is retained as a result rather than tuned away.
- **Out-of-contract control (`MUT-X6_k2, k5, k9`, 3 settings, 63 failing seed-runs):** `DCG` / `DCG-PR` records **21 wins / 38 losses / 4 ties**, with mean ratios $0.8\times$–$1.2\times$ and Holm-adjusted $p = 1.0$; DCG is worse than R2 on mean $k_1$ for `MUT-X6_k2` and `MUT-X6_k5`, and worse than B0 for `MUT-X6_k5`. This retains the negative result and bounds the claim to contract-visible faults.

---

## 9. Real-World Case Study: Pebble #5825 A/B Benchmark & Partial-Retention Priors

### 9.1 Background: CockroachDB Pebble Issue #5825

Pebble issue #5825 reports a randomized hard-crash failure in which a surviving marker references a missing MANIFEST [12]. During manifest rotation, Pebble creates a new `MANIFEST-*` file and then updates `marker.manifest.*` to point to it before syncing the parent directory. This leaves two unsynced directory entries coexisting in `"db"`. In the CraConTest model:

- **0% retention** — both `MANIFEST-*` and `marker.manifest.*` are dropped together; Pebble falls back to the previous manifest and recovers cleanly.
- **100% retention** — both entries survive; recovery succeeds normally.
- **50% partial retention** — `marker.manifest.*` can survive while `MANIFEST-*` is dropped. Pebble's `Open` follows the surviving pointer and fails with `"file does not exist"`. (Note: for intermediate retention `1..99%`, this project's partial-retention model (`deterministicPartialClone` in `internal/replay/replay.go`) captures exact `fileSyncedBytes` snapshots at each file's latest `Sync`/`SyncData`/`SyncTo`. For each file, the crash state is partitioned into 4KB blocks. Any 4KB block where the live file contents differ from the synced snapshot is sampled sequentially from the candidate's PCG PRNG stream (rand.New(rand.NewPCG(0, seed))) as directory entries are traversed in sorted lexicographical order (sort.Strings) and differing 4KB blocks are traversed in ascending offset order, deciding whether that block retains the unsynced live dirty bytes or reverts to the durable snapshot in `fileSyncedBytes`. Unsynced directory entries are similarly sampled independently in sorted lexicographical order, as verified in `TestDeterministicPartialCloneStatisticalFidelity` and `TestReplayPartialCloneInPlaceOverwriteAfterSync`, rather than calling Pebble's `MemFS.CrashClone`).

`BARRIER-5825` (Manifest-Rotation Barrier Control) enforces a parent directory `Sync` before creating the new `marker.manifest.*` file. Specifically, when `manifestMarker.Move` invokes `FS.Create("db/marker.manifest.<iter>.MANIFEST-xxxxxx")`, `BARRIER-5825` intercepts the call and runs `maybeSyncParentDir` **before** delegating to `fs.inner.Create`. In the evaluated VFS model, this makes the pending `MANIFEST-xxxxxx` directory entry durable before the marker that references it is created. The feature `UnsyncedDirEntryCount` captures the maximum number of unsynced directory entries coexisting in the same parent at each cutoff; when this value reaches 2, `dcg-pr` promotes 50% partial-retention candidates to the top of the ranking.

### 9.2 Barrier A/B Evaluation (MUT-0 vs. BARRIER-5825)
To test the directory-ordering mechanism, we benchmarked 32 deterministic workload seeds with manifest rotation enabled (`-max-manifest-size 1`) across retention policies 0%, 50%, and 100%:
1. **MUT-0 (Unfaulted Baseline):** 58 total consistency failures occurred across 25 of the 32 seeds (seeds 42, 300, 400, 1006, 1009, 1010, 1021 have 0 failures under partial retention; the remaining 25 seeds have 1–5 failures). Every single failure manifested exclusively on crash candidates evaluated under the **50% partial-retention policy** (~0.18%–1.11% failure density per seed).
2. **BARRIER-5825 (Manifest-Rotation Barrier Control):** By executing parent directory sync (`maybeSyncParentDir`) *before* `fs.inner.Create` when writing marker files, the unsynced `MANIFEST-xxxxxx` entry is flushed to durability prior to marker creation. No failures were observed under BARRIER-5825 across the 32 evaluated seeds. The intervention result is consistent with the hypothesized directory-ordering mechanism within this VFS model; it is not a population-level proof of defect elimination.

### 9.3 Discovery Performance: Post-Hoc Partial-Retention Prior (`dcg-pr`)
Standard durability-contract guided ranking (`dcg`) was designed with the core assumption that durability failures manifest predominantly at 0% retention (losing unpersisted data). Consequently, `dcg` sweeps all 0%-retention candidates first.

Crucially, as a central empirical finding (not a footnote), when evaluated on uninjected 50%-retention multi-entry directory races (`#5825`), **frozen 0%-first `DCG` underperforms uniform random `B0`**: revised `dcg` requires a median $k_1$ of **203.00** [195.00, 264.00] (mean 243.84) to discover the first failure, compared with accounting-corrected uniform random `B0` (median $k_1 = 178.73$, mean 192.72) and representative `B5` (median $k_1 = 62.00$, mean 57.68). This occurs because 0%-retention crashes drop both entries together and recover cleanly, leaving 0%-first DCG searching hundreds of non-failing states.

To target partial-retention directory vulnerabilities, we introduced `dcg-pr` (DCG Partial-Retention Prior). `dcg-pr` inspects `ContractFeatures.UnsyncedDirEntryCount`: candidates with `Policy > 0 && Policy < 100` and `UnsyncedDirEntryCount >= 2` are prioritized into Tier 1 (ahead of the 0% retention sweep). Only with this post-hoc partial-retention prior does `dcg-pr` achieve median $k_1 = \mathbf{2.00}$ (mean 12.40).

| Strategy | $k_1$ Failure Median [95% CI] | $k_1$ Failure Mean [95% CI] | $S_{25}$ Mean | $S_{100}$ Mean | AUC (Failures) | AUC (Bugs) |
|---|---|---|---|---|---|---|
| **DCG-PR (`dcg-pr`)** | **2.00** [2.00, 5.00] | **12.40** [0.00, 31.90] | **1.44** | **1.44** | **0.6733** | **0.7562** |
| **Representative (B5)** | 62.00 [17.00, 95.00] | 57.68 [39.05, 76.31] | 0.38 | 1.12 | 0.6551 | 0.7004 |
| **sync-boundary (B6)** | 53.00 [50.00, 134.00] | 121.64 [60.89, 182.39] | 0.22 | 0.56 | 0.4940 | 0.6147 |
| **combined (B4)** | 14.00 [8.00, 95.00] | 105.80 [50.96, 160.64] | 0.41 | 0.91 | 0.5026 | 0.6052 |
| **Uniform Random (B0)** | 178.73 [157.94, 243.95] | 192.72 [164.37, 221.07] | 0.08 | 0.31 | 0.3899 | 0.4878 |
| **fifo (B1)** | 29.00 [26.00, 230.00] | 175.28 [101.54, 249.02] | 0.00 | 0.41 | 0.3903 | 0.4987 |
| **rhg (B7)** | 211.00 [203.00, 340.00] | 301.60 [252.44, 350.76] | 0.00 | 0.00 | 0.2807 | 0.3062 |
| **0%-Retention Random (R1)** | 245.58 [234.45, 272.53] | 249.58 [239.19, 259.97] | 0.00 | 0.00 | 0.2728 | 0.2981 |
| **State-Representative Random (R2)** | 247.66 [228.31, 263.39] | 248.99 [238.41, 259.58] | 0.00 | 0.00 | 0.2735 | 0.3009 |
| **DCG Flag-Only (Ablation)** | 303.99 [282.42, 366.75] | 313.60 [293.28, 333.92] | 0.00 | 0.00 | 0.2619 | 0.2769 |
| **Durability-Contract Guided (DCG)** | 203.00 [195.00, 264.00] | 243.84 [219.21, 268.47] | 0.00 | 0.00 | 0.3228 | 0.3748 |

In this case study, `dcg-pr` reduces median $k_1$ from 203.00 to **2.00** relative to standard DCG and from 178.73 to **2.00** relative to B0. These are 101.5-fold and 89.4-fold ratios of median discovery positions, respectively, rather than wall-clock speedups; the corresponding ratios of mean $k_1$ are 19.7 and 15.5. The measured AUC is 0.6733 for failures and 0.7562 for signatures. The result shows that the post-hoc partial-retention prior targets this evaluated directory-race mechanism effectively.

---

## 10. Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race

### 10.1 Background: Checkpoint Directory Sync Omission
In Pebble v2.1.7, `db.Checkpoint(destDir)` creates hard-linked SSTables, `MANIFEST`, and `marker.manifest.*` inside `destDir` before the directory sync in `manifestMarker.Move`. Under 50% partial retention, a crash inside this multi-entry window can retain `marker.manifest.*` while dropping the referenced `MANIFEST-*` or linked SSTable, causing reopening to fail with `open_error`. Upstream commit `15b500fb` later fixed this race by syncing the destination directory after creating the MANIFEST and before creating its marker [13].

Crucially, the checkpoint race fixed by `15b500fb` belongs to the same structural bug class as Case Study 1 (`#5825`): both are multi-entry directory races in Pebble's marker protocol. Case Study 2 therefore evaluates **intra-class subsystem transfer** of `dcg-pr` from manifest rotation in `"db/"` to `db.Checkpoint` in `"checkpoint/"`, not generalization to an unseen bug class.

Enforcing immediate per-entry parent-directory synchronization (`BARRIER-CKPT`, `internal/faults/faults.go:641-653`, which syncs `parentDir` on `marker.manifest` operations across both `"db/"` and `"checkpoint/"`) eliminates this intra-window multi-entry race (`0/32` failing seeds).

### 10.2 Pre-Evaluation Code Freeze & Barrier A/B Evaluation
To guard against methodological overfitting, all prioritization algorithms—including `dcg` and `dcg-pr`—were frozen under git tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) before evaluating this intra-class subsystem transfer case study (since BARRIER-CKPT and dcg-pr share the same multi-entry directory mechanism as #5825, this is an intra-class transfer check rather than a blind test of an unseen bug class). Workload operations were extended with `-checkpoint-at 60`, executing a checkpoint creation operation mid-workload.

Across 32 deterministic seeds (`42, 100..900, 1001..1022`) under policies 0%, 50%, and 100% ($N = 18{,}588$ crash candidates across the 32 seeds under MUT-0; $37{,}560$ total across MUT-0 and BARRIER-CKPT):

1. **MUT-0 (Baseline):** 31 of 32 seeds exhibited checkpoint or manifest recovery failure under 50% partial retention (53 total failures). Every failure manifested as `open_error` at 50% partial retention where `marker.manifest.*` directory entries survived while linked `MANIFEST-*` or SSTable entries were dropped.
2. **BARRIER-CKPT (Checkpoint Directory Barrier Control):** Injecting a `parentDir.Sync()` barrier on relevant `Create`, `OpenReadWrite`, `Link`, and `Rename` operations yields **0 observed failures across the 32 evaluated seeds**, compared with 53 failures across 31 seeds without the intervention (`results/casestudy_checkpoint_summary.csv`).

### 10.3 Pre-Frozen Strategy Discovery Performance

| Strategy | $k_1$ Failure Median [95% CI] | $k_1$ Failure Mean [95% CI] | $S_{25}$ Mean | $S_{100}$ Mean | AUC (Failures) | AUC (Bugs) |
|---|---|---|---|---|---|---|
| **DCG-PR (`dcg-pr`, frozen)** | **245.00** [3.00, 306.00] | **156.39** [99.92, 212.86] | **0.53** | **0.53** | **0.5010** | **0.6321** |
| **sync-boundary** | 74.00 [71.00, 74.00] | 95.48 [58.37, 132.60] | 0.22 | 0.84 | 0.7111 | 0.8026 |
| **Representative** | 86.00 [17.00, 356.00] | 170.10 [109.41, 230.78] | 0.38 | 0.69 | 0.5829 | 0.6771 |
| **Uniform Random (B0)** | 262.26 [194.27, 281.19] | 238.53 [213.62, 263.45] | 0.07 | 0.27 | 0.4831 | 0.5535 |
| **Durability-Contract Guided (DCG)** | 288.00 [209.00, 300.00] | 258.65 [240.81, 276.48] | 0.00 | 0.00 | 0.3462 | 0.3634 |
| **State-Representative Random (R2)** | 280.41 [252.81, 293.63] | 271.55 [262.01, 281.09] | 0.00 | 0.00 | 0.3304 | 0.3413 |
| **0%-Retention-First Random (R1)** | 280.77 [259.31, 291.23] | 273.71 [264.40, 283.02] | 0.00 | 0.00 | 0.3286 | 0.3386 |
| **BARRIER-CKPT (all strategies)** | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 | 0.00 | 0.0000 | 0.0000 |

The accounting-corrected `dcg-pr` ranking has lower mean $k_1$ than R2 (156.39 vs. 271.55) but a broad distribution and median $245.00$; its paired comparison is 22 wins, 9 losses, and 0 ties (sign $p=0.0147$). Frozen 0%-first DCG underperforms corrected B0 (median $288.00$ vs. $262.26$; mean $258.65$ vs. $238.53$), while R2 records median/mean $280.41/271.55$. The corrected evidence therefore supports a mean and paired advantage for the partial prior on this intra-class transfer, not the historical claim of near-immediate median discovery.

---

## 11. Universe-Scaling Evaluation on MUT-5 (~300 to ~3,000 Candidates)

To evaluate whether durability-contract priors maintain their efficiency as workload size and candidate universe size scale, we evaluated `MUT-5` (single WAL sync drop) across 4 workload operations levels ($\{120, 300, 600, 1200\}$ ops) and 8 seeds ($\{42, 100, 200, 300, 400, 500, 600, 700\}$). Note that `scaling_test.go` aggregates per-seed rounded 100-shuffle medians (`K1Failure` / `math.Round(K1FailureMedian)`).

| Workload Operations | Mean Universe Size | DCG ($k_1$ Mean / Med) | R2 ($k_1$ Mean / Med) | R1 ($k_1$ Mean / Med) | B0 ($k_1$ Mean / Med) | B2 LIFO ($k_1$ Mean / Med) | Mean Plan+Rank Time | Mean Explore Time |
|---|---|---|---|---|---|---|---|---|
| **120 ops** | 337.5 | **1.00** / **1.00** | 12.00 / 12.00 | 12.12 / 12.50 | 26.75 / 27.00 | 260.50 / 264.00 | 1.9 ms | 339.5 ms |
| **300 ops** | 767.2 | **1.00** / **1.00** | 23.38 / 23.50 | 30.25 / 30.00 | 56.25 / 54.50 | 690.25 / 691.00 | 4.4 ms | 784.8 ms |
| **600 ops** | 1,505.0 | **1.00** / **1.00** | 41.50 / 41.00 | 53.12 / 52.50 | 116.75 / 116.00 | 1,428.00 / 1,425.00 | 12.4 ms | 2,387.0 ms |
| **1,200 ops** | 2,958.0 | **1.00** / **1.00** | 80.62 / 81.50 | 109.75 / 109.00 | 220.88 / 213.00 | 2,881.00 / 2,881.00 | 38.5 ms | 8,435.2 ms |

### Key Findings:
1. **Invariant Measured Discovery Position:** DCG achieves $k_1 = 1.00$ at all four evaluated operation counts, over candidate universes ranging from approximately 338 to 2,958 states. This finite range does not establish asymptotic complexity.
2. **Increasing Baseline Budgets:** Over the same range, B0's mean of per-seed rounded shuffle medians increases from 26.75 to 220.88, LIFO (B2) from 260.50 to 2,881.00, and R2 from 12.00 to 80.62. Four scale points are insufficient to identify a general growth law.
3. **Measured Planning Cost:** Mean graph-construction and ranking time increases from 1.9 ms at 120 operations to 38.5 ms at 1,200 operations, while mean exploration time increases from 339.5 ms to 8,435.2 ms.

---

## 12. Multi-Shape Null-Control Verification (108 Runs, No Observed Failures)

To establish that the testing framework and consistency oracles do not produce false alarms under diverse workload topologies, an extended null-control suite was evaluated across 12 distinct workload shapes and 9 seeds ($42, 100, 200, 300, 400, 500, 600, 700, 800$) under `MUT-0` at 0%/100% retention policies (108 runs total, exploring over 50,000 individual crash states):

| Shape ID | Operations | Key Count | Value Bytes | Sync Every | Flush Every | Compact At | Runs | Violations | Open Errors | Panics | Hangs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **1** | 80 | 8 | 32 B | 4 | 25 | 0 | 9 | 0 | 0 | 0 | 0 |
| **2** | 80 | 16 | 64 B | 8 | 40 | 0 | 9 | 0 | 0 | 0 | 0 |
| **3** | 120 | 8 | 128 B | 4 | 25 | 0 | 9 | 0 | 0 | 0 | 0 |
| **4** | 120 | 32 | 64 B | 8 | 40 | 0 | 9 | 0 | 0 | 0 | 0 |
| **5** | 120 | 16 | 256 B | 12 | 60 | 0 | 9 | 0 | 0 | 0 | 0 |
| **6** | 200 | 8 | 32 B | 4 | 40 | 0 | 9 | 0 | 0 | 0 | 0 |
| **7** | 200 | 64 | 64 B | 8 | 40 | 30 | 9 | 0 | 0 | 0 | 0 |
| **8** | 200 | 16 | 128 B | 12 | 60 | 0 | 9 | 0 | 0 | 0 | 0 |
| **9** | 300 | 8 | 32 B | 4 | 25 | 0 | 9 | 0 | 0 | 0 | 0 |
| **10** | 300 | 32 | 64 B | 8 | 40 | 0 | 9 | 0 | 0 | 0 | 0 |
| **11** | 300 | 16 | 128 B | 12 | 60 | 0 | 9 | 0 | 0 | 0 | 0 |
| **12** | 300 | 64 | 256 B | 12 | 60 | 60 | 9 | 0 | 0 | 0 | 0 |
| **Total** | — | — | — | — | — | — | **108** | **0** | **0** | **0** | **0** |

Across all 108 runs, no consistency violations, open errors, panics, or hangs were observed. This is a descriptive result for the 12 fixed workload shapes, nine seeds, and endpoint retention policies that were evaluated. Runs share workload construction and crash candidates within a trace are dependent, so these observations do not support an independent-trial confidence bound on a population false-alarm probability. The result is also scoped to non-ingest workloads under 0%/100% retention; §9, §10, and §13 show that 50% partial retention and Direct L0 `db.Ingest` exercise additional failure mechanisms.

---

## 13. Pebble Subsystem Crash-Consistency Audit and Empirical Evaluation (MUT-0)

To examine whether selected Pebble subsystems that perform multi-file and directory-marker mutations exhibit crash-consistency vulnerabilities under unfaulted execution (`MUT-0`), we conducted a targeted source audit and empirical discovery campaign across ingestion, format ratcheting, and checkpoints in Pebble `v2.1.7`:

### 13.1 Source-Level Audit of Pebble v2.1.7 Subsystems

1. **External SSTable Ingestion (`ingest.go` & `objstorageprovider`):**
   - Entry point: `db.Ingest(paths)` (`ingest.go:1111`) delegates to `d.ingest` (`ingest.go:1423–1783`).
   - External SSTables in staging are hard-linked into the DB data directory via `ingestLinkLocal` (`ingest.go:1477`, `objstorageprovider/provider.go:436`).
   - **Crucial Directory Sync Barrier (`ingest.go:1490`):** `d.objProvider.Sync()` unconditionally executes `p.fsDir.Sync()` (`objstorageprovider/vfs.go:100`), persisting all newly linked table directory entries. This occurs **before** sequence number allocation (`d.commit.AllocateSeqNum` at line 1703) and before branching into direct L0 vs. flushable ingestion paths.
   - **Direct L0 Ingestion (No Memtable Overlap, `ingest.go:1582`):** Direct L0 ingest commits atomically by logging a `VersionEdit` to the MANIFEST and syncing the file: `vs.manifestFile.Sync()` at `version_set.go:649`. WAL is not written.
   - **Flushable Ingestion (Memtable Overlap Exists, `ingest.go:1641`):** Calls `d.handleIngestAsFlushable` (`ingest.go:1338–1408`), encodes `InternalKeyKindIngestSST` in batch `b`, rotates the WAL (`ingest.go:1361`), and synchronously fsyncs the WAL via `d.commit.directWrite(b)` at `commit.go:289` (`ingest.go:1367`). No `VersionEdit` is written to the MANIFEST during the ingest call; MANIFEST publication is deferred until the memtable flush queue drains.

2. **Live Format-Major-Version Ratcheting (`format_major_version.go`):**
   - `d.RatchetFormatMajorVersion` (`format_major_version.go:436–480`) acquires `d.mu.Lock()` and executes migrations sequentially via `formatMajorVersionMigrations[nextVers]`.
   - Each migration calls `d.finalizeFormatVersUpgrade` (`format_major_version.go:486`), which invokes `d.writeFormatVersionMarker` (`line 495`), delegating to `atomicfs.Marker.Move` (`vfs/atomicfs/marker.go:189`).
   - **Zero MANIFEST Logging:** Neither `RatchetFormatMajorVersion` nor `finalizeFormatVersUpgrade` writes any `VersionEdit` to the MANIFEST. Active format versions are strictly decoupled from the MANIFEST log.
   - **Synchronous Directory Sync:** Directory synchronization occurs immediately and synchronously inside `atomicfs.Marker.Move` (`a.dirFD.Sync()` at `marker.go:236`).

3. **AtomicFS Marker Dynamics under Partial Retention (`vfs/atomicfs/marker.go`, `vfs/mem_fs.go`):**
   - Inside `atomicfs.Marker.Move` (`marker.go:189–240`), the file mutation sequence is: `fs.Create(newMarker)` -> `f.Close()` -> `fs.Remove(oldMarker)` -> `dirFD.Sync()`.
   - In `vfs/mem_fs.go:694–711` (`memNode.CrashClone`), directory cloning initializes `newNode.children = maps.Clone(f.syncedChildren)`. The unsynced data retention loop is purely additive over `f.children`.
   - Consequently, an unsynced `fs.Remove` **cannot** delete `oldMarker` from `syncedChildren`. Under this project's partial-retention model (`deterministicPartialClone`), which defines a sorted entry-and-4KB-block Bernoulli partial-retention model (whose marginal retention rates are checked in `TestDeterministicPartialCloneStatisticalFidelity`) rather than calling Pebble's raw `MemFS.CrashClone`, `oldMarker` is preserved **100% of the time**, while `newMarker` is preserved with 50% probability. There is a 0% probability that both markers are lost.
   - In `scanForMarker` (`marker.go:77–104`), `LocateMarker` deterministically selects the marker with the largest valid iteration index (`iter`), scheduling older iterations for cleanup via `RemoveObsolete` on startup.

4. **Checkpoint Subsystem Durability Invariants (`checkpoint.go`):**
   - Tracing `d.Checkpoint` (`checkpoint.go:144–455`), the destination directory `destDir` is synced exactly three times:
     - **Sync #1 (`checkpoint.go:287`):** Inside `versionMarker.Move`, durably persisting `OPTIONS` and `marker.format-version`.
     - **Sync #2 (`checkpoint.go:596`):** Inside `manifestMarker.Move`, durably persisting all SSTable hard links, `MANIFEST`, and `marker.manifest`.
     - **Sync #3 (`checkpoint.go:448`):** Explicit `dir.Sync()`, durably persisting WAL files.
   - Any crash occurring before Sync #2 lacks `marker.manifest`, causing `open.go:313` (`findCurrentManifest`) to cleanly return `ErrDBDoesNotExist`. No torn or corrupted checkpoint can be opened.

### 13.2 Workload Generator and Oracle Extensions

To exercise these subsystems during crash testing:
- **Workload Planning & Staging (`internal/workload/workload.go`):**
  - Added `IngestEvery int` and `RatchetAt int` to `workload.Config`. When zero, baseline workload execution and random number streams are 100% bit-for-bit invariant.
  - Ingestion staging: SSTables are constructed inside `"ingest_staging"` on the target VFS using `sstable.NewWriter`, verified for strict key ordering, synced (`w.Close()`), and directory synced before calling `db.Ingest`. For multi-file ingests, SSTables are constructed with mutually disjoint, non-overlapping key bounds (`max(SSTable_1) < min(SSTable_2)`).
  - Version ratcheting: opens database at `pebble.FormatMinSupported` and transitions to `pebble.FormatNewest` at operation `RatchetAt`.
- **Trace & Oracle Integration (`internal/trace/schema.go`, `internal/history/history.go`, `internal/oracle/oracle.go`):**
  - Integrated `"ingest"` as a mutating, synchronous operation (`Mutating: true`, `Sync: true`). All mutations within ingested SSTables are recorded in `history.Entry.Mutations`.
  - The prefix oracle enforces that acknowledged ingested operations advance the durability lower bound (`lower = len(states) - 1`), verifying that recovered database states never drop acknowledged ingested keys.

### 13.3 Systematic Unfaulted Discovery Results (results/subsystem_discovery.csv)

The dedicated discovery test `TestSubsystemDiscovery` (`internal/exp/subsystem_discovery_test.go`) evaluated Workloads A, B, and C under `MUT-0` across seeds `42, 100, 777` with retention policies `[0, 50, 100]` (5,415 candidate states evaluated):

| Workload | Seed | Operations | IngestEvery | RatchetAt | CheckpointAt | MaxManifestSize | Policies | Universe | Violations | Durably Committed | Prefix Violations | Open Errors | Panics | Hangs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Workload A (Ingestion)** | 42 | 80 | 25 | 0 | 0 | 0 | [0 50 100] | 576 | 15 | 0 | 15 | 0 | 0 | 0 |
| **Workload A (Ingestion)** | 100 | 80 | 25 | 0 | 0 | 0 | [0 50 100] | 621 | 0 | 0 | 0 | 2 | 0 | 0 |
| **Workload A (Ingestion)** | 777 | 80 | 25 | 0 | 0 | 0 | [0 50 100] | 567 | 25 | 0 | 25 | 1 | 0 | 0 |
| **Workload B (Ratchet)** | 42 | 80 | 0 | 50 | 0 | 0 | [0 50 100] | 462 | 0 | 0 | 0 | 0 | 0 | 0 |
| **Workload B (Ratchet)** | 100 | 80 | 0 | 50 | 0 | 0 | [0 50 100] | 465 | 0 | 0 | 0 | 2 | 0 | 0 |
| **Workload B (Ratchet)** | 777 | 80 | 0 | 50 | 0 | 0 | [0 50 100] | 405 | 0 | 0 | 0 | 1 | 0 | 0 |
| **Workload C (Combined)** | 42 | 80 | 30 | 40 | 60 | 1 | [0 50 100] | 798 | 0 | 0 | 0 | 4 | 0 | 0 |
| **Workload C (Combined)** | 100 | 80 | 30 | 40 | 60 | 1 | [0 50 100] | 762 | 0 | 0 | 0 | 7 | 0 | 0 |
| **Workload C (Combined)** | 777 | 80 | 30 | 40 | 60 | 1 | [0 50 100] | 759 | 0 | 0 | 0 | 2 | 0 | 0 |

### 13.4 Empirical Verification and Recovery Hazard Summary

1. **No Durably-Committed Data Loss Observed (`0` `durably_committed` violations, `0` panics):** Across all 5,415 candidate states in Workloads A, B, and C under `MUT-0`, no durably-committed data loss was observed in these workloads (`0` `durably_committed` violations and `0` recovery panics).
2. **Cross-Channel Commit-Order Hole on Direct L0 `Ingest` (`40` `prefix` violations across Seeds 42 and 777):** When `db.Ingest` takes the non-overlapping Direct L0 shortcut (`asFlushable = false`), Pebble allocates a global sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1 < s_2$ is lost from the unsynced `WAL`, creating a cross-channel sequence-number hole (whereas overlapping Flushable Ingest syncs the shared `WAL` via `commit.directWrite` at `commit.go:289` and preserves strict prefix order).
3. **Workload B (Live Format Ratcheting):** Zero violations, zero database corruptions. Coexisting format version markers under 50% partial retention are resolved deterministically by `atomicfs.LocateMarker` selecting the highest iteration index. The 3 `open_error` states under 50% retention (Seed 42: 0, Seed 100: 2, Seed 777: 1) occur exclusively during initial database creation: because `pebble.Open("db", opts)` is executed while the VFS recorder is active, initial database creation writes `"db/MANIFEST-000001"` and `"db/marker.manifest.000001"` prior to the initial directory sync on `"db"`. In Workload B, all 3 `open_error` states occur at those initial `pebble.Open("db")` cutoffs before operation 1; `RatchetFormatMajorVersion` at operation 50 causes **0** open errors and **0** violations.
4. **Workload C (Combined Lifecycle Stress):** Open errors under 50% retention (13 total: 4 on Seed 42, 7 on Seed 100, 2 on Seed 777) reproduced the expected multi-entry directory race on unsynced `MANIFEST-*` files and checkpoint directories prior to directory sync. All 0% and 100% retention cutoffs recovered cleanly with zero errors.
5. **Verdict:** Across all 5,415 candidate states in Workloads A, B, and C under `MUT-0`, no durably-committed data loss was observed in these workloads (`0` `durably_committed` violations and `0` recovery panics), while two recovery hazards surfaced under `MUT-0`: (1) an **input-dependent cross-channel `prefix` monotonicity hole** during non-overlapping Direct L0 `db.Ingest` (`40` prefix violations in Workload A, analyzed across 32 seeds in §13.5), and (2) **50%-retention intra-operation directory races (`open_error`)** when a crash occurs inside the multi-entry window between `MANIFEST-*`/`.sst` creation and `dirFD.Sync()` (19 total open errors across Workloads A, B, and C: 3 in Workload A, 3 in Workload B, and 13 in Workload C).

### 13.5 Direct L0 db.Ingest 32-Seed Case Study and Barrier Intervention (BARRIER-INGEST)

#### 13.5.1 Architectural Root Cause: Cross-Channel Sequence-Number Hole

During our subsystem audit (§13.1), we identified that Pebble v2.1.7 implements two distinct execution branches for `db.Ingest(paths)`:
1. **Flushable Ingest (`asFlushable = true`):** When ingested table key bounds overlap with keys currently residing in the unflushed memtable queue (`mem != nil`), Pebble routes ingestion through `handleIngestAsFlushable` (`ingest.go:1338–1408`). It inserts a flushable ingest marker into the memtable queue, rotates the WAL, and synchronously flushes the WAL via `commit.directWrite` (`commit.go:289`). The write-ahead log persists all preceding user mutations up to the current sequence number before the ingest completes.
2. **Direct L0 Ingest (`asFlushable = false`):** When ingested tables are mutually disjoint from active memtable keys, Pebble executes an optimized direct L0 shortcut. In `ingest.go:1703`, Pebble calls `d.commit.AllocateSeqNum` to allocate sequence number $s_2 > s_1$ for the ingested table, links the table into L0, and persists a `VersionEdit` by fsyncing the `MANIFEST` (`vs.manifestFile.Sync()` at `version_set.go:649`). Critically, **direct L0 ingest bypasses the WAL entirely**.

If preceding client writes were issued without an explicit synchronous barrier (`NoSync`, sequence $s_1 < s_2$), those writes remain buffered in dirty VFS memory for the active `.log` WAL file. When a crash occurs at 0% or 50% retention:
- The MANIFEST survives because it was explicitly fsynced during `db.Ingest`, making table $s_2$ durably visible upon recovery.
- The unpersisted dirty WAL buffer containing writes at $s_1$ is discarded by the crash clone.
- Upon restart, Pebble loads the MANIFEST, incorporates table $s_2$, replays whatever prefix survived in the WAL, and exposes state containing $s_2$ but missing $s_1$.

Because $s_1 < s_2$, client history recorded $s_1$ prior to $s_2$. Exposing $s_2$ without $s_1$ violates **Prefix Consistency** ($S \notin [S_{\text{lower}}, S_{\text{upper}}]$ in the prefix oracle). Note that because the client never issued an fsync on $s_1$, durability contract bounds allow rollback of $s_1$ *if and only if* subsequent states are also rolled back. Committing $s_2$ via the MANIFEST channel while dropping $s_1$ on the WAL channel creates an unrecoverable cross-channel commit-order hole.

We explicitly contextualize this divergence relative to durability models: RocksDB's `db.h` notes that `IngestExternalFile` does not imply `FlushWAL(true)`, and under a per-operation synced-only durability oracle, dropping $s_1$ while retaining $s_2$ is permissible. Under the global sequence-prefix model used by RocksDB's lost-buffered-write recovery test [14] and by CraConTest, however, retaining $s_2$ while losing $s_1 < s_2$ is a prefix violation. Direct L0 `db.Ingest` therefore exposes a cross-channel ordering behavior under this stronger oracle; it is not a loss of synced data and is not reported here as an upstream Pebble bug.

#### 13.5.2 Barrier Intervention: BARRIER-INGEST via Pre-Ingest WAL Sync

To test the hypothesized cross-channel ordering mechanism, we introduced **BARRIER-INGEST**.
Prior to invoking `db.Ingest`, the storage engine or application flushes the active write-ahead log. In Pebble's public API, this is achieved cleanly via:
```go
db.LogData(nil, pebble.Sync)
```
`db.LogData(nil, pebble.Sync)` writes a zero-byte sync record to the active WAL and invokes `commit.directWrite` with `pebble.Sync`, forcing an immediate `SyncData()` on the active WAL file before `db.Ingest` allocates sequence numbers or fsyncs the MANIFEST.

The recorder groups same-handle write/sync events when its bounded lookback permits, without crossing same-handle actions, locks, or client operation boundaries. Fault-wrapped synced mutations wait for WAL-write notification before `OpEnd`; ingest gates defer non-staging, non-marker removals until `db.Ingest` returns, and subsequent flushes wait for pending manifest cleanup. The current implementation uses configurable 30-second default deadlines, short sleep backoffs, and 2 ms settling delays rather than entirely event-driven synchronization. Repeatability is supported by repeated-run tests for evaluated shapes, not a guarantee of schedule independence. These gates also exclude some mid-ingest cleaner states; Section 15.2 describes the resulting scope limitation.

#### 13.5.3 32-Seed Empirical Evaluation (results/casestudy_ingest_summary.csv)

We evaluated 32 canonical seeds (`seed = 1..32`, 25 ops, `SyncEvery: 4, FlushEvery: 12, IngestEvery: 5, ValueSize: 64`) under both `MUT-0` (`BarrierIngest: false`) and `BARRIER-INGEST` (`BarrierIngest: true`) across 11 ranking strategies under endpoint retention policies `[0, 100]` (evaluating candidate crash states in `results/casestudy_ingest_per_seed.csv` and `results/casestudy_ingest_summary.csv`):

| Fault Condition | Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ P10 | $k_1$ P90 | $S_{25}$ Mean | $S_{100}$ Mean |
|---|---|---|---|---|---|---|---|
| **MUT-0 (Unpatched)** | **DCG-PR** | **9.00** [9.00, 19.00] | **15.32** [9.88, 20.76] | **4.00** | **35.70** | **0.75** | **1.41** |
| **MUT-0 (Unpatched)** | **DCG** | **9.00** [9.00, 19.00] | **15.32** [9.88, 20.76] | 4.00 | 35.70 | 0.75 | 1.41 |
| **MUT-0 (Unpatched)** | Uniform Random (B0) | 33.74 [24.58, 43.58] | 34.32 [28.69, 39.94] | 19.86 | 50.55 | 0.53 | 1.88 |
| **MUT-0 (Unpatched)** | State-Representative Random (R2) | 27.88 [22.65, 32.28] | 27.64 [23.57, 31.70] | 16.53 | 34.58 | 0.51 | 1.94 |
| **MUT-0 (Unpatched)** | 0%-Retention Random (R1) | 28.60 [21.80, 41.51] | 30.61 [24.93, 36.28] | 16.34 | 47.18 | 0.53 | 1.73 |
| **MUT-0 (Unpatched)** | DCG Flag-Only (Ablation) | 28.18 [26.92, 29.26] | 29.88 [25.85, 33.90] | 19.78 | 45.93 | 0.42 | 1.75 |
| **MUT-0 (Unpatched)** | sync-boundary (B6) | 81.00 [52.00, 90.00] | 80.18 [59.96, 100.40] | 24.00 | 152.00 | 0.31 | 1.31 |
| **BARRIER-INGEST** | **All Strategies** | **0 observed prefix violations** across 32 seeds ($S_{25}=0.00, S_{100}=0.00$) |

#### 13.5.4 Findings & Analysis
1. **Observed Elimination Under the Barrier Intervention:** No prefix consistency violations were observed with `BARRIER-INGEST` across the 32 evaluated seeds ($S_{25}=0.00, S_{100}=0.00$), compared with 276 failures across 22 seeds without it. This controlled result supports the WAL/MANIFEST ordering mechanism within the evaluated workload and crash model; it does not prove uniqueness of the mechanism outside that scope.
2. **Seed Overlap Dynamics:** Under endpoint (`0%/100%`) retention, **22 of 32 seeds** exhibit prefix violations (276 total failures; seeds `3, 5, 7, 8, 9, 16, 20, 23, 26, 29` have 0 failures because randomly generated ingest keys overlapped the active memtable and took the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`) while **0 of 32 seeds** fail under `BARRIER-INGEST`.
3. **Ranking Result:** Under `MUT-0`, `DCG` and `DCG-PR` achieve median/mean $k_1 = 9.00/15.32$, compared with R2 26.77/27.00 and corrected B0 33.74/34.32. The paired DCG-PR/R2 result (13/6/3, sign $p=0.0835$) does not reject equality at 0.05, so this evidence supports lower observed cost on the evaluated seeds rather than a statistically significant superiority claim.

---

## 14. Multi-Distribution Workload Diversity Evaluation

### 14.1 Motivation & Workload Presets

Real-world storage systems encounter workloads with radically divergent access patterns, key contention profiles, and mutation-to-sync ratios. To evaluate whether graph-guided crash testing generalizes beyond standard uniform key-value distributions without overfitting, we designed and implemented 5 distinct workload presets in `internal/workload/workload.go` (exposed via `-preset`):

1. **`ycsb-a` (Update-Heavy):** 50% update (`set`), 50% point read (`get`) over 8 hot keys (`keyCount = 8`) with periodic sync/flush. Simulates high-write transactional key-value serving.
2. **`ycsb-b` (Read-Predominant):** 75% point read (`get`), 25% update (`set`, 3 reads per write operation via `i%4 == 3`, adapted from YCSB-B for 25-operation traces so multiple sync cycles occur per run) over 12 keys (`keyCount = 12`) with periodic sync/flush. Tests sparse write persistence within dense read sequences.
3. **`ycsb-f` (Read-Modify-Write Transactions):** 100% read-modify-write two-key atomic batch transactions (reading 2 keys via `Get` and updating 2 distinct keys atomically via `Batch` per transaction) over 8 keys (`keyCount = 8`) with periodic sync/flush. Tests batch commit atomicity and multi-key durability horizons.
4. **`overwrite-heavy` (Deep Version Chains):** 100% overwrite (`set`) across 4 hot keys (`keyCount = 4`), driving deep per-key version chains and frequent memtable flush pressure. (Note in §14.3/§14.4 that `ycsb-f` and `overwrite-heavy` produce identical discovery metrics because both execute 100% mutating operations under the same sync/flush cadence.)
5. **`delete-heavy` (Tombstone & Lifecycle Stress):** Equal 1/3 cycling (`i%3`) of point `set`, point `delete`, and `range_delete` (`DeleteRange`) over 8 keys (`keyCount = 8`) with periodic checkpoints (`checkpointAt = 10`). Exercises range tombstones and directory hard-linking.

### 14.2 Multi-Fault Evaluation Matrix

Across all 5 presets, we evaluated 16 canonical workload seeds (`seed = 1..16`, 25 ops) across 4 fault models representing both dense and sparse failure distributions:
- `MUT-0`: Unfaulted control (null hypothesis baseline).
- `MUT-1`: All WAL syncs suppressed (dense WAL fault).
- `MUT-5`: Single $k$-th WAL sync dropped ($k=1$, sparse WAL fault).
- `MUT-9`: Single $k$-th directory sync dropped ($k=1$, parent directory entry fault).

Evaluated strategies: `DCG`, `DCG-PR`, `R2`, `Sync-Boundary (B6)`, and `Uniform Random (B0)`. The summary is committed in `results/workload_diversity_summary.csv` and unaggregated data in `results/workload_diversity_per_seed.csv`.

### 14.3 Empirical Summary Table ($k_1$ Mean [95% CI] and $S_{25}$ Mean)

| Workload Preset | Fault Model | Density | DCG $k_1$ Mean [95% CI] | DCG-PR $k_1$ Mean | R2 $k_1$ Mean | B0 $k_1$ Mean | DCG $S_{25}$ | R2 $S_{25}$ | B0 $S_{25}$ |
|---|---|---|---|---|---|---|---|---|---|
| **ycsb-a** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-a** | MUT-1 | 17.8% | **1.00** [1.00, 1.00] | 1.00 | 2.36 | 5.94 | **10.50** | 10.40 | 3.69 |
| **ycsb-a** | MUT-5 | 2.0% | **1.00** [1.00, 1.00] | 1.00 | 8.27 | 27.11 | **1.38** | 1.38 | 0.45 |
| **ycsb-a** | MUT-9 | 25.9% | **1.00** [1.00, 1.00] | 1.00 | 2.22 | 3.90 | **11.62** | 11.45 | 3.65 |
| **ycsb-b** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-b** | MUT-1 | 10.0% | **1.00** [1.00, 1.00] | 1.00 | 3.10 | 9.24 | **7.00** | 7.00 | 2.27 |
| **ycsb-b** | MUT-5 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-b** | MUT-9 | 14.4% | **1.00** [1.00, 1.00] | 1.00 | 3.45 | 6.86 | **7.00** | 6.42 | 2.58 |
| **ycsb-f** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-f** | MUT-1 | 17.5% | **1.00** [1.00, 1.00] | 1.00 | 1.82 | 5.49 | **13.00** | 13.00 | 4.00 |
| **ycsb-f** | MUT-5 | 1.6% | **1.00** [1.00, 1.00] | 1.00 | 8.49 | 39.71 | **2.00** | 2.00 | 0.44 |
| **ycsb-f** | MUT-9 | 28.3% | **1.00** [1.00, 1.00] | 1.00 | 1.82 | 3.59 | **13.00** | 13.00 | 4.04 |
| **overwrite-heavy** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **overwrite-heavy** | MUT-1 | 23.3% | **1.00** [1.00, 1.00] | 1.00 | 1.72 | 4.00 | **15.00** | 14.39 | 4.85 |
| **overwrite-heavy** | MUT-5 | 3.8% | **1.00** [1.00, 1.00] | 1.00 | 9.40 | 22.99 | **2.00** | 1.84 | 0.75 |
| **overwrite-heavy** | MUT-9 | 33.5% | **1.00** [1.00, 1.00] | 1.00 | 1.69 | 2.84 | **16.00** | 14.77 | 3.90 |
| **delete-heavy** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **delete-heavy** | MUT-1 | 15.7% | **3.00** [3.00, 3.00] | 3.00 | 2.37 | 6.82 | **14.19** | 10.09 | 3.46 |
| **delete-heavy** | MUT-5 | 1.6% | **20.54** [8.61, 32.47] | 20.54 | 35.59 | 57.16 | **0.88** | 0.61 | 0.31 |
| **delete-heavy** | MUT-9 | 8.7% | **3.00** [3.00, 3.00] | 3.00 | 4.44 | 10.88 | **7.44** | 5.19 | 1.74 |

### 14.4 Key Insights & Generalization Summary

1. **Null-Control Observation:** Across all 16 seeds for all five presets under unfaulted `MUT-0` (80 complete workload trials), CraConTest observed no failures. This result is limited to the evaluated presets, seeds, and endpoint retention policies.
2. **Lower Candidate Cost on Sparse Faults:** `DCG` records mean $k_1=1.00$ versus R2/B0 $8.49/39.71$ on `ycsb-f/MUT-5`, $9.40/22.99$ on `overwrite-heavy/MUT-5`, and $8.27/27.11$ on `ycsb-a/MUT-5`. These are candidate-count ratios on known settings, not wall-clock speedups or blind generalization.
3. **Delete-Heavy Dynamics & Equivalence Representation:** On `delete-heavy`, `DCG` records mean $k_1=3.00$ on `MUT-1` versus R2 2.37 and B0 6.82, while increasing $S_{25}$ from R2/B0 10.09/3.46 to 14.19. On `MUT-5`, DCG 20.54 compares with R2 35.59 and B0 57.16; on `MUT-9`, DCG 3.00 compares with R2 4.44 and B0 10.88, with $S_{25}$ 7.44 versus 5.19/1.74.
   - *Systems Mechanism for $k_1 = 3.00$ on `MUT-1` and `MUT-9`:* When early synced `Delete` or `DeleteRange` operations target keys that are not yet present in the committed lower-bound state $S_L$, rolling back those tombstones on crash leaves the recovered key-value state identical to $S_L$ (a logical no-op in key-value state space); thus the first two flagged `StateKey` representatives recover to a legal state and the third flagged representative ($k_1 = 3.00$) is the first observable key-value violation.
4. **Robustness on Sparse Invariant Presets (`ycsb-b`):** Under `ycsb-b/MUT-5`, read operations dominate so heavily that the $k=1$ WAL sync drop never intersects an unpersisted acknowledged write before shutdown, yielding a true negative 0.0% density. `DCG` correctly yields $k_1 = 0.00$ without false alarms.
5. **Early-Budget State Coverage ($S_{25}$) & Empirical Generalization:** Across the 14 failing preset × fault settings, `DCG` has lower mean $k_1$ than B0 on **14/14** and R2 on **13/14**, with one regression on `delete-heavy/MUT-1` (3.00 vs. 2.37). DCG $S_{25}$ matches or exceeds R2 on all 14 settings, strictly higher on 9 and tied on 5. Because these traces use 25 operations, $B=25$ approaches the full equivalence-class universe for several presets.

---

## 15. Related Work and Scope Limitations

### 15.1 Positioning Against Prior Crash-Consistency Testing Systems

**Torturing Databases** (Zheng et al., OSDI '14) pioneered transactional database crash testing using software-defined storage wrappers to simulate power cuts and fsync omissions, uncovering silent data loss and corruption across major database engines [1]. CraConTest builds upon this empirical foundation with trace-derived POSIX contract features and dependency-guided prioritization.

**ALICE** (OSDI '14) introduced application-level crash-consistency testing under file-system persistence models and showed that applications can rely on incorrect ordering and atomicity assumptions [2]. CraConTest focuses on ranking candidates from a recorded VFS trace rather than enumerating all modeled reorderings.

**Ferrite** (ASPLOS '16) formalized file-system crash-consistency models and checked application guarantees against permitted reorderings [3]. CraConTest uses a narrower VFS-level persistence model to construct contract indicators from application traces.

**CrashMonkey/B3** (OSDI '18) combines bounded workload generation with record-and-replay crash testing for file systems [4]. Its workloads are structured POSIX-call sequences; CraConTest instead evaluates a database-level prefix and atomicity oracle over crash states reconstructed from one storage-engine trace.

**Hydra** (SOSP '19) provided an extensible fuzzing framework for finding semantic file-system bugs, including crash-consistency failures [5]. CraConTest operates above the file system and ranks crash candidates from a fixed engine trace rather than generating file-system workloads.

**Witcher** (SOSP '21) targets persistent-memory key-value stores, indices, and libraries by inferring likely ordering and atomicity conditions from data and control dependencies over stores, flushes, and fences, then validating selected states through output equivalence [6]. CraConTest operates at the POSIX VFS layer and uses explicit durability-contract features. Silhouette and C3 illustrate complementary limitations of pairwise guardian-style inference for file-system mechanisms and counting correlations [9, 11]. The 108-run CraConTest null control produced no observed failures, but this fixed, dependent sample does not estimate a population false-alarm probability; oracle validity remains contingent on the modeled contract.

**Vinter** (USENIX ATC '22) used full-system dynamic binary translation and heuristic crash-image selection to test NVM systems [7].

**Chipmunk** (EuroSys '23) generates and replays crash images for persistent-memory file systems [8]. CraConTest targets application-level database consistency above the file system.

**Silhouette** (FAST '25) uses consistency-mechanism invariants to reduce crash-state exploration in persistent-memory file systems [9].

**Pathfinder** (OOPSLA '25) constructs persistence graphs from execution traces and groups correlated update behaviors for representative crash-state reduction and DPOR model checking [10]. CraConTest's `R2` baseline uses a substantially simpler `StateKey` partition rather than reimplementing Pathfinder. `DCG` addresses the complementary problem of budgeted ordering among these internal equivalence classes by adding trace-derived contract features.

### 15.2 Evaluation Chronology and Empirical Scope Boundaries

To ensure complete methodological transparency, we explicitly document the commit chronology of all evaluation suites relative to the code freeze tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**:
- **Pre-Freeze Fault Development (`c67a44a`, 2026-11-01):** The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault models were committed to evaluate whether the binary POSIX durability-contract flags generalize across arbitrary sync-drop indices and probabilities without re-tuning.
- **Code Freeze at Tag `v1.0-dcg-pr-freeze` (`67bbe7d`, 2026-11-02):** The ranking logic in `internal/strategy/dcg.go` and the feature extractor in `internal/graph/features.go` were frozen following development and calibration on Pebble `#5825` (§9).
- **Strictly Post-Freeze Evaluations (0 bytes changed to ranking/feature code):**
1. *Post-Freeze Evaluation 1 (Intra-Class Subsystem Transfer — checkpoint race later fixed by `15b500fb`, §10):* 32 seeds, `MUT-0` vs. `BARRIER-CKPT`. Case Study 2 evaluates intra-class subsystem transfer within the marker directory-race class.
  2. *Post-Freeze Evaluation 2 (Cross-Channel Ordering — Direct L0 `db.Ingest` Cross-Channel Prefix Hole, §13.5):* 32 seeds, `MUT-0` vs. `BARRIER-INGEST`. Evaluates input-dependent cross-channel prefix contract divergence.
  3. *Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §14):* 5 workload presets across 16 seeds and 4 fault models.

The following scope boundaries are established empirically across 32 seeds:

1. **Trace-Contract Linter Nature of DCG:** The 0%-retention indicators inspect the same missing barriers targeted by the synthetic sync-omission faults (`MUT-1..9`, `MUT-X1..X5`). The observed $k_1 = 1.00$ thus reflects substantial by-construction alignment plus `StateKey` equivalence deduplication; it does not establish equivalent performance on unrelated fault classes. `ContractFeatures` in `internal/graph/features.go` defines these indicators alongside `UnsyncedDirEntryCount`.

2. **StateKey Approximation Scope & Universe Boundaries:** `StateKey` represents a two-sided approximation: finer than the disk image on pending entries due to creating `trace.Event` ID inclusion, and coarser than the consistency oracle on dirty spans and client acknowledgment. Because `StateKey` hashes only VFS persistence state and does not encode client `OpBegin`/`OpEnd` boundaries, two cutoffs sharing a `StateKey` (such as before and after a client `OpEnd` with no intervening VFS mutation, as tested in `TestStateKeyIgnoresOpBeginAndOpEnd` in `internal/graph/graph_test.go`) yield identical 0%-retention disk images, but can be evaluated against different oracle `[L, U]` prefix bounds if a client operation was acknowledged between them. Therefore, `StateKey` representative selection (`R2`, `DCG`) is an equivalence reduction over the physical persistence state rather than an isomorphism over oracle verdicts. Within each `(seed, fault)` run, candidate universes are identical across all strategies, but universe size varies across fault models because `faults.FS` intercepts syncs prior to recording.

3. **Where frozen 0%-first DCG loses:** On uninjected 50%-retention multi-entry directory races requiring asymmetric partial retention, such as Pebble `#5825` and the checkpoint race later fixed by `15b500fb`, frozen 0%-first `DCG` underperforms corrected B0 (median $203.00$ vs. $178.73$ on #5825; $288.00$ vs. $262.26$ on Checkpoint). The post-hoc partial-retention prior finds #5825 at median $k_1=2.00$ but has corrected Checkpoint median $245.00$.

4. **Where DCG beats R2:** Single-resource sync-omission faults (`MUT-1..9` and parameterized/stochastic `MUT-X1..X5`) generally favor DCG on first-failure cost $k_1$ and early-budget state discovery $S_{25}$. In the revised parameterized reevaluation, DCG records 246 wins, 1 loss, and 48 ties across the 12 analyzed settings; the stochastic `MUT-X4_p04` loss is an explicit limitation. Across multi-distribution presets, DCG beats R2 on $k_1$ in 13 of 14 failing settings and matches or exceeds R2 on $S_{25}$ across all 14 settings.

5. **Where DCG-FlagOnly ties DCG:** Across almost all settings on `MUT-1..9`: `DCG` beats `R2` with `113 wins / 0 losses / 124 ties`, `DCG-FlagOnly` beats `R2` with `113 wins / 2 losses / 122 ties`, and `DCG` vs. `DCG-FlagOnly` ties on `235 of 237` active seed-runs (`2 wins / 0 losses / 235 ties`, differing on only 2 seeds of `MUT-3`), demonstrating that binary POSIX durability-contract flags combined with `StateKey` equivalence partitioning drive the speedup rather than continuous tier weights.

6. **Where DCG ties R2:** At full budget $S_{100}$ on moderate-scale traces where all ~35–40 `StateKey` equivalence classes are exhausted before 100 picks, and on out-of-contract non-sync faults (`MUT-X6`).

7. **Ingest Synchronization & Cleaner State-Space Representation:** With a fault-wrapped filesystem, synced client mutations (`set`, `delete`, `batch`, `range_delete`) wait on `WaitForWALWrite` before `OpEnd`, including dropped-write notifications for `MUT-X6`. During ingest workloads, `inIngestSection` gates non-staging, non-marker background `Remove` calls on `ingestCond` until a deferred `EndIngestSection()` call broadcasts upon `db.Ingest` return. Outside ingest sections, `FlushBegin` waits on pending obsolete-manifest deletion before starting another flush. Recorder waits and `drainBackgroundWork` use a configurable 30-second default deadline (`CRACONTEST_WAIT_TIMEOUT`); recorder deadline violations panic, whereas the workload drain returns an error. The workload also uses cooperative yielding, short sleep backoffs, and 2 ms settling delays, so synchronization is not entirely event-driven. Repeated-run tests support repeatability for the evaluated workload shapes but do not prove schedule independence. Ingest gating shifts background cleaner deletions to after `EndIngestSection()`, under-representing mid-ingest cleaner states and over-representing post-ingest states.

---

## 16. Claim-to-Command and Artifact Provenance Ledger

For step-by-step reproduction instructions, CLI invocations, and environment setup across Linux and Windows platforms, see the comprehensive [docs/reproduce.md](reproduce.md).

Every quantitative claim is linked below to a command and committed artifact. Tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) freezes only `internal/strategy/dcg.go` and `internal/graph/features.go`; it does not contain the later checkpoint, ingest, or workload-diversity extensions. The Stage 2 CSV artifacts record validation commit `ff6ac305e3e2`, which preserves the frozen ranking and feature files while including the post-freeze experiment code. Reproducing those measurements requires the recorded revision and the pinned analysis stack, not the freeze tag alone. Later ranking or feature revisions require a separate frozen snapshot and regenerated results; passing implementation tests does not establish equivalence to the historical rankings, and existing CSV provenance must not be changed independently of the measurements.

> **Platform Note:** On Windows, invoke statistical analysis commands using `python` or `py` rather than `python3` (and build with `CGO_ENABLED=0`).

| Claim | Reproduction Command | Committed Artifact |
|---|---|---|
| Stage 1 replay fidelity, null control, dense faults `MUT-0..4` | `go test -v ./internal/exp -run 'TestGateG4\|TestMainRunArtifacts'` | `results/smoke_mut0..1/`, `results/discovery_*.csv` |
| Stage 2 32-seed in-sample benchmark `MUT-0..9` | `go run ./cmd/cracontest benchmark -ops 120 -csv results/benchmark_summary.csv -per-seed-csv results/benchmark_per_seed.csv && python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9` | `results/benchmark_summary.csv`, `results/benchmark_per_seed.csv` |
| Paired stats (DCG vs. R1, R2, B0, B2) | `python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9` | — |
| Holdout `MUT-X1..X5` and out-of-contract `MUT-X6` | `go run ./cmd/cracontest benchmark -ops 120 -faults MUT-X1_k2,MUT-X1_k3,MUT-X1_k4,MUT-X1_k6,MUT-X1_k9,MUT-X2_k1,MUT-X2_k3,MUT-X2_k4,MUT-X2_k5,MUT-X3_k1,MUT-X3_k2,MUT-X3_k3,MUT-X4_p04,MUT-X5_p12,MUT-X6_k2,MUT-X6_k5,MUT-X6_k9 -csv results/holdout_summary.csv -per-seed-csv results/holdout_per_seed.csv -report "" && python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12` | `results/holdout_summary.csv`, `results/holdout_per_seed.csv` |
| Pebble `#5825` case study A/B and `dcg-pr` | `go run ./cmd/cracontest benchmark -faults MUT-0,BARRIER-5825 -max-manifest-size 1 -policies 0,50,100 -csv results/casestudy_pebble5825_summary.csv -per-seed-csv results/casestudy_pebble5825_per_seed.csv -report ""` | `results/casestudy_pebble5825_summary.csv`, `results/casestudy_pebble5825_per_seed.csv` |
| Checkpoint holdout A/B (frozen at `v1.0-dcg-pr-freeze`) | `go run ./cmd/cracontest benchmark -faults MUT-0,BARRIER-CKPT -checkpoint-at 60 -policies 0,50,100 -csv results/casestudy_checkpoint_summary.csv -per-seed-csv results/casestudy_checkpoint_per_seed.csv -report ""` | `results/casestudy_checkpoint_summary.csv`, `results/casestudy_checkpoint_per_seed.csv` |
| `MUT-5` universe-scaling (`120..1200` ops) | `go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update` | `results/scaling_mut5_summary.csv`, `results/scaling_mut5_per_seed.csv` |
| Multi-shape null control (`108` runs, `12` shapes) | `go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update` | `results/null_control_multishape.csv` |
| Subsystem empirical discovery (Workloads A, B, C under `MUT-0`) | `go test -v -count=1 -run TestSubsystemDiscovery ./internal/exp -update` | `results/subsystem_discovery.csv` |
| Direct L0 `db.Ingest` 32-seed benchmark | `go test -v -count=1 -run TestCaseStudyIngestBenchmark ./internal/exp -update-artifacts` | `results/casestudy_ingest_summary.csv`, `results/casestudy_ingest_per_seed.csv` |
| Workload diversity benchmark 5 presets | `go test -v -count=1 -run TestWorkloadDiversityBenchmark ./internal/exp -update-artifacts` | `results/workload_diversity_summary.csv`, `results/workload_diversity_per_seed.csv` |

> **Note on Code Freeze & Pre vs. Post-Freeze Chronology:** Tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) pins `internal/strategy/dcg.go` and `internal/graph/features.go`. Parameterized and stochastic fault models (`MUT-X1..X5`) and out-of-contract control (`MUT-X6`) were committed prior to freeze at `c67a44a` (2026-11-01). The three subsequent evaluations—the checkpoint race later fixed by `15b500fb` (§10), Direct L0 Ingest Cross-Channel Hole (§13.5), and Multi-Distribution Workload Suite (§14)—were designed and executed strictly post-freeze with 0 bytes of change to the frozen ranking and feature code.

---

## 17. References

1. M. Zheng, J. Tucek, D. Huang, F. Qin, M. Lillibridge, E. S. Yang, B. W. Zhao, and S. Singh. "Torturing Databases for Fun and Profit." *11th USENIX Symposium on Operating Systems Design and Implementation (OSDI '14)*, pp. 449–464, 2014. https://www.usenix.org/conference/osdi14/technical-sessions/presentation/zheng_mai
2. T. S. Pillai, V. Chidambaram, R. Alagappan, S. Al-Kiswany, A. C. Arpaci-Dusseau, and R. H. Arpaci-Dusseau. "All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications." *11th USENIX Symposium on Operating Systems Design and Implementation (OSDI '14)*, pp. 433–448, 2014. https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai
3. J. Bornholt, A. Kaufmann, J. Li, A. Krishnamurthy, E. Torlak, and X. Wang. "Specifying and Checking File System Crash-Consistency Models." *Proceedings of the 21st ACM International Conference on Architectural Support for Programming Languages and Operating Systems (ASPLOS '16)*, pp. 83–98, 2016. https://doi.org/10.1145/2980024.2872406
4. J. Mohan, A. Martinez, S. Ponnapalli, P. Raju, and V. Chidambaram. "Finding Crash-Consistency Bugs with Bounded Black-Box Crash Testing." *13th USENIX Symposium on Operating Systems Design and Implementation (OSDI '18)*, pp. 33–50, 2018. https://www.usenix.org/conference/osdi18/presentation/mohan
5. S. Kim, M. Xu, S. Kashyap, J. Yoon, W. Xu, and T. Kim. "Finding Semantic Bugs in File Systems with an Extensible Fuzzing Framework." *27th ACM Symposium on Operating Systems Principles (SOSP '19)*, pp. 147–161, 2019. https://doi.org/10.1145/3341301.3359662
6. X. Fu, W.-H. Kim, A. P. Shreepathi, M. Ismail, S. Wadkar, D. Lee, and C. Min. "Witcher: Systematic Crash Consistency Testing for Non-Volatile Memory Key-Value Stores." *28th ACM Symposium on Operating Systems Principles (SOSP '21)*, pp. 100–115, 2021. https://doi.org/10.1145/3477132.3483556
7. S. Kalbfleisch, L. Werling, and F. Bellosa. "Vinter: Automatic Non-Volatile Memory Crash Consistency Testing for Full Systems." *2022 USENIX Annual Technical Conference (USENIX ATC '22)*, pp. 933–950, 2022. https://www.usenix.org/conference/atc22/presentation/werling
8. H. LeBlanc, S. Pailoor, O. S. K. R. E., I. Dillig, J. Bornholt, and V. Chidambaram. "Chipmunk: Investigating Crash-Consistency in Persistent-Memory File Systems." *18th European Conference on Computer Systems (EuroSys '23)*, pp. 718–733, 2023. https://doi.org/10.1145/3552326.3567498
9. B. Jiao, A. Goel, and A.-I. A. Wang. "Silhouette: Leveraging Consistency Mechanisms to Detect Bugs in Persistent Memory-Based File Systems." *23rd USENIX Conference on File and Storage Technologies (FAST '25)*, 2025. https://www.usenix.org/conference/fast25/presentation/jiao
10. Y. Gu, I. Neal, J. Xu, S. C. Lee, A. Said, M. Haydar, J. Van Geffen, R. Kadekodi, A. Quinn, and B. Kasikci. "Scalable and Accurate Application-Level Crash-Consistency Testing via Representative Testing." *Proceedings of the ACM on Programming Languages*, vol. 9, OOPSLA1, Article 97, 2025. https://doi.org/10.1145/3720431
11. Y. Zhang, J. Qiu, H. Xu, and J. Xue. "C3: Finding Counting-Related Crash Consistency Atomicity Bugs for Persistent Memory." *ACM Transactions on Storage*, vol. 22, no. 3, 2026. https://doi.org/10.1145/3786994
12. CockroachDB Pebble. "crl-release-25.4, crl-release-26.1: TestWALFailoverRandomized failure," issue #5825, 2026. https://github.com/cockroachdb/pebble/issues/5825
13. CockroachDB Pebble. "checkpoint: ensure manifest durable before marker creation," commit `15b500fbdeb2878bdb6bbea2704332bccc32110c`, 2026. https://github.com/cockroachdb/pebble/commit/15b500fbdeb2878bdb6bbea2704332bccc32110c
14. A. Kryczka. "Verifying crash-recovery with lost buffered writes." RocksDB Blog, October 5, 2022. https://rocksdb.org/blog/2022/10/05/lost-buffered-write-recovery.html
15. CockroachDB Pebble. "v2.1.7," release `66d468e449bbd52ddf9e53c3a7cdec37e885aaee`, August 24, 2026. https://github.com/cockroachdb/pebble/releases/tag/v2.1.7
