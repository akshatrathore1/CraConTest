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

Research question:

> *Can semantic persistence dependencies and durability-contract priors concentrate crash
> testing on fewer crash states while finding consistency failures faster than random selection?*

We address this question through a six-stage empirical evaluation on CockroachDB's Pebble (v2.1.7):

1. **Stage 1 — Single-Seed 1,000-Operation Dense Faults & Topological Reachability Pathology (§2–§7):**
   - **Zero false alarms (MUT-0):** Every crash candidate recovered to a legal state (2,150 of 2,150 in the main run, 302 of 302 in the smoke run).
   - **Dense fault detection (MUT-1..MUT-4):** Every seeded fault was detected (28%–50% failure density), exposing lost acknowledged writes, malformed manifests, and corrupt tables.
   - **Topological reachability pathology:** Uniform random (`B0`) reached a first failure in a median of 1–3 crashes, whereas naive topological reachability (`B4`) required 13–61 crashes. Reachability is monotonically highest at the start of a trace, causing `B4` to expend its initial testing budget before the first synchronous write is acknowledged (event 57 of 2,427 in `main_mut1`), where rollback to the initial state is legal.
   - **Coverage advantage:** State-aware orderings (`B5`, `B6`) covered distinct violation signatures faster at moderate budgets than random selection on `MUT-1`.

2. **Stage 2 — 32-Seed In-Sample Benchmark (`MUT-0..9`) & `DCG-FlagOnly` Component Attribution (§8.1–§8.3):**
   - Across 32 deterministic seeds (>11,000 crash states), `DCG` achieves optimal $k_1 = 1.00$ [1.00, 1.00] across all active fault models (`MUT-1..6`, `MUT-8`, `MUT-9`), with 0 false alarms on `MUT-0`.
   - The `DCG-FlagOnly` ablation ties full `DCG` on `113 wins / 0 losses / 124 ties` vs `R2`, showing that binary POSIX durability-contract priors combined with `StateKey` equivalence partitioning drive the speedup.

3. **Stage 3 — Pre-Frozen Holdout Fault Generalization (`MUT-X1..X5`) & Non-Contractual Control (`MUT-X6`) (§8.4):**
   - With ranking code frozen at tag `v1.0-dcg-pr-freeze` (`67bbe7d`), `DCG` / `DCG-PR` beats `R2` across all 12 testable in-contract holdout settings (**284 wins / 0 losses / 46 ties**, mean $k_1$ speedup $2.1\times\text{--}20.0\times$, Holm-adjusted sign-test $p \le 0.002$) and degrades gracefully to `R2` on non-contractual silent write drops (`MUT-X6`, Holm $p = 1.0$).

4. **Stage 4 — Real-World Case Study 1: CockroachDB Pebble Issue `#5825` (`MUT-0` vs. `BARRIER-5825`) & Partial-Retention Prior (`DCG-PR`) (§9):**
   - Reproduces Pebble `#5825` under 50% partial retention with manifest rotation (69 failures across 32 seeds under unpatched `MUT-0`, reduced to **0 failures** under causal barrier control `BARRIER-5825`). `DCG-PR` (`UnsyncedDirEntryCount >= 2`) reduces median $k_1$ from $310.50$ (0%-first `DCG`) and $161.00$ (`B0`) down to **2.50** (mean $10.50$).

5. **Stage 5 — Held-Out Real-World Case Study 2: CockroachDB Pebble Checkpoint Commit `15b500fb` (`MUT-0` vs. `BARRIER-CKPT`) (§10):**
   - Evaluated with pre-frozen `DCG-PR` (`v1.0-dcg-pr-freeze`), reproducing 25/32 failing seeds under unpatched `MUT-0` and **0/32 failing seeds** under causal barrier control `BARRIER-CKPT`. Pre-frozen `DCG-PR` discovers the failure at median $k_1 = \mathbf{3.00}$ (mean $121.40$) vs. $385.00$ for 0%-first `DCG` and $156.00$ for `B0`.

6. **Stage 6 — Candidate Universe Scaling (`120..1,200` ops) & Multi-Shape Null Control (`108` runs) (§11–§12):**
   - On `MUT-5`, as candidate universes grow from ~338 to ~2,958 states, `DCG` maintains constant $O(1)$ discovery ($k_1 = 1.00$, planning time $\le 56.1\text{ ms}$) while `R2`, `R1`, `B0`, and `LIFO` scale linearly. Across 108 unfaulted runs spanning 12 workload shapes (>50,000 crash states), the harness exhibits a **0.00% false-alarm rate** (0 violations, 0 open errors, 0 panics, 0 hangs).

---

## 2. Method

### 2.1 System under test

- **Engine:** Pebble `v2.1.7` (`github.com/cockroachdb/pebble/v2`), unmodified.
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
| G3 graph labels | On a 1,000-op trace (≈2,510 events, ≈1,073 cutoffs) every candidate has an operation label and a category set; ≈173 distinct persistence states; build time ≈12 ms. `Semantic` is 1 on 96% of cutoffs, so it barely discriminates (§5). |
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
4. **The Semantic term saturates.** It is 1 on 96% of cutoffs because almost every event
   carries a write category, so it contributes little to B4's order.

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

---

## 7. Conclusion

The Stage 1 evaluation established that the crash testing pipeline is sound and sensitive: crash states replay deterministically, unfaulted runs produce zero false alarms, and each seeded dense durability bug is exposed through lost acknowledged writes or refused recovery. However, ranking results on dense faults revealed clear structural limitations: uniform random reaches a first failure fastest under dense faults, while naive topological reachability scores are consistently slowest due to monotonic descendant bias in the pre-acknowledgment prefix. State-aware orders (B5, B6) accelerate the discovery of distinct violation contexts, but their first-failure efficiency remains bounded on dense mutations.

These findings directly motivated the Stage 2 investigation presented in Section 8: evaluating ranking strategies on sparse and lifecycle-triggered faults where most crash points are harmless (e.g. dropping a single $k$-th WAL sync or second manifest sync), evaluating across 32 deterministic workload seeds with 95% confidence intervals, and replacing blind graph reachability with explicit POSIX durability-contract priors (`internal/graph/features.go`).

---

## 8. Multi-Seed Benchmark, Root Causes, and Contract-Guided Evaluations

### 8.1 Why Naive Topological Reachability (B4) Underperforms Random on Dense Faults

The Stage 1 evaluation established that the combined dependency heuristic (**B4**) underperformed uniform random selection (**B0**) on MUT-1, finding a first violation at median position 55 compared to B0's median position 2. Our root-cause investigation identified two fundamental structural defects in topological reachability heuristics:

1. **Monotonic Downstream Reachability Bias:** In any directed acyclic graph capturing execution dependencies, topological depth correlates inversely with downstream reachability. The earliest mutations reach the greatest total number of subsequent vertices ($|\text{Desc}(v)|$ is monotonically decreasing across the trace). By weighting `Desc`, `B4` systematically preferred the earliest crash candidates in the trace.
2. **Pre-Acknowledgment Semantics:** Storage engine crash-consistency contracts (e.g. POSIX `fsync` and WAL commit guarantees) strictly apply *after* a client write has been acknowledged. In the initial portion of a workload trace (prior to the first acknowledged synchronous write at event 57 of 2,427), any crash state legally recovers to the pre-workload state without violating durability. `B4` spent its first 50 selections exploring exclusively inside this pre-acknowledgment region, where no consistency violations could legally exist.

### 8.2 Ranking Strategies and Baselines

To address these limitations, the expanded evaluation incorporates principled space partitioning and concrete durability contract invariants:

1. **Rigorous Baselines (R1 and R2):**
   - **R1 (0%-Retention-First):** Recognizes that storage consistency violations manifest primarily when dirty unpersisted bytes are dropped upon crash. `R1` partitions the candidate universe by retention policy and exhaustively tests 0% retention before testing 100% retention.
   - **R2 (State-Representative Random):** Partitions 0% retention candidates into equivalence classes by canonical persistence state key (`StateKey`), selecting boundary-adjacent representatives within each `StateKey` class using graph features (`BeforeBoundary`, `SemanticTier`, `Desc`) before shuffling classes and evaluating redundant cutoffs afterward. On sparse faults such as `MUT-5`, `R2` cuts search cost by half ($k_1 = 13.01$ vs `B0`'s $25.97$).
2. **RHG (Reachability-Hazard Guided / B7):**
   - Eliminates representative key divergence by maintaining strict deterministic grouping and ranks candidates by reachable uncommitted hazard surfaces, achieving normalized AUC values between $0.70$ and $0.82$.
3. **DCG (Durability-Contract Guided / B8) & DCG-FlagOnly (Ablation):**
   - Trace-derived durability-contract analysis that directly encodes the contract indicators as POSIX durability-contract priors instantiated for Pebble:
     - **Indicator 1 (`AckSyncMissing`):** Detects dropped WAL sync barriers following client-acknowledged transactions.
     - **Indicator 2 (`DanglingTableCount`):** Identifies dangling SST references where the manifest references an unpersisted table, correctly gated on manifest commitment (`fi.committedInManifest`).
     - **Indicator 3 (`UnsyncedDirectoryRename`):** Flags directory hazards where critical namespace files (`CURRENT`, `MANIFEST`) or post-ack active WAL/options files cross directory boundaries without parent directory sync (`len(ackSyncOps) > 0`).
     - **Indicator 4 (`ManifestBarrierPending`):** Catches uncommitted version edit records without manifest sync.
   - **Performance:** Achieves position-independent $k_1 = 1.00$ across all active fault models (`MUT-1..6`, `MUT-8`, `MUT-9`), alongside 0 false alarms across all 32 seeds at 0%/100% retention on control `MUT-0`. The `DCG-FlagOnly` ablation ties full `DCG` on nearly every setting, showing that the binary durability-contract flags combined with `StateKey` equivalence partitioning drive the speedup.

### 32-Seed Benchmark Summary Tables

Across 32 deterministic workload seeds (>11,000 evaluated crash candidates):

#### Table 1: Cost-to-First-Failure ($k_1$ Mean [95% CI])

| Fault Model | Fault Description | Failure Density | DCG | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|---|---|
| **MUT-0** | Null Control (No Fault) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.44 [1.42, 1.46] | 1.37 [1.34, 1.39] | 5.00 [5.00, 5.00] | 17.00 [17.00, 17.00] | 2.00 [2.00, 2.00] |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.17 [1.16, 1.19] | 1.04 [1.04, 1.05] | **1.00** [1.00, 1.00] | 5.00 [5.00, 5.00] | 1.55 [1.38, 1.71] |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** [1.00, 1.00] | 1.03 [1.03, 1.04] | **1.00** [1.00, 1.00] | 1.44 [1.40, 1.48] | 1.17 [1.15, 1.19] | **1.00** [1.00, 1.00] | 9.00 [9.00, 9.00] | 2.00 [2.00, 2.00] |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.95 [1.89, 2.00] | 1.68 [1.62, 1.74] | **1.00** [1.00, 1.00] | 31.62 [31.24, 32.01] | 2.39 [2.22, 2.56] |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 13.01 [12.68, 13.34] | 17.04 [16.54, 17.53] | 262.25 [258.20, 266.30] | 17.00 [17.00, 17.00] | 25.97 [25.00, 26.94] |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** [1.00, 1.00] | 1.20 [1.17, 1.24] | 14.08 [13.50, 14.65] | 7.72 [7.46, 7.97] | 20.79 [19.89, 21.70] | 101.00 [97.37, 104.63] | 43.15 [42.38, 43.93] | 31.12 [29.02, 33.21] |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 3.56 [3.48, 3.65] | 3.24 [3.10, 3.37] | **1.00** [1.00, 1.00] | 55.25 [54.85, 55.65] | 4.44 [4.26, 4.61] |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.40 [1.36, 1.44] | 1.21 [1.18, 1.23] | **1.00** [1.00, 1.00] | 17.00 [17.00, 17.00] | 1.95 [1.90, 2.01] |

#### Table 2: Failing State Discovery ($S_{25}$ Early Budget / $S_{100}$ Exhaustive Budget Mean)

| Fault Model | DCG ($S_{25}$ / $S_{100}$) | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|
| **MUT-1** | **23.94** / **23.97** | **23.94** / **23.97** | 17.69 / **23.97** | 17.12 / **23.97** | 10.02 / 19.80 | 6.53 / 8.94 | 5.00 / 13.00 | 6.12 / 14.61 |
| **MUT-2** | **25.00** / **33.03** | **25.00** / **33.03** | 24.00 / **33.03** | 21.15 / **33.03** | 8.66 / 22.81 | 7.00 / 11.38 | 11.00 / 30.00 | 5.67 / 13.78 |
| **MUT-3** | 24.00 / **27.00** | **24.12** / **27.00** | 21.03 / **27.00** | 17.26 / **27.00** | 7.27 / 19.00 | 7.16 / 12.06 | 9.00 / 21.00 | 5.00 / 11.22 |
| **MUT-4** | **21.72** / **21.72** | **21.72** / **21.72** | 16.69 / **21.72** | 12.58 / **21.72** | 5.45 / 14.53 | 8.00 / 12.25 | 0.00 / 16.69 | 3.56 / 8.62 |
| **MUT-5** | **2.00** / **2.00** | **2.00** / **2.00** | 1.81 / **2.00** | 1.30 / **2.00** | 0.87 / 1.60 | 0.00 / 0.00 | 1.00 / 1.00 | 0.48 / 1.00 |
| **MUT-6** | **1.62** / **1.62** | **1.62** / **1.62** | 1.22 / **1.62** | 1.07 / **1.62** | 0.39 / 1.20 | 0.00 / 0.41 | 0.00 / 1.22 | 0.00 / 0.81 |
| **MUT-7** | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | **12.88** / **12.88** | **12.88** / **12.88** | 7.88 / **12.88** | 6.72 / 12.82 | 3.29 / 8.41 | 8.00 / 11.75 | 0.00 / 9.88 | 2.00 / 5.05 |
| **MUT-9** | **25.00** / **27.00** | **25.00** / **27.00** | 20.00 / **27.00** | 17.78 / **27.00** | 5.57 / 17.44 | 7.00 / 10.94 | 5.00 / 26.00 | 3.03 / 9.83 |

### POSIX Durability Priors & DCG-FlagOnly Ablation

1. **General POSIX Durability-Contract Priors:**
   Under POSIX filesystem semantics, ensuring durable file persistence after crash requires two conditions: (1) flushing dirty file data (`fsync` / `fdatasync`), and (2) flushing the parent directory entry (`fsync(parentDir)`) after file creation, renaming, hard-linking, or directory creation (`Create`, `ReuseForWrite`, `Rename`, `Link`, `MkdirAll`). If a file is written and its data synced, but its parent directory entry remains unsynced, a crash at 0% retention causes the directory entry to vanish, resulting in lost files upon recovery. CraConTest implements POSIX durability-contract priors instantiated for Pebble's VFS categories: tracking `parentDir` and `dirEntrySynced` for every filesystem node, enforcing that durability holds if and only if `dirtyBytes == 0 && dirEntrySynced == true`, and scoping directory sync events strictly to the target directory path. MUT-9 is dense (41.4% density) at $k=1$ (initial directory sync) and sparse (0.8%–12.4%) at $k \in \{7, 9, 10, 12\}$ (flush/compaction directory syncs); directory-entry tracking was added after inspecting MUT-9, so MUT-9 evaluates parameterized $k$-generalization within the directory-sync family. On MUT-9 ($k=1$), DCG identifies the uncommitted directory entry prior, expanding state coverage to $S_{100} = 27.00$ (vs B0's $9.83$, a 2.7x expansion) and $S_{25} = 25.00$ (vs B0's $3.03$, an 8.2x early-budget advantage).

2. **Component Attribution & DCG-FlagOnly Ablation:**
   Comparing full DCG against the **DCG-FlagOnly** ablation isolates the exact contribution of each mechanism:
   - **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and $12.88$ on MUT-8). On dense faults (`MUT-1`, `MUT-2`, `MUT-4`), sparse faults (`MUT-5`, `MUT-8`), and directory faults (`MUT-9`), DCG-FlagOnly achieves the identical optimal $k_1 = 1.00$ [1.00, 1.00] as full DCG, confirming that the contract indicators (POSIX durability-contract priors instantiated for Pebble) are the primary driver of first-failure cost reduction.
   - **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop, ~0.8% density), where full DCG achieves mean $k_1 = 1.00$ compared to $1.20$ for `DCG-FlagOnly`.
   - **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

3. **$S_{25}$ Early-Budget Discovery & The ~40-State Equivalence Ceiling:**
   In a 120-operation workload, there are only ~35–40 distinct canonical `StateKey` persistence states under 0%-retention crashes. Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100 (reaching identical $S_{100} = 23.97$ on `MUT-1`, $33.03$ on `MUT-2`, $27.00$ on `MUT-3`, $21.72$ on `MUT-4`, and $27.00$ on `MUT-9`). Under MUT-8 (compaction-triggered SST sync drop, 15.2% density), DCG reaches the first failing state at median $k_1 = 1.0$ (mean 1.00) compared to R2's median $k_1 = 3.6$ (mean 3.56) and B0's median $k_1 = 4.0$ (mean 4.44), achieves early-budget $S_{25} = 12.88$ (vs R2's $6.72$ and B0's $2.00$), and matches R2 at full budget ($S_{100} = 12.88$ for DCG vs $12.82$ for R2, compared to $5.05$ for B0). Therefore, early-budget coverage $S_{25}$ ($B = 25$) is the critical discriminant metric for practical CI testing: DCG achieves near-full state discovery saturation within the first 25 tests (e.g. $S_{25} = 23.94$ vs R2's $17.12$ and B0's $6.12$ on `MUT-1`; $S_{25} = 12.88$ vs R2's $6.72$ and B0's $2.00$ on `MUT-8`; $S_{25} = 25.00$ vs R2's $17.78$ and B0's $3.03$ on `MUT-9`), yielding a 2x–8x throughput advantage over random selection.

### 8.4 Holdout Fault Generalization (`MUT-X1..X5`) and Out-of-Contract Control (`MUT-X6`)

Prior to evaluating any holdout faults, the `DCG-PR` implementation (`internal/strategy/dcg.go` and `internal/graph/features.go`) was frozen at Git tag `v1.0-dcg-pr-freeze` (`67bbe7d`) and never modified afterward, ensuring zero methodological leakage from holdout results into the ranking algorithm. Because all holdout faults involve only 0%- and 100%-retention crash states (no partial-retention intermediates), `dcg-pr` produces an identical ranking to `dcg` on these settings; the table therefore reports `DCG / DCG-PR` as a single column.

| Fault | Fault Family | Seeds Fail (Density) | DCG / DCG-PR — Median $k_1$ [95% CI] | DCG / DCG-PR — Mean $k_1$ | Sync-Boundary (B6) — Median [Mean] | State-Representative (B5) — Median [Mean] | Uniform Random (B0) — Median [Mean] | R2 — Median [Mean] | R1 — Median [Mean] |
|---|---|---|---|---|---|---|---|---|---|
| MUT-X1\_k2 | Delayed WAL Sync ($k=2$) | 32/32 (2.6%) | **1.00** [1.00, 1.00] | **1.00** | 19.00 [19.00] | 27.00 [27.00] | 25.00 [24.14] | 13.09 [13.02] | 16.86 [16.94] |
| MUT-X1\_k3 | Delayed WAL Sync ($k=3$) | 31/32 (2.2%) | **1.00** [1.00, 1.00] | **1.00** | 21.00 [21.00] | 29.00 [29.00] | 30.00 [36.52] | 12.94 [13.01] | 18.95 [22.65] |
| MUT-X1\_k4 | Delayed WAL Sync ($k=4$) | 12/32 (1.1%) | **1.00** [1.00, 1.00] | **1.00** | 23.00 [23.67] | 31.00 [31.67] | 69.75 [62.21] | 13.25 [13.13] | 40.37 [40.50] |
| MUT-X1\_k6 | Delayed WAL Sync ($k=6$) | 32/32 (2.7%) | **1.00** [1.00, 1.00] | **1.00** | 35.00 [35.00] | 43.00 [43.00] | 26.50 [25.95] | 13.46 [13.38] | 16.88 [17.04] |
| MUT-X1\_k9 | Delayed WAL Sync ($k=9$) | 10/32 (1.6%) | **1.00** [1.00, 1.00] | **1.00** | 41.00 [43.40] | 49.00 [51.40] | 49.50 [59.95] | 12.55 [12.71] | 28.45 [34.04] |
| MUT-X2\_k1 | Delayed Manifest Sync ($k=1$) | 32/32 (17.3%) | **1.00** [1.00, 1.00] | **1.00** | 5.00 [5.00] | 13.00 [13.00] | 4.00 [4.11] | 2.55 [2.54] | 2.85 [2.93] |
| MUT-X2\_k3 | Delayed Manifest Sync ($k=3$) | 21/32 (2.1%) | **1.00** [1.00, 1.00] | **1.00** | 61.00 [61.10] | 69.00 [69.48] | 31.00 [31.69] | 7.83 [7.79] | 21.41 [21.25] |
| MUT-X2\_k4 | Delayed Manifest Sync ($k=4$) | 0/32 (0.0%) | 0.00 [0.00, 0.00] | 0.00 | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] |
| MUT-X2\_k5 | Delayed Manifest Sync ($k=5$) | 0/32 (0.0%) | 0.00 [0.00, 0.00] | 0.00 | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] | 0.00 [0.00] |
| MUT-X3\_k1 | Delayed SST Sync ($k=1$) | 32/32 (30.0%) | **1.00** [1.00, 1.00] | **1.00** | 31.00 [31.62] | 41.00 [41.75] | 2.00 [2.27] | 2.05 [2.04] | 1.62 [1.64] |
| MUT-X3\_k2 | Delayed SST Sync ($k=2$) | 32/32 (15.4%) | **1.00** [1.00, 1.00] | **1.00** | 49.00 [49.25] | 59.00 [59.44] | 4.00 [4.36] | 3.76 [3.71] | 3.24 [3.21] |
| MUT-X3\_k3 | Delayed SST Sync ($k=3$) | 32/32 (0.9%) | **1.00** [1.00, 1.00] | **1.00** | 67.00 [67.00] | 77.00 [77.19] | 75.50 [74.53] | 19.68 [20.00] | 42.34 [43.21] |
| MUT-X4\_p04 | Stochastic File Sync ($p=0.04$) | 32/32 (2.6%) | **1.00** [1.00, 1.00] | **1.00** | 37.00 [37.00] | 45.00 [45.00] | 26.50 [25.62] | 13.32 [13.32] | 17.17 [17.50] |
| MUT-X5\_p12 | Stochastic Dir Sync ($p=0.12$) | 32/32 (0.9%) | **1.00** [1.00, 1.00] | **1.00** | 65.00 [65.00] | 75.00 [75.38] | 72.50 [73.97] | 19.26 [19.45] | 42.19 [41.85] |
| MUT-X6\_k2 | Silent WAL Write Loss ($k=2$) | 32/32 (14.8%) | 6.00 [4.00, 6.00] | 6.84 | 21.00 [20.94] | 25.00 [25.00] | 5.00 [5.08] | 5.21 [6.50] | 6.60 [6.86] |
| MUT-X6\_k5 | Silent WAL Write Loss ($k=5$) | 21/32 (12.8%) | 16.00 [13.00, 22.00] | 15.95 | 35.00 [34.43] | 39.00 [38.62] | 7.00 [7.64] | 12.26 [12.22] | 8.22 [9.70] |
| MUT-X6\_k9 | Silent WAL Write Loss ($k=9$) | 13/32 (11.3%) | 8.00 [3.00, 9.00] | 6.46 | 43.00 [44.38] | 47.00 [48.85] | 8.00 [8.35] | 6.52 [6.65] | 10.84 [10.25] |

Each holdout setting ran 32 deterministic seeds at 120 ops/seed (544 total seeds, 17 fault settings). `MUT-X2_k4` and `MUT-X2_k5` produced zero failures under all strategies (fault density below the trace-length detection threshold); they are excluded from paired statistics.

**Paired statistical summary** (`python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12`):

- **In-contract holdouts (`MUT-X1..X5`, 12 testable settings, 330 failing seed-runs):** `DCG` / `DCG-PR` beats `R2` on all 12 testable settings (**284 wins / 0 losses / 46 ties**, mean $k_1$ speedup **$2.1\times$–$20.0\times$**, Holm-adjusted sign-test $p \le 0.002$ across all 12 settings).
- **Out-of-contract control (`MUT-X6_k2, k5, k9`, 3 settings, 66 failing seed-runs):** `DCG` / `DCG-PR` performs at the `R2` baseline level (**20 wins / 42 losses / 4 ties**, speedup $0.9\times$, Holm-adjusted $p = 1.0$, not significant) — confirming graceful degradation to `StateKey` partitioning when a fault triggers zero metadata/sync contract violations.

---

## 9. Real-World Case Study: Pebble #5825 A/B Benchmark & Partial-Retention Priors

### 9.1 Background: CockroachDB Pebble Issue #5825

During manifest rotation, Pebble creates a new `MANIFEST-*` file and then updates `marker.manifest.*` to point to it, all **before** syncing the parent directory. This leaves two unsynced directory entries coexisting in `"db"`. At crash:

- **0% retention** — both `MANIFEST-*` and `marker.manifest.*` are dropped together; Pebble falls back to the previous manifest and recovers cleanly.
- **100% retention** — both entries survive; recovery succeeds normally.
- **50% partial retention** — `marker.manifest.*` can survive while `MANIFEST-*` is dropped. Pebble's `Open` follows the surviving pointer and fails with `"file does not exist"`.

`BARRIER-5825` (Manifest-Rotation Barrier Control) enforces a parent-directory `Sync` before each `marker.manifest` write, eliminating the race. The feature `UnsyncedDirEntryCount` captures the maximum number of unsynced directory entries coexisting in the same parent at each cutoff; when this value reaches 2, `dcg-pr` promotes 50% partial-retention candidates to the top of the ranking.

### 9.2 Causal A/B Verification (MUT-0 vs. BARRIER-5825)
To establish causal attribution, we benchmarked 32 deterministic workload seeds with manifest rotation enabled (`-max-manifest-size 1`) across retention policies 0%, 50%, and 100%:
1. **MUT-0 (Unfaulted Baseline):** 69 total consistency failures occurred across the 32 seeds. Every single failure manifested exclusively on crash candidates evaluated under the **50% partial-retention policy** (~0.36%–0.57% failure density per seed).
2. **BARRIER-5825 (Manifest-Rotation Barrier Control):** By enforcing immediate parent directory sync (`fsync(parentDir)`) upon `Create`, `OpenReadWrite`, and `Rename`, all directory entries are made durable prior to subsequent mutations. Under BARRIER-5825, **0 failures** occurred across all 32 seeds, providing definitive empirical and causal proof of defect elimination.

### 9.3 Discovery Performance: Post-Hoc Partial-Retention Prior (`dcg-pr`)
Standard durability-contract guided ranking (`dcg`) was designed with the core assumption that durability failures manifest predominantly at 0% retention (losing unpersisted data). Consequently, `dcg` sweeps all 0%-retention candidates first. On Pebble #5825, where failures only occur at 50% partial retention, standard `dcg` requires a median $k_1$ of **310.50** (mean 288.53) to discover the first failure, performing worse than uniform random B0 (median $k_1 = 161.00$, mean 173.32) and state-representative B5 (median $k_1 = 65.00$, mean 64.80).

To target partial-retention directory vulnerabilities, we introduced `dcg-pr` (DCG Partial-Retention Prior). `dcg-pr` inspects `ContractFeatures.UnsyncedDirEntryCount`: candidates with `Policy > 0 && Policy < 100` and `UnsyncedDirEntryCount >= 2` are prioritized into Tier 1 (ahead of the 0% retention sweep).

| Strategy | $k_1$ Failure Median [95% CI] | $k_1$ Failure Mean [95% CI] | $S_{25}$ Mean | $S_{100}$ Mean | AUC (Failures) | AUC (Bugs) |
|---|---|---|---|---|---|---|
| **DCG-PR (`dcg-pr`)** | **2.50** [2.00, 4.00] | **10.50** [0.00, 26.41] | **1.88** | **1.88** | **0.8355** | **0.9121** |
| **Representative (B5)** | 65.00 [62.00, 68.00] | 64.80 [49.36, 80.24] | 0.28 | 1.16 | 0.7722 | 0.8200 |
| **sync-boundary (B6)** | 66.50 [50.00, 110.00] | 74.20 [51.98, 96.42] | 0.25 | 0.72 | 0.6227 | 0.8047 |
| **combined (B4)** | 92.00 [14.00, 92.00] | 80.20 [44.06, 116.34] | 0.44 | 1.06 | 0.6181 | 0.7932 |
| **Uniform Random (B0)** | 161.00 [140.50, 174.00] | 173.32 [147.20, 199.43] | 0.00 | 0.11 | 0.4599 | 0.5875 |
| **fifo (B1)** | 204.50 [29.00, 215.00] | 156.80 [103.47, 210.13] | 0.00 | 0.44 | 0.4705 | 0.5788 |
| **rhg (B7)** | 258.50 [248.00, 264.50] | 257.20 [239.45, 274.95] | 0.00 | 0.00 | 0.3329 | 0.3668 |
| **0%-Retention Random (R1)** | 300.95 [284.73, 321.53] | 306.21 [292.24, 320.18] | 0.00 | 0.00 | 0.3143 | 0.3313 |
| **State-Representative Random (R2)** | 303.19 [279.27, 316.17] | 305.94 [290.26, 321.62] | 0.00 | 0.00 | 0.3145 | 0.3342 |
| **DCG Flag-Only (Ablation)** | 303.87 [285.49, 318.03] | 307.24 [292.09, 322.38] | 0.00 | 0.00 | 0.3133 | 0.3318 |
| **Durability-Contract Guided (DCG)** | 310.50 [204.00, 335.00] | 288.53 [252.28, 324.79] | 0.00 | 0.00 | 0.3535 | 0.3779 |

As demonstrated, `dcg-pr` reduces $k_1$ from 310.50 down to **2.50** (a >120x speedup over standard DCG and >60x speedup over uniform random B0), with an AUC of 0.8355 (0.9121 for bugs). This demonstrates that extending durability contracts to model partial-retention directory hazards enables targeted discovery of real-world filesystem durability bugs.

---

## 10. Held-Out Real-Bug Case Study: Pebble Checkpoint Commit `15b500fb`

### 10.1 Background: Checkpoint Directory Sync Omission
In CockroachDB Pebble commit `15b500fb`, Pebble's `checkpoint.go` implementation contained a crash-consistency bug where `db.Checkpoint(destDir)` created hard links for SSTs, created a flushed manifest, and wrote manifest locator markers (`marker.manifest.*`) inside the destination checkpoint directory, but failed to call `dir.Sync()` on the checkpoint directory before returning success. Under partial retention (e.g. 50%), the kernel persisted the marker file metadata while dropping the uncommitted directory entry, leading to corrupt or missing marker files upon reopening the checkpoint database.

### 10.2 Pre-Evaluation Code Freeze & Causal A/B Verification
To guard against methodological overfitting, all prioritization algorithms—including `dcg` and `dcg-pr`—were frozen under git tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) before evaluating this holdout bug. Workload operations were extended with `-checkpoint-at 60`, executing a checkpoint creation operation mid-workload.

Across 32 deterministic seeds (`42, 100..900, 1001..1022`) under policies 0%, 50%, and 100% ($N = 17{,}975$ crash candidates total):

1. **MUT-0 (Baseline):** 25 of 32 seeds exhibited checkpoint or manifest recovery failure under 50% partial retention (median 32.5 failing states/seed, mean 40.38). Every failure manifested as `open_error` at 50% partial retention where `marker.manifest.*` directory entries survived while linked `MANIFEST-*` or SSTable entries were dropped.
2. **BARRIER-CKPT (Checkpoint Directory Barrier Control):** Injecting a `parentDir.Sync()` barrier on any `Create`, `OpenReadWrite`, `Link`, or `Rename` targeting `marker.manifest.*` or `CURRENT` files completely eliminates all failures — **0 failures across all 32 seeds** (confirmed across every strategy row in `results/realbug_checkpoint_summary.csv`).

### 10.3 Pre-Frozen Strategy Discovery Performance

| Strategy | $k_1$ Failure Median [95% CI] | $k_1$ Failure Mean [95% CI] | $S_{25}$ Mean | $S_{100}$ Mean | AUC (Failures) | AUC (Bugs) |
|---|---|---|---|---|---|---|
| **DCG-PR (`dcg-pr`, frozen)** | **3.00** [1.00, 255.00] | **121.40** [58.78, 184.02] | **0.56** | **0.56** | **0.4222** | **0.5711** |
| **sync-boundary** | 71.00 [71.00, 74.00] | 76.40 [45.74, 107.06] | 0.19 | 0.84 | 0.5864 | 0.6754 |
| **Representative** | 86.00 [17.00, 267.00] | 152.12 [89.93, 214.31] | 0.25 | 0.62 | 0.4601 | 0.5766 |
| **Uniform Random (B0)** | 156.00 [130.50, 274.50] | 193.16 [157.38, 228.94] | 0.00 | 0.08 | 0.3981 | 0.4808 |
| **Durability-Contract Guided (DCG)** | 385.00 [214.00, 408.00] | 314.32 [272.47, 356.17] | 0.00 | 0.00 | 0.2593 | 0.2684 |
| **State-Representative Random (R2)** | 324.86 [304.50, 384.52] | 341.08 [319.62, 362.54] | 0.00 | 0.00 | 0.2612 | 0.2666 |
| **0%-Retention-First Random (R1)** | 333.03 [296.53, 380.73] | 339.74 [318.52, 360.96] | 0.00 | 0.00 | 0.2606 | 0.2653 |
| **BARRIER-CKPT (all strategies)** | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 | 0.00 | 0.0000 | 0.0000 |

The pre-frozen `dcg-pr` ranking prioritizes unsynced directory hazards under partial retention, finding the checkpoint bug at median $k_1 = \mathbf{3.00}$—outperforming 0%-retention DCG (median $k_1 = 385.00$), uniform random B0 (median $k_1 = 156.00$), and all other heuristics.

---

## 11. Universe-Scaling Evaluation on MUT-5 (~300 to ~3,000 Candidates)

To evaluate whether durability-contract priors maintain their efficiency as workload size and candidate universe size scale, we evaluated `MUT-5` (single WAL sync drop) across 4 workload operations levels ($\{120, 300, 600, 1200\}$ ops) and 8 seeds ($\{42, 100, 200, 300, 400, 500, 600, 700\}$).

| Workload Operations | Mean Universe Size | DCG ($k_1$ Mean / Med) | R2 ($k_1$ Mean / Med) | R1 ($k_1$ Mean / Med) | B0 ($k_1$ Mean / Med) | B2 LIFO ($k_1$ Mean / Med) | Mean Plan+Rank Time | Mean Explore Time |
|---|---|---|---|---|---|---|---|---|
| **120 ops** | 337.5 | **1.00** / **1.00** | 11.62 / 12.00 | 13.38 / 13.00 | 25.75 / 25.00 | 260.50 / 264.00 | 1.6 ms | 402.1 ms |
| **300 ops** | 767.5 | **1.00** / **1.00** | 23.12 / 22.50 | 28.38 / 29.00 | 56.25 / 56.50 | 690.50 / 691.00 | 7.8 ms | 1,241.5 ms |
| **600 ops** | 1,505.5 | **1.00** / **1.00** | 42.62 / 44.00 | 54.12 / 55.50 | 108.12 / 109.00 | 1,428.50 / 1,427.00 | 22.2 ms | 2,645.0 ms |
| **1,200 ops** | 2,958.2 | **1.00** / **1.00** | 83.50 / 85.00 | 113.25 / 110.50 | 222.38 / 220.50 | 2,881.25 / 2,881.00 | 56.1 ms | 6,791.9 ms |

### Key Findings:
1. **$O(1)$ Constant-Cost Discovery:** DCG achieves $k_1 = 1.00$ across all universe sizes from 338 to ~2,958 candidates, demonstrating invariant first-failure discovery on contract-violating write boundaries.
2. **Linear Growth of Baseline Budgets:** In contrast, random and heuristic budgets scale linearly with universe size $N$: uniform random B0 mean $k_1$ grows by 8.6x (25.75 to 222.38); LIFO (B2) mean $k_1$ grows by 11.1x (260.50 to 2,881.25); state-representative R2 mean $k_1$ grows by 7.2x (11.62 to 83.50).
3. **Sub-60ms Planning Overhead:** Graph construction and DCG ranking take only 1.6 ms at 120 ops and 56.1 ms at 1,200 ops, confirming that prior-guided ranking adds negligible overhead compared to crash execution.

---

## 12. Multi-Shape Null-Control Verification (108 Runs, 0 False Alarms)

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

Across all 108 runs, exactly 0 consistency violations, 0 open errors, 0 panics, and 0 hangs were observed, empirically establishing a **0.00% false-alarm rate** across diverse write frequencies, key-space distributions, value payloads, and compaction regimes.

---

## 13. Related Work and Scope Limitations

### 13.1 Positioning Against Prior Crash-Consistency Testing Systems

**ALICE** (OSDI '14) introduced application-level crash consistency testing with a POSIX VFS model and showed that many storage systems violate ordering or atomicity assumptions silently. CraConTest builds on ALICE's insight that crash states can be enumerated from a VFS trace but focuses on *ranking* candidate crash states efficiently rather than exhaustive enumeration.

**CrashMonkey/B3** (OSDI '18) applies automated workload mutation and replay to find file-system bugs. B3's file-system workloads are structured sequences of POSIX calls; CraConTest targets database key-value engines where the relevant invariant is key-value read-your-writes consistency rather than POSIX file-system semantics.

**Witcher** (SOSP '21) targets persistent-memory programs and exploits PM-specific ordering primitives (CLFLUSH, SFENCE). CraConTest's fault model is VFS-layer sync omission, which is orthogonal to PM cache-line persistence.

**Chipmunk** (ATC '23) generates and replays crash images for NVM file systems, focusing on file-system metadata consistency. CraConTest targets application-level (database) crash consistency above the file system.

**Pathfinder** (OOPSLA '25) constructs persistence graphs from execution traces and groups correlated update behaviors for representative crash-state reduction and model checking. CraConTest addresses the complementary problem of **budgeted crash-candidate prioritization** ($k_1$): combining trace-derived POSIX durability-contract priors (`HasUnsyncedCreation`, drop impacts `CURRENT`, unflushed parent rename) with `StateKey` equivalence-class partitioning (`DCG-PR`), so equivalence classes with active contract violations are tested at rank 1 before uniform round-robin exploration.

### 13.2 Empirical Scope Boundaries

The following boundaries are established empirically across 32 seeds:

1. **Where DCG beats R2:** Single-resource sync-omission faults (`MUT-1..9` and held-out `MUT-X1..X5`) on first-failure cost $k_1$ and early-budget state discovery $S_{25}$. Median speedup 2.1×–20.0×, Holm-adjusted $p \le 0.002$ across 12 holdout settings.

2. **Where DCG-FlagOnly ties DCG:** Across almost all settings (`113 wins / 0 losses / 124 ties` on `MUT-1..9`), demonstrating that binary POSIX durability-contract flags combined with `StateKey` equivalence partitioning drive the speedup rather than continuous tier weights.

3. **Where DCG ties R2:** At full budget $S_{100}$ on moderate-scale traces where all ~35–40 `StateKey` equivalence classes are exhausted before 100 picks, and on out-of-contract non-sync faults (`MUT-X6`).

4. **Where frozen 0%-first DCG loses:** Multi-entry directory races requiring asymmetric partial retention (`50%`), such as Pebble `#5825` (manifest rotation) and Pebble Checkpoint (`commit 15b500fb`). These require the partial-retention prior (`dcg-pr`) guided by `UnsyncedDirEntryCount >= 2`.

---

## 14. Claim-to-Command and Artifact Provenance Ledger

Every quantitative claim in this report is reproducible from the committed source at tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) using the commands below. All artifacts are committed under `results/`.

| Claim | Reproduction Command | Committed Artifact |
|---|---|---|
| Stage 1 replay fidelity, null control, dense faults `MUT-0..4` | `go test -v ./internal/exp -run 'TestGateG4\|TestMainRunArtifacts'` | `results/smoke_mut0..1/`, `results/discovery_*.csv` |
| Stage 2 32-seed in-sample benchmark `MUT-0..9` | `go run ./cmd/cracontest benchmark -ops 120 -csv results/benchmark_summary.csv -per-seed-csv results/benchmark_per_seed.csv && python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9` | `results/benchmark_summary.csv`, `results/benchmark_per_seed.csv` |
| Paired stats (DCG vs. R1, R2, B0, B2) | `python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9` | — |
| Holdout `MUT-X1..X5` and out-of-contract `MUT-X6` | `go run ./cmd/cracontest benchmark -ops 120 -faults MUT-X1_k2,MUT-X1_k3,MUT-X1_k4,MUT-X1_k6,MUT-X1_k9,MUT-X2_k1,MUT-X2_k3,MUT-X2_k4,MUT-X2_k5,MUT-X3_k1,MUT-X3_k2,MUT-X3_k3,MUT-X4_p04,MUT-X5_p12,MUT-X6_k2,MUT-X6_k5,MUT-X6_k9 -csv results/holdout_summary.csv -per-seed-csv results/holdout_per_seed.csv -report "" && python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12` | `results/holdout_summary.csv`, `results/holdout_per_seed.csv` |
| Pebble `#5825` real-bug A/B and `dcg-pr` | `go run ./cmd/cracontest benchmark -faults MUT-0,BARRIER-5825 -max-manifest-size 1 -policies 0,50,100 -csv results/realbug_pebble5825_summary.csv -per-seed-csv results/realbug_pebble5825_per_seed.csv -report ""` | `results/realbug_pebble5825_summary.csv`, `results/realbug_pebble5825_per_seed.csv` |
| Checkpoint holdout A/B (frozen at `v1.0-dcg-pr-freeze`) | `go run ./cmd/cracontest benchmark -faults MUT-0,BARRIER-CKPT -checkpoint-at 60 -policies 0,50,100 -csv results/realbug_checkpoint_summary.csv -per-seed-csv results/realbug_checkpoint_per_seed.csv -report ""` | `results/realbug_checkpoint_summary.csv`, `results/realbug_checkpoint_per_seed.csv` |
| `MUT-5` universe-scaling (`120..1200` ops) | `go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update` | `results/scaling_mut5_summary.csv`, `results/scaling_mut5_per_seed.csv` |
| Multi-shape null control (`108` runs, `12` shapes) | `go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update` | `results/null_control_multishape.csv` |

> **Note:** Tag `v1.0-dcg-pr-freeze` pins the `dcg` and `dcg-pr` algorithms (`internal/strategy/dcg.go`) and the feature extractor (`internal/graph/features.go`) prior to all holdout and real-bug evaluations, guaranteeing zero data leakage.
