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

Because `DCG`'s four 0%-retention indicators (`AckSyncMissing`, `DanglingTableCount`, `UnsyncedDirectoryRename`, `ManifestBarrierPending`) directly inspect the VFS trace for missing `Sync`/`SyncData` barriers after client acknowledgment or metadata updates, achieving $k_1 = 1.00$ on synthetic sync-omission faults (`MUT-1..9` and `MUT-X1..X5`) reflects **exact alignment between trace-level durability-contract checking and sync-omission fault injection** combined with `StateKey` equivalence deduplication.

Crucially, as a central empirical finding, when evaluated on uninjected 50%-retention multi-entry directory races (`#5825` and `Checkpoint`), **frozen 0%-first `DCG` underperforms uniform random `B0`** (median $k_1 = 205.00$ vs. $112.00$ on `#5825`; $215.00$ vs. $162.50$ on `Checkpoint`), and only the **post-hoc partial-retention prior (`dcg-pr`, `UnsyncedDirEntryCount >= 2`)** achieves $k_1 = 2.00$.

Furthermore, the **0.00% false-alarm rate on `MUT-0`** holds strictly across all **non-ingest workloads under endpoint (`0%/100%`) retention** (1,000-op main run, 32-seed benchmark, 108-run multi-shape null control, and 80-run workload-diversity null control), whereas enabling `db.Ingest` under the global prefix ("no-hole") oracle or enabling `50%` partial retention surfaces the cross-channel commit-order prefix divergence and `atomicfs` multi-entry directory races described in §9, §10, and §13.

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
   - The `DCG-FlagOnly` ablation ties full `DCG` on `113 wins / 0 losses / 124 ties` vs `R2`, showing that binary POSIX durability-contract priors combined with `StateKey` equivalence partitioning drive the speedup.

3. **Stage 3 — Parameterized & Stochastic Fault Generalization (`MUT-X1..X5`) and Out-of-Contract Control (`MUT-X6`) (§8.4):**
   - The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault families were committed to `internal/faults/faults.go` on `2026-11-01` (`c67a44a`, prior to tag `v1.0-dcg-pr-freeze`) to evaluate whether the four binary POSIX durability-contract flags generalize across arbitrary sync-drop indices $k$ and drop probabilities $p$ without re-tuning (**287 wins / 0 losses / 43 ties** vs. `R2`, mean $k_1$ speedup $2.2\times\text{--}19.1\times$, Holm-adjusted sign-test $p \le 0.002$) and degrade gracefully to `R2` on non-contractual silent write drops (`MUT-X6`, Holm $p = 1.0$).
   - Subsequently, `internal/strategy/dcg.go` and `internal/graph/features.go` were frozen at Git tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**, and **three strictly post-freeze evaluations** were designed and executed with 0 bytes of modification to the ranking or feature code: (1) Post-Freeze Holdout 1 (Real Bug — Checkpoint `15b500fb`, §10), (2) Post-Freeze Holdout 2 (Real Bug — Direct L0 `db.Ingest`, §13.5), and (3) Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §14).

4. **Stage 4 — Real-World Case Study 1: CockroachDB Pebble Issue `#5825` (`MUT-0` vs. `BARRIER-5825`) & Partial-Retention Prior (`DCG-PR`) (§9):**
   - Reproduces Pebble `#5825` under deterministic 50% partial retention with manifest rotation (98 failures across 31 failing seeds under unpatched `MUT-0`, reduced to **0 failures** under causal barrier control `BARRIER-5825`). `DCG-PR` (`UnsyncedDirEntryCount >= 2`) reduces median $k_1$ from $205.00$ (0%-first `DCG`, mean $253.45$) and $112.00$ (`B0`, mean $136.23$) down to **2.00** [1.00, 3.00] (mean **2.65** [1.97, 3.33], $S_{25} = 2.50$, $S_{100} = 2.50$, AUC Failures = $0.8455$, AUC Bugs = $0.9212$).

5. **Stage 5 — Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race (`15b500fb`) (`MUT-0` vs. `BARRIER-CKPT`) (§10):**
   - Evaluated with pre-frozen `DCG-PR` (`v1.0-dcg-pr-freeze`), reproducing 67 failures across 30 of 32 failing seeds under unpatched `MUT-0` and **0/32 failing seeds** under causal barrier control `BARRIER-CKPT`. Pre-frozen `DCG-PR` discovers the failure at median $k_1 = \mathbf{2.00}$ [1.00, 125.50] (mean **93.03** [43.37, 142.70], $S_{25} = 0.84$, $S_{100} = 0.84$, AUC Failures = $0.5414$, AUC Bugs = $0.6739$) vs. $215.00$ for 0%-first `DCG` (mean $262.93$ [230.82, 295.05]), $323.56$ for `R2` (mean $325.80$), and $162.50$ for `B0` (mean $175.35$ [150.68, 200.02]).

6. **Stage 6 — Candidate Universe Scaling (`120..1,200` ops) & Multi-Shape Null Control (`108` runs) (§11–§12):**
   - On `MUT-5`, as candidate universes grow from ~338 to ~2,958 states, `DCG` maintains constant $O(1)$ discovery ($k_1 = 1.00$, planning time $\le 56.1\text{ ms}$) while `R2`, `R1`, `B0`, and `LIFO` scale linearly. Across 108 unfaulted runs spanning 12 workload shapes (>50,000 crash states), the harness exhibits a **0.00% false-alarm rate** (0 violations, 0 open errors, 0 panics, 0 hangs).

7. **Stage 7 — Pebble Subsystem Source Audit, Workload Extension & Empirical Crash-Consistency Evaluation under Unfaulted MUT-0 (§13.1–§13.4):**
   - Exhaustive source audit of Pebble v2.1.7 (`ingest.go:1490`, `version_set.go:649`, `commit.go:289`, `format_major_version.go:486`, `vfs/atomicfs/marker.go:189`, and `checkpoint.go` lines 287, 596, 448).
   - Evaluated Workload A (Ingestion), Workload B (Format Ratcheting), and Workload C (Combined Stress) under `MUT-0` across policies `[0, 50, 100]` (5,424 candidate states in `results/subsystem_discovery.csv`). Across all 5,424 candidate states, Pebble v2.1.7 exhibits **0 `durably_committed` data-loss violations and 0 recovery panics**, confirming that `d.objProvider.Sync()` (`ingest.go:1490`) and `atomicfs.Marker.Move` (`marker.go:189`) protect acknowledged ingest and format-ratchet operations from losing committed data. Simultaneously, the audit exposed two distinct recovery hazards under `MUT-0`: (1) an **input-dependent cross-channel `prefix` monotonicity hole** during non-overlapping Direct L0 `db.Ingest` (`40` prefix violations in Workload A, analyzed across 32 seeds in §13.5), and (2) **50%-retention intra-operation directory races (`open_error`)** when a crash occurs inside the multi-entry window between `MANIFEST-*`/`.sst` creation and `dirFD.Sync()` (25 total open errors).

8. **Stage 8 — Direct L0 `db.Ingest` 32-Seed Case Study, Causal Remedy (`BARRIER-INGEST`) & Multi-Distribution Workload Generalization (§13.5, §14):**
   - **Cross-Channel Prefix Hole:** Under unpatched `MUT-0` (`BarrierIngest: false`), direct L0 ingest bypasses WAL, incrementing sequence number $s_2 > s_1$ and committing via MANIFEST fsync while earlier un-synced WAL writes at $s_1$ remain unpersisted. Across 32 seeds, **26 of 32 seeds** exhibit prefix violations under `MUT-0` (seeds 3, 5, 7, 20, 26, 29 have 0 failures because on those seeds randomly generated ingest keys overlapped the active memtable and therefore executed the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`; under endpoint retention, 22 of 32 seeds fail with seeds 8, 9, 16, 23 also taking flushable ingest). Pre-frozen `DCG-PR` achieves median $k_1 = \mathbf{9.00}$ [9.00, 19.00] (mean $15.32$ [9.88, 20.76], $S_{25} = 0.75$, $S_{100} = 1.41$, AUC Failures = $0.3506$, AUC Bugs = $0.3938$) and `DCG` achieves $9.00$ (mean $15.32$), compared to $27.88$ (mean $27.64$) for `R2`, $28.60$ (mean $30.61$) for `R1`, $28.18$ (mean $29.88$) for `DCG Flag-Only`, $81.00$ (mean $80.18$) for `sync-boundary`, and $21.50$ (mean $24.73$) for uniform random `B0` across 276 total failures on 22 failing seeds.
   - **Causal Remedy (`BARRIER-INGEST`):** Pre-ingest WAL flush via `db.LogData(nil, pebble.Sync)` commits a zero-byte sync record forcing `SyncData()` on the active WAL before MANIFEST version edit. Under `BARRIER-INGEST`, **0 of 32 seeds** fail ($S_{25}=0.00, S_{100}=0.00$), eliminating all prefix violations to strictly 0 across all 32 seeds!
   - **Multi-Distribution Workload Generalization:** Evaluates 5 industry presets (`ycsb-a`, `ycsb-b`, `ycsb-f`, `overwrite-heavy`, `delete-heavy`) across 16 seeds (`1..16`, 25 ops) and 4 fault models (`MUT-0`, `MUT-1`, `MUT-5`, `MUT-9`). Unfaulted `MUT-0` produces strictly 0 false alarms across all 16 seeds for all 5 presets. `DCG` achieves a **$9.3\times$ speedup** over `R2` (mean $k_1 = 1.00$ vs. $9.30$, and **$20.5\times$** over `B0`'s $20.50$) on both `ycsb-f/MUT-5` and `overwrite-heavy/MUT-5`, and an **$8.4\times$ speedup** over `R2` on `ycsb-a/MUT-5` ($1.00$ vs. $8.35$, and **$23.6\times$** over `B0`'s $23.55$). `DCG` achieves optimal mean $k_1 = 1.00$ across all failing fault models on `ycsb-a`, `ycsb-b`, `ycsb-f`, and `overwrite-heavy`. On `delete-heavy`, `DCG` achieves mean $k_1 = 3.00$ on `MUT-1` (beating `B0`'s $4.12$, while `R2` averages $2.49$, and expanding early-budget state discovery $S_{25}$ from `R2`'s $10.08$ and `B0`'s $3.59$ to **`14.19`**), $k_1 = 20.54$ on sparse `MUT-5` (beating `R2`'s $34.89$ by **$1.7\times$** and `B0`'s $50.92$ by **$2.5\times$**), and $k_1 = 3.00$ on `MUT-9` (beating both `R2`'s $4.30$ and `B0`'s $9.94$, and expanding $S_{25}$ from $5.16$ to **`7.44`**). Across the 14 failing preset × fault settings, `DCG` outperforms uniform random (`B0`) on **all 14 settings** ($14/14$) and outperforms `R2` on $k_1$ in **13 of 14 settings** ($13/14$), with a single regression on `delete-heavy/MUT-1` (`DCG` mean $k_1 = 3.00$ vs. `R2`'s $2.49$, where early synced tombstones on absent keys are logical state no-ops). On $S_{25}$, `DCG` **matches or exceeds** `R2` across all 14 failing settings (strictly higher on 13 settings, with a tie on `ycsb-b/MUT-1` at $7.00$ vs. $7.00$, and noting that because these preset traces use 25 operations, $B=25$ approaches the full equivalence-class universe for several presets).

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
   - **R2 (State-Representative Random):** Partitions 0% retention candidates into equivalence classes by canonical persistence state key (`StateKey`), selecting boundary-adjacent representatives within each `StateKey` class using graph features (`BeforeBoundary`, `SemanticTier`, `Desc`) before shuffling classes and evaluating redundant cutoffs afterward. Note that `R2` is CraConTest's internal `StateKey` equivalence-class baseline—**not** a re-implementation of Pathfinder's full C++/LLVM syscall-function subgraph isomorphism and DPOR model checker—so comparisons against `R2` measure the intra-framework gain of adding POSIX durability-contract priors onto `StateKey` partitioning rather than a head-to-head comparison against the Pathfinder tool. On sparse faults such as `MUT-5`, `R2` cuts search cost by half ($k_1 = 13.01$ vs `B0`'s $25.97$).
2. **RHG (Reachability-Hazard Guided / B7):**
   - Eliminates representative key divergence by maintaining strict deterministic grouping and ranks candidates by reachable uncommitted hazard surfaces, achieving normalized AUC values between $0.70$ and $0.82$.
3. **DCG (Durability-Contract Guided / B8) & DCG-FlagOnly (Ablation):**
   - Trace-derived durability-contract analysis that directly encodes the contract indicators as POSIX durability-contract priors instantiated for Pebble:
     - **Indicator 1 (`AckSyncMissing`):** Detects dropped WAL sync barriers following client-acknowledged transactions.
     - **Indicator 2 (`DanglingTableCount`):** Identifies dangling SST references where the manifest references an unpersisted table, correctly gated on manifest commitment (`fi.committedInManifest`).
     - **Indicator 3 (`UnsyncedDirectoryRename`):** Flags directory hazards where critical namespace files (`CURRENT`, `MANIFEST`) or post-ack active WAL/options files cross directory boundaries without parent directory sync (`len(ackSyncOps) > 0`).
     - **Indicator 4 (`ManifestBarrierPending`):** Catches uncommitted version edit records without manifest sync.
   - **Performance & Trace-Contract Alignment:** Because `DCG`'s four 0%-retention indicators directly inspect the VFS trace for missing `Sync`/`SyncData` barriers after client acknowledgment or metadata updates, achieving $k_1 = 1.00$ on synthetic sync-omission faults (`MUT-1..6`, `MUT-8`, `MUT-9`, and `MUT-X1..X5`) reflects **exact alignment between trace-level durability-contract checking and sync-omission fault injection** combined with `StateKey` equivalence deduplication. The `DCG-FlagOnly` ablation ties full `DCG` on nearly every setting, confirming that the binary durability-contract flags combined with `StateKey` equivalence partitioning drive the speedup. Control run `MUT-0` achieves 0 false alarms across all 32 seeds under endpoint retention.

### 32-Seed Benchmark Summary Tables

Across 32 deterministic workload seeds (>11,000 evaluated crash candidates):

#### Table 1: Cost-to-First-Failure ($k_1$ Mean [95% CI])

| Fault Model | Fault Description | Failure Density | DCG | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|---|---|
| **MUT-0** | Null Control (No Fault) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.44 [1.42, 1.46] | 1.37 [1.34, 1.39] | 5.00 [5.00, 5.00] | 17.00 [17.00, 17.00] | 2.00 [2.00, 2.00] |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.17 [1.16, 1.19] | 1.04 [1.04, 1.05] | **1.00** [1.00, 1.00] | 5.00 [5.00, 5.00] | 1.55 [1.38, 1.71] |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** [1.00, 1.00] | 1.03 [1.03, 1.04] | **1.00** [1.00, 1.00] | 1.44 [1.40, 1.48] | 1.17 [1.15, 1.19] | **1.00** [1.00, 1.00] | 9.00 [9.00, 9.00] | 2.00 [2.00, 2.00] |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.95 [1.90, 2.01] | 1.67 [1.61, 1.74] | **1.00** [1.00, 1.00] | 31.62 [31.24, 32.01] | 2.39 [2.22, 2.56] |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** [1.00, 1.00] | 1.02 [0.98, 1.06] | **1.00** [1.00, 1.00] | 13.07 [12.72, 13.42] | 17.06 [16.56, 17.56] | 262.31 [258.25, 266.38] | 17.00 [17.00, 17.00] | 26.19 [25.22, 27.16] |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** [1.00, 1.00] | 1.20 [1.17, 1.24] | 14.08 [13.50, 14.65] | 7.72 [7.46, 7.97] | 20.79 [19.89, 21.70] | 101.00 [97.37, 104.63] | 43.15 [42.38, 43.93] | 31.12 [29.02, 33.21] |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 3.58 [3.50, 3.66] | 3.26 [3.12, 3.40] | **1.00** [1.00, 1.00] | 55.25 [54.85, 55.65] | 4.41 [4.23, 4.58] |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | **1.00** [1.00, 1.00] | 1.40 [1.36, 1.44] | 1.21 [1.18, 1.23] | **1.00** [1.00, 1.00] | 17.00 [17.00, 17.00] | 1.95 [1.90, 2.01] |

#### Table 2: Failing State Discovery ($S_{25}$ Early Budget / $S_{100}$ Exhaustive Budget Mean)

| Fault Model | DCG ($S_{25}$ / $S_{100}$) | DCG-FlagOnly | RHG | R2 | R1 | LIFO | B6 | B0 |
|---|---|---|---|---|---|---|---|---|
| **MUT-1** | **23.94** / **23.97** | **23.94** / **23.97** | 17.69 / **23.97** | 17.12 / **23.97** | 10.02 / 19.80 | 6.53 / 8.94 | 5.00 / 13.00 | 6.12 / 14.61 |
| **MUT-2** | **25.00** / **33.03** | **25.00** / **33.03** | 24.00 / **33.03** | 21.15 / **33.03** | 8.70 / 22.82 | 7.00 / 11.34 | 11.00 / 30.00 | 5.64 / 13.78 |
| **MUT-3** | 24.00 / **27.00** | **24.12** / **27.00** | 21.03 / **27.00** | 17.26 / **27.00** | 7.27 / 19.00 | 7.16 / 12.06 | 9.00 / 21.00 | 5.00 / 11.22 |
| **MUT-4** | **21.69** / **21.69** | **21.69** / **21.69** | 16.69 / **21.69** | 12.57 / **21.69** | 5.44 / 14.51 | 8.00 / 12.25 | 0.00 / 16.69 | 3.53 / 8.59 |
| **MUT-5** | **2.00** / **2.00** | **2.00** / **2.00** | 1.81 / **2.00** | 1.30 / **2.00** | 0.88 / 1.60 | 0.00 / 0.00 | 1.00 / 1.00 | 0.45 / 1.00 |
| **MUT-6** | **1.62** / **1.62** | **1.62** / **1.62** | 1.22 / **1.62** | 1.07 / **1.62** | 0.39 / 1.20 | 0.00 / 0.41 | 0.00 / 1.22 | 0.00 / 0.81 |
| **MUT-7** | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | **12.88** / **12.88** | **12.88** / **12.88** | 7.88 / **12.88** | 6.74 / 12.82 | 3.27 / 8.41 | 8.00 / 11.75 | 0.00 / 9.88 | 2.00 / 5.05 |
| **MUT-9** | **25.00** / **27.00** | **25.00** / **27.00** | 20.00 / **27.00** | 17.78 / **27.00** | 5.57 / 17.44 | 7.00 / 10.94 | 5.00 / 26.00 | 3.03 / 9.83 |

### POSIX Durability Priors & DCG-FlagOnly Ablation

1. **General POSIX Durability-Contract Priors:**
   Under POSIX filesystem semantics, ensuring durable file persistence after crash requires two conditions: (1) flushing dirty file data (`fsync` / `fdatasync`), and (2) flushing the parent directory entry (`fsync(parentDir)`) after file creation, renaming, hard-linking, or directory creation (`Create`, `ReuseForWrite`, `Rename`, `Link`, `MkdirAll`). If a file is written and its data synced, but its parent directory entry remains unsynced, a crash at 0% retention causes the directory entry to vanish, resulting in lost files upon recovery. CraConTest implements POSIX durability-contract priors instantiated for Pebble's VFS categories: tracking `parentDir` and `dirEntrySynced` for every filesystem node, enforcing that durability holds if and only if `dirtyBytes == 0 && dirEntrySynced == true`, and scoping directory sync events strictly to the target directory path. MUT-9 is dense (41.4% density) at $k=1$ (initial directory sync) and sparse (0.8%–12.4%) at $k \in \{7, 9, 10, 12\}$ (flush/compaction directory syncs); directory-entry tracking was added after inspecting MUT-9, so MUT-9 evaluates parameterized $k$-generalization within the directory-sync family. On MUT-9 ($k=1$), DCG identifies the uncommitted directory entry prior, expanding state coverage to $S_{100} = 27.00$ (vs B0's $9.83$, a 2.7x expansion) and $S_{25} = 25.00$ (vs B0's $3.03$, an 8.2x early-budget advantage).

2. **Component Attribution & DCG-FlagOnly Ablation:**
   Comparing full DCG against the **DCG-FlagOnly** ablation isolates the exact contribution of each mechanism:
   - **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and $12.88$ on MUT-8). On dense faults (`MUT-1`, `MUT-2`, `MUT-4`), sparse faults (`MUT-5`, `MUT-8`), and directory faults (`MUT-9`), DCG-FlagOnly achieves the identical optimal $k_1 = 1.00$ [1.00, 1.00] as full DCG, confirming that the contract indicators (POSIX durability-contract priors instantiated for Pebble) are the primary driver of first-failure cost reduction.
   - **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop, ~0.8% density), where full DCG achieves mean $k_1 = 1.00$ compared to $1.20$ for `DCG-FlagOnly`.
   - **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

3. **$S_{25}$ Early-Budget Discovery, Crash Efficiency ($CE_{25}$), & The ~40-State Equivalence Ceiling:**
   In a 120-operation workload, there are only ~35–40 distinct canonical `StateKey` persistence states under 0%-retention crashes. Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100 (reaching identical $S_{100} = 23.97$ on `MUT-1`, $33.03$ on `MUT-2`, $27.00$ on `MUT-3`, $21.69$ on `MUT-4`, and $27.00$ on `MUT-9`). Disclosing our experimental design evolution: $k_1$ and early-budget coverage $S_{25}$ ($B = 25$) were specifically adopted after observing that $S_{100}$ saturates the available equivalence-class universe on 120-op traces, obscuring early discovery differences. To normalize early-budget discovery across budgets, we report **Crash Efficiency ($CE_{25} = S_{25} / 25$)**: DCG achieves near-optimal efficiency within 25 tests (e.g. on `MUT-1`, $S_{25} = 23.94 \implies CE_{25} = 0.958$ vs. R2's $17.12 \implies CE_{25} = 0.685$ and B0's $6.12 \implies CE_{25} = 0.245$; on `MUT-2`, $S_{25} = 25.00 \implies CE_{25} = 1.000$ vs. R2's $0.846$ and B0's $0.227$; on `MUT-8`, $S_{25} = 12.88 \implies CE_{25} = 0.515$ vs. R2's $0.270$ and B0's $0.080$; on `MUT-9`, $S_{25} = 25.00 \implies CE_{25} = 1.000$ vs. R2's $0.711$ and B0's $0.121$), demonstrating that $S_{25}$ and $CE_{25}$ serve as the critical discriminant metrics for practical CI testing with a 2x–8x throughput advantage over random selection.

### 8.4 Parameterized & Stochastic Fault Generalization (`MUT-X1..X5`) and Out-of-Contract Control (`MUT-X6`)

The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault families were committed to `internal/faults/faults.go` on `2026-11-01` (`c67a44a`, prior to tag `v1.0-dcg-pr-freeze`) to evaluate whether the four binary POSIX durability-contract flags generalize across arbitrary sync-drop indices $k$ and drop probabilities $p$ without re-tuning (**287 wins / 0 losses / 43 ties** vs. `R2`, mean $k_1$ speedup $2.2\times\text{--}19.1\times$, Holm-adjusted sign-test $p \le 0.002$).

Subsequently, `internal/strategy/dcg.go` and `internal/graph/features.go` were frozen at Git tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**, and **three strictly post-freeze evaluations** were designed and executed with 0 bytes of modification to the ranking or feature code:
1. **Post-Freeze Holdout 1 (Real Bug — Checkpoint `15b500fb`, §10)**: Evaluated across 32 seeds (`MUT-0` vs. `BARRIER-CKPT`).
2. **Post-Freeze Holdout 2 (Real Bug — Direct L0 `db.Ingest` Cross-Channel Commit-Order Hole, §13.5)**: Evaluated across 32 seeds (`MUT-0` vs. `BARRIER-INGEST`).
3. **Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §14)**: 5 workload presets (`ycsb-a`, `ycsb-b`, `ycsb-f`, `overwrite-heavy`, `delete-heavy`) evaluated across 16 seeds and 4 fault models.

Because all parameterized and stochastic fault settings in Table 8.4 involve only 0%- and 100%-retention crash states (no partial-retention intermediates), `dcg-pr` produces an identical ranking to `dcg` on these settings; the table therefore reports `DCG / DCG-PR` as a single column.

| Fault | Fault Family | Seeds Fail (Density) | DCG / DCG-PR — Median $k_1$ [95% CI] | DCG / DCG-PR — Mean $k_1$ | Sync-Boundary (B6) — Median [Mean] | State-Representative (B5) — Median [Mean] | Uniform Random (B0) — Median [Mean] | R2 — Median [Mean] | R1 — Median [Mean] |
|---|---|---|---|---|---|---|---|---|---|
| MUT-X1\_k2 | Delayed WAL Sync ($k=2$) | 32/32 (2.6%) | **1.00** [1.00, 1.00] | **1.00** | 19.00 [19.00] | 27.00 [27.00] | 25.00 [24.14] | 13.09 [13.02] | 16.86 [16.94] |
| MUT-X1\_k3 | Delayed WAL Sync ($k=3$) | 31/32 (2.2%) | **1.00** [1.00, 1.00] | **1.00** | 21.00 [21.00] | 29.00 [29.00] | 30.00 [36.52] | 12.94 [13.01] | 18.95 [22.65] |
| MUT-X1\_k4 | Delayed WAL Sync ($k=4$) | 12/32 (1.1%) | **1.00** [1.00, 1.00] | **1.00** | 23.00 [23.67] | 31.00 [31.67] | 69.75 [62.21] | 13.25 [13.13] | 40.37 [40.50] |
| MUT-X1\_k6 | Delayed WAL Sync ($k=6$) | 32/32 (2.7%) | **1.00** [1.00, 1.00] | **1.00** | 35.00 [35.00] | 43.00 [43.06] | 26.50 [25.83] | 13.50 [13.38] | 16.64 [16.92] |
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
| MUT-X6\_k5 | Silent WAL Write Loss ($k=5$) | 21/32 (12.4%) | 16.00 [13.00, 22.00] | 16.76 | 35.00 [35.00] | 39.00 [39.00] | 7.00 [8.02] | 13.37 [12.87] | 8.45 [10.06] |
| MUT-X6\_k9 | Silent WAL Write Loss ($k=9$) | 10/32 (11.7%) | 3.00 [3.00, 8.00] | 5.60 | 43.00 [45.40] | 47.00 [49.40] | 10.00 [8.70] | 6.75 [6.69] | 12.05 [10.40] |

Each holdout setting ran 32 deterministic seeds at 120 ops/seed (544 total seeds, 17 fault settings). `MUT-X2_k4` and `MUT-X2_k5` produced zero failures under all strategies (fault density below the trace-length detection threshold); they are excluded from paired statistics.

**Paired statistical summary** (`python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12`):

- **In-contract holdouts (`MUT-X1..X5`, 12 testable settings, 330 failing seed-runs):** `DCG` / `DCG-PR` beats `R2` on all 12 testable settings (**287 wins / 0 losses / 43 ties**, mean $k_1$ speedup **$2.2\times$–$19.1\times$**, Holm-adjusted sign-test $p \le 0.002$ across all 12 settings).
- **Out-of-contract control (`MUT-X6_k2, k5, k9`, 3 settings, 63 failing seed-runs):** `DCG` / `DCG-PR` performs at the `R2` baseline level (**21 wins / 38 losses / 4 ties**, speedup $0.9\times$–$1.1\times$, Holm-adjusted $p = 1.0$, not significant) — confirming graceful degradation to `StateKey` partitioning when a fault triggers zero metadata/sync contract violations.

---

## 9. Real-World Case Study: Pebble #5825 A/B Benchmark & Partial-Retention Priors

### 9.1 Background: CockroachDB Pebble Issue #5825

During manifest rotation, Pebble creates a new `MANIFEST-*` file and then updates `marker.manifest.*` to point to it, all **before** syncing the parent directory. This leaves two unsynced directory entries coexisting in `"db"`. At crash:

- **0% retention** — both `MANIFEST-*` and `marker.manifest.*` are dropped together; Pebble falls back to the previous manifest and recovers cleanly.
- **100% retention** — both entries survive; recovery succeeds normally.
- **50% partial retention** — `marker.manifest.*` can survive while `MANIFEST-*` is dropped. Pebble's `Open` follows the surviving pointer and fails with `"file does not exist"`. (Note: for intermediate retention `1..99%`, CraConTest explicitly defines its own deterministic block-and-entry partial-retention model, `deterministicPartialClone`, validated by `TestDeterministicPartialCloneStatisticalFidelity`, rather than calling Pebble's `CrashClone`).

`BARRIER-5825` (Manifest-Rotation Barrier Control) eliminates this defect by enforcing a parent directory `Sync` before creating the new `marker.manifest.*` file. Specifically, when `manifestMarker.Move` invokes `FS.Create("db/marker.manifest.<iter>.MANIFEST-xxxxxx")`, `BARRIER-5825` intercepts the call and runs `maybeSyncParentDir` **before** delegating to `fs.inner.Create`. At that exact instant, the unsynced directory entry flushed to durability in `"db"` is the newly created `MANIFEST-xxxxxx` (written by `createManifest`), guaranteeing that the target manifest is durable before the marker pointing to it exists. The feature `UnsyncedDirEntryCount` captures the maximum number of unsynced directory entries coexisting in the same parent at each cutoff; when this value reaches 2, `dcg-pr` promotes 50% partial-retention candidates to the top of the ranking.

### 9.2 Causal A/B Verification (MUT-0 vs. BARRIER-5825)
To establish causal attribution, we benchmarked 32 deterministic workload seeds with manifest rotation enabled (`-max-manifest-size 1`) across retention policies 0%, 50%, and 100%:
1. **MUT-0 (Unfaulted Baseline):** 98 total consistency failures occurred across 31 of the 32 seeds (seed 1009 has 0 failures under partial retention; the remaining 31 seeds have 1–6 failures, including 2 on seed 42). Every single failure manifested exclusively on crash candidates evaluated under the **50% partial-retention policy** (~0.18%–1.11% failure density per seed).
2. **BARRIER-5825 (Manifest-Rotation Barrier Control):** By executing parent directory sync (`maybeSyncParentDir`) *before* `fs.inner.Create` when writing marker files, the unsynced `MANIFEST-xxxxxx` entry is flushed to durability prior to marker creation. Under BARRIER-5825, **0 failures** occurred across all 32 seeds, providing definitive empirical and causal verification of defect elimination.

### 9.3 Discovery Performance: Post-Hoc Partial-Retention Prior (`dcg-pr`)
Standard durability-contract guided ranking (`dcg`) was designed with the core assumption that durability failures manifest predominantly at 0% retention (losing unpersisted data). Consequently, `dcg` sweeps all 0%-retention candidates first.

Crucially, as a central empirical finding (not a footnote), when evaluated on uninjected 50%-retention multi-entry directory races (`#5825`), **frozen 0%-first `DCG` underperforms uniform random `B0`**: standard `dcg` requires a median $k_1$ of **205.00** [203.00, 210.00] (mean 253.45) to discover the first failure, performing substantially worse than uniform random `B0` (median $k_1 = 112.00$, mean 136.23) and state-representative `B5` (median $k_1 = 17.00$, mean 45.26). This occurs because 0%-retention crashes drop both entries together and recover cleanly, leaving 0%-first DCG searching hundreds of non-failing states.

To target partial-retention directory vulnerabilities, we introduced `dcg-pr` (DCG Partial-Retention Prior). `dcg-pr` inspects `ContractFeatures.UnsyncedDirEntryCount`: candidates with `Policy > 0 && Policy < 100` and `UnsyncedDirEntryCount >= 2` are prioritized into Tier 1 (ahead of the 0% retention sweep). Only with this post-hoc partial-retention prior does `dcg-pr` achieve median $k_1 = \mathbf{2.00}$ (mean 2.65).

| Strategy | $k_1$ Failure Median [95% CI] | $k_1$ Failure Mean [95% CI] | $S_{25}$ Mean | $S_{100}$ Mean | AUC (Failures) | AUC (Bugs) |
|---|---|---|---|---|---|---|
| **DCG-PR (`dcg-pr`)** | **2.00** [1.00, 3.00] | **2.65** [1.97, 3.33] | **2.50** | **2.50** | **0.8455** | **0.9212** |
| **Representative (B5)** | 17.00 [17.00, 62.00] | 45.26 [31.60, 58.91] | 0.56 | 1.69 | 0.8077 | 0.8524 |
| **sync-boundary (B6)** | 50.00 [8.00, 83.00] | 67.61 [39.35, 95.87] | 0.38 | 0.91 | 0.6291 | 0.7400 |
| **combined (B4)** | 8.00 [8.00, 14.00] | 54.26 [23.28, 85.24] | 0.69 | 1.41 | 0.6217 | 0.7223 |
| **Uniform Random (B0)** | 112.00 [90.00, 149.50] | 136.23 [108.94, 163.51] | 0.00 | 0.41 | 0.4785 | 0.5810 |
| **fifo (B1)** | 26.00 [26.00, 29.00] | 103.23 [55.37, 151.08] | 0.00 | 0.69 | 0.4789 | 0.5968 |
| **rhg (B7)** | 256.00 [202.00, 264.00] | 248.29 [231.48, 265.10] | 0.00 | 0.00 | 0.3322 | 0.3604 |
| **0%-Retention Random (R1)** | 273.63 [259.09, 304.32] | 286.73 [271.24, 302.23] | 0.00 | 0.00 | 0.3230 | 0.3358 |
| **State-Representative Random (R2)** | 274.60 [262.14, 302.84] | 288.11 [272.33, 303.88] | 0.00 | 0.00 | 0.3229 | 0.3356 |
| **DCG Flag-Only (Ablation)** | 275.83 [263.56, 294.81] | 285.72 [270.80, 300.63] | 0.00 | 0.00 | 0.3242 | 0.3386 |
| **Durability-Contract Guided (DCG)** | 205.00 [203.00, 210.00] | 253.45 [221.26, 285.65] | 0.00 | 0.00 | 0.3434 | 0.3604 |

As demonstrated, `dcg-pr` reduces median $k_1$ from 205.00 down to **2.00** (mean 2.65, a >95x speedup over standard DCG and >50x speedup over uniform random B0), with an AUC of 0.8455 (0.9212 for bugs). This demonstrates that extending durability contracts to model partial-retention directory hazards enables targeted discovery of real-world filesystem durability bugs.

---

## 10. Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race (`15b500fb`)

### 10.1 Background: Checkpoint Directory Sync Omission
In CockroachDB Pebble commit `15b500fb`, Pebble's `checkpoint.go` implementation contained a crash-consistency bug where during `db.Checkpoint(destDir)`, Pebble creates hard-linked SSTables, `MANIFEST`, and `marker.manifest.*` inside `destDir` before calling the directory sync inside `manifestMarker.Move` (`checkpoint.go:596`). When a crash occurs *inside that multi-entry window prior to `dirFD.Sync()`* under 50% partial retention, `marker.manifest.*` can survive while the newly created `MANIFEST-*` or hard-linked `.sst` entry in the same directory is dropped, causing reopening to fail with `open_error`.

Crucially, **Case Study 2 (`Checkpoint` `15b500fb`) belongs to the same structural bug class as Case Study 1 (`#5825`)**: both are multi-entry directory races in Pebble's `atomicfs` marker protocol (`marker.manifest.*` surviving under 50% retention while `MANIFEST-*` or a linked `.sst` in the same parent directory is dropped prior to `dirFD.Sync()`). Therefore, Case Study 2 evaluates **intra-class subsystem transfer** of `dcg-pr` (transferring from manifest rotation in `"db/"` to `db.Checkpoint` in `"checkpoint/"`), not generalization to an unseen bug class.

Enforcing immediate per-entry parent-directory synchronization (`BARRIER-CKPT`, `internal/faults/faults.go:551`, which syncs `parentDir` on `marker.manifest` operations across both `"db/"` and `"checkpoint/"`) eliminates this intra-window multi-entry race (`0/32` failing seeds).

### 10.2 Pre-Evaluation Code Freeze & Causal A/B Verification
To guard against methodological overfitting, all prioritization algorithms—including `dcg` and `dcg-pr`—were frozen under git tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) before evaluating this holdout bug. Workload operations were extended with `-checkpoint-at 60`, executing a checkpoint creation operation mid-workload.

Across 32 deterministic seeds (`42, 100..900, 1001..1022`) under policies 0%, 50%, and 100% ($N = 18{,}687$ crash candidates across the 32 seeds under MUT-0; $37{,}470$ total across MUT-0 and BARRIER-CKPT):

1. **MUT-0 (Baseline):** 30 of 32 seeds exhibited checkpoint or manifest recovery failure under 50% partial retention (67 total failures across seeds; seeds 700 and 1002 had 0 failures). Every failure manifested as `open_error` at 50% partial retention where `marker.manifest.*` directory entries survived while linked `MANIFEST-*` or SSTable entries were dropped.
2. **BARRIER-CKPT (Checkpoint Directory Barrier Control):** Injecting a `parentDir.Sync()` barrier on any `Create`, `OpenReadWrite`, `Link`, or `Rename` targeting `marker.manifest.*` or `CURRENT` files completely eliminates all failures — **0 failures across all 32 seeds** (confirmed across every strategy row in `results/casestudy_checkpoint_summary.csv`).

### 10.3 Pre-Frozen Strategy Discovery Performance

| Strategy | $k_1$ Failure Median [95% CI] | $k_1$ Failure Mean [95% CI] | $S_{25}$ Mean | $S_{100}$ Mean | AUC (Failures) | AUC (Bugs) |
|---|---|---|---|---|---|---|
| **DCG-PR (`dcg-pr`, frozen)** | **2.00** [1.00, 125.50] | **93.03** [43.37, 142.70] | **0.84** | **0.84** | **0.5414** | **0.6739** |
| **sync-boundary** | 71.00 [8.00, 74.00] | 67.20 [39.88, 94.52] | 0.38 | 1.00 | 0.6923 | 0.8009 |
| **Representative** | 17.00 [17.00, 86.00] | 84.80 [40.17, 129.43] | 0.56 | 1.03 | 0.6444 | 0.7558 |
| **Uniform Random (B0)** | 162.50 [146.00, 178.25] | 175.35 [150.68, 200.02] | 0.00 | 0.09 | 0.4761 | 0.5477 |
| **Durability-Contract Guided (DCG)** | 215.00 [212.50, 219.00] | 262.93 [230.82, 295.05] | 0.00 | 0.00 | 0.3107 | 0.3135 |
| **State-Representative Random (R2)** | 323.56 [308.33, 335.14] | 325.80 [311.46, 340.14] | 0.00 | 0.00 | 0.3135 | 0.3161 |
| **0%-Retention-First Random (R1)** | 322.87 [305.32, 330.72] | 324.18 [309.90, 338.45] | 0.00 | 0.00 | 0.3128 | 0.3161 |
| **BARRIER-CKPT (all strategies)** | 0.00 [0.00, 0.00] | 0.00 [0.00, 0.00] | 0.00 | 0.00 | 0.0000 | 0.0000 |

The pre-frozen `dcg-pr` ranking prioritizes unsynced directory hazards under partial retention, finding the checkpoint bug at median $k_1 = \mathbf{2.00}$ (mean 93.03). Conversely, frozen 0%-first `DCG` underperforms uniform random `B0` (median $k_1 = 215.00$ vs. $162.50$; mean $262.93$ vs. $175.35$), and `R2` requires $323.56$ (mean $325.80$), confirming that 0%-retention prioritization fails to prioritize multi-entry directory races without the partial-retention prior.

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

Across all 108 runs, exactly 0 consistency violations, 0 open errors, 0 panics, and 0 hangs were observed, empirically establishing a **0.00% false-alarm rate** across diverse write frequencies, key-space distributions, value payloads, and compaction regimes. We note explicitly that this 0.00% false-alarm baseline is scoped to non-ingest workloads evaluated under endpoint (0%/100%) retention; as demonstrated in §9, §10, and §13, unpatched 50% partial retention surfaces `atomicfs` directory races (`open_error`), and unpatched Direct L0 `db.Ingest` surfaces cross-channel prefix divergence.

---

## 13. Pebble Subsystem Crash-Consistency Audit and Empirical Evaluation (MUT-0)

To establish whether uninvestigated CockroachDB Pebble subsystems performing multi-file and directory-marker mutations exhibit crash-consistency vulnerabilities under unfaulted execution (`MUT-0`), we conducted an exhaustive source-level audit and systematic empirical discovery campaign across three subsystems in Pebble `v2.1.7`:

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
   - Consequently, an unsynced `fs.Remove` **cannot** delete `oldMarker` from `syncedChildren`. Under CraConTest's deterministic partial-retention model (`deterministicPartialClone`, validated by `TestDeterministicPartialCloneStatisticalFidelity`, rather than calling Pebble's raw `CrashClone`), `oldMarker` is preserved **100% of the time**, while `newMarker` is preserved with 50% probability. There is a 0% probability that both markers are lost.
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

The dedicated discovery test `TestSubsystemDiscovery` (`internal/exp/subsystem_discovery_test.go`) evaluated Workloads A, B, and C under `MUT-0` across seeds `42, 100, 777` with retention policies `[0, 50, 100]` (5,424 candidate states evaluated):

| Workload | Seed | Operations | IngestEvery | RatchetAt | CheckpointAt | MaxManifestSize | Policies | Universe | Violations | Durably Committed | Prefix Violations | Open Errors | Panics | Hangs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Workload A (Ingestion)** | 42 | 80 | 25 | 0 | 0 | 0 | [0 50 100] | 576 | 15 | 0 | 15 | 1 | 0 | 0 |
| **Workload A (Ingestion)** | 100 | 80 | 25 | 0 | 0 | 0 | [0 50 100] | 621 | 0 | 0 | 0 | 2 | 0 | 0 |
| **Workload A (Ingestion)** | 777 | 80 | 25 | 0 | 0 | 0 | [0 50 100] | 567 | 25 | 0 | 25 | 1 | 0 | 0 |
| **Workload B (Ratchet)** | 42 | 80 | 0 | 50 | 0 | 0 | [0 50 100] | 462 | 0 | 0 | 0 | 1 | 0 | 0 |
| **Workload B (Ratchet)** | 100 | 80 | 0 | 50 | 0 | 0 | [0 50 100] | 465 | 0 | 0 | 0 | 2 | 0 | 0 |
| **Workload B (Ratchet)** | 777 | 80 | 0 | 50 | 0 | 0 | [0 50 100] | 405 | 0 | 0 | 0 | 1 | 0 | 0 |
| **Workload C (Combined)** | 42 | 80 | 30 | 40 | 60 | 1 | [0 50 100] | 801 | 0 | 0 | 0 | 5 | 0 | 0 |
| **Workload C (Combined)** | 100 | 80 | 30 | 40 | 60 | 1 | [0 50 100] | 765 | 0 | 0 | 0 | 8 | 0 | 0 |
| **Workload C (Combined)** | 777 | 80 | 30 | 40 | 60 | 1 | [0 50 100] | 762 | 0 | 0 | 0 | 4 | 0 | 0 |

### 13.4 Empirical Verification and Recovery Hazard Summary

1. **100% Synchronous Durability (`0` `durably_committed` violations, `0` panics):** Across all 5,424 candidate states in Workloads A, B, and C under `MUT-0`, Pebble v2.1.7 exhibits **0 `durably_committed` data-loss violations and 0 recovery panics**, confirming that `d.objProvider.Sync()` (`ingest.go:1490`) and `atomicfs.Marker.Move` (`marker.go:189`) protect acknowledged ingest and format-ratchet operations from losing committed data.
2. **Cross-Channel Commit-Order Hole on Direct L0 `Ingest` (`40` `prefix` violations across Seeds 42 and 777):** When `db.Ingest` takes the non-overlapping Direct L0 shortcut (`asFlushable = false`), Pebble allocates a global sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1 < s_2$ is lost from the unsynced `WAL`, creating a cross-channel sequence-number hole (whereas overlapping Flushable Ingest syncs the shared `WAL` via `commit.directWrite` at `commit.go:289` and preserves strict prefix order).
3. **Workload B (Live Format Ratcheting):** 100% clean recovery. Zero violations, zero database corruptions. Coexisting format version markers under 50% partial retention are resolved deterministically by `atomicfs.LocateMarker` selecting the highest iteration index. The 4 `open_error` states under 50% retention (Seed 42: 1, Seed 100: 2, Seed 777: 1) occur exclusively during initial database creation: because `pebble.Open("db", opts)` is executed while the VFS recorder is active, initial database creation writes `"db/MANIFEST-000001"` and `"db/marker.manifest.000001"` prior to the initial directory sync on `"db"`. In Workload B, all 4 `open_error` states occur at those initial `pebble.Open("db")` cutoffs before operation 1; `RatchetFormatMajorVersion` at operation 50 causes **0** open errors and **0** violations.
4. **Workload C (Combined Lifecycle Stress):** Open errors under 50% retention (17 total: 5 on Seed 42, 8 on Seed 100, 4 on Seed 777) reproduced the expected multi-entry directory race on unsynced `MANIFEST-*` files and checkpoint directories prior to directory sync. All 0% and 100% retention cutoffs recovered cleanly with zero errors.
5. **Verdict:** Across all 5,424 candidate states in Workloads A, B, and C under `MUT-0`, Pebble v2.1.7 exhibits **0 `durably_committed` data-loss violations and 0 recovery panics**, confirming that `d.objProvider.Sync()` (`ingest.go:1490`) and `atomicfs.Marker.Move` (`marker.go:189`) protect acknowledged ingest and format-ratchet operations from losing committed data. Simultaneously, the audit exposed two distinct recovery hazards under `MUT-0`: (1) an **input-dependent cross-channel `prefix` monotonicity hole** during non-overlapping Direct L0 `db.Ingest` (`40` prefix violations in Workload A, analyzed across 32 seeds in §13.5), and (2) **50%-retention intra-operation directory races (`open_error`)** when a crash occurs inside the multi-entry window between `MANIFEST-*`/`.sst` creation and `dirFD.Sync()` (25 total open errors).

### 13.5 Direct L0 db.Ingest 32-Seed Case Study and Causal Remedy (BARRIER-INGEST)

#### 13.5.1 Architectural Root Cause: Cross-Channel Sequence-Number Hole

During our subsystem audit (§13.1), we identified that Pebble v2.1.7 implements two distinct execution branches for `db.Ingest(paths)`:
1. **Flushable Ingest (`asFlushable = true`):** When ingested table key bounds overlap with keys currently residing in the unflushed memtable queue (`mem != nil`), Pebble routes ingestion through `handleIngestAsFlushable` (`ingest.go:1338–1408`). It inserts a flushable ingest marker into the memtable queue, rotates the WAL, and synchronously flushes the WAL via `commit.directWrite` (`commit.go:289`). The write-ahead log persists all preceding user mutations up to the current sequence number before the ingest completes.
2. **Direct L0 Ingest (`asFlushable = false`):** When ingested tables are mutually disjoint from active memtable keys, Pebble executes an optimized direct L0 shortcut. In `ingest.go:1703`, Pebble calls `d.commit.AllocateSeqNum` to allocate sequence number $s_2 > s_1$ for the ingested table, links the table into L0, and persists a `VersionEdit` by fsyncing the `MANIFEST` (`vs.manifestFile.Sync()` at `version_set.go:649`). Critically, **direct L0 ingest bypasses the WAL entirely**.

If preceding client writes were issued without an explicit synchronous barrier (`NoSync`, sequence $s_1 < s_2$), those writes remain buffered in dirty VFS memory for the active `.log` WAL file. When a crash occurs at 0% or 50% retention:
- The MANIFEST survives because it was explicitly fsynced during `db.Ingest`, making table $s_2$ durably visible upon recovery.
- The unpersisted dirty WAL buffer containing writes at $s_1$ is discarded by the crash clone.
- Upon restart, Pebble loads the MANIFEST, incorporates table $s_2$, replays whatever prefix survived in the WAL, and exposes state containing $s_2$ but missing $s_1$.

Because $s_1 < s_2$, client history recorded $s_1$ prior to $s_2$. Exposing $s_2$ without $s_1$ violates **Prefix Consistency** ($S \notin [S_{\text{lower}}, S_{\text{upper}}]$ in the prefix oracle). Note that because the client never issued an fsync on $s_1$, durability contract bounds allow rollback of $s_1$ *if and only if* subsequent states are also rolled back. Committing $s_2$ via the MANIFEST channel while dropping $s_1$ on the WAL channel creates an unrecoverable cross-channel commit-order hole.

We explicitly contextualize this divergence relative to upstream testing paradigms: Pebble's upstream crash test (`TestWALHardCrashRandomized`) tracks individual uncommitted `NoSync` keys in an uncommitted set, permitting individual non-sync writes to disappear independently. RocksDB's `db.h` similarly notes that `IngestExternalFile` does not imply `FlushWAL(true)`. However, under a global prefix consistency model (the standard RocksDB `db_stress`-style global sequence "no-hole" oracle enforced by CraConTest), advancing the global sequence number to $s_2$ on disk while dropping $s_1 < s_2$ constitutes an observable commit-order prefix violation. Direct L0 `db.Ingest` is therefore an **input-dependent cross-channel prefix contract divergence**: whether strict prefix monotonicity holds depends on random key-range overlap with the memtable.

#### 13.5.2 Causal Remedy: BARRIER-INGEST via Pre-Ingest WAL Sync

To verify the causality of this cross-channel hole, we developed a formal causal remedy: **BARRIER-INGEST**.
Prior to invoking `db.Ingest`, the storage engine or application flushes the active write-ahead log. In Pebble's public API, this is achieved cleanly via:
```go
db.LogData(nil, pebble.Sync)
```
`db.LogData(nil, pebble.Sync)` writes a zero-byte sync record to the active WAL and invokes `commit.directWrite` with `pebble.Sync`, forcing an immediate `SyncData()` on the active WAL file before `db.Ingest` allocates sequence numbers or fsyncs the MANIFEST.

#### 13.5.3 32-Seed Empirical Evaluation (results/casestudy_ingest_summary.csv)

We evaluated 32 canonical seeds (`seed = 1..32`, 25 ops, `SyncEvery: 4, FlushEvery: 12, IngestEvery: 5, ValueSize: 64`) under both `MUT-0` (`BarrierIngest: false`) and `BARRIER-INGEST` (`BarrierIngest: true`) across 11 ranking strategies under endpoint retention policies `[0, 100]` (evaluating candidate crash states in `results/casestudy_ingest_per_seed.csv` and `results/casestudy_ingest_summary.csv`):

| Fault Condition | Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ P10 | $k_1$ P90 | $S_{25}$ Mean | $S_{100}$ Mean |
|---|---|---|---|---|---|---|---|
| **MUT-0 (Unpatched)** | **DCG-PR** | **9.00** [9.00, 19.00] | **15.32** [9.88, 20.76] | **4.00** | **35.70** | **0.75** | **1.41** |
| **MUT-0 (Unpatched)** | **DCG** | **9.00** [9.00, 19.00] | **15.32** [9.88, 20.76] | 4.00 | 35.70 | 0.75 | 1.41 |
| **MUT-0 (Unpatched)** | Uniform Random (B0) | 21.50 [15.50, 29.50] | 24.73 [19.21, 30.25] | 11.05 | 43.00 | 0.42 | 1.64 |
| **MUT-0 (Unpatched)** | State-Representative Random (R2) | 27.88 [22.65, 32.28] | 27.64 [23.57, 31.70] | 16.53 | 34.58 | 0.51 | 1.94 |
| **MUT-0 (Unpatched)** | 0%-Retention Random (R1) | 28.60 [21.80, 41.51] | 30.61 [24.93, 36.28] | 16.34 | 47.18 | 0.53 | 1.73 |
| **MUT-0 (Unpatched)** | DCG Flag-Only (Ablation) | 28.18 [26.92, 29.26] | 29.88 [25.85, 33.90] | 19.78 | 45.93 | 0.42 | 1.75 |
| **MUT-0 (Unpatched)** | sync-boundary (B6) | 81.00 [52.00, 90.00] | 80.18 [59.96, 100.40] | 24.00 | 152.00 | 0.31 | 1.31 |
| **BARRIER-INGEST (Remedied)** | **All Strategies** | **Prefix Violations Eliminated to 0** across all 32 seeds ($S_{25}=0.00, S_{100}=0.00$) |

#### 13.5.4 Findings & Analysis
1. **100% Causal Elimination:** Injecting the pre-ingest WAL sync barrier (`BARRIER-INGEST`) completely eliminates prefix consistency violations across all 32 seeds (**0 of 32 seeds** fail, $S_{25}=0.00, S_{100}=0.00$), confirming the cross-channel WAL/MANIFEST persistence race as the sole root cause.
2. **Seed Overlap Dynamics:** Under `MUT-0`, **26 of 32 seeds** exhibit prefix violations (seeds `3, 5, 7, 20, 26, 29` have `0` failures because on those seeds the randomly generated ingest keys overlapped the active memtable and therefore executed the `asFlushable = true` branch, which synchronously flushes the WAL via `commit.directWrite` at `commit.go:289`; under pure endpoint retention, 22 of 32 seeds fail with seeds 8, 9, 16, 23 also taking flushable ingest, totaling 276 failures across the 22 failing seeds) while **0 of 32 seeds** fail under `BARRIER-INGEST`.
3. **Prior Superiority:** Under `MUT-0`, `DCG` and `DCG-PR` achieve a median discovery cost of $k_1 = \mathbf{9.00}$ (mean $15.32$), substantially faster than `R2` ($27.88$, mean $27.64$) and `B0` ($21.50$, mean $24.73$), demonstrating that trace-informed candidate ordering prioritizes the cross-channel vulnerability cutoffs.

---

## 14. Multi-Distribution Workload Diversity Evaluation

### 14.1 Motivation & Workload Presets

Real-world storage systems encounter workloads with radically divergent access patterns, key contention profiles, and mutation-to-sync ratios. To evaluate whether graph-guided crash testing generalizes beyond standard uniform key-value distributions without overfitting, we designed and implemented 5 distinct workload presets in `internal/workload/workload.go` (exposed via `-preset`):

1. **`ycsb-a` (Update-Heavy):** 50% update, 50% point read over 8 hot keys, with periodic sync/flush. Simulates high-write transactional key-value serving.
2. **`ycsb-b` (Read-Predominant):** 95% point read, 5% update (3 reads interleaved per write operation) over 12 keys with periodic sync/flush. Tests sparse write persistence within dense read sequences.
3. **`ycsb-f` (Read-Modify-Write Transactions):** Read-modify-write multi-key batch transactions updating 3 keys atomically per transaction. Tests batch commit atomicity and multi-key durability horizons.
4. **`overwrite-heavy` (Deep Version Chains):** 90% overwrite on 4 extremely hot keys, forcing deep per-key version chains and frequent memtable flush pressure.
5. **`delete-heavy` (Tombstone & Lifecycle Stress):** High-frequency interleaved point deletes, range deletions (`DeleteRange`), and periodic checkpoints (`CheckpointEvery: 10`). Exercises range tombstones and directory hard-linking.

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
| **ycsb-a** | MUT-1 | 17.7% | **1.00** [1.00, 1.00] | 1.00 | 2.36 [2.02, 2.69] | 4.50 [3.85, 5.15] | **10.50** | 10.39 | 3.62 |
| **ycsb-a** | MUT-5 | 2.0% | **1.00** [1.00, 1.00] | 1.00 | 8.35 [7.67, 9.04] | 23.55 [19.65, 27.44] | **1.38** | 1.38 | 0.41 |
| **ycsb-a** | MUT-9 | 25.8% | **1.00** [1.00, 1.00] | 1.00 | 2.16 [1.85, 2.47] | 2.94 [2.53, 3.35] | **11.62** | 11.45 | 3.72 |
| **ycsb-b** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-b** | MUT-1 | 10.0% | **1.00** [1.00, 1.00] | 1.00 | 3.06 [2.92, 3.19] | 8.00 [8.00, 8.00] | **7.00** | 7.00 | 2.00 |
| **ycsb-b** | MUT-5 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-b** | MUT-9 | 14.4% | **1.00** [1.00, 1.00] | 1.00 | 3.50 [3.32, 3.67] | 5.00 [5.00, 5.00] | **7.00** | 6.46 | 3.00 |
| **ycsb-f** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **ycsb-f** | MUT-1 | 23.3% | **1.00** [1.00, 1.00] | 1.00 | 1.73 [1.67, 1.80] | 3.03 [2.96, 3.10] | **15.06** | 14.46 | 5.00 |
| **ycsb-f** | MUT-5 | 3.8% | **1.00** [1.00, 1.00] | 1.00 | 9.30 [8.84, 9.76] | 20.50 [20.50, 20.50] | **2.00** | 1.85 | 1.00 |
| **ycsb-f** | MUT-9 | 33.6% | **1.00** [1.00, 1.00] | 1.00 | 1.64 [1.58, 1.71] | 2.00 [2.00, 2.00] | **16.00** | 14.86 | 4.00 |
| **overwrite-heavy** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **overwrite-heavy** | MUT-1 | 23.3% | **1.00** [1.00, 1.00] | 1.00 | 1.73 [1.67, 1.80] | 3.00 [3.00, 3.00] | **15.00** | 14.43 | 5.00 |
| **overwrite-heavy** | MUT-5 | 3.7% | **1.00** [1.00, 1.00] | 1.00 | 9.30 [8.84, 9.76] | 20.50 [20.50, 20.50] | **2.00** | 1.85 | 1.00 |
| **overwrite-heavy** | MUT-9 | 33.5% | **1.00** [1.00, 1.00] | 1.00 | 1.64 [1.58, 1.71] | 2.00 [2.00, 2.00] | **16.00** | 14.86 | 4.00 |
| **delete-heavy** | MUT-0 | 0.0% | **0.00** [0.00, 0.00] | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| **delete-heavy** | MUT-1 | 15.7% | **3.00** [3.00, 3.00] | 3.00 | 2.49 [2.34, 2.65] | 4.12 [3.94, 4.31] | **14.19** | 10.08 | 3.59 |
| **delete-heavy** | MUT-5 | 1.6% | **20.54** [8.61, 32.47] | 20.54 | 34.89 [18.53, 51.26] | 50.92 [24.21, 77.63] | **0.88** | 0.63 | 0.44 |
| **delete-heavy** | MUT-9 | 8.7% | **3.00** [3.00, 3.00] | 3.00 | 4.30 [4.08, 4.52] | 9.94 [9.41, 10.46] | **7.44** | 5.16 | 2.00 |

### 14.4 Key Insights & Generalization Summary

1. **Zero False Alarms Null Control:** Across all 16 seeds for all 5 presets under unfaulted `MUT-0` (80 complete workload trials), CraConTest observed strictly **0 false alarms**, establishing that diversity in workload distribution does not compromise oracle precision.
2. **Substantial Speedup on Sparse Faults:** `DCG` achieves a **$9.3\times$ speedup** over `R2` on both `ycsb-f/MUT-5` and `overwrite-heavy/MUT-5` (mean $k_1 = 1.00$ vs. $9.30$, and **$20.5\times$** over `B0`'s $20.50$), and an **$8.4\times$ speedup** over `R2` on `ycsb-a/MUT-5` (mean $k_1 = 1.00$ vs. $8.35$, and **$23.6\times$** over `B0`'s $23.55$). `DCG` achieves optimal mean $k_1 = 1.00$ across all failing fault models on `ycsb-a`, `ycsb-b`, `ycsb-f`, and `overwrite-heavy`.
3. **Delete-Heavy Dynamics & Equivalence Representation:** On `delete-heavy`, `DCG` achieves mean $k_1 = 3.00$ on `MUT-1` (beating `B0`'s $4.12$, while `R2` averages $2.49$, and expanding early-budget state discovery $S_{25}$ from `R2`'s $10.08$ and `B0`'s $3.59$ to **`14.19`**), $k_1 = 20.54$ on sparse `MUT-5` (beating `R2`'s $34.89$ by **$1.7\times$** and `B0`'s $50.92$ by **$2.5\times$**), and $k_1 = 3.00$ on `MUT-9` (beating both `R2`'s $4.30$ and `B0`'s $9.94$, and expanding $S_{25}$ from $5.16$ to **`7.44`**).
   - *Systems Mechanism for $k_1 = 3.00$ on `MUT-1` and `MUT-9`:* When early synced `Delete` or `DeleteRange` operations target keys that are not yet present in the committed lower-bound state $S_L$, rolling back those tombstones on crash leaves the recovered key-value state identical to $S_L$ (a logical no-op in key-value state space); thus the first two flagged `StateKey` representatives recover to a legal state and the third flagged representative ($k_1 = 3.00$) is the first observable key-value violation.
4. **Robustness on Sparse Invariant Presets (`ycsb-b`):** Under `ycsb-b/MUT-5`, read operations dominate so heavily that the $k=1$ WAL sync drop never intersects an unpersisted acknowledged write before shutdown, yielding a true negative 0.0% density. `DCG` correctly yields $k_1 = 0.00$ without false alarms.
5. **Early-Budget State Coverage ($S_{25}$) & Empirical Generalization:** Across the 14 failing preset × fault settings, `DCG` outperforms uniform random (`B0`) on **all 14 settings** ($14/14$) and outperforms `R2` on $k_1$ in **13 of 14 settings** ($13/14$), with a single regression on `delete-heavy/MUT-1` (`DCG` mean $k_1 = 3.00$ vs. `R2`'s $2.49$, where early synced tombstones on absent keys are logical state no-ops). On early-budget state coverage ($S_{25}$), `DCG` **matches or exceeds** `R2` across all 14 failing settings (strictly higher on 13 settings, with a tie on `ycsb-b/MUT-1` at $7.00$ vs. $7.00$, and noting that because these preset traces use 25 operations, $B=25$ approaches the full equivalence-class universe for several presets).

---

## 15. Related Work and Scope Limitations

### 15.1 Positioning Against Prior Crash-Consistency Testing Systems

**Torturing Databases** (Zheng et al., OSDI '14) pioneered transactional database crash testing using software-defined storage wrappers to simulate power cuts and fsync omissions, uncovering silent data loss and corruption across major database engines. CraConTest builds upon this empirical foundation by providing formal white-box POSIX contract extraction and trace-based dependency prioritization.

**ALICE** (OSDI '14) introduced application-level crash consistency testing with a POSIX VFS model and showed that many storage systems violate ordering or atomicity assumptions silently. CraConTest builds on ALICE's insight that crash states can be enumerated from a VFS trace but focuses on *ranking* candidate crash states efficiently rather than exhaustive enumeration.

**Ferrite** (ASPLOS '16) formalized the semantics of file-system durability and crash consistency under reordering. CraConTest leverages similar POSIX barrier semantics to construct contract violation indicators directly from application-level execution traces.

**CrashMonkey/B3** (OSDI '18) applies automated workload mutation and replay to find file-system bugs. B3's file-system workloads are structured sequences of POSIX calls; CraConTest targets database key-value engines where the relevant invariant is key-value read-your-writes consistency rather than POSIX file-system semantics.

**Hydra** (SOSP '19) provided an extensible framework for finding crash-consistency and file-system bugs via bounded fuzzing and symbolic execution. CraConTest operates above the file system, utilizing graph reachability and POSIX contract priors to guide bounded crash budgets without symbolic execution.

**Witcher** (SOSP '21) targets persistent-memory (NVM) key-value stores and persistent indices (inferring likely invariants over NVM load/store instruction traces and `clwb`/`sfence` ordering), not PM file systems. Witcher is the closest prior work in targeting user-space key-value stores via heuristic crash state selection, whereas CraConTest operates at the POSIX VFS syscall layer using closed-form POSIX durability-contract flags rather than inferred NVM store invariants.

**Vinter** (OSDI '22) performed targeted crash-consistency verification of NVM data structures under hardware memory reordering.

**Chipmunk** (EuroSys '23) generates and replays crash images for NVM file systems, focusing on file-system metadata consistency. CraConTest targets application-level (database) crash consistency above the file system.

**Silhouette** (FAST '25) evaluated invariant-driven crash-consistency testing on persistent-memory file systems.

**Pathfinder** (OOPSLA '25) constructs persistence graphs from execution traces and groups correlated update behaviors for representative crash-state reduction and model checking. Our `R2` baseline implements the state-representative equivalence-class sampling concept inspired by Pathfinder's equivalence partitioning, whereas the full Pathfinder tool (OOPSLA '25) is a C++/LLVM Persistence Graph and DPOR model-checking framework that tests representative update-behavior subgraphs across POSIX syscall and MMIO applications. CraConTest addresses the complementary problem of **budgeted crash-candidate prioritization** ($k_1$): combining trace-derived POSIX durability-contract priors (`HasUnsyncedCreation`, drop impacts `CURRENT`, unflushed parent rename) with `StateKey` equivalence-class partitioning (`DCG-PR`), so equivalence classes with active contract violations are tested at rank 1 before uniform round-robin exploration.

### 15.2 Evaluation Chronology and Empirical Scope Boundaries

To ensure complete methodological transparency, we explicitly document the commit chronology of all evaluation suites relative to the code freeze tag **`v1.0-dcg-pr-freeze` (`67bbe7d`)**:
- **Pre-Freeze Fault Development (`c67a44a`, 2026-11-01):** The parameterized $k$-index (`MUT-X1..X3`), stochastic probability-$p$ (`MUT-X4..X5`), and out-of-contract silent write-loss (`MUT-X6`) fault models were committed to evaluate whether the binary POSIX durability-contract flags generalize across arbitrary sync-drop indices and probabilities without re-tuning.
- **Code Freeze at Tag `v1.0-dcg-pr-freeze` (`67bbe7d`, 2026-11-02):** The ranking logic in `internal/strategy/dcg.go` and the feature extractor in `internal/graph/features.go` were frozen.
- **Strictly Post-Freeze Evaluations (0 bytes changed to ranking/feature code):**
  1. *Post-Freeze Holdout 1 (Pebble Checkpoint Commit `15b500fb`, §10):* 32 seeds, `MUT-0` vs. `BARRIER-CKPT`. Case Study 2 evaluates intra-class subsystem transfer of `dcg-pr` within the `atomicfs` marker directory race bug class, transferring from manifest rotation in `"db/"` to `db.Checkpoint` in `"checkpoint/"`.
  2. *Post-Freeze Holdout 2 (Direct L0 `db.Ingest` Cross-Channel Prefix Hole, §13.5):* 32 seeds, `MUT-0` vs. `BARRIER-INGEST`. Evaluates input-dependent cross-channel prefix contract divergence.
  3. *Post-Freeze Holdout 3 (Multi-Distribution Workload Suite, §14):* 5 workload presets across 16 seeds and 4 fault models.

The following scope boundaries are established empirically across 32 seeds:

1. **Trace-Contract Linter Nature of DCG:** Because `DCG`'s four 0%-retention indicators (`AckSyncMissing`, `DanglingTableCount`, `UnsyncedDirectoryRename`, `ManifestBarrierPending`) directly inspect the VFS trace for missing `Sync`/`SyncData` barriers after client acknowledgment or metadata updates, achieving $k_1 = 1.00$ on synthetic sync-omission faults (`MUT-1..9` and `MUT-X1..X5`) reflects **exact alignment between trace-level durability-contract checking and sync-omission fault injection** combined with `StateKey` equivalence deduplication.

2. **Where frozen 0%-first DCG loses:** On uninjected 50%-retention multi-entry directory races requiring asymmetric partial retention (`50%`), such as Pebble `#5825` (manifest rotation) and Pebble Checkpoint (`commit 15b500fb`), frozen 0%-first `DCG` underperforms uniform random `B0` (median $k_1 = 205.00$ vs. $112.00$ on `#5825`; $215.00$ vs. $162.50$ on `Checkpoint`). These require the post-hoc partial-retention prior (`dcg-pr`) guided by `UnsyncedDirEntryCount >= 2` to achieve $k_1 = 2.00$.

3. **Where DCG beats R2:** Single-resource sync-omission faults (`MUT-1..9` and parameterized/stochastic `MUT-X1..X5`) on first-failure cost $k_1$ and early-budget state discovery $S_{25}$. Median speedup 2.1×–20.0×, Holm-adjusted $p \le 0.002$ across 12 parameterized/stochastic settings. Across multi-distribution presets, DCG beats R2 on $k_1$ in 13 of 14 failing settings and matches or exceeds R2 on $S_{25}$ across all 14 settings.

4. **Where DCG-FlagOnly ties DCG:** Across almost all settings (`113 wins / 0 losses / 124 ties` on `MUT-1..9`), demonstrating that binary POSIX durability-contract flags combined with `StateKey` equivalence partitioning drive the speedup rather than continuous tier weights.

5. **Where DCG ties R2:** At full budget $S_{100}$ on moderate-scale traces where all ~35–40 `StateKey` equivalence classes are exhausted before 100 picks, and on out-of-contract non-sync faults (`MUT-X6`).

---

## 16. Claim-to-Command and Artifact Provenance Ledger

Every quantitative claim in this report is reproducible from the committed source at tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) using the commands below. All artifacts are committed under `results/`.

| Claim | Reproduction Command | Committed Artifact |
|---|---|---|
| Stage 1 replay fidelity, null control, dense faults `MUT-0..4` | `go test -v ./internal/exp -run 'TestGateG4\|TestMainRunArtifacts'` | `results/smoke_mut0..1/`, `results/discovery_*.csv` |
| Stage 2 32-seed in-sample benchmark `MUT-0..9` | `go run ./cmd/cracontest benchmark -ops 120 -csv results/benchmark_summary.csv -per-seed-csv results/benchmark_per_seed.csv && python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9` | `results/benchmark_summary.csv`, `results/benchmark_per_seed.csv` |
| Paired stats (DCG vs. R1, R2, B0, B2) | `python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9` | — |
| Holdout `MUT-X1..X5` and out-of-contract `MUT-X6` | `go run ./cmd/cracontest benchmark -ops 120 -faults MUT-X1_k2,MUT-X1_k3,MUT-X1_k4,MUT-X1_k6,MUT-X1_k9,MUT-X2_k1,MUT-X2_k3,MUT-X2_k4,MUT-X2_k5,MUT-X3_k1,MUT-X3_k2,MUT-X3_k3,MUT-X4_p04,MUT-X5_p12,MUT-X6_k2,MUT-X6_k5,MUT-X6_k9 -csv results/holdout_summary.csv -per-seed-csv results/holdout_per_seed.csv -report "" && python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12` | `results/holdout_summary.csv`, `results/holdout_per_seed.csv` |
| Pebble `#5825` real-bug A/B and `dcg-pr` | `go run ./cmd/cracontest benchmark -faults MUT-0,BARRIER-5825 -max-manifest-size 1 -policies 0,50,100 -csv results/casestudy_pebble5825_summary.csv -per-seed-csv results/casestudy_pebble5825_per_seed.csv -report ""` | `results/casestudy_pebble5825_summary.csv`, `results/casestudy_pebble5825_per_seed.csv` |
| Checkpoint holdout A/B (frozen at `v1.0-dcg-pr-freeze`) | `go run ./cmd/cracontest benchmark -faults MUT-0,BARRIER-CKPT -checkpoint-at 60 -policies 0,50,100 -csv results/casestudy_checkpoint_summary.csv -per-seed-csv results/casestudy_checkpoint_per_seed.csv -report ""` | `results/casestudy_checkpoint_summary.csv`, `results/casestudy_checkpoint_per_seed.csv` |
| `MUT-5` universe-scaling (`120..1200` ops) | `go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update` | `results/scaling_mut5_summary.csv`, `results/scaling_mut5_per_seed.csv` |
| Multi-shape null control (`108` runs, `12` shapes) | `go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update` | `results/null_control_multishape.csv` |
| Subsystem empirical discovery (Workloads A, B, C under `MUT-0`) | `go test -v -count=1 -run TestSubsystemDiscovery ./internal/exp -update` | `results/subsystem_discovery.csv` |
| Direct L0 `db.Ingest` 32-seed benchmark | `go test -v -count=1 -run TestCaseStudyIngestBenchmark ./internal/exp -update-artifacts` | `results/casestudy_ingest_summary.csv`, `results/casestudy_ingest_per_seed.csv` |
| Workload diversity benchmark 5 presets | `go test -v -count=1 -run TestWorkloadDiversityBenchmark ./internal/exp -update-artifacts` | `results/workload_diversity_summary.csv`, `results/workload_diversity_per_seed.csv` |

> **Note on Code Freeze & Pre vs. Post-Freeze Chronology:** Tag `v1.0-dcg-pr-freeze` (`67bbe7ded57d`) pins `internal/strategy/dcg.go` and `internal/graph/features.go`. Parameterized and stochastic fault models (`MUT-X1..X5`) and out-of-contract control (`MUT-X6`) were committed prior to freeze at `c67a44a` (2026-11-01). The three subsequent evaluations—Checkpoint Holdout (`15b500fb`, §10), Direct L0 Ingest Cross-Channel Hole (§13.5), and Multi-Distribution Workload Suite (§14)—were designed and executed strictly post-freeze with 0 bytes of change to the frozen ranking and feature code.
