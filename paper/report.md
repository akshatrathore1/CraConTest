# CraConTest Technical Report: Dependency-Guided Crash Testing on Pebble

**Date:** October 2026
**Subject:** Experimental evaluation of persistence-dependency-guided crash-point ranking on CockroachDB's Pebble key-value storage engine (v2.1.7).

---

## 1. Summary

Crash-consistency testing must choose which crash states to examine: every filesystem
event is a possible crash point, and each point has several possible post-crash states
depending on which unsynced data survives. CraConTest records one execution, rebuilds any
crash state from that recording, and asks whether a ranking derived from persistence
dependencies finds consistency failures with fewer crash injections than random
selection or simpler baselines.

Research question:

> *Can semantic and persistence dependencies concentrate crash testing on fewer crash
> states while finding consistency failures at least as fast as random selection?*

Findings on Pebble v2.1.7 with one workload (seed 42) and four seeded faults:

1. **No false alarms.** On unfaulted runs (MUT-0) every crash candidate recovered and
   matched a legal state: 2,150 of 2,150 in the main 1,000-operation run and 302 of 302
   in the smoke run.
2. **Every seeded fault is detected**, each by a different symptom: lost acknowledged
   writes (dropped WAL and directory syncs), a malformed manifest (dropped manifest sync),
   and a table whose size disagrees with the manifest (dropped flush sync).
3. **All four faults are dense.** Between 28% and 50% of all candidates fail. Under such
   faults random order reaches a first failure within a handful of crashes (median 1–3),
   faster than every guided order.
4. **Guided orders cover more distinct violations at moderate budgets.** Under the dropped
   WAL sync, the sync-boundary order (B6) has found 13 of 15 violation signatures after
   100 crashes, more than any of ten random orders (7–11), and the representative order
   (B5) finds all 15 after 189 crashes, where random orders need 853 (10th–90th percentile
   431–1,450). This advantage is partly built in, because signatures and B5's groups
   both describe the pending state (§5), and it does not appear for the other faults.
   The dependency-weighted score (B4) is the slowest order on every fault.

The evidence does not support the claim that dependency-weighted ranking finds failures
sooner. It does show that state-aware orderings broaden coverage of distinct failure
contexts. The decisive test, a sparse fault where most crash points are harmless, is
still to be run.

---

## 2. Method

### 2.1 System under test

- **Engine:** Pebble `v2.1.7` (`github.com/cockroachdb/pebble/v2`), unmodified.
- **Recording:** Pebble runs on `vfs.NewMem()` wrapped by a recording filesystem that logs
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

## 6. Limitations

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

## 7. Conclusion and Next Steps

The pipeline is sound and sensitive: crash states replay exactly, unfaulted runs produce no
false positives, and each of four seeded durability bugs is exposed, through lost
acknowledged writes or through refused recovery. Ranking results are mixed. Random order
reaches a first failure fastest under every fault, and the dependency-weighted score is
consistently slowest. State-aware orders (B5, B6) cover distinct violation contexts faster
than random for the WAL fault, with the caveat above.

Next steps are faults that affect only a few crash points (for example dropping a single
manifest rotation's sync rather than every manifest sync), several workload seeds with
confidence intervals, and a ranking term that reflects whether acknowledged synchronous
data is exposed at a cutoff rather than how many events it reaches.
