# CraConTest Technical Report: Dependency-Guided Crash Testing on Pebble

**Date:** October 2026
**Subject:** Experimental evaluation of persistence-dependency-guided crash-point ranking on CockroachDB's Pebble key-value storage engine (v2.1.7).

---

## 1. Summary

Crash-consistency testing must choose which crash states to examine: every filesystem
event is a possible crash point, and each point has several possible post-crash states
depending on which unsynced data survives. CraConTest records one execution, rebuilds any
crash state from that recording, and asks whether a ranking derived from persistence
dependencies finds consistency violations with fewer crash injections than random
selection or simpler baselines.

Research question:

> *Can semantic and persistence dependencies concentrate crash testing on fewer crash
> states while finding consistency violations at least as fast as random selection?*

Findings on Pebble v2.1.7 with one workload (seed 42) and one seeded fault:

1. **The oracle raises no false alarms.** On unfaulted runs (MUT-0), every crash candidate
   recovered and matched a legal state: 2,150 of 2,150 in the main 1,000-operation run, and
   0 failures of any class in the separate acceptance test over a fresh recording.
2. **The seeded fault is found.** With WAL syncs suppressed (MUT-1), 865 of 1,978 candidates
   (44%) lose acknowledged synchronous writes. They fall into 3 distinct signatures, all of
   the `durably_committed` kind.
3. **No guided ranking beats random on this fault.** Random order finds a first violation
   at a median position of 2 and all three signatures at a median of 14. The best guided
   order, sync-boundary (B6), needs 17 and 47; the combined dependency score (B4) needs 55
   and 481. Under a fault this dense, random selection is a very strong baseline. The
   question is therefore still open for sparse faults, which is the most important next
   experiment.

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

### 2.2 Workloads

| Name | Config | Operations | Keys | Value size | Sync | Flush |
|---|---|---|---|---|---|---|
| main | `experiments/configs/main.json` | 1,000 | 64 | 128 B | every 10th mutation | every 100 ops |
| smoke | `experiments/configs/smoke.json` | 100 | 16 | 64 B | every 10th mutation | every 50 ops |

Operation mix for both: 50% set, 20% get, 10% delete, 10% two-key batch, 5% range delete,
5% iteration. One sequential client; base seed 42.

### 2.3 Faults

- **MUT-0 (null control):** no fault. Any failure is a false positive of the pipeline.
- **MUT-1 (dropped WAL sync):** `Sync` and `SyncData` on files opened with category
  `pebble-wal` (through `Create` or `ReuseForWrite`) return success without syncing. Manifest,
  table and directory syncs are untouched. In `main_mut1` the trace has 0 WAL syncs
  (84 in `main_mut0`) and the same 16 manifest and 30 directory syncs.

### 2.4 Candidate universe

Cutoffs are selected from the trace (the pretrace state, every namespace mutation, every
write with a positive length, every successful barrier, the event just before each
successful barrier or rename, and the end of every mutating client operation), each combined with the 0% and 100% retention
policies and one derived seed. Every candidate is executed exactly once; strategies are
compared offline against the stored outcome table and never see outcomes.

### 2.5 Strategies

1. **B0 random:** a seeded shuffle of the universe. Discovery curves average 10 fixed seeds
   (1, 2, 3, 4, 5, 42, 100, 777, 1337, 2026); exact discovery positions use 1,000 further
   seeds.
2. **B4 combined:** greedy order by `Desc + CrossFile + Semantic + Novelty` (all weights 1),
   where `Desc` and `CrossFile` count later persistence events reachable in the dependency
   graph, `Semantic` marks events inside client writes, with a write category, or named by an
   engine event, and
   `Novelty` rewards a not-yet-selected persistence state.
3. **B5 representative:** the first candidate of each distinct (persistence state,
   retention) group, then the rest, both in trace order.
4. **B6 sync-boundary:** candidates immediately before a successful sync or rename first,
   then the rest, both in trace order.

---

## 3. Acceptance Results

| Gate | Result |
|---|---|
| G1 replay fidelity | 1,000-op recording, 2,322 events (225 KB trace). At 20 seeded cutoffs the replayed 100%-retention state is byte-identical to the live state, and every replayed state reopens. Replay plus clone ≈0.5 ms per cutoff. |
| G2 null control | Every endpoint candidate of a fresh 1,000-op MUT-0 recording (≈2,140 candidates) is valid: 0 violations, open errors, panics, hangs or oracle errors (≈7 s). |
| G3 graph labels | On a 1,000-op trace (≈2,510 events, ≈1,073 cutoffs) every candidate has an operation label and a category set; ≈173 distinct persistence states; build time ≈12 ms. `Semantic` is 1 on 96% of cutoffs, so it barely discriminates (see §5). |
| G4 discovery curves | Curves for B0, B4, B5 and B6 are computed from the stored tables of the MUT-0 and MUT-1 runs, without re-executing any candidate. |

Counts in G1–G3 vary by a few events between recordings because Pebble schedules background
work itself; every reported experiment is computed from a stored trace.

---

## 4. Discovery Results (MUT-1)

### 4.1 Main run (`results/main_mut1`, 1,978 candidates, 865 violations, 3 signatures)

**Table 1. Distinct signatures found within a budget of *n* crash injections.**
B0 is the mean over 10 seeds with [min, max].

| Strategy | 1 | 5 | 10 | 25 | 50 | 100 | 200 |
|---|---|---|---|---|---|---|---|
| B0 random | 0.4 [0, 1] | 1.4 [0, 2] | 2.2 [1, 3] | 2.7 [2, 3] | 3.0 [3, 3] | 3.0 | 3.0 |
| B4 combined | 0 | 0 | 0 | 0 | 0 | 2 | 2 |
| B5 representative | 0 | 0 | 0 | 1 | 1 | 2 | 3 |
| B6 sync-boundary | 0 | 0 | 0 | 2 | 3 | 3 | 3 |

**Table 2. Violations found within a budget of *n*.**

| Strategy | 10 | 25 | 50 | 100 | 200 |
|---|---|---|---|---|---|
| B0 random | 4.1 [1, 6] | 9.6 [7, 12] | 19.5 [16, 26] | 41.3 [38, 48] | 85.1 [67, 102] |
| B4 combined | 0 | 0 | 0 | 22 | 72 |
| B5 representative | 0 | 1 | 12 | 37 | 83 |
| B6 sync-boundary | 0 | 5 | 17 | 41 | 62 |

**Table 3. Exact discovery positions** (1-based index in the ranking; random over 1,000
seeds, median with 10th–90th percentile).

| Strategy | First violation | All 3 signatures |
|---|---|---|
| B0 random | 2 [1, 5] | 14 [6, 32] |
| B6 sync-boundary | 17 | 47 |
| B5 representative | 25 | 101 |
| B4 combined | 55 | 481 |

Figure: `results/plot_main_mut1.svg`. Data: `results/discovery_main_mut1.csv`.

### 4.2 Smoke run (`results/smoke_mut1`, 280 candidates, 88 violations, 2 signatures)

The smaller run shows the same ordering. Random finds a first violation at a median of 2 and
both signatures at a median of 8 (10th–90th percentile 3–18). B6 needs 17 and 25, B5 needs
25 and 43, and B4 needs 55 and 137. Figure `results/plot_mut1.svg`, data
`results/discovery_mut1.csv`.

### 4.3 Cost

Replay, crash, recovery and oracle check take about 6.5 ms per candidate on the main
workload (2,150 candidates in 14.1 s) and about 1 ms on the smoke workload. Per-candidate
cost grows with trace length because each crash state replays a longer prefix.

---

## 5. Discussion

1. **The fault is dense.** Violations begin at the first acknowledged synchronous write
   (event 57 of 2,427), and from there on 44% of all candidates lose acknowledged data.
   A uniformly random order then finds a violation within a few draws. This makes the
   experiment a weak test of prioritization: no ordering can do much better than random,
   and several do worse.
2. **Why B4 is slowest.** The combined score weights downstream reachability. The events
   with the largest reach are the earliest ones, so B4 starts with cutoffs before the first
   acknowledged synchronous write. All of its first 50 candidates lie there, and none of them
   can lose acknowledged data. The dependency structure is correct, but reachability from an
   early event does not indicate that a crash there can violate a client guarantee.
3. **B6 and B5.** B6 tests barrier-adjacent cutoffs first and finds violations at about the
   random rate once it reaches them (17 in its first 50, against about 22 expected for
   random). B5 first walks distinct persistence states in trace order.
4. **The Semantic term saturates.** It is 1 on 96% of cutoffs because almost every event
   carries a write category, so it contributes little to B4's order.

These results do not support the hypothesis for dense faults. They neither confirm nor
rule out a benefit for sparse faults, where most crash points are harmless and random
selection is expected to be slow.

---

## 6. Limitations

1. **One workload and one seed.** A single YCSB-A-like mix at 1,000 operations (plus a
   100-operation smoke run); no repeated workload seeds, so no confidence intervals.
2. **One dense fault.** Only MUT-1 was evaluated. Sparser faults (manifest sync, directory
   sync, SST sync) were not.
3. **Endpoint retention only.** 0% and 100% retention; intermediate policies were not
   evaluated.
4. **In-process recovery.** Exploration runs in one process with a timeout; process
   isolation was not needed for these runs.
5. **VFS-level crash model.** Crashes are simulated at the filesystem interface; device-level
   behavior is out of scope.

---

## 7. Conclusion and Next Steps

The pipeline is sound: crash states replay exactly, the oracle reports no false positives
on thousands of unfaulted crash states, and a seeded durability bug is exposed reliably.
On that bug, however, none of the guided rankings found violations faster than random
order, and the dependency-weighted score was the slowest. The next experiments are sparse
faults (MUT-2 to MUT-4), several workload seeds with confidence intervals, and a sharper
semantic term, so that ranking is tested where random selection is not already near-optimal.
