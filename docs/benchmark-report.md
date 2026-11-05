# CraConTest: Multi-Seed Evaluation Across Fault Models

**Date:** 2026-11-05  
**Git Commit:** `708316a98795`  
**Evaluation Engine:** CockroachDB Pebble v2.1.7 (Go runtime: 2026-11-05T07:14:28Z)  
**Workload Seeds (32):** `[42 100 200 300 400 500 600 700 800 900 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 1011 1012 1013 1014 1015 1016 1017 1018 1019 1020 1021 1022]`  
**Execution Time:** 100.08 seconds total  
**Null Control (MUT-0):** 0 false alarms across all 32 seeds at 0%/100% retention  

---

## 1. Executive Summary

This report documents the multi-seed empirical evaluation of crash-consistency ranking strategies across 32 deterministic workload seeds on CockroachDB's Pebble key-value store. We evaluate Durability-Contract Guided Ranking (**DCG**), State-Representative Random (**R2**), 0%-Retention-First Random (**R1**), Sync-Boundary (**B6**), Uniform Random (**B0**), and baseline heuristics across 9 distinct fault models (MUT-1 through MUT-9), supplemented by an unfaulted null control run (MUT-0).

### 1.1 Key Findings

**Evaluation metrics detailed below.**

- **Control Invariance (MUT-0):** 0 false alarms across all 32 seeds at 0%/100% retention.

---

## 2. Experimental Methodology

### 2.1 Evaluated Storage Engine & Calibrated Workloads

- **Engine:** CockroachDB Pebble `v2.1.7` running in-memory with precise crashable virtual filesystem tracking (`internal/vfsrec`).
- **Workload Architecture:** 120 client operations per seed with realistic distribution (50% Set, 20% Get, 10% Delete, 10% Two-Key Atomic Batch, 5% Range Delete, 5% Iteration).
- **Sync Rhythm:** Synchronous barriers enforced every 8th operation (`SyncEvery = 8`), background memtable flushes triggered every 40 operations (`FlushEvery = 40`).
- **Statistical Population:** 32 deterministic workload seeds (`42, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1001..1022`), generating over 11,000 candidate crash states.

### 2.2 Fault Model Taxonomy

| Fault Model | Class | Target File / Trigger | Failure Signature | Failure Density |
|---|---|---|---|---|
| **MUT-0** | Control | Unfaulted filesystem | None (0 false alarms check) | 0.0% |
| **MUT-1** | Dense | All WAL `Sync()` calls suppressed | `v1:violation:durably_committed:lost_writes` | ~33% |
| **MUT-2** | Dense/Sparse | MANIFEST `Sync()` calls suppressed | `v1:open_error:manifest_corrupt` | ~45% |
| **MUT-3** | Dense/Sparse | Parent directory syncs suppressed | `v1:violation:durably_committed:lost_files` | ~37% |
| **MUT-4** | Dense/Sparse | Memtable SST flush syncs suppressed | `v1:open_error:table_corrupt` | ~23% |
| **MUT-5** | Sparse | Strictly $k$-th WAL sync dropped ($k=1$) | `v1:violation:durably_committed:lost_writes` | ~2.5% |
| **MUT-6** | Sparse | Second `MANIFEST` sync dropped (first post-open manifest sync) | `v1:open_error:manifest_rotation` | ~0.8% |
| **MUT-7** | No-op Control | Directory sync drop after `CURRENT`/`OPTIONS` rename | None (Pebble v2 creates `CURRENT` once on Open) | 0.0% |
| **MUT-8** | Windowed | Compaction/flush SST sync drop after `CompactionBegin` (RocksDB #10460) | `v1:open_error:compaction_state` | ~15.2% |
| **MUT-9** | Dense ($k=1$) / Sparse ($k \ge 7$) | Single $k$-th directory sync drop ($k=1$ default; $k \in \{7,9,10,12\}$ sparse) | `v1:violation:durably_committed:lost_files` | ~41.4% ($k=1$) |

### 2.3 Evaluated Ranking Strategies

1. **DCG (Durability-Contract Guided / B8):** Trace-derived durability-contract analysis that prioritizes concrete durability contract violations (dangling table references, uncommitted directory renames, dropped WAL sync records) before exploring uncontracted state representatives.
2. **DCG-FlagOnly (Contract-Flag Ablation):** Partitions candidate space strictly by contract violation flags without secondary scoring heuristics. DCG-FlagOnly ties full DCG on nearly every setting, showing that the binary durability-contract flags combined with StateKey equivalence partitioning drive the speedup.
3. **RHG (Reachability-Hazard Guided / B7):** Scores cutoffs by reachable uncommitted mutations and hazard surfaces.
4. **R2 (State-Representative Random):** Partitions 0% retention candidates into equivalence classes by canonical `StateKey`, selects boundary-adjacent representatives within each `StateKey` class using graph features (`BeforeBoundary`, `SemanticTier`, `Desc`) before shuffling classes, and tests redundant cutoffs afterward.
5. **R1 (0%-Retention-First Random):** Partitions universe into 0% unsynced data retention and 100% retention; evaluates 0% retention first in pure random order.
6. **LIFO (Recent-First):** Prioritizes the most recent crash cutoffs in reverse trace order.
7. **B6 (Sync-Boundary):** Boundary-adjacent candidate prioritization.
8. **B0 (Uniform Random):** Pure random selection across candidate cutoffs.

---

## 3. Empirical Results & Statistical Summary

---

## 4. Comparative Analysis: DCG vs. R2 and Baselines

### 4.2 State Coverage ($S_{25}$ Early-Budget vs $S_{100}$ Exhaustive Coverage) and Component Attribution

In a 120-operation workload, there are ~35–40 distinct 0%-retention persistence states (`StateKey` equivalence classes). Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100.

