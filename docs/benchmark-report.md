# CraConTest: Multi-Seed Evaluation Across Fault Models

**Date:** 2026-11-19  
**Git Commit:** `6eecbf865107`  
**Evaluation Engine:** CockroachDB Pebble v2.1.7 (Go runtime: go1.27.3)  
**Workload Seeds (32):** `[42 100 200 300 400 500 600 700 800 900 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 1011 1012 1013 1014 1015 1016 1017 1018 1019 1020 1021 1022]`  
**Execution Time:** 208.26 seconds total  
**Null Control (MUT-0):** 0 observed failures across 32 evaluated seeds at 0%/100% retention  

---

## 1. Executive Summary

This report documents the empirical evaluation of crash-consistency ranking strategies across 32 deterministic workload seeds on CockroachDB's Pebble key-value store. We evaluate Durability-Contract Guided Ranking (**DCG**), State-Representative Random (**R2**), 0%-Retention-First Random (**R1**), Sync-Boundary (**B6**), Uniform Random (**B0**), and baseline heuristics across nine fault models (MUT-1 through MUT-9), supplemented by an unfaulted null control run (MUT-0).

### 1.1 Key Findings

**DCG achieves lower cost-to-first-failure than R2 on sparse faults.**

Observed comparison:
- **MUT-5 (Sparse $k$-th WAL Sync Drop):** DCG median $k_1 = 1.0$ (mean $1.00$, 95% CI $[1.00, 1.00]$), R2 median $k_1 = 13.0$ (mean $13.04$, 95% CI $[12.71, 13.37]$), and B0 median $k_1 = 23.5$ (mean $32.53$). The ratios of median discovery positions are 13.0 for R2/DCG and 23.5 for B0/DCG; these are candidate-count ratios, not wall-clock speedups.
- **Null-control observation (MUT-0):** 0 failures across 32 evaluated seeds at 0%/100% retention.
- **MUT-8 (Compaction-Triggered SST Sync Drop):** DCG reaches the first failure at median $k_1 = 1.0$ (Mean 1.00) vs R2 median $k_1 = 3.4$ (Mean 3.42) and B0 median $k_1 = 4.0$ (Mean 6.81), discovers $S_{25} = 12.88$ mean distinct failing states at $B=25$ (vs R2 6.73 and B0 2.28), and matches R2 at full budget ($S_{100}$ Mean 12.88 vs R2 12.80 and B0 5.06).

---

## 2. Experimental Methodology

### 2.1 Evaluated Storage Engine & Calibrated Workloads

- **Engine:** CockroachDB Pebble `v2.1.7` running in-memory with precise crashable virtual filesystem tracking (`internal/vfsrec`).
- **Workload Architecture:** 120 client operations per seed with realistic distribution (50% Set, 20% Get, 10% Delete, 10% Two-Key Atomic Batch, 5% Range Delete, 5% Iteration).
- **Sync Rhythm:** Synchronous barriers enforced every 8th operation (`SyncEvery = 8`), background memtable flushes triggered every 40 operations (`FlushEvery = 40`).
- **Evaluated Seed Set:** 32 deterministic workload seeds, generating over 11,000 candidate crash states in the full benchmark.

### 2.2 Fault Model Taxonomy

| Fault Model | Class | Target File / Trigger | Failure Signature | Failure Density |
|---|---|---|---|---|
| **MUT-0** | Control | Unfaulted filesystem | No failure observed in the evaluated null control | 0.0% |
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
2. **DCG-FlagOnly (Contract-Flag Ablation):** Partitions candidate space strictly by contract violation flags without secondary scoring heuristics. Its near-ties with full DCG attribute most of the observed rank-position difference to binary contract flags and StateKey equivalence partitioning.
3. **RHG (Reachability-Hazard Guided / B7):** Scores cutoffs by reachable uncommitted mutations and hazard surfaces.
4. **R2 (State-Representative Random):** Partitions 0% retention candidates into equivalence classes by canonical `StateKey`, selects boundary-adjacent representatives within each `StateKey` class using graph features (`BeforeBoundary`, `BarrierProximity`, and canonical order `candidate.Less`) before shuffling classes, and tests redundant cutoffs afterward.
5. **R1 (0%-Retention-First Random):** Partitions universe into 0% unsynced data retention and 100% retention; evaluates 0% retention first in pure random order.
6. **LIFO (Recent-First):** Prioritizes the most recent crash cutoffs in reverse trace order.
7. **B6 (Sync-Boundary):** Boundary-adjacent candidate prioritization.
8. **B0 (Uniform Random):** Pure random selection across candidate cutoffs.

---

## 3. Empirical Results & Statistical Summary

### 3.1 MUT-1 Evaluation (32 Seeds: Avg 311 Candidates, Density: 36.4%)

- **Average Failures per Run:** 113.4 (Violations: 113.4, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.73

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 23.94 | 24.0 | 23.97 | 8.0 | 0.7837 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 23.94 | 24.0 | 23.97 | 8.0 | 0.7837 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 23.94 | 24.0 | 23.97 | 16.1 | 0.7490 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 18.0 | 17.69 | 24.0 | 23.97 | 8.0 | 0.7286 |
| **R2 (Representative)** | 1.4 [1.4, 1.5] | 1.42 [1.40, 1.45] | 1.3 | 1.5 | 17.1 | 17.14 | 24.0 | 23.97 | 23.5 | 0.7440 |
| R1 (0%-Retention) | 1.4 [1.3, 1.4] | 1.38 [1.35, 1.41] | 1.3 | 1.5 | 10.0 | 10.03 | 19.8 | 19.88 | 93.7 | 0.7472 |
| LIFO (Recent-First) | 5.0 [5.0, 5.0] | 5.00 [5.00, 5.00] | 5.0 | 5.0 | 7.0 | 6.53 | 8.5 | 8.94 | 98.0 | 0.5694 |
| B6 (Sync-Boundary) | 17.0 [17.0, 17.0] | 17.00 [17.00, 17.00] | 17.0 | 17.0 | 5.0 | 5.00 | 13.0 | 13.00 | 31.0 | 0.4300 |
| B0 (Uniform Random) | 1.5 [1.0, 3.0] | 2.41 [1.64, 3.17] | 1.0 | 4.9 | 6.0 | 5.66 | 15.0 | 14.59 | 189.2 | 0.5003 |

### 3.2 MUT-2 Evaluation (32 Seeds: Avg 333 Candidates, Density: 47.3%)

- **Average Failures per Run:** 157.6 (Violations: 0.0, Open Errors: 157.6)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.11

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 33.0 | 33.00 | 1.0 | 0.7451 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 33.0 | 33.00 | 1.0 | 0.7451 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 33.0 | 33.00 | 1.0 | 0.7410 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 24.00 | 33.0 | 33.00 | 1.0 | 0.7404 |
| **R2 (Representative)** | 1.2 [1.1, 1.2] | 1.17 [1.15, 1.19] | 1.1 | 1.2 | 21.1 | 21.12 | 33.0 | 33.00 | 1.2 | 0.7392 |
| R1 (0%-Retention) | 1.1 [1.0, 1.1] | 1.05 [1.04, 1.06] | 1.0 | 1.1 | 8.6 | 8.68 | 22.8 | 22.88 | 1.1 | 0.7453 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 7.0 | 7.00 | 11.0 | 11.34 | 1.0 | 0.5175 |
| B6 (Sync-Boundary) | 5.0 [5.0, 5.0] | 5.00 [5.00, 5.00] | 5.0 | 5.0 | 11.0 | 11.00 | 30.0 | 30.00 | 5.0 | 0.4818 |
| B0 (Uniform Random) | 1.5 [1.0, 2.0] | 2.03 [1.52, 2.55] | 1.0 | 3.0 | 5.5 | 5.50 | 14.0 | 13.80 | 2.0 | 0.5001 |

### 3.3 MUT-3 Evaluation (32 Seeds: Avg 307 Candidates, Density: 42.5%)

- **Average Failures per Run:** 130.6 (Violations: 130.6, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.34

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 24.00 | 27.0 | 27.00 | 17.0 | 0.7487 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 24.00 | 27.0 | 27.00 | 17.0 | 0.7487 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.1] | 1.04 [1.04, 1.05] | 1.0 | 1.1 | 24.1 | 24.10 | 27.0 | 27.00 | 4.9 | 0.7416 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 21.0 | 21.03 | 27.0 | 27.00 | 11.0 | 0.7367 |
| **R2 (Representative)** | 1.4 [1.4, 1.5] | 1.42 [1.40, 1.45] | 1.3 | 1.5 | 17.2 | 17.24 | 27.0 | 27.00 | 7.3 | 0.7361 |
| R1 (0%-Retention) | 1.2 [1.2, 1.2] | 1.17 [1.16, 1.19] | 1.1 | 1.2 | 7.2 | 7.19 | 18.9 | 18.94 | 5.3 | 0.7475 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 7.0 | 7.16 | 12.0 | 12.06 | 202.0 | 0.5579 |
| B6 (Sync-Boundary) | 9.0 [9.0, 9.0] | 9.00 [9.00, 9.00] | 9.0 | 9.0 | 9.0 | 9.00 | 21.0 | 21.00 | 15.0 | 0.4472 |
| B0 (Uniform Random) | 1.0 [1.0, 2.0] | 2.06 [1.23, 2.89] | 1.0 | 4.8 | 4.0 | 4.31 | 11.0 | 11.20 | 7.8 | 0.5005 |

### 3.4 MUT-4 Evaluation (32 Seeds: Avg 335 Candidates, Density: 29.8%)

- **Average Failures per Run:** 99.9 (Violations: 0.0, Open Errors: 99.9)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 3.34

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 22.0 | 21.69 | 22.0 | 21.69 | 1.0 | 0.7989 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 22.0 | 21.69 | 22.0 | 21.69 | 1.0 | 0.7989 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 22.0 | 21.69 | 22.0 | 21.69 | 1.0 | 0.7451 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 17.0 | 16.69 | 22.0 | 21.69 | 1.0 | 0.7020 |
| **R2 (Representative)** | 1.9 [1.9, 2.0] | 1.93 [1.88, 1.98] | 1.8 | 2.1 | 12.6 | 12.60 | 22.0 | 21.69 | 1.9 | 0.7358 |
| R1 (0%-Retention) | 1.7 [1.6, 1.7] | 1.68 [1.64, 1.71] | 1.6 | 1.8 | 5.4 | 5.39 | 14.7 | 14.60 | 1.7 | 0.7454 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 8.0 | 8.00 | 12.0 | 12.25 | 1.0 | 0.6980 |
| B6 (Sync-Boundary) | 31.0 [31.0, 32.0] | 31.62 [31.24, 32.01] | 31.0 | 33.0 | 0.0 | 0.00 | 17.0 | 16.69 | 31.0 | 0.3513 |
| B0 (Uniform Random) | 2.0 [1.0, 3.5] | 3.22 [2.15, 4.29] | 1.0 | 7.9 | 4.0 | 4.06 | 9.0 | 8.59 | 2.8 | 0.5004 |

### 3.5 MUT-5 Evaluation (32 Seeds: Avg 339 Candidates, Density: 2.7%)

- **Average Failures per Run:** 9.0 (Violations: 9.0, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 34.02

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 2.00 | 2.0 | 2.00 | 1.0 | 0.9128 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 2.00 | 2.0 | 2.00 | 1.0 | 0.9128 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 2.00 | 2.0 | 2.00 | 1.0 | 0.7571 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 1.81 | 2.0 | 2.00 | 1.0 | 0.8207 |
| **R2 (Representative)** | 13.0 [12.8, 13.2] | 13.04 [12.71, 13.37] | 11.9 | 14.4 | 1.3 | 1.31 | 2.0 | 2.00 | 13.0 | 0.7447 |
| R1 (0%-Retention) | 17.0 [16.4, 17.6] | 16.88 [16.39, 17.36] | 15.2 | 18.1 | 0.9 | 0.89 | 1.6 | 1.58 | 17.0 | 0.7470 |
| LIFO (Recent-First) | 264.0 [259.0, 267.0] | 262.25 [258.20, 266.30] | 243.6 | 275.0 | 0.0 | 0.00 | 0.0 | 0.00 | 264.0 | 0.2049 |
| B6 (Sync-Boundary) | 17.0 [17.0, 17.0] | 17.00 [17.00, 17.00] | 17.0 | 17.0 | 1.0 | 1.00 | 1.0 | 1.00 | 17.0 | 0.6664 |
| B0 (Uniform Random) | 23.5 [17.0, 32.5] | 32.53 [22.10, 42.96] | 4.5 | 85.5 | 1.0 | 0.83 | 1.0 | 1.00 | 25.0 | 0.4963 |

### 3.6 MUT-6 Evaluation (32 Seeds: Avg 339 Candidates, Density: 0.8%)

- **Average Failures per Run:** 2.8 (Violations: 2.8, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 17.07

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 0.0 | 1.62 | 0.0 | 1.62 | 3.0 | 0.3851 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 0.0 | 1.62 | 0.0 | 1.62 | 3.0 | 0.3851 |
| **DCG-FlagOnly** | 1.2 [1.2, 1.2] | 1.19 [1.16, 1.21] | 1.1 | 1.2 | 0.0 | 1.62 | 0.0 | 1.62 | 2.8 | 0.3502 |
| **RHG (Risk-Horizon)** | 14.0 [14.0, 15.0] | 14.08 [13.50, 14.65] | 13.0 | 15.0 | 0.0 | 1.22 | 0.0 | 1.62 | 19.0 | 0.3429 |
| **R2 (Representative)** | 7.7 [7.2, 7.9] | 7.59 [7.31, 7.87] | 7.0 | 8.1 | 0.0 | 1.09 | 0.0 | 1.62 | 17.7 | 0.3376 |
| R1 (0%-Retention) | 20.1 [19.4, 21.9] | 20.72 [19.34, 22.10] | 18.4 | 24.2 | 0.0 | 0.38 | 0.0 | 1.21 | 53.7 | 0.3025 |
| LIFO (Recent-First) | 103.0 [97.0, 103.0] | 101.00 [97.37, 104.63] | 95.0 | 108.2 | 0.0 | 0.00 | 0.0 | 0.41 | 107.0 | 0.2647 |
| B6 (Sync-Boundary) | 43.0 [43.0, 43.0] | 43.15 [42.38, 43.93] | 41.4 | 45.0 | 0.0 | 0.00 | 0.0 | 1.22 | 47.0 | 0.2013 |
| B0 (Uniform Random) | 26.0 [15.0, 48.0] | 36.38 [18.16, 54.61] | 7.0 | 68.4 | 0.0 | 0.22 | 0.0 | 0.81 | 97.5 | 0.2047 |

### 3.7 MUT-7 Evaluation (32 Seeds: Avg 335 Candidates, Density: 0.0%)

- **Average Failures per Run:** 0.0 (Violations: 0.0, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 0.00

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| **DCG-PR (Partial Prior)** | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| **DCG-FlagOnly** | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| **RHG (Risk-Horizon)** | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| **R2 (Representative)** | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| R1 (0%-Retention) | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| LIFO (Recent-First) | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| B6 (Sync-Boundary) | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
| B0 (Uniform Random) | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |

### 3.8 MUT-8 Evaluation (32 Seeds: Avg 357 Candidates, Density: 15.2%)

- **Average Failures per Run:** 54.2 (Violations: 0.0, Open Errors: 54.2)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 6.52

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 13.0 | 12.88 | 13.0 | 12.88 | 1.0 | 0.8466 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 13.0 | 12.88 | 13.0 | 12.88 | 1.0 | 0.8466 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 13.0 | 12.88 | 13.0 | 12.88 | 1.0 | 0.7539 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 8.0 | 7.88 | 13.0 | 12.88 | 1.0 | 0.6740 |
| **R2 (Representative)** | 3.4 [3.3, 3.5] | 3.42 [3.34, 3.51] | 3.1 | 3.7 | 6.7 | 6.73 | 12.9 | 12.80 | 3.4 | 0.7388 |
| R1 (0%-Retention) | 3.3 [3.2, 3.5] | 3.32 [3.19, 3.45] | 2.9 | 3.8 | 3.3 | 3.30 | 8.4 | 8.45 | 3.3 | 0.7454 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 8.0 | 8.00 | 12.0 | 11.75 | 1.0 | 0.8447 |
| B6 (Sync-Boundary) | 55.0 [55.0, 55.0] | 55.25 [54.85, 55.65] | 55.0 | 57.0 | 0.0 | 0.00 | 10.0 | 9.88 | 55.0 | 0.3240 |
| B0 (Uniform Random) | 4.0 [2.5, 6.0] | 6.81 [3.80, 9.83] | 1.0 | 15.0 | 2.0 | 2.28 | 5.0 | 5.06 | 5.0 | 0.4995 |

### 3.9 MUT-9 Evaluation (32 Seeds: Avg 337 Candidates, Density: 41.4%)

- **Average Failures per Run:** 139.6 (Violations: 139.6, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.41

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 27.0 | 27.00 | 27.0 | 0.7658 |
| **DCG-PR (Partial Prior)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 27.0 | 27.00 | 27.0 | 0.7658 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 27.0 | 27.00 | 15.7 | 0.7411 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 20.0 | 20.00 | 27.0 | 27.00 | 36.0 | 0.7276 |
| **R2 (Representative)** | 1.4 [1.3, 1.4] | 1.38 [1.35, 1.40] | 1.3 | 1.5 | 17.8 | 17.75 | 27.0 | 27.00 | 21.5 | 0.7372 |
| R1 (0%-Retention) | 1.2 [1.2, 1.2] | 1.20 [1.19, 1.22] | 1.1 | 1.3 | 5.6 | 5.61 | 17.4 | 17.49 | 44.8 | 0.7452 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 7.0 | 7.00 | 11.0 | 10.94 | 11.0 | 0.5621 |
| B6 (Sync-Boundary) | 17.0 [17.0, 17.0] | 17.00 [17.00, 17.00] | 17.0 | 17.0 | 5.0 | 5.00 | 26.0 | 26.00 | 111.0 | 0.4367 |
| B0 (Uniform Random) | 2.0 [2.0, 3.0] | 2.50 [1.95, 3.05] | 1.0 | 5.0 | 3.0 | 3.44 | 10.0 | 9.84 | 73.8 | 0.5001 |

---

## 4. Comparative Analysis: DCG vs. R2 and Baselines

### 4.1 Cost-to-First-Failure ($k_1$) on Sparse Faults

On sparse real-shaped crash bugs such as **MUT-5** (where only a single synchronous write barrier is dropped):

1. **R2 Pathology:** R2 partitions candidates into equivalence classes by persisted state and draws representative candidates uniformly at random. In a trace with ~40 distinct equivalence classes, the single class exposing the dropped sync has only a $1/40$ chance of being selected on trial 1. As proven by our 32-seed evaluation, R2 exhibits a median $k_1 = 13.0$ (mean $13.04$), with 10% of trials requiring $k_1 > 14.4$.
2. **DCG Mechanism:** DCG inspects the filesystem execution graph and detects the missing edge between client operation acknowledgment and durable write persistence (`AckSyncMissing = true`). It immediately elevates candidates at the unpersisted barrier to Tier 1, achieving a **median $k_1 = 1.0$ (mean $1.00$)** with zero variance ($P_{10} = 1.0, P_{90} = 1.0$).

### 4.2 State Coverage ($S_{25}$ Early-Budget vs $S_{100}$ Exhaustive Coverage) and Component Attribution

In a 120-operation workload, there are ~35–40 distinct 0%-retention persistence states (`StateKey` equivalence classes). Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100.

Under early testing budgets ($B = 25$, $S_{25}$), DCG discovers substantially more failing states than R2 and B0: on MUT-9 ($k=1$), DCG achieves mean $S_{25} = 25.00$ vs R2's 17.75 and B0's 3.44; on MUT-8, DCG achieves mean $S_{25} = 12.88$ vs R2's 6.73 and B0's 2.28. Comparing DCG against the **DCG-FlagOnly** ablation isolates the contribution of each mechanism:

1. **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and 12.88 on MUT-8).
2. **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop), where full DCG achieves mean $k_1 = 1.00$ compared to 1.19 for `DCG-FlagOnly`.
3. **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

### 4.3 MUT-8 Evaluation (Compaction-Triggered SST Sync Drop)

Under MUT-8 (compaction-triggered SST sync drop, 15.2% failure density), DCG reaches the first failing state at median $k_1 = 1.0$ (mean 1.00) compared to R2's median $k_1 = 3.4$ (mean 3.42) and B0's median $k_1 = 4.0$ (mean 6.81). At early budget $B = 25$, DCG discovers mean $S_{25} = 12.88$ distinct failing states versus 6.73 for R2 and 2.28 for B0. At full budget $B = 100$, DCG and R2 both exhaust the reachable failing equivalence classes ($S_{100}$ mean 12.88 for DCG vs 12.80 for R2, compared to 5.06 for B0).

