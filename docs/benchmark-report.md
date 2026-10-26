# CraConTest: Multi-Seed Evaluation Across Fault Models

**Date:** 2026-10-27  
**Evaluation Engine:** CockroachDB Pebble v2.1.7 (Go runtime: 2026-10-27T06:15:12Z)  
**Workload Seeds (32):** `[42 100 200 300 400 500 600 700 800 900 1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 1011 1012 1013 1014 1015 1016 1017 1018 1019 1020 1021 1022]`  
**Execution Time:** 127.90 seconds total  
**Null Control (MUT-0):** 0 false alarms across all 32 seeds (100% oracle accuracy)  

---

## 1. Executive Summary

This report documents the multi-seed empirical evaluation of crash-consistency ranking strategies across 32 deterministic workload seeds on CockroachDB's Pebble key-value store. We evaluate Durability-Contract Guided Ranking (**DCG**), State-Representative Random (**R2**), 0%-Retention-First Random (**R1**), Sync-Boundary (**B6**), Uniform Random (**B0**), and baseline heuristics across 9 distinct fault models (MUT-1 through MUT-9), supplemented by an unfaulted null control run (MUT-0).

### 1.1 Key Findings

**DCG achieves lower cost-to-first-failure than R2 on sparse faults.**

Empirical Confirmation:
- **MUT-5 (Sparse $k$-th WAL Sync Drop):** DCG median $k_1 = 1.0$ (Mean $1.00$, 95% CI $[1.00, 1.00]$) vs R2 median $k_1 = 13.1$ (Mean $13.01$, 95% CI $[12.68, 13.34]$) vs B0 median $k_1 = 25.5$ (Mean $25.97$). DCG achieves **$13.1x$ faster fault discovery** than R2 and **$25.5x$ faster** than B0.
- **Control Invariance (MUT-0):** 0 false alarms across all 32 workload seeds (100.0% oracle precision).
- **MUT-8 (Compaction-Triggered SST Sync Drop):** DCG reaches the first failure at median $k_1 = 1.0$ (Mean 1.00) vs R2 median $k_1 = 3.6$ (Mean 3.58) and B0 median $k_1 = 4.0$ (Mean 4.41), discovers $S_{25} = 12.88$ mean distinct failing states at $B=25$ (vs R2 6.74 and B0 2.00), and matches R2 at full budget ($S_{100}$ Mean 12.88 vs R2 12.82 and B0 5.08).

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

1. **DCG (Durability-Contract Guided / B8):** Prioritizes concrete durability contract violations (dangling table references, uncommitted directory renames, dropped WAL sync records) before exploring uncontracted state representatives.
2. **DCG-FlagOnly (Contract-Flag Ablation):** Partitions candidate space strictly by contract violation flags without secondary scoring heuristics.
3. **RHG (Reachability-Hazard Guided / B7):** Scores cutoffs by reachable uncommitted mutations and hazard surfaces.
4. **R2 (State-Representative R1):** Partitions 0% retention candidates into equivalence classes by canonical `StateKey`, selects one representative candidate per distinct persistence state, and shuffles representatives before testing redundant cutoffs.
5. **R1 (0%-Retention-First Random):** Partitions universe into 0% unsynced data retention and 100% retention; evaluates 0% retention first in pure random order.
6. **LIFO (Recent-First):** Prioritizes the most recent crash cutoffs in reverse trace order.
7. **B6 (Sync-Boundary):** Boundary-adjacent candidate prioritization.
8. **B0 (Uniform Random):** Pure random selection across candidate cutoffs.

---

## 3. Empirical Results & Statistical Summary

### 3.1 MUT-1 Evaluation (32 Seeds: Avg 311 Candidates, Density: 36.4%)

- **Average Failures per Run:** 113.5 (Violations: 113.5, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.73

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 23.97 | 24.0 | 24.00 | 8.0 | 0.7837 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 23.97 | 24.0 | 24.00 | 16.1 | 0.7492 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 18.0 | 17.69 | 24.0 | 24.00 | 8.0 | 0.7286 |
| **R2 (Representative)** | 1.4 [1.4, 1.5] | 1.44 [1.42, 1.46] | 1.4 | 1.5 | 17.1 | 17.13 | 24.0 | 24.00 | 23.4 | 0.7441 |
| R1 (0%-Retention) | 1.4 [1.3, 1.4] | 1.37 [1.34, 1.40] | 1.3 | 1.5 | 10.0 | 10.04 | 19.7 | 19.83 | 94.1 | 0.7471 |
| LIFO (Recent-First) | 5.0 [5.0, 5.0] | 5.00 [5.00, 5.00] | 5.0 | 5.0 | 7.0 | 6.53 | 8.5 | 8.94 | 98.0 | 0.5693 |
| B6 (Sync-Boundary) | 17.0 [17.0, 17.0] | 17.00 [17.00, 17.00] | 17.0 | 17.0 | 5.0 | 5.00 | 13.0 | 13.00 | 31.0 | 0.4300 |
| B0 (Uniform Random) | 2.0 [2.0, 2.0] | 2.00 [2.00, 2.00] | 2.0 | 2.0 | 6.0 | 6.12 | 15.0 | 14.61 | 188.2 | 0.5008 |

### 3.2 MUT-2 Evaluation (32 Seeds: Avg 333 Candidates, Density: 47.3%)

- **Average Failures per Run:** 157.7 (Violations: 0.0, Open Errors: 157.7)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.11

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 33.0 | 33.03 | 1.0 | 0.7451 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 33.0 | 33.03 | 1.0 | 0.7410 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 24.00 | 33.0 | 33.03 | 1.0 | 0.7404 |
| **R2 (Representative)** | 1.2 [1.1, 1.2] | 1.17 [1.16, 1.19] | 1.1 | 1.2 | 21.2 | 21.15 | 33.0 | 33.03 | 1.2 | 0.7392 |
| R1 (0%-Retention) | 1.0 [1.0, 1.1] | 1.04 [1.04, 1.05] | 1.0 | 1.1 | 8.7 | 8.68 | 22.8 | 22.81 | 1.0 | 0.7454 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 7.0 | 7.03 | 11.0 | 11.38 | 1.0 | 0.5174 |
| B6 (Sync-Boundary) | 5.0 [5.0, 5.0] | 5.00 [5.00, 5.00] | 5.0 | 5.0 | 11.0 | 11.00 | 30.0 | 30.00 | 5.0 | 0.4818 |
| B0 (Uniform Random) | 1.8 [1.2, 2.0] | 1.58 [1.41, 1.74] | 1.0 | 2.0 | 6.0 | 5.64 | 14.0 | 13.75 | 1.8 | 0.4988 |

### 3.3 MUT-3 Evaluation (32 Seeds: Avg 307 Candidates, Density: 42.5%)

- **Average Failures per Run:** 130.6 (Violations: 130.6, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.34

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 24.0 | 24.00 | 27.0 | 27.00 | 17.0 | 0.7487 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.03 [1.03, 1.04] | 1.0 | 1.1 | 24.1 | 24.12 | 27.0 | 27.00 | 5.0 | 0.7416 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 21.0 | 21.03 | 27.0 | 27.00 | 11.0 | 0.7367 |
| **R2 (Representative)** | 1.4 [1.4, 1.5] | 1.44 [1.40, 1.48] | 1.3 | 1.6 | 17.2 | 17.26 | 27.0 | 27.00 | 7.5 | 0.7361 |
| R1 (0%-Retention) | 1.2 [1.1, 1.2] | 1.17 [1.15, 1.19] | 1.1 | 1.2 | 7.3 | 7.27 | 18.9 | 19.00 | 5.1 | 0.7476 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 7.0 | 7.16 | 12.0 | 12.06 | 202.0 | 0.5579 |
| B6 (Sync-Boundary) | 9.0 [9.0, 9.0] | 9.00 [9.00, 9.00] | 9.0 | 9.0 | 9.0 | 9.00 | 21.0 | 21.00 | 15.0 | 0.4472 |
| B0 (Uniform Random) | 2.0 [2.0, 2.0] | 2.00 [2.00, 2.00] | 2.0 | 2.0 | 5.0 | 5.00 | 11.0 | 11.22 | 8.0 | 0.5000 |

### 3.4 MUT-4 Evaluation (32 Seeds: Avg 335 Candidates, Density: 29.8%)

- **Average Failures per Run:** 99.9 (Violations: 0.0, Open Errors: 99.9)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 3.34

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 22.0 | 21.69 | 22.0 | 21.69 | 1.0 | 0.7989 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 22.0 | 21.69 | 22.0 | 21.69 | 1.0 | 0.7452 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 17.0 | 16.69 | 22.0 | 21.69 | 1.0 | 0.7020 |
| **R2 (Representative)** | 1.9 [1.9, 2.0] | 1.95 [1.90, 2.01] | 1.8 | 2.1 | 12.5 | 12.57 | 22.0 | 21.69 | 1.9 | 0.7360 |
| R1 (0%-Retention) | 1.7 [1.6, 1.7] | 1.67 [1.61, 1.74] | 1.5 | 1.9 | 5.5 | 5.44 | 14.6 | 14.51 | 1.7 | 0.7450 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 8.0 | 8.00 | 12.0 | 12.25 | 1.0 | 0.6980 |
| B6 (Sync-Boundary) | 31.0 [31.0, 32.0] | 31.62 [31.24, 32.01] | 31.0 | 33.0 | 0.0 | 0.00 | 17.0 | 16.69 | 31.0 | 0.3513 |
| B0 (Uniform Random) | 2.0 [2.0, 2.8] | 2.39 [2.22, 2.56] | 2.0 | 3.0 | 3.5 | 3.53 | 9.0 | 8.59 | 2.0 | 0.4987 |

### 3.5 MUT-5 Evaluation (32 Seeds: Avg 339 Candidates, Density: 2.7%)

- **Average Failures per Run:** 9.0 (Violations: 9.0, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 34.02

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 2.00 | 2.0 | 2.00 | 1.0 | 0.9128 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 2.00 | 2.0 | 2.00 | 1.0 | 0.7565 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 2.0 | 1.81 | 2.0 | 2.00 | 1.0 | 0.8207 |
| **R2 (Representative)** | 13.1 [12.6, 13.4] | 13.01 [12.68, 13.34] | 11.9 | 14.0 | 1.3 | 1.30 | 2.0 | 2.00 | 13.1 | 0.7438 |
| R1 (0%-Retention) | 16.9 [16.3, 17.7] | 17.04 [16.54, 17.53] | 15.4 | 18.9 | 0.9 | 0.87 | 1.6 | 1.60 | 16.9 | 0.7455 |
| LIFO (Recent-First) | 264.0 [259.0, 267.0] | 262.25 [258.20, 266.30] | 243.6 | 275.0 | 0.0 | 0.00 | 0.0 | 0.00 | 264.0 | 0.2049 |
| B6 (Sync-Boundary) | 17.0 [17.0, 17.0] | 17.00 [17.00, 17.00] | 17.0 | 17.0 | 1.0 | 1.00 | 1.0 | 1.00 | 17.0 | 0.6664 |
| B0 (Uniform Random) | 25.5 [24.5, 26.0] | 25.97 [25.00, 26.94] | 22.6 | 29.5 | 0.5 | 0.48 | 1.0 | 1.00 | 25.5 | 0.4965 |

### 3.6 MUT-6 Evaluation (32 Seeds: Avg 339 Candidates, Density: 0.8%)

- **Average Failures per Run:** 2.8 (Violations: 2.8, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 17.07

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 0.0 | 1.62 | 0.0 | 1.62 | 3.0 | 0.3851 |
| **DCG-FlagOnly** | 1.2 [1.2, 1.2] | 1.20 [1.17, 1.24] | 1.1 | 1.3 | 0.0 | 1.62 | 0.0 | 1.62 | 2.8 | 0.3502 |
| **RHG (Risk-Horizon)** | 14.0 [14.0, 15.0] | 14.08 [13.50, 14.65] | 13.0 | 15.0 | 0.0 | 1.22 | 0.0 | 1.62 | 19.0 | 0.3429 |
| **R2 (Representative)** | 7.9 [7.5, 8.0] | 7.72 [7.46, 7.97] | 7.2 | 8.1 | 0.0 | 1.07 | 0.0 | 1.62 | 18.0 | 0.3382 |
| R1 (0%-Retention) | 21.0 [20.7, 21.2] | 20.79 [19.89, 21.70] | 18.4 | 22.7 | 0.0 | 0.39 | 0.0 | 1.20 | 54.8 | 0.3023 |
| LIFO (Recent-First) | 103.0 [97.0, 103.0] | 101.00 [97.37, 104.63] | 95.0 | 108.2 | 0.0 | 0.00 | 0.0 | 0.41 | 107.0 | 0.2647 |
| B6 (Sync-Boundary) | 43.0 [43.0, 43.0] | 43.15 [42.38, 43.93] | 41.4 | 45.0 | 0.0 | 0.00 | 0.0 | 1.22 | 47.0 | 0.2013 |
| B0 (Uniform Random) | 30.0 [28.5, 33.5] | 31.12 [29.02, 33.21] | 27.7 | 36.1 | 0.0 | 0.00 | 0.0 | 0.81 | 101.5 | 0.2033 |

### 3.7 MUT-7 Evaluation (32 Seeds: Avg 339 Candidates, Density: 0.0%)

- **Average Failures per Run:** 0.0 (Violations: 0.0, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 0.00

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 0.0 [0.0, 0.0] | 0.00 [0.00, 0.00] | 0.0 | 0.0 | 0.0 | 0.00 | 0.0 | 0.00 | 0.0 | 0.0000 |
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
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 13.0 | 12.88 | 13.0 | 12.88 | 1.0 | 0.7538 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 8.0 | 7.88 | 13.0 | 12.88 | 1.0 | 0.6739 |
| **R2 (Representative)** | 3.6 [3.5, 3.7] | 3.58 [3.50, 3.66] | 3.3 | 3.8 | 6.8 | 6.74 | 12.9 | 12.82 | 3.6 | 0.7390 |
| R1 (0%-Retention) | 3.3 [3.1, 3.4] | 3.28 [3.15, 3.41] | 2.8 | 3.8 | 3.2 | 3.27 | 8.4 | 8.41 | 3.3 | 0.7454 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 8.0 | 8.00 | 12.0 | 11.75 | 1.0 | 0.8448 |
| B6 (Sync-Boundary) | 55.0 [55.0, 55.0] | 55.25 [54.85, 55.65] | 55.0 | 57.0 | 0.0 | 0.00 | 10.0 | 9.88 | 55.0 | 0.3240 |
| B0 (Uniform Random) | 4.0 [4.0, 5.0] | 4.41 [4.23, 4.58] | 4.0 | 5.0 | 2.0 | 2.00 | 5.0 | 5.08 | 4.0 | 0.5024 |

### 3.9 MUT-9 Evaluation (32 Seeds: Avg 337 Candidates, Density: 41.4%)

- **Average Failures per Run:** 139.6 (Violations: 139.6, Open Errors: 0.0)
- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$:** 2.41

| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\text{full}}$ Median | $\text{AUC}_{\text{norm}}$ |
|---|---|---|---|---|---|---|---|---|---|---|
| **DCG (Guided)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 27.0 | 27.00 | 27.0 | 0.7658 |
| **DCG-FlagOnly** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 25.0 | 25.00 | 27.0 | 27.00 | 15.8 | 0.7411 |
| **RHG (Risk-Horizon)** | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 20.0 | 20.00 | 27.0 | 27.00 | 36.0 | 0.7276 |
| **R2 (Representative)** | 1.4 [1.4, 1.4] | 1.40 [1.36, 1.44] | 1.3 | 1.5 | 17.8 | 17.78 | 27.0 | 27.00 | 22.0 | 0.7373 |
| R1 (0%-Retention) | 1.2 [1.2, 1.2] | 1.21 [1.18, 1.23] | 1.1 | 1.3 | 5.5 | 5.57 | 17.4 | 17.44 | 46.6 | 0.7451 |
| LIFO (Recent-First) | 1.0 [1.0, 1.0] | 1.00 [1.00, 1.00] | 1.0 | 1.0 | 7.0 | 7.00 | 11.0 | 10.94 | 11.0 | 0.5621 |
| B6 (Sync-Boundary) | 17.0 [17.0, 17.0] | 17.00 [17.00, 17.00] | 17.0 | 17.0 | 5.0 | 5.00 | 26.0 | 26.00 | 111.0 | 0.4367 |
| B0 (Uniform Random) | 2.0 [2.0, 2.0] | 1.95 [1.90, 2.01] | 2.0 | 2.0 | 3.0 | 3.03 | 10.0 | 9.83 | 75.2 | 0.4990 |

---

## 4. Comparative Analysis: DCG vs. R2 and Baselines

### 4.1 Cost-to-First-Failure ($k_1$) on Sparse Faults

On sparse real-shaped crash bugs such as **MUT-5** (where only a single synchronous write barrier is dropped):

1. **R2 Pathology:** R2 partitions candidates into equivalence classes by persisted state and draws representative candidates uniformly at random. In a trace with ~40 distinct equivalence classes, the single class exposing the dropped sync has only a $1/40$ chance of being selected on trial 1. As proven by our 32-seed evaluation, R2 exhibits a median $k_1 = 13.1$ (mean $13.01$), with 10% of trials requiring $k_1 > 14.0$.
2. **DCG Mechanism:** DCG inspects the filesystem execution graph and detects the missing edge between client operation acknowledgment and durable write persistence (`AckSyncMissing = true`). It immediately elevates candidates at the unpersisted barrier to Tier 1, achieving a **median $k_1 = 1.0$ (mean $1.00$)** with zero variance ($P_{10} = 1.0, P_{90} = 1.0$).

### 4.2 State Coverage ($S_{25}$ Early-Budget vs $S_{100}$ Exhaustive Coverage) and Component Attribution

In a 120-operation workload, there are ~35–40 distinct 0%-retention persistence states (`StateKey` equivalence classes). Under an exhaustive testing budget ($B = 100$, $S_{100}$), both DCG and R2 discover all reachable failing equivalence classes and tie because both strategies exhaust distinct state representatives before step 100.

Under early testing budgets ($B = 25$, $S_{25}$), DCG discovers substantially more failing states than R2 and B0: on MUT-9 ($k=1$), DCG achieves mean $S_{25} = 25.00$ vs R2's 17.78 and B0's 3.03; on MUT-8, DCG achieves mean $S_{25} = 12.88$ vs R2's 6.74 and B0's 2.00. Comparing DCG against the **DCG-FlagOnly** ablation isolates the contribution of each mechanism:

1. **Binary Durability-Contract Flags + `StateKey` Equivalence Partitioning:** Drive the $k_1 = 1.00$ first-failure discovery on MUT-5, MUT-8, and MUT-9 as well as the early-budget $S_{25}$ lead (`DCG-FlagOnly` matches full `DCG` with mean $S_{25} = 25.00$ on MUT-9 and 12.88 on MUT-8).
2. **Tier 1B Continuous Severity Scoring (`Score()`):** Breaks ties when multiple file classes or sync points are flagged within a trace—most visibly on **MUT-6** (second manifest sync drop), where full DCG achieves mean $k_1 = 1.00$ compared to 1.20 for `DCG-FlagOnly`.
3. **Tier 3A Regime/Epoch Diversity Partitioning:** Operates in Pass 2 after unique `StateKey` representatives are exhausted, increasing full-budget failure-curve area (`AUCNormFailures`) rather than altering $k_1$ or $S_{25}$.

### 4.3 MUT-8 Evaluation (Compaction-Triggered SST Sync Drop)

Under MUT-8 (compaction-triggered SST sync drop, 15.2% failure density), DCG reaches the first failing state at median $k_1 = 1.0$ (mean 1.00) compared to R2's median $k_1 = 3.6$ (mean 3.58) and B0's median $k_1 = 4.0$ (mean 4.41). At early budget $B = 25$, DCG discovers mean $S_{25} = 12.88$ distinct failing states versus 6.74 for R2 and 2.00 for B0. At full budget $B = 100$, DCG and R2 both exhaust the reachable failing equivalence classes ($S_{100}$ mean 12.88 for DCG vs 12.82 for R2, compared to 5.08 for B0).

