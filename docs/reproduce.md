# Reproduction Runbook

This runbook lists the commands that regenerate every table, figure and acceptance check
in CraConTest, with the outputs and runtimes measured on Linux (go1.27.3, amd64).

**Determinism.** Exploring, ranking and evaluating a *stored* run directory is fully
deterministic: the same `trace.json` and `universe.json` always give the same outcomes,
rankings and curves. Recording is not bit-for-bit repeatable. Pebble schedules background
flushes and compactions itself, so two recordings of the same workload can differ by a few
events and therefore by a few candidates (for example 2,142, 2,144 and 2,150 candidates in
three MUT-0 recordings of the main workload). This is why crash states are always rebuilt
from a stored trace and never by re-running the engine. The committed run directories
under `results/` are the reference data for every figure and table.

---

## 1. Build

Prerequisites: Go as pinned in `go.mod`. On Windows set `CGO_ENABLED=0`; run the race
detector on Linux or WSL2.

```bash
go build -o cracontest ./cmd/cracontest
```

The CLI has five subcommands. Flags use a single dash:

| Command | Flags |
|---|---|
| `record` | `-spec <config.json> -out <run-dir> -fault <MUT-0 … MUT-9>` |
| `plan` | `-run <run-dir> [-seed <ranking seed>]` |
| `explore` | `-run <run-dir> [-timeout <duration>]` |
| `evaluate` | `-run <run-dir> [-budgets 0,1,5,...] [-discovery <csv> -plot <svg>]` |
| `benchmark` | `[-seeds 42,100,...] [-faults MUT-1,...] [-ops 120] [-report <md>] [-csv <csv>]` |

---

## 2. Main Experiment (1,000-operation workload)

Configuration `experiments/configs/main.json`: seed 42, 1,000 operations, 64 keys, 128-byte
values, a synchronous write every 10th mutation, an explicit flush every 100 operations.
Operation mix: 50% set, 20% get, 10% delete, 10% two-key batch, 5% range delete, 5% scan.
Retention policies 0% and 100%.

```bash
for m in 0 1 2 3 4; do
  d=results/main_mut$m
  ./cracontest record   -spec experiments/configs/main.json -out $d -fault MUT-$m
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery results/discovery_main_mut$m.csv \
                                -plot      results/plot_main_mut$m.svg
done
```

| Run | Fault | Trace events | Candidates | Outcomes | Failure kinds |
|---|---|---|---|---|---|
| `main_mut0` | none | 2,510 | 2,150 | 2,150 valid | 0 |
| `main_mut1` | WAL sync dropped | 2,427 | 1,978 | 865 violations (`durably_committed`) | 15 |
| `main_mut2` | manifest sync dropped | 2,494 | 2,112 | 1,047 open errors (malformed manifest) | 1 |
| `main_mut3` | directory sync dropped | 2,480 | 2,048 | 999 violations (`durably_committed`) | 3 |
| `main_mut4` | flushed-table sync dropped | 2,500 | 2,124 | 594 open errors (table size mismatch) | 2 |

`record`, `plan` and `evaluate` each take well under a second; `explore` takes 5–15 s per
run. `evaluate` writes `curves.csv` and `positions.csv` into the run directory:
`positions.csv` gives, for each ranking and for random order over 1,000 fixed seeds, the
position of the first failure and the position at which every failure kind has appeared.
To recompute curves and positions from a stored run without re-exploring, run only
`evaluate`. If a run directory already holds a `journal.ndjson`, `explore` reuses its
finished outcomes; delete the journal, `outcomes.json` and `curves.csv` to explore again.

Figures: `results/plot_main_mut*.svg`. Tables: `results/discovery_main_mut*.csv` and
`results/main_mut*/positions.csv`.

## 3. Smoke Experiment (100-operation workload)

Configuration `experiments/configs/smoke.json`: seed 42, 100 operations, 16 keys, 64-byte
values, sync every 10th mutation, flush every 50 operations. Use it for a quick end-to-end check.

```bash
for f in mut0:MUT-0 mut1:MUT-1; do
  d=results/smoke_${f%%:*}
  ./cracontest record   -spec experiments/configs/smoke.json -out $d -fault ${f#*:}
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery results/discovery_${f%%:*}.csv \
                                -plot      results/plot_${f%%:*}.svg
done
```

| Run | Trace events | Candidates | Outcomes | Distinct signatures | Total time |
|---|---|---|---|---|---|
| `smoke_mut0` | 316 | 302 | 302 valid | 0 | ≈0.6 s |
| `smoke_mut1` | 305 | 280 | 192 valid, 88 violations | 6 | ≈0.5 s |

## 4. Multi-Seed Benchmark (32 Seeds, MUT-0 to MUT-9)

To evaluate ranking strategies across both dense and sparse failure distributions with statistical confidence intervals, the multi-seed benchmark evaluates 32 deterministic workload seeds across all 10 fault models:

```bash
./cracontest benchmark -report docs/benchmark-report.md -csv results/benchmark_summary.csv
```

The multi-seed benchmark runs 32 deterministic workload seeds (`42, 100..900, 1001..1022`; 120 operations, `SyncEvery = 8`, `FlushEvery = 40`) across `MUT-0` through `MUT-9` with automatic background compactions and table stats disabled (`DisableAutomaticCompactions: true`, `DisableTableStats: true`, and deterministic mid-workload compaction `CompactAt = 80` for `MUT-8`) so traces and candidate counts are 100% repeatable across runs and platforms.

### Benchmark Results Summary (32 Seeds)

#### Cost-to-First-Failure ($k_1$ Mean) & Distinct Failing States ($S_{25}$ / $S_{100}$ Mean)

| Fault Model | Fault Description | Failure Density | DCG $k_1$ | R2 $k_1$ | B0 $k_1$ | DCG $S_{25}$ / $S_{100}$ | R2 $S_{25}$ / $S_{100}$ | B0 $S_{25}$ / $S_{100}$ |
|---|---|---|---|---|---|---|---|---|
| **MUT-0** | Null Control (No Fault) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** | 1.44 | 2.00 | **23.97** / **24.00** | 17.13 / **24.00** | 6.12 / 14.61 |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** | 1.17 | 1.58 | **25.00** / **33.03** | 21.15 / **33.03** | 5.64 / 13.75 |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** | 1.44 | 2.00 | 24.00 / **27.00** | 17.26 / **27.00** | 5.00 / 11.22 |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** | 1.95 | 2.39 | **21.69** / **21.69** | 12.57 / **21.69** | 3.53 / 8.59 |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** | 13.01 | 25.97 | **2.00** / **2.00** | 1.30 / **2.00** | 0.48 / 1.00 |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** | 7.72 | 31.12 | **1.62** / **1.62** | 1.07 / **1.62** | 0.00 / 0.81 |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** | 3.58 | 4.41 | **12.88** / **12.88** | 6.74 / 12.82 | 2.00 / 5.08 |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** | 1.40 | 1.95 | **25.00** / **27.00** | 17.78 / **27.00** | 3.03 / 9.83 |

## 5. Acceptance Tests

| Check | Command | Measured result | Runtime |
|---|---|---|---|
| G1 replay fidelity | `go test -v -run 'TestGateG1$' ./internal/replay` | 2,322 events, 20/20 seeded cutoffs byte-identical to the live 100% clone; every replayed state recovers | 0.1 s |
| G2 null control | `go test -v -run 'TestGateG2$' ./internal/workload` | every endpoint candidate of a fresh 1,000-op MUT-0 recording (≈2,140) valid; 0 violations, open errors, panics, hangs or oracle errors | 7 s |
| G3 graph labels | `go test -v -run 'TestGateG3$' ./internal/graph` | ≈2,510 events, ≈1,073 cutoffs, ≈173 distinct state keys, Semantic=1 on 96% of cutoffs, build time ≈12 ms | 0.05 s |
| G4 discovery curves | `go test -v -run 'TestGateG4$\|TestMainRunArtifacts' ./internal/exp` | curves recomputed from the stored smoke and main tables match the committed `results/discovery_*.csv`; nothing re-executed | 0.5 s |
| Real-disk recovery | `go test -v -run TestRealDisk ./internal/recovery` | 200 synced keys survive a clean close and an abrupt child-process exit on `vfs.Default` | 1.6 s |

G1, G2 and G3 record a fresh trace, so their counts vary by a few events between runs
(see Determinism above).

## 6. Full Suite

```bash
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...      # Linux or WSL2
```

All 13 packages pass. `go test` takes about 12 s and `go test -race` about 50 s on Linux.
