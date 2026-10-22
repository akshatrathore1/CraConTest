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

The CLI has four subcommands. Flags use a single dash:

| Command | Flags |
|---|---|
| `record` | `-spec <config.json> -out <run-dir> -fault <MUT-0\|MUT-1>` |
| `plan` | `-run <run-dir> [-seed <ranking seed>]` |
| `explore` | `-run <run-dir> [-timeout <duration>]` |
| `evaluate` | `-run <run-dir> [-budgets 0,1,5,...] [-discovery <csv> -plot <svg>]` |

---

## 2. Main Experiment (1,000-operation workload)

Configuration `experiments/configs/main.json`: seed 42, 1,000 operations, 64 keys, 128-byte
values, a synchronous write every 10th mutation, an explicit flush every 100 operations.
Operation mix: 50% set, 20% get, 10% delete, 10% two-key batch, 5% range delete, 5% scan.
Retention policies 0% and 100%.

```bash
for f in mut0:MUT-0 mut1:MUT-1; do
  d=results/main_${f%%:*}
  ./cracontest record   -spec experiments/configs/main.json -out $d -fault ${f#*:}
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery results/discovery_main_${f%%:*}.csv \
                                -plot      results/plot_main_${f%%:*}.svg
done
```

| Run | Trace events | Cutoffs | Candidates | Outcomes | Distinct signatures | Explore time |
|---|---|---|---|---|---|---|
| `main_mut0` | 2,510 | 1,075 | 2,150 | 2,150 valid | 0 | 14.1 s |
| `main_mut1` | 2,427 | 989 | 1,978 | 1,113 valid, 865 violations | 3 | 12.1 s |

`record`, `plan` and `evaluate` each take about 0.2 s. In `main_mut1` the trace contains no
WAL sync events (84 in `main_mut0`), while its 16 manifest syncs and 30 directory syncs are
unchanged.

Figures: `results/plot_main_mut0.svg`, `results/plot_main_mut1.svg`.
Tables: `results/discovery_main_mut0.csv`, `results/discovery_main_mut1.csv`.

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
| `smoke_mut1` | 305 | 280 | 192 valid, 88 violations | 2 | ≈0.5 s |

## 4. Acceptance Tests

| Check | Command | Measured result | Runtime |
|---|---|---|---|
| G1 replay fidelity | `go test -v -run 'TestGateG1$' ./internal/replay` | 2,322 events, 20/20 seeded cutoffs byte-identical to the live 100% clone; every replayed state recovers | 0.1 s |
| G2 null control | `go test -v -run 'TestGateG2$' ./internal/workload` | every endpoint candidate of a fresh 1,000-op MUT-0 recording (≈2,140) valid; 0 violations, open errors, panics, hangs or oracle errors | 7 s |
| G3 graph labels | `go test -v -run 'TestGateG3$' ./internal/graph` | ≈2,510 events, ≈1,073 cutoffs, ≈173 distinct state keys, Semantic=1 on 96% of cutoffs, build time ≈12 ms | 0.05 s |
| G4 discovery curves | `go test -v -run 'TestGateG4$' ./internal/exp` | curves recomputed from the stored smoke tables match the committed `results/discovery_mut*.csv`; nothing re-executed | 0.1 s |
| Real-disk recovery | `go test -v -run TestRealDisk ./internal/recovery` | 200 synced keys survive a clean close and an abrupt child-process exit on `vfs.Default` | 1.6 s |

G1, G2 and G3 record a fresh trace, so their counts vary by a few events between runs
(see Determinism above).

## 5. Full Suite

```bash
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...      # Linux or WSL2
```

All 13 packages pass. `go test` takes about 12 s and `go test -race` about 50 s on Linux.
