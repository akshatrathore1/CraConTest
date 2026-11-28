# Reproduction Runbook

This runbook lists the commands that regenerate every table, figure and acceptance check
in CraConTest, with the outputs and runtimes measured on Linux (go1.27.4 / go1.27.3, amd64).

**Determinism.** For a stored trace, candidate universe, and ranking configuration,
replay, ranking, and offline evaluation are deterministic. Recovery classification also
depends on the configured timeout and execution environment. Recording is not
bit-for-bit repeatable. Pebble schedules background
flushes and compactions itself, so two recordings of the same workload can differ by a few
events and therefore by a few candidates (for example 2,142, 2,144 and 2,150 candidates in
three MUT-0 recordings of the main workload). This is why crash states are always rebuilt
from a stored trace and never by re-running the engine. The committed run directories
under `results/` are the reference data for every figure and table.

**Measured source and reference artifacts.** All 14 source-stamped Stage 2 CSVs in this
release were generated from `v1.1.2-evaluation-freeze` (`f9202dfca579`, exported in
data-only commits `28cfc62..90b1ffe` with zero code changes under `internal/`, `cmd/`, or
`analysis/`); the `git_commit` stamp names the generating source commit rather than the
export commit containing the CSV file. Because their final exports were published after the
source freeze, the tag's own checked-in CSVs still contain earlier campaign results. Use
this release's `results/` tables for statistical reproduction.

Subsequent release maintenance on `main` (including the final release commit, after which
the repository is frozen) includes non-ranking/hardening refactors across `internal/`
(including code hygiene/helper extraction in `internal/strategy/dcg.go` and
`internal/graph/features.go`), strategy-code label normalization (`c1` -> `b6`) in
`results/casestudy_ingest_per_seed.csv` and `results/workload_diversity_per_seed.csv`,
95% CI lower-bound clamping (`>= 1.00`) in `results/holdout_summary.csv` and
`results/casestudy_pebble5825_summary.csv`, refreshed `results/discovery_main_mut1.csv` /
`results/discovery_main_mut4.csv` and `results/plot_main_mut1.svg`, removal of redundant
`results/plots/discovery_main_mut2.svg` and `results/plots/discovery_main_mut4.svg`, and
the Go `1.27.4` pin in `go.mod` and CI.

To regenerate measurements from the frozen implementation without replacing the final
artifacts or disturbing an existing checkout, create a separate source worktree:

Bash (Linux/macOS):

```bash
git worktree add --detach ../CraConTest-frozen-source v1.1.2-evaluation-freeze
```

PowerShell (Windows):

```powershell
git worktree add --detach ..\CraConTest-frozen-source v1.1.2-evaluation-freeze
```

Run build and regeneration commands in that worktree. Direct CLI exports to a separate
output directory; test-owned generators may replace files only in the disposable source
worktree. Start each independently generated artifact family from a clean worktree so
earlier exports do not change the Git status recorded for the next family.
Compare the new outputs with the reference tables in the release checkout,
rather than analyzing the source tag's older CSVs. Subsequent `main` maintenance is not
part of the measured source, and new recordings or runtime measurements need not be
byte-identical to the published results.

---

## 1. Build

Prerequisites are Go 1.27.4 (pinned in `go.mod` and CI; the stored Stage 2 reference
artifacts were measured on Go 1.27.3, Linux amd64, compatible `go1.27.x`), and Python 3.12 or newer
for statistical analysis. The analysis dependency is pinned to `scipy==1.18.1`
in `analysis/requirements.txt` (resolving `numpy` transitively); the reference statistical results were verified with CPython 3.15.2.
On Windows PowerShell set `$env:CGO_ENABLED="0"`, build `cracontest.exe`, and use `python` or `py` in place of `python3`; run the race detector on Linux or WSL2.

```bash
# Linux / macOS
go build -o cracontest ./cmd/cracontest
```

```powershell
# Windows PowerShell
$env:CGO_ENABLED="0"; go build -o cracontest.exe ./cmd/cracontest
```

The CLI has five subcommands. Flags use a single dash:

| Command | Flags |
|---|---|
| `record` | `-spec <config.json> -out <run-dir> [-run <run-dir>] [-fault <MUT-0 … MUT-9, MUT-X1_k<N> … MUT-X3_k<N>, MUT-X4_p04, MUT-X5_p12, MUT-X6_k<N>, BARRIER-5825, BARRIER-CKPT>] [-policies 0,100] [-seed <N>] [-wal-drop-k <N>] [-dir-drop-k <N>] [-ingest-every <N>] [-ratchet-at <N>] [-barrier-ingest] [-preset <name>]` |
| `plan` | `-run <run-dir> [-seed <ranking seed>]` |
| `explore` | `-run <run-dir> [-timeout <duration>]` |
| `evaluate` | `-run <run-dir> [-budgets 0,1,5,...] [-discovery <csv> -plot <svg>]` |
| `benchmark` | `[-seeds 42,100,...] [-faults MUT-1,...] [-ops 120] [-policies 0,100] [-max-manifest-size <bytes>] [-report <md>] [-csv <csv>] [-per-seed-csv <csv>] [-checkpoint-at <N>] [-ingest-every <N>] [-ratchet-at <N>] [-barrier-ingest] [-preset <name>] [-control]` |

### Directory Structure for Reproduction Artifacts

```
CraConTest/
├── analysis/
│   ├── paired_stats.py        # Paired sign/Wilcoxon tests and bootstrap ratios
│   └── requirements.txt       # Pinned SciPy 1.18.1 (with NumPy resolved transitively)
├── results/
│   ├── README.md              # Organization of Stage 1 & Stage 2 results artifacts
│   ├── benchmark_summary.csv  # Aggregated 32-seed benchmark metrics (MUT-1..9)
│   ├── benchmark_per_seed.csv # Unaggregated per-seed discovery metrics (MUT-1..9)
│   ├── holdout_summary.csv    # Aggregated 32-seed holdout metrics (MUT-X1..X6)
│   ├── holdout_per_seed.csv   # Unaggregated per-seed discovery metrics (MUT-X1..X6)
│   ├── casestudy_pebble5825_summary.csv  # Aggregated 32-seed Pebble #5825 A/B metrics
│   ├── casestudy_pebble5825_per_seed.csv # Unaggregated per-seed Pebble #5825 A/B metrics
│   ├── casestudy_checkpoint_summary.csv  # Aggregated 32-seed Checkpoint A/B metrics
│   ├── casestudy_checkpoint_per_seed.csv # Unaggregated per-seed Checkpoint A/B metrics
│   ├── casestudy_ingest_summary.csv      # Direct L0 Ingest 32-seed A/B benchmark summary
│   ├── casestudy_ingest_per_seed.csv     # Unaggregated per-seed Ingest A/B metrics
│   ├── workload_diversity_summary.csv    # 5-preset workload diversity summary (16 seeds)
│   ├── workload_diversity_per_seed.csv   # Unaggregated per-seed workload diversity metrics
│   ├── scaling_mut5_summary.csv        # MUT-5 universe-scaling summary (120..1200 ops)
│   ├── scaling_mut5_per_seed.csv       # Per-seed scaling metrics
│   ├── null_control_multishape.csv     # 108-run multi-shape null-control verification
│   └── subsystem_discovery.csv         # Pebble subsystem empirical discovery (Workloads A, B, C)
├── docs/
│   ├── report.md              # Technical report
│   ├── reproduce.md           # Reproduction runbook
│   └── benchmark-report.md    # Multi-seed benchmark empirical report
└── paper/                     # Reserved for publication materials
```

---

## 2. Main Experiment (1,000-operation workload)

Configuration `experiments/configs/main.json`: seed 42, 1,000 operations, 64 keys, 128-byte
values, a synchronous write every 10th mutation, an explicit flush every 100 operations.
Operation mix: 50% set, 20% get, 10% delete, 10% two-key batch, 5% range delete, 5% scan.
Retention policies 0% and 100%.

To re-run the full pipeline from scratch without disturbing the committed reference directories (which contain legacy `journal.ndjson` files without `context_hash` and the 4 canonical rankings `b0, b4, b5, b6` whereas `plan` now writes all 13 strategies), write to a fresh directory such as `/tmp/main_mut$m`:

```bash
# Linux / macOS
for m in 0 1 2 3 4; do
  d=/tmp/main_mut$m
  rm -rf "$d"
  ./cracontest record   -spec experiments/configs/main.json -out $d -fault MUT-$m
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery $d/discovery_main_mut$m.csv \
                                -plot      $d/plot_main_mut$m.svg
done
```

```powershell
# Windows PowerShell
foreach ($m in 0..4) {
  $d = Join-Path $env:TEMP "main_mut$m"
  if (Test-Path $d) { Remove-Item -Recurse -Force $d }
  .\cracontest.exe record   -spec experiments\configs\main.json -out $d -fault "MUT-$m"
  .\cracontest.exe plan     -run $d
  .\cracontest.exe explore  -run $d
  .\cracontest.exe evaluate -run $d -discovery "$d\discovery_main_mut$m.csv" -plot "$d\plot_main_mut$m.svg"
}
```

| Run | Fault | Trace events | Candidates | Outcomes | Failure kinds |
|---|---|---|---|---|---|
| `main_mut0` | none | 2,510 | 2,150 | 2,150 valid | 0 |
| `main_mut1` | WAL sync dropped | 2,427 | 1,978 | 865 violations (`durably_committed`) | 15 |
| `main_mut2` | manifest sync dropped | 2,494 | 2,112 | 1,047 open errors (malformed manifest) | 1 |
| `main_mut3` | directory sync dropped | 2,480 | 2,048 | 999 violations (`durably_committed`) | 3 |
| `main_mut4` | flushed-table sync dropped | 2,500 | 2,124 | 594 open errors (table size mismatch) | 1 |

`record`, `plan` and `evaluate` each take well under a second; `explore` takes 5–15 s per
run. `evaluate` writes `curves.csv` and `positions.csv` into the run directory:
`positions.csv` gives, for each ranking and for random order over 1,000 fixed seeds, the
position of the first failure and the position at which every failure kind has appeared.
To recompute curves and positions from a stored run without re-exploring, run only
`evaluate`. If a run directory already holds a `journal.ndjson` whose `context_hash` matches the current trace, history, directory, timeout, and categories, `explore` reuses its finished outcomes; delete the journal, `outcomes.json` and `curves.csv` (or use a fresh directory) to explore again.

Figures: `results/plot_main_mut*.svg`. Tables: `results/discovery_main_mut*.csv` and
`results/main_mut*/positions.csv`.

## 3. Smoke Experiment (100-operation workload)

Configuration `experiments/configs/smoke.json`: seed 42, 100 operations, 16 keys, 64-byte
values, sync every 10th mutation, flush every 50 operations. Use it for a quick end-to-end check.

```bash
# Linux / macOS
for f in mut0:MUT-0 mut1:MUT-1; do
  d=/tmp/smoke_${f%%:*}
  rm -rf "$d"
  ./cracontest record   -spec experiments/configs/smoke.json -out $d -fault ${f#*:}
  ./cracontest plan     -run $d
  ./cracontest explore  -run $d
  ./cracontest evaluate -run $d -discovery $d/discovery_${f%%:*}.csv \
                                -plot      $d/plot_${f%%:*}.svg
done
```

```powershell
# Windows PowerShell
foreach ($pair in @("mut0:MUT-0", "mut1:MUT-1")) {
  $k, $fault = $pair -split ":"
  $d = Join-Path $env:TEMP "smoke_$k"
  if (Test-Path $d) { Remove-Item -Recurse -Force $d }
  .\cracontest.exe record   -spec experiments\configs\smoke.json -out $d -fault $fault
  .\cracontest.exe plan     -run $d
  .\cracontest.exe explore  -run $d
  .\cracontest.exe evaluate -run $d -discovery "$d\discovery_$k.csv" -plot "$d\plot_$k.svg"
}
```

| Run | Trace events | Candidates | Outcomes | Distinct signatures | Total time |
|---|---|---|---|---|---|
| `smoke_mut0` | 316 | 302 | 302 valid | 0 | ≈0.6 s |
| `smoke_mut1` | 305 | 280 | 192 valid, 88 violations | 6 | ≈0.5 s |

## 4. Multi-Seed Benchmark (32 Seeds, MUT-0 to MUT-9)

To evaluate ranking strategies across both dense and sparse failure distributions with statistical confidence intervals, the multi-seed benchmark evaluates 32 deterministic workload seeds across `MUT-1` through `MUT-9` (exported to CSV) plus an unfaulted `MUT-0` control check:

```bash
# Linux / macOS
./cracontest benchmark -csv /tmp/benchmark_summary.csv -per-seed-csv /tmp/benchmark_per_seed.csv
```

```powershell
# Windows PowerShell
.\cracontest.exe benchmark -csv $env:TEMP\benchmark_summary.csv -per-seed-csv $env:TEMP\benchmark_per_seed.csv
```

To run the paired statistical comparison across seeds:

```bash
# Linux / macOS
python3 analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9
```

```powershell
# Windows PowerShell
python analysis/paired_stats.py results/benchmark_per_seed.csv "Seeded Benchmark Faults (32 seeds)" MUT-1 MUT-2 MUT-3 MUT-4 MUT-5 MUT-6 MUT-8 MUT-9
```

The multi-seed benchmark runs 32 deterministic workload seeds (`42, 100..900, 1001..1022`; 120 operations, `SyncEvery = 8`, `FlushEvery = 40`) across `MUT-0` through `MUT-9` with automatic background compactions and table stats disabled (`DisableAutomaticCompactions: true`, `DisableTableStats: true`, and deterministic mid-workload compaction `CompactAt = 80` for `MUT-8`). Repeated-run tests cover the evaluated shapes, but recording remains schedule-sensitive as described at the start of this runbook; publication results therefore use the committed source-stamped traces and tables.

### Benchmark Results Summary (32 Seeds)

#### Cost-to-First-Failure ($k_1$ Mean) & Distinct Failing States ($S_{25}$ / $S_{100}$ Mean)

| Fault Model | Fault Description | Failure Density | DCG $k_1$ | R2 $k_1$ | B0 $k_1$ | DCG $S_{25}$ / $S_{100}$ | R2 $S_{25}$ / $S_{100}$ | B0 $S_{25}$ / $S_{100}$ |
|---|---|---|---|---|---|---|---|---|
| **MUT-0*** | Null Control (No Fault, control check only) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-1** | All WAL Syncs Dropped | 36.4% | **1.00** | 1.42 | 2.73 | **23.94** / **23.97** | 17.14 / **23.97** | 6.34 / 14.58 |
| **MUT-2** | MANIFEST Syncs Dropped | 47.3% | **1.00** | 1.17 | 2.10 | **25.00** / **33.00** | 21.12 / **33.00** | 5.88 / 13.91 |
| **MUT-3** | Directory Syncs Dropped | 42.5% | **1.00** | 1.42 | 2.32 | 24.00 / **27.00** | 17.24 / **27.00** | 5.00 / 11.29 |
| **MUT-4** | SST Flush Syncs Dropped | 29.8% | **1.00** | 1.93 | 3.42 | **21.69** / **21.69** | 12.60 / **21.69** | 3.66 / 8.67 |
| **MUT-5** | Sparse $k$-th WAL Sync Dropped ($k=1$) | 2.7% | **1.00** | 13.04 | 33.75 | **2.00** / **2.00** | 1.31 / **2.00** | 0.53 / 1.22 |
| **MUT-6** | Second Manifest Sync Dropped | 0.8% | **1.00** | 7.59 | 41.24 | **1.62** / **1.62** | 1.09 / **1.62** | 0.21 / 0.72 |
| **MUT-7** | Options Dir Sync (No-op Control) | 0.0% | 0.00 | 0.00 | 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| **MUT-8** | Compaction SST Sync Drop | 15.2% | **1.00** | 3.42 | 6.49 | **12.88** / **12.88** | 6.73 / 12.80 | 2.12 / 5.19 |
| **MUT-9** | Single $k$-th Directory Sync Drop | 41.4% | **1.00** | 1.38 | 2.42 | **25.00** / **27.00** | 17.75 / **27.00** | 3.31 / 9.79 |

\* **Note:** `MUT-0` is executed as a separate 32-seed null-control verification (`TrackMUT0: true` / `-control`) and is not exported as a row inside `results/benchmark_summary.csv` or `results/benchmark_per_seed.csv` (which export `MUT-1` through `MUT-9`).

> **MUT-6 Denominators:** The summary tables average $S_{25}$ and $S_{100}$ over all 32 evaluated seeds: DCG $S_{25}=1.62$ ($52/32$) and R2 $S_{25}=1.09$. The analysis script restricts paired comparisons to the 13 seeds containing failures, giving conditional $S_{25}=4.00$ for DCG and $2.68$ for R2. These are different denominators applied to the same observations.

### 4.2 Holdout & Out-of-Contract Benchmark (32 Seeds, MUT-X1 to MUT-X6)

The accounting-corrected campaign reevaluates 17 known parameterized, delayed, stochastic, and out-of-contract settings across 32 deterministic seeds; it is not a newly blind holdout:

```bash
# Linux / macOS
./cracontest benchmark \
  -faults MUT-X1_k2,MUT-X1_k3,MUT-X1_k4,MUT-X1_k6,MUT-X1_k9,MUT-X2_k1,MUT-X2_k3,MUT-X2_k4,MUT-X2_k5,MUT-X3_k1,MUT-X3_k2,MUT-X3_k3,MUT-X4_p04,MUT-X5_p12,MUT-X6_k2,MUT-X6_k5,MUT-X6_k9 \
  -csv results/holdout_summary.csv \
  -per-seed-csv results/holdout_per_seed.csv
```

```powershell
# Windows PowerShell
.\cracontest.exe benchmark `
  -faults MUT-X1_k2,MUT-X1_k3,MUT-X1_k4,MUT-X1_k6,MUT-X1_k9,MUT-X2_k1,MUT-X2_k3,MUT-X2_k4,MUT-X2_k5,MUT-X3_k1,MUT-X3_k2,MUT-X3_k3,MUT-X4_p04,MUT-X5_p12,MUT-X6_k2,MUT-X6_k5,MUT-X6_k9 `
  -csv results/holdout_summary.csv `
  -per-seed-csv results/holdout_per_seed.csv
```

To run paired statistical comparisons on the holdout per-seed results (`python` on Windows):

```bash
# Holdout Faults (MUT-X1..X5)
python3 analysis/paired_stats.py results/holdout_per_seed.csv "Holdout Faults" MUT-X1_k2 MUT-X1_k3 MUT-X1_k4 MUT-X1_k6 MUT-X1_k9 MUT-X2_k1 MUT-X2_k3 MUT-X3_k1 MUT-X3_k2 MUT-X3_k3 MUT-X4_p04 MUT-X5_p12

# Out-of-Contract Negative Control (MUT-X6)
python3 analysis/paired_stats.py results/holdout_per_seed.csv "Out-of-Contract Control" MUT-X6_k2 MUT-X6_k5 MUT-X6_k9
```

### 4.3 Pebble #5825 Case Study A/B Benchmark (32 Seeds, MUT-0 vs BARRIER-5825)

To evaluate real-world crash inconsistency caused by unsynced parent directory entries under partial retention (CockroachDB Pebble #5825), the case study benchmark compares unfaulted `MUT-0` against the patched control `BARRIER-5825` (Manifest-Rotation Barrier Control) across 32 seeds under policies 0%, 50%, and 100% with manifest rotation (`-max-manifest-size 1`):

```bash
# Linux / macOS
./cracontest benchmark \
  -faults MUT-0,BARRIER-5825 \
  -max-manifest-size 1 \
  -policies 0,50,100 \
  -csv results/casestudy_pebble5825_summary.csv \
  -per-seed-csv results/casestudy_pebble5825_per_seed.csv
```

```powershell
# Windows PowerShell
.\cracontest.exe benchmark `
  -faults MUT-0,BARRIER-5825 `
  -max-manifest-size 1 `
  -policies 0,50,100 `
  -csv results/casestudy_pebble5825_summary.csv `
  -per-seed-csv results/casestudy_pebble5825_per_seed.csv
```

To run paired statistical comparisons on the Pebble #5825 case study per-seed results (`python` on Windows):

```bash
python3 analysis/paired_stats.py results/casestudy_pebble5825_per_seed.csv "Pebble #5825 Case Study (32 seeds)" MUT-0
```

- **Barrier intervention:** Under `MUT-0`, 58 failures occur across 25 of 32 seeds, all at 50% partial retention. Under `BARRIER-5825`, which syncs the parent directory before creating the marker that references the pending `MANIFEST-xxxxxx`, no failures are observed across the 32 evaluated seeds. This controlled contrast supports the directory-ordering mechanism within the evaluated VFS model.
- **Post-Hoc Partial-Retention Prior (`dcg-pr`):** On `v1.1.2-evaluation-freeze`, standard `dcg` prioritizes 0%-retention candidates first and requires median $k_1 = \mathbf{203.00}$ [195.00, 264.00] (mean 243.84) to discover the partial-retention failure. `dcg-pr` tracks `UnsyncedDirEntryCount >= 2` and discovers the first failure at median $k_1 = \mathbf{2.00}$ [2.00, 5.00] (mean 12.40, S25 = 1.44, S100 = 1.44, AUC Failures = 0.6733, AUC Bugs = 0.7562), compared with accounting-corrected uniform random B0 median $k_1 = 178.73$ (mean 192.72) and representative B5 median $k_1 = 62.00$ (mean 57.68).

### 4.4 Intra-Class Subsystem Transfer Case Study: Pebble Checkpoint Directory Race (32 Seeds, MUT-0 vs BARRIER-CKPT)

To evaluate the checkpoint directory race present in Pebble v2.1.7 and later fixed upstream by commit `15b500fb`, the holdout benchmark compares `MUT-0` with the `BARRIER-CKPT` intervention across 32 seeds under 0%, 50%, and 100% retention with `-checkpoint-at 60`:

```bash
# Linux / macOS
./cracontest benchmark \
  -faults MUT-0,BARRIER-CKPT \
  -checkpoint-at 60 \
  -policies 0,50,100 \
  -csv results/casestudy_checkpoint_summary.csv \
  -per-seed-csv results/casestudy_checkpoint_per_seed.csv
```

```powershell
# Windows PowerShell
.\cracontest.exe benchmark `
  -faults MUT-0,BARRIER-CKPT `
  -checkpoint-at 60 `
  -policies 0,50,100 `
  -csv results/casestudy_checkpoint_summary.csv `
  -per-seed-csv results/casestudy_checkpoint_per_seed.csv
```

To run paired statistical comparisons on the Pebble Checkpoint per-seed results:

```bash
python3 analysis/paired_stats.py results/casestudy_checkpoint_per_seed.csv "Pebble Checkpoint Case Study (32 seeds)" MUT-0
```

#### Results Summary:
- **Barrier intervention:** In Pebble `db.Checkpoint("checkpoint")`, checkpoint manifest and marker pointer files (`marker.manifest.*`) are written before `dir.Sync()`. Under 50% partial retention, checkpoint recovery fails on 31 of 32 seeds (53 total failures). Under `BARRIER-CKPT`, which adds parent-directory synchronization around the relevant entries, no failures are observed across the 32 evaluated seeds.
- **Accounting-Corrected Prior Discovery (`dcg-pr`):** On `v1.1.2-evaluation-freeze`, `dcg-pr` records median $k_1 = \mathbf{245.00}$ [3.00, 306.00] (mean 156.39, S25 = S100 = 0.53, AUC Failures = 0.5010, AUC Bugs = 0.6321), compared with DCG median/mean 288.00/258.65, R2 280.41/271.55, and corrected B0 262.26/238.53. The paired DCG-PR/R2 comparison is 22 wins, 9 losses, and 0 ties.

### 4.5 MUT-5 Universe-Scaling Benchmark (ops ∈ {120, 300, 600, 1200})

To evaluate candidate universe scaling behavior on single-sync drop faults (`MUT-5`, single WAL sync drop), the scaling experiment evaluates 8 seeds across 4 operations scales:

```bash
go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update
```

Outputs: `results/scaling_mut5_per_seed.csv` and `results/scaling_mut5_summary.csv`.

> **Note:** `TestScalingMUT5` and `TestNullControlMultiShape` validate the committed CSVs in `results/` by default during `go test ./...` (read-only — no files are written). To regenerate the CSVs from scratch, pass the `-update` flag:
> ```
> go test -v -count=1 -timeout 1200s -run TestScalingMUT5 ./internal/exp -update
> go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update
> ```

#### Results Summary:
- Candidate universes scale from ~338 (120 ops) to ~2,958 (1,200 ops).
- DCG has median and mean $k_1 = 1.00$ at each of the four evaluated scales; this finite experiment does not establish asymptotic complexity.
- Baseline discovery positions increase over the evaluated range: B0's mean of per-seed rounded shuffle medians from 26.75 to 220.88, B2 (LIFO) from 260.50 to 2,881.00, and R2 from 12.00 to 80.62.
- Mean planning/ranking time is 38.5 ms at 1,200 operations (approximately 2,958 candidates), as recorded in `scaling_mut5_summary.csv`.

### 4.6 Multi-Shape Null-Control Verification (108 Runs, 0 False Alarms)

The multi-shape null-control check evaluates 12 shapes across 9 seeds on `MUT-0` at 0%/100% policies:

```bash
go test -v -count=1 -timeout 1200s -run TestNullControlMultiShape ./internal/exp -update
```

Outputs: `results/null_control_multishape.csv`.
Result: No violations, open errors, panics, or hangs are observed across the 108 evaluated runs (>50,000 crash states). The fixed workload shapes and dependent candidates do not support an independent-trial confidence bound on a population false-alarm probability.

### 4.7 Pebble Subsystem Discovery Benchmark (Workloads A, B, C under MUT-0)

To evaluate whether unfaulted Pebble v2.1.7 (`MUT-0`) exhibits crash-consistency vulnerabilities during multi-file and directory-marker mutations, the subsystem discovery suite executes Workload A (External SSTable Ingestion), Workload B (Live Format-Version Ratcheting), and Workload C (Combined Lifecycle Stress) across seeds `42, 100, 777` under intermediate retention policies `[0, 50, 100]`:

```bash
# Read-only validation of committed artifact:
go test -v -run TestSubsystemDiscovery ./internal/exp

# Full regeneration of discovery CSV:
go test -v -run TestSubsystemDiscovery ./internal/exp -update
```

Outputs: `results/subsystem_discovery.csv` (9 data rows).

To regenerate `results/subsystem_discovery.csv`, run `TestSubsystemDiscovery ... -update` above—it is the canonical generator because CLI `benchmark` does not expose `-key-count`, `-value-bytes`, `-sync-every`, or `-spec` flags (CLI `benchmark` uses `KeyCount = 16, ValueBytes = 64, SyncEvery = 8`, whereas `TestSubsystemDiscovery` runs with `KeyCount = 64, ValueBytes = 128, SyncEvery = 10`). For exploratory standalone CLI runs with the same maintenance flags (omitting `-report` prints Markdown to stdout only when `-csv` and `-per-seed-csv` are also omitted):

```bash
# Workload A: External SSTable Ingestion (Linux / macOS)
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ingest-every 25 -policies 0,50,100 -csv /tmp/workload_a_summary.csv

# Workload B: Live Format-Version Ratcheting (Linux / macOS)
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ratchet-at 50 -policies 0,50,100 -csv /tmp/workload_b_summary.csv

# Workload C: Combined Lifecycle Stress (Linux / macOS)
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ingest-every 30 -ratchet-at 40 -checkpoint-at 60 -max-manifest-size 1 -policies 0,50,100 -csv /tmp/workload_c_summary.csv
```

```powershell
# Windows PowerShell (with $env:CGO_ENABLED="0")
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ingest-every 25 -policies 0,50,100 -csv $env:TEMP\workload_a_summary.csv
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ratchet-at 50 -policies 0,50,100 -csv $env:TEMP\workload_b_summary.csv
go run ./cmd/cracontest benchmark -faults MUT-0 -seeds 42,100,777 -ops 80 -ingest-every 30 -ratchet-at 40 -checkpoint-at 60 -max-manifest-size 1 -policies 0,50,100 -csv $env:TEMP\workload_c_summary.csv
```

#### Results Summary:
- **0 lost durably-committed keys** across all 9 runs (5,415 candidate states), with no durably-committed data loss observed in these workloads.
- **0 engine recovery panics or hangs**.
- **Workload A (External SSTable Ingestion):** 0 lost committed keys. 40 prefix violations across seeds 42 (15) and 777 (25) reveal a cross-channel commit-order hole on direct L0 ingest: when `db.Ingest` takes the non-overlapping direct L0 shortcut (`asFlushable = false`), Pebble allocates sequence number $s_2 > s_1$ (`ingest.go:1703`) and syncs `MANIFEST` (`version_set.go:649`) without syncing the active `.log` WAL containing preceding `NoSync` writes at sequence number $s_1 < s_2$. Upon a 0%-retention crash, $s_2$ survives via `MANIFEST` while $s_1$ is lost from the unsynced `WAL`, creating a cross-channel sequence hole (whereas overlapping flushable ingest syncs the shared WAL at `commit.go:289` and preserves strict prefix order).
- **Workload B (Live Format-Version Ratcheting):** 0 violations, 0 panics, and 3 open errors under 50% retention at initial database creation cutoffs (0 on seed 42, 2 on seed 100, 1 on seed 777).
- **Workload C (Combined Lifecycle Stress):** 0 lost committed keys, 0 panics. 15 open errors occur exclusively under 50% partial retention during manifest rotation and checkpointing prior to directory sync (5 on seed 42, 7 on seed 100, 3 on seed 777).

## 5. Acceptance Tests

| Check | Command | Measured result | Runtime |
|---|---|---|---|
| G1 replay fidelity | `go test -v -run 'TestGateG1$' ./internal/replay` | 2,322 events, 20/20 seeded cutoffs byte-identical to the live 100% clone; every replayed state recovers | 0.1 s |
| G2 null control | `go test -v -run 'TestGateG2$' ./internal/workload` | every endpoint candidate of a fresh 1,000-op MUT-0 recording (≈2,140) valid; 0 violations, open errors, panics, hangs or oracle errors | 7 s |
| G3 graph labels | `go test -v -run 'TestGateG3$' ./internal/graph` | ≈2,510 events, ≈1,073 cutoffs, ≈173 distinct state keys, Semantic=1 on 96% of cutoffs, build time ≈12 ms | 0.05 s |
| G4 discovery curves | `go test -v -run 'TestGateG4$|TestMainRunArtifacts' ./internal/exp` | curves recomputed from the stored smoke and main tables match the committed `results/discovery_*.csv`; nothing re-executed | 0.5 s |
| Real-disk recovery | `go test -v -run TestRealDisk ./internal/recovery` | 200 synced keys survive a clean close and an abrupt child-process exit on `vfs.Default` | 1.6 s |
| Subsystem discovery | `go test -v -run TestSubsystemDiscovery ./internal/exp` | 9 rows validated in `results/subsystem_discovery.csv`; 0 durably committed violations, 0 panics | <0.01 s |

G1, G2 and G3 record a fresh trace, so their counts vary by a few events between runs
(see Determinism above).

## 6. Full Suite

```bash
# Linux / macOS
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...      # Linux or WSL2
```

```powershell
# Windows PowerShell
$env:CGO_ENABLED="0"; go vet ./...; go test -count=1 ./...
```

All 13 packages pass. `go test` takes about 12 s and `go test -race` about 50 s on Linux.
