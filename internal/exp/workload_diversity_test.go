package exp

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/workload"
)

func diversityResultsDir() string {
	dir := filepath.Join("..", "..", "results")
	if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
		return dir
	}
	return "results"
}

// TestWorkloadDiversityNullControlSmoke verifies all 5 workload presets on Seed 1:
// - 0 false alarms under MUT-0 (0 violations, 0 open errors).
// - Fault recall under MUT-1 (k1 == 1 for ycsb-a/b/f/overwrite, k1 <= 3 for delete-heavy).
func TestWorkloadDiversityNullControlSmoke(t *testing.T) {
	presets := []string{
		workload.PresetYCSBA,
		workload.PresetYCSBB,
		workload.PresetYCSBF,
		workload.PresetOverwriteHeavy,
		workload.PresetDeleteHeavy,
	}

	for _, p := range presets {
		t.Run(p+"/MUT-0", func(t *testing.T) {
			benchCfg := BenchmarkConfig{
				Workload: workload.Config{
					Preset:     p,
					Operations: 25,
					SyncEvery:  5,
					FlushEvery: 10,
					Seed:       1,
				},
				Policies:        []int{0, 100},
				SafeOpenTimeout: 3 * time.Second,
				B0PercentileCnt: 10,
				Budgets:         []int{0, 1, 5, 10, 25},
			}
			res, err := RunSingleSeedFault(1, faults.MUT0, benchCfg)
			if err != nil {
				t.Fatalf("preset %s MUT-0 run failed: %v", p, err)
			}
			if res.TotalViolations != 0 || res.TotalOpenErrors != 0 {
				t.Fatalf("preset %s MUT-0 had false alarms: viols=%d open_errs=%d",
					p, res.TotalViolations, res.TotalOpenErrors)
			}
			t.Logf("preset %-16s MUT-0: 0 false alarms (universe=%d) PASS", p, res.UniverseSize)
		})

		t.Run(p+"/MUT-1", func(t *testing.T) {
			benchCfg := BenchmarkConfig{
				Workload: workload.Config{
					Preset:     p,
					Operations: 25,
					SyncEvery:  5,
					FlushEvery: 10,
					Seed:       1,
				},
				Policies:        []int{0, 100},
				SafeOpenTimeout: 3 * time.Second,
				B0PercentileCnt: 10,
				Budgets:         []int{0, 1, 5, 10, 25},
			}
			res, err := RunSingleSeedFault(1, faults.MUT1, benchCfg)
			if err != nil {
				t.Fatalf("preset %s MUT-1 run failed: %v", p, err)
			}
			if res.TotalFailures == 0 {
				t.Fatalf("preset %s MUT-1: expected failures > 0, got 0", p)
			}
			dcgMetrics, ok := res.Strategies["dcg"]
			if !ok {
				t.Fatalf("preset %s MUT-1: missing DCG strategy results", p)
			}
			k1 := dcgMetrics.K1Failure
			if p == workload.PresetDeleteHeavy {
				if k1 <= 0 || k1 > 3 {
					t.Fatalf("preset %s MUT-1: expected DCG k1 <= 3, got %d", p, k1)
				}
			} else {
				if k1 != 1 {
					t.Fatalf("preset %s MUT-1: expected DCG k1 == 1, got %d", p, k1)
				}
			}
			t.Logf("preset %-16s MUT-1: failures=%d DCG k1=%d PASS", p, res.TotalFailures, k1)
		})
	}
}

type diversityStratSpec struct {
	code        string
	resKey      string
	displayName string
	summaryName string
}

var diversityStratSpecs = []diversityStratSpec{
	{"dcg", "dcg", "dcg", "Durability-Contract Guided (DCG)"},
	{"dcg-pr", "dcg-pr", "dcg-pr", "DCG Partial-Retention Prior (DCG-PR)"},
	{"r2", "r2", "r2", "State-Representative Random (R2)"},
	{"c1", "b6", "sync-boundary", "sync-boundary"},
	{"b0", "b0", "Uniform Random (B0)", "Uniform Random (B0)"},
}

func validateWorkloadDiversityCSVs(t *testing.T, perSeedPath, summaryPath string) {
	t.Helper()

	f1, err := os.Open(perSeedPath)
	if err != nil {
		t.Fatalf("open per-seed csv %s: %v", perSeedPath, err)
	}
	defer f1.Close()

	r1 := csv.NewReader(f1)
	rows1, err := r1.ReadAll()
	if err != nil {
		t.Fatalf("read per-seed csv: %v", err)
	}
	if len(rows1) != 1601 {
		t.Fatalf("expected 1601 rows in %s, got %d", perSeedPath, len(rows1))
	}

	expectedHeader1 := []string{
		"fault_id", "seed", "universe_size", "total_failures", "fault_density_pct",
		"strategy_code", "strategy", "k1_single", "s25_single",
		"k1_failure_mean", "s25_mean", "s100_mean", "b_full_mean",
		"auc_norm_failures", "auc_norm_bugs", "git_commit",
	}
	for i, col := range expectedHeader1 {
		if rows1[0][i] != col {
			t.Fatalf("per-seed header col %d: expected %q, got %q", i, col, rows1[0][i])
		}
	}

	f2, err := os.Open(summaryPath)
	if err != nil {
		t.Fatalf("open summary csv %s: %v", summaryPath, err)
	}
	defer f2.Close()

	r2 := csv.NewReader(f2)
	rows2, err := r2.ReadAll()
	if err != nil {
		t.Fatalf("read summary csv: %v", err)
	}
	if len(rows2) != 101 {
		t.Fatalf("expected 101 rows in %s, got %d", summaryPath, len(rows2))
	}

	expectedHeader2 := []string{
		"fault_id", "strategy",
		"k1_failure_median", "k1_failure_median_ci95_lower", "k1_failure_median_ci95_upper",
		"k1_failure_mean", "k1_failure_mean_ci95_lower", "k1_failure_mean_ci95_upper",
		"k1_failure_p10", "k1_failure_p90",
		"s25_median", "s25_median_ci95_lower", "s25_median_ci95_upper",
		"s25_mean", "s25_mean_ci95_lower", "s25_mean_ci95_upper",
		"s100_median", "s100_median_ci95_lower", "s100_median_ci95_upper",
		"s100_mean", "s100_mean_ci95_lower", "s100_mean_ci95_upper",
		"b_full_median", "b_full_mean",
		"auc_norm_failures", "auc_norm_bugs",
		"theoretical_expectation_mean",
		"git_commit",
	}
	for i, col := range expectedHeader2 {
		if rows2[0][i] != col {
			t.Fatalf("summary header col %d: expected %q, got %q", i, col, rows2[0][i])
		}
	}

	for _, row := range rows2[1:] {
		fid := row[0]
		strat := row[1]
		if strings.HasSuffix(fid, "/MUT-0") {
			if row[2] != "0.00" || row[5] != "0.00" {
				t.Errorf("%s %s had non-zero failures: median=%s, mean=%s", fid, strat, row[2], row[5])
			}
		} else if strings.HasSuffix(fid, "/MUT-1") {
			if strat == "Durability-Contract Guided (DCG)" {
				k1Med, _ := strconv.ParseFloat(row[2], 64)
				if k1Med <= 0 {
					t.Errorf("%s DCG expected k1 median > 0, got %f", fid, k1Med)
				}
			}
		}
	}
	t.Logf("Validated %s (%d rows) and %s (%d rows) successfully", perSeedPath, len(rows1), summaryPath, len(rows2))
}

// TestWorkloadDiversityBenchmark evaluates 5 presets across 16 seeds (1..16) and 4 fault models (MUT-0, MUT-1, MUT-5, MUT-9)
// for 5 strategies (b0, r2, c1, dcg, dcg-pr).
// When -update-artifacts is false, it performs fast read-only validation of committed CSVs.
func TestWorkloadDiversityBenchmark(t *testing.T) {
	perSeedPath := filepath.Join(diversityResultsDir(), "workload_diversity_per_seed.csv")
	summaryPath := filepath.Join(diversityResultsDir(), "workload_diversity_summary.csv")

	if !*updateArtifacts {
		validateWorkloadDiversityCSVs(t, perSeedPath, summaryPath)
		return
	}

	gitCommit := CurrentGitCommit()
	presets := []string{
		workload.PresetYCSBA,
		workload.PresetYCSBB,
		workload.PresetYCSBF,
		workload.PresetOverwriteHeavy,
		workload.PresetDeleteHeavy,
	}

	seeds := make([]uint64, 16)
	for i := range seeds {
		seeds[i] = uint64(i + 1)
	}

	faultsList := []string{
		faults.MUT0,
		faults.MUT1,
		faults.MUT5,
		faults.MUT9,
	}

	if err := os.MkdirAll(diversityResultsDir(), 0755); err != nil {
		t.Fatalf("mkdir results dir: %v", err)
	}

	fSeed, err := os.Create(perSeedPath)
	if err != nil {
		t.Fatalf("create per-seed csv: %v", err)
	}
	defer fSeed.Close()
	wSeed := csv.NewWriter(fSeed)
	wSeed.UseCRLF = false
	defer wSeed.Flush()

	header1 := []string{
		"fault_id", "seed", "universe_size", "total_failures", "fault_density_pct",
		"strategy_code", "strategy", "k1_single", "s25_single",
		"k1_failure_mean", "s25_mean", "s100_mean", "b_full_mean",
		"auc_norm_failures", "auc_norm_bugs", "git_commit",
	}
	if err := wSeed.Write(header1); err != nil {
		t.Fatalf("write per-seed header: %v", err)
	}

	fSummary, err := os.Create(summaryPath)
	if err != nil {
		t.Fatalf("create summary csv: %v", err)
	}
	defer fSummary.Close()
	wSummary := csv.NewWriter(fSummary)
	wSummary.UseCRLF = false
	defer wSummary.Flush()

	header2 := []string{
		"fault_id", "strategy",
		"k1_failure_median", "k1_failure_median_ci95_lower", "k1_failure_median_ci95_upper",
		"k1_failure_mean", "k1_failure_mean_ci95_lower", "k1_failure_mean_ci95_upper",
		"k1_failure_p10", "k1_failure_p90",
		"s25_median", "s25_median_ci95_lower", "s25_median_ci95_upper",
		"s25_mean", "s25_mean_ci95_lower", "s25_mean_ci95_upper",
		"s100_median", "s100_median_ci95_lower", "s100_median_ci95_upper",
		"s100_mean", "s100_mean_ci95_lower", "s100_mean_ci95_upper",
		"b_full_median", "b_full_mean",
		"auc_norm_failures", "auc_norm_bugs",
		"theoretical_expectation_mean",
		"git_commit",
	}
	if err := wSummary.Write(header2); err != nil {
		t.Fatalf("write summary header: %v", err)
	}

	for _, p := range presets {
		for _, fid := range faultsList {
			settingID := p + "/" + fid
			t.Logf("Running setting %s across 16 seeds...", settingID)
			var runs []SingleRunResult

			for _, seed := range seeds {
				benchCfg := BenchmarkConfig{
					Workload: workload.Config{
						Preset:     p,
						Operations: 25,
						SyncEvery:  5,
						FlushEvery: 10,
						Seed:       seed,
					},
					Policies:        []int{0, 100},
					SafeOpenTimeout: 3 * time.Second,
					B0PercentileCnt: 50,
					Budgets:         []int{0, 1, 5, 10, 25},
				}

				res, err := RunSingleSeedFault(seed, fid, benchCfg)
				if err != nil {
					t.Fatalf("preset %s seed %d fault %s: RunSingleSeedFault failed: %v", p, seed, fid, err)
				}
				runs = append(runs, *res)

				densPct := 0.0
				if res.UniverseSize > 0 {
					densPct = float64(res.TotalFailures) / float64(res.UniverseSize) * 100.0
				}

				for _, spec := range diversityStratSpecs {
					var k1Single, s25Single int
					var k1Mean, s25Mean, s100Mean, bFullMean, aucF, aucB float64

					if spec.code == "b0" {
						k1Single = res.B0.K1SingleSeed
						s25Single = res.B0.S25SingleSeed
						k1Mean = res.B0.K1FailureMedian
						s25Mean = res.B0.S25Median
						s100Mean = res.B0.S100Median
						bFullMean = res.B0.BFullMedian
						aucF = res.B0.AUCNormFailures
						aucB = res.B0.AUCNormBugs
					} else {
						m := res.Strategies[spec.resKey]
						k1Single = m.K1SingleSeed
						s25Single = m.S25SingleSeed
						k1Mean = m.K1FailureFloat
						s25Mean = m.S25Float
						s100Mean = m.S100Float
						bFullMean = m.BFullFloat
						aucF = m.AUCNormFailures
						aucB = m.AUCNormBugs
					}

					row := []string{
						settingID,
						fmt.Sprintf("%d", seed),
						fmt.Sprintf("%d", res.UniverseSize),
						fmt.Sprintf("%d", res.TotalFailures),
						fmt.Sprintf("%.2f", densPct),
						spec.code,
						spec.displayName,
						fmt.Sprintf("%d", k1Single),
						fmt.Sprintf("%d", s25Single),
						fmt.Sprintf("%.2f", k1Mean),
						fmt.Sprintf("%.2f", s25Mean),
						fmt.Sprintf("%.2f", s100Mean),
						fmt.Sprintf("%.2f", bFullMean),
						fmt.Sprintf("%.4f", aucF),
						fmt.Sprintf("%.4f", aucB),
						gitCommit,
					}
					if err := wSeed.Write(row); err != nil {
						t.Fatalf("write per-seed row: %v", err)
					}
				}
			}

			// Compute summary metrics for this preset/fault setting
			for _, spec := range diversityStratSpecs {
				var k1Vals, s25Vals, s100Vals, bFullVals, aucFVals, aucBVals, theoVals []float64

				for _, r := range runs {
					var k1Val, s25Val, s100Val, bfVal, aF, aB float64

					if spec.code == "b0" {
						if r.TotalFailures > 0 {
							k1Val = r.B0.K1FailureMedian
							bfVal = r.B0.BFullMedian
						}
						s25Val = r.B0.S25Median
						s100Val = r.B0.S100Median
						aF = r.B0.AUCNormFailures
						aB = r.B0.AUCNormBugs
					} else {
						m := r.Strategies[spec.resKey]
						if r.TotalFailures > 0 {
							k1Val = m.K1FailureFloat
							bfVal = m.BFullFloat
						}
						s25Val = m.S25Float
						s100Val = m.S100Float
						aF = m.AUCNormFailures
						aB = m.AUCNormBugs
					}

					if r.TotalFailures > 0 && k1Val > 0 {
						k1Vals = append(k1Vals, k1Val)
						bFullVals = append(bFullVals, bfVal)
					}
					s25Vals = append(s25Vals, s25Val)
					s100Vals = append(s100Vals, s100Val)
					aucFVals = append(aucFVals, aF)
					aucBVals = append(aucBVals, aB)
					theoVals = append(theoVals, r.TheoreticalExpectation)
				}

				var medK1F, k1FMedLow, k1FMedHigh float64
				var meanK1F, k1FMeanLow, k1FMeanHigh float64
				var p10K1F, p90K1F float64
				var medBF, meanBF float64

				if len(k1Vals) > 0 {
					medK1F, k1FMedLow, k1FMedHigh = BootstrapMedianCI95(k1Vals, 10000, 42)
					meanK1F, k1FMeanLow, k1FMeanHigh = StudentTCI95(k1Vals)
					sortedK1 := append([]float64{}, k1Vals...)
					sort.Float64s(sortedK1)
					p10K1F = percentile(sortedK1, 10)
					p90K1F = percentile(sortedK1, 90)
					medBF = median(bFullVals)
					meanBF = average(bFullVals)
				}

				medS25, s25MedLow, s25MedHigh := BootstrapMedianCI95(s25Vals, 10000, 42)
				meanS25, s25MeanLow, s25MeanHigh := StudentTCI95(s25Vals)

				medS100, s100MedLow, s100MedHigh := BootstrapMedianCI95(s100Vals, 10000, 42)
				meanS100, s100MeanLow, s100MeanHigh := StudentTCI95(s100Vals)

				meanAUCF := average(aucFVals)
				meanAUCB := average(aucBVals)
				meanTheo := average(theoVals)

				row := []string{
					settingID,
					spec.summaryName,
					fmt.Sprintf("%.2f", medK1F),
					fmt.Sprintf("%.2f", k1FMedLow),
					fmt.Sprintf("%.2f", k1FMedHigh),
					fmt.Sprintf("%.2f", meanK1F),
					fmt.Sprintf("%.2f", k1FMeanLow),
					fmt.Sprintf("%.2f", k1FMeanHigh),
					fmt.Sprintf("%.2f", p10K1F),
					fmt.Sprintf("%.2f", p90K1F),
					fmt.Sprintf("%.2f", medS25),
					fmt.Sprintf("%.2f", s25MedLow),
					fmt.Sprintf("%.2f", s25MedHigh),
					fmt.Sprintf("%.2f", meanS25),
					fmt.Sprintf("%.2f", s25MeanLow),
					fmt.Sprintf("%.2f", s25MeanHigh),
					fmt.Sprintf("%.2f", medS100),
					fmt.Sprintf("%.2f", s100MedLow),
					fmt.Sprintf("%.2f", s100MedHigh),
					fmt.Sprintf("%.2f", meanS100),
					fmt.Sprintf("%.2f", s100MeanLow),
					fmt.Sprintf("%.2f", s100MeanHigh),
					fmt.Sprintf("%.2f", medBF),
					fmt.Sprintf("%.2f", meanBF),
					fmt.Sprintf("%.4f", meanAUCF),
					fmt.Sprintf("%.4f", meanAUCB),
					fmt.Sprintf("%.2f", meanTheo),
					gitCommit,
				}
				if err := wSummary.Write(row); err != nil {
					t.Fatalf("write summary row: %v", err)
				}
			}
		}
	}
}
