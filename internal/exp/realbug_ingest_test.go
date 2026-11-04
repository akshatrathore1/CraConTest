package exp

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func init() {
	if flag.Lookup("update-artifacts") == nil {
		flag.BoolVar(updateArtifacts, "update-artifacts", false, "alias for -update")
	}
}

func realbugIngestResultsDir() string {
	dir := filepath.Join("..", "..", "results")
	if fi, err := os.Stat(dir); err == nil && fi.IsDir() {
		return dir
	}
	return "results"
}

func runIngestTrial(t *testing.T, seed uint64, barrierIngest bool, policies []int) (
	*graph.View,
	[]candidate.Candidate,
	[]oracle.Outcome,
	[]oracle.Outcome,
	int,
	[]trace.Event,
) {
	t.Helper()

	wCfg := workload.Config{
		Seed:          seed,
		Operations:    25,
		KeyCount:      16,
		ValueBytes:    64,
		SyncEvery:     4,
		FlushEvery:    12,
		IngestEvery:   5,
		BarrierIngest: barrierIngest,
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	fCfg := faults.DefaultConfig(faults.MUT0)
	faultFS, err := faults.NewWithConfig(fCfg, rec)
	if err != nil {
		t.Fatalf("seed %d barrier=%v: init fault MUT-0 failed: %v", seed, barrierIngest, err)
	}

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                          faultFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		EventListener:               &listener,
		DisableAutomaticCompactions: true,
		DisableTableStats:           true,
		L0CompactionThreshold:       1000,
		L0StopWritesThreshold:       1000,
	}
	wCfg.FS = faultFS

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("seed %d barrier=%v: open pebble failed: %v", seed, barrierIngest, err)
	}

	rawHist, err := workload.Run(db, rec, wCfg)
	if err != nil {
		_ = db.Close()
		t.Fatalf("seed %d barrier=%v: run workload failed: %v", seed, barrierIngest, err)
	}
	db.TestOnlyWaitForCleaning()
	if err := db.Close(); err != nil {
		t.Fatalf("seed %d barrier=%v: close pebble failed: %v", seed, barrierIngest, err)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("seed %d barrier=%v: validate trace failed: %v", seed, barrierIngest, err)
	}

	correlated, err := workload.Correlate(rawHist, traceEvents)
	if err != nil {
		t.Fatalf("seed %d barrier=%v: correlate history failed: %v", seed, barrierIngest, err)
	}

	universe, err := candidate.Build(traceEvents, seed, policies)
	if err != nil {
		t.Fatalf("seed %d barrier=%v: build universe failed: %v", seed, barrierIngest, err)
	}

	view, err := graph.Build(traceEvents)
	if err != nil {
		t.Fatalf("seed %d barrier=%v: build graph failed: %v", seed, barrierIngest, err)
	}

	expRes, err := RunExplore(RunnerConfig{
		Universe: universe,
		Events:   traceEvents,
		History:  correlated,
		Dir:      "db",
		Timeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("seed %d barrier=%v: run explore failed: %v", seed, barrierIngest, err)
	}

	normOutcomes := NormalizeTable(expRes.Outcomes)

	var prefixViols int
	for _, o := range expRes.Outcomes {
		if o.Class == oracle.ClassViolation && (o.Rule == "prefix" || o.Rule == "prefix_consistency") {
			prefixViols++
		}
	}

	return view, universe, expRes.Outcomes, normOutcomes, prefixViols, traceEvents
}

// TestRealBugIngestABSmoke verifies the Direct L0 Ingest prefix-hole failure on Seed 1 at ret=0.00
// under MUT-0 (BarrierIngest: false), and asserts 0 violations under BARRIER-INGEST (BarrierIngest: true).
func TestRealBugIngestABSmoke(t *testing.T) {
	// Condition A: MUT-0 (BarrierIngest: false), ret=0.00
	viewA, universeA, _, normOutcomesA, prefixViolsA, _ := runIngestTrial(t, 1, false, []int{0})
	if prefixViolsA == 0 {
		t.Fatalf("Condition A (MUT-0): expected prefix violations > 0, got 0")
	}

	dcgStrat := NewDCGStrategy()
	rankedA := dcgStrat.Rank(viewA, universeA)
	if err := strategy.ValidatePermutation(universeA, rankedA); err != nil {
		t.Fatalf("validate DCG permutation: %v", err)
	}
	k1A := CalculateK1(rankedA, normOutcomesA, false)
	if k1A <= 0 || k1A > 25 {
		t.Fatalf("Condition A: expected DCG k1 in [1, 25], got %d", k1A)
	}
	t.Logf("Condition A (MUT-0): prefix violations=%d, DCG k1=%d (PASS)", prefixViolsA, k1A)

	// Condition B: BARRIER-INGEST (BarrierIngest: true), ret=0.00
	_, _, rawOutcomesB, _, prefixViolsB, _ := runIngestTrial(t, 1, true, []int{0})
	totalFailuresB := 0
	for _, o := range rawOutcomesB {
		if o.Class != oracle.ClassValid {
			totalFailuresB++
		}
	}
	if totalFailuresB != 0 || prefixViolsB != 0 {
		t.Fatalf("Condition B (BARRIER-INGEST): expected 0 violations, got totalFailures=%d, prefixViols=%d", totalFailuresB, prefixViolsB)
	}
	t.Logf("Condition B (BARRIER-INGEST): totalFailures=%d, prefix violations=%d (PASS)", totalFailuresB, prefixViolsB)
}

type ingestStratSpec struct {
	code        string
	resKey      string
	displayName string
	summaryName string
}

var ingestStratSpecs = []ingestStratSpec{
	{"dcg", "dcg", "dcg", "Durability-Contract Guided (DCG)"},
	{"dcg-pr", "dcg-pr", "dcg-pr", "DCG Partial-Retention Prior (DCG-PR)"},
	{"dcg-ablate", "dcg-flag-only", "dcg-flag-only", "DCG Flag-Only (Ablation)"},
	{"r2", "r2", "r2", "State-Representative Random (R2)"},
	{"r1", "r1", "r1", "0%-Retention-First Random (R1)"},
	{"c1", "b6", "sync-boundary", "sync-boundary"},
	{"b0", "b0", "Uniform Random (B0)", "Uniform Random (B0)"},
	{"b4", "b4", "combined", "combined"},
	{"b1", "b1", "fifo", "fifo"},
	{"b2", "b2", "lifo", "lifo"},
	{"b3", "b3", "stride", "stride"},
}

func validateRealBugIngestCSVs(t *testing.T, perSeedPath, summaryPath string) {
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
	if len(rows1) != 705 {
		t.Fatalf("expected 705 rows in %s, got %d", perSeedPath, len(rows1))
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
	if len(rows2) != 23 {
		t.Fatalf("expected 23 rows in %s, got %d", summaryPath, len(rows2))
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
		if fid == "BARRIER-INGEST" {
			if len(strat) == 0 {
				t.Errorf("BARRIER-INGEST has empty strategy")
			}
			if row[2] != "0.00" {
				t.Errorf("BARRIER-INGEST strategy %s expected k1_failure_median == 0.00, got %s", strat, row[2])
			}
			if row[5] != "0.00" {
				t.Errorf("BARRIER-INGEST strategy %s expected k1_failure_mean == 0.00, got %s", strat, row[5])
			}
		} else if fid == "MUT-0" {
			if strat == "Durability-Contract Guided (DCG)" {
				k1Med, _ := strconv.ParseFloat(row[2], 64)
				if k1Med <= 0 {
					t.Errorf("MUT-0 DCG expected k1 median > 0, got %f", k1Med)
				}
			}
		}
	}
	t.Logf("Validated %s (%d rows) and %s (%d rows) successfully", perSeedPath, len(rows1), summaryPath, len(rows2))
}

// TestRealBugIngestBenchmark evaluates the Direct L0 Ingest prefix race across 32 canonical seeds (1..32)
// under MUT-0 (BarrierIngest: false) and BARRIER-INGEST (BarrierIngest: true) across 11 strategies.
// When -update-artifacts is false, it performs fast read-only validation of committed CSVs.
func TestRealBugIngestBenchmark(t *testing.T) {
	perSeedPath := filepath.Join(realbugIngestResultsDir(), "realbug_ingest_per_seed.csv")
	summaryPath := filepath.Join(realbugIngestResultsDir(), "realbug_ingest_summary.csv")

	if !*updateArtifacts {
		validateRealBugIngestCSVs(t, perSeedPath, summaryPath)
		return
	}

	gitCommit := CurrentGitCommit()
	seeds := make([]uint64, 32)
	for i := range seeds {
		seeds[i] = uint64(i + 1)
	}

	conditions := []struct {
		id            string
		barrierIngest bool
	}{
		{"MUT-0", false},
		{"BARRIER-INGEST", true},
	}

	if err := os.MkdirAll(realbugIngestResultsDir(), 0755); err != nil {
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

	for _, cond := range conditions {
		t.Logf("Running condition %s (barrierIngest=%v) across 32 seeds...", cond.id, cond.barrierIngest)
		var runs []SingleRunResult

		for _, seed := range seeds {
			benchCfg := BenchmarkConfig{
				Workload: workload.Config{
					Seed:          seed,
					Operations:    25,
					KeyCount:      16,
					ValueBytes:    64,
					SyncEvery:     4,
					FlushEvery:    12,
					IngestEvery:   5,
					BarrierIngest: cond.barrierIngest,
				},
				Policies:        []int{0, 100},
				SafeOpenTimeout: 3 * time.Second,
				B0PercentileCnt: 50,
				Budgets:         []int{0, 1, 5, 10, 25},
			}

			res, err := RunSingleSeedFault(seed, faults.MUT0, benchCfg)
			if err != nil {
				t.Fatalf("seed %d cond %s: RunSingleSeedFault failed: %v", seed, cond.id, err)
			}
			runs = append(runs, *res)

			densPct := 0.0
			if res.UniverseSize > 0 {
				densPct = float64(res.TotalFailures) / float64(res.UniverseSize) * 100.0
			}

			for _, spec := range ingestStratSpecs {
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
					cond.id,
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

		// Compute summary metrics for this condition
		for _, spec := range ingestStratSpecs {
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
				cond.id,
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
