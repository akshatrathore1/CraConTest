package exp

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
		_, _ = safeCloseDB(db)
		t.Fatalf("seed %d barrier=%v: run workload failed: %v", seed, barrierIngest, err)
	}
	db.TestOnlyWaitForCleaning()
	if panicked, closeErr := safeCloseDB(db); panicked || closeErr != nil {
		t.Fatalf("seed %d barrier=%v: close pebble failed (panic=%v): %v", seed, barrierIngest, panicked, closeErr)
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
	if expRes.Summary.OracleErrors != 0 {
		t.Fatalf("seed %d barrier=%v: expected 0 oracle_errors, got %d", seed, barrierIngest, expRes.Summary.OracleErrors)
	}

	normOutcomes := NormalizeTable(expRes.Outcomes)
	if err := ValidateTable(universe, normOutcomes); err != nil {
		t.Fatalf("seed %d barrier=%v: ValidateTable failed: %v", seed, barrierIngest, err)
	}

	var prefixViols int
	for _, o := range expRes.Outcomes {
		if o.Class == oracle.ClassViolation && (o.Rule == "prefix" || o.Rule == "prefix_consistency") {
			prefixViols++
		}
	}

	return view, universe, expRes.Outcomes, normOutcomes, prefixViols, traceEvents
}

func assertDirectL0IngestPath(t *testing.T, events []trace.Event) int {
	t.Helper()
	inIngest := false
	var curEvents []trace.Event
	directL0Count := 0
	ingestOps := 0

	for _, e := range events {
		if e.Kind == trace.KindOpBegin && e.OpKind == "ingest" {
			inIngest = true
			curEvents = nil
			continue
		}
		if e.Kind == trace.KindOpEnd && e.OpKind == "ingest" {
			inIngest = false
			ingestOps++
			hasLink := false
			hasManifestSync := false
			hasFlush := false
			for _, ce := range curEvents {
				if ce.Kind == trace.KindLink && strings.HasPrefix(ce.File2, "db/") && strings.Contains(ce.File2, ".sst") {
					hasLink = true
				}
				if ce.Kind == trace.KindSync && strings.Contains(ce.File, "MANIFEST") {
					hasManifestSync = true
				}
				if ce.Kind == trace.KindEngineEvent && (ce.Note == "FlushBegin" || strings.Contains(ce.Note, "Flush")) {
					hasFlush = true
				}
			}
			if !hasLink || !hasManifestSync {
				t.Errorf("ingest op #%d missing SST link or MANIFEST sync: hasLink=%v hasManifestSync=%v",
					ingestOps, hasLink, hasManifestSync)
			} else if !hasFlush {
				directL0Count++
			}
			continue
		}
		if inIngest {
			curEvents = append(curEvents, e)
		}
	}
	if ingestOps == 0 || directL0Count == 0 {
		t.Fatalf("expected direct L0 ingest ops > 0, got ingestOps=%d directL0Count=%d", ingestOps, directL0Count)
	}
	return directL0Count
}

// TestCaseStudyIngestABSmoke verifies the Direct L0 Ingest prefix-hole failure on Seed 1 at ret=0.00
// under MUT-0 (BarrierIngest: false), and asserts 0 violations under BARRIER-INGEST (BarrierIngest: true).
func TestCaseStudyIngestABSmoke(t *testing.T) {
	// Condition A: MUT-0 (BarrierIngest: false), ret=0.00
	viewA, universeA, rawOutcomesA, normOutcomesA, prefixViolsA, eventsA := runIngestTrial(t, 1, false, []int{0})
	if prefixViolsA == 0 {
		t.Fatalf("Condition A (MUT-0): expected prefix violations > 0, got 0")
	}
	for _, o := range rawOutcomesA {
		if o.Class == oracle.ClassOracleError || o.Class == oracle.ClassPanic || o.Class == oracle.ClassHang {
			t.Fatalf("Condition A (MUT-0): unexpected non-prefix error outcome %+v", o)
		}
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
	_, _, rawOutcomesB, _, prefixViolsB, eventsB := runIngestTrial(t, 1, true, []int{0})
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

	// Assert that Condition A and Condition B execute the exact same number of Direct L0 (asFlushable == false) ingests.
	directL0CountA := assertDirectL0IngestPath(t, eventsA)
	directL0CountB := assertDirectL0IngestPath(t, eventsB)
	if directL0CountA != directL0CountB || directL0CountB <= 0 {
		t.Fatalf("expected identical direct L0 ingest counts > 0, got Condition A=%d, Condition B=%d", directL0CountA, directL0CountB)
	}
	t.Logf("Direct L0 ingest operations executed: Condition A=%d, Condition B=%d (PASS)", directL0CountA, directL0CountB)
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
	{"dcg-flag-only", "dcg-flag-only", "dcg-flag-only", "DCG Flag-Only (Ablation)"},
	{"r2", "r2", "r2", "State-Representative Random (R2)"},
	{"r1", "r1", "r1", "0%-Retention-First Random (R1)"},
	{"b6", "b6", "sync-boundary", "sync-boundary"},
	{"b0", "b0", "Uniform Random (B0)", "Uniform Random (B0)"},
	{"b4", "b4", "combined", "combined"},
	{"b1", "b1", "fifo", "fifo"},
	{"b2", "b2", "lifo", "lifo"},
	{"b3", "b3", "stride", "stride"},
}

func validateCaseStudyIngestCSVs(t *testing.T, perSeedPath, summaryPath string) {
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
	if len(rows1[0]) != len(expectedHeader1) {
		t.Fatalf("per-seed header length = %d, want %d", len(rows1[0]), len(expectedHeader1))
	}
	for i, col := range expectedHeader1 {
		if rows1[0][i] != col {
			t.Fatalf("per-seed header col %d: expected %q, got %q", i, col, rows1[0][i])
		}
	}
	for rowIdx, row := range rows1[1:] {
		if len(row) != len(expectedHeader1) {
			t.Fatalf("per-seed row %d length = %d, want %d", rowIdx+1, len(row), len(expectedHeader1))
		}
		if row[0] != "MUT-0" && row[0] != "BARRIER-INGEST" {
			t.Fatalf("per-seed row %d unexpected fault_id %q", rowIdx+1, row[0])
		}
		univ, err := strconv.Atoi(row[2])
		if err != nil || univ <= 0 {
			t.Fatalf("per-seed row %d invalid universe_size %q", rowIdx+1, row[2])
		}
		fails, err := strconv.Atoi(row[3])
		if err != nil || fails < 0 {
			t.Fatalf("per-seed row %d invalid total_failures %q", rowIdx+1, row[3])
		}
		if row[0] == "BARRIER-INGEST" && fails != 0 {
			t.Fatalf("per-seed row %d BARRIER-INGEST expected total_failures == 0, got %d", rowIdx+1, fails)
		}
		for _, colIdx := range []int{4, 7, 8, 9, 10, 11, 12, 13, 14} {
			v, err := strconv.ParseFloat(row[colIdx], 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
				t.Fatalf("per-seed row %d col %d (%s=%q) invalid", rowIdx+1, colIdx, expectedHeader1[colIdx], row[colIdx])
			}
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
	if len(rows2[0]) != len(expectedHeader2) {
		t.Fatalf("summary header length = %d, want %d", len(rows2[0]), len(expectedHeader2))
	}
	for i, col := range expectedHeader2 {
		if rows2[0][i] != col {
			t.Fatalf("summary header col %d: expected %q, got %q", i, col, rows2[0][i])
		}
	}

	for rowIdx, row := range rows2[1:] {
		if len(row) != len(expectedHeader2) {
			t.Fatalf("summary row %d length = %d, want %d", rowIdx+1, len(row), len(expectedHeader2))
		}
		fid := row[0]
		strat := row[1]
		if len(strat) == 0 {
			t.Errorf("summary row %d: empty strategy", rowIdx+1)
		}
		for colIdx := 2; colIdx < len(expectedHeader2)-1; colIdx++ {
			v, err := strconv.ParseFloat(row[colIdx], 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
				t.Fatalf("summary row %d col %d (%s=%q) invalid", rowIdx+1, colIdx, expectedHeader2[colIdx], row[colIdx])
			}
		}
		s25Mean, _ := strconv.ParseFloat(row[13], 64)
		s25MeanHi, _ := strconv.ParseFloat(row[15], 64)
		s100Mean, _ := strconv.ParseFloat(row[19], 64)
		if s25Mean > 25.0 || s25MeanHi > 25.0 {
			t.Errorf("%s %s s25 exceeds cap 25: mean=%.2f upper=%.2f", fid, strat, s25Mean, s25MeanHi)
		}
		if s100Mean > 100.0 {
			t.Errorf("%s %s s100 exceeds cap 100: mean=%.2f", fid, strat, s100Mean)
		}
		switch fid {
		case "BARRIER-INGEST":
			if row[2] != "0.00" || row[5] != "0.00" {
				t.Errorf("BARRIER-INGEST strategy %s expected k1_failure median/mean == 0.00, got %s / %s", strat, row[2], row[5])
			}
		case "MUT-0":
			k1Med, _ := strconv.ParseFloat(row[2], 64)
			k1Mean, _ := strconv.ParseFloat(row[5], 64)
			if k1Med <= 0 || k1Mean <= 0 {
				t.Errorf("MUT-0 strategy %s expected positive k1 median/mean, got %.2f / %.2f", strat, k1Med, k1Mean)
			}
		default:
			t.Errorf("summary row %d unexpected fault_id %q", rowIdx+1, fid)
		}
	}
	t.Logf("Validated %s (%d rows) and %s (%d rows) successfully", perSeedPath, len(rows1), summaryPath, len(rows2))
}

func writeAndRunIngestSuite(t *testing.T, perSeedPath, summaryPath string, seeds []uint64, policies []int, b0Shuffles int) {
	t.Helper()

	gitCommit := CurrentGitCommit()
	conditions := []struct {
		id            string
		barrierIngest bool
	}{
		{"MUT-0", false},
		{"BARRIER-INGEST", true},
	}

	var perSeedRows [][]string
	var summaryRows [][]string

	for _, cond := range conditions {
		t.Logf("Running condition %s (barrierIngest=%v, policies=%v) across %d seeds...", cond.id, cond.barrierIngest, policies, len(seeds))
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
				Policies:        policies,
				SafeOpenTimeout: 3 * time.Second,
				B0PercentileCnt: b0Shuffles,
				Budgets:         []int{0, 1, 5, 10, 25},
			}

			res, err := RunSingleSeedFault(seed, faults.MUT0, benchCfg)
			if err != nil {
				t.Fatalf("seed %d cond %s: RunSingleSeedFault failed: %v", seed, cond.id, err)
			}
			if cond.barrierIngest && res.TotalFailures != 0 {
				t.Fatalf("seed %d BARRIER-INGEST: expected 0 failures, got %d", seed, res.TotalFailures)
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
					k1Mean = res.B0.K1FailureMean
					s25Mean = res.B0.S25Mean
					s100Mean = res.B0.S100Mean
					bFullMean = res.B0.BFullMean
					aucF = res.B0.AUCNormFailures
					aucB = res.B0.AUCNormBugs
				} else {
					m, ok := res.Strategies[spec.resKey]
					if !ok {
						t.Fatalf("seed %d cond %s missing strategy %q", seed, cond.id, spec.resKey)
					}
					k1Single = m.K1SingleSeed
					s25Single = m.S25SingleSeed
					k1Mean = m.K1FailureFloat
					s25Mean = m.S25Float
					s100Mean = m.S100Float
					bFullMean = m.BFullFloat
					aucF = m.AUCNormFailures
					aucB = m.AUCNormBugs
				}

				perSeedRows = append(perSeedRows, []string{
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
				})
			}
		}

		if !cond.barrierIngest && len(runs) > 0 {
			activeCount := 0
			for _, r := range runs {
				if r.TotalFailures > 0 {
					activeCount++
				}
			}
			if activeCount == 0 {
				t.Fatalf("condition %s: expected at least one active failing seed", cond.id)
			}
		}

		// Align active-seed denominator across all ranking metrics and theoretical expectation.
		meanTheo := activeTheoreticalExpectationMean(runs)
		activeOnly := false
		for _, r := range runs {
			if r.TotalFailures > 0 {
				activeOnly = true
				break
			}
		}

		for _, spec := range ingestStratSpecs {
			var k1Vals, s25Vals, s100Vals, bFullVals, aucFVals, aucBVals []float64
			var k1Clusters, s25Clusters, s100Clusters, bFullClusters [][]float64

			for _, r := range runs {
				if activeOnly && r.TotalFailures == 0 {
					continue
				}
				var k1Val, s25Val, s100Val, bfVal, aF, aB float64
				var k1Samp, s25Samp, s100Samp, bfSamp []float64

				if spec.code == "b0" {
					k1Val = r.B0.K1FailureMean
					bfVal = r.B0.BFullMean
					s25Val = r.B0.S25Mean
					s100Val = r.B0.S100Mean
					aF = r.B0.AUCNormFailures
					aB = r.B0.AUCNormBugs
					k1Samp = r.B0.K1FailureSamples
					s25Samp = r.B0.S25Samples
					s100Samp = r.B0.S100Samples
					bfSamp = r.B0.BFullSamples
				} else {
					m := r.Strategies[spec.resKey]
					k1Val, _, bfVal, s25Val, s100Val = resolveStrategyMetrics(m)
					aF = m.AUCNormFailures
					aB = m.AUCNormBugs
					k1Samp = m.K1FailureSamples
					s25Samp = m.S25Samples
					s100Samp = m.S100Samples
					bfSamp = m.BFullSamples
				}

				if r.TotalFailures > 0 && k1Val > 0 {
					k1Vals = append(k1Vals, k1Val)
					bFullVals = append(bFullVals, bfVal)
					if len(k1Samp) > 0 {
						k1Clusters = append(k1Clusters, k1Samp)
					}
					if len(bfSamp) > 0 {
						bFullClusters = append(bFullClusters, bfSamp)
					}
				}
				if activeOnly {
					s25Vals = append(s25Vals, s25Val)
					s100Vals = append(s100Vals, s100Val)
					if len(s25Samp) > 0 {
						s25Clusters = append(s25Clusters, s25Samp)
					}
					if len(s100Samp) > 0 {
						s100Clusters = append(s100Clusters, s100Samp)
					}
					aucFVals = append(aucFVals, aF)
					aucBVals = append(aucBVals, aB)
				}
			}

			var medK1F, k1FMedLow, k1FMedHigh, meanK1F, k1FMeanLow, k1FMeanHigh, p10K1F, p90K1F float64
			var medBF, meanBF float64
			var medS25, s25MedLow, s25MedHigh, meanS25, s25MeanLow, s25MeanHigh float64
			var medS100, s100MedLow, s100MedHigh, meanS100, s100MeanLow, s100MeanHigh float64

			if len(k1Clusters) > 0 && len(k1Clusters) == len(k1Vals) {
				meanK1F, k1FMeanLow, k1FMeanHigh, medK1F, k1FMedLow, k1FMedHigh, p10K1F, p90K1F =
					ClusterBootstrapCI95(k1Clusters, 2000, 42, 1.0, math.Inf(1))
			} else if len(k1Vals) > 0 {
				medK1F, k1FMedLow, k1FMedHigh = BootstrapMedianCI95(k1Vals, 2000, 42)
				meanK1F, k1FMeanLow, k1FMeanHigh = StudentTCI95Range(k1Vals, 1.0, math.Inf(1))
				sortedK1 := append([]float64{}, k1Vals...)
				sort.Float64s(sortedK1)
				p10K1F = percentile(sortedK1, 10)
				p90K1F = percentile(sortedK1, 90)
			}
			if len(bFullClusters) > 0 && len(bFullClusters) == len(bFullVals) {
				meanBF, _, _, medBF, _, _, _, _ = ClusterBootstrapCI95(bFullClusters, 0, 42, 0.0, math.Inf(1))
			} else if len(bFullVals) > 0 {
				medBF = median(bFullVals)
				meanBF = average(bFullVals)
			}

			if len(s25Clusters) > 0 && len(s25Clusters) == len(s25Vals) {
				meanS25, s25MeanLow, s25MeanHigh, medS25, s25MedLow, s25MedHigh, _, _ =
					ClusterBootstrapCI95(s25Clusters, 2000, 42, 0.0, 25.0)
			} else if len(s25Vals) > 0 {
				medS25, s25MedLow, s25MedHigh = BootstrapMedianCI95(s25Vals, 2000, 42)
				meanS25, s25MeanLow, s25MeanHigh = StudentTCI95Range(s25Vals, 0.0, 25.0)
			}

			if len(s100Clusters) > 0 && len(s100Clusters) == len(s100Vals) {
				meanS100, s100MeanLow, s100MeanHigh, medS100, s100MedLow, s100MedHigh, _, _ =
					ClusterBootstrapCI95(s100Clusters, 2000, 42, 0.0, 100.0)
			} else if len(s100Vals) > 0 {
				medS100, s100MedLow, s100MedHigh = BootstrapMedianCI95(s100Vals, 2000, 42)
				meanS100, s100MeanLow, s100MeanHigh = StudentTCI95Range(s100Vals, 0.0, 100.0)
			}

			meanAUCF := average(aucFVals)
			meanAUCB := average(aucBVals)

			summaryRows = append(summaryRows, []string{
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
			})
		}
	}

	header1 := []string{
		"fault_id", "seed", "universe_size", "total_failures", "fault_density_pct",
		"strategy_code", "strategy", "k1_single", "s25_single",
		"k1_failure_mean", "s25_mean", "s100_mean", "b_full_mean",
		"auc_norm_failures", "auc_norm_bugs", "git_commit",
	}
	if err := writeCSVAtomic(perSeedPath, func(w *csv.Writer) error {
		if err := w.Write(header1); err != nil {
			return err
		}
		for _, row := range perSeedRows {
			if err := w.Write(row); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write per-seed csv %s: %v", perSeedPath, err)
	}

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
	if err := writeCSVAtomic(summaryPath, func(w *csv.Writer) error {
		if err := w.Write(header2); err != nil {
			return err
		}
		for _, row := range summaryRows {
			if err := w.Write(row); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write summary csv %s: %v", summaryPath, err)
	}
}

// TestCaseStudyIngestBenchmark evaluates the Direct L0 Ingest prefix race across 32 canonical seeds (1..32)
// under MUT-0 (BarrierIngest: false) and BARRIER-INGEST (BarrierIngest: true) across 11 strategies.
// When -update is false, it validates committed CSVs and runs a live 3-policy [0,50,100] smoke matrix into TempDir.
func TestCaseStudyIngestBenchmark(t *testing.T) {
	resultsDir := findResultsDirInternal(t)
	perSeedPath := filepath.Join(resultsDir, "casestudy_ingest_per_seed.csv")
	summaryPath := filepath.Join(resultsDir, "casestudy_ingest_summary.csv")

	if !*updateArtifacts {
		validateCaseStudyIngestCSVs(t, perSeedPath, summaryPath)
		if !testing.Short() {
			tmpDir := t.TempDir()
			livePerSeed := filepath.Join(tmpDir, "live_per_seed.csv")
			liveSummary := filepath.Join(tmpDir, "live_summary.csv")
			writeAndRunIngestSuite(t, livePerSeed, liveSummary, []uint64{1, 2}, []int{0, 50, 100}, 5)
		}
		return
	}

	seeds := make([]uint64, 32)
	for i := range seeds {
		seeds[i] = uint64(i + 1)
	}
	writeAndRunIngestSuite(t, perSeedPath, summaryPath, seeds, []int{0, 100}, 50)
}
