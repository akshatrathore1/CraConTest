package exp_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
)

func TestCalculateAUCNorm(t *testing.T) {
	// Case 1: All bugs found at budget 1 in a universe of 100
	pointsEarly := []exp.Point{
		{Budget: 0, Failures: 0, DistinctBugs: 0},
		{Budget: 1, Failures: 10, DistinctBugs: 5},
		{Budget: 100, Failures: 10, DistinctBugs: 5},
	}
	aucEarly := exp.CalculateAUCNorm(pointsEarly, 100, 10, false)
	// Trapezoid: (0+10)/2 * 1 + 10 * 99 = 5 + 990 = 995. Normalized: 995 / 1000 = 0.995
	if aucEarly < 0.99 || aucEarly > 1.0 {
		t.Errorf("expected early discovery AUC near 0.995, got %f", aucEarly)
	}

	// Case 2: Bugs found only at budget 100
	pointsLate := []exp.Point{
		{Budget: 0, Failures: 0, DistinctBugs: 0},
		{Budget: 99, Failures: 0, DistinctBugs: 0},
		{Budget: 100, Failures: 10, DistinctBugs: 5},
	}
	aucLate := exp.CalculateAUCNorm(pointsLate, 100, 10, false)
	// Trapezoid: (0+10)/2 * 1 = 5. Normalized: 5 / 1000 = 0.005
	if aucLate > 0.01 {
		t.Errorf("expected late discovery AUC near 0.005, got %f", aucLate)
	}

	// Case 3: Empty or zero targets
	if aucZero := exp.CalculateAUCNorm(nil, 100, 0, false); aucZero != 0.0 {
		t.Errorf("expected 0 AUC for 0 targets, got %f", aucZero)
	}
}

func TestCalculateBFull(t *testing.T) {
	c1 := candidate.Candidate{Event: 1, Percent: 0, Seed: 10}
	c2 := candidate.Candidate{Event: 2, Percent: 0, Seed: 20}
	c3 := candidate.Candidate{Event: 3, Percent: 0, Seed: 30}
	c4 := candidate.Candidate{Event: 4, Percent: 0, Seed: 40}

	outcomes := []oracle.Outcome{
		{Candidate: c1, Class: oracle.ClassViolation, Signature: "v1:sigA"},
		{Candidate: c2, Class: oracle.ClassValid},
		{Candidate: c3, Class: oracle.ClassOpenError, Signature: "v1:open_error:sigB"},
		{Candidate: c4, Class: oracle.ClassValid},
	}

	// Ranking 1: [c1, c3, c2, c4] -> sigA at 1, sigB at 2 -> B_full = 2
	r1 := []candidate.Candidate{c1, c3, c2, c4}
	if b := exp.CalculateBFull(r1, outcomes); b != 2 {
		t.Errorf("expected B_full=2, got %d", b)
	}

	// Ranking 2: [c2, c4, c3, c1] -> sigB at 3, sigA at 4 -> B_full = 4
	r2 := []candidate.Candidate{c2, c4, c3, c1}
	if b := exp.CalculateBFull(r2, outcomes); b != 4 {
		t.Errorf("expected B_full=4, got %d", b)
	}

	// Outcomes with zero bug signatures
	validOutcomes := []oracle.Outcome{
		{Candidate: c1, Class: oracle.ClassValid},
		{Candidate: c2, Class: oracle.ClassValid},
	}
	if b := exp.CalculateBFull([]candidate.Candidate{c1, c2}, validOutcomes); b != 0 {
		t.Errorf("expected B_full=0 when no bugs, got %d", b)
	}
}

func TestCalculateK1(t *testing.T) {
	c1 := candidate.Candidate{Event: 1, Percent: 0, Seed: 10}
	c2 := candidate.Candidate{Event: 2, Percent: 0, Seed: 20}
	c3 := candidate.Candidate{Event: 3, Percent: 0, Seed: 30}

	outcomes := []oracle.Outcome{
		{Candidate: c1, Class: oracle.ClassValid},
		{Candidate: c2, Class: oracle.ClassOpenError, Signature: "v1:open_error:manifest_corrupt"},
		{Candidate: c3, Class: oracle.ClassViolation, Signature: "v1:viol1"},
	}

	ranking := []candidate.Candidate{c1, c2, c3}

	// K1 failure (any non-valid): first failure is c2 at index 2
	if k1 := exp.CalculateK1(ranking, outcomes, false); k1 != 2 {
		t.Errorf("expected k1 failure=2, got %d", k1)
	}

	// K1 violation (only ClassViolation): first violation is c3 at index 3
	if k1 := exp.CalculateK1(ranking, outcomes, true); k1 != 3 {
		t.Errorf("expected k1 violation=3, got %d", k1)
	}
}

func TestSingleRun_MUT2_SparseFault_RHGOutperformsB0(t *testing.T) {
	cfg := exp.DefaultBenchmarkConfig()
	cfg.Workload.Seed = 42
	cfg.Workload.Operations = 100
	cfg.WorkloadSeeds = []uint64{42}
	cfg.FaultIDs = []string{faults.MUT2}
	cfg.B0PercentileCnt = 50

	res, err := exp.RunSingleSeedFault(42, faults.MUT2, cfg)
	if err != nil {
		t.Fatalf("RunSingleSeedFault failed: %v", err)
	}

	if res.TotalFailures == 0 {
		t.Fatalf("expected non-zero failures under MUT-2, got %d", res.TotalFailures)
	}

	b7Metrics := res.Strategies["b7"]
	t.Logf("MUT-2 seed 42: Universe=%d, Failures=%d, B0 K1 Failure Median=%.1f, RHG K1 Failure=%d",
		res.UniverseSize, res.TotalFailures, res.B0.K1FailureMedian, b7Metrics.K1Failure)

	// Verify RHG achieves k1 failure <= B0 median
	if b7Metrics.K1Failure > int(res.B0.K1FailureMedian) {
		t.Errorf("expected RHG k1 (%d) <= B0 median (%.1f)", b7Metrics.K1Failure, res.B0.K1FailureMedian)
	}
}

func TestBenchmarkRunner_FastMultiSeed(t *testing.T) {
	cfg := exp.DefaultBenchmarkConfig()
	cfg.WorkloadSeeds = []uint64{42, 100}
	cfg.FaultIDs = []string{faults.MUT1, faults.MUT2}
	cfg.Workload.Operations = 60
	cfg.Workload.FlushEvery = 20
	cfg.Workload.SyncEvery = 6
	cfg.B0PercentileCnt = 20
	cfg.TrackMUT0 = true

	res, err := exp.RunBenchmark(cfg, nil)
	if err != nil {
		t.Fatalf("RunBenchmark failed: %v", err)
	}

	if res.MUT0Violations != 0 {
		t.Errorf("MUT-0 produced %d false alarms, expected 0", res.MUT0Violations)
	}

	if len(res.FaultSummaries) != 2 {
		t.Fatalf("expected 2 fault summaries, got %d", len(res.FaultSummaries))
	}

	// Verify markdown report generation
	reportMD := exp.GenerateEmpiricalReportMarkdown(res)
	if len(reportMD) < 200 {
		t.Fatalf("generated markdown report suspiciously short (%d bytes)", len(reportMD))
	}

	tempDir := t.TempDir()
	outReport := filepath.Join(tempDir, "report_empirical.md")
	if err := exp.WriteEmpiricalReportToFile(res, outReport); err != nil {
		t.Fatalf("WriteEmpiricalReportToFile failed: %v", err)
	}

	outCSV := filepath.Join(tempDir, "summary.csv")
	if err := exp.WriteEmpiricalCSV(res, outCSV); err != nil {
		t.Fatalf("WriteEmpiricalCSV failed: %v", err)
	}

	if _, err := os.Stat(outReport); err != nil {
		t.Errorf("expected report file, stat failed: %v", err)
	}
	if _, err := os.Stat(outCSV); err != nil {
		t.Errorf("expected csv file, stat failed: %v", err)
	}
}

func TestGenerateEmpiricalReportMarkdown_B0PercentileSorted(t *testing.T) {
	// Provide runs with deliberately unsorted P10 and P90 values across seeds: [9.0, 1.0, 5.0]
	// The median of {1.0, 5.0, 9.0} is exactly 5.0.
	runs := []exp.SingleRunResult{
		{Seed: 1, B0: exp.B0Metrics{K1FailureP10: 9.0, K1FailureP90: 19.0}},
		{Seed: 2, B0: exp.B0Metrics{K1FailureP10: 1.0, K1FailureP90: 11.0}},
		{Seed: 3, B0: exp.B0Metrics{K1FailureP10: 5.0, K1FailureP90: 15.0}},
	}

	stratSummaries := make(map[string]exp.AggregatedStrategyStats)
	for _, code := range []string{"b0", "b7", "b6", "b4", "b5", "b1", "b2", "b3"} {
		stratSummaries[code] = exp.AggregatedStrategyStats{
			StrategyName:        code,
			K1FailureMedian:     5.0,
			K1FailureMean:       5.0,
			BFullMedian:         10.0,
			BFullMean:           10.0,
			AUCNormFailuresMean: 0.5,
			AUCNormBugsMean:     0.9,
			CurveMeanFailures:   make([]float64, 2),
			CurveMeanBugs:       make([]float64, 2),
		}
	}

	res := &exp.BenchmarkResult{
		Timestamp: "2026-10-25T00:00:00Z",
		Config: exp.BenchmarkConfig{
			WorkloadSeeds: []uint64{1, 2, 3},
		},
		FaultSummaries: map[string]exp.FaultModelSummary{
			faults.MUT1: {
				FaultID:           faults.MUT1,
				RunCount:          3,
				AvgUniverseSize:   100,
				AvgFailures:       10,
				AvgViolations:     10,
				AvgOpenErrors:     0,
				AvgDistinctBugs:   1,
				FaultDensityPct:   10.0,
				StrategySummaries: stratSummaries,
				Budgets:           []int{0, 1},
				Runs:              runs,
			},
		},
	}

	report := exp.GenerateEmpiricalReportMarkdown(res)

	// Since median of [9.0, 1.0, 5.0] is 5.0 and median of [19.0, 11.0, 15.0] is 15.0,
	// the formatted bracket must be [P10: 5.0, P90: 15.0].
	expectedBracket := "[P10: 5.0, P90: 15.0]"
	if !strings.Contains(report, expectedBracket) {
		t.Fatalf("expected report to contain %q, but got:\n%s", expectedBracket, report)
	}
}

func TestMultiSeed_CalculateS100(t *testing.T) {
	c1 := candidate.Candidate{Event: 1, Percent: 0, Seed: 10}
	c2 := candidate.Candidate{Event: 2, Percent: 0, Seed: 20}
	c3 := candidate.Candidate{Event: 3, Percent: 0, Seed: 30}
	c4 := candidate.Candidate{Event: 4, Percent: 0, Seed: 40}

	outcomes := []oracle.Outcome{
		{Candidate: c1, Class: oracle.ClassViolation},
		{Candidate: c2, Class: oracle.ClassValid},
		{Candidate: c3, Class: oracle.ClassOpenError},
		{Candidate: c4, Class: oracle.ClassViolation},
	}

	ranking := []candidate.Candidate{c1, c2, c3, c4}
	// Without view cuts, falls back to event IDs: event_1, event_3, event_4 -> 3 distinct states
	s100 := exp.CalculateS100(ranking, outcomes, nil)
	if s100 != 3 {
		t.Errorf("expected S100=3, got %d", s100)
	}

	bugs100 := exp.CalculateBugs100(ranking, []oracle.Outcome{
		{Candidate: c1, Class: oracle.ClassViolation, Signature: "sigA"},
		{Candidate: c2, Class: oracle.ClassValid},
		{Candidate: c3, Class: oracle.ClassOpenError, Signature: "sigB"},
		{Candidate: c4, Class: oracle.ClassViolation, Signature: "sigA"},
	})
	if bugs100 != 2 {
		t.Errorf("expected Bugs100=2, got %d", bugs100)
	}
}

func TestMultiSeed_TheoreticalExpectation(t *testing.T) {
	// N = 99, m = 9 -> E[K1] = (99+1)/(9+1) = 100/10 = 10.0
	e := exp.CalculateTheoreticalExpectation(99, 9)
	if e != 10.0 {
		t.Errorf("expected theoretical expectation 10.0, got %f", e)
	}

	// m = 0 -> 0.0
	if e0 := exp.CalculateTheoreticalExpectation(100, 0); e0 != 0.0 {
		t.Errorf("expected 0.0 for m=0, got %f", e0)
	}
}

func TestMultiSeed_StudentTCI95(t *testing.T) {
	vals := []float64{10, 12, 14, 16, 18}
	mean, low, high := exp.StudentTCI95(vals)
	if mean != 14.0 {
		t.Errorf("expected mean 14.0, got %f", mean)
	}
	if low >= mean || high <= mean {
		t.Errorf("expected valid CI range [low < mean < high], got [%f, %f, %f]", low, mean, high)
	}
}

func TestMultiSeed_BootstrapMedianCI95(t *testing.T) {
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	med, low, high := exp.BootstrapMedianCI95(vals, 1000, 42)
	if med != 5.5 {
		t.Errorf("expected median 5.5, got %f", med)
	}
	if low > med || high < med {
		t.Errorf("expected low <= med <= high, got [%f, %f, %f]", low, med, high)
	}
}

func TestMultiSeed_CSV_And_Report_Generation(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "benchmark_summary.csv")
	reportPath := filepath.Join(tmpDir, "benchmark-report.md")

	res := &exp.BenchmarkResult{
		Timestamp: "2026-10-25T00:00:00Z",
		Config: exp.BenchmarkConfig{
			WorkloadSeeds: []uint64{42},
		},
		FaultSummaries: map[string]exp.FaultModelSummary{
			faults.MUT5: {
				FaultID:         faults.MUT5,
				RunCount:        1,
				AvgUniverseSize: 100,
				AvgFailures:     2,
				FaultDensityPct: 2.0,
				StrategySummaries: map[string]exp.AggregatedStrategyStats{
					"dcg": {
						StrategyName:             "Durability-Contract Guided (DCG)",
						K1FailureMedian:          2.0,
						K1FailureMedianCI95Lower: 2.0,
						K1FailureMedianCI95Upper: 2.0,
						K1FailureMean:            2.0,
						K1FailureMeanCI95Lower:   2.0,
						K1FailureMeanCI95Upper:   2.0,
						S100Median:               2.0,
						S100Mean:                 2.0,
					},
					"r2": {
						StrategyName:             "State-Representative Random (R2)",
						K1FailureMedian:          11.0,
						K1FailureMedianCI95Lower: 8.0,
						K1FailureMedianCI95Upper: 15.0,
						K1FailureMean:            14.0,
						K1FailureMeanCI95Lower:   11.0,
						K1FailureMeanCI95Upper:   17.0,
						S100Median:               2.0,
						S100Mean:                 2.0,
					},
					"b0": {
						StrategyName:    "Uniform Random (B0)",
						K1FailureMedian: 20.0,
						K1FailureMean:   25.0,
						S100Median:      1.0,
						S100Mean:        1.2,
					},
				},
			},
		},
	}

	if err := exp.WriteMultiSeedCSV(res, csvPath); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	data, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if !strings.Contains(string(data), "MUT-5") || !strings.Contains(string(data), "DCG") {
		t.Errorf("csv missing expected contents: %s", string(data))
	}

	if err := exp.WriteMultiSeedReportToFile(res, reportPath); err != nil {
		t.Fatalf("write report: %v", err)
	}

	reportData, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if !strings.Contains(string(reportData), "DCG achieves lower cost-to-first-failure than R2") {
		t.Errorf("report missing expected superiority text: %s", string(reportData))
	}
}
