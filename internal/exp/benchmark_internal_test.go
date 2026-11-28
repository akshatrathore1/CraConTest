package exp

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestAggregateFaultSummaryUsesB0ShuffleMeans(t *testing.T) {
	summary := FaultModelSummary{
		FaultID: "test",
		Runs: []SingleRunResult{
			{
				UniverseSize:  100,
				TotalFailures: 2,
				B0: B0Metrics{
					K1FailureMean:   10,
					K1FailureMedian: 20,
					K1SingleSeed:    90,
					BFullMean:       12,
					BFullMedian:     22,
					S25Mean:         1,
					S25Median:       5,
					S25SingleSeed:   9,
					S100Mean:        3,
					S100Median:      7,
				},
			},
			{
				UniverseSize:  100,
				TotalFailures: 2,
				B0: B0Metrics{
					K1FailureMean:   20,
					K1FailureMedian: 30,
					K1SingleSeed:    80,
					BFullMean:       22,
					BFullMedian:     32,
					S25Mean:         2,
					S25Median:       6,
					S25SingleSeed:   8,
					S100Mean:        4,
					S100Median:      8,
				},
			},
		},
	}

	aggregateFaultSummary(&summary)
	b0 := summary.StrategySummaries["b0"]
	if b0.K1FailureMean != 15 || b0.BFullMean != 17 || b0.S25Mean != 1.5 || b0.S100Mean != 3.5 {
		t.Fatalf("B0 summary did not aggregate shuffle means: %+v", b0)
	}
	if b0.S25Mean > b0.S100Mean {
		t.Fatalf("cumulative discovery regressed: S25=%v S100=%v", b0.S25Mean, b0.S100Mean)
	}
}

type failingPlotWriter struct{}

func (failingPlotWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("plot write failed")
}

func TestBenchmarkMetricsPlotAndReportEdgeCases(t *testing.T) {
	t.Run("CalculateAUCNormHoldsTerminalWithoutExtrapolation", func(t *testing.T) {
		pts := []Point{
			{Budget: 0, Failures: 0},
			{Budget: 10, Failures: 2},
		}
		got := CalculateAUCNorm(pts, 100, 10, false)
		// (0+2)/2*10 + 2*90 = 190 out of 100*10 = 1000 => 0.19
		if math.Abs(got-0.19) > 1e-9 {
			t.Fatalf("CalculateAUCNorm = %.6f, want 0.19", got)
		}
		clamped := CalculateAUCNorm([]Point{{Budget: 0, Failures: 0}, {Budget: 500, Failures: 5}}, 100, 10, false)
		if clamped < 0 || clamped > 1.0 {
			t.Fatalf("CalculateAUCNorm with budget>universe out of bounds: %.6f", clamped)
		}
	})

	t.Run("StudentTCritical95AndBoundedCI", func(t *testing.T) {
		tc45 := StudentTCritical95(45)
		if tc45 <= StudentTCritical95(60) || tc45 >= StudentTCritical95(30) {
			t.Fatalf("StudentTCritical95(45)=%.4f outside expected (df60, df30) bracket", tc45)
		}
		mean, lo, hi := StudentTCI95Bounded([]float64{1, 1, 20}, 1.0)
		if mean <= 1.0 || lo < 1.0 || hi < mean {
			t.Fatalf("StudentTCI95Bounded failed lower bound >= 1.0: mean=%.2f lo=%.2f hi=%.2f", mean, lo, hi)
		}
	})

	t.Run("WriteG4PlotSVGPropagatesWriteErrorsAndHandlesEmptyB0", func(t *testing.T) {
		rep := &G4RunReport{
			RunID:    "r",
			FaultID:  "MUT-1",
			Universe: 4,
			Budgets:  []int{0, 2, 4},
			Curves: []StrategyCurve{
				{Name: "b4_combined", Points: []Point{{Budget: 0}, {Budget: 2, Failures: 1}, {Budget: 4, Failures: 1}}},
			},
		}
		var buf bytes.Buffer
		if err := WriteG4PlotSVG(&buf, rep); err != nil {
			t.Fatalf("WriteG4PlotSVG unexpected error: %v", err)
		}
		if strings.Contains(buf.String(), `<path d=""`) {
			t.Fatalf("SVG emitted empty path element on empty B0Summary")
		}
		if err := WriteG4PlotSVG(failingPlotWriter{}, rep); err == nil {
			t.Fatal("expected WriteG4PlotSVG error on failing writer")
		}
	})

	t.Run("GenerateEmpiricalReportMarkdownShortTimestampSafe", func(t *testing.T) {
		md := GenerateEmpiricalReportMarkdown(&BenchmarkResult{Timestamp: "2026"})
		if !strings.Contains(md, "2026") {
			t.Fatalf("expected short timestamp preserved in markdown, got:\n%s", md)
		}
	})

	t.Run("CalculateAUCNormDeterministicDuplicateClamping", func(t *testing.T) {
		ptsA := []Point{
			{Budget: -5, Failures: 1},
			{Budget: 0, Failures: 0},
			{Budget: 120, Failures: 3},
			{Budget: 200, Failures: 6},
		}
		ptsB := []Point{
			{Budget: 200, Failures: 6},
			{Budget: 120, Failures: 3},
			{Budget: 0, Failures: 0},
			{Budget: -5, Failures: 1},
		}
		aucA := CalculateAUCNorm(ptsA, 10, 6, false)
		aucB := CalculateAUCNorm(ptsB, 10, 6, false)
		if aucA != aucB || aucA <= 0 || aucA > 1.0 {
			t.Fatalf("CalculateAUCNorm non-deterministic or invalid: A=%.6f B=%.6f", aucA, aucB)
		}
	})

	t.Run("StudentTCI95RangeUpperCapsAndBelowMinSafety", func(t *testing.T) {
		mean, lo, hi := StudentTCI95Range([]float64{24.0, 25.0, 24.5}, 0.0, 25.0)
		if lo > mean || mean > hi || hi > 25.0 {
			t.Fatalf("S25 cap violated: lo=%.4f mean=%.4f hi=%.4f", lo, mean, hi)
		}
		// Even if input contains values below minBound, lo <= mean <= hi must hold.
		mean2, lo2, hi2 := StudentTCI95Range([]float64{0.2, 0.4, math.NaN()}, 1.0, math.Inf(1))
		if lo2 > mean2 || mean2 > hi2 {
			t.Fatalf("lower <= mean <= upper invariant broken: lo=%.4f mean=%.4f hi=%.4f", lo2, mean2, hi2)
		}
	})

	t.Run("AggregateFaultSummaryOmitsMissingAlignsBudgetsAndClusterBootstrap", func(t *testing.T) {
		summary := FaultModelSummary{
			FaultID: "MUT-X",
			Budgets: []int{0, 5, 5}, // duplicates must deduplicate cleanly
			Runs: []SingleRunResult{
				{
					Seed:            1,
					UniverseSize:    12,
					TotalFailures:   2,
					TotalViolations: 2,
					B0: B0Metrics{
						K1FailureMean:    2.0,
						K1FailureSamples: []float64{1, 3},
						BFullMean:        4.0,
						BFullSamples:     []float64{3, 5},
						S25Mean:          2.0,
						S25Samples:       []float64{2, 2},
						S100Mean:         2.0,
						S100Samples:      []float64{2, 2},
						AUCNormFailures:  0.8,
						SummaryPoints: []B0SummaryPoint{
							{Budget: 0, FailuresAvg: 0},
							{Budget: 5, FailuresAvg: 1.5},
							{Budget: 12, FailuresAvg: 2.0},
						},
					},
					Strategies: map[string]StrategyMetrics{
						"custom": {
							StrategyName:    "Custom",
							K1Failure:       1,
							S25:             2,
							S100:            2,
							BFull:           3,
							AUCNormFailures: 0.9,
							Points: []Point{
								{Budget: 0, Failures: 0},
								{Budget: 5, Failures: 2},
								{Budget: 12, Failures: 2},
							},
						},
					},
				},
				{
					Seed:          2,
					UniverseSize:  12,
					TotalFailures: 0, // inactive seed must not dilute active-seed ranking averages
					B0: B0Metrics{
						SummaryPoints: []B0SummaryPoint{{Budget: 0}, {Budget: 5}, {Budget: 12}},
					},
					Strategies: map[string]StrategyMetrics{
						"custom": {StrategyName: "Custom", Points: []Point{{Budget: 0}, {Budget: 5}, {Budget: 12}}},
					},
				},
			},
		}
		aggregateFaultSummary(&summary)
		if summary.ActiveSeeds != 1 || summary.TotalSeeds != 2 {
			t.Fatalf("unexpected ActiveSeeds=%d TotalSeeds=%d", summary.ActiveSeeds, summary.TotalSeeds)
		}
		// Strategies never seen ("dcg", "b1", ...) must NOT appear as fabricated zero entries!
		if _, exists := summary.StrategySummaries["dcg"]; exists {
			t.Fatal("missing strategy dcg was fabricated in StrategySummaries")
		}
		b0, okB0 := summary.StrategySummaries["b0"]
		custom, okCustom := summary.StrategySummaries["custom"]
		if !okB0 || !okCustom {
			t.Fatalf("expected b0 and custom summaries, got b0=%v custom=%v", okB0, okCustom)
		}
		if math.Abs(b0.K1FailureP10-1.2) > 1e-9 || math.Abs(b0.K1FailureP90-2.8) > 1e-9 || math.Abs(b0.K1FailureMedian-2.0) > 1e-9 {
			t.Fatalf("unexpected pooled cluster quantiles on B0: P10=%.2f Med=%.2f P90=%.2f",
				b0.K1FailureP10, b0.K1FailureMedian, b0.K1FailureP90)
		}
		// Custom AUC and S25 must reflect active seed only (0.9 / 2.0), not diluted across inactive seed 2!
		if custom.AUCNormFailuresMean != 0.9 || custom.S25Mean != 2.0 {
			t.Fatalf("inactive seed diluted active metrics: AUC=%.4f S25=%.2f", custom.AUCNormFailuresMean, custom.S25Mean)
		}
		// Terminal budget 12 from runs must be preserved in summary.Budgets and CurveMeanFailures!
		if len(summary.Budgets) != 3 || summary.Budgets[2] != 12 {
			t.Fatalf("expected deduplicated budgets [0 5 12], got %v", summary.Budgets)
		}
		if len(custom.CurveMeanFailures) != 3 || custom.CurveMeanFailures[2] != 2.0 {
			t.Fatalf("unexpected custom curve failures: %v", custom.CurveMeanFailures)
		}
	})
}
