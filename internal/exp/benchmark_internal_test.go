package exp

import "testing"

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
