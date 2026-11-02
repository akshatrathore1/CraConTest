package exp

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/faults"
)

func TestScalingMUT5(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MUT-5 scaling benchmark in short mode")
	}

	opsList := []int{120, 300, 600, 1200}
	seeds := []uint64{42, 100, 200, 300, 400, 500, 600, 700}

	resultsDir := filepath.Join("..", "..", "results")
	if fi, err := os.Stat(resultsDir); err != nil || !fi.IsDir() {
		resultsDir = "results"
	}

	type runRow struct {
		ops          int
		seed         uint64
		universeSize int
		k1DCG        int
		k1R2         int
		k1R1         int
		k1B0         int
		k1B2         int
		planRankMs   int64
		exploreMs    int64
	}

	var allRows []runRow
	rowsByOps := make(map[int][]runRow)

	for _, ops := range opsList {
		for _, seed := range seeds {
			t0 := time.Now()
			cfg := DefaultBenchmarkConfig()
			cfg.Workload.Operations = ops
			cfg.Policies = []int{0, 100}

			res, err := RunSingleSeedFault(seed, faults.MUT5, cfg)
			if err != nil {
				t.Fatalf("RunSingleSeedFault(seed=%d, ops=%d, MUT5) failed: %v", seed, ops, err)
			}

			k1DCG := res.Strategies["dcg"].K1Failure
			k1R2 := res.Strategies["r2"].K1Failure
			k1R1 := res.Strategies["r1"].K1Failure
			k1B0 := int(math.Round(res.B0.K1FailureMedian))
			k1B2 := res.Strategies["b2"].K1Failure

			row := runRow{
				ops:          ops,
				seed:         seed,
				universeSize: res.UniverseSize,
				k1DCG:        k1DCG,
				k1R2:         k1R2,
				k1R1:         k1R1,
				k1B0:         k1B0,
				k1B2:         k1B2,
				planRankMs:   res.PlanRankMs,
				exploreMs:    res.ExploreTotalMs,
			}
			allRows = append(allRows, row)
			rowsByOps[ops] = append(rowsByOps[ops], row)

			t.Logf("[ops=%4d seed=%3d] univ=%4d k1(dcg=%d r2=%d r1=%d b0=%d b2=%d) plan=%dms exp=%dms total=%v",
				ops, seed, res.UniverseSize, k1DCG, k1R2, k1R1, k1B0, k1B2,
				res.PlanRankMs, res.ExploreTotalMs, time.Since(t0).Round(time.Millisecond))
		}
	}

	// 1. Write results/scaling_mut5_per_seed.csv (32 rows + header)
	perSeedPath := filepath.Join(resultsDir, "scaling_mut5_per_seed.csv")
	fPerSeed, err := os.Create(perSeedPath)
	if err != nil {
		t.Fatalf("create %s failed: %v", perSeedPath, err)
	}
	defer fPerSeed.Close()

	wPerSeed := csv.NewWriter(fPerSeed)
	if err := wPerSeed.Write([]string{
		"ops", "seed", "universe_size", "k1_dcg", "k1_r2", "k1_r1", "k1_b0", "k1_b2", "plan_rank_ms", "explore_total_ms",
	}); err != nil {
		t.Fatalf("write per_seed header failed: %v", err)
	}

	for _, r := range allRows {
		record := []string{
			strconv.Itoa(r.ops),
			strconv.FormatUint(r.seed, 10),
			strconv.Itoa(r.universeSize),
			strconv.Itoa(r.k1DCG),
			strconv.Itoa(r.k1R2),
			strconv.Itoa(r.k1R1),
			strconv.Itoa(r.k1B0),
			strconv.Itoa(r.k1B2),
			strconv.FormatInt(r.planRankMs, 10),
			strconv.FormatInt(r.exploreMs, 10),
		}
		if err := wPerSeed.Write(record); err != nil {
			t.Fatalf("write per_seed row failed: %v", err)
		}
	}
	wPerSeed.Flush()
	if err := wPerSeed.Error(); err != nil {
		t.Fatalf("flush per_seed failed: %v", err)
	}
	t.Logf("Wrote %d rows to %s", len(allRows), perSeedPath)

	// 2. Write results/scaling_mut5_summary.csv (4 rows + header)
	calcMeanMedian := func(vals []float64) (float64, float64) {
		if len(vals) == 0 {
			return 0, 0
		}
		sum := 0.0
		for _, v := range vals {
			sum += v
		}
		mean := sum / float64(len(vals))

		sorted := append([]float64{}, vals...)
		sort.Float64s(sorted)
		n := len(sorted)
		var med float64
		if n%2 == 1 {
			med = sorted[n/2]
		} else {
			med = (sorted[n/2-1] + sorted[n/2]) / 2.0
		}
		return mean, med
	}

	summaryPath := filepath.Join(resultsDir, "scaling_mut5_summary.csv")
	fSummary, err := os.Create(summaryPath)
	if err != nil {
		t.Fatalf("create %s failed: %v", summaryPath, err)
	}
	defer fSummary.Close()

	wSummary := csv.NewWriter(fSummary)
	if err := wSummary.Write([]string{
		"ops", "avg_universe_size",
		"k1_dcg_mean", "k1_dcg_median",
		"k1_r2_mean", "k1_r2_median",
		"k1_r1_mean", "k1_r1_median",
		"k1_b0_mean", "k1_b0_median",
		"k1_b2_mean", "k1_b2_median",
		"avg_plan_rank_ms", "avg_explore_total_ms",
	}); err != nil {
		t.Fatalf("write summary header failed: %v", err)
	}

	for _, ops := range opsList {
		rows := rowsByOps[ops]
		n := float64(len(rows))
		sumUniv := 0.0
		sumPlan := 0.0
		sumExp := 0.0
		var dcgVals, r2Vals, r1Vals, b0Vals, b2Vals []float64
		for _, r := range rows {
			sumUniv += float64(r.universeSize)
			sumPlan += float64(r.planRankMs)
			sumExp += float64(r.exploreMs)
			dcgVals = append(dcgVals, float64(r.k1DCG))
			r2Vals = append(r2Vals, float64(r.k1R2))
			r1Vals = append(r1Vals, float64(r.k1R1))
			b0Vals = append(b0Vals, float64(r.k1B0))
			b2Vals = append(b2Vals, float64(r.k1B2))
		}
		dcgMean, dcgMed := calcMeanMedian(dcgVals)
		r2Mean, r2Med := calcMeanMedian(r2Vals)
		r1Mean, r1Med := calcMeanMedian(r1Vals)
		b0Mean, b0Med := calcMeanMedian(b0Vals)
		b2Mean, b2Med := calcMeanMedian(b2Vals)

		record := []string{
			strconv.Itoa(ops),
			fmt.Sprintf("%.1f", sumUniv/n),
			fmt.Sprintf("%.2f", dcgMean), fmt.Sprintf("%.2f", dcgMed),
			fmt.Sprintf("%.2f", r2Mean), fmt.Sprintf("%.2f", r2Med),
			fmt.Sprintf("%.2f", r1Mean), fmt.Sprintf("%.2f", r1Med),
			fmt.Sprintf("%.2f", b0Mean), fmt.Sprintf("%.2f", b0Med),
			fmt.Sprintf("%.2f", b2Mean), fmt.Sprintf("%.2f", b2Med),
			fmt.Sprintf("%.1f", sumPlan/n),
			fmt.Sprintf("%.1f", sumExp/n),
		}
		if err := wSummary.Write(record); err != nil {
			t.Fatalf("write summary row failed: %v", err)
		}
	}
	wSummary.Flush()
	if err := wSummary.Error(); err != nil {
		t.Fatalf("flush summary failed: %v", err)
	}
	t.Logf("Wrote 4 rows to %s", summaryPath)
}
