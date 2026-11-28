package exp

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/faults"
)

// validateScalingCSV reads the committed per-seed CSV and checks all 32 rows including k1_dcg == 1.
func validateScalingCSV(t *testing.T, perSeedPath string) {
	t.Helper()
	f, err := os.Open(perSeedPath)
	if err != nil {
		t.Fatalf("scaling_mut5_per_seed.csv not found at %s: %v (run with -update to regenerate)", perSeedPath, err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		t.Fatalf("read header: %v", err)
	}
	expectedHeader := []string{
		"ops", "seed", "universe_size", "k1_dcg", "k1_r2", "k1_r1", "k1_b0", "k1_b2", "plan_rank_ms", "explore_total_ms",
	}
	if len(header) != len(expectedHeader) {
		t.Fatalf("per-seed header column count = %d, want %d: %v", len(header), len(expectedHeader), header)
	}
	colMap := make(map[string]int, len(header))
	for i, h := range header {
		if h != expectedHeader[i] {
			t.Fatalf("per-seed header col %d = %q, want %q", i, h, expectedHeader[i])
		}
		colMap[h] = i
	}

	rows := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read row %d: %v", rows+1, err)
		}
		rows++
		if len(rec) != len(expectedHeader) {
			t.Fatalf("row %d column count = %d, want %d", rows, len(rec), len(expectedHeader))
		}
		univ, err := strconv.Atoi(rec[colMap["universe_size"]])
		if err != nil || univ <= 0 {
			t.Fatalf("row %d: invalid universe_size %q", rows, rec[colMap["universe_size"]])
		}
		v, err := strconv.ParseFloat(rec[colMap["k1_dcg"]], 64)
		if err != nil || v != 1.0 {
			t.Fatalf("row %d: k1_dcg=%s expected == 1.0 (DCG should always find the bug first on MUT-5)", rows, rec[colMap["k1_dcg"]])
		}
		for _, col := range []string{"k1_r2", "k1_r1", "k1_b0", "k1_b2"} {
			val, err := strconv.ParseFloat(rec[colMap[col]], 64)
			if err != nil || val < 1.0 {
				t.Fatalf("row %d: invalid %s=%q", rows, col, rec[colMap[col]])
			}
		}
	}
	if rows != 32 {
		t.Fatalf("expected 32 data rows in scaling_mut5_per_seed.csv, got %d (run with -update to regenerate)", rows)
	}
	t.Logf("Validated %d rows with k1_dcg == 1.0 in %s", rows, perSeedPath)
}

func TestScalingMUT5(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MUT-5 scaling benchmark in short mode")
	}

	resultsDir := findResultsDirInternal(t)
	perSeedPath := filepath.Join(resultsDir, "scaling_mut5_per_seed.csv")

	if !*updateArtifacts {
		validateScalingCSV(t, perSeedPath)
		validateScalingSummaryCSV(t, filepath.Join(resultsDir, "scaling_mut5_summary.csv"))

		cfg := DefaultBenchmarkConfig()
		cfg.Workload.Operations = 60
		cfg.Workload.FlushEvery = 20
		cfg.Workload.SyncEvery = 6
		cfg.Policies = []int{0, 100}
		cfg.B0PercentileCnt = 10
		res, err := RunSingleSeedFault(42, faults.MUT5, cfg)
		if err != nil {
			t.Fatalf("live scaling smoke RunSingleSeedFault failed: %v", err)
		}
		if res.TotalFailures == 0 {
			t.Fatal("live scaling smoke MUT-5 expected TotalFailures > 0")
		}
		k1DCG := res.Strategies["dcg"].K1Failure
		k1B2 := res.Strategies["b2"].K1Failure
		if k1DCG != 1 {
			t.Fatalf("live scaling smoke MUT-5 k1_dcg = %d, want 1", k1DCG)
		}
		if k1B2 < k1DCG {
			t.Fatalf("live scaling smoke MUT-5 k1_b2=%d < k1_dcg=%d", k1B2, k1DCG)
		}
		return
	}

	// Generation mode: run the full scaling suite and write both CSVs atomically.
	runScalingSuite(t, resultsDir)
}

func runScalingSuite(t *testing.T, resultsDir string) {
	t.Helper()

	opsList := []int{120, 300, 600, 1200}
	seeds := []uint64{42, 100, 200, 300, 400, 500, 600, 700}

	type runRow struct {
		ops          int
		seed         uint64
		universeSize int
		k1DCG        int
		k1R2         int
		k1R1         int
		k1B0         float64
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
			if res.TotalFailures == 0 {
				t.Fatalf("RunSingleSeedFault(seed=%d, ops=%d, MUT5): expected failures > 0", seed, ops)
			}

			k1DCG := res.Strategies["dcg"].K1Failure
			if k1DCG != 1 {
				t.Fatalf("RunSingleSeedFault(seed=%d, ops=%d, MUT5): k1_dcg=%d, want 1", seed, ops, k1DCG)
			}
			k1R2 := res.Strategies["r2"].K1Failure
			k1R1 := res.Strategies["r1"].K1Failure
			k1B0 := res.B0.K1FailureMedian
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

			t.Logf("[ops=%4d seed=%3d] univ=%4d k1(dcg=%d r2=%d r1=%d b0=%.1f b2=%d) plan=%dms exp=%dms total=%v",
				ops, seed, res.UniverseSize, k1DCG, k1R2, k1R1, k1B0, k1B2,
				res.PlanRankMs, res.ExploreTotalMs, time.Since(t0).Round(time.Millisecond))
		}
	}

	perSeedPath := filepath.Join(resultsDir, "scaling_mut5_per_seed.csv")
	if err := writeCSVAtomic(perSeedPath, func(w *csv.Writer) error {
		if err := w.Write([]string{
			"ops", "seed", "universe_size", "k1_dcg", "k1_r2", "k1_r1", "k1_b0", "k1_b2", "plan_rank_ms", "explore_total_ms",
		}); err != nil {
			return err
		}
		for _, r := range allRows {
			record := []string{
				strconv.Itoa(r.ops),
				strconv.FormatUint(r.seed, 10),
				strconv.Itoa(r.universeSize),
				strconv.Itoa(r.k1DCG),
				strconv.Itoa(r.k1R2),
				strconv.Itoa(r.k1R1),
				fmt.Sprintf("%.2f", r.k1B0),
				strconv.Itoa(r.k1B2),
				strconv.FormatInt(r.planRankMs, 10),
				strconv.FormatInt(r.exploreMs, 10),
			}
			if err := w.Write(record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write %s failed: %v", perSeedPath, err)
	}
	t.Logf("Wrote %d rows to %s", len(allRows), perSeedPath)

	summaryPath := filepath.Join(resultsDir, "scaling_mut5_summary.csv")
	if err := writeCSVAtomic(summaryPath, func(w *csv.Writer) error {
		if err := w.Write([]string{
			"ops", "avg_universe_size",
			"k1_dcg_mean", "k1_dcg_median",
			"k1_r2_mean", "k1_r2_median",
			"k1_r1_mean", "k1_r1_median",
			"k1_b0_mean", "k1_b0_median",
			"k1_b2_mean", "k1_b2_median",
			"avg_plan_rank_ms", "avg_explore_total_ms",
		}); err != nil {
			return err
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
				b0Vals = append(b0Vals, r.k1B0)
				b2Vals = append(b2Vals, float64(r.k1B2))
			}
			record := []string{
				strconv.Itoa(ops),
				fmt.Sprintf("%.1f", sumUniv/n),
				fmt.Sprintf("%.2f", average(dcgVals)), fmt.Sprintf("%.2f", median(dcgVals)),
				fmt.Sprintf("%.2f", average(r2Vals)), fmt.Sprintf("%.2f", median(r2Vals)),
				fmt.Sprintf("%.2f", average(r1Vals)), fmt.Sprintf("%.2f", median(r1Vals)),
				fmt.Sprintf("%.2f", average(b0Vals)), fmt.Sprintf("%.2f", median(b0Vals)),
				fmt.Sprintf("%.2f", average(b2Vals)), fmt.Sprintf("%.2f", median(b2Vals)),
				fmt.Sprintf("%.1f", sumPlan/n),
				fmt.Sprintf("%.1f", sumExp/n),
			}
			if err := w.Write(record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write %s failed: %v", summaryPath, err)
	}
	t.Logf("Wrote 4 rows to %s", summaryPath)
}

func validateScalingSummaryCSV(t *testing.T, summaryPath string) {
	t.Helper()
	f, err := os.Open(summaryPath)
	if err != nil {
		t.Fatalf("scaling_mut5_summary.csv not found at %s: %v", summaryPath, err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read summary csv: %v", err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows (header + 4 op shapes) in %s, got %d", summaryPath, len(rows))
	}
	expectedHeader := []string{
		"ops", "avg_universe_size",
		"k1_dcg_mean", "k1_dcg_median",
		"k1_r2_mean", "k1_r2_median",
		"k1_r1_mean", "k1_r1_median",
		"k1_b0_mean", "k1_b0_median",
		"k1_b2_mean", "k1_b2_median",
		"avg_plan_rank_ms", "avg_explore_total_ms",
	}
	if len(rows[0]) != len(expectedHeader) {
		t.Fatalf("expected %d columns in %s header, got %d", len(expectedHeader), summaryPath, len(rows[0]))
	}
	colMap := make(map[string]int, len(expectedHeader))
	for i, col := range expectedHeader {
		if rows[0][i] != col {
			t.Fatalf("summary header col %d = %q, want %q", i, rows[0][i], col)
		}
		colMap[col] = i
	}
	for idx, rec := range rows[1:] {
		if len(rec) != len(expectedHeader) {
			t.Fatalf("row %d column count = %d, want %d", idx+1, len(rec), len(expectedHeader))
		}
		for colIdx, cell := range rec {
			v, err := strconv.ParseFloat(cell, 64)
			if err != nil || v < 0 {
				t.Fatalf("row %d col %d (%s=%q) invalid: %v", idx+1, colIdx, rows[0][colIdx], cell, err)
			}
		}
		univMean, _ := strconv.ParseFloat(rec[colMap["avg_universe_size"]], 64)
		k1DCGMean, _ := strconv.ParseFloat(rec[colMap["k1_dcg_mean"]], 64)
		k1DCGMed, _ := strconv.ParseFloat(rec[colMap["k1_dcg_median"]], 64)
		k1B2Mean, _ := strconv.ParseFloat(rec[colMap["k1_b2_mean"]], 64)
		if univMean <= 0 {
			t.Fatalf("row %d: invalid avg_universe_size=%.2f", idx+1, univMean)
		}
		if k1DCGMean != 1.0 || k1DCGMed != 1.0 {
			t.Fatalf("row %d: invalid k1_dcg mean=%.2f median=%.2f (want 1.00)", idx+1, k1DCGMean, k1DCGMed)
		}
		if k1DCGMean > k1B2Mean {
			t.Fatalf("row %d: invalid k1_dcg_mean=%.2f > k1_b2_mean=%.2f", idx+1, k1DCGMean, k1B2Mean)
		}
	}
}
