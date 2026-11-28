package exp

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/workload"
)

// DefaultPebbleVersion defines the single-source-of-truth fallback Pebble version
// when dynamic build info is unavailable at runtime.
const DefaultPebbleVersion = "v2.1.7"

// DetectPebbleVersion queries runtime build info to determine the active Pebble module version,
// falling back to DefaultPebbleVersion if build info is not embedded.
func DetectPebbleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return DefaultPebbleVersion
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/cockroachdb/pebble/v2" || dep.Path == "github.com/cockroachdb/pebble" {
			if dep.Replace != nil && dep.Replace.Version != "" {
				return dep.Replace.Version
			}
			if dep.Version != "" {
				return dep.Version
			}
		}
	}
	return DefaultPebbleVersion
}

// DefaultMultiSeedSeeds defines the 32 deterministic workload seeds used in the 32-seed multi-fault benchmark.
var DefaultMultiSeedSeeds = []uint64{
	42, 100, 200, 300, 400, 500, 600, 700, 800, 900,
	1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010,
	1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020,
	1021, 1022,
}

// DefaultMultiSeedFaults defines the 9 fault models evaluated in the 32-seed multi-fault benchmark.
var DefaultMultiSeedFaults = []string{
	faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4,
	faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8,
	faults.MUT9,
}

// DefaultMultiSeedBenchmarkConfig returns the calibrated configuration for the 32-seed multi-fault benchmark.
func DefaultMultiSeedBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		WorkloadSeeds: append([]uint64{}, DefaultMultiSeedSeeds...),
		FaultIDs:      append([]string{}, DefaultMultiSeedFaults...),
		Workload: workload.Config{
			Seed:       42,
			Operations: 120,
			KeyCount:   16,
			ValueBytes: 64,
			SyncEvery:  8,
			FlushEvery: 40,
		},
		Policies:        []int{0, 100},
		SafeOpenTimeout: 3 * 1e9,
		B0PercentileCnt: DefaultB0PercentileCnt,
		Budgets:         []int{0, 1, 5, 10, 25, 50, 100, 150, 200},
		TrackMUT0:       true,
	}
}

// CalculateTheoreticalExpectation computes E[K1] = (N+1)/(m+1) for drawing without replacement
// until the first failure from a finite population of N cutoffs containing m failing cutoffs.
func CalculateTheoreticalExpectation(universeSize int, totalFailures int) float64 {
	if universeSize <= 0 || totalFailures <= 0 {
		return 0.0
	}
	return float64(universeSize+1) / float64(totalFailures+1)
}

func buildValidatedOutcomeIndex(outcomes []oracle.Outcome) (map[candidate.Candidate]oracle.Outcome, error) {
	outcomeMap := make(map[candidate.Candidate]oracle.Outcome, len(outcomes))
	for _, o := range outcomes {
		if o.Class == oracle.ClassOracleError {
			return nil, fmt.Errorf("metrics: oracle_error for %+v makes table unusable", o.Candidate)
		}
		if _, dup := outcomeMap[o.Candidate]; dup {
			return nil, fmt.Errorf("metrics: duplicate outcome candidate %+v", o.Candidate)
		}
		outcomeMap[o.Candidate] = o
	}
	return outcomeMap, nil
}

func validateRankingPermutationForMetrics(ranking []candidate.Candidate, outcomeMap map[candidate.Candidate]oracle.Outcome) error {
	if len(ranking) != len(outcomeMap) {
		return fmt.Errorf("metrics: ranking length %d != outcomes length %d", len(ranking), len(outcomeMap))
	}
	seen := make(map[candidate.Candidate]bool, len(ranking))
	for _, c := range ranking {
		if _, ok := outcomeMap[c]; !ok {
			return fmt.Errorf("metrics: ranking contains foreign candidate %+v", c)
		}
		if seen[c] {
			return fmt.Errorf("metrics: ranking contains duplicate candidate %+v", c)
		}
		seen[c] = true
	}
	return nil
}

// CalculateS25 counts distinct failing persistence states (by StateKey) discovered within budget B=25 crashes.
func CalculateS25(ranking []candidate.Candidate, outcomes []oracle.Outcome, view *graph.View) int {
	return CalculateSBudget(ranking, outcomes, view, 25)
}

// CalculateS100 counts distinct failing persistence states (by StateKey) discovered within 100 crashes.
func CalculateS100(ranking []candidate.Candidate, outcomes []oracle.Outcome, view *graph.View) int {
	return CalculateSBudget(ranking, outcomes, view, 100)
}

// CalculateSBudget counts distinct failing persistence states (by StateKey) discovered within budget crashes.
func CalculateSBudget(ranking []candidate.Candidate, outcomes []oracle.Outcome, view *graph.View, budget int) int {
	if budget <= 0 || len(ranking) == 0 || len(outcomes) == 0 {
		return 0
	}
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0
	}
	limit := budget
	if len(ranking) < limit {
		limit = len(ranking)
	}
	seenCand := make(map[candidate.Candidate]bool, limit)
	seenStates := make(map[string]bool)
	for _, c := range ranking[:limit] {
		if seenCand[c] {
			continue
		}
		seenCand[c] = true
		o, ok := outcomeMap[c]
		if ok && isFailureClass(o.Class) {
			// Key by (StateKey, Percent) to match paired_stats.py which indexes
			// distinct failing states by (anchor, percent).
			stateKey := ""
			if view != nil && view.Cuts != nil {
				if feat, hasCut := view.Cuts[c.Event]; hasCut {
					stateKey = feat.StateKey
				}
			}
			if stateKey == "" {
				stateKey = fmt.Sprintf("event_%d", c.Event)
			}
			key := fmt.Sprintf("%s|p%d", stateKey, c.Percent)
			seenStates[key] = true
		}
	}
	return len(seenStates)
}

// CalculateSBudgetChecked validates ranking and outcomes strictly before computing distinct failing states within budget.
func CalculateSBudgetChecked(ranking []candidate.Candidate, outcomes []oracle.Outcome, view *graph.View, budget int) (int, error) {
	if budget < 0 {
		return 0, fmt.Errorf("metrics: negative budget %d", budget)
	}
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0, err
	}
	if err := validateRankingPermutationForMetrics(ranking, outcomeMap); err != nil {
		return 0, err
	}
	return CalculateSBudget(ranking, outcomes, view, budget), nil
}

// CalculateBugs100 counts distinct bug signatures discovered within the first 100 crashes.
func CalculateBugs100(ranking []candidate.Candidate, outcomes []oracle.Outcome) int {
	if len(ranking) == 0 || len(outcomes) == 0 {
		return 0
	}
	outcomes = NormalizeTable(outcomes)
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0
	}
	limit := 100
	if len(ranking) < limit {
		limit = len(ranking)
	}
	seenCand := make(map[candidate.Candidate]bool, limit)
	seenSigs := make(map[string]bool)
	for _, c := range ranking[:limit] {
		if seenCand[c] {
			continue
		}
		seenCand[c] = true
		o, ok := outcomeMap[c]
		if ok && isFailureClass(o.Class) && o.Signature != "" {
			seenSigs[o.Signature] = true
		}
	}
	return len(seenSigs)
}

// CalculateBugs100Checked validates ranking and outcomes strictly before computing distinct signatures in top 100.
func CalculateBugs100Checked(ranking []candidate.Candidate, outcomes []oracle.Outcome) (int, error) {
	outcomes = NormalizeTable(outcomes)
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0, err
	}
	if err := validateRankingPermutationForMetrics(ranking, outcomeMap); err != nil {
		return 0, err
	}
	return CalculateBugs100(ranking, outcomes), nil
}

func betaCF(a, b, x float64) float64 {
	const maxIter = 200
	const eps = 3e-14
	const fpMin = 1e-300

	qab := a + b
	qap := a + 1.0
	qam := a - 1.0
	c := 1.0
	d := 1.0 - qab*x/qap
	if math.Abs(d) < fpMin {
		d = fpMin
	}
	d = 1.0 / d
	h := d
	for m := 1; m <= maxIter; m++ {
		m2 := float64(2 * m)
		aa := float64(m) * (b - float64(m)) * x / ((qam + m2) * (a + m2))
		d = 1.0 + aa*d
		if math.Abs(d) < fpMin {
			d = fpMin
		}
		c = 1.0 + aa/c
		if math.Abs(c) < fpMin {
			c = fpMin
		}
		d = 1.0 / d
		h *= d * c
		aa = -(a + float64(m)) * (qab + float64(m)) * x / ((a + m2) * (qap + m2))
		d = 1.0 + aa*d
		if math.Abs(d) < fpMin {
			d = fpMin
		}
		c = 1.0 + aa/c
		if math.Abs(c) < fpMin {
			c = fpMin
		}
		d = 1.0 / d
		delta := d * c
		h *= delta
		if math.Abs(delta-1.0) < eps {
			break
		}
	}
	return h
}

func regIncBeta(a, b, x float64) float64 {
	if x <= 0.0 {
		return 0.0
	}
	if x >= 1.0 {
		return 1.0
	}
	la, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	lab, _ := math.Lgamma(a + b)
	bt := math.Exp(lab - la - lb + a*math.Log(x) + b*math.Log(1.0-x))
	if x < (a+1.0)/(a+b+2.0) {
		return bt * betaCF(a, b, x) / a
	}
	return 1.0 - bt*betaCF(b, a, 1.0-x)/b
}

// StudentTCritical95 returns two-sided 95% critical value for Student's t distribution with df degrees of freedom.
func StudentTCritical95(df int) float64 {
	table := map[int]float64{
		1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
		6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
		11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
		16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
		21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
		26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
		31: 2.039513, 32: 2.037, 40: 2.021, 50: 2.009, 60: 2.000,
		100: 1.984, 120: 1.980,
	}
	if v, ok := table[df]; ok {
		return v
	}
	if df <= 0 {
		return 1.960
	}
	v := float64(df)
	lo, hi := 1.95996398, 12.70620474
	for iter := 0; iter < 60; iter++ {
		mid := 0.5 * (lo + hi)
		x := v / (v + mid*mid)
		pTwoSided := regIncBeta(0.5*v, 0.5, x)
		if pTwoSided > 0.05 {
			lo = mid
		} else {
			hi = mid
		}
	}
	return 0.5 * (lo + hi)
}

// StudentTCI95 calculates sample mean and Student's t 95% confidence interval bounds, floored at 0.0.
func StudentTCI95(vals []float64) (mean, lower, upper float64) {
	return StudentTCI95Range(vals, 0.0, math.Inf(1))
}

// StudentTCI95Bounded calculates sample mean and Student's t 95% confidence interval bounds,
// clamping lower at minBound while preserving lower <= mean <= upper.
func StudentTCI95Bounded(vals []float64, minBound float64) (mean, lower, upper float64) {
	return StudentTCI95Range(vals, minBound, math.Inf(1))
}

// StudentTCI95Range calculates sample mean and Student's t 95% confidence interval bounds,
// clamping lower at minBound and upper at maxBound (when finite), guaranteeing lower <= mean <= upper.
func StudentTCI95Range(vals []float64, minBound, maxBound float64) (mean, lower, upper float64) {
	finite := make([]float64, 0, len(vals))
	for _, v := range vals {
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			finite = append(finite, v)
		}
	}
	n := len(finite)
	if n == 0 {
		return 0, 0, 0
	}
	if n == 1 {
		v := finite[0]
		return v, v, v
	}

	sum := 0.0
	for _, v := range finite {
		sum += v
	}
	mean = sum / float64(n)

	variance := 0.0
	for _, v := range finite {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(n - 1)
	stdDev := math.Sqrt(variance)
	se := stdDev / math.Sqrt(float64(n))

	tCrit := StudentTCritical95(n - 1)
	margin := tCrit * se
	lower = math.Min(mean, math.Max(minBound, mean-margin))
	upper = mean + margin
	if !math.IsInf(maxBound, 1) {
		upper = math.Max(mean, math.Min(maxBound, upper))
	}
	return mean, lower, upper
}

func quickSelect(a []float64, k int) float64 {
	lo, hi := 0, len(a)-1
	for lo < hi {
		mid := lo + (hi-lo)/2
		pivot := a[mid]
		i, j := lo, hi
		for i <= j {
			for a[i] < pivot {
				i++
			}
			for a[j] > pivot {
				j--
			}
			if i <= j {
				a[i], a[j] = a[j], a[i]
				i++
				j--
			}
		}
		if k <= j {
			hi = j
		} else if k >= i {
			lo = i
		} else {
			return a[k]
		}
	}
	return a[lo]
}

func medianInPlace(a []float64) float64 {
	n := len(a)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return quickSelect(a, n/2)
	}
	hi := quickSelect(a, n/2)
	// After quickSelect(n/2), all elements in a[:n/2] are <= hi, so the (n/2-1)-th
	// element is simply the maximum of a[:n/2].
	lo := a[0]
	for i := 1; i < n/2; i++ {
		if a[i] > lo {
			lo = a[i]
		}
	}
	return 0.5 * (lo + hi)
}

// BootstrapMedianCI95 computes sample median and 95% non-parametric bootstrap confidence interval
// using deterministic PCG pseudo-random resamples.
func BootstrapMedianCI95(vals []float64, resamples int, seed uint64) (med, lower, upper float64) {
	n := len(vals)
	if n == 0 {
		return 0, 0, 0
	}
	med = median(vals)
	if n <= 2 || resamples <= 0 {
		return med, med, med
	}

	rng := rand.New(rand.NewPCG(seed, 2026))
	bootMedians := make([]float64, resamples)
	sample := make([]float64, n)

	for b := 0; b < resamples; b++ {
		for i := 0; i < n; i++ {
			sample[i] = vals[rng.IntN(n)]
		}
		bootMedians[b] = medianInPlace(sample)
	}

	sort.Float64s(bootMedians)
	lowIdx := int(math.Floor(0.025 * float64(resamples)))
	highIdx := int(math.Ceil(0.975*float64(resamples))) - 1
	if lowIdx < 0 {
		lowIdx = 0
	}
	if highIdx >= resamples {
		highIdx = resamples - 1
	}

	lower = bootMedians[lowIdx]
	upper = bootMedians[highIdx]
	return med, lower, upper
}

// ClusterBootstrapCI95 computes two-stage hierarchical bootstrap 95% CIs for mean and median
// over clustered per-seed shuffle samples, resampling seeds then shuffles within each drawn seed.
func ClusterBootstrapCI95(
	clusters [][]float64,
	resamples int,
	seed uint64,
	minBound float64,
	maxBound float64,
) (mean, meanLow, meanHigh, med, medLow, medHigh, p10, p90 float64) {
	if len(clusters) == 0 {
		return 0, 0, 0, 0, 0, 0, 0, 0
	}

	seedMeans := make([]float64, 0, len(clusters))
	totalTrials := 0
	for _, cl := range clusters {
		if len(cl) == 0 {
			continue
		}
		seedMeans = append(seedMeans, average(cl))
		totalTrials += len(cl)
	}
	if totalTrials == 0 {
		return 0, 0, 0, 0, 0, 0, 0, 0
	}

	pooled := make([]float64, 0, totalTrials)
	nonEmptyClusters := make([][]float64, 0, len(clusters))
	for _, cl := range clusters {
		if len(cl) > 0 {
			nonEmptyClusters = append(nonEmptyClusters, cl)
			pooled = append(pooled, cl...)
		}
	}

	mean = average(seedMeans)
	sortedPooled := append([]float64(nil), pooled...)
	sort.Float64s(sortedPooled)
	med = percentile(sortedPooled, 50)
	p10 = percentile(sortedPooled, 10)
	p90 = percentile(sortedPooled, 90)

	k := len(nonEmptyClusters)
	if resamples <= 0 || (k <= 1 && totalTrials <= 1) {
		return mean, mean, mean, med, med, med, p10, p90
	}

	rng := rand.New(rand.NewPCG(seed, 2026))
	bootMeans := make([]float64, resamples)
	bootMedians := make([]float64, resamples)
	sample := make([]float64, 0, totalTrials)

	for b := 0; b < resamples; b++ {
		sample = sample[:0]
		sumClusterMeans := 0.0
		for i := 0; i < k; i++ {
			cl := nonEmptyClusters[rng.IntN(k)]
			m := len(cl)
			cSum := 0.0
			for j := 0; j < m; j++ {
				v := cl[rng.IntN(m)]
				sample = append(sample, v)
				cSum += v
			}
			sumClusterMeans += cSum / float64(m)
		}
		bootMeans[b] = sumClusterMeans / float64(k)
		bootMedians[b] = medianInPlace(sample)
	}

	sort.Float64s(bootMeans)
	sort.Float64s(bootMedians)
	lowIdx := int(math.Floor(0.025 * float64(resamples)))
	highIdx := int(math.Ceil(0.975*float64(resamples))) - 1
	if lowIdx < 0 {
		lowIdx = 0
	}
	if highIdx >= resamples {
		highIdx = resamples - 1
	}

	meanLow = math.Min(mean, math.Max(minBound, bootMeans[lowIdx]))
	meanHigh = bootMeans[highIdx]
	if !math.IsInf(maxBound, 1) {
		meanHigh = math.Max(mean, math.Min(maxBound, meanHigh))
	} else {
		meanHigh = math.Max(mean, meanHigh)
	}
	medLow = math.Min(med, bootMedians[lowIdx])
	medHigh = math.Max(med, bootMedians[highIdx])
	return mean, meanLow, meanHigh, med, medLow, medHigh, p10, p90
}

// --- Durability-Contract Guided (DCG) Strategy ---------------------------

// NewDCGStrategy creates a Durability-Contract Guided ranking strategy delegating
// to the canonical strategy.NewDCG() implementation.
func NewDCGStrategy() strategy.Strategy {
	return strategy.NewDCG()
}

// NewDCGPartialStrategy creates a post-hoc partial-retention prior (dcg-pr) ranking strategy
// delegating to the canonical strategy.NewDCGPartial() implementation.
func NewDCGPartialStrategy() strategy.Strategy {
	return strategy.NewDCGPartial()
}

var canonicalStrategyDisplayNames = map[string]string{
	"b0":            "Uniform Random (B0)",
	"r1":            "0%-Retention-First Random (R1)",
	"r2":            "State-Representative Random (R2)",
	"dcg":           "Durability-Contract Guided (DCG)",
	"dcg-pr":        "DCG Partial-Retention Prior (DCG-PR)",
	"dcg-flag-only": "DCG Flag-Only (Ablation)",
}

func strategyDisplayName(code, fallback string) string {
	if s, ok := canonicalStrategyDisplayNames[code]; ok {
		return s
	}
	if fallback != "" {
		return fallback
	}
	return code
}

func resolveStrategyMetrics(m StrategyMetrics) (k1Fail, k1Viol, bFull, s25, s100 float64) {
	k1Fail = float64(m.K1Failure)
	if m.K1FailureFloat > 0 {
		k1Fail = m.K1FailureFloat
	}
	k1Viol = float64(m.K1Violation)
	if m.K1ViolationFloat > 0 {
		k1Viol = m.K1ViolationFloat
	}
	bFull = float64(m.BFull)
	if m.BFullFloat > 0 {
		bFull = m.BFullFloat
	}
	s25 = float64(m.S25)
	if m.S25Float > 0 {
		s25 = m.S25Float
	}
	s100 = float64(m.S100)
	if m.S100Float > 0 {
		s100 = m.S100Float
	}
	return k1Fail, k1Viol, bFull, s25, s100
}

func orderedSummaryFaults(faultSummaries map[string]FaultModelSummary, baseOrder []string) []string {
	seen := make(map[string]bool, len(baseOrder))
	out := make([]string, 0, len(faultSummaries))
	for _, fid := range baseOrder {
		seen[fid] = true
		if _, ok := faultSummaries[fid]; ok {
			out = append(out, fid)
		}
	}
	var extra []string
	for fid := range faultSummaries {
		if !seen[fid] {
			extra = append(extra, fid)
		}
	}
	sort.Strings(extra)
	return append(out, extra...)
}

func orderedCodesFromSummaryMap(m map[string]AggregatedStrategyStats, baseOrder []string) []string {
	seen := make(map[string]bool, len(baseOrder))
	out := make([]string, 0, len(m))
	for _, code := range baseOrder {
		seen[code] = true
		if _, ok := m[code]; ok {
			out = append(out, code)
		}
	}
	var extra []string
	for code := range m {
		if !seen[code] {
			extra = append(extra, code)
		}
	}
	sort.Strings(extra)
	return append(out, extra...)
}

func activeTheoreticalExpectationMean(runs []SingleRunResult) float64 {
	var theoSum float64
	active := 0
	for _, r := range runs {
		if r.TotalFailures > 0 {
			theoSum += r.TheoreticalExpectation
			active++
		}
	}
	if active == 0 {
		return 0.0
	}
	return theoSum / float64(active)
}

// WriteMultiSeedCSV exports the complete multi-seed aggregated metrics with 95% CIs to CSV.
func WriteMultiSeedCSV(res *BenchmarkResult, csvPath string) error {
	if res == nil {
		return errors.New("write multi-seed csv: nil BenchmarkResult")
	}
	return writeCSVAtomic(csvPath, func(w *csv.Writer) error {
		header := []string{
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
		if err := w.Write(header); err != nil {
			return err
		}

		gitCommit := res.GitCommit
		if gitCommit == "" {
			gitCommit = "unknown"
		}

		faultOrder := []string{
			faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4,
			faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8,
			faults.MUT9,
		}
		allFaults := orderedSummaryFaults(res.FaultSummaries, faultOrder)
		stratOrder := []string{"dcg", "dcg-pr", "dcg-flag-only", "r2", "r1", "b6", "b0", "b7", "b5", "b4", "b1", "b2", "b3"}

		for _, fid := range allFaults {
			summ := res.FaultSummaries[fid]
			theoMean := activeTheoreticalExpectationMean(summ.Runs)

			codes := orderedCodesFromSummaryMap(summ.StrategySummaries, stratOrder)
			for _, code := range codes {
				s := summ.StrategySummaries[code]
				row := []string{
					fid,
					s.StrategyName,
					fmt.Sprintf("%.2f", s.K1FailureMedian),
					fmt.Sprintf("%.2f", s.K1FailureMedianCI95Lower),
					fmt.Sprintf("%.2f", s.K1FailureMedianCI95Upper),
					fmt.Sprintf("%.2f", s.K1FailureMean),
					fmt.Sprintf("%.2f", s.K1FailureMeanCI95Lower),
					fmt.Sprintf("%.2f", s.K1FailureMeanCI95Upper),
					fmt.Sprintf("%.2f", s.K1FailureP10),
					fmt.Sprintf("%.2f", s.K1FailureP90),
					fmt.Sprintf("%.2f", s.S25Median),
					fmt.Sprintf("%.2f", s.S25MedianCI95Lower),
					fmt.Sprintf("%.2f", s.S25MedianCI95Upper),
					fmt.Sprintf("%.2f", s.S25Mean),
					fmt.Sprintf("%.2f", s.S25MeanCI95Lower),
					fmt.Sprintf("%.2f", s.S25MeanCI95Upper),
					fmt.Sprintf("%.2f", s.S100Median),
					fmt.Sprintf("%.2f", s.S100MedianCI95Lower),
					fmt.Sprintf("%.2f", s.S100MedianCI95Upper),
					fmt.Sprintf("%.2f", s.S100Mean),
					fmt.Sprintf("%.2f", s.S100MeanCI95Lower),
					fmt.Sprintf("%.2f", s.S100MeanCI95Upper),
					fmt.Sprintf("%.2f", s.BFullMedian),
					fmt.Sprintf("%.2f", s.BFullMean),
					fmt.Sprintf("%.4f", s.AUCNormFailuresMean),
					fmt.Sprintf("%.4f", s.AUCNormBugsMean),
					fmt.Sprintf("%.2f", theoMean),
					gitCommit,
				}
				if err := w.Write(row); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func formatPolicies(policies []int) string {
	if len(policies) == 0 {
		return "0%/100%"
	}
	parts := make([]string, len(policies))
	for i, p := range policies {
		parts[i] = fmt.Sprintf("%d%%", p)
	}
	return strings.Join(parts, "/")
}

func workloadDescription(cfg workload.Config) string {
	ops := cfg.Operations
	if ops <= 0 {
		ops = 120
	}
	syncEvery := cfg.SyncEvery
	if syncEvery <= 0 {
		syncEvery = 8
	}
	flushEvery := cfg.FlushEvery
	if flushEvery <= 0 {
		flushEvery = 40
	}
	if cfg.Preset != "" {
		keys := "preset default"
		if cfg.KeyCount > 0 {
			keys = fmt.Sprintf("%d", cfg.KeyCount)
		}
		valBytes := "preset default (64B)"
		if cfg.ValueBytes > 0 {
			valBytes = fmt.Sprintf("%dB", cfg.ValueBytes)
		}
		return fmt.Sprintf("%d client operations per seed using preset `%s` (keys: %s, value size: %s, `SyncEvery = %d`, `FlushEvery = %d`)",
			ops, cfg.Preset, keys, valBytes, syncEvery, flushEvery)
	}
	keys := cfg.KeyCount
	if keys <= 0 {
		keys = 16
	}
	valBytes := cfg.ValueBytes
	if valBytes <= 0 {
		valBytes = 64
	}
	return fmt.Sprintf("%d client operations per seed (50%% Set, 20%% Get, 10%% Delete, 10%% Two-Key Atomic Batch, 5%% Range Delete, 5%% Iteration; %d keys, %dB values, `SyncEvery = %d`, `FlushEvery = %d`)",
		ops, keys, valBytes, syncEvery, flushEvery)
}

// GenerateMultiSeedReportMarkdown renders the comprehensive empirical comparison report as Markdown.
func GenerateMultiSeedReportMarkdown(res *BenchmarkResult) string {
	if res == nil {
		return "# CraConTest: Multi-Seed Evaluation Across Fault Models\n\n_No benchmark results available._\n"
	}

	var sb strings.Builder

	dateStr := res.Timestamp
	if len(dateStr) >= 10 {
		dateStr = dateStr[:10]
	} else if dateStr == "" {
		dateStr = "not recorded"
	}
	commit := res.GitCommit
	if commit == "" {
		commit = "unknown"
	}
	goVersion := res.GoVersion
	if goVersion == "" {
		goVersion = "not recorded"
	}
	pebbleVersion := res.PebbleVersion
	if pebbleVersion == "" {
		pebbleVersion = DetectPebbleVersion()
	}

	policiesStr := formatPolicies(res.Config.Policies)
	walDropK := res.Config.WALDropK
	if walDropK <= 0 {
		walDropK = 1
	}
	dirDropK := res.Config.DirDropK
	if dirDropK <= 0 {
		dirDropK = 1
	}

	faultOrder := []string{
		faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4,
		faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8,
		faults.MUT9,
	}
	allFaults := orderedSummaryFaults(res.FaultSummaries, faultOrder)

	totalCandidatesEvaluated := 0
	for _, fid := range allFaults {
		for _, r := range res.FaultSummaries[fid].Runs {
			totalCandidatesEvaluated += r.UniverseSize
		}
	}

	sb.WriteString("# CraConTest: Multi-Seed Evaluation Across Fault Models\n\n")
	sb.WriteString(fmt.Sprintf("**Date:** %s  \n", dateStr))
	sb.WriteString(fmt.Sprintf("**Git Commit:** `%s`  \n", commit))
	sb.WriteString(fmt.Sprintf("**Evaluation Engine:** CockroachDB Pebble %s (Go runtime: %s)  \n", pebbleVersion, goVersion))
	sb.WriteString(fmt.Sprintf("**Workload Seeds (%d):** `%v`  \n", len(res.Config.WorkloadSeeds), res.Config.WorkloadSeeds))
	sb.WriteString(fmt.Sprintf("**Retention Policies:** `%s`  \n", policiesStr))
	sb.WriteString(fmt.Sprintf("**Execution Time:** %.2f seconds total  \n", res.TotalDuration.Seconds()))
	if res.MUT0Runs > 0 {
		sb.WriteString(fmt.Sprintf("**Null Control (MUT-0):** %d observed failures across %d evaluated seeds at %s retention  \n\n",
			res.MUT0Violations, res.MUT0Runs, policiesStr))
	} else {
		sb.WriteString("**Null Control (MUT-0):** not evaluated (`TrackMUT0=false`)  \n\n")
	}
	sb.WriteString("---\n\n")

	sb.WriteString("## 1. Executive Summary\n\n")
	faultListDesc := "none"
	if len(allFaults) > 0 {
		faultListDesc = strings.Join(allFaults, ", ")
	}
	sb.WriteString(fmt.Sprintf("This report documents the empirical evaluation of crash-consistency ranking strategies across %d deterministic workload seeds on CockroachDB's Pebble key-value store, covering %d fault model(s) (`%s`) at %s retention.\n\n",
		len(res.Config.WorkloadSeeds), len(allFaults), faultListDesc, policiesStr))

	sb.WriteString("### 1.1 Key Findings\n\n")

	mut5Summ, hasM5 := res.FaultSummaries[faults.MUT5]
	dcgM5, hasDCGM5 := mut5Summ.StrategySummaries["dcg"]
	r2M5, hasR2M5 := mut5Summ.StrategySummaries["r2"]
	b0M5, hasB0M5 := mut5Summ.StrategySummaries["b0"]
	if hasM5 && hasDCGM5 && hasR2M5 && effectiveActiveSeeds(mut5Summ) > 0 && dcgM5.K1FailureMedian > 0 && dcgM5.K1FailureMedian < r2M5.K1FailureMedian {
		sb.WriteString("**DCG achieves lower cost-to-first-failure than R2 on sparse faults.**\n\n")
		sb.WriteString("Observed comparison:\n")
		medianRatioR2 := r2M5.K1FailureMedian / dcgM5.K1FailureMedian
		b0Clause := ""
		if hasB0M5 && b0M5.K1FailureMedian > 0 {
			medianRatioB0 := b0M5.K1FailureMedian / dcgM5.K1FailureMedian
			b0Clause = fmt.Sprintf(", and B0 median $k_1 = %.1f$ (mean $%.2f$; B0/DCG median ratio %.1f)",
				b0M5.K1FailureMedian, b0M5.K1FailureMean, medianRatioB0)
		}
		sb.WriteString(fmt.Sprintf("- **MUT-5 (Sparse $k$-th WAL Sync Drop, $k=%d$):** DCG median $k_1 = %.1f$ (mean $%.2f$, 95%% CI $[%.2f, %.2f]$), R2 median $k_1 = %.1f$ (mean $%.2f$, 95%% CI $[%.2f, %.2f]$; R2/DCG median ratio %.1f)%s; ratios are candidate-count ratios, not wall-clock speedups.\n",
			walDropK, dcgM5.K1FailureMedian, dcgM5.K1FailureMean, dcgM5.K1FailureMeanCI95Lower, dcgM5.K1FailureMeanCI95Upper,
			r2M5.K1FailureMedian, r2M5.K1FailureMean, r2M5.K1FailureMeanCI95Lower, r2M5.K1FailureMeanCI95Upper,
			medianRatioR2, b0Clause))
	} else {
		sb.WriteString("**Evaluation metrics detailed below.**\n\n")
	}

	if res.MUT0Runs > 0 {
		sb.WriteString(fmt.Sprintf("- **Null-control observation (MUT-0):** %d failures across %d evaluated seeds at %s retention.\n",
			res.MUT0Violations, res.MUT0Runs, policiesStr))
	} else {
		sb.WriteString("- **Null-control observation (MUT-0):** not evaluated.\n")
	}

	mut8Summ, hasM8 := res.FaultSummaries[faults.MUT8]
	dcgM8, hasDCGM8 := mut8Summ.StrategySummaries["dcg"]
	r2M8, hasR2M8 := mut8Summ.StrategySummaries["r2"]
	b0M8, hasB0M8 := mut8Summ.StrategySummaries["b0"]
	if hasM8 && hasDCGM8 && hasR2M8 && hasB0M8 {
		sb.WriteString(fmt.Sprintf("- **MUT-8 (Compaction-Triggered SST Sync Drop):** DCG reaches the first failure at median $k_1 = %.1f$ (Mean %.2f) vs R2 median $k_1 = %.1f$ (Mean %.2f) and B0 median $k_1 = %.1f$ (Mean %.2f), discovers $S_{25} = %.2f$ mean distinct failing states at $B=25$ (vs R2 %.2f and B0 %.2f), and $S_{100}$ Mean %.2f (vs R2 %.2f and B0 %.2f).\n",
			dcgM8.K1FailureMedian, dcgM8.K1FailureMean,
			r2M8.K1FailureMedian, r2M8.K1FailureMean,
			b0M8.K1FailureMedian, b0M8.K1FailureMean,
			dcgM8.S25Mean, r2M8.S25Mean, b0M8.S25Mean,
			dcgM8.S100Mean, r2M8.S100Mean, b0M8.S100Mean))
	}

	sb.WriteString("\n---\n\n")
	sb.WriteString("## 2. Experimental Methodology\n\n")
	sb.WriteString("### 2.1 Evaluated Storage Engine & Calibrated Workloads\n\n")
	sb.WriteString(fmt.Sprintf("- **Engine:** CockroachDB Pebble `%s` running in-memory with virtual filesystem recording (`internal/vfsrec`).\n", pebbleVersion))
	sb.WriteString(fmt.Sprintf("- **Workload Configuration:** %s.\n", workloadDescription(res.Config.Workload)))
	sb.WriteString(fmt.Sprintf("- **Evaluated Seed Set:** %d deterministic workload seeds across %d fault summary group(s), totaling %d evaluated candidate states.\n\n",
		len(res.Config.WorkloadSeeds), len(allFaults), totalCandidatesEvaluated))

	sb.WriteString("### 2.2 Fault Model Taxonomy\n\n")
	sb.WriteString("| Fault Model | Class | Target File / Trigger | Recovery Symptom | Failure Density |\n")
	sb.WriteString("|---|---|---|---|---|\n")
	densityOf := func(fid, fallback string) string {
		if summ, ok := res.FaultSummaries[fid]; ok {
			return fmt.Sprintf("%.1f%%", summ.FaultDensityPct)
		}
		return fallback
	}
	if res.MUT0Runs > 0 {
		sb.WriteString(fmt.Sprintf("| **MUT-0** | Control | Unfaulted filesystem | %d observed failures across %d runs | 0.0%% (observed: %d) |\n",
			res.MUT0Violations, res.MUT0Runs, res.MUT0Violations))
	} else {
		sb.WriteString("| **MUT-0** | Control | Unfaulted filesystem | Not evaluated | not evaluated |\n")
	}
	sb.WriteString(fmt.Sprintf("| **MUT-1** | Dense | All WAL `Sync()` calls suppressed | Prefix / durability violation (`v1:<sha256>`) | %s |\n", densityOf(faults.MUT1, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-2** | Dense/Sparse | MANIFEST `Sync()` calls suppressed | Open error (`v1:open_error:manifest_corrupt`) | %s |\n", densityOf(faults.MUT2, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-3** | Dense/Sparse | Parent directory syncs suppressed | Prefix / durability violation (`v1:<sha256>`) | %s |\n", densityOf(faults.MUT3, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-4** | Dense/Sparse | Memtable SST flush syncs suppressed | Open error (`v1:open_error:table_corrupt`) | %s |\n", densityOf(faults.MUT4, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-5** | Sparse | Strictly $k$-th WAL sync dropped ($k=%d$) | Prefix / durability violation (`v1:<sha256>`) | %s |\n", walDropK, densityOf(faults.MUT5, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-6** | Sparse | Second `MANIFEST` sync dropped (first post-open manifest sync) | Prefix / durability violation (`v1:<sha256>`) | %s |\n", densityOf(faults.MUT6, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-7** | No-op Control | Directory sync drop after `CURRENT`/`OPTIONS` rename | None (Pebble v2 creates `CURRENT` once on Open) | %s |\n", densityOf(faults.MUT7, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-8** | Windowed | Compaction/flush SST sync drop after `CompactionBegin` (RocksDB #10460) | Open error (`v1:open_error:table_corrupt`) | %s |\n", densityOf(faults.MUT8, "not evaluated")))
	sb.WriteString(fmt.Sprintf("| **MUT-9** | Dense/Sparse | Single $k$-th directory sync drop ($k=%d$) | Prefix / durability violation (`v1:<sha256>`) | %s ($k=%d$) |\n\n", dirDropK, densityOf(faults.MUT9, "not evaluated"), dirDropK))

	sb.WriteString("### 2.3 Evaluated Ranking Strategies\n\n")
	sb.WriteString("1. **DCG (Durability-Contract Guided):** Trace-derived durability-contract analysis that prioritizes concrete durability contract violations (dangling table references, uncommitted directory renames, dropped WAL sync records) before exploring uncontracted state representatives.\n")
	sb.WriteString("2. **DCG-PR (Partial-Retention Prior):** Variant of DCG that promotes partial-retention cutoffs with coexisting unsynced directory entries (coinciding with `DCG` under endpoint 0%/100% benchmark policies).\n")
	sb.WriteString("3. **DCG-FlagOnly (Contract-Flag Ablation):** Partitions candidate space strictly by contract violation flags without secondary scoring heuristics.\n")
	sb.WriteString("4. **RHG (Reachability-Hazard Guided / B7):** Scores cutoffs by reachable uncommitted mutations and hazard surfaces.\n")
	sb.WriteString("5. **R2 (State-Representative Random):** Partitions 0% retention candidates into equivalence classes by canonical `StateKey`, selects boundary-adjacent representatives within each `StateKey` class before shuffling classes, and tests redundant cutoffs afterward.\n")
	sb.WriteString("6. **R1 (0%-Retention-First Random):** Partitions universe into 0% unsynced data retention and 100% retention; evaluates 0% retention first in random order.\n")
	sb.WriteString("7. **B1/B2/B3/B4/B5/B6 (Deterministic Baselines):** FIFO (`b1`), LIFO (`b2`), Stride (`b3`), Combined (`b4`), Representative (`b5`), and Sync-Boundary (`b6`).\n")
	sb.WriteString("8. **B0 (Uniform Random):** Pure random selection across candidate cutoffs.\n\n")

	sb.WriteString("---\n\n")
	sb.WriteString("## 3. Empirical Results & Statistical Summary\n\n")

	stratsOrder := []string{"dcg", "dcg-pr", "dcg-flag-only", "b7", "r2", "r1", "b6", "b5", "b4", "b1", "b2", "b3", "b0"}
	for idx, fid := range allFaults {
		summ := res.FaultSummaries[fid]
		theoMean := activeTheoreticalExpectationMean(summ.Runs)

		sb.WriteString(fmt.Sprintf("### 3.%d %s Evaluation (%d Seeds, %d Active w/ Failures: Avg %.0f Candidates, Density: %.1f%%)\n\n",
			idx+1, fid, summ.RunCount, effectiveActiveSeeds(summ), summ.AvgUniverseSize, summ.FaultDensityPct))

		sb.WriteString(fmt.Sprintf("- **Average Failures per Run:** %.1f (Violations: %.1f, Open Errors: %.1f, Panics: %.1f, Hangs: %.1f)\n",
			summ.AvgFailures, summ.AvgViolations, summ.AvgOpenErrors, summ.AvgPanics, summ.AvgHangs))
		sb.WriteString(fmt.Sprintf("- **Theoretical Expectation $E[K_1] = (N+1)/(m+1)$ (active seeds):** %.2f\n\n", theoMean))

		sb.WriteString("| Strategy | $k_1$ Median [95% CI] | $k_1$ Mean [95% CI] | $k_1$ $P_{10}$ | $k_1$ $P_{90}$ | $S_{25}$ Median | $S_{25}$ Mean | $S_{100}$ Median | $S_{100}$ Mean | $B_{\\text{full}}$ Median | $\\text{AUC}_{\\text{norm}}$ |\n")
		sb.WriteString("|---|---|---|---|---|---|---|---|---|---|---|\n")

		codes := orderedCodesFromSummaryMap(summ.StrategySummaries, stratsOrder)
		for _, code := range codes {
			s := summ.StrategySummaries[code]
			nameStr := s.StrategyName
			switch code {
			case "dcg":
				nameStr = "**DCG (Guided)**"
			case "dcg-pr":
				nameStr = "**DCG-PR (Partial Prior)**"
			case "dcg-flag-only":
				nameStr = "**DCG-FlagOnly**"
			case "b7":
				nameStr = "**RHG (Risk-Horizon)**"
			case "r2":
				nameStr = "**R2 (Representative)**"
			case "r1":
				nameStr = "R1 (0%-Retention)"
			case "b2":
				nameStr = "LIFO (Recent-First)"
			case "b6":
				nameStr = "B6 (Sync-Boundary)"
			case "b0":
				nameStr = "B0 (Uniform Random)"
			}

			medCI := fmt.Sprintf("%.1f [%.1f, %.1f]", s.K1FailureMedian, s.K1FailureMedianCI95Lower, s.K1FailureMedianCI95Upper)
			meanCI := fmt.Sprintf("%.2f [%.2f, %.2f]", s.K1FailureMean, s.K1FailureMeanCI95Lower, s.K1FailureMeanCI95Upper)

			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %.1f | %.1f | %.1f | %.2f | %.1f | %.2f | %.1f | %.4f |\n",
				nameStr, medCI, meanCI, s.K1FailureP10, s.K1FailureP90,
				s.S25Median, s.S25Mean, s.S100Median, s.S100Mean, s.BFullMedian, s.AUCNormFailuresMean))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("---\n\n")
	sb.WriteString("## 4. Comparative Analysis\n\n")
	if hasM5 && hasDCGM5 && hasR2M5 {
		sb.WriteString("### 4.1 Cost-to-First-Failure ($k_1$) on MUT-5\n\n")
		sb.WriteString(fmt.Sprintf("On **MUT-5** ($k=%d$ WAL sync drop across %d active seeds):\n\n", walDropK, effectiveActiveSeeds(mut5Summ)))
		sb.WriteString(fmt.Sprintf("1. **R2 (Representative Random):** Observed median $k_1 = %.1f$ (mean $%.2f$, $P_{10} = %.1f$, $P_{90} = %.1f$).\n",
			r2M5.K1FailureMedian, r2M5.K1FailureMean, r2M5.K1FailureP10, r2M5.K1FailureP90))
		sb.WriteString(fmt.Sprintf("2. **DCG (Durability-Contract Guided):** Observed median $k_1 = %.1f$ (mean $%.2f$, $P_{10} = %.1f$, $P_{90} = %.1f$).\n\n",
			dcgM5.K1FailureMedian, dcgM5.K1FailureMean, dcgM5.K1FailureP10, dcgM5.K1FailureP90))
	}

	mut9Summ, hasM9 := res.FaultSummaries[faults.MUT9]
	dcgM9, hasDCGM9 := mut9Summ.StrategySummaries["dcg"]
	flagM9, hasFlagM9 := mut9Summ.StrategySummaries["dcg-flag-only"]
	r2M9, hasR2M9 := mut9Summ.StrategySummaries["r2"]
	b0M9, hasB0M9 := mut9Summ.StrategySummaries["b0"]
	flagM8, hasFlagM8 := mut8Summ.StrategySummaries["dcg-flag-only"]
	if hasM8 && hasM9 && hasDCGM8 && hasR2M8 && hasB0M8 && hasDCGM9 && hasR2M9 && hasB0M9 {
		sb.WriteString("### 4.2 Early-Budget State Coverage ($S_{25}$ vs $S_{100}$) and Ablation\n\n")
		sb.WriteString(fmt.Sprintf("On **MUT-9** ($k=%d$), DCG observed mean $S_{25} = %.2f$ (R2: %.2f, B0: %.2f) and mean $S_{100} = %.2f$ (R2: %.2f, B0: %.2f). On **MUT-8**, DCG observed mean $S_{25} = %.2f$ (R2: %.2f, B0: %.2f) and mean $S_{100} = %.2f$ (R2: %.2f, B0: %.2f).\n\n",
			dirDropK, dcgM9.S25Mean, r2M9.S25Mean, b0M9.S25Mean, dcgM9.S100Mean, r2M9.S100Mean, b0M9.S100Mean,
			dcgM8.S25Mean, r2M8.S25Mean, b0M8.S25Mean, dcgM8.S100Mean, r2M8.S100Mean, b0M8.S100Mean))
		if hasFlagM9 && hasFlagM8 {
			sb.WriteString(fmt.Sprintf("- **DCG-FlagOnly Ablation:** Mean $k_1 = %.2f$ / $S_{25} = %.2f$ on MUT-9 and mean $k_1 = %.2f$ / $S_{25} = %.2f$ on MUT-8.\n",
				flagM9.K1FailureMean, flagM9.S25Mean, flagM8.K1FailureMean, flagM8.S25Mean))
		}
		if mut6Summ, hasM6 := res.FaultSummaries[faults.MUT6]; hasM6 {
			dcgM6, okD6 := mut6Summ.StrategySummaries["dcg"]
			flagM6, okF6 := mut6Summ.StrategySummaries["dcg-flag-only"]
			if okD6 && okF6 {
				sb.WriteString(fmt.Sprintf("- **MUT-6 Tie-Breaking (`ContractScore`):** Full DCG achieved mean $k_1 = %.2f$ compared to %.2f for `DCG-FlagOnly`.\n",
					dcgM6.K1FailureMean, flagM6.K1FailureMean))
			}
		}
		sb.WriteString("\n")
	}

	if hasM8 && hasDCGM8 && hasR2M8 && hasB0M8 {
		sb.WriteString("### 4.3 MUT-8 Evaluation (Compaction-Triggered SST Sync Drop)\n\n")
		sb.WriteString(fmt.Sprintf("Under MUT-8 (%.1f%% failure density across %d seeds), DCG observed median $k_1 = %.1f$ (mean %.2f) vs R2 median $k_1 = %.1f$ (mean %.2f) and B0 median $k_1 = %.1f$ (mean %.2f), with $S_{25}$ mean %.2f (R2: %.2f, B0: %.2f) and $S_{100}$ mean %.2f (R2: %.2f, B0: %.2f).\n\n",
			mut8Summ.FaultDensityPct, mut8Summ.RunCount,
			dcgM8.K1FailureMedian, dcgM8.K1FailureMean,
			r2M8.K1FailureMedian, r2M8.K1FailureMean,
			b0M8.K1FailureMedian, b0M8.K1FailureMean,
			dcgM8.S25Mean, r2M8.S25Mean, b0M8.S25Mean,
			dcgM8.S100Mean, r2M8.S100Mean, b0M8.S100Mean))
	}

	return sb.String()
}

// WriteMultiSeedReportToFile writes the markdown report atomically to targetPath.
func WriteMultiSeedReportToFile(res *BenchmarkResult, targetPath string) error {
	if res == nil {
		return errors.New("write multi-seed report: nil BenchmarkResult")
	}
	content := GenerateMultiSeedReportMarkdown(res)
	return writeBytesAtomic(targetPath, func(w io.Writer) error {
		_, err := w.Write([]byte(content))
		return err
	})
}

// WriteMultiSeedPerSeedCSV exports unaggregated per-seed discovery metrics for all evaluated strategies.
func WriteMultiSeedPerSeedCSV(res *BenchmarkResult, csvPath string) error {
	if res == nil {
		return errors.New("write multi-seed per-seed csv: nil BenchmarkResult")
	}
	return writeCSVAtomic(csvPath, func(w *csv.Writer) error {
		header := []string{
			"fault_id", "seed", "universe_size", "total_failures", "fault_density_pct",
			"strategy_code", "strategy", "k1_single", "s25_single",
			"k1_failure_mean", "s25_mean", "s100_mean", "b_full_mean",
			"auc_norm_failures", "auc_norm_bugs", "git_commit",
		}
		if err := w.Write(header); err != nil {
			return err
		}

		gitCommit := res.GitCommit
		if gitCommit == "" {
			gitCommit = "unknown"
		}

		faultOrder := []string{
			faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4,
			faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8,
			faults.MUT9,
		}
		allFaults := orderedSummaryFaults(res.FaultSummaries, faultOrder)
		stratOrder := []string{"dcg", "dcg-pr", "dcg-flag-only", "r2", "r1", "b6", "b0", "b7", "b5", "b4", "b1", "b2", "b3"}

		for _, fid := range allFaults {
			summ := res.FaultSummaries[fid]
			if len(summ.Runs) == 0 {
				continue
			}

			for _, r := range summ.Runs {
				densPct := 0.0
				if r.UniverseSize > 0 {
					densPct = float64(r.TotalFailures) / float64(r.UniverseSize) * 100.0
				}

				stratKeysSeen := make(map[string]bool)
				for _, code := range stratOrder {
					stratKeysSeen[code] = true
					if code == "b0" {
						if !hasB0Metrics(r) {
							continue
						}
						row := []string{
							fid,
							fmt.Sprintf("%d", r.Seed),
							fmt.Sprintf("%d", r.UniverseSize),
							fmt.Sprintf("%d", r.TotalFailures),
							fmt.Sprintf("%.2f", densPct),
							"b0",
							strategyDisplayName("b0", ""),
							fmt.Sprintf("%d", r.B0.K1SingleSeed),
							fmt.Sprintf("%d", r.B0.S25SingleSeed),
							fmt.Sprintf("%.2f", r.B0.K1FailureMean),
							fmt.Sprintf("%.2f", r.B0.S25Mean),
							fmt.Sprintf("%.2f", r.B0.S100Mean),
							fmt.Sprintf("%.2f", r.B0.BFullMean),
							fmt.Sprintf("%.4f", r.B0.AUCNormFailures),
							fmt.Sprintf("%.4f", r.B0.AUCNormBugs),
							gitCommit,
						}
						if err := w.Write(row); err != nil {
							return err
						}
					} else if m, exists := r.Strategies[code]; exists {
						k1Fail, _, bFull, s25, s100 := resolveStrategyMetrics(m)
						sName := m.StrategyName
						if sName == "" {
							sName = strategyDisplayName(code, code)
						}
						row := []string{
							fid,
							fmt.Sprintf("%d", r.Seed),
							fmt.Sprintf("%d", r.UniverseSize),
							fmt.Sprintf("%d", r.TotalFailures),
							fmt.Sprintf("%.2f", densPct),
							code,
							sName,
							fmt.Sprintf("%d", m.K1SingleSeed),
							fmt.Sprintf("%d", m.S25SingleSeed),
							fmt.Sprintf("%.2f", k1Fail),
							fmt.Sprintf("%.2f", s25),
							fmt.Sprintf("%.2f", s100),
							fmt.Sprintf("%.2f", bFull),
							fmt.Sprintf("%.4f", m.AUCNormFailures),
							fmt.Sprintf("%.4f", m.AUCNormBugs),
							gitCommit,
						}
						if err := w.Write(row); err != nil {
							return err
						}
					}
				}

				var extraCodes []string
				for code := range r.Strategies {
					if !stratKeysSeen[code] {
						extraCodes = append(extraCodes, code)
					}
				}
				sort.Strings(extraCodes)
				for _, code := range extraCodes {
					m := r.Strategies[code]
					k1Fail, _, bFull, s25, s100 := resolveStrategyMetrics(m)
					sName := m.StrategyName
					if sName == "" {
						sName = strategyDisplayName(code, code)
					}
					row := []string{
						fid,
						fmt.Sprintf("%d", r.Seed),
						fmt.Sprintf("%d", r.UniverseSize),
						fmt.Sprintf("%d", r.TotalFailures),
						fmt.Sprintf("%.2f", densPct),
						code,
						sName,
						fmt.Sprintf("%d", m.K1SingleSeed),
						fmt.Sprintf("%d", m.S25SingleSeed),
						fmt.Sprintf("%.2f", k1Fail),
						fmt.Sprintf("%.2f", s25),
						fmt.Sprintf("%.2f", s100),
						fmt.Sprintf("%.2f", bFull),
						fmt.Sprintf("%.4f", m.AUCNormFailures),
						fmt.Sprintf("%.4f", m.AUCNormBugs),
						gitCommit,
					}
					if err := w.Write(row); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}
