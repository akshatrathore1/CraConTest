package exp

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os/exec"
	"path"
	"runtime"
	"sort"
	"strings"
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

// DefaultWorkloadSeeds provides the 10 deterministic workload seeds for baseline benchmarks.
var DefaultWorkloadSeeds = []uint64{42, 100, 200, 300, 400, 500, 600, 700, 800, 900}

// DefaultFaultModels provides the 4 mutation models evaluated in baseline benchmarks.
var DefaultFaultModels = []string{faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4}

// canonicalB0Seeds are the 10 fixed random seeds for curve averaging.
// Use CanonicalB0Seeds() to obtain a fresh copy safe for mutation.
var canonicalB0Seeds = []uint64{1, 2, 3, 4, 5, 42, 100, 777, 1337, 2026}

// CanonicalB0Seeds returns a fresh copy of the 10 fixed random seeds for curve averaging.
// Callers must not retain or mutate the returned slice across goroutines; use the copy.
func CanonicalB0Seeds() []uint64 {
	out := make([]uint64, len(canonicalB0Seeds))
	copy(out, canonicalB0Seeds)
	return out
}

// DefaultB0PercentileCnt defines the standard number of shuffle seeds (100)
// used for computing B0 / random baseline percentiles (p10..p90) and empirical means.
// Test suites may deliberately configure smaller values (e.g. 10 or 50) to keep test
// execution runtimes fast while exercising the shuffle percentile logic.
const DefaultB0PercentileCnt = 100

// MixShuffleSeed hashes a base seed, a domain separator (e.g. strategy hash or stream ID),
// and a shuffle iteration index into a high-entropy 64-bit seed using SplitMix64 mixing
// to eliminate seed collision risks and avoid linear stride patterns.
func MixShuffleSeed(baseSeed uint64, domain uint64, iter int) uint64 {
	z := baseSeed ^ (domain * 0x9e3779b97f4a7c15) + uint64(iter)*0xbf58476d1ce4e5b9 + 0xd2a80a39e703936f
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// BenchmarkConfig defines parameters for multi-seed empirical benchmarking.
type BenchmarkConfig struct {
	WorkloadSeeds   []uint64        `json:"workload_seeds,omitempty"`
	FaultIDs        []string        `json:"fault_ids,omitempty"`
	Workload        workload.Config `json:"workload"`
	Policies        []int           `json:"policies,omitempty"`
	SafeOpenTimeout time.Duration   `json:"safe_open_timeout,omitempty"`
	B0PercentileCnt int             `json:"b0_percentile_cnt,omitempty"` // number of shuffle seeds for B0 percentiles (defaults to DefaultB0PercentileCnt = 100)
	Budgets         []int           `json:"budgets,omitempty"`
	TrackMUT0       bool            `json:"track_mut0,omitempty"` // run MUT-0 control check to verify 0 false alarms
	WALDropK        int             `json:"wal_drop_k,omitempty"` // Drop strictly the k-th WAL sync for MUT5 (default 1)
	DirDropK        int             `json:"dir_drop_k,omitempty"` // Drop strictly the k-th directory sync for MUT9 (default 1)
}

// DefaultBenchmarkConfig returns a calibrated configuration for baseline empirical trials.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		WorkloadSeeds: append([]uint64{}, DefaultWorkloadSeeds...),
		FaultIDs:      append([]string{}, DefaultFaultModels...),
		Workload: workload.Config{
			Seed:       42, // will be overridden per seed run
			Operations: 120,
			KeyCount:   16,
			ValueBytes: 64,
			SyncEvery:  8,
			FlushEvery: 40,
		},
		Policies:        []int{0, 100},
		SafeOpenTimeout: 3 * time.Second,
		B0PercentileCnt: DefaultB0PercentileCnt,
		Budgets:         []int{0, 1, 5, 10, 25, 50, 100, 150, 200},
		TrackMUT0:       true,
	}
}

// StrategyMetrics captures discovery performance for a ranking strategy.
type StrategyMetrics struct {
	StrategyName       string           `json:"strategy_name"`
	K1Failure          int              `json:"k1_failure"` // 1-based index of first non-valid state
	K1FailureFloat     float64          `json:"k1_failure_float,omitempty"`
	K1Violation        int              `json:"k1_violation"` // 1-based index of first consistency violation
	K1ViolationFloat   float64          `json:"k1_violation_float,omitempty"`
	K1SingleSeed       int              `json:"k1_single_seed,omitempty"`
	BFull              int              `json:"b_full"` // budget required to discover all unique bug signatures
	BFullFloat         float64          `json:"b_full_float,omitempty"`
	S25                int              `json:"s25"` // distinct failing states discovered within 25 crashes
	S25Float           float64          `json:"s25_float,omitempty"`
	S25SingleSeed      int              `json:"s25_single_seed,omitempty"`
	S100               int              `json:"s100"` // distinct failing states discovered within 100 crashes
	S100Float          float64          `json:"s100_float,omitempty"`
	Bugs100            int              `json:"bugs100"` // distinct bug signatures discovered within 100 crashes
	AUCNormFailures    float64          `json:"auc_norm_failures"`
	AUCNormBugs        float64          `json:"auc_norm_bugs"`
	Points             []Point          `json:"points"`
	SummaryPoints      []B0SummaryPoint `json:"summary_points,omitempty"`
	K1FailureSamples   []float64        `json:"-"`
	K1ViolationSamples []float64        `json:"-"`
	BFullSamples       []float64        `json:"-"`
	S25Samples         []float64        `json:"-"`
	S100Samples        []float64        `json:"-"`
}

// B0Metrics captures statistical discovery performance for B0 random across shuffle seeds.
type B0Metrics struct {
	K1FailureMean      float64          `json:"k1_failure_mean"`
	K1FailureMedian    float64          `json:"k1_failure_median"`
	K1FailureP10       float64          `json:"k1_failure_p10"`
	K1FailureP90       float64          `json:"k1_failure_p90"`
	K1ViolationMean    float64          `json:"k1_violation_mean"`
	K1ViolationMedian  float64          `json:"k1_violation_median"`
	K1ViolationP10     float64          `json:"k1_violation_p10"`
	K1ViolationP90     float64          `json:"k1_violation_p90"`
	K1SingleSeed       int              `json:"k1_single_seed,omitempty"`
	BFullMean          float64          `json:"b_full_mean"`
	BFullMedian        float64          `json:"b_full_median"`
	BFullP10           float64          `json:"b_full_p10"`
	BFullP90           float64          `json:"b_full_p90"`
	S25Mean            float64          `json:"s25_mean"`
	S25Median          float64          `json:"s25_median"`
	S25P10             float64          `json:"s25_p10"`
	S25P90             float64          `json:"s25_p90"`
	S25SingleSeed      int              `json:"s25_single_seed,omitempty"`
	S100Mean           float64          `json:"s100_mean"`
	S100Median         float64          `json:"s100_median"`
	S100P10            float64          `json:"s100_p10"`
	S100P90            float64          `json:"s100_p90"`
	AUCNormFailures    float64          `json:"auc_norm_failures_mean"`
	AUCNormBugs        float64          `json:"auc_norm_bugs_mean"`
	SummaryPoints      []B0SummaryPoint `json:"summary_points,omitempty"`
	K1FailureSamples   []float64        `json:"-"`
	K1ViolationSamples []float64        `json:"-"`
	BFullSamples       []float64        `json:"-"`
	S25Samples         []float64        `json:"-"`
	S100Samples        []float64        `json:"-"`
}

// SingleRunResult holds execution and strategy metrics for one (seed, fault) pair.
type SingleRunResult struct {
	Seed                   uint64                     `json:"seed"`
	FaultID                string                     `json:"fault_id"`
	UniverseSize           int                        `json:"universe_size"`
	TotalFailures          int                        `json:"total_failures"`
	TotalViolations        int                        `json:"total_violations"`
	TotalOpenErrors        int                        `json:"total_open_errors"`
	TotalPanics            int                        `json:"total_panics,omitempty"`
	TotalHangs             int                        `json:"total_hangs,omitempty"`
	DistinctBugs           int                        `json:"distinct_bugs"`
	TheoreticalExpectation float64                    `json:"theoretical_expectation"` // E[K1] = (N+1)/(m+1)
	B0                     B0Metrics                  `json:"b0"`
	Strategies             map[string]StrategyMetrics `json:"strategies"`
	ElapsedTime            time.Duration              `json:"elapsed_time_ns"`
	PlanRankMs             int64                      `json:"plan_rank_ms,omitempty"`
	ExploreTotalMs         int64                      `json:"explore_total_ms,omitempty"`
}

func hasB0Metrics(r SingleRunResult) bool {
	b := r.B0
	return len(b.SummaryPoints) > 0 ||
		len(b.K1FailureSamples) > 0 ||
		len(b.S25Samples) > 0 ||
		b.K1FailureMean != 0 || b.K1FailureMedian != 0 || b.K1FailureP10 != 0 || b.K1FailureP90 != 0 || b.K1SingleSeed != 0 ||
		b.BFullMean != 0 || b.BFullMedian != 0 || b.BFullP10 != 0 || b.BFullP90 != 0 ||
		b.S25Mean != 0 || b.S25Median != 0 || b.S25P10 != 0 || b.S25P90 != 0 || b.S25SingleSeed != 0 ||
		b.S100Mean != 0 || b.S100Median != 0 || b.S100P10 != 0 || b.S100P90 != 0 ||
		b.AUCNormFailures != 0 || b.AUCNormBugs != 0
}

func effectiveActiveSeeds(s FaultModelSummary) int {
	if s.ActiveSeeds > 0 {
		return s.ActiveSeeds
	}
	if len(s.Runs) > 0 {
		cnt := 0
		for _, r := range s.Runs {
			if r.TotalFailures > 0 {
				cnt++
			}
		}
		return cnt
	}
	if s.TotalSeeds == 0 && s.RunCount > 0 && s.AvgFailures > 0 {
		return s.RunCount
	}
	return s.ActiveSeeds
}

func effectiveTotalSeeds(s FaultModelSummary) int {
	if s.TotalSeeds > 0 {
		return s.TotalSeeds
	}
	if len(s.Runs) > 0 {
		return len(s.Runs)
	}
	return s.RunCount
}

// AggregatedStrategyStats holds aggregated metrics for a strategy across multiple workload seeds.
type AggregatedStrategyStats struct {
	StrategyName             string    `json:"strategy_name"`
	K1FailureMean            float64   `json:"k1_failure_mean"`
	K1FailureMeanCI95Lower   float64   `json:"k1_failure_mean_ci95_lower"`
	K1FailureMeanCI95Upper   float64   `json:"k1_failure_mean_ci95_upper"`
	K1FailureMedian          float64   `json:"k1_failure_median"`
	K1FailureMedianCI95Lower float64   `json:"k1_failure_median_ci95_lower"`
	K1FailureMedianCI95Upper float64   `json:"k1_failure_median_ci95_upper"`
	K1FailureP10             float64   `json:"k1_failure_p10"`
	K1FailureP90             float64   `json:"k1_failure_p90"`
	K1ViolationMean          float64   `json:"k1_violation_mean"`
	K1ViolationMedian        float64   `json:"k1_violation_median"`
	BFullMean                float64   `json:"b_full_mean"`
	BFullMedian              float64   `json:"b_full_median"`
	S25Mean                  float64   `json:"s25_mean"`
	S25MeanCI95Lower         float64   `json:"s25_mean_ci95_lower"`
	S25MeanCI95Upper         float64   `json:"s25_mean_ci95_upper"`
	S25Median                float64   `json:"s25_median"`
	S25MedianCI95Lower       float64   `json:"s25_median_ci95_lower"`
	S25MedianCI95Upper       float64   `json:"s25_median_ci95_upper"`
	S100Mean                 float64   `json:"s100_mean"`
	S100MeanCI95Lower        float64   `json:"s100_mean_ci95_lower"`
	S100MeanCI95Upper        float64   `json:"s100_mean_ci95_upper"`
	S100Median               float64   `json:"s100_median"`
	S100MedianCI95Lower      float64   `json:"s100_median_ci95_lower"`
	S100MedianCI95Upper      float64   `json:"s100_median_ci95_upper"`
	AUCNormFailuresMean      float64   `json:"auc_norm_failures_mean"`
	AUCNormBugsMean          float64   `json:"auc_norm_bugs_mean"`
	CurveMeanFailures        []float64 `json:"curve_mean_failures"`
	CurveMeanBugs            []float64 `json:"curve_mean_bugs"`
}

// FaultModelSummary captures multi-seed aggregation for one fault model.
type FaultModelSummary struct {
	FaultID           string                             `json:"fault_id"`
	RunCount          int                                `json:"run_count"`
	ActiveSeeds       int                                `json:"active_seeds"`
	TotalSeeds        int                                `json:"total_seeds"`
	AvgUniverseSize   float64                            `json:"avg_universe_size"`
	AvgFailures       float64                            `json:"avg_failures"`
	AvgViolations     float64                            `json:"avg_violations"`
	AvgOpenErrors     float64                            `json:"avg_open_errors"`
	AvgPanics         float64                            `json:"avg_panics,omitempty"`
	AvgHangs          float64                            `json:"avg_hangs,omitempty"`
	AvgDistinctBugs   float64                            `json:"avg_distinct_bugs"`
	FaultDensityPct   float64                            `json:"fault_density_pct"` // avgFailures / avgUniverse * 100
	StrategySummaries map[string]AggregatedStrategyStats `json:"strategy_summaries"`
	Budgets           []int                              `json:"budgets"`
	Runs              []SingleRunResult                  `json:"runs"`
}

// BenchmarkResult encapsulates the complete multi-seed benchmarking results.
type BenchmarkResult struct {
	Timestamp      string                       `json:"timestamp"`
	GitCommit      string                       `json:"git_commit,omitempty"`
	GoVersion      string                       `json:"go_version,omitempty"`
	PebbleVersion  string                       `json:"pebble_version,omitempty"`
	Config         BenchmarkConfig              `json:"config"`
	FaultSummaries map[string]FaultModelSummary `json:"fault_summaries"`
	MUT0Violations int                          `json:"mut0_violations"`
	MUT0Runs       int                          `json:"mut0_runs"`
	TotalDuration  time.Duration                `json:"total_duration_ns"`
}

// CalculateAUCNorm computes normalized area under the discovery curve using the trapezoidal rule.
// Normalized by universeSize * totalTarget, returning a value in [0, 1].
func CalculateAUCNorm(points []Point, universeSize int, totalTarget int, useBugs bool) float64 {
	if universeSize <= 0 || totalTarget <= 0 || len(points) == 0 {
		return 0.0
	}

	// Clamp budgets into [0, universeSize] and deduplicate deterministically,
	// keeping the highest discovered count if multiple points clamp to the same budget.
	byBudget := make(map[int]float64, len(points)+1)
	for _, p := range points {
		b := p.Budget
		if b < 0 {
			b = 0
		}
		if b > universeSize {
			b = universeSize
		}
		yVal := float64(p.Failures)
		if useBugs {
			yVal = float64(p.DistinctBugs)
		}
		if prev, ok := byBudget[b]; !ok || yVal > prev {
			byBudget[b] = yVal
		}
	}
	if _, ok := byBudget[0]; !ok {
		byBudget[0] = 0.0
	}

	budgets := make([]int, 0, len(byBudget)+1)
	for b := range byBudget {
		budgets = append(budgets, b)
	}
	sort.Ints(budgets)

	// If last evaluated budget < universeSize, hold the observed terminal value
	// conservatively rather than fabricating undiscovered failures.
	if lastB := budgets[len(budgets)-1]; lastB < universeSize {
		byBudget[universeSize] = byBudget[lastB]
		budgets = append(budgets, universeSize)
	}

	auc := 0.0
	for i := 1; i < len(budgets); i++ {
		db := float64(budgets[i] - budgets[i-1])
		avgY := (byBudget[budgets[i-1]] + byBudget[budgets[i]]) / 2.0
		auc += avgY * db
	}

	maxPossibleAUC := float64(universeSize) * float64(totalTarget)
	if maxPossibleAUC <= 0 {
		return 0.0
	}
	norm := auc / maxPossibleAUC
	if norm > 1.0 {
		norm = 1.0
	}
	if norm < 0.0 {
		norm = 0.0
	}
	return norm
}

// CalculateBFull computes the budget required to discover all unique bug signatures.
// Returns 0 if there are no bug signatures in outcomes or if outcomes contain oracle_error/duplicates.
func CalculateBFull(ranking []candidate.Candidate, outcomes []oracle.Outcome) int {
	outcomes = NormalizeTable(outcomes)
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0
	}

	targetSigs := make(map[string]bool)
	for _, o := range outcomes {
		if isFailureClass(o.Class) && o.Signature != "" {
			targetSigs[o.Signature] = true
		}
	}
	if len(targetSigs) == 0 {
		return 0
	}

	seenCand := make(map[candidate.Candidate]bool, len(ranking))
	seenSigs := make(map[string]bool, len(targetSigs))
	for i, c := range ranking {
		if seenCand[c] {
			continue
		}
		seenCand[c] = true
		o, ok := outcomeMap[c]
		if ok && isFailureClass(o.Class) && o.Signature != "" {
			seenSigs[o.Signature] = true
			if len(seenSigs) == len(targetSigs) {
				return i + 1 // 1-based budget
			}
		}
	}

	return len(ranking)
}

// CalculateBFullChecked strictly validates ranking and outcomes before computing B_full.
func CalculateBFullChecked(ranking []candidate.Candidate, outcomes []oracle.Outcome) (int, error) {
	outcomes = NormalizeTable(outcomes)
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0, err
	}
	if err := validateRankingPermutationForMetrics(ranking, outcomeMap); err != nil {
		return 0, err
	}
	return CalculateBFull(ranking, outcomes), nil
}

// CalculateK1 returns the 1-based index of the first failure or violation.
// If onlyViolations is true, only ClassViolation counts; otherwise any non-valid failure state counts.
// Returns 0 if outcomes contain oracle_error/duplicates or no matching failure occurs.
func CalculateK1(ranking []candidate.Candidate, outcomes []oracle.Outcome, onlyViolations bool) int {
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0
	}

	seenCand := make(map[candidate.Candidate]bool, len(ranking))
	for i, c := range ranking {
		if seenCand[c] {
			continue
		}
		seenCand[c] = true
		o, ok := outcomeMap[c]
		if !ok {
			continue
		}
		if onlyViolations {
			if o.Class == oracle.ClassViolation {
				return i + 1
			}
		} else if isFailureClass(o.Class) {
			return i + 1
		}
	}
	return 0
}

// CalculateK1Checked strictly validates ranking and outcomes before computing k1.
func CalculateK1Checked(ranking []candidate.Candidate, outcomes []oracle.Outcome, onlyViolations bool) (int, error) {
	outcomeMap, err := buildValidatedOutcomeIndex(outcomes)
	if err != nil {
		return 0, err
	}
	if err := validateRankingPermutationForMetrics(ranking, outcomeMap); err != nil {
		return 0, err
	}
	return CalculateK1(ranking, outcomes, onlyViolations), nil
}

// NormalizeDBDir validates and normalizes a Pebble DB directory path used inside crashable VFS.
func NormalizeDBDir(dir string) (string, error) {
	d := strings.TrimSpace(dir)
	if d == "" {
		return "db", nil
	}
	if strings.ContainsRune(d, 0) || strings.Contains(d, ":") {
		return "", fmt.Errorf("invalid db dir %q", dir)
	}
	d = strings.ReplaceAll(d, "\\", "/")
	d = path.Clean(d)
	if d == "" || d == "." || d == "/" || d == ".." || strings.HasPrefix(d, "/") || strings.HasPrefix(d, "../") {
		return "", fmt.Errorf("invalid db dir %q", dir)
	}
	return d, nil
}

func canonicalFaultKey(fid string) (string, error) {
	norm, err := faults.NormalizeID(fid)
	if err != nil {
		return "", err
	}
	if strings.ContainsRune(fid, '_') {
		return fid, nil
	}
	return norm, nil
}

func normalizeBudgetsForRun(raw []int, universeLen int) ([]int, error) {
	seen := make(map[int]bool, len(raw)+1)
	budgets := make([]int, 0, len(raw)+1)
	for _, b := range raw {
		if b < 0 {
			return nil, fmt.Errorf("negative budget %d", b)
		}
		if universeLen >= 0 && b > universeLen {
			continue
		}
		if !seen[b] {
			seen[b] = true
			budgets = append(budgets, b)
		}
	}
	if universeLen >= 0 && !seen[universeLen] {
		seen[universeLen] = true
		budgets = append(budgets, universeLen)
	}
	sort.Ints(budgets)
	return budgets, nil
}

type indexedUniverse struct {
	n         int
	candToIdx map[candidate.Candidate]int
	isFail    []bool
	isViol    []bool
	sigID     []int
	stateID   []int
	numSigs   int
	candSeen  []int
	sigSeen   []int
	stateSeen []int
	epoch     int
}

func newIndexedUniverse(universe []candidate.Candidate, normOutcomes []oracle.Outcome, view *graph.View) (*indexedUniverse, error) {
	if err := ValidateTable(universe, normOutcomes); err != nil {
		return nil, err
	}
	n := len(universe)
	u := &indexedUniverse{
		n:         n,
		candToIdx: make(map[candidate.Candidate]int, n),
		isFail:    make([]bool, n),
		isViol:    make([]bool, n),
		sigID:     make([]int, n),
		stateID:   make([]int, n),
		candSeen:  make([]int, n),
	}
	sigMap := make(map[string]int)
	stateMap := make(map[string]int)
	for i, o := range normOutcomes {
		u.candToIdx[universe[i]] = i
		u.sigID[i] = -1
		u.stateID[i] = -1
		if !isFailureClass(o.Class) {
			continue
		}
		u.isFail[i] = true
		if o.Class == oracle.ClassViolation {
			u.isViol[i] = true
		}
		if o.Signature != "" {
			sid, ok := sigMap[o.Signature]
			if !ok {
				sid = len(sigMap)
				sigMap[o.Signature] = sid
			}
			u.sigID[i] = sid
		}
		stateKey := ""
		if view != nil && view.Cuts != nil {
			if feat, ok := view.Cuts[o.Candidate.Event]; ok {
				stateKey = feat.StateKey
			}
		}
		if stateKey == "" {
			stateKey = fmt.Sprintf("event_%d", o.Candidate.Event)
		}
		sKey := fmt.Sprintf("%s|p%d", stateKey, o.Candidate.Percent)
		stid, ok := stateMap[sKey]
		if !ok {
			stid = len(stateMap)
			stateMap[sKey] = stid
		}
		u.stateID[i] = stid
	}
	u.numSigs = len(sigMap)
	u.sigSeen = make([]int, u.numSigs)
	u.stateSeen = make([]int, len(stateMap))
	return u, nil
}

type evaluatedRanking struct {
	k1Failure       int
	k1Violation     int
	bFull           int
	s25             int
	s100            int
	bugs100         int
	aucNormFailures float64
	aucNormBugs     float64
	points          []Point
}

func (u *indexedUniverse) eval(ranking []candidate.Candidate, budgets []int, totalFailures int) (evaluatedRanking, error) {
	if len(ranking) != u.n {
		return evaluatedRanking{}, fmt.Errorf("evaluate: ranking length %d != universe length %d", len(ranking), u.n)
	}
	u.epoch++
	ep := u.epoch

	pts := make([]Point, 0, len(budgets))
	bIdx := 0
	for bIdx < len(budgets) && budgets[bIdx] == 0 {
		pts = append(pts, Point{Budget: 0, Tested: 0, Failures: 0, DistinctBugs: 0})
		bIdx++
	}

	var res evaluatedRanking
	failures := 0
	distinctBugs := 0
	distinctStates := 0
	aucSumF := 0.0
	aucSumB := 0.0

	for i, c := range ranking {
		idx, ok := u.candToIdx[c]
		if !ok {
			return evaluatedRanking{}, fmt.Errorf("evaluate: ranking contains foreign candidate %+v", c)
		}
		if u.candSeen[idx] == ep {
			return evaluatedRanking{}, fmt.Errorf("evaluate: ranking contains duplicate candidate %+v", c)
		}
		u.candSeen[idx] = ep

		prevF := failures
		prevB := distinctBugs
		pos := i + 1

		if u.isFail[idx] {
			failures++
			if res.k1Failure == 0 {
				res.k1Failure = pos
			}
			if u.isViol[idx] && res.k1Violation == 0 {
				res.k1Violation = pos
			}
			if sid := u.sigID[idx]; sid >= 0 && u.sigSeen[sid] != ep {
				u.sigSeen[sid] = ep
				distinctBugs++
				if distinctBugs == u.numSigs && res.bFull == 0 {
					res.bFull = pos
				}
			}
			if stid := u.stateID[idx]; stid >= 0 && u.stateSeen[stid] != ep {
				u.stateSeen[stid] = ep
				distinctStates++
			}
		}

		aucSumF += 0.5 * float64(prevF+failures)
		aucSumB += 0.5 * float64(prevB+distinctBugs)

		if pos == 25 {
			res.s25 = distinctStates
		}
		if pos == 100 {
			res.s100 = distinctStates
			res.bugs100 = distinctBugs
		}
		for bIdx < len(budgets) && budgets[bIdx] == pos {
			pts = append(pts, Point{
				Budget:       pos,
				Tested:       pos,
				Failures:     failures,
				DistinctBugs: distinctBugs,
			})
			bIdx++
		}
	}

	if u.n < 25 {
		res.s25 = distinctStates
	}
	if u.n < 100 {
		res.s100 = distinctStates
		res.bugs100 = distinctBugs
	}
	if u.n > 0 && totalFailures > 0 {
		res.aucNormFailures = clampUnit(aucSumF / (float64(u.n) * float64(totalFailures)))
	}
	if u.n > 0 && u.numSigs > 0 {
		res.aucNormBugs = clampUnit(aucSumB / (float64(u.n) * float64(u.numSigs)))
	}
	res.points = pts
	return res, nil
}

func clampUnit(v float64) float64 {
	if v < 0.0 {
		return 0.0
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}

// percentile calculates the specified percentile (0..100) from a sorted slice of float64.
func percentile(sorted []float64, pct float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 || pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[n-1]
	}

	idx := (pct / 100.0) * float64(n-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	weight := idx - float64(lower)
	return sorted[lower]*(1.0-weight) + sorted[upper]*weight
}

// RunSingleSeedFault executes a single deterministic run of workload recording, graph building,
// crash exploration, and strategy evaluation for a given (seed, fault) pair.
func RunSingleSeedFault(seed uint64, faultID string, cfg BenchmarkConfig) (*SingleRunResult, error) {
	start := time.Now()

	normFault, err := faults.NormalizeID(faultID)
	if err != nil {
		return nil, fmt.Errorf("normalize fault %q: %w", faultID, err)
	}
	if cfg.WALDropK < 0 {
		return nil, fmt.Errorf("wal_drop_k must be >= 0, got %d", cfg.WALDropK)
	}
	if cfg.DirDropK < 0 {
		return nil, fmt.Errorf("dir_drop_k must be >= 0, got %d", cfg.DirDropK)
	}

	wCfg := cfg.Workload
	wCfg.Seed = seed
	if normFault == faults.MUT8 && wCfg.CompactAt == 0 {
		wCfg.CompactAt = 80
	}
	if normFault == faults.MUT8 && wCfg.CompactAt > wCfg.Operations {
		return nil, fmt.Errorf("MUT-8 compact_at (%d) exceeds operations (%d); fault would not execute", wCfg.CompactAt, wCfg.Operations)
	}
	dbDir, err := NormalizeDBDir(wCfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("workload dir: %w", err)
	}
	wCfg.Dir = dbDir
	if err := workload.Validate(wCfg); err != nil {
		return nil, fmt.Errorf("validate workload: %w", err)
	}

	policies, err := normalizePolicies(cfg.Policies)
	if err != nil {
		return nil, err
	}
	if _, err := normalizeBudgetsForRun(cfg.Budgets, -1); err != nil {
		return nil, fmt.Errorf("validate budgets: %w", err)
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	fCfg := faults.DefaultConfig(faultID)
	fCfg.Seed = seed
	if cfg.WALDropK > 0 {
		fCfg.WALDropK = cfg.WALDropK
	}
	if cfg.DirDropK > 0 {
		fCfg.DirDropK = cfg.DirDropK
	}
	faultFS, err := faults.NewWithConfig(fCfg, rec)
	if err != nil {
		return nil, fmt.Errorf("init fault %s: %w", faultID, err)
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
	if wCfg.MaxManifestFileSize > 0 {
		opts.MaxManifestFileSize = wCfg.MaxManifestFileSize
	}
	if wCfg.RatchetAt > 0 {
		opts.FormatMajorVersion = pebble.FormatMinSupported
	}
	wCfg.FS = faultFS

	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	rawHist, err := workload.Run(db, rec, wCfg)
	if err != nil {
		_, _ = safeCloseDB(db)
		return nil, fmt.Errorf("run workload: %w", err)
	}
	db.TestOnlyWaitForCleaning()
	if panicked, closeErr := safeCloseDB(db); panicked || closeErr != nil {
		if panicked {
			return nil, fmt.Errorf("close pebble panic: %w", closeErr)
		}
		return nil, fmt.Errorf("close pebble: %w", closeErr)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		return nil, fmt.Errorf("validate trace: %w", err)
	}

	correlated, err := workload.Correlate(rawHist, traceEvents)
	if err != nil {
		return nil, fmt.Errorf("correlate history: %w", err)
	}

	startPlan := time.Now()
	universe, err := candidate.Build(traceEvents, seed, policies)
	if err != nil {
		return nil, fmt.Errorf("build universe: %w", err)
	}

	view, err := graph.Build(traceEvents)
	if err != nil {
		return nil, fmt.Errorf("build graph: %w", err)
	}
	planGraphDuration := time.Since(startPlan)

	timeout := cfg.SafeOpenTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	categories := make(map[uint64][]string, len(view.Cuts))
	for cut, feat := range view.Cuts {
		categories[cut] = feat.Categories
	}

	expRes, err := RunExplore(RunnerConfig{
		Universe:   universe,
		Events:     traceEvents,
		History:    correlated,
		Categories: categories,
		Dir:        dbDir,
		Timeout:    timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("run explore: %w", err)
	}

	normOutcomes := NormalizeTable(expRes.Outcomes)
	evalIdx, err := newIndexedUniverse(universe, normOutcomes, view)
	if err != nil {
		return nil, fmt.Errorf("validate explored outcomes: %w", err)
	}
	totalFailures := expRes.Summary.Violations + expRes.Summary.OpenErrors + expRes.Summary.Panics + expRes.Summary.Hangs

	budgets, err := normalizeBudgetsForRun(cfg.Budgets, len(universe))
	if err != nil {
		return nil, fmt.Errorf("normalize budgets: %w", err)
	}

	numShuffle := cfg.B0PercentileCnt
	if numShuffle <= 0 {
		numShuffle = DefaultB0PercentileCnt
	}

	type shuffleEnsemble struct {
		name             string
		k1FailsSorted    []float64
		k1ViolsSorted    []float64
		bFullsSorted     []float64
		s25Sorted        []float64
		s100Sorted       []float64
		b100Sorted       []float64
		k1FailSamples    []float64
		k1ViolSamples    []float64
		bFullSamples     []float64
		s25Samples       []float64
		s100Samples      []float64
		meanK1F          float64
		meanK1V          float64
		meanBF           float64
		meanS25          float64
		meanS100         float64
		meanB100         float64
		meanAUCF         float64
		meanAUCB         float64
		k1Single         int
		s25Single        int
		summaryPoints    []B0SummaryPoint
		averagedCurvePts []Point
	}

	evaluateShuffleEnsemble := func(code string, domain uint64, makeStrat func(uint64) strategy.Strategy) (*shuffleEnsemble, error) {
		k1Fails := make([]float64, numShuffle)
		k1Viols := make([]float64, numShuffle)
		bFulls := make([]float64, numShuffle)
		s25s := make([]float64, numShuffle)
		s100s := make([]float64, numShuffle)
		b100s := make([]float64, numShuffle)

		minF := make([]int, len(budgets))
		maxF := make([]int, len(budgets))
		sumF := make([]float64, len(budgets))
		minB := make([]int, len(budgets))
		maxB := make([]int, len(budgets))
		sumB := make([]float64, len(budgets))
		for bIdx := range budgets {
			minF[bIdx] = math.MaxInt32
			maxF[bIdx] = -1
			minB[bIdx] = math.MaxInt32
			maxB[bIdx] = -1
		}

		var sumK1F, sumK1V, sumBF, sumS25, sumS100, sumB100, sumAUCF, sumAUCB float64
		var stratName string
		for i := 0; i < numShuffle; i++ {
			shuffleSeed := MixShuffleSeed(seed, domain, i)
			strat := makeStrat(shuffleSeed)
			if i == 0 {
				stratName = strat.Name()
			}
			ranked := strat.Rank(view, universe)
			evalRes, err := evalIdx.eval(ranked, budgets, totalFailures)
			if err != nil {
				return nil, fmt.Errorf("eval %s shuffle %d: %w", code, i, err)
			}

			for bIdx, pt := range evalRes.points {
				if pt.Failures < minF[bIdx] {
					minF[bIdx] = pt.Failures
				}
				if pt.Failures > maxF[bIdx] {
					maxF[bIdx] = pt.Failures
				}
				sumF[bIdx] += float64(pt.Failures)
				if pt.DistinctBugs < minB[bIdx] {
					minB[bIdx] = pt.DistinctBugs
				}
				if pt.DistinctBugs > maxB[bIdx] {
					maxB[bIdx] = pt.DistinctBugs
				}
				sumB[bIdx] += float64(pt.DistinctBugs)
			}

			k1F := float64(evalRes.k1Failure)
			k1V := float64(evalRes.k1Violation)
			bf := float64(evalRes.bFull)
			s25 := float64(evalRes.s25)
			s100 := float64(evalRes.s100)
			b100 := float64(evalRes.bugs100)

			k1Fails[i] = k1F
			k1Viols[i] = k1V
			bFulls[i] = bf
			s25s[i] = s25
			s100s[i] = s100
			b100s[i] = b100

			sumK1F += k1F
			sumK1V += k1V
			sumBF += bf
			sumS25 += s25
			sumS100 += s100
			sumB100 += b100
			sumAUCF += evalRes.aucNormFailures
			sumAUCB += evalRes.aucNormBugs
		}

		nF := float64(numShuffle)
		summaryPoints := make([]B0SummaryPoint, len(budgets))
		avgCurvePts := make([]Point, len(budgets))
		for bIdx, b := range budgets {
			fAvg := sumF[bIdx] / nF
			bAvg := sumB[bIdx] / nF
			summaryPoints[bIdx] = B0SummaryPoint{
				Budget:      b,
				FailuresMin: minF[bIdx],
				FailuresMax: maxF[bIdx],
				FailuresAvg: fAvg,
				BugsMin:     minB[bIdx],
				BugsMax:     maxB[bIdx],
				BugsAvg:     bAvg,
			}
			avgCurvePts[bIdx] = Point{
				Budget:       b,
				Tested:       b,
				Failures:     int(math.Round(fAvg)),
				DistinctBugs: int(math.Round(bAvg)),
			}
		}

		k1FailsSorted := append([]float64(nil), k1Fails...)
		k1ViolsSorted := append([]float64(nil), k1Viols...)
		bFullsSorted := append([]float64(nil), bFulls...)
		s25Sorted := append([]float64(nil), s25s...)
		s100Sorted := append([]float64(nil), s100s...)
		b100Sorted := append([]float64(nil), b100s...)
		sort.Float64s(k1FailsSorted)
		sort.Float64s(k1ViolsSorted)
		sort.Float64s(bFullsSorted)
		sort.Float64s(s25Sorted)
		sort.Float64s(s100Sorted)
		sort.Float64s(b100Sorted)

		rankedSingle := makeStrat(seed).Rank(view, universe)
		singleRes, err := evalIdx.eval(rankedSingle, budgets, totalFailures)
		if err != nil {
			return nil, fmt.Errorf("eval %s single seed: %w", code, err)
		}

		return &shuffleEnsemble{
			name:             stratName,
			k1FailsSorted:    k1FailsSorted,
			k1ViolsSorted:    k1ViolsSorted,
			bFullsSorted:     bFullsSorted,
			s25Sorted:        s25Sorted,
			s100Sorted:       s100Sorted,
			b100Sorted:       b100Sorted,
			k1FailSamples:    k1Fails,
			k1ViolSamples:    k1Viols,
			bFullSamples:     bFulls,
			s25Samples:       s25s,
			s100Samples:      s100s,
			meanK1F:          sumK1F / nF,
			meanK1V:          sumK1V / nF,
			meanBF:           sumBF / nF,
			meanS25:          sumS25 / nF,
			meanS100:         sumS100 / nF,
			meanB100:         sumB100 / nF,
			meanAUCF:         sumAUCF / nF,
			meanAUCB:         sumAUCB / nF,
			k1Single:         singleRes.k1Failure,
			s25Single:        singleRes.s25,
			summaryPoints:    summaryPoints,
			averagedCurvePts: avgCurvePts,
		}, nil
	}

	// 1. Evaluate B0 across shuffle seeds
	b0Ens, err := evaluateShuffleEnsemble("b0", 0xb0, strategy.NewRandom)
	if err != nil {
		return nil, err
	}
	b0Metrics := B0Metrics{
		K1FailureMean:      b0Ens.meanK1F,
		K1FailureMedian:    percentile(b0Ens.k1FailsSorted, 50),
		K1FailureP10:       percentile(b0Ens.k1FailsSorted, 10),
		K1FailureP90:       percentile(b0Ens.k1FailsSorted, 90),
		K1ViolationMean:    b0Ens.meanK1V,
		K1ViolationMedian:  percentile(b0Ens.k1ViolsSorted, 50),
		K1ViolationP10:     percentile(b0Ens.k1ViolsSorted, 10),
		K1ViolationP90:     percentile(b0Ens.k1ViolsSorted, 90),
		K1SingleSeed:       b0Ens.k1Single,
		BFullMean:          b0Ens.meanBF,
		BFullMedian:        percentile(b0Ens.bFullsSorted, 50),
		BFullP10:           percentile(b0Ens.bFullsSorted, 10),
		BFullP90:           percentile(b0Ens.bFullsSorted, 90),
		S25Mean:            b0Ens.meanS25,
		S25Median:          percentile(b0Ens.s25Sorted, 50),
		S25P10:             percentile(b0Ens.s25Sorted, 10),
		S25P90:             percentile(b0Ens.s25Sorted, 90),
		S25SingleSeed:      b0Ens.s25Single,
		S100Mean:           b0Ens.meanS100,
		S100Median:         percentile(b0Ens.s100Sorted, 50),
		S100P10:            percentile(b0Ens.s100Sorted, 10),
		S100P90:            percentile(b0Ens.s100Sorted, 90),
		AUCNormFailures:    b0Ens.meanAUCF,
		AUCNormBugs:        b0Ens.meanAUCB,
		SummaryPoints:      b0Ens.summaryPoints,
		K1FailureSamples:   b0Ens.k1FailSamples,
		K1ViolationSamples: b0Ens.k1ViolSamples,
		BFullSamples:       b0Ens.bFullSamples,
		S25Samples:         b0Ens.s25Samples,
		S100Samples:        b0Ens.s100Samples,
	}

	// 2. Evaluate deterministic strategies
	combStrat, err := strategy.NewCombined(strategy.DefaultWeights())
	if err != nil {
		return nil, fmt.Errorf("init combined: %w", err)
	}

	deterministicStrats := []struct {
		code  string
		strat strategy.Strategy
	}{
		{"b1", strategy.NewFIFO()},
		{"b2", strategy.NewLIFO()},
		{"b3", strategy.NewStride()},
		{"b4", combStrat},
		{"b5", strategy.NewRepresentative()},
		{"b6", strategy.NewSyncBoundary()},
		{"b7", strategy.NewRiskHorizon(strategy.DefaultRHGWeights())},
		{"dcg", NewDCGStrategy()},
		{"dcg-pr", NewDCGPartialStrategy()},
	}

	stratMap := make(map[string]StrategyMetrics, len(deterministicStrats)+3)
	var dcgRankDuration time.Duration

	for _, s := range deterministicStrats {
		tRankStart := time.Now()
		ranked := s.strat.Rank(view, universe)
		if s.code == "dcg" {
			dcgRankDuration = time.Since(tRankStart)
		}
		evalRes, err := evalIdx.eval(ranked, budgets, totalFailures)
		if err != nil {
			return nil, fmt.Errorf("eval %s: %w", s.code, err)
		}

		stratMap[s.code] = StrategyMetrics{
			StrategyName:     s.strat.Name(),
			K1Failure:        evalRes.k1Failure,
			K1FailureFloat:   float64(evalRes.k1Failure),
			K1Violation:      evalRes.k1Violation,
			K1ViolationFloat: float64(evalRes.k1Violation),
			K1SingleSeed:     evalRes.k1Failure,
			BFull:            evalRes.bFull,
			BFullFloat:       float64(evalRes.bFull),
			S25:              evalRes.s25,
			S25Float:         float64(evalRes.s25),
			S25SingleSeed:    evalRes.s25,
			S100:             evalRes.s100,
			S100Float:        float64(evalRes.s100),
			Bugs100:          evalRes.bugs100,
			AUCNormFailures:  evalRes.aucNormFailures,
			AUCNormBugs:      evalRes.aucNormBugs,
			Points:           evalRes.points,
		}
	}

	// 3. Multi-shuffle evaluation for random strategies R1, R2, and DCG-FlagOnly
	evalRandomStrat := func(code string, makeStrat func(s uint64) strategy.Strategy) error {
		var domain uint64
		for _, ch := range []byte(code) {
			domain = domain*31 + uint64(ch)
		}
		ens, err := evaluateShuffleEnsemble(code, domain, makeStrat)
		if err != nil {
			return err
		}

		stratMap[code] = StrategyMetrics{
			StrategyName:       ens.name,
			K1Failure:          int(math.Round(percentile(ens.k1FailsSorted, 50))),
			K1FailureFloat:     ens.meanK1F,
			K1Violation:        int(math.Round(percentile(ens.k1ViolsSorted, 50))),
			K1ViolationFloat:   ens.meanK1V,
			K1SingleSeed:       ens.k1Single,
			BFull:              int(math.Round(percentile(ens.bFullsSorted, 50))),
			BFullFloat:         ens.meanBF,
			S25:                int(math.Round(percentile(ens.s25Sorted, 50))),
			S25Float:           ens.meanS25,
			S25SingleSeed:      ens.s25Single,
			S100:               int(math.Round(percentile(ens.s100Sorted, 50))),
			S100Float:          ens.meanS100,
			Bugs100:            int(math.Round(percentile(ens.b100Sorted, 50))),
			AUCNormFailures:    ens.meanAUCF,
			AUCNormBugs:        ens.meanAUCB,
			Points:             ens.averagedCurvePts,
			SummaryPoints:      ens.summaryPoints,
			K1FailureSamples:   ens.k1FailSamples,
			K1ViolationSamples: ens.k1ViolSamples,
			BFullSamples:       ens.bFullSamples,
			S25Samples:         ens.s25Samples,
			S100Samples:        ens.s100Samples,
		}
		return nil
	}

	if err := evalRandomStrat("r1", strategy.NewR1); err != nil {
		return nil, err
	}
	if err := evalRandomStrat("r2", strategy.NewR2); err != nil {
		return nil, err
	}
	if err := evalRandomStrat("dcg-flag-only", strategy.NewDCGFlagOnly); err != nil {
		return nil, err
	}

	return &SingleRunResult{
		Seed:                   seed,
		FaultID:                faultID,
		UniverseSize:           len(universe),
		TotalFailures:          totalFailures,
		TotalViolations:        expRes.Summary.Violations,
		TotalOpenErrors:        expRes.Summary.OpenErrors,
		TotalPanics:            expRes.Summary.Panics,
		TotalHangs:             expRes.Summary.Hangs,
		DistinctBugs:           evalIdx.numSigs,
		TheoreticalExpectation: CalculateTheoreticalExpectation(len(universe), totalFailures),
		B0:                     b0Metrics,
		Strategies:             stratMap,
		ElapsedTime:            time.Since(start),
		PlanRankMs:             (planGraphDuration + dcgRankDuration).Milliseconds(),
		ExploreTotalMs:         expRes.Summary.TotalTime.Milliseconds(),
	}, nil
}

func normalizePolicies(raw []int) ([]int, error) {
	if len(raw) == 0 {
		return []int{0, 100}, nil
	}
	seen := make(map[int]bool, len(raw))
	out := make([]int, 0, len(raw))
	for _, p := range raw {
		if p < 0 || p > 100 {
			return nil, fmt.Errorf("invalid policy %d (must be 0..100)", p)
		}
		if seen[p] {
			return nil, fmt.Errorf("duplicate policy %d", p)
		}
		seen[p] = true
		out = append(out, p)
	}
	return out, nil
}

// CurrentGitCommit returns the 12-character short git commit hash of HEAD
// (with "-dirty" suffix when the working tree has uncommitted changes), or "unknown".
func CurrentGitCommit() string {
	hash, dir := tryGitCommit("", "")
	if hash == "" {
		hash, dir = tryGitCommit("-C", "CraConTest")
	}
	if hash == "" {
		return "unknown"
	}
	if isWorkingTreeDirty(dir) {
		return hash + "-dirty"
	}
	return hash
}

// tryGitCommit runs git rev-parse in the given directory (empty = cwd).
// Returns the 12-char hash and the -C arg used (empty string = cwd).
func tryGitCommit(flag, dir string) (string, string) {
	var args []string
	if flag != "" && dir != "" {
		args = append(args, flag, dir)
	}
	args = append(args, "rev-parse", "--short=12", "HEAD")
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return "", ""
	}
	return strings.TrimSpace(string(out)), dir
}

// isWorkingTreeDirty returns true when git status --porcelain reports any change.
func isWorkingTreeDirty(dir string) bool {
	var args []string
	if dir != "" {
		args = append(args, "-C", dir)
	}
	args = append(args, "status", "--porcelain")
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(out)) != ""
}

// RunBenchmark executes empirical benchmarking across all configured workload seeds and fault models.
func RunBenchmark(cfg BenchmarkConfig, progress func(string)) (*BenchmarkResult, error) {
	benchStart := time.Now()

	if len(cfg.WorkloadSeeds) == 0 {
		cfg.WorkloadSeeds = append([]uint64{}, DefaultWorkloadSeeds...)
	} else {
		cfg.WorkloadSeeds = append([]uint64(nil), cfg.WorkloadSeeds...)
	}
	seenSeeds := make(map[uint64]bool, len(cfg.WorkloadSeeds))
	for _, s := range cfg.WorkloadSeeds {
		if seenSeeds[s] {
			return nil, fmt.Errorf("duplicate workload seed %d", s)
		}
		seenSeeds[s] = true
	}

	if len(cfg.FaultIDs) == 0 {
		cfg.FaultIDs = append([]string{}, DefaultFaultModels...)
	} else {
		cfg.FaultIDs = append([]string(nil), cfg.FaultIDs...)
	}
	seenFaults := make(map[string]string, len(cfg.FaultIDs))
	hasMUT8 := false
	for _, fid := range cfg.FaultIDs {
		canonKey, err := canonicalFaultKey(fid)
		if err != nil {
			return nil, fmt.Errorf("invalid fault id %q: %w", fid, err)
		}
		if prev, dup := seenFaults[canonKey]; dup {
			return nil, fmt.Errorf("duplicate fault id %q (conflicts with %q under canonical key %q)", fid, prev, canonKey)
		}
		seenFaults[canonKey] = fid
		if canonKey == faults.MUT8 {
			hasMUT8 = true
		}
	}

	policies, err := normalizePolicies(cfg.Policies)
	if err != nil {
		return nil, fmt.Errorf("invalid policies: %w", err)
	}
	cfg.Policies = policies

	cleanedBudgets, err := normalizeBudgetsForRun(cfg.Budgets, -1)
	if err != nil {
		return nil, fmt.Errorf("invalid budgets: %w", err)
	}
	cfg.Budgets = cleanedBudgets

	if cfg.WALDropK < 0 {
		return nil, fmt.Errorf("wal_drop_k must be >= 0, got %d", cfg.WALDropK)
	}
	if cfg.DirDropK < 0 {
		return nil, fmt.Errorf("dir_drop_k must be >= 0, got %d", cfg.DirDropK)
	}

	dbDir, err := NormalizeDBDir(cfg.Workload.Dir)
	if err != nil {
		return nil, fmt.Errorf("invalid workload dir: %w", err)
	}
	wCfgCheck := cfg.Workload
	wCfgCheck.Dir = dbDir
	if hasMUT8 && wCfgCheck.CompactAt == 0 {
		wCfgCheck.CompactAt = 80
	}
	if hasMUT8 && wCfgCheck.CompactAt > wCfgCheck.Operations {
		return nil, fmt.Errorf("MUT-8 compact_at (%d) exceeds operations (%d); fault would not execute", wCfgCheck.CompactAt, wCfgCheck.Operations)
	}
	if err := workload.Validate(wCfgCheck); err != nil {
		return nil, fmt.Errorf("invalid workload config: %w", err)
	}

	totalRuns := len(cfg.WorkloadSeeds) * len(cfg.FaultIDs)
	currentRun := 0

	faultSummaries := make(map[string]FaultModelSummary, len(cfg.FaultIDs))
	for _, fid := range cfg.FaultIDs {
		faultSummaries[fid] = FaultModelSummary{
			FaultID:           fid,
			StrategySummaries: make(map[string]AggregatedStrategyStats),
			Budgets:           append([]int(nil), cfg.Budgets...),
		}
	}

	for _, fid := range cfg.FaultIDs {
		fSummary := faultSummaries[fid]

		for _, seed := range cfg.WorkloadSeeds {
			currentRun++
			if progress != nil {
				progress(fmt.Sprintf("[%d/%d] Running Seed %d under %s...", currentRun, totalRuns, seed, fid))
			}

			res, err := RunSingleSeedFault(seed, fid, cfg)
			if err != nil {
				return nil, fmt.Errorf("benchmark seed=%d fault=%s: %w", seed, fid, err)
			}
			fSummary.Runs = append(fSummary.Runs, *res)
		}

		// Aggregate results across runs for this fault model
		aggregateFaultSummary(&fSummary)
		faultSummaries[fid] = fSummary
	}

	// MUT-0 null control check if enabled
	mut0Violations := 0
	mut0Runs := 0
	mut0AlreadyRun := false
	for _, fid := range cfg.FaultIDs {
		norm, _ := faults.NormalizeID(fid)
		if norm == faults.MUT0 {
			mut0AlreadyRun = true
			for _, run := range faultSummaries[fid].Runs {
				mut0Violations += run.TotalFailures
				mut0Runs++
			}
		}
	}
	if cfg.TrackMUT0 && !mut0AlreadyRun {
		if progress != nil {
			progress("Verifying null control MUT-0 across workload seeds (0 false alarms check)...")
		}
		for _, seed := range cfg.WorkloadSeeds {
			res, err := RunSingleSeedFault(seed, faults.MUT0, cfg)
			if err != nil {
				return nil, fmt.Errorf("control run seed %d: %w", seed, err)
			}
			mut0Violations += res.TotalFailures
			mut0Runs++
		}
	}

	return &BenchmarkResult{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		GitCommit:      CurrentGitCommit(),
		GoVersion:      runtime.Version(),
		PebbleVersion:  DetectPebbleVersion(),
		Config:         cfg,
		FaultSummaries: faultSummaries,
		MUT0Violations: mut0Violations,
		MUT0Runs:       mut0Runs,
		TotalDuration:  time.Since(benchStart),
	}, nil
}

func collectUnionBudgets(base []int, runs []SingleRunResult) []int {
	seen := make(map[int]bool, len(base)+16)
	var out []int
	add := func(b int) {
		if b < 0 || seen[b] {
			return
		}
		seen[b] = true
		out = append(out, b)
	}
	for _, b := range base {
		add(b)
	}
	for _, r := range runs {
		for _, p := range r.B0.SummaryPoints {
			add(p.Budget)
		}
		for _, m := range r.Strategies {
			for _, p := range m.SummaryPoints {
				add(p.Budget)
			}
			for _, p := range m.Points {
				add(p.Budget)
			}
		}
	}
	sort.Ints(out)
	return out
}

func accumulateCurveAtBudgets(budgets []int, curveFailSums, curveBugSums []float64, pts []B0SummaryPoint) {
	if len(budgets) == 0 || len(pts) == 0 {
		return
	}
	sorted := append([]B0SummaryPoint(nil), pts...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Budget < sorted[j].Budget })

	pIdx := -1
	for idx, b := range budgets {
		for pIdx+1 < len(sorted) && sorted[pIdx+1].Budget <= b {
			pIdx++
		}
		if pIdx >= 0 {
			curveFailSums[idx] += sorted[pIdx].FailuresAvg
			curveBugSums[idx] += sorted[pIdx].BugsAvg
		}
	}
}

func pointsToSummaryPoints(pts []Point) []B0SummaryPoint {
	if len(pts) == 0 {
		return nil
	}
	out := make([]B0SummaryPoint, len(pts))
	for i, p := range pts {
		out[i] = B0SummaryPoint{
			Budget:      p.Budget,
			FailuresMin: p.Failures,
			FailuresMax: p.Failures,
			FailuresAvg: float64(p.Failures),
			BugsMin:     p.DistinctBugs,
			BugsMax:     p.DistinctBugs,
			BugsAvg:     float64(p.DistinctBugs),
		}
	}
	return out
}

func aggregateFaultSummary(fs *FaultModelSummary) {
	n := len(fs.Runs)
	if n == 0 {
		return
	}
	fs.RunCount = n

	var sumUniv, sumFail, sumViol, sumOpen, sumPanic, sumHang, sumBugs float64
	activeSeeds := 0
	for _, r := range fs.Runs {
		sumUniv += float64(r.UniverseSize)
		sumFail += float64(r.TotalFailures)
		sumViol += float64(r.TotalViolations)
		sumOpen += float64(r.TotalOpenErrors)
		sumPanic += float64(r.TotalPanics)
		sumHang += float64(r.TotalHangs)
		sumBugs += float64(r.DistinctBugs)
		if r.TotalFailures > 0 {
			activeSeeds++
		}
	}

	fs.AvgUniverseSize = sumUniv / float64(n)
	fs.AvgFailures = sumFail / float64(n)
	fs.AvgViolations = sumViol / float64(n)
	fs.AvgOpenErrors = sumOpen / float64(n)
	fs.AvgPanics = sumPanic / float64(n)
	fs.AvgHangs = sumHang / float64(n)
	fs.AvgDistinctBugs = sumBugs / float64(n)
	if fs.AvgUniverseSize > 0 {
		fs.FaultDensityPct = (fs.AvgFailures / fs.AvgUniverseSize) * 100.0
	}

	fs.ActiveSeeds = activeSeeds
	fs.TotalSeeds = n
	budgets := collectUnionBudgets(fs.Budgets, fs.Runs)
	fs.Budgets = budgets

	if fs.StrategySummaries == nil {
		fs.StrategySummaries = make(map[string]AggregatedStrategyStats)
	}

	// Discover only strategy codes actually present across runs in canonical order.
	baseOrder := []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "r1", "r2", "dcg", "dcg-pr", "dcg-flag-only"}
	presentMap := make(map[string]bool)
	firstNames := make(map[string]string)
	for _, r := range fs.Runs {
		if hasB0Metrics(r) {
			presentMap["b0"] = true
		}
		for code, m := range r.Strategies {
			presentMap[code] = true
			if firstNames[code] == "" && m.StrategyName != "" {
				firstNames[code] = m.StrategyName
			}
		}
	}
	var stratCodes, extraCodes []string
	seenOrder := make(map[string]bool)
	for _, code := range baseOrder {
		if presentMap[code] {
			stratCodes = append(stratCodes, code)
			seenOrder[code] = true
		}
	}
	for code := range presentMap {
		if !seenOrder[code] {
			extraCodes = append(extraCodes, code)
		}
	}
	sort.Strings(extraCodes)
	stratCodes = append(stratCodes, extraCodes...)

	activeOnly := activeSeeds > 0

	for _, code := range stratCodes {
		var k1FailVals, k1ViolVals, bFullVals, s25Vals, s100Vals []float64
		var k1FailClusters, k1ViolClusters, bFullClusters, s25Clusters, s100Clusters [][]float64
		var sumAUCF, sumAUCB float64
		var evalDenom int

		curveFailSums := make([]float64, len(budgets))
		curveBugSums := make([]float64, len(budgets))

		for _, r := range fs.Runs {
			includeEval := !activeOnly || r.TotalFailures > 0
			if code == "b0" {
				if !hasB0Metrics(r) {
					continue
				}
				if r.TotalFailures > 0 {
					k1FailVals = append(k1FailVals, r.B0.K1FailureMean)
					bFullVals = append(bFullVals, r.B0.BFullMean)
					if len(r.B0.K1FailureSamples) > 0 {
						k1FailClusters = append(k1FailClusters, r.B0.K1FailureSamples)
					}
					if len(r.B0.BFullSamples) > 0 {
						bFullClusters = append(bFullClusters, r.B0.BFullSamples)
					}
				}
				if r.TotalViolations > 0 {
					k1ViolVals = append(k1ViolVals, r.B0.K1ViolationMean)
					if len(r.B0.K1ViolationSamples) > 0 {
						k1ViolClusters = append(k1ViolClusters, r.B0.K1ViolationSamples)
					}
				}
				if includeEval {
					evalDenom++
					s25Vals = append(s25Vals, r.B0.S25Mean)
					s100Vals = append(s100Vals, r.B0.S100Mean)
					if len(r.B0.S25Samples) > 0 {
						s25Clusters = append(s25Clusters, r.B0.S25Samples)
					}
					if len(r.B0.S100Samples) > 0 {
						s100Clusters = append(s100Clusters, r.B0.S100Samples)
					}
					sumAUCF += r.B0.AUCNormFailures
					sumAUCB += r.B0.AUCNormBugs
					accumulateCurveAtBudgets(budgets, curveFailSums, curveBugSums, r.B0.SummaryPoints)
				}
			} else {
				m, ok := r.Strategies[code]
				if !ok {
					continue
				}
				k1Fail, k1Viol, bfVal, s25Val, s100Val := resolveStrategyMetrics(m)
				if r.TotalFailures > 0 {
					k1FailVals = append(k1FailVals, k1Fail)
					bFullVals = append(bFullVals, bfVal)
					if len(m.K1FailureSamples) > 0 {
						k1FailClusters = append(k1FailClusters, m.K1FailureSamples)
					}
					if len(m.BFullSamples) > 0 {
						bFullClusters = append(bFullClusters, m.BFullSamples)
					}
				}
				if r.TotalViolations > 0 {
					k1ViolVals = append(k1ViolVals, k1Viol)
					if len(m.K1ViolationSamples) > 0 {
						k1ViolClusters = append(k1ViolClusters, m.K1ViolationSamples)
					}
				}
				if includeEval {
					evalDenom++
					s25Vals = append(s25Vals, s25Val)
					s100Vals = append(s100Vals, s100Val)
					if len(m.S25Samples) > 0 {
						s25Clusters = append(s25Clusters, m.S25Samples)
					}
					if len(m.S100Samples) > 0 {
						s100Clusters = append(s100Clusters, m.S100Samples)
					}
					sumAUCF += m.AUCNormFailures
					sumAUCB += m.AUCNormBugs
					if len(m.SummaryPoints) > 0 {
						accumulateCurveAtBudgets(budgets, curveFailSums, curveBugSums, m.SummaryPoints)
					} else {
						accumulateCurveAtBudgets(budgets, curveFailSums, curveBugSums, pointsToSummaryPoints(m.Points))
					}
				}
			}
		}

		var meanK1F, k1FMeanLow, k1FMeanHigh float64
		var medK1F, k1FMedLow, k1FMedHigh float64
		var p10K1F, p90K1F float64
		if len(k1FailClusters) > 0 && len(k1FailClusters) == len(k1FailVals) {
			meanK1F, k1FMeanLow, k1FMeanHigh, medK1F, k1FMedLow, k1FMedHigh, p10K1F, p90K1F =
				ClusterBootstrapCI95(k1FailClusters, 2000, 42, 1.0, math.Inf(1))
		} else if len(k1FailVals) > 0 {
			meanK1F, k1FMeanLow, k1FMeanHigh = StudentTCI95Range(k1FailVals, 1.0, math.Inf(1))
			medK1F, k1FMedLow, k1FMedHigh = BootstrapMedianCI95(k1FailVals, 2000, 42)
			sortedK1F := append([]float64(nil), k1FailVals...)
			sort.Float64s(sortedK1F)
			p10K1F = percentile(sortedK1F, 10)
			p90K1F = percentile(sortedK1F, 90)
		}

		var meanK1V, medK1V float64
		if len(k1ViolClusters) > 0 && len(k1ViolClusters) == len(k1ViolVals) {
			meanK1V, _, _, medK1V, _, _, _, _ = ClusterBootstrapCI95(k1ViolClusters, 0, 42, 1.0, math.Inf(1))
		} else if len(k1ViolVals) > 0 {
			meanK1V = average(k1ViolVals)
			medK1V = median(k1ViolVals)
		}

		var meanBF, medBF float64
		if len(bFullClusters) > 0 && len(bFullClusters) == len(bFullVals) {
			meanBF, _, _, medBF, _, _, _, _ = ClusterBootstrapCI95(bFullClusters, 0, 42, 0.0, math.Inf(1))
		} else if len(bFullVals) > 0 {
			meanBF = average(bFullVals)
			medBF = median(bFullVals)
		}

		var meanS25, s25MeanLow, s25MeanHigh, medS25, s25MedLow, s25MedHigh float64
		if len(s25Clusters) > 0 && len(s25Clusters) == len(s25Vals) {
			meanS25, s25MeanLow, s25MeanHigh, medS25, s25MedLow, s25MedHigh, _, _ =
				ClusterBootstrapCI95(s25Clusters, 2000, 42, 0.0, 25.0)
		} else if len(s25Vals) > 0 {
			meanS25, s25MeanLow, s25MeanHigh = StudentTCI95Range(s25Vals, 0.0, 25.0)
			medS25, s25MedLow, s25MedHigh = BootstrapMedianCI95(s25Vals, 2000, 42)
		}

		var meanS100, s100MeanLow, s100MeanHigh, medS100, s100MedLow, s100MedHigh float64
		if len(s100Clusters) > 0 && len(s100Clusters) == len(s100Vals) {
			meanS100, s100MeanLow, s100MeanHigh, medS100, s100MedLow, s100MedHigh, _, _ =
				ClusterBootstrapCI95(s100Clusters, 2000, 42, 0.0, 100.0)
		} else if len(s100Vals) > 0 {
			meanS100, s100MeanLow, s100MeanHigh = StudentTCI95Range(s100Vals, 0.0, 100.0)
			medS100, s100MedLow, s100MedHigh = BootstrapMedianCI95(s100Vals, 2000, 42)
		}

		meanCurveF := make([]float64, len(budgets))
		meanCurveB := make([]float64, len(budgets))
		var meanAUCF, meanAUCB float64
		if evalDenom > 0 {
			denomF := float64(evalDenom)
			for i := range budgets {
				meanCurveF[i] = curveFailSums[i] / denomF
				meanCurveB[i] = curveBugSums[i] / denomF
			}
			meanAUCF = sumAUCF / denomF
			meanAUCB = sumAUCB / denomF
		}

		sName := strategyDisplayName(code, firstNames[code])
		fs.StrategySummaries[code] = AggregatedStrategyStats{
			StrategyName:             sName,
			K1FailureMean:            meanK1F,
			K1FailureMeanCI95Lower:   k1FMeanLow,
			K1FailureMeanCI95Upper:   k1FMeanHigh,
			K1FailureMedian:          medK1F,
			K1FailureMedianCI95Lower: k1FMedLow,
			K1FailureMedianCI95Upper: k1FMedHigh,
			K1FailureP10:             p10K1F,
			K1FailureP90:             p90K1F,
			K1ViolationMean:          meanK1V,
			K1ViolationMedian:        medK1V,
			BFullMean:                meanBF,
			BFullMedian:              medBF,
			S25Mean:                  meanS25,
			S25MeanCI95Lower:         s25MeanLow,
			S25MeanCI95Upper:         s25MeanHigh,
			S25Median:                medS25,
			S25MedianCI95Lower:       s25MedLow,
			S25MedianCI95Upper:       s25MedHigh,
			S100Mean:                 meanS100,
			S100MeanCI95Lower:        s100MeanLow,
			S100MeanCI95Upper:        s100MeanHigh,
			S100Median:               medS100,
			S100MedianCI95Lower:      s100MedLow,
			S100MedianCI95Upper:      s100MedHigh,
			AUCNormFailuresMean:      meanAUCF,
			AUCNormBugsMean:          meanAUCB,
			CurveMeanFailures:        meanCurveF,
			CurveMeanBugs:            meanCurveB,
		}
	}
}

func average(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := 0.0
	for _, v := range vals {
		s += v
	}
	return s / float64(len(vals))
}

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64{}, vals...)
	sort.Float64s(sorted)
	return percentile(sorted, 50)
}

// GenerateEmpiricalReportMarkdown renders the comprehensive empirical comparison report as Markdown.
func GenerateEmpiricalReportMarkdown(res *BenchmarkResult) string {
	if res == nil {
		return "No benchmark results available.\n"
	}

	var sb strings.Builder
	goVersion := res.GoVersion
	if goVersion == "" {
		goVersion = "not recorded"
	}
	pebbleVersion := res.PebbleVersion
	if pebbleVersion == "" {
		pebbleVersion = DetectPebbleVersion()
	}
	dateStr := res.Timestamp
	if len(dateStr) >= 10 {
		dateStr = dateStr[:10]
	} else if dateStr == "" {
		dateStr = "not recorded"
	}
	seedCount := len(res.Config.WorkloadSeeds)
	policiesStr := formatPolicies(res.Config.Policies)
	walDropK := res.Config.WALDropK
	if walDropK <= 0 {
		walDropK = 1
	}
	dirDropK := res.Config.DirDropK
	if dirDropK <= 0 {
		dirDropK = 1
	}

	sb.WriteString("# CraConTest Empirical Report: Multi-Seed Benchmarking & Discovery Curves\n\n")
	sb.WriteString(fmt.Sprintf("**Date:** %s  \n", dateStr))
	sb.WriteString(fmt.Sprintf("**Evaluation Engine:** Pebble %s (Go runtime: %s)  \n", pebbleVersion, goVersion))
	sb.WriteString(fmt.Sprintf("**Workload Seeds (%d):** `%v`  \n", seedCount, res.Config.WorkloadSeeds))
	sb.WriteString(fmt.Sprintf("**Retention Policies:** `%s`  \n", policiesStr))
	sb.WriteString(fmt.Sprintf("**Execution Time:** %.2f seconds total  \n", res.TotalDuration.Seconds()))
	if res.MUT0Runs > 0 {
		sb.WriteString(fmt.Sprintf("**Null Control (MUT-0):** %d observed failures across %d evaluated seeds  \n\n", res.MUT0Violations, res.MUT0Runs))
	} else {
		sb.WriteString("**Null Control (MUT-0):** not evaluated (`TrackMUT0` disabled)  \n\n")
	}
	sb.WriteString("---\n\n")

	sb.WriteString("## 1. Executive Summary\n\n")
	sb.WriteString(fmt.Sprintf("This report presents the empirical multi-seed evaluation of crash candidate ranking strategies on CockroachDB's Pebble key-value engine across %d deterministic workload seeds and %d evaluated fault model(s), comparing **Durability-Contract Guided (DCG)**, **DCG-PR**, **DCG Flag-Only**, **R1**, **R2**, **RHG (B7)**, **Uniform Random (B0)**, **Sync-Boundary (B6)**, **Representative (B5)**, **Combined (B4)**, and ordering baselines.\n\n", seedCount, len(res.FaultSummaries)))

	sb.WriteString("### Summary of Findings\n\n")

	faultOrder := []string{faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4, faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8, faults.MUT9}
	allFaultOrder := orderedSummaryFaults(res.FaultSummaries, faultOrder)

	var rhgComparisons []string
	rhgBeatB0 := false
	for _, sf := range allFaultOrder {
		summ, ok := res.FaultSummaries[sf]
		if !ok || effectiveActiveSeeds(summ) == 0 {
			continue
		}
		b0, okB0 := summ.StrategySummaries["b0"]
		b7, okB7 := summ.StrategySummaries["b7"]
		if !okB0 || !okB7 {
			continue
		}
		if b7.K1FailureMedian < b0.K1FailureMedian || b7.BFullMedian < b0.BFullMedian || b7.AUCNormFailuresMean > b0.AUCNormFailuresMean {
			rhgBeatB0 = true
			rhgComparisons = append(rhgComparisons, fmt.Sprintf(
				"- **%s**: RHG (B7) median $k_1 = %.1f$ (vs B0 median $k_1 = %.1f$), median $B_{\\text{full}} = %.1f$ (vs B0 median $B_{\\text{full}} = %.1f$), $\\text{AUC}_{\\text{norm}} = %.3f$ (vs B0 $\\text{AUC}_{\\text{norm}} = %.3f$)",
				sf, b7.K1FailureMedian, b0.K1FailureMedian, b7.BFullMedian, b0.BFullMedian, b7.AUCNormFailuresMean, b0.AUCNormFailuresMean,
			))
		}
	}

	if rhgBeatB0 {
		sb.WriteString("**RHG has lower measured discovery cost than uniform random on at least one evaluated active fault:**\n\n")
		for _, ev := range rhgComparisons {
			sb.WriteString(ev)
			sb.WriteString("\n")
		}
	} else {
		sb.WriteString("**Empirical trial data summarized below.**\n")
	}

	sb.WriteString("\n---\n\n")
	sb.WriteString("## 2. Experimental Methodology\n\n")
	sb.WriteString("### 2.1 Storage Engine & Calibrated Workload\n\n")
	sb.WriteString(fmt.Sprintf("- **Engine:** CockroachDB Pebble `%s` (`github.com/cockroachdb/pebble/v2`) running with in-memory crashable VFS recording.\n", pebbleVersion))
	sb.WriteString(fmt.Sprintf("- **Workload Specification:** %s.\n", workloadDescription(res.Config.Workload)))
	sb.WriteString(fmt.Sprintf("- **Retention Policies:** `%s` across trace-derived crash candidates.\n\n", policiesStr))

	sb.WriteString("### 2.2 Fault Models Evaluated\n\n")
	sb.WriteString("| Fault Model | Mechanism Summary | Active Seeds | Avg Failures | Fault Density |\n")
	sb.WriteString("|---|---|---|---|---|\n")
	for _, fid := range allFaultOrder {
		summ := res.FaultSummaries[fid]
		sb.WriteString(fmt.Sprintf("| **%s** | %s | %d/%d | %.1f | %.1f%% |\n",
			fid, describeEmpiricalFaultMechanism(fid, walDropK, dirDropK), effectiveActiveSeeds(summ), effectiveTotalSeeds(summ), summ.AvgFailures, summ.FaultDensityPct))
	}
	sb.WriteString("\n")

	sb.WriteString("### 2.3 Evaluated Ranking Strategies\n\n")
	sb.WriteString("1. **DCG (Durability-Contract Guided):** Ranks 0%-retention representatives by trace-derived POSIX durability contract features, Barrier Proximity, and Reverse Risk Horizon.\n")
	sb.WriteString("2. **DCG-PR (Partial-Retention Prior):** Prioritizes intermediate-retention candidates with coexisting unsynced directory entries ahead of the 0% sweep.\n")
	sb.WriteString("3. **DCG-FlagOnly (Ablation):** Shuffles uniformly within DCG contract-violation tiers to isolate boolean flag signal.\n")
	sb.WriteString("4. **R2 (State-Representative Random):** Shuffles 0%-retention `(StateKey)` representatives before non-representatives and remaining retention candidates.\n")
	sb.WriteString("5. **R1 (0%-Retention-First Random):** Shuffles 0%-retention candidates uniformly ahead of remaining retention candidates.\n")
	sb.WriteString("6. **B0 (Uniform Random):** Uniform baseline shuffle across the full candidate universe.\n")
	sb.WriteString("7. **B1 (FIFO / Chronological):** Ordering by ascending trace event cutoff.\n")
	sb.WriteString("8. **B2 (LIFO / Reverse-Chronological):** Ordering by descending trace event cutoff.\n")
	sb.WriteString("9. **B3 (Stride / Interleaved):** Interleaved grid sampling across candidate cutoffs.\n")
	sb.WriteString("10. **B4 (Combined Heuristic):** Greedy weighted score combining risk reach, cross-file impact, semantic class, and novelty.\n")
	sb.WriteString("11. **B5 (Representative):** First candidate per distinct `(StateKey, Percent)` equivalence class.\n")
	sb.WriteString("12. **B6 (Sync-Boundary):** Boundary-adjacent candidate prioritization.\n")
	sb.WriteString("13. **B7 (Risk-Horizon Guided / RHG):** Graph ranking combining Reverse Risk Horizon, Barrier Proximity, 5-Tier Semantics, and Equivalence Partitioning.\n\n")

	sb.WriteString("---\n\n")
	sb.WriteString("## 3. Empirical Multi-Seed Results\n\n")

	curveVal := func(summ FaultModelSummary, code string, bIdx int, useBugs bool) float64 {
		s, ok := summ.StrategySummaries[code]
		if !ok {
			return 0.0
		}
		if useBugs {
			if bIdx < len(s.CurveMeanBugs) {
				return s.CurveMeanBugs[bIdx]
			}
			return 0.0
		}
		if bIdx < len(s.CurveMeanFailures) {
			return s.CurveMeanFailures[bIdx]
		}
		return 0.0
	}

	preferredStratOrder := []string{"dcg", "dcg-pr", "dcg-flag-only", "r2", "r1", "b0", "b7", "b6", "b4", "b5", "b1", "b2", "b3"}

	faultSection := 0
	for _, fid := range allFaultOrder {
		summ, ok := res.FaultSummaries[fid]
		if !ok {
			continue
		}
		faultSection++

		sb.WriteString(fmt.Sprintf("### 3.%d %s Evaluation (%d Seeds, %d Active: Avg %.0f Candidates, Density: %.1f%%)\n\n",
			faultSection, fid, summ.RunCount, effectiveActiveSeeds(summ), summ.AvgUniverseSize, summ.FaultDensityPct))

		sb.WriteString(fmt.Sprintf("**Description:** Average %.1f failures (violations: %.1f, open errors: %.1f, panics: %.1f, hangs: %.1f) with %.1f distinct bug signatures per run.\n\n",
			summ.AvgFailures, summ.AvgViolations, summ.AvgOpenErrors, summ.AvgPanics, summ.AvgHangs, summ.AvgDistinctBugs))

		sb.WriteString(fmt.Sprintf("**Table 1: Discovery Position & Budget Metrics (Mean and Median Across %d Active Seeds)**\n\n", effectiveActiveSeeds(summ)))
		sb.WriteString("| Strategy | First Failure $k_1$ (Median) | First Failure $k_1$ (Mean) | Full Budget $B_{\\text{full}}$ (Median) | Full Budget $B_{\\text{full}}$ (Mean) | $\\text{AUC}_{\\text{norm}}$ (Failures) | $\\text{AUC}_{\\text{norm}}$ (Bugs) |\n")
		sb.WriteString("|---|---|---|---|---|---|---|\n")

		codes := orderedCodesFromSummaryMap(summ.StrategySummaries, preferredStratOrder)
		for _, c := range codes {
			s := summ.StrategySummaries[c]
			label := s.StrategyName
			if c == "b0" {
				b0MedStr := fmt.Sprintf("%.1f [P10: %.1f, P90: %.1f]", s.K1FailureMedian,
					median(extractB0Field(summ.Runs, func(r SingleRunResult) float64 { return r.B0.K1FailureP10 })),
					median(extractB0Field(summ.Runs, func(r SingleRunResult) float64 { return r.B0.K1FailureP90 })))
				sb.WriteString(fmt.Sprintf("| **B0 Random** | %s | %.2f | %.1f | %.2f | %.4f | %.4f |\n",
					b0MedStr, s.K1FailureMean, s.BFullMedian, s.BFullMean, s.AUCNormFailuresMean, s.AUCNormBugsMean))
			} else {
				highlight := ""
				switch c {
				case "dcg":
					highlight = " **(DCG)**"
				case "b7":
					highlight = " **(RHG)**"
				}
				sb.WriteString(fmt.Sprintf("| %s%s | %.1f | %.2f | %.1f | %.2f | %.4f | %.4f |\n",
					label, highlight, s.K1FailureMedian, s.K1FailureMean, s.BFullMedian, s.BFullMean, s.AUCNormFailuresMean, s.AUCNormBugsMean))
			}
		}

		sb.WriteString("\n**Table 2: Cumulative Failures Discovered vs Injection Budget**\n\n")
		sb.WriteString("| Budget $b$ | B0 Random (Mean) | B7 RHG | B6 Sync-Boundary | B4 Combined | B5 Representative |\n")
		sb.WriteString("|---|---|---|---|---|---|\n")

		budgets := summ.Budgets
		for bIdx, b := range budgets {
			b0F := curveVal(summ, "b0", bIdx, false)
			b7F := curveVal(summ, "b7", bIdx, false)
			b6F := curveVal(summ, "b6", bIdx, false)
			b4F := curveVal(summ, "b4", bIdx, false)
			b5F := curveVal(summ, "b5", bIdx, false)
			sb.WriteString(fmt.Sprintf("| **%d** | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
				b, b0F, b7F, b6F, b4F, b5F))
		}

		sb.WriteString("\n**Table 3: Distinct Bug Signatures Discovered vs Injection Budget**\n\n")
		sb.WriteString("| Budget $b$ | B0 Random (Mean) | B7 RHG | B6 Sync-Boundary | B4 Combined | B5 Representative |\n")
		sb.WriteString("|---|---|---|---|---|---|\n")
		for bIdx, b := range budgets {
			b0B := curveVal(summ, "b0", bIdx, true)
			b7B := curveVal(summ, "b7", bIdx, true)
			b6B := curveVal(summ, "b6", bIdx, true)
			b4B := curveVal(summ, "b4", bIdx, true)
			b5B := curveVal(summ, "b5", bIdx, true)
			sb.WriteString(fmt.Sprintf("| **%d** | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
				b, b0B, b7B, b6B, b4B, b5B))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("---\n\n")
	sb.WriteString("## 4. Observed Comparative Summary Across Evaluated Faults\n\n")
	sb.WriteString("| Fault Model | Active / Total | DCG Median $k_1$ | RHG (B7) Median $k_1$ | B0 Median $k_1$ | DCG $\\text{AUC}_{\\text{norm}}$ (Failures) | B0 $\\text{AUC}_{\\text{norm}}$ (Failures) |\n")
	sb.WriteString("|---|---|---|---|---|---|---|\n")
	for _, fid := range allFaultOrder {
		summ := res.FaultSummaries[fid]
		dcg, hasDCG := summ.StrategySummaries["dcg"]
		b7, hasB7 := summ.StrategySummaries["b7"]
		b0, hasB0 := summ.StrategySummaries["b0"]
		fmtMetric := func(ok bool, v float64, decimals int) string {
			if !ok {
				return "n/a"
			}
			return fmt.Sprintf("%.*f", decimals, v)
		}
		sb.WriteString(fmt.Sprintf("| **%s** | %d/%d | %s | %s | %s | %s | %s |\n",
			fid, effectiveActiveSeeds(summ), effectiveTotalSeeds(summ),
			fmtMetric(hasDCG, dcg.K1FailureMedian, 1),
			fmtMetric(hasB7, b7.K1FailureMedian, 1),
			fmtMetric(hasB0, b0.K1FailureMedian, 1),
			fmtMetric(hasDCG, dcg.AUCNormFailuresMean, 4),
			fmtMetric(hasB0, b0.AUCNormFailuresMean, 4),
		))
	}
	sb.WriteString("\n")

	return sb.String()
}

func describeEmpiricalFaultMechanism(fid string, walDropK, dirDropK int) string {
	norm, err := faults.NormalizeID(fid)
	if err != nil {
		norm = fid
	}
	switch norm {
	case faults.MUT0:
		return "Null control (no injected sync faults)"
	case faults.MUT1:
		return "Suppresses sync calls on WAL files (`pebble-wal`)"
	case faults.MUT2:
		return "Suppresses sync calls on manifest files (`pebble-manifest`)"
	case faults.MUT3:
		return "Suppresses all parent directory syncs (`OpenDir`)"
	case faults.MUT4:
		return "Suppresses `SyncData` during memtable SST flushes"
	case faults.MUT5:
		return fmt.Sprintf("Drops strictly the $k$-th WAL sync ($k=%d$)", walDropK)
	case faults.MUT6:
		return "Drops directory sync during manifest rotation"
	case faults.MUT7:
		return "Fails writes targeting SST compaction outputs (control)"
	case faults.MUT8:
		return "Suppresses sync calls on compaction output SSTs"
	case faults.MUT9:
		return fmt.Sprintf("Drops strictly the $k$-th directory sync ($k=%d$)", dirDropK)
	default:
		return fmt.Sprintf("Custom/parameterized fault model (`%s`)", fid)
	}
}

func extractB0Field(runs []SingleRunResult, fieldFn func(SingleRunResult) float64) []float64 {
	var res []float64
	for _, r := range runs {
		if hasB0Metrics(r) {
			res = append(res, fieldFn(r))
		}
	}
	return res
}

// WriteEmpiricalReportToFile generates the report and writes it atomically to targetPath.
func WriteEmpiricalReportToFile(res *BenchmarkResult, targetPath string) error {
	if res == nil {
		return fmt.Errorf("write empirical report: nil result")
	}
	content := GenerateEmpiricalReportMarkdown(res)
	return writeBytesAtomic(targetPath, func(w io.Writer) error {
		_, err := w.Write([]byte(content))
		return err
	})
}

// WriteEmpiricalCSV exports the tabular benchmark comparison atomically to CSV.
func WriteEmpiricalCSV(res *BenchmarkResult, csvPath string) error {
	if res == nil {
		return fmt.Errorf("write empirical csv: nil result")
	}
	return writeCSVAtomic(csvPath, func(w *csv.Writer) error {
		header := []string{
			"fault_id", "strategy", "k1_failure_median", "k1_failure_mean",
			"k1_violation_median", "k1_violation_mean", "b_full_median", "b_full_mean",
			"auc_norm_failures", "auc_norm_bugs",
		}
		if err := w.Write(header); err != nil {
			return err
		}

		faultOrder := []string{
			faults.MUT1, faults.MUT2, faults.MUT3, faults.MUT4,
			faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8, faults.MUT9,
		}
		allFaults := orderedSummaryFaults(res.FaultSummaries, faultOrder)
		stratOrder := []string{"dcg", "dcg-pr", "dcg-flag-only", "r2", "r1", "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7"}

		for _, fid := range allFaults {
			summ := res.FaultSummaries[fid]
			codes := orderedCodesFromSummaryMap(summ.StrategySummaries, stratOrder)
			for _, c := range codes {
				s := summ.StrategySummaries[c]
				row := []string{
					fid,
					s.StrategyName,
					fmt.Sprintf("%.2f", s.K1FailureMedian),
					fmt.Sprintf("%.2f", s.K1FailureMean),
					fmt.Sprintf("%.2f", s.K1ViolationMedian),
					fmt.Sprintf("%.2f", s.K1ViolationMean),
					fmt.Sprintf("%.2f", s.BFullMedian),
					fmt.Sprintf("%.2f", s.BFullMean),
					fmt.Sprintf("%.4f", s.AUCNormFailuresMean),
					fmt.Sprintf("%.4f", s.AUCNormBugsMean),
				}
				if err := w.Write(row); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
