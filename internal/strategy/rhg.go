package strategy

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
)

// RHGWeights are the coefficients for Risk-Horizon Guided ranking:
// Score = Risk * normRisk + Barrier * barrierProx + Semantic * normSem + Novelty * novelty.
type RHGWeights struct {
	Risk     float64
	Barrier  float64
	Semantic float64
	Novelty  float64
}

const (
	// legacySemanticFallbackWeight is the calibrated fallback weight when SemanticWeight is 0 but legacy Semantic > 0.
	legacySemanticFallbackWeight = 2.0
	// defaultBoundaryProximity is the barrier proximity assigned when BeforeBoundary is true but BarrierProximity is 0.
	defaultBoundaryProximity = 1.0
	// maxRHGSemanticWeight is the 5-tier maximum semantic weight used for normalization.
	maxRHGSemanticWeight = 10.0
)

type effectiveRHG struct {
	risk float64
	bp   float64
	semW float64
}

func effectiveRHGFeatures(f graph.Features) effectiveRHG {
	bp := f.BarrierProximity
	if bp == 0 && f.BeforeBoundary {
		bp = defaultBoundaryProximity
	}
	semW := f.SemanticWeight
	if semW == 0 && f.Semantic > 0 {
		semW = legacySemanticFallbackWeight
	}
	risk := f.RiskScore
	if risk == 0 && f.Desc > 0 {
		risk = float64(f.Desc)
	}
	return effectiveRHG{risk: risk, bp: bp, semW: semW}
}

func computeRHGScore(w RHGWeights, eff effectiveRHG, maxRisk, nov float64) float64 {
	normRisk := eff.risk / maxRisk
	normSem := eff.semW / maxRHGSemanticWeight
	return w.Risk*normRisk + w.Barrier*eff.bp + w.Semantic*normSem + w.Novelty*nov
}

func isRHGBetterRep(effA, effB effectiveRHG, candA, candB candidate.Candidate) bool {
	if dec, gt := cmpFloatDesc(effA.bp, effB.bp); dec {
		return gt
	}
	if dec, gt := cmpFloatDesc(effA.risk, effB.risk); dec {
		return gt
	}
	if dec, gt := cmpFloatDesc(effA.semW, effB.semW); dec {
		return gt
	}
	return candidate.Less(candA, candB)
}

// defaultRHGWeights are the literature-grounded default weights:
// Barrier proximity has highest weight (4.0), followed by Reverse Risk (3.0),
// Discriminative Semantics (2.0), and State Novelty (1.0).
var defaultRHGWeights = RHGWeights{
	Risk:     3.0,
	Barrier:  4.0,
	Semantic: 2.0,
	Novelty:  1.0,
}

// DefaultRHGWeights returns the default RHG weights.
// A copy is returned so callers cannot mutate the package-level value.
func DefaultRHGWeights() RHGWeights { return defaultRHGWeights }

func validateRHGWeights(w RHGWeights) error {
	return validateNamedWeights("strategy: rhg weight", []namedWeight{
		{name: "risk", value: w.Risk},
		{name: "barrier", value: w.Barrier},
		{name: "semantic", value: w.Semantic},
		{name: "novelty", value: w.Novelty},
	})
}

// rhgStrategy implements the Risk-Horizon Guided ranking strategy (RHG / B7).
type rhgStrategy struct {
	weights RHGWeights
}

// NewRiskHorizon returns a new Risk-Horizon Guided ranking strategy.
// If weights are provided, they are validated; otherwise DefaultRHGWeights are used.
func NewRiskHorizon(weights ...RHGWeights) Strategy {
	w := DefaultRHGWeights()
	if len(weights) > 0 {
		if err := validateRHGWeights(weights[0]); err != nil {
			panic(fmt.Sprintf("strategy: invalid rhg weights: %v", err))
		}
		w = weights[0]
	}
	return rhgStrategy{weights: w}
}

// NewRiskHorizonWithWeights returns an RHG strategy with explicit error checking for invalid weights.
func NewRiskHorizonWithWeights(weights RHGWeights) (Strategy, error) {
	if err := validateRHGWeights(weights); err != nil {
		return nil, err
	}
	return rhgStrategy{weights: weights}, nil
}

func (rhgStrategy) Name() string { return "rhg" }

// Rank orders a universe using Risk-Horizon Guided ranking:
//  1. Candidate Equivalence Partitioning: groups candidates by (StateKey, Epoch, Percent)
//     and selects boundary representatives with highest Barrier Proximity.
//  2. Greedy lazy max-heap ranking of representatives using Risk Horizon, Barrier Proximity,
//     5-Tier Discriminative Semantics, and State Novelty.
//  3. Tail fill of remaining non-representative candidates in deterministic canonical order.
func (strategy rhgStrategy) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ranked, _ := strategy.rankDetailed(view, universe)
	return ranked
}

func (strategy rhgStrategy) rankDetailed(view *graph.View, universe []candidate.Candidate) ([]candidate.Candidate, []RHGScoreRecord) {
	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil, nil
	}
	if len(ordered) == 0 {
		return []candidate.Candidate{}, []RHGScoreRecord{}
	}

	w := strategy.weights

	// Step 1: Candidate Equivalence Partitioning
	// Cluster candidates by (StateKey, Epoch, Percent).
	// Epoch is NextBarrier (the nearest enclosing subsequent barrier, 0 if none).
	type eqKey struct {
		stateKey string
		epoch    uint64
		percent  int
	}

	reps := make(map[eqKey]candidate.Candidate)
	repIndices := make(map[eqKey]int)
	var nonReps0 []candidate.Candidate

	for i, cand := range ordered {
		f := labels[i]
		epoch := f.NextBarrier
		if epoch == 0 && f.BeforeBoundary {
			epoch = cand.Event + 1
		}

		key := eqKey{
			stateKey: f.StateKey,
			epoch:    epoch,
			percent:  cand.Percent,
		}

		existing, exists := reps[key]
		if !exists {
			reps[key] = cand
			repIndices[key] = i
		} else {
			existIdx := repIndices[key]
			// Boundary representative selection:
			// Prioritize higher effective barrier proximity; break ties by effective risk score,
			// effective semantic weight, then canonical order.
			if isRHGBetterRep(effectiveRHGFeatures(f), effectiveRHGFeatures(labels[existIdx]), cand, existing) {
				if existing.Percent == 0 {
					nonReps0 = append(nonReps0, existing)
				}
				reps[key] = cand
				repIndices[key] = i
			} else if cand.Percent == 0 {
				nonReps0 = append(nonReps0, cand)
			}
		}
	}

	// Build representative set to avoid any key recomputation mismatch
	repSet := make(map[candidate.Candidate]bool, len(reps))
	for _, rep := range reps {
		repSet[rep] = true
	}

	// Step 2: Separate representatives into 0% retention (reps0), partial retention (tailMid),
	// and remaining non-zero/100% retention (tail100).
	// Max-heap scoring is applied strictly to 0% retention representatives!
	repList0 := make([]candidate.Candidate, 0, len(reps))
	repFeats0 := make([]graph.Features, 0, len(reps))
	var tailMid []candidate.Candidate
	var tail100 []candidate.Candidate

	for i, cand := range ordered {
		if cand.Percent == 0 {
			if repSet[cand] {
				repList0 = append(repList0, cand)
				repFeats0 = append(repFeats0, labels[i])
			}
		} else if cand.Percent > 0 && cand.Percent < 100 {
			tailMid = append(tailMid, cand)
		} else {
			tail100 = append(tail100, cand)
		}
	}

	maxRisk := 1.0
	for _, f := range repFeats0 {
		eff := effectiveRHGFeatures(f)
		if !math.IsNaN(eff.risk) && !math.IsInf(eff.risk, 0) && eff.risk > maxRisk {
			maxRisk = eff.risk
		}
	}

	baseScores := make([]float64, len(repList0))
	queue := make(scoreHeap, 0, len(repList0))

	for i := range repList0 {
		eff := effectiveRHGFeatures(repFeats0[i])
		base := computeRHGScore(w, eff, maxRisk, 0.0)
		baseScores[i] = base
		queue = append(queue, scored{position: i, score: base + w.Novelty, novel: true})
	}
	heap.Init(&queue)

	// Step 3: Lazy re-scoring heap ranking of 0% representatives
	seen := make(map[statePercentKey]bool)
	rankedReps0 := make([]candidate.Candidate, 0, len(repList0))
	repScores := make(map[candidate.Candidate]float64, len(repList0))
	repNovelty := make(map[candidate.Candidate]float64, len(repList0))

	for queue.Len() > 0 {
		top := heap.Pop(&queue).(scored)
		cand := repList0[top.position]
		key := statePercentKey{state: repFeats0[top.position].StateKey, percent: cand.Percent}

		if top.novel && seen[key] {
			heap.Push(&queue, scored{position: top.position, score: baseScores[top.position], novel: false})
			continue
		}
		seen[key] = true
		rankedReps0 = append(rankedReps0, cand)
		repScores[cand] = top.score
		if top.novel {
			repNovelty[cand] = 1.0
		} else {
			repNovelty[cand] = 0.0
		}
	}

	// Step 4: Append 0% Non-Representatives first, then partial-retention (0 < Percent < 100),
	// then 100% retention candidates (each tail tier ordered by Percent ascending, then canonical order).
	candidate.Sort(nonReps0)
	sortByPercentThenCanonical := func(s []candidate.Candidate) {
		sort.Slice(s, func(i, j int) bool {
			if s[i].Percent != s[j].Percent {
				return s[i].Percent < s[j].Percent
			}
			return candidate.Less(s[i], s[j])
		})
	}
	sortByPercentThenCanonical(tailMid)
	sortByPercentThenCanonical(tail100)

	finalRanked := make([]candidate.Candidate, 0, len(universe))
	finalRanked = append(finalRanked, rankedReps0...)
	finalRanked = append(finalRanked, nonReps0...)
	finalRanked = append(finalRanked, tailMid...)
	finalRanked = append(finalRanked, tail100...)

	if err := ValidatePermutation(universe, finalRanked); err != nil {
		return nil, nil
	}

	// Populate detailed audit records
	records := make([]RHGScoreRecord, len(finalRanked))
	for rank, cand := range finalRanked {
		f := view.Cuts[cand.Event]
		eff := effectiveRHGFeatures(f)
		isRep := repSet[cand]
		var score float64
		var nov float64
		if isRep && cand.Percent == 0 {
			score = repScores[cand]
			nov = repNovelty[cand]
		} else {
			nov = 0.0
			score = computeRHGScore(w, eff, maxRisk, nov)
		}

		records[rank] = RHGScoreRecord{
			Candidate:        cand,
			Rank:             rank,
			IsRepresentative: isRep,
			RiskScore:        eff.risk,
			BarrierProximity: eff.bp,
			SemanticTier:     f.SemanticTier,
			SemanticWeight:   eff.semW,
			Novelty:          nov,
			Score:            score,
		}
	}

	return finalRanked, records
}

// RHGScoreRecord contains detailed audit records of an RHG ranking.
// RiskScore, BarrierProximity, and SemanticWeight record the effective values
// (including legacy Desc, BeforeBoundary, and Semantic fallbacks) used to compute Score.
// Novelty is 1.0 for the first 0% representative selected per (StateKey, Percent) and 0.0
// for duplicate-state representatives, non-representatives, and un-heaped non-zero tail candidates.
type RHGScoreRecord struct {
	Candidate        candidate.Candidate `json:"candidate"`
	Rank             int                 `json:"rank"`
	IsRepresentative bool                `json:"is_representative"`
	RiskScore        float64             `json:"risk_score"`
	BarrierProximity float64             `json:"barrier_proximity"`
	SemanticTier     int                 `json:"semantic_tier"`
	SemanticWeight   float64             `json:"semantic_weight"`
	Novelty          float64             `json:"novelty"`
	Score            float64             `json:"score"`
}

// ExplainRHG audits the ranking sequence of NewRiskHorizon(weights).
func ExplainRHG(view *graph.View, universe []candidate.Candidate, weights ...RHGWeights) ([]RHGScoreRecord, error) {
	w := DefaultRHGWeights()
	if len(weights) > 0 {
		w = weights[0]
	}
	if err := validateRHGWeights(w); err != nil {
		return nil, err
	}
	strat := rhgStrategy{weights: w}
	_, records := strat.rankDetailed(view, universe)
	if len(universe) > 0 && (records == nil || len(records) != len(universe)) {
		return nil, errors.New("strategy: view missing features for candidates or permutation validation failed")
	}
	return records, nil
}
