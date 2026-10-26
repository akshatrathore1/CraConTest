package strategy

import (
	"container/heap"
	"errors"
	"fmt"
	"math"

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

// DefaultRHGWeights are the literature-grounded default weights:
// Barrier proximity has highest weight (4.0), followed by Reverse Risk (3.0),
// Discriminative Semantics (2.0), and State Novelty (1.0).
var DefaultRHGWeights = RHGWeights{
	Risk:     3.0,
	Barrier:  4.0,
	Semantic: 2.0,
	Novelty:  1.0,
}

func validateRHGWeights(w RHGWeights) error {
	for name, value := range map[string]float64{
		"risk": w.Risk, "barrier": w.Barrier, "semantic": w.Semantic, "novelty": w.Novelty,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("strategy: rhg weight %s=%v must be finite and nonnegative", name, value)
		}
	}
	return nil
}

// rhgStrategy implements the Risk-Horizon Guided ranking strategy (RHG / B7).
type rhgStrategy struct {
	weights RHGWeights
}

// NewRiskHorizon returns a new Risk-Horizon Guided ranking strategy.
// If weights are provided, they are validated; otherwise DefaultRHGWeights are used.
func NewRiskHorizon(weights ...RHGWeights) Strategy {
	w := DefaultRHGWeights
	if len(weights) > 0 {
		if err := validateRHGWeights(weights[0]); err == nil {
			w = weights[0]
		}
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
	var nonReps100 []candidate.Candidate

	for i, cand := range ordered {
		f := labels[i]
		epoch := f.NextBarrier
		if epoch == 0 && f.BeforeBoundary {
			epoch = cand.Event + 1
		}
		bp := f.BarrierProximity
		if bp == 0 && f.BeforeBoundary {
			bp = 1.0
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
			existBP := labels[existIdx].BarrierProximity
			if existBP == 0 && labels[existIdx].BeforeBoundary {
				existBP = 1.0
			}

			// Boundary representative selection:
			// Prioritize higher barrier proximity; break ties by risk score, semantic weight, then canonical order.
			promote := false
			if bp > existBP {
				promote = true
			} else if bp == existBP {
				if f.RiskScore > labels[existIdx].RiskScore {
					promote = true
				} else if f.RiskScore == labels[existIdx].RiskScore && f.SemanticWeight > labels[existIdx].SemanticWeight {
					promote = true
				}
			}

			if promote {
				if existing.Percent == 0 {
					nonReps0 = append(nonReps0, existing)
				} else {
					nonReps100 = append(nonReps100, existing)
				}
				reps[key] = cand
				repIndices[key] = i
			} else {
				if cand.Percent == 0 {
					nonReps0 = append(nonReps0, cand)
				} else {
					nonReps100 = append(nonReps100, cand)
				}
			}
		}
	}

	// Build representative set to avoid any key recomputation mismatch
	repSet := make(map[candidate.Candidate]bool, len(reps))
	for _, rep := range reps {
		repSet[rep] = true
	}

	// Step 2: Separate representatives into 0% retention (reps0) and 100% retention (reps100).
	// Max-heap scoring is applied strictly to 0% retention representatives!
	repList0 := make([]candidate.Candidate, 0, len(reps))
	repFeats0 := make([]graph.Features, 0, len(reps))
	var reps100 []candidate.Candidate

	for i, cand := range ordered {
		if repSet[cand] {
			if cand.Percent == 0 {
				repList0 = append(repList0, cand)
				repFeats0 = append(repFeats0, labels[i])
			} else {
				reps100 = append(reps100, cand)
			}
		}
	}

	maxRisk := 1.0
	for _, f := range repFeats0 {
		if f.RiskScore > maxRisk {
			maxRisk = f.RiskScore
		}
	}
	for _, cand := range reps100 {
		f := view.Cuts[cand.Event]
		if f.RiskScore > maxRisk {
			maxRisk = f.RiskScore
		}
	}
	const maxSem = 10.0 // 5-tier maximum weight

	baseScores := make([]float64, len(repList0))
	queue := make(scoreHeap, 0, len(repList0))

	for i := range repList0 {
		f := repFeats0[i]
		bp := f.BarrierProximity
		if bp == 0 && f.BeforeBoundary {
			bp = 1.0
		}
		semW := f.SemanticWeight
		if semW == 0 && f.Semantic > 0 {
			semW = 2.0 // fallback for synthetic view with legacy Semantic=1
		}
		risk := f.RiskScore
		if risk == 0 && f.Desc > 0 {
			risk = float64(f.Desc)
		}

		normRisk := risk / maxRisk
		normSem := semW / maxSem

		base := w.Risk*normRisk + w.Barrier*bp + w.Semantic*normSem
		baseScores[i] = base
		queue = append(queue, scored{position: i, score: base + w.Novelty, novel: true})
	}
	heap.Init(&queue)

	// Step 3: Lazy re-scoring heap ranking of 0% representatives
	seen := make(map[groupKey]bool)
	rankedReps0 := make([]candidate.Candidate, 0, len(repList0))
	repScores := make(map[candidate.Candidate]float64, len(reps))
	repNovelty := make(map[candidate.Candidate]float64, len(reps))

	for queue.Len() > 0 {
		top := heap.Pop(&queue).(scored)
		cand := repList0[top.position]
		key := groupKey{state: repFeats0[top.position].StateKey, percent: cand.Percent}

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

	// For reps100, calculate representative scores for audit records
	for _, cand := range reps100 {
		f := view.Cuts[cand.Event]
		bp := f.BarrierProximity
		if bp == 0 && f.BeforeBoundary {
			bp = 1.0
		}
		semW := f.SemanticWeight
		if semW == 0 && f.Semantic > 0 {
			semW = 2.0
		}
		risk := f.RiskScore
		if risk == 0 && f.Desc > 0 {
			risk = float64(f.Desc)
		}
		normRisk := risk / maxRisk
		normSem := semW / maxSem
		repScores[cand] = w.Risk*normRisk + w.Barrier*bp + w.Semantic*normSem
		repNovelty[cand] = 0.0
	}

	// Step 4: Append 0% Non-Representatives first, then tail 100% candidates in canonical order
	candidate.Sort(nonReps0)
	candidate.Sort(reps100)
	candidate.Sort(nonReps100)

	tail100 := make([]candidate.Candidate, 0, len(reps100)+len(nonReps100))
	tail100 = append(tail100, reps100...)
	tail100 = append(tail100, nonReps100...)

	finalRanked := make([]candidate.Candidate, 0, len(universe))
	finalRanked = append(finalRanked, rankedReps0...)
	finalRanked = append(finalRanked, nonReps0...)
	finalRanked = append(finalRanked, tail100...)

	if err := ValidatePermutation(universe, finalRanked); err != nil {
		panic(fmt.Sprintf("strategy: rhg permutation invalid: %v", err))
	}

	// Populate detailed audit records
	records := make([]RHGScoreRecord, len(finalRanked))
	for rank, cand := range finalRanked {
		f := view.Cuts[cand.Event]
		isRep := repSet[cand]
		var score float64
		var nov float64
		if isRep {
			score = repScores[cand]
			nov = repNovelty[cand]
		} else {
			bp := f.BarrierProximity
			if bp == 0 && f.BeforeBoundary {
				bp = 1.0
			}
			semW := f.SemanticWeight
			if semW == 0 && f.Semantic > 0 {
				semW = 2.0
			}
			risk := f.RiskScore
			if risk == 0 && f.Desc > 0 {
				risk = float64(f.Desc)
			}
			normRisk := risk / maxRisk
			normSem := semW / maxSem
			score = w.Risk*normRisk + w.Barrier*bp + w.Semantic*normSem
			nov = 0.0
		}

		records[rank] = RHGScoreRecord{
			Candidate:        cand,
			Rank:             rank,
			IsRepresentative: isRep,
			RiskScore:        f.RiskScore,
			BarrierProximity: f.BarrierProximity,
			SemanticTier:     f.SemanticTier,
			SemanticWeight:   f.SemanticWeight,
			Novelty:          nov,
			Score:            score,
		}
	}

	return finalRanked, records
}

// RHGScoreRecord contains detailed audit records of an RHG ranking.
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
	w := DefaultRHGWeights
	if len(weights) > 0 {
		w = weights[0]
	}
	if err := validateRHGWeights(w); err != nil {
		return nil, err
	}
	strat := rhgStrategy{weights: w}
	ranked, records := strat.rankDetailed(view, universe)
	if ranked == nil && len(universe) > 0 {
		return nil, errors.New("strategy: view missing features for candidates")
	}
	return records, nil
}
