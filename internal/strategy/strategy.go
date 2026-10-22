// Package strategy ranks a candidate universe using trace-derived features.
//
// Strategies see only the graph view and the universe. They never observe
// outcomes, the client history, recovery results or explore timings; a test
// in this package checks the import graph to enforce that. Every strategy
// returns a deterministic permutation of the whole universe, and a budget of
// n crashes means "the first n entries of the ranking".
package strategy

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
)

// Strategy orders a universe. Rank receives a validated view and universe
// and returns a new slice; it never modifies its inputs. If a candidate has
// no features in the view, Rank returns nil, which ValidatePermutation
// rejects.
type Strategy interface {
	Name() string
	Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate
}

// Weights are the coefficients of the combined score
// alpha*Desc + beta*CrossFile + gamma*Semantic + delta*Novelty.
type Weights struct {
	Alpha float64
	Beta  float64
	Gamma float64
	Delta float64
}

// DefaultWeights are the initial combined-strategy weights.
var DefaultWeights = Weights{Alpha: 1, Beta: 1, Gamma: 1, Delta: 1}

// ValidatePermutation checks that ranking contains exactly the tuples of
// universe, each once.
func ValidatePermutation(universe, ranking []candidate.Candidate) error {
	if len(universe) != len(ranking) {
		return fmt.Errorf("strategy: ranking has %d candidates, universe has %d", len(ranking), len(universe))
	}
	remaining := make(map[candidate.Candidate]bool, len(universe))
	for _, item := range universe {
		if remaining[item] {
			return fmt.Errorf("strategy: universe contains duplicate %+v", item)
		}
		remaining[item] = true
	}
	for position, item := range ranking {
		if !remaining[item] {
			return fmt.Errorf("strategy: ranking position %d holds %+v, which is duplicated or not in the universe", position, item)
		}
		delete(remaining, item)
	}
	return nil
}

func canonical(universe []candidate.Candidate) []candidate.Candidate {
	result := append(make([]candidate.Candidate, 0, len(universe)), universe...)
	candidate.Sort(result)
	return result
}

// features looks up every candidate's cutoff; ok is false if one is missing.
func features(view *graph.View, universe []candidate.Candidate) ([]graph.Features, bool) {
	if view == nil {
		return nil, len(universe) == 0
	}
	result := make([]graph.Features, len(universe))
	for index, item := range universe {
		value, found := view.Cuts[item.Event]
		if !found {
			return nil, false
		}
		result[index] = value
	}
	return result, true
}

type groupKey struct {
	state   string
	percent int
}

// --- B0: random ---------------------------------------------------------

type random struct{ seed uint64 }

// NewRandom returns the random baseline: a PCG(0, seed) shuffle of the
// canonical universe.
func NewRandom(seed uint64) Strategy { return random{seed: seed} }

func (random) Name() string { return "random" }

func (strategy random) Rank(_ *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	result := canonical(universe)
	generator := rand.New(rand.NewPCG(0, strategy.seed))
	generator.Shuffle(len(result), func(left, right int) { result[left], result[right] = result[right], result[left] })
	return result
}

// --- B5: representative -------------------------------------------------

type representative struct{}

// NewRepresentative returns the representative-sampling baseline: the first
// canonical tuple of each (StateKey, Percent) group, then all remaining
// tuples in canonical order.
func NewRepresentative() Strategy { return representative{} }

func (representative) Name() string { return "representative" }

func (representative) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}
	seen := make(map[groupKey]bool)
	first := make([]candidate.Candidate, 0, len(ordered))
	var rest []candidate.Candidate
	for index, item := range ordered {
		key := groupKey{labels[index].StateKey, item.Percent}
		if seen[key] {
			rest = append(rest, item)
			continue
		}
		seen[key] = true
		first = append(first, item)
	}
	return append(first, rest...)
}

// --- B6: sync-boundary --------------------------------------------------

type syncBoundary struct{}

// NewSyncBoundary returns the sync-boundary baseline: candidates just
// before a successful sync or rename first, then all others, each group in
// canonical order. It reorders and never removes candidates.
func NewSyncBoundary() Strategy { return syncBoundary{} }

func (syncBoundary) Name() string { return "sync-boundary" }

func (syncBoundary) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}
	boundary := make([]candidate.Candidate, 0, len(ordered))
	var others []candidate.Candidate
	for index, item := range ordered {
		if labels[index].BeforeBoundary {
			boundary = append(boundary, item)
		} else {
			others = append(others, item)
		}
	}
	return append(boundary, others...)
}

// --- B4: combined -------------------------------------------------------

type combined struct{ weights Weights }

// NewCombined returns the combined strategy. Weights must be finite and
// nonnegative. Persistence-only uses Gamma=0, semantic-only uses
// Alpha=Beta=0, and an ablation zeroes exactly its named term.
func NewCombined(weights Weights) (Strategy, error) {
	for name, value := range map[string]float64{"alpha": weights.Alpha, "beta": weights.Beta, "gamma": weights.Gamma, "delta": weights.Delta} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return nil, fmt.Errorf("strategy: weight %s=%v must be finite and nonnegative", name, value)
		}
	}
	return combined{weights: weights}, nil
}

func (combined) Name() string { return "combined" }

// scored is one heap entry. novel records whether score includes the
// novelty bonus; such entries are re-scored once their group is seen.
type scored struct {
	position int
	score    float64
	novel    bool
}

type scoreHeap []scored

func (h scoreHeap) Len() int { return len(h) }
func (h scoreHeap) Less(left, right int) bool {
	if h[left].score != h[right].score {
		return h[left].score > h[right].score
	}
	return h[left].position < h[right].position
}
func (h scoreHeap) Swap(left, right int) { h[left], h[right] = h[right], h[left] }
func (h *scoreHeap) Push(value any)      { *h = append(*h, value.(scored)) }
func (h *scoreHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// Rank selects greedily: at each step the remaining candidate with the
// highest score, where Novelty is one only while no selected candidate
// shares its (StateKey, Percent). Ties go to the earlier canonical tuple.
// Scores never increase, so a lazily re-scored max-heap gives exactly the
// order of recomputing every remaining score at each step.
func (strategy combined) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}
	w := strategy.weights
	base := make([]float64, len(ordered))
	queue := make(scoreHeap, 0, len(ordered))
	for index, label := range labels {
		base[index] = w.Alpha*float64(label.Desc) + w.Beta*float64(label.CrossFile) + w.Gamma*float64(label.Semantic)
		queue = append(queue, scored{position: index, score: base[index] + w.Delta, novel: true})
	}
	heap.Init(&queue)
	seen := make(map[groupKey]bool)
	result := make([]candidate.Candidate, 0, len(ordered))
	for queue.Len() > 0 {
		top := heap.Pop(&queue).(scored)
		key := groupKey{labels[top.position].StateKey, ordered[top.position].Percent}
		if top.novel && seen[key] {
			heap.Push(&queue, scored{position: top.position, score: base[top.position], novel: false})
			continue
		}
		seen[key] = true
		result = append(result, ordered[top.position])
	}
	return result
}

// ScoreRecord records the exact components and total score of a candidate at selection time.
type ScoreRecord struct {
	Candidate candidate.Candidate `json:"candidate"`
	Rank      int                 `json:"rank"` // zero-based position in the ranking
	Desc      uint64              `json:"desc"`
	CrossFile uint64              `json:"cross_file"`
	Semantic  uint64              `json:"semantic"`
	Novelty   uint64              `json:"novelty"` // at selection time
	Score     float64             `json:"score"`
}

// Explain produces the detailed audit records explaining combined strategy ranking,
// reproducing NewCombined(w).Rank exactly.
func Explain(view *graph.View, universe []candidate.Candidate, w Weights) ([]ScoreRecord, error) {
	for name, value := range map[string]float64{"alpha": w.Alpha, "beta": w.Beta, "gamma": w.Gamma, "delta": w.Delta} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return nil, fmt.Errorf("strategy: weight %s=%v must be finite and nonnegative", name, value)
		}
	}

	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil, errors.New("strategy: view missing features for candidates")
	}

	base := make([]float64, len(ordered))
	queue := make(scoreHeap, 0, len(ordered))
	for index, label := range labels {
		base[index] = w.Alpha*float64(label.Desc) + w.Beta*float64(label.CrossFile) + w.Gamma*float64(label.Semantic)
		queue = append(queue, scored{position: index, score: base[index] + w.Delta, novel: true})
	}
	heap.Init(&queue)

	seen := make(map[groupKey]bool)
	records := make([]ScoreRecord, 0, len(ordered))
	for queue.Len() > 0 {
		top := heap.Pop(&queue).(scored)
		key := groupKey{labels[top.position].StateKey, ordered[top.position].Percent}
		if top.novel && seen[key] {
			heap.Push(&queue, scored{position: top.position, score: base[top.position], novel: false})
			continue
		}
		seen[key] = true
		var novelty uint64
		if top.novel {
			novelty = 1
		}
		records = append(records, ScoreRecord{
			Candidate: ordered[top.position],
			Rank:      len(records),
			Desc:      labels[top.position].Desc,
			CrossFile: labels[top.position].CrossFile,
			Semantic:  labels[top.position].Semantic,
			Novelty:   novelty,
			Score:     top.score,
		})
	}

	return records, nil
}

// ErrUnknownStrategy is returned by ByName for an unrecognized name.
var ErrUnknownStrategy = errors.New("strategy: unknown strategy name")

// ByName constructs one of the four initial strategies by its fixed name.
// The seed is used only by random; weights only by combined.
func ByName(name string, seed uint64, weights Weights) (Strategy, error) {
	switch name {
	case "random":
		return NewRandom(seed), nil
	case "representative":
		return NewRepresentative(), nil
	case "sync-boundary":
		return NewSyncBoundary(), nil
	case "combined":
		return NewCombined(weights)
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownStrategy, name)
}
