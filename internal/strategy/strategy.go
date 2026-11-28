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
	"sort"

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

// defaultWeights are the initial combined-strategy weights.
var defaultWeights = Weights{Alpha: 1, Beta: 1, Gamma: 1, Delta: 1}

// DefaultWeights returns the default combined-strategy weights.
// A copy is returned so callers cannot mutate the package-level value.
func DefaultWeights() Weights { return defaultWeights }

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

// cmpFloatDesc compares two float64 values in descending order.
// Finite and infinite values rank ahead of NaN, and two NaNs tie.
func cmpFloatDesc(a, b float64) (decided, greater bool) {
	aNaN, bNaN := math.IsNaN(a), math.IsNaN(b)
	if aNaN && bNaN {
		return false, false
	}
	if aNaN != bNaN {
		return true, !aNaN
	}
	if a != b {
		return true, a > b
	}
	return false, false
}

type namedWeight struct {
	name  string
	value float64
}

func validateNamedWeights(prefix string, pairs []namedWeight) error {
	for _, p := range pairs {
		if math.IsNaN(p.value) || math.IsInf(p.value, 0) || p.value < 0 {
			return fmt.Errorf("%s %s=%v must be finite and nonnegative", prefix, p.name, p.value)
		}
	}
	return nil
}

func validateCombinedWeights(w Weights) error {
	return validateNamedWeights("strategy: weight", []namedWeight{
		{name: "alpha", value: w.Alpha},
		{name: "beta", value: w.Beta},
		{name: "gamma", value: w.Gamma},
		{name: "delta", value: w.Delta},
	})
}

func validateDCGWeightsFromWeights(name string, w Weights) (DCGWeights, error) {
	if err := validateNamedWeights("strategy: dcg weight", []namedWeight{
		{name: "wal", value: w.Alpha},
		{name: "manifest", value: w.Beta},
		{name: "rename", value: w.Gamma},
	}); err != nil {
		return DCGWeights{}, err
	}
	if math.IsNaN(w.Delta) || math.IsInf(w.Delta, 0) || w.Delta != 0 {
		return DCGWeights{}, fmt.Errorf("strategy: %s does not accept non-zero delta weight=%v", name, w.Delta)
	}
	return DCGWeights{
		WALWeight:      w.Alpha,
		ManifestWeight: w.Beta,
		RenameWeight:   w.Gamma,
	}, nil
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

type statePercentKey struct {
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

// --- B1: fifo / chronological -------------------------------------------

type fifo struct{}

// NewFIFO returns the FIFO / chronological baseline: orders candidates
// by event cutoff in ascending chronological order (canonical).
func NewFIFO() Strategy { return fifo{} }

func (fifo) Name() string { return "fifo" }

func (fifo) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if _, ok := features(view, ordered); !ok {
		return nil
	}
	return ordered
}

// --- B2: lifo / reverse chronological -----------------------------------

type lifo struct{}

// NewLIFO returns the LIFO / reverse chronological baseline: orders candidates
// in reverse chronological order (latest cutoffs first).
func NewLIFO() Strategy { return lifo{} }

func (lifo) Name() string { return "lifo" }

func (lifo) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if _, ok := features(view, ordered); !ok {
		return nil
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Event != ordered[j].Event {
			return ordered[i].Event > ordered[j].Event
		}
		if ordered[i].Percent != ordered[j].Percent {
			return ordered[i].Percent < ordered[j].Percent
		}
		return ordered[i].Seed < ordered[j].Seed
	})
	return ordered
}

// --- B3: stride / even interval -----------------------------------------

type stride struct {
	customStride int
}

// NewStride returns the Stride / even interval baseline: systematically
// strides across the candidate timeline in interleaved passes.
// If customStride is omitted or <= 0, stride is dynamically set to ceil(sqrt(N)).
func NewStride(customStride ...int) Strategy {
	s := 0
	if len(customStride) > 0 && customStride[0] > 0 {
		s = customStride[0]
	}
	return stride{customStride: s}
}

func (stride) Name() string { return "stride" }

func (s stride) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if _, ok := features(view, ordered); !ok {
		return nil
	}
	n := len(ordered)
	if n <= 1 {
		return ordered
	}

	step := s.customStride
	if step <= 0 {
		step = int(math.Ceil(math.Sqrt(float64(n))))
	}
	if step < 1 {
		step = 1
	}
	if step > n {
		step = n
	}

	result := make([]candidate.Candidate, 0, n)
	for offset := 0; offset < step; offset++ {
		for i := offset; i < n; i += step {
			result = append(result, ordered[i])
		}
	}
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
	seen := make(map[statePercentKey]bool)
	first := make([]candidate.Candidate, 0, len(ordered))
	var rest []candidate.Candidate
	for index, item := range ordered {
		key := statePercentKey{labels[index].StateKey, item.Percent}
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
	if err := validateCombinedWeights(weights); err != nil {
		return nil, err
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
	if dec, gt := cmpFloatDesc(h[left].score, h[right].score); dec {
		return gt
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

func runCombined(view *graph.View, universe []candidate.Candidate, w Weights, wantRecords bool) ([]candidate.Candidate, []ScoreRecord, bool) {
	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil, nil, false
	}

	base := make([]float64, len(ordered))
	queue := make(scoreHeap, 0, len(ordered))
	for index, label := range labels {
		base[index] = w.Alpha*float64(label.Desc) + w.Beta*float64(label.CrossFile) + w.Gamma*float64(label.Semantic)
		queue = append(queue, scored{position: index, score: base[index] + w.Delta, novel: true})
	}
	heap.Init(&queue)

	seen := make(map[statePercentKey]bool)
	var result []candidate.Candidate
	var records []ScoreRecord
	if wantRecords {
		records = make([]ScoreRecord, 0, len(ordered))
	} else {
		result = make([]candidate.Candidate, 0, len(ordered))
	}
	for queue.Len() > 0 {
		top := heap.Pop(&queue).(scored)
		key := statePercentKey{labels[top.position].StateKey, ordered[top.position].Percent}
		if top.novel && seen[key] {
			heap.Push(&queue, scored{position: top.position, score: base[top.position], novel: false})
			continue
		}
		seen[key] = true
		if wantRecords {
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
		} else {
			result = append(result, ordered[top.position])
		}
	}
	return result, records, true
}

// Rank selects greedily: at each step the remaining candidate with the
// highest score, where Novelty is one only while no selected candidate
// shares its (StateKey, Percent). Ties go to the earlier canonical tuple.
// Scores never increase, so a lazily re-scored max-heap gives exactly the
// order of recomputing every remaining score at each step.
func (strategy combined) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	result, _, ok := runCombined(view, universe, strategy.weights, false)
	if !ok {
		return nil
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
	if err := validateCombinedWeights(w); err != nil {
		return nil, err
	}
	_, records, ok := runCombined(view, universe, w, true)
	if !ok {
		return nil, errors.New("strategy: view missing features for candidates")
	}
	return records, nil
}

// ErrUnknownStrategy is returned by ByName for an unrecognized name.
var ErrUnknownStrategy = errors.New("strategy: unknown strategy name")

// ByName constructs a ranking strategy by its name or canonical baseline code.
// The seed is used by randomized strategies (random, r1, r2, dcg-flag-only).
// When weights is omitted, each weighted strategy uses its own canonical defaults
// (DefaultWeights for combined, DefaultRHGWeights for rhg, DefaultDCGWeights for
// dcg and dcg-pr). When weights is provided, it supplies explicit custom weights
// (including all-zero ablation Weights{}) and is validated as finite and nonnegative;
// for dcg and dcg-pr, Delta must be zero.
func ByName(name string, seed uint64, weights ...Weights) (Strategy, error) {
	if len(weights) > 1 {
		return nil, fmt.Errorf("strategy: ByName accepts at most one Weights override, got %d", len(weights))
	}
	hasCustomWeights := len(weights) == 1
	var custom Weights
	if hasCustomWeights {
		custom = weights[0]
	}

	switch name {
	case "random", "b0":
		return NewRandom(seed), nil
	case "fifo", "b1", "chronological":
		return NewFIFO(), nil
	case "lifo", "b2", "reverse_chronological", "reverse-chronological":
		return NewLIFO(), nil
	case "stride", "b3", "even_interval", "even-interval":
		return NewStride(), nil
	case "combined", "b4":
		if hasCustomWeights {
			return NewCombined(custom)
		}
		return NewCombined(DefaultWeights())
	case "representative", "b5":
		return NewRepresentative(), nil
	case "sync-boundary", "b6", "sync_boundary", "c1":
		return NewSyncBoundary(), nil
	case "rhg", "b7", "risk_horizon", "risk-horizon":
		if hasCustomWeights {
			w := RHGWeights{Risk: custom.Alpha, Barrier: custom.Beta, Semantic: custom.Gamma, Novelty: custom.Delta}
			return NewRiskHorizonWithWeights(w)
		}
		return NewRiskHorizon(DefaultRHGWeights()), nil
	case "r1", "r1-random", "r1_random", "r1_zero_retention_first", "r0_retention_first_random":
		return NewR1(seed), nil
	case "r2", "r2-representative", "r2_representative", "r2_state_representative", "state_representative_r1":
		return NewR2(seed), nil
	case "dcg", "b8", "durability_contract", "durability-contract":
		if hasCustomWeights {
			w, err := validateDCGWeightsFromWeights(name, custom)
			if err != nil {
				return nil, err
			}
			return NewDCG(w), nil
		}
		return NewDCG(DefaultDCGWeights()), nil
	case "dcg-flag-only", "dcg_flag_only", "dcg-flagonly":
		return NewDCGFlagOnly(seed), nil
	case "dcg-pr", "dcg_partial", "dcg-partial":
		if hasCustomWeights {
			w, err := validateDCGWeightsFromWeights(name, custom)
			if err != nil {
				return nil, err
			}
			return NewDCGPartial(w), nil
		}
		return NewDCGPartial(DefaultDCGWeights()), nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownStrategy, name)
}
