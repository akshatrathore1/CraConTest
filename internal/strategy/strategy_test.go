package strategy_test

import (
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
)

func allStrategies(test *testing.T) []strategy.Strategy {
	test.Helper()
	combined, err := strategy.NewCombined(strategy.DefaultWeights)
	if err != nil {
		test.Fatal(err)
	}
	return []strategy.Strategy{strategy.NewRandom(7), strategy.NewRepresentative(), strategy.NewSyncBoundary(), combined}
}

// syntheticView builds a view with hand-chosen features. Strategies read
// only Cuts, so no trace is needed.
func syntheticView(features map[uint64]graph.Features) *graph.View {
	return &graph.View{Cuts: features}
}

func referenceView() (*graph.View, []candidate.Candidate) {
	view := syntheticView(map[uint64]graph.Features{
		1: {Desc: 2, CrossFile: 3, Semantic: 1, StateKey: "A"},
		2: {Desc: 2, CrossFile: 3, Semantic: 1, StateKey: "A"},
		3: {Desc: 2, CrossFile: 4, Semantic: 0, StateKey: "B"},
	})
	return view, []candidate.Candidate{{Event: 1, Percent: 0, Seed: 1}, {Event: 2, Percent: 0, Seed: 2}, {Event: 3, Percent: 100, Seed: 3}}
}

func TestNames(test *testing.T) {
	want := []string{"random", "representative", "sync-boundary", "combined"}
	for index, item := range allStrategies(test) {
		if item.Name() != want[index] {
			test.Fatalf("name %q, want %q", item.Name(), want[index])
		}
		built, err := strategy.ByName(want[index], 7, strategy.DefaultWeights)
		if err != nil || built.Name() != want[index] {
			test.Fatalf("ByName(%q) failed: %v", want[index], err)
		}
	}
	if _, err := strategy.ByName("future", 0, strategy.DefaultWeights); err == nil {
		test.Fatal("unknown strategy accepted")
	}
}

func TestCombinedReferenceGreedyOrder(test *testing.T) {
	view, universe := referenceView()
	original := append([]candidate.Candidate(nil), universe...)
	combined, _ := strategy.NewCombined(strategy.DefaultWeights)
	ranking := combined.Rank(view, universe)
	var order []uint64
	for _, item := range ranking {
		order = append(order, item.Event)
	}
	if !reflect.DeepEqual(order, []uint64{1, 3, 2}) {
		test.Fatalf("greedy order %v, want [1 3 2]", order)
	}
	if !reflect.DeepEqual(universe, original) {
		test.Fatal("Rank modified its universe")
	}
}

// naiveCombined recomputes every remaining score at every step, exactly as
// the contract describes, to check the heap implementation.
func naiveCombined(view *graph.View, universe []candidate.Candidate, w strategy.Weights) []candidate.Candidate {
	remaining := append([]candidate.Candidate(nil), universe...)
	candidate.Sort(remaining)
	type key struct {
		state   string
		percent int
	}
	seen := map[key]bool{}
	var result []candidate.Candidate
	for len(remaining) > 0 {
		best, bestScore := 0, math.Inf(-1)
		for index, item := range remaining {
			f := view.Cuts[item.Event]
			novel := 0.0
			if !seen[key{f.StateKey, item.Percent}] {
				novel = 1
			}
			score := w.Alpha*float64(f.Desc) + w.Beta*float64(f.CrossFile) + w.Gamma*float64(f.Semantic) + w.Delta*novel
			if score > bestScore {
				best, bestScore = index, score
			}
		}
		chosen := remaining[best]
		seen[key{view.Cuts[chosen.Event].StateKey, chosen.Percent}] = true
		result = append(result, chosen)
		remaining = append(remaining[:best], remaining[best+1:]...)
	}
	return result
}

func randomInstance(seed uint64, size int) (*graph.View, []candidate.Candidate) {
	generator := rand.New(rand.NewPCG(1, seed))
	cuts := map[uint64]graph.Features{}
	var universe []candidate.Candidate
	for event := uint64(0); event < uint64(size); event++ {
		cuts[event] = graph.Features{
			Desc:           uint64(generator.IntN(4)),
			CrossFile:      uint64(generator.IntN(3)),
			Semantic:       uint64(generator.IntN(2)),
			StateKey:       string(rune('A' + generator.IntN(4))),
			BeforeBoundary: generator.IntN(3) == 0,
		}
		for _, percent := range []int{0, 100} {
			universe = append(universe, candidate.Candidate{Event: event, Percent: percent, Seed: generator.Uint64()})
		}
	}
	generator.Shuffle(len(universe), func(l, r int) { universe[l], universe[r] = universe[r], universe[l] })
	return syntheticView(cuts), universe
}

func TestCombinedMatchesNaiveGreedy(test *testing.T) {
	for _, weights := range []strategy.Weights{
		strategy.DefaultWeights,
		{Alpha: 1, Beta: 1, Gamma: 0, Delta: 1},   // persistence-only
		{Alpha: 0, Beta: 0, Gamma: 1, Delta: 1},   // semantic-only
		{Alpha: 0.5, Beta: 2, Gamma: 3, Delta: 0}, // no novelty
		{Alpha: 0, Beta: 0, Gamma: 0, Delta: 0},   // all ties
	} {
		combined, err := strategy.NewCombined(weights)
		if err != nil {
			test.Fatal(err)
		}
		for seed := uint64(0); seed < 25; seed++ {
			view, universe := randomInstance(seed, 30)
			if got, want := combined.Rank(view, universe), naiveCombined(view, universe, weights); !reflect.DeepEqual(got, want) {
				test.Fatalf("weights %+v seed %d: heap order differs from naive greedy", weights, seed)
			}
		}
	}
}

func TestPermutationDeterminismImmutability(test *testing.T) {
	for seed := uint64(0); seed < 10; seed++ {
		view, universe := randomInstance(seed, 40)
		original := append([]candidate.Candidate(nil), universe...)
		cutsBefore := len(view.Cuts)
		for _, item := range allStrategies(test) {
			first := item.Rank(view, universe)
			second := item.Rank(view, universe)
			if err := strategy.ValidatePermutation(universe, first); err != nil {
				test.Fatalf("%s: %v", item.Name(), err)
			}
			if !reflect.DeepEqual(first, second) {
				test.Fatalf("%s is not deterministic", item.Name())
			}
			if !reflect.DeepEqual(universe, original) || len(view.Cuts) != cutsBefore {
				test.Fatalf("%s modified its inputs", item.Name())
			}
			if len(first) > 0 {
				first[0].Seed++
				if reflect.DeepEqual(first, item.Rank(view, universe)) {
					test.Fatalf("%s returns aliased storage", item.Name())
				}
			}
		}
	}
}

func TestEmptyUniverse(test *testing.T) {
	view := syntheticView(map[uint64]graph.Features{})
	for _, item := range allStrategies(test) {
		ranking := item.Rank(view, nil)
		if err := strategy.ValidatePermutation(nil, ranking); err != nil || len(ranking) != 0 {
			test.Fatalf("%s on empty universe: %v %v", item.Name(), ranking, err)
		}
	}
}

func TestMissingFeaturesRejected(test *testing.T) {
	view := syntheticView(map[uint64]graph.Features{0: {}})
	universe := []candidate.Candidate{{Event: 0}, {Event: 5}}
	for _, item := range allStrategies(test)[1:] {
		if err := strategy.ValidatePermutation(universe, item.Rank(view, universe)); err == nil {
			test.Fatalf("%s ranked a candidate without features", item.Name())
		}
	}
}

func TestRandomUsesItsSeedOnly(test *testing.T) {
	view, universe := randomInstance(3, 50)
	shuffled := append([]candidate.Candidate(nil), universe...)
	rand.New(rand.NewPCG(9, 9)).Shuffle(len(shuffled), func(l, r int) { shuffled[l], shuffled[r] = shuffled[r], shuffled[l] })
	if !reflect.DeepEqual(strategy.NewRandom(1).Rank(view, universe), strategy.NewRandom(1).Rank(nil, shuffled)) {
		test.Fatal("random depends on input order or on the view")
	}
	if reflect.DeepEqual(strategy.NewRandom(1).Rank(view, universe), strategy.NewRandom(2).Rank(view, universe)) {
		test.Fatal("random ignores its seed")
	}
}

func TestRepresentativeOrder(test *testing.T) {
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A"}, 2: {StateKey: "A"}, 3: {StateKey: "B"}, 4: {StateKey: "A"},
	})
	universe := []candidate.Candidate{{Event: 4}, {Event: 3}, {Event: 2}, {Event: 1}, {Event: 2, Percent: 100}, {Event: 1, Percent: 100}}
	got := strategy.NewRepresentative().Rank(view, universe)
	want := []candidate.Candidate{{Event: 1}, {Event: 1, Percent: 100}, {Event: 3}, {Event: 2}, {Event: 2, Percent: 100}, {Event: 4}}
	if !reflect.DeepEqual(got, want) {
		test.Fatalf("representative order %v, want %v", got, want)
	}
}

func TestSyncBoundaryReordersWithoutFiltering(test *testing.T) {
	view := syntheticView(map[uint64]graph.Features{1: {}, 2: {BeforeBoundary: true}, 3: {}, 4: {BeforeBoundary: true}})
	universe := []candidate.Candidate{{Event: 1}, {Event: 2}, {Event: 3}, {Event: 4}}
	got := strategy.NewSyncBoundary().Rank(view, universe)
	want := []candidate.Candidate{{Event: 2}, {Event: 4}, {Event: 1}, {Event: 3}}
	if !reflect.DeepEqual(got, want) {
		test.Fatalf("sync-boundary order %v, want %v", got, want)
	}
}

func TestInvalidWeights(test *testing.T) {
	for _, weights := range []strategy.Weights{
		{Alpha: -1}, {Beta: math.NaN()}, {Gamma: math.Inf(1)}, {Delta: math.Inf(-1)},
	} {
		if _, err := strategy.NewCombined(weights); err == nil {
			test.Fatalf("weights %+v accepted", weights)
		}
	}
}

func TestValidatePermutationRejects(test *testing.T) {
	universe := []candidate.Candidate{{Event: 0}, {Event: 1}, {Event: 1, Seed: 5}}
	for name, ranking := range map[string][]candidate.Candidate{
		"omission":   universe[:2],
		"duplicate":  {universe[0], universe[1], universe[1]},
		"fabricated": {universe[0], universe[1], {Event: 1, Percent: 50}},
		"extra":      {universe[0], universe[1], universe[2], {Event: 9}},
	} {
		if err := strategy.ValidatePermutation(universe, ranking); err == nil {
			test.Fatalf("%s ranking accepted", name)
		}
	}
	if err := strategy.ValidatePermutation([]candidate.Candidate{{}, {}}, []candidate.Candidate{{}, {}}); err == nil {
		test.Fatal("duplicate universe accepted")
	}
	if err := strategy.ValidatePermutation(universe, []candidate.Candidate{universe[2], universe[0], universe[1]}); err != nil {
		test.Fatal(err)
	}
}
