package strategy_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func allStrategies(test *testing.T) []strategy.Strategy {
	test.Helper()
	combined, err := strategy.NewCombined(strategy.DefaultWeights())
	if err != nil {
		test.Fatal(err)
	}
	return []strategy.Strategy{
		strategy.NewRandom(7),
		strategy.NewFIFO(),
		strategy.NewLIFO(),
		strategy.NewStride(),
		combined,
		strategy.NewRepresentative(),
		strategy.NewSyncBoundary(),
		strategy.NewRiskHorizon(),
		strategy.NewR1(7),
		strategy.NewR2(7),
		strategy.NewDCG(),
		strategy.NewDCGFlagOnly(7),
		strategy.NewDCGPartial(),
	}
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
	want := []string{
		"random",
		"fifo",
		"lifo",
		"stride",
		"combined",
		"representative",
		"sync-boundary",
		"rhg",
		"r1",
		"r2",
		"dcg",
		"dcg-flag-only",
		"dcg-pr",
	}
	strats := allStrategies(test)
	if len(strats) != len(want) {
		test.Fatalf("allStrategies count %d != want %d", len(strats), len(want))
	}
	for index, item := range strats {
		if item.Name() != want[index] {
			test.Fatalf("name %q, want %q", item.Name(), want[index])
		}
		built, err := strategy.ByName(want[index], 7)
		if err != nil || built.Name() != want[index] {
			test.Fatalf("ByName(%q) failed: %v", want[index], err)
		}
	}
	// Extended coverage for all supported strategy codes
	allCodes := []struct {
		code     string
		expected string
	}{
		{"b0", "random"},
		{"b1", "fifo"},
		{"b2", "lifo"},
		{"b3", "stride"},
		{"b4", "combined"},
		{"b5", "representative"},
		{"b6", "sync-boundary"},
		{"c1", "sync-boundary"},
		{"b7", "rhg"},
		{"b8", "dcg"},
		{"r1", "r1"},
		{"r2", "r2"},
		{"dcg", "dcg"},
		{"dcg-pr", "dcg-pr"},
		{"dcg-flag-only", "dcg-flag-only"},
	}
	for _, tc := range allCodes {
		s, err := strategy.ByName(tc.code, 42)
		if err != nil {
			test.Fatalf("ByName(%q) failed: %v", tc.code, err)
		}
		if s.Name() != tc.expected {
			test.Fatalf("ByName(%q).Name() = %q, want %q", tc.code, s.Name(), tc.expected)
		}
	}
	if _, err := strategy.ByName("future", 0); err == nil {
		test.Fatal("unknown strategy accepted")
	}
}

func TestCombinedReferenceGreedyOrder(test *testing.T) {
	view, universe := referenceView()
	original := append([]candidate.Candidate(nil), universe...)
	combined, _ := strategy.NewCombined(strategy.DefaultWeights())
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
		strategy.DefaultWeights(),
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
		for _, v := range []*graph.View{view, nil} {
			ranking := item.Rank(v, nil)
			if err := strategy.ValidatePermutation(nil, ranking); err != nil || len(ranking) != 0 {
				test.Fatalf("%s on empty universe (view=%v): %v %v", item.Name(), v != nil, ranking, err)
			}
		}
	}
}

func TestMissingFeaturesRejected(test *testing.T) {
	view := syntheticView(map[uint64]graph.Features{0: {}})
	universe := []candidate.Candidate{{Event: 0}, {Event: 5}}
	dupView := syntheticView(map[uint64]graph.Features{0: {StateKey: "A"}})
	dupUniverse := []candidate.Candidate{{Event: 0}, {Event: 0}}

	for _, item := range allStrategies(test)[1:] {
		if got := item.Rank(view, universe); got != nil {
			test.Fatalf("%s did not return nil on missing features: got %+v", item.Name(), got)
		}
		if err := strategy.ValidatePermutation(universe, item.Rank(view, universe)); err == nil {
			test.Fatalf("%s ranked a candidate without features", item.Name())
		}
		if got := item.Rank(nil, universe); got != nil {
			test.Fatalf("%s did not return nil on nil view with non-empty universe: got %+v", item.Name(), got)
		}
		if err := strategy.ValidatePermutation(dupUniverse, item.Rank(dupView, dupUniverse)); err == nil {
			test.Fatalf("%s accepted duplicate universe", item.Name())
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

func TestExplainScoreAudit(t *testing.T) {
	view, universe := referenceView()
	w := strategy.DefaultWeights()

	// Combined rank
	comb, err := strategy.NewCombined(w)
	if err != nil {
		t.Fatalf("NewCombined: %v", err)
	}
	ranked := comb.Rank(view, universe)

	// Explain
	records, err := strategy.Explain(view, universe, w)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}

	if len(records) != len(ranked) {
		t.Fatalf("records len %d != ranked len %d", len(records), len(ranked))
	}

	for i := range ranked {
		rec := records[i]
		if rec.Candidate != ranked[i] {
			t.Errorf("at rank %d: candidate mismatch: %+v vs %+v", i, rec.Candidate, ranked[i])
		}
		if rec.Rank != i {
			t.Errorf("at rank %d: Rank field %d != %d", i, rec.Rank, i)
		}
		expectedScore := w.Alpha*float64(rec.Desc) + w.Beta*float64(rec.CrossFile) + w.Gamma*float64(rec.Semantic) + w.Delta*float64(rec.Novelty)
		if rec.Score != expectedScore {
			t.Errorf("at rank %d: score %.2f != expected %.2f", i, rec.Score, expectedScore)
		}
	}

	// Invalid weights should fail Explain
	if _, err := strategy.Explain(view, universe, strategy.Weights{Alpha: -1}); err == nil {
		t.Error("expected error with negative weight in Explain, got nil")
	}

	// Empty universe should return empty records without error
	emptyRecords, err := strategy.Explain(view, nil, w)
	if err != nil {
		t.Errorf("Explain on empty universe returned error: %v", err)
	}
	if len(emptyRecords) != 0 {
		t.Errorf("Explain on empty universe returned %d records, want 0", len(emptyRecords))
	}

	// Missing view features should fail Explain
	badView := syntheticView(map[uint64]graph.Features{1: {}})
	if _, err := strategy.Explain(badView, universe, w); err == nil {
		t.Error("expected error with missing features in Explain, got nil")
	}
}

func TestB1FIFOOrder(t *testing.T) {
	strat := strategy.NewFIFO()
	if strat.Name() != "fifo" {
		t.Fatalf("name = %q, want 'fifo'", strat.Name())
	}

	for seed := uint64(1); seed <= 5; seed++ {
		view, universe := randomInstance(seed, 30)
		ranked := strat.Rank(view, universe)

		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			t.Fatalf("B1 ValidatePermutation failed: %v", err)
		}

		// Verify strict ascending chronological order
		for i := 1; i < len(ranked); i++ {
			if ranked[i].Event < ranked[i-1].Event {
				t.Fatalf("B1 Event not ascending at index %d: %d < %d", i, ranked[i].Event, ranked[i-1].Event)
			}
			if ranked[i].Event == ranked[i-1].Event && ranked[i].Percent < ranked[i-1].Percent {
				t.Fatalf("B1 Percent tie-break violated at index %d: %d < %d", i, ranked[i].Percent, ranked[i-1].Percent)
			}
		}

		// Verify determinism
		if !reflect.DeepEqual(ranked, strat.Rank(view, universe)) {
			t.Fatalf("B1 is non-deterministic for seed %d", seed)
		}
	}
}

func TestB2LIFOOrder(t *testing.T) {
	strat := strategy.NewLIFO()
	if strat.Name() != "lifo" {
		t.Fatalf("name = %q, want 'lifo'", strat.Name())
	}

	for seed := uint64(1); seed <= 5; seed++ {
		view, universe := randomInstance(seed, 30)
		ranked := strat.Rank(view, universe)

		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			t.Fatalf("B2 ValidatePermutation failed: %v", err)
		}

		// Verify reverse chronological order (Event descending)
		for i := 1; i < len(ranked); i++ {
			if ranked[i].Event > ranked[i-1].Event {
				t.Fatalf("B2 Event not descending at index %d: %d > %d", i, ranked[i].Event, ranked[i-1].Event)
			}
			if ranked[i].Event == ranked[i-1].Event && ranked[i].Percent < ranked[i-1].Percent {
				t.Fatalf("B2 Percent tie-break violated at index %d: %d < %d", i, ranked[i].Percent, ranked[i-1].Percent)
			}
		}

		// Verify determinism
		if !reflect.DeepEqual(ranked, strat.Rank(view, universe)) {
			t.Fatalf("B2 is non-deterministic for seed %d", seed)
		}
	}
}

func TestB3StrideInterleavedOrder(t *testing.T) {
	strat := strategy.NewStride()
	if strat.Name() != "stride" {
		t.Fatalf("name = %q, want 'stride'", strat.Name())
	}

	for seed := uint64(1); seed <= 5; seed++ {
		view, universe := randomInstance(seed, 35)
		ranked := strat.Rank(view, universe)

		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			t.Fatalf("B3 ValidatePermutation failed: %v", err)
		}

		// Verify determinism
		if !reflect.DeepEqual(ranked, strat.Rank(view, universe)) {
			t.Fatalf("B3 is non-deterministic for seed %d", seed)
		}
	}

	// Exact strided index order verification on 10 single-policy candidates (0..9).
	cuts10 := make(map[uint64]graph.Features, 10)
	univ10 := make([]candidate.Candidate, 0, 10)
	for ev := uint64(0); ev < 10; ev++ {
		cuts10[ev] = graph.Features{}
		univ10 = append(univ10, candidate.Candidate{Event: 9 - ev}) // unsorted input
	}
	view10 := syntheticView(cuts10)

	// Default stride: ceil(sqrt(10)) = 4 -> [0, 4, 8, 1, 5, 9, 2, 6, 3, 7]
	rankedDef := strat.Rank(view10, univ10)
	gotDef := make([]uint64, len(rankedDef))
	for i, c := range rankedDef {
		gotDef[i] = c.Event
	}
	wantDef := []uint64{0, 4, 8, 1, 5, 9, 2, 6, 3, 7}
	if !reflect.DeepEqual(gotDef, wantDef) {
		t.Fatalf("default stride order = %v, want %v", gotDef, wantDef)
	}

	// Explicit stride 3 -> [0, 3, 6, 9, 1, 4, 7, 2, 5, 8]
	rankedCustom := strategy.NewStride(3).Rank(view10, univ10)
	gotCustom := make([]uint64, len(rankedCustom))
	for i, c := range rankedCustom {
		gotCustom[i] = c.Event
	}
	wantCustom := []uint64{0, 3, 6, 9, 1, 4, 7, 2, 5, 8}
	if !reflect.DeepEqual(gotCustom, wantCustom) {
		t.Fatalf("stride(3) order = %v, want %v", gotCustom, wantCustom)
	}

	// Oversized stride must clamp to n without spinning.
	rankedHuge := strategy.NewStride(math.MaxInt).Rank(view10, univ10[:5])
	if err := strategy.ValidatePermutation(univ10[:5], rankedHuge); err != nil {
		t.Fatalf("huge stride ValidatePermutation failed: %v", err)
	}
}

func TestByNameAllBaselinesAndAliases(t *testing.T) {
	testCases := map[string]string{
		"b0":                    "random",
		"random":                "random",
		"b1":                    "fifo",
		"fifo":                  "fifo",
		"chronological":         "fifo",
		"b2":                    "lifo",
		"lifo":                  "lifo",
		"reverse_chronological": "lifo",
		"reverse-chronological": "lifo",
		"b3":                    "stride",
		"stride":                "stride",
		"even_interval":         "stride",
		"even-interval":         "stride",
		"b4":                    "combined",
		"combined":              "combined",
		"b5":                    "representative",
		"representative":        "representative",
		"b6":                    "sync-boundary",
		"sync-boundary":         "sync-boundary",
		"sync_boundary":         "sync-boundary",
		"c1":                    "sync-boundary",
		"b7":                    "rhg",
		"rhg":                   "rhg",
		"risk_horizon":          "rhg",
		"risk-horizon":          "rhg",
		"b8":                    "dcg",
		"dcg":                   "dcg",
		"durability_contract":   "dcg",
		"durability-contract":   "dcg",
		"dcg-flag-only":         "dcg-flag-only",
		"dcg_flag_only":         "dcg-flag-only",
		"dcg-flagonly":          "dcg-flag-only",
		"dcg-pr":                "dcg-pr",
		"dcg_partial":           "dcg-pr",
		"dcg-partial":           "dcg-pr",
		"r1":                    "r1",
		"r2":                    "r2",
	}

	for alias, expectedName := range testCases {
		strat, err := strategy.ByName(alias, 42)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", alias, err)
		}
		if strat.Name() != expectedName {
			t.Fatalf("ByName(%q).Name() = %q, want %q", alias, strat.Name(), expectedName)
		}
	}
}

func TestR1PermutationAndDeterminism(t *testing.T) {
	for seed := uint64(0); seed < 10; seed++ {
		view, universe := randomInstance(seed, 30)
		strat := strategy.NewR1(seed)

		ranked := strat.Rank(view, universe)
		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			t.Fatalf("R1 ValidatePermutation failed on seed %d: %v", seed, err)
		}

		// Determinism check: same seed produces identical order
		rankedAgain := strat.Rank(view, universe)
		if !reflect.DeepEqual(ranked, rankedAgain) {
			t.Fatalf("R1 is non-deterministic for seed %d", seed)
		}

		// Diversity check: different seeds produce different permutations
		otherStrat := strategy.NewR1(seed + 999)
		rankedOther := otherStrat.Rank(view, universe)
		if reflect.DeepEqual(ranked, rankedOther) {
			t.Fatalf("R1 produced identical rankings across different seeds %d and %d", seed, seed+999)
		}
	}
}

func TestR1ZeroRetentionFirstInvariant(t *testing.T) {
	for seed := uint64(0); seed < 10; seed++ {
		view, universe := randomInstance(seed, 25)
		strat := strategy.NewR1(seed)
		ranked := strat.Rank(view, universe)

		numZero := 0
		for _, c := range universe {
			if c.Percent == 0 {
				numZero++
			}
		}

		// First numZero elements MUST all be Percent == 0
		for i := 0; i < numZero; i++ {
			if ranked[i].Percent != 0 {
				t.Fatalf("R1 invariant violation at index %d: expected Percent 0, got %d", i, ranked[i].Percent)
			}
		}

		// Remaining elements MUST all be Percent != 0
		for i := numZero; i < len(ranked); i++ {
			if ranked[i].Percent == 0 {
				t.Fatalf("R1 invariant violation at index %d: expected Percent > 0, got 0", i)
			}
		}
	}
}

func TestR2PermutationAndDeterminism(t *testing.T) {
	for seed := uint64(0); seed < 10; seed++ {
		view, universe := randomInstance(seed, 30)
		strat := strategy.NewR2(seed)

		ranked := strat.Rank(view, universe)
		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			t.Fatalf("R2 ValidatePermutation failed on seed %d: %v", seed, err)
		}

		// Determinism check: same seed produces identical order
		rankedAgain := strat.Rank(view, universe)
		if !reflect.DeepEqual(ranked, rankedAgain) {
			t.Fatalf("R2 is non-deterministic for seed %d", seed)
		}

		// Diversity check: different seeds produce different permutations
		otherStrat := strategy.NewR2(seed + 999)
		rankedOther := otherStrat.Rank(view, universe)
		if reflect.DeepEqual(ranked, rankedOther) {
			t.Fatalf("R2 produced identical rankings across different seeds %d and %d", seed, seed+999)
		}
	}
}

func TestR2StateRepresentativeInvariant(t *testing.T) {
	for seed := uint64(0); seed < 10; seed++ {
		view, universe := randomInstance(seed, 30)
		strat := strategy.NewR2(seed)
		ranked := strat.Rank(view, universe)

		// Count distinct StateKeys in 0% retention
		zeroStates := make(map[string]bool)
		numZero := 0
		for _, c := range universe {
			if c.Percent == 0 {
				numZero++
				zeroStates[view.Cuts[c.Event].StateKey] = true
			}
		}
		numDistinctZeroStates := len(zeroStates)

		// 1. The first numDistinctZeroStates elements MUST all be Percent == 0
		// and MUST all have mutually distinct StateKeys!
		repStatesSeen := make(map[string]bool)
		for i := 0; i < numDistinctZeroStates; i++ {
			c := ranked[i]
			if c.Percent != 0 {
				t.Fatalf("R2 rep prefix violation at index %d: expected Percent 0, got %d", i, c.Percent)
			}
			sk := view.Cuts[c.Event].StateKey
			if repStatesSeen[sk] {
				t.Fatalf("R2 rep prefix duplicate StateKey %q at index %d", sk, i)
			}
			repStatesSeen[sk] = true
		}

		if len(repStatesSeen) != numDistinctZeroStates {
			t.Fatalf("R2 did not cover all %d distinct 0%% states in representative head: got %d",
				numDistinctZeroStates, len(repStatesSeen))
		}

		// 2. Next (numZero - numDistinctZeroStates) elements must also be Percent == 0
		for i := numDistinctZeroStates; i < numZero; i++ {
			if ranked[i].Percent != 0 {
				t.Fatalf("R2 non-rep zero violation at index %d: expected Percent 0, got %d", i, ranked[i].Percent)
			}
		}

		// 3. Remaining elements must all be 100% retention
		for i := numZero; i < len(ranked); i++ {
			if ranked[i].Percent == 0 {
				t.Fatalf("R2 tail violation at index %d: expected 100%% retention, got 0%%", i)
			}
		}
	}
}

func TestR2SelectsBoundaryRepresentatives(t *testing.T) {
	// Cutoff 1: BeforeBoundary = false, BarrierProximity = 0.2, StateKey = "StateX"
	// Cutoff 2: BeforeBoundary = true, BarrierProximity = 1.0, StateKey = "StateX"
	view := syntheticView(map[uint64]graph.Features{
		1: {BeforeBoundary: false, BarrierProximity: 0.2, StateKey: "StateX"},
		2: {BeforeBoundary: true, BarrierProximity: 1.0, StateKey: "StateX"},
	})
	universe := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 1, Percent: 100, Seed: 3},
		{Event: 2, Percent: 100, Seed: 4},
	}

	strat := strategy.NewR2(42)
	ranked := strat.Rank(view, universe)

	// In the ranking:
	// Only 1 distinct state for 0% retention ("StateX").
	// So position 0 MUST be the representative, and it MUST be cutoff 2 (boundary cutoff)!
	if ranked[0].Event != 2 || ranked[0].Percent != 0 {
		t.Fatalf("R2 did not select boundary cutoff 2 as representative: got %+v", ranked[0])
	}
	// Position 1 must be cutoff 1 (non-rep 0%)
	if ranked[1].Event != 1 || ranked[1].Percent != 0 {
		t.Fatalf("R2 position 1 should be non-rep cutoff 1: got %+v", ranked[1])
	}
}

func TestR1AndR2ByNameAliases(t *testing.T) {
	r1Aliases := []string{"r1", "r1-random", "r1_random", "r1_zero_retention_first", "r0_retention_first_random"}
	for _, alias := range r1Aliases {
		strat, err := strategy.ByName(alias, 7)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", alias, err)
		}
		if strat.Name() != "r1" {
			t.Fatalf("ByName(%q).Name() = %q, want r1", alias, strat.Name())
		}
	}

	r2Aliases := []string{"r2", "r2-representative", "r2_representative", "r2_state_representative", "state_representative_r1"}
	for _, alias := range r2Aliases {
		strat, err := strategy.ByName(alias, 7)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", alias, err)
		}
		if strat.Name() != "r2" {
			t.Fatalf("ByName(%q).Name() = %q, want r2", alias, strat.Name())
		}
	}
}

func TestR1AndR2MissingFeatures(t *testing.T) {
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A"},
	})
	universe := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 99, Percent: 0, Seed: 2}, // missing feature
	}

	if strategy.NewR1(42).Rank(view, universe) != nil {
		t.Fatal("R1 should return nil on missing features")
	}
	if strategy.NewR2(42).Rank(view, universe) != nil {
		t.Fatal("R2 should return nil on missing features")
	}
}

func TestDCGByNameAliases(t *testing.T) {
	aliases := []string{"dcg", "b8", "durability_contract", "durability-contract"}
	for _, alias := range aliases {
		strat, err := strategy.ByName(alias, 0)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", alias, err)
		}
		if strat.Name() != "dcg" {
			t.Fatalf("ByName(%q).Name() = %q, want dcg", alias, strat.Name())
		}
	}
}

func TestDCGMultiTierRanking(t *testing.T) {
	data100 := make([]byte, 100)
	data50 := make([]byte, 50)
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000001.log", Handle: 1, Data: data100, Len: 100},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""}, // Acked write without sync!
		{ID: 5, Kind: trace.KindWrite, File: "000001.log", Handle: 1, Data: data50, Len: 50, Off: 100},
		{ID: 6, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 7, Kind: trace.KindSync, File: "000001.log", Handle: 1}, // Sync makes op durable
		{ID: 8, Kind: trace.KindOpEnd, Op: 2, OpKind: "set", Err: ""},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	universe := []candidate.Candidate{
		{Event: 4, Percent: 0, Seed: 10},   // Contract-violating representative (State 1)
		{Event: 3, Percent: 0, Seed: 20},   // Redundant/non-rep 0% candidate (State 1)
		{Event: 8, Percent: 0, Seed: 30},   // Clean 0% representative (State 2)
		{Event: 4, Percent: 100, Seed: 40}, // 100% retention candidate
		{Event: 8, Percent: 100, Seed: 50}, // 100% retention candidate
	}

	strat := strategy.NewDCG()
	ranking := strat.Rank(view, universe)
	if ranking == nil {
		t.Fatal("DCG.Rank returned nil")
	}

	if err := strategy.ValidatePermutation(universe, ranking); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	// Ranking must place:
	// 1. Contract-violating representative (Event 4, 0%) in Tier 1
	// 2. Clean representative (Event 8, 0%) in Tier 2
	// 3. Redundant non-rep (Event 3, 0%) in Tier 3
	// 4-5. 100% retention candidates in Tier 4
	if ranking[0].Event != 4 || ranking[0].Percent != 0 {
		t.Fatalf("Rank 0: expected contract-violating rep (Event 4, 0%%), got %+v", ranking[0])
	}
	if ranking[1].Event != 8 || ranking[1].Percent != 0 {
		t.Fatalf("Rank 1: expected clean rep (Event 8, 0%%), got %+v", ranking[1])
	}
	if ranking[2].Event != 3 || ranking[2].Percent != 0 {
		t.Fatalf("Rank 2: expected redundant candidate (Event 3, 0%%), got %+v", ranking[2])
	}
	if ranking[3].Percent != 100 || ranking[4].Percent != 100 {
		t.Fatalf("Ranks 3-4: expected 100%% retention candidates, got %+v, %+v", ranking[3], ranking[4])
	}
}

func TestDCGPermutationBijectivityAndDeterminism(t *testing.T) {
	data100 := make([]byte, 100)
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000001.log", Handle: 1, Data: data100, Len: 100},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},
		{ID: 5, Kind: trace.KindSync, File: "000001.log", Handle: 1},
	}
	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	var universe []candidate.Candidate
	for cut := uint64(0); cut <= 5; cut++ {
		for _, pct := range []int{0, 100} {
			seed, _ := candidate.DeriveSeed(42, cut, pct)
			universe = append(universe, candidate.Candidate{Event: cut, Percent: pct, Seed: seed})
		}
	}

	strat := strategy.NewDCG()
	rank1 := strat.Rank(view, universe)
	rank2 := strat.Rank(view, universe)

	if err := strategy.ValidatePermutation(universe, rank1); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	if !reflect.DeepEqual(rank1, rank2) {
		t.Fatal("DCG ranking is not deterministic across runs")
	}
}

func TestDCGMissingFeatures(t *testing.T) {
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A"},
	})
	universe := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 99, Percent: 0, Seed: 2}, // missing feature
	}

	if strategy.NewDCG().Rank(view, universe) != nil {
		t.Fatal("DCG should return nil on missing features")
	}
}

func TestDCGFlagOnlyByNameAliases(t *testing.T) {
	aliases := []string{"dcg-flag-only", "dcg_flag_only", "dcg-flagonly"}
	for _, alias := range aliases {
		strat, err := strategy.ByName(alias, 42)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", alias, err)
		}
		if strat.Name() != "dcg-flag-only" {
			t.Fatalf("ByName(%q).Name() = %q, want dcg-flag-only", alias, strat.Name())
		}
	}
}

func TestDCGFlagOnlyTiersAndShuffling(t *testing.T) {
	data100 := make([]byte, 100)
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: "", Handle: 2, Dir: true}, // Parent directory synced!
		{ID: 4, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindWrite, File: "000001.log", Cat: "pebble-wal", Handle: 1, Data: data100, Len: 100},
		{ID: 6, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""}, // AckSyncMissing -> Violating!
		{ID: 7, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 8, Kind: trace.KindWrite, File: "000001.log", Cat: "pebble-wal", Handle: 1, Data: data100, Len: 100, Off: 100},
		{ID: 9, Kind: trace.KindOpEnd, Op: 2, OpKind: "set", Err: ""},                    // AckSyncMissing -> Violating!
		{ID: 10, Kind: trace.KindSync, File: "000001.log", Cat: "pebble-wal", Handle: 1}, // Sync WAL -> Clean!
		{ID: 11, Kind: trace.KindOpBegin, Op: 3, OpKind: "set"},
		{ID: 12, Kind: trace.KindOpEnd, Op: 3, OpKind: "set", Err: ""}, // Clean
		{ID: 13, Kind: trace.KindOpBegin, Op: 4, OpKind: "set"},
		{ID: 14, Kind: trace.KindOpEnd, Op: 4, OpKind: "set", Err: ""}, // Clean
	}

	cuts := map[uint64]graph.Features{
		6:  {StateKey: "STATE_V1", BeforeBoundary: true},
		5:  {StateKey: "STATE_V1", BeforeBoundary: false},
		9:  {StateKey: "STATE_V2", BeforeBoundary: true},
		8:  {StateKey: "STATE_V2", BeforeBoundary: false},
		12: {StateKey: "STATE_C1", BeforeBoundary: true},
		11: {StateKey: "STATE_C1", BeforeBoundary: false},
		14: {StateKey: "STATE_C2", BeforeBoundary: true},
		13: {StateKey: "STATE_C2", BeforeBoundary: false},
	}
	view := &graph.View{
		Events: events,
		Cuts:   cuts,
	}

	// 4 states:
	// State V1 (violating): Event 6 (rep), Event 5 (non-rep)
	// State V2 (violating): Event 9 (rep), Event 8 (non-rep)
	// State C1 (clean):     Event 12 (rep), Event 11 (non-rep)
	// State C2 (clean):     Event 14 (rep), Event 13 (non-rep)
	// Plus 100% retention candidates
	universe := []candidate.Candidate{
		{Event: 6, Percent: 0, Seed: 1},   // Violating rep candidate (State V1)
		{Event: 5, Percent: 0, Seed: 2},   // Violating non-rep (State V1)
		{Event: 9, Percent: 0, Seed: 3},   // Violating rep candidate (State V2)
		{Event: 8, Percent: 0, Seed: 4},   // Violating non-rep (State V2)
		{Event: 12, Percent: 0, Seed: 5},  // Clean rep candidate (State C1)
		{Event: 11, Percent: 0, Seed: 6},  // Clean non-rep (State C1)
		{Event: 14, Percent: 0, Seed: 7},  // Clean rep candidate (State C2)
		{Event: 13, Percent: 0, Seed: 8},  // Clean non-rep (State C2)
		{Event: 6, Percent: 100, Seed: 9}, // 100% retention
		{Event: 9, Percent: 100, Seed: 10},
		{Event: 12, Percent: 100, Seed: 11},
		{Event: 14, Percent: 100, Seed: 12},
	}

	seenTier1Orders := make(map[string]bool)
	seenTier2Orders := make(map[string]bool)

	for seed := uint64(1); seed <= 30; seed++ {
		strat := strategy.NewDCGFlagOnly(seed)
		ranking := strat.Rank(view, universe)
		if ranking == nil {
			t.Fatalf("Rank returned nil for seed %d", seed)
		}

		if err := strategy.ValidatePermutation(universe, ranking); err != nil {
			t.Fatalf("ValidatePermutation failed for seed %d: %v", seed, err)
		}

		// Tier 1: positions 0 and 1 must be representatives 6 and 9 (both ContractScore > 0, Percent 0)
		t1A, t1B := ranking[0], ranking[1]
		if (t1A.Event != 6 && t1A.Event != 9) || (t1B.Event != 6 && t1B.Event != 9) || t1A.Event == t1B.Event || t1A.Percent != 0 || t1B.Percent != 0 {
			t.Fatalf("seed %d: Tier 1 reps expected {6, 9} with Percent 0, got %+v, %+v", seed, t1A, t1B)
		}
		seenTier1Orders[fmt.Sprintf("%d-%d", t1A.Event, t1B.Event)] = true

		// Tier 2: positions 2 and 3 must be representatives 12 and 14 (both ContractScore == 0, Percent 0)
		t2A, t2B := ranking[2], ranking[3]
		if (t2A.Event != 12 && t2A.Event != 14) || (t2B.Event != 12 && t2B.Event != 14) || t2A.Event == t2B.Event || t2A.Percent != 0 || t2B.Percent != 0 {
			t.Fatalf("seed %d: Tier 2 reps expected {12, 14} with Percent 0, got %+v, %+v", seed, t2A, t2B)
		}
		seenTier2Orders[fmt.Sprintf("%d-%d", t2A.Event, t2B.Event)] = true

		// Tier 3: positions 4..7 must be non-reps {5, 8, 11, 13} (all Percent 0)
		for pos := 4; pos <= 7; pos++ {
			c := ranking[pos]
			if c.Percent != 0 {
				t.Fatalf("seed %d: position %d expected non-rep Percent 0, got %+v", seed, pos, c)
			}
			if c.Event != 5 && c.Event != 8 && c.Event != 11 && c.Event != 13 {
				t.Fatalf("seed %d: position %d expected non-rep from {5, 8, 11, 13}, got %+v", seed, pos, c)
			}
		}

		// Tier 4: positions 8..11 must be 100% retention candidates
		for pos := 8; pos <= 11; pos++ {
			c := ranking[pos]
			if c.Percent != 100 {
				t.Fatalf("seed %d: position %d expected 100%% retention candidate, got %+v", seed, pos, c)
			}
		}
	}

	// Verify that shuffling within tiers actually produced different orders across seeds
	if len(seenTier1Orders) < 2 {
		t.Fatalf("Tier 1 was not shuffled across 30 seeds: only saw orders %v", seenTier1Orders)
	}
	if len(seenTier2Orders) < 2 {
		t.Fatalf("Tier 2 was not shuffled across 30 seeds: only saw orders %v", seenTier2Orders)
	}
}

func TestDCGFlagOnlyPermutationBijectivityAndDeterminism(t *testing.T) {
	data100 := make([]byte, 100)
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000001.log", Handle: 1, Data: data100, Len: 100},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},
		{ID: 5, Kind: trace.KindSync, File: "000001.log", Handle: 1},
	}
	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	var universe []candidate.Candidate
	for cut := uint64(0); cut <= 5; cut++ {
		for _, pct := range []int{0, 100} {
			seed, _ := candidate.DeriveSeed(42, cut, pct)
			universe = append(universe, candidate.Candidate{Event: cut, Percent: pct, Seed: seed})
		}
	}

	strat := strategy.NewDCGFlagOnly(12345)
	rank1 := strat.Rank(view, universe)
	rank2 := strat.Rank(view, universe)

	if err := strategy.ValidatePermutation(universe, rank1); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}
	if !reflect.DeepEqual(rank1, rank2) {
		t.Fatal("DCGFlagOnly ranking is not deterministic for same seed")
	}

	// Missing features should return nil
	missingView := syntheticView(map[uint64]graph.Features{1: {StateKey: "A"}})
	missingUniverse := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 99, Percent: 0, Seed: 2},
	}
	if strat.Rank(missingView, missingUniverse) != nil {
		t.Fatal("Rank should return nil on missing features")
	}

	// Nil view with non-empty universe must return nil (missing-features contract)
	if got := strat.Rank(nil, universe); got != nil {
		t.Fatalf("Rank on nil view with non-empty universe should return nil, got %+v", got)
	}

	// Empty universe should return empty slice
	if got := strat.Rank(view, nil); len(got) != 0 {
		t.Fatalf("Rank on empty universe returned len %d", len(got))
	}
}

func TestDCGTier3OrderingViolatingBeforeClean(t *testing.T) {
	data100 := make([]byte, 100)
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: "", Handle: 2, Dir: true}, // Parent directory synced!
		{ID: 4, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindWrite, File: "000001.log", Cat: "pebble-wal", Handle: 1, Data: data100, Len: 100},
		{ID: 6, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""}, // AckSyncMissing -> Violating!
		{ID: 7, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 8, Kind: trace.KindWrite, File: "000001.log", Cat: "pebble-wal", Handle: 1, Data: data100, Len: 100, Off: 100},
		{ID: 9, Kind: trace.KindOpEnd, Op: 2, OpKind: "set", Err: ""},                    // AckSyncMissing -> Violating!
		{ID: 10, Kind: trace.KindSync, File: "000001.log", Cat: "pebble-wal", Handle: 1}, // Sync WAL -> Clean!
		{ID: 11, Kind: trace.KindOpBegin, Op: 3, OpKind: "set"},
		{ID: 12, Kind: trace.KindOpEnd, Op: 3, OpKind: "set", Err: ""}, // Clean
		{ID: 13, Kind: trace.KindOpBegin, Op: 4, OpKind: "set"},
		{ID: 14, Kind: trace.KindOpEnd, Op: 4, OpKind: "set", Err: ""}, // Clean
	}

	cuts := map[uint64]graph.Features{
		6:  {StateKey: "STATE_V_REP", BarrierProximity: 0.95, RiskScore: 100, BeforeBoundary: true},
		9:  {StateKey: "STATE_V_GRP", BarrierProximity: 0.90, RiskScore: 90, BeforeBoundary: true},
		7:  {StateKey: "STATE_V_GRP", BarrierProximity: 0.70, RiskScore: 70, BeforeBoundary: false},
		8:  {StateKey: "STATE_V_GRP", BarrierProximity: 0.40, RiskScore: 40, BeforeBoundary: false},
		12: {StateKey: "STATE_C_REP", BarrierProximity: 0.85, RiskScore: 30, BeforeBoundary: true},
		14: {StateKey: "STATE_C_GRP", BarrierProximity: 0.80, RiskScore: 25, BeforeBoundary: true},
		11: {StateKey: "STATE_C_GRP", BarrierProximity: 0.60, RiskScore: 60, BeforeBoundary: false},
		13: {StateKey: "STATE_C_GRP", BarrierProximity: 0.20, RiskScore: 10, BeforeBoundary: false},
	}
	view := &graph.View{
		Events: events,
		Cuts:   cuts,
	}

	// Universe contains:
	// Reps: 6 (V), 9 (V), 12 (C), 14 (C)
	// Non-reps:
	// - Violating: 7 (prox=0.70), 8 (prox=0.40)
	// - Clean:     11 (prox=0.60), 13 (prox=0.20)
	// Plus 100% retention candidates
	universe := []candidate.Candidate{
		{Event: 6, Percent: 0, Seed: 1},
		{Event: 9, Percent: 0, Seed: 2},
		{Event: 7, Percent: 0, Seed: 3}, // Violating non-rep (prox=0.70)
		{Event: 8, Percent: 0, Seed: 4}, // Violating non-rep (prox=0.40)
		{Event: 12, Percent: 0, Seed: 5},
		{Event: 14, Percent: 0, Seed: 6},
		{Event: 11, Percent: 0, Seed: 7}, // Clean non-rep (prox=0.60, risk=60)
		{Event: 13, Percent: 0, Seed: 8}, // Clean non-rep (prox=0.20, risk=10)
		{Event: 6, Percent: 100, Seed: 9},
		{Event: 12, Percent: 100, Seed: 10},
	}

	strat := strategy.NewDCG()
	ranking := strat.Rank(view, universe)
	if ranking == nil {
		t.Fatal("DCG.Rank returned nil")
	}

	if err := strategy.ValidatePermutation(universe, ranking); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	// Tiers:
	// Tier 1: Reps 6 and 9 (both Violating reps)
	if (ranking[0].Event != 6 && ranking[0].Event != 9) || (ranking[1].Event != 6 && ranking[1].Event != 9) {
		t.Fatalf("Ranks 0-1 expected violating reps {6, 9}, got %+v, %+v", ranking[0], ranking[1])
	}

	// Tier 2: Reps 12 and 14 (both Clean reps)
	if (ranking[2].Event != 12 && ranking[2].Event != 14) || (ranking[3].Event != 12 && ranking[3].Event != 14) {
		t.Fatalf("Ranks 2-3 expected clean reps {12, 14}, got %+v, %+v", ranking[2], ranking[3])
	}

	// Tier 3A (Violating non-reps): MUST BE positions 4 and 5!
	// Sorted by BarrierProximity DESC: Event 7 (prox=0.70) before Event 8 (prox=0.40)
	if ranking[4].Event != 7 || ranking[4].Percent != 0 {
		t.Fatalf("Rank 4: expected violating non-rep Event 7 (higher prox 0.70), got %+v", ranking[4])
	}
	if ranking[5].Event != 8 || ranking[5].Percent != 0 {
		t.Fatalf("Rank 5: expected violating non-rep Event 8 (lower prox 0.40), got %+v", ranking[5])
	}

	// Tier 3B (Clean non-reps): MUST BE positions 6 and 7!
	// Sorted by BarrierProximity DESC: Event 11 (prox=0.60) before Event 13 (prox=0.20)
	if ranking[6].Event != 11 || ranking[6].Percent != 0 {
		t.Fatalf("Rank 6: expected clean non-rep Event 11 (higher prox 0.60), got %+v", ranking[6])
	}
	if ranking[7].Event != 13 || ranking[7].Percent != 0 {
		t.Fatalf("Rank 7: expected clean non-rep Event 13 (lower prox 0.20), got %+v", ranking[7])
	}

	// Tier 4: Positions 8 and 9 must be 100% retention candidates
	if ranking[8].Percent != 100 || ranking[9].Percent != 100 {
		t.Fatalf("Ranks 8-9 expected 100%% retention candidates, got %+v, %+v", ranking[8], ranking[9])
	}
}

func TestDCGPartialByNameAndRanking(t *testing.T) {
	stratObj, err := strategy.ByName("dcg-pr", 0)
	if err != nil {
		t.Fatalf("ByName(\"dcg-pr\") failed: %v", err)
	}
	if stratObj.Name() != "dcg-pr" {
		t.Fatalf("Name() = %q, want \"dcg-pr\"", stratObj.Name())
	}

	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		{ID: 2, Kind: trace.KindCreate, File: "db/marker.manifest.000002", Handle: 2},
		{ID: 3, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
		{ID: 4, Kind: trace.KindSync, File: "db", Dir: true, Handle: 3},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// 1. When universe has only 0% and 100%, dcg-pr produces the exact same ranking as dcg.
	u0100 := []candidate.Candidate{
		{Event: 2, Percent: 0, Seed: 1},
		{Event: 4, Percent: 0, Seed: 2},
		{Event: 4, Percent: 100, Seed: 3},
	}
	dcgBase := strategy.NewDCG()
	dcgRank := dcgBase.Rank(view, u0100)
	prRank := stratObj.Rank(view, u0100)
	if !reflect.DeepEqual(dcgRank, prRank) {
		t.Fatalf("dcg-pr rank for 0/100 mismatch with dcg:\n got %v\nwant %v", prRank, dcgRank)
	}

	// 2. When intermediate 50% candidate with UnsyncedDirEntryCount >= 2 is present,
	// dcg-pr places it at position 0 (front of Tier 1), before dcg does.
	uWithMid := []candidate.Candidate{
		{Event: 2, Percent: 0, Seed: 1},
		{Event: 2, Percent: 50, Seed: 2},
		{Event: 4, Percent: 0, Seed: 3},
		{Event: 4, Percent: 100, Seed: 4},
	}
	rankingPR := stratObj.Rank(view, uWithMid)
	if err := strategy.ValidatePermutation(uWithMid, rankingPR); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	if rankingPR[0].Event != 2 || rankingPR[0].Percent != 50 {
		t.Fatalf("dcg-pr expected position 0 to be Event 2 at 50%%, got %+v", rankingPR[0])
	}

	// In contrast, base dcg ranks 0% before 50%
	rankingDCG := dcgBase.Rank(view, uWithMid)
	if rankingDCG[0].Percent != 0 {
		t.Fatalf("dcg expected position 0 to be 0%% candidate, got %+v", rankingDCG[0])
	}

	// 3. Verify H-10 tier ranking: intermediate representatives without high unsynced dir count
	// (midRepsOther) must precede 0% non-representatives (Tier 3).
	// Create a universe where cutoff 4 has two 0% candidates (one rep, one non-rep),
	// and cutoff 4 also has a 50% candidate (intermediate rep with UnsyncedDirEntryCount == 0).
	uTierOrder := []candidate.Candidate{
		{Event: 4, Percent: 0, Seed: 1},  // 0% rep
		{Event: 4, Percent: 0, Seed: 2},  // 0% non-rep
		{Event: 4, Percent: 50, Seed: 3}, // intermediate rep (UnsyncedDirEntryCount == 0)
		{Event: 4, Percent: 100, Seed: 4},
	}
	rankingTier := stratObj.Rank(view, uTierOrder)
	if err := strategy.ValidatePermutation(uTierOrder, rankingTier); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}
	// Expected order:
	// rankingTier[0] -> 0% rep (Event 4, Percent 0, Seed 1)
	// rankingTier[1] -> midRepsOther (Event 4, Percent 50, Seed 3)
	// rankingTier[2] -> 0% non-rep (Event 4, Percent 0, Seed 2)
	// rankingTier[3] -> 100% (Event 4, Percent 100, Seed 4)
	if rankingTier[1].Percent != 50 {
		t.Fatalf("expected intermediate rep (Percent 50) before 0%% non-rep at index 1, got %+v (full: %+v)",
			rankingTier[1], rankingTier)
	}
}

func TestStrategyByNameZeroWeightsPartialSanitizeAndSingleCandidate(t *testing.T) {
	strat, err := strategy.ByName("combined", 1, strategy.Weights{})
	if err != nil {
		t.Fatalf("ByName(combined, zero weights) failed: %v", err)
	}
	// Verify zero weights truly execute all-zero ablation (preserving canonical order),
	// whereas ByName("combined", 1) uses DefaultWeights {1,1,1,1} (ranking Event 2 first).
	view := syntheticView(map[uint64]graph.Features{
		1: {Desc: 0, Semantic: 0, StateKey: "A"},
		2: {Desc: 10, Semantic: 1, StateKey: "B"},
	})
	univ := []candidate.Candidate{{Event: 1}, {Event: 2}}
	order := strat.Rank(view, univ)
	if err := strategy.ValidatePermutation(univ, order); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}
	if order[0].Event != 1 {
		t.Fatalf("ByName(combined, Weights{}) did not apply zero ablation: got Event %d first, want 1", order[0].Event)
	}
	defStrat, err := strategy.ByName("combined", 1)
	if err != nil {
		t.Fatalf("ByName(combined) failed: %v", err)
	}
	if defOrder := defStrat.Rank(view, univ); defOrder[0].Event != 2 {
		t.Fatalf("ByName(combined) did not use DefaultWeights: got Event %d first, want 2", defOrder[0].Event)
	}

	partial := strategy.NewDCGPartial(strategy.DCGWeights{WALWeight: -5, ManifestWeight: math.NaN(), RenameWeight: -1})
	partialOrder := partial.Rank(view, univ)
	if err := strategy.ValidatePermutation(univ, partialOrder); err != nil {
		t.Fatalf("NewDCGPartial sanitized ranking invalid: %v", err)
	}

	single := []candidate.Candidate{{Event: 1}}
	singleOrder := strat.Rank(view, single)
	if err := strategy.ValidatePermutation(single, singleOrder); err != nil {
		t.Fatalf("single-candidate Rank failed: %v", err)
	}
}

func TestByNameWeightValidationAndFidelity(t *testing.T) {
	// Custom valid weights for dcg and dcg-pr (Delta == 0).
	for _, name := range []string{"dcg", "dcg-pr"} {
		s, err := strategy.ByName(name, 0, strategy.Weights{Alpha: 10, Beta: 20, Gamma: 30})
		if err != nil {
			t.Fatalf("ByName(%q, custom weights) failed: %v", name, err)
		}
		if s.Name() != name {
			t.Fatalf("ByName(%q).Name() = %q", name, s.Name())
		}
		// Non-zero Delta must fail instead of being silently dropped.
		if _, err := strategy.ByName(name, 0, strategy.DefaultWeights()); err == nil {
			t.Fatalf("ByName(%q, DefaultWeights with Delta=1) should fail", name)
		}
	}

	// Negative / NaN / Inf weights must return explicit errors across all weighted strategies.
	badWeights := []strategy.Weights{
		{Alpha: -1},
		{Beta: math.NaN()},
		{Gamma: math.Inf(1)},
		{Delta: -0.5},
	}
	for _, name := range []string{"combined", "rhg", "dcg", "dcg-pr"} {
		for _, bw := range badWeights {
			if _, err := strategy.ByName(name, 0, bw); err == nil {
				t.Fatalf("ByName(%q, %+v) should fail", name, bw)
			}
		}
	}

	// Deterministic error messages on multiple invalid weights.
	_, err1 := strategy.NewCombined(strategy.Weights{Alpha: -1, Delta: math.NaN()})
	_, err2 := strategy.NewCombined(strategy.Weights{Alpha: -1, Delta: math.NaN()})
	if err1 == nil || err2 == nil || err1.Error() != err2.Error() {
		t.Fatalf("non-deterministic combined weight validation error: %v vs %v", err1, err2)
	}

	// Passing more than one Weights override must fail.
	if _, err := strategy.ByName("combined", 0, strategy.DefaultWeights(), strategy.DefaultWeights()); err == nil {
		t.Fatal("ByName with 2 Weights arguments should fail")
	}
}

func TestR1AndR2PartialRetentionAndNaNSafety(t *testing.T) {
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", BeforeBoundary: true, BarrierProximity: math.NaN()},
		2: {StateKey: "A", BeforeBoundary: true, BarrierProximity: 0.5},
		3: {StateKey: "B", BeforeBoundary: false, BarrierProximity: 0.1},
	})
	univ := []candidate.Candidate{
		{Event: 1, Percent: 100, Seed: 1},
		{Event: 2, Percent: 50, Seed: 2},
		{Event: 3, Percent: 25, Seed: 3},
		{Event: 1, Percent: 0, Seed: 4},
		{Event: 2, Percent: 0, Seed: 5},
		{Event: 3, Percent: 0, Seed: 6},
	}

	for _, strat := range []strategy.Strategy{strategy.NewR1(42), strategy.NewR2(42)} {
		ranked := strat.Rank(view, univ)
		if err := strategy.ValidatePermutation(univ, ranked); err != nil {
			t.Fatalf("%s ValidatePermutation failed: %v", strat.Name(), err)
		}
		// Positions 0..2: 0%; positions 3..4: partial (25/50); position 5: 100%.
		for i := 0; i < 3; i++ {
			if ranked[i].Percent != 0 {
				t.Fatalf("%s pos %d: got percent %d, want 0", strat.Name(), i, ranked[i].Percent)
			}
		}
		for i := 3; i < 5; i++ {
			if ranked[i].Percent <= 0 || ranked[i].Percent >= 100 {
				t.Fatalf("%s pos %d: got percent %d, want 0 < p < 100", strat.Name(), i, ranked[i].Percent)
			}
		}
		if ranked[5].Percent != 100 {
			t.Fatalf("%s pos 5: got percent %d, want 100", strat.Name(), ranked[5].Percent)
		}
	}

	// Verify R2 selects finite BarrierProximity (Event 2) over NaN (Event 1) regardless of input order.
	r2 := strategy.NewR2(7)
	uOnlyZero := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
	}
	rankedR2 := r2.Rank(view, uOnlyZero)
	if rankedR2[0].Event != 2 {
		t.Fatalf("R2 should select finite BarrierProximity Event 2 over NaN Event 1, got %+v", rankedR2)
	}
}

func TestDCGContractTierLevelsCategoryInterleavingAndNaN(t *testing.T) {
	// Build a trace exercising:
	// - Clean Tier 2 boundary cutoffs (Manifest, Dir, WAL, unclassified tier)
	// - Level 3: AckSyncMissing
	// - Level 2: UnsyncedDirectoryRename (with RenameWeight == 0 ablation)
	// - Level 1: Dangling SST in MANIFEST
	events := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 2},
		{ID: 3, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true}, // Clean TierDir at 3
		// Op 1: WAL write acked without sync -> Level 3 (AckSyncMissing) at cutoff 6
		{ID: 4, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindWrite, File: "db/000001.log", Cat: "pebble-wal", Handle: 2, Data: []byte("abc"), Len: 3},
		{ID: 6, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 7, Kind: trace.KindSync, File: "db/000001.log", Cat: "pebble-wal", Handle: 2}, // Clean TierWAL at 7
		// Clean MANIFEST write+sync -> Clean TierManifest at 9, Clean TierData/unclassified at 10
		{ID: 8, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 3},
		{ID: 9, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},                               // sync dir entry for MANIFEST
		{ID: 10, Kind: trace.KindSync, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 3}, // Clean TierManifest at 10
		{ID: 11, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},                              // Clean unclassified at 11
		{ID: 12, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},                              // Clean non-boundary at 12
		// Pending rename without dir sync (Level 2) crossed by OpBegin at event 15
		{ID: 13, Kind: trace.KindCreate, File: "db/tmp.txt", Handle: 4},
		{ID: 14, Kind: trace.KindRename, File: "db/tmp.txt", File2: "db/CURRENT.tmp"},
		{ID: 15, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"}, // Level 2 at cutoff 15
		{ID: 16, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},
		{ID: 17, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true}, // clears Level 2
		// Dangling SSTable committed in MANIFEST (Level 1) at cutoff 22
		{ID: 18, Kind: trace.KindEngineEvent, Note: "FlushBegin"},
		{ID: 19, Kind: trace.KindCreate, File: "db/000010.sst", Cat: "pebble-memtable-flush", Handle: 5},
		{ID: 20, Kind: trace.KindWrite, File: "db/000010.sst", Cat: "pebble-memtable-flush", Handle: 5, Data: []byte("sst"), Len: 3},
		{ID: 21, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 3, Data: []byte("man"), Len: 3},
		{ID: 22, Kind: trace.KindSync, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 3}, // Level 1 at cutoff 22
	}

	view := &graph.View{
		Events: events,
		Cuts: map[uint64]graph.Features{
			// Tier 1 violating candidates (Level 3 at 6, Level 2 at 15, Level 1 at 22)
			6:  {StateKey: "V3", BarrierProximity: 1.0},
			15: {StateKey: "V2", BarrierProximity: 0.2},
			22: {StateKey: "V1", BarrierProximity: 0.1},
			// Tier 2 clean boundary candidates across semantic tiers + unclassified tier
			3:  {StateKey: "C_DIR", BeforeBoundary: true, SemanticTier: graph.TierDir, BarrierProximity: 1.0},
			7:  {StateKey: "C_WAL", BeforeBoundary: true, SemanticTier: graph.TierWAL, BarrierProximity: 1.0},
			10: {StateKey: "C_MAN", BeforeBoundary: true, SemanticTier: graph.TierManifest, BarrierProximity: 1.0},
			11: {StateKey: "C_UNCLASS", BeforeBoundary: true, SemanticTier: 99, BarrierProximity: 1.0},
			12: {StateKey: "C_NONBOUND", BeforeBoundary: false, SemanticTier: graph.TierData, BarrierProximity: math.NaN(), RiskScore: math.NaN()},
		},
	}

	univ := []candidate.Candidate{
		{Event: 6, Percent: 0, Seed: 1},
		{Event: 15, Percent: 0, Seed: 2},
		{Event: 22, Percent: 0, Seed: 3},
		{Event: 3, Percent: 0, Seed: 4},
		{Event: 7, Percent: 0, Seed: 5},
		{Event: 10, Percent: 0, Seed: 6},
		{Event: 11, Percent: 0, Seed: 7},
		{Event: 12, Percent: 0, Seed: 8},
	}

	// RenameWeight == 0 ablation must STILL keep Event 15 (UnsyncedDirectoryRename) in Tier 1!
	strat := strategy.NewDCG(strategy.DCGWeights{WALWeight: 30, ManifestWeight: 40, RenameWeight: 0})
	ranked := strat.Rank(view, univ)
	if err := strategy.ValidatePermutation(univ, ranked); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	gotEvents := make([]uint64, len(ranked))
	for i, c := range ranked {
		gotEvents[i] = c.Event
	}
	// Expected:
	// Tier 1 (Level 1 -> Level 2 -> Level 3): 22, 15, 6
	// Tier 2 boundary: unclassified tier (11) drained first, then categoryOrder (Manifest=10, Dir=3, WAL=7)
	// Tier 2 non-boundary: 12
	wantEvents := []uint64{22, 15, 6, 11, 10, 3, 7, 12}
	if !reflect.DeepEqual(gotEvents, wantEvents) {
		t.Fatalf("DCG Tier1/Tier2 order = %v, want %v", gotEvents, wantEvents)
	}

	// Also verify DCG-FlagOnly keeps Event 15 in Tier 1 and places 50% ahead of 100%.
	univMid := append([]candidate.Candidate(nil), univ...)
	univMid = append(univMid, candidate.Candidate{Event: 12, Percent: 100, Seed: 9}, candidate.Candidate{Event: 6, Percent: 50, Seed: 10})
	flagRanked := strategy.NewDCGFlagOnly(99).Rank(view, univMid)
	if err := strategy.ValidatePermutation(univMid, flagRanked); err != nil {
		t.Fatalf("DCG-FlagOnly ValidatePermutation failed: %v", err)
	}
	if flagRanked[len(flagRanked)-2].Percent != 50 || flagRanked[len(flagRanked)-1].Percent != 100 {
		t.Fatalf("DCG-FlagOnly did not separate 50%% before 100%%: tail=%+v", flagRanked[len(flagRanked)-2:])
	}
}

func TestDCGPartialMultiMidEmptyU0AndNaNSafety(t *testing.T) {
	// Events where cutoffs have distinct UnsyncedDirEntryCount and contract violations.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "db/000001.log", Cat: "pebble-wal", Handle: 1, Data: []byte("abc"), Len: 3},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"}, // UnsyncedDirEntryCount=1, AckSyncMissing=true
		{ID: 5, Kind: trace.KindCreate, File: "db/000002.sst", Handle: 2},
		{ID: 6, Kind: trace.KindCreate, File: "db/000003.sst", Handle: 3}, // UnsyncedDirEntryCount=3
		{ID: 7, Kind: trace.KindCreate, File: "db/000004.sst", Handle: 4},
		{ID: 8, Kind: trace.KindCreate, File: "db/000005.sst", Handle: 5}, // UnsyncedDirEntryCount=5
		{ID: 9, Kind: trace.KindSync, File: "db/000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 10, Kind: trace.KindOpenDir, File: "db", Handle: 6, Dir: true},
		{ID: 11, Kind: trace.KindSync, File: "db", Handle: 6, Dir: true}, // Clean, UnsyncedDirEntryCount=0
	}

	view := &graph.View{
		Events: events,
		Cuts: map[uint64]graph.Features{
			4:  {StateKey: "MID_VIOL", BarrierProximity: 0.2, RiskScore: math.NaN()},
			6:  {StateKey: "MID_HIGH", BarrierProximity: 0.9, RiskScore: 10},
			8:  {StateKey: "MID_HIGH", BarrierProximity: 0.5, RiskScore: 5},
			11: {StateKey: "MID_CLEAN", BarrierProximity: 1.0, RiskScore: 10},
		},
	}

	// Empty u0, multiple intermediate percentages (25%, 50%), same group preferring higher UnsyncedDirEntryCount (8 over 6),
	// and midRepsOther ranking violating (Event 4) before clean (Event 11).
	univ := []candidate.Candidate{
		{Event: 11, Percent: 100, Seed: 1},
		{Event: 11, Percent: 25, Seed: 2},
		{Event: 4, Percent: 50, Seed: 3},
		{Event: 6, Percent: 50, Seed: 4},
		{Event: 8, Percent: 50, Seed: 5},
	}
	pr := strategy.NewDCGPartial()
	ranked := pr.Rank(view, univ)
	if err := strategy.ValidatePermutation(univ, ranked); err != nil {
		t.Fatalf("dcg-pr empty u0 ValidatePermutation failed: %v", err)
	}
	// Expected:
	// 0: Event 8 (50%, UnsyncedDirEntryCount=5 > Event 6's 3 in MID_HIGH)
	// 1: Event 4 (50%, midRepsOther Tier 1 violating before Event 11's clean Tier 2!)
	// 2: Event 11 (25%, midRepsOther Tier 2 clean)
	// 3: Event 6 (50%, midNonRep)
	// 4: Event 11 (100%, u100)
	want := []uint64{8, 4, 11, 6, 11}
	got := []uint64{ranked[0].Event, ranked[1].Event, ranked[2].Event, ranked[3].Event, ranked[4].Event}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dcg-pr empty u0 order = %v, want %v (full=%+v)", got, want, ranked)
	}
}
