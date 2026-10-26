package strategy_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
)

func TestRHGValidPermutation(t *testing.T) {
	strat := strategy.NewRiskHorizon()
	if strat.Name() != "rhg" {
		t.Fatalf("strat.Name() = %q, want %q", strat.Name(), "rhg")
	}

	for seed := uint64(1); seed <= 10; seed++ {
		view, universe := randomInstance(seed, 35)
		ranked := strat.Rank(view, universe)
		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			t.Fatalf("seed %d: ValidatePermutation failed: %v", seed, err)
		}
	}
}

func TestRHGEquivalencePartitioningAndBoundaryRepresentatives(t *testing.T) {
	// Cutoffs 1, 2, 3 all belong to the same StateKey "A", same NextBarrier 10.
	// Cutoff 3 is immediately before the barrier: BarrierProximity = 1.0.
	// Cutoff 2: BarrierProximity = 0.5.
	// Cutoff 1: BarrierProximity = 0.33.
	// Cutoff 4 belongs to StateKey "B", NextBarrier 20: BarrierProximity = 1.0.
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, BarrierProximity: 0.33, RiskScore: 5.0, SemanticWeight: 8.0},
		2: {StateKey: "A", NextBarrier: 10, BarrierProximity: 0.50, RiskScore: 8.0, SemanticWeight: 8.0},
		3: {StateKey: "A", NextBarrier: 10, BarrierProximity: 1.00, RiskScore: 10.0, SemanticWeight: 8.0},
		4: {StateKey: "B", NextBarrier: 20, BarrierProximity: 1.00, RiskScore: 10.0, SemanticWeight: 10.0},
	})

	universe := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 101},
		{Event: 2, Percent: 0, Seed: 102},
		{Event: 3, Percent: 0, Seed: 103},
		{Event: 4, Percent: 0, Seed: 104},
	}

	strat := strategy.NewRiskHorizon()
	ranked := strat.Rank(view, universe)

	if err := strategy.ValidatePermutation(universe, ranked); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	// For equivalence class (StateKey: "A", Epoch: 10, Percent: 0), candidate 3 has the highest
	// barrier proximity (1.00) vs 1 (0.33) and 2 (0.50).
	// Therefore, candidate 3 MUST be selected as representative.
	// Candidate 4 is the only representative for class ("B", 20, 0).
	// The first two positions in ranking MUST be the representatives ({3, 4} or {4, 3}).
	firstTwo := []uint64{ranked[0].Event, ranked[1].Event}
	if (firstTwo[0] != 3 && firstTwo[0] != 4) || (firstTwo[1] != 3 && firstTwo[1] != 4) {
		t.Fatalf("expected boundary representatives {3, 4} at head of ranking, got %v", firstTwo)
	}

	// The remaining candidates {1, 2} are non-representatives and MUST appear at the tail.
	tailTwo := []uint64{ranked[2].Event, ranked[3].Event}
	if !reflect.DeepEqual(tailTwo, []uint64{1, 2}) {
		t.Fatalf("expected non-representatives {1, 2} at tail, got %v", tailTwo)
	}
}

func TestRHGDiscriminativeSemanticTiering(t *testing.T) {
	// Cut 1: Tier 4 Manifest VersionEdit (weight 10.0)
	// Cut 2: Tier 3 Synchronous WAL commit (weight 8.0)
	// Cut 3: Tier 0 Routine data append (weight 0.0)
	// All cutoffs have equal barrier proximity (1.0) and equal risk score (10.0).
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "M", NextBarrier: 10, BarrierProximity: 1.0, RiskScore: 10.0, SemanticTier: 4, SemanticWeight: 10.0},
		2: {StateKey: "W", NextBarrier: 20, BarrierProximity: 1.0, RiskScore: 10.0, SemanticTier: 3, SemanticWeight: 8.0},
		3: {StateKey: "D", NextBarrier: 30, BarrierProximity: 1.0, RiskScore: 10.0, SemanticTier: 0, SemanticWeight: 0.0},
	})

	universe := []candidate.Candidate{
		{Event: 3, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 1, Percent: 0, Seed: 3},
	}

	strat := strategy.NewRiskHorizon()
	ranked := strat.Rank(view, universe)

	if err := strategy.ValidatePermutation(universe, ranked); err != nil {
		t.Fatalf("ValidatePermutation failed: %v", err)
	}

	// Tier 4 (Manifest, event 1) must be ranked before Tier 3 (WAL, event 2),
	// which must be ranked before Tier 0 (Routine data, event 3).
	order := []uint64{ranked[0].Event, ranked[1].Event, ranked[2].Event}
	want := []uint64{1, 2, 3}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("semantic tiering order = %v, want %v", order, want)
	}
}

func TestRHGReverseRiskHorizonPrioritization(t *testing.T) {
	// Cut 1: Pre-commit prefix cutoff: zero dirty bytes, zero uncommitted ops (RiskScore: 0.0)
	// Cut 2: High risk cutoff: large unpersisted dirty byte volume and uncommitted sync writes (RiskScore: 50.0)
	// Both have equal barrier proximity (0.5) and equal semantic weight (8.0).
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "PreCommit", NextBarrier: 10, BarrierProximity: 0.5, RiskScore: 0.0, SemanticWeight: 8.0},
		2: {StateKey: "HighRisk", NextBarrier: 20, BarrierProximity: 0.5, RiskScore: 50.0, SemanticWeight: 8.0},
	})

	universe := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
	}

	strat := strategy.NewRiskHorizon()
	ranked := strat.Rank(view, universe)

	if ranked[0].Event != 2 {
		t.Fatalf("expected high risk candidate (Event 2) to be ranked before pre-commit candidate (Event 1), got: %+v", ranked)
	}
}

func TestRHGDeterminismAndImmutability(t *testing.T) {
	strat := strategy.NewRiskHorizon()
	for seed := uint64(1); seed <= 5; seed++ {
		view, universe := randomInstance(seed, 25)
		original := append([]candidate.Candidate(nil), universe...)
		cutsCount := len(view.Cuts)

		r1 := strat.Rank(view, universe)
		r2 := strat.Rank(view, universe)

		if !reflect.DeepEqual(r1, r2) {
			t.Fatalf("RHG is not deterministic for seed %d", seed)
		}
		if !reflect.DeepEqual(universe, original) {
			t.Fatal("RHG modified universe slice")
		}
		if len(view.Cuts) != cutsCount {
			t.Fatal("RHG modified view cuts map")
		}

		if len(r1) > 0 {
			r1[0].Seed++
			if reflect.DeepEqual(r1, strat.Rank(view, universe)) {
				t.Fatal("RHG returns aliased internal storage")
			}
		}
	}
}

func TestRHGEmptyAndMissingFeatures(t *testing.T) {
	strat := strategy.NewRiskHorizon()

	// Empty universe
	view := syntheticView(map[uint64]graph.Features{})
	emptyRanked := strat.Rank(view, nil)
	if err := strategy.ValidatePermutation(nil, emptyRanked); err != nil || len(emptyRanked) != 0 {
		t.Fatalf("empty universe failed: %v %v", emptyRanked, err)
	}

	// Missing candidate features in view must return nil (rejected by ValidatePermutation)
	universe := []candidate.Candidate{{Event: 1}, {Event: 99}}
	viewWithMissing := syntheticView(map[uint64]graph.Features{1: {}})
	rankedMissing := strat.Rank(viewWithMissing, universe)
	if rankedMissing != nil {
		t.Fatalf("expected nil ranking for missing features, got: %v", rankedMissing)
	}
}

func TestRHGWeightsValidation(t *testing.T) {
	for _, w := range []strategy.RHGWeights{
		{Risk: -1, Barrier: 1, Semantic: 1, Novelty: 1},
		{Risk: 1, Barrier: math.NaN(), Semantic: 1, Novelty: 1},
		{Risk: 1, Barrier: 1, Semantic: math.Inf(1), Novelty: 1},
		{Risk: 1, Barrier: 1, Semantic: 1, Novelty: -0.5},
	} {
		if _, err := strategy.NewRiskHorizonWithWeights(w); err == nil {
			t.Errorf("expected error for invalid weights %+v, got nil", w)
		}
	}

	// Valid weights
	valid := strategy.RHGWeights{Risk: 2.5, Barrier: 3.5, Semantic: 1.5, Novelty: 0.5}
	if _, err := strategy.NewRiskHorizonWithWeights(valid); err != nil {
		t.Errorf("unexpected error for valid weights: %v", err)
	}
}

func TestRHGByNameRegistration(t *testing.T) {
	aliases := []string{"rhg", "b7", "risk_horizon", "risk-horizon"}
	for _, name := range aliases {
		strat, err := strategy.ByName(name, 42, strategy.DefaultWeights)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", name, err)
		}
		if strat == nil || strat.Name() != "rhg" {
			t.Fatalf("ByName(%q) = %+v, want strat with Name() == 'rhg'", name, strat)
		}
	}
}

func TestRHGExplainAudit(t *testing.T) {
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, BarrierProximity: 1.0, RiskScore: 10.0, SemanticTier: 4, SemanticWeight: 10.0},
		2: {StateKey: "B", NextBarrier: 20, BarrierProximity: 0.5, RiskScore: 5.0, SemanticTier: 2, SemanticWeight: 6.0},
	})
	universe := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},   // Representative for (A, 10, 0)
		{Event: 1, Percent: 0, Seed: 99},  // Redundant non-representative in same class (A, 10, 0)
		{Event: 2, Percent: 100, Seed: 2}, // Representative for (B, 20, 100)
	}

	records, err := strategy.ExplainRHG(view, universe)
	if err != nil {
		t.Fatalf("ExplainRHG failed: %v", err)
	}
	if len(records) != len(universe) {
		t.Fatalf("records length %d != universe %d", len(records), len(universe))
	}

	repCount := 0
	nonRepCount := 0
	for i, r := range records {
		if r.Rank != i {
			t.Errorf("record %d has Rank %d", i, r.Rank)
		}
		if r.Score <= 0 {
			t.Errorf("record %d has invalid Score %f, want > 0", i, r.Score)
		}
		if r.IsRepresentative {
			repCount++
		} else {
			nonRepCount++
		}
	}
	if repCount != 2 {
		t.Errorf("expected 2 representatives, got %d", repCount)
	}
	if nonRepCount != 1 {
		t.Errorf("expected 1 non-representative, got %d", nonRepCount)
	}
}
