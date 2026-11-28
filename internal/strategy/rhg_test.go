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

	// Defect 2.7: NewRiskHorizon panics on invalid weights instead of silently falling back to defaults.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic for invalid weights in NewRiskHorizon, got none")
			}
		}()
		_ = strategy.NewRiskHorizon(strategy.RHGWeights{Risk: -1})
	}()
}

func TestRHGByNameRegistration(t *testing.T) {
	aliases := []string{"rhg", "b7", "risk_horizon", "risk-horizon"}
	for _, name := range aliases {
		strat, err := strategy.ByName(name, 42)
		if err != nil {
			t.Fatalf("ByName(%q) returned error: %v", name, err)
		}
		if strat == nil || strat.Name() != "rhg" {
			t.Fatalf("ByName(%q) = %+v, want strat with Name() == 'rhg'", name, strat)
		}
	}

	// Verify ByName without weights preserves DefaultRHGWeights {3,4,2,1},
	// whereas explicit DefaultWeights() sets {1,1,1,1}.
	// Candidate 1 has high Risk (normRisk=1.0), 0 Barrier; Candidate 2 has 0 Risk, 0.7 Barrier.
	// With DefaultRHGWeights (Risk=3, Barrier=4): C1 = 3.0+1 = 4.0 > C2 = 2.8+1 = 3.8 -> C1 first.
	// With DefaultWeights (Risk=1, Barrier=1): C1 = 1.0+1 = 2.0 > C2 = 0.7+1 = 1.7;
	// pick Barrier=0.8 so: DefaultRHG: 3*1.0=3.0 vs 4*0.8=3.2 (C2 first!); {1,1,1,1}: 1*1.0=1.0 vs 1*0.8=0.8 (C1 first!).
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, RiskScore: 10.0, BarrierProximity: 0.0},
		2: {StateKey: "B", NextBarrier: 20, RiskScore: 0.0, BarrierProximity: 0.8},
	})
	univ := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
	}
	defStrat, err := strategy.ByName("rhg", 42)
	if err != nil {
		t.Fatalf("ByName(rhg) failed: %v", err)
	}
	if got := defStrat.Rank(view, univ); got[0].Event != 2 {
		t.Fatalf("ByName(rhg) did not use DefaultRHGWeights {3,4,2,1}: got event %d first, want 2", got[0].Event)
	}
	customStrat, err := strategy.ByName("rhg", 42, strategy.DefaultWeights())
	if err != nil {
		t.Fatalf("ByName(rhg, DefaultWeights) failed: %v", err)
	}
	if got := customStrat.Rank(view, univ); got[0].Event != 1 {
		t.Fatalf("ByName(rhg, DefaultWeights) did not apply explicit {1,1,1,1}: got event %d first, want 1", got[0].Event)
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

	w := strategy.DefaultRHGWeights()
	const maxRisk = 10.0 // Event 1 (only 0% representative)
	const maxSem = 10.0
	repCount := 0
	nonRepCount := 0
	for i, r := range records {
		if r.Rank != i {
			t.Errorf("record %d has Rank %d", i, r.Rank)
		}
		wantScore := w.Risk*(r.RiskScore/maxRisk) + w.Barrier*r.BarrierProximity + w.Semantic*(r.SemanticWeight/maxSem) + w.Novelty*r.Novelty
		if math.Abs(r.Score-wantScore) > 1e-12 {
			t.Errorf("record %d Score = %v, want exact %v", i, r.Score, wantScore)
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

func TestRHGNormalizationExcludesTail100AndNonFinite(t *testing.T) {
	// Event 3 (100% only) has huge RiskScore 10000; it must NOT skew maxRisk for 0% reps.
	// Event 1 uses legacy Desc=10 (RiskScore=0) while Event 2 has RiskScore=5.
	// maxRisk over 0% reps must be 10.0 (from Event 1's Desc fallback), not 10000.0 or 1.0!
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, Desc: 10, BarrierProximity: 0.2},
		2: {StateKey: "B", NextBarrier: 20, RiskScore: 5.0, BarrierProximity: 0.4},
		3: {StateKey: "C", NextBarrier: 30, RiskScore: 10000.0, BarrierProximity: 1.0},
	})
	univ := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 3, Percent: 100, Seed: 3},
	}

	records, err := strategy.ExplainRHG(view, univ)
	if err != nil {
		t.Fatalf("ExplainRHG failed: %v", err)
	}
	w := strategy.DefaultRHGWeights()
	// Event 1: normRisk=1.0 -> 3*1 + 4*0.2 + 1 = 4.8
	// Event 2: normRisk=0.5 -> 3*0.5 + 4*0.4 + 1 = 4.1
	if records[0].Candidate.Event != 1 || records[1].Candidate.Event != 2 {
		t.Fatalf("expected Event 1 then Event 2, got %+v", records)
	}
	if math.Abs(records[0].Score-(w.Risk*1.0+w.Barrier*0.2+w.Novelty)) > 1e-12 {
		t.Fatalf("Event 1 score = %v, want %v", records[0].Score, w.Risk*1.0+w.Barrier*0.2+w.Novelty)
	}
	if records[0].RiskScore != 10.0 {
		t.Fatalf("Event 1 effective RiskScore = %v, want 10.0", records[0].RiskScore)
	}

	// Non-finite +Inf / NaN RiskScore in 0% pool must not produce NaN for finite candidates
	// or turn maxRisk to +Inf.
	viewInf := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, RiskScore: math.Inf(1), BarrierProximity: 0.5},
		2: {StateKey: "B", NextBarrier: 20, RiskScore: 4.0, BarrierProximity: 0.5},
		3: {StateKey: "C", NextBarrier: 30, RiskScore: math.NaN(), BarrierProximity: 0.5},
	})
	univInf := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 3, Percent: 0, Seed: 3},
	}
	recInf, err := strategy.ExplainRHG(viewInf, univInf)
	if err != nil {
		t.Fatalf("ExplainRHG (+Inf/NaN) failed: %v", err)
	}
	// Event 1 (+Inf) ranks first, Event 2 (finite 4.0 -> normRisk=1.0) second, Event 3 (NaN) last.
	if recInf[0].Candidate.Event != 1 || recInf[1].Candidate.Event != 2 || recInf[2].Candidate.Event != 3 {
		t.Fatalf("unexpected +Inf/NaN ranking order: %+v", recInf)
	}
	if math.IsNaN(recInf[0].Score) || !math.IsInf(recInf[0].Score, 1) {
		t.Fatalf("Event 1 (+Inf) score = %v, want +Inf (not NaN)", recInf[0].Score)
	}
	if math.IsNaN(recInf[1].Score) || math.Abs(recInf[1].Score-(w.Risk*1.0+w.Barrier*0.5+w.Novelty)) > 1e-12 {
		t.Fatalf("Event 2 finite score corrupted: got %v", recInf[1].Score)
	}
}

func TestRHGLegacyFallbackRepSelectionAndNaN(t *testing.T) {
	// Events 1, 2, 3 share (StateKey: "A", NextBarrier: 10, Percent: 0) and equal BarrierProximity=1.0.
	// Event 1 has NaN RiskScore; Event 2 has Desc=2 (legacy Risk fallback); Event 3 has Desc=5.
	// Event 3 must be selected as representative regardless of slice order.
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, BarrierProximity: 1.0, RiskScore: math.NaN()},
		2: {StateKey: "A", NextBarrier: 10, BarrierProximity: 1.0, Desc: 2, Semantic: 1},
		3: {StateKey: "A", NextBarrier: 10, BarrierProximity: 1.0, Desc: 5, Semantic: 0},
	})
	univ := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 3, Percent: 0, Seed: 3},
	}
	records, err := strategy.ExplainRHG(view, univ)
	if err != nil {
		t.Fatalf("ExplainRHG failed: %v", err)
	}
	if !records[0].IsRepresentative || records[0].Candidate.Event != 3 {
		t.Fatalf("expected Event 3 (highest Desc fallback) as representative, got %+v", records[0])
	}
	if records[0].RiskScore != 5.0 || records[2].SemanticWeight != 2.0 {
		t.Fatalf("expected effective fallback values recorded in RHGScoreRecord, got rec0=%+v rec2=%+v", records[0], records[2])
	}

	// Semantic fallback tie-break when bp and risk match (BeforeBoundary=true fallback bp=1.0).
	viewSem := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, BeforeBoundary: true, Semantic: 0},
		2: {StateKey: "A", NextBarrier: 10, BeforeBoundary: true, Semantic: 1},
	})
	univSem := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
	}
	recSem, err := strategy.ExplainRHG(viewSem, univSem)
	if err != nil {
		t.Fatalf("ExplainRHG sem fallback failed: %v", err)
	}
	if recSem[0].Candidate.Event != 2 || recSem[0].BarrierProximity != 1.0 || recSem[0].SemanticWeight != 2.0 {
		t.Fatalf("expected Event 2 with effective bp=1.0, semW=2.0, got %+v", recSem[0])
	}
}

func TestRHGPartialRetentionMultiEpochAndDuplicates(t *testing.T) {
	// Partial retention: 0% must precede 25%, which must precede 50%, which must precede 100%,
	// even when 100% has an earlier Event ID.
	// Also test multi-epoch novelty decay: two 0% reps share StateKey "A" across epochs 10 and 20.
	view := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, BarrierProximity: 1.0, RiskScore: 10.0},
		2: {StateKey: "A", NextBarrier: 20, BarrierProximity: 1.0, RiskScore: 10.0},
	})
	univ := []candidate.Candidate{
		{Event: 1, Percent: 100, Seed: 1},
		{Event: 2, Percent: 50, Seed: 2},
		{Event: 1, Percent: 25, Seed: 3},
		{Event: 1, Percent: 0, Seed: 4},
		{Event: 2, Percent: 0, Seed: 5},
	}
	records, err := strategy.ExplainRHG(view, univ)
	if err != nil {
		t.Fatalf("ExplainRHG failed: %v", err)
	}
	gotOrder := make([]int, len(records))
	for i, r := range records {
		gotOrder[i] = r.Candidate.Percent
	}
	wantOrder := []int{0, 0, 25, 50, 100}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("percent order = %v, want %v", gotOrder, wantOrder)
	}
	if records[0].Novelty != 1.0 || records[1].Novelty != 0.0 {
		t.Fatalf("multi-epoch novelty decay failed: rec0.Novelty=%v, rec1.Novelty=%v", records[0].Novelty, records[1].Novelty)
	}

	// Duplicate universe must return nil from Rank and error from ExplainRHG.
	dupUniv := []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 1, Percent: 0, Seed: 1},
	}
	if got := strategy.NewRiskHorizon().Rank(view, dupUniv); got != nil {
		t.Fatalf("expected nil on duplicate universe, got %+v", got)
	}
	if _, err := strategy.ExplainRHG(view, dupUniv); err == nil {
		t.Fatal("expected error from ExplainRHG on duplicate universe")
	}

	// Multiple NaN scores in scored 0% representative pool must break ties deterministically
	// via canonical position in scoreHeap.Less regardless of input order.
	viewMultiNaN := syntheticView(map[uint64]graph.Features{
		1: {StateKey: "A", NextBarrier: 10, RiskScore: math.NaN(), BarrierProximity: 0.5},
		2: {StateKey: "B", NextBarrier: 20, RiskScore: math.NaN(), BarrierProximity: 0.5},
		3: {StateKey: "C", NextBarrier: 30, RiskScore: 5.0, BarrierProximity: 0.5},
	})
	fwd := strategy.NewRiskHorizon().Rank(viewMultiNaN, []candidate.Candidate{
		{Event: 1, Percent: 0, Seed: 1},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 3, Percent: 0, Seed: 3},
	})
	rev := strategy.NewRiskHorizon().Rank(viewMultiNaN, []candidate.Candidate{
		{Event: 3, Percent: 0, Seed: 3},
		{Event: 2, Percent: 0, Seed: 2},
		{Event: 1, Percent: 0, Seed: 1},
	})
	if fwd[0].Event != 3 || fwd[1].Event != 1 || fwd[2].Event != 2 || !reflect.DeepEqual(fwd, rev) {
		t.Fatalf("multi-NaN scoreHeap tie-breaking failed: fwd=%+v rev=%+v", fwd, rev)
	}
}
