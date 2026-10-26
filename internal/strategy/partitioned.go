package strategy

import (
	"math/rand/v2"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
)

// --- R1: 0%-Retention-First Random Baseline -------------------------------

type r1 struct {
	seed uint64
}

// NewR1 returns the R1 baseline (0%-Retention-First Random):
// It partitions the candidate universe into 0% unsynced data retention candidates
// (U_0%) and non-zero / 100% retention candidates (U_100%).
// It shuffles U_0% using a deterministic PCG generator, shuffles U_100% using
// an independent deterministic PCG generator, and returns U_0% followed by U_100%.
// This establishes a fair random baseline by eliminating trials on 100% retention
// candidates (which have zero failure density) while maintaining pure random ordering.
func NewR1(seed uint64) Strategy {
	return r1{seed: seed}
}

// NewR0RetentionFirstRandom is an alias constructor for NewR1.
func NewR0RetentionFirstRandom(seed uint64) Strategy {
	return r1{seed: seed}
}

func (r1) Name() string { return "r1" }

func (s r1) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if _, ok := features(view, ordered); !ok {
		return nil
	}

	u0 := make([]candidate.Candidate, 0, len(ordered))
	u100 := make([]candidate.Candidate, 0, len(ordered))
	for _, c := range ordered {
		if c.Percent == 0 {
			u0 = append(u0, c)
		} else {
			u100 = append(u100, c)
		}
	}

	rng0 := rand.New(rand.NewPCG(0, s.seed))
	rng0.Shuffle(len(u0), func(left, right int) { u0[left], u0[right] = u0[right], u0[left] })

	rng1 := rand.New(rand.NewPCG(1, s.seed))
	rng1.Shuffle(len(u100), func(left, right int) { u100[left], u100[right] = u100[right], u100[left] })

	return append(u0, u100...)
}

// --- R2: State-Representative R1 Baseline ---------------------------------

type r2 struct {
	seed uint64
}

// NewR2 returns the R2 baseline (State-Representative R1):
// It partitions U_0% into equivalence classes by canonical persistence state (StateKey).
// Exactly one representative candidate per distinct persistence state is selected
// (prioritizing boundary cutoffs with highest barrier proximity), and the distinct
// representatives are shuffled and placed first.
// Then, remaining 0% candidates (shuffled) and 100% retention candidates (shuffled)
// are appended, guaranteeing that all distinct persistence states are explored
// at 0% retention before any redundant cutoff is tested, while strictly returning
// a valid permutation of the candidate universe.
func NewR2(seed uint64) Strategy {
	return r2{seed: seed}
}

// NewStateRepresentativeR1 is an alias constructor for NewR2.
func NewStateRepresentativeR1(seed uint64) Strategy {
	return r2{seed: seed}
}

func (r2) Name() string { return "r2" }

func (s r2) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}

	// 1. Partition universe into 0% retention candidates and 100% retention candidates.
	var u0 []candidate.Candidate
	var u0Labels []graph.Features
	var u100 []candidate.Candidate

	for i, c := range ordered {
		if c.Percent == 0 {
			u0 = append(u0, c)
			u0Labels = append(u0Labels, labels[i])
		} else {
			u100 = append(u100, c)
		}
	}

	// 2. Group U_0% into equivalence classes by StateKey.
	type stateGroup struct {
		key        string
		candidates []candidate.Candidate
		feats      []graph.Features
	}

	groupIndex := make(map[string]int)
	var groups []stateGroup

	for i, c := range u0 {
		key := u0Labels[i].StateKey
		idx, found := groupIndex[key]
		if !found {
			idx = len(groups)
			groupIndex[key] = idx
			groups = append(groups, stateGroup{key: key})
		}
		groups[idx].candidates = append(groups[idx].candidates, c)
		groups[idx].feats = append(groups[idx].feats, u0Labels[i])
	}

	// 3. For each distinct StateKey in U_0%, select ONE representative candidate.
	// Preference:
	// a. BeforeBoundary == true
	// b. Higher BarrierProximity
	// c. Canonical Less (lowest event, percent, seed)
	zeroReps := make([]candidate.Candidate, 0, len(groups))
	var zeroNonReps []candidate.Candidate

	for _, g := range groups {
		bestIdx := 0
		for j := 1; j < len(g.candidates); j++ {
			if isR2BetterRepresentative(g.feats[j], g.feats[bestIdx], g.candidates[j], g.candidates[bestIdx]) {
				bestIdx = j
			}
		}
		zeroReps = append(zeroReps, g.candidates[bestIdx])
		for j := 0; j < len(g.candidates); j++ {
			if j != bestIdx {
				zeroNonReps = append(zeroNonReps, g.candidates[j])
			}
		}
	}

	// 4. Ensure canonical order before shuffling to eliminate map/group iteration non-determinism.
	candidate.Sort(zeroReps)
	candidate.Sort(zeroNonReps)
	candidate.Sort(u100)

	// 5. Shuffle partitions using independent PCG generators seeded deterministically.
	rng0 := rand.New(rand.NewPCG(0, s.seed))
	rng0.Shuffle(len(zeroReps), func(i, j int) { zeroReps[i], zeroReps[j] = zeroReps[j], zeroReps[i] })

	rng1 := rand.New(rand.NewPCG(1, s.seed))
	rng1.Shuffle(len(zeroNonReps), func(i, j int) { zeroNonReps[i], zeroNonReps[j] = zeroNonReps[j], zeroNonReps[i] })

	rng2 := rand.New(rand.NewPCG(2, s.seed))
	rng2.Shuffle(len(u100), func(i, j int) { u100[i], u100[j] = u100[j], u100[i] })

	// 6. Concatenate: distinct state representatives first, then remaining 0%, then 100%.
	result := make([]candidate.Candidate, 0, len(ordered))
	result = append(result, zeroReps...)
	result = append(result, zeroNonReps...)
	result = append(result, u100...)

	return result
}

func isR2BetterRepresentative(fA, fB graph.Features, cA, cB candidate.Candidate) bool {
	if fA.BeforeBoundary != fB.BeforeBoundary {
		return fA.BeforeBoundary // boundary cutoffs preferred
	}
	if fA.BarrierProximity != fB.BarrierProximity {
		return fA.BarrierProximity > fB.BarrierProximity
	}
	return candidate.Less(cA, cB)
}
