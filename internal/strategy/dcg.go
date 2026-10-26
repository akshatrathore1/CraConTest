package strategy

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
)

// DCGWeights configures term weights for DCG ranking.
type DCGWeights = graph.DCGWeights

// DefaultDCGWeights provides standard term weights.
var DefaultDCGWeights = graph.DefaultDCGWeights

type dcg struct {
	weights DCGWeights
}

// NewDCG creates a new Durability-Contract Guided ranking strategy.
// Optional custom weights can be supplied.
func NewDCG(weights ...DCGWeights) Strategy {
	w := DefaultDCGWeights
	if len(weights) > 0 {
		w = weights[0]
	}
	if math.IsNaN(w.WALWeight) || w.WALWeight < 0 {
		w.WALWeight = DefaultDCGWeights.WALWeight
	}
	if math.IsNaN(w.ManifestWeight) || w.ManifestWeight < 0 {
		w.ManifestWeight = DefaultDCGWeights.ManifestWeight
	}
	if math.IsNaN(w.RenameWeight) || w.RenameWeight < 0 {
		w.RenameWeight = DefaultDCGWeights.RenameWeight
	}
	return dcg{weights: w}
}

func (dcg) Name() string { return "dcg" }

// Rank implements Durability-Contract Guided candidate ranking:
//  1. Partitions candidate universe by (StateKey, Percent).
//  2. Isolates 0% retention canonical state representatives.
//  3. Tier 1: 0% retention state representatives with contract violations (ContractScore > 0),
//     ranked by contractTierLevel, BarrierProximity, ContractScore.
//  4. Tier 2: 0% retention state representatives without contract violations (ContractScore == 0),
//     ranked by BarrierProximity and reverse risk horizon (RiskScore) with semantic barrier interleaving.
//  5. Tier 3A: Remaining 0% retention violating non-representatives (ContractScore > 0).
//  6. Tier 3B: Remaining 0% retention clean non-representatives (ContractScore == 0).
//  7. Tier 4: 100% retention candidates in canonical order.
//
// Strictly validates that the returned slice is a bijection of universe (ValidatePermutation).
func (s dcg) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if len(ordered) <= 1 {
		return ordered
	}
	if view == nil {
		return ordered
	}
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}

	// 1. Compute contract features if trace events are available in view.
	var contractMap map[uint64]graph.ContractFeatures
	if view != nil && len(view.Events) > 0 {
		contractMap = graph.ComputeContractFeaturesWithWeights(view.Events, s.weights)
	}

	// 2. Separate into 0% retention pool (U_0) and non-zero / 100% retention pool (U_100).
	var u0 []candidate.Candidate
	var u0Labels []graph.Features
	var u0Contract []graph.ContractFeatures
	var u100 []candidate.Candidate

	for i, c := range ordered {
		if c.Percent == 0 {
			u0 = append(u0, c)
			u0Labels = append(u0Labels, labels[i])
			if contractMap != nil {
				u0Contract = append(u0Contract, contractMap[c.Event])
			} else {
				u0Contract = append(u0Contract, graph.ContractFeatures{})
			}
		} else {
			u100 = append(u100, c)
		}
	}

	// 3. Group U_0 by StateKey.
	type stateGroup struct {
		key        string
		candidates []candidate.Candidate
		feats      []graph.Features
		contracts  []graph.ContractFeatures
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
		groups[idx].contracts = append(groups[idx].contracts, u0Contract[i])
	}

	// 4. For each distinct StateKey in U_0, select exactly ONE canonical representative.
	type repInfo struct {
		cand     candidate.Candidate
		feat     graph.Features
		contract graph.ContractFeatures
	}

	var zeroReps []repInfo
	var zeroNonReps []repInfo

	for _, g := range groups {
		bestIdx := 0
		for j := 1; j < len(g.candidates); j++ {
			if isDCGBetterRep(g.feats[j], g.feats[bestIdx], g.contracts[j], g.contracts[bestIdx], g.candidates[j], g.candidates[bestIdx]) {
				bestIdx = j
			}
		}
		zeroReps = append(zeroReps, repInfo{
			cand:     g.candidates[bestIdx],
			feat:     g.feats[bestIdx],
			contract: g.contracts[bestIdx],
		})
		for j := 0; j < len(g.candidates); j++ {
			if j != bestIdx {
				zeroNonReps = append(zeroNonReps, repInfo{
					cand:     g.candidates[j],
					feat:     g.feats[j],
					contract: g.contracts[j],
				})
			}
		}
	}

	// 5. Partition representatives into Tier 1 and Tier 2.
	// Tier 1: Contract violations (ContractScore > 0).
	// Tier 2: Non-violating state representatives (ContractScore == 0).
	var tier1 []repInfo
	var tier2 []repInfo

	for _, rep := range zeroReps {
		if rep.contract.ContractScore > 0 {
			tier1 = append(tier1, rep)
		} else {
			tier2 = append(tier2, rep)
		}
	}

	// Sort Tier 1:
	// Level 1: Dangling SST / Unpersisted Manifest (MUT-4, MUT-2, MUT-6, MUT-8)
	// Level 2: Unsynced Directory Rename (MUT-3, MUT-7)
	// Level 3: AckSyncMissing (MUT-5, MUT-1)
	// Then: Descending BarrierProximity
	// Then: Descending ContractScore
	// Deterministic tie-breaker: candidate.Less
	sort.Slice(tier1, func(i, j int) bool {
		li, lj := contractTierLevel(tier1[i].contract), contractTierLevel(tier1[j].contract)
		if li != lj {
			return li < lj
		}
		if tier1[i].feat.BarrierProximity != tier1[j].feat.BarrierProximity {
			return tier1[i].feat.BarrierProximity > tier1[j].feat.BarrierProximity
		}
		if tier1[i].contract.ContractScore != tier1[j].contract.ContractScore {
			return tier1[i].contract.ContractScore > tier1[j].contract.ContractScore
		}
		return candidate.Less(tier1[i].cand, tier1[j].cand)
	})

	// In Tier 2 (ContractScore == 0), separate boundary-adjacent representatives (BarrierProximity >= 1.0 or BeforeBoundary)
	// from non-boundary representatives.
	var tier2Barrier []repInfo
	var tier2NonBarrier []repInfo

	for _, rep := range tier2 {
		if rep.feat.BarrierProximity >= 1.0 || rep.feat.BeforeBoundary {
			tier2Barrier = append(tier2Barrier, rep)
		} else {
			tier2NonBarrier = append(tier2NonBarrier, rep)
		}
	}

	// Diversity-aware interleaving across distinct barrier categories:
	// Manifest (Tier 4), Dir (Tier 2), Flush (Tier 1), WAL (Tier 3), Data (Tier 0).
	categoryOrder := []int{graph.TierManifest, graph.TierDir, graph.TierFlush, graph.TierWAL, graph.TierData}
	buckets := make(map[int][]repInfo)
	for _, rep := range tier2Barrier {
		tier := rep.feat.SemanticTier
		buckets[tier] = append(buckets[tier], rep)
	}

	// Sort inside each bucket by BarrierProximity DESC, RiskScore DESC, then candidate.Less
	for tier := range buckets {
		b := buckets[tier]
		sort.Slice(b, func(i, j int) bool {
			if b[i].feat.BarrierProximity != b[j].feat.BarrierProximity {
				return b[i].feat.BarrierProximity > b[j].feat.BarrierProximity
			}
			if b[i].feat.RiskScore != b[j].feat.RiskScore {
				return b[i].feat.RiskScore > b[j].feat.RiskScore
			}
			return candidate.Less(b[i].cand, b[j].cand)
		})
		buckets[tier] = b
	}

	bucketIndices := make(map[int]int)
	var tier2Interleaved []repInfo
	remaining := len(tier2Barrier)

	for remaining > 0 {
		for _, cat := range categoryOrder {
			idx := bucketIndices[cat]
			b := buckets[cat]
			if idx < len(b) {
				tier2Interleaved = append(tier2Interleaved, b[idx])
				bucketIndices[cat] = idx + 1
				remaining--
			}
		}
	}

	// Sort non-boundary Tier 2 representatives by BarrierProximity DESC, then RiskScore DESC, then candidate.Less
	sort.Slice(tier2NonBarrier, func(i, j int) bool {
		if tier2NonBarrier[i].feat.BarrierProximity != tier2NonBarrier[j].feat.BarrierProximity {
			return tier2NonBarrier[i].feat.BarrierProximity > tier2NonBarrier[j].feat.BarrierProximity
		}
		if tier2NonBarrier[i].feat.RiskScore != tier2NonBarrier[j].feat.RiskScore {
			return tier2NonBarrier[i].feat.RiskScore > tier2NonBarrier[j].feat.RiskScore
		}
		return candidate.Less(tier2NonBarrier[i].cand, tier2NonBarrier[j].cand)
	})

	var tier2Sorted []repInfo
	tier2Sorted = append(tier2Sorted, tier2Interleaved...)
	tier2Sorted = append(tier2Sorted, tier2NonBarrier...)

	// Tier 3: Partition non-representatives into Tier 3A (violating) and Tier 3B (clean).
	var tier3Violating []repInfo
	var tier3Clean []repInfo

	for _, nonRep := range zeroNonReps {
		if nonRep.contract.ContractScore > 0 {
			tier3Violating = append(tier3Violating, nonRep)
		} else {
			tier3Clean = append(tier3Clean, nonRep)
		}
	}

	// Sort Tier 3 Violating: contractTierLevel ASC, BarrierProximity DESC, ContractScore DESC, candidate.Less
	sort.Slice(tier3Violating, func(i, j int) bool {
		li, lj := contractTierLevel(tier3Violating[i].contract), contractTierLevel(tier3Violating[j].contract)
		if li != lj {
			return li < lj
		}
		if tier3Violating[i].feat.BarrierProximity != tier3Violating[j].feat.BarrierProximity {
			return tier3Violating[i].feat.BarrierProximity > tier3Violating[j].feat.BarrierProximity
		}
		if tier3Violating[i].contract.ContractScore != tier3Violating[j].contract.ContractScore {
			return tier3Violating[i].contract.ContractScore > tier3Violating[j].contract.ContractScore
		}
		return candidate.Less(tier3Violating[i].cand, tier3Violating[j].cand)
	})

	// Sort Tier 3 Clean: BarrierProximity DESC, RiskScore DESC, candidate.Less
	sort.Slice(tier3Clean, func(i, j int) bool {
		if tier3Clean[i].feat.BarrierProximity != tier3Clean[j].feat.BarrierProximity {
			return tier3Clean[i].feat.BarrierProximity > tier3Clean[j].feat.BarrierProximity
		}
		if tier3Clean[i].feat.RiskScore != tier3Clean[j].feat.RiskScore {
			return tier3Clean[i].feat.RiskScore > tier3Clean[j].feat.RiskScore
		}
		return candidate.Less(tier3Clean[i].cand, tier3Clean[j].cand)
	})

	// Tier 4: 100% retention candidates in canonical order.
	candidate.Sort(u100)

	// 6. Concatenate tiers:
	ranking := make([]candidate.Candidate, 0, len(ordered))
	for _, r := range tier1 {
		ranking = append(ranking, r.cand)
	}
	for _, r := range tier2Sorted {
		ranking = append(ranking, r.cand)
	}
	for _, r := range tier3Violating {
		ranking = append(ranking, r.cand)
	}
	for _, r := range tier3Clean {
		ranking = append(ranking, r.cand)
	}
	ranking = append(ranking, u100...)

	// 7. Strictly validate permutation bijection.
	if err := ValidatePermutation(universe, ranking); err != nil {
		panic(fmt.Sprintf("strategy: dcg permutation invalid: %v", err))
	}

	return ranking
}

func contractTierLevel(c graph.ContractFeatures) int {
	if c.DanglingTableCount > 0 {
		return 1
	}
	if c.UnsyncedDirectoryRename {
		return 2
	}
	if c.AckSyncMissing {
		return 3
	}
	return 4
}

func isDCGBetterRep(fA, fB graph.Features, cA, cB graph.ContractFeatures, candA, candB candidate.Candidate) bool {
	lA, lB := contractTierLevel(cA), contractTierLevel(cB)
	if lA != lB {
		return lA < lB
	}
	if cA.ContractScore != cB.ContractScore {
		return cA.ContractScore > cB.ContractScore
	}
	if fA.BeforeBoundary != fB.BeforeBoundary {
		return fA.BeforeBoundary
	}
	if fA.BarrierProximity != fB.BarrierProximity {
		return fA.BarrierProximity > fB.BarrierProximity
	}
	if fA.RiskScore != fB.RiskScore {
		return fA.RiskScore > fB.RiskScore
	}
	return candidate.Less(candA, candB)
}

// --- DCG-FlagOnly: Durability-Contract Flag Ablation Strategy ------------

type dcgFlagOnly struct {
	seed uint64
}

// NewDCGFlagOnly creates the DCG-FlagOnly ablation strategy:
// It collapses the U_0 candidate universe into canonical persistence state representatives,
// partitions representatives into Tier 1 (ContractScore > 0) and Tier 2 (ContractScore == 0),
// and shuffles randomly within tiers using independent deterministic PCG generators:
// - Tier 1 reps shuffled with rand.NewPCG(0, s.seed)
// - Tier 2 reps shuffled with rand.NewPCG(1, s.seed)
// - Non-reps (Tier 3) shuffled with rand.NewPCG(2, s.seed)
// - 100% retention (Tier 4) shuffled with rand.NewPCG(3, s.seed)
// Concatenates Tier 1 + Tier 2 + Tier 3 + Tier 4 and validates permutation bijection.
func NewDCGFlagOnly(seed uint64) Strategy {
	return dcgFlagOnly{seed: seed}
}

func (dcgFlagOnly) Name() string { return "dcg-flag-only" }

func (s dcgFlagOnly) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if len(ordered) <= 1 {
		return ordered
	}
	if view == nil {
		return ordered
	}
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}

	var contractMap map[uint64]graph.ContractFeatures
	if len(view.Events) > 0 {
		contractMap = graph.ComputeContractFeaturesWithWeights(view.Events, DefaultDCGWeights)
	}

	var u0 []candidate.Candidate
	var u0Labels []graph.Features
	var u0Contract []graph.ContractFeatures
	var u100 []candidate.Candidate

	for i, c := range ordered {
		if c.Percent == 0 {
			u0 = append(u0, c)
			u0Labels = append(u0Labels, labels[i])
			if contractMap != nil {
				u0Contract = append(u0Contract, contractMap[c.Event])
			} else {
				u0Contract = append(u0Contract, graph.ContractFeatures{})
			}
		} else {
			u100 = append(u100, c)
		}
	}

	type stateGroup struct {
		candidates []candidate.Candidate
		feats      []graph.Features
		contracts  []graph.ContractFeatures
	}
	groupIndex := make(map[string]int)
	var groups []stateGroup

	for i, c := range u0 {
		key := u0Labels[i].StateKey
		idx, found := groupIndex[key]
		if !found {
			idx = len(groups)
			groupIndex[key] = idx
			groups = append(groups, stateGroup{})
		}
		groups[idx].candidates = append(groups[idx].candidates, c)
		groups[idx].feats = append(groups[idx].feats, u0Labels[i])
		groups[idx].contracts = append(groups[idx].contracts, u0Contract[i])
	}

	var tier1Reps []candidate.Candidate
	var tier2Reps []candidate.Candidate
	var zeroNonReps []candidate.Candidate

	for _, g := range groups {
		bestIdx := 0
		for j := 1; j < len(g.candidates); j++ {
			if isDCGBetterRep(g.feats[j], g.feats[bestIdx], g.contracts[j], g.contracts[bestIdx], g.candidates[j], g.candidates[bestIdx]) {
				bestIdx = j
			}
		}
		repCand := g.candidates[bestIdx]
		if g.contracts[bestIdx].ContractScore > 0 {
			tier1Reps = append(tier1Reps, repCand)
		} else {
			tier2Reps = append(tier2Reps, repCand)
		}
		for j := 0; j < len(g.candidates); j++ {
			if j != bestIdx {
				zeroNonReps = append(zeroNonReps, g.candidates[j])
			}
		}
	}

	// Ensure canonical sorting before shuffling for determinism
	candidate.Sort(tier1Reps)
	candidate.Sort(tier2Reps)
	candidate.Sort(zeroNonReps)
	candidate.Sort(u100)

	rng0 := rand.New(rand.NewPCG(0, s.seed))
	rng0.Shuffle(len(tier1Reps), func(i, j int) { tier1Reps[i], tier1Reps[j] = tier1Reps[j], tier1Reps[i] })

	rng1 := rand.New(rand.NewPCG(1, s.seed))
	rng1.Shuffle(len(tier2Reps), func(i, j int) { tier2Reps[i], tier2Reps[j] = tier2Reps[j], tier2Reps[i] })

	rng2 := rand.New(rand.NewPCG(2, s.seed))
	rng2.Shuffle(len(zeroNonReps), func(i, j int) { zeroNonReps[i], zeroNonReps[j] = zeroNonReps[j], zeroNonReps[i] })

	rng3 := rand.New(rand.NewPCG(3, s.seed))
	rng3.Shuffle(len(u100), func(i, j int) { u100[i], u100[j] = u100[j], u100[i] })

	result := make([]candidate.Candidate, 0, len(ordered))
	result = append(result, tier1Reps...)
	result = append(result, tier2Reps...)
	result = append(result, zeroNonReps...)
	result = append(result, u100...)

	if err := ValidatePermutation(universe, result); err != nil {
		panic(fmt.Sprintf("strategy: dcg-flag-only permutation invalid: %v", err))
	}
	return result
}
