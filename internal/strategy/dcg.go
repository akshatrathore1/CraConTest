package strategy

import (
	"math"
	"math/rand/v2"
	"sort"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
)

// DCGWeights configures term weights for DCG ranking.
type DCGWeights = graph.DCGWeights

// DefaultDCGWeights returns the standard term weights (struct copy).
func DefaultDCGWeights() DCGWeights { return graph.DefaultDCGWeights() }

type dcg struct {
	weights DCGWeights
}

func sanitizeDCGWeights(weights ...DCGWeights) DCGWeights {
	def := DefaultDCGWeights()
	w := def
	if len(weights) > 0 {
		w = weights[0]
	}
	if math.IsNaN(w.WALWeight) || math.IsInf(w.WALWeight, 0) || w.WALWeight < 0 {
		w.WALWeight = def.WALWeight
	}
	if math.IsNaN(w.ManifestWeight) || math.IsInf(w.ManifestWeight, 0) || w.ManifestWeight < 0 {
		w.ManifestWeight = def.ManifestWeight
	}
	if math.IsNaN(w.RenameWeight) || math.IsInf(w.RenameWeight, 0) || w.RenameWeight < 0 {
		w.RenameWeight = def.RenameWeight
	}
	return w
}

// NewDCG creates a new Durability-Contract Guided ranking strategy.
// Optional custom weights can be supplied.
func NewDCG(weights ...DCGWeights) Strategy {
	return dcg{weights: sanitizeDCGWeights(weights...)}
}

func (dcg) Name() string { return "dcg" }

// Rank implements Durability-Contract Guided candidate ranking:
//  1. Partitions candidate universe by (StateKey, Percent).
//  2. Isolates 0% retention canonical state representatives.
//  3. Tier 1: 0% retention state representatives with contract violations
//     (ContractViolationMask != 0 || ContractScore > 0), ranked by
//     contractTierLevel, BarrierProximity, ContractScore.
//  4. Tier 2: 0% retention state representatives without contract violations,
//     ranked by BarrierProximity and reverse risk horizon (RiskScore) with
//     semantic barrier interleaving.
//  5. Tier 3A: Remaining 0% retention violating non-representatives.
//  6. Tier 3B: Remaining 0% retention clean non-representatives.
//  7. Tier 4: Non-zero retention candidates sorted by retention percent ascending,
//     then in canonical order.
//
// Strictly validates that the returned slice is a bijection of universe (ValidatePermutation).
func (s dcg) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if len(ordered) == 0 {
		return ordered
	}
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}
	if len(ordered) == 1 {
		return ordered
	}

	contractMap := contractFeaturesForView(view, s.weights)

	// Separate into 0% retention pool (u0) and non-zero retention pool (uNonZero).
	var u0 []dcgCandidate
	var uNonZero []candidate.Candidate

	for i, c := range ordered {
		if c.Percent == 0 {
			var cf graph.ContractFeatures
			if contractMap != nil {
				cf = contractMap[c.Event]
			}
			u0 = append(u0, dcgCandidate{cand: c, feat: labels[i], contract: cf})
		} else {
			uNonZero = append(uNonZero, c)
		}
	}

	repsRanked, nonRepsRanked := rankDCGZeroPool(u0)

	// Tier 4: Non-zero retention candidates (uNonZero) sorted by retention percent ascending, then canonically.
	sort.Slice(uNonZero, func(i, j int) bool {
		if uNonZero[i].Percent != uNonZero[j].Percent {
			return uNonZero[i].Percent < uNonZero[j].Percent
		}
		return candidate.Less(uNonZero[i], uNonZero[j])
	})

	ranking := make([]candidate.Candidate, 0, len(ordered))
	ranking = append(ranking, repsRanked...)
	ranking = append(ranking, nonRepsRanked...)
	ranking = append(ranking, uNonZero...)

	if err := ValidatePermutation(universe, ranking); err != nil {
		return nil
	}
	return ranking
}

type dcgCandidate struct {
	cand     candidate.Candidate
	feat     graph.Features
	contract graph.ContractFeatures
}

func contractFeaturesForView(view *graph.View, weights DCGWeights) map[uint64]graph.ContractFeatures {
	if view == nil || len(view.Events) == 0 {
		return nil
	}
	if weights == DefaultDCGWeights() {
		return view.AllContractFeatures()
	}
	return graph.ComputeContractFeaturesWithWeights(view.Events, weights)
}

func hasContractViolation(c graph.ContractFeatures) bool {
	return c.ContractViolationMask != 0 || c.ContractScore > 0
}

func sortDCGViolating(items []dcgCandidate) {
	sort.Slice(items, func(i, j int) bool {
		li, lj := contractTierLevel(items[i].contract), contractTierLevel(items[j].contract)
		if li != lj {
			return li < lj
		}
		if dec, gt := cmpFloatDesc(items[i].feat.BarrierProximity, items[j].feat.BarrierProximity); dec {
			return gt
		}
		if dec, gt := cmpFloatDesc(items[i].contract.ContractScore, items[j].contract.ContractScore); dec {
			return gt
		}
		return candidate.Less(items[i].cand, items[j].cand)
	})
}

func sortDCGClean(items []dcgCandidate) {
	sort.Slice(items, func(i, j int) bool {
		if dec, gt := cmpFloatDesc(items[i].feat.BarrierProximity, items[j].feat.BarrierProximity); dec {
			return gt
		}
		if dec, gt := cmpFloatDesc(items[i].feat.RiskScore, items[j].feat.RiskScore); dec {
			return gt
		}
		return candidate.Less(items[i].cand, items[j].cand)
	})
}

func rankDCGRepresentatives(reps []dcgCandidate) []candidate.Candidate {
	var tier1 []dcgCandidate
	var tier2 []dcgCandidate

	for _, rep := range reps {
		if hasContractViolation(rep.contract) {
			tier1 = append(tier1, rep)
		} else {
			tier2 = append(tier2, rep)
		}
	}

	sortDCGViolating(tier1)

	var tier2Barrier []dcgCandidate
	var tier2NonBarrier []dcgCandidate
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
	buckets := make(map[int][]dcgCandidate)
	for _, rep := range tier2Barrier {
		buckets[rep.feat.SemanticTier] = append(buckets[rep.feat.SemanticTier], rep)
	}
	for tier, b := range buckets {
		sortDCGClean(b)
		buckets[tier] = b
	}

	categorySet := make(map[int]bool, len(categoryOrder))
	for _, cat := range categoryOrder {
		categorySet[cat] = true
	}

	var unclassifiedTiers []int
	for tier := range buckets {
		if !categorySet[tier] {
			unclassifiedTiers = append(unclassifiedTiers, tier)
		}
	}
	sort.Ints(unclassifiedTiers)

	tier2Interleaved := make([]dcgCandidate, 0, len(tier2Barrier))
	remaining := len(tier2Barrier)
	for _, tier := range unclassifiedTiers {
		b := buckets[tier]
		tier2Interleaved = append(tier2Interleaved, b...)
		remaining -= len(b)
	}

	bucketIndices := make(map[int]int, len(categoryOrder))
	for remaining > 0 {
		progress := false
		for _, cat := range categoryOrder {
			idx := bucketIndices[cat]
			b := buckets[cat]
			if idx < len(b) {
				tier2Interleaved = append(tier2Interleaved, b[idx])
				bucketIndices[cat] = idx + 1
				remaining--
				progress = true
			}
		}
		if !progress {
			break
		}
	}

	sortDCGClean(tier2NonBarrier)

	ranked := make([]candidate.Candidate, 0, len(reps))
	for _, r := range tier1 {
		ranked = append(ranked, r.cand)
	}
	for _, r := range tier2Interleaved {
		ranked = append(ranked, r.cand)
	}
	for _, r := range tier2NonBarrier {
		ranked = append(ranked, r.cand)
	}
	return ranked
}

func rankDCGZeroPool(u0 []dcgCandidate) (repsRanked []candidate.Candidate, nonRepsRanked []candidate.Candidate) {
	if len(u0) == 0 {
		return nil, nil
	}

	groupIndex := make(map[string]int)
	var groups [][]dcgCandidate

	for _, item := range u0 {
		key := item.feat.StateKey
		idx, found := groupIndex[key]
		if !found {
			idx = len(groups)
			groupIndex[key] = idx
			groups = append(groups, nil)
		}
		groups[idx] = append(groups[idx], item)
	}

	zeroReps := make([]dcgCandidate, 0, len(groups))
	var zeroNonReps []dcgCandidate

	for _, g := range groups {
		bestIdx := 0
		for j := 1; j < len(g); j++ {
			if isDCGBetterRep(g[j].feat, g[bestIdx].feat, g[j].contract, g[bestIdx].contract, g[j].cand, g[bestIdx].cand) {
				bestIdx = j
			}
		}
		zeroReps = append(zeroReps, g[bestIdx])
		for j := 0; j < len(g); j++ {
			if j != bestIdx {
				zeroNonReps = append(zeroNonReps, g[j])
			}
		}
	}

	repsRanked = rankDCGRepresentatives(zeroReps)

	var tier3Violating []dcgCandidate
	var tier3Clean []dcgCandidate
	for _, nonRep := range zeroNonReps {
		if hasContractViolation(nonRep.contract) {
			tier3Violating = append(tier3Violating, nonRep)
		} else {
			tier3Clean = append(tier3Clean, nonRep)
		}
	}
	sortDCGViolating(tier3Violating)
	sortDCGClean(tier3Clean)

	nonRepsRanked = make([]candidate.Candidate, 0, len(zeroNonReps))
	for _, r := range tier3Violating {
		nonRepsRanked = append(nonRepsRanked, r.cand)
	}
	for _, r := range tier3Clean {
		nonRepsRanked = append(nonRepsRanked, r.cand)
	}
	return repsRanked, nonRepsRanked
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
	if dec, gt := cmpFloatDesc(cA.ContractScore, cB.ContractScore); dec {
		return gt
	}
	if fA.BeforeBoundary != fB.BeforeBoundary {
		return fA.BeforeBoundary
	}
	if dec, gt := cmpFloatDesc(fA.BarrierProximity, fB.BarrierProximity); dec {
		return gt
	}
	if dec, gt := cmpFloatDesc(fA.RiskScore, fB.RiskScore); dec {
		return gt
	}
	return candidate.Less(candA, candB)
}

// --- DCG-FlagOnly: Durability-Contract Flag Ablation Strategy ------------

type dcgFlagOnly struct {
	seed uint64
}

// NewDCGFlagOnly creates the DCG-FlagOnly ablation strategy:
// It collapses the U_0 candidate universe into canonical persistence state representatives,
// partitions representatives into Tier 1 (contract violation) and Tier 2 (clean),
// and shuffles randomly within tiers using independent deterministic PCG generators:
// - Tier 1 reps shuffled with rand.NewPCG(0, s.seed)
// - Tier 2 reps shuffled with rand.NewPCG(1, s.seed)
// - Non-reps (Tier 3) shuffled with rand.NewPCG(2, s.seed)
// - Partial retention (0 < Percent < 100, Tier 4A) shuffled with rand.NewPCG(4, s.seed)
// - Remaining non-zero / 100% retention (Tier 4B) shuffled with rand.NewPCG(3, s.seed)
// Concatenates Tier 1 + Tier 2 + Tier 3 + Tier 4A + Tier 4B and validates permutation bijection.
func NewDCGFlagOnly(seed uint64) Strategy {
	return dcgFlagOnly{seed: seed}
}

func (dcgFlagOnly) Name() string { return "dcg-flag-only" }

func (s dcgFlagOnly) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if len(ordered) == 0 {
		return ordered
	}
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}
	if len(ordered) == 1 {
		return ordered
	}

	contractMap := contractFeaturesForView(view, DefaultDCGWeights())

	var u0 []dcgCandidate
	var uMid []candidate.Candidate
	var u100 []candidate.Candidate

	for i, c := range ordered {
		if c.Percent == 0 {
			var cf graph.ContractFeatures
			if contractMap != nil {
				cf = contractMap[c.Event]
			}
			u0 = append(u0, dcgCandidate{cand: c, feat: labels[i], contract: cf})
		} else if c.Percent > 0 && c.Percent < 100 {
			uMid = append(uMid, c)
		} else {
			u100 = append(u100, c)
		}
	}

	groupIndex := make(map[string]int)
	var groups [][]dcgCandidate

	for _, item := range u0 {
		key := item.feat.StateKey
		idx, found := groupIndex[key]
		if !found {
			idx = len(groups)
			groupIndex[key] = idx
			groups = append(groups, nil)
		}
		groups[idx] = append(groups[idx], item)
	}

	var tier1Reps []candidate.Candidate
	var tier2Reps []candidate.Candidate
	var zeroNonReps []candidate.Candidate

	for _, g := range groups {
		bestIdx := 0
		for j := 1; j < len(g); j++ {
			if isDCGBetterRep(g[j].feat, g[bestIdx].feat, g[j].contract, g[bestIdx].contract, g[j].cand, g[bestIdx].cand) {
				bestIdx = j
			}
		}
		rep := g[bestIdx]
		if hasContractViolation(rep.contract) {
			tier1Reps = append(tier1Reps, rep.cand)
		} else {
			tier2Reps = append(tier2Reps, rep.cand)
		}
		for j := 0; j < len(g); j++ {
			if j != bestIdx {
				zeroNonReps = append(zeroNonReps, g[j].cand)
			}
		}
	}

	// Ensure canonical sorting before shuffling for determinism.
	// uMid and u100 were appended from ordered (canonical) and are already sorted.
	candidate.Sort(tier1Reps)
	candidate.Sort(tier2Reps)
	candidate.Sort(zeroNonReps)

	rng0 := rand.New(rand.NewPCG(0, s.seed))
	rng0.Shuffle(len(tier1Reps), func(i, j int) { tier1Reps[i], tier1Reps[j] = tier1Reps[j], tier1Reps[i] })

	rng1 := rand.New(rand.NewPCG(1, s.seed))
	rng1.Shuffle(len(tier2Reps), func(i, j int) { tier2Reps[i], tier2Reps[j] = tier2Reps[j], tier2Reps[i] })

	rng2 := rand.New(rand.NewPCG(2, s.seed))
	rng2.Shuffle(len(zeroNonReps), func(i, j int) { zeroNonReps[i], zeroNonReps[j] = zeroNonReps[j], zeroNonReps[i] })

	if len(uMid) > 0 {
		rngMid := rand.New(rand.NewPCG(4, s.seed))
		rngMid.Shuffle(len(uMid), func(i, j int) { uMid[i], uMid[j] = uMid[j], uMid[i] })
	}

	rng3 := rand.New(rand.NewPCG(3, s.seed))
	rng3.Shuffle(len(u100), func(i, j int) { u100[i], u100[j] = u100[j], u100[i] })

	result := make([]candidate.Candidate, 0, len(ordered))
	result = append(result, tier1Reps...)
	result = append(result, tier2Reps...)
	result = append(result, zeroNonReps...)
	result = append(result, uMid...)
	result = append(result, u100...)

	if err := ValidatePermutation(universe, result); err != nil {
		return nil
	}
	return result
}

// --- DCGPartial: post-hoc partial-retention prior (dcg-pr) -------------------

type dcgPartial struct {
	weights DCGWeights
}

// NewDCGPartial creates the post-hoc partial-retention prior strategy (dcg-pr).
// When the universe contains only 0% and 100% candidates, it produces the same
// ranking as dcg. When intermediate-retention candidates (0 < Percent < 100) are
// present, it places representatives whose cutoff has UnsyncedDirEntryCount >= 2
// at the front of Tier 1, followed by standard dcg 0% Tier 1, then 0% Tier 2,
// then remaining intermediate-retention representatives (ranked by contract
// tier/proximity/risk), then non-representatives, then 100% candidates.
func NewDCGPartial(weights ...DCGWeights) Strategy {
	return dcgPartial{weights: sanitizeDCGWeights(weights...)}
}

func (dcgPartial) Name() string { return "dcg-pr" }

func (s dcgPartial) Rank(view *graph.View, universe []candidate.Candidate) []candidate.Candidate {
	ordered := canonical(universe)
	if len(ordered) == 0 {
		return ordered
	}
	labels, ok := features(view, ordered)
	if !ok {
		return nil
	}
	if len(ordered) == 1 {
		return ordered
	}

	contractMap := contractFeaturesForView(view, s.weights)

	var u0 []dcgCandidate
	var uMid []dcgCandidate
	var u100 []candidate.Candidate

	for i, c := range ordered {
		var cf graph.ContractFeatures
		if contractMap != nil {
			cf = contractMap[c.Event]
		}
		if c.Percent == 0 {
			u0 = append(u0, dcgCandidate{cand: c, feat: labels[i], contract: cf})
		} else if c.Percent > 0 && c.Percent < 100 {
			uMid = append(uMid, dcgCandidate{cand: c, feat: labels[i], contract: cf})
		} else {
			u100 = append(u100, c)
		}
	}

	dcg0Reps, dcg0NonReps := rankDCGZeroPool(u0)
	sort.Slice(u100, func(i, j int) bool {
		if u100[i].Percent != u100[j].Percent {
			return u100[i].Percent < u100[j].Percent
		}
		return candidate.Less(u100[i], u100[j])
	})

	// If no intermediate candidates, match standard dcg ranking without recomputing contracts.
	if len(uMid) == 0 {
		ranking := make([]candidate.Candidate, 0, len(ordered))
		ranking = append(ranking, dcg0Reps...)
		ranking = append(ranking, dcg0NonReps...)
		ranking = append(ranking, u100...)
		if err := ValidatePermutation(universe, ranking); err != nil {
			return nil
		}
		return ranking
	}

	// Group intermediate-retention candidates by (StateKey, Percent) and select one
	// canonical representative per group (preferring higher UnsyncedDirEntryCount when >= 2,
	// then isDCGBetterRep).
	midGroupIndex := make(map[statePercentKey]int)
	var midGroups [][]dcgCandidate

	for _, item := range uMid {
		key := statePercentKey{state: item.feat.StateKey, percent: item.cand.Percent}
		idx, found := midGroupIndex[key]
		if !found {
			idx = len(midGroups)
			midGroupIndex[key] = idx
			midGroups = append(midGroups, nil)
		}
		midGroups[idx] = append(midGroups[idx], item)
	}

	var midRepsHighUnsynced []dcgCandidate // UnsyncedDirEntryCount >= 2
	var midRepsOther []dcgCandidate        // other intermediate reps
	var midNonReps []candidate.Candidate

	for _, g := range midGroups {
		bestIdx := 0
		for j := 1; j < len(g); j++ {
			aHigh := g[j].contract.UnsyncedDirEntryCount >= 2
			bHigh := g[bestIdx].contract.UnsyncedDirEntryCount >= 2
			if aHigh != bHigh {
				if aHigh {
					bestIdx = j
				}
				continue
			}
			if aHigh && bHigh && g[j].contract.UnsyncedDirEntryCount != g[bestIdx].contract.UnsyncedDirEntryCount {
				if g[j].contract.UnsyncedDirEntryCount > g[bestIdx].contract.UnsyncedDirEntryCount {
					bestIdx = j
				}
				continue
			}
			if isDCGBetterRep(g[j].feat, g[bestIdx].feat, g[j].contract, g[bestIdx].contract, g[j].cand, g[bestIdx].cand) {
				bestIdx = j
			}
		}
		rep := g[bestIdx]
		if rep.contract.UnsyncedDirEntryCount >= 2 {
			midRepsHighUnsynced = append(midRepsHighUnsynced, rep)
		} else {
			midRepsOther = append(midRepsOther, rep)
		}
		for j := 0; j < len(g); j++ {
			if j != bestIdx {
				midNonReps = append(midNonReps, g[j].cand)
			}
		}
	}

	// Sort midRepsHighUnsynced: BeforeBoundary DESC, BarrierProximity DESC,
	// UnsyncedDirEntryCount DESC, SemanticTier DESC, RiskScore DESC, candidate.Less.
	sort.Slice(midRepsHighUnsynced, func(i, j int) bool {
		a, b := midRepsHighUnsynced[i], midRepsHighUnsynced[j]
		if a.feat.BeforeBoundary != b.feat.BeforeBoundary {
			return a.feat.BeforeBoundary
		}
		if dec, gt := cmpFloatDesc(a.feat.BarrierProximity, b.feat.BarrierProximity); dec {
			return gt
		}
		if a.contract.UnsyncedDirEntryCount != b.contract.UnsyncedDirEntryCount {
			return a.contract.UnsyncedDirEntryCount > b.contract.UnsyncedDirEntryCount
		}
		if a.feat.SemanticTier != b.feat.SemanticTier {
			return a.feat.SemanticTier > b.feat.SemanticTier
		}
		if dec, gt := cmpFloatDesc(a.feat.RiskScore, b.feat.RiskScore); dec {
			return gt
		}
		return candidate.Less(a.cand, b.cand)
	})

	midRepsOtherRanked := rankDCGRepresentatives(midRepsOther)
	candidate.Sort(midNonReps)

	// Assemble final ranking:
	// 1. Intermediate reps with UnsyncedDirEntryCount >= 2 (front of Tier 1)
	// 2. Standard 0% dcg Tier 1+2 ranking (0% representatives)
	// 3. Remaining intermediate-retention representatives (midRepsOther)
	// 4. Non-representatives: 0% non-reps (dcg0NonReps) followed by intermediate non-reps (midNonReps)
	// 5. 100% candidates
	ranking := make([]candidate.Candidate, 0, len(ordered))
	for _, r := range midRepsHighUnsynced {
		ranking = append(ranking, r.cand)
	}
	ranking = append(ranking, dcg0Reps...)
	ranking = append(ranking, midRepsOtherRanked...)
	ranking = append(ranking, dcg0NonReps...)
	ranking = append(ranking, midNonReps...)
	ranking = append(ranking, u100...)

	if err := ValidatePermutation(universe, ranking); err != nil {
		return nil
	}
	return ranking
}
