package exp

import (
	"fmt"
	"math"
	"sort"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
)

// FailureKind returns the normalized deduplication key of a failed outcome:
// the violation signature, or the normalized v1 signature for open errors,
// panics, and hangs. Valid and oracle_error outcomes have no failure kind.
func FailureKind(o oracle.Outcome) string {
	switch o.Class {
	case oracle.ClassViolation:
		return o.Signature
	case oracle.ClassOpenError, oracle.ClassPanic, oracle.ClassHang:
		return NormalizeOutcome(o).Signature
	default:
		return ""
	}
}

// Positions reports where a ranking first exposes failures. Positions are
// one-based indices into the ranking, or zero when never reached.
type Positions struct {
	// FirstFailure is the position of the first violation, open error,
	// panic or hang.
	FirstFailure int
	// AllKinds is the position at which every failure kind of the table
	// has appeared at least once.
	AllKinds int
	// Kinds is the number of distinct failure kinds in the table.
	Kinds int
}

func kindTable(universe []candidate.Candidate, outcomes []oracle.Outcome) (map[candidate.Candidate]string, int, error) {
	if err := ValidateTable(universe, outcomes); err != nil {
		return nil, 0, err
	}
	kinds := make(map[candidate.Candidate]string)
	distinct := make(map[string]bool)
	for _, o := range outcomes {
		if kind := FailureKind(o); kind != "" {
			kinds[o.Candidate] = kind
			distinct[kind] = true
		}
	}
	return kinds, len(distinct), nil
}

func positionsIn(ranking []candidate.Candidate, kinds map[candidate.Candidate]string, total int) Positions {
	result := Positions{Kinds: total}
	seen := make(map[string]bool)
	for index, c := range ranking {
		kind, failed := kinds[c]
		if !failed {
			continue
		}
		if result.FirstFailure == 0 {
			result.FirstFailure = index + 1
		}
		seen[kind] = true
		if len(seen) == total {
			result.AllKinds = index + 1
			break
		}
	}
	return result
}

// DiscoveryPositions measures one ranking against a stored outcome table.
// It never executes a candidate.
func DiscoveryPositions(universe, ranking []candidate.Candidate, outcomes []oracle.Outcome) (Positions, error) {
	kinds, total, err := kindTable(universe, outcomes)
	if err != nil {
		return Positions{}, err
	}
	if err := strategy.ValidatePermutation(universe, ranking); err != nil {
		return Positions{}, err
	}
	return positionsIn(ranking, kinds, total), nil
}

// RandomPositionSummary aggregates DiscoveryPositions over random orders.
type RandomPositionSummary struct {
	Seeds                                    int
	FirstMedian, FirstP10, FirstP90          int
	AllKindsMedian, AllKindsP10, AllKindsP90 int
	Kinds                                    int
}

// RandomSeeds are the fixed ranking seeds used for the random-order
// distribution of discovery positions.
func RandomSeeds() []uint64 {
	seeds := make([]uint64, 1000)
	for i := range seeds {
		seeds[i] = 10000 + uint64(i)
	}
	return seeds
}

// RandomPositions measures the random strategy under every seed and
// reports the median and the 10th and 90th percentiles (nearest rank).
func RandomPositions(universe []candidate.Candidate, outcomes []oracle.Outcome, seeds []uint64) (RandomPositionSummary, error) {
	kinds, total, err := kindTable(universe, outcomes)
	if err != nil {
		return RandomPositionSummary{}, err
	}
	if len(seeds) == 0 {
		return RandomPositionSummary{}, fmt.Errorf("positions: no seeds")
	}
	firsts := make([]int, 0, len(seeds))
	alls := make([]int, 0, len(seeds))
	for _, seed := range seeds {
		p := positionsIn(strategy.NewRandom(seed).Rank(nil, universe), kinds, total)
		firsts = append(firsts, p.FirstFailure)
		alls = append(alls, p.AllKinds)
	}
	sort.Ints(firsts)
	sort.Ints(alls)
	at := func(xs []int, q float64) int {
		if len(xs) == 0 {
			return 0
		}
		if q <= 0 {
			return xs[0]
		}
		if q >= 1 {
			return xs[len(xs)-1]
		}
		idx := int(math.Round(q * float64(len(xs)-1)))
		if idx >= len(xs) {
			idx = len(xs) - 1
		}
		return xs[idx]
	}
	return RandomPositionSummary{
		Seeds:          len(seeds),
		FirstMedian:    at(firsts, 0.5),
		FirstP10:       at(firsts, 0.1),
		FirstP90:       at(firsts, 0.9),
		AllKindsMedian: at(alls, 0.5),
		AllKindsP10:    at(alls, 0.1),
		AllKindsP90:    at(alls, 0.9),
		Kinds:          total,
	}, nil
}
