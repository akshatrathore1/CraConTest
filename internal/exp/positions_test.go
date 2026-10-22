package exp_test

import (
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
)

func positionsTable() ([]candidate.Candidate, []oracle.Outcome) {
	universe := []candidate.Candidate{{Event: 1}, {Event: 2}, {Event: 3}, {Event: 4}, {Event: 5}, {Event: 6}}
	outcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: oracle.ClassValid},
		{Candidate: universe[1], Class: oracle.ClassOpenError, Detail: `malformed manifest file "MANIFEST-000005"`},
		{Candidate: universe[2], Class: oracle.ClassViolation, Rule: "prefix", Signature: "v1:a"},
		{Candidate: universe[3], Class: oracle.ClassOpenError, Detail: `malformed manifest file "MANIFEST-000017"`},
		{Candidate: universe[4], Class: oracle.ClassViolation, Rule: "prefix", Signature: "v1:b"},
		{Candidate: universe[5], Class: oracle.ClassValid},
	}
	return universe, outcomes
}

func TestFailureKindNormalizesNumbers(t *testing.T) {
	_, outcomes := positionsTable()
	if exp.FailureKind(outcomes[1]) != exp.FailureKind(outcomes[3]) {
		t.Fatal("open errors that differ only in file numbers must share a kind")
	}
	if exp.FailureKind(outcomes[0]) != "" || exp.FailureKind(outcomes[2]) != "v1:a" {
		t.Fatal("valid outcomes have no kind; violations use their signature")
	}
}

func TestDiscoveryPositions(t *testing.T) {
	universe, outcomes := positionsTable()
	// Three kinds: the manifest open error, v1:a and v1:b.
	for _, c := range []struct {
		order      []int
		first, all int
	}{
		{[]int{0, 1, 2, 3, 4, 5}, 2, 5},
		{[]int{5, 0, 4, 3, 2, 1}, 3, 5},
		{[]int{4, 2, 1, 0, 3, 5}, 1, 3},
	} {
		ranking := make([]candidate.Candidate, len(c.order))
		for i, j := range c.order {
			ranking[i] = universe[j]
		}
		p, err := exp.DiscoveryPositions(universe, ranking, outcomes)
		if err != nil {
			t.Fatal(err)
		}
		if p.FirstFailure != c.first || p.AllKinds != c.all || p.Kinds != 3 {
			t.Fatalf("order %v: got %+v, want first %d all %d kinds 3", c.order, p, c.first, c.all)
		}
	}
	if _, err := exp.DiscoveryPositions(universe, universe[:5], outcomes); err == nil {
		t.Fatal("a ranking that is not a permutation was accepted")
	}
	bad := append([]oracle.Outcome(nil), outcomes...)
	bad[0] = oracle.Outcome{Candidate: universe[0], Class: oracle.ClassOracleError, Rule: "x", Detail: "y"}
	if _, err := exp.DiscoveryPositions(universe, universe, bad); err == nil {
		t.Fatal("an oracle_error table was accepted")
	}
}

func TestRandomPositionsAreDeterministic(t *testing.T) {
	universe, outcomes := positionsTable()
	a, err := exp.RandomPositions(universe, outcomes, exp.RandomSeeds())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := exp.RandomPositions(universe, outcomes, exp.RandomSeeds())
	if a != b || a.Seeds != 1000 || a.Kinds != 3 {
		t.Fatalf("summaries differ or are malformed: %+v %+v", a, b)
	}
	if a.FirstP10 > a.FirstMedian || a.FirstMedian > a.FirstP90 || a.AllKindsMedian < 3 {
		t.Fatalf("percentiles out of order: %+v", a)
	}
}
