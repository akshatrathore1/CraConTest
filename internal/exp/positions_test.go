package exp_test

import (
	"strings"
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

	// Verify FailureKind handles Panic, Hang, empty Detail, ClassOracleError, and unnormalized vs normalized agreement.
	oeUnnorm := oracle.Outcome{Class: oracle.ClassOpenError, Detail: "corrupt MANIFEST-000042"}
	oeNorm := exp.NormalizeOutcome(oeUnnorm)
	if exp.FailureKind(oeUnnorm) != oeNorm.Signature || exp.FailureKind(oeUnnorm) != "v1:open_error:manifest_corrupt" {
		t.Fatalf("FailureKind open_error mismatch: unnorm=%q norm=%q", exp.FailureKind(oeUnnorm), oeNorm.Signature)
	}

	p1 := oracle.Outcome{Class: oracle.ClassPanic, Detail: "fatal at 0xc0001a2b00 step 42"}
	p2 := oracle.Outcome{Class: oracle.ClassPanic, Detail: "fatal at 0xdeadbeef step 99"}
	if exp.FailureKind(p1) != exp.FailureKind(p2) || !strings.HasPrefix(exp.FailureKind(p1), "v1:panic:") {
		t.Fatalf("panic hex/number normalization failed: %q vs %q", exp.FailureKind(p1), exp.FailureKind(p2))
	}

	hEmpty := oracle.Outcome{Class: oracle.ClassHang, Detail: ""}
	if got := exp.FailureKind(hEmpty); got != "v1:hang:timeout" {
		t.Fatalf("empty hang FailureKind = %q, want v1:hang:timeout", got)
	}

	oErr := oracle.Outcome{Class: oracle.ClassOracleError, Rule: "missing_features", Signature: "v1:should_ignore"}
	if got := exp.FailureKind(oErr); got != "" {
		t.Fatalf("ClassOracleError FailureKind = %q, want empty", got)
	}
	validWithSig := oracle.Outcome{Class: oracle.ClassValid, Signature: "v1:stale"}
	if got := exp.FailureKind(validWithSig); got != "" {
		t.Fatalf("ClassValid FailureKind = %q, want empty", got)
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
	dupRanking := append([]candidate.Candidate(nil), universe[:5]...)
	dupRanking = append(dupRanking, universe[0])
	if _, err := exp.DiscoveryPositions(universe, dupRanking, outcomes); err == nil {
		t.Fatal("a ranking with duplicate candidate was accepted")
	}
	foreignRanking := append([]candidate.Candidate(nil), universe[:5]...)
	foreignRanking = append(foreignRanking, candidate.Candidate{Event: 9999})
	if _, err := exp.DiscoveryPositions(universe, foreignRanking, outcomes); err == nil {
		t.Fatal("a ranking with foreign candidate was accepted")
	}
	bad := append([]oracle.Outcome(nil), outcomes...)
	bad[0] = oracle.Outcome{Candidate: universe[0], Class: oracle.ClassOracleError, Rule: "x", Detail: "y"}
	if _, err := exp.DiscoveryPositions(universe, universe, bad); err == nil {
		t.Fatal("an oracle_error table was accepted")
	}
}

func TestRandomPositionsEdgeCases(t *testing.T) {
	universe, outcomes := positionsTable()
	if _, err := exp.RandomPositions(universe, outcomes, nil); err == nil {
		t.Fatal("expected error for empty seeds slice")
	}

	allValid := make([]oracle.Outcome, len(universe))
	for i, c := range universe {
		allValid[i] = oracle.Outcome{Candidate: c, Class: oracle.ClassValid}
	}
	zeroSummary, err := exp.RandomPositions(universe, allValid, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("RandomPositions all valid: %v", err)
	}
	if zeroSummary.FirstMedian != 0 || zeroSummary.AllKindsMedian != 0 || zeroSummary.Kinds != 0 || zeroSummary.Seeds != 3 {
		t.Fatalf("unexpected zero-failure summary: %+v", zeroSummary)
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
