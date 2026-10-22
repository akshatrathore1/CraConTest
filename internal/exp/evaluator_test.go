package exp_test

import (
	"bytes"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
)

func makeTestUniverse(n int) []candidate.Candidate {
	u := make([]candidate.Candidate, n)
	for i := 0; i < n; i++ {
		u[i] = candidate.Candidate{
			Event:   uint64(i + 1),
			Percent: 100,
			Seed:    uint64(i + 10),
		}
	}
	candidate.Sort(u)
	return u
}

// TestEvaluatorHandBuiltSixTupleCurves verifies Target C3:
// "A hand-built table with at least six tuples, two repeated signatures,
// valid outcomes and a recovery failure gives exact curves at budgets
// 0/1/3/N for two differing rankings against the same table."
func TestEvaluatorHandBuiltSixTupleCurves(t *testing.T) {
	universe := makeTestUniverse(6)
	c0, c1, c2, c3, c4, c5 := universe[0], universe[1], universe[2], universe[3], universe[4], universe[5]

	sigA := "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sigB := "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	outcomes := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassValid},
		{Candidate: c1, Class: oracle.ClassViolation, Rule: "durably_committed", Signature: sigA},
		{Candidate: c2, Class: oracle.ClassOpenError, Detail: "corrupt MANIFEST"},
		{Candidate: c3, Class: oracle.ClassViolation, Rule: "atomic_batch", Signature: sigA}, // repeated signature
		{Candidate: c4, Class: oracle.ClassViolation, Rule: "prefix", Signature: sigB},        // distinct bug
		{Candidate: c5, Class: oracle.ClassValid},
	}

	if err := exp.ValidateTable(universe, outcomes); err != nil {
		t.Fatalf("ValidateTable failed on hand-built table: %v", err)
	}

	budgets := []int{0, 1, 3, 6} // 0, 1, 3, N

	// Ranking 1: [c0, c1, c2, c3, c4, c5]
	ranking1 := []candidate.Candidate{c0, c1, c2, c3, c4, c5}
	points1, err := exp.Evaluate(universe, ranking1, outcomes, budgets)
	if err != nil {
		t.Fatalf("Evaluate ranking1: %v", err)
	}

	expectedPoints1 := []exp.Point{
		{Budget: 0, Tested: 0, Failures: 0, DistinctBugs: 0},
		{Budget: 1, Tested: 1, Failures: 0, DistinctBugs: 0}, // c0 (valid)
		{Budget: 3, Tested: 3, Failures: 2, DistinctBugs: 1}, // c0, c1 (viol sigA), c2 (open_error)
		{Budget: 6, Tested: 6, Failures: 4, DistinctBugs: 2}, // all: c1(sigA), c2(err), c3(sigA), c4(sigB)
	}

	if len(points1) != len(expectedPoints1) {
		t.Fatalf("ranking1 points len %d != %d", len(points1), len(expectedPoints1))
	}
	for i := range points1 {
		if points1[i] != expectedPoints1[i] {
			t.Errorf("ranking1 point at %d: got %+v, want %+v", i, points1[i], expectedPoints1[i])
		}
	}

	// Ranking 2: [c4, c2, c1, c0, c3, c5]
	// Differing ranking prioritizing bugB (c4) and open_error (c2) first.
	ranking2 := []candidate.Candidate{c4, c2, c1, c0, c3, c5}
	points2, err := exp.Evaluate(universe, ranking2, outcomes, budgets)
	if err != nil {
		t.Fatalf("Evaluate ranking2: %v", err)
	}

	expectedPoints2 := []exp.Point{
		{Budget: 0, Tested: 0, Failures: 0, DistinctBugs: 0},
		{Budget: 1, Tested: 1, Failures: 1, DistinctBugs: 1}, // c4 (viol sigB)
		{Budget: 3, Tested: 3, Failures: 3, DistinctBugs: 2}, // c4 (sigB), c2 (open_error), c1 (sigA)
		{Budget: 6, Tested: 6, Failures: 4, DistinctBugs: 2}, // all
	}

	if len(points2) != len(expectedPoints2) {
		t.Fatalf("ranking2 points len %d != %d", len(points2), len(expectedPoints2))
	}
	for i := range points2 {
		if points2[i] != expectedPoints2[i] {
			t.Errorf("ranking2 point at %d: got %+v, want %+v", i, points2[i], expectedPoints2[i])
		}
	}
}

func TestEvaluatorRejectInvalidInputs(t *testing.T) {
	universe := makeTestUniverse(3)
	sig := "v1:1111111111111111111111111111111111111111111111111111111111111111"

	validOutcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: oracle.ClassValid},
		{Candidate: universe[1], Class: oracle.ClassViolation, Rule: "prefix", Signature: sig},
		{Candidate: universe[2], Class: oracle.ClassPanic, Detail: "fatal"},
	}

	// 1. Missing outcome (shorter length).
	if err := exp.ValidateTable(universe, validOutcomes[:2]); err == nil {
		t.Error("expected error on missing outcome, got nil")
	}

	// 2. Duplicate outcome candidate.
	dupOutcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: oracle.ClassValid},
		{Candidate: universe[0], Class: oracle.ClassValid},
		{Candidate: universe[2], Class: oracle.ClassValid},
	}
	if err := exp.ValidateTable(universe, dupOutcomes); err == nil {
		t.Error("expected error on duplicate outcome, got nil")
	}

	// 3. Fabricated / foreign candidate in ranking.
	foreignCand := candidate.Candidate{Event: 999, Percent: 100, Seed: 999}
	badRanking := []candidate.Candidate{universe[0], universe[1], foreignCand}
	if _, err := exp.Evaluate(universe, badRanking, validOutcomes, []int{1}); err == nil {
		t.Error("expected error on foreign candidate in ranking, got nil")
	}

	// 4. Duplicate candidate in ranking.
	dupRanking := []candidate.Candidate{universe[0], universe[0], universe[2]}
	if _, err := exp.Evaluate(universe, dupRanking, validOutcomes, []int{1}); err == nil {
		t.Error("expected error on duplicate candidate in ranking, got nil")
	}

	// 5. Invalid class.
	badClassOutcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: "unknown_class"},
		{Candidate: universe[1], Class: oracle.ClassValid},
		{Candidate: universe[2], Class: oracle.ClassValid},
	}
	if err := exp.ValidateTable(universe, badClassOutcomes); err == nil {
		t.Error("expected error on invalid class, got nil")
	}

	// 6. Missing violation signature.
	missingSigOutcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: oracle.ClassViolation, Rule: "prefix"},
		{Candidate: universe[1], Class: oracle.ClassValid},
		{Candidate: universe[2], Class: oracle.ClassValid},
	}
	if err := exp.ValidateTable(universe, missingSigOutcomes); err == nil {
		t.Error("expected error on missing violation signature, got nil")
	}

	// 7. Negative budget.
	if _, err := exp.Evaluate(universe, universe, validOutcomes, []int{-1}); err == nil {
		t.Error("expected error on negative budget, got nil")
	}

	// 8. Too-large budget (> len(universe)).
	if _, err := exp.Evaluate(universe, universe, validOutcomes, []int{4}); err == nil {
		t.Error("expected error on budget > universe length, got nil")
	}

	// 9. Duplicate budget.
	if _, err := exp.Evaluate(universe, universe, validOutcomes, []int{1, 1}); err == nil {
		t.Error("expected error on duplicate budget, got nil")
	}

	// 10. Non-increasing budgets.
	if _, err := exp.Evaluate(universe, universe, validOutcomes, []int{2, 1}); err == nil {
		t.Error("expected error on non-increasing budgets, got nil")
	}

	// 11. oracle_error input.
	oracleErrOutcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: oracle.ClassOracleError, Detail: "infrastructure failure"},
		{Candidate: universe[1], Class: oracle.ClassValid},
		{Candidate: universe[2], Class: oracle.ClassValid},
	}
	if err := exp.ValidateTable(universe, oracleErrOutcomes); err == nil {
		t.Error("expected error on oracle_error input, got nil")
	}
	if _, err := exp.Evaluate(universe, universe, oracleErrOutcomes, []int{1}); err == nil {
		t.Error("expected Evaluate to abort on oracle_error input, got nil")
	}
}

func TestOutcomeDocumentRoundTrip(t *testing.T) {
	universe := makeTestUniverse(3)
	sig := "v1:abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	outcomes := []oracle.Outcome{
		{Candidate: universe[0], Class: oracle.ClassValid},
		{Candidate: universe[1], Class: oracle.ClassViolation, Rule: "prefix", Signature: sig},
		{Candidate: universe[2], Class: oracle.ClassHang, Detail: "timeout"},
	}

	var buf bytes.Buffer
	if err := exp.EncodeOutcomes(&buf, universe, outcomes); err != nil {
		t.Fatalf("EncodeOutcomes: %v", err)
	}

	decoded, err := exp.DecodeOutcomes(&buf)
	if err != nil {
		t.Fatalf("DecodeOutcomes: %v", err)
	}

	if len(decoded) != len(outcomes) {
		t.Fatalf("decoded len %d != %d", len(decoded), len(outcomes))
	}
	for i := range outcomes {
		if decoded[i] != outcomes[i] {
			t.Errorf("decoded mismatch at %d: got %+v, want %+v", i, decoded[i], outcomes[i])
		}
	}

	// Test trailing data rejection.
	buf.Reset()
	if err := exp.EncodeOutcomes(&buf, universe, outcomes); err != nil {
		t.Fatalf("EncodeOutcomes: %v", err)
	}
	buf.WriteString("{\"trailing\": 1}")
	if _, err := exp.DecodeOutcomes(&buf); err == nil {
		t.Error("expected error on trailing data, got nil")
	}

	// Test missing array rejection.
	badJSON := []byte(`{"schema_version": 1}`)
	if _, err := exp.DecodeOutcomes(bytes.NewReader(badJSON)); err == nil {
		t.Error("expected error on missing outcomes array, got nil")
	}

	// Test unsupported schema version.
	badVersionJSON := []byte(`{"schema_version": 2, "outcomes": []}`)
	if _, err := exp.DecodeOutcomes(bytes.NewReader(badVersionJSON)); err == nil {
		t.Error("expected error on unsupported schema_version, got nil")
	}
}

func TestRankingDocumentRoundTrip(t *testing.T) {
	universe := makeTestUniverse(3)
	ranking := []candidate.Candidate{universe[2], universe[0], universe[1]}

	doc := exp.RankingDocument{
		Strategy:    "combined",
		RankingSeed: 42,
		Candidates:  ranking,
	}

	var buf bytes.Buffer
	if err := exp.EncodeRanking(&buf, doc); err != nil {
		t.Fatalf("EncodeRanking: %v", err)
	}

	decoded, err := exp.DecodeRanking(&buf)
	if err != nil {
		t.Fatalf("DecodeRanking: %v", err)
	}

	if decoded.SchemaVersion != exp.Version {
		t.Errorf("schema version: got %d, want %d", decoded.SchemaVersion, exp.Version)
	}
	if decoded.Strategy != doc.Strategy {
		t.Errorf("strategy: got %s, want %s", decoded.Strategy, doc.Strategy)
	}
	if decoded.RankingSeed != doc.RankingSeed {
		t.Errorf("ranking_seed: got %d, want %d", decoded.RankingSeed, doc.RankingSeed)
	}
	if len(decoded.Candidates) != len(doc.Candidates) {
		t.Fatalf("candidates len: got %d, want %d", len(decoded.Candidates), len(doc.Candidates))
	}
	for i := range doc.Candidates {
		if decoded.Candidates[i] != doc.Candidates[i] {
			t.Errorf("candidate mismatch at %d: got %+v, want %+v", i, decoded.Candidates[i], doc.Candidates[i])
		}
	}

	// Trailing data check.
	buf.Reset()
	if err := exp.EncodeRanking(&buf, doc); err != nil {
		t.Fatalf("EncodeRanking: %v", err)
	}
	buf.WriteString("   trailing garbage")
	if _, err := exp.DecodeRanking(&buf); err == nil {
		t.Error("expected error on trailing data, got nil")
	}
}
