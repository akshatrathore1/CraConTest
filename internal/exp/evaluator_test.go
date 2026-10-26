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
		{Candidate: c4, Class: oracle.ClassViolation, Rule: "prefix", Signature: sigB},       // distinct bug
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

func TestOpenErrorSignatureNormalization(t *testing.T) {
	testCases := []struct {
		detail   string
		expected string
	}{
		{"corrupt MANIFEST", "v1:open_error:manifest_corrupt"},
		{"MANIFEST-000001 corrupt", "v1:open_error:manifest_corrupt"},
		{"pebble: file does not exist", "v1:open_error:not_exist"},
		{"no such file or directory: db/CURRENT", "v1:open_error:not_exist"},
		{"checksum mismatch on 000002.sst", "v1:open_error:checksum_mismatch"},
		{"unexpected EOF reading version edit", "v1:open_error:unexpected_eof"},
		{"SafeOpen returned nil DB", "v1:open_error:nil_db"},
		{"corrupt sstable footer in 000004.sst", "v1:open_error:table_corrupt"},
		{"operation timed out", "v1:open_error:timeout"},
		{"corrupt record data", "v1:open_error:corrupt"},
		{"v1:open_error:custom_manifest", "v1:open_error:custom_manifest"},
		{"arbitrary unknown failure", "v1:open_error:arbitrary_unknown_failure"},
	}

	for _, tc := range testCases {
		got := exp.FormatOpenErrorSignature(tc.detail)
		if got != tc.expected {
			t.Errorf("FormatOpenErrorSignature(%q): got %q, want %q", tc.detail, got, tc.expected)
		}
	}

	// Test NormalizeOutcome
	u := makeTestUniverse(1)[0]
	o := oracle.Outcome{
		Candidate: u,
		Class:     oracle.ClassOpenError,
		Detail:    "corrupt MANIFEST-000005",
	}
	norm := exp.NormalizeOutcome(o)
	if norm.Signature != "v1:open_error:manifest_corrupt" {
		t.Errorf("NormalizeOutcome signature: got %q, want v1:open_error:manifest_corrupt", norm.Signature)
	}

	// Test NormalizeTable
	table := []oracle.Outcome{o, {Candidate: u, Class: oracle.ClassValid}}
	normTable := exp.NormalizeTable(table)
	if len(normTable) != 2 || normTable[0].Signature != "v1:open_error:manifest_corrupt" || normTable[1].Signature != "" {
		t.Errorf("NormalizeTable unexpected result: %+v", normTable)
	}
}

func TestTabulateSummary(t *testing.T) {
	universe := makeTestUniverse(6)
	c0, c1, c2, c3, c4, c5 := universe[0], universe[1], universe[2], universe[3], universe[4], universe[5]

	sigA := "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sigB := "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	outcomes := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassValid},
		{Candidate: c1, Class: oracle.ClassOpenError, Detail: "corrupt MANIFEST"},
		{Candidate: c2, Class: oracle.ClassValid},
		{Candidate: c3, Class: oracle.ClassViolation, Rule: "durably_committed", Signature: sigA},
		{Candidate: c4, Class: oracle.ClassViolation, Rule: "prefix", Signature: sigB},
		{Candidate: c5, Class: oracle.ClassValid},
	}

	// Ranking 1: [c0, c1, c2, c3, c4, c5]
	// c0: Valid (pos 1)
	// c1: OpenError (pos 2) -> FirstFailure = 2
	// c3: Violation (pos 4) -> FirstViolation = 4
	ranking1 := []candidate.Candidate{c0, c1, c2, c3, c4, c5}
	s1, err := exp.TabulateSummary(universe, ranking1, outcomes)
	if err != nil {
		t.Fatalf("TabulateSummary ranking1: %v", err)
	}

	if s1.TotalTested != 6 {
		t.Errorf("TotalTested: got %d, want 6", s1.TotalTested)
	}
	if s1.FirstFailure != 2 {
		t.Errorf("FirstFailure: got %d, want 2", s1.FirstFailure)
	}
	if s1.FirstViolation != 4 {
		t.Errorf("FirstViolation: got %d, want 4", s1.FirstViolation)
	}
	if s1.Failures != 3 {
		t.Errorf("Failures: got %d, want 3", s1.Failures)
	}
	if s1.OpenErrors != 1 {
		t.Errorf("OpenErrors: got %d, want 1", s1.OpenErrors)
	}
	if s1.Violations != 2 {
		t.Errorf("Violations: got %d, want 2", s1.Violations)
	}
	if s1.DistinctBugs != 3 {
		t.Errorf("DistinctBugs: got %d, want 3", s1.DistinctBugs)
	}

	// Verify Tabulate alias produces identical output
	s1Alias, err := exp.Tabulate(universe, ranking1, outcomes)
	if err != nil {
		t.Fatalf("Tabulate alias: %v", err)
	}
	if s1Alias != s1 {
		t.Errorf("Tabulate alias mismatch: %+v vs %+v", s1Alias, s1)
	}

	// Ranking 2: Violation first at pos 1: [c3, c1, c0, c2, c4, c5]
	ranking2 := []candidate.Candidate{c3, c1, c0, c2, c4, c5}
	s2, err := exp.TabulateSummary(universe, ranking2, outcomes)
	if err != nil {
		t.Fatalf("TabulateSummary ranking2: %v", err)
	}
	if s2.FirstFailure != 1 {
		t.Errorf("ranking2 FirstFailure: got %d, want 1", s2.FirstFailure)
	}
	if s2.FirstViolation != 1 {
		t.Errorf("ranking2 FirstViolation: got %d, want 1", s2.FirstViolation)
	}

	// All-valid case
	validUniverse := []candidate.Candidate{c0, c2, c5}
	validOutcomes := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassValid},
		{Candidate: c2, Class: oracle.ClassValid},
		{Candidate: c5, Class: oracle.ClassValid},
	}
	sValid, err := exp.TabulateSummary(validUniverse, validUniverse, validOutcomes)
	if err != nil {
		t.Fatalf("TabulateSummary valid: %v", err)
	}
	if sValid.FirstFailure != 0 {
		t.Errorf("valid FirstFailure: got %d, want 0", sValid.FirstFailure)
	}
	if sValid.FirstViolation != 0 {
		t.Errorf("valid FirstViolation: got %d, want 0", sValid.FirstViolation)
	}
	if sValid.Failures != 0 {
		t.Errorf("valid Failures: got %d, want 0", sValid.Failures)
	}

	// Error cases: length mismatch and duplicate candidate
	if _, err := exp.TabulateSummary(universe, ranking1[:5], outcomes); err == nil {
		t.Error("expected error on ranking length mismatch, got nil")
	}
	badRanking := []candidate.Candidate{c0, c0, c2, c3, c4, c5}
	if _, err := exp.TabulateSummary(universe, badRanking, outcomes); err == nil {
		t.Error("expected error on duplicate candidate in ranking, got nil")
	}
}

func TestValidateTableOpenErrorSignatures(t *testing.T) {
	universe := makeTestUniverse(2)
	c0, c1 := universe[0], universe[1]

	// 1. Valid open_error with normalized v1: signature
	validTable := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassValid},
		{Candidate: c1, Class: oracle.ClassOpenError, Signature: "v1:open_error:manifest_corrupt", Detail: "corrupt"},
	}
	if err := exp.ValidateTable(universe, validTable); err != nil {
		t.Errorf("ValidateTable failed on valid open_error signature: %v", err)
	}

	// 2. Invalid open_error signature missing v1: prefix
	invalidTable := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassValid},
		{Candidate: c1, Class: oracle.ClassOpenError, Signature: "bad_signature_prefix", Detail: "corrupt"},
	}
	if err := exp.ValidateTable(universe, invalidTable); err == nil {
		t.Error("expected error on open_error signature without v1: prefix, got nil")
	}

	// 3. Valid open_error with empty signature (backwards compatibility)
	emptySigTable := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassValid},
		{Candidate: c1, Class: oracle.ClassOpenError, Detail: "corrupt"},
	}
	if err := exp.ValidateTable(universe, emptySigTable); err != nil {
		t.Errorf("ValidateTable failed on empty open_error signature: %v", err)
	}
}

func TestEvaluateWithNormalizedOpenErrors(t *testing.T) {
	universe := makeTestUniverse(4)
	c0, c1, c2, c3 := universe[0], universe[1], universe[2], universe[3]

	sigA := "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sigOpen1 := "v1:open_error:manifest_corrupt"
	sigOpen2 := "v1:open_error:not_exist"

	outcomes := []oracle.Outcome{
		{Candidate: c0, Class: oracle.ClassOpenError, Signature: sigOpen1, Detail: "corrupt MANIFEST"},
		{Candidate: c1, Class: oracle.ClassOpenError, Signature: sigOpen2, Detail: "file not found"},
		{Candidate: c2, Class: oracle.ClassViolation, Rule: "durably_committed", Signature: sigA},
		{Candidate: c3, Class: oracle.ClassValid},
	}

	budgets := []int{0, 1, 2, 3, 4}
	ranking := []candidate.Candidate{c0, c1, c2, c3}

	points, err := exp.Evaluate(universe, ranking, outcomes, budgets)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	expected := []exp.Point{
		{Budget: 0, Tested: 0, Failures: 0, DistinctBugs: 0},
		{Budget: 1, Tested: 1, Failures: 1, DistinctBugs: 1}, // c0 (open_error 1)
		{Budget: 2, Tested: 2, Failures: 2, DistinctBugs: 2}, // c0, c1 (open_error 2)
		{Budget: 3, Tested: 3, Failures: 3, DistinctBugs: 3}, // c0, c1, c2 (viol sigA)
		{Budget: 4, Tested: 4, Failures: 3, DistinctBugs: 3}, // c0, c1, c2, c3 (valid)
	}

	for i := range points {
		if points[i] != expected[i] {
			t.Errorf("point %d: got %+v, want %+v", i, points[i], expected[i])
		}
	}
}
