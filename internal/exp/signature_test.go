package exp_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func loadStoredRun(t *testing.T, dir string) ([]trace.Event, []history.Entry, []candidate.Candidate) {
	t.Helper()
	open := func(name string) *os.File {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		return f
	}
	events, err := trace.Decode(open("trace.json"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := history.Decode(open("history.json"))
	if err != nil {
		t.Fatal(err)
	}
	universe, err := candidate.Decode(open("universe.json"))
	if err != nil {
		t.Fatal(err)
	}
	return events, entries, universe
}

// Violation signatures must be built from the write categories of the
// exact candidate cutoff, as the outcome contract specifies.
func TestViolationSignaturesUseCutoffCategories(t *testing.T) {
	events, entries, universe := loadStoredRun(t, filepath.Join(findResultsDir(t), "smoke_mut1"))
	categories := make(map[uint64][]string)
	for _, c := range universe {
		if _, exists := categories[c.Event]; !exists {
			categories[c.Event] = []string{fmt.Sprintf("sentinel-cat-%d", c.Event)}
		}
	}
	res, err := exp.RunExplore(exp.RunnerConfig{Universe: universe, Events: events, History: entries, Categories: categories})
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.OracleErrors != 0 {
		t.Fatalf("unexpected oracle_errors: %d", res.Summary.OracleErrors)
	}
	if err := exp.ValidateTable(universe, exp.NormalizeTable(res.Outcomes)); err != nil {
		t.Fatalf("ValidateTable failed: %v", err)
	}

	validRules := map[string]bool{
		"durably_committed": true,
		"atomic_batch":      true,
		"prefix":            true,
		"structural":        true,
	}
	opKinds := []string{"set", "delete", "range_delete", "batch", "checkpoint", "ingest", "ratchet", "unknown"}

	violations := 0
	for _, o := range res.Outcomes {
		if o.Class != oracle.ClassViolation {
			continue
		}
		violations++
		if !validRules[o.Rule] {
			t.Fatalf("unexpected violation rule %q on %+v", o.Rule, o.Candidate)
		}
		cutoffCats := categories[o.Candidate.Event]
		matched := false
		for _, kind := range opKinds {
			if o.Signature == oracle.FormatSignature(o.Rule, kind, cutoffCats) {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("signature %s of %+v (rule=%s) was not built from cutoff categories %v",
				o.Signature, o.Candidate, o.Rule, cutoffCats)
		}
	}
	if violations == 0 {
		t.Fatal("expected violations in the stored MUT-1 run")
	}
}

// A cutoff without features yields an infrastructure error, never an
// invented signature.
func TestMissingCategoriesAreOracleErrors(t *testing.T) {
	events, entries, universe := loadStoredRun(t, filepath.Join(findResultsDir(t), "smoke_mut1"))
	if len(universe) < 4 {
		t.Fatalf("expected stored universe len >= 4, got %d", len(universe))
	}
	res, err := exp.RunExplore(exp.RunnerConfig{Universe: universe[:4], Events: events, History: entries, Categories: map[uint64][]string{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Outcomes) != 4 {
		t.Fatalf("expected 4 outcomes, got %d", len(res.Outcomes))
	}
	for _, o := range res.Outcomes {
		if o.Class != oracle.ClassOracleError || o.Rule != "missing_features" || o.Signature != "" {
			t.Fatalf("outcome %+v, want oracle_error/missing_features with empty Signature", o)
		}
	}
}

// Passing Categories: nil computes cutoff features automatically via graph.Build.
func TestNilCategoriesFallBackToGraphBuild(t *testing.T) {
	events, entries, universe := loadStoredRun(t, filepath.Join(findResultsDir(t), "smoke_mut1"))
	if len(universe) == 0 {
		t.Fatal("empty stored universe")
	}
	res, err := exp.RunExplore(exp.RunnerConfig{Universe: universe, Events: events, History: entries, Categories: nil})
	if err != nil {
		t.Fatal(err)
	}
	if res.Summary.OracleErrors != 0 {
		t.Fatalf("expected 0 oracle_errors when Categories is nil, got %d", res.Summary.OracleErrors)
	}
	if res.Summary.Violations == 0 {
		t.Fatal("expected violations in smoke_mut1 with nil Categories fallback")
	}
}
