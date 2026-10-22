package exp_test

import (
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
// cutoff, as the outcome contract specifies.
func TestViolationSignaturesUseCutoffCategories(t *testing.T) {
	events, entries, universe := loadStoredRun(t, filepath.Join("..", "..", "results", "smoke_mut1"))
	sentinel := []string{"sentinel-category"}
	categories := make(map[uint64][]string)
	for _, c := range universe {
		categories[c.Event] = sentinel
	}
	res, err := exp.RunExplore(exp.RunnerConfig{Universe: universe, Events: events, History: entries, Categories: categories})
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{}
	for _, rule := range []string{"durably_committed", "atomic_batch", "prefix", "structural"} {
		for _, kind := range []string{"set", "delete", "range_delete", "batch", "unknown"} {
			allowed[oracle.FormatSignature(rule, kind, sentinel)] = true
		}
	}
	violations := 0
	for _, o := range res.Outcomes {
		if o.Class != oracle.ClassViolation {
			continue
		}
		violations++
		if !allowed[o.Signature] {
			t.Fatalf("signature %s of %+v was not built from the supplied categories", o.Signature, o.Candidate)
		}
	}
	if violations == 0 {
		t.Fatal("expected violations in the stored MUT-1 run")
	}
}

// A cutoff without features yields an infrastructure error, never an
// invented signature.
func TestMissingCategoriesAreOracleErrors(t *testing.T) {
	events, entries, universe := loadStoredRun(t, filepath.Join("..", "..", "results", "smoke_mut1"))
	res, err := exp.RunExplore(exp.RunnerConfig{Universe: universe[:4], Events: events, History: entries, Categories: map[uint64][]string{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range res.Outcomes {
		if o.Class != oracle.ClassOracleError || o.Rule != "missing_features" {
			t.Fatalf("outcome %+v, want oracle_error/missing_features", o)
		}
	}
}
