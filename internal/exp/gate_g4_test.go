package exp_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/exp"
)

// TestGateG4 checks the Gate G4 acceptance criteria:
//   - B0 (mean and min/max over fixed ranking seeds), B4, B5 and B6 discovery curves
//     computed from the stored MUT-0 and MUT-1 outcome tables.
//   - Generated into a temporary directory as CSV and SVG, and the CSV compared
//     with the committed results/discovery_mut0.csv and results/discovery_mut1.csv,
//     so the test never rewrites tracked files. Regenerate the committed
//     artifacts with "cracontest evaluate -discovery ... -plot ...".
//   - Zero candidate re-execution.
func TestGateG4(t *testing.T) {
	// Root paths for stored results
	mut0Dir := filepath.Join("..", "..", "results", "smoke_mut0")
	mut1Dir := filepath.Join("..", "..", "results", "smoke_mut1")

	// Check that stored tables exist
	if _, err := os.Stat(filepath.Join(mut0Dir, "outcomes.json")); err != nil {
		t.Fatalf("stored MUT-0 outcome table not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(mut1Dir, "outcomes.json")); err != nil {
		t.Fatalf("stored MUT-1 outcome table not found: %v", err)
	}

	resultsDir := filepath.Join("..", "..", "results")
	outDir := t.TempDir()
	matchesCommitted := func(generated, committed string) {
		t.Helper()
		got, err := os.ReadFile(generated)
		if err != nil {
			t.Fatalf("read generated %s: %v", generated, err)
		}
		want, err := os.ReadFile(committed)
		if err != nil {
			t.Fatalf("read committed %s: %v", committed, err)
		}
		normalize := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n")) }
		if !bytes.Equal(normalize(got), normalize(want)) {
			t.Errorf("%s is stale: regenerate it from the stored outcome table", committed)
		}
	}

	// 1. Process MUT-0
	t.Log("Evaluating Gate G4 discovery curves for MUT-0...")
	csvMUT0 := filepath.Join(outDir, "discovery_mut0.csv")
	svgMUT0 := filepath.Join(outDir, "plot_mut0.svg")

	report0, err := exp.GenerateG4Artifacts(mut0Dir, csvMUT0, svgMUT0)
	if err != nil {
		t.Fatalf("GenerateG4Artifacts MUT-0: %v", err)
	}
	matchesCommitted(csvMUT0, filepath.Join(resultsDir, "discovery_mut0.csv"))

	// Verify MUT-0 has 0 failures and 0 bugs across all strategies and budgets
	for _, b0 := range report0.B0Summary {
		if b0.FailuresMax != 0 || b0.BugsMax != 0 {
			t.Errorf("MUT-0 B0 at budget %d has failures=%d, bugs=%d (expected 0)",
				b0.Budget, b0.FailuresMax, b0.BugsMax)
		}
	}
	for _, c := range report0.Curves {
		for _, pt := range c.Points {
			if pt.Failures != 0 || pt.DistinctBugs != 0 {
				t.Errorf("MUT-0 %s at budget %d has failures=%d, bugs=%d (expected 0)",
					c.Name, pt.Budget, pt.Failures, pt.DistinctBugs)
			}
		}
	}

	// 2. Process MUT-1
	t.Log("Evaluating Gate G4 discovery curves for MUT-1...")
	csvMUT1 := filepath.Join(outDir, "discovery_mut1.csv")
	svgMUT1 := filepath.Join(outDir, "plot_mut1.svg")

	report1, err := exp.GenerateG4Artifacts(mut1Dir, csvMUT1, svgMUT1)
	if err != nil {
		t.Fatalf("GenerateG4Artifacts MUT-1: %v", err)
	}
	matchesCommitted(csvMUT1, filepath.Join(resultsDir, "discovery_mut1.csv"))

	// Verify MUT-1 reveals failures
	terminalB0 := report1.B0Summary[len(report1.B0Summary)-1]
	if terminalB0.FailuresAvg != 88 {
		t.Errorf("MUT-1 terminal B0 failures: got %.1f, want 88", terminalB0.FailuresAvg)
	}
	if terminalB0.BugsAvg != 2 {
		t.Errorf("MUT-1 terminal B0 bugs: got %.1f, want 2", terminalB0.BugsAvg)
	}

	for _, c := range report1.Curves {
		terminalPt := c.Points[len(c.Points)-1]
		if terminalPt.Failures != 88 {
			t.Errorf("MUT-1 %s terminal failures: got %d, want 88", c.Name, terminalPt.Failures)
		}
		if terminalPt.DistinctBugs != 2 {
			t.Errorf("MUT-1 %s terminal distinct bugs: got %d, want 2", c.Name, terminalPt.DistinctBugs)
		}
	}

	// Log summary table
	t.Logf("=== Gate G4 Acceptance Summary ===")
	t.Logf("MUT-0: %d candidates, 0 failures, discovery curves written to %s and %s",
		report0.Universe, csvMUT0, svgMUT0)
	t.Logf("MUT-1: %d candidates, 88 failures, 2 bugs, discovery curves written to %s and %s",
		report1.Universe, csvMUT1, svgMUT1)
	t.Logf("B0 Random seeds evaluated: 10 seeds (mean/min/max)")
	t.Logf("Strategies evaluated: B0 (random), B4 (combined), B5 (representative), B6 (sync-boundary)")
}
