package exp_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	resultsDir := findResultsDir(t)
	mut0Dir := filepath.Join(resultsDir, "smoke_mut0")
	mut1Dir := filepath.Join(resultsDir, "smoke_mut1")

	// Check that stored tables exist
	if _, err := os.Stat(filepath.Join(mut0Dir, "outcomes.json")); err != nil {
		t.Fatalf("stored MUT-0 outcome table not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(mut1Dir, "outcomes.json")); err != nil {
		t.Fatalf("stored MUT-1 outcome table not found: %v", err)
	}

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
		if msg := diffCSVLines(got, want); msg != "" {
			t.Errorf("%s is stale (%s): regenerate it from the stored outcome table", committed, msg)
		}
	}

	assertG4ReportAndSVG := func(label string, report *exp.G4RunReport, svgPath string) {
		t.Helper()
		if len(report.B0Summary) == 0 {
			t.Fatalf("%s: expected non-empty B0Summary", label)
		}
		if len(report.Curves) != 3 {
			t.Fatalf("%s: expected 3 deterministic curves (B4, B5, B6), got %d", label, len(report.Curves))
		}
		gotNames := make([]string, len(report.Curves))
		for i, c := range report.Curves {
			if len(c.Points) == 0 {
				t.Fatalf("%s: curve %s has empty points", label, c.Name)
			}
			gotNames[i] = c.Name
		}
		wantNames := []string{"b4_combined", "b5_representative", "b6_sync_boundary"}
		if !reflect.DeepEqual(gotNames, wantNames) {
			t.Fatalf("%s: curve names = %v, want %v", label, gotNames, wantNames)
		}
		svgBytes, err := os.ReadFile(svgPath)
		if err != nil {
			t.Fatalf("%s: read SVG %s: %v", label, svgPath, err)
		}
		svgText := string(svgBytes)
		if !strings.Contains(svgText, "<svg") || !strings.Contains(svgText, "</svg>") {
			t.Fatalf("%s: malformed SVG %s", label, svgPath)
		}
		if !strings.Contains(svgText, "B0") || !strings.Contains(svgText, "B4") || !strings.Contains(svgText, "B5") || !strings.Contains(svgText, "B6") {
			t.Fatalf("%s: SVG legend missing expected strategy labels in %s", label, svgPath)
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
	assertG4ReportAndSVG("MUT-0", report0, svgMUT0)
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
	assertG4ReportAndSVG("MUT-1", report1, svgMUT1)
	matchesCommitted(csvMUT1, filepath.Join(resultsDir, "discovery_mut1.csv"))

	// Verify MUT-1 reveals failures
	terminalB0 := report1.B0Summary[len(report1.B0Summary)-1]
	if terminalB0.FailuresAvg != 88 {
		t.Errorf("MUT-1 terminal B0 failures: got %.1f, want 88", terminalB0.FailuresAvg)
	}
	if terminalB0.BugsAvg != 6 {
		t.Errorf("MUT-1 terminal B0 bugs: got %.1f, want 6", terminalB0.BugsAvg)
	}

	for _, c := range report1.Curves {
		terminalPt := c.Points[len(c.Points)-1]
		if terminalPt.Failures != 88 {
			t.Errorf("MUT-1 %s terminal failures: got %d, want 88", c.Name, terminalPt.Failures)
		}
		if terminalPt.DistinctBugs != 6 {
			t.Errorf("MUT-1 %s terminal distinct bugs: got %d, want 6", c.Name, terminalPt.DistinctBugs)
		}
	}

	// Log summary table
	t.Logf("=== Gate G4 Acceptance Summary ===")
	t.Logf("MUT-0: %d candidates, 0 failures, discovery curves written to %s and %s",
		report0.Universe, csvMUT0, svgMUT0)
	t.Logf("MUT-1: %d candidates, 88 failures, 6 distinct signatures, discovery curves written to %s and %s",
		report1.Universe, csvMUT1, svgMUT1)
	t.Logf("B0 Random seeds evaluated: 10 seeds (mean/min/max)")
	t.Logf("Strategies evaluated: B0 (random), B4 (combined), B5 (representative), B6 (sync-boundary)")
}
