package exp_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/exp"
)

// TestMainRunArtifactsMatchStoredTables recomputes the discovery curves of the
// main 1,000-operation runs from their stored outcome tables and checks them
// against the committed CSVs and the reported totals. No candidate is executed.
func TestMainRunArtifactsMatchStoredTables(t *testing.T) {
	results := filepath.Join("..", "..", "results")
	for _, run := range []struct {
		name               string
		failures, distinct int
	}{
		{"mut0", 0, 0},
		{"mut1", 865, 15},
	} {
		runDir := filepath.Join(results, "main_"+run.name)
		if _, err := os.Stat(filepath.Join(runDir, "outcomes.json")); err != nil {
			t.Fatalf("stored outcome table for main_%s not found: %v", run.name, err)
		}
		out := t.TempDir()
		csvPath := filepath.Join(out, "discovery.csv")
		report, err := exp.GenerateG4Artifacts(runDir, csvPath, filepath.Join(out, "plot.svg"))
		if err != nil {
			t.Fatalf("main_%s: %v", run.name, err)
		}
		for _, curve := range report.Curves {
			last := curve.Points[len(curve.Points)-1]
			if last.Failures != run.failures || last.DistinctBugs != run.distinct {
				t.Errorf("main_%s %s: terminal failures=%d distinct=%d, want %d and %d",
					run.name, curve.Name, last.Failures, last.DistinctBugs, run.failures, run.distinct)
			}
		}
		got, err := os.ReadFile(csvPath)
		if err != nil {
			t.Fatal(err)
		}
		want, err := os.ReadFile(filepath.Join(results, "discovery_main_"+run.name+".csv"))
		if err != nil {
			t.Fatal(err)
		}
		crlf := []byte("\r\n")
		if !bytes.Equal(bytes.ReplaceAll(got, crlf, []byte("\n")), bytes.ReplaceAll(want, crlf, []byte("\n"))) {
			t.Errorf("results/discovery_main_%s.csv is stale: regenerate it with cracontest evaluate", run.name)
		}
	}
}
