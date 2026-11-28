package exp_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func buildPlotTestFixture(t *testing.T) ([]candidate.Candidate, *graph.View, []oracle.Outcome, []trace.Event) {
	t.Helper()
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "db/000001.log", Handle: 1, Data: []byte("abc"), Len: 3},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindSync, File: "db/000001.log", Handle: 1},
	}
	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}
	universe, err := candidate.Build(events, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	outcomes := make([]oracle.Outcome, len(universe))
	sig := "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for i, c := range universe {
		if c.Percent == 0 && i == 0 {
			outcomes[i] = oracle.Outcome{Candidate: c, Class: oracle.ClassViolation, Rule: "durably_committed", Signature: sig}
		} else {
			outcomes[i] = oracle.Outcome{Candidate: c, Class: oracle.ClassValid}
		}
	}
	return universe, view, outcomes, events
}

func TestEvaluateG4ValidationAndCurves(t *testing.T) {
	universe, view, outcomes, _ := buildPlotTestFixture(t)
	budgets := []int{0, 1, len(universe)}

	if _, err := exp.EvaluateG4("r1", "MUT-1", universe, nil, outcomes, budgets, nil); err == nil {
		t.Error("expected error for nil graph view")
	}
	if _, err := exp.EvaluateG4("r1", "MUT-1", universe, view, outcomes, nil, nil); err == nil {
		t.Error("expected error for empty budgets")
	}
	badOutcomes := append([]oracle.Outcome(nil), outcomes...)
	badOutcomes[0] = oracle.Outcome{Candidate: universe[0], Class: oracle.ClassOracleError, Rule: "err", Detail: "boom"}
	if _, err := exp.EvaluateG4("r1", "MUT-1", universe, view, badOutcomes, budgets, nil); err == nil {
		t.Error("expected error for ClassOracleError in outcomes")
	}

	report, err := exp.EvaluateG4("run1", "MUT-1", universe, view, outcomes, budgets, nil)
	if err != nil {
		t.Fatalf("EvaluateG4: %v", err)
	}
	if report.Universe != len(universe) || len(report.B0Summary) != len(budgets) {
		t.Fatalf("unexpected report shape: %+v", report)
	}
	if len(report.Curves) != 3 {
		t.Fatalf("expected 3 G4 curves, got %d", len(report.Curves))
	}
	// Terminal failure counts match fixture (1 failure, 1 distinct bug).
	lastB0 := report.B0Summary[len(report.B0Summary)-1]
	if lastB0.FailuresAvg != 1.0 || lastB0.BugsAvg != 1.0 {
		t.Fatalf("unexpected terminal B0 summary: %+v", lastB0)
	}

	// Custom rankingSeeds override.
	customRep, err := exp.EvaluateG4("run2", "MUT-1", universe, view, outcomes, budgets, []uint64{99, 101})
	if err != nil {
		t.Fatalf("EvaluateG4 custom seeds: %v", err)
	}
	if len(customRep.B0Summary) != len(budgets) {
		t.Fatalf("unexpected custom B0Summary length: %d", len(customRep.B0Summary))
	}
}

func TestWriteG4CSVAndSVGEdgeCases(t *testing.T) {
	if err := exp.WriteG4CSV(&bytes.Buffer{}, nil); err == nil {
		t.Error("expected WriteG4CSV error on nil report")
	}
	if err := exp.WriteG4PlotSVG(&bytes.Buffer{}, nil); err == nil {
		t.Error("expected WriteG4PlotSVG error on nil report")
	}

	rep := &exp.G4RunReport{
		RunID:    `run<1>&"test"`,
		FaultID:  `MUT-<2>`,
		Universe: 10, // smaller than max budget 150 to test overflow clamp/scale
		Budgets:  []int{0, 1, 5, 150},
		B0Summary: []exp.B0SummaryPoint{
			{Budget: 0, FailuresMin: 0, FailuresMax: 0, FailuresAvg: 0, BugsMin: 0, BugsMax: 0, BugsAvg: 0},
			{Budget: 1, FailuresMin: 0, FailuresMax: 1, FailuresAvg: 0.5, BugsMin: 0, BugsMax: 1, BugsAvg: 0.5},
			{Budget: 150, FailuresMin: 1, FailuresMax: 1, FailuresAvg: 1.0, BugsMin: 1, BugsMax: 1, BugsAvg: 1.0},
		},
		Curves: []exp.StrategyCurve{
			{Name: "b4_combined", Points: []exp.Point{{Budget: 0}, {Budget: 150, Failures: 1, DistinctBugs: 1}}},
			{Name: "b5_representative", Points: []exp.Point{{Budget: 0}, {Budget: 150, Failures: 1, DistinctBugs: 1}}},
			{Name: "b6_sync_boundary", Points: nil}, // empty Points must NOT emit path or legend
			{Name: "fifo", Points: []exp.Point{{Budget: 0}, {Budget: 150, Failures: 1, DistinctBugs: 1}}},
			{Name: "lifo", Points: []exp.Point{{Budget: 0}, {Budget: 150, Failures: 1, DistinctBugs: 1}}},
			{Name: "stride", Points: []exp.Point{{Budget: 0}, {Budget: 150, Failures: 1, DistinctBugs: 1}}},
		},
	}

	var csvBuf bytes.Buffer
	if err := exp.WriteG4CSV(&csvBuf, rep); err != nil {
		t.Fatalf("WriteG4CSV: %v", err)
	}
	rows, err := csv.NewReader(&csvBuf).ReadAll()
	if err != nil {
		t.Fatalf("parse generated CSV: %v", err)
	}
	if len(rows) < 6 {
		t.Fatalf("expected >= 6 CSV rows, got %d", len(rows))
	}

	var svgBuf bytes.Buffer
	if err := exp.WriteG4PlotSVG(&svgBuf, rep); err != nil {
		t.Fatalf("WriteG4PlotSVG: %v", err)
	}
	svg := svgBuf.String()
	// HTML-escaped title tokens
	if !strings.Contains(svg, "&lt;1&gt;&amp;&#34;test&#34;") || !strings.Contains(svg, "MUT-&lt;2&gt;") {
		t.Errorf("expected HTML-escaped title in SVG:\n%s", svg)
	}
	// Empty-point b6_sync_boundary must NOT appear in SVG path or legend!
	if strings.Contains(svg, "b6-line") && strings.Contains(svg, "B6 Sync-Boundary") {
		t.Errorf("empty-point curve should be omitted from legend:\n%s", svg)
	}
	// fifo, lifo, stride legend labels must appear cleanly
	for _, wantLabel := range []string{"B1 FIFO", "B2 LIFO", "B3 Stride"} {
		if !strings.Contains(svg, wantLabel) {
			t.Errorf("SVG missing legend label %q", wantLabel)
		}
	}
	// Integer yTicks only (with maxFail=1, must NOT emit 0.2/0.4/0.6 fractional ticks)
	if strings.Contains(svg, ">0.2<") || strings.Contains(svg, ">0.4<") {
		t.Errorf("SVG contains fractional tick labels for integer failure counts:\n%s", svg)
	}
}

func TestGenerateG4ArtifactsWithBudgetsAndErrors(t *testing.T) {
	universe, _, outcomes, events := buildPlotTestFixture(t)
	runDir := t.TempDir()

	writeJSON := func(name string, fn func(*os.File) error) {
		f, err := os.Create(filepath.Join(runDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := fn(f); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writeJSON("trace.json", func(f *os.File) error { return trace.Encode(f, events) })
	writeJSON("universe.json", func(f *os.File) error { return candidate.Encode(f, universe) })
	writeJSON("outcomes.json", func(f *os.File) error { return exp.EncodeOutcomes(f, universe, outcomes) })

	// Missing/wrong schema_version must error cleanly.
	writeJSON("run.json", func(f *os.File) error {
		return json.NewEncoder(f).Encode(map[string]any{"schema_version": 99, "run_id": "bad", "fault_id": "MUT-1"})
	})
	if _, err := exp.GenerateG4Artifacts(runDir, filepath.Join(t.TempDir(), "c.csv"), ""); err == nil {
		t.Error("expected schema_version error in GenerateG4Artifacts")
	}

	writeJSON("run.json", func(f *os.File) error {
		return json.NewEncoder(f).Encode(map[string]any{"schema_version": 1, "run_id": "ok", "fault_id": "MUT-1"})
	})
	outDir := t.TempDir()
	csvOut := filepath.Join(outDir, "custom.csv")
	svgOut := filepath.Join(outDir, "custom.svg")
	customBudgets := []int{0, 1, len(universe)}
	rep, err := exp.GenerateG4ArtifactsWithBudgets(runDir, csvOut, svgOut, customBudgets)
	if err != nil {
		t.Fatalf("GenerateG4ArtifactsWithBudgets: %v", err)
	}
	if len(rep.Budgets) != len(customBudgets) {
		t.Fatalf("got budgets %v, want %v", rep.Budgets, customBudgets)
	}
	if _, err := os.Stat(csvOut); err != nil {
		t.Errorf("expected CSV output: %v", err)
	}
	if _, err := os.Stat(svgOut); err != nil {
		t.Errorf("expected SVG output: %v", err)
	}
}
