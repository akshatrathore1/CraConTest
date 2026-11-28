package exp

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// B0SummaryPoint captures aggregated discovery statistics for B0 random across multiple ranking seeds.
type B0SummaryPoint struct {
	Budget      int
	FailuresMin int
	FailuresMax int
	FailuresAvg float64
	BugsMin     int
	BugsMax     int
	BugsAvg     float64
}

// StrategyCurve holds the evaluated points for a strategy.
type StrategyCurve struct {
	Name   string
	Points []Point
}

// G4RunReport collects discovery curve results for Gate G4.
type G4RunReport struct {
	RunID     string
	FaultID   string
	Universe  int
	Budgets   []int
	B0Summary []B0SummaryPoint
	Curves    []StrategyCurve // B4, B5, B6, etc.
}

// EvaluateG4 performs offline evaluation on cached universe and outcomes without re-executing any candidates:
// - B0 evaluated over rankingSeeds, with min, max, and mean computed per budget.
// - B4 (combined), B5 (representative), and B6 (sync-boundary) evaluated deterministically.
func EvaluateG4(
	runID string,
	faultID string,
	universe []candidate.Candidate,
	view *graph.View,
	outcomes []oracle.Outcome,
	budgets []int,
	rankingSeeds []uint64,
) (*G4RunReport, error) {
	if view == nil {
		return nil, errors.New("g4: graph view cannot be nil")
	}
	if len(budgets) == 0 {
		return nil, errors.New("g4: budgets cannot be empty")
	}
	if err := ValidateTable(universe, outcomes); err != nil {
		return nil, fmt.Errorf("g4: %w", err)
	}

	report := &G4RunReport{
		RunID:    runID,
		FaultID:  faultID,
		Universe: len(universe),
		Budgets:  append([]int{}, budgets...),
	}

	// 1. Evaluate B0 random across seeds.
	if len(rankingSeeds) == 0 {
		rankingSeeds = CanonicalB0Seeds()
	}

	allB0Points := make([][]Point, len(rankingSeeds))
	for i, seed := range rankingSeeds {
		randStrat := strategy.NewRandom(seed)
		ranked := randStrat.Rank(view, universe)
		pts, err := Evaluate(universe, ranked, outcomes, budgets)
		if err != nil {
			return nil, fmt.Errorf("eval b0 seed %d: %w", seed, err)
		}
		allB0Points[i] = pts
	}

	report.B0Summary = make([]B0SummaryPoint, len(budgets))
	for bIdx, b := range budgets {
		minFail, maxFail := math.MaxInt32, -1
		minBug, maxBug := math.MaxInt32, -1
		var sumFail, sumBug float64

		for sIdx := range rankingSeeds {
			pt := allB0Points[sIdx][bIdx]
			if pt.Failures < minFail {
				minFail = pt.Failures
			}
			if pt.Failures > maxFail {
				maxFail = pt.Failures
			}
			sumFail += float64(pt.Failures)

			if pt.DistinctBugs < minBug {
				minBug = pt.DistinctBugs
			}
			if pt.DistinctBugs > maxBug {
				maxBug = pt.DistinctBugs
			}
			sumBug += float64(pt.DistinctBugs)
		}

		n := float64(len(rankingSeeds))
		report.B0Summary[bIdx] = B0SummaryPoint{
			Budget:      b,
			FailuresMin: minFail,
			FailuresMax: maxFail,
			FailuresAvg: sumFail / n,
			BugsMin:     minBug,
			BugsMax:     maxBug,
			BugsAvg:     sumBug / n,
		}
	}

	// 2. Evaluate deterministic strategies: B4 (combined), B5 (representative), B6 (sync-boundary).
	combStrat, err := strategy.NewCombined(strategy.DefaultWeights())
	if err != nil {
		return nil, fmt.Errorf("init combined: %w", err)
	}

	strats := []struct {
		name  string
		strat strategy.Strategy
	}{
		{"b4_combined", combStrat},
		{"b5_representative", strategy.NewRepresentative()},
		{"b6_sync_boundary", strategy.NewSyncBoundary()},
	}

	for _, s := range strats {
		ranked := s.strat.Rank(view, universe)
		pts, err := Evaluate(universe, ranked, outcomes, budgets)
		if err != nil {
			return nil, fmt.Errorf("eval %s: %w", s.name, err)
		}
		report.Curves = append(report.Curves, StrategyCurve{
			Name:   s.name,
			Points: pts,
		})
	}

	return report, nil
}

// WriteG4CSV writes discovery curve metrics as CSV.
func WriteG4CSV(w io.Writer, report *G4RunReport) error {
	if report == nil {
		return errors.New("g4 csv: nil report")
	}
	writer := csv.NewWriter(w)
	writer.UseCRLF = false

	header := []string{
		"strategy", "budget", "tested", "failures_mean", "failures_min", "failures_max",
		"distinct_bugs_mean", "distinct_bugs_min", "distinct_bugs_max",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// B0 summary rows
	for _, b0 := range report.B0Summary {
		row := []string{
			"b0_random",
			strconv.Itoa(b0.Budget),
			strconv.Itoa(b0.Budget),
			fmt.Sprintf("%.2f", b0.FailuresAvg),
			strconv.Itoa(b0.FailuresMin),
			strconv.Itoa(b0.FailuresMax),
			fmt.Sprintf("%.2f", b0.BugsAvg),
			strconv.Itoa(b0.BugsMin),
			strconv.Itoa(b0.BugsMax),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	// Deterministic strategy rows: For deterministic strategies, failures_mean, failures_min,
	// and failures_max (and likewise distinct_bugs_*) are identical by definition, as a single
	// ranking has zero variance across identical evaluations (unlike b0_random which aggregates
	// over multiple shuffle seeds).
	for _, c := range report.Curves {
		for _, pt := range c.Points {
			row := []string{
				c.Name,
				strconv.Itoa(pt.Budget),
				strconv.Itoa(pt.Tested),
				strconv.Itoa(pt.Failures),
				strconv.Itoa(pt.Failures),
				strconv.Itoa(pt.Failures),
				strconv.Itoa(pt.DistinctBugs),
				strconv.Itoa(pt.DistinctBugs),
				strconv.Itoa(pt.DistinctBugs),
			}
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	}

	writer.Flush()
	return writer.Error()
}

// niceIntTicks chooses evenly spaced integer axis ticks [0, step, ..., axisMax] covering [0, maxRaw].
func niceIntTicks(maxRaw float64, fallback int) (float64, []int) {
	maxVal := int(math.Ceil(maxRaw))
	if maxVal <= 0 {
		maxVal = fallback
	}
	if maxVal <= 5 {
		ticks := make([]int, maxVal+1)
		for i := 0; i <= maxVal; i++ {
			ticks[i] = i
		}
		return float64(maxVal), ticks
	}

	step := 1
	pow := 1
	multipliers := []int{1, 2, 5}
outer:
	for {
		for _, m := range multipliers {
			cand := m * pow
			if (maxVal+cand-1)/cand <= 6 {
				step = cand
				break outer
			}
		}
		pow *= 10
	}

	nSteps := (maxVal + step - 1) / step
	if nSteps < 1 {
		nSteps = 1
	}
	axisMax := nSteps * step
	ticks := make([]int, nSteps+1)
	for i := 0; i <= nSteps; i++ {
		ticks[i] = i * step
	}
	return float64(axisMax), ticks
}

// WriteG4PlotSVG generates an SVG discovery-curve plot.
func WriteG4PlotSVG(w io.Writer, report *G4RunReport) error {
	if report == nil {
		return errors.New("plot: nil report")
	}

	width := 800
	height := 500
	margin := 70
	plotW := width - 2*margin
	plotH := height - 2*margin

	var writeErr error
	writef := func(format string, args ...any) {
		if writeErr != nil {
			return
		}
		_, writeErr = fmt.Fprintf(w, format, args...)
	}

	rawMaxBudget := float64(report.Universe)
	for _, b := range report.Budgets {
		if float64(b) > rawMaxBudget {
			rawMaxBudget = float64(b)
		}
	}
	for _, b0 := range report.B0Summary {
		if float64(b0.Budget) > rawMaxBudget {
			rawMaxBudget = float64(b0.Budget)
		}
	}
	for _, c := range report.Curves {
		for _, pt := range c.Points {
			if float64(pt.Budget) > rawMaxBudget {
				rawMaxBudget = float64(pt.Budget)
			}
		}
	}
	maxBudget, xTicks := niceIntTicks(rawMaxBudget, 100)

	rawMaxFail := 0.0
	for _, b0 := range report.B0Summary {
		if float64(b0.FailuresMax) > rawMaxFail {
			rawMaxFail = float64(b0.FailuresMax)
		}
		if b0.FailuresAvg > rawMaxFail {
			rawMaxFail = b0.FailuresAvg
		}
	}
	for _, c := range report.Curves {
		for _, pt := range c.Points {
			if float64(pt.Failures) > rawMaxFail {
				rawMaxFail = float64(pt.Failures)
			}
		}
	}
	maxFail, yTicks := niceIntTicks(rawMaxFail, 10)

	xScale := func(val float64) float64 {
		return float64(margin) + (val/maxBudget)*float64(plotW)
	}
	yScale := func(val float64) float64 {
		return float64(height-margin) - (val/maxFail)*float64(plotH)
	}

	writef(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d">
  <style>
    .title { font: bold 16px sans-serif; fill: #1f2937; }
    .label { font: 12px sans-serif; fill: #4b5563; }
    .legend { font: 11px sans-serif; fill: #374151; }
    .grid { stroke: #e5e7eb; stroke-width: 1; }
    .axis { stroke: #9ca3af; stroke-width: 1.5; }
    .b0-line { stroke: #6b7280; stroke-width: 2.5; stroke-dasharray: 4,3; stroke-opacity: 0.9; fill: none; }
    .b4-line { stroke: #2563eb; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .b5-line { stroke: #10b981; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .b6-line { stroke: #f59e0b; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .dcg-line { stroke: #dc2626; stroke-width: 3; stroke-opacity: 0.9; fill: none; }
    .dcg-pr-line { stroke: #991b1b; stroke-width: 3; stroke-dasharray: 6,2; stroke-opacity: 0.9; fill: none; }
    .dcg-flag-line { stroke: #ea580c; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .r1-line { stroke: #db2777; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .r2-line { stroke: #7c3aed; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .rhg-line { stroke: #0891b2; stroke-width: 2.5; stroke-opacity: 0.9; fill: none; }
    .fifo-line { stroke: #9ca3af; stroke-width: 2; stroke-opacity: 0.9; fill: none; }
    .lifo-line { stroke: #475569; stroke-width: 2; stroke-opacity: 0.9; fill: none; }
    .stride-line { stroke: #d97706; stroke-width: 2; stroke-opacity: 0.9; fill: none; }
  </style>
  <rect width="100%%" height="100%%" fill="#ffffff" />
  <text x="%d" y="35" class="title" text-anchor="middle">Discovery Curves: %s (%s)</text>
`, width, height, width, height, width/2, html.EscapeString(report.RunID), html.EscapeString(report.FaultID))

	// Grid & Axes
	for _, yTick := range yTicks {
		yPx := yScale(float64(yTick))
		writef(`  <line x1="%d" y1="%.1f" x2="%d" y2="%.1f" class="grid" />
  <text x="%d" y="%.1f" class="label" text-anchor="end" alignment-baseline="middle">%d</text>
`, margin, yPx, width-margin, yPx, margin-10, yPx, yTick)
	}

	for _, xTick := range xTicks {
		xPx := xScale(float64(xTick))
		writef(`  <line x1="%.1f" y1="%d" x2="%.1f" y2="%d" class="grid" />
  <text x="%.1f" y="%d" class="label" text-anchor="middle">%d</text>
`, xPx, margin, xPx, height-margin, xPx, height-margin+20, xTick)
	}

	// Axis lines
	writef(`  <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis" />
  <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis" />
  <text x="%d" y="%d" class="label" text-anchor="middle">Test Budget</text>
  <text x="20" y="%d" class="label" text-anchor="middle" transform="rotate(-90, 20, %d)">Failures Discovered</text>
`, margin, height-margin, width-margin, height-margin,
		margin, margin, margin, height-margin,
		width/2, height-15,
		height/2, height/2)

	// Plot B0 curve when summary points are present
	if len(report.B0Summary) > 0 {
		writef(`  <path class="b0-line" d="`)
		for i, b0 := range report.B0Summary {
			cmd := "L"
			if i == 0 {
				cmd = "M"
			}
			writef(`%s%.1f,%.1f `, cmd, xScale(float64(b0.Budget)), yScale(b0.FailuresAvg))
		}
		writef(`" />
`)
	}

	// Plot strategy curves (supports both baseline and modern strategies)
	classes := map[string]string{
		"b4_combined":       "b4-line",
		"combined":          "b4-line",
		"b5_representative": "b5-line",
		"representative":    "b5-line",
		"b6_sync_boundary":  "b6-line",
		"sync-boundary":     "b6-line",
		"dcg":               "dcg-line",
		"dcg-pr":            "dcg-pr-line",
		"dcg-flag-only":     "dcg-flag-line",
		"r1":                "r1-line",
		"r2":                "r2-line",
		"rhg":               "rhg-line",
		"fifo":              "fifo-line",
		"lifo":              "lifo-line",
		"stride":            "stride-line",
	}
	for _, c := range report.Curves {
		if len(c.Points) == 0 {
			continue
		}
		cls, ok := classes[c.Name]
		if !ok {
			cls = "b0-line"
		}
		writef(`  <path class="%s" d="`, cls)
		for i, pt := range c.Points {
			cmd := "L"
			if i == 0 {
				cmd = "M"
			}
			writef(`%s%.1f,%.1f `, cmd, xScale(float64(pt.Budget)), yScale(float64(pt.Failures)))
		}
		writef(`" />
`)
	}

	// Dynamic legend based on non-empty curves present
	type legendItem struct {
		label string
		class string
	}
	var legendItems []legendItem
	if len(report.B0Summary) > 0 {
		legendItems = append(legendItems, legendItem{label: "B0 Random (mean)", class: "b0-line"})
	}
	for _, c := range report.Curves {
		if len(c.Points) == 0 {
			continue
		}
		cls, ok := classes[c.Name]
		if !ok {
			cls = "b0-line"
		}
		label := c.Name
		switch c.Name {
		case "b4_combined", "combined":
			label = "B4 Combined"
		case "b5_representative", "representative":
			label = "B5 Representative"
		case "b6_sync_boundary", "sync-boundary":
			label = "B6 Sync-Boundary"
		case "dcg":
			label = "DCG"
		case "dcg-pr":
			label = "DCG-PR"
		case "dcg-flag-only":
			label = "DCG Flag-Only"
		case "r1":
			label = "R1 (0%-First)"
		case "r2":
			label = "R2 (State-Rep)"
		case "rhg":
			label = "RHG"
		case "fifo":
			label = "B1 FIFO"
		case "lifo":
			label = "B2 LIFO"
		case "stride":
			label = "B3 Stride"
		}
		legendItems = append(legendItems, legendItem{label: label, class: cls})
	}

	if len(legendItems) > 0 {
		cols := 1
		rowStep := 20
		if len(legendItems)*rowStep+15 > plotH-20 {
			cols = 2
			rowStep = 18
		}
		rows := (len(legendItems) + cols - 1) / cols
		legWidth := cols * 175
		legX := width - margin - legWidth - 10
		legHeight := rows*rowStep + 14
		// Place legend in bottom-right so it does not occlude the top-right convergence corner.
		legY := height - margin - legHeight - 10
		if legY < margin+5 {
			legY = margin + 5
		}
		writef(`  <rect x="%d" y="%d" width="%d" height="%d" fill="#f9fafb" fill-opacity="0.9" stroke="#d1d5db" rx="4" />
`, legX, legY, legWidth, legHeight)
		for idx, item := range legendItems {
			col := idx / rows
			row := idx % rows
			colX := legX + col*175
			itemY := legY + rowStep*(row+1)
			writef(`  <line x1="%d" y1="%d" x2="%d" y2="%d" class="%s" />
  <text x="%d" y="%d" class="legend">%s</text>
`, colX+10, itemY, colX+35, itemY, item.class, colX+42, itemY+4, html.EscapeString(item.label))
		}
	}
	writef("</svg>\n")

	return writeErr
}

// GenerateG4Artifacts loads cached outcomes and universe from runDir, executes EvaluateG4
// with default budget steps, and saves CSV and SVG discovery curves atomically without candidate re-execution.
func GenerateG4Artifacts(runDir, csvOut, svgOut string) (*G4RunReport, error) {
	return GenerateG4ArtifactsWithBudgets(runDir, csvOut, svgOut, nil)
}

// GenerateG4ArtifactsWithBudgets loads cached outcomes and universe from runDir, executes EvaluateG4
// at the specified budgets (or defaults when budgets is empty), and saves CSV and SVG discovery curves atomically.
func GenerateG4ArtifactsWithBudgets(runDir, csvOut, svgOut string, budgets []int) (*G4RunReport, error) {
	metaFile, err := os.Open(filepath.Join(runDir, "run.json"))
	if err != nil {
		return nil, fmt.Errorf("open run.json: %w", err)
	}
	var meta struct {
		SchemaVersion int    `json:"schema_version"`
		RunID         string `json:"run_id"`
		FaultID       string `json:"fault_id"`
		BaseSeed      uint64 `json:"base_seed"`
	}
	decodeErr := json.NewDecoder(metaFile).Decode(&meta)
	closeErr := metaFile.Close()
	if decodeErr != nil {
		return nil, fmt.Errorf("decode run.json: %w", decodeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close run.json: %w", closeErr)
	}
	if meta.SchemaVersion != Version {
		return nil, fmt.Errorf("run.json: unsupported or missing schema_version %d", meta.SchemaVersion)
	}

	traceFile, err := os.Open(filepath.Join(runDir, "trace.json"))
	if err != nil {
		return nil, fmt.Errorf("open trace.json: %w", err)
	}
	events, decodeErr := trace.Decode(traceFile)
	closeErr = traceFile.Close()
	if decodeErr != nil {
		return nil, fmt.Errorf("decode trace.json: %w", decodeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close trace.json: %w", closeErr)
	}

	univFile, err := os.Open(filepath.Join(runDir, "universe.json"))
	if err != nil {
		return nil, fmt.Errorf("open universe.json: %w", err)
	}
	universe, decodeErr := candidate.Decode(univFile)
	closeErr = univFile.Close()
	if decodeErr != nil {
		return nil, fmt.Errorf("decode universe.json: %w", decodeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close universe.json: %w", closeErr)
	}

	outcomesFile, err := os.Open(filepath.Join(runDir, "outcomes.json"))
	if err != nil {
		return nil, fmt.Errorf("open outcomes.json: %w", err)
	}
	outcomes, decodeErr := DecodeOutcomes(outcomesFile)
	closeErr = outcomesFile.Close()
	if decodeErr != nil {
		return nil, fmt.Errorf("decode outcomes.json: %w", decodeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close outcomes.json: %w", closeErr)
	}
	outcomes = NormalizeTable(outcomes)

	view, err := graph.Build(events)
	if err != nil {
		return nil, fmt.Errorf("build graph: %w", err)
	}

	evalBudgets := append([]int(nil), budgets...)
	if len(evalBudgets) == 0 {
		// Default budget steps: 0, 1, 5, 10, 25, 50, 100, 200, N
		steps := []int{0, 1, 5, 10, 25, 50, 100, 200, len(universe)}
		seen := make(map[int]bool)
		for _, s := range steps {
			if s <= len(universe) && !seen[s] {
				seen[s] = true
				evalBudgets = append(evalBudgets, s)
			}
		}
	}

	report, err := EvaluateG4(meta.RunID, meta.FaultID, universe, view, outcomes, evalBudgets, nil)
	if err != nil {
		return nil, err
	}

	if csvOut != "" {
		if err := writeBytesAtomic(csvOut, func(w io.Writer) error {
			return WriteG4CSV(w, report)
		}); err != nil {
			return nil, fmt.Errorf("write csv: %w", err)
		}
	}

	if svgOut != "" {
		if err := writeBytesAtomic(svgOut, func(w io.Writer) error {
			return WriteG4PlotSVG(w, report)
		}); err != nil {
			return nil, fmt.Errorf("write svg: %w", err)
		}
	}

	return report, nil
}
