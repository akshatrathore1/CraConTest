// Command cracontest is the unified CLI driver for recording workloads,
// planning candidate rankings, exploring crash states, and evaluating discovery curves.
package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// RunMetadata records complete environment and execution parameters for a run directory.
type RunMetadata struct {
	SchemaVersion int             `json:"schema_version"`
	RunID         string          `json:"run_id"`
	Timestamp     string          `json:"timestamp"`
	FaultID       string          `json:"fault_id"`
	BaseSeed      uint64          `json:"base_seed"`
	Policies      []int           `json:"policies"`
	Workload      workload.Config `json:"workload"`
	PebbleVersion string          `json:"pebble_version"`
	GoVersion     string          `json:"go_version"`
	GOOS          string          `json:"goos"`
	GOARCH        string          `json:"goarch"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	var err error
	switch command {
	case "record":
		err = runRecord(args)
	case "plan":
		err = runPlan(args)
	case "explore":
		err = runExploreCmd(args)
	case "evaluate":
		err = runEvaluate(args)
	case "help", "-h", "--help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "cracontest: unknown command %q\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: cracontest <command> [options]

Commands:
  record    Execute deterministic workload and record VFS trace and mutation ledger
  plan      Build candidate universe and compute strategy rankings
  explore   Execute crash states and produce outcome table with journal
  evaluate  Evaluate discovery curves and generate CSV summary
`)
}

// ----------------------------------------------------------------------
// record
// ----------------------------------------------------------------------

func runRecord(args []string) error {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	specPath := fs.String("spec", "", "path to workload config JSON (e.g. experiments/configs/smoke.json)")
	outDir := fs.String("out", "", "output run directory")
	runDir := fs.String("run", "", "output run directory (alias for -out)")
	faultID := fs.String("fault", faults.MUT0, "seeded fault ID: "+strings.Join(faults.IDs(), ", "))
	if err := fs.Parse(args); err != nil {
		return err
	}

	targetDir := *outDir
	if targetDir == "" {
		targetDir = *runDir
	}
	if targetDir == "" {
		return errors.New("record: must specify -out or -run directory")
	}

	cfg := workload.DefaultConfig()
	if *specPath != "" {
		data, err := os.ReadFile(*specPath)
		if err != nil {
			return fmt.Errorf("read spec: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return fmt.Errorf("parse spec: %w", err)
		}
	}

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create run dir: %w", err)
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	faultFS, err := faults.New(*faultID, rec)
	if err != nil {
		return fmt.Errorf("init fault %q: %w", *faultID, err)
	}

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            faultFS,
		Logger:        recovery.PanicLogger{},
		MemTableSize:  64 * 1024,
		EventListener: &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		return fmt.Errorf("open pebble: %w", err)
	}

	rawHist, err := workload.Run(db, rec, cfg)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("run workload: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close pebble: %w", err)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		return fmt.Errorf("validate trace: %w", err)
	}

	correlated, err := workload.Correlate(rawHist, traceEvents)
	if err != nil {
		return fmt.Errorf("correlate history: %w", err)
	}
	if err := history.Validate(correlated); err != nil {
		return fmt.Errorf("validate history: %w", err)
	}

	meta := RunMetadata{
		SchemaVersion: 1,
		RunID:         filepath.Base(targetDir),
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		FaultID:       *faultID,
		BaseSeed:      cfg.Seed,
		Policies:      []int{0, 100},
		Workload:      cfg,
		PebbleVersion: "v2.1.7",
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
	}

	// Write run.json
	metaFile, err := os.Create(filepath.Join(targetDir, "run.json"))
	if err != nil {
		return fmt.Errorf("create run.json: %w", err)
	}
	defer metaFile.Close()
	enc := json.NewEncoder(metaFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(meta); err != nil {
		return fmt.Errorf("encode run.json: %w", err)
	}

	// Write trace.json
	traceFile, err := os.Create(filepath.Join(targetDir, "trace.json"))
	if err != nil {
		return fmt.Errorf("create trace.json: %w", err)
	}
	defer traceFile.Close()
	if err := trace.Encode(traceFile, traceEvents); err != nil {
		return fmt.Errorf("encode trace.json: %w", err)
	}

	// Write history.json
	histFile, err := os.Create(filepath.Join(targetDir, "history.json"))
	if err != nil {
		return fmt.Errorf("create history.json: %w", err)
	}
	defer histFile.Close()
	if err := history.Encode(histFile, correlated); err != nil {
		return fmt.Errorf("encode history.json: %w", err)
	}

	fmt.Printf("Recorded run %s (fault %s): %d events, %d history entries -> %s\n",
		meta.RunID, meta.FaultID, len(traceEvents), len(correlated), targetDir)
	return nil
}

// ----------------------------------------------------------------------
// plan
// ----------------------------------------------------------------------

func runPlan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	runDir := fs.String("run", "", "path to run directory")
	rankingSeed := fs.Uint64("seed", 42, "ranking seed for randomized strategies")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return errors.New("plan: must specify -run directory")
	}

	meta, traceEvents, _, err := loadRunData(*runDir, false)
	if err != nil {
		return err
	}

	policies := meta.Policies
	if len(policies) == 0 {
		policies = []int{0, 100}
	}

	universe, err := candidate.Build(traceEvents, meta.BaseSeed, policies)
	if err != nil {
		return fmt.Errorf("build universe: %w", err)
	}

	univFile, err := os.Create(filepath.Join(*runDir, "universe.json"))
	if err != nil {
		return fmt.Errorf("create universe.json: %w", err)
	}
	defer univFile.Close()
	if err := candidate.Encode(univFile, universe); err != nil {
		return fmt.Errorf("encode universe.json: %w", err)
	}

	view, err := graph.Build(traceEvents)
	if err != nil {
		return fmt.Errorf("build graph: %w", err)
	}

	combinedStrat, err := strategy.NewCombined(strategy.DefaultWeights)
	if err != nil {
		return fmt.Errorf("init combined strategy: %w", err)
	}

	strategies := []struct {
		strat strategy.Strategy
		seed  uint64
		code  string
	}{
		{strategy.NewRandom(*rankingSeed), *rankingSeed, "b0"},
		{combinedStrat, 0, "b4"},
		{strategy.NewRepresentative(), 0, "b5"},
		{strategy.NewSyncBoundary(), 0, "b6"},
	}

	for _, s := range strategies {
		ranked := s.strat.Rank(view, universe)
		if err := strategy.ValidatePermutation(universe, ranked); err != nil {
			return fmt.Errorf("validate permutation for %s: %w", s.strat.Name(), err)
		}

		doc := exp.RankingDocument{
			SchemaVersion: 1,
			Strategy:      s.strat.Name(),
			RankingSeed:   s.seed,
			Candidates:    ranked,
		}

		fname := fmt.Sprintf("ranking_%s.json", s.code)
		rFile, err := os.Create(filepath.Join(*runDir, fname))
		if err != nil {
			return fmt.Errorf("create %s: %w", fname, err)
		}
		if err := exp.EncodeRanking(rFile, doc); err != nil {
			_ = rFile.Close()
			return fmt.Errorf("encode %s: %w", fname, err)
		}
		_ = rFile.Close()
	}

	fmt.Printf("Planned %d universe candidates with 4 strategies (b0, b4, b5, b6) -> %s\n",
		len(universe), *runDir)
	return nil
}

// ----------------------------------------------------------------------
// explore
// ----------------------------------------------------------------------

func runExploreCmd(args []string) error {
	fs := flag.NewFlagSet("explore", flag.ExitOnError)
	runDir := fs.String("run", "", "path to run directory")
	timeout := fs.Duration("timeout", 5*time.Second, "per-candidate SafeOpen timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return errors.New("explore: must specify -run directory")
	}

	_, traceEvents, histEntries, err := loadRunData(*runDir, true)
	if err != nil {
		return err
	}

	uFile, err := os.Open(filepath.Join(*runDir, "universe.json"))
	if err != nil {
		return fmt.Errorf("open universe.json: %w", err)
	}
	defer uFile.Close()
	universe, err := candidate.Decode(uFile)
	if err != nil {
		return fmt.Errorf("decode universe.json: %w", err)
	}

	journalPath := filepath.Join(*runDir, "journal.ndjson")
	res, err := exp.RunExplore(exp.RunnerConfig{
		Universe:    universe,
		Events:      traceEvents,
		History:     histEntries,
		Dir:         "db",
		Timeout:     *timeout,
		JournalPath: journalPath,
	})
	if err != nil {
		return fmt.Errorf("run explore: %w", err)
	}

	// Write outcomes.json
	outFile, err := os.Create(filepath.Join(*runDir, "outcomes.json"))
	if err != nil {
		return fmt.Errorf("create outcomes.json: %w", err)
	}
	defer outFile.Close()
	if err := exp.EncodeOutcomes(outFile, universe, res.Outcomes); err != nil {
		return fmt.Errorf("encode outcomes.json: %w", err)
	}

	fmt.Printf("Explored %d candidates in %v: valid=%d, viol=%d, open_err=%d, panic=%d, hang=%d, oracle_err=%d\n",
		res.Summary.TotalCandidates, res.Summary.TotalTime,
		res.Summary.Valid, res.Summary.Violations, res.Summary.OpenErrors,
		res.Summary.Panics, res.Summary.Hangs, res.Summary.OracleErrors)
	return nil
}

// ----------------------------------------------------------------------
// evaluate
// ----------------------------------------------------------------------

func runEvaluate(args []string) error {
	fs := flag.NewFlagSet("evaluate", flag.ExitOnError)
	runDir := fs.String("run", "", "path to run directory")
	budgetsFlag := fs.String("budgets", "", "comma-separated budgets (e.g. 0,1,5,10,25,50,100)")
	discoveryOut := fs.String("discovery", "", "optional path for the discovery-curve CSV (B0 averaged over fixed seeds)")
	plotOut := fs.String("plot", "", "optional path for the discovery-curve SVG plot")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return errors.New("evaluate: must specify -run directory")
	}

	// Load universe.
	uFile, err := os.Open(filepath.Join(*runDir, "universe.json"))
	if err != nil {
		return fmt.Errorf("open universe.json: %w", err)
	}
	defer uFile.Close()
	universe, err := candidate.Decode(uFile)
	if err != nil {
		return fmt.Errorf("decode universe.json: %w", err)
	}

	// Load outcomes.
	oFile, err := os.Open(filepath.Join(*runDir, "outcomes.json"))
	if err != nil {
		return fmt.Errorf("open outcomes.json: %w", err)
	}
	defer oFile.Close()
	outcomes, err := exp.DecodeOutcomes(oFile)
	if err != nil {
		return fmt.Errorf("decode outcomes.json: %w", err)
	}

	// Determine budgets.
	var budgets []int
	if *budgetsFlag != "" {
		for _, part := range strings.Split(*budgetsFlag, ",") {
			val, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				return fmt.Errorf("invalid budget %q: %w", part, err)
			}
			budgets = append(budgets, val)
		}
	} else {
		// Default budget sequence up to universe size.
		steps := []int{0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, len(universe)}
		seen := make(map[int]bool)
		for _, s := range steps {
			if s <= len(universe) && !seen[s] {
				seen[s] = true
				budgets = append(budgets, s)
			}
		}
	}

	// Find ranking files.
	matches, err := filepath.Glob(filepath.Join(*runDir, "ranking_*.json"))
	if err != nil || len(matches) == 0 {
		return fmt.Errorf("no ranking_*.json files found in %s", *runDir)
	}

	csvFile, err := os.Create(filepath.Join(*runDir, "curves.csv"))
	if err != nil {
		return fmt.Errorf("create curves.csv: %w", err)
	}
	defer csvFile.Close()

	w := csv.NewWriter(csvFile)
	defer w.Flush()

	if err := w.Write([]string{"strategy", "budget", "tested", "failures", "distinct_bugs"}); err != nil {
		return err
	}

	fmt.Println("Strategy evaluation curves:")
	fmt.Printf("%-18s %-8s %-8s %-10s %-14s\n", "Strategy", "Budget", "Tested", "Failures", "DistinctBugs")
	fmt.Println(strings.Repeat("-", 62))

	for _, rPath := range matches {
		rFile, err := os.Open(rPath)
		if err != nil {
			return fmt.Errorf("open %s: %w", rPath, err)
		}
		rDoc, err := exp.DecodeRanking(rFile)
		_ = rFile.Close()
		if err != nil {
			return fmt.Errorf("decode %s: %w", rPath, err)
		}

		points, err := exp.Evaluate(universe, rDoc.Candidates, outcomes, budgets)
		if err != nil {
			return fmt.Errorf("evaluate %s: %w", rDoc.Strategy, err)
		}

		for _, p := range points {
			record := []string{
				rDoc.Strategy,
				strconv.Itoa(p.Budget),
				strconv.Itoa(p.Tested),
				strconv.Itoa(p.Failures),
				strconv.Itoa(p.DistinctBugs),
			}
			if err := w.Write(record); err != nil {
				return err
			}
			fmt.Printf("%-18s %-8d %-8d %-10d %-14d\n",
				rDoc.Strategy, p.Budget, p.Tested, p.Failures, p.DistinctBugs)
		}
	}

	fmt.Printf("\nSaved discovery curves to %s\n", filepath.Join(*runDir, "curves.csv"))

	if err := writePositions(*runDir, universe, outcomes, matches); err != nil {
		return err
	}

	if (*discoveryOut == "") != (*plotOut == "") {
		return errors.New("evaluate: -discovery and -plot must be given together")
	}
	if *discoveryOut != "" {
		if _, err := exp.GenerateG4Artifacts(*runDir, *discoveryOut, *plotOut); err != nil {
			return fmt.Errorf("generate discovery artifacts: %w", err)
		}
		fmt.Printf("Saved discovery CSV to %s and plot to %s\n", *discoveryOut, *plotOut)
	}
	return nil
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

func loadRunData(dir string, loadHistory bool) (*RunMetadata, []trace.Event, []history.Entry, error) {
	metaFile, err := os.Open(filepath.Join(dir, "run.json"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open run.json: %w", err)
	}
	defer metaFile.Close()

	var meta RunMetadata
	if err := json.NewDecoder(metaFile).Decode(&meta); err != nil {
		return nil, nil, nil, fmt.Errorf("decode run.json: %w", err)
	}

	traceFile, err := os.Open(filepath.Join(dir, "trace.json"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open trace.json: %w", err)
	}
	defer traceFile.Close()

	events, err := trace.Decode(traceFile)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode trace.json: %w", err)
	}

	var hist []history.Entry
	if loadHistory {
		histFile, err := os.Open(filepath.Join(dir, "history.json"))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("open history.json: %w", err)
		}
		defer histFile.Close()

		hist, err = history.Decode(histFile)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("decode history.json: %w", err)
		}
	}

	return &meta, events, hist, nil
}

// writePositions records, for every stored ranking and for random order over
// fixed seeds, the position of the first failure and the position at which
// every failure kind has appeared.
func writePositions(runDir string, universe []candidate.Candidate, outcomes []oracle.Outcome, rankingPaths []string) error {
	path := filepath.Join(runDir, "positions.csv")
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create positions.csv: %w", err)
	}
	defer file.Close()
	w := csv.NewWriter(file)
	if err := w.Write([]string{"strategy", "first_failure", "first_failure_p10", "first_failure_p90",
		"all_kinds", "all_kinds_p10", "all_kinds_p90", "failure_kinds"}); err != nil {
		return err
	}
	itoa := strconv.Itoa
	for _, rPath := range rankingPaths {
		rFile, err := os.Open(rPath)
		if err != nil {
			return err
		}
		doc, err := exp.DecodeRanking(rFile)
		_ = rFile.Close()
		if err != nil {
			return fmt.Errorf("decode %s: %w", rPath, err)
		}
		p, err := exp.DiscoveryPositions(universe, doc.Candidates, outcomes)
		if err != nil {
			return fmt.Errorf("positions %s: %w", doc.Strategy, err)
		}
		if err := w.Write([]string{doc.Strategy, itoa(p.FirstFailure), itoa(p.FirstFailure), itoa(p.FirstFailure),
			itoa(p.AllKinds), itoa(p.AllKinds), itoa(p.AllKinds), itoa(p.Kinds)}); err != nil {
			return err
		}
	}
	r, err := exp.RandomPositions(universe, outcomes, exp.RandomSeeds())
	if err != nil {
		return fmt.Errorf("random positions: %w", err)
	}
	if err := w.Write([]string{fmt.Sprintf("random_x%d", r.Seeds), itoa(r.FirstMedian), itoa(r.FirstP10), itoa(r.FirstP90),
		itoa(r.AllKindsMedian), itoa(r.AllKindsP10), itoa(r.AllKindsP90), itoa(r.Kinds)}); err != nil {
		return err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	fmt.Printf("Saved discovery positions to %s\n", path)
	return nil
}
