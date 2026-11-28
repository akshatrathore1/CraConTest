// Command cracontest is the unified CLI driver for recording workloads,
// planning candidate rankings, exploring crash states, and evaluating discovery curves.
package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// DefaultPebbleVersion defines the single-source-of-truth fallback Pebble version
// when dynamic build info is unavailable at runtime.
const DefaultPebbleVersion = exp.DefaultPebbleVersion

// detectPebbleVersion queries runtime build info to determine the active Pebble module version,
// falling back to DefaultPebbleVersion if build info is not embedded.
func detectPebbleVersion() string {
	return exp.DetectPebbleVersion()
}

// RunMetadata records complete environment and execution parameters for a run directory.
type RunMetadata struct {
	SchemaVersion int             `json:"schema_version"`
	RunID         string          `json:"run_id"`
	Timestamp     string          `json:"timestamp"`
	FaultID       string          `json:"fault_id"`
	BaseSeed      uint64          `json:"base_seed"`
	Policies      []int           `json:"policies"`
	Workload      workload.Config `json:"workload"`
	FaultConfig   *faults.Config  `json:"fault_config,omitempty"`
	PebbleVersion string          `json:"pebble_version"`
	GoVersion     string          `json:"go_version"`
	GOOS          string          `json:"goos"`
	GOARCH        string          `json:"goarch"`
	GitCommit     string          `json:"git_commit,omitempty"`
}

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runCLI(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		printUsage(stderr)
		return 1
	}

	command := args[0]
	cmdArgs := args[1:]

	var err error
	switch command {
	case "record":
		err = runRecordWithWriters(cmdArgs, stdout, stderr)
	case "plan":
		err = runPlanWithWriters(cmdArgs, stdout, stderr)
	case "explore":
		err = runExploreCmdWithWriters(cmdArgs, stdout, stderr)
	case "evaluate":
		err = runEvaluateWithWriters(cmdArgs, stdout, stderr)
	case "benchmark":
		err = runBenchmarkCmdWithWriters(cmdArgs, stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "cracontest: unknown command %q\n", command)
		printUsage(stderr)
		return 1
	}

	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

func writeFileAtomic(targetPath string, writeFn func(*os.File) error) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	base := filepath.Base(targetPath)
	tmp, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := writeFn(tmp); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func safeCloseDB(db *pebble.DB) (panicked bool, err error) {
	if db == nil {
		return false, nil
	}
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = fmt.Errorf("%v", r)
		}
	}()
	return false, db.Close()
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `Usage: cracontest <command> [options]

Commands:
  record    Execute deterministic workload and record VFS trace and mutation ledger
  plan      Build candidate universe and compute strategy rankings
  explore   Execute crash states and produce outcome table with journal
  evaluate  Evaluate discovery curves and generate CSV summary
  benchmark Execute multi-seed empirical benchmark across fault models and strategies
`)
}

func parseCSVParts(raw, cmd, label string) ([]string, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			return nil, fmt.Errorf("%s: empty element in %s %q", cmd, label, raw)
		}
		out = append(out, trimmed)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: %s cannot be empty", cmd, label)
	}
	return out, nil
}

func parsePoliciesFlag(raw, cmd string) ([]int, error) {
	parts, err := parseCSVParts(raw, cmd, "-policies")
	if err != nil {
		return nil, err
	}
	seen := make(map[int]bool, len(parts))
	policies := make([]int, 0, len(parts))
	for _, part := range parts {
		p, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid policy %q: %w", cmd, part, err)
		}
		if p < 0 || p > 100 {
			return nil, fmt.Errorf("%s: invalid policy %q (must be 0..100)", cmd, part)
		}
		if seen[p] {
			return nil, fmt.Errorf("%s: duplicate retention policy %d", cmd, p)
		}
		seen[p] = true
		policies = append(policies, p)
	}
	return policies, nil
}

func parseBudgetsFlag(raw string) ([]int, error) {
	parts, err := parseCSVParts(raw, "evaluate", "-budgets")
	if err != nil {
		return nil, err
	}
	budgets := make([]int, 0, len(parts))
	for i, part := range parts {
		val, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("evaluate: invalid budget %q: %w", part, err)
		}
		if val < 0 {
			return nil, fmt.Errorf("evaluate: negative budget %d", val)
		}
		if i > 0 && val <= budgets[i-1] {
			return nil, fmt.Errorf("evaluate: budgets must be strictly increasing and unique: %d <= %d", val, budgets[i-1])
		}
		budgets = append(budgets, val)
	}
	return budgets, nil
}

func parseSeedsFlag(raw string) ([]uint64, error) {
	parts, err := parseCSVParts(raw, "benchmark", "-seeds")
	if err != nil {
		return nil, err
	}
	seen := make(map[uint64]bool, len(parts))
	seeds := make([]uint64, 0, len(parts))
	for _, part := range parts {
		s, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("benchmark: invalid seed %q: %w", part, err)
		}
		if seen[s] {
			return nil, fmt.Errorf("benchmark: duplicate seed %d", s)
		}
		seen[s] = true
		seeds = append(seeds, s)
	}
	return seeds, nil
}

func parseFaultsFlag(raw string) ([]string, error) {
	parts, err := parseCSVParts(raw, "benchmark", "-faults")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]string, len(parts))
	for _, fid := range parts {
		norm, err := faults.NormalizeID(fid)
		if err != nil {
			return nil, fmt.Errorf("benchmark: invalid fault %q: %w", fid, err)
		}
		canon := norm
		if strings.ContainsRune(fid, '_') {
			canon = fid
		}
		if prev, dup := seen[canon]; dup {
			return nil, fmt.Errorf("benchmark: duplicate fault %q (conflicts with %q)", fid, prev)
		}
		seen[canon] = fid
	}
	return parts, nil
}

func decodeStrictJSONObject(data []byte, dst any) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("expected JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("trailing content: %w", err)
		}
		return errors.New("unexpected trailing JSON token")
	}
	return nil
}

func parseSpecStrict(specPath string, cfg *workload.Config) (hasKeyCount, hasValueBytes bool, err error) {
	data, err := os.ReadFile(filepath.Clean(specPath))
	if err != nil {
		return false, false, fmt.Errorf("read spec: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false, false, errors.New("parse spec: expected JSON object")
	}
	var rawMap map[string]json.RawMessage
	decMap := json.NewDecoder(bytes.NewReader(trimmed))
	if err := decMap.Decode(&rawMap); err != nil {
		return false, false, fmt.Errorf("parse spec: %w", err)
	}
	var trailingMap any
	if err := decMap.Decode(&trailingMap); !errors.Is(err, io.EOF) {
		return false, false, errors.New("parse spec: unexpected trailing JSON token")
	}
	if _, ok := rawMap["key_count"]; ok {
		hasKeyCount = true
	}
	if _, ok := rawMap["value_bytes"]; ok {
		hasValueBytes = true
	}
	if err := decodeStrictJSONObject(trimmed, cfg); err != nil {
		return false, false, fmt.Errorf("parse spec: %w", err)
	}
	return hasKeyCount, hasValueBytes, nil
}

type workloadFlagPtrs struct {
	ops        *int
	keyCount   *int
	valueBytes *int
	syncEvery  *int
	flushEvery *int
	compactAt  *int
}

func registerWorkloadFlags(fs *flag.FlagSet, defaults workload.Config) workloadFlagPtrs {
	return workloadFlagPtrs{
		ops:        fs.Int("ops", defaults.Operations, "operations per workload run"),
		keyCount:   fs.Int("key-count", defaults.KeyCount, "distinct key count"),
		valueBytes: fs.Int("value-bytes", defaults.ValueBytes, "payload value size in bytes"),
		syncEvery:  fs.Int("sync-every", defaults.SyncEvery, "client barrier interval in operations (0 = disabled)"),
		flushEvery: fs.Int("flush-every", defaults.FlushEvery, "memtable flush interval in operations (0 = disabled)"),
		compactAt:  fs.Int("compact-at", defaults.CompactAt, "manual compaction trigger operation (0 = auto/disabled)"),
	}
}

func applyWorkloadFlags(visited map[string]bool, wf workloadFlagPtrs, cfg *workload.Config) {
	if visited["ops"] {
		cfg.Operations = *wf.ops
	}
	if visited["key-count"] {
		cfg.KeyCount = *wf.keyCount
	}
	if visited["value-bytes"] {
		cfg.ValueBytes = *wf.valueBytes
	}
	if visited["sync-every"] {
		cfg.SyncEvery = *wf.syncEvery
	}
	if visited["flush-every"] {
		cfg.FlushEvery = *wf.flushEvery
	}
	if visited["compact-at"] {
		cfg.CompactAt = *wf.compactAt
	}
}

// ----------------------------------------------------------------------
// record
// ----------------------------------------------------------------------

func runRecord(args []string) error {
	return runRecordWithWriters(args, os.Stdout, os.Stderr)
}

func runRecordWithWriters(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("record", flag.ContinueOnError)
	fs.SetOutput(stderr)
	specPath := fs.String("spec", "", "path to workload config JSON (e.g. experiments/configs/smoke.json)")
	outDir := fs.String("out", "", "output run directory")
	runDir := fs.String("run", "", "output run directory (alias for -out)")
	faultID := fs.String("fault", faults.MUT0, "seeded fault ID: "+strings.Join(faults.IDs(), ", "))
	policiesFlag := fs.String("policies", "0,100", "comma-separated retention percentages in 0..100")
	ingestEveryFlag := fs.Int("ingest-every", 0, "emit an external SSTable ingest op every N operations (0 = disabled)")
	ratchetAtFlag := fs.Int("ratchet-at", 0, "ratchet format major version at operation N (0 = disabled)")
	barrierIngestFlag := fs.Bool("barrier-ingest", false, "execute WAL sync barrier prior to db.Ingest")
	presetFlag := fs.String("preset", "", "workload diversity preset (ycsb-a, ycsb-b, ycsb-f, overwrite-heavy, delete-heavy)")
	seedFlag := fs.Uint64("seed", 0, "workload and fault seed (overrides spec seed)")
	walDropKFlag := fs.Int("wal-drop-k", 0, "drop strictly the k-th WAL sync for MUT-5")
	dirDropKFlag := fs.Int("dir-drop-k", 0, "drop strictly the k-th directory sync for MUT-9")
	wf := registerWorkloadFlags(fs, workload.DefaultConfig())
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("record: unexpected positional arguments: %v", fs.Args())
	}

	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})

	if *outDir != "" && *runDir != "" {
		return errors.New("record: specify either -out or -run, not both")
	}

	policies, err := parsePoliciesFlag(*policiesFlag, "record")
	if err != nil {
		return err
	}

	if visited["wal-drop-k"] && *walDropKFlag <= 0 {
		return fmt.Errorf("record: -wal-drop-k must be > 0, got %d", *walDropKFlag)
	}
	if visited["dir-drop-k"] && *dirDropKFlag <= 0 {
		return fmt.Errorf("record: -dir-drop-k must be > 0, got %d", *dirDropKFlag)
	}

	targetDir := *outDir
	if targetDir == "" {
		targetDir = *runDir
	}
	if targetDir == "" {
		return errors.New("record: must specify -out or -run directory")
	}
	targetDir = filepath.Clean(targetDir)

	cfg := workload.DefaultConfig()
	var specHasKeyCount, specHasValueBytes bool
	if *specPath != "" {
		*specPath = filepath.Clean(*specPath)
		specHasKeyCount, specHasValueBytes, err = parseSpecStrict(*specPath, &cfg)
		if err != nil {
			return err
		}
	}

	applyWorkloadFlags(visited, wf, &cfg)
	if visited["seed"] {
		cfg.Seed = *seedFlag
	}
	if visited["ingest-every"] {
		cfg.IngestEvery = *ingestEveryFlag
	}
	if visited["ratchet-at"] {
		cfg.RatchetAt = *ratchetAtFlag
	}
	if visited["barrier-ingest"] {
		cfg.BarrierIngest = *barrierIngestFlag
	}
	if visited["preset"] {
		cfg.Preset = *presetFlag
	}
	if cfg.Preset != "" {
		if !visited["key-count"] && !specHasKeyCount {
			cfg.KeyCount = 0
		}
		if !visited["value-bytes"] && !specHasValueBytes {
			cfg.ValueBytes = 0
		}
	}

	dbDir, err := exp.NormalizeDBDir(cfg.Dir)
	if err != nil {
		return fmt.Errorf("record: invalid db dir: %w", err)
	}
	cfg.Dir = dbDir

	normFault, err := faults.NormalizeID(*faultID)
	if err != nil {
		return fmt.Errorf("init fault %q: %w", *faultID, err)
	}
	if normFault == faults.MUT8 && cfg.CompactAt == 0 {
		cfg.CompactAt = 80
	}
	if normFault == faults.MUT8 && cfg.CompactAt > cfg.Operations {
		return fmt.Errorf("record: MUT-8 compact_at (%d) exceeds operations (%d); fault would not trigger", cfg.CompactAt, cfg.Operations)
	}

	if err := workload.Validate(cfg); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create run dir: %w", err)
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	fCfg := faults.DefaultConfig(*faultID)
	fCfg.Seed = cfg.Seed
	if visited["wal-drop-k"] {
		fCfg.WALDropK = *walDropKFlag
	}
	if visited["dir-drop-k"] {
		fCfg.DirDropK = *dirDropKFlag
	}
	faultFS, err := faults.NewWithConfig(fCfg, rec)
	if err != nil {
		return fmt.Errorf("init fault %q: %w", *faultID, err)
	}
	cfg.FS = faultFS

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                          faultFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		EventListener:               &listener,
		DisableAutomaticCompactions: true,
		DisableTableStats:           true,
		L0CompactionThreshold:       1000,
		L0StopWritesThreshold:       1000,
	}
	if cfg.MaxManifestFileSize > 0 {
		opts.MaxManifestFileSize = cfg.MaxManifestFileSize
	}
	if cfg.RatchetAt > 0 {
		opts.FormatMajorVersion = pebble.FormatMinSupported
	}

	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		return fmt.Errorf("open pebble: %w", err)
	}

	rawHist, err := workload.Run(db, rec, cfg)
	if err != nil {
		_, _ = safeCloseDB(db)
		return fmt.Errorf("run workload: %w", err)
	}
	db.TestOnlyWaitForCleaning()
	if panicked, closeErr := safeCloseDB(db); panicked || closeErr != nil {
		if panicked {
			return fmt.Errorf("close pebble panic: %w", closeErr)
		}
		return fmt.Errorf("close pebble: %w", closeErr)
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
		Policies:      policies,
		Workload:      cfg,
		FaultConfig:   &fCfg,
		PebbleVersion: detectPebbleVersion(),
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		GitCommit:     exp.CurrentGitCommit(),
	}

	// Write run.json, trace.json, history.json atomically
	if err := writeFileAtomic(filepath.Join(targetDir, "run.json"), func(f *os.File) error {
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		return enc.Encode(meta)
	}); err != nil {
		return fmt.Errorf("write run.json: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(targetDir, "trace.json"), func(f *os.File) error {
		return trace.Encode(f, traceEvents)
	}); err != nil {
		return fmt.Errorf("write trace.json: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(targetDir, "history.json"), func(f *os.File) error {
		return history.Encode(f, correlated)
	}); err != nil {
		return fmt.Errorf("write history.json: %w", err)
	}

	fmt.Fprintf(stdout, "Recorded run %s (fault %s): %d events, %d history entries -> %s\n",
		meta.RunID, meta.FaultID, len(traceEvents), len(correlated), targetDir)
	return nil
}

// ----------------------------------------------------------------------
// plan
// ----------------------------------------------------------------------

func runPlan(args []string) error {
	return runPlanWithWriters(args, os.Stdout, os.Stderr)
}

func runPlanWithWriters(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runDir := fs.String("run", "", "path to run directory")
	rankingSeed := fs.Uint64("seed", 42, "ranking seed for randomized strategies")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("plan: unexpected positional arguments: %v", fs.Args())
	}
	if *runDir == "" {
		return errors.New("plan: must specify -run directory")
	}
	*runDir = filepath.Clean(*runDir)

	meta, traceEvents, _, err := loadRunData(*runDir, false)
	if err != nil {
		return err
	}

	universe, err := candidate.Build(traceEvents, meta.BaseSeed, meta.Policies)
	if err != nil {
		return fmt.Errorf("build universe: %w", err)
	}

	if err := writeFileAtomic(filepath.Join(*runDir, "universe.json"), func(f *os.File) error {
		return candidate.Encode(f, universe)
	}); err != nil {
		return fmt.Errorf("write universe.json: %w", err)
	}

	view, err := graph.Build(traceEvents)
	if err != nil {
		return fmt.Errorf("build graph: %w", err)
	}

	combinedStrat, err := strategy.NewCombined(strategy.DefaultWeights())
	if err != nil {
		return fmt.Errorf("init combined strategy: %w", err)
	}

	strategies := []struct {
		strat strategy.Strategy
		seed  uint64
		code  string
	}{
		{strategy.NewRandom(*rankingSeed), *rankingSeed, "b0"},
		{strategy.NewFIFO(), 0, "b1"},
		{strategy.NewLIFO(), 0, "b2"},
		{strategy.NewStride(), 0, "b3"},
		{combinedStrat, 0, "b4"},
		{strategy.NewRepresentative(), 0, "b5"},
		{strategy.NewSyncBoundary(), 0, "b6"},
		{strategy.NewRiskHorizon(strategy.DefaultRHGWeights()), 0, "b7"},
		{strategy.NewR1(*rankingSeed), *rankingSeed, "r1"},
		{strategy.NewR2(*rankingSeed), *rankingSeed, "r2"},
		{exp.NewDCGStrategy(), 0, "dcg"},
		{exp.NewDCGPartialStrategy(), 0, "dcg-pr"},
		{strategy.NewDCGFlagOnly(*rankingSeed), *rankingSeed, "dcg-flag-only"},
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
		if err := writeFileAtomic(filepath.Join(*runDir, fname), func(f *os.File) error {
			return exp.EncodeRanking(f, doc)
		}); err != nil {
			return fmt.Errorf("write %s: %w", fname, err)
		}
	}

	fmt.Fprintf(stdout, "Planned %d universe candidates with %d strategies -> %s\n",
		len(universe), len(strategies), *runDir)
	return nil
}

// ----------------------------------------------------------------------
// explore
// ----------------------------------------------------------------------

func runExploreCmd(args []string) error {
	return runExploreCmdWithWriters(args, os.Stdout, os.Stderr)
}

func runExploreCmdWithWriters(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("explore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runDir := fs.String("run", "", "path to run directory")
	timeout := fs.Duration("timeout", 5*time.Second, "per-candidate SafeOpen timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("explore: unexpected positional arguments: %v", fs.Args())
	}
	if *runDir == "" {
		return errors.New("explore: must specify -run directory")
	}
	if *timeout <= 0 {
		return fmt.Errorf("explore: -timeout must be > 0, got %v", *timeout)
	}
	*runDir = filepath.Clean(*runDir)

	meta, traceEvents, histEntries, err := loadRunData(*runDir, true)
	if err != nil {
		return err
	}

	dbDir, err := exp.NormalizeDBDir(meta.Workload.Dir)
	if err != nil {
		return fmt.Errorf("explore: invalid workload dir in run.json: %w", err)
	}

	uFile, err := os.Open(filepath.Join(*runDir, "universe.json"))
	if err != nil {
		return fmt.Errorf("open universe.json: %w", err)
	}
	universe, decErr := candidate.Decode(uFile)
	if cerr := uFile.Close(); cerr != nil && decErr == nil {
		return fmt.Errorf("close universe.json: %w", cerr)
	}
	if decErr != nil {
		return fmt.Errorf("decode universe.json: %w", decErr)
	}

	journalPath := filepath.Join(*runDir, "journal.ndjson")
	res, err := exp.RunExplore(exp.RunnerConfig{
		Universe:    universe,
		Events:      traceEvents,
		History:     histEntries,
		Dir:         dbDir,
		Timeout:     *timeout,
		JournalPath: journalPath,
	})
	if err != nil {
		return fmt.Errorf("run explore: %w", err)
	}

	// Write outcomes.json atomically so errors do not truncate prior outputs.
	if err := writeFileAtomic(filepath.Join(*runDir, "outcomes.json"), func(f *os.File) error {
		return exp.EncodeOutcomes(f, universe, res.Outcomes)
	}); err != nil {
		return fmt.Errorf("write outcomes.json: %w", err)
	}

	fmt.Fprintf(stdout, "Explored %d candidates in %v: valid=%d, viol=%d, open_err=%d, panic=%d, hang=%d, oracle_err=%d\n",
		res.Summary.TotalCandidates, res.Summary.TotalTime,
		res.Summary.Valid, res.Summary.Violations, res.Summary.OpenErrors,
		res.Summary.Panics, res.Summary.Hangs, res.Summary.OracleErrors)
	return nil
}

// ----------------------------------------------------------------------
// evaluate
// ----------------------------------------------------------------------

func runEvaluate(args []string) error {
	return runEvaluateWithWriters(args, os.Stdout, os.Stderr)
}

func runEvaluateWithWriters(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("evaluate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runDir := fs.String("run", "", "path to run directory")
	budgetsFlag := fs.String("budgets", "", "comma-separated budgets (e.g. 0,1,5,10,25,50,100)")
	discoveryOut := fs.String("discovery", "", "optional path for the discovery-curve CSV (B0 averaged over fixed seeds)")
	plotOut := fs.String("plot", "", "optional path for the discovery-curve SVG plot")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("evaluate: unexpected positional arguments: %v", fs.Args())
	}
	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})

	if *runDir == "" {
		return errors.New("evaluate: must specify -run directory")
	}
	*runDir = filepath.Clean(*runDir)
	if *discoveryOut != "" {
		*discoveryOut = filepath.Clean(*discoveryOut)
	}
	if *plotOut != "" {
		*plotOut = filepath.Clean(*plotOut)
	}
	if (*discoveryOut == "") != (*plotOut == "") {
		return errors.New("evaluate: -discovery and -plot must be given together")
	}

	var customBudgets []int
	if visited["budgets"] {
		var err error
		customBudgets, err = parseBudgetsFlag(*budgetsFlag)
		if err != nil {
			return err
		}
	}

	// Load universe.
	uFile, err := os.Open(filepath.Join(*runDir, "universe.json"))
	if err != nil {
		return fmt.Errorf("open universe.json: %w", err)
	}
	universe, decErr := candidate.Decode(uFile)
	if cerr := uFile.Close(); cerr != nil && decErr == nil {
		return fmt.Errorf("close universe.json: %w", cerr)
	}
	if decErr != nil {
		return fmt.Errorf("decode universe.json: %w", decErr)
	}

	// Load outcomes.
	oFile, err := os.Open(filepath.Join(*runDir, "outcomes.json"))
	if err != nil {
		return fmt.Errorf("open outcomes.json: %w", err)
	}
	outcomes, decErr := exp.DecodeOutcomes(oFile)
	if cerr := oFile.Close(); cerr != nil && decErr == nil {
		return fmt.Errorf("close outcomes.json: %w", cerr)
	}
	if decErr != nil {
		return fmt.Errorf("decode outcomes.json: %w", decErr)
	}
	outcomes = exp.NormalizeTable(outcomes)

	// Determine budgets.
	var budgets []int
	if visited["budgets"] {
		budgets = customBudgets
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

	// Find ranking files without glob metacharacter issues.
	dirEntries, err := os.ReadDir(*runDir)
	if err != nil {
		return fmt.Errorf("read run directory %s: %w", *runDir, err)
	}
	var matches []string
	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "ranking_") && strings.HasSuffix(name, ".json") {
			matches = append(matches, filepath.Join(*runDir, name))
		}
	}
	if len(matches) == 0 {
		return fmt.Errorf("no ranking_*.json files found in %s", *runDir)
	}

	type evaluatedRankingDoc struct {
		doc       exp.RankingDocument
		points    []exp.Point
		positions exp.Positions
	}
	evaluatedDocs := make([]evaluatedRankingDoc, 0, len(matches))
	for _, rPath := range matches {
		doc, err := func() (doc exp.RankingDocument, err error) {
			rFile, err := os.Open(rPath)
			if err != nil {
				return doc, fmt.Errorf("open %s: %w", rPath, err)
			}
			defer func() {
				if cerr := rFile.Close(); cerr != nil && err == nil {
					err = fmt.Errorf("close %s: %w", rPath, cerr)
				}
			}()
			doc, err = exp.DecodeRanking(rFile)
			return doc, err
		}()
		if err != nil {
			return fmt.Errorf("decode %s: %w", rPath, err)
		}

		points, err := exp.Evaluate(universe, doc.Candidates, outcomes, budgets)
		if err != nil {
			return fmt.Errorf("evaluate %s: %w", doc.Strategy, err)
		}

		pos, err := exp.DiscoveryPositions(universe, doc.Candidates, outcomes)
		if err != nil {
			return fmt.Errorf("positions %s: %w", doc.Strategy, err)
		}

		evaluatedDocs = append(evaluatedDocs, evaluatedRankingDoc{
			doc:       doc,
			points:    points,
			positions: pos,
		})
	}

	randPos, err := exp.RandomPositions(universe, outcomes, exp.RandomSeeds())
	if err != nil {
		return fmt.Errorf("random positions: %w", err)
	}

	curvesPath := filepath.Join(*runDir, "curves.csv")
	if err := writeFileAtomic(curvesPath, func(f *os.File) error {
		w := csv.NewWriter(f)
		w.UseCRLF = false
		if err := w.Write([]string{"strategy", "budget", "tested", "failures", "distinct_bugs"}); err != nil {
			return err
		}
		for _, item := range evaluatedDocs {
			for _, p := range item.points {
				record := []string{
					item.doc.Strategy,
					strconv.Itoa(p.Budget),
					strconv.Itoa(p.Tested),
					strconv.Itoa(p.Failures),
					strconv.Itoa(p.DistinctBugs),
				}
				if err := w.Write(record); err != nil {
					return err
				}
			}
		}
		w.Flush()
		return w.Error()
	}); err != nil {
		return fmt.Errorf("write curves.csv: %w", err)
	}

	fmt.Fprintln(stdout, "Strategy evaluation curves:")
	fmt.Fprintf(stdout, "%-18s %-8s %-8s %-10s %-14s\n", "Strategy", "Budget", "Tested", "Failures", "DistinctBugs")
	fmt.Fprintln(stdout, strings.Repeat("-", 62))
	for _, item := range evaluatedDocs {
		for _, p := range item.points {
			fmt.Fprintf(stdout, "%-18s %-8d %-8d %-10d %-14d\n",
				item.doc.Strategy, p.Budget, p.Tested, p.Failures, p.DistinctBugs)
		}
	}
	fmt.Fprintf(stdout, "\nSaved discovery curves to %s\n", curvesPath)

	positionsPath := filepath.Join(*runDir, "positions.csv")
	if err := writeFileAtomic(positionsPath, func(f *os.File) error {
		w := csv.NewWriter(f)
		w.UseCRLF = false
		if err := w.Write([]string{"strategy", "first_failure", "first_failure_p10", "first_failure_p90",
			"all_kinds", "all_kinds_p10", "all_kinds_p90", "failure_kinds"}); err != nil {
			return err
		}
		itoa := strconv.Itoa
		for _, item := range evaluatedDocs {
			p := item.positions
			if err := w.Write([]string{item.doc.Strategy, itoa(p.FirstFailure), itoa(p.FirstFailure), itoa(p.FirstFailure),
				itoa(p.AllKinds), itoa(p.AllKinds), itoa(p.AllKinds), itoa(p.Kinds)}); err != nil {
				return err
			}
		}
		if err := w.Write([]string{fmt.Sprintf("random_x%d", randPos.Seeds), itoa(randPos.FirstMedian), itoa(randPos.FirstP10), itoa(randPos.FirstP90),
			itoa(randPos.AllKindsMedian), itoa(randPos.AllKindsP10), itoa(randPos.AllKindsP90), itoa(randPos.Kinds)}); err != nil {
			return err
		}
		w.Flush()
		return w.Error()
	}); err != nil {
		return fmt.Errorf("write positions.csv: %w", err)
	}
	fmt.Fprintf(stdout, "Saved discovery positions to %s\n", positionsPath)

	if *discoveryOut != "" {
		if visited["budgets"] {
			if _, err := exp.GenerateG4ArtifactsWithBudgets(*runDir, *discoveryOut, *plotOut, budgets); err != nil {
				return fmt.Errorf("generate discovery artifacts: %w", err)
			}
		} else {
			if _, err := exp.GenerateG4Artifacts(*runDir, *discoveryOut, *plotOut); err != nil {
				return fmt.Errorf("generate discovery artifacts: %w", err)
			}
		}
		fmt.Fprintf(stderr, "Note: -discovery/-plot generates canonical Gate G4 curves (B0 averaged over fixed seeds plus B4, B5, B6); all evaluated strategies are saved in curves.csv.\n")
		fmt.Fprintf(stdout, "Saved discovery CSV to %s and plot to %s\n", *discoveryOut, *plotOut)
	}
	return nil
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

func loadRunData(dir string, loadHistory bool) (*RunMetadata, []trace.Event, []history.Entry, error) {
	metaBytes, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open run.json: %w", err)
	}
	var meta RunMetadata
	if err := decodeStrictJSONObject(metaBytes, &meta); err != nil {
		return nil, nil, nil, fmt.Errorf("decode run.json: %w", err)
	}
	if meta.SchemaVersion != exp.Version {
		return nil, nil, nil, fmt.Errorf("decode run.json: unsupported or missing schema_version %d", meta.SchemaVersion)
	}
	if len(meta.Policies) == 0 {
		return nil, nil, nil, errors.New("decode run.json: policies cannot be empty")
	}
	seenPolicy := make(map[int]bool, len(meta.Policies))
	for _, p := range meta.Policies {
		if p < 0 || p > 100 {
			return nil, nil, nil, fmt.Errorf("decode run.json: invalid policy %d (must be 0..100)", p)
		}
		if seenPolicy[p] {
			return nil, nil, nil, fmt.Errorf("decode run.json: duplicate policy %d", p)
		}
		seenPolicy[p] = true
	}

	traceFile, err := os.Open(filepath.Join(dir, "trace.json"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open trace.json: %w", err)
	}
	events, decErr := trace.Decode(traceFile)
	if cerr := traceFile.Close(); cerr != nil && decErr == nil {
		return nil, nil, nil, fmt.Errorf("close trace.json: %w", cerr)
	}
	if decErr != nil {
		return nil, nil, nil, fmt.Errorf("decode trace.json: %w", decErr)
	}

	var hist []history.Entry
	if loadHistory {
		histFile, err := os.Open(filepath.Join(dir, "history.json"))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("open history.json: %w", err)
		}
		hist, decErr = history.Decode(histFile)
		if cerr := histFile.Close(); cerr != nil && decErr == nil {
			return nil, nil, nil, fmt.Errorf("close history.json: %w", cerr)
		}
		if decErr != nil {
			return nil, nil, nil, fmt.Errorf("decode history.json: %w", decErr)
		}
	}

	return &meta, events, hist, nil
}

// ----------------------------------------------------------------------
// benchmark
// ----------------------------------------------------------------------

func runBenchmarkCmd(args []string) error {
	return runBenchmarkCmdWithWriters(args, os.Stdout, os.Stderr)
}

func runBenchmarkCmdWithWriters(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seedsFlag := fs.String("seeds", "", "comma-separated workload seeds")
	faultsFlag := fs.String("faults", "", "comma-separated fault IDs")
	policiesFlag := fs.String("policies", "", "comma-separated retention percentages (default: 0,100)")
	maxManifestSizeFlag := fs.Int64("max-manifest-size", 0, "optional MaxManifestFileSize in bytes")
	reportFlag := fs.String("report", "", "path to output empirical markdown report")
	csvFlag := fs.String("csv", "", "optional path to output summary CSV")
	perSeedCSVFlag := fs.String("per-seed-csv", "", "path for per-seed CSV output")
	controlFlag := fs.Bool("control", true, "run MUT-0 null control check")
	checkpointAt := fs.Int("checkpoint-at", 0, "emit a Checkpoint op every N operations (0 = disabled)")
	ingestEvery := fs.Int("ingest-every", 0, "emit an external SSTable ingest op every N operations (0 = disabled)")
	ratchetAt := fs.Int("ratchet-at", 0, "ratchet format major version at operation N (0 = disabled)")
	barrierIngestFlag := fs.Bool("barrier-ingest", false, "execute WAL sync barrier prior to db.Ingest")
	presetFlag := fs.String("preset", "", "workload diversity preset (ycsb-a, ycsb-b, ycsb-f, overwrite-heavy, delete-heavy)")
	walDropKFlag := fs.Int("wal-drop-k", 0, "drop strictly the k-th WAL sync for MUT-5")
	dirDropKFlag := fs.Int("dir-drop-k", 0, "drop strictly the k-th directory sync for MUT-9")

	cfg := exp.DefaultMultiSeedBenchmarkConfig()
	wf := registerWorkloadFlags(fs, cfg.Workload)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("benchmark: unexpected positional arguments: %v", fs.Args())
	}

	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})

	applyWorkloadFlags(visited, wf, &cfg.Workload)
	if visited["checkpoint-at"] {
		cfg.Workload.CheckpointAt = *checkpointAt
	}
	if visited["ingest-every"] {
		cfg.Workload.IngestEvery = *ingestEvery
	}
	if visited["ratchet-at"] {
		cfg.Workload.RatchetAt = *ratchetAt
	}
	if visited["barrier-ingest"] {
		cfg.Workload.BarrierIngest = *barrierIngestFlag
	}
	if visited["preset"] {
		cfg.Workload.Preset = *presetFlag
	}
	if visited["control"] {
		cfg.TrackMUT0 = *controlFlag
	}
	if visited["max-manifest-size"] {
		cfg.Workload.MaxManifestFileSize = *maxManifestSizeFlag
	}
	if cfg.Workload.Preset != "" {
		if !visited["key-count"] {
			cfg.Workload.KeyCount = 0
		}
		if !visited["value-bytes"] {
			cfg.Workload.ValueBytes = 0
		}
	}

	if visited["wal-drop-k"] {
		if *walDropKFlag <= 0 {
			return fmt.Errorf("benchmark: -wal-drop-k must be > 0, got %d", *walDropKFlag)
		}
		cfg.WALDropK = *walDropKFlag
	}
	if visited["dir-drop-k"] {
		if *dirDropKFlag <= 0 {
			return fmt.Errorf("benchmark: -dir-drop-k must be > 0, got %d", *dirDropKFlag)
		}
		cfg.DirDropK = *dirDropKFlag
	}

	if visited["policies"] {
		policies, err := parsePoliciesFlag(*policiesFlag, "benchmark")
		if err != nil {
			return err
		}
		cfg.Policies = policies
	}

	if visited["seeds"] {
		seeds, err := parseSeedsFlag(*seedsFlag)
		if err != nil {
			return err
		}
		cfg.WorkloadSeeds = seeds
	}

	if visited["faults"] {
		fids, err := parseFaultsFlag(*faultsFlag)
		if err != nil {
			return err
		}
		cfg.FaultIDs = fids
	}

	targetReport := *reportFlag
	if targetReport == "none" {
		targetReport = ""
	}

	targetCSV := *csvFlag
	if targetCSV == "none" {
		targetCSV = ""
	}

	targetPerSeedCSV := *perSeedCSVFlag
	if targetPerSeedCSV == "none" {
		targetPerSeedCSV = ""
	}

	fmt.Fprintf(stderr, "Starting Multi-Seed Benchmark across %d seeds and %d faults (%d ops/run)...\n",
		len(cfg.WorkloadSeeds), len(cfg.FaultIDs), cfg.Workload.Operations)

	res, err := exp.RunBenchmark(cfg, func(msg string) {
		fmt.Fprintln(stderr, msg)
	})
	if err != nil {
		return fmt.Errorf("run benchmark: %w", err)
	}

	if targetReport != "" {
		if err := os.MkdirAll(filepath.Dir(targetReport), 0755); err != nil {
			return fmt.Errorf("mkdir for report: %w", err)
		}
		if err := exp.WriteMultiSeedReportToFile(res, targetReport); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
		fmt.Fprintf(stderr, "Generated empirical report -> %s\n", targetReport)
	}

	if targetCSV != "" {
		if err := os.MkdirAll(filepath.Dir(targetCSV), 0755); err != nil {
			return fmt.Errorf("mkdir for csv: %w", err)
		}
		if err := exp.WriteMultiSeedCSV(res, targetCSV); err != nil {
			return fmt.Errorf("write csv: %w", err)
		}
		fmt.Fprintf(stderr, "Generated summary CSV -> %s\n", targetCSV)
	}

	if targetPerSeedCSV != "" {
		if err := os.MkdirAll(filepath.Dir(targetPerSeedCSV), 0755); err != nil {
			return fmt.Errorf("mkdir for per-seed csv: %w", err)
		}
		if err := exp.WriteMultiSeedPerSeedCSV(res, targetPerSeedCSV); err != nil {
			return fmt.Errorf("write per-seed csv: %w", err)
		}
		fmt.Fprintf(stderr, "Generated per-seed CSV -> %s\n", targetPerSeedCSV)
	}

	if targetReport == "" && targetCSV == "" && targetPerSeedCSV == "" {
		fmt.Fprint(stdout, exp.GenerateMultiSeedReportMarkdown(res))
	}

	fmt.Fprintf(stderr, "Benchmark completed successfully in %v.\n", res.TotalDuration)
	return nil
}
