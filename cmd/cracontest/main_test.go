package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/strategy"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	fallback := filepath.Join("..", "..")
	if _, err := os.Stat(filepath.Join(fallback, "experiments")); err == nil {
		return fallback
	}
	t.Fatal("could not locate repository root")
	return ""
}

func smokeConfigPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(findRepoRoot(t), "experiments", "configs", "smoke.json")
}

func TestMainHelperProcess(t *testing.T) {
	if os.Getenv("CRACONTEST_TEST_MAIN_HELPER") != "1" {
		return
	}
	os.Args = append([]string{"cracontest"}, os.Args[len(os.Args):]...)
	if raw := os.Getenv("CRACONTEST_TEST_MAIN_ARGS"); raw != "" {
		os.Args = append([]string{"cracontest"}, strings.Split(raw, "\x00")...)
	}
	main()
}

func TestMainEntrypointSubprocess(t *testing.T) {
	runSub := func(args ...string) (string, string, int) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestMainHelperProcess$")
		cmd.Env = append(os.Environ(),
			"CRACONTEST_TEST_MAIN_HELPER=1",
			"CRACONTEST_TEST_MAIN_ARGS="+strings.Join(args, "\x00"),
		)
		var outBuf, errBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &errBuf
		err := cmd.Run()
		code := 0
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				code = exitErr.ExitCode()
			} else {
				t.Fatalf("exec main helper: %v", err)
			}
		}
		return outBuf.String(), errBuf.String(), code
	}

	out, errOut, code := runSub()
	if code != 1 || out != "" || !strings.Contains(errOut, "Usage: cracontest") {
		t.Fatalf("main() with no args: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	out, errOut, code = runSub("help")
	if code != 0 || !strings.Contains(out, "Usage: cracontest") || errOut != "" {
		t.Fatalf("main() help: code=%d stdout=%q stderr=%q", code, out, errOut)
	}

	_, errOut, code = runSub("no-such-cmd")
	if code != 1 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("main() unknown command: code=%d stderr=%q", code, errOut)
	}
}

func TestCLIFullPipelineSmoke(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "smoke_run")
	smokeConfig := smokeConfigPath(t)

	// 1. Record command
	if err := runRecord([]string{"-spec", smokeConfig, "-out", runDir, "-fault", "MUT-0"}); err != nil {
		t.Fatalf("runRecord failed: %v", err)
	}
	meta, events, hist, err := loadRunData(runDir, true)
	if err != nil {
		t.Fatalf("loadRunData after record failed: %v", err)
	}
	if meta.SchemaVersion != 1 || meta.FaultID != "MUT-0" || meta.PebbleVersion == "" {
		t.Fatalf("unexpected run metadata: %+v", meta)
	}
	if len(meta.Policies) != 2 || meta.Policies[0] != 0 || meta.Policies[1] != 100 {
		t.Fatalf("unexpected policies: %v", meta.Policies)
	}
	if len(events) == 0 || len(hist) == 0 {
		t.Fatalf("expected non-empty trace (%d) and history (%d)", len(events), len(hist))
	}

	// 2. Plan command
	if err := runPlan([]string{"-run", runDir, "-seed", "42"}); err != nil {
		t.Fatalf("runPlan failed: %v", err)
	}
	uF, err := os.Open(filepath.Join(runDir, "universe.json"))
	if err != nil {
		t.Fatalf("open universe.json: %v", err)
	}
	universe, decErr := candidate.Decode(uF)
	_ = uF.Close()
	if decErr != nil || len(universe) == 0 {
		t.Fatalf("decode universe.json failed: len=%d err=%v", len(universe), decErr)
	}

	stratCodes := []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "r1", "r2", "dcg", "dcg-pr", "dcg-flag-only"}
	for _, code := range stratCodes {
		rf, err := os.Open(filepath.Join(runDir, fmt.Sprintf("ranking_%s.json", code)))
		if err != nil {
			t.Fatalf("open ranking_%s.json: %v", code, err)
		}
		doc, err := exp.DecodeRanking(rf)
		_ = rf.Close()
		if err != nil {
			t.Fatalf("decode ranking_%s.json: %v", code, err)
		}
		if err := strategy.ValidatePermutation(universe, doc.Candidates); err != nil {
			t.Fatalf("invalid permutation in ranking_%s.json: %v", code, err)
		}
	}

	// 3. Explore command
	if err := runExploreCmd([]string{"-run", runDir, "-timeout", "5s"}); err != nil {
		t.Fatalf("runExploreCmd failed: %v", err)
	}
	jf, err := os.Open(filepath.Join(runDir, "journal.ndjson"))
	if err != nil {
		t.Fatalf("open journal.ndjson: %v", err)
	}
	jMap, err := exp.ReadJournal(jf)
	_ = jf.Close()
	if err != nil || len(jMap) != len(universe) {
		t.Fatalf("journal read mismatch: entries=%d universe=%d err=%v", len(jMap), len(universe), err)
	}

	of, err := os.Open(filepath.Join(runDir, "outcomes.json"))
	if err != nil {
		t.Fatalf("open outcomes.json: %v", err)
	}
	outcomes, err := exp.DecodeOutcomes(of)
	_ = of.Close()
	if err != nil {
		t.Fatalf("decode outcomes.json: %v", err)
	}
	outcomes = exp.NormalizeTable(outcomes)
	if err := exp.ValidateTable(universe, outcomes); err != nil {
		t.Fatalf("ValidateTable outcomes.json: %v", err)
	}
	for _, o := range outcomes {
		if o.Class != oracle.ClassValid {
			t.Fatalf("MUT-0 smoke run expected valid outcome, got %+v", o)
		}
	}

	// 4. Evaluate command (including -discovery and -plot with custom -budgets)
	discCSV := filepath.Join(tempDir, "discovery.csv")
	plotSVG := filepath.Join(tempDir, "plot.svg")
	if err := runEvaluate([]string{
		"-run", runDir,
		"-budgets", "0,1,5,10,25",
		"-discovery", discCSV,
		"-plot", plotSVG,
	}); err != nil {
		t.Fatalf("runEvaluate failed: %v", err)
	}

	cf, err := os.Open(filepath.Join(runDir, "curves.csv"))
	if err != nil {
		t.Fatalf("open curves.csv: %v", err)
	}
	curveRows, err := csv.NewReader(cf).ReadAll()
	_ = cf.Close()
	if err != nil {
		t.Fatalf("read curves.csv: %v", err)
	}
	// Header + 13 strategies * 5 budgets = 66 rows.
	if len(curveRows) != 1+len(stratCodes)*5 {
		t.Fatalf("curves.csv row count = %d, want %d", len(curveRows), 1+len(stratCodes)*5)
	}

	pf, err := os.Open(filepath.Join(runDir, "positions.csv"))
	if err != nil {
		t.Fatalf("open positions.csv: %v", err)
	}
	posRows, err := csv.NewReader(pf).ReadAll()
	_ = pf.Close()
	if err != nil {
		t.Fatalf("read positions.csv: %v", err)
	}
	// Header + 13 deterministic rankings + random_x1000 = 15 rows.
	if len(posRows) != 1+len(stratCodes)+1 {
		t.Fatalf("positions.csv row count = %d, want %d", len(posRows), 1+len(stratCodes)+1)
	}

	df, err := os.Open(discCSV)
	if err != nil {
		t.Fatalf("open discovery CSV: %v", err)
	}
	discRows, err := csv.NewReader(df).ReadAll()
	_ = df.Close()
	if err != nil || len(discRows) < 10 {
		t.Fatalf("invalid discovery CSV rows=%d err=%v", len(discRows), err)
	}
	// Verify custom -budgets ("25" included, "50"/"100" absent).
	found25 := false
	for _, r := range discRows[1:] {
		if r[1] == "25" {
			found25 = true
		}
		if r[1] == "50" || r[1] == "100" {
			t.Errorf("discovery CSV ignored custom -budgets filter; found budget %s", r[1])
		}
	}
	if !found25 {
		t.Errorf("discovery CSV missing custom budget 25")
	}

	svgBytes, err := os.ReadFile(plotSVG)
	if err != nil || !strings.Contains(string(svgBytes), "<svg") || !strings.Contains(string(svgBytes), "</svg>") {
		t.Fatalf("invalid discovery SVG: err=%v", err)
	}
}

func TestCLIMUT1Record(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mut1_run")
	smokeConfig := smokeConfigPath(t)

	err := runRecord([]string{
		"-spec", smokeConfig,
		"-out", runDir,
		"-fault", "MUT-1",
	})
	if err != nil {
		t.Fatalf("runRecord MUT-1 failed: %v", err)
	}

	meta, traceEvents, _, err := loadRunData(runDir, false)
	if err != nil {
		t.Fatalf("loadRunData MUT-1: %v", err)
	}
	if meta.FaultID != "MUT-1" {
		t.Errorf("metadata FaultID: got %s, want MUT-1", meta.FaultID)
	}
	if len(traceEvents) == 0 {
		t.Fatal("MUT-1 recorded an empty trace")
	}

	walWrites := 0
	walSyncCount := 0
	for _, e := range traceEvents {
		if !e.Dir && (e.Cat == "pebble-wal" || strings.HasSuffix(e.File, ".log")) {
			if e.Kind == trace.KindWrite {
				walWrites++
			}
			if e.Kind == trace.KindSync || e.Kind == trace.KindSyncData || e.Kind == trace.KindSyncTo {
				walSyncCount++
			}
		}
	}
	if walWrites == 0 {
		t.Fatal("expected pebble-wal Write events in MUT-1 trace")
	}
	if walSyncCount != 0 {
		t.Errorf("MUT-1 trace has %d WAL sync events, want 0", walSyncCount)
	}
}

func TestCLISubcommandAndFlagValidation(t *testing.T) {
	// Missing required flags and incompatible combinations.
	if err := runRecord([]string{}); err == nil {
		t.Error("runRecord([]string{}): expected error when -out/-run absent")
	}
	if err := runRecord([]string{"-out", t.TempDir(), "-run", t.TempDir()}); err == nil {
		t.Error("runRecord with both -out and -run: expected error")
	}
	if err := runRecord([]string{"-out", t.TempDir(), "-wal-drop-k", "0"}); err == nil {
		t.Error("runRecord with -wal-drop-k 0: expected error")
	}
	if err := runRecord([]string{"-out", t.TempDir(), "-dir-drop-k", "-1"}); err == nil {
		t.Error("runRecord with -dir-drop-k -1: expected error")
	}
	if err := runRecord([]string{"-out", t.TempDir(), "-fault", "MUT-8", "-ops", "20", "-compact-at", "50"}); err == nil {
		t.Error("runRecord with MUT-8 compact-at > ops: expected error")
	}
	if err := runPlan([]string{}); err == nil {
		t.Error("runPlan([]string{}): expected error when -run absent")
	}
	if err := runExploreCmd([]string{}); err == nil {
		t.Error("runExploreCmd([]string{}): expected error when -run absent")
	}
	if err := runExploreCmd([]string{"-run", t.TempDir(), "-timeout", "0s"}); err == nil {
		t.Error("runExploreCmd with -timeout 0s: expected error")
	}
	if err := runEvaluate([]string{"-run", t.TempDir(), "-discovery", "only_one.csv"}); err == nil || !strings.Contains(err.Error(), "-discovery and -plot") {
		t.Errorf("runEvaluate with -discovery but no -plot: got %v", err)
	}
	if err := runEvaluate([]string{"-run", t.TempDir(), "-budgets", "0,,10"}); err == nil {
		t.Error("runEvaluate with empty budget item: expected error")
	}

	// Positional trailing arguments must be rejected across ALL subcommands.
	for name, fn := range map[string]func([]string) error{
		"record":    runRecord,
		"plan":      runPlan,
		"explore":   runExploreCmd,
		"evaluate":  runEvaluate,
		"benchmark": runBenchmarkCmd,
	} {
		if err := fn([]string{"-run", t.TempDir(), "extra_arg"}); err == nil {
			t.Errorf("%s accepted unexpected positional argument", name)
		}
	}

	// runCLI exit codes & stdout/stderr routing.
	var out, errOut bytes.Buffer
	if code := runCLI(nil, &out, &errOut); code != 1 || !strings.Contains(errOut.String(), "Usage:") || out.Len() != 0 {
		t.Errorf("runCLI(nil) = %d out=%q err=%q", code, out.String(), errOut.String())
	}
	out.Reset()
	errOut.Reset()
	if code := runCLI([]string{"help"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "Usage:") || errOut.Len() != 0 {
		t.Errorf("runCLI(help) = %d out=%q err=%q", code, out.String(), errOut.String())
	}
	out.Reset()
	errOut.Reset()
	if code := runCLI([]string{"record", "-h"}, &out, &errOut); code != 0 {
		t.Errorf("runCLI(record -h) = %d err=%q", code, errOut.String())
	}

	// Benchmark duplicate/invalid flags rejected cleanly upfront.
	for name, args := range map[string][]string{
		"dup_seeds":     {"-seeds", "42,42", "-faults", "MUT-1"},
		"empty_seed":    {"-seeds", "42,,100", "-faults", "MUT-1"},
		"dup_faults":    {"-seeds", "42", "-faults", "MUT-5,MUT-WAL-K"},
		"empty_fault":   {"-seeds", "42", "-faults", "MUT-1,"},
		"bad_wal_drop":  {"-seeds", "42", "-faults", "MUT-5", "-wal-drop-k", "0"},
		"bad_dir_drop":  {"-seeds", "42", "-faults", "MUT-9", "-dir-drop-k", "0"},
		"dup_policies":  {"-seeds", "42", "-faults", "MUT-1", "-policies", "0,0"},
		"empty_policy":  {"-seeds", "42", "-faults", "MUT-1", "-policies", ""},
		"mut8_overflow": {"-seeds", "42", "-faults", "MUT-8", "-ops", "20", "-compact-at", "40"},
	} {
		if err := runBenchmarkCmd(args); err == nil {
			t.Errorf("runBenchmarkCmd(%s) unexpectedly succeeded", name)
		}
	}
}

func TestCLISpecStrictCustomDirAndPresetDefaults(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Strict JSON -spec checks ("null", unknown field, trailing object).
	for name, body := range map[string]string{
		"null":     "null",
		"unknown":  `{"operations":10,"key_count":4,"value_bytes":32,"unknown_field":true}`,
		"trailing": `{"operations":10,"key_count":4,"value_bytes":32}{"extra":1}`,
	} {
		specPath := filepath.Join(tempDir, name+".json")
		if err := os.WriteFile(specPath, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		if err := runRecord([]string{"-spec", specPath, "-out", filepath.Join(tempDir, "r_"+name)}); err == nil {
			t.Errorf("-spec %s should have failed strict JSON parsing", name)
		}
	}

	// 2. Custom Dir via -spec works end-to-end across record -> plan -> explore!
	customSpec := filepath.Join(tempDir, "custom_dir.json")
	if err := os.WriteFile(customSpec, []byte(`{"dir":"customdb","operations":15,"key_count":4,"value_bytes":32,"sync_every":5,"flush_every":10}`), 0644); err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(tempDir, "custom_dir_run")
	if err := runRecord([]string{"-spec", customSpec, "-out", runDir, "-fault", "MUT-0"}); err != nil {
		t.Fatalf("runRecord custom dir failed: %v", err)
	}
	if err := runPlan([]string{"-run", runDir}); err != nil {
		t.Fatalf("runPlan custom dir failed: %v", err)
	}
	if err := runExploreCmd([]string{"-run", runDir, "-timeout", "3s"}); err != nil {
		t.Fatalf("runExplore custom dir failed: %v", err)
	}
	meta, evs, _, err := loadRunData(runDir, false)
	if err != nil || meta.Workload.Dir != "customdb" || len(evs) == 0 {
		t.Fatalf("unexpected custom dir metadata/events: %+v len=%d err=%v", meta, len(evs), err)
	}

	// 3. -preset without explicit -key-count does NOT get overwritten by default KeyCount (64).
	presetDir := filepath.Join(tempDir, "preset_run")
	if err := runRecord([]string{"-out", presetDir, "-preset", "ycsb-a", "-ops", "15", "-sync-every", "5", "-flush-every", "10"}); err != nil {
		t.Fatalf("runRecord -preset failed: %v", err)
	}
	pMeta, _, _, err := loadRunData(presetDir, false)
	if err != nil {
		t.Fatalf("loadRunData preset run: %v", err)
	}
	if pMeta.Workload.Preset != "ycsb-a" || pMeta.Workload.KeyCount != 0 || pMeta.Workload.ValueBytes != 0 {
		t.Fatalf("preset KeyCount/ValueBytes overwritten: %+v", pMeta.Workload)
	}

	// 4. loadRunData rejects unsupported schema_version and trailing JSON tokens.
	badRunDir := filepath.Join(tempDir, "bad_meta")
	if err := os.MkdirAll(badRunDir, 0755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(badRunDir, "trace.json"), []byte(`{"schema_version":1,"events":[]}`), 0644)
	_ = os.WriteFile(filepath.Join(badRunDir, "run.json"), []byte(`{"schema_version":99,"policies":[0,100]}`), 0644)
	if _, _, _, err := loadRunData(badRunDir, false); err == nil {
		t.Error("loadRunData accepted invalid schema_version 99")
	}
}

func TestCLIEvaluateAtomicCSVsOnMalformedRanking(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "atomic_eval")
	if err := runRecord([]string{"-spec", smokeConfigPath(t), "-out", runDir, "-fault", "MUT-0"}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := runPlan([]string{"-run", runDir}); err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := runExploreCmd([]string{"-run", runDir, "-timeout", "3s"}); err != nil {
		t.Fatalf("explore: %v", err)
	}
	if err := runEvaluate([]string{"-run", runDir}); err != nil {
		t.Fatalf("initial evaluate: %v", err)
	}

	origCurves, _ := os.ReadFile(filepath.Join(runDir, "curves.csv"))
	origPos, _ := os.ReadFile(filepath.Join(runDir, "positions.csv"))

	// Introduce corrupt ranking_zzz.json -> evaluate must fail without modifying curves.csv or positions.csv!
	if err := os.WriteFile(filepath.Join(runDir, "ranking_zzz.json"), []byte(`{corrupt`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := runEvaluate([]string{"-run", runDir}); err == nil {
		t.Fatal("expected evaluate to fail on corrupt ranking_zzz.json")
	}
	gotCurves, _ := os.ReadFile(filepath.Join(runDir, "curves.csv"))
	gotPos, _ := os.ReadFile(filepath.Join(runDir, "positions.csv"))
	if !bytes.Equal(origCurves, gotCurves) || !bytes.Equal(origPos, gotPos) {
		t.Fatal("evaluate error truncated or mutated prior curves.csv / positions.csv")
	}
}

func TestCLIBenchmarkCommand(t *testing.T) {
	tempDir := t.TempDir()
	repPath := filepath.Join(tempDir, "report_empirical.md")
	csvPath := filepath.Join(tempDir, "summary.csv")
	perSeedPath := filepath.Join(tempDir, "per_seed.csv")

	err := runBenchmarkCmd([]string{
		"-seeds", "42",
		"-faults", "MUT-1",
		"-ops", "30",
		"-key-count", "8",
		"-value-bytes", "32",
		"-sync-every", "5",
		"-flush-every", "15",
		"-report", repPath,
		"-csv", csvPath,
		"-per-seed-csv", perSeedPath,
		"-control=false",
	})
	if err != nil {
		t.Fatalf("runBenchmarkCmd failed: %v", err)
	}

	repData, err := os.ReadFile(repPath)
	if err != nil || !strings.Contains(string(repData), "MUT-1") || !strings.Contains(string(repData), "not evaluated") {
		t.Errorf("unexpected benchmark report contents (err=%v):\n%s", err, string(repData))
	}

	sf, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open summary csv: %v", err)
	}
	sRows, err := csv.NewReader(sf).ReadAll()
	_ = sf.Close()
	if err != nil || len(sRows) != 14 {
		t.Fatalf("summary csv rows = %d (want 14 = header + 13 strategies), err=%v", len(sRows), err)
	}

	psf, err := os.Open(perSeedPath)
	if err != nil {
		t.Fatalf("open per-seed csv: %v", err)
	}
	psRows, err := csv.NewReader(psf).ReadAll()
	_ = psf.Close()
	if err != nil || len(psRows) != 14 {
		t.Fatalf("per-seed csv rows = %d (want 14 = header + 13 strategies), err=%v", len(psRows), err)
	}

	// Verify stdout vs stderr separation when benchmark prints Markdown report to stdout.
	var outBuf, errBuf bytes.Buffer
	code := runCLI([]string{
		"benchmark", "-seeds", "42", "-faults", "MUT-5", "-preset", "ycsb-a",
		"-ops", "20", "-wal-drop-k", "2", "-control=false",
	}, &outBuf, &errBuf)
	if code != 0 {
		t.Fatalf("runCLI benchmark preset failed (code=%d): %s", code, errBuf.String())
	}
	if !strings.HasPrefix(outBuf.String(), "# CraConTest") {
		t.Errorf("stdout should contain clean Markdown report without progress logs, got:\n%s", outBuf.String())
	}
	if !strings.Contains(errBuf.String(), "Starting Multi-Seed Benchmark") || !strings.Contains(outBuf.String(), "$k=2$") {
		t.Errorf("expected progress on stderr and k=2 preset details on stdout; stderr=%q", errBuf.String())
	}
}

func TestCLIRecordWithPolicies(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "policy_run")

	err := runRecord([]string{
		"-out", runDir,
		"-ops", "20",
		"-key-count", "4",
		"-value-bytes", "32",
		"-sync-every", "5",
		"-flush-every", "10",
		"-policies", "0,50,100",
	})
	if err != nil {
		t.Fatalf("runRecord with policies failed: %v", err)
	}

	meta, _, _, err := loadRunData(runDir, false)
	if err != nil {
		t.Fatalf("loadRunData failed: %v", err)
	}

	expected := []int{0, 50, 100}
	if len(meta.Policies) != len(expected) {
		t.Fatalf("meta.Policies length: got %d, want %d", len(meta.Policies), len(expected))
	}
	for i, p := range expected {
		if meta.Policies[i] != p {
			t.Errorf("meta.Policies[%d]: got %d, want %d", i, meta.Policies[i], p)
		}
	}

	// Invalid policies including duplicate/empty elements return errors using isolated subdirs.
	for i, invalid := range []string{"-1", "101", "abc", "", "0,0", "0,,100", "0,100,"} {
		err := runRecord([]string{
			"-out", filepath.Join(tempDir, fmt.Sprintf("err_run_%d", i)),
			"-ops", "10",
			"-key-count", "4",
			"-value-bytes", "32",
			"-policies", invalid,
		})
		if err == nil {
			t.Errorf("expected error for -policies %q, got nil", invalid)
		}
	}
}

func TestCLIBenchmarkWithPoliciesAndManifestSize(t *testing.T) {
	tempDir := t.TempDir()
	repPath := filepath.Join(tempDir, "rep.md")
	csvPath := filepath.Join(tempDir, "sum.csv")

	err := runBenchmarkCmd([]string{
		"-seeds", "42",
		"-faults", "MUT-1",
		"-ops", "20",
		"-policies", "0,50,100",
		"-max-manifest-size", "1048576",
		"-report", repPath,
		"-csv", csvPath,
		"-per-seed-csv", "none",
		"-control=false",
	})
	if err != nil {
		t.Fatalf("runBenchmarkCmd with policies and manifest size failed: %v", err)
	}
	repData, err := os.ReadFile(repPath)
	if err != nil || !strings.Contains(string(repData), "0%/50%/100%") {
		t.Errorf("report missing expected 0%%/50%%/100%% retention policies (err=%v)", err)
	}
	cf, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	rows, err := csv.NewReader(cf).ReadAll()
	_ = cf.Close()
	if err != nil || len(rows) < 2 {
		t.Fatalf("expected CSV summary rows, got %d err=%v", len(rows), err)
	}

	if err := runBenchmarkCmd([]string{"-seeds", "42", "-faults", "MUT-1", "-policies", "150"}); err == nil {
		t.Errorf("expected error for invalid benchmark -policies, got nil")
	}
}

func TestCLIRecordWithSeedAndDropKFlags(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "record_seed_run")

	err := runRecord([]string{
		"-spec", smokeConfigPath(t),
		"-out", runDir,
		"-fault", "MUT-5",
		"-seed", "123",
		"-wal-drop-k", "2",
		"-dir-drop-k", "2",
	})
	if err != nil {
		t.Fatalf("runRecord with seed and drop-k flags failed: %v", err)
	}

	meta, evs, hist, err := loadRunData(runDir, true)
	if err != nil {
		t.Fatalf("loadRunData: %v", err)
	}
	if meta.BaseSeed != 123 || meta.Workload.Seed != 123 {
		t.Errorf("seed propagation mismatch: base=%d workload=%d", meta.BaseSeed, meta.Workload.Seed)
	}
	if meta.FaultConfig == nil || meta.FaultConfig.WALDropK != 2 || meta.FaultConfig.DirDropK != 2 {
		t.Fatalf("drop-k propagation mismatch: %+v", meta.FaultConfig)
	}
	if len(evs) == 0 || len(hist) == 0 {
		t.Fatalf("expected non-empty trace/history: events=%d hist=%d", len(evs), len(hist))
	}

	// Confirm non-default WALDropK=2 keeps WAL syncs (only dropping the 2nd WAL sync).
	walSyncs := 0
	for _, e := range evs {
		if (e.Kind == trace.KindSync || e.Kind == trace.KindSyncData || e.Kind == trace.KindSyncTo) && !e.Dir &&
			(e.Cat == "pebble-wal" || strings.HasSuffix(e.File, ".log")) {
			walSyncs++
		}
	}
	if walSyncs == 0 {
		t.Errorf("expected k=2 WALDropK to retain 1st WAL sync (got 0 WAL sync events)")
	}
	if _, err := strconv.Atoi(strconv.FormatUint(meta.BaseSeed, 10)); err != nil {
		t.Fatal(err)
	}
}
