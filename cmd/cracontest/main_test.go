package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIFullPipelineSmoke(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "smoke_run")
	smokeConfig := filepath.Join("..", "..", "experiments", "configs", "smoke.json")

	// 1. Record command
	t.Log("Testing cracontest record...")
	err := runRecord([]string{
		"-spec", smokeConfig,
		"-out", runDir,
		"-fault", "MUT-0",
	})
	if err != nil {
		t.Fatalf("runRecord failed: %v", err)
	}

	// Verify generated artifacts from record
	for _, fname := range []string{"run.json", "trace.json", "history.json"} {
		if _, err := os.Stat(filepath.Join(runDir, fname)); err != nil {
			t.Errorf("expected %s after record, but stat failed: %v", fname, err)
		}
	}

	// 2. Plan command
	t.Log("Testing cracontest plan...")
	err = runPlan([]string{
		"-run", runDir,
		"-seed", "42",
	})
	if err != nil {
		t.Fatalf("runPlan failed: %v", err)
	}

	// Verify universe and strategy ranking documents
	for _, fname := range []string{"universe.json", "ranking_b0.json", "ranking_b4.json", "ranking_b5.json", "ranking_b6.json"} {
		if _, err := os.Stat(filepath.Join(runDir, fname)); err != nil {
			t.Errorf("expected %s after plan, but stat failed: %v", fname, err)
		}
	}

	// 3. Explore command
	t.Log("Testing cracontest explore...")
	err = runExploreCmd([]string{
		"-run", runDir,
		"-timeout", "5s",
	})
	if err != nil {
		t.Fatalf("runExploreCmd failed: %v", err)
	}

	// Verify journal and outcomes
	for _, fname := range []string{"journal.ndjson", "outcomes.json"} {
		if _, err := os.Stat(filepath.Join(runDir, fname)); err != nil {
			t.Errorf("expected %s after explore, but stat failed: %v", fname, err)
		}
	}

	// 4. Evaluate command
	t.Log("Testing cracontest evaluate...")
	err = runEvaluate([]string{
		"-run", runDir,
		"-budgets", "0,1,5,10,25",
	})
	if err != nil {
		t.Fatalf("runEvaluate failed: %v", err)
	}

	// Verify curves.csv
	if _, err := os.Stat(filepath.Join(runDir, "curves.csv")); err != nil {
		t.Errorf("expected curves.csv after evaluate, but stat failed: %v", err)
	}
}

func TestCLIMUT1Record(t *testing.T) {
	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mut1_run")
	smokeConfig := filepath.Join("..", "..", "experiments", "configs", "smoke.json")

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

	// Verify no WAL sync events exist in the trace
	walSyncCount := 0
	for _, e := range traceEvents {
		if e.Cat == "pebble-wal" && (e.Kind == "Sync" || e.Kind == "SyncData") {
			walSyncCount++
		}
	}
	if walSyncCount != 0 {
		t.Errorf("MUT-1 trace has %d WAL sync events, want 0", walSyncCount)
	}
}

// TestCLISubcommandAndFlagValidation verifies that each CLI subcommand returns a
// non-nil error when required flags are absent or when incompatible flag
// combinations are supplied.
func TestCLISubcommandAndFlagValidation(t *testing.T) {
	// 1. runRecord must return an error when neither -out nor -run is provided.
	if err := runRecord([]string{}); err == nil {
		t.Error("runRecord([]string{}): expected non-nil error when -out/-run absent, got nil")
	}

	// 2. runPlan must return an error when -run is absent.
	if err := runPlan([]string{}); err == nil {
		t.Error("runPlan([]string{}): expected non-nil error when -run absent, got nil")
	}

	// 3. runExploreCmd must return an error when -run is absent.
	if err := runExploreCmd([]string{}); err == nil {
		t.Error("runExploreCmd([]string{}): expected non-nil error when -run absent, got nil")
	}

	// 4. runEvaluate must return an error when -discovery is supplied without -plot.
	if err := runEvaluate([]string{"-run", t.TempDir(), "-discovery", "only_one.csv"}); err == nil || !strings.Contains(err.Error(), "-discovery and -plot") {
		t.Errorf("runEvaluate with -discovery but no -plot: expected flag validation error mentioning -discovery and -plot, got %v", err)
	}
}

func TestCLIBenchmarkCommand(t *testing.T) {
	tempDir := t.TempDir()
	repPath := filepath.Join(tempDir, "report_empirical.md")
	csvPath := filepath.Join(tempDir, "summary.csv")

	err := runBenchmarkCmd([]string{
		"-seeds", "42",
		"-faults", "MUT-1",
		"-ops", "30",
		"-report", repPath,
		"-csv", csvPath,
		"-control=false",
	})
	if err != nil {
		t.Fatalf("runBenchmarkCmd failed: %v", err)
	}

	if _, err := os.Stat(repPath); err != nil {
		t.Errorf("expected empirical report at %s, stat failed: %v", repPath, err)
	}
	if _, err := os.Stat(csvPath); err != nil {
		t.Errorf("expected summary csv at %s, stat failed: %v", csvPath, err)
	}
}
