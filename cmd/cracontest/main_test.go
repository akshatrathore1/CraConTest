package main

import (
	"os"
	"path/filepath"
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
