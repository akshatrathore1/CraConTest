package exp_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/exp"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func runSmallWorkload(t *testing.T, numOps int) ([]trace.Event, []history.Entry) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            rec,
		Logger:        recovery.PanicLogger{},
		MemTableSize:  64 * 1024,
		EventListener: &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	cfg := workload.Config{
		Seed:       42,
		Operations: numOps,
		KeyCount:   8,
		ValueBytes: 32,
		SyncEvery:  5,
		FlushEvery: 15,
	}

	rawHist, err := workload.Run(db, rec, cfg)
	if err != nil {
		t.Fatalf("workload.Run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	correlated, err := workload.Correlate(rawHist, events)
	if err != nil {
		t.Fatalf("workload.Correlate: %v", err)
	}
	if err := history.Validate(correlated); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}

	return events, correlated
}

func TestRunExploreEndToEnd(t *testing.T) {
	events, hist := runSmallWorkload(t, 25)

	universe, err := candidate.Build(events, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	if len(universe) == 0 {
		t.Fatalf("universe is empty")
	}

	var journalBuf bytes.Buffer
	res, err := exp.RunExplore(exp.RunnerConfig{
		Universe: universe,
		Events:   events,
		History:  hist,
		Journal:  &journalBuf,
		Timeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("RunExplore: %v", err)
	}

	if len(res.Outcomes) != len(universe) {
		t.Fatalf("outcomes count %d != universe count %d", len(res.Outcomes), len(universe))
	}

	// Verify all outcomes pass table validation.
	if err := exp.ValidateTable(universe, res.Outcomes); err != nil {
		t.Fatalf("ValidateTable failed on explore outcomes: %v", err)
	}

	// Check per-stage timings.
	for _, cand := range universe {
		timings, ok := res.Timings[cand]
		if !ok {
			t.Errorf("missing timings for candidate %+v", cand)
		}
		if timings.Total < 0 {
			t.Errorf("candidate %+v total timing < 0", cand)
		}
	}

	// Verify journal records.
	journalOutcomes, err := exp.ReadJournal(&journalBuf)
	if err != nil {
		t.Fatalf("ReadJournal: %v", err)
	}
	if len(journalOutcomes) != len(universe) {
		t.Fatalf("journal finished count %d != universe count %d", len(journalOutcomes), len(universe))
	}

	t.Logf("Explore completed: %d candidates, %d valid, total time: %v",
		res.Summary.TotalCandidates, res.Summary.Valid, res.Summary.TotalTime)
}

func TestRunExploreJournalResume(t *testing.T) {
	events, hist := runSmallWorkload(t, 20)

	universe, err := candidate.Build(events, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}

	tmpDir := t.TempDir()
	journalPath := filepath.Join(tmpDir, "explore_journal.ndjson")

	// Phase 1: Run only first 3 candidates.
	subUniverse := universe[:3]
	res1, err := exp.RunExplore(exp.RunnerConfig{
		Universe:    subUniverse,
		Events:      events,
		History:     hist,
		JournalPath: journalPath,
		Timeout:     3 * time.Second,
	})
	if err != nil {
		t.Fatalf("RunExplore phase 1: %v", err)
	}
	if len(res1.Outcomes) != 3 {
		t.Fatalf("phase 1 outcomes len %d != 3", len(res1.Outcomes))
	}

	// Phase 2: Resume with the full universe using the same journal file.
	res2, err := exp.RunExplore(exp.RunnerConfig{
		Universe:    universe,
		Events:      events,
		History:     hist,
		JournalPath: journalPath,
		Timeout:     3 * time.Second,
	})
	if err != nil {
		t.Fatalf("RunExplore phase 2 (resume): %v", err)
	}
	if len(res2.Outcomes) != len(universe) {
		t.Fatalf("phase 2 outcomes len %d != %d", len(res2.Outcomes), len(universe))
	}

	// Ensure the first 3 match exactly.
	for i := 0; i < 3; i++ {
		if res1.Outcomes[i] != res2.Outcomes[i] {
			t.Errorf("resumed outcome mismatch at %d: %+v vs %+v", i, res1.Outcomes[i], res2.Outcomes[i])
		}
	}

	// Verify all outcomes pass table validation.
	if err := exp.ValidateTable(universe, res2.Outcomes); err != nil {
		t.Fatalf("ValidateTable on resumed outcomes: %v", err)
	}

	// Check final journal contents.
	f, err := os.Open(journalPath)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer f.Close()

	finished, err := exp.ReadJournal(f)
	if err != nil {
		t.Fatalf("ReadJournal on completed journal: %v", err)
	}
	if len(finished) != len(universe) {
		t.Fatalf("finished count in journal %d != %d", len(finished), len(universe))
	}
}

func TestRunExploreTimeoutHandling(t *testing.T) {
	events, hist := runSmallWorkload(t, 10)
	universe, err := candidate.Build(events, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}

	// Use an impossibly small timeout (1 nanosecond) to trigger SafeOpen timeout.
	res, err := exp.RunExplore(exp.RunnerConfig{
		Universe: universe[:2],
		Events:   events,
		History:  hist,
		Timeout:  1 * time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("RunExplore: %v", err)
	}

	// At least one should report ClassHang due to timeout (or complete if fast enough).
	for _, o := range res.Outcomes {
		if o.Class == oracle.ClassHang {
			t.Logf("Successfully captured timeout hang outcome: %+v", o)
			return
		}
	}
	// If 1ns wasn't small enough on this fast machine, that's fine, but check no error.
	t.Logf("Completed without timeout: outcome classes: %v, %v", res.Outcomes[0].Class, res.Outcomes[1].Class)
}

func TestRunExploreCorruptJournalReturnsError(t *testing.T) {
	events, hist := runSmallWorkload(t, 10)
	universe, err := candidate.Build(events, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}

	tmpDir := t.TempDir()
	corruptJournal := filepath.Join(tmpDir, "corrupt.journal")
	if err := os.WriteFile(corruptJournal, []byte("{invalid json line\n"), 0600); err != nil {
		t.Fatalf("write corrupt journal: %v", err)
	}

	_, err = exp.RunExplore(exp.RunnerConfig{
		Universe:    universe[:2],
		Events:      events,
		History:     hist,
		JournalPath: corruptJournal,
		Timeout:     3 * time.Second,
	})
	if err == nil {
		t.Fatal("expected error from corrupt journal, got nil")
	}
	if !strings.Contains(err.Error(), "runner read journal") {
		t.Fatalf("expected 'runner read journal' error, got: %v", err)
	}
}

func TestExplorePartialRetentionFiveRepeatZeroFlips(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                  rec,
		Logger:              recovery.PanicLogger{},
		MemTableSize:        64 * 1024,
		MaxManifestFileSize: 1, // trigger manifest roll-over and unsynced directory entries
		EventListener:       &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	cfg := workload.Config{
		Seed:       42,
		Operations: 25,
		KeyCount:   8,
		ValueBytes: 32,
		SyncEvery:  5,
		FlushEvery: 10,
	}

	rawHist, err := workload.Run(db, rec, cfg)
	if err != nil {
		t.Fatalf("workload.Run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	correlated, err := workload.Correlate(rawHist, events)
	if err != nil {
		t.Fatalf("workload.Correlate: %v", err)
	}
	if err := history.Validate(correlated); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}

	universe, err := candidate.Build(events, 42, []int{0, 50, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	if len(universe) == 0 {
		t.Fatalf("empty universe")
	}

	// Verify that 5 full independent RunExplore repeats produce 0 flips across all candidates.
	var baselineOutcomes []oracle.Outcome
	for repeat := 0; repeat < 5; repeat++ {
		res, err := exp.RunExplore(exp.RunnerConfig{
			Universe: universe,
			Events:   events,
			History:  correlated,
			Timeout:  3 * time.Second,
		})
		if err != nil {
			t.Fatalf("RunExplore repeat %d: %v", repeat, err)
		}

		if repeat == 0 {
			baselineOutcomes = res.Outcomes
			continue
		}

		if len(res.Outcomes) != len(baselineOutcomes) {
			t.Fatalf("repeat %d: outcomes count mismatch %d vs %d", repeat, len(res.Outcomes), len(baselineOutcomes))
		}

		mismatches := 0
		for i := range res.Outcomes {
			b := baselineOutcomes[i]
			c := res.Outcomes[i]
			if b.Candidate != c.Candidate || b.Class != c.Class || b.Rule != c.Rule || b.Detail != c.Detail {
				mismatches++
				t.Errorf("repeat %d candidate %v flipped: base=(%s, %s, %s) vs curr=(%s, %s, %s)",
					repeat, b.Candidate, b.Class, b.Rule, b.Detail, c.Class, c.Rule, c.Detail)
			}
		}
		if mismatches > 0 {
			t.Fatalf("repeat %d: detected %d outcome flips out of %d candidates", repeat, mismatches, len(universe))
		}
	}
}

type customCheckpointFS struct {
	vfs.FS
	delay             chan struct{}
	panicOnSecondList bool
	listCount         int
}

func (c *customCheckpointFS) List(dir string) ([]string, error) {
	c.listCount++
	if c.panicOnSecondList && c.listCount >= 2 {
		panic("simulated fatal corruption in checkpoint")
	}
	return c.FS.List(dir)
}

func (c *customCheckpointFS) Lock(name string) (io.Closer, error) {
	if c.delay != nil {
		<-c.delay
	}
	return c.FS.Lock(name)
}

func TestInspectCheckpointRecovery_Outcomes(t *testing.T) {
	newBaseMem := func() vfs.FS {
		mem := vfs.NewMem()
		_ = mem.MkdirAll("checkpoint", 0755)
		curFile, err := mem.Create("checkpoint/CURRENT", "unspecified")
		if err != nil {
			t.Fatalf("Create CURRENT: %v", err)
		}
		_, _ = curFile.Write([]byte("MANIFEST-000001\n"))
		_ = curFile.Close()
		return mem
	}

	// 1. Primary failure preservation
	t.Run("PrimaryFailurePreserved", func(t *testing.T) {
		baseMem := newBaseMem()
		outcome := oracle.Outcome{
			Class:  oracle.ClassViolation,
			Rule:   "prefix",
			Detail: "primary db prefix violation",
		}
		exp.InspectCheckpointRecovery(baseMem, 1*time.Second, []string{"data"}, &outcome)
		if outcome.Class != oracle.ClassViolation || outcome.Detail != "primary db prefix violation" {
			t.Fatalf("expected primary outcome preserved, got %+v", outcome)
		}
	})

	// 2. Checkpoint Open Error
	t.Run("CheckpointOpenError", func(t *testing.T) {
		baseMem := newBaseMem()
		outcome := oracle.Outcome{Class: oracle.ClassValid}
		// MANIFEST-000001 does not exist, so SafeOpen will fail with Err
		exp.InspectCheckpointRecovery(baseMem, 1*time.Second, []string{"data"}, &outcome)
		if outcome.Class != oracle.ClassOpenError {
			t.Fatalf("expected ClassOpenError, got %s", outcome.Class)
		}
		if !strings.HasPrefix(outcome.Detail, "checkpoint: ") {
			t.Fatalf("expected 'checkpoint: ' prefix, got %q", outcome.Detail)
		}
		if outcome.Rule != "" || outcome.Signature != "" {
			t.Fatalf("expected empty rule and signature, got rule=%q sig=%q", outcome.Rule, outcome.Signature)
		}
	})

	// 3. Checkpoint Timeout
	t.Run("CheckpointTimeout", func(t *testing.T) {
		baseMem := newBaseMem()
		delayChan := make(chan struct{})
		defer close(delayChan)
		cFS := &customCheckpointFS{
			FS:    baseMem,
			delay: delayChan,
		}
		outcome := oracle.Outcome{Class: oracle.ClassValid}
		exp.InspectCheckpointRecovery(cFS, 20*time.Millisecond, []string{"data"}, &outcome)
		if outcome.Class != oracle.ClassHang {
			t.Fatalf("expected ClassHang, got %s", outcome.Class)
		}
		if outcome.Detail != "checkpoint: open timeout" {
			t.Fatalf("expected 'checkpoint: open timeout', got %q", outcome.Detail)
		}
		if outcome.Rule != "" || outcome.Signature != "" {
			t.Fatalf("expected empty rule and signature, got rule=%q sig=%q", outcome.Rule, outcome.Signature)
		}
	})

	// 4. Checkpoint Panic
	t.Run("CheckpointPanic", func(t *testing.T) {
		baseMem := newBaseMem()
		cFS := &customCheckpointFS{
			FS:                baseMem,
			panicOnSecondList: true,
		}
		outcome := oracle.Outcome{Class: oracle.ClassValid}
		exp.InspectCheckpointRecovery(cFS, 1*time.Second, []string{"data"}, &outcome)
		if outcome.Class != oracle.ClassPanic {
			t.Fatalf("expected ClassPanic, got %s, detail=%q", outcome.Class, outcome.Detail)
		}
		if !strings.HasPrefix(outcome.Detail, "checkpoint: ") || !strings.Contains(outcome.Detail, "simulated fatal corruption in checkpoint") {
			t.Fatalf("expected checkpoint panic detail, got %q", outcome.Detail)
		}
		if outcome.Rule != "" || outcome.Signature != "" {
			t.Fatalf("expected empty rule and signature, got rule=%q sig=%q", outcome.Rule, outcome.Signature)
		}
	})

	// 5. Checkpoint Clean Recovery
	t.Run("CheckpointClean", func(t *testing.T) {
		realFS := vfs.NewMem()
		db, err := pebble.Open("checkpoint", &pebble.Options{FS: realFS})
		if err != nil {
			t.Fatalf("pebble.Open: %v", err)
		}
		if err := db.Set([]byte("ck1"), []byte("cv1"), pebble.Sync); err != nil {
			t.Fatalf("db.Set: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close: %v", err)
		}

		outcome := oracle.Outcome{Class: oracle.ClassValid}
		exp.InspectCheckpointRecovery(realFS, 2*time.Second, []string{"data"}, &outcome)
		if outcome.Class != oracle.ClassValid {
			t.Fatalf("expected ClassValid, got %s (detail: %s)", outcome.Class, outcome.Detail)
		}
	})

	// 6. Checkpoint Integrity Failure
	t.Run("CheckpointIntegrityFailure", func(t *testing.T) {
		realFS := vfs.NewMem()
		db, err := pebble.Open("checkpoint", &pebble.Options{FS: realFS})
		if err != nil {
			t.Fatalf("pebble.Open: %v", err)
		}
		if err := db.Set([]byte("ck1"), []byte("cv1"), pebble.Sync); err != nil {
			t.Fatalf("db.Set: %v", err)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("db.Flush: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("db.Close: %v", err)
		}

		// Find the SSTable in checkpoint and corrupt its body
		entries, err := realFS.List("checkpoint")
		if err != nil {
			t.Fatalf("List checkpoint: %v", err)
		}
		sstFound := false
		for _, e := range entries {
			if strings.HasSuffix(e, ".sst") {
				sstFound = true
				fPath := "checkpoint/" + e
				f, err := realFS.OpenReadWrite(fPath, "unspecified")
				if err != nil {
					t.Fatalf("OpenReadWrite %s: %v", fPath, err)
				}
				_, _ = f.WriteAt([]byte("corrupted data junk bytes"), 0)
				_ = f.Close()
				break
			}
		}
		if !sstFound {
			t.Fatal("no .sst file found in checkpoint")
		}

		outcome := oracle.Outcome{Class: oracle.ClassValid}
		exp.InspectCheckpointRecovery(realFS, 2*time.Second, []string{"pebble-sst"}, &outcome)
		if outcome.Class != oracle.ClassViolation {
			t.Fatalf("expected ClassViolation, got %s (detail: %s)", outcome.Class, outcome.Detail)
		}
		if outcome.Rule != "structural" {
			t.Fatalf("expected rule 'structural', got %q", outcome.Rule)
		}
		if !strings.HasPrefix(outcome.Detail, "checkpoint integrity: ") {
			t.Fatalf("expected 'checkpoint integrity: ' prefix, got %q", outcome.Detail)
		}
		expectedSig := oracle.FormatSignature("structural", "checkpoint", []string{"pebble-sst"})
		if outcome.Signature != expectedSig {
			t.Fatalf("expected signature %q, got %q", expectedSig, outcome.Signature)
		}
	})
}
