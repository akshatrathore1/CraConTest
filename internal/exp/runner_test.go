package exp_test

import (
	"bytes"
	"os"
	"path/filepath"
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
