package exp_test

import (
	"bytes"
	"encoding/json"
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

type slowOpenFS struct {
	vfs.FS
	delay time.Duration
}

func (s *slowOpenFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	time.Sleep(s.delay)
	return s.FS.Open(name, opts...)
}

func (s *slowOpenFS) OpenDir(name string) (vfs.File, error) {
	time.Sleep(s.delay)
	return s.FS.OpenDir(name)
}

func TestRunExploreTimeoutHandling(t *testing.T) {
	events, hist := runSmallWorkload(t, 10)
	universe, err := candidate.Build(events, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}

	restore := exp.SetSafeOpenFSWrapperForTesting(func(fs vfs.FS) vfs.FS {
		return &slowOpenFS{FS: fs, delay: 40 * time.Millisecond}
	})
	defer restore()

	res, err := exp.RunExplore(exp.RunnerConfig{
		Universe: universe[:2],
		Events:   events,
		History:  hist,
		Timeout:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("RunExplore: %v", err)
	}

	for _, o := range res.Outcomes {
		if o.Class != oracle.ClassHang {
			t.Fatalf("expected oracle.ClassHang on timed-out open, got %s (%+v)", o.Class, o)
		}
	}
}

func TestReadJournalRejectsInvalidRecords(t *testing.T) {
	valid := oracle.Outcome{Candidate: candidate.Candidate{Event: 1}, Class: oracle.ClassValid}
	mismatched := valid
	mismatched.Candidate.Event = 2
	mismatchedOracleError := oracle.Outcome{
		Candidate: candidate.Candidate{Event: 2},
		Class:     oracle.ClassOracleError,
		Detail:    "injected infrastructure failure",
	}
	unknownClass := valid
	unknownClass.Class = "unknown"
	cases := []struct {
		name   string
		record exp.JournalRecord
	}{
		{"mismatched candidate", exp.JournalRecord{Stage: exp.StageFinished, Candidate: valid.Candidate, Outcome: &mismatched}},
		{"mismatched oracle error candidate", exp.JournalRecord{Stage: exp.StageFinished, Candidate: valid.Candidate, Outcome: &mismatchedOracleError}},
		{"missing outcome", exp.JournalRecord{Stage: exp.StageFinished, Candidate: valid.Candidate}},
		{"unknown stage", exp.JournalRecord{Stage: "unknown", Candidate: valid.Candidate}},
		{"invalid outcome", exp.JournalRecord{Stage: exp.StageFinished, Candidate: valid.Candidate, Outcome: &unknownClass}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buffer bytes.Buffer
			if err := json.NewEncoder(&buffer).Encode(tc.record); err != nil {
				t.Fatal(err)
			}
			if _, err := exp.ReadJournal(&buffer); err == nil {
				t.Fatal("malformed journal was accepted")
			}
		})
	}
}

func TestRunExploreRejectsStaleJournal(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "get"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "get"},
	}
	hist := []history.Entry{{Op: 1, Kind: "get", BeginEvent: 1, EndEvent: 2}}
	cfg := exp.RunnerConfig{
		Universe: []candidate.Candidate{{Event: 0}},
		Events:   events,
		History:  hist,
		Timeout:  time.Second,
	}
	cases := []string{"trace", "history", "timeout"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			initial := cfg
			initial.JournalPath = filepath.Join(t.TempDir(), "journal.ndjson")
			if _, err := exp.RunExplore(initial); err != nil {
				t.Fatal(err)
			}
			changed := initial
			switch name {
			case "trace":
				changed.Events = trace.Clone(events)
				changed.Events[1].Err = "changed read result"
				changed.History = history.Clone(hist)
				changed.History[0].Err = changed.Events[1].Err
			case "history":
				changed.Events = nil
				changed.History = nil
			case "timeout":
				changed.Timeout = 2 * time.Second
			}
			if _, err := exp.RunExplore(changed); err == nil {
				t.Fatal("stale journal was reused with different execution inputs")
			}
		})
	}
}

func TestRunExploreRejectsInvalidUniverse(t *testing.T) {
	cases := [][]candidate.Candidate{
		{{Event: 1}},
		{{Percent: -1}},
		{{Percent: 101}},
		{{Event: 0}, {Event: 0}},
	}
	for _, universe := range cases {
		if _, err := exp.RunExplore(exp.RunnerConfig{Universe: universe}); err == nil {
			t.Fatalf("invalid universe accepted: %+v", universe)
		}
	}
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
		if outcome.Rule != "" {
			t.Fatalf("expected empty rule, got rule=%q", outcome.Rule)
		}
		expectedSig := exp.FormatOpenErrorSignature(outcome.Detail)
		if outcome.Signature != expectedSig {
			t.Fatalf("expected signature %q, got %q", expectedSig, outcome.Signature)
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

func TestRunExploreJournalStartedOnlyRecordsHang(t *testing.T) {
	events, hist := runSmallWorkload(t, 6)
	universe, err := candidate.Build(events, 7, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	if len(universe) < 2 {
		t.Fatalf("expected at least 2 candidates, got %d", len(universe))
	}
	journalPath := filepath.Join(t.TempDir(), "resume_started.journal")
	cfg := exp.RunnerConfig{
		Universe:    universe[:1],
		Events:      events,
		History:     hist,
		JournalPath: journalPath,
		Timeout:     3 * time.Second,
	}
	if _, err := exp.RunExplore(cfg); err != nil {
		t.Fatalf("initial RunExplore: %v", err)
	}

	raw, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile journal: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	var firstRec exp.JournalRecord
	if err := json.Unmarshal([]byte(lines[0]), &firstRec); err != nil {
		t.Fatalf("Unmarshal journal record: %v", err)
	}

	startedRec, err := json.Marshal(exp.JournalRecord{
		Stage:       exp.StageStarted,
		Candidate:   universe[1],
		Time:        time.Now().UTC(),
		ContextHash: firstRec.ContextHash,
	})
	if err != nil {
		t.Fatalf("Marshal started record: %v", err)
	}
	f, err := os.OpenFile(journalPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("OpenFile journal: %v", err)
	}
	_, _ = f.Write(append(startedRec, '\n'))
	_ = f.Close()

	// Snapshot the interrupted journal before the default resume writes StageFinished(ClassHang).
	snapRaw, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatalf("ReadFile snapshot journal: %v", err)
	}
	retryJournalPath := journalPath + ".retry"
	if err := os.WriteFile(retryJournalPath, snapRaw, 0600); err != nil {
		t.Fatalf("WriteFile retry journal: %v", err)
	}

	cfg.Universe = universe[:2]
	res, err := exp.RunExplore(cfg)
	if err != nil {
		t.Fatalf("resumed RunExplore: %v", err)
	}
	if len(res.Outcomes) != 2 || res.Outcomes[1].Class != oracle.ClassHang {
		t.Fatalf("expected interrupted candidate to record ClassHang, got %+v", res.Outcomes)
	}

	// With RetryInterrupted enabled, StageStarted-only candidates are re-executed instead of marked ClassHang.
	cfg.JournalPath = retryJournalPath
	cfg.RetryInterrupted = true
	retryRes, err := exp.RunExplore(cfg)
	if err != nil {
		t.Fatalf("RetryInterrupted RunExplore: %v", err)
	}
	if len(retryRes.Outcomes) != 2 || retryRes.Outcomes[1].Class == oracle.ClassHang {
		t.Fatalf("expected StageStarted-only candidate to be re-executed, got %+v", retryRes.Outcomes)
	}
}

func TestRunnerValidationJournalAndCheckpointEdgeCases(t *testing.T) {
	events, hist := runSmallWorkload(t, 4)

	// Empty universe returns empty result cleanly.
	emptyRes, err := exp.RunExplore(exp.RunnerConfig{
		Universe: []candidate.Candidate{},
		Events:   events,
		History:  hist,
	})
	if err != nil {
		t.Fatalf("empty universe RunExplore failed: %v", err)
	}
	if len(emptyRes.Outcomes) != 0 {
		t.Fatalf("expected 0 outcomes on empty universe, got %d", len(emptyRes.Outcomes))
	}

	// Conflicting JournalPath and Journal rejected.
	if _, err := exp.RunExplore(exp.RunnerConfig{
		Universe:    []candidate.Candidate{},
		Events:      events,
		History:     hist,
		JournalPath: filepath.Join(t.TempDir(), "j.ndjson"),
		Journal:     &bytes.Buffer{},
	}); err == nil {
		t.Error("expected error when both JournalPath and Journal are set")
	}

	// ReadJournal error paths and >1 MiB line support.
	c1 := candidate.Candidate{Event: 1, Percent: 0, Seed: 1}
	for name, content := range map[string]string{
		"started_with_outcome": `{"stage":"started","candidate":{"event":1,"percent":0,"seed":1},"outcome":{"candidate":{"event":1,"percent":0,"seed":1},"class":"valid"}}` + "\n",
		"bad_percent":          `{"stage":"finished","candidate":{"event":1,"percent":150,"seed":1},"outcome":{"candidate":{"event":1,"percent":150,"seed":1},"class":"valid"}}` + "\n",
		"trailing_tokens":      `{"stage":"finished","candidate":{"event":1,"percent":0,"seed":1},"outcome":{"candidate":{"event":1,"percent":0,"seed":1},"class":"valid"}} {"extra":1}` + "\n",
		"conflicting_finished": `{"stage":"finished","candidate":{"event":1,"percent":0,"seed":1},"outcome":{"candidate":{"event":1,"percent":0,"seed":1},"class":"valid"}}` + "\n" +
			`{"stage":"finished","candidate":{"event":1,"percent":0,"seed":1},"outcome":{"candidate":{"event":1,"percent":0,"seed":1},"class":"hang","detail":"timeout"}}` + "\n",
		"mixed_context_hash": `{"stage":"finished","candidate":{"event":1,"percent":0,"seed":1},"outcome":{"candidate":{"event":1,"percent":0,"seed":1},"class":"valid"},"context_hash":"aaa"}` + "\n" +
			`{"stage":"finished","candidate":{"event":2,"percent":0,"seed":1},"outcome":{"candidate":{"event":2,"percent":0,"seed":1},"class":"valid"},"context_hash":"bbb"}` + "\n",
	} {
		if _, err := exp.ReadJournal(strings.NewReader(content)); err == nil {
			t.Errorf("ReadJournal(%s): expected error, got nil", name)
		}
	}

	// Verify > 1 MiB journal record succeeds cleanly (bufio.Reader without 1 MiB Scanner cap).
	bigDetail := strings.Repeat("x", 1200*1024)
	rec := exp.JournalRecord{
		Stage:       exp.StageFinished,
		Candidate:   c1,
		Outcome:     &oracle.Outcome{Candidate: c1, Class: oracle.ClassPanic, Detail: bigDetail},
		Time:        time.Now().UTC(),
		ContextHash: "ctx1",
	}
	rawBig, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	finishedBig, err := exp.ReadJournal(bytes.NewReader(append(rawBig, '\n')))
	if err != nil {
		t.Fatalf("ReadJournal >1MiB record failed: %v", err)
	}
	if len(finishedBig) != 1 || len(finishedBig[c1].Detail) != len(bigDetail) {
		t.Fatalf("ReadJournal >1MiB mismatch: %+v", finishedBig)
	}

	// Verify checkpoint marker.manifest.* detection triggers checkpoint recovery open error when target manifest is missing.
	mem := vfs.NewMem()
	_ = mem.MkdirAll("checkpoint", 0755)
	mf, err := mem.Create("checkpoint/marker.manifest.000001.MANIFEST-000099", "unspecified")
	if err != nil {
		t.Fatal(err)
	}
	_ = mf.Close()
	ckptOutcome := oracle.Outcome{Class: oracle.ClassValid}
	exp.InspectCheckpointRecovery(mem, time.Second, []string{"pebble-manifest"}, &ckptOutcome)
	if ckptOutcome.Class != oracle.ClassOpenError {
		t.Fatalf("expected ClassOpenError for dangling marker.manifest checkpoint, got %s (%s)", ckptOutcome.Class, ckptOutcome.Detail)
	}

	// Verify StageTimings Open/Dump attribution via RunExplore when a checkpoint trace executes.
	eventsCkpt, histCkpt := func() ([]trace.Event, []history.Entry) {
		mem2 := vfs.NewCrashableMem()
		rec2 := vfsrec.New(mem2)
		l2 := rec2.EventListener()
		db2, err := pebble.Open("db", &pebble.Options{FS: rec2, Logger: recovery.PanicLogger{}, EventListener: &l2})
		if err != nil {
			t.Fatal(err)
		}
		h, err := workload.Run(db2, rec2, workload.Config{
			Seed:         42,
			Operations:   12,
			KeyCount:     4,
			ValueBytes:   32,
			SyncEvery:    4,
			FlushEvery:   6,
			CheckpointAt: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		db2.TestOnlyWaitForCleaning()
		_ = db2.Close()
		ev := rec2.Events()
		corr, err := workload.Correlate(h, ev)
		if err != nil {
			t.Fatal(err)
		}
		return ev, corr
	}()
	univCkpt, err := candidate.Build(eventsCkpt, 42, []int{100})
	if err != nil || len(univCkpt) == 0 {
		t.Fatalf("candidate.Build checkpoint trace: %v", err)
	}
	lastCand := univCkpt[len(univCkpt)-1]
	ckptRunRes, err := exp.RunExplore(exp.RunnerConfig{
		Universe: []candidate.Candidate{lastCand},
		Events:   eventsCkpt,
		History:  histCkpt,
	})
	if err != nil {
		t.Fatalf("RunExplore checkpoint timing: %v", err)
	}
	if ckptRunRes.Timings[lastCand].Open <= 0 || ckptRunRes.Timings[lastCand].Total <= 0 {
		t.Errorf("expected positive Open & Total timings with checkpoint recovery: %+v", ckptRunRes.Timings[lastCand])
	}

	// Verify ClassOracleError in journal is NOT treated as permanently cached valid result on resume.
	univ, err := candidate.Build(events, 7, []int{0})
	if err != nil || len(univ) == 0 {
		t.Fatalf("candidate.Build: %v", err)
	}
	jPath := filepath.Join(t.TempDir(), "oracle_err_retry.ndjson")
	oeRec, err := json.Marshal(exp.JournalRecord{
		Stage:     exp.StageFinished,
		Candidate: univ[0],
		Outcome:   &oracle.Outcome{Candidate: univ[0], Class: oracle.ClassOracleError, Rule: "clone_failure", Detail: "transient"},
		Time:      time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// RunExplore ignores ContextHash only if empty? No, matching hash; write via RunExplore missing_features first, then fix categories!
	if _, err := exp.RunExplore(exp.RunnerConfig{
		Universe:    univ[:1],
		Events:      events,
		History:     hist,
		Categories:  map[uint64][]string{}, // triggers ClassOracleError(missing_features)
		JournalPath: jPath,
	}); err != nil {
		t.Fatalf("first explore with missing_features: %v", err)
	}
	_ = oeRec
	// Resuming with valid nil Categories on a fresh journal should compute features; same ContextHash check applies if hash differs.
	// Using ReadJournal confirms ClassOracleError was not cached in finished outcomes map:
	jfRead, err := os.Open(jPath)
	if err != nil {
		t.Fatal(err)
	}
	defer jfRead.Close()
	cached, err := exp.ReadJournal(jfRead)
	if err != nil {
		t.Fatalf("ReadJournal on ClassOracleError journal: %v", err)
	}
	if len(cached) != 0 {
		t.Fatalf("expected ClassOracleError NOT cached as finished outcome, got %d entries", len(cached))
	}
}
