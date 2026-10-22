package recovery_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// TestOneCandidateCrashPipeline checks the single-candidate crash pipeline:
// An end-to-end crash-recovery pipeline test executing:
// replay -> clone -> SafeOpen -> CheckIntegrity -> dump -> oracle
// on endpoint candidates and obtaining verified outcome classes.
func TestOneCandidateCrashPipeline(t *testing.T) {
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

	var hist []history.Entry
	numOps := 30

	for i := 0; i < numOps; i++ {
		opID := uint64(i + 1)
		k := fmt.Sprintf("pipeline-k%03d", i)
		v := fmt.Sprintf("pipeline-v%03d", i)
		sync := (i%5 == 0)

		rec.Begin(opID, "set")
		wo := pebble.NoSync
		if sync {
			wo = pebble.Sync
		}
		if err := db.Set([]byte(k), []byte(v), wo); err != nil {
			rec.End(opID, "set", err.Error())
			t.Fatalf("Set %d: %v", i, err)
		}
		rec.End(opID, "set", "")

		events := rec.Events()
		var beginEv, endEv uint64
		for _, e := range events {
			if e.Op == opID {
				if e.Kind == trace.KindOpBegin {
					beginEv = e.ID
				} else if e.Kind == trace.KindOpEnd {
					endEv = e.ID
				}
			}
		}

		hist = append(hist, history.Entry{
			Op:         opID,
			Kind:       "set",
			Mutations:  []history.Mutation{{Kind: "set", Key: []byte(k), Value: []byte(v)}},
			Sync:       sync,
			BeginEvent: beginEv,
			EndEvent:   endEv,
		})
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}
	if err := history.Validate(hist); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}

	universe, err := candidate.Build(traceEvents, 42, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	if len(universe) == 0 {
		t.Fatalf("empty candidate universe")
	}

	// Test 1: Terminal 100% retention candidate at the end of the trace.
	terminalCand := universe[len(universe)-1]
	if terminalCand.Percent != 100 {
		terminalCand = universe[len(universe)-2]
	}

	// Step A: Replay trace prefix up to candidate cutoff.
	state, err := replay.Build(traceEvents, terminalCand.Event)
	if err != nil {
		t.Fatalf("replay.Build at cut %d: %v", terminalCand.Event, err)
	}

	// Step B: CrashClone with candidate's retention policy and seed.
	clonedFS, err := state.Clone(terminalCand.Percent, terminalCand.Seed)
	if err != nil {
		_ = state.Close()
		t.Fatalf("state.Clone: %v", err)
	}
	_ = state.Close()

	// Step C: SafeOpen with panic recovery and timeout protection.
	openRes := recovery.SafeOpen(clonedFS, "db", 10*time.Second)
	if openRes.Panic != "" || openRes.Timeout || openRes.Err != nil {
		t.Fatalf("recovery.SafeOpen failed on terminal candidate: err=%v, panic=%s, timeout=%v",
			openRes.Err, openRes.Panic, openRes.Timeout)
	}
	if openRes.DB == nil {
		t.Fatalf("recovery.SafeOpen returned nil DB")
	}

	// Step D: CheckIntegrity (CheckLevels and bidirectional key scans).
	if err := recovery.CheckIntegrity(openRes.DB); err != nil {
		_ = openRes.DB.Close()
		t.Fatalf("recovery.CheckIntegrity failed: %v", err)
	}

	// Step E: DumpContents.
	dump, err := recovery.DumpContents(openRes.DB)
	_ = openRes.DB.Close()
	if err != nil {
		t.Fatalf("recovery.DumpContents failed: %v", err)
	}

	// Step F: Oracle verification.
	outcome := oracle.Check(oracle.Context{
		Candidate: terminalCand,
		Events:    traceEvents,
		History:   hist,
		Recovered: dump,
	})

	if outcome.Class != oracle.ClassValid {
		t.Fatalf("oracle outcome for terminal candidate: got %s (rule=%s, detail=%s), want %s",
			outcome.Class, outcome.Rule, outcome.Detail, oracle.ClassValid)
	}

	t.Logf("Candidate (%d, %d%%, seed=%d): Outcome=%s",
		terminalCand.Event, terminalCand.Percent, terminalCand.Seed, outcome.Class)
}
