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

// TestEndpointCrashPipeline checks the crash pipeline end-to-end:
// replay -> clone -> SafeOpen -> CheckIntegrity -> dump -> oracle
// across all 0% and 100% endpoint candidates, verifying ClassValid outcomes.
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
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	var hist []history.Entry
	numOps := 30

	for i := 0; i < numOps; i++ {
		opID := uint64(i + 1)
		k := fmt.Sprintf("pipeline-k%03d", i)
		v := fmt.Sprintf("pipeline-v%03d", i)
		sync := (i%5 == 0)

		beginEv := rec.BeginEvent(opID, "set")
		wo := pebble.NoSync
		if sync {
			wo = pebble.Sync
		}
		if err := db.Set([]byte(k), []byte(v), wo); err != nil {
			_ = rec.EndEvent(opID, "set", err.Error())
			t.Fatalf("Set %d: %v", i, err)
		}
		endEv := rec.EndEvent(opID, "set", "")

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
	closed = true
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

	state, err := replay.Build(traceEvents, 0)
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer func() {
		if err := state.Close(); err != nil {
			t.Errorf("state.Close: %v", err)
		}
	}()

	var currentCut uint64
	var seenZero, seenHundred int
	for i, cand := range universe {
		if cand.Event > currentCut {
			if err := state.Advance(cand.Event); err != nil {
				t.Fatalf("candidate %d state.Advance(%d): %v", i, cand.Event, err)
			}
			currentCut = cand.Event
		}
		clonedFS, err := state.Clone(cand.Percent, cand.Seed)
		if err != nil {
			t.Fatalf("candidate %d state.Clone: %v", i, err)
		}

		openRes := recovery.SafeOpen(clonedFS, "db", 10*time.Second)
		if openRes.Panic != "" || openRes.Timeout || openRes.Err != nil {
			t.Fatalf("candidate %d (%+v) SafeOpen failed: err=%v, panic=%s, timeout=%v",
				i, cand, openRes.Err, openRes.Panic, openRes.Timeout)
		}
		if openRes.DB == nil {
			t.Fatalf("candidate %d (%+v) SafeOpen returned nil DB", i, cand)
		}

		if err := recovery.CheckIntegrity(openRes.DB); err != nil {
			_ = openRes.DB.Close()
			t.Fatalf("candidate %d (%+v) CheckIntegrity failed: %v", i, cand, err)
		}

		dump, err := recovery.DumpContents(openRes.DB)
		closeErr := openRes.DB.Close()
		if err != nil {
			t.Fatalf("candidate %d (%+v) DumpContents failed: %v", i, cand, err)
		}
		if closeErr != nil {
			t.Fatalf("candidate %d (%+v) Close failed: %v", i, cand, closeErr)
		}

		outcome := oracle.Check(oracle.Context{
			Candidate: cand,
			Events:    traceEvents,
			History:   hist,
			Recovered: dump,
		})
		if outcome.Class != oracle.ClassValid {
			t.Fatalf("oracle outcome for candidate %+v: got %s (rule=%s, detail=%s), want %s",
				cand, outcome.Class, outcome.Rule, outcome.Detail, oracle.ClassValid)
		}
		switch cand.Percent {
		case 0:
			seenZero++
		case 100:
			seenHundred++
		}
	}
	if seenZero == 0 || seenHundred == 0 {
		t.Fatalf("expected both 0%% and 100%% endpoint candidates, got zero=%d hundred=%d", seenZero, seenHundred)
	}
	t.Logf("Verified %d endpoint candidates (0%%=%d, 100%%=%d): all %s", len(universe), seenZero, seenHundred, oracle.ClassValid)
}
