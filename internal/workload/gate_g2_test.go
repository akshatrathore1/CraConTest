package workload_test

import (
	"strings"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// TestGateG2 checks the Gate G2 acceptance criteria:
// 1. Hand-built oracle reference cases are all classified correctly.
// 2. Real 1,000-op workload trace passes trace.Validate and exhibits WAL,
//    manifest, table, file sync, and directory sync events.
// 3. MUT-0 null control over the whole endpoint universe has zero violations,
//    open errors, panics, hangs, or oracle errors.
// 4. Exact counts (universe size, cutoffs, events, latency) are recorded.
func TestGateG2(t *testing.T) {
	// Part 1: Hand-built oracle cases classification check.
	t.Run("HandBuiltOracleCases", func(t *testing.T) {
		verifyOracleReferenceCases(t)
	})

	// Part 2: Execute real 1,000-op workload on recording VFS.
	t.Log("Running 1,000-operation workload with DefaultConfig...")
	startWorkload := time.Now()

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

	cfg := workload.DefaultConfig()
	rawHistory, err := workload.Run(db, rec, cfg)
	if err != nil {
		t.Fatalf("workload.Run: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
	workloadDuration := time.Since(startWorkload)
	t.Logf("Workload finished in %v", workloadDuration)

	// Validate trace and history.
	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}
	t.Logf("Recorded trace event count: %d", len(traceEvents))

	// Correlate history with trace events.
	correlatedHistory, err := workload.Correlate(rawHistory, traceEvents)
	if err != nil {
		t.Fatalf("workload.Correlate: %v", err)
	}
	if err := history.Validate(correlatedHistory); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}
	t.Logf("Correlated history entry count: %d", len(correlatedHistory))

	// Part 3: Verify trace activity diversity (WAL, manifest, table, file sync, dir sync).
	verifyTraceActivity(t, traceEvents)

	// Part 4: Build endpoint universe.
	universe, err := candidate.Build(traceEvents, cfg.Seed, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	cuts, err := trace.CutIDs(traceEvents)
	if err != nil {
		t.Fatalf("trace.CutIDs: %v", err)
	}
	t.Logf("Endpoint universe: %d cutoffs, %d total candidates (policies [0, 100])", len(cuts), len(universe))

	// Part 5: Execute MUT-0 null control over the whole endpoint universe.
	t.Log("Executing MUT-0 null control over full endpoint universe...")
	startReplay := time.Now()

	replayer, err := replay.Build(traceEvents, 0)
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer replayer.Close()

	var currentCut uint64
	var validCount, violationCount, openErrCount, panicCount, hangCount, oracleErrCount int

	for i, cand := range universe {
		if cand.Event > currentCut {
			if err := replayer.Advance(cand.Event); err != nil {
				t.Fatalf("candidate %d (cut=%d, pol=%d): replayer.Advance: %v", i, cand.Event, cand.Percent, err)
			}
			currentCut = cand.Event
		}

		clonedFS, err := replayer.Clone(cand.Percent, cand.Seed)
		if err != nil {
			t.Fatalf("candidate %d (cut=%d, pol=%d): Clone: %v", i, cand.Event, cand.Percent, err)
		}

		openRes := recovery.SafeOpen(clonedFS, "db", 5*time.Second)
		if openRes.Panic != "" {
			panicCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): panic: %s", i, cand.Event, cand.Percent, openRes.Panic)
			continue
		}
		if openRes.Timeout {
			hangCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): hang/timeout", i, cand.Event, cand.Percent)
			continue
		}
		if openRes.Err != nil {
			openErrCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): open error: %v", i, cand.Event, cand.Percent, openRes.Err)
			continue
		}
		if openRes.DB == nil {
			openErrCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): nil DB without error", i, cand.Event, cand.Percent)
			continue
		}

		if err := recovery.CheckIntegrity(openRes.DB); err != nil {
			_ = openRes.DB.Close()
			openErrCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): CheckIntegrity: %v", i, cand.Event, cand.Percent, err)
			continue
		}

		dump, err := recovery.DumpContents(openRes.DB)
		_ = openRes.DB.Close()
		if err != nil {
			openErrCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): DumpContents: %v", i, cand.Event, cand.Percent, err)
			continue
		}

		outcome := oracle.Check(oracle.Context{
			Candidate: cand,
			Events:    traceEvents,
			History:   correlatedHistory,
			Recovered: dump,
		})

		switch outcome.Class {
		case oracle.ClassValid:
			validCount++
		case oracle.ClassViolation:
			violationCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): violation: rule=%s sig=%s detail=%s",
				i, cand.Event, cand.Percent, outcome.Rule, outcome.Signature, outcome.Detail)
		case oracle.ClassOracleError:
			oracleErrCount++
			t.Errorf("candidate %d (cut=%d, pol=%d): oracle error: rule=%s detail=%s",
				i, cand.Event, cand.Percent, outcome.Rule, outcome.Detail)
		default:
			t.Errorf("candidate %d (cut=%d, pol=%d): unexpected outcome class: %s",
				i, cand.Event, cand.Percent, outcome.Class)
		}
	}

	evalDuration := time.Since(startReplay)
	avgLatency := time.Duration(0)
	if len(universe) > 0 {
		avgLatency = evalDuration / time.Duration(len(universe))
	}

	t.Logf("=== Gate G2 Results ===")
	t.Logf("Universe size:       %d candidates", len(universe))
	t.Logf("Cutoffs:             %d distinct event cutoffs", len(cuts))
	t.Logf("Trace events:        %d", len(traceEvents))
	t.Logf("Valid outcomes:      %d", validCount)
	t.Logf("Violations:          %d", violationCount)
	t.Logf("Open errors:         %d", openErrCount)
	t.Logf("Panics:              %d", panicCount)
	t.Logf("Hangs:               %d", hangCount)
	t.Logf("Oracle errors:       %d", oracleErrCount)
	t.Logf("Total eval time:     %v (avg %v per candidate)", evalDuration, avgLatency)

	if violationCount != 0 || openErrCount != 0 || panicCount != 0 || hangCount != 0 || oracleErrCount != 0 {
		t.Fatalf("Gate G2 failed: violations=%d, open_errors=%d, panics=%d, hangs=%d, oracle_errors=%d",
			violationCount, openErrCount, panicCount, hangCount, oracleErrCount)
	}
	if validCount != len(universe) {
		t.Fatalf("Gate G2 failed: valid count %d != universe length %d", validCount, len(universe))
	}
}

func verifyTraceActivity(t *testing.T, events []trace.Event) {
	var walCount, manifestCount, tableCount, fileSyncCount, dirSyncCount int

	for _, e := range events {
		if e.Cat == "pebble-wal" || strings.HasSuffix(e.File, ".log") {
			walCount++
		}
		if e.Cat == "pebble-manifest" || strings.Contains(e.File, "MANIFEST") {
			manifestCount++
		}
		if e.Cat == "pebble-memtable-flush" || e.Cat == "pebble-compaction" || strings.HasSuffix(e.File, ".sst") {
			tableCount++
		}
		if (e.Kind == trace.KindSync || e.Kind == trace.KindSyncData) && !e.Dir {
			fileSyncCount++
		}
		if e.Kind == trace.KindSync && e.Dir {
			dirSyncCount++
		}
	}

	t.Logf("Trace activity counts: wal=%d, manifest=%d, table=%d, fileSync=%d, dirSync=%d",
		walCount, manifestCount, tableCount, fileSyncCount, dirSyncCount)

	if walCount == 0 {
		t.Errorf("trace has zero WAL events")
	}
	if manifestCount == 0 {
		t.Errorf("trace has zero manifest events")
	}
	if tableCount == 0 {
		t.Errorf("trace has zero table (flush/compaction/sst) events")
	}
	if fileSyncCount == 0 {
		t.Errorf("trace has zero file sync events")
	}
	if dirSyncCount == 0 {
		t.Errorf("trace has zero directory sync events")
	}
}

func verifyOracleReferenceCases(t *testing.T) {
	setMut := func(k, v string) history.Mutation {
		return history.Mutation{Kind: "set", Key: []byte(k), Value: []byte(v)}
	}
	entry := func(op, begin, end uint64, sync bool, kind string, muts ...history.Mutation) history.Entry {
		return history.Entry{
			Op:         op,
			Kind:       kind,
			Mutations:  muts,
			Sync:       sync,
			BeginEvent: begin,
			EndEvent:   end,
		}
	}

	first := entry(1, 1, 2, true, "set", setMut("a", "A"))
	second := entry(2, 3, 4, false, "set", setMut("b", "B"))
	batch := entry(2, 3, 0, true, "batch", setMut("b", "B"), setMut("c", "C"))
	failed := second
	failed.Err = "write failed"

	cases := []struct {
		name  string
		hist  []history.Entry
		cut   uint64
		state oracle.State
		class oracle.Class
		rule  string
	}{
		{"pretrace", nil, 0, oracle.State{}, oracle.ClassValid, ""},
		{"ack at cutoff", []history.Entry{first}, 2, oracle.State{"a": "A"}, oracle.ClassValid, ""},
		{"lost synced prefix", []history.Entry{first}, 2, oracle.State{}, oracle.ClassViolation, "durably_committed"},
		{"optional NoSync suffix", []history.Entry{first, second}, 4, oracle.State{"a": "A"}, oracle.ClassValid, ""},
		{"hole", []history.Entry{first, second}, 4, oracle.State{"b": "B"}, oracle.ClassViolation, "prefix"},
		{"inflight batch absent", []history.Entry{first, batch}, 3, oracle.State{"a": "A"}, oracle.ClassValid, ""},
		{"inflight batch whole", []history.Entry{first, batch}, 3, oracle.State{"a": "A", "b": "B", "c": "C"}, oracle.ClassValid, ""},
		{"inflight batch torn", []history.Entry{first, batch}, 3, oracle.State{"a": "A", "b": "B"}, oracle.ClassViolation, "atomic_batch"},
		{"before batch begin", []history.Entry{first, batch}, 2, oracle.State{"a": "A"}, oracle.ClassValid, ""},
		{"failed acknowledged mutation", []history.Entry{first, failed}, 4, oracle.State{"a": "A"}, oracle.ClassOracleError, "failed_mutation"},
	}

	for _, tc := range cases {
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: tc.cut, Percent: 100, Seed: 1},
			History:   tc.hist,
			Recovered: tc.state,
		}
		outcome := oracle.Check(ctx)
		if outcome.Class != tc.class {
			t.Fatalf("oracle reference case %q: got class %s, want %s", tc.name, outcome.Class, tc.class)
		}
		if outcome.Rule != tc.rule {
			t.Fatalf("oracle reference case %q: got rule %s, want %s", tc.name, outcome.Rule, tc.rule)
		}
	}
}
