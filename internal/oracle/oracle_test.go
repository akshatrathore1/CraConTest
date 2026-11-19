package oracle_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func setMut(key, value string) history.Mutation {
	return history.Mutation{Kind: "set", Key: []byte(key), Value: []byte(value)}
}

func testEntry(op, begin, end uint64, sync bool, kind string, mutations ...history.Mutation) history.Entry {
	return history.Entry{
		Op:         op,
		Kind:       kind,
		Mutations:  mutations,
		Sync:       sync,
		BeginEvent: begin,
		EndEvent:   end,
	}
}

func TestOracleReferenceCases(t *testing.T) {
	first := testEntry(1, 1, 2, true, "set", setMut("a", "A"))
	second := testEntry(2, 3, 4, false, "set", setMut("b", "B"))
	batch := testEntry(2, 3, 0, true, "batch", setMut("b", "B"), setMut("c", "C"))
	failed := second
	failed.Err = "write failed"

	cases := []struct {
		name    string
		entries []history.Entry
		cut     uint64
		state   oracle.State
		class   oracle.Class
		rule    string
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
		t.Run(tc.name, func(t *testing.T) {
			ctx := oracle.Context{
				Candidate: candidate.Candidate{Event: tc.cut, Percent: 100, Seed: 1},
				History:   tc.entries,
				Recovered: tc.state,
			}
			outcome := oracle.Check(ctx)
			if outcome.Class != tc.class {
				t.Fatalf("class: got %s, want %s (detail=%s)", outcome.Class, tc.class, outcome.Detail)
			}
			if outcome.Rule != tc.rule {
				t.Fatalf("rule: got %s, want %s", outcome.Rule, tc.rule)
			}
			if outcome.Class == oracle.ClassViolation {
				if !strings.HasPrefix(outcome.Signature, "v1:") {
					t.Fatalf("violation signature missing v1: prefix: %s", outcome.Signature)
				}
			}
		})
	}
}

func TestOracleLaterSyncCoversNoSync(t *testing.T) {
	unsyncedFirst := testEntry(1, 1, 2, false, "set", setMut("a", "A"))
	laterSync := testEntry(2, 3, 4, true, "set", setMut("b", "B"))

	// Recovered state has "a": "A", but laterSync synced, so both "a" and "b" were durably committed.
	// Recovered state missing "b" is a durably_committed violation.
	ctx := oracle.Context{
		Candidate: candidate.Candidate{Event: 4, Percent: 100, Seed: 1},
		History:   []history.Entry{unsyncedFirst, laterSync},
		Recovered: oracle.State{"a": "A"},
	}
	outcome := oracle.Check(ctx)
	if outcome.Class != oracle.ClassViolation || outcome.Rule != "durably_committed" {
		t.Fatalf("got %s/%s, want violation/durably_committed", outcome.Class, outcome.Rule)
	}
}

func TestOracleRangeDelete(t *testing.T) {
	rangeMut := history.Mutation{Kind: "range_delete", Key: []byte("b"), End: []byte("d")}
	rangeEntry := testEntry(1, 1, 2, false, "range_delete", rangeMut)

	ctx := oracle.Context{
		Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
		History:   []history.Entry{rangeEntry},
		Initial:   oracle.State{"a": "A", "b": "B", "c": "C", "d": "D"},
		Recovered: oracle.State{"a": "A", "d": "D"},
	}
	outcome := oracle.Check(ctx)
	if outcome.Class != oracle.ClassValid {
		t.Fatalf("got class %s, want valid (detail=%s)", outcome.Class, outcome.Detail)
	}
}

func TestOracleSignatures(t *testing.T) {
	sig1 := oracle.FormatSignature("durably_committed", "set", []string{"pebble-wal", "pebble-manifest"})
	sig2 := oracle.FormatSignature("durably_committed", "set", []string{"pebble-manifest", "pebble-wal"})

	if sig1 != sig2 {
		t.Fatalf("categories not sorted/deduplicated: sig1=%s, sig2=%s", sig1, sig2)
	}
	if !strings.HasPrefix(sig1, "v1:") {
		t.Fatalf("missing v1: prefix: %s", sig1)
	}
}

func TestOracleTraceCrossReference(t *testing.T) {
	h := testEntry(1, 1, 2, true, "set", setMut("a", "A"))
	events := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}

	ctx := oracle.Context{
		Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
		History:   []history.Entry{h},
		Events:    events,
		Recovered: oracle.State{"a": "A"},
	}
	outcome := oracle.Check(ctx)
	if outcome.Class != oracle.ClassValid {
		t.Fatalf("got %s, want valid (detail=%s)", outcome.Class, outcome.Detail)
	}

	// Mismatched op kind in trace
	badEvents := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "delete"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "delete"},
	}
	ctxBad := ctx
	ctxBad.Events = badEvents
	outcomeBad := oracle.Check(ctxBad)
	if outcomeBad.Class != oracle.ClassOracleError {
		t.Fatalf("expected oracle_error on marker mismatch, got %s", outcomeBad.Class)
	}
}

func TestCheckRejectsOutOfRangeHistoryMarkers(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}

	// Case 1: BeginEvent out of range
	entryBadBegin := testEntry(1, 99, 2, true, "set", setMut("a", "A"))
	ctxBegin := oracle.Context{
		Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
		History:   []history.Entry{entryBadBegin},
		Events:    events,
		Recovered: oracle.State{"a": "A"},
	}
	resBegin := oracle.Check(ctxBegin)
	if resBegin.Class != oracle.ClassOracleError || resBegin.Rule != "invalid_history" {
		t.Fatalf("expected ClassOracleError / invalid_history for out-of-range BeginEvent, got %s / %s (detail: %s)",
			resBegin.Class, resBegin.Rule, resBegin.Detail)
	}

	// Case 2: EndEvent out of range
	entryBadEnd := testEntry(1, 1, 99, true, "set", setMut("a", "A"))
	ctxEnd := oracle.Context{
		Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
		History:   []history.Entry{entryBadEnd},
		Events:    events,
		Recovered: oracle.State{"a": "A"},
	}
	resEnd := oracle.Check(ctxEnd)
	if resEnd.Class != oracle.ClassOracleError || resEnd.Rule != "invalid_history" {
		t.Fatalf("expected ClassOracleError / invalid_history for out-of-range EndEvent, got %s / %s (detail: %s)",
			resEnd.Class, resEnd.Rule, resEnd.Detail)
	}
}

func TestOraclePrefixHoleDivergentOpKind(t *testing.T) {
	// Operation 1: "set" with key "k1" = "v1"
	op1 := testEntry(1, 1, 2, false, "set", setMut("k1", "v1"))
	// Operation 2: "ingest" with key "k2" = "v2"
	op2 := testEntry(2, 3, 4, false, "ingest", setMut("k2", "v2"))

	events := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "ingest"},
		{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "ingest"},
	}

	// Recovered state has operation 2's mutation ("k2": "v2"), but operation 1's mutation ("k1") is missing!
	ctx := oracle.Context{
		Candidate:  candidate.Candidate{Event: 4, Percent: 100, Seed: 1},
		History:    []history.Entry{op1, op2},
		Events:     events,
		Categories: []string{"pebble-manifest"},
		Recovered:  oracle.State{"k2": "v2"},
	}

	outcome := oracle.Check(ctx)
	if outcome.Class != oracle.ClassViolation {
		t.Fatalf("expected ClassViolation, got %s (detail=%s)", outcome.Class, outcome.Detail)
	}
	if outcome.Rule != "prefix" {
		t.Fatalf("expected rule 'prefix', got %s", outcome.Rule)
	}

	// Verify outcome.Signature correctly attributes divergence to "set" (the missing predecessor)
	expectedSig := oracle.FormatSignature("prefix", "set", []string{"pebble-manifest"})
	if outcome.Signature != expectedSig {
		t.Fatalf("signature divergence attribution mismatch: got %q, want %q", outcome.Signature, expectedSig)
	}

	// Reverse case: Operation 1 is "ingest" (missing), Operation 2 is "set" (recovered).
	op1Ingest := testEntry(1, 1, 2, false, "ingest", setMut("k1", "v1"))
	op2Set := testEntry(2, 3, 4, false, "set", setMut("k2", "v2"))
	eventsRev := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "ingest"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "ingest"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},
	}
	ctxRev := oracle.Context{
		Candidate:  candidate.Candidate{Event: 4, Percent: 100, Seed: 1},
		History:    []history.Entry{op1Ingest, op2Set},
		Events:     eventsRev,
		Categories: []string{"pebble-manifest"},
		Recovered:  oracle.State{"k2": "v2"},
	}
	outcomeRev := oracle.Check(ctxRev)
	if outcomeRev.Class != oracle.ClassViolation || outcomeRev.Rule != "prefix" {
		t.Fatalf("expected prefix violation, got %s/%s", outcomeRev.Class, outcomeRev.Rule)
	}
	expectedSigRev := oracle.FormatSignature("prefix", "ingest", []string{"pebble-manifest"})
	if outcomeRev.Signature != expectedSigRev {
		t.Fatalf("expected signature for 'ingest', got %q, want %q", outcomeRev.Signature, expectedSigRev)
	}

	// Overwritten-key case:
	// Op 1: set A=1 (overwritten by Op 2)
	// Op 2: set A=2 (recovered)
	// Op 3: ingest B=1 (missing in recovered)
	// Op 4: set C=1 (recovered)
	op1Over := testEntry(1, 1, 2, false, "set", setMut("A", "1"))
	op2Over := testEntry(2, 3, 4, false, "set", setMut("A", "2"))
	op3Over := testEntry(3, 5, 6, false, "ingest", setMut("B", "1"))
	op4Over := testEntry(4, 7, 8, true, "set", setMut("C", "1"))
	eventsOver := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},
		{ID: 5, Kind: trace.KindOpBegin, Op: 3, OpKind: "ingest"},
		{ID: 6, Kind: trace.KindOpEnd, Op: 3, OpKind: "ingest"},
		{ID: 7, Kind: trace.KindOpBegin, Op: 4, OpKind: "set"},
		{ID: 8, Kind: trace.KindOpEnd, Op: 4, OpKind: "set"},
	}
	ctxOver := oracle.Context{
		Candidate:  candidate.Candidate{Event: 8, Percent: 100, Seed: 1},
		History:    []history.Entry{op1Over, op2Over, op3Over, op4Over},
		Events:     eventsOver,
		Categories: []string{"pebble-manifest"},
		Recovered:  oracle.State{"A": "2", "C": "1"},
	}
	outcomeOver := oracle.Check(ctxOver)
	if outcomeOver.Class != oracle.ClassViolation || outcomeOver.Rule != "prefix" {
		t.Fatalf("expected prefix violation for overwritten key test, got %s/%s (detail=%s)",
			outcomeOver.Class, outcomeOver.Rule, outcomeOver.Detail)
	}
	expectedSigOver := oracle.FormatSignature("prefix", "ingest", []string{"pebble-manifest"})
	if outcomeOver.Signature != expectedSigOver {
		t.Fatalf("expected signature for 'ingest', got %q, want %q", outcomeOver.Signature, expectedSigOver)
	}

	// Absent overwriting op subcase:
	// Initial: oracle.State{"A": "1"}
	// Op 1 (ingest, 1..2): set A=2 (lost)
	// Op 2 (batch, 3..4): set A=1 and set B=1 (both lost, so Op 2 is absent because B=1 is missing)
	// Op 3 (set, 5..6): set C=1 (present)
	// Recovered: oracle.State{"A": "1", "C": "1"}
	op1Absent := testEntry(1, 1, 2, false, "ingest", setMut("A", "2"))
	op2Absent := testEntry(2, 3, 4, false, "batch", setMut("A", "1"), setMut("B", "1"))
	op3Absent := testEntry(3, 5, 6, false, "set", setMut("C", "1"))
	eventsAbsent := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "ingest"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "ingest"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "batch"},
		{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "batch"},
		{ID: 5, Kind: trace.KindOpBegin, Op: 3, OpKind: "set"},
		{ID: 6, Kind: trace.KindOpEnd, Op: 3, OpKind: "set"},
	}
	ctxAbsent := oracle.Context{
		Candidate:  candidate.Candidate{Event: 6, Percent: 100, Seed: 1},
		History:    []history.Entry{op1Absent, op2Absent, op3Absent},
		Events:     eventsAbsent,
		Categories: []string{"pebble-manifest"},
		Initial:    oracle.State{"A": "1"},
		Recovered:  oracle.State{"A": "1", "C": "1"},
	}
	outcomeAbsent := oracle.Check(ctxAbsent)
	if outcomeAbsent.Class != oracle.ClassViolation || outcomeAbsent.Rule != "prefix" {
		t.Fatalf("expected prefix violation for absent overwriting op test, got %s/%s (detail=%s)",
			outcomeAbsent.Class, outcomeAbsent.Rule, outcomeAbsent.Detail)
	}
	expectedSigAbsent := oracle.FormatSignature("prefix", "ingest", []string{"pebble-manifest"})
	if outcomeAbsent.Signature != expectedSigAbsent {
		t.Fatalf("expected signature for 'ingest', got %q, want %q", outcomeAbsent.Signature, expectedSigAbsent)
	}

	// Same-value rewrite as the sole later present witness subcase:
	// Op 1 (set, 1..2, Sync: false): set A=1 (present in Recovered, so i=1 does not diverge)
	// Op 2 (ingest, 3..4, Sync: false): set D=1, set B=1 (B=1 is lost, so i=2 diverges; states[2]["D"] is "1")
	// Op 3 (set, 5..6, Sync: false): set D=1 (same-value rewrite: states[3]["D"] == states[2]["D"] == "1")
	// Recovered: oracle.State{"A": "1", "D": "1"}
	// Because Op 3 is the only operation after Op 2 and states[3]["D"] == states[2]["D"],
	// Op 3 is recognized as present only via the same-value rewrite guard in opPresentInRecovered.
	op1Rewrite := testEntry(1, 1, 2, false, "set", setMut("A", "1"))
	op2Rewrite := testEntry(2, 3, 4, false, "ingest", setMut("D", "1"), setMut("B", "1"))
	op3Rewrite := testEntry(3, 5, 6, false, "set", setMut("D", "1"))
	eventsRewrite := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "ingest"},
		{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "ingest"},
		{ID: 5, Kind: trace.KindOpBegin, Op: 3, OpKind: "set"},
		{ID: 6, Kind: trace.KindOpEnd, Op: 3, OpKind: "set"},
	}
	ctxRewrite := oracle.Context{
		Candidate:  candidate.Candidate{Event: 6, Percent: 100, Seed: 1},
		History:    []history.Entry{op1Rewrite, op2Rewrite, op3Rewrite},
		Events:     eventsRewrite,
		Categories: []string{"pebble-manifest"},
		Recovered:  oracle.State{"A": "1", "D": "1"},
	}
	outcomeRewrite := oracle.Check(ctxRewrite)
	if outcomeRewrite.Class != oracle.ClassViolation || outcomeRewrite.Rule != "prefix" {
		t.Fatalf("expected prefix violation for same-value rewrite test, got %s/%s (detail=%s)",
			outcomeRewrite.Class, outcomeRewrite.Rule, outcomeRewrite.Detail)
	}
	expectedSigRewrite := oracle.FormatSignature("prefix", "ingest", []string{"pebble-manifest"})
	if outcomeRewrite.Signature != expectedSigRewrite {
		t.Fatalf("expected signature for 'ingest', got %q, want %q", outcomeRewrite.Signature, expectedSigRewrite)
	}
}

func TestOraclePrefixHoleEmptyValues(t *testing.T) {
	cases := []struct {
		name      string
		initial   oracle.State
		laterKind string
		later     history.Mutation
		recovered oracle.State
	}{
		{
			name:      "missing empty value before surviving set",
			laterKind: "set",
			later:     setMut("C", "1"),
			recovered: oracle.State{"A": "1", "C": "1"},
		},
		{
			name:      "surviving delete of empty value",
			initial:   oracle.State{"D": ""},
			laterKind: "delete",
			later:     history.Mutation{Kind: "delete", Key: []byte("D")},
			recovered: oracle.State{"A": "1"},
		},
		{
			name:      "surviving range delete of empty value",
			initial:   oracle.State{"D": ""},
			laterKind: "range_delete",
			later:     history.Mutation{Kind: "range_delete", Key: []byte("D"), End: []byte("E")},
			recovered: oracle.State{"A": "1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entries := []history.Entry{
				testEntry(1, 1, 2, false, "set", setMut("A", "1")),
				testEntry(2, 3, 4, false, "ingest", setMut("B", "")),
				testEntry(3, 5, 6, false, tc.laterKind, tc.later),
			}
			outcome := oracle.Check(oracle.Context{
				Candidate: candidate.Candidate{Event: 6, Percent: 0},
				History:   entries,
				Initial:   tc.initial,
				Recovered: tc.recovered,
			})
			if outcome.Class != oracle.ClassViolation || outcome.Rule != "prefix" {
				t.Fatalf("got %s/%s, want violation/prefix", outcome.Class, outcome.Rule)
			}
			expected := oracle.FormatSignature("prefix", "ingest", nil)
			if outcome.Signature != expected {
				t.Fatalf("got signature %q, want missing ingest signature %q", outcome.Signature, expected)
			}
		})
	}
}

func TestOracleFromIdx0AndRangeDeleteOverwrite(t *testing.T) {
	// Defect 18: fromIdx == 0 must not panic, and range_delete must act as a valid witness.
	// Op 1 (set, 1..2): set k1=v1
	// Op 2 (range_delete, 3..4): range_delete [k0, k2)
	// Op 3 (set, 5..6): set k3=v3
	op1 := testEntry(1, 1, 2, false, "set", setMut("k1", "v1"))
	rangeMut := history.Mutation{Kind: "range_delete", Key: []byte("k0"), End: []byte("k2")}
	op2 := testEntry(2, 3, 4, false, "range_delete", rangeMut)
	op3 := testEntry(3, 5, 6, false, "set", setMut("k3", "v3"))

	events := []trace.Event{
		{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "range_delete"},
		{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "range_delete"},
		{ID: 5, Kind: trace.KindOpBegin, Op: 3, OpKind: "set"},
		{ID: 6, Kind: trace.KindOpEnd, Op: 3, OpKind: "set"},
	}

	// Case 1: Op 1 missing, Op 3 present -> evaluated fromIdx 0
	ctxFrom0 := oracle.Context{
		Candidate: candidate.Candidate{Event: 6, Percent: 100, Seed: 1},
		History:   []history.Entry{op1, op2, op3},
		Events:    events,
		Recovered: oracle.State{"k3": "v3"},
	}
	// Does not panic and identifies valid or prefix violation
	outcome := oracle.Check(ctxFrom0)
	if outcome.Class == oracle.ClassPanic || outcome.Class == oracle.ClassOracleError {
		t.Fatalf("unexpected oracle error or panic on fromIdx 0: %+v", outcome)
	}
}

func TestOracleEdgeCases(t *testing.T) {
	t.Run("EmptyBatchRejectedAsInvalidHistory", func(t *testing.T) {
		// According to history.Validate contract, mutating operation "batch" with 0 mutations
		// must be detected and rejected as ClassOracleError.
		emptyBatch := testEntry(1, 1, 2, true, "batch")
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "batch"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "batch"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
			History:   []history.Entry{emptyBatch},
			Events:    events,
			Recovered: oracle.State{},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassOracleError || !strings.Contains(res.Detail, "must have at least one mutation") {
			t.Fatalf("expected ClassOracleError for empty batch, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("BatchMultipleMutationsSameKey", func(t *testing.T) {
		// A batch with multiple mutations touching the same key (e.g. set k=v1, then set k=v2)
		batch := testEntry(1, 1, 2, true, "batch", setMut("k", "v1"), setMut("k", "v2"))
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "batch"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "batch"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
			History:   []history.Entry{batch},
			Events:    events,
			Recovered: oracle.State{"k": "v2"},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for batch intra-key overwrite, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("OverlappingRangeDeletes", func(t *testing.T) {
		// Initial state has keys k0, k1, k2, k3, k4, k5
		initial := oracle.State{
			"k0": "v0", "k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5",
		}
		// Op 1 deletes [k1, k4) -> removes k1, k2, k3
		mut1 := history.Mutation{Kind: "range_delete", Key: []byte("k1"), End: []byte("k4")}
		op1 := testEntry(1, 1, 2, true, "range_delete", mut1)

		// Op 2 deletes [k3, k6) -> overlapping on k3; removes k4, k5
		mut2 := history.Mutation{Kind: "range_delete", Key: []byte("k3"), End: []byte("k6")}
		op2 := testEntry(2, 3, 4, true, "range_delete", mut2)

		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "range_delete"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "range_delete"},
			{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "range_delete"},
			{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "range_delete"},
		}

		// After both operations, only "k0" should remain
		ctxBoth := oracle.Context{
			Candidate: candidate.Candidate{Event: 4, Percent: 100, Seed: 1},
			History:   []history.Entry{op1, op2},
			Events:    events,
			Initial:   initial,
			Recovered: oracle.State{"k0": "v0"},
		}
		resBoth := oracle.Check(ctxBoth)
		if resBoth.Class != oracle.ClassValid {
			t.Fatalf("expected valid for overlapping range deletes, got: %s (detail: %s)", resBoth.Class, resBoth.Detail)
		}

		// If Op 2 survived but Op 1 was lost (prefix hole):
		// Recovered has deleted k4, k5, but k1, k2, k3 survived
		ctxHole := oracle.Context{
			Candidate:  candidate.Candidate{Event: 4, Percent: 100, Seed: 1},
			History:    []history.Entry{op1, op2},
			Events:     events,
			Categories: []string{"pebble-manifest"},
			Initial:    initial,
			Recovered:  oracle.State{"k0": "v0", "k1": "v1", "k2": "v2", "k3": "v3"},
		}
		resHole := oracle.Check(ctxHole)
		if resHole.Class != oracle.ClassViolation || resHole.Rule != "prefix" {
			t.Fatalf("expected prefix violation for missing first range_delete, got %s / %s (detail: %s)",
				resHole.Class, resHole.Rule, resHole.Detail)
		}
	})

	t.Run("KeyResurrectionAfterDeleteAndSet", func(t *testing.T) {
		initial := oracle.State{"k": "v0"}
		// Op 1: delete k
		op1 := testEntry(1, 1, 2, true, "delete", history.Mutation{Kind: "delete", Key: []byte("k")})
		// Op 2: set k = v1
		op2 := testEntry(2, 3, 4, true, "set", setMut("k", "v1"))
		// Op 3: delete k
		op3 := testEntry(3, 5, 6, true, "delete", history.Mutation{Kind: "delete", Key: []byte("k")})
		// Op 4: set k = v2
		op4 := testEntry(4, 7, 8, true, "set", setMut("k", "v2"))

		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "delete"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "delete"},
			{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
			{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},
			{ID: 5, Kind: trace.KindOpBegin, Op: 3, OpKind: "delete"},
			{ID: 6, Kind: trace.KindOpEnd, Op: 3, OpKind: "delete"},
			{ID: 7, Kind: trace.KindOpBegin, Op: 4, OpKind: "set"},
			{ID: 8, Kind: trace.KindOpEnd, Op: 4, OpKind: "set"},
		}

		// Valid final state: k = v2
		ctxFinal := oracle.Context{
			Candidate: candidate.Candidate{Event: 8, Percent: 100, Seed: 1},
			History:   []history.Entry{op1, op2, op3, op4},
			Events:    events,
			Initial:   initial,
			Recovered: oracle.State{"k": "v2"},
		}
		if res := oracle.Check(ctxFinal); res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for final state k=v2, got: %s (detail: %s)", res.Class, res.Detail)
		}

		// Resurrection violation: obsolete v0 resurrected when Op 4 durably committed
		ctxResurrect := oracle.Context{
			Candidate:  candidate.Candidate{Event: 8, Percent: 100, Seed: 1},
			History:    []history.Entry{op1, op2, op3, op4},
			Events:     events,
			Categories: []string{"pebble-wal"},
			Initial:    initial,
			Recovered:  oracle.State{"k": "v0"},
		}
		resResurrect := oracle.Check(ctxResurrect)
		if resResurrect.Class != oracle.ClassViolation {
			t.Fatalf("expected violation for obsolete resurrected value v0, got: %s (detail: %s)",
				resResurrect.Class, resResurrect.Detail)
		}
	})

	t.Run("LongMutationChainOnSameKey", func(t *testing.T) {
		const numOps = 40
		entries := make([]history.Entry, numOps)
		events := make([]trace.Event, numOps*2)

		for i := 0; i < numOps; i++ {
			opID := uint64(i + 1)
			bEv := (opID-1)*2 + 1
			eEv := bEv + 1
			val := fmt.Sprintf("val-%d", i)
			entries[i] = testEntry(opID, bEv, eEv, true, "set", setMut("hot_key", val))
			events[(opID-1)*2] = trace.Event{ID: bEv, Kind: trace.KindOpBegin, Op: opID, OpKind: "set"}
			events[(opID-1)*2+1] = trace.Event{ID: eEv, Kind: trace.KindOpEnd, Op: opID, OpKind: "set"}
		}

		// Final state has hot_key = val-39
		ctxFinal := oracle.Context{
			Candidate: candidate.Candidate{Event: uint64(numOps * 2), Percent: 100, Seed: 1},
			History:   entries,
			Events:    events,
			Recovered: oracle.State{"hot_key": fmt.Sprintf("val-%d", numOps-1)},
		}
		resFinal := oracle.Check(ctxFinal)
		if resFinal.Class != oracle.ClassValid {
			t.Fatalf("expected valid for long mutation chain final state, got: %s (detail: %s)",
				resFinal.Class, resFinal.Detail)
		}

		// Intermediate valid state hot_key = val-20 at cutoff event 42
		ctxInter := oracle.Context{
			Candidate: candidate.Candidate{Event: 42, Percent: 100, Seed: 1},
			History:   entries[:21],
			Events:    events[:42],
			Recovered: oracle.State{"hot_key": "val-20"},
		}
		resInter := oracle.Check(ctxInter)
		if resInter.Class != oracle.ClassValid {
			t.Fatalf("expected valid for intermediate cutoff in long chain, got: %s (detail: %s)",
				resInter.Class, resInter.Detail)
		}
	})

	t.Run("DeleteNonexistentKey", func(t *testing.T) {
		op := testEntry(1, 1, 2, true, "delete", history.Mutation{Kind: "delete", Key: []byte("never_existed")})
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "delete"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "delete"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
			History:   []history.Entry{op},
			Events:    events,
			Initial:   oracle.State{"other": "val"},
			Recovered: oracle.State{"other": "val"},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for deleting nonexistent key, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("RangeDeleteEmptyRange", func(t *testing.T) {
		initial := oracle.State{"a": "1", "z": "9"}
		op := testEntry(1, 1, 2, true, "range_delete", history.Mutation{
			Kind: "range_delete",
			Key:  []byte("m"),
			End:  []byte("p"),
		})
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "range_delete"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "range_delete"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
			History:   []history.Entry{op},
			Events:    events,
			Initial:   initial,
			Recovered: oracle.State{"a": "1", "z": "9"},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for empty range delete, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("SubsumedRangeDeletes", func(t *testing.T) {
		initial := oracle.State{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}
		// Op 1 deletes ["b", "d") -> removes "b", "c"
		op1 := testEntry(1, 1, 2, true, "range_delete", history.Mutation{
			Kind: "range_delete",
			Key:  []byte("b"),
			End:  []byte("d"),
		})
		// Op 2 deletes ["a", "e") -> subsumes Op 1 and removes "a", "d"
		op2 := testEntry(2, 3, 4, true, "range_delete", history.Mutation{
			Kind: "range_delete",
			Key:  []byte("a"),
			End:  []byte("e"),
		})
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "range_delete"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "range_delete"},
			{ID: 3, Kind: trace.KindOpBegin, Op: 2, OpKind: "range_delete"},
			{ID: 4, Kind: trace.KindOpEnd, Op: 2, OpKind: "range_delete"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 4, Percent: 100, Seed: 1},
			History:   []history.Entry{op1, op2},
			Events:    events,
			Initial:   initial,
			Recovered: oracle.State{"e": "5"},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for subsumed range deletes, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("BatchMixedSetAndDeleteSameKey", func(t *testing.T) {
		// Single batch with multiple intra-batch operations on the same key:
		// set k1=v1, delete k1, set k1=v2
		batch := testEntry(1, 1, 2, true, "batch",
			setMut("k1", "v1"),
			history.Mutation{Kind: "delete", Key: []byte("k1")},
			setMut("k1", "v2"),
		)
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "batch"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "batch"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
			History:   []history.Entry{batch},
			Events:    events,
			Recovered: oracle.State{"k1": "v2"},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for batch mixed set/delete/set, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("CandidateAtCutoff0EmptyHistory", func(t *testing.T) {
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 0, Percent: 0, Seed: 1},
			History:   nil,
			Events:    nil,
			Initial:   nil,
			Recovered: oracle.State{},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid for candidate at cutoff 0 with empty history, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})

	t.Run("UntouchedKeyNoPanic", func(t *testing.T) {
		entry := testEntry(1, 1, 2, true, "set", setMut("touched_key", "v1"))
		events := []trace.Event{
			{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
			{ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		}
		ctx := oracle.Context{
			Candidate: candidate.Candidate{Event: 2, Percent: 100, Seed: 1},
			History:   []history.Entry{entry},
			Events:    events,
			Initial:   oracle.State{"untouched_init": "val0"},
			Recovered: oracle.State{"untouched_init": "val0", "touched_key": "v1"},
		}
		res := oracle.Check(ctx)
		if res.Class != oracle.ClassValid {
			t.Fatalf("expected valid, got: %s (detail: %s)", res.Class, res.Detail)
		}
	})
}
