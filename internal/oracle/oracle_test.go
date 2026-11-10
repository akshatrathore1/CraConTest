package oracle_test

import (
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
