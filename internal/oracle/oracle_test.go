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
		name        string
		entries     []history.Entry
		cut         uint64
		state       oracle.State
		class       oracle.Class
		rule        string
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
