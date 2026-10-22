package history_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/history"
)

func sampleValidLedger() []history.Entry {
	return []history.Entry{
		{
			Op:         1,
			Kind:       "set",
			Mutations:  []history.Mutation{{Kind: "set", Key: []byte("k1"), Value: []byte("v1")}},
			Sync:       false,
			BeginEvent: 1,
			EndEvent:   2,
		},
		{
			Op:   2,
			Kind: "batch",
			Mutations: []history.Mutation{
				{Kind: "set", Key: []byte("k2"), Value: []byte("v2")},
				{Kind: "delete", Key: []byte("k1")},
				{Kind: "range_delete", Key: []byte("a"), End: []byte("z")},
			},
			Sync:       true,
			BeginEvent: 3,
			EndEvent:   4,
		},
		{
			Op:         3,
			Kind:       "get",
			BeginEvent: 5,
			EndEvent:   6,
		},
		{
			Op:         4,
			Kind:       "flush",
			BeginEvent: 7,
			EndEvent:   8,
		},
		{
			Op:         5,
			Kind:       "set",
			Mutations:  []history.Mutation{{Kind: "set", Key: []byte("k3"), Value: []byte("v3")}},
			Sync:       true,
			BeginEvent: 9,
		},
	}
}

func TestHistoryValidLedger(t *testing.T) {
	entries := sampleValidLedger()
	if err := history.Validate(entries); err != nil {
		t.Fatalf("Validate failed on valid ledger: %v", err)
	}
}

func TestHistoryClone(t *testing.T) {
	original := sampleValidLedger()
	cloned := history.Clone(original)

	if len(cloned) != len(original) {
		t.Fatalf("len mismatch: %d vs %d", len(cloned), len(original))
	}

	// Mutate original byte slices to ensure clone is independent.
	original[0].Mutations[0].Key[0] = 'X'
	original[0].Mutations[0].Value[0] = 'Y'

	if cloned[0].Mutations[0].Key[0] == 'X' || cloned[0].Mutations[0].Value[0] == 'Y' {
		t.Fatalf("Clone did not deep copy byte slices")
	}
}

func TestHistoryEncodeDecode(t *testing.T) {
	entries := sampleValidLedger()

	var buf bytes.Buffer
	if err := history.Encode(&buf, entries); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := history.Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if len(decoded) != len(entries) {
		t.Fatalf("decoded len %d != %d", len(decoded), len(entries))
	}
	if string(decoded[0].Mutations[0].Key) != "k1" {
		t.Fatalf("decoded key mismatch: %q", string(decoded[0].Mutations[0].Key))
	}
}

func TestHistoryDecodeStrictness(t *testing.T) {
	// 1. Unknown fields rejected.
	jsonUnknown := `{"schema_version":1,"entries":[],"unexpected_field":123}`
	if _, err := history.Decode(strings.NewReader(jsonUnknown)); err == nil {
		t.Fatalf("expected error for unknown field, got nil")
	}

	// 2. Unsupported schema version.
	jsonVersion := `{"schema_version":2,"entries":[]}`
	if _, err := history.Decode(strings.NewReader(jsonVersion)); err == nil {
		t.Fatalf("expected error for version 2, got nil")
	}

	// 3. Trailing garbage.
	jsonTrailing := `{"schema_version":1,"entries":[]} trailing`
	if _, err := history.Decode(strings.NewReader(jsonTrailing)); err == nil {
		t.Fatalf("expected error for trailing data, got nil")
	}
}

func TestHistoryMalformedCases(t *testing.T) {
	valid := history.Entry{
		Op:         1,
		Kind:       "set",
		Mutations:  []history.Mutation{{Kind: "set", Key: []byte("a"), Value: []byte("A")}},
		BeginEvent: 1,
		EndEvent:   2,
	}

	cases := map[string]func(*history.Entry){
		"zero operation":      func(e *history.Entry) { e.Op = 0 },
		"zero begin":          func(e *history.Entry) { e.BeginEvent = 0 },
		"same marker":         func(e *history.Entry) { e.EndEvent = 1 },
		"end before begin":    func(e *history.Entry) { e.BeginEvent = 5; e.EndEvent = 4 },
		"unknown operation":   func(e *history.Entry) { e.Kind = "unknown" },
		"empty mutation":      func(e *history.Entry) { e.Mutations = nil },
		"multiple single":     func(e *history.Entry) { e.Mutations = append(e.Mutations, e.Mutations[0]) },
		"wrong mutation kind": func(e *history.Entry) { e.Mutations[0].Kind = "delete" },
		"invalid range": func(e *history.Entry) {
			e.Kind = "range_delete"
			e.Mutations[0] = history.Mutation{Kind: "range_delete", Key: []byte("z"), End: []byte("a")}
		},
		"range equal endpoints": func(e *history.Entry) {
			e.Kind = "range_delete"
			e.Mutations[0] = history.Mutation{Kind: "range_delete", Key: []byte("a"), End: []byte("a")}
		},
		"read with mutations":  func(e *history.Entry) { e.Kind = "get" },
		"read with sync":       func(e *history.Entry) { e.Kind = "get"; e.Mutations = nil; e.Sync = true },
		"unacknowledged error": func(e *history.Entry) { e.EndEvent = 0; e.Err = "failed" },
		"delete with value": func(e *history.Entry) {
			e.Kind = "delete"
			e.Mutations[0] = history.Mutation{Kind: "delete", Key: []byte("k"), Value: []byte("v")}
		},
		"set with end": func(e *history.Entry) {
			e.Mutations[0].End = []byte("end")
		},
	}

	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			entry := valid
			entry.Mutations = history.Clone([]history.Entry{valid})[0].Mutations
			corrupt(&entry)
			if err := history.Validate([]history.Entry{entry}); err == nil {
				t.Fatalf("case %q: expected validation error, got nil", name)
			}
		})
	}
}

func TestHistorySequenceInvariants(t *testing.T) {
	e1 := history.Entry{
		Op:         1,
		Kind:       "set",
		Mutations:  []history.Mutation{{Kind: "set", Key: []byte("a"), Value: []byte("A")}},
		BeginEvent: 1,
		EndEvent:   2,
	}

	// Decreasing op
	e2BadOp := history.Entry{
		Op:         1,
		Kind:       "set",
		Mutations:  []history.Mutation{{Kind: "set", Key: []byte("b"), Value: []byte("B")}},
		BeginEvent: 3,
		EndEvent:   4,
	}
	if err := history.Validate([]history.Entry{e1, e2BadOp}); err == nil {
		t.Fatalf("expected error for non-increasing op, got nil")
	}

	// Overlapping events
	e2Overlap := history.Entry{
		Op:         2,
		Kind:       "set",
		Mutations:  []history.Mutation{{Kind: "set", Key: []byte("b"), Value: []byte("B")}},
		BeginEvent: 2, // overlaps with e1.EndEvent = 2
		EndEvent:   4,
	}
	if err := history.Validate([]history.Entry{e1, e2Overlap}); err == nil {
		t.Fatalf("expected error for overlapping events, got nil")
	}

	// In-flight operation followed by another operation
	e1InFlight := e1
	e1InFlight.EndEvent = 0
	e2Next := history.Entry{
		Op:         2,
		Kind:       "set",
		Mutations:  []history.Mutation{{Kind: "set", Key: []byte("b"), Value: []byte("B")}},
		BeginEvent: 3,
		EndEvent:   4,
	}
	if err := history.Validate([]history.Entry{e1InFlight, e2Next}); err == nil {
		t.Fatalf("expected error for operation following unacknowledged entry, got nil")
	}
}
