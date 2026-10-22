// Package history defines the mutation history ledger recorded during client workloads.
//
// The history ledger records every high-level client operation (Set, Delete, Batch, etc.)
// and maps client operations to the underlying VFS event boundaries (BeginEvent, EndEvent).
package history

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Version is the schema version of history documents.
const Version = 1

// Mutation represents a single key-value mutation within a client operation.
type Mutation struct {
	Kind  string `json:"kind"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
	End   []byte `json:"end,omitempty"`
}

// Entry represents a single client operation in the workload history.
type Entry struct {
	Op         uint64     `json:"op"`
	Kind       string     `json:"kind"`
	Mutations  []Mutation `json:"mutations,omitempty"`
	Sync       bool       `json:"sync,omitempty"`
	BeginEvent uint64     `json:"begin_event"`
	EndEvent   uint64     `json:"end_event,omitempty"`
	Err        string     `json:"err,omitempty"`
}

// Document is the version-one serialization envelope for history ledgers.
type Document struct {
	SchemaVersion int     `json:"schema_version"`
	Entries       []Entry `json:"entries"`
}

// Clone returns a deep copy of entries with owned byte slices.
func Clone(entries []Entry) []Entry {
	if entries == nil {
		return nil
	}
	result := make([]Entry, len(entries))
	for i, e := range entries {
		result[i] = e
		if len(e.Mutations) > 0 {
			result[i].Mutations = make([]Mutation, len(e.Mutations))
			for j, m := range e.Mutations {
				result[i].Mutations[j] = Mutation{
					Kind:  m.Kind,
					Key:   bytes.Clone(m.Key),
					Value: bytes.Clone(m.Value),
					End:   bytes.Clone(m.End),
				}
			}
		}
	}
	return result
}

func isValidKind(kind string) bool {
	switch kind {
	case "set", "delete", "range_delete", "batch", "get", "iterate", "flush", "compact":
		return true
	default:
		return false
	}
}

func isMutatingKind(kind string) bool {
	return kind == "set" || kind == "delete" || kind == "range_delete" || kind == "batch"
}

// Validate validates that entries form a strictly well-formed history ledger.
func Validate(entries []Entry) error {
	var lastOp, lastEnd uint64
	for i, e := range entries {
		if e.Op == 0 {
			return fmt.Errorf("entry %d: op cannot be zero", i)
		}
		if i > 0 && e.Op <= lastOp {
			return fmt.Errorf("entry %d: op %d must be strictly greater than previous op %d", i, e.Op, lastOp)
		}
		lastOp = e.Op

		if e.BeginEvent == 0 {
			return fmt.Errorf("entry %d (op %d): begin_event cannot be zero", i, e.Op)
		}
		if i > 0 {
			if entries[i-1].EndEvent == 0 {
				return fmt.Errorf("entry %d (op %d): previous entry is unacknowledged", i, e.Op)
			}
			if e.BeginEvent <= lastEnd {
				return fmt.Errorf("entry %d (op %d): begin_event %d must be greater than previous end_event %d",
					i, e.Op, e.BeginEvent, lastEnd)
			}
		}

		if e.EndEvent != 0 {
			if e.EndEvent <= e.BeginEvent {
				return fmt.Errorf("entry %d (op %d): end_event %d must be greater than begin_event %d",
					i, e.Op, e.EndEvent, e.BeginEvent)
			}
			lastEnd = e.EndEvent
		} else {
			if e.Err != "" {
				return fmt.Errorf("entry %d (op %d): unacknowledged entry cannot have error", i, e.Op)
			}
			lastEnd = e.BeginEvent
		}

		if !isValidKind(e.Kind) {
			return fmt.Errorf("entry %d (op %d): invalid kind %q", i, e.Op, e.Kind)
		}

		if !isMutatingKind(e.Kind) {
			if len(e.Mutations) > 0 {
				return fmt.Errorf("entry %d (op %d): read/maintenance operation %q cannot have mutations", i, e.Op, e.Kind)
			}
			if e.Sync {
				return fmt.Errorf("entry %d (op %d): read/maintenance operation %q cannot specify sync", i, e.Op, e.Kind)
			}
			continue
		}

		if len(e.Mutations) == 0 {
			return fmt.Errorf("entry %d (op %d): mutating operation %q must have at least one mutation", i, e.Op, e.Kind)
		}

		switch e.Kind {
		case "set":
			if len(e.Mutations) != 1 {
				return fmt.Errorf("entry %d (op %d): set must have exactly one mutation", i, e.Op)
			}
			m := e.Mutations[0]
			if m.Kind != "set" {
				return fmt.Errorf("entry %d (op %d): mutation kind must match set", i, e.Op)
			}
			if len(m.End) != 0 {
				return fmt.Errorf("entry %d (op %d): set mutation cannot have end", i, e.Op)
			}

		case "delete":
			if len(e.Mutations) != 1 {
				return fmt.Errorf("entry %d (op %d): delete must have exactly one mutation", i, e.Op)
			}
			m := e.Mutations[0]
			if m.Kind != "delete" {
				return fmt.Errorf("entry %d (op %d): mutation kind must match delete", i, e.Op)
			}
			if len(m.Value) != 0 || len(m.End) != 0 {
				return fmt.Errorf("entry %d (op %d): delete mutation cannot have value or end", i, e.Op)
			}

		case "range_delete":
			if len(e.Mutations) != 1 {
				return fmt.Errorf("entry %d (op %d): range_delete must have exactly one mutation", i, e.Op)
			}
			m := e.Mutations[0]
			if m.Kind != "range_delete" {
				return fmt.Errorf("entry %d (op %d): mutation kind must match range_delete", i, e.Op)
			}
			if len(m.Value) != 0 {
				return fmt.Errorf("entry %d (op %d): range_delete cannot have value", i, e.Op)
			}
			if bytes.Compare(m.Key, m.End) >= 0 {
				return fmt.Errorf("entry %d (op %d): range_delete key must be strictly less than end", i, e.Op)
			}

		case "batch":
			for j, m := range e.Mutations {
				switch m.Kind {
				case "set":
					if len(m.End) != 0 {
						return fmt.Errorf("entry %d (op %d) mutation %d: batch set cannot have end", i, e.Op, j)
					}
				case "delete":
					if len(m.Value) != 0 || len(m.End) != 0 {
						return fmt.Errorf("entry %d (op %d) mutation %d: batch delete cannot have value or end", i, e.Op, j)
					}
				case "range_delete":
					if len(m.Value) != 0 {
						return fmt.Errorf("entry %d (op %d) mutation %d: batch range_delete cannot have value", i, e.Op, j)
					}
					if bytes.Compare(m.Key, m.End) >= 0 {
						return fmt.Errorf("entry %d (op %d) mutation %d: batch range_delete key must be strictly less than end", i, e.Op, j)
					}
				default:
					return fmt.Errorf("entry %d (op %d) mutation %d: unsupported batch mutation kind %q", i, e.Op, j, m.Kind)
				}
			}
		}
	}
	return nil
}

// Encode validates and serializes entries to writer as strict version-one JSON.
func Encode(writer io.Writer, entries []Entry) error {
	if err := Validate(entries); err != nil {
		return fmt.Errorf("history: %w", err)
	}
	doc := Document{
		SchemaVersion: Version,
		Entries:       Clone(entries),
	}
	return json.NewEncoder(writer).Encode(doc)
}

// Decode deserializes entries from reader using strict version-one JSON validation.
func Decode(reader io.Reader) ([]Entry, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var doc Document
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("history decode: %w", err)
	}
	if doc.SchemaVersion != Version || doc.Entries == nil {
		return nil, errors.New("history decode: unsupported schema version or missing entries")
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("history decode: trailing data after history document")
	}

	if err := Validate(doc.Entries); err != nil {
		return nil, fmt.Errorf("history decode: %w", err)
	}
	return doc.Entries, nil
}
