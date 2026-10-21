// Package candidate defines crash candidates and the candidate universe.
//
// A candidate is the full tuple (cutoff event, retention percent, seed). The
// complete tuple is its identity: two candidates with equal event and percent
// but different seeds are distinct. A universe is the canonically ordered set
// of candidates that every strategy ranks and every outcome table covers.
package candidate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// Version is the schema version of universe documents.
const Version = 1

// seedDomain separates candidate seeds from any other use of SHA-256.
const seedDomain = "cracontest/candidate-seed/v1\x00"

// Candidate is one crash to test: replay events 1..Event, keep Percent of the
// unsynced data, and use Seed for the retention RNG.
type Candidate struct {
	Event   uint64 `json:"event"`
	Percent int    `json:"percent"`
	Seed    uint64 `json:"seed"`
}

// Document is the version-one wire format of a universe.
type Document struct {
	SchemaVersion int         `json:"schema_version"`
	Candidates    []Candidate `json:"candidates"`
}

// Less reports whether left precedes right in canonical order
// (Event, then Percent, then Seed).
func Less(left, right Candidate) bool {
	if left.Event != right.Event {
		return left.Event < right.Event
	}
	if left.Percent != right.Percent {
		return left.Percent < right.Percent
	}
	return left.Seed < right.Seed
}

// Sort orders candidates canonically in place.
func Sort(candidates []Candidate) {
	sort.Slice(candidates, func(left, right int) bool { return Less(candidates[left], candidates[right]) })
}

// DeriveSeed returns the deterministic retention seed for a tuple. It is the
// first eight bytes, read big-endian, of SHA-256 over the domain string, the
// big-endian base seed and event, and the percent as one byte.
func DeriveSeed(base, event uint64, percent int) (uint64, error) {
	if percent < 0 || percent > 100 {
		return 0, fmt.Errorf("candidate: percent %d outside 0..100", percent)
	}
	encoded := make([]byte, 0, len(seedDomain)+17)
	encoded = append(encoded, seedDomain...)
	encoded = binary.BigEndian.AppendUint64(encoded, base)
	encoded = binary.BigEndian.AppendUint64(encoded, event)
	encoded = append(encoded, byte(percent))
	digest := sha256.Sum256(encoded)
	return binary.BigEndian.Uint64(digest[:8]), nil
}

// Build returns the canonical universe for a trace: every cutoff chosen by
// trace.CutIDs combined with every policy, one derived seed per tuple. The
// initial execution profile accepts only the endpoint policies 0 and 100.
func Build(events []trace.Event, base uint64, policies []int) ([]Candidate, error) {
	if len(policies) == 0 {
		return nil, errors.New("candidate: no retention policies")
	}
	seen := make(map[int]bool, len(policies))
	for _, policy := range policies {
		if policy != 0 && policy != 100 {
			return nil, fmt.Errorf("candidate: unsupported execution policy %d (only 0 and 100)", policy)
		}
		if seen[policy] {
			return nil, fmt.Errorf("candidate: duplicate policy %d", policy)
		}
		seen[policy] = true
	}
	cuts, err := trace.CutIDs(events)
	if err != nil {
		return nil, fmt.Errorf("candidate: %w", err)
	}
	universe := make([]Candidate, 0, len(cuts)*len(policies))
	for _, cut := range cuts {
		for _, policy := range policies {
			seed, err := DeriveSeed(base, cut, policy)
			if err != nil {
				return nil, err
			}
			universe = append(universe, Candidate{Event: cut, Percent: policy, Seed: seed})
		}
	}
	Sort(universe)
	return universe, nil
}

// Validate checks a universe against its trace: the trace itself is valid,
// every cutoff lies in 0..len(events), every percent in 0..100, and no full
// tuple appears twice.
func Validate(events []trace.Event, universe []Candidate) error {
	if err := trace.Validate(events); err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	last := uint64(len(events))
	seen := make(map[Candidate]bool, len(universe))
	for index, candidate := range universe {
		if candidate.Event > last {
			return fmt.Errorf("candidate %d: cutoff %d beyond trace length %d", index, candidate.Event, last)
		}
		if err := checkPercent(candidate); err != nil {
			return fmt.Errorf("candidate %d: %w", index, err)
		}
		if seen[candidate] {
			return fmt.Errorf("candidate %d: duplicate tuple %+v", index, candidate)
		}
		seen[candidate] = true
	}
	return nil
}

func checkPercent(candidate Candidate) error {
	if candidate.Percent < 0 || candidate.Percent > 100 {
		return fmt.Errorf("percent %d outside 0..100", candidate.Percent)
	}
	return nil
}

// checkCanonical requires strictly increasing canonical order, which also
// rules out duplicates, and valid percents.
func checkCanonical(universe []Candidate) error {
	for index, candidate := range universe {
		if err := checkPercent(candidate); err != nil {
			return fmt.Errorf("candidate %d: %w", index, err)
		}
		if index > 0 && !Less(universe[index-1], candidate) {
			return fmt.Errorf("candidate %d: universe is not in strict canonical order", index)
		}
	}
	return nil
}

// Encode writes a version-one universe document. The universe must be in
// strict canonical order.
func Encode(writer io.Writer, universe []Candidate) error {
	if err := checkCanonical(universe); err != nil {
		return err
	}
	owned := append([]Candidate{}, universe...)
	return json.NewEncoder(writer).Encode(Document{SchemaVersion: Version, Candidates: owned})
}

// Decode reads a version-one universe document. It rejects unknown fields,
// other versions, a missing or null array, trailing data and non-canonical
// order. Cutoff range checks need the trace and belong to Validate.
func Decode(reader io.Reader) ([]Candidate, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var raw struct {
		SchemaVersion *int            `json:"schema_version"`
		Candidates    json.RawMessage `json:"candidates"`
	}
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	if raw.SchemaVersion == nil || *raw.SchemaVersion != Version {
		return nil, errors.New("decode universe: unsupported or missing schema_version")
	}
	if len(raw.Candidates) == 0 || bytes.Equal(bytes.TrimSpace(raw.Candidates), []byte("null")) {
		return nil, errors.New("decode universe: missing or null candidates array")
	}
	inner := json.NewDecoder(bytes.NewReader(raw.Candidates))
	inner.DisallowUnknownFields()
	var universe []Candidate
	if err := inner.Decode(&universe); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	if universe == nil {
		universe = []Candidate{}
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode universe: trailing data after document")
	}
	if err := checkCanonical(universe); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	return universe, nil
}
