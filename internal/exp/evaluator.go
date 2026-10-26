// Package exp provides offline outcome table validation, discovery curve evaluation,
// and experiment serialization for crash-recovery test results.
package exp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
)

// Version is the schema version of outcome and ranking documents.
const Version = 1

// Point represents a single evaluation discovery point at a specified candidate budget.
type Point struct {
	Budget       int `json:"budget"`
	Tested       int `json:"tested"`
	Failures     int `json:"failures"`
	DistinctBugs int `json:"distinct_bugs"`
}

// Summary captures the discovery evaluation summary for a candidate ranking permutation.
type Summary struct {
	TotalTested    int `json:"total_tested"`
	FirstFailure   int `json:"first_failure"`   // 1-based index of first non-valid state (0 if none)
	FirstViolation int `json:"first_violation"` // 1-based index of first consistency violation (0 if none)
	Failures       int `json:"failures"`
	Violations     int `json:"violations"`
	OpenErrors     int `json:"open_errors"`
	Panics         int `json:"panics"`
	Hangs          int `json:"hangs"`
	DistinctBugs   int `json:"distinct_bugs"`
}

func (s Summary) String() string {
	return fmt.Sprintf("Tested: %d, FirstFailure: %d, FirstViolation: %d, Failures: %d (Violations: %d, OpenErrors: %d, Panics: %d, Hangs: %d), DistinctBugs: %d",
		s.TotalTested, s.FirstFailure, s.FirstViolation, s.Failures, s.Violations, s.OpenErrors, s.Panics, s.Hangs, s.DistinctBugs)
}

// OutcomeDocument is the strict version-one serialization format for outcome tables.
type OutcomeDocument struct {
	SchemaVersion int              `json:"schema_version"`
	Outcomes      []oracle.Outcome `json:"outcomes"`
}

// RankingDocument is the strict version-one serialization format for strategy ranking permutations.
type RankingDocument struct {
	SchemaVersion int                   `json:"schema_version"`
	Strategy      string                `json:"strategy"`
	RankingSeed   uint64                `json:"ranking_seed"`
	Candidates    []candidate.Candidate `json:"candidates"`
}

// ValidateTable checks that outcomes forms a valid, complete outcome table for universe:
// - Outcomes matches universe length and canonical ordering 1:1.
// - Each outcome has a valid Class.
// - Violation outcomes have non-empty Rule and valid v1: hex SHA-256 Signature.
// - Valid outcomes have empty Rule and Signature.
// - oracle_error outcomes are rejected as an unusable run.
func ValidateTable(universe []candidate.Candidate, outcomes []oracle.Outcome) error {
	if len(universe) != len(outcomes) {
		return fmt.Errorf("exp: outcomes length %d does not match universe length %d", len(outcomes), len(universe))
	}

	seen := make(map[candidate.Candidate]bool, len(outcomes))
	for i, o := range outcomes {
		expectedCand := universe[i]
		if o.Candidate != expectedCand {
			return fmt.Errorf("exp: outcome at index %d has candidate %+v, expected canonically ordered %+v",
				i, o.Candidate, expectedCand)
		}
		if seen[o.Candidate] {
			return fmt.Errorf("exp: duplicate outcome candidate %+v", o.Candidate)
		}
		seen[o.Candidate] = true

		switch o.Class {
		case oracle.ClassValid:
			if o.Rule != "" {
				return fmt.Errorf("exp: valid outcome for candidate %+v must not have rule %q", o.Candidate, o.Rule)
			}
			if o.Signature != "" {
				return fmt.Errorf("exp: valid outcome for candidate %+v must not have signature %q", o.Candidate, o.Signature)
			}

		case oracle.ClassViolation:
			if o.Rule == "" {
				return fmt.Errorf("exp: violation outcome for candidate %+v missing rule", o.Candidate)
			}
			if o.Signature == "" {
				return fmt.Errorf("exp: violation outcome for candidate %+v missing signature", o.Candidate)
			}
			if !strings.HasPrefix(o.Signature, "v1:") {
				return fmt.Errorf("exp: violation outcome signature %q missing v1: prefix", o.Signature)
			}

		case oracle.ClassOpenError:
			// If a signature is provided for an open_error, it must have a valid prefix (v1: or v1:open_error:).
			if o.Signature != "" && !strings.HasPrefix(o.Signature, "v1:") {
				return fmt.Errorf("exp: open_error outcome for candidate %+v signature %q missing v1: prefix", o.Candidate, o.Signature)
			}

		case oracle.ClassPanic, oracle.ClassHang:
			// Non-violation recovery failures.

		case oracle.ClassOracleError:
			return fmt.Errorf("exp: oracle_error encountered for candidate %+v: %s", o.Candidate, o.Detail)

		default:
			return fmt.Errorf("exp: invalid outcome class %q for candidate %+v", o.Class, o.Candidate)
		}
	}

	return nil
}

// Evaluate computes the discovery curve Points for the given ranking and budgets against outcomes.
// - Re-execution of candidates and strategy observation of the table are strictly forbidden.
// - Budgets must be unique, nonnegative, <= len(universe), and strictly increasing.
// - ranking must be a valid permutation of universe without duplicates or foreign candidates.
// - Failures counts violations, open_errors, panics, and hangs.
// - DistinctBugs counts unique nonempty signatures of violation and open_error outcomes.
func Evaluate(universe, ranking []candidate.Candidate, outcomes []oracle.Outcome, budgets []int) ([]Point, error) {
	if err := ValidateTable(universe, outcomes); err != nil {
		return nil, fmt.Errorf("evaluate: %w", err)
	}

	if len(ranking) != len(universe) {
		return nil, fmt.Errorf("evaluate: ranking length %d != universe length %d", len(ranking), len(universe))
	}

	// Validate ranking permutation.
	universeSet := make(map[candidate.Candidate]bool, len(universe))
	for _, c := range universe {
		universeSet[c] = true
	}
	rankingSeen := make(map[candidate.Candidate]bool, len(ranking))
	for _, c := range ranking {
		if !universeSet[c] {
			return nil, fmt.Errorf("evaluate: ranking contains foreign candidate %+v", c)
		}
		if rankingSeen[c] {
			return nil, fmt.Errorf("evaluate: ranking contains duplicate candidate %+v", c)
		}
		rankingSeen[c] = true
	}

	// Validate budgets.
	for i, b := range budgets {
		if b < 0 {
			return nil, fmt.Errorf("evaluate: negative budget %d", b)
		}
		if b > len(universe) {
			return nil, fmt.Errorf("evaluate: budget %d exceeds universe size %d", b, len(universe))
		}
		if i > 0 && b <= budgets[i-1] {
			return nil, fmt.Errorf("evaluate: budgets must be strictly increasing and unique: %d <= %d", b, budgets[i-1])
		}
	}

	outcomeMap := make(map[candidate.Candidate]oracle.Outcome, len(outcomes))
	for _, o := range outcomes {
		outcomeMap[o.Candidate] = o
	}

	points := make([]Point, 0, len(budgets))
	for _, b := range budgets {
		failures := 0
		seenBugs := make(map[string]bool)

		prefix := ranking[:b]
		for _, c := range prefix {
			o := outcomeMap[c]
			switch o.Class {
			case oracle.ClassViolation:
				failures++
				if o.Signature != "" && !seenBugs[o.Signature] {
					seenBugs[o.Signature] = true
				}
			case oracle.ClassOpenError:
				failures++
				if o.Signature != "" && !seenBugs[o.Signature] {
					seenBugs[o.Signature] = true
				}
			case oracle.ClassPanic, oracle.ClassHang:
				failures++
			case oracle.ClassValid:
				// no failure
			}
		}

		points = append(points, Point{
			Budget:       b,
			Tested:       b,
			Failures:     failures,
			DistinctBugs: len(seenBugs),
		})
	}

	return points, nil
}

// NormalizeOpenErrorReason maps an error message or detail string to a stable normalized reason tag.
func NormalizeOpenErrorReason(detail string) string {
	s := strings.TrimSpace(detail)
	if strings.HasPrefix(s, "v1:open_error:") {
		s = strings.TrimPrefix(s, "v1:open_error:")
	}
	lower := strings.ToLower(s)
	switch {
	case strings.Contains(lower, "not exist") || strings.Contains(lower, "no such file"):
		return "not_exist"
	case strings.Contains(lower, "checksum"):
		return "checksum_mismatch"
	case strings.Contains(lower, "manifest"):
		return "manifest_corrupt"
	case strings.Contains(lower, "eof"):
		return "unexpected_eof"
	case strings.Contains(lower, "nil db"):
		return "nil_db"
	case strings.Contains(lower, "table") || strings.Contains(lower, "sst"):
		return "table_corrupt"
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "timed out"):
		return "timeout"
	case strings.Contains(lower, "corrupt"):
		return "corrupt"
	default:
		// Clean any arbitrary string to lower snake_case alphanumeric.
		var b strings.Builder
		lastUnderscore := false
		for _, r := range lower {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				b.WriteRune(r)
				lastUnderscore = false
			} else if !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
		res := strings.Trim(b.String(), "_")
		if res == "" {
			return "unknown"
		}
		if len(res) > 32 {
			res = res[:32]
		}
		return res
	}
}

// FormatOpenErrorSignature constructs a deterministic, normalized signature v1:open_error:<reason>.
func FormatOpenErrorSignature(reason string) string {
	if strings.HasPrefix(reason, "v1:open_error:") {
		return reason
	}
	norm := NormalizeOpenErrorReason(reason)
	return fmt.Sprintf("v1:open_error:%s", norm)
}

// NormalizeOutcome ensures that an outcome with ClassOpenError has a deterministic,
// normalized signature v1:open_error:<reason>.
func NormalizeOutcome(o oracle.Outcome) oracle.Outcome {
	if o.Class == oracle.ClassOpenError {
		if o.Signature == "" {
			reason := o.Detail
			if reason == "" {
				reason = "unknown"
			}
			o.Signature = FormatOpenErrorSignature(reason)
		} else if !strings.HasPrefix(o.Signature, "v1:") {
			o.Signature = FormatOpenErrorSignature(o.Signature)
		}
	}
	return o
}

// NormalizeTable returns a copy of outcomes with all ClassOpenError outcomes normalized.
func NormalizeTable(outcomes []oracle.Outcome) []oracle.Outcome {
	res := make([]oracle.Outcome, len(outcomes))
	for i, o := range outcomes {
		res[i] = NormalizeOutcome(o)
	}
	return res
}

// TabulateSummary evaluates a ranking against outcomes and computes the summary metrics,
// including FirstFailure (1-based index of first non-valid candidate) and
// FirstViolation (1-based index of first ClassViolation candidate).
func TabulateSummary(universe, ranking []candidate.Candidate, outcomes []oracle.Outcome) (Summary, error) {
	if err := ValidateTable(universe, outcomes); err != nil {
		return Summary{}, fmt.Errorf("tabulate summary: %w", err)
	}

	if len(ranking) != len(universe) {
		return Summary{}, fmt.Errorf("tabulate summary: ranking length %d != universe length %d", len(ranking), len(universe))
	}

	universeSet := make(map[candidate.Candidate]bool, len(universe))
	for _, c := range universe {
		universeSet[c] = true
	}
	rankingSeen := make(map[candidate.Candidate]bool, len(ranking))
	for _, c := range ranking {
		if !universeSet[c] {
			return Summary{}, fmt.Errorf("tabulate summary: ranking contains foreign candidate %+v", c)
		}
		if rankingSeen[c] {
			return Summary{}, fmt.Errorf("tabulate summary: ranking contains duplicate candidate %+v", c)
		}
		rankingSeen[c] = true
	}

	outcomeMap := make(map[candidate.Candidate]oracle.Outcome, len(outcomes))
	for _, o := range outcomes {
		outcomeMap[o.Candidate] = o
	}

	var s Summary
	s.TotalTested = len(ranking)
	seenBugs := make(map[string]bool)

	for i, c := range ranking {
		pos := i + 1
		o := outcomeMap[c]

		switch o.Class {
		case oracle.ClassValid:
			// no failure

		case oracle.ClassViolation:
			if s.FirstFailure == 0 {
				s.FirstFailure = pos
			}
			if s.FirstViolation == 0 {
				s.FirstViolation = pos
			}
			s.Failures++
			s.Violations++
			if o.Signature != "" && !seenBugs[o.Signature] {
				seenBugs[o.Signature] = true
			}

		case oracle.ClassOpenError:
			if s.FirstFailure == 0 {
				s.FirstFailure = pos
			}
			s.Failures++
			s.OpenErrors++
			sig := o.Signature
			if sig == "" && o.Detail != "" {
				sig = FormatOpenErrorSignature(o.Detail)
			}
			if sig != "" && !seenBugs[sig] {
				seenBugs[sig] = true
			}

		case oracle.ClassPanic:
			if s.FirstFailure == 0 {
				s.FirstFailure = pos
			}
			s.Failures++
			s.Panics++

		case oracle.ClassHang:
			if s.FirstFailure == 0 {
				s.FirstFailure = pos
			}
			s.Failures++
			s.Hangs++
		}
	}

	s.DistinctBugs = len(seenBugs)
	return s, nil
}

// Tabulate is an alias for TabulateSummary.
func Tabulate(universe, ranking []candidate.Candidate, outcomes []oracle.Outcome) (Summary, error) {
	return TabulateSummary(universe, ranking, outcomes)
}

// EncodeOutcomes serializes outcomes as a strict version-one JSON outcome document.
func EncodeOutcomes(writer io.Writer, universe []candidate.Candidate, outcomes []oracle.Outcome) error {
	if err := ValidateTable(universe, outcomes); err != nil {
		return fmt.Errorf("encode outcomes: %w", err)
	}
	doc := OutcomeDocument{
		SchemaVersion: Version,
		Outcomes:      append([]oracle.Outcome{}, outcomes...),
	}
	return json.NewEncoder(writer).Encode(doc)
}

// DecodeOutcomes deserializes and strictly validates a version-one JSON outcome document.
func DecodeOutcomes(reader io.Reader) ([]oracle.Outcome, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var raw struct {
		SchemaVersion *int            `json:"schema_version"`
		Outcomes      json.RawMessage `json:"outcomes"`
	}
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode outcomes: %w", err)
	}
	if raw.SchemaVersion == nil || *raw.SchemaVersion != Version {
		return nil, errors.New("decode outcomes: unsupported or missing schema_version")
	}
	if len(raw.Outcomes) == 0 || bytes.Equal(bytes.TrimSpace(raw.Outcomes), []byte("null")) {
		return nil, errors.New("decode outcomes: missing or null outcomes array")
	}

	inner := json.NewDecoder(bytes.NewReader(raw.Outcomes))
	inner.DisallowUnknownFields()
	var outcomes []oracle.Outcome
	if err := inner.Decode(&outcomes); err != nil {
		return nil, fmt.Errorf("decode outcomes: %w", err)
	}
	if outcomes == nil {
		outcomes = []oracle.Outcome{}
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode outcomes: trailing data after outcome document")
	}

	// Validate outcomes integrity.
	seen := make(map[candidate.Candidate]bool, len(outcomes))
	for i, o := range outcomes {
		if seen[o.Candidate] {
			return nil, fmt.Errorf("decode outcomes: duplicate outcome candidate %+v", o.Candidate)
		}
		seen[o.Candidate] = true

		if i > 0 && candidate.Less(o.Candidate, outcomes[i-1].Candidate) {
			return nil, fmt.Errorf("decode outcomes: non-canonical outcome order at index %d", i)
		}

		switch o.Class {
		case oracle.ClassValid:
			if o.Rule != "" || o.Signature != "" {
				return nil, fmt.Errorf("decode outcomes: valid outcome must not have rule or signature: %+v", o)
			}
		case oracle.ClassViolation:
			if o.Rule == "" || o.Signature == "" {
				return nil, fmt.Errorf("decode outcomes: violation outcome missing rule or signature: %+v", o)
			}
			if !strings.HasPrefix(o.Signature, "v1:") {
				return nil, fmt.Errorf("decode outcomes: violation signature missing v1: prefix: %s", o.Signature)
			}
		case oracle.ClassOpenError:
			if o.Signature != "" && !strings.HasPrefix(o.Signature, "v1:") {
				return nil, fmt.Errorf("decode outcomes: open_error signature missing v1: prefix: %s", o.Signature)
			}
		case oracle.ClassPanic, oracle.ClassHang:
		case oracle.ClassOracleError:
			return nil, fmt.Errorf("decode outcomes: oracle_error encountered: %s", o.Detail)
		default:
			return nil, fmt.Errorf("decode outcomes: invalid outcome class %q", o.Class)
		}
	}

	return outcomes, nil
}

// EncodeRanking serializes doc as a strict version-one JSON ranking document.
func EncodeRanking(writer io.Writer, doc RankingDocument) error {
	if doc.Strategy == "" {
		return errors.New("encode ranking: strategy cannot be empty")
	}
	if doc.Candidates == nil {
		return errors.New("encode ranking: candidates array cannot be nil")
	}

	// Check duplicates.
	seen := make(map[candidate.Candidate]bool, len(doc.Candidates))
	for _, c := range doc.Candidates {
		if seen[c] {
			return fmt.Errorf("encode ranking: duplicate candidate %+v", c)
		}
		seen[c] = true
	}

	doc.SchemaVersion = Version
	return json.NewEncoder(writer).Encode(doc)
}

// DecodeRanking deserializes and strictly validates a version-one JSON ranking document.
func DecodeRanking(reader io.Reader) (RankingDocument, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var doc RankingDocument
	if err := decoder.Decode(&doc); err != nil {
		return RankingDocument{}, fmt.Errorf("decode ranking: %w", err)
	}
	if doc.SchemaVersion != Version {
		return RankingDocument{}, errors.New("decode ranking: unsupported or missing schema_version")
	}
	if doc.Strategy == "" {
		return RankingDocument{}, errors.New("decode ranking: missing strategy")
	}
	if doc.Candidates == nil {
		return RankingDocument{}, errors.New("decode ranking: missing candidates array")
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return RankingDocument{}, errors.New("decode ranking: trailing data after ranking document")
	}

	seen := make(map[candidate.Candidate]bool, len(doc.Candidates))
	for _, c := range doc.Candidates {
		if seen[c] {
			return RankingDocument{}, fmt.Errorf("decode ranking: duplicate candidate %+v", c)
		}
		seen[c] = true
	}

	return doc, nil
}
