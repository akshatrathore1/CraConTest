// Package oracle implements the consistency oracle that checks recovered database
// contents against the legal prefix states allowed by the workload history ledger.
package oracle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// Class represents the outcome classification of a crash-recovery test.
type Class string

const (
	ClassValid       Class = "valid"
	ClassViolation   Class = "violation"
	ClassOpenError   Class = "open_error"
	ClassPanic       Class = "panic"
	ClassHang        Class = "hang"
	ClassOracleError Class = "oracle_error"
)

// Outcome represents the full verification verdict for a crash candidate.
type Outcome struct {
	Candidate candidate.Candidate `json:"candidate"`
	Class     Class               `json:"class"`
	Rule      string              `json:"rule,omitempty"`
	Signature string              `json:"signature,omitempty"`
	Detail    string              `json:"detail,omitempty"`
}

// State represents the key-value contents of a database state.
type State map[string]string

// Context provides all inputs required by the oracle to check a candidate.
type Context struct {
	Candidate  candidate.Candidate
	Events     []trace.Event
	History    []history.Entry
	Initial    State
	Recovered  State
	Categories []string
}

func copyState(s State) State {
	res := make(State, len(s))
	for k, v := range s {
		res[k] = v
	}
	return res
}

func stateEqual(a, b State) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			return false
		}
	}
	return true
}

func isMutating(kind string) bool {
	return kind == "set" || kind == "delete" || kind == "range_delete" || kind == "batch"
}

// FormatSignature computes the canonical lower-case hex SHA-256 signature
// of the ordered JSON array [rule, opKind, categories...], prefixed with "v1:".
func FormatSignature(rule string, opKind string, categories []string) string {
	cats := make([]string, 0, len(categories))
	seen := make(map[string]bool, len(categories))
	for _, c := range categories {
		if !seen[c] {
			seen[c] = true
			cats = append(cats, c)
		}
	}
	sort.Strings(cats)

	arr := make([]string, 0, 2+len(cats))
	arr = append(arr, rule, opKind)
	arr = append(arr, cats...)

	data, _ := json.Marshal(arr)
	digest := sha256.Sum256(data)
	return fmt.Sprintf("v1:%x", digest)
}

// Check evaluates the recovered state against the workload history and candidate cutoff.
func Check(ctx Context) Outcome {
	cand := ctx.Candidate

	// 1. Validate history ledger.
	if err := history.Validate(ctx.History); err != nil {
		return Outcome{
			Candidate: cand,
			Class:     ClassOracleError,
			Rule:      "invalid_history",
			Detail:    err.Error(),
		}
	}

	// 2. Validate trace if present.
	if ctx.Events != nil {
		if err := trace.Validate(ctx.Events); err != nil {
			return Outcome{
				Candidate: cand,
				Class:     ClassOracleError,
				Rule:      "invalid_trace",
				Detail:    err.Error(),
			}
		}
		if cand.Event > uint64(len(ctx.Events)) {
			return Outcome{
				Candidate: cand,
				Class:     ClassOracleError,
				Rule:      "invalid_candidate",
				Detail:    fmt.Sprintf("candidate event %d exceeds trace length %d", cand.Event, len(ctx.Events)),
			}
		}
		// Cross-reference history markers with trace events.
		for _, e := range ctx.History {
			if e.BeginEvent > 0 && e.BeginEvent <= uint64(len(ctx.Events)) {
				ev := ctx.Events[e.BeginEvent-1]
				if ev.Kind != trace.KindOpBegin || ev.Op != e.Op || ev.OpKind != e.Kind {
					return Outcome{
						Candidate: cand,
						Class:     ClassOracleError,
						Rule:      "invalid_history",
						Detail:    fmt.Sprintf("marker mismatch on begin_event %d for op %d", e.BeginEvent, e.Op),
					}
				}
			}
			if e.EndEvent > 0 && e.EndEvent <= uint64(len(ctx.Events)) {
				ev := ctx.Events[e.EndEvent-1]
				if ev.Kind != trace.KindOpEnd || ev.Op != e.Op || ev.OpKind != e.Kind || ev.Err != e.Err {
					return Outcome{
						Candidate: cand,
						Class:     ClassOracleError,
						Rule:      "invalid_history",
						Detail:    fmt.Sprintf("marker mismatch on end_event %d for op %d", e.EndEvent, e.Op),
					}
				}
			}
		}
	}

	// 3. Check for mutation errors in history.
	for _, e := range ctx.History {
		if isMutating(e.Kind) && e.Err != "" {
			return Outcome{
				Candidate: cand,
				Class:     ClassOracleError,
				Rule:      "failed_mutation",
				Detail:    e.Err,
			}
		}
	}

	// 4. Construct legal prefix states S_0 .. S_U.
	recovered := ctx.Recovered
	if recovered == nil {
		recovered = make(State)
	}

	initial := ctx.Initial
	if initial == nil {
		initial = make(State)
	}

	states := []State{copyState(initial)}
	type interiorState struct {
		state  State
		opKind string
	}
	var interiors []interiorState
	type stateEntryMeta struct {
		entryIndex int
		opKind     string
	}
	stateMeta := []stateEntryMeta{{entryIndex: -1, opKind: "unknown"}}

	lower := 0
	cut := cand.Event

	for entryIdx, entry := range ctx.History {
		if !isMutating(entry.Kind) {
			continue
		}
		if entry.BeginEvent > cut {
			continue
		}

		acknowledged := entry.EndEvent > 0 && entry.EndEvent <= cut
		curr := copyState(states[len(states)-1])

		for mIdx, m := range entry.Mutations {
			switch m.Kind {
			case "set":
				curr[string(m.Key)] = string(m.Value)
			case "delete":
				delete(curr, string(m.Key))
			case "range_delete":
				startKey := string(m.Key)
				endKey := string(m.End)
				for k := range curr {
					if k >= startKey && k < endKey {
						delete(curr, k)
					}
				}
			}

			if entry.Kind == "batch" && mIdx+1 < len(entry.Mutations) {
				interiors = append(interiors, interiorState{
					state:  copyState(curr),
					opKind: "batch",
				})
			}
		}

		states = append(states, curr)
		stateMeta = append(stateMeta, stateEntryMeta{
			entryIndex: entryIdx,
			opKind:     entry.Kind,
		})

		if acknowledged && entry.Sync {
			lower = len(states) - 1
		}
	}

	// 5. Check if recovered matches any legal whole state S_L .. S_U.
	for _, s := range states[lower:] {
		if stateEqual(s, recovered) {
			return Outcome{
				Candidate: cand,
				Class:     ClassValid,
			}
		}
	}

	// 6. Diagnose content violation according to precedence:
	// Rule 1: durably_committed (matches an excluded state below lower).
	for idx, s := range states[:lower] {
		if stateEqual(s, recovered) {
			divergentOpKind := stateMeta[idx+1].opKind
			if divergentOpKind == "" {
				divergentOpKind = "unknown"
			}
			return Outcome{
				Candidate: cand,
				Class:     ClassViolation,
				Rule:      "durably_committed",
				Signature: FormatSignature("durably_committed", divergentOpKind, ctx.Categories),
			}
		}
	}

	// Rule 2: atomic_batch (matches an interior state of a batch).
	for _, interior := range interiors {
		if stateEqual(interior.state, recovered) {
			return Outcome{
				Candidate: cand,
				Class:     ClassViolation,
				Rule:      "atomic_batch",
				Signature: FormatSignature("atomic_batch", "batch", ctx.Categories),
			}
		}
	}

	// Rule 3: prefix (all other content divergence).
	// Identify earliest divergent mutation kind when comparing recovered against S_L.
	divergentOpKind := "unknown"
	sl := states[lower]

	// Find the earliest entry in states that diverges from recovered.
	for i := 1; i <= lower && i < len(stateMeta); i++ {
		si := states[i]
		if !stateEqual(si, recovered) {
			divergentOpKind = stateMeta[i].opKind
			break
		}
	}
	if divergentOpKind == "unknown" && lower < len(states)-1 {
		divergentOpKind = stateMeta[lower+1].opKind
	}
	if divergentOpKind == "unknown" && len(ctx.History) > 0 {
		divergentOpKind = ctx.History[0].Kind
	}

	_ = sl // used in divergence analysis
	return Outcome{
		Candidate: cand,
		Class:     ClassViolation,
		Rule:      "prefix",
		Signature: FormatSignature("prefix", divergentOpKind, ctx.Categories),
	}
}
