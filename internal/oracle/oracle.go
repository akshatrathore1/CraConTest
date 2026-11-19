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

func keyStateEqual(left, right State, key string) bool {
	leftValue, leftPresent := left[key]
	rightValue, rightPresent := right[key]
	return leftPresent == rightPresent && leftValue == rightValue
}

func isMutating(kind string) bool {
	return kind == "set" || kind == "delete" || kind == "range_delete" || kind == "batch" || kind == "ingest"
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
			if e.BeginEvent > uint64(len(ctx.Events)) {
				return Outcome{
					Candidate: cand,
					Class:     ClassOracleError,
					Rule:      "invalid_history",
					Detail:    fmt.Sprintf("begin_event %d exceeds trace length %d for op %d", e.BeginEvent, len(ctx.Events), e.Op),
				}
			}
			if e.EndEvent > uint64(len(ctx.Events)) {
				return Outcome{
					Candidate: cand,
					Class:     ClassOracleError,
					Rule:      "invalid_history",
					Detail:    fmt.Sprintf("end_event %d exceeds trace length %d for op %d", e.EndEvent, len(ctx.Events), e.Op),
				}
			}
			if e.BeginEvent > 0 {
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
			if e.EndEvent > 0 {
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
	// Overwrite-aware divergence detection: identify the earliest operation i whose
	// mutations are absent/divergent in recovered (and not overwritten by a later
	// surviving operation) while a subsequent operation j > i is present.
	maxTouchMemo := make(map[string]int)
	maxTouch := func(key string) int {
		if maxK, ok := maxTouchMemo[key]; ok {
			return maxK
		}
		maxK := -1
		for k := len(stateMeta) - 1; k >= 1; k-- {
			entryK := ctx.History[stateMeta[k].entryIndex]
			touched := false
			for _, mk := range entryK.Mutations {
				if mk.Kind == "range_delete" {
					if key >= string(mk.Key) && key < string(mk.End) {
						touched = true
						break
					}
				} else if string(mk.Key) == key {
					touched = true
					break
				}
			}
			if touched {
				maxK = k
				break
			}
		}
		maxTouchMemo[key] = maxK
		return maxK
	}

	rawKeyTouchedAfter := func(fromIdx int, key string) bool {
		return maxTouch(key) > fromIdx
	}

	satMemo := make([]int8, len(stateMeta))
	for idx := range satMemo {
		satMemo[idx] = -1
	}

	opSatisfiedModuloLater := func(idx int) bool {
		if satMemo[idx] != -1 {
			return satMemo[idx] == 1
		}
		entry := ctx.History[stateMeta[idx].entryIndex]
		sIdx := states[idx]
		satisfied := true
		for _, m := range entry.Mutations {
			if m.Kind == "range_delete" {
				startKey := string(m.Key)
				endKey := string(m.End)
				for rk := range recovered {
					if rk >= startKey && rk < endKey && !rawKeyTouchedAfter(idx, rk) {
						satisfied = false
						break
					}
				}
			} else {
				k := string(m.Key)
				if !keyStateEqual(recovered, sIdx, k) && !rawKeyTouchedAfter(idx, k) {
					satisfied = false
					break
				}
			}
			if !satisfied {
				break
			}
		}
		if satisfied {
			satMemo[idx] = 1
		} else {
			satMemo[idx] = 0
		}
		return satisfied
	}

	keyOverwrittenToRecoveredAfter := func(fromIdx int, key string) bool {
		var sBefore State
		if fromIdx > 0 {
			sBefore = states[fromIdx-1]
		} else {
			sBefore = make(State)
		}
		for k := fromIdx + 1; k < len(stateMeta); k++ {
			entryK := ctx.History[stateMeta[k].entryIndex]
			touched := false
			for _, mk := range entryK.Mutations {
				if mk.Kind == "range_delete" {
					if key >= string(mk.Key) && key < string(mk.End) {
						touched = true
						break
					}
				} else if string(mk.Key) == key {
					touched = true
					break
				}
			}
			if !touched || !keyStateEqual(states[k], recovered, key) {
				continue
			}
			if !opSatisfiedModuloLater(k) {
				continue
			}
			// If k sets key to the exact value it already had before fromIdx,
			// require k to have at least one witness key in recovered that differs from sBefore.
			if keyStateEqual(recovered, sBefore, key) {
				hasWitness := false
				for _, mk := range entryK.Mutations {
					if mk.Kind == "range_delete" {
						start := string(mk.Key)
						end := string(mk.End)
						for wk := range sBefore {
							if wk >= start && wk < end {
								if keyStateEqual(recovered, states[k], wk) && !keyStateEqual(recovered, sBefore, wk) {
									hasWitness = true
									break
								}
							}
						}
						if hasWitness {
							break
						}
					} else {
						wk := string(mk.Key)
						if keyStateEqual(recovered, states[k], wk) && !keyStateEqual(recovered, sBefore, wk) {
							hasWitness = true
							break
						}
					}
				}
				if !hasWitness {
					continue
				}
			}
			return true
		}
		return false
	}

	divMemo := make([]int8, len(stateMeta))
	for idx := range divMemo {
		divMemo[idx] = -1
	}

	opDivergesInRecovered := func(idx int) bool {
		if divMemo[idx] != -1 {
			return divMemo[idx] == 1
		}
		entry := ctx.History[stateMeta[idx].entryIndex]
		sIdx := states[idx]
		diverges := false
		for _, m := range entry.Mutations {
			if m.Kind == "range_delete" {
				startKey := string(m.Key)
				endKey := string(m.End)
				for rk := range recovered {
					if rk >= startKey && rk < endKey && !keyOverwrittenToRecoveredAfter(idx, rk) {
						diverges = true
						break
					}
				}
			} else {
				k := string(m.Key)
				if !keyStateEqual(recovered, sIdx, k) && !keyOverwrittenToRecoveredAfter(idx, k) {
					diverges = true
					break
				}
			}
			if diverges {
				break
			}
		}
		if diverges {
			divMemo[idx] = 1
		} else {
			divMemo[idx] = 0
		}
		return diverges
	}

	presMemo := make([]int8, len(stateMeta))
	for idx := range presMemo {
		presMemo[idx] = -1
	}

	opPresentInRecovered := func(idx int) bool {
		if presMemo[idx] != -1 {
			return presMemo[idx] == 1
		}
		entry := ctx.History[stateMeta[idx].entryIndex]
		if len(entry.Mutations) == 0 {
			presMemo[idx] = 0
			return false
		}
		sPrev := states[idx-1]
		sIdx := states[idx]
		if opDivergesInRecovered(idx) {
			presMemo[idx] = 0
			return false
		}
		present := false
		for _, m := range entry.Mutations {
			switch m.Kind {
			case "range_delete":
				startKey := string(m.Key)
				endKey := string(m.End)
				for pk := range sPrev {
					_, pres := recovered[pk]
					if pk >= startKey && pk < endKey && !pres {
						present = true
						break
					}
				}
			case "set":
				k := string(m.Key)
				if val, ok := recovered[k]; ok && val == sIdx[k] {
					present = true
				}
			default:
				k := string(m.Key)
				_, previouslyPresent := sPrev[k]
				if _, ok := recovered[k]; !ok && previouslyPresent {
					present = true
				}
			}
			if present {
				break
			}
		}
		if present {
			presMemo[idx] = 1
		} else {
			presMemo[idx] = 0
		}
		return present
	}

	lastPresent := -1
	for j := len(stateMeta) - 1; j >= 1; j-- {
		if opPresentInRecovered(j) {
			lastPresent = j
			break
		}
	}

	divergentOpKind := "unknown"
	for i := 1; i < len(stateMeta); i++ {
		// Use >= so that when the diverging operation is also the last present
		// operation (i == lastPresent), it is still correctly attributed.
		if opDivergesInRecovered(i) && lastPresent >= i {
			divergentOpKind = stateMeta[i].opKind
			break
		}
	}

	// Secondary fallback: attribute to the last-present operation if it itself
	// diverges (covers tail-divergent uncommitted batches/ingests).
	if divergentOpKind == "unknown" && lastPresent >= 1 && opDivergesInRecovered(lastPresent) {
		divergentOpKind = stateMeta[lastPresent].opKind
	}

	if divergentOpKind == "unknown" && lower < len(states)-1 {
		divergentOpKind = stateMeta[lower+1].opKind
	}
	if divergentOpKind == "unknown" && len(ctx.History) > 0 {
		// Use the last history entry rather than the first to avoid
		// misattributing tail violations to the initial operation.
		divergentOpKind = ctx.History[len(ctx.History)-1].Kind
	}

	return Outcome{
		Candidate: cand,
		Class:     ClassViolation,
		Rule:      "prefix",
		Signature: FormatSignature("prefix", divergentOpKind, ctx.Categories),
	}
}
