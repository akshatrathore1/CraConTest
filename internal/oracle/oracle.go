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

func isTornBatchState(base, full, recovered State) bool {
	if stateEqual(base, full) {
		return false
	}
	for k := range recovered {
		if !keyStateEqual(recovered, base, k) && !keyStateEqual(recovered, full, k) {
			return false
		}
	}
	for k := range base {
		if !keyStateEqual(recovered, base, k) && !keyStateEqual(recovered, full, k) {
			return false
		}
	}
	for k := range full {
		if !keyStateEqual(recovered, base, k) && !keyStateEqual(recovered, full, k) {
			return false
		}
	}
	return !stateEqual(recovered, base) && !stateEqual(recovered, full)
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
	if cand.Percent < 0 || cand.Percent > 100 {
		return Outcome{
			Candidate: cand,
			Class:     ClassOracleError,
			Rule:      "invalid_candidate",
			Detail:    fmt.Sprintf("candidate percent %d out of range [0, 100]", cand.Percent),
		}
	}

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
	for idx := lower - 1; idx >= 0; idx-- {
		if stateEqual(states[idx], recovered) {
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

	// Rule 2: atomic_batch (matches an interior or torn subset state of a batch).
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
	for idx := 1; idx < len(stateMeta); idx++ {
		if stateMeta[idx].opKind != "batch" {
			continue
		}
		if isTornBatchState(states[idx-1], states[idx], recovered) {
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
	divMemo := make([]int8, len(stateMeta))
	for idx := range divMemo {
		divMemo[idx] = -1
	}

	// Indices built during the reverse pass (len(stateMeta)-1 down to 1) over
	// non-diverging operations so keyOverwrittenToRecoveredAfter inspects only k > fromIdx
	// in O(1) for non-witness overwrites instead of O(N) scans.
	survivingAnyMatch := make(map[string]int)
	survivingWitnessOps := make(map[string][]int)
	var survivingRangeDeletes []int

	opHasRangeDeleteCovering := func(entry history.Entry, key string) bool {
		for _, mk := range entry.Mutations {
			if mk.Kind == "range_delete" && key >= string(mk.Key) && key < string(mk.End) {
				return true
			}
		}
		return false
	}

	hasWitnessAt := func(k int, sBefore State) bool {
		entryK := ctx.History[stateMeta[k].entryIndex]
		sPrevK := states[k-1]
		for _, mk := range entryK.Mutations {
			if mk.Kind == "range_delete" {
				start := string(mk.Key)
				end := string(mk.End)
				for wk := range sPrevK {
					if wk >= start && wk < end {
						if _, inRec := recovered[wk]; !inRec && keyStateEqual(recovered, states[k], wk) {
							return true
						}
					}
				}
				for wk := range sBefore {
					if wk >= start && wk < end {
						if keyStateEqual(recovered, states[k], wk) && !keyStateEqual(recovered, sBefore, wk) {
							return true
						}
					}
				}
			} else if mk.Kind == "delete" {
				wk := string(mk.Key)
				_, inPrev := sPrevK[wk]
				_, inRec := recovered[wk]
				if keyStateEqual(recovered, states[k], wk) && ((inPrev && !inRec) || !keyStateEqual(recovered, sBefore, wk)) {
					return true
				}
			} else {
				wk := string(mk.Key)
				if keyStateEqual(recovered, states[k], wk) && !keyStateEqual(recovered, sBefore, wk) {
					return true
				}
			}
		}
		return false
	}

	type overwriteMemoKey struct {
		fromIdx int
		key     string
	}
	overwriteMemo := make(map[overwriteMemoKey]bool)

	keyOverwrittenToRecoveredAfter := func(fromIdx int, key string) bool {
		mk := overwriteMemoKey{fromIdx: fromIdx, key: key}
		if cached, ok := overwriteMemo[mk]; ok {
			return cached
		}
		var sBefore State
		if fromIdx > 0 {
			sBefore = states[fromIdx-1]
		} else {
			sBefore = make(State)
		}
		needWitness := keyStateEqual(recovered, sBefore, key)
		matched := false
		if !needWitness {
			if survivingAnyMatch[key] > fromIdx {
				matched = true
			} else {
				for _, k := range survivingRangeDeletes {
					if k > fromIdx && opHasRangeDeleteCovering(ctx.History[stateMeta[k].entryIndex], key) && keyStateEqual(states[k], recovered, key) {
						matched = true
						break
					}
				}
			}
		} else {
			for _, k := range survivingWitnessOps[key] {
				if k > fromIdx && (k <= lower || hasWitnessAt(k, sBefore)) {
					matched = true
					break
				}
			}
			if !matched {
				for _, k := range survivingRangeDeletes {
					if k > fromIdx && opHasRangeDeleteCovering(ctx.History[stateMeta[k].entryIndex], key) && keyStateEqual(states[k], recovered, key) && (k <= lower || hasWitnessAt(k, sBefore)) {
						matched = true
						break
					}
				}
			}
			if !matched && survivingAnyMatch[key] > fromIdx && survivingAnyMatch[key] <= lower {
				matched = true
			}
		}
		overwriteMemo[mk] = matched
		return matched
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
					if rk >= startKey && rk < endKey && !keyStateEqual(recovered, sIdx, rk) && !keyOverwrittenToRecoveredAfter(idx, rk) {
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

	// Evaluate divergence in strictly descending index order so every k > idx
	// is already finalized and indexed before opDivergesInRecovered(idx) queries it.
	for idx := len(stateMeta) - 1; idx >= 1; idx-- {
		if opDivergesInRecovered(idx) {
			continue
		}
		entry := ctx.History[stateMeta[idx].entryIndex]
		if len(entry.Mutations) == 0 {
			continue
		}
		canWitness := len(entry.Mutations) > 1 || entry.Mutations[0].Kind != "set"
		hasRangeDelete := false
		var seenKeys map[string]bool
		if len(entry.Mutations) > 1 {
			seenKeys = make(map[string]bool, len(entry.Mutations))
		}
		for _, m := range entry.Mutations {
			if m.Kind == "range_delete" {
				hasRangeDelete = true
				continue
			}
			kStr := string(m.Key)
			if seenKeys != nil {
				if seenKeys[kStr] {
					continue
				}
				seenKeys[kStr] = true
			}
			if keyStateEqual(states[idx], recovered, kStr) {
				if survivingAnyMatch[kStr] == 0 {
					survivingAnyMatch[kStr] = idx
				}
				if canWitness {
					survivingWitnessOps[kStr] = append(survivingWitnessOps[kStr], idx)
				}
			}
		}
		if hasRangeDelete {
			survivingRangeDeletes = append(survivingRangeDeletes, idx)
		}
	}

	opHasEvidenceInRecovered := func(idx int) bool {
		entry := ctx.History[stateMeta[idx].entryIndex]
		if len(entry.Mutations) == 0 {
			return false
		}
		sPrev := states[idx-1]
		sIdx := states[idx]
		for _, m := range entry.Mutations {
			switch m.Kind {
			case "range_delete":
				startKey := string(m.Key)
				endKey := string(m.End)
				for pk := range sPrev {
					if _, pres := recovered[pk]; pk >= startKey && pk < endKey && !pres {
						return true
					}
				}
			case "set":
				k := string(m.Key)
				sVal, inS := sIdx[k]
				if val, ok := recovered[k]; ok && inS && val == sVal {
					return true
				}
			default:
				k := string(m.Key)
				_, previouslyPresent := sPrev[k]
				if _, ok := recovered[k]; !ok && previouslyPresent {
					return true
				}
			}
		}
		return false
	}

	lastPartiallyPresent := -1
	for j := len(stateMeta) - 1; j >= 1; j-- {
		if opHasEvidenceInRecovered(j) {
			lastPartiallyPresent = j
			break
		}
	}

	divergentOpKind := "unknown"
	for i := 1; i < len(stateMeta); i++ {
		if opDivergesInRecovered(i) && lastPartiallyPresent >= i {
			divergentOpKind = stateMeta[i].opKind
			break
		}
	}

	if divergentOpKind == "unknown" && lastPartiallyPresent >= 1 && opDivergesInRecovered(lastPartiallyPresent) {
		divergentOpKind = stateMeta[lastPartiallyPresent].opKind
	}

	if divergentOpKind == "unknown" && lower < len(states)-1 {
		divergentOpKind = stateMeta[lower+1].opKind
	}
	if divergentOpKind == "unknown" && len(stateMeta) > 1 {
		divergentOpKind = stateMeta[len(stateMeta)-1].opKind
	}
	if divergentOpKind == "" {
		divergentOpKind = "unknown"
	}

	return Outcome{
		Candidate: cand,
		Class:     ClassViolation,
		Rule:      "prefix",
		Signature: FormatSignature("prefix", divergentOpKind, ctx.Categories),
	}
}
