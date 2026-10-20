package trace_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type referenceCandidate struct {
	Event   uint64 `json:"event"`
	Percent int    `json:"percent"`
	Seed    uint64 `json:"seed"`
}

type referenceMutation struct {
	Kind  string `json:"kind"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
	End   []byte `json:"end,omitempty"`
}

type referenceEntry struct {
	Op         uint64              `json:"op"`
	Kind       string              `json:"kind"`
	Mutations  []referenceMutation `json:"mutations,omitempty"`
	Sync       bool                `json:"sync,omitempty"`
	BeginEvent uint64              `json:"begin_event"`
	EndEvent   uint64              `json:"end_event,omitempty"`
	Err        string              `json:"err,omitempty"`
}

type referenceOutcome struct {
	Candidate referenceCandidate `json:"candidate"`
	Class     string             `json:"class"`
	Rule      string             `json:"rule,omitempty"`
	Signature string             `json:"signature,omitempty"`
	Detail    string             `json:"detail,omitempty"`
}

func referenceSeed(base, event uint64, percent int) uint64 {
	encoded := []byte("cracontest/candidate-seed/v1\x00")
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], base)
	encoded = append(encoded, number[:]...)
	binary.BigEndian.PutUint64(number[:], event)
	encoded = append(encoded, number[:]...)
	encoded = append(encoded, byte(percent))
	digest := sha256.Sum256(encoded)
	return binary.BigEndian.Uint64(digest[:8])
}

func TestContractReferenceSeedsAndIdentity(test *testing.T) {
	vectors := []struct {
		base, event uint64
		percent     int
		want        uint64
	}{
		{0, 0, 0, 5324358737731858600},
		{0, 1, 0, 14675981572963742630},
		{1, 9, 100, 7177736550350473416},
		{42, 9, 50, 11532854960572963643},
	}
	for _, vector := range vectors {
		if got := referenceSeed(vector.base, vector.event, vector.percent); got != vector.want {
			test.Fatalf("seed vector: got %d, want %d", got, vector.want)
		}
	}
	candidate := referenceCandidate{Event: 0, Percent: 0, Seed: 0}
	encoded, err := json.Marshal(candidate)
	if err != nil {
		test.Fatal(err)
	}
	if string(encoded) != `{"event":0,"percent":0,"seed":0}` {
		test.Fatal(string(encoded))
	}
	var decoded referenceCandidate
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		test.Fatal(err)
	}
	identities := map[referenceCandidate]bool{candidate: true, {Event: 0, Percent: 0, Seed: 1}: true}
	if candidate != decoded || len(identities) != 2 {
		test.Fatal("candidate identity is not the full tuple")
	}
}

func referenceUniverseValid(universe []referenceCandidate, lastEvent uint64) bool {
	seen := make(map[referenceCandidate]bool)
	for _, candidate := range universe {
		if candidate.Event > lastEvent || candidate.Percent < 0 || candidate.Percent > 100 || seen[candidate] {
			return false
		}
		seen[candidate] = true
	}
	return true
}

func referencePermutation(universe, ranking []referenceCandidate) bool {
	if len(universe) != len(ranking) {
		return false
	}
	remaining := make(map[referenceCandidate]bool)
	for _, candidate := range universe {
		if remaining[candidate] {
			return false
		}
		remaining[candidate] = true
	}
	for _, candidate := range ranking {
		if !remaining[candidate] {
			return false
		}
		delete(remaining, candidate)
	}
	return len(remaining) == 0
}

func TestContractReferenceUniverseAndPermutations(test *testing.T) {
	universe := []referenceCandidate{{0, 0, 0}, {9, 100, 7177736550350473416}, {9, 100, 0}}
	if !referenceUniverseValid(universe, 9) || !referencePermutation(universe, []referenceCandidate{universe[2], universe[0], universe[1]}) {
		test.Fatal("valid endpoint universe or full-tuple permutation rejected")
	}
	for _, invalid := range [][]referenceCandidate{
		{{10, 0, 0}}, {{0, -1, 0}}, {{0, 101, 0}}, {universe[0], universe[0]},
	} {
		if referenceUniverseValid(invalid, 9) {
			test.Fatal("invalid universe accepted")
		}
	}
	for _, invalid := range [][]referenceCandidate{
		universe[:2], {universe[0], universe[1], universe[1]},
		{universe[0], universe[1], {9, 50, 0}},
	} {
		if referencePermutation(universe, invalid) {
			test.Fatal("ranking omission, duplicate or fabricated tuple accepted")
		}
	}
	if !referenceUniverseValid([]referenceCandidate{{0, 50, 0}}, 0) {
		test.Fatal("representable intermediate policy rejected")
	}
	var encoded bytes.Buffer
	document := struct {
		SchemaVersion int                  `json:"schema_version"`
		Candidates    []referenceCandidate `json:"candidates"`
	}{1, universe}
	if err := json.NewEncoder(&encoded).Encode(document); err != nil {
		test.Fatal(err)
	}
	var decoded = document
	if err := json.NewDecoder(&encoded).Decode(&decoded); err != nil || !reflect.DeepEqual(document, decoded) {
		test.Fatal("universe round trip changed uint64 identity")
	}
}

func copyState(state map[string]string) map[string]string {
	result := make(map[string]string, len(state))
	for key, value := range state {
		result[key] = value
	}
	return result
}

func applyReference(state map[string]string, mutation referenceMutation) bool {
	key := string(mutation.Key)
	switch mutation.Kind {
	case "set":
		state[key] = string(mutation.Value)
	case "delete":
		delete(state, key)
	case "range_delete":
		end := string(mutation.End)
		if key >= end {
			return false
		}
		for current := range state {
			if current >= key && current < end {
				delete(state, current)
			}
		}
	default:
		return false
	}
	return true
}

func referenceOracle(entries []referenceEntry, cut uint64, initial, recovered map[string]string) (string, string) {
	states := []map[string]string{copyState(initial)}
	var interiors []map[string]string
	lower := 0
	var previousOp, previousBegin, previousEnd uint64
	for index, entry := range entries {
		if entry.Op <= previousOp || entry.BeginEvent == 0 || entry.BeginEvent <= previousBegin ||
			(entry.EndEvent != 0 && entry.EndEvent <= entry.BeginEvent) ||
			(entry.EndEvent == 0 && entry.Err != "") ||
			(index > 0 && (previousEnd == 0 || previousEnd >= entry.BeginEvent)) {
			return "oracle_error", "invalid_history"
		}
		previousOp, previousBegin, previousEnd = entry.Op, entry.BeginEvent, entry.EndEvent
		mutating := trace.MutatingOperation(entry.Kind)
		if mutating {
			if len(entry.Mutations) == 0 || (entry.Kind != "batch" && len(entry.Mutations) != 1) {
				return "oracle_error", "invalid_history"
			}
			if entry.Err != "" {
				return "oracle_error", "failed_mutation"
			}
		} else {
			switch entry.Kind {
			case "get", "iterate", "flush", "compact":
			default:
				return "oracle_error", "invalid_history"
			}
			if len(entry.Mutations) != 0 || entry.Sync {
				return "oracle_error", "invalid_history"
			}
		}
		for _, mutation := range entry.Mutations {
			if entry.Kind != "batch" && entry.Kind != mutation.Kind {
				return "oracle_error", "invalid_mutation"
			}
			switch mutation.Kind {
			case "set":
				if len(mutation.End) != 0 {
					return "oracle_error", "invalid_mutation"
				}
			case "delete":
				if len(mutation.Value) != 0 || len(mutation.End) != 0 {
					return "oracle_error", "invalid_mutation"
				}
			case "range_delete":
				if len(mutation.Value) != 0 || bytes.Compare(mutation.Key, mutation.End) >= 0 {
					return "oracle_error", "invalid_mutation"
				}
			default:
				return "oracle_error", "invalid_mutation"
			}
		}
		if entry.BeginEvent > cut {
			continue
		}
		if !mutating {
			continue
		}
		acknowledged := entry.EndEvent > 0 && entry.EndEvent <= cut
		state := copyState(states[len(states)-1])
		for mutationIndex, mutation := range entry.Mutations {
			if !applyReference(state, mutation) {
				return "oracle_error", "invalid_mutation"
			}
			if entry.Kind == "batch" && mutationIndex+1 < len(entry.Mutations) {
				interiors = append(interiors, copyState(state))
			}
		}
		states = append(states, state)
		if acknowledged && entry.Sync {
			lower = len(states) - 1
		}
	}
	for _, state := range states[lower:] {
		if reflect.DeepEqual(state, recovered) {
			return "valid", ""
		}
	}
	for _, state := range states[:lower] {
		if reflect.DeepEqual(state, recovered) {
			return "violation", "durably_committed"
		}
	}
	for _, state := range interiors {
		if reflect.DeepEqual(state, recovered) {
			return "violation", "atomic_batch"
		}
	}
	return "violation", "prefix"
}

func TestContractReferenceOracle(test *testing.T) {
	set := func(key, value string) referenceMutation {
		return referenceMutation{Kind: "set", Key: []byte(key), Value: []byte(value)}
	}
	entry := func(op, begin, end uint64, sync bool, mutations ...referenceMutation) referenceEntry {
		return referenceEntry{Op: op, Kind: "set", Mutations: mutations, Sync: sync, BeginEvent: begin, EndEvent: end}
	}
	first := entry(1, 1, 2, true, set("a", "A"))
	second := entry(2, 3, 4, false, set("b", "B"))
	batch := entry(2, 3, 0, true, set("b", "B"), set("c", "C"))
	batch.Kind = "batch"
	failed := second
	failed.Err = "write failed"
	cases := []struct {
		name        string
		entries     []referenceEntry
		cut         uint64
		state       map[string]string
		class, rule string
	}{
		{"pretrace", nil, 0, map[string]string{}, "valid", ""},
		{"ack at cutoff", []referenceEntry{first}, 2, map[string]string{"a": "A"}, "valid", ""},
		{"lost synced prefix", []referenceEntry{first}, 2, map[string]string{}, "violation", "durably_committed"},
		{"optional NoSync suffix", []referenceEntry{first, second}, 4, map[string]string{"a": "A"}, "valid", ""},
		{"hole", []referenceEntry{first, second}, 4, map[string]string{"b": "B"}, "violation", "prefix"},
		{"inflight batch absent", []referenceEntry{first, batch}, 3, map[string]string{"a": "A"}, "valid", ""},
		{"inflight batch whole", []referenceEntry{first, batch}, 3, map[string]string{"a": "A", "b": "B", "c": "C"}, "valid", ""},
		{"inflight batch torn", []referenceEntry{first, batch}, 3, map[string]string{"a": "A", "b": "B"}, "violation", "atomic_batch"},
		{"before batch begin", []referenceEntry{first, batch}, 2, map[string]string{"a": "A"}, "valid", ""},
		{"failed acknowledged mutation", []referenceEntry{first, failed}, 4, map[string]string{"a": "A"}, "oracle_error", "failed_mutation"},
	}
	laterSync := second
	laterSync.Sync = true
	unsyncedFirst := first
	unsyncedFirst.Sync = false
	cases = append(cases, struct {
		name        string
		entries     []referenceEntry
		cut         uint64
		state       map[string]string
		class, rule string
	}{
		"later Sync covers NoSync", []referenceEntry{unsyncedFirst, laterSync}, 4, map[string]string{"a": "A"}, "violation", "durably_committed"})
	overwrite := entry(2, 3, 4, false, set("a", "new"))
	deletion := entry(2, 3, 4, false, referenceMutation{Kind: "delete", Key: []byte("a")})
	deletion.Kind = "delete"
	for _, sample := range []struct {
		name  string
		next  referenceEntry
		state map[string]string
	}{
		{"overwrite", overwrite, map[string]string{"a": "new"}}, {"delete", deletion, map[string]string{}},
	} {
		class, _ := referenceOracle([]referenceEntry{first, sample.next}, 4, map[string]string{}, sample.state)
		if class != "valid" {
			test.Fatalf("%s rejected", sample.name)
		}
	}
	for _, sample := range cases {
		test.Run(sample.name, func(test *testing.T) {
			class, rule := referenceOracle(sample.entries, sample.cut, map[string]string{}, sample.state)
			if class != sample.class || rule != sample.rule {
				test.Fatalf("got %s/%s, want %s/%s", class, rule, sample.class, sample.rule)
			}
		})
	}
	rangeEntry := entry(1, 1, 2, false, referenceMutation{Kind: "range_delete", Key: []byte("b"), End: []byte("d")})
	rangeEntry.Kind = "range_delete"
	class, _ := referenceOracle([]referenceEntry{rangeEntry}, 2, map[string]string{"a": "A", "b": "B", "c": "C", "d": "D"}, map[string]string{"a": "A", "d": "D"})
	if class != "valid" {
		test.Fatal("range-delete boundaries are incorrect")
	}
	repeated := entry(2, 3, 4, true, set("a", "A"))
	class, _ = referenceOracle([]referenceEntry{first, repeated}, 4, map[string]string{}, map[string]string{"a": "A"})
	if class != "valid" {
		test.Fatal("equivalent legal prefixes were rejected")
	}
	overlap := second
	overlap.BeginEvent = 2
	class, _ = referenceOracle([]referenceEntry{first, overlap}, 4, map[string]string{}, map[string]string{"a": "A"})
	if class != "oracle_error" {
		test.Fatal("overlapping histories were accepted")
	}
}

func TestContractReferenceInvalidHistory(test *testing.T) {
	valid := referenceEntry{Op: 1, Kind: "set", Mutations: []referenceMutation{{Kind: "set", Key: []byte("a"), Value: []byte("A")}}, BeginEvent: 1, EndEvent: 2}
	cases := map[string]func(*referenceEntry){
		"zero operation":      func(entry *referenceEntry) { entry.Op = 0 },
		"zero begin":          func(entry *referenceEntry) { entry.BeginEvent = 0 },
		"same marker":         func(entry *referenceEntry) { entry.EndEvent = 1 },
		"unknown operation":   func(entry *referenceEntry) { entry.Kind = "unknown" },
		"empty mutation":      func(entry *referenceEntry) { entry.Mutations = nil },
		"multiple single":     func(entry *referenceEntry) { entry.Mutations = append(entry.Mutations, entry.Mutations[0]) },
		"wrong mutation kind": func(entry *referenceEntry) { entry.Mutations[0].Kind = "delete" },
		"invalid range": func(entry *referenceEntry) {
			entry.Kind = "range_delete"
			entry.Mutations[0] = referenceMutation{Kind: "range_delete", Key: []byte("z"), End: []byte("a")}
		},
		"read with mutations":  func(entry *referenceEntry) { entry.Kind = "get" },
		"read with sync":       func(entry *referenceEntry) { entry.Kind = "get"; entry.Mutations = nil; entry.Sync = true },
		"unacknowledged error": func(entry *referenceEntry) { entry.EndEvent = 0; entry.Err = "failed" },
	}
	for name, corrupt := range cases {
		test.Run(name, func(test *testing.T) {
			entry := valid
			entry.Mutations = append([]referenceMutation(nil), valid.Mutations...)
			corrupt(&entry)
			class, _ := referenceOracle([]referenceEntry{entry}, 0, map[string]string{}, map[string]string{})
			if class != "oracle_error" {
				test.Fatal("malformed ledger accepted even before its first operation")
			}
		})
	}
	unfinished := valid
	unfinished.EndEvent = 0
	next := valid
	next.Op, next.BeginEvent, next.EndEvent = 2, 3, 4
	for _, entries := range [][]referenceEntry{{valid, valid}, {unfinished, next}} {
		class, _ := referenceOracle(entries, 4, map[string]string{}, map[string]string{})
		if class != "oracle_error" {
			test.Fatal("repeated or concurrent operation accepted")
		}
	}
}

func TestContractReferenceOutcomesAndScores(test *testing.T) {
	for _, class := range []string{"valid", "violation", "open_error", "panic", "hang", "oracle_error"} {
		outcome := referenceOutcome{Candidate: referenceCandidate{Event: 8, Percent: 0, Seed: 0}, Class: class}
		encoded, err := json.Marshal(outcome)
		if err != nil {
			test.Fatal(err)
		}
		var decoded referenceOutcome
		if err = json.Unmarshal(encoded, &decoded); err != nil {
			test.Fatal(err)
		}
		if outcome != decoded {
			test.Fatal("outcome changed its tuple or class")
		}
	}
	type features struct {
		desc, cross, semantic float64
		state                 string
	}
	values := map[uint64]features{1: {2, 3, 1, "A"}, 2: {2, 3, 1, "A"}, 3: {2, 4, 0, "B"}}
	universe := []referenceCandidate{{1, 0, 1}, {2, 0, 2}, {3, 100, 3}}
	original := append([]referenceCandidate(nil), universe...)
	remaining := append([]referenceCandidate(nil), universe...)
	seen := make(map[string]bool)
	var order []uint64
	for len(remaining) > 0 {
		best, bestScore := 0, -1.0
		for index, candidate := range remaining {
			feature := values[candidate.Event]
			stateKey := fmt.Sprintf("%s/%d", feature.state, candidate.Percent)
			novel := 0.0
			if !seen[stateKey] {
				novel = 1
			}
			score := feature.desc + feature.cross + feature.semantic + novel
			if score > bestScore {
				best, bestScore = index, score
			}
		}
		selected := remaining[best]
		order = append(order, selected.Event)
		seen[fmt.Sprintf("%s/%d", values[selected.Event].state, selected.Percent)] = true
		remaining = append(remaining[:best], remaining[best+1:]...)
	}
	if !reflect.DeepEqual(order, []uint64{1, 3, 2}) || !reflect.DeepEqual(original, universe) {
		test.Fatal("four-term score, greedy novelty, tie order or input immutability changed")
	}
}

func TestTraceV1ReplayFilesystemEffects(test *testing.T) {
	filesystem := vfs.NewMem()
	files := make(map[uint64]vfs.File)
	locks := make(map[uint64]io.Closer)
	for _, event := range contractTrace() {
		var err error
		switch event.Kind {
		case trace.KindMkdirAll:
			err = filesystem.MkdirAll(event.File, os.FileMode(event.Mode))
		case trace.KindCreate:
			files[event.Handle], err = filesystem.Create(event.File, vfs.WriteCategoryUnspecified)
		case trace.KindOpenReadWrite:
			files[event.Handle], err = filesystem.OpenReadWrite(event.File, vfs.WriteCategoryUnspecified)
		case trace.KindOpenDir:
			files[event.Handle], err = filesystem.OpenDir(event.File)
		case trace.KindRead:
			var count int
			count, err = files[event.Handle].Read(make([]byte, int(event.Size)))
			if count != event.Len {
				test.Fatal("read result mismatch")
			}
		case trace.KindWrite:
			var count int
			count, err = files[event.Handle].Write(bytes.Clone(event.Data[:event.Len]))
			if count != event.Len {
				test.Fatal("write result mismatch")
			}
		case trace.KindWriteAt:
			var count int
			count, err = files[event.Handle].WriteAt(bytes.Clone(event.Data[:event.Len]), event.Off)
			if count != event.Len {
				test.Fatal("WriteAt result mismatch")
			}
		case trace.KindSync:
			err = files[event.Handle].Sync()
		case trace.KindSyncData:
			err = files[event.Handle].SyncData()
		case trace.KindSyncTo:
			var full bool
			full, err = files[event.Handle].SyncTo(event.Size)
			if full != event.FullSync {
				test.Fatal("SyncTo result mismatch")
			}
		case trace.KindLink:
			err = filesystem.Link(event.File, event.File2)
		case trace.KindRename:
			err = filesystem.Rename(event.File, event.File2)
		case trace.KindRemove:
			err = filesystem.Remove(event.File)
		case trace.KindClose:
			err = files[event.Handle].Close()
			delete(files, event.Handle)
		case trace.KindLock:
			locks[event.Handle], err = filesystem.Lock(event.File)
		case trace.KindUnlock:
			err = locks[event.Handle].Close()
			delete(locks, event.Handle)
		}
		if err != nil {
			test.Fatalf("event %d: %v", event.ID, err)
		}
	}
	file, err := filesystem.Open("db/link")
	if err != nil {
		test.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		test.Fatal(err)
	}
	if err = file.Close(); err != nil {
		test.Fatal(err)
	}
	if string(data) != "UZQd" {
		test.Fatalf("handle/alias/cursor replay got %q", data)
	}
	if err = filesystem.Rename("db/link", "missing/new"); err == nil {
		test.Fatal("expected failed rename")
	}
	if _, err = filesystem.Stat("db/link"); !os.IsNotExist(err) {
		test.Fatalf("failed rename side effect was lost: %v", err)
	}
	test.Log(fmt.Sprintf("replayed %d events; failed rename effects retained", len(contractTrace())))
}
