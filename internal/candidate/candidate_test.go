package candidate_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func smallTrace() []trace.Event {
	return []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindCreate, File: "db/log", Handle: 1, Cat: "pebble-wal"},
		{ID: 3, Kind: trace.KindWrite, File: "db/log", Handle: 1, Data: []byte("ab"), Len: 2},
		{ID: 4, Kind: trace.KindSyncData, File: "db/log", Handle: 1},
		{ID: 5, Kind: trace.KindClose, File: "db/log", Handle: 1},
	}
}

func TestSeedVectors(test *testing.T) {
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
		got, err := candidate.DeriveSeed(vector.base, vector.event, vector.percent)
		if err != nil || got != vector.want {
			test.Fatalf("DeriveSeed(%d,%d,%d) = %d, %v; want %d", vector.base, vector.event, vector.percent, got, err, vector.want)
		}
	}
	for _, percent := range []int{-1, 101} {
		if _, err := candidate.DeriveSeed(0, 0, percent); err == nil {
			test.Fatalf("percent %d accepted", percent)
		}
	}
}

func TestFullTupleIdentity(test *testing.T) {
	zero := candidate.Candidate{}
	other := candidate.Candidate{Seed: 1}
	identities := map[candidate.Candidate]bool{zero: true, other: true}
	if len(identities) != 2 {
		test.Fatal("equal event/percent with different seeds collapsed")
	}
	if err := candidate.Validate(smallTrace(), []candidate.Candidate{zero, other, {Event: 5, Percent: 100}}); err != nil {
		test.Fatalf("zero cutoff, zero seed and last endpoint rejected: %v", err)
	}
	encoded, err := json.Marshal(zero)
	if err != nil || string(encoded) != `{"event":0,"percent":0,"seed":0}` {
		test.Fatalf("wire shape changed: %s %v", encoded, err)
	}
}

func TestBuildDeterministicCanonical(test *testing.T) {
	events := smallTrace()
	first, err := candidate.Build(events, 42, []int{100, 0})
	if err != nil {
		test.Fatal(err)
	}
	second, err := candidate.Build(trace.Clone(events), 42, []int{0, 100})
	if err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		test.Fatal("Build is not deterministic or depends on policy order")
	}
	cuts, _ := trace.CutIDs(events)
	if len(first) != 2*len(cuts) {
		test.Fatalf("universe size %d, want %d", len(first), 2*len(cuts))
	}
	for index, item := range first {
		if index > 0 && !candidate.Less(first[index-1], item) {
			test.Fatal("universe not in strict canonical order")
		}
		seed, _ := candidate.DeriveSeed(42, item.Event, item.Percent)
		if item.Seed != seed {
			test.Fatal("seed not derived from (base, event, percent)")
		}
	}
	if first[0].Event != 0 || first[0].Percent != 0 || first[1].Percent != 100 {
		test.Fatal("pretrace cutoff missing for both endpoint policies")
	}
	if first[len(first)-1].Event > uint64(len(events)) {
		test.Fatal("cutoff beyond trace length")
	}
	first[0].Seed = 7
	if second[0].Seed == 7 {
		test.Fatal("Build results alias each other")
	}
}

func TestBuildRejects(test *testing.T) {
	events := smallTrace()
	for name, policies := range map[string][]int{
		"empty":        {},
		"intermediate": {0, 50},
		"duplicate":    {0, 0},
		"out of range": {101},
	} {
		if _, err := candidate.Build(events, 0, policies); err == nil {
			test.Fatalf("%s policies accepted", name)
		}
	}
	broken := smallTrace()
	broken[2].ID = 9
	if _, err := candidate.Build(broken, 0, []int{0}); err == nil {
		test.Fatal("invalid trace accepted")
	}
}

func TestValidateRejects(test *testing.T) {
	events := smallTrace()
	for name, universe := range map[string][]candidate.Candidate{
		"beyond trace": {{Event: 6}},
		"negative":     {{Percent: -1}},
		"over 100":     {{Percent: 101}},
		"duplicate":    {{Event: 1}, {Event: 1}},
	} {
		if err := candidate.Validate(events, universe); err == nil {
			test.Fatalf("%s universe accepted", name)
		}
	}
	if err := candidate.Validate(events, []candidate.Candidate{{Event: 3, Percent: 50}}); err != nil {
		test.Fatalf("representable intermediate tuple rejected by Validate: %v", err)
	}
	broken := smallTrace()
	broken[0].File = "/db"
	if err := candidate.Validate(broken, nil); err == nil {
		test.Fatal("invalid trace accepted by Validate")
	}
}

func TestWireRoundTrip(test *testing.T) {
	universe := []candidate.Candidate{
		{Event: 0, Percent: 0, Seed: 0},
		{Event: 0, Percent: 0, Seed: 1<<63 + 12345},
		{Event: 9, Percent: 50, Seed: 11532854960572963643},
		{Event: 9, Percent: 100, Seed: 7177736550350473416},
	}
	var buffer bytes.Buffer
	if err := candidate.Encode(&buffer, universe); err != nil {
		test.Fatal(err)
	}
	if !strings.Contains(buffer.String(), "9223372036854788153") {
		test.Fatalf("uint64 above 2^53 not written exactly: %s", buffer.String())
	}
	decoded, err := candidate.Decode(&buffer)
	if err != nil || !reflect.DeepEqual(decoded, universe) {
		test.Fatalf("round trip changed universe: %v %v", decoded, err)
	}
	buffer.Reset()
	if err := candidate.Encode(&buffer, nil); err != nil {
		test.Fatal(err)
	}
	if !strings.Contains(buffer.String(), `"candidates":[]`) {
		test.Fatalf("empty universe not encoded as []: %s", buffer.String())
	}
	empty, err := candidate.Decode(&buffer)
	if err != nil || empty == nil || len(empty) != 0 {
		test.Fatal("empty universe round trip failed")
	}
	if err := candidate.Encode(&buffer, []candidate.Candidate{universe[1], universe[0]}); err == nil {
		test.Fatal("non-canonical universe encoded")
	}
}

func TestDecodeRejects(test *testing.T) {
	for _, document := range []string{
		`{"schema_version":2,"candidates":[]}`,
		`{"candidates":[]}`,
		`{"schema_version":1}`,
		`{"schema_version":1,"candidates":null}`,
		`{"schema_version":1,"candidates":[],"extra":1}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":0,"seed":0,"x":1}]}`,
		`{"schema_version":1,"candidates":[]} {}`,
		`{"schema_version":1,"candidates":[{"event":2,"percent":0,"seed":0},{"event":1,"percent":0,"seed":0}]}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":0,"seed":0},{"event":1,"percent":0,"seed":0}]}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":101,"seed":0}]}`,
		`{"schema_version":1,"candidates":[{"event":1.5,"percent":0,"seed":0}]}`,
	} {
		if _, err := candidate.Decode(strings.NewReader(document)); err == nil {
			test.Fatalf("invalid document accepted: %s", document)
		}
	}
}

func opTrace() []trace.Event {
	return []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindCreate, File: "db/000004.log", Handle: 1, Cat: "pebble-wal"},
		{ID: 3, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 4, Kind: trace.KindWrite, File: "db/000004.log", Handle: 1, Data: []byte("val1"), Len: 4},
		{ID: 5, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"}, // unsynced OpEnd: redundant!
		{ID: 6, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 7, Kind: trace.KindWrite, File: "db/000004.log", Handle: 1, Off: 4, Data: []byte("val2"), Len: 4},
		{ID: 8, Kind: trace.KindSyncData, File: "db/000004.log", Handle: 1}, // sync barrier
		{ID: 9, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},                // synchronous OpEnd: advances durability epoch
		{ID: 10, Kind: trace.KindClose, File: "db/000004.log", Handle: 1},
	}
}

func TestStateHashDeterminism(t *testing.T) {
	events := opTrace()
	for cut := uint64(0); cut <= uint64(len(events)); cut++ {
		for _, percent := range []int{0, 100} {
			h1 := candidate.StateHash(events, cut, percent)
			h2 := candidate.StateHash(trace.Clone(events), cut, percent)
			if h1 != h2 {
				t.Fatalf("cut %d percent %d: non-deterministic hash %s vs %s", cut, percent, h1, h2)
			}
			if len(h1) != 64 {
				t.Fatalf("cut %d percent %d: expected 64-char hex sha256, got %d (%s)", cut, percent, len(h1), h1)
			}
		}
	}
}

func TestStateHashOpEndEquivalence(t *testing.T) {
	events := opTrace()
	// Cutoff 4 is the un-synced Write. Cutoff 5 is the OpEnd.
	// Because no sync occurred, both cutoffs share the identical durable state at 0% retention.
	hWrite0 := candidate.StateHash(events, 4, 0)
	hOpEnd0 := candidate.StateHash(events, 5, 0)
	if hWrite0 != hOpEnd0 {
		t.Fatalf("unsynced OpEnd should be equivalent to preceding Write at 0%% retention: %s != %s", hWrite0, hOpEnd0)
	}

	// At 100% retention, OpEnd performs 0 VFS mutations, so volatile filesystem is also identical.
	hWrite100 := candidate.StateHash(events, 4, 100)
	hOpEnd100 := candidate.StateHash(events, 5, 100)
	if hWrite100 != hOpEnd100 {
		t.Fatalf("unsynced OpEnd should be equivalent to preceding Write at 100%% retention: %s != %s", hWrite100, hOpEnd100)
	}
}

func TestStateHashSyncAdvancesDurableState(t *testing.T) {
	events := opTrace()
	// Before sync (cutoff 7) vs After sync (cutoff 8)
	hPreSync := candidate.StateHash(events, 7, 0)
	hPostSync := candidate.StateHash(events, 8, 0)
	if hPreSync == hPostSync {
		t.Fatalf("SyncData must advance canonical durable state: both hashes are %s", hPreSync)
	}
}

func TestCollapseEquivalenceStatesOpEndCollapsed(t *testing.T) {
	events := opTrace()
	universe, err := candidate.Build(events, 123, []int{0, 100})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	collapsed := candidate.CollapseEquivalenceStates(universe, events)
	if len(collapsed) >= len(universe) {
		t.Fatalf("expected collapsing to reduce universe: original %d, collapsed %d", len(universe), len(collapsed))
	}

	// Verify that the unsynced OpEnd cutoff (cut 5) is NOT in collapsed universe
	// because cut 4 was chosen as the mutation representative.
	for _, c := range collapsed {
		if c.Event == 5 {
			t.Fatalf("redundant unsynced OpEnd cutoff 5 was not collapsed: %+v", c)
		}
	}

	// Verify collapsed list is strictly sorted and contains no duplicates
	for i := 1; i < len(collapsed); i++ {
		if !candidate.Less(collapsed[i-1], collapsed[i]) {
			t.Fatalf("collapsed universe not strictly canonically ordered at %d: %+v >= %+v",
				i, collapsed[i-1], collapsed[i])
		}
	}
}

func TestCollapseEquivalenceStatesRepresentativeSelection(t *testing.T) {
	events := opTrace()
	// Cutoff 7 is BeforeBoundary == true (next event is SyncData at 8).
	if !trace.BeforeBoundary(events, 7) {
		t.Fatalf("expected event 7 to be BeforeBoundary")
	}

	universe, err := candidate.Build(events, 100, []int{0, 100})
	if err != nil {
		t.Fatal(err)
	}

	collapsed := candidate.CollapseEquivalenceStates(universe, events)
	foundBoundary := false
	for _, c := range collapsed {
		if c.Event == 7 && c.Percent == 0 {
			foundBoundary = true
			break
		}
	}
	if !foundBoundary {
		t.Fatalf("expected boundary cutoff 7 to be selected as representative")
	}
}

func TestCollapseEquivalenceStatesWithoutTrace(t *testing.T) {
	// Call CollapseEquivalenceStates with duplicate candidates and no trace
	candidates := []candidate.Candidate{
		{Event: 2, Percent: 0, Seed: 1},
		{Event: 1, Percent: 0, Seed: 2},
		{Event: 1, Percent: 0, Seed: 3}, // duplicate (Event, Percent)
	}
	collapsed := candidate.CollapseEquivalenceStates(candidates)
	if len(collapsed) != 2 {
		t.Fatalf("expected 2 distinct candidates, got %d", len(collapsed))
	}
	if collapsed[0].Event != 1 || collapsed[1].Event != 2 {
		t.Fatalf("expected canonical sort: %+v", collapsed)
	}
}

func TestEquivalenceClasses(t *testing.T) {
	events := opTrace()
	universe, err := candidate.Build(events, 50, []int{0, 100})
	if err != nil {
		t.Fatal(err)
	}

	classes := candidate.EquivalenceClasses(universe, events)
	if len(classes) == 0 {
		t.Fatal("empty equivalence classes")
	}

	totalInClasses := 0
	for _, members := range classes {
		totalInClasses += len(members)
	}
	if totalInClasses != len(universe) {
		t.Fatalf("expected all %d candidates to be partitioned, got %d", len(universe), totalInClasses)
	}
}

func TestBuildCollapsed(t *testing.T) {
	events := opTrace()
	collapsed, err := candidate.BuildCollapsed(events, 999, []int{0, 100})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := candidate.Build(events, 999, []int{0, 100})
	if err != nil {
		t.Fatal(err)
	}

	if len(collapsed) >= len(raw) {
		t.Fatalf("BuildCollapsed (%d) did not reduce raw universe (%d)", len(collapsed), len(raw))
	}
}
