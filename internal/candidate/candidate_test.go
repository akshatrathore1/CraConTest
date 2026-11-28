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
		"duplicate":    {0, 0},
		"out of range": {101},
		"negative":     {-1},
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

func TestBuildIntermediate(test *testing.T) {
	events := smallTrace()
	universe, err := candidate.Build(events, 42, []int{0, 50, 100})
	if err != nil {
		test.Fatalf("Build failed: %v", err)
	}
	cuts, err := trace.CutIDs(events)
	if err != nil {
		test.Fatalf("CutIDs failed: %v", err)
	}
	if len(universe) != 3*len(cuts) {
		test.Fatalf("universe size %d, want %d", len(universe), 3*len(cuts))
	}
	for index, item := range universe {
		if index > 0 && !candidate.Less(universe[index-1], item) {
			test.Fatalf("universe not in strict canonical order at index %d", index)
		}
		seed, err := candidate.DeriveSeed(42, item.Event, item.Percent)
		if err != nil {
			test.Fatalf("DeriveSeed failed: %v", err)
		}
		if item.Seed != seed {
			test.Fatalf("seed mismatch at index %d: got %d, want %d", index, item.Seed, seed)
		}
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
		`{"schema_version":1,"candidates":[null]}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":0,"seed":0},null]}`,
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
	// Without parent directory syncs, SyncData on db/000004.log does not make
	// the file durable under 0% retention (parent directory entry is still pending).
	unsyncedDirEvents := opTrace()
	if h7, h8 := candidate.StateHash(unsyncedDirEvents, 7, 0), candidate.StateHash(unsyncedDirEvents, 8, 0); h7 != h8 {
		t.Fatalf("SyncData without parent directory sync should not alter 0%% durable state: %s vs %s", h7, h8)
	}

	// Once "." and "db" are synced, SyncData on db/000004.log advances 0% durable state
	// while leaving 100% volatile state unchanged.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
		{ID: 4, Kind: trace.KindCreate, File: "db/000004.log", Handle: 2, Cat: "pebble-wal"},
		{ID: 5, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
		{ID: 6, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
		{ID: 7, Kind: trace.KindWrite, File: "db/000004.log", Handle: 2, Data: []byte("val1"), Len: 4},
		{ID: 8, Kind: trace.KindSyncData, File: "db/000004.log", Handle: 2},
	}
	hPreSync := candidate.StateHash(events, 7, 0)
	hPostSync := candidate.StateHash(events, 8, 0)
	if hPreSync == hPostSync {
		t.Fatalf("SyncData on dir-synced file must advance canonical 0%% durable state: both hashes are %s", hPreSync)
	}
	if candidate.StateHash(events, 7, 100) != candidate.StateHash(events, 8, 100) {
		t.Fatalf("SyncData must not alter 100%% retention state hash")
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

func TestStateHashReuseForWrite(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
		{ID: 4, Kind: trace.KindCreate, File: "db/old.wal", Handle: 2, Cat: "pebble-wal"},
		{ID: 5, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
		{ID: 6, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
		{ID: 7, Kind: trace.KindWrite, File: "db/old.wal", Handle: 2, Data: []byte("initial"), Len: 7},
		{ID: 8, Kind: trace.KindSync, File: "db/old.wal", Handle: 2},
		{ID: 9, Kind: trace.KindClose, File: "db/old.wal", Handle: 2},
		{ID: 10, Kind: trace.KindReuseForWrite, File: "db/old.wal", File2: "db/new.wal", Handle: 4, Cat: "pebble-wal"},
		{ID: 11, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
		{ID: 12, Kind: trace.KindWrite, File: "db/new.wal", Handle: 4, Data: []byte("recycled_data"), Len: 13},
		{ID: 13, Kind: trace.KindSync, File: "db/new.wal", Handle: 4},
	}

	hPreReuse := candidate.StateHash(events, 9, 0)
	hPostReuse := candidate.StateHash(events, 10, 0)
	hPostDirSync := candidate.StateHash(events, 11, 0)
	hPostWrite := candidate.StateHash(events, 12, 100)
	hPostSync := candidate.StateHash(events, 13, 0)

	if hPreReuse == hPostReuse {
		t.Fatalf("ReuseForWrite must alter namespace state")
	}
	if hPostReuse == hPostDirSync {
		t.Fatalf("Dir sync after reuse must alter 0%% durable state")
	}
	if candidate.StateHash(events, 11, 100) == hPostWrite {
		t.Fatalf("Write after reuse should change 100%% retention hash")
	}
	if hPostDirSync == hPostSync {
		t.Fatalf("Sync after reuse should change 0%% durable hash")
	}
}

func TestStateHashScopedDirectorySync(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindMkdirAll, File: "staging", Mode: 0755},
		{ID: 3, Kind: trace.KindCreate, File: "db/file1", Handle: 1},
		{ID: 4, Kind: trace.KindCreate, File: "staging/file2", Handle: 2},
		// Sync db directory only (handle 3)
		{ID: 5, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
	}

	// At cutoff 4, both db/file1 and staging/file2 are pending.
	// At cutoff 5, db/file1 should be cleared, but staging/file2 MUST remain pending.
	hCut4 := candidate.StateHash(events, 4, 0)
	hCut5 := candidate.StateHash(events, 5, 0)
	if hCut4 == hCut5 {
		t.Fatalf("sync of db must alter pending set")
	}

	// Now if we sync staging (handle 4), state hash should advance again
	events = append(events, trace.Event{ID: 6, Kind: trace.KindSync, File: "staging", Handle: 4, Dir: true})
	hCut6 := candidate.StateHash(events, 6, 0)
	if hCut5 == hCut6 {
		t.Fatalf("subsequent sync of staging directory must clear remaining pending entry")
	}
}

func TestStateHashHandleBasedDirectorySync(t *testing.T) {
	// Verifies that when KindSync on a directory has an empty File field (handle-only sync),
	// candidate.buildCutoffStates correctly resolves the directory path from the handle.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindMkdirAll, File: "staging", Mode: 0755},
		{ID: 3, Kind: trace.KindCreate, File: "db/file1", Handle: 1},
		{ID: 4, Kind: trace.KindCreate, File: "staging/file2", Handle: 2},
		{ID: 5, Kind: trace.KindOpenDir, File: "db", Handle: 10, Dir: true},
		// Sync directory via handle 10 with empty File:
		{ID: 6, Kind: trace.KindSync, File: "", Handle: 10, Dir: true},
	}

	hCut4 := candidate.StateHash(events, 4, 0)
	hCut6 := candidate.StateHash(events, 6, 0)
	if hCut4 == hCut6 {
		t.Fatalf("handle-based directory sync with empty File field must clear db pending entries")
	}

	// Staging pending op must still be present; syncing staging should advance hash again:
	events = append(events,
		trace.Event{ID: 7, Kind: trace.KindOpenDir, File: "staging", Handle: 20, Dir: true},
		trace.Event{ID: 8, Kind: trace.KindSync, File: "", Handle: 20, Dir: true},
	)
	hCut8 := candidate.StateHash(events, 8, 0)
	if hCut6 == hCut8 {
		t.Fatalf("handle-based sync of staging directory must clear staging pending entries")
	}
}

func TestZeroByteSyncedFileAndEmptyDirectoryStateHash(t *testing.T) {
	// Defect 14 & 15: 0-byte synced files and empty synced directories must produce
	// distinct state hashes at 0% retention compared to pre-creation state.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "db", Handle: 10, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: ".", Handle: 1, Dir: true}, // sync root to persist "db" dir
		{ID: 4, Kind: trace.KindCreate, File: "db/LOCK", Handle: 2},
		{ID: 5, Kind: trace.KindSync, File: "db/LOCK", Handle: 2},
		{ID: 6, Kind: trace.KindSync, File: "db", Handle: 10, Dir: true}, // sync db dir to persist "db/LOCK"
	}

	hPreCreation := candidate.StateHash(events, 0, 0)
	hDirSynced := candidate.StateHash(events, 3, 0)
	hLockSynced := candidate.StateHash(events, 6, 0)

	if hPreCreation == hDirSynced {
		t.Fatalf("empty synced directory must have distinct StateHash from pre-creation at 0%% retention")
	}
	if hDirSynced == hLockSynced {
		t.Fatalf("0-byte synced file must have distinct StateHash from state before file creation at 0%% retention")
	}
}

func TestStateHashReuseForWriteResetsSyncedAndLinkPropagates(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindSync, File: ".", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "db/file1", Handle: 2},
		{ID: 4, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 5, Kind: trace.KindSync, File: "db/file1", Handle: 2},
		// db/file1 is now a 0-byte synced file in a synced dir
		{ID: 6, Kind: trace.KindLink, File: "db/file1", File2: "db/file1_link"},
		{ID: 7, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		// Reuse db/file1 for db/file2
		{ID: 8, Kind: trace.KindReuseForWrite, File: "db/file1", File2: "db/file2", Handle: 3},
		{ID: 9, Kind: trace.KindWrite, File: "db/file2", Handle: 3, Data: []byte("dirty"), Len: 5},
	}

	hPreLink := candidate.StateHash(events, 5, 0)
	hPostLink := candidate.StateHash(events, 7, 0)
	if hPreLink == hPostLink {
		t.Fatalf("hard-linked 0-byte synced file must produce distinct StateHash at 0%% retention")
	}

	// At cutoff 8 (reuse occurred, but no new sync) and cutoff 9 (unsynced write):
	// db/file2 is NOT synced, so at 0% retention it should not have durable data.
	hPostReuse0 := candidate.StateHash(events, 8, 0)
	hPostWrite0 := candidate.StateHash(events, 9, 0)
	if hPostReuse0 != hPostWrite0 {
		t.Fatalf("unsynced write to reused file at 0%% retention should not change durable state")
	}
}

func TestGapEventIDs(t *testing.T) {
	// Defect 2.1: Events with gap IDs must not corrupt intermediate cutoff states.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/file1", Handle: 1},
		{ID: 5, Kind: trace.KindCreate, File: "db/file2", Handle: 2},
	}
	h0 := candidate.StateHash(events, 0, 0)
	h1 := candidate.StateHash(events, 1, 0)
	h2 := candidate.StateHash(events, 2, 0)
	h3 := candidate.StateHash(events, 3, 0)
	h4 := candidate.StateHash(events, 4, 0)
	h5 := candidate.StateHash(events, 5, 0)

	if h1 == h0 {
		t.Fatalf("cutoff 1 should reflect file1 creation")
	}
	if h2 != h1 || h3 != h1 || h4 != h1 {
		t.Fatalf("cutoffs 2, 3, 4 before gap event 5 must match cutoff 1 state, got h2=%s h1=%s", h2, h1)
	}
	if h5 == h1 {
		t.Fatalf("cutoff 5 should reflect file2 creation and differ from cutoff 1")
	}
}

func TestCutoffCacheCollisionDifferentFiles(t *testing.T) {
	trace1 := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/file1", Handle: 1},
	}
	trace2 := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/file2", Handle: 2},
	}
	h1 := candidate.StateHash(trace1, 1, 0)
	h2 := candidate.StateHash(trace2, 1, 0)
	if h1 == h2 {
		t.Fatalf("different files must produce different state hashes, got same: %s", h1)
	}
}

func TestCandidateLinkSharingOpenCreatedAndRootDirSync(t *testing.T) {
	t.Run("HardLinkSharesInodeLengths", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
			{ID: 3, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
			{ID: 4, Kind: trace.KindCreate, File: "db/a.sst", Handle: 2, Cat: "pebble-sst"},
			{ID: 5, Kind: trace.KindWrite, File: "db/a.sst", Handle: 2, Data: []byte("1234"), Len: 4},
			{ID: 6, Kind: trace.KindSyncData, File: "db/a.sst", Handle: 2},
			{ID: 7, Kind: trace.KindLink, File: "db/a.sst", File2: "db/b.sst"},
			{ID: 8, Kind: trace.KindWrite, File: "db/a.sst", Handle: 2, Off: 4, Data: []byte("56"), Len: 2},
			{ID: 9, Kind: trace.KindRemove, File: "db/a.sst"},
			{ID: 10, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		}
		h0AfterRemove := candidate.StateHash(events, 9, 0)
		if h0AfterRemove == "" {
			t.Fatal("empty StateHash")
		}
		// Writing to Handle 2 before Remove updates shared inode so db/b.sst at cut 9 differs between 0% and 100%.
		h100AfterRemove := candidate.StateHash(events, 9, 100)
		if h0AfterRemove == h100AfterRemove {
			t.Fatalf("expected hard-linked db/b.sst to preserve shared inode durable vs current lengths at cut 9")
		}
	})

	t.Run("RootDirSyncClearsPendingRootAndOpenCreated", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
			{ID: 3, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
			{ID: 4, Kind: trace.KindOpenReadWrite, File: "db/rw.log", Handle: 2, Created: true},
			{ID: 5, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
			{ID: 6, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
		}
		// Before root sync at cut 1, "db" is pending so excluded at 0%; after root sync at cut 3, "db" is durable.
		if candidate.StateHash(events, 1, 0) == candidate.StateHash(events, 3, 0) {
			t.Fatalf("expected root dir sync at cut 3 to promote db into durable 0%% state")
		}
		// OpenReadWrite with Created=true adds a pending dir create excluded at 0% until db dir sync at cut 6.
		if candidate.StateHash(events, 4, 0) == candidate.StateHash(events, 6, 0) {
			t.Fatalf("expected OpenReadWrite(Created=true) to register pending dir entry at cut 4")
		}
	})
}

func TestCandidateAuditFixes(t *testing.T) {
	t.Run("WriteAtOverwriteInsideCurrentLen", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
			{ID: 3, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
			{ID: 4, Kind: trace.KindCreate, File: "db/f", Handle: 2},
			{ID: 5, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
			{ID: 6, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
			{ID: 7, Kind: trace.KindWrite, File: "db/f", Handle: 2, Data: []byte("0123456789"), Len: 10},
			{ID: 8, Kind: trace.KindSyncData, File: "db/f", Handle: 2},
			{ID: 9, Kind: trace.KindWriteAt, File: "db/f", Handle: 2, Off: 2, Data: []byte("XX"), Len: 2},
			{ID: 10, Kind: trace.KindSyncData, File: "db/f", Handle: 2},
		}

		// Before vs after in-place WriteAt: 50% and 100% must change; 0% stays unchanged until SyncData.
		if candidate.StateHash(events, 8, 50) == candidate.StateHash(events, 9, 50) {
			t.Fatal("in-place WriteAt must change 50% StateHash")
		}
		if candidate.StateHash(events, 8, 100) == candidate.StateHash(events, 9, 100) {
			t.Fatal("in-place WriteAt must change 100% StateHash")
		}
		if candidate.StateHash(events, 8, 0) != candidate.StateHash(events, 9, 0) {
			t.Fatal("unsynced in-place WriteAt must not alter 0% StateHash before SyncData")
		}
		if candidate.StateHash(events, 8, 0) == candidate.StateHash(events, 10, 0) {
			t.Fatal("SyncData after in-place WriteAt must advance 0% StateHash")
		}
		if candidate.StateHash(events, 9, 100) != candidate.StateHash(events, 10, 100) {
			t.Fatal("SyncData after WriteAt must not alter 100% StateHash")
		}
	})

	t.Run("ReadOnlyOpenAndOpenDirDoNotMutateState", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
			{ID: 3, Kind: trace.KindOpen, File: "db/existing", Handle: 2},
			{ID: 4, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
			{ID: 5, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
		}
		for _, p := range []int{0, 50, 100} {
			h1 := candidate.StateHash(events, 1, p)
			h2 := candidate.StateHash(events, 2, p)
			h3 := candidate.StateHash(events, 3, p)
			h4 := candidate.StateHash(events, 4, p)
			if h1 != h2 || h1 != h3 || h1 != h4 {
				t.Fatalf("percent %d: read-only Open/OpenDir altered StateHash: %s vs %s vs %s vs %s", p, h1, h2, h3, h4)
			}
		}
	})

	t.Run("DurableRemovePersistsAtZeroUntilDirSync", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
			{ID: 3, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
			{ID: 4, Kind: trace.KindCreate, File: "db/a", Handle: 2},
			{ID: 5, Kind: trace.KindWrite, File: "db/a", Handle: 2, Data: []byte("hello"), Len: 5},
			{ID: 6, Kind: trace.KindSyncData, File: "db/a", Handle: 2},
			{ID: 7, Kind: trace.KindClose, File: "db/a", Handle: 2},
			{ID: 8, Kind: trace.KindOpenDir, File: "db", Handle: 3, Dir: true},
			{ID: 9, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
			{ID: 10, Kind: trace.KindRemove, File: "db/a"},
			{ID: 11, Kind: trace.KindSync, File: "db", Handle: 3, Dir: true},
		}
		// At 100%, Remove immediately removes db/a (cut 10 == cut 11).
		if candidate.StateHash(events, 9, 100) == candidate.StateHash(events, 10, 100) {
			t.Fatal("Remove must immediately alter 100% StateHash")
		}
		if candidate.StateHash(events, 10, 100) != candidate.StateHash(events, 11, 100) {
			t.Fatal("dir sync after Remove must not alter 100% StateHash")
		}
		// At 0%, dir sync at cut 11 removes db/a from durable set, differing from cut 10.
		if candidate.StateHash(events, 10, 0) == candidate.StateHash(events, 11, 0) {
			t.Fatal("dir sync after Remove of durable file must alter 0% StateHash")
		}
	})

	t.Run("PartialRetentionPreservesDistinctSeeds", func(t *testing.T) {
		events := opTrace()
		c1 := candidate.Candidate{Event: 4, Percent: 50, Seed: 1}
		c2 := candidate.Candidate{Event: 4, Percent: 50, Seed: 2}

		if got := candidate.CollapseEquivalenceStates([]candidate.Candidate{c1, c2}, events); len(got) != 2 {
			t.Fatalf("with trace: expected 2 distinct 50%% seed candidates, got %d (%+v)", len(got), got)
		}
		if got := candidate.CollapseEquivalenceStates([]candidate.Candidate{c1, c2}); len(got) != 2 {
			t.Fatalf("without trace: expected 2 distinct 50%% seed candidates, got %d (%+v)", len(got), got)
		}
		if cls := candidate.EquivalenceClasses([]candidate.Candidate{c1, c2}, events); len(cls) != 2 {
			t.Fatalf("EquivalenceClasses with trace: expected 2 classes, got %d", len(cls))
		}
		if cls := candidate.EquivalenceClasses([]candidate.Candidate{c1, c2}); len(cls) != 2 {
			t.Fatalf("EquivalenceClasses without trace: expected 2 classes, got %d", len(cls))
		}
	})

	t.Run("OutOfBoundsAndGapIDRepresentative", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "db/f1", Handle: 1},
			{ID: 4, Kind: trace.KindWrite, File: "db/f1", Handle: 1, Data: []byte("ab"), Len: 2},
			{ID: 5, Kind: trace.KindSyncData, File: "db/f1", Handle: 1},
		}
		if h := candidate.StateHash(events, 999, 0); h != "" {
			t.Fatalf("expected empty StateHash for out-of-range cut 999, got %q", h)
		}
		valid := candidate.Candidate{Event: 4, Percent: 0, Seed: 1}
		oob1 := candidate.Candidate{Event: 99, Percent: 0, Seed: 1}
		oob2 := candidate.Candidate{Event: 100, Percent: 0, Seed: 1}
		collapsed := candidate.CollapseEquivalenceStates([]candidate.Candidate{valid, oob1, oob2}, events)
		if len(collapsed) != 3 {
			t.Fatalf("expected out-of-bounds cutoffs not to collapse with valid or each other, got %+v", collapsed)
		}
		classes := candidate.EquivalenceClasses([]candidate.Candidate{valid, oob1, oob2}, events)
		if len(classes) != 3 {
			t.Fatalf("expected 3 equivalence classes, got %d", len(classes))
		}

		// Gap-ID representative selection: cut 4 is BeforeBoundary (ID 5 is SyncData) and a mutation.
		// Cut 2 shares state with cut 1; cut 4 is distinct or boundary-preferred when states tie.
		gapPair := []candidate.Candidate{
			{Event: 1, Percent: 100, Seed: 1},
			{Event: 2, Percent: 100, Seed: 1},
		}
		rep := candidate.CollapseEquivalenceStates(gapPair, events)
		if len(rep) != 1 || rep[0].Event != 1 {
			t.Fatalf("expected gap-ID mutation cut 1 to beat non-mutation cut 2, got %+v", rep)
		}
	})

	t.Run("RenameUpdatesPendingDirAndRemoveAllRootHasNoSpuriousPending", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "old", Mode: 0755},
			{ID: 2, Kind: trace.KindCreate, File: "old/sub", Handle: 1},
			{ID: 3, Kind: trace.KindRename, File: "old", File2: "new"},
			{ID: 4, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
			{ID: 5, Kind: trace.KindSync, File: "", Handle: 2, Dir: true},
			{ID: 6, Kind: trace.KindOpenDir, File: "new", Handle: 3, Dir: true},
			{ID: 7, Kind: trace.KindSync, File: "new", Handle: 3, Dir: true},
		}
		if candidate.StateHash(events, 5, 0) == candidate.StateHash(events, 7, 0) {
			t.Fatal("Sync(new) after Rename(old->new) must clear moved child pending entry and persist new/sub at 0%")
		}

		rootEvents := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindRemoveAll, File: ""},
		}
		if h0, h2 := candidate.StateHash(rootEvents, 0, 0), candidate.StateHash(rootEvents, 2, 0); h0 != h2 {
			t.Fatalf("RemoveAll on root should return to empty pre-trace state at 0%%: %s vs %s", h0, h2)
		}
	})
}
