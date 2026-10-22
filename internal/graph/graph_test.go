package graph_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// walTrace is a small, realistic sequence: a client set writes the WAL,
// syncs it, then a flush creates a table that a compaction later consumes.
func walTrace() []trace.Event {
	return []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "db/000002.log", Handle: 2, Cat: "pebble-wal"},
		{ID: 4, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 5, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 6, Kind: trace.KindWrite, File: "db/000002.log", Handle: 2, Data: []byte("rec1"), Len: 4},
		{ID: 7, Kind: trace.KindSyncData, File: "db/000002.log", Handle: 2},
		{ID: 8, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 9, Kind: trace.KindCreate, File: "db/000004.sst", Handle: 3, Cat: "pebble-memtable-flush"},
		{ID: 10, Kind: trace.KindWrite, File: "db/000004.sst", Handle: 3, Data: []byte("table"), Len: 5},
		{ID: 11, Kind: trace.KindSyncData, File: "db/000004.sst", Handle: 3},
		{ID: 12, Kind: trace.KindClose, File: "db/000004.sst", Handle: 3},
		{ID: 13, Kind: trace.KindEngineEvent, Note: "FlushEnd", Job: 1, Inputs: []string{"db/000002.log"}, Outputs: []string{"db/000004.sst"}},
		{ID: 14, Kind: trace.KindSyncTo, File: "db/000002.log", Handle: 2, Size: 4},
		{ID: 15, Kind: trace.KindRename, File: "db/000004.sst", File2: "db/000005.sst"},
		{ID: 16, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
	}
}

func build(test *testing.T, events []trace.Event) *graph.View {
	test.Helper()
	view, err := graph.Build(events)
	if err != nil {
		test.Fatal(err)
	}
	return view
}

func TestEveryCutoffHasFeatures(test *testing.T) {
	events := walTrace()
	view := build(test, events)
	if len(view.Cuts) != len(events)+1 {
		test.Fatalf("features for %d cutoffs, want %d", len(view.Cuts), len(events)+1)
	}
	for cut := uint64(0); cut <= uint64(len(events)); cut++ {
		features, ok := view.Cuts[cut]
		if !ok || features.StateKey == "" || features.OpKind == "" || features.Categories == nil {
			test.Fatalf("cutoff %d has incomplete features: %+v", cut, features)
		}
	}
	zero := view.Cuts[0]
	if zero.Desc != 0 || zero.CrossFile != 0 || zero.Semantic != 0 || zero.OpKind != graph.Background || len(zero.Categories) != 0 {
		test.Fatalf("pretrace cutoff: %+v", zero)
	}
}

func TestEmptyStateKeyIsStable(test *testing.T) {
	view := build(test, walTrace())
	empty := build(test, []trace.Event{}).Cuts[0].StateKey
	if view.Cuts[0].StateKey != empty {
		test.Fatal("empty state key depends on the trace")
	}
	if len(empty) != 64 {
		test.Fatalf("state key is not a hex SHA-256: %q", empty)
	}
}

func TestStateKeyTracksSyncs(test *testing.T) {
	view := build(test, walTrace())
	cuts := view.Cuts
	if cuts[6].StateKey == cuts[7].StateKey {
		test.Fatal("SyncData did not change the state")
	}
	if cuts[5].StateKey != cuts[7].StateKey {
		test.Fatal("write then SyncData must return to the pre-write state")
	}
	if cuts[13].StateKey != cuts[14].StateKey {
		test.Fatal("SyncTo(false) changed the persistence state")
	}
	if !reflect.DeepEqual(cuts[6].Categories, []string{"pebble-wal"}) {
		test.Fatalf("dirty WAL categories: %v", cuts[6].Categories)
	}
	if !reflect.DeepEqual(cuts[9].Categories, []string{"pebble-memtable-flush"}) {
		test.Fatalf("pending table categories: %v", cuts[9].Categories)
	}
	// After the final directory sync only the root's mkdir entry for db
	// remains pending, exactly as after the first directory sync.
	if len(cuts[16].Categories) != 0 || cuts[16].StateKey != cuts[4].StateKey || cuts[16].StateKey == cuts[0].StateKey {
		test.Fatalf("directory sync must clear pending entries in db only: %+v", cuts[16])
	}
}

func TestBoundaryLabels(test *testing.T) {
	view := build(test, walTrace())
	for cut, want := range map[uint64]bool{3: true, 6: true, 10: true, 14: true, 15: true, 13: false, 7: false} {
		if view.Cuts[cut].BeforeBoundary != want {
			test.Fatalf("BeforeBoundary at %d = %v, want %v", cut, !want, want)
		}
	}
	if view.IsVertex(14) {
		test.Fatal("SyncTo(false) became a persistence vertex")
	}
}

func TestOperationLabels(test *testing.T) {
	view := build(test, walTrace())
	for cut, want := range map[uint64]string{0: graph.Background, 4: graph.Background, 5: "set", 7: "set", 8: "set", 9: graph.Background} {
		if got := view.Cuts[cut].OpKind; got != want {
			test.Fatalf("OpKind at %d = %q, want %q", cut, got, want)
		}
	}
	if view.Cuts[6].Semantic != 1 {
		test.Fatal("write inside a set interval is not semantic")
	}
}

func TestEdgesAndReachability(test *testing.T) {
	view := build(test, walTrace())
	for id, successors := range map[uint64][]uint64{
		1:  {3},    // mkdir db -> create in db (directory namespace order)
		3:  {4, 6}, // create log -> dir sync of db, first write
		6:  {7},    // write -> sync on the same resource
		7:  {11},   // flush input (WAL) -> latest vertex of its output table
		11: {15},   // table sync -> rename of the table
		15: {16},   // rename -> directory sync
	} {
		if got := view.Successors(id); !reflect.DeepEqual(got, successors) {
			test.Fatalf("successors of %d = %v, want %v", id, got, successors)
		}
	}
	for _, event := range view.Events {
		for _, successor := range view.Successors(event.ID) {
			if successor <= event.ID || !view.IsVertex(successor) {
				test.Fatalf("edge %d -> %d is not forward between vertices", event.ID, successor)
			}
		}
	}
	// From 7 (WAL sync): 11, 15 (table) and 16 (directory) are reachable,
	// all on resources other than the WAL.
	if got := view.Cuts[7]; got.Desc != 3 || got.CrossFile != 3 {
		test.Fatalf("reachability from 7: desc=%d cross=%d", got.Desc, got.CrossFile)
	}
	// Cutoff 8 (OpEnd) anchors at vertex 7, the closest earlier vertex.
	if view.Cuts[8].Desc != view.Cuts[7].Desc {
		test.Fatal("annotation cutoff did not use the preceding vertex as anchor")
	}
	if got := view.Cuts[16]; got.Desc != 0 || got.CrossFile != 0 {
		test.Fatalf("last vertex has descendants: %+v", got)
	}
}

func TestDeterministicAndImmutable(test *testing.T) {
	events := walTrace()
	original := trace.Clone(events)
	first := build(test, events)
	events[5].Data[0] = 'X'
	events[12].Inputs[0] = "changed"
	if string(first.Events[5].Data) != "rec1" || first.Events[12].Inputs[0] != "db/000002.log" {
		test.Fatal("view aliases caller payloads")
	}
	second := build(test, original)
	if !reflect.DeepEqual(first.Cuts, second.Cuts) {
		test.Fatal("repeated builds disagree on features")
	}
	for _, event := range original {
		if !reflect.DeepEqual(first.Successors(event.ID), second.Successors(event.ID)) {
			test.Fatal("repeated builds disagree on edges")
		}
	}
}

func TestRejectsUnsupportedTraces(test *testing.T) {
	failedSync := walTrace()
	failedSync[6].Err = "injected"
	if _, err := graph.Build(failedSync); err == nil {
		test.Fatal("failed sync accepted")
	}
	invalid := walTrace()
	invalid[3].ID = 40
	if _, err := graph.Build(invalid); err == nil {
		test.Fatal("invalid trace accepted")
	}
}

func TestContractTraceBuilds(test *testing.T) {
	// The shared contract example: handles, aliases, a failed later rename
	// is not included, a lock and a compaction annotation.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "db/log", Handle: 2, Cat: "pebble-wal"},
		{ID: 4, Kind: trace.KindWrite, File: "db/log", Handle: 2, Data: []byte("abcd"), Len: 4},
		{ID: 5, Kind: trace.KindOpenReadWrite, File: "db/log", Handle: 3},
		{ID: 6, Kind: trace.KindRead, File: "db/log", Handle: 3, Size: 2, Len: 2},
		{ID: 7, Kind: trace.KindWriteAt, File: "db/log", Handle: 3, Off: 1, Data: []byte("Z"), Len: 1},
		{ID: 8, Kind: trace.KindWrite, File: "db/log", Handle: 3, Off: 2, Data: []byte("Q"), Len: 1},
		{ID: 9, Kind: trace.KindSyncData, File: "db/log", Handle: 2},
		{ID: 10, Kind: trace.KindSyncTo, File: "db/log", Handle: 2, Size: 4},
		{ID: 11, Kind: trace.KindLink, File: "db/log", File2: "db/link"},
		{ID: 12, Kind: trace.KindRename, File: "db/log", File2: "db/new"},
		{ID: 13, Kind: trace.KindRemove, File: "db/new"},
		{ID: 14, Kind: trace.KindWriteAt, File: "db/log", Handle: 3, Data: []byte("U"), Len: 1},
		{ID: 15, Kind: trace.KindClose, File: "db/log", Handle: 2},
		{ID: 16, Kind: trace.KindClose, File: "db/log", Handle: 3},
		{ID: 17, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 18, Kind: trace.KindClose, File: "db", Handle: 1, Dir: true},
		{ID: 19, Kind: trace.KindLock, File: "db/LOCK", Handle: 4},
		{ID: 20, Kind: trace.KindUnlock, File: "db/LOCK", Handle: 4},
		{ID: 21, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 22, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 23, Kind: trace.KindEngineEvent, Note: "CompactionEnd", Job: 1, Inputs: []string{"db/link"}, Outputs: []string{"db/004.sst"}},
	}
	view := build(test, events)
	// Event 14 writes through handle 3 after the file lost its original
	// name: it must stay on the same resource chain as event 9.
	found := false
	for _, successor := range view.Successors(9) {
		found = found || successor == 11
	}
	if !found {
		test.Fatalf("link of the synced log is not ordered after its sync: %v", view.Successors(9))
	}
	if view.Cuts[22].OpKind != "set" || view.Cuts[23].OpKind != graph.Background {
		test.Fatal("OpEnd cutoff must carry the completed kind")
	}
}


func TestWriteDOT(t *testing.T) {
	events := walTrace()
	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}

	var buf1, buf2 bytes.Buffer
	if err := view.WriteDOT(&buf1); err != nil {
		t.Fatalf("WriteDOT 1: %v", err)
	}
	if err := view.WriteDOT(&buf2); err != nil {
		t.Fatalf("WriteDOT 2: %v", err)
	}

	out1 := buf1.String()
	out2 := buf2.String()

	if out1 != out2 {
		t.Error("WriteDOT output is not deterministic")
	}

	if !strings.HasPrefix(out1, "digraph G {") {
		t.Errorf("expected digraph header, got:\n%s", out1[:50])
	}
	if !strings.HasSuffix(strings.TrimSpace(out1), "}") {
		t.Errorf("expected closing brace, got:\n%s", out1[len(out1)-50:])
	}
	if !strings.Contains(out1, "->") {
		t.Errorf("expected edges in DOT output, got none:\n%s", out1)
	}

	var nilView *graph.View
	var nilBuf bytes.Buffer
	if err := nilView.WriteDOT(&nilBuf); err == nil {
		t.Error("expected error on nil view WriteDOT, got nil")
	}
}
