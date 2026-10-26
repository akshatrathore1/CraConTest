package graph_test

import (
	"bytes"
	"math"
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

func TestDeferredEngineEdgesAndUnknownPaths(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindCreate, File: "db/a", Handle: 1, Cat: "pebble-wal"},
		{ID: 3, Kind: trace.KindCreate, File: "db/b", Handle: 2, Cat: "pebble-memtable-flush"},
		{ID: 4, Kind: trace.KindWrite, File: "db/a", Handle: 1, Data: []byte("val"), Len: 3},
		{ID: 5, Kind: trace.KindSyncData, File: "db/a", Handle: 1},
		// EngineEvent where output "db/b" has no later vertex than producer (event 5).
		// Also includes unknown paths in Inputs and Outputs to verify safe omission.
		{ID: 6, Kind: trace.KindEngineEvent, Note: "FlushEnd", Inputs: []string{"db/a", "nonexistent/in"}, Outputs: []string{"db/b", "nonexistent/out"}},
		// Subsequent write and sync on "db/b" must resolve the deferred edge from event 5 to event 7.
		{ID: 7, Kind: trace.KindWrite, File: "db/b", Handle: 2, Data: []byte("out"), Len: 3},
		{ID: 8, Kind: trace.KindSyncData, File: "db/b", Handle: 2},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}

	// Verify deferred edge: producer 5 -> consumer 7
	successors5 := view.Successors(5)
	found := false
	for _, succ := range successors5 {
		if succ == 7 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected deferred edge 5 -> 7, got successors of 5: %v", successors5)
	}

	// Reachability from vertex 5 (SyncData on a) should reach 7 and 8 on resource b (CrossFile=2)
	cut5 := view.Cuts[5]
	if cut5.Desc != 2 || cut5.CrossFile != 2 {
		t.Errorf("at cut 5: Desc=%d (want 2), CrossFile=%d (want 2)", cut5.Desc, cut5.CrossFile)
	}

	// At cut 6 (EngineEvent), anchor is 5, and named <= cut is true for resource a and b
	cut6 := view.Cuts[6]
	if cut6.Semantic != 1 {
		t.Errorf("at cut 6: Semantic=%d, want 1", cut6.Semantic)
	}
}

func TestRemoveAllPendingLifecycle(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "dir/sub", Mode: 0755},
		{ID: 2, Kind: trace.KindCreate, File: "dir/sub/f1", Handle: 1},
		{ID: 3, Kind: trace.KindCreate, File: "dir/sub/f2", Handle: 2},
		{ID: 4, Kind: trace.KindRemoveAll, File: "dir/sub"},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}

	cut4 := view.Cuts[4]
	if cut4.StateKey == "" {
		t.Fatal("cut 4 has empty StateKey")
	}
}

func TestBarrierProximityCalculation(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/a", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "db/a", Handle: 1, Len: 10, Data: []byte("0123456789")},
		{ID: 3, Kind: trace.KindWrite, File: "db/a", Handle: 1, Off: 10, Len: 10, Data: []byte("0123456789")},
		{ID: 4, Kind: trace.KindSyncData, File: "db/a", Handle: 1},
		{ID: 5, Kind: trace.KindWrite, File: "db/a", Handle: 1, Off: 20, Len: 5, Data: []byte("hello")},
		{ID: 6, Kind: trace.KindSync, File: "db/a", Handle: 1},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}

	// At cut 3 (immediately before barrier event 4):
	// delta = (4 - 1) - 3 = 0 -> proximity = 1.0, nextBarrier = 4
	c3 := view.Cuts[3]
	if c3.NextBarrier != 4 {
		t.Errorf("cut 3: NextBarrier=%d, want 4", c3.NextBarrier)
	}
	if math.Abs(c3.BarrierProximity-1.0) > 1e-9 {
		t.Errorf("cut 3: BarrierProximity=%.4f, want 1.0", c3.BarrierProximity)
	}

	// At cut 2 (1 event before cut 3):
	// delta = (4 - 1) - 2 = 1 -> proximity = 1 / (1 + 1) = 0.5, nextBarrier = 4
	c2 := view.Cuts[2]
	if c2.NextBarrier != 4 {
		t.Errorf("cut 2: NextBarrier=%d, want 4", c2.NextBarrier)
	}
	if math.Abs(c2.BarrierProximity-0.5) > 1e-9 {
		t.Errorf("cut 2: BarrierProximity=%.4f, want 0.5", c2.BarrierProximity)
	}

	// At cut 5 (immediately before barrier event 6):
	// delta = (6 - 1) - 5 = 0 -> proximity = 1.0, nextBarrier = 6
	c5 := view.Cuts[5]
	if c5.NextBarrier != 6 {
		t.Errorf("cut 5: NextBarrier=%d, want 6", c5.NextBarrier)
	}
	if math.Abs(c5.BarrierProximity-1.0) > 1e-9 {
		t.Errorf("cut 5: BarrierProximity=%.4f, want 1.0", c5.BarrierProximity)
	}

	// At cut 6 (barrier already happened, no more barriers in trace):
	// proximity = 0.0, nextBarrier = 0
	c6 := view.Cuts[6]
	if c6.NextBarrier != 0 {
		t.Errorf("cut 6: NextBarrier=%d, want 0", c6.NextBarrier)
	}
	if c6.BarrierProximity != 0.0 {
		t.Errorf("cut 6: BarrierProximity=%.4f, want 0.0", c6.BarrierProximity)
	}
}

func TestFiveTierSemanticClassification(t *testing.T) {
	// Verify raw tier weights
	if graph.TierWeight(graph.TierManifest) != 10.0 {
		t.Errorf("TierManifest weight != 10.0")
	}
	if graph.TierWeight(graph.TierWAL) != 8.0 {
		t.Errorf("TierWAL weight != 8.0")
	}
	if graph.TierWeight(graph.TierDir) != 6.0 {
		t.Errorf("TierDir weight != 6.0")
	}
	if graph.TierWeight(graph.TierFlush) != 2.0 {
		t.Errorf("TierFlush weight != 2.0")
	}
	if graph.TierWeight(graph.TierData) != 0.0 {
		t.Errorf("TierData weight != 0.0")
	}

	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 1, Len: 10, Data: []byte("0123456789")},
		{ID: 3, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 1},
		{ID: 4, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 2},
		{ID: 5, Kind: trace.KindWrite, File: "db/000002.log", Handle: 2, Len: 10, Data: []byte("0123456789")},
		{ID: 6, Kind: trace.KindSyncData, File: "db/000002.log", Handle: 2},
		{ID: 7, Kind: trace.KindMkdirAll, File: "db/dir", Mode: 0755},
		{ID: 8, Kind: trace.KindCreate, File: "db/000003.sst", Cat: "pebble-memtable-flush", Handle: 3},
		{ID: 9, Kind: trace.KindWrite, File: "db/000003.sst", Handle: 3, Len: 10, Data: []byte("0123456789")},
		{ID: 10, Kind: trace.KindSyncData, File: "db/000003.sst", Handle: 3},
		{ID: 11, Kind: trace.KindCreate, File: "db/000004.sst", Cat: "pebble-compaction", Handle: 4},
		{ID: 12, Kind: trace.KindWrite, File: "db/000004.sst", Handle: 4, Len: 10, Data: []byte("0123456789")},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}

	// Cut 2: Manifest write -> Tier 4 (10.0)
	c2 := view.Cuts[2]
	if c2.SemanticTier != graph.TierManifest || c2.SemanticWeight != 10.0 {
		t.Errorf("cut 2 (manifest): got tier %d (weight %.1f), want Tier 4 (10.0)", c2.SemanticTier, c2.SemanticWeight)
	}

	// Cut 5: WAL write -> Tier 3 (8.0)
	c5 := view.Cuts[5]
	if c5.SemanticTier != graph.TierWAL || c5.SemanticWeight != 8.0 {
		t.Errorf("cut 5 (WAL): got tier %d (weight %.1f), want Tier 3 (8.0)", c5.SemanticTier, c5.SemanticWeight)
	}

	// Cut 7: MkdirAll -> Tier 2 (6.0)
	c7 := view.Cuts[7]
	if c7.SemanticTier != graph.TierDir || c7.SemanticWeight != 6.0 {
		t.Errorf("cut 7 (dir): got tier %d (weight %.1f), want Tier 2 (6.0)", c7.SemanticTier, c7.SemanticWeight)
	}

	// Cut 9: Memtable flush -> Tier 1 (2.0)
	c9 := view.Cuts[9]
	if c9.SemanticTier != graph.TierFlush || c9.SemanticWeight != 2.0 {
		t.Errorf("cut 9 (flush): got tier %d (weight %.1f), want Tier 1 (2.0)", c9.SemanticTier, c9.SemanticWeight)
	}

	// Cut 12: Compaction write -> Tier 0 (0.0)
	c12 := view.Cuts[12]
	if c12.SemanticTier != graph.TierData || c12.SemanticWeight != 0.0 {
		t.Errorf("cut 12 (compaction): got tier %d (weight %.1f), want Tier 0 (0.0)", c12.SemanticTier, c12.SemanticWeight)
	}
}

func TestReverseRiskHorizonComputation(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "db/000001.log", Handle: 1, Len: 100, Data: make([]byte, 100)},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 6, Kind: trace.KindWrite, File: "db/000001.log", Handle: 1, Off: 100, Len: 200, Data: make([]byte, 200)},
		{ID: 7, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},
		{ID: 8, Kind: trace.KindSyncData, File: "db/000001.log", Handle: 1},
	}

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build: %v", err)
	}

	// At cut 0: dirty bytes = 0, risk = 0
	if view.Cuts[0].DirtyBytes != 0 || view.Cuts[0].RiskScore != 0.0 {
		t.Errorf("cut 0: dirty=%d, risk=%.2f, want 0, 0.0", view.Cuts[0].DirtyBytes, view.Cuts[0].RiskScore)
	}

	// At cut 3: 100 bytes written to WAL, in-flight set operation
	c3 := view.Cuts[3]
	if c3.DirtyBytes != 100 {
		t.Errorf("cut 3: DirtyBytes=%d, want 100", c3.DirtyBytes)
	}
	if c3.RiskScore <= 0 {
		t.Errorf("cut 3: RiskScore=%.2f, want > 0", c3.RiskScore)
	}

	// At cut 7: 300 bytes total dirty in WAL, 2 completed client sync ops uncommitted
	c7 := view.Cuts[7]
	if c7.DirtyBytes != 300 {
		t.Errorf("cut 7: DirtyBytes=%d, want 300", c7.DirtyBytes)
	}
	if c7.UncommittedSyncOps != 2 {
		t.Errorf("cut 7: UncommittedSyncOps=%d, want 2", c7.UncommittedSyncOps)
	}
	if c7.RiskScore <= c3.RiskScore {
		t.Errorf("cut 7 RiskScore (%.2f) should exceed cut 3 RiskScore (%.2f)", c7.RiskScore, c3.RiskScore)
	}

	// At cut 8: SyncData executed -> DirtyBytes drops to 0, UncommittedSyncOps drops to 0
	c8 := view.Cuts[8]
	if c8.DirtyBytes != 0 {
		t.Errorf("cut 8: DirtyBytes=%d after sync, want 0", c8.DirtyBytes)
	}
	if c8.UncommittedSyncOps != 0 {
		t.Errorf("cut 8: UncommittedSyncOps=%d after sync, want 0", c8.UncommittedSyncOps)
	}
	if c8.RiskScore >= c7.RiskScore {
		t.Errorf("cut 8 RiskScore (%.2f) should be lower than pre-sync cut 7 RiskScore (%.2f)", c8.RiskScore, c7.RiskScore)
	}
}
