package graph

import (
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// step applies events one at a time and returns the builder for inspection.
func step(test *testing.T, events []trace.Event) *builder {
	test.Helper()
	if err := trace.Validate(events); err != nil {
		test.Fatal(err)
	}
	b := newBuilder()
	for _, event := range events {
		if _, err := b.apply(event); err != nil {
			test.Fatal(err)
		}
	}
	return b
}

func (b *builder) dirtyOf(name string) []interval { return b.resources[b.names[name]].dirty }

func (b *builder) pendingPaths() []string {
	var paths []string
	for _, entry := range b.pending {
		paths = append(paths, entry.kind+":"+entry.path)
	}
	return paths
}

func TestDirtyAndPendingLifecycle(test *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "db/000001.log", Handle: 2, Cat: "pebble-wal"},
		{ID: 4, Kind: trace.KindWrite, File: "db/000001.log", Handle: 2, Data: []byte("abcd"), Len: 4},
		{ID: 5, Kind: trace.KindWriteAt, File: "db/000001.log", Handle: 2, Off: 10, Data: []byte("xy"), Len: 2},
		{ID: 6, Kind: trace.KindSyncData, File: "db/000001.log", Handle: 2},
		{ID: 7, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
	}
	b := step(test, events[:5])
	if got := b.dirtyOf("db/000001.log"); len(got) != 2 || got[0] != (interval{0, 4}) || got[1] != (interval{10, 12}) {
		test.Fatalf("dirty ranges before sync: %v", got)
	}
	b = step(test, events[:6])
	if got := b.dirtyOf("db/000001.log"); len(got) != 0 {
		test.Fatalf("file sync left dirty ranges: %v", got)
	}
	// The directory entry survives a file sync. The mkdir of db is pending in
	// the root directory, which this trace never syncs.
	if got := b.pendingPaths(); len(got) != 2 || got[0] != "mkdir:db" || got[1] != "create:db/000001.log" {
		test.Fatalf("pending entries after file sync: %v", got)
	}
	b = step(test, events)
	for _, entry := range b.pending {
		if entry.dir == "db" {
			test.Fatalf("directory sync left entry %+v pending", entry)
		}
	}
	if got := b.pendingPaths(); len(got) != 1 || got[0] != "mkdir:db" {
		test.Fatalf("root entry must stay pending until the root is synced: %v", got)
	}
}

func TestIntervalMerge(test *testing.T) {
	var list []interval
	for _, span := range []interval{{10, 12}, {0, 4}, {4, 6}, {20, 30}, {11, 21}, {50, 50}} {
		list = addInterval(list, span)
	}
	if len(list) != 2 || list[0] != (interval{0, 6}) || list[1] != (interval{10, 30}) {
		test.Fatalf("merged intervals: %v", list)
	}
}

func TestResourceIdentity(test *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "a", Handle: 1, Cat: "pebble-wal"},
		{ID: 2, Kind: trace.KindOpen, File: "a", Handle: 2},
		{ID: 3, Kind: trace.KindOpenReadWrite, File: "a", Handle: 3},
		{ID: 4, Kind: trace.KindOpenReadWrite, File: "b", Handle: 4, Created: true},
		{ID: 5, Kind: trace.KindLink, File: "a", File2: "c"},
		{ID: 6, Kind: trace.KindRename, File: "c", File2: "d"},
		{ID: 7, Kind: trace.KindRemove, File: "a"},
		{ID: 8, Kind: trace.KindWrite, File: "a", Handle: 1, Data: []byte("x"), Len: 1},
		{ID: 9, Kind: trace.KindClose, File: "a", Handle: 1},
		{ID: 10, Kind: trace.KindCreate, File: "d", Handle: 5},
	}
	b := step(test, events[:9])
	first := b.handles[2]
	if b.handles[3] != first {
		test.Fatal("multiple opens of one file must share its resource")
	}
	if b.handles[4] == first {
		test.Fatal("OpenReadWrite of a new file must create a new resource")
	}
	r := b.resources[first]
	if !r.aliases["d"] || r.aliases["a"] || r.aliases["c"] || len(r.aliases) != 1 {
		test.Fatalf("aliases after link, rename and unlink: %v", r.aliases)
	}
	if len(r.dirty) != 1 {
		test.Fatal("write through a handle that survived unlink must dirty the same resource")
	}
	b = step(test, events)
	if b.names["d"] == first || b.resources[first].aliases["d"] {
		test.Fatal("replacement Create must make a distinct resource and remove the old alias")
	}
	if !b.resources[first].categories["pebble-wal"] || len(b.resources[b.names["d"]].categories) != 0 {
		test.Fatal("categories must come only from each resource's own opening events")
	}
}

func TestFailedNamespaceIsUnknownAndInventsNothing(test *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "a", Handle: 1},
		{ID: 2, Kind: trace.KindClose, File: "a", Handle: 1},
		{ID: 3, Kind: trace.KindRename, File: "a", File2: "missing/b", Err: "no such directory"},
	}
	b := step(test, events)
	if len(b.unknown) != 1 || b.unknown[0] != 3 {
		test.Fatalf("failed rename not recorded as unknown effect: %v", b.unknown)
	}
	if _, invented := b.names["missing"]; invented {
		test.Fatal("failed rename invented a directory")
	}
}

func TestCrossDirectoryRenameRecordsBothParents(test *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "x", Mode: 0755},
		{ID: 2, Kind: trace.KindMkdirAll, File: "y", Mode: 0755},
		{ID: 3, Kind: trace.KindCreate, File: "x/f", Handle: 1},
		{ID: 4, Kind: trace.KindRename, File: "x/f", File2: "y/f"},
	}
	b := step(test, events)
	dirs := map[string]bool{}
	for _, entry := range b.pending {
		if entry.event == 4 {
			dirs[entry.dir] = true
		}
	}
	if !dirs["x"] || !dirs["y"] {
		test.Fatalf("cross-directory rename parents: %v", dirs)
	}
}
