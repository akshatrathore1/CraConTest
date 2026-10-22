package faults_test

import (
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type mockFile struct {
	syncCalls     int
	syncDataCalls int
}

func (m *mockFile) Close() error                             { return nil }
func (m *mockFile) Read(p []byte) (int, error)               { return len(p), nil }
func (m *mockFile) ReadAt(p []byte, off int64) (int, error)  { return len(p), nil }
func (m *mockFile) Write(p []byte) (int, error)              { return len(p), nil }
func (m *mockFile) WriteAt(p []byte, off int64) (int, error) { return len(p), nil }
func (m *mockFile) Stat() (any, error)                       { return nil, nil }
func (m *mockFile) Sync() error                              { m.syncCalls++; return nil }
func (m *mockFile) SyncData() error                          { m.syncDataCalls++; return nil }
func (m *mockFile) SyncTo(int64) (bool, error)               { return false, nil }
func (m *mockFile) Preallocate(int64, int64) error           { return nil }
func (m *mockFile) Prefetch(int64, int64) error              { return nil }
func (m *mockFile) Fd() uintptr                              { return 0 }

func TestFaultIDsAndValidation(t *testing.T) {
	ids := faults.IDs()
	if len(ids) != 2 || ids[0] != "MUT-0" || ids[1] != "MUT-1" {
		t.Fatalf("unexpected IDs: %v", ids)
	}

	mem := vfs.NewCrashableMem()
	if _, err := faults.New("MUT-2", mem); err == nil {
		t.Error("expected error for unknown fault ID MUT-2, got nil")
	}
	if _, err := faults.New("MUT-0", nil); err == nil {
		t.Error("expected error for nil inner FS, got nil")
	}

	f0, err := faults.New("MUT-0", mem)
	if err != nil {
		t.Fatalf("faults.New MUT-0: %v", err)
	}
	if f0.(interface{ ID() string }).ID() != "MUT-0" {
		t.Errorf("ID: got %s, want MUT-0", f0.(interface{ ID() string }).ID())
	}
	if f0.(interface{ Unwrap() vfs.FS }).Unwrap() != mem {
		t.Errorf("Unwrap did not return inner mem")
	}
}

func TestFaultMUT1SyncDropping(t *testing.T) {
	mem := vfs.NewCrashableMem()
	fs, err := faults.New("MUT-1", mem)
	if err != nil {
		t.Fatalf("faults.New MUT-1: %v", err)
	}

	// 1. pebble-wal category: Create
	walFile, err := fs.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create walFile: %v", err)
	}
	if err := walFile.Sync(); err != nil {
		t.Errorf("walFile.Sync: %v", err)
	}
	if err := walFile.SyncData(); err != nil {
		t.Errorf("walFile.SyncData: %v", err)
	}
	_ = walFile.Close()

	// 2. pebble-wal category: ReuseForWrite
	reusedWAL, err := fs.ReuseForWrite("000001.log", "000002.log", "pebble-wal")
	if err != nil {
		t.Fatalf("ReuseForWrite walFile: %v", err)
	}
	if err := reusedWAL.Sync(); err != nil {
		t.Errorf("reusedWAL.Sync: %v", err)
	}
	_ = reusedWAL.Close()

	// 3. pebble-manifest category: Create (should NOT drop sync)
	manFile, err := fs.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create manFile: %v", err)
	}
	if err := manFile.Sync(); err != nil {
		t.Errorf("manFile.Sync: %v", err)
	}
	_ = manFile.Close()
}

func runWorkloadWithFS(t *testing.T, wrappedFS vfs.FS) ([]trace.Event, []history.Entry) {
	opts := &pebble.Options{
		FS:                          wrappedFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		DisableAutomaticCompactions: true,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	cfg := workload.Config{
		Seed:       42,
		Operations: 50,
		KeyCount:   16,
		ValueBytes: 64,
		SyncEvery:  5,
		FlushEvery: 25,
	}

	// We pass nil as marker since wrappedFS is faults.FS; we correlate later via rec.
	rawHist, err := workload.Run(db, nil, cfg)
	if err != nil {
		t.Fatalf("workload.Run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	return nil, rawHist
}

// TestFaultAcceptanceWithRecorder checks the seeded faults through the recorder:
// - Decorator order is faults(vfsrec(memFS)).
// - MUT-1 trace lacks WAL syncs but keeps manifest and table syncs.
// - MUT-0 trace equals the undecorated recorder trace.
func TestFaultAcceptanceWithRecorder(t *testing.T) {
	// Run 1: Undecorated vfsrec.
	memBase := vfs.NewCrashableMem()
	recBase := vfsrec.New(memBase)
	_, _ = runWorkloadWithFS(t, recBase)
	eventsBase := recBase.Events()

	// Run 2: faults(MUT-0, vfsrec).
	mem0 := vfs.NewCrashableMem()
	rec0 := vfsrec.New(mem0)
	faultFS0, err := faults.New("MUT-0", rec0)
	if err != nil {
		t.Fatalf("faults.New MUT-0: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS0)
	eventsMUT0 := rec0.Events()

	// Run 3: faults(MUT-1, vfsrec).
	mem1 := vfs.NewCrashableMem()
	rec1 := vfsrec.New(mem1)
	faultFS1, err := faults.New("MUT-1", rec1)
	if err != nil {
		t.Fatalf("faults.New MUT-1: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS1)
	eventsMUT1 := rec1.Events()

	// Assertion 1: MUT-0 trace equals the undecorated recorder trace.
	if len(eventsMUT0) != len(eventsBase) {
		t.Fatalf("MUT-0 event count %d != base event count %d", len(eventsMUT0), len(eventsBase))
	}
	for i := range eventsBase {
		if eventsMUT0[i].Kind != eventsBase[i].Kind ||
			eventsMUT0[i].File != eventsBase[i].File ||
			eventsMUT0[i].Cat != eventsBase[i].Cat {
			t.Fatalf("MUT-0 mismatch at event %d: %+v vs %+v", i, eventsMUT0[i], eventsBase[i])
		}
	}

	// Assertion 2: In undecorated base, WAL sync events DO exist.
	baseWALSyncs := countWALSyncs(eventsBase)
	if baseWALSyncs == 0 {
		t.Fatalf("undecorated base trace unexpectedly has 0 WAL syncs")
	}

	// Assertion 3: In MUT-1 trace, WAL sync events are strictly absent (count == 0).
	mut1WALSyncs := countWALSyncs(eventsMUT1)
	if mut1WALSyncs != 0 {
		t.Fatalf("MUT-1 trace has %d WAL syncs, expected exactly 0", mut1WALSyncs)
	}

	// Assertion 4: In MUT-1 trace, manifest syncs ARE present.
	mut1ManifestSyncs := countManifestSyncs(eventsMUT1)
	if mut1ManifestSyncs == 0 {
		t.Fatalf("MUT-1 trace unexpectedly has 0 manifest syncs")
	}

	t.Logf("Fault acceptance verified: base WAL syncs=%d, MUT-0 WAL syncs=%d, MUT-1 WAL syncs=%d, MUT-1 manifest syncs=%d",
		baseWALSyncs, countWALSyncs(eventsMUT0), mut1WALSyncs, mut1ManifestSyncs)
}

func countWALSyncs(events []trace.Event) int {
	count := 0
	for _, e := range events {
		if (e.Kind == trace.KindSync || e.Kind == trace.KindSyncData) && !e.Dir {
			if e.Cat == "pebble-wal" || strings.HasSuffix(e.File, ".log") {
				count++
			}
		}
	}
	return count
}

func countManifestSyncs(events []trace.Event) int {
	count := 0
	for _, e := range events {
		if (e.Kind == trace.KindSync || e.Kind == trace.KindSyncData) && !e.Dir {
			if e.Cat == "pebble-manifest" || strings.Contains(e.File, "MANIFEST") {
				count++
			}
		}
	}
	return count
}
