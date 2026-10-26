package faults_test

import (
	"fmt"
	"strings"
	"sync"
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
	syncToCalls   int
}

func (m *mockFile) Close() error                             { return nil }
func (m *mockFile) Read(p []byte) (int, error)               { return len(p), nil }
func (m *mockFile) ReadAt(p []byte, off int64) (int, error)  { return len(p), nil }
func (m *mockFile) Write(p []byte) (int, error)              { return len(p), nil }
func (m *mockFile) WriteAt(p []byte, off int64) (int, error) { return len(p), nil }
func (m *mockFile) Stat() (vfs.FileInfo, error)              { return nil, nil }
func (m *mockFile) Sync() error                              { m.syncCalls++; return nil }
func (m *mockFile) SyncData() error                          { m.syncDataCalls++; return nil }
func (m *mockFile) SyncTo(int64) (bool, error)               { m.syncToCalls++; return true, nil }
func (m *mockFile) Preallocate(int64, int64) error           { return nil }
func (m *mockFile) Prefetch(int64, int64) error              { return nil }
func (m *mockFile) Fd() uintptr                              { return 0 }

type mockFS struct {
	vfs.FS
	lastFile *mockFile
	lastDir  *mockFile
}

func newMockFS() *mockFS {
	return &mockFS{
		FS:       vfs.NewCrashableMem(),
		lastFile: &mockFile{},
		lastDir:  &mockFile{},
	}
}

func (m *mockFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	m.lastFile = &mockFile{}
	return m.lastFile, nil
}

func (m *mockFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	m.lastFile = &mockFile{}
	return m.lastFile, nil
}

func (m *mockFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	m.lastFile = &mockFile{}
	return m.lastFile, nil
}

func (m *mockFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	m.lastFile = &mockFile{}
	return m.lastFile, nil
}

func (m *mockFS) OpenDir(name string) (vfs.File, error) {
	m.lastDir = &mockFile{}
	return m.lastDir, nil
}

func (m *mockFS) Rename(oldname, newname string) error {
	return nil
}

func (m *mockFS) PathBase(path string) string {
	parts := strings.Split(strings.ReplaceAll(path, "\\", "/"), "/")
	return parts[len(parts)-1]
}

func TestFaultIDsAndValidation(t *testing.T) {
	ids := faults.IDs()
	expected := []string{"MUT-0", "MUT-1", "MUT-2", "MUT-3", "MUT-4", "MUT-5", "MUT-6", "MUT-7", "MUT-8", "MUT-9"}
	if len(ids) != len(expected) {
		t.Fatalf("unexpected IDs length: got %d, want %d", len(ids), len(expected))
	}
	for i, id := range expected {
		if ids[i] != id {
			t.Errorf("IDs[%d]: got %s, want %s", i, ids[i], id)
		}
	}

	// Verify aliases
	aliases := faults.Aliases()
	expectedAliases := []string{"MUT-WAL-K", "MUT-MAN-ROT", "MUT-ROCKS-10573", "MUT-ROCKS-10460", "MUT-DIR-K"}
	if len(aliases) != len(expectedAliases) {
		t.Fatalf("unexpected Aliases length: got %d, want %d", len(aliases), len(expectedAliases))
	}

	mem := vfs.NewCrashableMem()
	if _, err := faults.New("MUT-99", mem); err == nil {
		t.Error("expected error for unknown fault ID MUT-99, got nil")
	}
	if _, err := faults.New("UNKNOWN", mem); err == nil {
		t.Error("expected error for unknown fault ID UNKNOWN, got nil")
	}
	if _, err := faults.New("MUT-0", nil); err == nil {
		t.Error("expected error for nil inner FS, got nil")
	}

	// Verify all canonical IDs construct properly
	for _, id := range expected {
		fs, err := faults.New(id, mem)
		if err != nil {
			t.Fatalf("faults.New %s: %v", id, err)
		}
		if fs.(interface{ ID() string }).ID() != id {
			t.Errorf("ID: got %s, want %s", fs.(interface{ ID() string }).ID(), id)
		}
		if fs.(interface{ Unwrap() vfs.FS }).Unwrap() != mem {
			t.Errorf("Unwrap did not return inner mem for %s", id)
		}
	}

	// Verify aliases construct and normalize properly
	for _, alias := range expectedAliases {
		fs, err := faults.New(alias, mem)
		if err != nil {
			t.Fatalf("faults.New alias %s: %v", alias, err)
		}
		norm, _ := faults.NormalizeID(alias)
		if fs.(interface{ ID() string }).ID() != norm {
			t.Errorf("alias %s ID: got %s, want %s", alias, fs.(interface{ ID() string }).ID(), norm)
		}
	}

	// Verify IsSparse
	for _, id := range []string{faults.MUT2, faults.MUT3, faults.MUT4, faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8, faults.MUT9} {
		if !faults.IsSparse(id) {
			t.Errorf("%s should be sparse", id)
		}
	}
	if !faults.IsSparse("MUT-9") || !faults.IsSparse("MUT-DIR-K") {
		t.Error("MUT-9 and MUT-DIR-K should be sparse")
	}
	if faults.IsSparse(faults.MUT0) || faults.IsSparse(faults.MUT1) {
		t.Error("MUT-0 and MUT-1 should not be sparse")
	}

	// Verify NewWithConfig defaulting
	fsCustom, err := faults.NewWithConfig(faults.Config{ID: "MUT-WAL-K", WALDropK: 0, DropBudget: 0}, mem)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	cfg := fsCustom.(interface{ Config() faults.Config }).Config()
	if cfg.ID != faults.MUT5 {
		t.Errorf("expected normalized ID MUT-5, got %s", cfg.ID)
	}
	if cfg.WALDropK != 1 {
		t.Errorf("expected WALDropK defaulted to 1, got %d", cfg.WALDropK)
	}
	if cfg.DirDropK != 1 {
		t.Errorf("expected DirDropK defaulted to 1, got %d", cfg.DirDropK)
	}
	if cfg.DropBudget != 1 {
		t.Errorf("expected DropBudget defaulted to 1, got %d", cfg.DropBudget)
	}
	if cfg.TriggerName != "CompactionBegin" {
		t.Errorf("expected TriggerName defaulted to CompactionBegin, got %s", cfg.TriggerName)
	}

	fsCustomDir, err := faults.NewWithConfig(faults.Config{ID: "MUT-DIR-K", DirDropK: 0}, mem)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	cfgDir := fsCustomDir.(interface{ Config() faults.Config }).Config()
	if cfgDir.ID != faults.MUT9 {
		t.Errorf("expected normalized ID MUT-9, got %s", cfgDir.ID)
	}
	if cfgDir.DirDropK != 1 {
		t.Errorf("expected DirDropK defaulted to 1, got %d", cfgDir.DirDropK)
	}
}

func TestFaultMUT0PassThrough(t *testing.T) {
	mock := &mockFS{}
	fs, err := faults.New("MUT-0", mock)
	if err != nil {
		t.Fatalf("faults.New MUT-0: %v", err)
	}

	categories := []vfs.DiskWriteCategory{"pebble-wal", "pebble-manifest", "pebble-memtable-flush"}
	for _, cat := range categories {
		f, err := fs.Create("file", cat)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := f.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
			t.Errorf("cat %s: Sync failed or inner not called: %d", cat, mock.lastFile.syncCalls)
		}
		if err := f.SyncData(); err != nil || mock.lastFile.syncDataCalls != 1 {
			t.Errorf("cat %s: SyncData failed or inner not called: %d", cat, mock.lastFile.syncDataCalls)
		}
		if full, err := f.SyncTo(100); err != nil || !full || mock.lastFile.syncToCalls != 1 {
			t.Errorf("cat %s: SyncTo failed or inner not called: %d", cat, mock.lastFile.syncToCalls)
		}
	}

	dir, err := fs.OpenDir("dir")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
		t.Errorf("dir.Sync failed or inner not called: %d", mock.lastDir.syncCalls)
	}
}

func TestFaultMUT1WALSyncDropping(t *testing.T) {
	mock := &mockFS{}
	fs, err := faults.New("MUT-1", mock)
	if err != nil {
		t.Fatalf("faults.New MUT-1: %v", err)
	}

	// 1. pebble-wal: should drop Sync, SyncData, SyncTo
	wal, err := fs.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Errorf("wal.Sync returned error: %v", err)
	}
	if mock.lastFile.syncCalls != 0 {
		t.Errorf("wal.Sync should be dropped, but inner was called %d times", mock.lastFile.syncCalls)
	}
	if err := wal.SyncData(); err != nil {
		t.Errorf("wal.SyncData returned error: %v", err)
	}
	if mock.lastFile.syncDataCalls != 0 {
		t.Errorf("wal.SyncData should be dropped, but inner was called %d times", mock.lastFile.syncDataCalls)
	}
	if full, err := wal.SyncTo(50); err != nil || full {
		t.Errorf("wal.SyncTo should return (false, nil), got (%v, %v)", full, err)
	}
	if mock.lastFile.syncToCalls != 0 {
		t.Errorf("wal.SyncTo should be dropped, but inner was called %d times", mock.lastFile.syncToCalls)
	}

	// 2. pebble-manifest: should NOT drop
	man, err := fs.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man: %v", err)
	}
	if err := man.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("man.Sync should not be dropped, inner calls: %d", mock.lastFile.syncCalls)
	}
	if err := man.SyncData(); err != nil || mock.lastFile.syncDataCalls != 1 {
		t.Errorf("man.SyncData should not be dropped, inner calls: %d", mock.lastFile.syncDataCalls)
	}

	// 3. pebble-memtable-flush: should NOT drop
	flush, err := fs.Create("000004.sst", "pebble-memtable-flush")
	if err != nil {
		t.Fatalf("Create flush: %v", err)
	}
	if err := flush.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("flush.Sync should not be dropped, inner calls: %d", mock.lastFile.syncCalls)
	}

	// 4. OpenDir: should NOT drop
	dir, err := fs.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
		t.Errorf("dir.Sync should not be dropped, inner calls: %d", mock.lastDir.syncCalls)
	}
}

func TestFaultMUT2ManifestSyncDropping(t *testing.T) {
	mock := &mockFS{}
	fs, err := faults.New("MUT-2", mock)
	if err != nil {
		t.Fatalf("faults.New MUT-2: %v", err)
	}

	// 1. pebble-manifest: should drop Sync, SyncData, SyncTo
	man, err := fs.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man: %v", err)
	}
	if err := man.Sync(); err != nil {
		t.Errorf("man.Sync returned error: %v", err)
	}
	if mock.lastFile.syncCalls != 0 {
		t.Errorf("man.Sync should be dropped under MUT-2, but inner was called %d times", mock.lastFile.syncCalls)
	}
	if err := man.SyncData(); err != nil {
		t.Errorf("man.SyncData returned error: %v", err)
	}
	if mock.lastFile.syncDataCalls != 0 {
		t.Errorf("man.SyncData should be dropped under MUT-2, but inner was called %d times", mock.lastFile.syncDataCalls)
	}
	if full, err := man.SyncTo(100); err != nil || full {
		t.Errorf("man.SyncTo should return (false, nil), got (%v, %v)", full, err)
	}
	if mock.lastFile.syncToCalls != 0 {
		t.Errorf("man.SyncTo should be dropped under MUT-2, but inner was called %d times", mock.lastFile.syncToCalls)
	}

	// OpenReadWrite with pebble-manifest
	rwMan, err := fs.OpenReadWrite("MANIFEST-000002", "pebble-manifest")
	if err != nil {
		t.Fatalf("OpenReadWrite man: %v", err)
	}
	if err := rwMan.Sync(); err != nil || mock.lastFile.syncCalls != 0 {
		t.Errorf("rwMan.Sync should be dropped under MUT-2, inner calls: %d", mock.lastFile.syncCalls)
	}

	// ReuseForWrite with pebble-manifest
	reuseMan, err := fs.ReuseForWrite("MANIFEST-000001", "MANIFEST-000003", "pebble-manifest")
	if err != nil {
		t.Fatalf("ReuseForWrite man: %v", err)
	}
	if err := reuseMan.Sync(); err != nil || mock.lastFile.syncCalls != 0 {
		t.Errorf("reuseMan.Sync should be dropped under MUT-2, inner calls: %d", mock.lastFile.syncCalls)
	}

	// 2. pebble-wal: should NOT drop
	wal, err := fs.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("wal.Sync should not be dropped under MUT-2, inner calls: %d", mock.lastFile.syncCalls)
	}

	// 3. OpenDir: should NOT drop
	dir, err := fs.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
		t.Errorf("dir.Sync should not be dropped under MUT-2, inner calls: %d", mock.lastDir.syncCalls)
	}
}

func TestFaultMUT3DirSyncDropping(t *testing.T) {
	mock := &mockFS{}
	fs, err := faults.New("MUT-3", mock)
	if err != nil {
		t.Fatalf("faults.New MUT-3: %v", err)
	}

	// 1. OpenDir handle: Sync should be dropped!
	dir, err := fs.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := dir.Sync(); err != nil {
		t.Errorf("dir.Sync returned error: %v", err)
	}
	if mock.lastDir.syncCalls != 0 {
		t.Errorf("dir.Sync should be dropped under MUT-3, but inner was called %d times", mock.lastDir.syncCalls)
	}

	// 2. Regular files: Sync should NOT be dropped
	wal, err := fs.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("wal.Sync should not be dropped under MUT-3, inner calls: %d", mock.lastFile.syncCalls)
	}

	man, err := fs.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man: %v", err)
	}
	if err := man.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("man.Sync should not be dropped under MUT-3, inner calls: %d", mock.lastFile.syncCalls)
	}

	flush, err := fs.Create("000004.sst", "pebble-memtable-flush")
	if err != nil {
		t.Fatalf("Create flush: %v", err)
	}
	if err := flush.SyncData(); err != nil || mock.lastFile.syncDataCalls != 1 {
		t.Errorf("flush.SyncData should not be dropped under MUT-3, inner calls: %d", mock.lastFile.syncDataCalls)
	}
}

func TestFaultMUT4FlushSyncDataDropping(t *testing.T) {
	mock := &mockFS{}
	fs, err := faults.New("MUT-4", mock)
	if err != nil {
		t.Fatalf("faults.New MUT-4: %v", err)
	}

	// 1. pebble-memtable-flush: SyncData and SyncTo should be dropped, Sync should pass through
	flush, err := fs.Create("000004.sst", "pebble-memtable-flush")
	if err != nil {
		t.Fatalf("Create flush: %v", err)
	}
	if err := flush.SyncData(); err != nil {
		t.Errorf("flush.SyncData returned error: %v", err)
	}
	if mock.lastFile.syncDataCalls != 0 {
		t.Errorf("flush.SyncData should be dropped under MUT-4, but inner was called %d times", mock.lastFile.syncDataCalls)
	}
	if full, err := flush.SyncTo(200); err != nil || full {
		t.Errorf("flush.SyncTo should return (false, nil), got (%v, %v)", full, err)
	}
	if mock.lastFile.syncToCalls != 0 {
		t.Errorf("flush.SyncTo should be dropped under MUT-4, but inner was called %d times", mock.lastFile.syncToCalls)
	}
	if err := flush.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("flush.Sync should not be dropped under MUT-4, inner calls: %d", mock.lastFile.syncCalls)
	}

	// 2. pebble-wal: SyncData should NOT be dropped
	wal, err := fs.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.SyncData(); err != nil || mock.lastFile.syncDataCalls != 1 {
		t.Errorf("wal.SyncData should not be dropped under MUT-4, inner calls: %d", mock.lastFile.syncDataCalls)
	}

	// 3. pebble-manifest: Sync should NOT be dropped
	man, err := fs.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man: %v", err)
	}
	if err := man.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("man.Sync should not be dropped under MUT-4, inner calls: %d", mock.lastFile.syncCalls)
	}

	// 4. OpenDir: Sync should NOT be dropped
	dir, err := fs.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
		t.Errorf("dir.Sync should not be dropped under MUT-4, inner calls: %d", mock.lastDir.syncCalls)
	}
}

func TestFaultMUT5WALSyncK(t *testing.T) {
	// Case 1: Default k = 1.
	mock1 := newMockFS()
	fs1, err := faults.New("MUT-5", mock1)
	if err != nil {
		t.Fatalf("faults.New MUT-5: %v", err)
	}

	wal1, err := fs1.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal1: %v", err)
	}

	// 1st sync on WAL: should be dropped!
	if err := wal1.Sync(); err != nil {
		t.Fatalf("wal1.Sync error: %v", err)
	}
	if mock1.lastFile.syncCalls != 0 {
		t.Errorf("1st WAL sync under MUT-5 (k=1) should be dropped, but inner was called %d times", mock1.lastFile.syncCalls)
	}

	// 2nd sync on WAL: should succeed
	if err := wal1.Sync(); err != nil {
		t.Fatalf("wal1.Sync error: %v", err)
	}
	if mock1.lastFile.syncCalls != 1 {
		t.Errorf("2nd WAL sync under MUT-5 (k=1) should succeed, inner calls: %d", mock1.lastFile.syncCalls)
	}

	// 3rd sync on WAL: should succeed
	if err := wal1.Sync(); err != nil {
		t.Fatalf("wal1.Sync error: %v", err)
	}
	if mock1.lastFile.syncCalls != 2 {
		t.Errorf("3rd WAL sync under MUT-5 (k=1) should succeed, inner calls: %d", mock1.lastFile.syncCalls)
	}

	// Non-WAL file: syncs should succeed normally
	man, err := fs1.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man: %v", err)
	}
	if err := man.Sync(); err != nil || mock1.lastFile.syncCalls != 1 {
		t.Errorf("manifest sync should succeed under MUT-5, inner calls: %d", mock1.lastFile.syncCalls)
	}

	stateFS1 := fs1.(*faults.FS)
	if stateFS1.WALSyncCount() != 3 {
		t.Errorf("expected WALSyncCount=3, got %d", stateFS1.WALSyncCount())
	}
	if !stateFS1.WALSyncDropped() {
		t.Errorf("expected WALSyncDropped=true")
	}

	// Case 2: Configured k = 3 with SyncData and SyncTo.
	mock2 := newMockFS()
	fs2, err := faults.NewWithConfig(faults.Config{ID: faults.MUT5, WALDropK: 3}, mock2)
	if err != nil {
		t.Fatalf("faults.NewWithConfig MUT-5: %v", err)
	}

	wal2, err := fs2.Create("000002.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal2: %v", err)
	}

	// 1st sync (SyncData): succeeds
	if err := wal2.SyncData(); err != nil || mock2.lastFile.syncDataCalls != 1 {
		t.Errorf("1st sync under k=3 should succeed, inner calls: %d", mock2.lastFile.syncDataCalls)
	}
	// 2nd sync (SyncTo): succeeds
	if full, err := wal2.SyncTo(100); err != nil || !full || mock2.lastFile.syncToCalls != 1 {
		t.Errorf("2nd sync under k=3 should succeed, inner calls: %d", mock2.lastFile.syncToCalls)
	}
	// 3rd sync (Sync): DROPPED!
	if err := wal2.Sync(); err != nil {
		t.Fatalf("3rd sync error: %v", err)
	}
	if mock2.lastFile.syncCalls != 0 {
		t.Errorf("3rd WAL sync under MUT-5 (k=3) should be dropped, but inner calls: %d", mock2.lastFile.syncCalls)
	}
	// 4th sync (SyncData): succeeds
	if err := wal2.SyncData(); err != nil || mock2.lastFile.syncDataCalls != 2 {
		t.Errorf("4th sync under k=3 should succeed, inner calls: %d", mock2.lastFile.syncDataCalls)
	}
	// 5th sync (Sync): succeeds
	if err := wal2.Sync(); err != nil || mock2.lastFile.syncCalls != 1 {
		t.Errorf("5th sync under k=3 should succeed, inner calls: %d", mock2.lastFile.syncCalls)
	}

	stateFS2 := fs2.(*faults.FS)
	if stateFS2.WALSyncCount() != 5 {
		t.Errorf("expected WALSyncCount=5, got %d", stateFS2.WALSyncCount())
	}
	if !stateFS2.WALSyncDropped() {
		t.Errorf("expected WALSyncDropped=true")
	}
}

func TestFaultMUT6ManifestRotationSyncDropping(t *testing.T) {
	// Mode A: Physical Manifest File Rotation
	mockA := newMockFS()
	fsA, err := faults.New("MUT-6", mockA)
	if err != nil {
		t.Fatalf("faults.New MUT-6: %v", err)
	}

	// Initial manifest file creation & sync (should succeed)
	man1, err := fsA.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man1: %v", err)
	}
	if err := man1.Sync(); err != nil || mockA.lastFile.syncCalls != 1 {
		t.Errorf("initial manifest sync should succeed, inner calls: %d", mockA.lastFile.syncCalls)
	}

	// Manifest rotation creates a second manifest file
	man2, err := fsA.Create("MANIFEST-000002", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create man2: %v", err)
	}
	// First sync on rotated manifest: DROPPED!
	if err := man2.Sync(); err != nil {
		t.Fatalf("man2.Sync error: %v", err)
	}
	if mockA.lastFile.syncCalls != 0 {
		t.Errorf("rotated manifest sync should be dropped, but inner calls: %d", mockA.lastFile.syncCalls)
	}

	// Subsequent sync on rotated manifest: succeeds
	if err := man2.Sync(); err != nil || mockA.lastFile.syncCalls != 1 {
		t.Errorf("subsequent rotated manifest sync should succeed, inner calls: %d", mockA.lastFile.syncCalls)
	}

	// WAL syncs must succeed normally
	wal, err := fsA.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.Sync(); err != nil || mockA.lastFile.syncCalls != 1 {
		t.Errorf("wal sync should succeed under MUT-6, inner calls: %d", mockA.lastFile.syncCalls)
	}

	stateFSA := fsA.(*faults.FS)
	if !stateFSA.ManifestSyncDropped() {
		t.Errorf("expected ManifestSyncDropped=true")
	}

	// Mode B: Unrotated Manifest Runtime VersionEdit Sync Drop
	mockB := newMockFS()
	fsB, err := faults.New("MUT-MAN-ROT", mockB)
	if err != nil {
		t.Fatalf("faults.New alias: %v", err)
	}

	manB, err := fsB.Create("MANIFEST-000001", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create manB: %v", err)
	}

	// Sync #1: initial setup sync -> succeeds
	if err := manB.Sync(); err != nil || mockB.lastFile.syncCalls != 1 {
		t.Errorf("initial manifest sync should succeed, inner calls: %d", mockB.lastFile.syncCalls)
	}

	// Sync #2: first post-init VersionEdit sync -> DROPPED!
	if err := manB.Sync(); err != nil {
		t.Fatalf("manB.Sync #2 error: %v", err)
	}
	if mockB.lastFile.syncCalls != 1 {
		t.Errorf("post-init VersionEdit sync should be dropped, but inner calls: %d", mockB.lastFile.syncCalls)
	}

	// Sync #3: subsequent manifest sync -> succeeds
	if err := manB.Sync(); err != nil || mockB.lastFile.syncCalls != 2 {
		t.Errorf("subsequent manifest sync should succeed, inner calls: %d", mockB.lastFile.syncCalls)
	}

	stateFSB := fsB.(*faults.FS)
	if !stateFSB.ManifestSyncDropped() {
		t.Errorf("expected ManifestSyncDropped=true")
	}
}

func TestFaultMUT7RocksDB10573DirSyncDropping(t *testing.T) {
	mock := newMockFS()
	fs, err := faults.New("MUT-7", mock)
	if err != nil {
		t.Fatalf("faults.New MUT-7: %v", err)
	}

	// 1. Directory sync before any CURRENT rename: should succeed
	dir, err := fs.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
		t.Errorf("dir.Sync before rename should succeed, inner calls: %d", mock.lastDir.syncCalls)
	}

	// 2. Atomic rename targeting CURRENT
	if err := fs.Rename("db/CURRENT.tmp", "db/CURRENT"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	stateFS := fs.(*faults.FS)
	if !stateFS.PendingDirSyncDrop() {
		t.Errorf("expected PendingDirSyncDrop=true after CURRENT rename")
	}

	// 3. Directory sync following CURRENT rename: DROPPED!
	if err := dir.Sync(); err != nil {
		t.Fatalf("dir.Sync error: %v", err)
	}
	if mock.lastDir.syncCalls != 1 {
		t.Errorf("dir.Sync after CURRENT rename should be dropped, inner calls: %d", mock.lastDir.syncCalls)
	}
	if stateFS.PendingDirSyncDrop() {
		t.Errorf("expected PendingDirSyncDrop=false after drop consumed")
	}
	if !stateFS.DirSyncDropped() {
		t.Errorf("expected DirSyncDropped=true")
	}

	// 4. Subsequent directory sync: succeeds
	if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 2 {
		t.Errorf("subsequent dir.Sync should succeed, inner calls: %d", mock.lastDir.syncCalls)
	}

	// 5. Test with OPTIONS metadata rename
	if err := fs.Rename("db/OPTIONS-000001.tmp", "db/OPTIONS-000001"); err != nil {
		t.Fatalf("Rename OPTIONS: %v", err)
	}
	if !stateFS.PendingDirSyncDrop() {
		t.Errorf("expected PendingDirSyncDrop=true after OPTIONS rename")
	}
	dir2, err := fs.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir db: %v", err)
	}
	if err := dir2.Sync(); err != nil || mock.lastDir.syncCalls != 0 {
		t.Errorf("dir2.Sync after OPTIONS rename should be dropped, inner calls: %d", mock.lastDir.syncCalls)
	}
	if err := dir2.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
		t.Errorf("dir2.Sync second call should succeed, inner calls: %d", mock.lastDir.syncCalls)
	}

	// 6. Regular file syncs pass through unaffected
	wal, err := fs.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
		t.Errorf("wal.Sync should succeed under MUT-7, inner calls: %d", mock.lastFile.syncCalls)
	}
}

func TestFaultMUT8RocksDB10460TriggerDropping(t *testing.T) {
	// Subtest 1: Automatic category trigger (category == pebble-compaction)
	mock1 := newMockFS()
	fs1, err := faults.New("MUT-8", mock1)
	if err != nil {
		t.Fatalf("faults.New MUT-8: %v", err)
	}

	// Before compaction trigger: flush sync succeeds
	flush, err := fs1.Create("000004.sst", "pebble-memtable-flush")
	if err != nil {
		t.Fatalf("Create flush: %v", err)
	}
	if err := flush.SyncData(); err != nil || mock1.lastFile.syncDataCalls != 1 {
		t.Errorf("flush.SyncData before trigger should succeed, inner calls: %d", mock1.lastFile.syncDataCalls)
	}

	// Create compaction output table (triggers fault)
	comp, err := fs1.Create("000008.sst", "pebble-compaction")
	if err != nil {
		t.Fatalf("Create comp: %v", err)
	}
	stateFS1 := fs1.(*faults.FS)
	if !stateFS1.IsTriggered() {
		t.Errorf("expected IsTriggered=true after pebble-compaction Create")
	}

	// 1st compaction sync: DROPPED!
	if err := comp.SyncData(); err != nil {
		t.Fatalf("comp.SyncData error: %v", err)
	}
	if mock1.lastFile.syncDataCalls != 0 {
		t.Errorf("comp.SyncData should be dropped under MUT-8, inner calls: %d", mock1.lastFile.syncDataCalls)
	}
	if stateFS1.TriggerDropCount() != 1 {
		t.Errorf("expected TriggerDropCount=1, got %d", stateFS1.TriggerDropCount())
	}

	// 2nd compaction sync: succeeds (budget 1 exhausted)
	if err := comp.SyncData(); err != nil || mock1.lastFile.syncDataCalls != 1 {
		t.Errorf("subsequent comp.SyncData should succeed, inner calls: %d", mock1.lastFile.syncDataCalls)
	}

	// WAL syncs pass through unaffected
	wal, err := fs1.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create wal: %v", err)
	}
	if err := wal.Sync(); err != nil || mock1.lastFile.syncCalls != 1 {
		t.Errorf("wal.Sync should succeed under MUT-8, inner calls: %d", mock1.lastFile.syncCalls)
	}

	// Subtest 2: Explicit lifecycle Trigger and custom DropBudget = 2
	mock2 := newMockFS()
	fs2, err := faults.NewWithConfig(faults.Config{
		ID:          faults.MutRocks10460,
		DropBudget:  2,
		TriggerName: "CompactionBegin",
	}, mock2)
	if err != nil {
		t.Fatalf("faults.NewWithConfig MUT-8: %v", err)
	}

	stateFS2 := fs2.(*faults.FS)
	stateFS2.Trigger("CompactionBegin")
	if !stateFS2.IsTriggered() {
		t.Errorf("expected IsTriggered=true after Trigger call")
	}

	flushTable, err := fs2.Create("000005.sst", "pebble-memtable-flush")
	if err != nil {
		t.Fatalf("Create flushTable: %v", err)
	}
	// Drop 1 of 2
	if err := flushTable.SyncData(); err != nil || mock2.lastFile.syncDataCalls != 0 {
		t.Errorf("drop 1 should be suppressed, inner calls: %d", mock2.lastFile.syncDataCalls)
	}
	// Drop 2 of 2
	if err := flushTable.SyncData(); err != nil || mock2.lastFile.syncDataCalls != 0 {
		t.Errorf("drop 2 should be suppressed, inner calls: %d", mock2.lastFile.syncDataCalls)
	}
	// Drop 3: budget exhausted -> succeeds
	if err := flushTable.SyncData(); err != nil || mock2.lastFile.syncDataCalls != 1 {
		t.Errorf("drop 3 should succeed, inner calls: %d", mock2.lastFile.syncDataCalls)
	}
	if stateFS2.TriggerDropCount() != 2 {
		t.Errorf("expected TriggerDropCount=2, got %d", stateFS2.TriggerDropCount())
	}
}

func TestFaultMUT9DirSyncK(t *testing.T) {
	t.Run("DefaultK1", func(t *testing.T) {
		mock := newMockFS()
		fs, err := faults.New("MUT-9", mock)
		if err != nil {
			t.Fatalf("faults.New MUT-9: %v", err)
		}

		dir, err := fs.OpenDir("db")
		if err != nil {
			t.Fatalf("OpenDir: %v", err)
		}

		// 1st directory sync: should be dropped!
		if err := dir.Sync(); err != nil {
			t.Fatalf("1st dir.Sync error: %v", err)
		}
		if mock.lastDir.syncCalls != 0 {
			t.Errorf("1st dir sync under MUT-9 (k=1) should be dropped, but inner called %d times", mock.lastDir.syncCalls)
		}

		// 2nd directory sync: should succeed
		if err := dir.Sync(); err != nil {
			t.Fatalf("2nd dir.Sync error: %v", err)
		}
		if mock.lastDir.syncCalls != 1 {
			t.Errorf("2nd dir sync under MUT-9 (k=1) should succeed, inner calls: %d", mock.lastDir.syncCalls)
		}

		// 3rd directory sync: should succeed
		if err := dir.Sync(); err != nil {
			t.Fatalf("3rd dir.Sync error: %v", err)
		}
		if mock.lastDir.syncCalls != 2 {
			t.Errorf("3rd dir sync under MUT-9 (k=1) should succeed, inner calls: %d", mock.lastDir.syncCalls)
		}

		// Non-directory files (WAL, MANIFEST, SST) sync normally
		wal, err := fs.Create("000001.log", "pebble-wal")
		if err != nil {
			t.Fatalf("Create wal: %v", err)
		}
		if err := wal.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
			t.Errorf("wal.Sync should succeed under MUT-9, inner calls: %d", mock.lastFile.syncCalls)
		}

		man, err := fs.Create("MANIFEST-000001", "pebble-manifest")
		if err != nil {
			t.Fatalf("Create man: %v", err)
		}
		if err := man.Sync(); err != nil || mock.lastFile.syncCalls != 1 {
			t.Errorf("man.Sync should succeed under MUT-9, inner calls: %d", mock.lastFile.syncCalls)
		}

		sst, err := fs.Create("000004.sst", "pebble-memtable-flush")
		if err != nil {
			t.Fatalf("Create sst: %v", err)
		}
		if err := sst.SyncData(); err != nil || mock.lastFile.syncDataCalls != 1 {
			t.Errorf("sst.SyncData should succeed under MUT-9, inner calls: %d", mock.lastFile.syncDataCalls)
		}

		stateFS := fs.(*faults.FS)
		if stateFS.DirSyncCount() != 3 {
			t.Errorf("expected DirSyncCount=3, got %d", stateFS.DirSyncCount())
		}
		if !stateFS.DirSyncDropped() {
			t.Errorf("expected DirSyncDropped=true")
		}
		if !stateFS.KDirSyncDropped() {
			t.Errorf("expected KDirSyncDropped=true")
		}
	})

	t.Run("ConfiguredK3", func(t *testing.T) {
		mock := newMockFS()
		fs, err := faults.NewWithConfig(faults.Config{
			ID:       faults.MUT9,
			DirDropK: 3,
		}, mock)
		if err != nil {
			t.Fatalf("faults.NewWithConfig MUT-9: %v", err)
		}

		dir, err := fs.OpenDir("db")
		if err != nil {
			t.Fatalf("OpenDir: %v", err)
		}

		// 1st directory sync: succeeds
		if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 1 {
			t.Errorf("1st dir sync under k=3 should succeed, inner calls: %d", mock.lastDir.syncCalls)
		}

		// 2nd directory sync: succeeds
		if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 2 {
			t.Errorf("2nd dir sync under k=3 should succeed, inner calls: %d", mock.lastDir.syncCalls)
		}

		// 3rd directory sync: dropped!
		if err := dir.Sync(); err != nil {
			t.Fatalf("3rd dir.Sync error: %v", err)
		}
		if mock.lastDir.syncCalls != 2 {
			t.Errorf("3rd dir sync under MUT-9 (k=3) should be dropped, but inner calls: %d", mock.lastDir.syncCalls)
		}

		// 4th directory sync: succeeds
		if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 3 {
			t.Errorf("4th dir sync under k=3 should succeed, inner calls: %d", mock.lastDir.syncCalls)
		}

		// 5th directory sync: succeeds
		if err := dir.Sync(); err != nil || mock.lastDir.syncCalls != 4 {
			t.Errorf("5th dir sync under k=3 should succeed, inner calls: %d", mock.lastDir.syncCalls)
		}

		stateFS := fs.(*faults.FS)
		if stateFS.DirSyncCount() != 5 {
			t.Errorf("expected DirSyncCount=5, got %d", stateFS.DirSyncCount())
		}
		if !stateFS.DirSyncDropped() {
			t.Errorf("expected DirSyncDropped=true")
		}
		if !stateFS.KDirSyncDropped() {
			t.Errorf("expected KDirSyncDropped=true")
		}
	})
}

func TestFaultConcurrencySafety(t *testing.T) {
	for _, id := range []string{faults.MUT5, faults.MUT6, faults.MUT7, faults.MUT8, faults.MUT9} {
		t.Run(id, func(t *testing.T) {
			mem := vfs.NewCrashableMem()
			fs, err := faults.NewWithConfig(faults.Config{
				ID:       id,
				WALDropK: 2,
				DirDropK: 2,
			}, mem)
			if err != nil {
				t.Fatalf("NewWithConfig: %v", err)
			}

			var wg sync.WaitGroup
			numGoroutines := 20
			numOps := 50

			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < numOps; i++ {
						switch i % 5 {
						case 0:
							f, err := fs.Create(fmt.Sprintf("wal_%d_%d.log", gid, i), "pebble-wal")
							if err == nil {
								_ = f.Sync()
								_ = f.SyncData()
								_, _ = f.SyncTo(100)
								_ = f.Close()
							}
						case 1:
							f, err := fs.Create(fmt.Sprintf("man_%d_%d", gid, i), "pebble-manifest")
							if err == nil {
								_ = f.Sync()
								_ = f.Close()
							}
						case 2:
							d, err := fs.OpenDir("dir")
							if err == nil {
								_ = d.Sync()
								_ = d.Close()
							}
						case 3:
							_ = fs.Rename("CURRENT.tmp", "CURRENT")
						case 4:
							f, err := fs.Create(fmt.Sprintf("comp_%d_%d.sst", gid, i), "pebble-compaction")
							if err == nil {
								_ = f.SyncData()
								_ = f.Close()
							}
						}
					}
				}(g)
			}
			wg.Wait()
		})
	}
}

func TestFaultAcceptanceWithRecorderPhase2(t *testing.T) {
	// Baseline undecorated recorder
	memBase := vfs.NewCrashableMem()
	recBase := vfsrec.New(memBase)
	_, _ = runWorkloadWithFS(t, recBase)
	eventsBase := recBase.Events()
	baseWALSyncs := countWALSyncs(eventsBase)
	baseManifestSyncs := countManifestSyncs(eventsBase)
	baseDirSyncs := countDirSyncs(eventsBase)

	// MUT-0: Zero syncs dropped (control run)
	mem0 := vfs.NewCrashableMem()
	rec0 := vfsrec.New(mem0)
	faultFS0, err := faults.New(faults.MUT0, rec0)
	if err != nil {
		t.Fatalf("faults.New MUT-0: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS0)
	events0 := rec0.Events()
	if countWALSyncs(events0) != baseWALSyncs {
		t.Errorf("MUT-0 WAL syncs mismatch: got %d, want %d", countWALSyncs(events0), baseWALSyncs)
	}
	if countManifestSyncs(events0) != baseManifestSyncs {
		t.Errorf("MUT-0 manifest syncs mismatch: got %d, want %d", countManifestSyncs(events0), baseManifestSyncs)
	}
	if countDirSyncs(events0) != baseDirSyncs {
		t.Errorf("MUT-0 dir syncs mismatch: got %d, want %d", countDirSyncs(events0), baseDirSyncs)
	}

	// MUT-5: Exactly 1 WAL sync dropped (k=1)
	mem5 := vfs.NewCrashableMem()
	rec5 := vfsrec.New(mem5)
	faultFS5, err := faults.New(faults.MUT5, rec5)
	if err != nil {
		t.Fatalf("faults.New MUT-5: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS5)
	events5 := rec5.Events()
	mut5WALSyncs := countWALSyncs(events5)
	if mut5WALSyncs != baseWALSyncs-1 {
		t.Errorf("MUT-5 should drop exactly 1 WAL sync: base=%d, got=%d", baseWALSyncs, mut5WALSyncs)
	}
	if countManifestSyncs(events5) != baseManifestSyncs {
		t.Errorf("MUT-5 manifest syncs should not be dropped: base=%d, got=%d", baseManifestSyncs, countManifestSyncs(events5))
	}

	// MUT-6: Exactly 1 manifest sync dropped
	mem6 := vfs.NewCrashableMem()
	rec6 := vfsrec.New(mem6)
	faultFS6, err := faults.New(faults.MUT6, rec6)
	if err != nil {
		t.Fatalf("faults.New MUT-6: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS6)
	events6 := rec6.Events()
	mut6ManifestSyncs := countManifestSyncs(events6)
	if mut6ManifestSyncs != baseManifestSyncs-1 {
		t.Errorf("MUT-6 should drop exactly 1 manifest sync: base=%d, got=%d", baseManifestSyncs, mut6ManifestSyncs)
	}
	if countWALSyncs(events6) != baseWALSyncs {
		t.Errorf("MUT-6 WAL syncs should not be dropped: base=%d, got=%d", baseWALSyncs, countWALSyncs(events6))
	}

	// MUT-9: Exactly 1 directory sync dropped (k=1)
	mem9 := vfs.NewCrashableMem()
	rec9 := vfsrec.New(mem9)
	faultFS9, err := faults.New(faults.MUT9, rec9)
	if err != nil {
		t.Fatalf("faults.New MUT-9: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS9)
	events9 := rec9.Events()
	mut9DirSyncs := countDirSyncs(events9)
	if mut9DirSyncs != baseDirSyncs-1 {
		t.Errorf("MUT-9 should drop exactly 1 dir sync: base=%d, got=%d", baseDirSyncs, mut9DirSyncs)
	}
	if countWALSyncs(events9) != baseWALSyncs {
		t.Errorf("MUT-9 WAL syncs should not be dropped: base=%d, got=%d", baseWALSyncs, countWALSyncs(events9))
	}
	if countManifestSyncs(events9) != baseManifestSyncs {
		t.Errorf("MUT-9 manifest syncs should not be dropped: base=%d, got=%d", baseManifestSyncs, countManifestSyncs(events9))
	}

	t.Logf("Sparse fault acceptance: Base(WAL=%d, Man=%d, Dir=%d) | MUT-0(WAL=%d, Man=%d) | MUT-5(WAL=%d) | MUT-6(Man=%d) | MUT-9(Dir=%d)",
		baseWALSyncs, baseManifestSyncs, baseDirSyncs,
		countWALSyncs(events0), countManifestSyncs(events0),
		mut5WALSyncs, mut6ManifestSyncs, mut9DirSyncs)
}

func runWorkloadWithFS(t *testing.T, wrappedFS vfs.FS) ([]trace.Event, []history.Entry) {
	opts := &pebble.Options{
		FS:                          wrappedFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		DisableAutomaticCompactions: true,
		DisableTableStats:           true,
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
	db.TestOnlyWaitForCleaning()
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	return nil, rawHist
}

// TestFaultAcceptanceWithRecorder checks the seeded faults through the recorder:
// - Decorator order is faults(vfsrec(memFS)).
// - MUT-0 trace equals the undecorated recorder trace.
// - MUT-1 trace lacks WAL syncs but keeps manifest and table syncs.
// - MUT-2 trace lacks manifest syncs but keeps WAL syncs.
// - MUT-3 trace lacks directory syncs.
// - MUT-4 trace lacks SSTable flush syncs.
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

	// Run 4: faults(MUT-2, vfsrec).
	mem2 := vfs.NewCrashableMem()
	rec2 := vfsrec.New(mem2)
	faultFS2, err := faults.New("MUT-2", rec2)
	if err != nil {
		t.Fatalf("faults.New MUT-2: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS2)
	eventsMUT2 := rec2.Events()

	// Run 5: faults(MUT-3, vfsrec).
	mem3 := vfs.NewCrashableMem()
	rec3 := vfsrec.New(mem3)
	faultFS3, err := faults.New("MUT-3", rec3)
	if err != nil {
		t.Fatalf("faults.New MUT-3: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS3)
	eventsMUT3 := rec3.Events()

	// Run 6: faults(MUT-4, vfsrec).
	mem4 := vfs.NewCrashableMem()
	rec4 := vfsrec.New(mem4)
	faultFS4, err := faults.New("MUT-4", rec4)
	if err != nil {
		t.Fatalf("faults.New MUT-4: %v", err)
	}
	_, _ = runWorkloadWithFS(t, faultFS4)
	eventsMUT4 := rec4.Events()

	// Assertion 1: MUT-0 trace equals the undecorated recorder trace.
	trimShutdown := func(events []trace.Event) []trace.Event {
		for len(events) > 0 {
			last := events[len(events)-1]
			if last.Kind == trace.KindClose || last.Kind == trace.KindUnlock {
				events = events[:len(events)-1]
			} else {
				break
			}
		}
		return events
	}
	eventsBase = trimShutdown(eventsBase)
	eventsMUT0 = trimShutdown(eventsMUT0)
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

	// Base counts
	baseWALSyncs := countWALSyncs(eventsBase)
	baseManifestSyncs := countManifestSyncs(eventsBase)
	baseDirSyncs := countDirSyncs(eventsBase)
	baseFlushSyncs := countFlushSyncs(eventsBase)

	if baseWALSyncs == 0 {
		t.Fatalf("undecorated base trace unexpectedly has 0 WAL syncs")
	}
	if baseManifestSyncs == 0 {
		t.Fatalf("undecorated base trace unexpectedly has 0 manifest syncs")
	}

	if baseDirSyncs == 0 {
		t.Fatalf("undecorated base trace unexpectedly has 0 directory syncs")
	}
	if baseFlushSyncs == 0 {
		t.Fatalf("undecorated base trace unexpectedly has 0 flush syncs")
	}

	// MUT-1: WAL syncs dropped (0), manifest syncs intact
	mut1WALSyncs := countWALSyncs(eventsMUT1)
	if mut1WALSyncs != 0 {
		t.Fatalf("MUT-1 trace has %d WAL syncs, expected exactly 0", mut1WALSyncs)
	}
	if countManifestSyncs(eventsMUT1) == 0 {
		t.Fatalf("MUT-1 trace unexpectedly has 0 manifest syncs")
	}

	// MUT-2: Manifest syncs dropped (0), WAL syncs intact
	mut2ManifestSyncs := countManifestSyncs(eventsMUT2)
	if mut2ManifestSyncs != 0 {
		t.Fatalf("MUT-2 trace has %d manifest syncs, expected exactly 0", mut2ManifestSyncs)
	}
	if countWALSyncs(eventsMUT2) == 0 {
		t.Fatalf("MUT-2 trace unexpectedly has 0 WAL syncs")
	}

	// MUT-3: Directory syncs dropped (0), WAL syncs intact
	mut3DirSyncs := countDirSyncs(eventsMUT3)
	if mut3DirSyncs != 0 {
		t.Fatalf("MUT-3 trace has %d directory syncs, expected exactly 0", mut3DirSyncs)
	}
	if countWALSyncs(eventsMUT3) == 0 {
		t.Fatalf("MUT-3 trace unexpectedly has 0 WAL syncs")
	}

	// MUT-4: SSTable flush SyncData dropped (0), WAL syncs intact
	mut4FlushSyncs := countFlushSyncs(eventsMUT4)
	if mut4FlushSyncs != 0 {
		t.Fatalf("MUT-4 trace has %d flush syncs, expected exactly 0", mut4FlushSyncs)
	}
	if countWALSyncs(eventsMUT4) == 0 {
		t.Fatalf("MUT-4 trace unexpectedly has 0 WAL syncs")
	}

	t.Logf("Fault acceptance verified: base WAL=%d Man=%d Dir=%d Flush=%d | MUT-1 WAL=%d | MUT-2 Man=%d | MUT-3 Dir=%d | MUT-4 Flush=%d",
		baseWALSyncs, baseManifestSyncs, baseDirSyncs, baseFlushSyncs,
		mut1WALSyncs, mut2ManifestSyncs, mut3DirSyncs, mut4FlushSyncs)
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

func countDirSyncs(events []trace.Event) int {
	count := 0
	for _, e := range events {
		if e.Kind == trace.KindSync && e.Dir {
			count++
		}
	}
	return count
}

func countFlushSyncs(events []trace.Event) int {
	count := 0
	for _, e := range events {
		if (e.Kind == trace.KindSync || e.Kind == trace.KindSyncData) && !e.Dir {
			if e.Cat == "pebble-memtable-flush" || strings.HasSuffix(e.File, ".sst") {
				count++
			}
		}
	}
	return count
}
