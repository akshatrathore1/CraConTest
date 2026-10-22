package vfsrec_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestWriteBufferMutation(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	file, err := rec.Create("test.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	buf := []byte("hello world")
	n, err := file.Write(buf)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("Write returned %d, want %d", n, len(buf))
	}

	// Mutate buffer after write.
	buf[0] = 'X'
	buf[1] = 'Y'

	// Test WriteAt buffer mutation.
	bufAt := []byte("1234")
	nAt, err := file.WriteAt(bufAt, 5)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if nAt != len(bufAt) {
		t.Fatalf("WriteAt returned %d, want %d", nAt, len(bufAt))
	}
	bufAt[0] = '9'

	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	// Find the write events.
	var writeEv, writeAtEv *trace.Event
	for i := range events {
		if events[i].Kind == trace.KindWrite {
			writeEv = &events[i]
		} else if events[i].Kind == trace.KindWriteAt {
			writeAtEv = &events[i]
		}
	}

	if writeEv == nil || string(writeEv.Data) != "hello world" {
		t.Fatalf("expected write payload 'hello world', got %q", string(writeEv.Data))
	}
	if writeAtEv == nil || string(writeAtEv.Data) != "1234" {
		t.Fatalf("expected writeAt payload '1234', got %q", string(writeAtEv.Data))
	}
}

func TestSyncDataVsSync(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	file, err := rec.Create("data.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if _, err := file.Write([]byte("chunk1")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if _, err := file.Write([]byte("chunk2")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := file.SyncData(); err != nil {
		t.Fatalf("SyncData failed: %v", err)
	}

	if fullSync, err := file.SyncTo(6); err != nil || fullSync {
		t.Fatalf("SyncTo returned (%v, %v), want (false, nil)", fullSync, err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := rec.MkdirAll("dir", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	dir, err := rec.OpenDir("dir")
	if err != nil {
		t.Fatalf("OpenDir failed: %v", err)
	}
	if err := dir.Sync(); err != nil {
		t.Fatalf("Dir Sync failed: %v", err)
	}
	if err := dir.Close(); err != nil {
		t.Fatalf("Dir Close failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	var hasSync, hasSyncData, hasSyncTo, hasDirSync bool
	for _, ev := range events {
		switch ev.Kind {
		case trace.KindSync:
			if ev.Dir {
				hasDirSync = true
				if ev.File != "dir" {
					t.Fatalf("expected dir sync on 'dir', got %q", ev.File)
				}
			} else {
				hasSync = true
				if ev.File != "data.log" {
					t.Fatalf("expected sync on 'data.log', got %q", ev.File)
				}
			}
		case trace.KindSyncData:
			hasSyncData = true
			if ev.File != "data.log" || ev.Dir {
				t.Fatalf("unexpected SyncData event: %+v", ev)
			}
		case trace.KindSyncTo:
			hasSyncTo = true
			if ev.File != "data.log" || ev.Size != 6 {
				t.Fatalf("unexpected SyncTo event: %+v", ev)
			}
		}
	}

	if !hasSync || !hasSyncData || !hasSyncTo || !hasDirSync {
		t.Fatalf("missing expected sync kinds: sync=%v syncData=%v syncTo=%v dirSync=%v",
			hasSync, hasSyncData, hasSyncTo, hasDirSync)
	}
}

func TestReuseForWriteLabels(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	file, err := rec.Create("old.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := file.Write([]byte("old content")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reused, err := rec.ReuseForWrite("old.log", "new.log", "pebble-wal")
	if err != nil {
		t.Fatalf("ReuseForWrite failed: %v", err)
	}

	if _, err := reused.Write([]byte("reused content")); err != nil {
		t.Fatalf("reused Write failed: %v", err)
	}
	if err := reused.Sync(); err != nil {
		t.Fatalf("reused Sync failed: %v", err)
	}
	if err := reused.Close(); err != nil {
		t.Fatalf("reused Close failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	var reuseEv, writeEv, syncEv, closeEv *trace.Event
	for i := range events {
		ev := &events[i]
		switch ev.Kind {
		case trace.KindReuseForWrite:
			reuseEv = ev
		case trace.KindWrite:
			if ev.Handle == 2 {
				writeEv = ev
			}
		case trace.KindSync:
			if ev.Handle == 2 {
				syncEv = ev
			}
		case trace.KindClose:
			if ev.Handle == 2 {
				closeEv = ev
			}
		}
	}

	if reuseEv == nil || reuseEv.File != "old.log" || reuseEv.File2 != "new.log" || reuseEv.Cat != "pebble-wal" {
		t.Fatalf("unexpected ReuseForWrite event: %+v", reuseEv)
	}
	if writeEv == nil || writeEv.File != "new.log" {
		t.Fatalf("expected write on 'new.log', got %+v", writeEv)
	}
	if syncEv == nil || syncEv.File != "new.log" {
		t.Fatalf("expected sync on 'new.log', got %+v", syncEv)
	}
	if closeEv == nil || closeEv.File != "new.log" {
		t.Fatalf("expected close on 'new.log', got %+v", closeEv)
	}
}

// partialWriteFS wraps a vfs.FS to inject a partial write error.
type partialWriteFS struct {
	vfs.FS
	limit int
}

func (fs *partialWriteFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &partialWriteFile{File: f, limit: fs.limit}, nil
}

type partialWriteFile struct {
	vfs.File
	limit int
}

var errPartialWrite = errors.New("short write occurred")

func (f *partialWriteFile) Write(p []byte) (int, error) {
	if len(p) > f.limit {
		n, _ := f.File.Write(p[:f.limit])
		return n, errPartialWrite
	}
	return f.File.Write(p)
}

func TestPartialWriteLen(t *testing.T) {
	mem := vfs.NewMem()
	mock := &partialWriteFS{FS: mem, limit: 3}
	rec := vfsrec.New(mock)

	file, err := rec.Create("partial.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := []byte("12345678")
	n, err := file.Write(data)
	if !errors.Is(err, errPartialWrite) {
		t.Fatalf("expected errPartialWrite, got: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected 3 bytes written, got %d", n)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	var writeEv *trace.Event
	for i := range events {
		if events[i].Kind == trace.KindWrite {
			writeEv = &events[i]
		}
	}
	if writeEv == nil {
		t.Fatal("missing write event")
	}
	if writeEv.Len != 3 {
		t.Fatalf("expected write event Len 3, got %d", writeEv.Len)
	}
	if len(writeEv.Data) != 8 {
		t.Fatalf("expected write event Data len 8, got %d", len(writeEv.Data))
	}
	if writeEv.Err != errPartialWrite.Error() {
		t.Fatalf("expected write event Err %q, got %q", errPartialWrite.Error(), writeEv.Err)
	}
}

func TestLockUnlockRecording(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	if err := rec.MkdirAll("db", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	closer, err := rec.Lock("db/LOCK")
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if closer == nil {
		t.Fatal("Lock returned nil closer")
	}

	if err := closer.Close(); err != nil {
		t.Fatalf("Close closer failed: %v", err)
	}

	// Calling Close again should be a no-op.
	if err := closer.Close(); err != nil {
		t.Fatalf("second Close closer failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected exactly 3 events (MkdirAll, Lock, Unlock), got %d", len(events))
	}
	if events[0].Kind != trace.KindMkdirAll || events[0].File != "db" {
		t.Fatalf("unexpected mkdir event: %+v", events[0])
	}
	if events[1].Kind != trace.KindLock || events[1].File != "db/LOCK" || events[1].Handle != 1 {
		t.Fatalf("unexpected lock event: %+v", events[1])
	}
	if events[2].Kind != trace.KindUnlock || events[2].File != "db/LOCK" || events[2].Handle != 1 {
		t.Fatalf("unexpected unlock event: %+v", events[2])
	}
}

func TestFailedRenameKeepsSideEffect(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	if err := rec.MkdirAll("src", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	f, err := rec.Create("src/file", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := f.Write([]byte("content")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Rename into a non-existent directory in MemFS causes an error,
	// but MemFS unlinks the source before discovering destination failure.
	err = rec.Rename("src/file", "missing/file")
	if err == nil {
		t.Fatal("expected rename to fail")
	}

	// Verify the side effect: src/file no longer exists.
	if _, statErr := rec.Stat("src/file"); !os.IsNotExist(statErr) {
		t.Fatalf("expected source file to be removed despite rename failure, statErr: %v", statErr)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	var renameEv *trace.Event
	for i := range events {
		if events[i].Kind == trace.KindRename {
			renameEv = &events[i]
		}
	}
	if renameEv == nil {
		t.Fatal("missing rename event")
	}
	if renameEv.Err == "" {
		t.Fatal("expected non-empty Err on failed rename event")
	}
	if renameEv.File != "src/file" || renameEv.File2 != "missing/file" {
		t.Fatalf("unexpected rename paths: %+v", renameEv)
	}
}

func TestMarkerAPI(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	// Verify Marker interface compatibility.
	type marker interface {
		Begin(uint64, string)
		End(uint64, string, string)
	}
	var m marker = rec

	m.Begin(1, "set")
	f, err := rec.Create("key", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := f.Write([]byte("val")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	m.End(1, "set", "")

	m.Begin(2, "compact")
	m.End(2, "compact", "aborted")

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	if events[0].Kind != trace.KindOpBegin || events[0].Op != 1 || events[0].OpKind != "set" {
		t.Fatalf("unexpected event 0: %+v", events[0])
	}
	if events[4].Kind != trace.KindOpEnd || events[4].Op != 1 || events[4].OpKind != "set" || events[4].Err != "" {
		t.Fatalf("unexpected event 4: %+v", events[4])
	}
	if events[5].Kind != trace.KindOpBegin || events[5].Op != 2 || events[5].OpKind != "compact" {
		t.Fatalf("unexpected event 5: %+v", events[5])
	}
	if events[6].Kind != trace.KindOpEnd || events[6].Op != 2 || events[6].OpKind != "compact" || events[6].Err != "aborted" {
		t.Fatalf("unexpected event 6: %+v", events[6])
	}
}

func TestSetHook(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	var hookCalls []uint64
	rec.SetHook(func(id uint64) {
		hookCalls = append(hookCalls, id)
	})

	rec.Begin(1, "set")
	rec.End(1, "set", "")

	if len(hookCalls) != 2 || hookCalls[0] != 1 || hookCalls[1] != 2 {
		t.Fatalf("unexpected hook calls: %v", hookCalls)
	}
}

func TestCanonicalPathHandling(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	// Create with backslashes and redundant components.
	path := filepath.Join("subdir", "nested", "..", "file.log")
	f, err := rec.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := f.Write([]byte("test")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	for _, ev := range events {
		if ev.File != "subdir/file.log" {
			t.Fatalf("expected canonical path 'subdir/file.log', got %q", ev.File)
		}
	}
}

func TestFullPebbleLifecycle(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            rec,
		EventListener: &listener,
	}

	db, err := pebble.Open("testdb", opts)
	if err != nil {
		t.Fatalf("pebble.Open failed: %v", err)
	}

	// Perform mutating client operations with markers.
	rec.Begin(1, "set")
	if err := db.Set([]byte("alpha"), []byte("111"), pebble.Sync); err != nil {
		t.Fatalf("db.Set failed: %v", err)
	}
	rec.End(1, "set", "")

	rec.Begin(2, "batch")
	batch := db.NewBatch()
	if err := batch.Set([]byte("beta"), []byte("222"), nil); err != nil {
		t.Fatalf("batch.Set failed: %v", err)
	}
	if err := batch.Set([]byte("gamma"), []byte("333"), nil); err != nil {
		t.Fatalf("batch.Set failed: %v", err)
	}
	if err := db.Apply(batch, pebble.Sync); err != nil {
		t.Fatalf("db.Apply failed: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("batch.Close failed: %v", err)
	}
	rec.End(2, "batch", "")

	rec.Begin(3, "flush")
	if err := db.Flush(); err != nil {
		t.Fatalf("db.Flush failed: %v", err)
	}
	rec.End(3, "flush", "")

	rec.Begin(4, "compact")
	if err := db.Compact(context.Background(), []byte("a"), []byte("z"), false); err != nil {
		t.Fatalf("db.Compact failed: %v", err)
	}
	rec.End(4, "compact", "")

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	events := rec.Events()
	if len(events) == 0 {
		t.Fatal("no events recorded during Pebble lifecycle")
	}

	// The full trace must pass validation.
	if err := trace.Validate(events); err != nil {
		t.Fatalf("recorded Pebble trace failed validation: %v", err)
	}

	// CutIDs must compute valid cutoffs.
	cutoffs, err := trace.CutIDs(events)
	if err != nil {
		t.Fatalf("CutIDs failed on Pebble trace: %v", err)
	}
	if len(cutoffs) == 0 {
		t.Fatal("expected non-empty cutoffs")
	}

	// Verify that EngineEvents were recorded.
	var engineEvents int
	for _, ev := range events {
		if ev.Kind == trace.KindEngineEvent {
			engineEvents++
		}
	}
	if engineEvents == 0 {
		t.Fatal("expected engine events from EventListener")
	}
}

func TestOpenReadWriteCreatedFlag(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	// First OpenReadWrite creates the file.
	f1, err := rec.OpenReadWrite("rw.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("OpenReadWrite 1 failed: %v", err)
	}
	if _, err := f1.Write([]byte("data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Second OpenReadWrite opens existing file.
	f2, err := rec.OpenReadWrite("rw.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("OpenReadWrite 2 failed: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(f2, buf); err != nil {
		t.Fatalf("ReadFull failed: %v", err)
	}
	if !bytes.Equal(buf, []byte("data")) {
		t.Fatalf("read %q, want 'data'", buf)
	}
	if err := f2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}

	// Check that first OpenReadWrite has Created=true, second has Created=false.
	if !events[0].Created {
		t.Fatalf("expected Created=true on first OpenReadWrite: %+v", events[0])
	}
	// Event 4 is the second OpenReadWrite.
	if events[3].Created {
		t.Fatalf("expected Created=false on second OpenReadWrite: %+v", events[3])
	}
}
