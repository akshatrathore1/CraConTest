package vfsrec_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

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

	// MemFS uses "/" as its only separator, ignores a leading "/" and
	// collapses repeated separators, so these spellings name the same file.
	// (It does not resolve "." or "..", so those are not equivalent names.)
	if err := rec.MkdirAll("/subdir", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	f, err := rec.Create("/subdir//file.log", vfs.WriteCategoryUnspecified)
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
	if len(events) == 0 || events[0].Kind != trace.KindMkdirAll || events[0].File != "subdir" {
		t.Fatalf("expected canonical MkdirAll of 'subdir', got %+v", events)
	}
	for _, ev := range events[1:] {
		if ev.File != "subdir/file.log" {
			t.Fatalf("expected canonical path 'subdir/file.log', got %q", ev.File)
		}
	}
	// The canonical trace path must name the file the inner FS really created.
	if _, err := mem.Stat("subdir/file.log"); err != nil {
		t.Fatalf("canonical path does not exist in the inner FS: %v", err)
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

func TestAppendSyncEventStopsAtSameHandleCloseAndOpBoundary(t *testing.T) {
	// 1. Verify that an interleaved Close on a different handle (handle 2) or an EngineEvent
	// between Write(handle 1) and SyncData(handle 1) is reordered so SyncData(handle 1)
	// sits immediately after Write(handle 1).
	rec1 := vfsrec.New(vfs.NewMem())
	rec1.AppendEventForTesting(trace.Event{Kind: trace.KindWrite, Handle: 1, File: "wal.log"})
	rec1.AppendEventForTesting(trace.Event{Kind: trace.KindClose, Handle: 2, File: "other.sst"})
	rec1.AppendEventForTesting(trace.Event{Kind: trace.KindEngineEvent, File: "", Note: "TableCreated"})
	rec1.AppendSyncEventForTesting(trace.Event{Kind: trace.KindSyncData, Handle: 1, File: "wal.log"})

	evs1 := rec1.Events()
	if len(evs1) != 4 {
		t.Fatalf("expected 4 events, got %d", len(evs1))
	}
	if evs1[0].Kind != trace.KindWrite || evs1[0].Handle != 1 {
		t.Errorf("evs1[0]: got %v, want Write(1)", evs1[0])
	}
	if evs1[1].Kind != trace.KindSyncData || evs1[1].Handle != 1 {
		t.Errorf("evs1[1]: got %v, want SyncData(1) immediately after Write(1)", evs1[1])
	}
	if evs1[2].Kind != trace.KindClose || evs1[2].Handle != 2 {
		t.Errorf("evs1[2]: got %v, want Close(2)", evs1[2])
	}
	if evs1[3].Kind != trace.KindEngineEvent {
		t.Errorf("evs1[3]: got %v, want EngineEvent", evs1[3])
	}
	for i, e := range evs1 {
		if e.ID != uint64(i+1) {
			t.Errorf("evs1[%d].ID = %d, want %d", i, e.ID, i+1)
		}
	}

	// 2. Verify that same-handle Close(1), OpBegin, OpEnd, or Write(2) between Write(1)
	// and a subsequent SyncData(1) acts as a hard stop and is never crossed.
	hardStopCases := []struct {
		name      string
		stopEvent trace.Event
	}{
		{"SameHandleClose", trace.Event{Kind: trace.KindClose, Handle: 1, File: "wal.log"}},
		{"OpBegin", trace.Event{Kind: trace.KindOpBegin, OpKind: "set"}},
		{"OpEnd", trace.Event{Kind: trace.KindOpEnd, OpKind: "set"}},
		{"WriteDifferentHandle", trace.Event{Kind: trace.KindWrite, Handle: 2, File: "other.log", Len: 10}},
	}

	for _, tc := range hardStopCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := vfsrec.New(vfs.NewMem())
			rec.AppendEventForTesting(trace.Event{Kind: trace.KindWrite, Handle: 1, File: "wal.log"})
			rec.AppendEventForTesting(tc.stopEvent)
			rec.AppendSyncEventForTesting(trace.Event{Kind: trace.KindSyncData, Handle: 1, File: "wal.log"})

			evs := rec.Events()
			if len(evs) != 3 {
				t.Fatalf("expected 3 events, got %d", len(evs))
			}
			if evs[0].Kind != trace.KindWrite || evs[0].Handle != 1 {
				t.Errorf("evs[0]: got %v, want Write(1)", evs[0])
			}
			if evs[1].Kind != tc.stopEvent.Kind || evs[1].Handle != tc.stopEvent.Handle {
				t.Errorf("evs[1]: got %v, want hard stop event %v", evs[1], tc.stopEvent)
			}
			if evs[2].Kind != trace.KindSyncData || evs[2].Handle != 1 {
				t.Errorf("evs[2]: got %v, want SyncData(1) kept after hard stop", evs[2])
			}
		})
	}
}

func TestRecFileConcurrentReadAtAndClose(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		mem := vfs.NewMem()
		rec := vfsrec.New(mem)
		f, err := rec.Create("testfile.dat", vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		data := make([]byte, 1024)
		if _, err := f.Write(data); err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(4)

		go func() {
			defer wg.Done()
			buf := make([]byte, 64)
			for i := 0; i < 100; i++ {
				_, _ = f.ReadAt(buf, int64((i*16)%512))
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, _ = f.Stat()
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = f.Preallocate(0, 1024)
				_ = f.Prefetch(0, 512)
				_ = f.Fd()
			}
		}()

		go func() {
			defer wg.Done()
			_ = f.Close()
		}()

		wg.Wait()
	}
}

func TestIsDBWALAndManifestPathPredicates(t *testing.T) {
	walTrueCases := []string{
		"000001.log",
		"db/000001.log",
		"db_ycsb_a/000001.log",
		"testdb/000001.log",
	}
	for _, p := range walTrueCases {
		if !vfsrec.IsDBWALPathForTesting(p) {
			t.Errorf("IsDBWALPathForTesting(%q) = false, want true", p)
		}
	}

	walFalseCases := []string{
		"",
		"checkpoint/000001.log",
		"ingest_staging/000001.log",
		"other/000001.log",
		"db/sub/000001.log",
		"db/000001.sst",
	}
	for _, p := range walFalseCases {
		if vfsrec.IsDBWALPathForTesting(p) {
			t.Errorf("IsDBWALPathForTesting(%q) = true, want false", p)
		}
	}

	manifestTrueCases := []string{
		"MANIFEST-000001",
		"db/MANIFEST-000001",
		"db_compact/MANIFEST-000002",
		"testdb/MANIFEST-000003",
	}
	for _, p := range manifestTrueCases {
		if !vfsrec.IsDBManifestPathForTesting(p) {
			t.Errorf("IsDBManifestPathForTesting(%q) = false, want true", p)
		}
	}

	manifestFalseCases := []string{
		"",
		"checkpoint/MANIFEST-000001",
		"db/marker.manifest.000001.MANIFEST-000001",
		"other/MANIFEST-000001",
		"db/sub/MANIFEST-000001",
		"db/CURRENT",
	}
	for _, p := range manifestFalseCases {
		if vfsrec.IsDBManifestPathForTesting(p) {
			t.Errorf("IsDBManifestPathForTesting(%q) = true, want false", p)
		}
	}
}

func TestWaitForWALWriteSynchronizationAndExclusion(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)

	if rec.WALWriteCount() != 0 {
		t.Fatalf("WALWriteCount() initial: got %d, want 0", rec.WALWriteCount())
	}
	rec.WaitForWALWrite(0)

	// Writing to root-level "000001.log" ("pebble-wal") increments WALWriteCount() to 1.
	f1, err := rec.Create("000001.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create 000001.log failed: %v", err)
	}
	if _, err := f1.Write([]byte("data1")); err != nil {
		t.Fatalf("Write f1 failed: %v", err)
	}
	if got := rec.WALWriteCount(); got != 1 {
		t.Fatalf("WALWriteCount() after f1 write: got %d, want 1", got)
	}

	// Writing to "db/000002.log" (vfs.WriteCategoryUnspecified) increments WALWriteCount() to 2.
	if err := rec.MkdirAll("db", 0755); err != nil {
		t.Fatalf("MkdirAll db failed: %v", err)
	}
	f2, err := rec.Create("db/000002.log", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create db/000002.log failed: %v", err)
	}
	if _, err := f2.Write([]byte("data2")); err != nil {
		t.Fatalf("Write f2 failed: %v", err)
	}
	if got := rec.WALWriteCount(); got != 2 {
		t.Fatalf("WALWriteCount() after f2 write: got %d, want 2", got)
	}

	// Writing to "checkpoint/000003.log", "other/000004.log", or "db/nonwal.log" with "pebble-manifest" does NOT increment.
	if err := rec.MkdirAll("checkpoint", 0755); err != nil {
		t.Fatalf("MkdirAll checkpoint failed: %v", err)
	}
	f3, err := rec.Create("checkpoint/000003.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create checkpoint/000003.log failed: %v", err)
	}
	if _, err := f3.Write([]byte("data3")); err != nil {
		t.Fatalf("Write f3 failed: %v", err)
	}

	if err := rec.MkdirAll("other", 0755); err != nil {
		t.Fatalf("MkdirAll other failed: %v", err)
	}
	f4, err := rec.Create("other/000004.log", "pebble-wal")
	if err != nil {
		t.Fatalf("Create other/000004.log failed: %v", err)
	}
	if _, err := f4.Write([]byte("data4")); err != nil {
		t.Fatalf("Write f4 failed: %v", err)
	}

	f5, err := rec.Create("db/nonwal.log", "pebble-manifest")
	if err != nil {
		t.Fatalf("Create db/nonwal.log failed: %v", err)
	}
	if _, err := f5.Write([]byte("data5")); err != nil {
		t.Fatalf("Write f5 failed: %v", err)
	}

	if got := rec.WALWriteCount(); got != 2 {
		t.Fatalf("WALWriteCount() after non-primary/non-wal writes: got %d, want 2", got)
	}

	// Spawns a goroutine that writes to "db/000002.log" and another goroutine that calls
	// NotifyDroppedWALWrite(), verifying that a foreground call to WaitForWALWrite(4) blocks
	// until woken by those broadcasts and returns cleanly once WALWriteCount() == 4.
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = f2.Write([]byte("more data"))
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		rec.NotifyDroppedWALWrite()
	}()

	rec.WaitForWALWrite(4)
	if got := rec.WALWriteCount(); got != 4 {
		t.Fatalf("WALWriteCount() after wait: got %d, want 4", got)
	}
}

func TestWaitForWALWriteAndFlushBeginDeadlinePanics(t *testing.T) {
	t.Run("WaitForWALWrite_TimeoutPanics", func(t *testing.T) {
		rec := vfsrec.New(vfs.NewMem())
		rec.SetWaitTimeoutForTesting(15 * time.Millisecond)

		var panicked any
		func() {
			defer func() {
				panicked = recover()
			}()
			rec.WaitForWALWrite(1)
		}()

		want := "vfsrec: WaitForWALWrite exceeded 5s deadline"
		if panicked != want {
			t.Fatalf("WaitForWALWrite panic mismatch: got %v, want %q", panicked, want)
		}
	})

	t.Run("FlushBegin_TimeoutPanics", func(t *testing.T) {
		rec := vfsrec.New(vfs.NewMem())
		rec.SetWaitTimeoutForTesting(15 * time.Millisecond)
		rec.SetPendingManifestDeletionsForTesting(1)

		var panicked any
		func() {
			defer func() {
				panicked = recover()
			}()
			listener := rec.EventListener()
			listener.FlushBegin(pebble.FlushInfo{JobID: 1})
		}()

		want := "vfsrec: FlushBegin manifestCond wait exceeded 5s deadline"
		if panicked != want {
			t.Fatalf("FlushBegin panic mismatch: got %v, want %q", panicked, want)
		}
	})
}
