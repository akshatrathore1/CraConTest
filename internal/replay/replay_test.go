package replay_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestReplayBasicBuildAndClone(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	rec.Begin(1, "set")
	f, err := rec.Create("file.txt", "unspecified")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Pebble's MemFS overwrites the caller's buffer after a write in race
	// builds, so keep the expected bytes separate from the written buffer.
	payload := []byte("hello world")
	if _, err := f.Write(bytes.Clone(payload)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	rec.End(1, "set", "")

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	state, err := replay.Build(events, uint64(len(events)))
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer state.Close()

	if state.CurrentCut() != uint64(len(events)) {
		t.Fatalf("currentCut: got %d, want %d", state.CurrentCut(), len(events))
	}

	cloned, err := state.Clone(100, 42)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}

	cf, err := cloned.Open("file.txt")
	if err != nil {
		t.Fatalf("cloned.Open: %v", err)
	}
	defer cf.Close()

	readData, err := io.ReadAll(cf)
	if err != nil {
		t.Fatalf("read cloned: %v", err)
	}
	if !bytes.Equal(readData, payload) {
		t.Fatalf("cloned data mismatch: got %q, want %q", readData, payload)
	}
}

func TestReplayIncrementalAdvance(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	f, err := rec.Create("step.txt", "unspecified")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("part1")); err != nil {
		t.Fatalf("Write part1: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := f.Write([]byte("part2")); err != nil {
		t.Fatalf("Write part2: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync 2: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	events := rec.Events()
	state, err := replay.Build(events, 0)
	if err != nil {
		t.Fatalf("replay.Build(cut=0): %v", err)
	}
	defer state.Close()

	// Advance backwards should fail.
	if err := state.Advance(3); err != nil {
		t.Fatalf("Advance to 3: %v", err)
	}
	if err := state.Advance(1); err == nil {
		t.Fatalf("expected error advancing backwards, got nil")
	}

	// Advance to end.
	if err := state.Advance(uint64(len(events))); err != nil {
		t.Fatalf("Advance to end: %v", err)
	}

	cloned, err := state.Clone(100, 1)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	cf, err := cloned.Open("step.txt")
	if err != nil {
		t.Fatalf("Open step.txt: %v", err)
	}
	defer cf.Close()

	content, err := io.ReadAll(cf)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(content) != "part1part2" {
		t.Fatalf("content got %q, want %q", string(content), "part1part2")
	}
}

func TestReplayHandlesStayOpenDuringClone(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	f, err := rec.Create("open.txt", "unspecified")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("synced data")); err != nil {
		t.Fatalf("Write synced: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	df, err := rec.OpenDir("")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := df.Sync(); err != nil {
		t.Fatalf("dir Sync: %v", err)
	}
	if err := df.Close(); err != nil {
		t.Fatalf("dir Close: %v", err)
	}

	if _, err := f.Write([]byte(" trailing unsynced")); err != nil {
		t.Fatalf("Write unsynced: %v", err)
	}
	// Do NOT close f in trace before cutoff.

	events := rec.Events()
	state, err := replay.Build(events, uint64(len(events)))
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer state.Close()

	// 0% retention clone: unsynced trailing data lost.
	crashed0, err := state.Clone(0, 123)
	if err != nil {
		t.Fatalf("Clone(0): %v", err)
	}
	c0f, err := crashed0.Open("open.txt")
	if err != nil {
		t.Fatalf("Open crashed0: %v", err)
	}
	defer c0f.Close()
	data0, err := io.ReadAll(c0f)
	if err != nil {
		t.Fatalf("ReadAll c0: %v", err)
	}
	if string(data0) != "synced data" {
		t.Fatalf("0%% retention: got %q, want %q", string(data0), "synced data")
	}

	// 100% retention clone: trailing data retained.
	crashed100, err := state.Clone(100, 123)
	if err != nil {
		t.Fatalf("Clone(100): %v", err)
	}
	c100f, err := crashed100.Open("open.txt")
	if err != nil {
		t.Fatalf("Open crashed100: %v", err)
	}
	defer c100f.Close()
	data100, err := io.ReadAll(c100f)
	if err != nil {
		t.Fatalf("ReadAll c100: %v", err)
	}
	if string(data100) != "synced data trailing unsynced" {
		t.Fatalf("100%% retention: got %q, want %q", string(data100), "synced data trailing unsynced")
	}
}

func TestReplayNamespaceAndErrorOperations(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	if err := rec.MkdirAll("dir/sub", 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	f, err := rec.Create("dir/sub/file1.txt", "unspecified")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("test")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := rec.Link("dir/sub/file1.txt", "dir/sub/file2.txt"); err != nil {
		t.Fatalf("Link: %v", err)
	}

	if err := rec.Rename("dir/sub/file2.txt", "dir/sub/file3.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Remove nonexistent file to record error.
	_ = rec.Remove("dir/sub/nonexistent.txt")

	events := rec.Events()
	state, err := replay.Build(events, uint64(len(events)))
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer state.Close()

	cloned, err := state.Clone(100, 99)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}

	c3, err := cloned.Open("dir/sub/file3.txt")
	if err != nil {
		t.Fatalf("Open file3: %v", err)
	}
	defer c3.Close()
	content, _ := io.ReadAll(c3)
	if string(content) != "test" {
		t.Fatalf("content got %q, want %q", string(content), "test")
	}
}

func TestReplayCursorTrackingWithReadAndWrite(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	f, err := rec.OpenReadWrite("rw.txt", "unspecified")
	if err != nil {
		t.Fatalf("OpenReadWrite: %v", err)
	}
	if _, err := f.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen for reading.
	rf, err := rec.Open("rw.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, 3)
	n, err := rf.Read(buf)
	if err != nil || n != 3 {
		t.Fatalf("Read: n=%d, err=%v", n, err)
	}
	buf2 := make([]byte, 3)
	n2, err := rf.Read(buf2)
	if err != nil || n2 != 3 {
		t.Fatalf("Read 2: n=%d, err=%v", n2, err)
	}
	if err := rf.Close(); err != nil {
		t.Fatalf("Close rf: %v", err)
	}

	events := rec.Events()
	state, err := replay.Build(events, uint64(len(events)))
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer state.Close()
}

func TestReplayPebbleCrashRecovery(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            rec,
		Logger:        recovery.PanicLogger{},
		EventListener: &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	for i := 0; i < 50; i++ {
		rec.Begin(uint64(i+1), "set")
		k := []byte("key")
		v := []byte("val")
		if err := db.Set(k, v, pebble.Sync); err != nil {
			t.Fatalf("Set: %v", err)
		}
		rec.End(uint64(i+1), "set", "")
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	state, err := replay.Build(events, uint64(len(events)))
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer state.Close()

	cloned, err := state.Clone(100, 777)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}

	res := recovery.SafeOpen(cloned, "db", 10*time.Second)
	if res.DB == nil || res.Err != nil || res.Panic != "" {
		t.Fatalf("recovery.SafeOpen failed: err=%v, panic=%s, timeout=%v", res.Err, res.Panic, res.Timeout)
	}
	defer res.DB.Close()

	val, closer, err := res.DB.Get([]byte("key"))
	if err != nil {
		t.Fatalf("DB.Get: %v", err)
	}
	defer closer.Close()
	if string(val) != "val" {
		t.Fatalf("DB.Get got %q, want %q", string(val), "val")
	}
}

func TestReplayUnsupportedPolicy(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "a.txt", Handle: 1},
		{ID: 2, Kind: trace.KindClose, File: "a.txt", Handle: 1},
	}
	state, err := replay.Build(events, 2)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer state.Close()

	if _, err := state.Clone(50, 1); err == nil {
		t.Fatalf("expected error for 50%% retention, got nil")
	}
}
