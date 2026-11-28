package replay_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
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
	content, err := io.ReadAll(c3)
	if err != nil {
		t.Fatalf("ReadAll file3: %v", err)
	}
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

	cloned, err := state.Clone(100, 1)
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	cf, err := cloned.Open("rw.txt")
	if err != nil {
		t.Fatalf("cloned.Open(rw.txt): %v", err)
	}
	defer cf.Close()
	got, err := io.ReadAll(cf)
	if err != nil {
		t.Fatalf("ReadAll(rw.txt): %v", err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("replayed rw.txt content = %q, want %q", string(got), "abcdef")
	}
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

	for _, invalid := range []int{-1, 101} {
		if _, err := state.Clone(invalid, 1); err == nil {
			t.Fatalf("expected error for %d%% retention, got nil", invalid)
		}
	}

	if _, err := state.Clone(50, 1); err != nil {
		t.Fatalf("expected success for 50%% retention, got: %v", err)
	}
}

func computeFSSHA256(t *testing.T, fs vfs.FS) string {
	t.Helper()
	var walk func(dir string) (map[string][]byte, error)
	walk = func(dir string) (map[string][]byte, error) {
		res := make(map[string][]byte)
		entries, err := fs.List(dir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			p := e
			if dir != "" {
				p = dir + "/" + e
			}
			info, err := fs.Stat(p)
			if err != nil {
				return nil, err
			}
			if info.IsDir() {
				sub, err := walk(p)
				if err != nil {
					return nil, err
				}
				for k, v := range sub {
					res[k] = v
				}
			} else {
				f, err := fs.Open(p)
				if err != nil {
					return nil, err
				}
				content, err := io.ReadAll(f)
				_ = f.Close()
				if err != nil {
					return nil, err
				}
				h := sha256.Sum256(content)
				res[p] = h[:]
			}
		}
		return res, nil
	}

	files, err := walk("")
	if err != nil {
		t.Fatalf("computeFSSHA256 walk: %v", err)
	}

	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	topH := sha256.New()
	for _, k := range keys {
		topH.Write([]byte(k))
		topH.Write(files[k])
	}
	return hex.EncodeToString(topH.Sum(nil))
}

func TestReplayClonePartialRetentionSeedDeterminism(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	// Step 1: Synced base state with initial manifest and marker
	rec.Begin(1, "set")
	mf, err := rec.Create("MANIFEST-000001", "unspecified")
	if err != nil {
		t.Fatalf("Create base manifest: %v", err)
	}
	if _, err := mf.Write([]byte("synced manifest header")); err != nil {
		t.Fatalf("Write base manifest: %v", err)
	}
	if err := mf.Sync(); err != nil {
		t.Fatalf("Sync base manifest: %v", err)
	}
	if err := mf.Close(); err != nil {
		t.Fatalf("Close base manifest: %v", err)
	}

	df, err := rec.OpenDir("")
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}
	if err := df.Sync(); err != nil {
		t.Fatalf("Sync dir: %v", err)
	}
	if err := df.Close(); err != nil {
		t.Fatalf("Close dir: %v", err)
	}
	rec.End(1, "set", "")

	// Step 2: Create 20 files with unsynced directory entries (MANIFEST-*, marker.manifest.*, SSTs)
	// and unsynced multi-block writes (> 4096 bytes).
	rec.Begin(2, "set")
	for i := 0; i < 20; i++ {
		var fname string
		switch i % 3 {
		case 0:
			fname = fmt.Sprintf("0000%02d.sst", i)
		case 1:
			fname = fmt.Sprintf("MANIFEST-%06d", i)
		case 2:
			fname = fmt.Sprintf("marker.manifest.%06d", i)
		}

		f, err := rec.Create(fname, "unspecified")
		if err != nil {
			t.Fatalf("Create %s: %v", fname, err)
		}
		// Multi-block payload: 10KB (spanning multiple 4096-byte blocks)
		payload := bytes.Repeat([]byte{byte('A' + (i % 26))}, 10240)
		if _, err := f.Write(payload); err != nil {
			t.Fatalf("Write %s: %v", fname, err)
		}
		// Some files sync their data, but directory entries remain unsynced
		if i%2 == 0 {
			if err := f.Sync(); err != nil {
				t.Fatalf("Sync %s: %v", fname, err)
			}
		}
		// Unsynced multi-block append on some files
		if i%3 == 0 {
			if _, err := f.Write(bytes.Repeat([]byte{0xFF}, 4096)); err != nil {
				t.Fatalf("Write append %s: %v", fname, err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close %s: %v", fname, err)
		}
	}
	rec.End(2, "set", "")

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	state, err := replay.Build(events, uint64(len(events)))
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	defer state.Close()

	// Clone 32 times with seed 42 and percent 50
	digests := make(map[string]int)
	for r := 0; r < 32; r++ {
		cloned, err := state.Clone(50, 42)
		if err != nil {
			t.Fatalf("Clone(50, 42) round %d: %v", r, err)
		}
		digest := computeFSSHA256(t, cloned)
		digests[digest]++
	}

	if len(digests) != 1 {
		t.Fatalf("expected exactly 1 identical digest across 32 repeats of seed 42, got %d distinct digests: %v", len(digests), digests)
	}

	// Clone with a different seed (43) and verify distinct digest
	cloned43, err := state.Clone(50, 43)
	if err != nil {
		t.Fatalf("Clone(50, 43): %v", err)
	}
	digest43 := computeFSSHA256(t, cloned43)

	for d42 := range digests {
		if digest43 == d42 {
			t.Fatalf("expected seed 43 to produce a distinct digest from seed 42, but both produced %s", d42)
		}
	}
}

func TestDeterministicPartialCloneStatisticalFidelity(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	rec.Begin(1, "set")

	// 1. Creates and syncs a base file "db/synced.dat" (4,096 bytes) and syncs directory "db".
	if err := rec.MkdirAll("db", 0755); err != nil {
		t.Fatalf("MkdirAll db: %v", err)
	}
	if rootD, err := rec.OpenDir(""); err == nil {
		_ = rootD.Sync()
		_ = rootD.Close()
	}
	syncedData := bytes.Repeat([]byte{0x5A}, 4096)
	sf, err := rec.Create("db/synced.dat", "unspecified")
	if err != nil {
		t.Fatalf("Create synced.dat: %v", err)
	}
	if _, err := sf.Write(bytes.Clone(syncedData)); err != nil {
		t.Fatalf("Write synced.dat: %v", err)
	}
	if err := sf.Sync(); err != nil {
		t.Fatalf("Sync synced.dat: %v", err)
	}
	if err := sf.Close(); err != nil {
		t.Fatalf("Close synced.dat: %v", err)
	}

	// Creates "db/blocks.dat", syncs its creation in "db", and then writes 10 unsynced 4,096-byte blocks
	// at offsets 0, 4096, ..., 36864 without syncing the file.
	bf, err := rec.Create("db/blocks.dat", "unspecified")
	if err != nil {
		t.Fatalf("Create blocks.dat: %v", err)
	}
	if err := bf.Sync(); err != nil {
		t.Fatalf("Sync blocks.dat: %v", err)
	}
	if err := bf.Close(); err != nil {
		t.Fatalf("Close blocks.dat: %v", err)
	}

	// Sync directory "db" so "db/synced.dat" and "db/blocks.dat" are durably recorded in "db".
	d, err := rec.OpenDir("db")
	if err != nil {
		t.Fatalf("OpenDir db: %v", err)
	}
	if err := d.Sync(); err != nil {
		t.Fatalf("Sync dir db: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close dir db: %v", err)
	}

	// 2. Creates 10 unsynced files "db/unsynced-00" .. "db/unsynced-09" without syncing "db".
	unsyncedFiles := make([]string, 10)
	for i := 0; i < 10; i++ {
		unsyncedFiles[i] = fmt.Sprintf("db/unsynced-%02d", i)
		uf, err := rec.Create(unsyncedFiles[i], "unspecified")
		if err != nil {
			t.Fatalf("Create %s: %v", unsyncedFiles[i], err)
		}
		if _, err := uf.Write(fmt.Appendf(nil, "unsynced content %d", i)); err != nil {
			t.Fatalf("Write %s: %v", unsyncedFiles[i], err)
		}
		if err := uf.Close(); err != nil {
			t.Fatalf("Close %s: %v", unsyncedFiles[i], err)
		}
	}

	// 3. Writes 10 unsynced 4,096-byte blocks at offsets 0, 4096, ..., 36864 without syncing the file.
	bf, err = rec.OpenReadWrite("db/blocks.dat", "unspecified")
	if err != nil {
		t.Fatalf("OpenReadWrite blocks.dat: %v", err)
	}
	blocks := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		blocks[i] = bytes.Repeat([]byte{byte(i + 1)}, 4096)
		if _, err := bf.Write(bytes.Clone(blocks[i])); err != nil {
			t.Fatalf("Write block %d: %v", i, err)
		}
	}
	if err := bf.Close(); err != nil {
		t.Fatalf("Close blocks.dat: %v", err)
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

	// Construct equivalent live vfs.NewCrashableMem() state for raw MemFS.CrashClone comparison.
	createLiveMemFS := func() *vfs.MemFS {
		memLive := vfs.NewCrashableMem()
		if err := memLive.MkdirAll("db", 0755); err != nil {
			t.Fatalf("MkdirAll db: %v", err)
		}
		if rootD, err := memLive.OpenDir(""); err == nil {
			_ = rootD.Sync()
			_ = rootD.Close()
		}
		sf, err := memLive.Create("db/synced.dat", vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatalf("Create synced.dat: %v", err)
		}
		if _, err := sf.Write(bytes.Clone(syncedData)); err != nil {
			t.Fatalf("Write synced.dat: %v", err)
		}
		if err := sf.Sync(); err != nil {
			t.Fatalf("Sync synced.dat: %v", err)
		}
		if err := sf.Close(); err != nil {
			t.Fatalf("Close synced.dat: %v", err)
		}

		bf, err := memLive.Create("db/blocks.dat", vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatalf("Create blocks.dat: %v", err)
		}
		if err := bf.Sync(); err != nil {
			t.Fatalf("Sync blocks.dat: %v", err)
		}
		if err := bf.Close(); err != nil {
			t.Fatalf("Close blocks.dat: %v", err)
		}

		d, err := memLive.OpenDir("db")
		if err != nil {
			t.Fatalf("OpenDir db: %v", err)
		}
		if err := d.Sync(); err != nil {
			t.Fatalf("Sync dir db: %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("Close dir db: %v", err)
		}

		for i := 0; i < 10; i++ {
			uf, err := memLive.Create(unsyncedFiles[i], vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatalf("Create %s: %v", unsyncedFiles[i], err)
			}
			if _, err := uf.Write(fmt.Appendf(nil, "unsynced content %d", i)); err != nil {
				t.Fatalf("Write %s: %v", unsyncedFiles[i], err)
			}
			if err := uf.Close(); err != nil {
				t.Fatalf("Close %s: %v", unsyncedFiles[i], err)
			}
		}

		bf, err = memLive.OpenReadWrite("db/blocks.dat", vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatalf("OpenReadWrite blocks.dat: %v", err)
		}
		for i := 0; i < 10; i++ {
			if _, err := bf.Write(bytes.Clone(blocks[i])); err != nil {
				t.Fatalf("Write block %d: %v", i, err)
			}
		}
		if err := bf.Close(); err != nil {
			t.Fatalf("Close blocks.dat: %v", err)
		}
		return memLive
	}

	memLive := createLiveMemFS()
	policies := []int{25, 50, 75}
	totalSeeds := 200

	for _, pct := range policies {
		targetRate := float64(pct) / 100.0

		var totalDetEntry, totalDetBlock int
		var totalPebbleEntry, totalPebbleBlock int

		for seed := 1; seed <= totalSeeds; seed++ {
			// 1. Deterministic sampler (state.Clone)
			detCloned, err := state.Clone(pct, uint64(seed))
			if err != nil {
				t.Fatalf("Clone(%d, %d): %v", pct, seed, err)
			}

			// Synced file must be 100% retained across all clones
			cf, err := detCloned.Open("db/synced.dat")
			if err != nil {
				t.Fatalf("det pct %d seed %d: Open db/synced.dat: %v", pct, seed, err)
			}
			data, err := io.ReadAll(cf)
			_ = cf.Close()
			if err != nil || !bytes.Equal(data, syncedData) {
				t.Fatalf("det pct %d seed %d: db/synced.dat payload corrupt or incomplete", pct, seed)
			}

			for i := 0; i < 10; i++ {
				if _, err := detCloned.Stat(unsyncedFiles[i]); err == nil {
					totalDetEntry++
				}
			}

			bCloned, err := detCloned.Open("db/blocks.dat")
			if err != nil {
				t.Fatalf("det pct %d seed %d: Open db/blocks.dat: %v", pct, seed, err)
			}
			bData, err := io.ReadAll(bCloned)
			_ = bCloned.Close()
			if err != nil {
				t.Fatalf("det pct %d seed %d: ReadAll db/blocks.dat: %v", pct, seed, err)
			}
			for i := 0; i < 10; i++ {
				start := i * 4096
				end := start + 4096
				if len(bData) >= end && bytes.Equal(bData[start:end], blocks[i]) {
					totalDetBlock++
				}
			}

			// 2. Pebble raw MemFS.CrashClone sampler
			pebbleCloned := memLive.CrashClone(vfs.CrashCloneCfg{
				UnsyncedDataPercent: pct,
				RNG:                 rand.New(rand.NewPCG(0, uint64(seed))),
			})

			pcf, err := pebbleCloned.Open("db/synced.dat")
			if err != nil {
				t.Fatalf("pebble pct %d seed %d: Open db/synced.dat: %v", pct, seed, err)
			}
			pData, err := io.ReadAll(pcf)
			_ = pcf.Close()
			if err != nil || !bytes.Equal(pData, syncedData) {
				t.Fatalf("pebble pct %d seed %d: db/synced.dat payload corrupt or incomplete", pct, seed)
			}

			for i := 0; i < 10; i++ {
				if _, err := pebbleCloned.Stat(unsyncedFiles[i]); err == nil {
					totalPebbleEntry++
				}
			}

			pbCloned, err := pebbleCloned.Open("db/blocks.dat")
			if err != nil {
				t.Fatalf("pebble pct %d seed %d: Open db/blocks.dat: %v", pct, seed, err)
			}
			pbData, err := io.ReadAll(pbCloned)
			_ = pbCloned.Close()
			if err != nil {
				t.Fatalf("pebble pct %d seed %d: ReadAll db/blocks.dat: %v", pct, seed, err)
			}
			for i := 0; i < 10; i++ {
				start := i * 4096
				end := start + 4096
				if len(pbData) >= end && bytes.Equal(pbData[start:end], blocks[i]) {
					totalPebbleBlock++
				}
			}
		}

		rDetEntry := float64(totalDetEntry) / float64(10*totalSeeds)
		rDetBlock := float64(totalDetBlock) / float64(10*totalSeeds)
		rPebbleEntry := float64(totalPebbleEntry) / float64(10*totalSeeds)
		rPebbleBlock := float64(totalPebbleBlock) / float64(10*totalSeeds)

		// Assertions:
		// |R_det,entry(pct) - pct/100.0| <= 0.05 and |R_det,entry(pct) - R_pebble,entry(pct)| <= 0.05
		if math.Abs(rDetEntry-targetRate) > 0.05 {
			t.Fatalf("pct %d: |R_det,entry (%.4f) - target (%.2f)| > 0.05", pct, rDetEntry, targetRate)
		}
		if math.Abs(rDetEntry-rPebbleEntry) > 0.05 {
			t.Fatalf("pct %d: |R_det,entry (%.4f) - R_pebble,entry (%.4f)| > 0.05", pct, rDetEntry, rPebbleEntry)
		}

		// |R_det,block(pct) - pct/100.0| <= 0.05 and |R_det,block(pct) - R_pebble,block(pct)| <= 0.05
		if math.Abs(rDetBlock-targetRate) > 0.05 {
			t.Fatalf("pct %d: |R_det,block (%.4f) - target (%.2f)| > 0.05", pct, rDetBlock, targetRate)
		}
		if math.Abs(rDetBlock-rPebbleBlock) > 0.05 {
			t.Fatalf("pct %d: |R_det,block (%.4f) - R_pebble,block (%.4f)| > 0.05", pct, rDetBlock, rPebbleBlock)
		}

		// Repeating state.Clone(pct, 42) must yield one digest. Raw MemFS CrashClone
		// remains a comparison point, but a particular runtime is allowed to traverse
		// its maps in the same order across these diagnostic repetitions.
		detDigests := make(map[string]bool)
		for r := 0; r < 32; r++ {
			cloned, err := state.Clone(pct, 42)
			if err != nil {
				t.Fatalf("state.Clone(%d, 42): %v", pct, err)
			}
			detDigests[computeFSSHA256(t, cloned)] = true
		}
		if len(detDigests) != 1 {
			t.Fatalf("pct %d: expected 1 unique digest for state.Clone(pct, 42), got %d", pct, len(detDigests))
		}

		pebbleDigests := make(map[string]bool)
		for r := 0; r < 32; r++ {
			cloned := memLive.CrashClone(vfs.CrashCloneCfg{
				UnsyncedDataPercent: pct,
				RNG:                 rand.New(rand.NewPCG(0, 42)),
			})
			pebbleDigests[computeFSSHA256(t, cloned)] = true
		}
		t.Logf("pct %d: raw MemFS produced %d unique digests in 32 diagnostic repetitions", pct, len(pebbleDigests))
	}
}

func TestReplayPartialCloneInPlaceOverwriteAfterSync(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	dataA := bytes.Repeat([]byte("A"), 4096)
	dataB := bytes.Repeat([]byte("B"), 4096)

	// 1. Synced directory case for testing state.Clone(0, seed)
	if err := rec.MkdirAll("synced_dir", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	rootD, err := rec.OpenDir("")
	if err != nil {
		t.Fatalf("OpenDir root failed: %v", err)
	}
	if err := rootD.Sync(); err != nil {
		t.Fatalf("Sync root failed: %v", err)
	}
	if err := rootD.Close(); err != nil {
		t.Fatalf("Close root failed: %v", err)
	}
	f1, err := rec.Create("synced_dir/file.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := f1.Write(bytes.Clone(dataA)); err != nil {
		t.Fatalf("Write dataA failed: %v", err)
	}
	if err := f1.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := f1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	d1, err := rec.OpenDir("synced_dir")
	if err != nil {
		t.Fatalf("OpenDir failed: %v", err)
	}
	if err := d1.Sync(); err != nil {
		t.Fatalf("Sync dir failed: %v", err)
	}
	if err := d1.Close(); err != nil {
		t.Fatalf("Close dir failed: %v", err)
	}
	// Reopen with OpenReadWrite and overwrite offset 0 with BBBB without syncing
	f1b, err := rec.OpenReadWrite("synced_dir/file.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("OpenReadWrite failed: %v", err)
	}
	if _, err := f1b.Write(bytes.Clone(dataB)); err != nil {
		t.Fatalf("Write dataB failed: %v", err)
	}
	if err := f1b.Close(); err != nil {
		t.Fatalf("Close f1b failed: %v", err)
	}

	// 2. Unsynced directory case for testing deterministicPartialClone baseline
	if err := rec.MkdirAll("unsynced_dir", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	f2, err := rec.Create("unsynced_dir/file.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := f2.Write(bytes.Clone(dataA)); err != nil {
		t.Fatalf("Write dataA failed: %v", err)
	}
	if err := f2.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := f2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	// Reopen with OpenReadWrite and overwrite offset 0 with BBBB without syncing
	f2b, err := rec.OpenReadWrite("unsynced_dir/file.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("OpenReadWrite failed: %v", err)
	}
	if _, err := f2b.Write(bytes.Clone(dataB)); err != nil {
		t.Fatalf("Write dataB failed: %v", err)
	}
	if err := f2b.Close(); err != nil {
		t.Fatalf("Close f2b failed: %v", err)
	}

	events := rec.Events()
	state, err := replay.Replay(events)
	if err != nil {
		t.Fatalf("replay.Replay failed: %v", err)
	}
	defer state.Close()

	// Verify state.Clone(0, seed): synced directory file must contain dataA (AAAA), not dataB (BBBB)
	cl0, err := state.Clone(0, 42)
	if err != nil {
		t.Fatalf("state.Clone(0, 42) failed: %v", err)
	}
	f0, err := cl0.Open("synced_dir/file.txt")
	if err != nil {
		t.Fatalf("open synced_dir/file.txt in Clone(0): %v", err)
	}
	buf0, err := io.ReadAll(f0)
	_ = f0.Close()
	if err != nil {
		t.Fatalf("read Clone(0) content: %v", err)
	}
	if !bytes.Equal(buf0, dataA) {
		t.Fatalf("Clone(0) content mismatch: got %q, want dataA (AAAA)", buf0[:16])
	}

	// Verify state.Clone(50, seed):
	// Test both files across 64 seeds to verify that unretained blocks contain dataA (AAAA) rather than dataB (BBBB)
	sawUnretainedSynced := false
	sawRetainedSynced := false
	sawUnretainedUnsynced := false
	sawRetainedUnsynced := false

	for seed := uint64(1); seed <= 64; seed++ {
		cl50, err := state.Clone(50, seed)
		if err != nil {
			t.Fatalf("state.Clone(50, %d) failed: %v", seed, err)
		}

		// Check synced_dir file:
		fSynced, err := cl50.Open("synced_dir/file.txt")
		if err != nil {
			t.Fatalf("seed %d: open synced_dir/file.txt: %v", seed, err)
		}
		b, readErr := io.ReadAll(fSynced)
		closeErr := fSynced.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatalf("seed %d: read synced_dir/file.txt: %v", seed, err)
		}
		if bytes.Equal(b, dataA) {
			sawUnretainedSynced = true
		} else if bytes.Equal(b, dataB) {
			sawRetainedSynced = true
		} else {
			t.Fatalf("seed %d: synced_dir/file.txt contains corrupted bytes: %q", seed, b[:16])
		}

		// Check unsynced_dir file (when directory entry is retained):
		fUnsynced, err := cl50.Open("unsynced_dir/file.txt")
		if err == nil {
			b, readErr := io.ReadAll(fUnsynced)
			closeErr := fUnsynced.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatalf("seed %d: read unsynced_dir/file.txt: %v", seed, err)
			}
			if bytes.Equal(b, dataA) {
				sawUnretainedUnsynced = true
			} else if bytes.Equal(b, dataB) {
				sawRetainedUnsynced = true
			} else {
				t.Fatalf("seed %d: unsynced_dir/file.txt contains corrupted bytes: %q", seed, b[:16])
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("seed %d: open unsynced_dir/file.txt unexpected error: %v", seed, err)
		}
	}

	if !sawUnretainedSynced || !sawRetainedSynced {
		t.Fatalf("expected both unretained (AAAA) and retained (BBBB) blocks for synced_dir across 64 seeds; sawUnretained=%v sawRetained=%v", sawUnretainedSynced, sawRetainedSynced)
	}
	if !sawUnretainedUnsynced || !sawRetainedUnsynced {
		t.Fatalf("expected both unretained (AAAA) and retained (BBBB) blocks for unsynced_dir across 64 seeds; sawUnretained=%v sawRetained=%v", sawUnretainedUnsynced, sawRetainedUnsynced)
	}
}

func TestReplayPartialCloneSyncAfterNamespaceChange(t *testing.T) {
	for _, operation := range []string{"rename", "link", "directory rename"} {
		t.Run(operation, func(t *testing.T) {
			recorder := vfsrec.New(vfs.NewCrashableMem())
			if err := recorder.MkdirAll("before", 0755); err != nil {
				t.Fatal(err)
			}
			file, err := recorder.Create("before/data", vfs.WriteCategoryUnspecified)
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte("durable bytes after namespace change")
			if _, err := file.Write(bytes.Clone(payload)); err != nil {
				t.Fatal(err)
			}
			target := "before/alias"
			switch operation {
			case "rename":
				err = recorder.Rename("before/data", target)
			case "link":
				err = recorder.Link("before/data", target)
			case "directory rename":
				err = recorder.Rename("before", "after")
				target = "after/data"
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			state, err := replay.Replay(recorder.Events())
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			retained := 0
			for seed := uint64(1); seed <= 64; seed++ {
				cloned, err := state.Clone(50, seed)
				if err != nil {
					t.Fatal(err)
				}
				clonedFile, err := cloned.Open(target)
				if err != nil {
					continue
				}
				retained++
				data, err := io.ReadAll(clonedFile)
				if closeErr := clonedFile.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(data, payload) {
					t.Fatalf("seed %d: retained %s lost synced bytes: got %q, want %q", seed, target, data, payload)
				}
			}
			if retained == 0 {
				t.Fatal("no retained alias was checked")
			}
		})
	}
}

func TestReplayEndpointEquivalenceAndBoundaryContainment(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	// Ensure root directory survives at 0% retention.
	rootD, err := rec.OpenDir("")
	if err != nil {
		t.Fatalf("OpenDir root: %v", err)
	}
	if err := rootD.Sync(); err != nil {
		t.Fatalf("Sync root: %v", err)
	}
	_ = rootD.Close()

	// 1. Synced directory with partially overwritten file and fully synced file
	if err := rec.MkdirAll("synced_dir", 0755); err != nil {
		t.Fatalf("MkdirAll synced_dir: %v", err)
	}
	dSync, err := rec.OpenDir("synced_dir")
	if err != nil {
		t.Fatalf("OpenDir synced_dir: %v", err)
	}
	if err := dSync.Sync(); err != nil {
		t.Fatalf("Sync synced_dir: %v", err)
	}
	_ = dSync.Close()

	dataA := bytes.Repeat([]byte("A"), 8192)
	dataB := bytes.Repeat([]byte("B"), 4096)
	f1, err := rec.Create("synced_dir/file1.dat", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create file1: %v", err)
	}
	if _, err := f1.Write(dataA); err != nil {
		t.Fatalf("Write file1: %v", err)
	}
	if err := f1.Sync(); err != nil {
		t.Fatalf("Sync file1: %v", err)
	}
	_ = f1.Close()

	// Partially overwrite block 0 of file1.dat with BBBB without syncing
	f1rw, err := rec.OpenReadWrite("synced_dir/file1.dat", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("OpenReadWrite file1: %v", err)
	}
	if _, err := f1rw.Write(dataB); err != nil {
		t.Fatalf("Write file1rw: %v", err)
	}
	_ = f1rw.Close()

	// Fully synced file
	f2, err := rec.Create("synced_dir/file2.dat", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create file2: %v", err)
	}
	if _, err := f2.Write(bytes.Repeat([]byte("X"), 4096)); err != nil {
		t.Fatalf("Write file2: %v", err)
	}
	if err := f2.Sync(); err != nil {
		t.Fatalf("Sync file2: %v", err)
	}
	_ = f2.Close()

	// 2. Unsynced directory and unsynced file
	if err := rec.MkdirAll("unsynced_dir", 0755); err != nil {
		t.Fatalf("MkdirAll unsynced_dir: %v", err)
	}
	f3, err := rec.Create("unsynced_dir/file3.dat", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create file3: %v", err)
	}
	if _, err := f3.Write(bytes.Repeat([]byte("Y"), 4096)); err != nil {
		t.Fatalf("Write file3: %v", err)
	}
	_ = f3.Close()

	// 3. Unsynced file in root
	f4, err := rec.Create("unsynced_root.dat", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create unsynced_root: %v", err)
	}
	if _, err := f4.Write(bytes.Repeat([]byte("Z"), 4096)); err != nil {
		t.Fatalf("Write unsynced_root: %v", err)
	}
	_ = f4.Close()

	events := rec.Events()
	state, err := replay.Replay(events)
	if err != nil {
		t.Fatalf("replay.Replay: %v", err)
	}
	defer state.Close()

	seeds := []uint64{0, 1, 42, 999}

	// (a) state.Clone(100, seed) produces the exact same SHA-256 filesystem digest regardless of seed
	var digest100Ref string
	for i, seed := range seeds {
		c100, err := state.Clone(100, seed)
		if err != nil {
			t.Fatalf("Clone(100, %d): %v", seed, err)
		}
		digest := computeFSSHA256(t, c100)
		if i == 0 {
			digest100Ref = digest
		} else if digest != digest100Ref {
			t.Fatalf("Clone(100, %d) digest %s != reference %s", seed, digest, digest100Ref)
		}
	}

	// (b) deterministicPartialClone with percent = 0 produces exact same digest as state.Clone(0, seed),
	// and with percent = 100 produces exact same digest as state.Clone(100, seed).
	for _, seed := range seeds {
		c0, err := state.Clone(0, seed)
		if err != nil {
			t.Fatalf("Clone(0, %d): %v", seed, err)
		}
		digest0 := computeFSSHA256(t, c0)

		base0 := state.MemFSForTesting().CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
		full100 := state.MemFSForTesting().CrashClone(vfs.CrashCloneCfg{
			UnsyncedDataPercent: 100,
			RNG:                 rand.New(rand.NewPCG(0, 0)),
		})

		dp0, err := replay.DeterministicPartialCloneForTesting(base0, full100, 0, rand.New(rand.NewPCG(0, seed)), state.FileSyncedBytesForTesting())
		if err != nil {
			t.Fatalf("deterministicPartialClone(0, seed=%d): %v", seed, err)
		}
		digestDP0 := computeFSSHA256(t, dp0)
		if digestDP0 != digest0 {
			t.Fatalf("seed %d: deterministicPartialClone(0) digest %s != Clone(0) %s", seed, digestDP0, digest0)
		}

		dp100, err := replay.DeterministicPartialCloneForTesting(base0, full100, 100, rand.New(rand.NewPCG(0, seed)), state.FileSyncedBytesForTesting())
		if err != nil {
			t.Fatalf("deterministicPartialClone(100, seed=%d): %v", seed, err)
		}
		digestDP100 := computeFSSHA256(t, dp100)
		if digestDP100 != digest100Ref {
			t.Fatalf("seed %d: deterministicPartialClone(100) digest %s != Clone(100) %s", seed, digestDP100, digest100Ref)
		}
	}

	// (c) For any intermediate policy pct in {25, 50, 75} and any seed:
	// - every file/directory path in Clone(0, seed) is present in Clone(pct, seed)
	// - every path in Clone(pct, seed) is present in Clone(100, seed)
	// - every 4KB block of every retained file in Clone(pct, seed) equals either its synced 4KB block
	//   in Clone(0, seed) (fileSyncedBytes) or its live 4KB block in Clone(100, seed).
	collectPaths := func(fs vfs.FS) map[string]bool {
		paths := make(map[string]bool)
		var walk func(dir string)
		walk = func(dir string) {
			entries, err := fs.List(dir)
			if err != nil {
				return
			}
			for _, e := range entries {
				p := e
				if dir != "" {
					p = dir + "/" + e
				}
				paths[p] = true
				if fi, err := fs.Stat(p); err == nil && fi.IsDir() {
					walk(p)
				}
			}
		}
		walk("")
		return paths
	}

	policies := []int{25, 50, 75}
	for _, pct := range policies {
		for _, seed := range seeds {
			fs0, err := state.Clone(0, seed)
			if err != nil {
				t.Fatalf("Clone(0, %d): %v", seed, err)
			}
			fsPct, err := state.Clone(pct, seed)
			if err != nil {
				t.Fatalf("Clone(%d, %d): %v", pct, seed, err)
			}
			fs100, err := state.Clone(100, seed)
			if err != nil {
				t.Fatalf("Clone(100, %d): %v", seed, err)
			}

			paths0 := collectPaths(fs0)
			pathsPct := collectPaths(fsPct)
			paths100 := collectPaths(fs100)

			// Every path in Clone(0) must be present in Clone(pct)
			for p := range paths0 {
				if !pathsPct[p] {
					t.Fatalf("pct %d seed %d: path %s in Clone(0) missing from Clone(%d)", pct, seed, p, pct)
				}
			}

			// Every path in Clone(pct) must be present in Clone(100)
			for p := range pathsPct {
				if !paths100[p] {
					t.Fatalf("pct %d seed %d: path %s in Clone(%d) missing from Clone(100)", pct, seed, p, pct)
				}
			}

			// Check block containment for every regular file in Clone(pct)
			for p := range pathsPct {
				fi, err := fsPct.Stat(p)
				if err != nil || fi.IsDir() {
					continue
				}

				fPct, err := fsPct.Open(p)
				if err != nil {
					t.Fatalf("Open %s in Clone(%d): %v", p, pct, err)
				}
				bytesPct, readErr := io.ReadAll(fPct)
				closeErr := fPct.Close()
				if err := errors.Join(readErr, closeErr); err != nil {
					t.Fatalf("ReadAll %s in Clone(%d): %v", p, pct, err)
				}

				var bytes0 []byte
				if f0, err := fs0.Open(p); err == nil {
					bytes0, readErr = io.ReadAll(f0)
					closeErr = f0.Close()
					if err := errors.Join(readErr, closeErr); err != nil {
						t.Fatalf("ReadAll %s in Clone(0): %v", p, err)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("Open %s in Clone(0): %v", p, err)
				} else if state.FileSyncedBytesForTesting() != nil {
					bytes0 = state.FileSyncedBytesForTesting()[p]
				}

				f100, err := fs100.Open(p)
				if err != nil {
					t.Fatalf("Open %s in Clone(100): %v", p, err)
				}
				bytes100, readErr := io.ReadAll(f100)
				closeErr = f100.Close()
				if err := errors.Join(readErr, closeErr); err != nil {
					t.Fatalf("ReadAll %s in Clone(100): %v", p, err)
				}

				for off := 0; off < len(bytesPct); off += 4096 {
					endPct := off + 4096
					if endPct > len(bytesPct) {
						endPct = len(bytesPct)
					}
					chunkPct := bytesPct[off:endPct]

					var chunk0 []byte
					if off < len(bytes0) {
						end0 := off + 4096
						if end0 > len(bytes0) {
							end0 = len(bytes0)
						}
						chunk0 = bytes0[off:end0]
					}

					var chunk100 []byte
					if off < len(bytes100) {
						end100 := off + 4096
						if end100 > len(bytes100) {
							end100 = len(bytes100)
						}
						chunk100 = bytes100[off:end100]
					}

					if !bytes.Equal(chunkPct, chunk0) && !bytes.Equal(chunkPct, chunk100) {
						t.Fatalf("pct %d seed %d file %s block [%d:%d]: chunk neither matches synced block (%q) nor live block (%q); got %q",
							pct, seed, p, off, endPct, chunk0, chunk100, chunkPct)
					}
				}
			}
		}
	}

	// (d) Scan seed 1..200 comparing paths25 and paths75 to directly demonstrate the documented
	// intermediate p1 < p2 PRNG shift where unsynced directory retention at 75% consumes a child RNG
	// draw for file3.dat, shifting the subsequent draw so unsynced_root.dat is retained at 25% but dropped at 75%.
	var sawNonSubsetShift bool
	for seed := uint64(1); seed <= 200; seed++ {
		fs25, err := state.Clone(25, seed)
		if err != nil {
			t.Fatalf("Clone(25, %d): %v", seed, err)
		}
		fs75, err := state.Clone(75, seed)
		if err != nil {
			t.Fatalf("Clone(75, %d): %v", seed, err)
		}

		paths25 := collectPaths(fs25)
		paths75 := collectPaths(fs75)

		if !paths25["unsynced_dir"] && paths75["unsynced_dir"] &&
			paths25["unsynced_root.dat"] && !paths75["unsynced_root.dat"] {
			sawNonSubsetShift = true
			break
		}
	}
	if !sawNonSubsetShift {
		t.Fatalf("expected at least one seed in 1..200 exhibiting intermediate non-subset PRNG shift (!unsynced_dir at 25%%, +unsynced_dir at 75%%, +unsynced_root.dat at 25%%, !unsynced_root.dat at 75%%)")
	}
}

func TestReplaySyncToNonFullSyncDoesNotSnapshot(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	rec.Begin(1, "set")
	f, err := rec.Create("syncto_test.dat", "unspecified")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	payload := []byte("hello syncto non-full-sync data")
	if _, err := f.Write(bytes.Clone(payload)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// In MemFS, SyncTo returns (false, nil), recording FullSync: false.
	fullSync, err := f.SyncTo(4)
	if err != nil {
		t.Fatalf("SyncTo: %v", err)
	}
	if fullSync {
		t.Fatalf("expected MemFS SyncTo to return fullSync=false")
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

	syncedMap := state.FileSyncedBytesForTesting()
	if snap, exists := syncedMap["syncto_test.dat"]; exists && len(snap) > 0 {
		t.Fatalf("expected syncto_test.dat prefix not to be snapshotted in fileSyncedBytes when FullSync is false, but got: %q", string(snap))
	}

	// Verify that replaying events2 (where KindSyncTo has FullSync=true) directly into raw MemFS
	// returns an error containing "syncTo fullSync mismatch" because raw MemFS returns fullSync=false.
	events2 := make([]trace.Event, len(events))
	copy(events2, events)
	for i := range events2 {
		if events2[i].Kind == trace.KindSyncTo {
			events2[i].Size = 5
			events2[i].FullSync = true
		}
	}
	if _, err := replay.Build(events2, uint64(len(events2))); err == nil || !strings.Contains(err.Error(), "syncTo fullSync mismatch") {
		t.Fatalf("expected 'syncTo fullSync mismatch' error from replay.Build on raw MemFS with FullSync=true, got: %v", err)
	}

	// Replay events2 up to cutoff 3, wrap handle 1 with fullSyncTestFile, advance, and verify snapshot.
	state2, err := replay.Build(events2, 3)
	if err != nil {
		t.Fatalf("replay.Build up to cutoff 3: %v", err)
	}
	defer state2.Close()
	state2.WrapHandleForTesting(1, func(inner vfs.File) vfs.File {
		return fullSyncTestFile{File: inner}
	})
	if err := state2.Advance(uint64(len(events2))); err != nil {
		t.Fatalf("state2.Advance: %v", err)
	}

	syncedMap2 := state2.FileSyncedBytesForTesting()
	if snapshot := syncedMap2["syncto_test.dat"]; !bytes.Equal(snapshot, payload) {
		t.Fatalf("FullSync must snapshot the entire file: got %q, want %q", snapshot, payload)
	}
}

type fullSyncTestFile struct {
	vfs.File
}

func (f fullSyncTestFile) SyncTo(length int64) (bool, error) {
	err := f.File.Sync()
	return err == nil, err
}

func TestDeterministicPartialCloneUnsyncedTruncation(t *testing.T) {
	mem := vfs.NewCrashableMem()
	f, err := mem.Create("test.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	syncedData := []byte("original long content that was synced")
	if _, err := f.Write(bytes.Clone(syncedData)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := mem.OpenDir("")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()

	base0 := mem.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	full100 := mem.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 100, RNG: rand.New(rand.NewPCG(0, 0))})
	f2, err := full100.Create("test.txt", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	shortData := []byte("short")
	if _, err := f2.Write(bytes.Clone(shortData)); err != nil {
		t.Fatal(err)
	}
	_ = f2.Close()

	syncedMap := map[string][]byte{"test.txt": syncedData}

	// With 0% retention, truncation must not persist
	rng0 := rand.New(rand.NewPCG(0, 42))
	clone0, err := replay.DeterministicPartialCloneForTesting(base0, full100, 0, rng0, syncedMap)
	if err != nil {
		t.Fatal(err)
	}
	cf, err := clone0.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	got0, err := io.ReadAll(cf)
	if closeErr := cf.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got0, syncedData) {
		t.Fatalf("percent=0: expected synced data %q, got %q", string(syncedData), string(got0))
	}

	// With 100% retention, truncation must persist
	rng100 := rand.New(rand.NewPCG(0, 42))
	clone100, err := replay.DeterministicPartialCloneForTesting(base0, full100, 100, rng100, syncedMap)
	if err != nil {
		t.Fatal(err)
	}
	cf100, err := clone100.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	got100, err := io.ReadAll(cf100)
	if closeErr := cf100.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got100, shortData) {
		t.Fatalf("percent=100: expected short data %q, got %q", string(shortData), string(got100))
	}

	// Verify 50% truncation via State.Clone across seeds: each clone has either full syncedData or shortData.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "test.txt", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "test.txt", Handle: 1, Data: bytes.Clone(syncedData), Len: len(syncedData)},
		{ID: 3, Kind: trace.KindSync, File: "test.txt", Handle: 1},
		{ID: 4, Kind: trace.KindClose, File: "test.txt", Handle: 1},
		{ID: 5, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 6, Kind: trace.KindSync, File: "", Handle: 2, Dir: true},
		{ID: 7, Kind: trace.KindClose, File: "", Handle: 2, Dir: true},
		{ID: 8, Kind: trace.KindCreate, File: "test.txt", Handle: 3},
		{ID: 9, Kind: trace.KindWrite, File: "test.txt", Handle: 3, Data: bytes.Clone(shortData), Len: len(shortData)},
		{ID: 10, Kind: trace.KindClose, File: "test.txt", Handle: 3},
	}
	state, err := replay.Replay(events)
	if err != nil {
		t.Fatalf("Replay truncation trace: %v", err)
	}
	defer state.Close()

	var sawFull, sawShort bool
	for seed := uint64(1); seed <= 64; seed++ {
		cl50, err := state.Clone(50, seed)
		if err != nil {
			t.Fatalf("Clone(50, %d): %v", seed, err)
		}
		f50, err := cl50.Open("test.txt")
		if err != nil {
			t.Fatalf("seed %d Open(test.txt): %v", seed, err)
		}
		got50, err := io.ReadAll(f50)
		if closeErr := f50.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case bytes.Equal(got50, syncedData):
			sawFull = true
		case bytes.Equal(got50, shortData):
			sawShort = true
		default:
			t.Fatalf("seed %d: unexpected hybrid truncation content %q", seed, got50)
		}
	}
	if !sawFull || !sawShort {
		t.Fatalf("expected both full and truncated outcomes at 50%%, sawFull=%v sawShort=%v", sawFull, sawShort)
	}
}

func TestReplaySafeClose(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "f1.txt", Handle: 1},
		{ID: 2, Kind: trace.KindCreate, File: "f2.txt", Handle: 2},
	}
	state, err := replay.Build(events, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	// Calling Close a second time must succeed without panic
	if err := state.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestReplayWriteBoundsCheck(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "f.txt", Handle: 1},
	}
	state, err := replay.Build(events, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	// Defect 2.9: KindWrite with event.Len > len(Data) must not panic with slice out of bounds.
	corruptWrite := trace.Event{
		ID:     2,
		Kind:   trace.KindWrite,
		File:   "f.txt",
		Handle: 1,
		Data:   []byte("hello"),
		Len:    100, // exceeds len(Data) = 5
	}
	errWrite := state.AdvanceStepForTesting(corruptWrite)
	if errWrite == nil {
		t.Errorf("expected error on corrupt write length, got nil")
	}

	// KindWriteAt with event.Len > len(Data) must also error cleanly.
	corruptWriteAt := trace.Event{
		ID:     3,
		Kind:   trace.KindWriteAt,
		File:   "f.txt",
		Handle: 1,
		Data:   []byte("world"),
		Len:    100, // exceeds len(Data) = 5
	}
	errWriteAt := state.AdvanceStepForTesting(corruptWriteAt)
	if errWriteAt == nil {
		t.Errorf("expected error on corrupt writeAt length, got nil")
	}
}

func TestReplayHardLinkZeroReadAndUnlinkedZeroHandle(t *testing.T) {
	t.Run("HardLinkedAliasesStayByteIdenticalOnPartialClone", func(t *testing.T) {
		blockA := bytes.Repeat([]byte("A"), 4096)
		blockZ := bytes.Repeat([]byte("Z"), 4096)
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 1, Dir: true},
			{ID: 3, Kind: trace.KindSync, File: "", Handle: 1, Dir: true},
			{ID: 4, Kind: trace.KindOpenDir, File: "db", Handle: 2, Dir: true},
			{ID: 5, Kind: trace.KindCreate, File: "db/a.sst", Handle: 3, Cat: "pebble-sst"},
			{ID: 6, Kind: trace.KindWrite, File: "db/a.sst", Handle: 3, Data: blockA, Len: len(blockA)},
			{ID: 7, Kind: trace.KindSyncData, File: "db/a.sst", Handle: 3},
			{ID: 8, Kind: trace.KindClose, File: "db/a.sst", Handle: 3},
			{ID: 9, Kind: trace.KindLink, File: "db/a.sst", File2: "db/b.sst"},
			{ID: 10, Kind: trace.KindSync, File: "db", Handle: 2, Dir: true},
			{ID: 11, Kind: trace.KindOpenReadWrite, File: "db/a.sst", Handle: 4, Cat: "pebble-sst"},
			{ID: 12, Kind: trace.KindWriteAt, File: "db/a.sst", Handle: 4, Off: 0, Data: blockZ, Len: len(blockZ)},
			{ID: 13, Kind: trace.KindClose, File: "db/a.sst", Handle: 4},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatalf("replay.Replay: %v", err)
		}
		defer state.Close()

		for seed := uint64(0); seed < 10; seed++ {
			fs50, err := state.Clone(50, seed)
			if err != nil {
				t.Fatalf("Clone(50, %d): %v", seed, err)
			}
			fa, err := fs50.Open("db/a.sst")
			if err != nil {
				t.Fatalf("open db/a.sst: %v", err)
			}
			ba, readErr := io.ReadAll(fa)
			closeErr := fa.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatalf("read db/a.sst: %v", err)
			}

			fb, err := fs50.Open("db/b.sst")
			if err != nil {
				t.Fatalf("open db/b.sst: %v", err)
			}
			bb, readErr := io.ReadAll(fb)
			closeErr = fb.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatalf("read db/b.sst: %v", err)
			}

			if !bytes.Equal(ba, bb) {
				t.Fatalf("seed %d: hard-linked aliases diverged under 50%% clone", seed)
			}
		}
	})

	t.Run("ZeroSizeReadChecksRecordedError", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "db.txt", Handle: 1},
			{ID: 2, Kind: trace.KindWrite, File: "db.txt", Handle: 1, Data: []byte("hi"), Len: 2},
			{ID: 3, Kind: trace.KindClose, File: "db.txt", Handle: 1},
			{ID: 4, Kind: trace.KindOpenReadWrite, File: "db.txt", Handle: 2},
			{ID: 5, Kind: trace.KindRead, File: "db.txt", Handle: 2, Size: 0, Err: "injected read error"},
		}
		if _, err := replay.Replay(events); err == nil {
			t.Fatal("expected error mismatch on zero-byte KindRead with non-empty Err")
		}
	})

	t.Run("UnlinkedOpenZeroByteHandleSnapshot", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "tmp.txt", Handle: 1},
			{ID: 2, Kind: trace.KindRemove, File: "tmp.txt"},
			{ID: 3, Kind: trace.KindSyncData, File: "tmp.txt", Handle: 1},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatalf("replay.Replay: %v", err)
		}
		defer state.Close()
		if len(state.FileSyncedBytesForTesting()["tmp.txt"]) != 0 {
			t.Fatalf("expected empty snapshot for unlinked 0-byte file, got %q", state.FileSyncedBytesForTesting()["tmp.txt"])
		}
	})
}

func TestReplayPartialWritesAndBoundsValidation(t *testing.T) {
	t.Run("PartialKindWriteAndKindWriteAt", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "data.txt", Handle: 1},
			{ID: 2, Kind: trace.KindWrite, File: "data.txt", Handle: 1, Off: 0, Data: []byte("abcd"), Len: 2, Err: "short write"},
			{ID: 3, Kind: trace.KindWrite, File: "data.txt", Handle: 1, Off: 2, Data: []byte("ef"), Len: 2},
			{ID: 4, Kind: trace.KindWriteAt, File: "data.txt", Handle: 1, Off: 1, Data: []byte("XYZW"), Len: 2, Err: "short writeAt"},
			{ID: 5, Kind: trace.KindWrite, File: "data.txt", Handle: 1, Off: 4, Data: []byte("zz"), Len: 0, Err: "enospc"},
			{ID: 6, Kind: trace.KindClose, File: "data.txt", Handle: 1},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatalf("replay.Replay partial writes: %v", err)
		}
		defer state.Close()

		cloned, err := state.Clone(100, 1)
		if err != nil {
			t.Fatalf("Clone(100): %v", err)
		}
		f, err := cloned.Open("data.txt")
		if err != nil {
			t.Fatalf("Open(data.txt): %v", err)
		}
		defer f.Close()
		got, err := io.ReadAll(f)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != "aXYf" {
			t.Fatalf("partial write replay content = %q, want %q", string(got), "aXYf")
		}
	})

	t.Run("ErrorBearingWriteBoundsCheck", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "f.txt", Handle: 1},
		}
		state, err := replay.Build(events, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer state.Close()

		badCases := []trace.Event{
			{ID: 2, Kind: trace.KindWrite, File: "f.txt", Handle: 1, Data: []byte("hi"), Len: 5, Err: "corrupt"},
			{ID: 2, Kind: trace.KindWriteAt, File: "f.txt", Handle: 1, Off: 0, Data: []byte("hi"), Len: 5, Err: "corrupt"},
			{ID: 2, Kind: trace.KindWrite, File: "f.txt", Handle: 1, Data: []byte("hi"), Len: -1, Err: "neg"},
			{ID: 2, Kind: trace.KindWriteAt, File: "f.txt", Handle: 1, Off: -1, Data: []byte("hi"), Len: 2},
			{ID: 2, Kind: trace.KindWrite, File: "f.txt", Handle: 1, Data: []byte("hi"), Len: 1},
		}
		for i, ev := range badCases {
			if err := state.AdvanceStepForTesting(ev); err == nil {
				t.Fatalf("badCases[%d]: expected error, got nil", i)
			}
		}
	})
}

func TestReplayReuseForWritePreservesSnapshotAndLinks(t *testing.T) {
	blockA := bytes.Repeat([]byte("A"), 4096)
	blockB := bytes.Repeat([]byte("B"), 4096)

	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "old.log", Handle: 1, Cat: "pebble-wal"},
		{ID: 2, Kind: trace.KindWrite, File: "old.log", Handle: 1, Data: blockA, Len: len(blockA)},
		{ID: 3, Kind: trace.KindSyncData, File: "old.log", Handle: 1},
		{ID: 4, Kind: trace.KindClose, File: "old.log", Handle: 1},
		{ID: 5, Kind: trace.KindLink, File: "old.log", File2: "alias.log"},
		{ID: 6, Kind: trace.KindReuseForWrite, File: "old.log", File2: "new.log", Handle: 2, Cat: "pebble-wal"},
		{ID: 7, Kind: trace.KindWrite, File: "new.log", Handle: 2, Data: blockB, Len: len(blockB)},
		{ID: 8, Kind: trace.KindClose, File: "new.log", Handle: 2},
	}

	state, err := replay.Replay(events)
	if err != nil {
		t.Fatalf("replay.Replay: %v", err)
	}
	defer state.Close()

	synced := state.FileSyncedBytesForTesting()
	if !bytes.Equal(synced["new.log"], blockA) {
		t.Fatalf("new.log synced snapshot len=%d, want %d (blockA)", len(synced["new.log"]), len(blockA))
	}

	sawNew := false
	for seed := uint64(1); seed <= 64; seed++ {
		fs50, err := state.Clone(50, seed)
		if err != nil {
			t.Fatalf("Clone(50, %d): %v", seed, err)
		}
		fNew, errNew := fs50.Open("new.log")
		if errNew == nil {
			sawNew = true
			bNew, err := io.ReadAll(fNew)
			_ = fNew.Close()
			if err != nil {
				t.Fatal(err)
			}
			// Durable blockA must be intact at offset 0 even under 50% clone,
			// with optional unsynced blockB overwrite.
			if len(bNew) != len(blockA) || (!bytes.Equal(bNew, blockA) && !bytes.Equal(bNew, blockB)) {
				t.Fatalf("seed %d: new.log corrupted after ReuseForWrite: len=%d", seed, len(bNew))
			}

			fAlias, errAlias := fs50.Open("alias.log")
			if errAlias == nil {
				bAlias, err := io.ReadAll(fAlias)
				_ = fAlias.Close()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(bNew, bAlias) {
					t.Fatalf("seed %d: hard-linked alias.log diverged from reused new.log", seed)
				}
			}
		} else if !errors.Is(errNew, os.ErrNotExist) {
			t.Fatalf("seed %d: unexpected Open(new.log) error: %v", seed, errNew)
		}
	}
	if !sawNew {
		t.Fatal("expected new.log to be retained in at least one 50% clone")
	}

	// Failed ReuseForWrite must prune unlinked source from filePaths.
	failEvents := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "src.log", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "src.log", Handle: 1, Data: []byte("abc"), Len: 3},
		{ID: 3, Kind: trace.KindSync, File: "src.log", Handle: 1},
		{ID: 4, Kind: trace.KindClose, File: "src.log", Handle: 1},
		{ID: 5, Kind: trace.KindReuseForWrite, File: "src.log", File2: "missing/dst.log", Err: "no such file"},
	}
	failState, err := replay.Replay(failEvents)
	if err != nil {
		t.Fatalf("Replay failed ReuseForWrite: %v", err)
	}
	defer failState.Close()
	if _, exists := failState.FileSyncedBytesForTesting()["src.log"]; exists {
		t.Fatal("expected unlinked src.log to be pruned from filePaths after failed ReuseForWrite")
	}
}

type failingTestFile struct {
	vfs.File
	statErr   error
	readAtErr error
	closeErr  error
	syncErr   error
}

func (f failingTestFile) Stat() (vfs.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	return f.File.Stat()
}

func (f failingTestFile) ReadAt(p []byte, off int64) (int, error) {
	if f.readAtErr != nil {
		return 0, f.readAtErr
	}
	return f.File.ReadAt(p, off)
}

func (f failingTestFile) Close() error {
	_ = f.File.Close()
	return f.closeErr
}

func (f failingTestFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}

type failingListFS struct {
	vfs.FS
	listErr error
}

func (fs failingListFS) List(dir string) ([]string, error) {
	if fs.listErr != nil {
		return nil, fs.listErr
	}
	return fs.FS.List(dir)
}

func TestReplayPartialCloneTypeReplacementNestedSyncAndErrors(t *testing.T) {
	t.Run("SyncedFileReplacedByDir", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "node", Handle: 1},
			{ID: 2, Kind: trace.KindWrite, File: "node", Handle: 1, Data: []byte("durable"), Len: 7},
			{ID: 3, Kind: trace.KindSync, File: "node", Handle: 1},
			{ID: 4, Kind: trace.KindClose, File: "node", Handle: 1},
			{ID: 5, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
			{ID: 6, Kind: trace.KindSync, File: "", Handle: 2, Dir: true},
			{ID: 7, Kind: trace.KindClose, File: "", Handle: 2, Dir: true},
			{ID: 8, Kind: trace.KindRemove, File: "node"},
			{ID: 9, Kind: trace.KindMkdirAll, File: "node", Mode: 0755},
			{ID: 10, Kind: trace.KindCreate, File: "node/child.txt", Handle: 3},
			{ID: 11, Kind: trace.KindWrite, File: "node/child.txt", Handle: 3, Data: []byte("kid"), Len: 3},
			{ID: 12, Kind: trace.KindClose, File: "node/child.txt", Handle: 3},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatal(err)
		}
		defer state.Close()

		var sawFile, sawDir bool
		for seed := uint64(1); seed <= 64; seed++ {
			cl50, err := state.Clone(50, seed)
			if err != nil {
				t.Fatalf("Clone(50, %d): %v", seed, err)
			}
			fi, err := cl50.Stat("node")
			if err != nil {
				t.Fatalf("seed %d Stat(node): %v", seed, err)
			}
			if fi.IsDir() {
				sawDir = true
			} else {
				sawFile = true
				f, err := cl50.Open("node")
				if err != nil {
					t.Fatal(err)
				}
				b, err := io.ReadAll(f)
				_ = f.Close()
				if err != nil || string(b) != "durable" {
					t.Fatalf("seed %d: durable file corrupted: %q %v", seed, b, err)
				}
			}
		}
		if !sawFile || !sawDir {
			t.Fatalf("expected both file and dir outcomes across seeds, sawFile=%v sawDir=%v", sawFile, sawDir)
		}
	})

	t.Run("SyncedDirReplacedByFile", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "node", Mode: 0755},
			{ID: 2, Kind: trace.KindCreate, File: "node/keep.txt", Handle: 1},
			{ID: 3, Kind: trace.KindWrite, File: "node/keep.txt", Handle: 1, Data: []byte("keep"), Len: 4},
			{ID: 4, Kind: trace.KindSync, File: "node/keep.txt", Handle: 1},
			{ID: 5, Kind: trace.KindClose, File: "node/keep.txt", Handle: 1},
			{ID: 6, Kind: trace.KindOpenDir, File: "node", Handle: 2, Dir: true},
			{ID: 7, Kind: trace.KindSync, File: "node", Handle: 2, Dir: true},
			{ID: 8, Kind: trace.KindClose, File: "node", Handle: 2, Dir: true},
			{ID: 9, Kind: trace.KindOpenDir, File: "", Handle: 3, Dir: true},
			{ID: 10, Kind: trace.KindSync, File: "", Handle: 3, Dir: true},
			{ID: 11, Kind: trace.KindClose, File: "", Handle: 3, Dir: true},
			{ID: 12, Kind: trace.KindRemoveAll, File: "node"},
			{ID: 13, Kind: trace.KindCreate, File: "node", Handle: 4},
			{ID: 14, Kind: trace.KindWrite, File: "node", Handle: 4, Data: []byte("newfile"), Len: 7},
			{ID: 15, Kind: trace.KindClose, File: "node", Handle: 4},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatal(err)
		}
		defer state.Close()

		var sawFile, sawDir bool
		for seed := uint64(1); seed <= 64; seed++ {
			cl50, err := state.Clone(50, seed)
			if err != nil {
				t.Fatalf("Clone(50, %d): %v", seed, err)
			}
			fi, err := cl50.Stat("node")
			if err != nil {
				t.Fatalf("seed %d Stat(node): %v", seed, err)
			}
			if fi.IsDir() {
				sawDir = true
				f, err := cl50.Open("node/keep.txt")
				if err != nil {
					t.Fatalf("seed %d: Open(node/keep.txt): %v", seed, err)
				}
				b, err := io.ReadAll(f)
				_ = f.Close()
				if err != nil || string(b) != "keep" {
					t.Fatalf("seed %d: keep.txt mismatch: %q %v", seed, b, err)
				}
			} else {
				sawFile = true
			}
		}
		if !sawFile || !sawDir {
			t.Fatalf("expected both dir and file outcomes across seeds, sawDir=%v sawFile=%v", sawDir, sawFile)
		}
	})

	t.Run("NestedSyncedDirUnderUnsyncedParentRetainsSyncedChildrenAtomically", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "unsynced_parent", Mode: 0755},
			{ID: 2, Kind: trace.KindCreate, File: "unsynced_parent/keep.txt", Handle: 1},
			{ID: 3, Kind: trace.KindWrite, File: "unsynced_parent/keep.txt", Handle: 1, Data: []byte("durable"), Len: 7},
			{ID: 4, Kind: trace.KindSync, File: "unsynced_parent/keep.txt", Handle: 1},
			{ID: 5, Kind: trace.KindClose, File: "unsynced_parent/keep.txt", Handle: 1},
			{ID: 6, Kind: trace.KindOpenDir, File: "unsynced_parent", Handle: 2, Dir: true},
			{ID: 7, Kind: trace.KindSync, File: "unsynced_parent", Handle: 2, Dir: true},
			{ID: 8, Kind: trace.KindClose, File: "unsynced_parent", Handle: 2, Dir: true},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatal(err)
		}
		defer state.Close()

		sawDir := false
		for seed := uint64(1); seed <= 64; seed++ {
			cl50, err := state.Clone(50, seed)
			if err != nil {
				t.Fatalf("Clone(50, %d): %v", seed, err)
			}
			if _, err := cl50.Stat("unsynced_parent"); err == nil {
				sawDir = true
				f, err := cl50.Open("unsynced_parent/keep.txt")
				if err != nil {
					t.Fatalf("seed %d: synced child missing inside retained nested-synced dir: %v", seed, err)
				}
				b, err := io.ReadAll(f)
				_ = f.Close()
				if err != nil || string(b) != "durable" {
					t.Fatalf("seed %d: keep.txt content = %q, err=%v", seed, b, err)
				}
			}
		}
		if !sawDir {
			t.Fatal("expected unsynced_parent to be retained in at least one seed")
		}
	})

	t.Run("ErrorPropagationInPartialCloneAndUnlinkedSnapshot", func(t *testing.T) {
		mem := vfs.NewCrashableMem()
		boom := errors.New("injected io failure")
		_, err := replay.DeterministicPartialCloneForTesting(
			failingListFS{FS: mem, listErr: boom},
			mem,
			50,
			rand.New(rand.NewPCG(0, 1)),
			nil,
		)
		if !errors.Is(err, boom) {
			t.Fatalf("expected List error propagation, got %v", err)
		}

		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "tmp.txt", Handle: 1},
			{ID: 2, Kind: trace.KindWrite, File: "tmp.txt", Handle: 1, Data: []byte("abc"), Len: 3},
			{ID: 3, Kind: trace.KindRemove, File: "tmp.txt"},
		}
		state, err := replay.Build(events, 3)
		if err != nil {
			t.Fatal(err)
		}
		defer state.Close()
		state.WrapHandleForTesting(1, func(inner vfs.File) vfs.File {
			return failingTestFile{File: inner, readAtErr: boom}
		})
		err = state.AdvanceStepForTesting(trace.Event{ID: 4, Kind: trace.KindSyncData, File: "tmp.txt", Handle: 1})
		if !errors.Is(err, boom) {
			t.Fatalf("expected unlinked snapshot ReadAt error propagation, got %v", err)
		}
	})
}

func TestReplayAdvancePoisonCloseRetryPreallocateRemoveAllLock(t *testing.T) {
	t.Run("PreallocateRemoveAllAndLock", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
			{ID: 2, Kind: trace.KindLock, File: "db/LOCK", Handle: 1},
			{ID: 3, Kind: trace.KindCreate, File: "db/data.sst", Handle: 2, Cat: "pebble-sst"},
			{ID: 4, Kind: trace.KindPreallocate, File: "db/data.sst", Handle: 2, Off: 0, Size: 4096},
			{ID: 5, Kind: trace.KindWrite, File: "db/data.sst", Handle: 2, Data: []byte("hello"), Len: 5},
			{ID: 6, Kind: trace.KindClose, File: "db/data.sst", Handle: 2},
			{ID: 7, Kind: trace.KindUnlock, File: "db/LOCK", Handle: 1},
			{ID: 8, Kind: trace.KindRemoveAll, File: "db"},
		}
		state, err := replay.Build(events, 7)
		if err != nil {
			t.Fatalf("Build(7): %v", err)
		}
		defer state.Close()
		if _, exists := state.FileSyncedBytesForTesting()["db/LOCK"]; !exists {
			t.Fatal("expected db/LOCK to be tracked in filePaths after KindLock")
		}
		if err := state.Advance(8); err != nil {
			t.Fatalf("Advance(8): %v", err)
		}
		if _, exists := state.FileSyncedBytesForTesting()["db/LOCK"]; exists {
			t.Fatal("expected db/LOCK to be pruned after KindRemoveAll")
		}
		if _, exists := state.FileSyncedBytesForTesting()["db/data.sst"]; exists {
			t.Fatal("expected db/data.sst to be pruned after KindRemoveAll")
		}
	})

	t.Run("FailedDirectoryRenamePrunesChildSnapshots", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindMkdirAll, File: "src", Mode: 0755},
			{ID: 2, Kind: trace.KindCreate, File: "src/child.txt", Handle: 1},
			{ID: 3, Kind: trace.KindWrite, File: "src/child.txt", Handle: 1, Data: []byte("data"), Len: 4},
			{ID: 4, Kind: trace.KindSync, File: "src/child.txt", Handle: 1},
			{ID: 5, Kind: trace.KindClose, File: "src/child.txt", Handle: 1},
			{ID: 6, Kind: trace.KindRename, File: "src", File2: "missing/dst", Err: "open missing/dst: file does not exist"},
		}
		state, err := replay.Replay(events)
		if err != nil {
			t.Fatalf("Replay failed dir rename: %v", err)
		}
		defer state.Close()
		if _, exists := state.FileSyncedBytesForTesting()["src/child.txt"]; exists {
			t.Fatal("expected src/child.txt to be pruned after failed directory Rename unlinked src")
		}
	})

	t.Run("AdvanceFailurePoisonsStateAndCloseDoesNotRetryErroredHandle", func(t *testing.T) {
		events := []trace.Event{
			{ID: 1, Kind: trace.KindCreate, File: "a.txt", Handle: 1},
			{ID: 2, Kind: trace.KindWrite, File: "a.txt", Handle: 1, Data: []byte("abc"), Len: 3},
			{ID: 3, Kind: trace.KindRead, File: "a.txt", Handle: 1, Off: 3, Size: 3, Len: 0},
		}
		state, err := replay.Build(events, 1)
		if err != nil {
			t.Fatal(err)
		}

		// Close failure should still remove handle so second Close does not retry.
		closeBoom := errors.New("close failed")
		state.WrapHandleForTesting(1, func(inner vfs.File) vfs.File {
			return failingTestFile{File: inner, closeErr: closeBoom}
		})
		if err := state.Close(); !errors.Is(err, closeBoom) {
			t.Fatalf("first Close got %v, want %v", err, closeBoom)
		}
		if err := state.Close(); err != nil {
			t.Fatalf("second Close retried errored handle: %v", err)
		}

		// Advance failure must poison State for subsequent Advance and Clone.
		state2, err := replay.Build(events, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer state2.Close()
		if err := state2.Advance(3); err == nil {
			t.Fatal("expected Advance(3) to fail on read error mismatch")
		}
		if err := state2.Advance(3); err == nil {
			t.Fatal("expected subsequent Advance after failure to return error")
		}
		if _, err := state2.Clone(100, 1); err == nil {
			t.Fatal("expected Clone after Advance failure to return error")
		}

		// Negative sizes in Read/Preallocate/SyncTo must fail without panicking.
		for _, bad := range []trace.Event{
			{ID: 2, Kind: trace.KindRead, File: "a.txt", Handle: 1, Size: -5},
			{ID: 2, Kind: trace.KindPreallocate, File: "a.txt", Handle: 1, Off: 0, Size: -1},
			{ID: 2, Kind: trace.KindSyncTo, File: "a.txt", Handle: 1, Size: -1},
		} {
			if err := state2.AdvanceStepForTesting(bad); err == nil {
				t.Fatalf("expected error on negative size event %+v", bad)
			}
		}
	})
}

func TestReplayIncrementalAppendSyncSnapshot(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	f, err := rec.Create("wal.log", "pebble-wal")
	if err != nil {
		t.Fatal(err)
	}
	var expected []byte
	for i := 0; i < 20; i++ {
		chunk := bytes.Repeat([]byte{byte('a' + i)}, 128)
		if _, err := f.Write(bytes.Clone(chunk)); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, chunk...)
		if err := f.SyncData(); err != nil {
			t.Fatal(err)
		}
	}
	// Now perform an in-place overwrite at offset 0 and sync again.
	overwrite := []byte("OVERWRITE!")
	if _, err := f.WriteAt(bytes.Clone(overwrite), 0); err != nil {
		t.Fatal(err)
	}
	copy(expected[0:], overwrite)
	if err := f.SyncData(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	state, err := replay.Replay(rec.Events())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()

	got := state.FileSyncedBytesForTesting()["wal.log"]
	if !bytes.Equal(got, expected) {
		t.Fatalf("incremental + overwrite synced snapshot mismatch: len=%d want %d", len(got), len(expected))
	}
}
