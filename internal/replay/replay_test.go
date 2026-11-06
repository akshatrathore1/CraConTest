package replay_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sort"
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
		if _, err := uf.Write([]byte(fmt.Sprintf("unsynced content %d", i))); err != nil {
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
			if _, err := uf.Write([]byte(fmt.Sprintf("unsynced content %d", i))); err != nil {
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

		// Repeating state.Clone(pct, 42) 32 times yields 1 unique digest, whereas repeating
		// memFS.CrashClone(pct, 42) 32 times yields >1 digests due to Go map iteration order.
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
		if len(pebbleDigests) <= 1 {
			t.Fatalf("pct %d: expected >1 unique digests for memFS.CrashClone(pct, 42) due to map iteration order, got %d", pct, len(pebbleDigests))
		}
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
	if _, err := f1.Write(dataA); err != nil {
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
	if _, err := f1b.Write(dataB); err != nil {
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
	if _, err := f2.Write(dataA); err != nil {
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
	if _, err := f2b.Write(dataB); err != nil {
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
		if err == nil {
			b, _ := io.ReadAll(fSynced)
			_ = fSynced.Close()
			if bytes.Equal(b, dataA) {
				sawUnretainedSynced = true
			} else if bytes.Equal(b, dataB) {
				sawRetainedSynced = true
			} else {
				t.Fatalf("seed %d: synced_dir/file.txt contains corrupted bytes: %q", seed, b[:16])
			}
		}

		// Check unsynced_dir file (when directory entry is retained):
		fUnsynced, err := cl50.Open("unsynced_dir/file.txt")
		if err == nil {
			b, _ := io.ReadAll(fUnsynced)
			_ = fUnsynced.Close()
			if bytes.Equal(b, dataA) {
				sawUnretainedUnsynced = true
			} else if bytes.Equal(b, dataB) {
				sawRetainedUnsynced = true
			} else {
				t.Fatalf("seed %d: unsynced_dir/file.txt contains corrupted bytes: %q", seed, b[:16])
			}
		}
	}

	if !sawUnretainedSynced || !sawRetainedSynced {
		t.Fatalf("expected both unretained (AAAA) and retained (BBBB) blocks for synced_dir across 64 seeds; sawUnretained=%v sawRetained=%v", sawUnretainedSynced, sawRetainedSynced)
	}
	if !sawUnretainedUnsynced || !sawRetainedUnsynced {
		t.Fatalf("expected both unretained (AAAA) and retained (BBBB) blocks for unsynced_dir across 64 seeds; sawUnretained=%v sawRetained=%v", sawUnretainedUnsynced, sawRetainedUnsynced)
	}
}
