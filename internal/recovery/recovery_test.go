package recovery_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
)

// TestCrashCycle exercises the full Open→Write→Read→Record→Replay→Crash→Recover→Verify
// cycle using Pebble's in-memory CrashableMem filesystem and trace replay.
func TestCrashCycle(t *testing.T) {
	// --- RECORD PHASE: Open, Write, Sync ---

	memFS := vfs.NewCrashableMem()
	rec := vfsrec.New(memFS)
	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            rec,
		Logger:        recovery.PanicLogger{},
		EventListener: &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	// Write 10 key-value pairs with sync.
	syncOpt := pebble.Sync
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := fmt.Sprintf("value-%03d", i)
		if err := db.Set([]byte(key), []byte(val), syncOpt); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	// Read back to confirm writes are visible before crash.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val, closer, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("pre-crash get %s: %v", key, err)
		}
		expected := fmt.Sprintf("value-%03d", i)
		if string(val) != expected {
			_ = closer.Close()
			t.Fatalf("pre-crash get %s: got %q, want %q", key, val, expected)
		}
		if err := closer.Close(); err != nil {
			t.Fatalf("pre-crash close %s: %v", key, err)
		}
	}

	// Capture cutoff before Close so the crash snapshot represents an unclosed WAL.
	preCloseCut := rec.CurrentEventID()

	// Close the original database cleanly before replaying the prefix.
	closed = true
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// --- CRASH PHASE: Replay prefix and CrashClone with 100% retention ---
	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}
	state, err := replay.Build(events, preCloseCut)
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	crashedFS, err := state.Clone(100, 42)
	if closeErr := state.Close(); closeErr != nil {
		t.Fatalf("state.Close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("state.Clone: %v", err)
	}

	// --- RECOVER PHASE: SafeOpen on the crashed filesystem ---

	result := recovery.SafeOpen(crashedFS, "db", 30*time.Second)
	if result.Err != nil {
		t.Fatalf("recovery open error: %v", result.Err)
	}
	if result.Panic != "" {
		t.Fatalf("recovery panic: %s", result.Panic)
	}
	if result.Timeout {
		t.Fatal("recovery timed out")
	}
	if result.DB == nil {
		t.Fatal("recovery returned nil DB without error")
	}
	defer func() {
		if err := result.DB.Close(); err != nil {
			t.Errorf("close recovered DB: %v", err)
		}
	}()

	// --- VERIFY PHASE: Check integrity and data ---

	if err := recovery.CheckIntegrity(result.DB); err != nil {
		t.Fatalf("integrity check: %v", err)
	}

	contents, err := recovery.DumpContents(result.DB)
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i)
		expected := fmt.Sprintf("value-%03d", i)
		got, ok := contents[key]
		if !ok {
			t.Errorf("synced key %s missing after recovery", key)
		} else if got != expected {
			t.Errorf("key %s: got %q, want %q", key, got, expected)
		}
	}

	if len(contents) != 10 {
		t.Errorf("expected 10 keys, got %d", len(contents))
	}
}

// TestCrashCyclePartialRetention tests crash recovery with 0% retention
// of unsynced data via trace replay. Synced writes must still survive; unsynced
// writes may be lost.
func TestCrashCyclePartialRetention(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	rec := vfsrec.New(memFS)
	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:     rec,
		Logger: recovery.PanicLogger{},
		// Small memtable to force flushes.
		MemTableSize:  1024 * 64,
		EventListener: &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	// Write synced data.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("synced-%03d", i)
		val := fmt.Sprintf("sval-%03d", i)
		if err := db.Set([]byte(key), []byte(val), pebble.Sync); err != nil {
			t.Fatalf("synced set %s: %v", key, err)
		}
	}

	// Write unsynced data (NoSync).
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("unsynced-%03d", i)
		val := fmt.Sprintf("uval-%03d", i)
		if err := db.Set([]byte(key), []byte(val), pebble.NoSync); err != nil {
			t.Fatalf("unsynced set %s: %v", key, err)
		}
	}

	preCloseCut := rec.CurrentEventID()

	closed = true
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}
	state, err := replay.Build(events, preCloseCut)
	if err != nil {
		t.Fatalf("replay.Build: %v", err)
	}
	crashedFS, err := state.Clone(0, 99)
	if closeErr := state.Close(); closeErr != nil {
		t.Fatalf("state.Close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("state.Clone: %v", err)
	}

	result := recovery.SafeOpen(crashedFS, "db", 30*time.Second)
	if result.Panic != "" {
		t.Fatalf("recovery panic: %s", result.Panic)
	}
	if result.Timeout {
		t.Fatal("recovery timed out")
	}
	if result.Err != nil {
		t.Fatalf("recovery open error: %v", result.Err)
	}
	if result.DB == nil {
		t.Fatal("recovery returned nil DB without error")
	}
	defer func() {
		if err := result.DB.Close(); err != nil {
			t.Errorf("close recovered DB: %v", err)
		}
	}()

	// If recovery succeeded, check integrity.
	if err := recovery.CheckIntegrity(result.DB); err != nil {
		t.Fatalf("integrity check: %v", err)
	}

	contents, err := recovery.DumpContents(result.DB)
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	// Synced keys must survive.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("synced-%03d", i)
		expected := fmt.Sprintf("sval-%03d", i)
		got, ok := contents[key]
		if !ok {
			t.Errorf("synced key %s missing after recovery", key)
		} else if got != expected {
			t.Errorf("key %s: got %q, want %q", key, got, expected)
		}
	}

	unsyncedCount := 0
	missingUnsynced := false
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("unsynced-%03d", i)
		got, ok := contents[key]
		if !ok {
			missingUnsynced = true
			continue
		}
		if missingUnsynced {
			t.Errorf("unsynced key %s survived after an earlier key was lost", key)
		}
		expected := fmt.Sprintf("uval-%03d", i)
		if got != expected {
			t.Errorf("key %s: got %q, want %q", key, got, expected)
		}
		unsyncedCount++
	}
	if len(contents) != 5+unsyncedCount {
		t.Errorf("expected %d keys, got %d", 5+unsyncedCount, len(contents))
	}
	t.Logf("unsynced keys surviving: %d/5 (0%% retention)", unsyncedCount)
}

// TestCrashCycleManyOperations runs a larger workload (100 operations)
// with mixed synced and unsynced writes, then crashes and recovers.
// This is a stepping stone toward the 1,000-operation cost check.
func TestCrashCycleManyOperations(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:           memFS,
		Logger:       recovery.PanicLogger{},
		MemTableSize: 1024 * 64, // Force flushes.
	}

	db, err := pebble.Open("", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	// Write 100 keys with alternating sync.
	syncedKeys := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k-%05d", i)
		val := fmt.Sprintf("v-%05d", i)
		wo := pebble.NoSync
		if i%5 == 0 {
			wo = pebble.Sync
			syncedKeys[key] = val
		}
		if err := db.Set([]byte(key), []byte(val), wo); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	// Force a flush to exercise table creation + manifest update.
	if err := db.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	closed = true
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 100% retention crash.
	rng := rand.New(rand.NewPCG(7, 0))
	crashedFS := memFS.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: 100,
		RNG:                 rng,
	})

	result := recovery.SafeOpen(crashedFS, "", 30*time.Second)
	if result.Err != nil {
		t.Fatalf("recovery open error: %v", result.Err)
	}
	if result.Panic != "" {
		t.Fatalf("recovery panic: %s", result.Panic)
	}
	if result.Timeout {
		t.Fatal("recovery timed out")
	}
	defer result.DB.Close()

	if err := recovery.CheckIntegrity(result.DB); err != nil {
		t.Fatalf("integrity check: %v", err)
	}

	contents, err := recovery.DumpContents(result.DB)
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	// With 100% retention, all 100 keys should be present.
	if len(contents) != 100 {
		t.Errorf("expected 100 keys, got %d", len(contents))
	}
}

type blockingFS struct {
	vfs.FS
	delayChan    chan struct{}
	lockClosed   chan struct{}
	postListSync chan struct{}
	mu           sync.Mutex
	listReturned bool
	dirSyncOnce  sync.Once
	lockOnce     sync.Once
}

func (b *blockingFS) List(dir string) ([]string, error) {
	if b.delayChan != nil {
		<-b.delayChan
	}
	b.mu.Lock()
	b.listReturned = true
	b.mu.Unlock()
	return b.FS.List(dir)
}

func (b *blockingFS) OpenDir(name string) (vfs.File, error) {
	f, err := b.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &blockingDirFile{File: f, fs: b}, nil
}

type blockingDirFile struct {
	vfs.File
	fs *blockingFS
}

func (f *blockingDirFile) Sync() error {
	err := f.File.Sync()
	f.fs.mu.Lock()
	postList := f.fs.listReturned
	f.fs.mu.Unlock()
	if postList && f.fs.postListSync != nil {
		f.fs.dirSyncOnce.Do(func() { close(f.fs.postListSync) })
	}
	return err
}

func (b *blockingFS) Lock(name string) (io.Closer, error) {
	closer, err := b.FS.Lock(name)
	if err != nil {
		return nil, err
	}
	return &lockCloserWrapper{
		Closer: closer,
		onClose: func() {
			if b.lockClosed != nil {
				b.lockOnce.Do(func() { close(b.lockClosed) })
			}
		},
	}, nil
}

type lockCloserWrapper struct {
	io.Closer
	onClose func()
}

func (l *lockCloserWrapper) Close() error {
	err := l.Closer.Close()
	if l.onClose != nil {
		l.onClose()
		l.onClose = nil
	}
	return err
}

func TestSafeOpenTimeoutClosesLateDB(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:     memFS,
		Logger: recovery.PanicLogger{},
	}
	db, err := pebble.Open("testdb", opts)
	if err != nil {
		t.Fatalf("setup open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("setup close: %v", err)
	}

	delayChan := make(chan struct{})
	lockClosed := make(chan struct{})
	postListSync := make(chan struct{})

	bfs := &blockingFS{
		FS:           memFS,
		delayChan:    delayChan,
		lockClosed:   lockClosed,
		postListSync: postListSync,
	}

	res := recovery.SafeOpen(bfs, "testdb", 20*time.Millisecond)
	if !res.Timeout {
		t.Fatalf("expected SafeOpen to timeout, got %+v", res)
	}

	// Unblock pebble.Open now.
	close(delayChan)

	select {
	case <-postListSync:
		// Late recovery continued through the underlying filesystem without
		// converting the timeout into an injected fsync failure.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for late pebble.Open to complete its directory sync")
	}

	select {
	case <-lockClosed:
		// Late DB was closed by the cleanup goroutine!
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for late pebble.DB to be closed by SafeOpen cleanup goroutine")
	}

	// Reopening testdb should succeed because the lock was released on close.
	db2, err := pebble.Open("testdb", opts)
	if err != nil {
		t.Fatalf("re-open after cleanup failed: %v", err)
	}
	_ = db2.Close()
}

func TestAllEntriesGracefulOnClosedOrValidDB(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:     memFS,
		Logger: recovery.PanicLogger{},
	}
	db, err := pebble.Open("test_iter", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2"), pebble.Sync); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	got := make(map[string]string)
	for k, v := range recovery.AllEntries(db) {
		got[k] = v
	}
	if len(got) != 2 || got["k1"] != "v1" || got["k2"] != "v2" {
		t.Errorf("unexpected AllEntries output: %#v", got)
	}

	// Close database and verify AllEntries panics on invalid/closed DB
	closed = true
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		for range recovery.AllEntries(db) {
		}
	}()
	if !panicked {
		t.Errorf("expected AllEntries to panic on closed DB")
	}
}

func TestAllEntriesPropagatesYieldPanic(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:     memFS,
		Logger: recovery.PanicLogger{},
	}
	db, err := pebble.Open("test_yield_panic", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
		t.Fatalf("set k1: %v", err)
	}

	var recoveredVal any
	func() {
		defer func() {
			recoveredVal = recover()
		}()
		for range recovery.AllEntries(db) {
			panic("caller custom assertion panic")
		}
	}()

	if recoveredVal != "caller custom assertion panic" {
		t.Fatalf("expected caller panic to propagate through AllEntries, got: %v", recoveredVal)
	}
}

type hangingFS struct {
	vfs.FS
	block         chan struct{}
	createStarted chan struct{}
	lockClosed    chan struct{}
	createOnce    sync.Once
	lockOnce      sync.Once
}

func (h *hangingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if h.block != nil {
		if h.createStarted != nil {
			h.createOnce.Do(func() { close(h.createStarted) })
		}
		<-h.block
	}
	return h.FS.Create(name, category)
}

func (h *hangingFS) Lock(name string) (io.Closer, error) {
	closer, err := h.FS.Lock(name)
	if err != nil {
		return nil, err
	}
	return &lockCloserWrapper{
		Closer: closer,
		onClose: func() {
			if h.lockClosed != nil {
				h.lockOnce.Do(func() { close(h.lockClosed) })
			}
		},
	}, nil
}

func TestSafeOpenTimeoutCleansUpAfterBlockedCreate(t *testing.T) {
	mem := vfs.NewCrashableMem()
	block := make(chan struct{})
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(block) }) }
	t.Cleanup(unblock)
	fs := &hangingFS{
		FS:            mem,
		block:         block,
		createStarted: make(chan struct{}),
		lockClosed:    make(chan struct{}),
	}
	resultCh := make(chan recovery.OpenResult, 1)
	go func() {
		resultCh <- recovery.SafeOpen(fs, "db", 20*time.Millisecond)
	}()
	select {
	case <-fs.createStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not reach the blocked Create")
	}
	var res recovery.OpenResult
	select {
	case res = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("SafeOpen did not return while Create was blocked")
	}
	if !res.Timeout {
		if res.DB != nil {
			_ = res.DB.Close()
		}
		t.Fatalf("expected timeout, got: %+v", res)
	}

	unblock()
	select {
	case <-fs.lockClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("late recovery did not release the database lock")
	}

	db, err := pebble.Open("db", &pebble.Options{FS: mem, Logger: recovery.PanicLogger{}})
	if err != nil {
		t.Fatalf("re-open after late cleanup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close after late cleanup: %v", err)
	}
}

type errorFS struct {
	vfs.FS
	failCreate bool
}

func (e *errorFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if e.failCreate {
		return nil, fmt.Errorf("injected create error")
	}
	return e.FS.Create(name, category)
}

func TestRecoveryPipelineErrorPaths(t *testing.T) {
	t.Run("CheckIntegrityAndDumpContentsOnClosedDB", func(t *testing.T) {
		mem := vfs.NewCrashableMem()
		db, err := pebble.Open("db", &pebble.Options{
			FS:     mem,
			Logger: recovery.PanicLogger{},
		})
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		closed := false
		defer func() {
			if !closed {
				_ = db.Close()
			}
		}()
		if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
			t.Fatalf("set: %v", err)
		}
		closed = true
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		// CheckIntegrity on closed DB should return error
		if err := recovery.CheckIntegrity(db); err == nil {
			t.Errorf("expected CheckIntegrity to fail on closed DB, got nil")
		}

		// DumpContents on closed DB should return error
		if _, err := recovery.DumpContents(db); err == nil {
			t.Errorf("expected DumpContents to fail on closed DB, got nil")
		}
	})

	t.Run("SafeOpenCorruptManifest", func(t *testing.T) {
		mem := vfs.NewCrashableMem()
		db, err := pebble.Open("db", &pebble.Options{
			FS:     mem,
			Logger: recovery.PanicLogger{},
		})
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		closed := false
		defer func() {
			if !closed {
				_ = db.Close()
			}
		}()
		if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
			t.Fatalf("set: %v", err)
		}
		closed = true
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		// Find and corrupt the MANIFEST file
		entries, err := mem.List("db")
		if err != nil {
			t.Fatalf("list db: %v", err)
		}
		corrupted := false
		for _, e := range entries {
			if len(e) >= 8 && e[:8] == "MANIFEST" {
				f, err := mem.Create("db/"+e, vfs.WriteCategoryUnspecified)
				if err != nil {
					t.Fatalf("open manifest: %v", err)
				}
				_, _ = f.Write([]byte("GARBAGE_CORRUPTED_MANIFEST_HEADER_AND_RECORD"))
				_ = f.Close()
				corrupted = true
				break
			}
		}
		if !corrupted {
			t.Fatalf("MANIFEST file not found in db directory")
		}

		// SafeOpen on corrupt manifest should return Err or Panic without unhandled crash
		res := recovery.SafeOpen(mem, "db", 5*time.Second)
		if res.DB != nil {
			res.DB.Close()
			t.Fatalf("expected SafeOpen to fail on corrupt manifest, got active DB")
		}
		if res.Err == nil && res.Panic == "" {
			t.Fatalf("expected res.Err != nil or res.Panic != empty on corrupt manifest, got %+v", res)
		}
	})

	t.Run("SafeOpenFilesystemError", func(t *testing.T) {
		mem := vfs.NewCrashableMem()
		efs := &errorFS{FS: mem, failCreate: true}
		res := recovery.SafeOpen(efs, "db", 5*time.Second)
		if res.DB != nil {
			res.DB.Close()
			t.Fatalf("expected SafeOpen to fail on errorFS, got active DB")
		}
		if res.Err == nil && res.Panic == "" {
			t.Fatalf("expected error from SafeOpen on errorFS, got %+v", res)
		}
	})

	t.Run("ConcurrentSafeOpen", func(t *testing.T) {
		const numWorkers = 5
		var wg sync.WaitGroup
		errCh := make(chan error, numWorkers)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				mem := vfs.NewCrashableMem()
				dbDir := fmt.Sprintf("db_%d", workerID)
				db, err := pebble.Open(dbDir, &pebble.Options{
					FS:     mem,
					Logger: recovery.PanicLogger{},
				})
				if err != nil {
					errCh <- fmt.Errorf("worker %d initial open: %w", workerID, err)
					return
				}
				k := fmt.Sprintf("k%d", workerID)
				v := fmt.Sprintf("v%d", workerID)
				if err := db.Set([]byte(k), []byte(v), pebble.Sync); err != nil {
					_ = db.Close()
					errCh <- fmt.Errorf("worker %d set: %w", workerID, err)
					return
				}
				if err := db.Close(); err != nil {
					errCh <- fmt.Errorf("worker %d close: %w", workerID, err)
					return
				}

				res := recovery.SafeOpen(mem, dbDir, 5*time.Second)
				if res.Err != nil || res.Panic != "" || res.DB == nil {
					errCh <- fmt.Errorf("worker %d SafeOpen failed: err=%v panic=%s", workerID, res.Err, res.Panic)
					return
				}
				defer res.DB.Close()

				if err := recovery.CheckIntegrity(res.DB); err != nil {
					errCh <- fmt.Errorf("worker %d CheckIntegrity: %w", workerID, err)
					return
				}
				contents, err := recovery.DumpContents(res.DB)
				if err != nil {
					errCh <- fmt.Errorf("worker %d DumpContents: %w", workerID, err)
					return
				}
				if contents[k] != v {
					errCh <- fmt.Errorf("worker %d content mismatch: got %q, want %q", workerID, contents[k], v)
					return
				}
			}(i)
		}

		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Errorf("concurrent worker error: %v", err)
		}
	})

	t.Run("NilDBGuards", func(t *testing.T) {
		if err := recovery.CheckIntegrity(nil); err == nil {
			t.Errorf("expected CheckIntegrity(nil) to return error, got nil")
		}
		if _, err := recovery.DumpContents(nil); err == nil {
			t.Errorf("expected DumpContents(nil) to return error, got nil")
		}
		count := 0
		for range recovery.AllEntries(nil) {
			count++
		}
		if count != 0 {
			t.Errorf("expected AllEntries(nil) to yield 0 entries, got %d", count)
		}
	})

	t.Run("SafeOpenCorruptOptionsFile", func(t *testing.T) {
		mem := vfs.NewCrashableMem()
		db, err := pebble.Open("db", &pebble.Options{
			FS:     mem,
			Logger: recovery.PanicLogger{},
		})
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		closed := false
		defer func() {
			if !closed {
				_ = db.Close()
			}
		}()
		if err := db.Set([]byte("k"), []byte("v"), pebble.Sync); err != nil {
			t.Fatalf("set: %v", err)
		}
		closed = true
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		// Corrupt OPTIONS file
		entries, err := mem.List("db")
		if err != nil {
			t.Fatalf("list db: %v", err)
		}
		corrupted := false
		for _, e := range entries {
			if strings.HasPrefix(e, "OPTIONS") {
				opt, err := mem.Create("db/"+e, vfs.WriteCategoryUnspecified)
				if err != nil {
					t.Fatalf("create OPTIONS: %v", err)
				}
				_, _ = opt.Write([]byte("INVALID_GARBAGE_OPTIONS_SYNTAX_ERROR\n"))
				_ = opt.Close()
				corrupted = true
				break
			}
		}
		if !corrupted {
			t.Fatalf("no OPTIONS file found")
		}

		res := recovery.SafeOpen(mem, "db", 5*time.Second)
		if res.DB != nil {
			res.DB.Close()
			t.Fatalf("expected SafeOpen to fail on corrupted OPTIONS, got open DB")
		}
		if res.Err == nil && res.Panic == "" {
			t.Fatalf("expected error or panic on corrupted OPTIONS, got %+v", res)
		}
	})

	t.Run("CorruptSSTableIteratorErrorPath", func(t *testing.T) {
		mem := vfs.NewCrashableMem()
		db, err := pebble.Open("db", &pebble.Options{
			FS:     mem,
			Logger: recovery.PanicLogger{},
		})
		if err != nil {
			t.Fatalf("setup open: %v", err)
		}
		closed := false
		defer func() {
			if !closed {
				_ = db.Close()
			}
		}()
		// Write several keys and flush to create an SSTable
		for i := 0; i < 50; i++ {
			if err := db.Set(fmt.Appendf(nil, "key-%03d", i), fmt.Appendf(nil, "val-%03d", i), pebble.NoSync); err != nil {
				t.Fatalf("set key-%03d: %v", i, err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		closed = true
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		// Find the .sst file and corrupt it
		entries, err := mem.List("db")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		var sstName string
		for _, e := range entries {
			if strings.HasSuffix(e, ".sst") {
				sstName = e
				break
			}
		}
		if sstName == "" {
			t.Fatalf("no .sst file found after flush")
		}

		// Overwrite the .sst with corrupted garbage bytes
		f, err := mem.Create("db/"+sstName, vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatalf("create sst: %v", err)
		}
		_, _ = f.Write([]byte("CORRUPTED_SST_HEADER_GARBAGE_BYTES_TRUNCATED"))
		_ = f.Close()

		// Attempt SafeOpen on DB with corrupt SST
		res := recovery.SafeOpen(mem, "db", 5*time.Second)
		if res.DB != nil {
			defer func() {
				_ = res.DB.Close()
			}()
			// If Pebble opened without validating all SSTs upfront, BOTH CheckIntegrity and DumpContents MUST catch corruption
			if errInt := recovery.CheckIntegrity(res.DB); errInt == nil {
				t.Fatalf("expected CheckIntegrity to fail on corrupt SST")
			}
			if _, errDump := recovery.DumpContents(res.DB); errDump == nil {
				t.Fatalf("expected DumpContents to fail on corrupt SST")
			}
		} else {
			// If SafeOpen caught corruption immediately, that is also a valid error outcome
			if res.Err == nil && res.Panic == "" {
				t.Fatalf("expected error or panic from SafeOpen on corrupt SST")
			}
		}
	})
}

type panicFS struct {
	vfs.FS
	panicVal any
}

func (p *panicFS) List(dir string) ([]string, error) {
	panic(p.panicVal)
}

func TestSafeOpenInputGuardsAndEmptyPanic(t *testing.T) {
	resNilFS := recovery.SafeOpen(nil, "db", time.Second)
	if resNilFS.Err == nil || resNilFS.DB != nil || resNilFS.Timeout {
		t.Fatalf("expected non-timeout error on nil FS, got %+v", resNilFS)
	}

	mem := vfs.NewCrashableMem()
	for _, d := range []time.Duration{0, -5 * time.Millisecond} {
		resBadTimeout := recovery.SafeOpen(mem, "db", d)
		if resBadTimeout.Err == nil || resBadTimeout.DB != nil || resBadTimeout.Timeout {
			t.Fatalf("expected error (not Timeout) for timeout=%v, got %+v", d, resBadTimeout)
		}
	}

	pfs := &panicFS{FS: mem, panicVal: ""}
	resPanic := recovery.SafeOpen(pfs, "db", time.Second)
	if resPanic.Panic == "" || resPanic.Err != nil || resPanic.Timeout || resPanic.DB != nil {
		t.Fatalf("expected non-empty Panic for panic(\"\"), got %+v", resPanic)
	}
}

func TestCheckIntegrityAndDumpContentsReturnPanicErrorOnClosedDB(t *testing.T) {
	mem := vfs.NewCrashableMem()
	db, err := pebble.Open("db", &pebble.Options{
		FS:     mem,
		Logger: recovery.PanicLogger{},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	errInt := recovery.CheckIntegrity(db)
	if errInt == nil {
		t.Fatal("expected CheckIntegrity error on closed DB")
	}
	var peInt *recovery.PanicError
	if !errors.As(errInt, &peInt) {
		t.Fatalf("expected *recovery.PanicError from CheckIntegrity on closed DB, got %T: %v", errInt, errInt)
	}

	_, errDump := recovery.DumpContents(db)
	if errDump == nil {
		t.Fatal("expected DumpContents error on closed DB")
	}
	var peDump *recovery.PanicError
	if !errors.As(errDump, &peDump) {
		t.Fatalf("expected *recovery.PanicError from DumpContents on closed DB, got %T: %v", errDump, errDump)
	}
}

func TestAllEntriesEarlyBreakChecksIterError(t *testing.T) {
	mem := vfs.NewCrashableMem()
	db, err := pebble.Open("db", &pebble.Options{
		FS:                          mem,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                4 * 1024,
		DisableAutomaticCompactions: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 40; i++ {
		if err := db.Set(fmt.Appendf(nil, "k-%03d", i), fmt.Appendf(nil, "v-%03d-padding", i), pebble.Sync); err != nil {
			_ = db.Close()
			t.Fatalf("set: %v", err)
		}
	}
	if err := db.Flush(); err != nil {
		_ = db.Close()
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt a byte in the middle of the SST data block (leaving footer/header readable by Open)
	entries, err := mem.List("db")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var sstPath string
	for _, e := range entries {
		if strings.HasSuffix(e, ".sst") {
			sstPath = "db/" + e
			break
		}
	}
	if sstPath == "" {
		t.Fatal("expected .sst file after flush")
	}
	f, err := mem.Open(sstPath)
	if err != nil {
		t.Fatalf("open sst: %v", err)
	}
	raw, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil || len(raw) < 64 {
		t.Fatalf("read sst: err=%v len=%d", err, len(raw))
	}
	raw[0] ^= 0xff
	wf, err := mem.Create(sstPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("recreate sst: %v", err)
	}
	if _, err := wf.Write(raw); err != nil {
		_ = wf.Close()
		t.Fatalf("write corrupt sst: %v", err)
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("close corrupt sst: %v", err)
	}

	res := recovery.SafeOpen(mem, "db", 5*time.Second)
	if res.DB == nil {
		t.Fatalf("expected SafeOpen to open DB with data-block bitflip, got %+v", res)
	}
	defer func() {
		_ = res.DB.Close()
	}()

	// Verify CheckIntegrity catches the corruption
	if err := recovery.CheckIntegrity(res.DB); err == nil {
		t.Fatal("expected CheckIntegrity to fail on SST data-block bitflip")
	}
	// Verify DumpContents catches the corruption cleanly without panicking from use-after-close
	if _, err := recovery.DumpContents(res.DB); err == nil {
		t.Fatal("expected DumpContents to fail on SST data-block bitflip")
	}

	// Verify AllEntries catches iterator error (does not swallow corruption error).
	var iterPanic any
	func() {
		defer func() {
			iterPanic = recover()
		}()
		for range recovery.AllEntries(res.DB) {
			break
		}
	}()
	if iterPanic == nil {
		t.Fatal("expected AllEntries to panic on corrupt SST even with early break")
	}

	// Also verify early break on a healthy DB closes iterator cleanly without leaking references.
	healthyMem := vfs.NewCrashableMem()
	hdb, err := pebble.Open("db", &pebble.Options{FS: healthyMem, Logger: recovery.PanicLogger{}})
	if err != nil {
		t.Fatalf("healthy open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := hdb.Set(fmt.Appendf(nil, "hk-%02d", i), []byte("hv"), pebble.Sync); err != nil {
			_ = hdb.Close()
			t.Fatalf("healthy set: %v", err)
		}
	}
	seen := 0
	for range recovery.AllEntries(hdb) {
		seen++
		break
	}
	if seen != 1 {
		_ = hdb.Close()
		t.Fatalf("expected 1 entry before early break, got %d", seen)
	}
	if err := hdb.Close(); err != nil {
		t.Fatalf("healthy close after early break (iterator leaked if non-nil/panic): %v", err)
	}
}
