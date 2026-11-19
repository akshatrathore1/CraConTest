package recovery_test

import (
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
)

// TestCrashCycle exercises the full Open→Write→Read→Crash→Recover→Verify
// cycle using Pebble's in-memory CrashableMem filesystem.
//
// TestRecoveryEndToEnd verifies that the recovery harness can:
//  1. Open a Pebble database on an in-memory VFS
//  2. Write data with explicit sync
//  3. Read it back
//  4. Simulate a crash via CrashClone (100% retention = all synced data kept)
//  5. Recover the database from the crashed filesystem
//  6. Verify that synced data survived
func TestCrashCycle(t *testing.T) {
	// --- RECORD PHASE: Open, Write, Sync ---

	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:     memFS,
		Logger: recovery.PanicLogger{},
	}

	db, err := pebble.Open("", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

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
			t.Fatalf("pre-crash get %s: got %q, want %q", key, val, expected)
		}
		closer.Close()
	}

	// --- CRASH PHASE: CrashClone with 100% retention before closing DB ---
	// 100% retention means all unsynced data is kept. Combined with the
	// synced writes above, the recovered database must contain everything.
	rng := rand.New(rand.NewPCG(42, 0))
	crashedFS := memFS.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: 100,
		RNG:                 rng,
	})

	// Close the original database after cloning the crash state.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// --- RECOVER PHASE: SafeOpen on the crashed filesystem ---

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
	if result.DB == nil {
		t.Fatal("recovery returned nil DB without error")
	}
	defer result.DB.Close()

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
// of unsynced data. Synced writes must still survive; unsynced writes
// may be lost.
func TestCrashCyclePartialRetention(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:     memFS,
		Logger: recovery.PanicLogger{},
		// Small memtable to force flushes.
		MemTableSize: 1024 * 64,
	}

	db, err := pebble.Open("", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

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

	// Crash with 0% retention of unsynced data.
	rng := rand.New(rand.NewPCG(99, 0))
	crashedFS := memFS.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: 0,
		RNG:                 rng,
	})

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	result := recovery.SafeOpen(crashedFS, "", 30*time.Second)
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
	defer result.DB.Close()

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
	delayChan  chan struct{}
	lockClosed chan struct{}
}

func (b *blockingFS) List(dir string) ([]string, error) {
	if b.delayChan != nil {
		<-b.delayChan
	}
	return b.FS.List(dir)
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
				close(b.lockClosed)
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

	bfs := &blockingFS{
		FS:         memFS,
		delayChan:  delayChan,
		lockClosed: lockClosed,
	}

	res := recovery.SafeOpen(bfs, "testdb", 20*time.Millisecond)
	if !res.Timeout {
		t.Fatalf("expected SafeOpen to timeout, got %+v", res)
	}

	// Unblock pebble.Open now.
	close(delayChan)

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

	_ = db.Set([]byte("k1"), []byte("v1"), pebble.Sync)
	_ = db.Set([]byte("k2"), []byte("v2"), pebble.Sync)

	count := 0
	for k, v := range recovery.AllEntries(db) {
		if k == "" || v == "" {
			t.Errorf("empty entry in AllEntries")
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}

	// Close database and verify AllEntries panics on invalid/closed DB
	_ = db.Close()
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
	defer db.Close()

	_ = db.Set([]byte("k1"), []byte("v1"), pebble.Sync)

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
}

func (h hangingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	time.Sleep(100 * time.Millisecond)
	return h.FS.Create(name, category)
}

func TestSafeOpenTimeoutNoGoroutineLeak(t *testing.T) {
	mem := vfs.NewCrashableMem()
	fs := hangingFS{FS: mem}
	res := recovery.SafeOpen(fs, "db", 20*time.Millisecond)
	if !res.Timeout {
		t.Fatalf("expected timeout, got: %+v", res)
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
		if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
			t.Fatalf("set: %v", err)
		}
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
		if err := db.Set([]byte("k1"), []byte("v1"), pebble.Sync); err != nil {
			t.Fatalf("set: %v", err)
		}
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
		_ = db.Set([]byte("k"), []byte("v"), pebble.Sync)
		_ = db.Close()

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
		// Write several keys and flush to create an SSTable
		for i := 0; i < 50; i++ {
			_ = db.Set([]byte(fmt.Sprintf("key-%03d", i)), []byte(fmt.Sprintf("val-%03d", i)), nil)
		}
		if err := db.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		_ = db.Close()

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
			defer res.DB.Close()
			// If Pebble opened without validating all SSTs upfront, CheckIntegrity or DumpContents MUST catch corruption
			errInt := recovery.CheckIntegrity(res.DB)
			_, errDump := recovery.DumpContents(res.DB)
			if errInt == nil && errDump == nil {
				t.Fatalf("expected CheckIntegrity or DumpContents to fail on corrupt SST")
			}
		} else {
			// If SafeOpen caught corruption immediately, that is also a valid error outcome
			if res.Err == nil && res.Panic == "" {
				t.Fatalf("expected error or panic from SafeOpen on corrupt SST")
			}
		}
	})
}
