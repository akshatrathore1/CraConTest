package recovery_test

import (
	"fmt"
	"math/rand/v2"
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

	// Close the database before crashing the filesystem.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// --- CRASH PHASE: CrashClone with 100% retention ---

	// 100% retention means all unsynced data is kept. Combined with the
	// synced writes above, the recovered database must contain everything.
	rng := rand.New(rand.NewPCG(42, 0))
	crashedFS := memFS.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: 100,
		RNG:                 rng,
	})

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
