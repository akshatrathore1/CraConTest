package recovery_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
)

// TestCostCheck1000Ops runs a 1,000-operation workload through vfsrec, replays
// up to the pre-close cutoff into a quiescent CrashableMem, and measures the
// cost per crash-recovery candidate: clone time, recovery time, and check time.
func TestCostCheck1000Ops(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	rec := vfsrec.New(memFS)
	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            rec,
		Logger:        recovery.PanicLogger{},
		MemTableSize:  1024 * 64, // Small memtable to force flushes.
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

	// Write 1,000 keys with mixed sync (every 10th is synced).
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("value-%06d-padding-to-increase-size", i)
		wo := pebble.NoSync
		if i%10 == 0 {
			wo = pebble.Sync
		}
		if err := db.Set([]byte(key), []byte(val), wo); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}

		// Force flush every 200 operations.
		if i > 0 && i%200 == 0 {
			if err := db.Flush(); err != nil {
				t.Fatalf("flush at %d: %v", i, err)
			}
		}
	}

	// Capture pre-close cutoff before closing so unsynced buffers in the replayed
	// trace are retained without mutating memFS concurrently with CrashClone.
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
	defer func() {
		if err := state.Close(); err != nil {
			t.Errorf("state.Close: %v", err)
		}
	}()

	// Measure costs across retention percentages and seeds.
	retentions := []int{0, 25, 50, 75, 100}
	seeds := []uint64{1, 2, 3, 4}

	var (
		totalClone   time.Duration
		totalRecover time.Duration
		totalCheck   time.Duration
		candidates   int
		checkedCount int
		openErrors   int
		openPanics   int
		timeouts     int
	)

	for _, ret := range retentions {
		for _, seed := range seeds {
			// --- Clone ---
			cloneStart := time.Now()
			crashedFS, err := state.Clone(ret, seed)
			cloneDur := time.Since(cloneStart)
			if err != nil {
				t.Fatalf("state.Clone(%d, %d): %v", ret, seed, err)
			}
			totalClone += cloneDur

			// --- Recover ---
			recoverStart := time.Now()
			result := recovery.SafeOpen(crashedFS, "db", 30*time.Second)
			recoverDur := time.Since(recoverStart)
			totalRecover += recoverDur
			candidates++

			if result.Timeout {
				timeouts++
				t.Errorf("timeout at retention=%d seed=%d", ret, seed)
				continue
			}
			if result.Panic != "" {
				openPanics++
				t.Errorf("panic at retention=%d seed=%d: %s", ret, seed, result.Panic)
				continue
			}
			if result.Err != nil {
				openErrors++
				t.Errorf("open error at retention=%d seed=%d: %v", ret, seed, result.Err)
				continue
			}
			if result.DB == nil {
				openErrors++
				t.Errorf("nil DB without error at retention=%d seed=%d", ret, seed)
				continue
			}

			// --- Check ---
			checkStart := time.Now()
			checkErr := recovery.CheckIntegrity(result.DB)
			checkDur := time.Since(checkStart)
			totalCheck += checkDur
			checkedCount++

			if checkErr != nil {
				t.Errorf("integrity error at retention=%d seed=%d: %v", ret, seed, checkErr)
			}

			if err := result.DB.Close(); err != nil {
				t.Errorf("close recovered DB at retention=%d seed=%d: %v", ret, seed, err)
			}
		}
	}

	if candidates == 0 || checkedCount == 0 {
		t.Fatalf("expected checked candidates > 0, got candidates=%d checked=%d", candidates, checkedCount)
	}
	if timeouts > 0 || openErrors > 0 || openPanics > 0 {
		t.Fatalf("cost check encountered recovery failures: timeouts=%d openErrors=%d openPanics=%d",
			timeouts, openErrors, openPanics)
	}

	// Report.
	avgClone := totalClone / time.Duration(candidates)
	avgRecover := totalRecover / time.Duration(candidates)
	avgCheck := totalCheck / time.Duration(checkedCount)
	avgTotal := avgClone + avgRecover + avgCheck

	t.Logf("=== COST CHECK REPORT (1,000 operations) ===")
	t.Logf("Candidates tested: %d (checked: %d)", candidates, checkedCount)
	t.Logf("Open errors: %d, Panics: %d, Timeouts: %d", openErrors, openPanics, timeouts)
	t.Logf("Total clone time:   %v (avg %v/candidate)", totalClone, avgClone)
	t.Logf("Total recover time: %v (avg %v/candidate)", totalRecover, avgRecover)
	t.Logf("Total check time:   %v (avg %v/checked)", totalCheck, avgCheck)
	t.Logf("Avg total per candidate: %v", avgTotal)

	// Estimate affordable universe size (target: 10 minutes total).
	tenMin := 10 * time.Minute
	if avgTotal > 0 {
		affordable := int(tenMin / avgTotal)
		t.Logf("Affordable candidates in 10 minutes: ~%d", affordable)
	} else {
		t.Logf("Affordable candidates in 10 minutes: >%d (avgTotal < clock resolution)", int64(tenMin))
	}
	t.Logf("=============================================")
}
