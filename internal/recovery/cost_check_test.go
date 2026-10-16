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

// TestCostCheck1000Ops runs a 1,000-operation workload, then measures
// the cost per crash-recovery candidate: clone time, recovery time,
// and check time. This determines how many candidates an exhaustive
// pass can afford.
func TestCostCheck1000Ops(t *testing.T) {
	memFS := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:           memFS,
		Logger:       recovery.PanicLogger{},
		MemTableSize: 1024 * 64, // Small memtable to force flushes.
	}

	db, err := pebble.Open("", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

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

	// Final flush and close.
	if err := db.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Measure costs at 20 evenly spaced "crash points" by using different
	// retention percentages to simulate variety.
	retentions := []int{0, 25, 50, 75, 100}
	seeds := []uint64{1, 2, 3, 4}

	var (
		totalClone    time.Duration
		totalRecover  time.Duration
		totalCheck    time.Duration
		candidates    int
		openErrors    int
		openPanics    int
	)

	for _, ret := range retentions {
		for _, seed := range seeds {
			// --- Clone ---
			cloneStart := time.Now()
			rng := rand.New(rand.NewPCG(seed, 0))
			crashedFS := memFS.CrashClone(vfs.CrashCloneCfg{
				UnsyncedDataPercent: ret,
				RNG:                 rng,
			})
			cloneDur := time.Since(cloneStart)
			totalClone += cloneDur

			// --- Recover ---
			recoverStart := time.Now()
			result := recovery.SafeOpen(crashedFS, "", 30*time.Second)
			recoverDur := time.Since(recoverStart)
			totalRecover += recoverDur

			if result.Timeout {
				t.Errorf("timeout at retention=%d seed=%d", ret, seed)
				candidates++
				continue
			}
			if result.Panic != "" {
				openPanics++
				candidates++
				continue
			}
			if result.Err != nil {
				openErrors++
				candidates++
				continue
			}

			// --- Check ---
			checkStart := time.Now()
			checkErr := recovery.CheckIntegrity(result.DB)
			checkDur := time.Since(checkStart)
			totalCheck += checkDur

			if checkErr != nil {
				t.Logf("integrity error at retention=%d seed=%d: %v", ret, seed, checkErr)
			}

			result.DB.Close()
			candidates++
		}
	}

	// Report.
	t.Logf("=== COST CHECK REPORT (1,000 operations) ===")
	t.Logf("Candidates tested: %d", candidates)
	t.Logf("Open errors: %d, Panics: %d", openErrors, openPanics)
	t.Logf("Total clone time:   %v (avg %v/candidate)", totalClone, totalClone/time.Duration(candidates))
	t.Logf("Total recover time: %v (avg %v/candidate)", totalRecover, totalRecover/time.Duration(candidates))
	t.Logf("Total check time:   %v (avg %v/candidate)", totalCheck, totalCheck/time.Duration(candidates))
	avgTotal := (totalClone + totalRecover + totalCheck) / time.Duration(candidates)
	t.Logf("Avg total per candidate: %v", avgTotal)

	// Estimate affordable universe size (target: 10 minutes total).
	tenMin := 10 * time.Minute
	affordable := int(tenMin / avgTotal)
	t.Logf("Affordable candidates in 10 minutes: ~%d", affordable)
	t.Logf("=============================================")
}
