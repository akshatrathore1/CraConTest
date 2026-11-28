package replay_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
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

func run1000OpWorkload(t *testing.T, recFS *vfsrec.FS) {
	listener := recFS.EventListener()
	opts := &pebble.Options{
		FS:                          recFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		DisableAutomaticCompactions: true,
		EventListener:               &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	for i := 0; i < 1000; i++ {
		opID := uint64(i + 1)
		recFS.Begin(opID, "set")

		key := fmt.Sprintf("k%05d", i)
		val := fmt.Sprintf("v%05d-data-payload-%04d", i, i*7%1000)
		wo := pebble.NoSync
		if i%10 == 0 {
			wo = pebble.Sync
		}
		if err := db.Set([]byte(key), []byte(val), wo); err != nil {
			recFS.End(opID, "set", err.Error())
			t.Fatalf("Set op %d: %v", i, err)
		}
		recFS.End(opID, "set", "")

		if i > 0 && i%200 == 0 {
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush at %d: %v", i, err)
			}
		}
	}

	if err := db.Flush(); err != nil {
		t.Fatalf("final Flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
}

func computeFSDigest(fs vfs.FS, dir string) (map[string][32]byte, error) {
	entries, err := fs.List(dir)
	if err != nil {
		return nil, err
	}
	digest := make(map[string][32]byte)
	for _, entry := range entries {
		p := fs.PathJoin(dir, entry)
		info, err := fs.Stat(p)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			sub, err := computeFSDigest(fs, p)
			if err != nil {
				return nil, err
			}
			for k, v := range sub {
				digest[k] = v
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
			digest[p] = sha256.Sum256(content)
		}
	}
	return digest, nil
}

func digestsEqual(a, b map[string][32]byte) (bool, string) {
	if len(a) != len(b) {
		return false, fmt.Sprintf("file count mismatch: len(a)=%d, len(b)=%d", len(a), len(b))
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false, fmt.Sprintf("file %q missing in b", k)
		}
		if va != vb {
			return false, fmt.Sprintf("content mismatch for file %q: %x vs %x", k, va, vb)
		}
	}
	return true, ""
}

// TestGateG1 executes the Gate G1 acceptance verification:
// 1. Records a 1,000-operation Pebble workload on CrashableMem.
// 2. Selects 20 seeded cutoffs from the trace.
// 3. Captures live 100% clone snapshots at exactly those cutoffs via SetHook.
// 4. Replays each prefix with replay.Build and clones at 100%.
// 5. Verifies byte-for-byte digest equality between live and replayed clones.
// 6. Recovers the database from replayed crash clones via recovery.SafeOpen.
// 7. Records event count, trace size, and replay+clone latency.
func TestGateG1(t *testing.T) {
	// Step 1: Preliminary pass to record trace and pick 20 cutoffs.
	memPre := vfs.NewCrashableMem()
	recPre := vfsrec.New(memPre)
	run1000OpWorkload(t, recPre)

	events := recPre.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("Gate G1: preliminary trace invalid: %v", err)
	}

	cuts, err := trace.CutIDs(events)
	if err != nil {
		t.Fatalf("Gate G1: CutIDs: %v", err)
	}
	if len(cuts) < 20 {
		t.Fatalf("Gate G1: expected >= 20 cuts, got %d", len(cuts))
	}

	// Pick 20 seeded cutoffs (excluding the final tail cuts so minor background cleaner
	// variance at db.Close cannot place a chosen cut past the live trace length).
	pool := cuts
	if len(pool) > 30 {
		pool = cuts[:len(cuts)-10]
	}
	rng := rand.New(rand.NewPCG(42, 1))
	perm := rng.Perm(len(pool))
	chosenCuts := make([]uint64, 20)
	for i := 0; i < 20; i++ {
		chosenCuts[i] = pool[perm[i]]
	}
	sort.Slice(chosenCuts, func(i, j int) bool { return chosenCuts[i] < chosenCuts[j] })
	targetSet := make(map[uint64]bool, 20)
	for _, c := range chosenCuts {
		targetSet[c] = true
	}

	// Step 2: Live recording pass with SetHook to snapshot live 0% and 100% clones at chosen cuts.
	memLive := vfs.NewCrashableMem()
	recLive := vfsrec.New(memLive)
	liveDigests0 := make(map[uint64]map[string][32]byte, 20)
	liveDigests100 := make(map[uint64]map[string][32]byte, 20)
	var hookErr error

	digestClone := func(fs vfs.FS) (map[string][32]byte, error) {
		d, err := computeFSDigest(fs, "db")
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// db directory may not exist yet at early cutoffs.
				return make(map[string][32]byte), nil
			}
			return nil, err
		}
		return d, nil
	}

	recLive.SetHook(func(id uint64) {
		if targetSet[id] {
			// Take live 0% and 100% clone snapshots with mutex held.
			clone0 := memLive.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
			d0, err := digestClone(clone0)
			if err != nil && hookErr == nil {
				hookErr = fmt.Errorf("cut %d live 0%% digest: %w", id, err)
				return
			}
			liveDigests0[id] = d0

			clone100 := memLive.CrashClone(vfs.CrashCloneCfg{
				UnsyncedDataPercent: 100,
				RNG:                 rand.New(rand.NewPCG(0, id)),
			})
			d100, err := digestClone(clone100)
			if err != nil && hookErr == nil {
				hookErr = fmt.Errorf("cut %d live 100%% digest: %w", id, err)
				return
			}
			liveDigests100[id] = d100
		}
	})

	run1000OpWorkload(t, recLive)
	if hookErr != nil {
		t.Fatalf("Gate G1: hook error: %v", hookErr)
	}
	liveEvents := recLive.Events()
	if err := trace.Validate(liveEvents); err != nil {
		t.Fatalf("Gate G1: live trace validation failed: %v", err)
	}

	var traceBuf bytes.Buffer
	if err := trace.Encode(&traceBuf, liveEvents); err != nil {
		t.Fatalf("Gate G1: trace encode failed: %v", err)
	}
	traceSize := traceBuf.Len()

	// Step 3: Replay and compare live vs replayed clones at each of the 20 cutoffs.
	var totalReplayCloneDur time.Duration

	for _, cut := range chosenCuts {
		liveDigest0, ok0 := liveDigests0[cut]
		liveDigest100, ok100 := liveDigests100[cut]
		if !ok0 || !ok100 {
			t.Fatalf("Gate G1: missing live snapshot for cut %d (len=%d)", cut, len(liveEvents))
		}

		start := time.Now()

		state, err := replay.Build(liveEvents, cut)
		if err != nil {
			t.Fatalf("replay.Build at cut %d: %v", cut, err)
		}

		replayedClone0, err := state.Clone(0, cut)
		if err != nil {
			_ = state.Close()
			t.Fatalf("state.Clone(0) at cut %d: %v", cut, err)
		}

		replayedClone50a, err := state.Clone(50, cut)
		if err != nil {
			_ = state.Close()
			t.Fatalf("state.Clone(50) at cut %d: %v", cut, err)
		}
		replayedClone50b, err := state.Clone(50, cut)
		if err != nil {
			_ = state.Close()
			t.Fatalf("state.Clone(50) repeat at cut %d: %v", cut, err)
		}

		replayedClone, err := state.Clone(100, cut)
		if err != nil {
			_ = state.Close()
			t.Fatalf("state.Clone(100) at cut %d: %v", cut, err)
		}
		if err := state.Close(); err != nil {
			t.Fatalf("state.Close at cut %d: %v", cut, err)
		}

		dur := time.Since(start)
		totalReplayCloneDur += dur

		// Verify 0% byte-for-byte equality against live 0% clone digest.
		replayedDigest0, err := digestClone(replayedClone0)
		if err != nil {
			t.Fatalf("Gate G1: replayed 0%% digest at cut %d: %v", cut, err)
		}
		if ok, diff := digestsEqual(liveDigest0, replayedDigest0); !ok {
			t.Fatalf("Gate G1: 0%% byte-for-byte mismatch at cut %d: %s", cut, diff)
		}

		// Verify 50% deterministic seed reproducibility and path containment (0% <= 50% <= 100%).
		replayedDigest50a, err := digestClone(replayedClone50a)
		if err != nil {
			t.Fatalf("Gate G1: replayed 50%% digest at cut %d: %v", cut, err)
		}
		replayedDigest50b, err := digestClone(replayedClone50b)
		if err != nil {
			t.Fatalf("Gate G1: replayed 50%% repeat digest at cut %d: %v", cut, err)
		}
		if ok, diff := digestsEqual(replayedDigest50a, replayedDigest50b); !ok {
			t.Fatalf("Gate G1: 50%% non-deterministic at cut %d: %s", cut, diff)
		}

		// Verify 100% byte-for-byte equality against live 100% clone digest.
		replayedDigest, err := digestClone(replayedClone)
		if err != nil {
			t.Fatalf("Gate G1: replayed 100%% digest at cut %d: %v", cut, err)
		}
		if ok, diff := digestsEqual(liveDigest100, replayedDigest); !ok {
			t.Fatalf("Gate G1: 100%% byte-for-byte mismatch at cut %d: %s", cut, diff)
		}
		for p := range replayedDigest0 {
			if _, ok := replayedDigest50a[p]; !ok {
				t.Fatalf("Gate G1: path %q in Clone(0) missing from Clone(50) at cut %d", p, cut)
			}
		}
		for p := range replayedDigest50a {
			if _, ok := replayedDigest[p]; !ok {
				t.Fatalf("Gate G1: path %q in Clone(50) missing from Clone(100) at cut %d", p, cut)
			}
		}

		// Verify recovery via recovery.SafeOpen.
		res := recovery.SafeOpen(replayedClone, "db", 10*time.Second)
		if res.Err != nil || res.Panic != "" || res.Timeout {
			t.Fatalf("Gate G1: SafeOpen did not recover at cut %d: err=%v, panic=%s, timeout=%v",
				cut, res.Err, res.Panic, res.Timeout)
		}
		if res.DB != nil {
			_ = res.DB.Close()
		}
	}

	avgReplayCloneTime := totalReplayCloneDur / time.Duration(len(chosenCuts))
	t.Logf("=== Gate G1 Acceptance Report ===")
	t.Logf("Total Operations: 1,000")
	t.Logf("Total Events: %d", len(liveEvents))
	t.Logf("Encoded Trace Size: %d bytes (%.2f KB)", traceSize, float64(traceSize)/1024.0)
	t.Logf("Verified Cutoffs: 20/20 byte-for-byte exact matches")
	t.Logf("Average Replay+Clone Time: %v per candidate", avgReplayCloneTime)
	t.Logf("Total Replay+Clone Time: %v", totalReplayCloneDur)
}
