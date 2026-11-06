package exp

import (
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func run5825ProbeWorkload(t *testing.T, seed uint64, faultID string) (
	[]candidate.Candidate,
	[]trace.Event,
	[]oracle.Outcome,
	*graph.View,
	[]history.Entry,
) {
	t.Helper()
	wCfg := workload.Config{
		Seed:                seed,
		Operations:          100,
		KeyCount:            32,
		ValueBytes:          96,
		SyncEvery:           7,
		FlushEvery:          25,
		MaxManifestFileSize: 1,
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	fCfg := faults.DefaultConfig(faultID)
	faultFS, err := faults.NewWithConfig(fCfg, rec)
	if err != nil {
		t.Fatalf("seed %d fault %s: init fault: %v", seed, faultID, err)
	}

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                          faultFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		EventListener:               &listener,
		DisableAutomaticCompactions: true,
		DisableTableStats:           true,
		L0CompactionThreshold:       1000,
		L0StopWritesThreshold:       1000,
		MaxManifestFileSize:         1,
	}
	wCfg.FS = faultFS

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("seed %d fault %s: open pebble: %v", seed, faultID, err)
	}

	rawHist, err := workload.Run(db, rec, wCfg)
	if err != nil {
		_ = db.Close()
		t.Fatalf("seed %d fault %s: run workload: %v", seed, faultID, err)
	}
	db.TestOnlyWaitForCleaning()
	if err := db.Close(); err != nil {
		t.Fatalf("seed %d fault %s: close pebble: %v", seed, faultID, err)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("seed %d fault %s: validate trace: %v", seed, faultID, err)
	}

	correlated, err := workload.Correlate(rawHist, traceEvents)
	if err != nil {
		t.Fatalf("seed %d fault %s: correlate history: %v", seed, faultID, err)
	}

	policies := []int{0, 50, 100}
	universe, err := candidate.Build(traceEvents, seed, policies)
	if err != nil {
		t.Fatalf("seed %d fault %s: build universe: %v", seed, faultID, err)
	}

	view, err := graph.Build(traceEvents)
	if err != nil {
		t.Fatalf("seed %d fault %s: build graph: %v", seed, faultID, err)
	}

	expRes, err := RunExplore(RunnerConfig{
		Universe: universe,
		Events:   traceEvents,
		History:  correlated,
		Dir:      "db",
		Timeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("seed %d fault %s: run explore: %v", seed, faultID, err)
	}

	return universe, traceEvents, expRes.Outcomes, view, correlated
}

// TestCaseStudyPebble5825CrashPointProbe provides empirical proof of the Pebble #5825
// causal directory race and its single-entry surgical counterfactual repair.
func TestCaseStudyPebble5825CrashPointProbe(t *testing.T) {
	seeds := []uint64{42, 100, 200}

	contains := func(list []string, item string) bool {
		for _, x := range list {
			if x == item {
				return true
			}
		}
		return false
	}

	for _, seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			_, traceEvents, outcomes, view, correlated := run5825ProbeWorkload(t, seed, faults.MUT0)

			type failureRecord struct {
				cand    candidate.Candidate
				outcome oracle.Outcome
			}
			var failingCandidates []failureRecord

			for _, o := range outcomes {
				if o.Class != oracle.ClassValid {
					if o.Candidate.Percent != 50 {
						t.Fatalf("seed %d: expected failing candidate to have Percent==50, got %d", seed, o.Candidate.Percent)
					}
					failingCandidates = append(failingCandidates, failureRecord{
						cand:    o.Candidate,
						outcome: o,
					})
				}
			}

			if len(failingCandidates) == 0 {
				t.Fatalf("seed %d: expected failing candidates on MUT-0, got 0", seed)
			}
			t.Logf("seed %d: verified %d failing candidates (100%% at 50%% retention)", seed, len(failingCandidates))

			for _, rec := range failingCandidates {
				cand := rec.cand
				outcome := rec.outcome

				// 1. Replay traceEvents[:cand.Event]
				state, err := replay.Replay(traceEvents[:cand.Event])
				if err != nil {
					t.Fatalf("seed %d cand %v: replay failed: %v", seed, cand, err)
				}

				// 2. Produce three directory snapshots of "db"
				base0, err := state.Clone(0, cand.Seed)
				if err != nil {
					_ = state.Close()
					t.Fatalf("seed %d cand %v: clone 0 failed: %v", seed, cand, err)
				}
				full100, err := state.Clone(100, cand.Seed)
				if err != nil {
					_ = state.Close()
					t.Fatalf("seed %d cand %v: clone 100 failed: %v", seed, cand, err)
				}
				crash50, err := state.Clone(50, cand.Seed)
				if err != nil {
					_ = state.Close()
					t.Fatalf("seed %d cand %v: clone 50 failed: %v", seed, cand, err)
				}
				_ = state.Close()

				// 3. Inspect base0, full100, and crash50
				entries0, err := base0.List("db")
				if err != nil {
					t.Fatalf("seed %d cand %v: base0.List failed: %v", seed, cand, err)
				}
				entries100, err := full100.List("db")
				if err != nil {
					t.Fatalf("seed %d cand %v: full100.List failed: %v", seed, cand, err)
				}
				entries50, err := crash50.List("db")
				if err != nil {
					t.Fatalf("seed %d cand %v: crash50.List failed: %v", seed, cand, err)
				}

				var latestMarker string
				var latestIter uint64
				var targetManifest string

				for _, e := range entries100 {
					if strings.HasPrefix(e, "marker.manifest.") {
						parts := strings.Split(e, ".")
						if len(parts) >= 4 {
							iter, err := strconv.ParseUint(parts[2], 16, 64)
							if err != nil {
								iter, err = strconv.ParseUint(parts[2], 10, 64)
							}
							if iter >= latestIter {
								latestIter = iter
								latestMarker = e
								targetManifest = strings.Join(parts[3:], ".")
							}
						}
					}
				}

				if latestMarker == "" || targetManifest == "" {
					t.Fatalf("seed %d cand %v: could not find latest marker.manifest in full100", seed, cand)
				}

				t.Logf("seed %d cand %v detail=%q\n  base0=%v\n  full100=%v\n  crash50=%v\n  latestMarker=%s targetManifest=%s",
					seed, cand, outcome.Detail, entries0, entries100, entries50, latestMarker, targetManifest)

				// Assert in full100 both exist, neither in base0
				if !contains(entries100, targetManifest) {
					t.Fatalf("seed %d cand %v: full100 missing target manifest %s", seed, cand, targetManifest)
				}
				if !contains(entries100, latestMarker) {
					t.Fatalf("seed %d cand %v: full100 missing marker %s", seed, cand, latestMarker)
				}
				if contains(entries0, targetManifest) {
					t.Fatalf("seed %d cand %v: base0 contains target manifest %s", seed, cand, targetManifest)
				}
				if contains(entries0, latestMarker) {
					t.Fatalf("seed %d cand %v: base0 contains marker %s", seed, cand, latestMarker)
				}

				// Assert in crash50: marker survived, targetManifest lost
				if !contains(entries50, latestMarker) {
					t.Fatalf("seed %d cand %v: crash50 expected marker %s to survive", seed, cand, latestMarker)
				}
				if contains(entries50, targetManifest) {
					t.Fatalf("seed %d cand %v: crash50 expected target manifest %s to be lost", seed, cand, targetManifest)
				}

				// Assert outcome.Detail contains targetManifest
				if !strings.Contains(outcome.Detail, targetManifest) {
					t.Fatalf("seed %d cand %v: outcome.Detail %q does not contain target manifest %q", seed, cand, outcome.Detail, targetManifest)
				}

				// 4. Counterfactual Surgical Single-Entry Repair Proof
				stateRepair, err := replay.Replay(traceEvents[:cand.Event])
				if err != nil {
					t.Fatalf("seed %d cand %v: replay for repair failed: %v", seed, cand, err)
				}
				freshCrash50, err := stateRepair.Clone(50, cand.Seed)
				_ = stateRepair.Close()
				if err != nil {
					t.Fatalf("seed %d cand %v: fresh clone 50 failed: %v", seed, cand, err)
				}

				srcPath := "db/" + targetManifest
				srcF, err := full100.Open(srcPath)
				if err != nil {
					t.Fatalf("seed %d cand %v: open full100 %s: %v", seed, cand, srcPath, err)
				}
				data, err := io.ReadAll(srcF)
				_ = srcF.Close()
				if err != nil {
					t.Fatalf("seed %d cand %v: read full100 %s: %v", seed, cand, srcPath, err)
				}

				dstF, err := freshCrash50.Create(srcPath, vfs.WriteCategoryUnspecified)
				if err != nil {
					t.Fatalf("seed %d cand %v: create freshCrash50 %s: %v", seed, cand, srcPath, err)
				}
				if _, err := dstF.Write(data); err != nil {
					t.Fatalf("seed %d cand %v: write freshCrash50 %s: %v", seed, cand, srcPath, err)
				}
				_ = dstF.Sync()
				if err := dstF.Close(); err != nil {
					t.Fatalf("seed %d cand %v: close freshCrash50 %s: %v", seed, cand, srcPath, err)
				}

				openRes := recovery.SafeOpen(freshCrash50, "db", 3*time.Second)
				if openRes.Err != nil || openRes.DB == nil {
					t.Fatalf("seed %d cand %v: SafeOpen after repair failed: %v (detail: %s)", seed, cand, openRes.Err, openRes.Panic)
				}
				if err := recovery.CheckIntegrity(openRes.DB); err != nil {
					_ = openRes.DB.Close()
					t.Fatalf("seed %d cand %v: CheckIntegrity after repair failed: %v", seed, cand, err)
				}
				dump, err := recovery.DumpContents(openRes.DB)
				_ = openRes.DB.Close()
				if err != nil {
					t.Fatalf("seed %d cand %v: DumpContents after repair failed: %v", seed, cand, err)
				}

				cutCategories := view.Cuts[cand.Event].Categories
				repairedOutcome := oracle.Check(oracle.Context{
					Candidate:  cand,
					Events:     traceEvents,
					History:    correlated,
					Recovered:  dump,
					Categories: cutCategories,
				})
				if repairedOutcome.Class != oracle.ClassValid {
					t.Fatalf("seed %d cand %v: expected ClassValid after surgical repair, got %s (detail: %s)",
						seed, cand, repairedOutcome.Class, repairedOutcome.Detail)
				}
			}

			// Assert that running the same seed with BARRIER-5825 produces 0 failures
			_, _, barrierOutcomes, _, _ := run5825ProbeWorkload(t, seed, faults.Barrier5825)
			for _, bo := range barrierOutcomes {
				if bo.Class != oracle.ClassValid {
					t.Fatalf("seed %d BARRIER-5825: expected 0 failures, got outcome %v (class: %s, detail: %s)",
						seed, bo.Candidate, bo.Class, bo.Detail)
				}
			}
			t.Logf("seed %d: verified BARRIER-5825 produces 0 failures across full universe (PASS)", seed)
		})
	}
}
