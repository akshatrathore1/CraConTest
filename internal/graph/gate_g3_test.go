package graph_test

import (
	"sort"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// TestGateG3 verifies Milestone M8.1 (Gate G3 Acceptance):
// 1. graph.Build succeeds on the real 1,000-operation fixed-workload trace.
// 2. Every universe candidate has an OpKind and non-nil Categories.
// 3. Vertex/edge counts and build time are recorded.
// 4. Semantic share, distinct StateKeys, and Desc distribution are measured.
func TestGateG3(t *testing.T) {
	t.Log("Generating real 1,000-operation workload trace...")
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:            rec,
		Logger:        recovery.PanicLogger{},
		MemTableSize:  64 * 1024,
		EventListener: &listener,
	}

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	cfg := workload.DefaultConfig()
	_, err = workload.Run(db, rec, cfg)
	if err != nil {
		t.Fatalf("workload.Run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}
	t.Logf("Real workload trace: %d events", len(events))

	// 1. Build graph view on real trace
	startBuild := time.Now()
	view, err := graph.Build(events)
	buildDuration := time.Since(startBuild)
	if err != nil {
		t.Fatalf("graph.Build failed: %v", err)
	}

	// 2. Build candidate universe
	universe, err := candidate.Build(events, cfg.Seed, []int{0, 100})
	if err != nil {
		t.Fatalf("candidate.Build: %v", err)
	}
	cuts, err := trace.CutIDs(events)
	if err != nil {
		t.Fatalf("trace.CutIDs: %v", err)
	}

	// 3. Verify features for every universe candidate
	distinctStateKeys := make(map[string]bool)
	var semanticCount int
	var descValues []uint64

	for _, c := range universe {
		feat, ok := view.Cuts[c.Event]
		if !ok {
			t.Fatalf("missing features for cutoff %d", c.Event)
		}
		if feat.OpKind == "" {
			t.Errorf("candidate %+v has empty OpKind", c)
		}
		if feat.Categories == nil {
			t.Errorf("candidate %+v has nil Categories", c)
		}
	}

	// Calculate cutoff-level distribution
	for _, cut := range cuts {
		feat := view.Cuts[cut]
		distinctStateKeys[feat.StateKey] = true
		if feat.Semantic == 1 {
			semanticCount++
		}
		descValues = append(descValues, feat.Desc)
	}

	sort.Slice(descValues, func(i, j int) bool { return descValues[i] < descValues[j] })
	semanticShare := float64(semanticCount) / float64(len(cuts))
	medianDesc := descValues[len(descValues)/2]
	minDesc := descValues[0]
	maxDesc := descValues[len(descValues)-1]

	var sumDesc uint64
	for _, d := range descValues {
		sumDesc += d
	}
	avgDesc := float64(sumDesc) / float64(len(descValues))

	t.Logf("=== Gate G3 Acceptance Results ===")
	t.Logf("Trace events:        %d", len(events))
	t.Logf("Graph build time:    %v", buildDuration)
	t.Logf("Cutoffs:             %d", len(cuts))
	t.Logf("Universe size:       %d candidates", len(universe))
	t.Logf("Distinct StateKeys:  %d", len(distinctStateKeys))
	t.Logf("Semantic=1 cutoffs:  %d / %d (%.2f%%)", semanticCount, len(cuts), semanticShare*100)
	t.Logf("Desc distribution:   min=%d, max=%d, median=%d, avg=%.2f", minDesc, maxDesc, medianDesc, avgDesc)

	if len(distinctStateKeys) < 10 {
		t.Errorf("expected diverse StateKeys, got only %d", len(distinctStateKeys))
	}
	if semanticShare <= 0 {
		t.Errorf("semantic share is 0%%")
	}
}
