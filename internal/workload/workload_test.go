package workload_test

import (
	"bytes"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type testMarker struct {
	begins []uint64
	ends   []uint64
}

func (m *testMarker) Begin(op uint64, kind string) {
	m.begins = append(m.begins, op)
}

func (m *testMarker) End(op uint64, kind string, err string) {
	m.ends = append(m.ends, op)
}

func TestWorkloadPlanDeterminism(t *testing.T) {
	cfg := workload.DefaultConfig()
	p1 := workload.Plan(cfg)
	p2 := workload.Plan(cfg)

	if len(p1) != len(p2) {
		t.Fatalf("plan len mismatch: %d vs %d", len(p1), len(p2))
	}
	for i := range p1 {
		if p1[i].Op != p2[i].Op || p1[i].Kind != p2[i].Kind || p1[i].Sync != p2[i].Sync ||
			!bytes.Equal(p1[i].Key, p2[i].Key) || !bytes.Equal(p1[i].Value, p2[i].Value) {
			t.Fatalf("plan mismatch at step %d: %+v vs %+v", i, p1[i], p2[i])
		}
	}
}

func TestWorkloadMixDistribution(t *testing.T) {
	cfg := workload.DefaultConfig()
	cfg.Operations = 10000 // larger sample to check ratio
	plan := workload.Plan(cfg)

	counts := make(map[string]int)
	for _, op := range plan {
		if !op.IsFlushOp {
			counts[op.Kind]++
		}
	}

	total := cfg.Operations
	setRatio := float64(counts["set"]) / float64(total)
	getRatio := float64(counts["get"]) / float64(total)
	delRatio := float64(counts["delete"]) / float64(total)
	batchRatio := float64(counts["batch"]) / float64(total)
	rangeRatio := float64(counts["range_delete"]) / float64(total)
	iterRatio := float64(counts["iterate"]) / float64(total)

	// Expected: 50%, 20%, 10%, 10%, 5%, 5% (within 2% tolerance on 10k ops)
	if setRatio < 0.48 || setRatio > 0.52 {
		t.Errorf("set ratio: got %.3f, want ~0.50", setRatio)
	}
	if getRatio < 0.18 || getRatio > 0.22 {
		t.Errorf("get ratio: got %.3f, want ~0.20", getRatio)
	}
	if delRatio < 0.08 || delRatio > 0.12 {
		t.Errorf("delete ratio: got %.3f, want ~0.10", delRatio)
	}
	if batchRatio < 0.08 || batchRatio > 0.12 {
		t.Errorf("batch ratio: got %.3f, want ~0.10", batchRatio)
	}
	if rangeRatio < 0.04 || rangeRatio > 0.06 {
		t.Errorf("range_delete ratio: got %.3f, want ~0.05", rangeRatio)
	}
	if iterRatio < 0.04 || iterRatio > 0.06 {
		t.Errorf("iterate ratio: got %.3f, want ~0.05", iterRatio)
	}
}

func TestWorkloadExecutionWithPebble(t *testing.T) {
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

	cfg := workload.Config{
		Seed:       42,
		Operations: 100,
		KeyCount:   16,
		ValueBytes: 64,
		SyncEvery:  10,
		FlushEvery: 50,
	}

	entries, err := workload.Run(db, rec, cfg)
	if err != nil {
		t.Fatalf("workload.Run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	if err := history.Validate(entries); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	correlated, err := workload.Correlate(entries, traceEvents)
	if err != nil {
		t.Fatalf("workload.Correlate: %v", err)
	}
	if err := history.Validate(correlated); err != nil {
		t.Fatalf("correlated history.Validate: %v", err)
	}
}

func TestWorkloadCompactionPlanningAndExecution(t *testing.T) {
	cfg := workload.Config{
		Seed:       42,
		Operations: 50,
		KeyCount:   16,
		ValueBytes: 64,
		SyncEvery:  5,
		FlushEvery: 20,
		CompactAt:  25,
	}

	plan := workload.Plan(cfg)
	foundCompact := false
	for _, op := range plan {
		if op.Kind == "compact" {
			foundCompact = true
			if op.Mutating {
				t.Errorf("compact operation must be non-mutating")
			}
		}
	}
	if !foundCompact {
		t.Fatalf("expected compact operation in plan, but none found")
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                          rec,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		EventListener:               &listener,
		DisableAutomaticCompactions: true,
	}

	db, err := pebble.Open("db_compact", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	entries, err := workload.Run(db, rec, cfg)
	if err != nil {
		_ = db.Close()
		t.Fatalf("workload.Run: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	if err := history.Validate(entries); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}

	foundHistCompact := false
	for _, e := range entries {
		if e.Kind == "compact" {
			foundHistCompact = true
			if e.Err != "" {
				t.Errorf("compact history entry has unexpected error: %s", e.Err)
			}
			if len(e.Mutations) != 0 {
				t.Errorf("compact history entry should have no mutations")
			}
		}
	}
	if !foundHistCompact {
		t.Fatalf("expected compact entry in history ledger, but none found")
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	correlated, err := workload.Correlate(entries, traceEvents)
	if err != nil {
		t.Fatalf("workload.Correlate: %v", err)
	}
	if err := history.Validate(correlated); err != nil {
		t.Fatalf("correlated history.Validate: %v", err)
	}
}
