package workload_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
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

func TestConfigMaxManifestFileSize(t *testing.T) {
	cfg := workload.DefaultConfig()
	if cfg.MaxManifestFileSize != 0 {
		t.Fatalf("expected default MaxManifestFileSize 0, got %d", cfg.MaxManifestFileSize)
	}

	cfg.MaxManifestFileSize = 1024 * 1024
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var parsed workload.Config
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.MaxManifestFileSize != 1024*1024 {
		t.Fatalf("expected MaxManifestFileSize %d, got %d", 1024*1024, parsed.MaxManifestFileSize)
	}
}

func TestWorkloadValidateAndPlanInvalidConfig(t *testing.T) {
	valid := workload.DefaultConfig()
	if err := workload.Validate(valid); err != nil {
		t.Fatalf("expected valid default config, got: %v", err)
	}

	invalidConfigs := []struct {
		name string
		cfg  workload.Config
	}{
		{"zero operations", func() workload.Config { c := valid; c.Operations = 0; return c }()},
		{"negative operations", func() workload.Config { c := valid; c.Operations = -5; return c }()},
		{"zero key count", func() workload.Config { c := valid; c.KeyCount = 0; return c }()},
		{"negative key count", func() workload.Config { c := valid; c.KeyCount = -1; return c }()},
		{"zero value bytes", func() workload.Config { c := valid; c.ValueBytes = 0; return c }()},
		{"negative value bytes", func() workload.Config { c := valid; c.ValueBytes = -10; return c }()},
		{"negative sync every", func() workload.Config { c := valid; c.SyncEvery = -1; return c }()},
		{"negative flush every", func() workload.Config { c := valid; c.FlushEvery = -1; return c }()},
		{"negative ingest every", func() workload.Config { c := valid; c.IngestEvery = -1; return c }()},
		{"negative ratchet at", func() workload.Config { c := valid; c.RatchetAt = -1; return c }()},
	}

	for _, tc := range invalidConfigs {
		t.Run(tc.name, func(t *testing.T) {
			if err := workload.Validate(tc.cfg); err == nil {
				t.Fatalf("expected Validate to return error for %s", tc.name)
			}
			plan := workload.Plan(tc.cfg)
			if plan != nil {
				t.Fatalf("expected Plan to return nil for invalid config %s, got len %d", tc.name, len(plan))
			}
			_, err := workload.Run(nil, nil, tc.cfg)
			if err == nil {
				t.Fatalf("expected Run to return error for invalid config %s", tc.name)
			}
		})
	}
}

func TestWorkloadCheckpointPlan(t *testing.T) {
	cfg := workload.Config{
		Seed:         42,
		Operations:   9,
		KeyCount:     8,
		ValueBytes:   16,
		SyncEvery:    0,
		FlushEvery:   0,
		CheckpointAt: 3,
	}
	plan := workload.Plan(cfg)
	if len(plan) != 12 {
		t.Fatalf("expected 12 planned ops (9 client + 3 checkpoint), got %d", len(plan))
	}

	var ckptIndices []int
	var ckptOps []uint64
	for idx, op := range plan {
		if op.Kind == "checkpoint" {
			ckptIndices = append(ckptIndices, idx)
			ckptOps = append(ckptOps, op.Op)
			if op.Mutating {
				t.Errorf("checkpoint op should not be mutating")
			}
		}
	}

	if len(ckptIndices) != 3 {
		t.Fatalf("expected 3 checkpoint ops, got %d", len(ckptIndices))
	}
	expectedOps := []uint64{4, 8, 12}
	for i, opID := range expectedOps {
		if ckptOps[i] != opID {
			t.Errorf("checkpoint %d: got Op %d, want %d", i, ckptOps[i], opID)
		}
	}
}

func TestWorkloadIngestAndRatchetPlan(t *testing.T) {
	cfg := workload.Config{
		Seed:        42,
		Operations:  20,
		KeyCount:    16,
		ValueBytes:  64,
		SyncEvery:   5,
		FlushEvery:  0,
		IngestEvery: 5,
		RatchetAt:   7,
	}

	plan := workload.Plan(cfg)
	if plan == nil {
		t.Fatalf("workload.Plan returned nil")
	}

	var ingestOps []workload.PlanOperation
	var ratchetOps []workload.PlanOperation

	for _, op := range plan {
		if op.Kind == "ingest" {
			ingestOps = append(ingestOps, op)
		} else if op.Kind == "ratchet" {
			ratchetOps = append(ratchetOps, op)
		}
	}

	// Operations: 20, IngestEvery: 5 => 4 ingests at i=4, 9, 14, 19
	if len(ingestOps) != 4 {
		t.Fatalf("expected 4 ingest ops, got %d", len(ingestOps))
	}

	// RatchetAt: 7 => 1 ratchet at i=6
	if len(ratchetOps) != 1 {
		t.Fatalf("expected 1 ratchet op, got %d", len(ratchetOps))
	}
	if ratchetOps[0].Mutating {
		t.Errorf("ratchet op must be non-mutating")
	}

	// Verify alternating single-file vs multi-file pairs
	for idx, op := range ingestOps {
		if !op.Mutating {
			t.Errorf("ingest op %d must be mutating", idx)
		}
		if !op.Sync {
			t.Errorf("ingest op %d must specify sync", idx)
		}

		expectedMulti := (idx%2 == 1)
		if op.MultiFile != expectedMulti {
			t.Errorf("ingest op %d: expected MultiFile %v, got %v", idx, expectedMulti, op.MultiFile)
		}

		if !op.MultiFile {
			if len(op.Mutations) != 2 {
				t.Fatalf("ingest op %d: expected 2 mutations for single-file, got %d", idx, len(op.Mutations))
			}
			if bytes.Compare(op.Mutations[0].Key, op.Mutations[1].Key) >= 0 {
				t.Errorf("ingest op %d: single-file keys must be strictly ascending: %s >= %s",
					idx, op.Mutations[0].Key, op.Mutations[1].Key)
			}
		} else {
			if len(op.Mutations) != 4 {
				t.Fatalf("ingest op %d: expected 4 mutations for multi-file, got %d", idx, len(op.Mutations))
			}
			// SSTable 1: mutations[0..1]
			if bytes.Compare(op.Mutations[0].Key, op.Mutations[1].Key) >= 0 {
				t.Errorf("ingest op %d: SSTable 1 keys must be strictly ascending: %s >= %s",
					idx, op.Mutations[0].Key, op.Mutations[1].Key)
			}
			// SSTable 2: mutations[2..3]
			if bytes.Compare(op.Mutations[2].Key, op.Mutations[3].Key) >= 0 {
				t.Errorf("ingest op %d: SSTable 2 keys must be strictly ascending: %s >= %s",
					idx, op.Mutations[2].Key, op.Mutations[3].Key)
			}
			// Cross-SSTable non-overlap: SSTable 1 max key < SSTable 2 min key
			if bytes.Compare(op.Mutations[1].Key, op.Mutations[2].Key) >= 0 {
				t.Errorf("ingest op %d: SSTables must not overlap: %s >= %s",
					idx, op.Mutations[1].Key, op.Mutations[2].Key)
			}
		}

		for mIdx, m := range op.Mutations {
			if m.Kind != "set" {
				t.Errorf("ingest op %d mutation %d: expected kind 'set', got %q", idx, mIdx, m.Kind)
			}
			if len(m.Key) == 0 {
				t.Errorf("ingest op %d mutation %d: key cannot be empty", idx, mIdx)
			}
			if len(m.Value) == 0 {
				t.Errorf("ingest op %d mutation %d: value cannot be empty", idx, mIdx)
			}
			if len(m.End) != 0 {
				t.Errorf("ingest op %d mutation %d: end key must be empty", idx, mIdx)
			}
		}
	}
}

func TestWorkloadBaselineInvariance(t *testing.T) {
	baseCfg := workload.DefaultConfig()
	pBase := workload.Plan(baseCfg)

	zeroCfg := baseCfg
	zeroCfg.IngestEvery = 0
	zeroCfg.RatchetAt = 0
	pZero := workload.Plan(zeroCfg)

	if len(pBase) != len(pZero) {
		t.Fatalf("baseline plan len mismatch: %d vs %d", len(pBase), len(pZero))
	}
	for i := range pBase {
		if pBase[i].Op != pZero[i].Op ||
			pBase[i].Kind != pZero[i].Kind ||
			pBase[i].Mutating != pZero[i].Mutating ||
			pBase[i].Sync != pZero[i].Sync ||
			!bytes.Equal(pBase[i].Key, pZero[i].Key) ||
			!bytes.Equal(pBase[i].Value, pZero[i].Value) ||
			!bytes.Equal(pBase[i].EndKey, pZero[i].EndKey) ||
			!bytes.Equal(pBase[i].Key2, pZero[i].Key2) ||
			!bytes.Equal(pBase[i].Value2, pZero[i].Value2) ||
			pBase[i].IsFlushOp != pZero[i].IsFlushOp ||
			pBase[i].MultiFile != pZero[i].MultiFile ||
			len(pBase[i].Mutations) != len(pZero[i].Mutations) {
			t.Fatalf("baseline plan mismatch at index %d: %+v vs %+v", i, pBase[i], pZero[i])
		}
	}
}

func TestWorkloadExecutionWithIngestAndRatchet(t *testing.T) {
	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                          rec,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		EventListener:               &listener,
		DisableAutomaticCompactions: true,
		FormatMajorVersion:          pebble.FormatMinSupported,
	}

	db, err := pebble.Open("db_ingest_ratchet", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	// 25 ops with IngestEvery: 5 yields 5 ingests:
	// op 1 (single-file), op 2 (multi-file), op 3 (single-file), op 4 (multi-file), op 5 (single-file)
	// RatchetAt: 12 tests live major version ratchet
	cfg := workload.Config{
		Seed:        42,
		Operations:  25,
		KeyCount:    16,
		ValueBytes:  64,
		SyncEvery:   5,
		FlushEvery:  10,
		IngestEvery: 5,
		RatchetAt:   12,
		FS:          rec,
	}

	entries, err := workload.Run(db, rec, cfg)
	if err != nil {
		_ = db.Close()
		t.Fatalf("workload.Run: %v", err)
	}

	// Dump contents before closing to verify oracle state matches
	dump, err := recovery.DumpContents(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("recovery.DumpContents: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}

	// 1. Verify history validation
	if err := history.Validate(entries); err != nil {
		t.Fatalf("history.Validate: %v", err)
	}

	// Verify ingest and ratchet entries exist in history
	var hasIngestSingle, hasIngestMulti, hasRatchet bool
	for _, e := range entries {
		if e.Kind == "ingest" {
			if len(e.Mutations) == 2 {
				hasIngestSingle = true
			} else if len(e.Mutations) == 4 {
				hasIngestMulti = true
			}
			if !e.Sync {
				t.Errorf("history ingest entry must be Sync: true")
			}
		} else if e.Kind == "ratchet" {
			hasRatchet = true
			if len(e.Mutations) != 0 {
				t.Errorf("history ratchet entry must have no mutations")
			}
		}
	}
	if !hasIngestSingle {
		t.Fatalf("expected single-file ingest entry in history")
	}
	if !hasIngestMulti {
		t.Fatalf("expected multi-file ingest entry in history")
	}
	if !hasRatchet {
		t.Fatalf("expected ratchet entry in history")
	}

	// 2. Verify trace validation
	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("trace.Validate: %v", err)
	}

	// 3. Verify history correlation
	correlated, err := workload.Correlate(entries, traceEvents)
	if err != nil {
		t.Fatalf("workload.Correlate: %v", err)
	}
	if err := history.Validate(correlated); err != nil {
		t.Fatalf("correlated history.Validate: %v", err)
	}

	// 4. Verify oracle check: all synchronous acknowledged mutations (including ingested keys) are valid
	cand := candidate.Candidate{
		Event:   uint64(len(traceEvents)),
		Percent: 100,
		Seed:    42,
	}
	outcome := oracle.Check(oracle.Context{
		Candidate: cand,
		Events:    traceEvents,
		History:   correlated,
		Recovered: dump,
	})
	if outcome.Class != oracle.ClassValid {
		t.Fatalf("oracle.Check expected ClassValid, got %s: rule=%s detail=%s",
			outcome.Class, outcome.Rule, outcome.Detail)
	}

	// 5. Verify staging SSTables were cleaned up by Pebble after ingestion
	stagingEntries, err := rec.List("ingest_staging")
	if err == nil {
		for _, s := range stagingEntries {
			if strings.HasSuffix(s, ".sst") {
				t.Errorf("staging file %s was not cleaned up after ingest", s)
			}
		}
	}
}

func TestWorkloadIngestMissingVFS(t *testing.T) {
	mem := vfs.NewCrashableMem()
	opts := &pebble.Options{
		FS:                          mem,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		DisableAutomaticCompactions: true,
	}
	db, err := pebble.Open("db_missing_vfs", opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	defer db.Close()

	cfg := workload.Config{
		Seed:        42,
		Operations:  5,
		KeyCount:    8,
		ValueBytes:  32,
		IngestEvery: 2,
	}

	marker := &testMarker{}
	_, err = workload.Run(db, marker, cfg)
	if err == nil {
		t.Fatalf("expected workload.Run to return error when ingest planned without VFS")
	}
	if !strings.Contains(err.Error(), "workload: ingest requires a valid VFS") {
		t.Fatalf("expected error containing 'workload: ingest requires a valid VFS', got: %v", err)
	}
}

func TestHistoryValidationIngestAndRatchet(t *testing.T) {
	// Valid ingest entry
	validIngest := []history.Entry{
		{
			Op:         1,
			Kind:       "ingest",
			Sync:       true,
			BeginEvent: 1,
			EndEvent:   2,
			Mutations: []history.Mutation{
				{Kind: "set", Key: []byte("k1"), Value: []byte("v1")},
				{Kind: "set", Key: []byte("k2"), Value: []byte("v2")},
			},
		},
	}
	if err := history.Validate(validIngest); err != nil {
		t.Errorf("expected valid ingest entry to pass history.Validate: %v", err)
	}

	// Valid ratchet entry
	validRatchet := []history.Entry{
		{
			Op:         1,
			Kind:       "ratchet",
			BeginEvent: 1,
			EndEvent:   2,
		},
	}
	if err := history.Validate(validRatchet); err != nil {
		t.Errorf("expected valid ratchet entry to pass history.Validate: %v", err)
	}

	// Ingest with no mutations (rejected)
	emptyMutsIngest := []history.Entry{
		{
			Op:         1,
			Kind:       "ingest",
			Sync:       true,
			BeginEvent: 1,
			EndEvent:   2,
		},
	}
	if err := history.Validate(emptyMutsIngest); err == nil {
		t.Errorf("expected history.Validate to reject ingest with empty mutations")
	}

	// Ingest with delete mutation (rejected)
	deleteMutsIngest := []history.Entry{
		{
			Op:         1,
			Kind:       "ingest",
			Sync:       true,
			BeginEvent: 1,
			EndEvent:   2,
			Mutations: []history.Mutation{
				{Kind: "delete", Key: []byte("k1")},
			},
		},
	}
	if err := history.Validate(deleteMutsIngest); err == nil {
		t.Errorf("expected history.Validate to reject ingest with delete mutation")
	}

	// Ingest with End key on set mutation (rejected)
	endMutsIngest := []history.Entry{
		{
			Op:         1,
			Kind:       "ingest",
			Sync:       true,
			BeginEvent: 1,
			EndEvent:   2,
			Mutations: []history.Mutation{
				{Kind: "set", Key: []byte("k1"), Value: []byte("v1"), End: []byte("k2")},
			},
		},
	}
	if err := history.Validate(endMutsIngest); err == nil {
		t.Errorf("expected history.Validate to reject ingest with End on mutation")
	}

	// Ratchet with mutations (rejected)
	mutsRatchet := []history.Entry{
		{
			Op:         1,
			Kind:       "ratchet",
			BeginEvent: 1,
			EndEvent:   2,
			Mutations: []history.Mutation{
				{Kind: "set", Key: []byte("k1"), Value: []byte("v1")},
			},
		},
	}
	if err := history.Validate(mutsRatchet); err == nil {
		t.Errorf("expected history.Validate to reject ratchet with mutations")
	}

	// Ratchet with Sync: true (rejected)
	syncRatchet := []history.Entry{
		{
			Op:         1,
			Kind:       "ratchet",
			Sync:       true,
			BeginEvent: 1,
			EndEvent:   2,
		},
	}
	if err := history.Validate(syncRatchet); err == nil {
		t.Errorf("expected history.Validate to reject ratchet with Sync: true")
	}
}
