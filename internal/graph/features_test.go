package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func requireValidTrace(t *testing.T, events []trace.Event) {
	t.Helper()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace.Validate failed: %v", err)
	}
}

// TestAckSyncMissing tests detection of unpersisted acknowledged WAL writes ("ack <- sync" missing edge).
func TestAckSyncMissing(t *testing.T) {
	// Case 1: Mutating write acknowledged with OpEnd, but WAL sync was dropped/omitted.
	unpersistedTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000002.log", Handle: 1, Off: 0, Len: 256, Data: make([]byte, 256)},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""}, // Acknowledged commit!
	}
	requireValidTrace(t, unpersistedTrace)

	missing, ops, bytes := graph.AckSyncMissing(unpersistedTrace, 4)
	if !missing {
		t.Fatalf("expected AckSyncMissing=true at cutoff 4, got false")
	}
	if ops != 1 {
		t.Fatalf("expected UnpersistedAckOps=1, got %d", ops)
	}
	if bytes != 256 {
		t.Fatalf("expected DanglingWALBytes=256, got %d", bytes)
	}

	cf := graph.ExtractContractFeatures(unpersistedTrace, 4)
	if cf.ContractViolationMask&graph.MaskAckSyncMissing == 0 {
		t.Fatalf("expected MaskAckSyncMissing to be set, got mask %b", cf.ContractViolationMask)
	}
	if cf.ContractScore <= 0 {
		t.Fatalf("expected positive ContractScore, got %f", cf.ContractScore)
	}

	// Case 2: Mutating write properly synced with both WAL sync and parent directory sync before OpEnd (normal durability).
	durableTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: "", Handle: 2, Dir: true}, // Parent directory synced!
		{ID: 4, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindWrite, File: "000002.log", Handle: 1, Off: 0, Len: 256, Data: make([]byte, 256)},
		{ID: 6, Kind: trace.KindSync, File: "000002.log", Handle: 1}, // WAL sync executed!
		{ID: 7, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},
	}
	requireValidTrace(t, durableTrace)

	missingClean, opsClean, bytesClean := graph.AckSyncMissing(durableTrace, 7)
	if missingClean {
		t.Fatalf("expected AckSyncMissing=false after sync, got true")
	}
	if opsClean != 0 || bytesClean != 0 {
		t.Fatalf("expected 0 unpersisted ops/bytes, got ops=%d, bytes=%d", opsClean, bytesClean)
	}
	cfClean := graph.ExtractContractFeatures(durableTrace, 7)
	if cfClean.ContractViolationMask&graph.MaskAckSyncMissing != 0 {
		t.Fatalf("expected MaskAckSyncMissing to be clear, got mask %b", cfClean.ContractViolationMask)
	}

	// Case 3: Mutating write has data synced, but directory sync omitted (MUT-3 / MUT-9 dir entry hazard).
	unsyncedDirTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 1},
		// Parent directory sync omitted!
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000002.log", Handle: 1, Off: 0, Len: 256, Data: make([]byte, 256)},
		{ID: 4, Kind: trace.KindSync, File: "000002.log", Handle: 1}, // WAL inode data synced, but dir entry unsynced
		{ID: 5, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},
	}
	requireValidTrace(t, unsyncedDirTrace)

	missingUnsynced, opsUnsynced, bytesUnsynced := graph.AckSyncMissing(unsyncedDirTrace, 5)
	if !missingUnsynced {
		t.Fatalf("expected AckSyncMissing=true when directory sync omitted, got false")
	}
	if opsUnsynced != 1 {
		t.Fatalf("expected UnpersistedAckOps=1, got %d", opsUnsynced)
	}
	if bytesUnsynced != 256 {
		t.Fatalf("expected DanglingWALBytes=256, got %d", bytesUnsynced)
	}
	cfUnsynced := graph.ExtractContractFeatures(unsyncedDirTrace, 5)
	if cfUnsynced.ContractViolationMask&graph.MaskAckSyncMissing == 0 {
		t.Fatalf("expected MaskAckSyncMissing to be set when directory sync omitted")
	}
}

// TestDanglingTableReferences tests detection of Manifest VersionEdits referencing unsynced SSTables.
func TestDanglingTableReferences(t *testing.T) {
	// Case 1: SSTable created and written, but not synced when Manifest is updated.
	danglingTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "000004.sst", Handle: 1, Off: 0, Len: 4096, Data: make([]byte, 4096)},
		// Missing Sync on 000004.sst (MUT-4)!
		{ID: 3, Kind: trace.KindCreate, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 2},
		{ID: 4, Kind: trace.KindWrite, File: "MANIFEST-000001", Handle: 2, Off: 0, Len: 512, Data: make([]byte, 512)},
		{ID: 5, Kind: trace.KindSync, File: "MANIFEST-000001", Handle: 2},
	}
	requireValidTrace(t, danglingTrace)

	count, bytes := graph.DanglingTableReferences(danglingTrace, 5)
	if count != 1 {
		t.Fatalf("expected DanglingTableCount=1, got %d", count)
	}
	if bytes != 4096 {
		t.Fatalf("expected DanglingTableBytes=4096, got %d", bytes)
	}

	cf := graph.ExtractContractFeatures(danglingTrace, 5)
	if cf.ContractViolationMask&graph.MaskDanglingTable == 0 {
		t.Fatalf("expected MaskDanglingTable to be set, got mask %b", cf.ContractViolationMask)
	}
	if cf.ContractScore < 40.0 {
		t.Fatalf("expected ContractScore >= 40.0, got %f", cf.ContractScore)
	}

	// Case 2: SSTable properly synced and parent directory synced before Manifest update.
	persistedTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "000004.sst", Handle: 1, Off: 0, Len: 4096, Data: make([]byte, 4096)},
		{ID: 3, Kind: trace.KindSync, File: "000004.sst", Handle: 1}, // Table data synced!
		{ID: 4, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 5, Kind: trace.KindSync, File: "", Handle: 2, Dir: true}, // Parent directory synced!
		{ID: 6, Kind: trace.KindCreate, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 3},
		{ID: 7, Kind: trace.KindWrite, File: "MANIFEST-000001", Handle: 3, Off: 0, Len: 512, Data: make([]byte, 512)},
		{ID: 8, Kind: trace.KindSync, File: "MANIFEST-000001", Handle: 3},
		{ID: 9, Kind: trace.KindSync, File: "", Handle: 2, Dir: true}, // Manifest parent directory synced!
	}
	requireValidTrace(t, persistedTrace)

	countClean, bytesClean := graph.DanglingTableReferences(persistedTrace, 9)
	if countClean != 0 || bytesClean != 0 {
		t.Fatalf("expected 0 dangling tables, got count=%d, bytes=%d", countClean, bytesClean)
	}

	// Case 3: SSTable data synced, but parent directory sync omitted (MUT-3 / MUT-9 SST directory hazard).
	unsyncedDirSSTTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "000004.sst", Handle: 1, Off: 0, Len: 4096, Data: make([]byte, 4096)},
		{ID: 3, Kind: trace.KindSync, File: "000004.sst", Handle: 1}, // Table data synced!
		// Parent directory sync omitted (MUT-3 / MUT-9)!
		{ID: 4, Kind: trace.KindCreate, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 2},
		{ID: 5, Kind: trace.KindWrite, File: "MANIFEST-000001", Handle: 2, Off: 0, Len: 512, Data: make([]byte, 512)},
		{ID: 6, Kind: trace.KindSync, File: "MANIFEST-000001", Handle: 2},
	}
	requireValidTrace(t, unsyncedDirSSTTrace)

	countUnsynced, bytesUnsynced := graph.DanglingTableReferences(unsyncedDirSSTTrace, 6)
	if countUnsynced != 1 {
		t.Fatalf("expected DanglingTableCount=1 when SST directory sync omitted, got %d", countUnsynced)
	}
	if bytesUnsynced != 4096 {
		t.Fatalf("expected DanglingTableBytes=4096, got %d", bytesUnsynced)
	}
	cfUnsyncedSST := graph.ExtractContractFeatures(unsyncedDirSSTTrace, 6)
	if cfUnsyncedSST.ContractViolationMask&graph.MaskDanglingTable == 0 {
		t.Fatalf("expected MaskDanglingTable to be set when SST directory sync omitted")
	}
	if cfUnsyncedSST.ContractScore < 40.0 {
		t.Fatalf("expected ContractScore >= 40.0, got %f", cfUnsyncedSST.ContractScore)
	}
}

// TestUnsyncedDirectoryRename tests detection of atomic Rename lacking parent directory sync.
func TestUnsyncedDirectoryRename(t *testing.T) {
	// Case 1: Rename CURRENT.tmp -> CURRENT without parent directory sync (RocksDB #10573 / MUT-3).
	unsyncedTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "CURRENT.tmp", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "CURRENT.tmp", Handle: 1, Off: 0, Len: 16, Data: make([]byte, 16)},
		{ID: 3, Kind: trace.KindSync, File: "CURRENT.tmp", Handle: 1},
		{ID: 4, Kind: trace.KindClose, File: "CURRENT.tmp", Handle: 1},
		{ID: 5, Kind: trace.KindRename, File: "CURRENT.tmp", File2: "CURRENT"}, // Atomic rename
		{ID: 6, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},                 // Enclosing step crossed without dir sync!
	}
	requireValidTrace(t, unsyncedTrace)

	// At cut 5 (immediately at Rename before lifecycle boundary), unsynced is false to avoid false positives during normal initialization
	unsyncedPre, _, _ := graph.UnsyncedDirectoryRename(unsyncedTrace, 5)
	if unsyncedPre {
		t.Fatalf("expected UnsyncedDirectoryRename=false immediately at cut 5, got true")
	}

	// At cut 6 (after lifecycle boundary crossed), unsynced is true
	unsynced, pending, crit := graph.UnsyncedDirectoryRename(unsyncedTrace, 6)
	if !unsynced {
		t.Fatalf("expected UnsyncedDirectoryRename=true at cut 6, got false")
	}
	if pending != 1 {
		t.Fatalf("expected PendingRenames=1, got %d", pending)
	}
	if crit != 1 {
		t.Fatalf("expected CriticalPendingRenames=1, got %d", crit)
	}

	cf := graph.ExtractContractFeatures(unsyncedTrace, 6)
	if cf.ContractViolationMask&graph.MaskUnsyncedRename == 0 {
		t.Fatalf("expected MaskUnsyncedRename to be set, got mask %b", cf.ContractViolationMask)
	}
	if cf.ContractScore < 50.0 {
		t.Fatalf("expected ContractScore >= 50.0 for critical CURRENT rename, got %f", cf.ContractScore)
	}

	// Case 2: Parent directory properly synced after atomic rename.
	syncedTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "CURRENT.tmp", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "CURRENT.tmp", Handle: 1, Off: 0, Len: 16, Data: make([]byte, 16)},
		{ID: 3, Kind: trace.KindSync, File: "CURRENT.tmp", Handle: 1},
		{ID: 4, Kind: trace.KindClose, File: "CURRENT.tmp", Handle: 1},
		{ID: 5, Kind: trace.KindRename, File: "CURRENT.tmp", File2: "CURRENT"},
		{ID: 6, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 7, Kind: trace.KindSync, File: "", Handle: 2, Dir: true}, // Directory sync executed!
	}
	requireValidTrace(t, syncedTrace)

	unsyncedClean, pendingClean, critClean := graph.UnsyncedDirectoryRename(syncedTrace, 7)
	if unsyncedClean {
		t.Fatalf("expected UnsyncedDirectoryRename=false after dir sync, got true")
	}
	if pendingClean != 0 || critClean != 0 {
		t.Fatalf("expected 0 pending renames, got pending=%d, crit=%d", pendingClean, critClean)
	}
}

// TestViewContractMethods tests contract methods integrated on graph.View.
func TestViewContractMethods(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000001.log", Handle: 1, Data: []byte("unpersisted wal record"), Len: 22},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},
	}
	requireValidTrace(t, events)

	view, err := graph.Build(events)
	if err != nil {
		t.Fatalf("graph.Build failed: %v", err)
	}

	score := view.ContractScore(4)
	if score <= 0 {
		t.Fatalf("expected positive ContractScore from view at cut 4, got %f", score)
	}

	cf := view.ContractFeatures(4)
	if !cf.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=true in view.ContractFeatures(4)")
	}

	all := view.AllContractFeatures()
	if len(all) != len(events)+1 {
		t.Fatalf("expected all contract features map length %d, got %d", len(events)+1, len(all))
	}
	if all[4].ContractScore != score {
		t.Fatalf("expected matching score between AllContractFeatures and ContractScore")
	}

	// Mutating the returned map must NOT mutate internal cached features.
	delete(all, 4)
	if view.ContractScore(4) != score {
		t.Fatalf("mutating AllContractFeatures map aliased internal view cache")
	}

	// Non-nil empty View must return {0: {}} consistent with ComputeContractFeatures(nil).
	emptyView, err := graph.Build(nil)
	if err != nil {
		t.Fatalf("graph.Build(nil): %v", err)
	}
	emptyAll := emptyView.AllContractFeatures()
	if len(emptyAll) != 1 {
		t.Fatalf("emptyView.AllContractFeatures length=%d, want 1", len(emptyAll))
	}
}

// TestContractScore_ZeroFalsePositives_MUT0 verifies that on healthy unfaulted traces (smoke_mut0 & main_mut0),
// ContractScore is exactly 0.0 across 100% of cutoffs with zero false alarms.
func TestContractScore_ZeroFalsePositives_MUT0(t *testing.T) {
	tracePaths := []string{
		filepath.Join("..", "..", "results", "smoke_mut0", "trace.json"),
		filepath.Join("..", "..", "results", "main_mut0", "trace.json"),
	}

	type traceDoc struct {
		Events []trace.Event `json:"events"`
	}

	for _, p := range tracePaths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("trace file %s not found: %v", p, err)
		}
		var doc traceDoc
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("decode %s: %v", p, err)
		}
		if len(doc.Events) == 0 {
			t.Fatalf("empty events in %s", p)
		}
		requireValidTrace(t, doc.Events)

		features := graph.ComputeContractFeatures(doc.Events)
		var falsePositives []uint64
		for cut := uint64(0); cut <= uint64(len(doc.Events)); cut++ {
			cf := features[cut]
			if cf.ContractScore > 0 {
				falsePositives = append(falsePositives, cut)
			}
		}

		if len(falsePositives) > 0 {
			t.Errorf("Path %s: expected 0 contract violations on healthy MUT-0 trace, found %d false positives at cutoffs: %v",
				p, len(falsePositives), falsePositives)
		}
	}
}

// TestContractScore_ClosedWhileDirtySST verifies that an SST closed while dirty only becomes
// a committed dangling table violation (DanglingTableCount > 0) once MANIFEST write is synced.
func TestContractScore_ClosedWhileDirtySST(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/000004.sst", Cat: "pebble-memtable-flush", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "db/000004.sst", Handle: 1, Len: 1024, Data: make([]byte, 1024)},
		// SyncData omitted (MUT-4 / MUT-8)
		{ID: 3, Kind: trace.KindClose, File: "db/000004.sst", Handle: 1},
		{ID: 4, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 2},
		{ID: 5, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 2, Len: 256, Data: make([]byte, 256)},
		{ID: 6, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 2},
	}
	requireValidTrace(t, events)

	// At cut 3 (immediately after Close, before MANIFEST sync), DanglingTableCount is 0 to avoid premature activation.
	cfPre := graph.ExtractContractFeatures(events, 3)
	if cfPre.DanglingTableCount != 0 {
		t.Fatalf("expected DanglingTableCount=0 before MANIFEST sync at cut 3, got %d", cfPre.DanglingTableCount)
	}

	// At cut 6 (after MANIFEST Sync commits the reference), DanglingTableCount is 1.
	cfPost := graph.ExtractContractFeatures(events, 6)
	if cfPost.DanglingTableCount != 1 {
		t.Fatalf("expected DanglingTableCount=1 after MANIFEST Sync at cut 6, got %d", cfPost.DanglingTableCount)
	}
	if cfPost.ContractViolationMask&graph.MaskDanglingTable == 0 {
		t.Fatalf("expected MaskDanglingTable to be set on committed dirty SST")
	}
	if cfPost.ContractScore <= 0 {
		t.Fatalf("expected positive ContractScore on committed dirty SST")
	}
}

// TestContractScore_UnpersistedManifest verifies that an uncommitted MANIFEST write left dirty across
// lifecycle boundaries (FlushEnd, CompactionEnd, OpBegin) is immediately flagged as a contract violation.
func TestContractScore_UnpersistedManifest(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 1, Len: 64, Data: make([]byte, 64)},
		// Sync omitted (MUT-2 / MUT-6)
		{ID: 3, Kind: trace.KindEngineEvent, Note: "FlushEnd"},
	}
	requireValidTrace(t, events)

	cf := graph.ExtractContractFeatures(events, 3)
	if cf.DanglingTableCount != 1 {
		t.Fatalf("expected DanglingTableCount=1 after FlushEnd with uncommitted manifest, got %d", cf.DanglingTableCount)
	}
	if cf.ContractScore <= 0 {
		t.Fatalf("expected positive ContractScore on uncommitted manifest, got %f", cf.ContractScore)
	}
}

// TestContractScore_ReuseForWrite verifies that ReuseForWrite correctly updates handle tracking,
// category classification, and dirty write persistence on recycled files.
func TestContractScore_ReuseForWrite(t *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindSync, File: "db/000002.log", Handle: 1},
		{ID: 3, Kind: trace.KindClose, File: "db/000002.log", Handle: 1},
		{ID: 4, Kind: trace.KindReuseForWrite, File: "db/000002.log", File2: "db/000006.log", Cat: "pebble-wal", Handle: 2},
		{ID: 5, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 6, Kind: trace.KindWrite, File: "db/000006.log", Handle: 2, Len: 128, Data: make([]byte, 128)},
		{ID: 7, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""}, // WAL sync omitted
	}
	requireValidTrace(t, events)

	cf := graph.ExtractContractFeatures(events, 7)
	if !cf.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=true on reused WAL with dropped sync, got false")
	}
	if cf.UnpersistedAckOps != 1 {
		t.Fatalf("expected UnpersistedAckOps=1, got %d", cf.UnpersistedAckOps)
	}
	if cf.ContractScore <= 0 {
		t.Fatalf("expected positive ContractScore on reused WAL with dropped sync")
	}
}

// TestContractScore_PinpointsVulnerabilities_MUT1ThroughMUT8 verifies that on MUT-1 through MUT-8
// (and across varying WALDropK values on MUT-5), ContractScore > 0 accurately pinpoints vulnerable cutoffs,
// whereas on MUT-0, ContractScore == 0 across 100% of cutoffs.
func TestContractScore_PinpointsVulnerabilities_MUT1ThroughMUT8(t *testing.T) {
	runTrace := func(faultID string, walDropK int) []trace.Event {
		mem := vfs.NewCrashableMem()
		rec := vfsrec.New(mem)
		fCfg := faults.DefaultConfig(faultID)
		if walDropK > 0 {
			fCfg.WALDropK = walDropK
		}
		faultFS, err := faults.NewWithConfig(fCfg, rec)
		if err != nil {
			t.Fatalf("faults.NewWithConfig(%s): %v", faultID, err)
		}
		listener := rec.EventListener()
		opts := &pebble.Options{
			FS:                          faultFS,
			Logger:                      recovery.PanicLogger{},
			MemTableSize:                64 * 1024,
			EventListener:               &listener,
			DisableAutomaticCompactions: faultID != faults.MUT8,
			DisableTableStats:           true,
		}
		db, err := pebble.Open("db", opts)
		if err != nil {
			t.Fatalf("pebble.Open(%s): %v", faultID, err)
		}
		wCfg := workload.Config{
			Seed:       42,
			Operations: 60,
			KeyCount:   8,
			ValueBytes: 32,
			SyncEvery:  3,
			FlushEvery: 15,
		}
		_, err = workload.Run(db, rec, wCfg)
		if err != nil {
			_ = db.Close()
			t.Fatalf("workload.Run(%s): %v", faultID, err)
		}
		if faultID == faults.MUT8 {
			_ = db.Compact(context.Background(), nil, []byte("\xff"), false)
		}
		_ = db.Close()
		return rec.Events()
	}

	// 1. MUT-0 Control check
	events0 := runTrace(faults.MUT0, 0)
	cf0 := graph.ComputeContractFeatures(events0)
	for cut := uint64(0); cut <= uint64(len(events0)); cut++ {
		if cf0[cut].ContractScore > 0 {
			t.Fatalf("MUT-0 false positive at cutoff %d: score %f", cut, cf0[cut].ContractScore)
		}
	}

	// 2. MUT-1 through MUT-8 checks
	faultList := []struct {
		id       string
		walDropK int
		wantMask uint8
	}{
		{id: faults.MUT1, wantMask: graph.MaskAckSyncMissing},
		{id: faults.MUT2, wantMask: graph.MaskDanglingTable},
		{id: faults.MUT3, wantMask: graph.MaskAckSyncMissing | graph.MaskDanglingTable | graph.MaskUnsyncedRename},
		{id: faults.MUT4, wantMask: graph.MaskDanglingTable},
		{id: faults.MUT5, walDropK: 1, wantMask: graph.MaskAckSyncMissing},
		{id: faults.MUT5, walDropK: 3, wantMask: graph.MaskAckSyncMissing},
		{id: faults.MUT6, wantMask: graph.MaskDanglingTable},
		{id: faults.MUT7, wantMask: graph.MaskUnsyncedRename},
		{id: faults.MUT8, wantMask: graph.MaskDanglingTable},
	}

	for _, f := range faultList {
		label := f.id
		if f.walDropK > 0 {
			label = fmt.Sprintf("%s(k=%d)", f.id, f.walDropK)
		}
		t.Run(label, func(t *testing.T) {
			evs := runTrace(f.id, f.walDropK)
			cfs := graph.ComputeContractFeatures(evs)
			violationCount := 0
			var unionMask uint8
			for cut := uint64(0); cut <= uint64(len(evs)); cut++ {
				if cfs[cut].ContractScore > 0 {
					violationCount++
					unionMask |= cfs[cut].ContractViolationMask
				}
			}
			if violationCount == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", label)
			}
			if unionMask != f.wantMask {
				t.Fatalf("%s: expected ContractViolationMask union %#x, got %#x", label, f.wantMask, unionMask)
			}
		})
	}
}

func TestContractFeatures_FlushHealsClosedDirtyWAL(t *testing.T) {
	// Phase 1: WAL written + acknowledged, no sync, then closed → closedWhileDirty.
	events := []trace.Event{
		// Pre-existing MANIFEST from DB initialization.
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		// WAL creation and acknowledged write (sync dropped / absent).
		{ID: 2, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 2},
		{ID: 3, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 4, Kind: trace.KindWrite, File: "db/000002.log", Handle: 2, Off: 0, Len: 256, Data: make([]byte, 256)},
		{ID: 5, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},
		// WAL closed without syncing its dirty spans.
		{ID: 6, Kind: trace.KindClose, File: "db/000002.log", Handle: 2},

		// Phase 2: full flush sequence that makes the WAL content durable.
		{ID: 7, Kind: trace.KindEngineEvent, Note: "FlushBegin"},
		// SST created, written, synced (SyncData), and closed.
		{ID: 8, Kind: trace.KindCreate, File: "db/000004.sst", Cat: "pebble-memtable-flush", Handle: 3},
		{ID: 9, Kind: trace.KindWrite, File: "db/000004.sst", Handle: 3, Off: 0, Len: 4096, Data: make([]byte, 4096)},
		{ID: 10, Kind: trace.KindSyncData, File: "db/000004.sst", Handle: 3},
		{ID: 11, Kind: trace.KindClose, File: "db/000004.sst", Handle: 3},
		// Parent directory synced (makes SST and MANIFEST dir entries durable).
		{ID: 12, Kind: trace.KindOpenDir, File: "db", Handle: 4, Dir: true},
		{ID: 13, Kind: trace.KindSync, File: "db", Handle: 4, Dir: true},
		// MANIFEST written and synced (references the flush SST).
		{ID: 14, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 1, Off: 0, Len: 512, Data: make([]byte, 512)},
		{ID: 15, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 1},
		// FlushEnd marks completion of the flush lifecycle.
		{ID: 16, Kind: trace.KindEngineEvent, Note: "FlushEnd"},
	}
	requireValidTrace(t, events)

	// For all cutoffs prior to flush completion (cutoff 6 after WAL close through
	// cutoff 14 after MANIFEST write), AckSyncMissing must be true and
	// HasClosedDirtyLog must be true.
	for cut := uint64(6); cut <= 14; cut++ {
		cf := graph.ExtractContractFeatures(events, cut)
		if !cf.AckSyncMissing {
			t.Errorf("cutoff %d: expected AckSyncMissing=true prior to flush completion, got false", cut)
		}
		if !cf.HasClosedDirtyLog {
			t.Errorf("cutoff %d: expected HasClosedDirtyLog=true prior to flush completion, got false", cut)
		}
	}

	// Once the complete flush sequence finishes (cutoffs 15 and 16), the closed WAL
	// must be healed: AckSyncMissing == false, DirtyLogBytes == 0, and HasClosedDirtyLog == false.
	for _, cut := range []uint64{15, 16} {
		cf := graph.ExtractContractFeatures(events, cut)
		if cf.AckSyncMissing {
			t.Errorf("cutoff %d: expected AckSyncMissing=false after durable flush heals closed WAL, got true", cut)
		}
		if cf.DirtyLogBytes != 0 {
			t.Errorf("cutoff %d: expected DirtyLogBytes=0 after flush healing, got %d", cut, cf.DirtyLogBytes)
		}
		if cf.HasClosedDirtyLog {
			t.Errorf("cutoff %d: expected HasClosedDirtyLog=false after flush healing, got true", cut)
		}
	}
}

// TestContractScore_ScopedDirectorySyncIsolation verifies that syncing directory A
// isolates its durability from pending files and renames in directory B.
func TestContractScore_ScopedDirectorySyncIsolation(t *testing.T) {
	events := []trace.Event{
		// 1. Setup handles and files in dirA and dirB
		{ID: 1, Kind: trace.KindOpenDir, File: "dirA", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindOpenDir, File: "dirB", Handle: 2, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "dirA/000001.log", Cat: "pebble-wal", Handle: 3},
		{ID: 4, Kind: trace.KindCreate, File: "dirB/000002.log", Cat: "pebble-wal", Handle: 4},
		{ID: 5, Kind: trace.KindCreate, File: "dirA/CURRENT.tmp", Handle: 5},
		{ID: 6, Kind: trace.KindClose, File: "dirA/CURRENT.tmp", Handle: 5},
		{ID: 7, Kind: trace.KindCreate, File: "dirB/OPTIONS.tmp", Handle: 6},
		{ID: 8, Kind: trace.KindClose, File: "dirB/OPTIONS.tmp", Handle: 6},

		// 2. Sync only dirA
		{ID: 9, Kind: trace.KindSync, File: "dirA", Handle: 1, Dir: true},

		// 3. Mutating Op 1 writes and syncs WAL in dirA
		{ID: 10, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 11, Kind: trace.KindWrite, File: "dirA/000001.log", Handle: 3, Len: 128, Data: make([]byte, 128)},
		{ID: 12, Kind: trace.KindSync, File: "dirA/000001.log", Handle: 3},
		{ID: 13, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},

		// 4. Mutating Op 2 writes and syncs WAL in dirB (where dirB directory entry was NOT synced)
		{ID: 14, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 15, Kind: trace.KindWrite, File: "dirB/000002.log", Handle: 4, Len: 256, Data: make([]byte, 256)},
		{ID: 16, Kind: trace.KindSync, File: "dirB/000002.log", Handle: 4},
		{ID: 17, Kind: trace.KindOpEnd, Op: 2, OpKind: "set", Err: ""},

		// 5. Atomic renames in both directories
		{ID: 18, Kind: trace.KindRename, File: "dirA/CURRENT.tmp", File2: "dirA/CURRENT"},
		{ID: 19, Kind: trace.KindRename, File: "dirB/OPTIONS.tmp", File2: "dirB/OPTIONS"},

		// 6. Sync dirA again
		{ID: 20, Kind: trace.KindSync, File: "dirA", Handle: 1, Dir: true},
		// Crossing lifecycle boundary
		{ID: 21, Kind: trace.KindOpBegin, Op: 3, OpKind: "get"},
	}
	requireValidTrace(t, events)

	cfCut13 := graph.ExtractContractFeatures(events, 13)
	if cfCut13.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=false at cut 13 for dirA after dirA sync, got true")
	}

	cfCut17 := graph.ExtractContractFeatures(events, 17)
	if !cfCut17.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=true at cut 17 for dirB because dirB was never synced")
	}
	if cfCut17.UnpersistedAckOps != 1 {
		t.Fatalf("expected UnpersistedAckOps=1 at cut 17, got %d", cfCut17.UnpersistedAckOps)
	}
	if cfCut17.DanglingWALBytes != 256 {
		t.Fatalf("expected DanglingWALBytes=256 at cut 17, got %d", cfCut17.DanglingWALBytes)
	}

	cfCut21 := graph.ExtractContractFeatures(events, 21)
	// dirA rename was cleared by cut 20, but dirB rename remains violating!
	if !cfCut21.UnsyncedDirectoryRename {
		t.Fatalf("expected UnsyncedDirectoryRename=true at cut 21 for unsynced dirB rename")
	}
	if cfCut21.CriticalPendingRenames != 1 {
		t.Fatalf("expected CriticalPendingRenames=1 at cut 21, got %d", cfCut21.CriticalPendingRenames)
	}
}

// TestContractFeatures_InitialUnsyncedManifestAfterDirSync verifies that when
// db/MANIFEST-000001 is created, written (Len: 64) with its file Sync omitted
// (uint64(size) == dirtyBytes), and the parent directory "db" is synced
// (KindSync, Dir: true) prior to OpBegin, DanglingTableCount == 1 and
// ContractScore > 0 immediately at the directory sync cutoff (ID=4). This
// captures the MUT-2 scenario where the initial MANIFEST sync at ID=14 during
// pebble.Open is dropped, resulting in a 0-byte MANIFEST on disk once the
// directory entry is flushed.
func TestContractFeatures_InitialUnsyncedManifestAfterDirSync(t *testing.T) {
	events := []trace.Event{
		// MANIFEST-000001 created and written without a file Sync.
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 1, Off: 0, Len: 64, Data: make([]byte, 64)},
		// File Sync omitted: uint64(f.size) == f.dirtyBytes == 64.
		// Parent directory "db" is synced, which exposes a 0-byte MANIFEST to any subsequent Open.
		{ID: 3, Kind: trace.KindOpenDir, File: "db", Handle: 2, Dir: true},
		{ID: 4, Kind: trace.KindSync, File: "db", Handle: 2, Dir: true},
		// OpBegin has not yet occurred; cutoff is at ID=4.
	}
	requireValidTrace(t, events)

	// At the directory-sync cutoff (ID=4), the MANIFEST directory entry is now
	// durable (dirEntrySynced == true) but the file itself has zero synced bytes
	// on disk. Any subsequent Open would encounter an EOF and fail. The contract
	// oracle must flag this immediately.
	cf := graph.ExtractContractFeatures(events, 4)
	if cf.DanglingTableCount != 1 {
		t.Fatalf("expected DanglingTableCount=1 at directory-sync cutoff ID=4 (MUT-2 initial MANIFEST), got %d",
			cf.DanglingTableCount)
	}
	if cf.ContractScore <= 0 {
		t.Fatalf("expected ContractScore > 0 at directory-sync cutoff ID=4, got %f", cf.ContractScore)
	}
}

// TestContractFeatures_PostOpenManifestAppendRequiresRetirementOrDirtyWAL verifies
// the MUT-6 scenario: when MANIFEST-000001 is created, written (Len: 64), and
// synced (ID=1..3) during pebble.Open, and a subsequent flush appends to
// MANIFEST-000001 (Off: 64, Len: 64) without syncing MANIFEST while the prior
// WAL was cleanly synced before closing (closedWhileDirty == false), the oracle
// must not flag a violation at FlushEnd alone (DanglingTableCount == 0), because
// reverting MANIFEST to its synced prefix is still safe. Only once KindReuseForWrite
// or KindRemove retires the prior WAL while MANIFEST remains dirty must the oracle
// set DanglingTableCount == 1.
func TestContractFeatures_PostOpenManifestAppendRequiresRetirementOrDirtyWAL(t *testing.T) {
	// Base trace: MANIFEST synced on Open, WAL cleanly synced before close,
	// subsequent flush appends to MANIFEST without a MANIFEST Sync.
	baseEvents := []trace.Event{
		// pebble.Open sequence: MANIFEST created, written, synced.
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 1, Off: 0, Len: 64, Data: make([]byte, 64)},
		{ID: 3, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 1},
		// WAL opened, written, synced, and cleanly closed (closedWhileDirty == false).
		{ID: 4, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 2},
		{ID: 5, Kind: trace.KindWrite, File: "db/000002.log", Handle: 2, Off: 0, Len: 128, Data: make([]byte, 128)},
		{ID: 6, Kind: trace.KindSync, File: "db/000002.log", Handle: 2},
		{ID: 7, Kind: trace.KindClose, File: "db/000002.log", Handle: 2},
		// Flush appends to MANIFEST without syncing: uint64(f.size) > f.dirtyBytes.
		{ID: 8, Kind: trace.KindEngineEvent, Note: "FlushBegin"},
		{ID: 9, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 1, Off: 64, Len: 64, Data: make([]byte, 64)},
		// MANIFEST Sync omitted (MUT-6).
		{ID: 10, Kind: trace.KindEngineEvent, Note: "FlushEnd"},
	}
	requireValidTrace(t, baseEvents)

	// At FlushEnd (ID=10), with no retirement and no closed dirty WAL,
	// the oracle must not flag a violation: revert to synced prefix is safe.
	cfFlushEnd := graph.ExtractContractFeatures(baseEvents, 10)
	if cfFlushEnd.DanglingTableCount != 0 {
		t.Fatalf("expected DanglingTableCount=0 at FlushEnd (ID=10) when WAL was cleanly synced and no retirement occurred, got %d",
			cfFlushEnd.DanglingTableCount)
	}

	// Sub-test A: KindReuseForWrite retires the prior WAL while MANIFEST is dirty.
	eventsWithReuse := append(append([]trace.Event{}, baseEvents...), trace.Event{
		ID: 11, Kind: trace.KindReuseForWrite, File: "db/000002.log", File2: "db/000006.log", Cat: "pebble-wal", Handle: 3,
	})
	requireValidTrace(t, eventsWithReuse)
	cfReuse := graph.ExtractContractFeatures(eventsWithReuse, 11)
	if cfReuse.DanglingTableCount != 1 {
		t.Fatalf("sub-test A: expected DanglingTableCount=1 at ID=11 after KindReuseForWrite retires WAL with dirty MANIFEST, got %d",
			cfReuse.DanglingTableCount)
	}

	// Sub-test B: KindRemove retires the prior WAL while MANIFEST is dirty.
	eventsWithRemove := append(append([]trace.Event{}, baseEvents...), trace.Event{
		ID: 11, Kind: trace.KindRemove, File: "db/000002.log",
	})
	requireValidTrace(t, eventsWithRemove)
	cfRemove := graph.ExtractContractFeatures(eventsWithRemove, 11)
	if cfRemove.DanglingTableCount != 1 {
		t.Fatalf("sub-test B: expected DanglingTableCount=1 at ID=11 after KindRemove retires WAL with dirty MANIFEST, got %d",
			cfRemove.DanglingTableCount)
	}
}

func TestUnsyncedDirEntryCount(t *testing.T) {
	// Build a trace where two files are created in the same directory before dir sync.
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 1},
		{ID: 2, Kind: trace.KindCreate, File: "db/marker.manifest.000002", Handle: 2},
		{ID: 3, Kind: trace.KindOpenDir, File: "db", Dir: true, Handle: 3},
		{ID: 4, Kind: trace.KindSync, File: "db", Dir: true, Handle: 3},
	}
	requireValidTrace(t, events)

	cuts := graph.ComputeContractFeatures(events)
	// After event 2 (two files created, no dir sync yet), UnsyncedDirEntryCount must be >= 2.
	cf2 := cuts[2]
	if cf2.UnsyncedDirEntryCount < 2 {
		t.Errorf("UnsyncedDirEntryCount at cut 2 = %d, want >= 2", cf2.UnsyncedDirEntryCount)
	}
	// After event 4 (dir sync), UnsyncedDirEntryCount should drop to 0.
	cf4 := cuts[4]
	if cf4.UnsyncedDirEntryCount != 0 {
		t.Errorf("UnsyncedDirEntryCount at cut 4 = %d, want 0", cf4.UnsyncedDirEntryCount)
	}
}

func TestContractFeaturesCacheCollisionDifferentTraces(t *testing.T) {
	// Two traces with identical event structures and path lengths, but different
	// write categories (WAL vs SST) and filenames ("000002.log" vs "000002.sst").
	traceWAL := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000002.log", Handle: 1, Off: 0, Len: 256, Data: make([]byte, 256)},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}
	traceSST := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000002.sst", Cat: "pebble-table", Handle: 1},
		{ID: 2, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000002.sst", Handle: 1, Off: 0, Len: 256, Data: make([]byte, 256)},
		{ID: 4, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}
	requireValidTrace(t, traceWAL)
	requireValidTrace(t, traceSST)

	cfWAL := graph.ComputeContractFeatures(traceWAL)[4]
	cfSST := graph.ComputeContractFeatures(traceSST)[4]

	// traceWAL must have AckSyncMissing because WAL write was not persisted.
	if !cfWAL.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=true for unpersisted WAL trace")
	}
	// traceSST is a table file write, NOT a WAL write, so it should not flag AckSyncMissing.
	if cfSST.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=false for SST table trace, but got cached WAL features")
	}
}

func TestOverwrittenWALAndLinkAttributes(t *testing.T) {
	// 1. Overwritten WAL via KindCreate must clear displaced entry from active opWrites.
	overwritten := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 2},
		{ID: 3, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 4, Kind: trace.KindWrite, File: "db/000002.log", Handle: 2, Len: 64, Data: make([]byte, 64)},
		{ID: 5, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 6, Kind: trace.KindClose, File: "db/000002.log", Handle: 2},
		{ID: 7, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 3},
		{ID: 8, Kind: trace.KindSync, File: "db/000002.log", Handle: 3},
		{ID: 9, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
	}
	requireValidTrace(t, overwritten)
	cfOver := graph.ExtractContractFeatures(overwritten, 9)
	if cfOver.AckSyncMissing {
		t.Fatalf("displaced WAL leaked AckSyncMissing after overwrite and replacement sync")
	}

	// 2. KindLink inherits SST inode classification, size, and dirty spans.
	linkedSST := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindCreate, File: "db/tmp.sst", Cat: "pebble-memtable-flush", Handle: 2},
		{ID: 3, Kind: trace.KindWrite, File: "db/tmp.sst", Handle: 2, Len: 4096, Data: make([]byte, 4096)},
		{ID: 4, Kind: trace.KindClose, File: "db/tmp.sst", Handle: 2},
		{ID: 5, Kind: trace.KindLink, File: "db/tmp.sst", File2: "db/000004.sst"},
		{ID: 6, Kind: trace.KindRemove, File: "db/tmp.sst"},
		{ID: 7, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 3},
		{ID: 8, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 3, Len: 64, Data: make([]byte, 64)},
		{ID: 9, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 3},
	}
	requireValidTrace(t, linkedSST)
	cfLinked := graph.ExtractContractFeatures(linkedSST, 9)
	if cfLinked.DanglingTableCount != 1 || cfLinked.DanglingTableBytes != 4096 {
		t.Fatalf("KindLink lost SST attributes/size: count=%d bytes=%d, want 1/4096",
			cfLinked.DanglingTableCount, cfLinked.DanglingTableBytes)
	}
}

func TestRenameDirChildrenUnsyncedDirsAndCrossDir(t *testing.T) {
	// 1. Directory rename updates child WAL path and avoids double-counting unsyncedDirs.
	dirRename := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "a", Mode: 0755},
		{ID: 2, Kind: trace.KindCreate, File: "a/000001.log", Cat: "pebble-wal", Handle: 1},
		{ID: 3, Kind: trace.KindRename, File: "a", File2: "b"},
		{ID: 4, Kind: trace.KindOpenDir, File: "", Handle: 2, Dir: true},
		{ID: 5, Kind: trace.KindSync, File: "", Handle: 2, Dir: true},
		{ID: 6, Kind: trace.KindOpenDir, File: "b", Handle: 3, Dir: true},
		{ID: 7, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 8, Kind: trace.KindWrite, File: "a/000001.log", Handle: 1, Len: 64, Data: make([]byte, 64)},
		{ID: 9, Kind: trace.KindSyncData, File: "a/000001.log", Handle: 1},
		{ID: 10, Kind: trace.KindSync, File: "b", Handle: 3, Dir: true},
		{ID: 11, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}
	requireValidTrace(t, dirRename)
	cuts := graph.ComputeContractFeatures(dirRename)
	if cuts[3].UnsyncedDirEntryCount != 1 {
		t.Fatalf("cut 3 UnsyncedDirEntryCount=%d, want 1 (no double-count of a and b)", cuts[3].UnsyncedDirEntryCount)
	}
	if cuts[11].AckSyncMissing {
		t.Fatalf("Sync(\"b\") failed to clear dirEntrySynced on renamed child b/000001.log")
	}

	// 2. Cross-directory rename requires syncing BOTH source and destination parents.
	crossDir := []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "dirA", Mode: 0755},
		{ID: 2, Kind: trace.KindMkdirAll, File: "dirB", Mode: 0755},
		{ID: 3, Kind: trace.KindCreate, File: "dirA/CURRENT.tmp", Handle: 1},
		{ID: 4, Kind: trace.KindWrite, File: "dirA/CURRENT.tmp", Handle: 1, Len: 16, Data: make([]byte, 16)},
		{ID: 5, Kind: trace.KindSync, File: "dirA/CURRENT.tmp", Handle: 1},
		{ID: 6, Kind: trace.KindClose, File: "dirA/CURRENT.tmp", Handle: 1},
		{ID: 7, Kind: trace.KindRename, File: "dirA/CURRENT.tmp", File2: "dirB/CURRENT"},
		{ID: 8, Kind: trace.KindOpenDir, File: "dirB", Handle: 2, Dir: true},
		{ID: 9, Kind: trace.KindSync, File: "dirB", Handle: 2, Dir: true},
		{ID: 10, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 11, Kind: trace.KindOpenDir, File: "dirA", Handle: 3, Dir: true},
		{ID: 12, Kind: trace.KindSync, File: "dirA", Handle: 3, Dir: true},
	}
	requireValidTrace(t, crossDir)
	crossCuts := graph.ComputeContractFeatures(crossDir)
	if !crossCuts[10].UnsyncedDirectoryRename {
		t.Fatalf("cut 10: cross-dir rename cleared prematurely after syncing only dirB")
	}
	if crossCuts[12].UnsyncedDirectoryRename {
		t.Fatalf("cut 12: cross-dir rename still pending after syncing both dirA and dirB")
	}
}

func TestDirtySpansOverlapMergeAndManifestSyncedPrefix(t *testing.T) {
	// 1. Overlapping WriteAt must merge dirtySpans so DanglingWALBytes is union length (150), not sum (200).
	overlapWAL := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 2},
		{ID: 3, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 4, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 5, Kind: trace.KindWriteAt, File: "db/000001.log", Handle: 2, Off: 0, Len: 100, Data: make([]byte, 100)},
		{ID: 6, Kind: trace.KindWriteAt, File: "db/000001.log", Handle: 2, Off: 50, Len: 100, Data: make([]byte, 100)},
		{ID: 7, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}
	requireValidTrace(t, overlapWAL)
	cfOverlap := graph.ExtractContractFeatures(overlapWAL, 7)
	if cfOverlap.DanglingWALBytes != 150 {
		t.Fatalf("overlapping WriteAt DanglingWALBytes=%d, want 150", cfOverlap.DanglingWALBytes)
	}

	// 2. Previously synced MANIFEST overwritten via WriteAt must not falsely trigger Case 1.
	manifestOverwrite := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 2},
		{ID: 3, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 2, Off: 0, Len: 64, Data: make([]byte, 64)},
		{ID: 4, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 2},
		{ID: 5, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 6, Kind: trace.KindWriteAt, File: "db/MANIFEST-000001", Handle: 2, Off: 0, Len: 64, Data: make([]byte, 64)},
		{ID: 7, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
	}
	requireValidTrace(t, manifestOverwrite)
	cfMan := graph.ExtractContractFeatures(manifestOverwrite, 7)
	if cfMan.DanglingTableCount != 0 {
		t.Fatalf("previously synced MANIFEST overwritten via WriteAt falsely triggered Case 1")
	}
}

func TestClosingSecondaryHandleAndCurrentOpHadSyncIsolation(t *testing.T) {
	// 1. Closing reader handle while dirty writer handle remains open must not set HasClosedDirtyLog or DanglingTableCount.
	twoHandles := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 2},
		{ID: 3, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 4, Kind: trace.KindOpen, File: "db/000001.log", Handle: 3},
		{ID: 5, Kind: trace.KindWrite, File: "db/000001.log", Handle: 2, Len: 64, Data: make([]byte, 64)},
		{ID: 6, Kind: trace.KindClose, File: "db/000001.log", Handle: 3},
		{ID: 7, Kind: trace.KindClose, File: "db/000001.log", Handle: 2},
	}
	requireValidTrace(t, twoHandles)
	cuts := graph.ComputeContractFeatures(twoHandles)
	if cuts[6].HasClosedDirtyLog {
		t.Fatalf("cut 6: closing secondary reader handle falsely marked HasClosedDirtyLog=true")
	}
	if !cuts[7].HasClosedDirtyLog {
		t.Fatalf("cut 7: closing last dirty handle failed to set HasClosedDirtyLog=true")
	}

	// 2. Background compaction SST SyncData inside a client op must not count as WAL sync (ackSyncOps).
	bgSync := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/000005.sst", Cat: "pebble-compaction", Handle: 1},
		{ID: 2, Kind: trace.KindCreate, File: "db/OPTIONS.tmp", Handle: 2},
		{ID: 3, Kind: trace.KindClose, File: "db/OPTIONS.tmp", Handle: 2},
		{ID: 4, Kind: trace.KindRename, File: "db/OPTIONS.tmp", File2: "db/OPTIONS-000003"},
		{ID: 5, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 6, Kind: trace.KindSyncData, File: "db/000005.sst", Handle: 1},
		{ID: 7, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
	}
	requireValidTrace(t, bgSync)
	cfBg := graph.ExtractContractFeatures(bgSync, 7)
	if cfBg.UnsyncedDirectoryRename {
		t.Fatalf("background compaction SST SyncData falsely enabled ackSyncOps for OPTIONS rename")
	}
}

func TestMarkerManifestAndConcurrentJobsAndUnsyncedSubdir(t *testing.T) {
	// 1. 0-byte marker.manifest.* must not trigger manifestDirtyUncommitted across OpBegin.
	markerTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "db/marker.manifest.000001.MANIFEST-000001", Cat: "unspecified", Handle: 1},
		{ID: 2, Kind: trace.KindClose, File: "db/marker.manifest.000001.MANIFEST-000001", Handle: 1},
		{ID: 3, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
	}
	requireValidTrace(t, markerTrace)
	if cf := graph.ExtractContractFeatures(markerTrace, 3); cf.DanglingTableCount != 0 {
		t.Fatalf("0-byte marker.manifest falsely flagged DanglingTableCount=%d", cf.DanglingTableCount)
	}

	// 2. Concurrent compaction SST does not block flush healing of covered WAL, while post-FlushBegin WAL stays dirty.
	concurrentJobs := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 2},
		{ID: 4, Kind: trace.KindCreate, File: "db/000001.log", Cat: "pebble-wal", Handle: 3},
		{ID: 5, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 6, Kind: trace.KindWrite, File: "db/000001.log", Handle: 3, Len: 64, Data: make([]byte, 64)},
		{ID: 7, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 8, Kind: trace.KindClose, File: "db/000001.log", Handle: 3},
		{ID: 9, Kind: trace.KindEngineEvent, Note: "FlushBegin", Job: 1},
		{ID: 10, Kind: trace.KindEngineEvent, Note: "CompactionBegin", Job: 2},
		{ID: 11, Kind: trace.KindCreate, File: "db/000008.sst", Cat: "pebble-compaction", Handle: 4},
		{ID: 12, Kind: trace.KindWrite, File: "db/000008.sst", Handle: 4, Len: 128, Data: make([]byte, 128)},
		{ID: 13, Kind: trace.KindCreate, File: "db/000009.log", Cat: "pebble-wal", Handle: 5},
		{ID: 14, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 15, Kind: trace.KindWrite, File: "db/000009.log", Handle: 5, Len: 32, Data: make([]byte, 32)},
		{ID: 16, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"},
		{ID: 17, Kind: trace.KindClose, File: "db/000009.log", Handle: 5},
		{ID: 18, Kind: trace.KindCreate, File: "db/000005.sst", Cat: "pebble-memtable-flush", Handle: 6},
		{ID: 19, Kind: trace.KindEngineEvent, Note: "TableCreated", Job: 1, Outputs: []string{"db/000005.sst"}},
		{ID: 20, Kind: trace.KindWrite, File: "db/000005.sst", Handle: 6, Len: 256, Data: make([]byte, 256)},
		{ID: 21, Kind: trace.KindSyncData, File: "db/000005.sst", Handle: 6},
		{ID: 22, Kind: trace.KindClose, File: "db/000005.sst", Handle: 6},
		{ID: 23, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 24, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 2, Len: 64, Data: make([]byte, 64)},
		{ID: 25, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 2},
		{ID: 26, Kind: trace.KindEngineEvent, Note: "FlushEnd", Job: 1},
	}
	requireValidTrace(t, concurrentJobs)
	cfJobs := graph.ExtractContractFeatures(concurrentJobs, 26)
	// WAL 000001.log (64 bytes, closed at ID 8 < FlushBegin 9) must be healed despite active dirty compaction SST!
	// WAL 000009.log (32 bytes, closed at ID 17 > FlushBegin 9) must NOT be healed!
	if !cfJobs.HasClosedDirtyLog || cfJobs.DirtyLogBytes != 32 {
		t.Fatalf("expected only post-FlushBegin WAL (32 bytes) unhealed, got dirty=%v bytes=%d",
			cfJobs.HasClosedDirtyLog, cfJobs.DirtyLogBytes)
	}

	// 3. SST in unsynced subdirectory (unsyncedDirs[sub]) triggers DanglingTableCount with full SST size until parent synced.
	unsyncedSubdirSST := []trace.Event{
		{ID: 1, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindMkdirAll, File: "db/sub", Mode: 0755},
		{ID: 4, Kind: trace.KindCreate, File: "db/sub/000004.sst", Cat: "pebble-memtable-flush", Handle: 2},
		{ID: 5, Kind: trace.KindWrite, File: "db/sub/000004.sst", Handle: 2, Len: 2048, Data: make([]byte, 2048)},
		{ID: 6, Kind: trace.KindSyncData, File: "db/sub/000004.sst", Handle: 2},
		{ID: 7, Kind: trace.KindOpenDir, File: "db/sub", Handle: 3, Dir: true},
		{ID: 8, Kind: trace.KindSync, File: "db/sub", Handle: 3, Dir: true},
		{ID: 9, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 4},
		{ID: 10, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 4, Len: 64, Data: make([]byte, 64)},
		{ID: 11, Kind: trace.KindSync, File: "db/MANIFEST-000001", Handle: 4},
		{ID: 12, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
	}
	requireValidTrace(t, unsyncedSubdirSST)
	subCuts := graph.ComputeContractFeatures(unsyncedSubdirSST)
	if subCuts[11].DanglingTableCount != 1 || subCuts[11].DanglingTableBytes != 2048 {
		t.Fatalf("cut 11 unsynced subdir SST: count=%d bytes=%d, want 1/2048",
			subCuts[11].DanglingTableCount, subCuts[11].DanglingTableBytes)
	}
	if subCuts[12].DanglingTableCount != 0 {
		t.Fatalf("cut 12 after parent dir sync: DanglingTableCount=%d, want 0", subCuts[12].DanglingTableCount)
	}

	// 4. Concurrent ComputeContractFeatures misses do not corrupt or duplicate cache results.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := graph.ComputeContractFeatures(concurrentJobs)
			if res[26].DirtyLogBytes != 32 {
				t.Errorf("concurrent ComputeContractFeatures mismatch: %+v", res[26])
			}
		}()
	}
	wg.Wait()
}
