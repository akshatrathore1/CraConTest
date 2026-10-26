package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

// TestAckSyncMissing tests detection of unpersisted acknowledged WAL writes ("ack <- sync" missing edge).
func TestAckSyncMissing(t *testing.T) {
	// Case 1: Mutating write acknowledged with OpEnd, but WAL sync was dropped/omitted.
	unpersistedTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 10},
		{ID: 2, Kind: trace.KindOpBegin, Op: 101, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000002.log", Cat: "pebble-wal", Handle: 10, Off: 0, Len: 256},
		{ID: 4, Kind: trace.KindOpEnd, Op: 101, OpKind: "set", Err: ""}, // Acknowledged commit!
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 10},
		{ID: 2, Kind: trace.KindOpenDir, File: "", Handle: 20, Dir: true},
		{ID: 3, Kind: trace.KindSync, File: "", Handle: 20, Dir: true}, // Parent directory synced!
		{ID: 4, Kind: trace.KindOpBegin, Op: 101, OpKind: "set"},
		{ID: 5, Kind: trace.KindWrite, File: "000002.log", Cat: "pebble-wal", Handle: 10, Off: 0, Len: 256},
		{ID: 6, Kind: trace.KindSync, File: "000002.log", Cat: "pebble-wal", Handle: 10}, // WAL sync executed!
		{ID: 7, Kind: trace.KindOpEnd, Op: 101, OpKind: "set", Err: ""},
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "000002.log", Cat: "pebble-wal", Handle: 10},
		// Parent directory sync omitted!
		{ID: 2, Kind: trace.KindOpBegin, Op: 101, OpKind: "set"},
		{ID: 3, Kind: trace.KindWrite, File: "000002.log", Cat: "pebble-wal", Handle: 10, Off: 0, Len: 256},
		{ID: 4, Kind: trace.KindSync, File: "000002.log", Cat: "pebble-wal", Handle: 10}, // WAL inode data synced, but dir entry unsynced
		{ID: 5, Kind: trace.KindOpEnd, Op: 101, OpKind: "set", Err: ""},
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20},
		{ID: 2, Kind: trace.KindWrite, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20, Off: 0, Len: 4096},
		// Missing Sync on 000004.sst (MUT-4)!
		{ID: 3, Kind: trace.KindCreate, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30},
		{ID: 4, Kind: trace.KindWrite, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30, Off: 0, Len: 512},
		{ID: 5, Kind: trace.KindSync, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30},
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20},
		{ID: 2, Kind: trace.KindWrite, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20, Off: 0, Len: 4096},
		{ID: 3, Kind: trace.KindSync, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20}, // Table data synced!
		{ID: 4, Kind: trace.KindOpenDir, File: "", Handle: 50, Dir: true},
		{ID: 5, Kind: trace.KindSync, File: "", Handle: 50, Dir: true}, // Parent directory synced!
		{ID: 6, Kind: trace.KindCreate, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30},
		{ID: 7, Kind: trace.KindWrite, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30, Off: 0, Len: 512},
		{ID: 8, Kind: trace.KindSync, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30},
		{ID: 9, Kind: trace.KindSync, File: "", Handle: 50, Dir: true}, // Manifest parent directory synced!
	}

	countClean, bytesClean := graph.DanglingTableReferences(persistedTrace, 9)
	if countClean != 0 || bytesClean != 0 {
		t.Fatalf("expected 0 dangling tables, got count=%d, bytes=%d", countClean, bytesClean)
	}

	// Case 3: SSTable data synced, but parent directory sync omitted (MUT-3 / MUT-9 SST directory hazard).
	unsyncedDirSSTTrace := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20},
		{ID: 2, Kind: trace.KindWrite, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20, Off: 0, Len: 4096},
		{ID: 3, Kind: trace.KindSync, File: "000004.sst", Cat: "pebble-memtable-flush", Handle: 20}, // Table data synced!
		// Parent directory sync omitted (MUT-3 / MUT-9)!
		{ID: 4, Kind: trace.KindCreate, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30},
		{ID: 5, Kind: trace.KindWrite, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30, Off: 0, Len: 512},
		{ID: 6, Kind: trace.KindSync, File: "MANIFEST-000001", Cat: "pebble-manifest", Handle: 30},
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "CURRENT.tmp", Handle: 40},
		{ID: 2, Kind: trace.KindWrite, File: "CURRENT.tmp", Handle: 40, Off: 0, Len: 16},
		{ID: 3, Kind: trace.KindSync, File: "CURRENT.tmp", Handle: 40},
		{ID: 4, Kind: trace.KindClose, File: "CURRENT.tmp", Handle: 40},
		{ID: 5, Kind: trace.KindRename, File: "CURRENT.tmp", File2: "CURRENT"}, // Atomic rename
		{ID: 6, Kind: trace.KindOpBegin, Op: 101, OpKind: "set"},               // Enclosing step crossed without dir sync!
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "CURRENT.tmp", Handle: 40},
		{ID: 2, Kind: trace.KindWrite, File: "CURRENT.tmp", Handle: 40, Off: 0, Len: 16},
		{ID: 3, Kind: trace.KindSync, File: "CURRENT.tmp", Handle: 40},
		{ID: 4, Kind: trace.KindClose, File: "CURRENT.tmp", Handle: 40},
		{ID: 5, Kind: trace.KindRename, File: "CURRENT.tmp", File2: "CURRENT"},
		{ID: 6, Kind: trace.KindOpenDir, File: "", Handle: 50, Dir: true},
		{ID: 7, Kind: trace.KindSync, File: "", Handle: 50, Dir: true}, // Directory sync executed!
	}

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
			t.Skipf("trace file %s not found (skipping): %v", p, err)
			continue
		}
		var doc traceDoc
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatalf("decode %s: %v", p, err)
		}
		if len(doc.Events) == 0 {
			t.Fatalf("empty events in %s", p)
		}

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
		{ID: 1, Kind: trace.KindCreate, File: "db/000004.sst", Cat: "pebble-memtable-flush", Handle: 10},
		{ID: 2, Kind: trace.KindWrite, File: "db/000004.sst", Handle: 10, Len: 1024},
		// SyncData omitted (MUT-4 / MUT-8)
		{ID: 3, Kind: trace.KindClose, File: "db/000004.sst", Handle: 10},
		{ID: 4, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 20},
		{ID: 5, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 20, Len: 256},
		{ID: 6, Kind: trace.KindSync, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 20},
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "db/MANIFEST-000001", Cat: "pebble-manifest", Handle: 20},
		{ID: 2, Kind: trace.KindWrite, File: "db/MANIFEST-000001", Handle: 20, Len: 64},
		// Sync omitted (MUT-2 / MUT-6)
		{ID: 3, Kind: trace.KindEngineEvent, Note: "FlushEnd"},
	}

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
		{ID: 1, Kind: trace.KindCreate, File: "db/000002.log", Cat: "pebble-wal", Handle: 10},
		{ID: 2, Kind: trace.KindSync, File: "db/000002.log", Handle: 10},
		{ID: 3, Kind: trace.KindClose, File: "db/000002.log", Handle: 10},
		{ID: 4, Kind: trace.KindReuseForWrite, File: "db/000002.log", File2: "db/000006.log", Cat: "pebble-wal", Handle: 20},
		{ID: 5, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 6, Kind: trace.KindWrite, File: "db/000006.log", Handle: 20, Len: 128},
		{ID: 7, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""}, // WAL sync omitted
	}

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
		check    func(t *testing.T, id string, violations int, last graph.ContractFeatures)
	}{
		{id: faults.MUT1, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT2, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT3, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT4, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT5, walDropK: 1, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s (k=1): expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT5, walDropK: 3, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s (k=3): expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT6, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT7, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
		{id: faults.MUT8, check: func(t *testing.T, id string, v int, last graph.ContractFeatures) {
			if v == 0 {
				t.Fatalf("%s: expected positive ContractScore, got 0 violations", id)
			}
		}},
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
			var last graph.ContractFeatures
			for cut := uint64(0); cut <= uint64(len(evs)); cut++ {
				if cfs[cut].ContractScore > 0 {
					violationCount++
					last = cfs[cut]
				}
			}
			f.check(t, f.id, violationCount, last)
		})
	}
}

// TestContractScore_ScopedDirectorySyncIsolation verifies that syncing directory A
// isolates its durability from pending files and renames in directory B.
func TestContractScore_ScopedDirectorySyncIsolation(t *testing.T) {
	events := []trace.Event{
		// 1. Setup handles and files in dirA and dirB
		{ID: 1, Kind: trace.KindOpenDir, File: "dirA", Handle: 1, Dir: true},
		{ID: 2, Kind: trace.KindOpenDir, File: "dirB", Handle: 2, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "dirA/000001.log", Cat: "pebble-wal", Handle: 11},
		{ID: 4, Kind: trace.KindCreate, File: "dirB/000002.log", Cat: "pebble-wal", Handle: 12},

		// 2. Sync only dirA
		{ID: 5, Kind: trace.KindSync, File: "dirA", Handle: 1, Dir: true},

		// 3. Mutating Op 1 writes and syncs WAL in dirA
		{ID: 6, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 7, Kind: trace.KindWrite, File: "dirA/000001.log", Handle: 11, Len: 128},
		{ID: 8, Kind: trace.KindSync, File: "dirA/000001.log", Handle: 11},
		{ID: 9, Kind: trace.KindOpEnd, Op: 1, OpKind: "set", Err: ""},

		// 4. Mutating Op 2 writes and syncs WAL in dirB (where dirB directory entry was NOT synced)
		{ID: 10, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"},
		{ID: 11, Kind: trace.KindWrite, File: "dirB/000002.log", Handle: 12, Len: 256},
		{ID: 12, Kind: trace.KindSync, File: "dirB/000002.log", Handle: 12},
		{ID: 13, Kind: trace.KindOpEnd, Op: 2, OpKind: "set", Err: ""},

		// 5. Atomic renames in both directories
		{ID: 14, Kind: trace.KindRename, File: "dirA/CURRENT.tmp", File2: "dirA/CURRENT"},
		{ID: 15, Kind: trace.KindRename, File: "dirB/OPTIONS.tmp", File2: "dirB/OPTIONS"},

		// 6. Sync dirA again
		{ID: 16, Kind: trace.KindSync, File: "dirA", Handle: 1, Dir: true},
		// Crossing lifecycle boundary
		{ID: 17, Kind: trace.KindOpBegin, Op: 3, OpKind: "get"},
	}

	cfCut9 := graph.ExtractContractFeatures(events, 9)
	if cfCut9.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=false at cut 9 for dirA after dirA sync, got true")
	}

	cfCut13 := graph.ExtractContractFeatures(events, 13)
	if !cfCut13.AckSyncMissing {
		t.Fatalf("expected AckSyncMissing=true at cut 13 for dirB because dirB was never synced")
	}
	if cfCut13.UnpersistedAckOps != 1 {
		t.Fatalf("expected UnpersistedAckOps=1 at cut 13, got %d", cfCut13.UnpersistedAckOps)
	}
	if cfCut13.DanglingWALBytes != 256 {
		t.Fatalf("expected DanglingWALBytes=256 at cut 13, got %d", cfCut13.DanglingWALBytes)
	}

	cfCut17 := graph.ExtractContractFeatures(events, 17)
	// dirA rename was cleared by cut 16, but dirB rename remains violating!
	if !cfCut17.UnsyncedDirectoryRename {
		t.Fatalf("expected UnsyncedDirectoryRename=true at cut 17 for unsynced dirB rename")
	}
	if cfCut17.CriticalPendingRenames != 1 {
		t.Fatalf("expected CriticalPendingRenames=1 at cut 17, got %d", cfCut17.CriticalPendingRenames)
	}
}
