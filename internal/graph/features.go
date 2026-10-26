package graph

import (
	"math"
	"sort"
	"strings"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// Tier constants for 5-tier discriminative semantic weighting.
const (
	TierManifest = 4 // Manifest VersionEdits, CURRENT pointer updates, directory renames
	TierWAL      = 3 // Synchronous WAL commits
	TierDir      = 2 // Directory namespace updates (mkdir, link, rename, remove, dir sync)
	TierFlush    = 1 // Memtable flush writes (SST creation)
	TierData     = 0 // Routine data appends, background compaction, unspecified writes
)

// TierWeight returns the exact weight for a given semantic tier:
// - Tier 4: 10.0
// - Tier 3: 8.0
// - Tier 2: 6.0
// - Tier 1: 2.0
// - Tier 0: 0.0
func TierWeight(tier int) float64 {
	switch tier {
	case TierManifest:
		return 10.0
	case TierWAL:
		return 8.0
	case TierDir:
		return 6.0
	case TierFlush:
		return 2.0
	default:
		return 0.0
	}
}

// EventTier classifies a trace event into one of the 5 semantic tiers and returns its weight.
func EventTier(event trace.Event) (int, float64) {
	// Tier 4 (Manifest VersionEdits): 10.0
	if event.Cat == "pebble-manifest" ||
		strings.Contains(event.File, "MANIFEST") ||
		strings.Contains(event.File, "CURRENT") ||
		strings.Contains(event.File2, "MANIFEST") ||
		strings.Contains(event.File2, "CURRENT") ||
		event.Note == "ManifestCreated" {
		return TierManifest, 10.0
	}

	// Tier 3 (Synchronous WAL commits): 8.0
	if event.Cat == "pebble-wal" ||
		strings.HasSuffix(event.File, ".log") ||
		strings.HasSuffix(event.File2, ".log") ||
		event.Note == "WALCreated" {
		return TierWAL, 8.0
	}

	// Tier 2 (Directory namespace updates): 6.0
	if event.Dir ||
		event.Kind == trace.KindOpenDir ||
		event.Kind == trace.KindMkdirAll ||
		event.Kind == trace.KindLink ||
		event.Kind == trace.KindRename ||
		event.Kind == trace.KindRemove ||
		event.Kind == trace.KindRemoveAll {
		return TierDir, 6.0
	}

	// Tier 1 (Memtable flush writes): 2.0
	if event.Cat == "pebble-memtable-flush" ||
		event.Note == "FlushBegin" ||
		event.Note == "FlushEnd" ||
		event.Note == "TableCreated" {
		return TierFlush, 2.0
	}

	// Tier 0 (Routine data appends): 0.0
	return TierData, 0.0
}

// ResourceTier evaluates the semantic tier for a resource's categories and attributes.
func ResourceTier(categories map[string]bool, aliases map[string]bool, directory bool) (int, float64) {
	if categories["pebble-manifest"] {
		return TierManifest, 10.0
	}
	for alias := range aliases {
		if strings.Contains(alias, "MANIFEST") || strings.Contains(alias, "CURRENT") {
			return TierManifest, 10.0
		}
	}
	if categories["pebble-wal"] {
		return TierWAL, 8.0
	}
	for alias := range aliases {
		if strings.HasSuffix(alias, ".log") {
			return TierWAL, 8.0
		}
	}
	if directory {
		return TierDir, 6.0
	}
	if categories["pebble-memtable-flush"] {
		return TierFlush, 2.0
	}
	return TierData, 0.0
}

// CutoffTier computes the effective semantic tier and weight for a cutoff.
// If the cutoff immediately precedes a barrier, the tier of that enclosing
// barrier promotes the cutoff's tier if higher.
func CutoffTier(events []trace.Event, cut uint64, anchor uint64, nextBarrier uint64, r *resource) (int, float64) {
	bestTier := TierData
	bestWeight := 0.0

	// Check anchor vertex if present.
	if anchor > 0 && int(anchor) <= len(events) {
		t, w := EventTier(events[anchor-1])
		if w > bestWeight {
			bestTier = t
			bestWeight = w
		}
	}

	// Check resource categories if present.
	if r != nil {
		t, w := ResourceTier(r.categories, r.aliases, r.directory)
		if w > bestWeight {
			bestTier = t
			bestWeight = w
		}
	}

	// If cutoff immediately precedes a barrier, that barrier determines the omission target.
	if nextBarrier > 0 && cut == nextBarrier-1 && int(nextBarrier) <= len(events) {
		t, w := EventTier(events[nextBarrier-1])
		if w > bestWeight {
			bestTier = t
			bestWeight = w
		}
	}

	return bestTier, bestWeight
}

// extractBarriers finds all barrier and rename event IDs in the trace in ascending order.
func extractBarriers(events []trace.Event) []uint64 {
	var barriers []uint64
	for _, event := range events {
		if event.IsBarrier() || (event.Kind == trace.KindRename && event.Err == "") {
			barriers = append(barriers, event.ID)
		}
	}
	return barriers
}

// computeBarrierProximity calculates the inverse distance 1 / (1 + delta)
// to the nearest enclosing subsequent barrier, and returns that barrier's event ID.
// When cut == barrier - 1 (immediately before the barrier), delta is 0 and proximity is 1.0.
func computeBarrierProximity(cut uint64, barriers []uint64) (float64, uint64) {
	// Binary search or linear scan for the smallest barrier > cut
	for _, b := range barriers {
		if b > cut {
			delta := (b - 1) - cut
			proximity := 1.0 / (1.0 + float64(delta))
			return proximity, b
		}
	}
	return 0.0, 0
}

// computeResourceDirtyBytes calculates the unpersisted dirty byte count on a resource.
func computeResourceDirtyBytes(r *resource) uint64 {
	if r.directory {
		return 0
	}
	var total uint64
	for _, span := range r.dirty {
		if span.end > span.start {
			total += uint64(span.end - span.start)
		}
	}
	return total
}

// resourceCategoryWeight returns the discriminative weight for dirty byte volume on a resource.
func resourceCategoryWeight(r *resource) float64 {
	if r.categories["pebble-manifest"] {
		return 10.0
	}
	for alias := range r.aliases {
		if strings.Contains(alias, "MANIFEST") || strings.Contains(alias, "CURRENT") {
			return 10.0
		}
	}
	if r.categories["pebble-wal"] {
		return 8.0
	}
	for alias := range r.aliases {
		if strings.HasSuffix(alias, ".log") {
			return 8.0
		}
	}
	if r.directory {
		return 6.0
	}
	if r.categories["pebble-memtable-flush"] {
		return 2.0
	}
	return 1.0
}

// computeRiskHorizon calculates the backward unpersisted dirty byte volume,
// pending namespace mutations, and active uncommitted synchronous client exposure.
// Formula:
//
//	RiskScore = sum_{r in dirty} w_cat(r) * ln(1 + DirtyBytes(r)) + 6.0 * len(pending) + 4.0 * uncommittedSyncOps
func computeRiskHorizon(resources map[uint64]*resource, pending []pendingEntry, uncommittedSyncOps uint64) (uint64, float64) {
	var totalDirtyBytes uint64
	var dirtyRisk float64

	ids := make([]uint64, 0, len(resources))
	for id := range resources {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		r := resources[id]
		if r.directory {
			continue
		}
		bytes := computeResourceDirtyBytes(r)
		if bytes > 0 {
			totalDirtyBytes += bytes
			w := resourceCategoryWeight(r)
			dirtyRisk += w * math.Log1p(float64(bytes))
		}
	}

	pendingRisk := 6.0 * float64(len(pending))
	syncRisk := 4.0 * float64(uncommittedSyncOps)

	riskScore := dirtyRisk + pendingRisk + syncRisk
	return totalDirtyBytes, riskScore
}

// Durability Contract Violation bitmask flags.
const (
	MaskAckSyncMissing uint8 = 1 << 0 // 1: Unpersisted acknowledged synchronous writes
	MaskDanglingTable  uint8 = 1 << 1 // 2: Manifest VersionEdit referencing unsynced SSTables
	MaskUnsyncedRename uint8 = 1 << 2 // 4: Namespace Rename lacking parent directory sync
)

// DCGWeights configures term weights for composite ContractScore.
type DCGWeights struct {
	WALWeight      float64 // weight for unpersisted acknowledged WAL writes (default 30.0)
	ManifestWeight float64 // weight for dangling SSTable references (default 40.0)
	RenameWeight   float64 // weight for uncommitted critical directory renames (default 50.0)
}

// DefaultDCGWeights provides standard term weights calibrated from crash consistency models.
var DefaultDCGWeights = DCGWeights{
	WALWeight:      30.0,
	ManifestWeight: 40.0,
	RenameWeight:   50.0,
}

// ContractFeatures captures durability-contract and dangling-dependency violations at a cutoff.
type ContractFeatures struct {
	// Indicator 1: AckSyncMissing ("ack <- sync" missing edge)
	AckSyncMissing    bool   `json:"ack_sync_missing"`    // true if any acknowledged sync write has unpersisted WAL bytes
	UnpersistedAckOps uint64 `json:"unpersisted_ack_ops"` // count of acknowledged mutating ops whose WAL bytes remain unpersisted
	DanglingWALBytes  uint64 `json:"dangling_wal_bytes"`  // byte volume of unpersisted WAL bytes belonging to acknowledged ops

	// Indicator 2: DanglingTableCount (Manifest VersionEdit referencing unsynced SSTables)
	DanglingTableCount int    `json:"dangling_table_count"` // count of SST files referenced by manifest that remain unsynced
	DanglingTableBytes uint64 `json:"dangling_table_bytes"` // byte volume of unsynced SSTables referenced by manifest

	// Indicator 3: UnsyncedDirectoryRename (Namespace Rename lacking parent directory sync)
	UnsyncedDirectoryRename bool `json:"unsynced_directory_rename"` // true if any rename lacks parent directory sync
	PendingRenames          int  `json:"pending_renames"`           // total count of uncommitted renames in pending namespace
	CriticalPendingRenames  int  `json:"critical_pending_renames"`  // count of uncommitted renames targeting CURRENT or MANIFEST

	// Composite indicators:
	ContractViolationMask uint8   `json:"contract_violation_mask"` // bitmask: 1=WAL, 2=SST, 4=Rename
	ContractScore         float64 `json:"contract_score"`          // weighted composite contract violation score
}

type writeSpan struct {
	off int64
	end int64
}

type trackFileInfo struct {
	path                string
	parentDir           string
	dirEntrySynced      bool
	size                int64
	isWAL               bool
	isSST               bool
	isCompactionSST     bool
	isFlushSST          bool
	isManifest          bool
	isDir               bool
	dirtySpans          []writeSpan
	dirtyBytes          uint64
	createdEvent        uint64
	closedWhileDirty    bool
	stagedInManifest    bool
	committedInManifest bool
}

type opWalWrite struct {
	file string
	span writeSpan
}

type renameRecord struct {
	from       string
	to         string
	isCritical bool
	dir        string
	eventID    uint64
	violating  bool
}

// ComputeContractFeatures computes ContractFeatures for every cutoff 0..len(events) using DefaultDCGWeights.
func ComputeContractFeatures(events []trace.Event) map[uint64]ContractFeatures {
	return ComputeContractFeaturesWithWeights(events, DefaultDCGWeights)
}

// ComputeContractFeaturesWithWeights computes ContractFeatures for every cutoff 0..len(events) using custom weights.
func ComputeContractFeaturesWithWeights(events []trace.Event, weights DCGWeights) map[uint64]ContractFeatures {
	cuts := make(map[uint64]ContractFeatures, len(events)+1)
	cuts[0] = ContractFeatures{}

	files := make(map[string]*trackFileInfo)
	handles := make(map[uint64]*trackFileInfo)
	opWrites := make(map[uint64][]opWalWrite)
	ackOps := make(map[uint64]bool)
	ackSyncOps := make(map[uint64]bool)
	var currentOp uint64
	var currentOpHadSync bool
	var manifestDirtyUncommitted bool
	var pendingRenames []renameRecord
	var activeFlush bool
	var activeCompaction bool
	unsyncedDirs := make(map[string]string) // dirPath -> parentDir

	for _, event := range events {
		switch event.Kind {
		case trace.KindOpBegin:
			if trace.MutatingOperation(event.OpKind) {
				currentOp = event.Op
				currentOpHadSync = false
			}
		case trace.KindCreate:
			isWAL := event.Cat == "pebble-wal" || strings.HasSuffix(event.File, ".log") || strings.Contains(event.File, ".log") || event.Note == "WALCreated"
			isCompactionSST := event.Cat == "pebble-compaction"
			isFlushSST := event.Cat == "pebble-memtable-flush" || event.Note == "FlushBegin"
			isSST := isCompactionSST || isFlushSST || strings.HasSuffix(event.File, ".sst") || strings.Contains(event.File, ".sst") || event.Note == "TableCreated"
			isManifest := event.Cat == "pebble-manifest" || strings.Contains(event.File, "MANIFEST") || strings.Contains(event.File, "CURRENT") || event.Note == "ManifestCreated"
			fi := &trackFileInfo{
				path:            event.File,
				parentDir:       parent(event.File),
				dirEntrySynced:  false,
				isWAL:           isWAL,
				isSST:           isSST,
				isCompactionSST: isCompactionSST,
				isFlushSST:      isFlushSST,
				isManifest:      isManifest,
				isDir:           event.Dir,
				createdEvent:    event.ID,
			}
			files[event.File] = fi
			if event.Handle > 0 {
				handles[event.Handle] = fi
			}
		case trace.KindReuseForWrite:
			isWAL := event.Cat == "pebble-wal" || strings.HasSuffix(event.File2, ".log") || strings.Contains(event.File2, ".log") || event.Note == "WALCreated"
			isCompactionSST := event.Cat == "pebble-compaction"
			isFlushSST := event.Cat == "pebble-memtable-flush" || event.Note == "FlushBegin"
			isSST := isCompactionSST || isFlushSST || strings.HasSuffix(event.File2, ".sst") || strings.Contains(event.File2, ".sst") || event.Note == "TableCreated"
			isManifest := event.Cat == "pebble-manifest" || strings.Contains(event.File2, "MANIFEST") || strings.Contains(event.File2, "CURRENT") || event.Note == "ManifestCreated"
			delete(files, event.File)
			fi := &trackFileInfo{
				path:            event.File2,
				parentDir:       parent(event.File2),
				dirEntrySynced:  false,
				isWAL:           isWAL,
				isSST:           isSST,
				isCompactionSST: isCompactionSST,
				isFlushSST:      isFlushSST,
				isManifest:      isManifest,
				isDir:           false,
				createdEvent:    event.ID,
			}
			files[event.File2] = fi
			if event.Handle > 0 {
				handles[event.Handle] = fi
			}
		case trace.KindOpen, trace.KindOpenReadWrite, trace.KindOpenDir:
			fi := files[event.File]
			if fi == nil {
				isWAL := event.Cat == "pebble-wal" || strings.HasSuffix(event.File, ".log") || strings.Contains(event.File, ".log")
				isCompactionSST := event.Cat == "pebble-compaction"
				isFlushSST := event.Cat == "pebble-memtable-flush"
				isSST := isCompactionSST || isFlushSST || strings.HasSuffix(event.File, ".sst") || strings.Contains(event.File, ".sst")
				isManifest := event.Cat == "pebble-manifest" || strings.Contains(event.File, "MANIFEST") || strings.Contains(event.File, "CURRENT")
				dirEntrySynced := true
				if event.Created {
					dirEntrySynced = false
				}
				fi = &trackFileInfo{
					path:            event.File,
					parentDir:       parent(event.File),
					dirEntrySynced:  dirEntrySynced,
					isWAL:           isWAL,
					isSST:           isSST,
					isCompactionSST: isCompactionSST,
					isFlushSST:      isFlushSST,
					isManifest:      isManifest,
					isDir:           event.Dir || event.Kind == trace.KindOpenDir,
					createdEvent:    event.ID,
				}
				files[event.File] = fi
			} else {
				if event.Created {
					fi.dirEntrySynced = false
				}
			}
			if event.Handle > 0 {
				handles[event.Handle] = fi
			}
		case trace.KindWrite, trace.KindWriteAt:
			fi := handles[event.Handle]
			if fi == nil && event.File != "" {
				fi = files[event.File]
			}
			if fi == nil && event.File != "" {
				isWAL := event.Cat == "pebble-wal" || strings.HasSuffix(event.File, ".log") || strings.Contains(event.File, ".log")
				isCompactionSST := event.Cat == "pebble-compaction"
				isFlushSST := event.Cat == "pebble-memtable-flush"
				isSST := isCompactionSST || isFlushSST || strings.HasSuffix(event.File, ".sst") || strings.Contains(event.File, ".sst")
				isManifest := event.Cat == "pebble-manifest" || strings.Contains(event.File, "MANIFEST") || strings.Contains(event.File, "CURRENT")
				fi = &trackFileInfo{
					path:            event.File,
					parentDir:       parent(event.File),
					dirEntrySynced:  true,
					isWAL:           isWAL,
					isSST:           isSST,
					isCompactionSST: isCompactionSST,
					isFlushSST:      isFlushSST,
					isManifest:      isManifest,
					isDir:           event.Dir,
					createdEvent:    event.ID,
				}
				files[event.File] = fi
			}
			if fi != nil && event.Len > 0 {
				span := writeSpan{off: event.Off, end: event.Off + int64(event.Len)}
				fi.dirtySpans = append(fi.dirtySpans, span)
				fi.dirtyBytes += uint64(event.Len)
				if span.end > fi.size {
					fi.size = span.end
				}
				if fi.isWAL && currentOp > 0 {
					opWrites[currentOp] = append(opWrites[currentOp], opWalWrite{file: fi.path, span: span})
				}
				isMan := fi.isManifest || event.Cat == "pebble-manifest" || strings.Contains(event.File, "MANIFEST")
				if isMan {
					for _, sst := range files {
						if sst.isSST && (sst.dirtyBytes > 0 || len(sst.dirtySpans) > 0 || !sst.dirEntrySynced) {
							sst.stagedInManifest = true
						}
					}
				}
			}
		case trace.KindSync, trace.KindSyncData, trace.KindSyncTo:
			if event.IsBarrier() {
				if currentOp > 0 {
					currentOpHadSync = true
				}
				if event.Dir {
					syncedDir := event.File
					if hFi := handles[event.Handle]; hFi != nil && hFi.path != "" {
						syncedDir = hFi.path
					}
					if syncedDir == "." || syncedDir == "/" {
						syncedDir = ""
					}
					var keptRenames []renameRecord
					for _, r := range pendingRenames {
						if r.dir != syncedDir {
							keptRenames = append(keptRenames, r)
						}
					}
					pendingRenames = keptRenames

					for dirPath, pdir := range unsyncedDirs {
						if pdir == syncedDir {
							delete(unsyncedDirs, dirPath)
						}
					}

					for _, f := range files {
						if f.parentDir == syncedDir {
							f.dirEntrySynced = true
							if f.isSST && f.dirtyBytes == 0 && len(f.dirtySpans) == 0 {
								f.stagedInManifest = false
								f.committedInManifest = false
							}
						}
					}
				} else {
					fi := handles[event.Handle]
					if fi == nil && event.File != "" {
						fi = files[event.File]
					}
					isMan := (fi != nil && fi.isManifest) || event.Cat == "pebble-manifest" || strings.Contains(event.File, "MANIFEST")
					if isMan {
						for _, sst := range files {
							if sst.isSST && sst.stagedInManifest {
								if activeFlush && !activeCompaction {
									if sst.isFlushSST || (!sst.isCompactionSST && !sst.isFlushSST) {
										sst.committedInManifest = true
									}
								} else if activeCompaction && !activeFlush {
									if sst.isCompactionSST || (!sst.isCompactionSST && !sst.isFlushSST) {
										sst.committedInManifest = true
									}
								} else {
									sst.committedInManifest = true
								}
							}
						}
					}
					if fi != nil {
						fi.dirtySpans = nil
						fi.dirtyBytes = 0
						fi.closedWhileDirty = false
						if fi.isSST && fi.dirEntrySynced {
							fi.stagedInManifest = false
							fi.committedInManifest = false
						}
					}
				}
			}
		case trace.KindEngineEvent:
			if event.Note == "FlushBegin" {
				activeFlush = true
			} else if event.Note == "FlushEnd" {
				activeFlush = false
				for _, sst := range files {
					if sst.isSST && sst.isFlushSST && sst.stagedInManifest {
						sst.committedInManifest = true
					}
				}
			} else if event.Note == "CompactionBegin" {
				activeCompaction = true
			} else if event.Note == "CompactionEnd" {
				activeCompaction = false
				for _, sst := range files {
					if sst.isSST && sst.isCompactionSST && sst.stagedInManifest {
						sst.committedInManifest = true
					}
				}
			}
		case trace.KindMkdirAll:
			if event.Err == "" && event.File != "" {
				pdir := parent(event.File)
				if pdir == "." || pdir == "/" {
					pdir = ""
				}
				unsyncedDirs[event.File] = pdir
			}
		case trace.KindLink:
			if event.Err == "" {
				isWAL := event.Cat == "pebble-wal" || strings.HasSuffix(event.File2, ".log") || strings.Contains(event.File2, ".log")
				isCompactionSST := event.Cat == "pebble-compaction"
				isFlushSST := event.Cat == "pebble-memtable-flush"
				isSST := isCompactionSST || isFlushSST || strings.HasSuffix(event.File2, ".sst") || strings.Contains(event.File2, ".sst")
				isManifest := event.Cat == "pebble-manifest" || strings.Contains(event.File2, "MANIFEST") || strings.Contains(event.File2, "CURRENT")
				fi := &trackFileInfo{
					path:            event.File2,
					parentDir:       parent(event.File2),
					dirEntrySynced:  false,
					isWAL:           isWAL,
					isSST:           isSST,
					isCompactionSST: isCompactionSST,
					isFlushSST:      isFlushSST,
					isManifest:      isManifest,
					isDir:           false,
					createdEvent:    event.ID,
				}
				files[event.File2] = fi
			}
		case trace.KindRename:
			if event.Err == "" {
				fi := files[event.File]
				if fi != nil {
					delete(files, event.File)
					fi.path = event.File2
					fi.parentDir = parent(event.File2)
					fi.dirEntrySynced = false
					files[event.File2] = fi
				} else {
					isWAL := event.Cat == "pebble-wal" || strings.HasSuffix(event.File2, ".log") || strings.Contains(event.File2, ".log")
					isCompactionSST := event.Cat == "pebble-compaction"
					isFlushSST := event.Cat == "pebble-memtable-flush"
					isSST := isCompactionSST || isFlushSST || strings.HasSuffix(event.File2, ".sst") || strings.Contains(event.File2, ".sst")
					isManifest := event.Cat == "pebble-manifest" || strings.Contains(event.File2, "MANIFEST") || strings.Contains(event.File2, "CURRENT")
					fi := &trackFileInfo{
						path:            event.File2,
						parentDir:       parent(event.File2),
						dirEntrySynced:  false,
						isWAL:           isWAL,
						isSST:           isSST,
						isCompactionSST: isCompactionSST,
						isFlushSST:      isFlushSST,
						isManifest:      isManifest,
						createdEvent:    event.ID,
					}
					files[event.File2] = fi
				}
				isCrit := strings.Contains(event.File2, "CURRENT") || strings.Contains(event.File2, "MANIFEST") || strings.Contains(event.File2, "OPTIONS")
				pendingRenames = append(pendingRenames, renameRecord{
					from:       event.File,
					to:         event.File2,
					isCritical: isCrit,
					dir:        parent(event.File2),
					eventID:    event.ID,
					violating:  false,
				})
			}
		case trace.KindRemove, trace.KindRemoveAll:
			delete(files, event.File)
		case trace.KindClose:
			fi := handles[event.Handle]
			if fi == nil && event.File != "" {
				fi = files[event.File]
			}
			if fi != nil && fi.dirtyBytes > 0 {
				fi.closedWhileDirty = true
			}
			delete(handles, event.Handle)
		case trace.KindOpEnd:
			if trace.MutatingOperation(event.OpKind) && event.Err == "" {
				ackOps[event.Op] = true
				if currentOpHadSync {
					ackSyncOps[event.Op] = true
				}
			}
			if event.Op == currentOp {
				currentOp = 0
				currentOpHadSync = false
			}
		}

		// Check lifecycle boundary for uncommitted renames
		isLifecycleBoundary := false
		switch event.Kind {
		case trace.KindOpBegin, trace.KindOpEnd:
			isLifecycleBoundary = true
		case trace.KindEngineEvent:
			if event.Note == "FlushEnd" || event.Note == "CompactionEnd" {
				isLifecycleBoundary = true
			}
		case trace.KindCreate, trace.KindWrite, trace.KindWriteAt, trace.KindReuseForWrite:
			for _, r := range pendingRenames {
				target := event.File
				if event.Kind == trace.KindReuseForWrite && event.File2 != "" {
					target = event.File2
				}
				if target != "" && target != r.to && target != r.from {
					isLifecycleBoundary = true
					break
				}
			}
		}
		if isLifecycleBoundary {
			for i := range pendingRenames {
				pendingRenames[i].violating = true
			}
		}

		// Track unpersisted manifest past lifecycle boundaries
		if (event.Kind == trace.KindOpBegin && !activeFlush && !activeCompaction) || (event.Kind == trace.KindEngineEvent && (event.Note == "FlushEnd" || event.Note == "CompactionEnd")) {
			for _, f := range files {
				if f.isManifest && (f.dirtyBytes > 0 || !f.dirEntrySynced) {
					manifestDirtyUncommitted = true
					break
				}
			}
		}
		for _, f := range files {
			if f.isManifest && (f.dirtyBytes > 0 || !f.dirEntrySynced) && f.closedWhileDirty {
				manifestDirtyUncommitted = true
				break
			}
		}
		hasDirtyManifest := false
		for _, f := range files {
			if f.isManifest && (f.dirtyBytes > 0 || !f.dirEntrySynced) {
				hasDirtyManifest = true
				break
			}
		}
		if !hasDirtyManifest {
			manifestDirtyUncommitted = false
		}

		// Calculate contract snapshot at event.ID:
		// 1. Indicator 1: AckSyncMissing
		var unpersistedOps uint64
		var danglingWAL uint64
		for opID := range ackOps {
			writes := opWrites[opID]
			if len(writes) == 0 {
				continue
			}
			opHasDirty := false
			var opDirtyBytes uint64
			for _, w := range writes {
				fi := files[w.file]
				if fi != nil {
					_, parentUnsynced := unsyncedDirs[fi.parentDir]
					if !fi.dirEntrySynced || parentUnsynced {
						opHasDirty = true
						opDirtyBytes += uint64(w.span.end - w.span.off)
					} else if len(fi.dirtySpans) > 0 {
						for _, ds := range fi.dirtySpans {
							overlapStart := maxInt64(w.span.off, ds.off)
							overlapEnd := minInt64(w.span.end, ds.end)
							if overlapEnd > overlapStart {
								opHasDirty = true
								opDirtyBytes += uint64(overlapEnd - overlapStart)
							}
						}
					}
				}
			}
			if opHasDirty {
				unpersistedOps++
				danglingWAL += opDirtyBytes
			}
		}
		ackMissing := unpersistedOps > 0

		// 2. Indicator 2: Dangling SSTables & Unpersisted Manifest
		danglingSSTCount := 0
		var danglingSSTBytes uint64
		for _, fi := range files {
			if fi.isSST && (fi.dirtyBytes > 0 || len(fi.dirtySpans) > 0 || !fi.dirEntrySynced) {
				if fi.committedInManifest {
					danglingSSTCount++
					vol := fi.dirtyBytes
					if vol == 0 && fi.size > 0 {
						vol = uint64(fi.size)
					}
					danglingSSTBytes += vol
				}
			} else if fi.isManifest && (fi.dirtyBytes > 0 || !fi.dirEntrySynced) {
				if fi.closedWhileDirty || manifestDirtyUncommitted {
					danglingSSTCount++
					vol := fi.dirtyBytes
					if vol == 0 && fi.size > 0 {
						vol = uint64(fi.size)
					}
					danglingSSTBytes += vol
				}
			}
		}

		// 3. Indicator 3: Unsynced Directory Renames
		pendingCount := 0
		critCount := 0
		for _, r := range pendingRenames {
			if r.violating {
				isOptionsOrWAL := strings.Contains(r.to, "OPTIONS") || strings.HasSuffix(r.to, ".log") || strings.Contains(r.to, ".log")
				if isOptionsOrWAL {
					// WAL/options directory hazards require at least one acknowledged sync operation
					if len(ackSyncOps) > 0 {
						pendingCount++
						if r.isCritical {
							critCount++
						}
					}
				} else {
					pendingCount++
					if r.isCritical {
						critCount++
					}
				}
			}
		}
		unsyncedRename := pendingCount > 0

		// Composite bitmask & score
		var mask uint8
		if ackMissing {
			mask |= MaskAckSyncMissing
		}
		if danglingSSTCount > 0 {
			mask |= MaskDanglingTable
		}
		if unsyncedRename {
			mask |= MaskUnsyncedRename
		}

		score := 0.0
		if critCount > 0 {
			score += weights.RenameWeight * float64(critCount)
		}
		if pendingCount > critCount {
			score += 15.0 * float64(pendingCount-critCount)
		}
		if danglingSSTCount > 0 {
			score += weights.ManifestWeight*float64(danglingSSTCount) + 2.0*math.Log1p(float64(danglingSSTBytes))
		}
		if unpersistedOps > 0 {
			score += weights.WALWeight*float64(unpersistedOps) + 3.0*math.Log1p(float64(danglingWAL))
		}

		cuts[event.ID] = ContractFeatures{
			AckSyncMissing:          ackMissing,
			UnpersistedAckOps:       unpersistedOps,
			DanglingWALBytes:        danglingWAL,
			DanglingTableCount:      danglingSSTCount,
			DanglingTableBytes:      danglingSSTBytes,
			UnsyncedDirectoryRename: unsyncedRename,
			PendingRenames:          pendingCount,
			CriticalPendingRenames:  critCount,
			ContractViolationMask:   mask,
			ContractScore:           score,
		}
	}

	return cuts
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ExtractContractFeatures computes ContractFeatures for a single cutoff.
func ExtractContractFeatures(events []trace.Event, cut uint64) ContractFeatures {
	m := ComputeContractFeatures(events)
	return m[cut]
}

// ContractScore returns the composite contract violation score for a single cutoff.
func ContractScore(events []trace.Event, cut uint64) float64 {
	return ExtractContractFeatures(events, cut).ContractScore
}

// AckSyncMissing reports whether acknowledged synchronous writes have unpersisted WAL bytes,
// and returns the count of unpersisted acknowledged operations and dangling WAL byte volume.
func AckSyncMissing(events []trace.Event, cut uint64) (bool, uint64, uint64) {
	cf := ExtractContractFeatures(events, cut)
	return cf.AckSyncMissing, cf.UnpersistedAckOps, cf.DanglingWALBytes
}

// DanglingTableReferences reports the count and byte volume of SSTables referenced by manifest
// that remain non-durable at the given cutoff.
func DanglingTableReferences(events []trace.Event, cut uint64) (int, uint64) {
	cf := ExtractContractFeatures(events, cut)
	return cf.DanglingTableCount, cf.DanglingTableBytes
}

// UnsyncedDirectoryRename reports whether namespace renames lack an enclosing directory sync,
// and returns the total pending renames and critical pending renames.
func UnsyncedDirectoryRename(events []trace.Event, cut uint64) (bool, int, int) {
	cf := ExtractContractFeatures(events, cut)
	return cf.UnsyncedDirectoryRename, cf.PendingRenames, cf.CriticalPendingRenames
}

// ContractFeatures returns the contract features for cutoff cut from the view.
func (view *View) ContractFeatures(cut uint64) ContractFeatures {
	if view == nil || len(view.Events) == 0 {
		return ContractFeatures{}
	}
	m := view.AllContractFeatures()
	return m[cut]
}

// ContractScore returns the composite contract score for cutoff cut from the view.
func (view *View) ContractScore(cut uint64) float64 {
	return view.ContractFeatures(cut).ContractScore
}

// AllContractFeatures returns contract features for all cutoffs in the view.
func (view *View) AllContractFeatures() map[uint64]ContractFeatures {
	if view == nil || len(view.Events) == 0 {
		return nil
	}
	return ComputeContractFeatures(view.Events)
}
