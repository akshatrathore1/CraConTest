package graph

import (
	"maps"
	"math"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func pathBase(p string) string {
	if p == "" {
		return ""
	}
	return path.Base(p)
}

func isManifestBase(base string) bool {
	return strings.HasPrefix(base, "MANIFEST") || base == "CURRENT" || strings.HasPrefix(base, "CURRENT.") || strings.HasPrefix(base, "marker.manifest.")
}

func isManifestDataBase(base string) bool {
	return strings.HasPrefix(base, "MANIFEST") || base == "CURRENT"
}

func isWALFile(cat, p string) bool {
	return cat == "pebble-wal" || strings.HasSuffix(p, ".log")
}

func isSSTFile(cat, p string) bool {
	return cat == "pebble-compaction" || cat == "pebble-memtable-flush" || strings.HasSuffix(p, ".sst")
}

func isManifestDataFile(cat, p string) bool {
	base := pathBase(p)
	if strings.HasPrefix(base, "marker.manifest.") {
		return false
	}
	return cat == "pebble-manifest" || isManifestDataBase(base)
}

func isOptionsPath(p string) bool {
	return strings.HasPrefix(pathBase(p), "OPTIONS")
}

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
		isManifestBase(pathBase(event.File)) ||
		isManifestBase(pathBase(event.File2)) ||
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
		if isManifestBase(pathBase(alias)) {
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

func isWALResource(r *resource) bool {
	if r == nil || r.directory {
		return false
	}
	if r.categories["pebble-wal"] {
		return true
	}
	for alias := range r.aliases {
		if strings.HasSuffix(alias, ".log") {
			return true
		}
	}
	return false
}

func cutoffTierFromCausal(events []trace.Event, cut uint64, anchor uint64, nextBarrier uint64, anchorTier int, anchorWeight float64, barrierTier int, barrierWeight float64) (int, float64) {
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

	// Check causal resource state at cutoff.
	if anchorWeight > bestWeight {
		bestTier = anchorTier
		bestWeight = anchorWeight
	}

	// If cutoff immediately precedes a barrier, that barrier (and its resource) promotes the tier if higher.
	if nextBarrier > 0 && cut == nextBarrier-1 && int(nextBarrier) <= len(events) {
		t, w := EventTier(events[nextBarrier-1])
		if w > bestWeight {
			bestTier = t
			bestWeight = w
		}
		if barrierWeight > bestWeight {
			bestTier = barrierTier
			bestWeight = barrierWeight
		}
	}

	return bestTier, bestWeight
}

// CutoffTier computes the effective semantic tier and weight for a cutoff.
// If the cutoff immediately precedes a barrier, the tier of that enclosing
// barrier promotes the cutoff's tier if higher.
func CutoffTier(events []trace.Event, cut uint64, anchor uint64, nextBarrier uint64, r *resource) (int, float64) {
	var anchorTier int
	var anchorWeight float64
	if r != nil {
		anchorTier, anchorWeight = ResourceTier(r.categories, r.aliases, r.directory)
	}
	return cutoffTierFromCausal(events, cut, anchor, nextBarrier, anchorTier, anchorWeight, TierData, 0.0)
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
	idx := sort.Search(len(barriers), func(i int) bool { return barriers[i] > cut })
	if idx < len(barriers) {
		b := barriers[idx]
		delta := (b - 1) - cut
		proximity := 1.0 / (1.0 + float64(delta))
		return proximity, b
	}
	return 0.0, 0
}

func sumIntervals(spans []interval) uint64 {
	var total uint64
	for _, span := range spans {
		if span.end > span.start {
			total += uint64(span.end - span.start)
		}
	}
	return total
}

// computeResourceDirtyBytes calculates the unpersisted dirty byte count on a resource.
func computeResourceDirtyBytes(r *resource) uint64 {
	if r.directory {
		return 0
	}
	return sumIntervals(r.dirty)
}

// resourceCategoryWeight returns the discriminative weight for dirty byte volume on a resource.
func resourceCategoryWeight(r *resource) float64 {
	tier, w := ResourceTier(r.categories, r.aliases, r.directory)
	if tier == TierData {
		return 1.0
	}
	return w
}

// computeRiskHorizonParts calculates the dirty bytes, dirty byte risk, and pending risk
// components over only active dirty resources and pending entries.
func computeRiskHorizonParts(resources map[uint64]*resource, dirtyResources map[uint64]bool, pending []pendingEntry) (uint64, float64, float64) {
	var totalDirtyBytes uint64
	var dirtyRisk float64

	if len(dirtyResources) > 0 {
		ids := make([]uint64, 0, len(dirtyResources))
		for id := range dirtyResources {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		for _, id := range ids {
			r := resources[id]
			if r == nil || r.directory {
				continue
			}
			bytes := computeResourceDirtyBytes(r)
			if bytes > 0 {
				totalDirtyBytes += bytes
				w := resourceCategoryWeight(r)
				dirtyRisk += w * math.Log1p(float64(bytes))
			}
		}
	}

	pendingRisk := 6.0 * float64(len(pending))
	return totalDirtyBytes, dirtyRisk, pendingRisk
}

// computeRiskHorizon calculates the backward unpersisted dirty byte volume,
// pending namespace mutations, and active uncommitted mutating client exposure.
// Formula:
//
//	RiskScore = sum_{r in dirty} w_cat(r) * ln(1 + DirtyBytes(r)) + 6.0 * len(pending) + 4.0 * uncommittedMutatingOps
func computeRiskHorizon(resources map[uint64]*resource, pending []pendingEntry, uncommittedMutatingOps uint64) (uint64, float64) {
	dirtySet := make(map[uint64]bool)
	for id, r := range resources {
		if len(r.dirty) > 0 {
			dirtySet[id] = true
		}
	}
	totalDirtyBytes, dirtyRisk, pendingRisk := computeRiskHorizonParts(resources, dirtySet, pending)
	return totalDirtyBytes, dirtyRisk + pendingRisk + 4.0*float64(uncommittedMutatingOps)
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

// defaultDCGWeights holds the standard term weights calibrated from crash consistency models.
// Use DefaultDCGWeights() to obtain a copy; never reference this var directly from other packages.
var defaultDCGWeights = DCGWeights{
	WALWeight:      30.0,
	ManifestWeight: 40.0,
	RenameWeight:   50.0,
}

// DefaultDCGWeights returns the standard DCG term weights as a struct copy safe for caller modification.
func DefaultDCGWeights() DCGWeights {
	return defaultDCGWeights
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

	// Closed-WAL lifecycle indicators:
	// HasClosedDirtyLog reports whether any WAL file was closed while holding
	// unpersisted dirty bytes (i.e., closedWhileDirty == true) at this cutoff.
	HasClosedDirtyLog bool `json:"has_closed_dirty_log"`
	// DirtyLogBytes is the total byte volume of dirty spans still present on
	// WAL files that were closed before their data was synced.
	DirtyLogBytes uint64 `json:"dirty_log_bytes"`

	// UnsyncedDirEntryCount is the maximum count of unsynced directory entries (files with
	// dirEntrySynced==false plus unsyncedDirs entries) coexisting in the same parent directory
	// at this cutoff.
	UnsyncedDirEntryCount int `json:"unsynced_dir_entry_count"`
}

type trackInode struct {
	size             int64
	isWAL            bool
	isSST            bool
	isCompactionSST  bool
	isFlushSST       bool
	isManifest       bool
	isDir            bool
	hasSyncedData    bool
	dirtySpans       []interval
	dirtyBytes       uint64
	openHandles      int
	createdEvent     uint64
	closedEvent      uint64
	closedWhileDirty bool
	flushJob         uint64
	entries          map[*trackFileInfo]bool
}

type trackFileInfo struct {
	path                string
	parentDir           string
	dirEntrySynced      bool
	stagedInManifest    bool
	committedInManifest bool
	removed             bool
	inode               *trackInode
}

type trackHandle struct {
	inode *trackInode
	entry *trackFileInfo
}

type opWalWrite struct {
	entry *trackFileInfo
	inode *trackInode
	spans []interval
}

type renameRecord struct {
	from       string
	to         string
	isCritical bool
	dir        string
	srcDir     string
	dstSynced  bool
	srcSynced  bool
	eventID    uint64
	violating  bool
}

type contractCacheEntry struct {
	length  int
	hash1   uint64
	hash2   uint64
	weights DCGWeights
	cuts    map[uint64]ContractFeatures
}

var (
	contractCacheMu sync.Mutex
	contractCache   = make([]contractCacheEntry, 0, 16)
)

func traceFingerprint(events []trace.Event) (uint64, uint64) {
	h1 := uint64(14695981039346656037)
	h2 := uint64(0x9e3779b97f4a7c15)
	const prime1 = 1099511628211
	const prime2 = 0xbf58476d1ce4e5b9
	mixU64 := func(v uint64) {
		for k := 0; k < 8; k++ {
			b := v & 0xff
			h1 ^= b
			h1 *= prime1
			h2 ^= b ^ uint64(k+1)
			h2 *= prime2
			v >>= 8
		}
	}
	mixStr := func(s string) {
		mixU64(uint64(len(s)))
		for j := 0; j < len(s); j++ {
			b := uint64(s[j])
			h1 ^= b
			h1 *= prime1
			h2 ^= b
			h2 *= prime2
		}
	}
	mixU64(uint64(len(events)))
	for i := range events {
		ev := &events[i]
		mixU64(ev.ID)
		mixStr(string(ev.Kind))
		mixStr(ev.File)
		mixStr(ev.File2)
		mixStr(ev.Cat)
		mixStr(ev.Err)
		mixStr(ev.Note)
		mixStr(ev.OpKind)
		mixU64(uint64(len(ev.Data)))
		mixU64(uint64(len(ev.Inputs)))
		for _, in := range ev.Inputs {
			mixStr(in)
		}
		mixU64(uint64(len(ev.Outputs)))
		for _, out := range ev.Outputs {
			mixStr(out)
		}
		mixU64(uint64(ev.Mode))
		mixU64(uint64(ev.Off))
		mixU64(uint64(ev.Len))
		mixU64(ev.Handle)
		mixU64(ev.Op)
		mixU64(ev.Job)
		mixU64(uint64(ev.Size))
		var flags uint64
		if ev.Dir {
			flags |= 1
		}
		if ev.FullSync {
			flags |= 2
		}
		if ev.Created {
			flags |= 4
		}
		mixU64(flags)
	}
	return h1, h2
}

// ComputeContractFeatures computes ContractFeatures for every cutoff 0..len(events) using DefaultDCGWeights.
func ComputeContractFeatures(events []trace.Event) map[uint64]ContractFeatures {
	return ComputeContractFeaturesWithWeights(events, DefaultDCGWeights())
}

// ComputeContractFeaturesWithWeights computes ContractFeatures for every cutoff 0..len(events) using custom weights.
func ComputeContractFeaturesWithWeights(events []trace.Event, weights DCGWeights) map[uint64]ContractFeatures {
	if len(events) == 0 {
		return computeContractFeaturesWithWeightsUncached(events, weights)
	}

	length := len(events)
	fp1, fp2 := traceFingerprint(events)

	contractCacheMu.Lock()
	for i, entry := range contractCache {
		if entry.length == length && entry.hash1 == fp1 && entry.hash2 == fp2 && entry.weights == weights {
			res := maps.Clone(entry.cuts)
			if i > 0 {
				copy(contractCache[1:i+1], contractCache[0:i])
				contractCache[0] = entry
			}
			contractCacheMu.Unlock()
			return res
		}
	}
	contractCacheMu.Unlock()

	cuts := computeContractFeaturesWithWeightsUncached(events, weights)

	contractCacheMu.Lock()
	for i, existing := range contractCache {
		if existing.length == length && existing.hash1 == fp1 && existing.hash2 == fp2 && existing.weights == weights {
			if i > 0 {
				copy(contractCache[1:i+1], contractCache[0:i])
				contractCache[0] = existing
			}
			contractCacheMu.Unlock()
			return cuts
		}
	}
	newEntry := contractCacheEntry{
		length:  length,
		hash1:   fp1,
		hash2:   fp2,
		weights: weights,
		cuts:    maps.Clone(cuts),
	}
	if len(contractCache) < 16 {
		contractCache = append([]contractCacheEntry{newEntry}, contractCache...)
	} else {
		copy(contractCache[1:], contractCache[:15])
		contractCache[0] = newEntry
	}
	contractCacheMu.Unlock()

	return cuts
}

func newTrackInode(cat, p string, isDir bool, createdEvent uint64) *trackInode {
	return &trackInode{
		isWAL:           !isDir && isWALFile(cat, p),
		isSST:           !isDir && isSSTFile(cat, p),
		isCompactionSST: !isDir && cat == "pebble-compaction",
		isFlushSST:      !isDir && cat == "pebble-memtable-flush",
		isManifest:      !isDir && isManifestDataFile(cat, p),
		isDir:           isDir,
		createdEvent:    createdEvent,
		entries:         make(map[*trackFileInfo]bool),
	}
}

func applyFileClassification(inode *trackInode, cat, p string) {
	if inode == nil || inode.isDir {
		return
	}
	if isWALFile(cat, p) {
		inode.isWAL = true
	}
	if cat == "pebble-compaction" {
		inode.isCompactionSST = true
	}
	if cat == "pebble-memtable-flush" {
		inode.isFlushSST = true
	}
	if isSSTFile(cat, p) {
		inode.isSST = true
	}
	if isManifestDataFile(cat, p) {
		inode.isManifest = true
	}
}

func computeContractFeaturesWithWeightsUncached(events []trace.Event, weights DCGWeights) map[uint64]ContractFeatures {
	cuts := make(map[uint64]ContractFeatures, len(events)+1)
	cuts[0] = ContractFeatures{}

	files := make(map[string]*trackFileInfo)
	handles := make(map[uint64]*trackHandle)
	opWrites := make(map[uint64][]opWalWrite)
	ackOps := make(map[uint64]bool)
	var hasAckSyncOp bool
	var currentOp uint64
	var currentOpHadSync bool
	var manifestDirtyUncommitted bool
	var fileRetiredWhileManifestDirty bool
	var walSyncedWhileManifestDirty bool
	var pendingRenames []renameRecord
	activeFlushes := make(map[uint64][]uint64) // job -> stack of FlushBegin event IDs
	activeCompactions := make(map[uint64]int)  // job -> active compaction count
	unsyncedDirs := make(map[string]string)    // dirPath -> parentDir

	detachEntry := func(entry *trackFileInfo) {
		if entry == nil {
			return
		}
		entry.removed = true
		if entry.inode != nil {
			delete(entry.inode.entries, entry)
		}
		if cur, ok := files[entry.path]; ok && cur == entry {
			delete(files, entry.path)
		}
	}

	attachNewEntry := func(p string, inode *trackInode, dirEntrySynced bool) *trackFileInfo {
		if old := files[p]; old != nil {
			detachEntry(old)
		}
		entry := &trackFileInfo{
			path:           p,
			parentDir:      parent(p),
			dirEntrySynced: dirEntrySynced,
			inode:          inode,
		}
		inode.entries[entry] = true
		files[p] = entry
		return entry
	}

	bindHandle := func(handle uint64, entry *trackFileInfo, inode *trackInode) {
		if handle == 0 || inode == nil {
			return
		}
		inode.openHandles++
		handles[handle] = &trackHandle{inode: inode, entry: entry}
	}

	syncInode := func(inode *trackInode) {
		if inode == nil {
			return
		}
		inode.dirtySpans = nil
		inode.dirtyBytes = 0
		inode.closedWhileDirty = false
		if inode.size > 0 {
			inode.hasSyncedData = true
		}
		if inode.isSST {
			for entry := range inode.entries {
				if entry.removed {
					continue
				}
				_, parentUnsynced := unsyncedDirs[entry.parentDir]
				if entry.dirEntrySynced && !parentUnsynced {
					entry.stagedInManifest = false
					entry.committedInManifest = false
				}
			}
		}
	}

	// healClosedWALsIfFlushDurable clears durability-violation markers on closed
	// WAL files covered by a completed/synced memtable flush (closedEvent < maxClosedEvent).
	healClosedWALsIfFlushDurable := func(job uint64, maxClosedEvent uint64) {
		for _, f := range files {
			in := f.inode
			isRelevantFlushSST := in.isSST && !in.isCompactionSST && (job == 0 || in.flushJob == 0 || in.flushJob == job)
			if isRelevantFlushSST || in.isManifest {
				_, parentUnsynced := unsyncedDirs[f.parentDir]
				if in.dirtyBytes > 0 || len(in.dirtySpans) > 0 || !f.dirEntrySynced || parentUnsynced {
					return
				}
			}
		}
		for _, f := range files {
			in := f.inode
			if in.isWAL && in.closedWhileDirty && in.closedEvent < maxClosedEvent {
				in.dirtySpans = nil
				in.dirtyBytes = 0
				in.closedWhileDirty = false
			}
		}
	}

	prevCut := uint64(0)
	for idx, event := range events {
		cutID := event.ID
		if cutID == 0 || cutID != uint64(idx+1) {
			cutID = uint64(idx + 1)
		}

		noOp := false
		switch event.Kind {
		case trace.KindOpBegin:
			if trace.MutatingOperation(event.OpKind) {
				currentOp = event.Op
				currentOpHadSync = false
			}
		case trace.KindCreate:
			if event.Err != "" {
				noOp = true
				break
			}
			inode := newTrackInode(event.Cat, event.File, false, event.ID)
			fi := attachNewEntry(event.File, inode, false)
			bindHandle(event.Handle, fi, inode)
		case trace.KindReuseForWrite:
			if event.Err != "" {
				noOp = true
				break
			}
			// Record if a .log or .sst file is recycled while any MANIFEST file is dirty.
			if strings.HasSuffix(event.File, ".log") || strings.HasSuffix(event.File, ".sst") {
				for _, mf := range files {
					if mf.inode.isManifest && (mf.inode.dirtyBytes > 0 || !mf.dirEntrySynced) {
						fileRetiredWhileManifestDirty = true
						break
					}
				}
			}
			if oldFi := files[event.File]; oldFi != nil {
				detachEntry(oldFi)
			}
			inode := newTrackInode(event.Cat, event.File2, false, event.ID)
			fi := attachNewEntry(event.File2, inode, false)
			bindHandle(event.Handle, fi, inode)
		case trace.KindOpen, trace.KindOpenReadWrite, trace.KindOpenDir:
			if event.Err != "" {
				noOp = true
				break
			}
			fi := files[event.File]
			createdNew := false
			if fi == nil {
				createdNew = true
				inode := newTrackInode(event.Cat, event.File, event.Dir || event.Kind == trace.KindOpenDir, event.ID)
				fi = attachNewEntry(event.File, inode, !event.Created)
			} else {
				if event.Created {
					createdNew = true
					fi.dirEntrySynced = false
				}
				if event.Kind == trace.KindOpenReadWrite && event.Cat != "" {
					applyFileClassification(fi.inode, event.Cat, event.File)
				}
			}
			bindHandle(event.Handle, fi, fi.inode)
			if !createdNew && (event.Kind == trace.KindOpen || event.Kind == trace.KindOpenDir) {
				noOp = true
			}
		case trace.KindLock, trace.KindUnlock, trace.KindPreallocate, trace.KindRead:
			noOp = true
		case trace.KindWrite, trace.KindWriteAt:
			if event.Len <= 0 || event.Off < 0 || event.Off > math.MaxInt64-int64(event.Len) {
				noOp = true
				break
			}
			var fi *trackFileInfo
			var inode *trackInode
			if h := handles[event.Handle]; h != nil {
				fi = h.entry
				inode = h.inode
			}
			if inode == nil && event.File != "" {
				if fi = files[event.File]; fi != nil {
					inode = fi.inode
				}
			}
			if inode == nil && event.File != "" {
				inode = newTrackInode(event.Cat, event.File, false, event.ID)
				fi = attachNewEntry(event.File, inode, true)
			}
			if inode != nil {
				span := interval{start: event.Off, end: event.Off + int64(event.Len)}
				inode.dirtySpans = addInterval(inode.dirtySpans, span)
				inode.dirtyBytes = sumIntervals(inode.dirtySpans)
				if span.end > inode.size {
					inode.size = span.end
				}
				if inode.isWAL && currentOp > 0 {
					merged := false
					writes := opWrites[currentOp]
					for i := range writes {
						if writes[i].entry == fi && writes[i].inode == inode {
							writes[i].spans = addInterval(writes[i].spans, span)
							merged = true
							break
						}
					}
					if !merged {
						opWrites[currentOp] = append(writes, opWalWrite{
							entry: fi,
							inode: inode,
							spans: []interval{span},
						})
					}
				}
				isMan := inode.isManifest || isManifestDataFile(event.Cat, event.File)
				if isMan {
					for _, sst := range files {
						_, parentUnsynced := unsyncedDirs[sst.parentDir]
						if sst.inode.isSST && (sst.inode.dirtyBytes > 0 || len(sst.inode.dirtySpans) > 0 || !sst.dirEntrySynced || parentUnsynced) {
							sst.stagedInManifest = true
						}
					}
				}
			}
		case trace.KindSync, trace.KindSyncData, trace.KindSyncTo:
			if event.Err != "" || !event.IsBarrier() {
				noOp = true
				break
			}
			if event.Dir {
				syncedDir := event.File
				if h := handles[event.Handle]; h != nil && h.entry != nil && h.entry.path != "" {
					syncedDir = h.entry.path
				}
				if syncedDir == "." || syncedDir == "/" {
					syncedDir = ""
				}
				var keptRenames []renameRecord
				for _, r := range pendingRenames {
					if r.dir == syncedDir {
						r.dstSynced = true
					}
					if r.srcDir == syncedDir {
						r.srcSynced = true
					}
					if !(r.dstSynced && r.srcSynced) {
						keptRenames = append(keptRenames, r)
					}
				}
				pendingRenames = keptRenames

				var newlySyncedDirs map[string]bool
				for dirPath, pdir := range unsyncedDirs {
					if pdir == syncedDir {
						delete(unsyncedDirs, dirPath)
						if newlySyncedDirs == nil {
							newlySyncedDirs = make(map[string]bool)
						}
						newlySyncedDirs[dirPath] = true
					}
				}

				for _, f := range files {
					if f.parentDir == syncedDir {
						f.dirEntrySynced = true
					}
					if f.parentDir == syncedDir || newlySyncedDirs[f.parentDir] {
						_, parentUnsynced := unsyncedDirs[f.parentDir]
						if f.inode.isSST && f.dirEntrySynced && !parentUnsynced && f.inode.dirtyBytes == 0 && len(f.inode.dirtySpans) == 0 {
							f.stagedInManifest = false
							f.committedInManifest = false
						}
					}
				}
			} else {
				var fi *trackFileInfo
				var inode *trackInode
				if h := handles[event.Handle]; h != nil {
					fi = h.entry
					inode = h.inode
				}
				if inode == nil && event.File != "" {
					if fi = files[event.File]; fi != nil {
						inode = fi.inode
					}
				}
				if currentOp > 0 && inode != nil && inode.isWAL {
					currentOpHadSync = true
				}
				isMan := (inode != nil && inode.isManifest) || isManifestDataFile(event.Cat, event.File)
				if isMan {
					hasFlush := len(activeFlushes) > 0
					hasCompaction := len(activeCompactions) > 0
					for _, sst := range files {
						if sst.inode.isSST && sst.stagedInManifest {
							if hasFlush && !hasCompaction {
								if sst.inode.isFlushSST || (!sst.inode.isCompactionSST && !sst.inode.isFlushSST) {
									sst.committedInManifest = true
								}
							} else if hasCompaction && !hasFlush {
								if sst.inode.isCompactionSST || (!sst.inode.isCompactionSST && !sst.inode.isFlushSST) {
									sst.committedInManifest = true
								}
							} else {
								sst.committedInManifest = true
							}
						}
					}
				}
				syncInode(inode)
				if isMan && len(activeFlushes) > 0 {
					for job, starts := range activeFlushes {
						if len(starts) > 0 {
							healClosedWALsIfFlushDurable(job, starts[0])
						}
					}
				}
				// Record if a WAL file is synced while any MANIFEST file has dirty bytes.
				if inode != nil && inode.isWAL {
					for _, mf := range files {
						if mf.inode.isManifest && mf.inode.dirtyBytes > 0 {
							walSyncedWhileManifestDirty = true
							break
						}
					}
				}
			}
		case trace.KindEngineEvent:
			switch event.Note {
			case "FlushBegin":
				activeFlushes[event.Job] = append(activeFlushes[event.Job], event.ID)
			case "TableCreated":
				for _, out := range event.Outputs {
					if fi := files[out]; fi != nil && fi.inode != nil {
						fi.inode.flushJob = event.Job
					}
				}
			case "FlushEnd":
				flushStart := event.ID
				if starts := activeFlushes[event.Job]; len(starts) > 0 {
					flushStart = starts[0]
					if len(starts) == 1 {
						delete(activeFlushes, event.Job)
					} else {
						activeFlushes[event.Job] = starts[1:]
					}
				}
				for _, sst := range files {
					if sst.inode.isSST && sst.inode.isFlushSST && sst.stagedInManifest &&
						(event.Job == 0 || sst.inode.flushJob == 0 || sst.inode.flushJob == event.Job) {
						sst.committedInManifest = true
					}
				}
				healClosedWALsIfFlushDurable(event.Job, flushStart)
			case "CompactionBegin":
				activeCompactions[event.Job]++
			case "CompactionEnd":
				if activeCompactions[event.Job] > 1 {
					activeCompactions[event.Job]--
				} else {
					delete(activeCompactions, event.Job)
				}
				for _, sst := range files {
					if sst.inode.isSST && sst.inode.isCompactionSST && sst.stagedInManifest {
						sst.committedInManifest = true
					}
				}
			}
		case trace.KindMkdirAll:
			if event.Err != "" || event.File == "" {
				noOp = true
				break
			}
			parts := strings.Split(path.Clean(event.File), "/")
			for idx := range parts {
				sub := strings.Join(parts[:idx+1], "/")
				if sub == "" || sub == "." {
					continue
				}
				pdir := parent(sub)
				if pdir == "." || pdir == "/" {
					pdir = ""
				}
				if _, exists := files[sub]; !exists {
					unsyncedDirs[sub] = pdir
				}
			}
		case trace.KindLink:
			if event.Err != "" {
				noOp = true
				break
			}
			if src := files[event.File]; src != nil && src.inode != nil {
				attachNewEntry(event.File2, src.inode, false)
			} else {
				inode := newTrackInode(event.Cat, event.File2, false, event.ID)
				attachNewEntry(event.File2, inode, false)
			}
		case trace.KindRename:
			if event.Err != "" {
				noOp = true
				break
			}
			srcPrefix := event.File + "/"
			type fileMove struct {
				oldPath string
				newPath string
				entry   *trackFileInfo
			}
			var childMoves []fileMove
			for p, entry := range files {
				if strings.HasPrefix(p, srcPrefix) {
					childMoves = append(childMoves, fileMove{
						oldPath: p,
						newPath: event.File2 + strings.TrimPrefix(p, event.File),
						entry:   entry,
					})
				}
			}
			type dirMove struct {
				oldDir string
				newDir string
			}
			var childDirMoves []dirMove
			for d := range unsyncedDirs {
				if strings.HasPrefix(d, srcPrefix) {
					childDirMoves = append(childDirMoves, dirMove{
						oldDir: d,
						newDir: event.File2 + strings.TrimPrefix(d, event.File),
					})
				}
			}

			_, wasUnsyncedDir := unsyncedDirs[event.File]
			delete(unsyncedDirs, event.File)
			delete(unsyncedDirs, event.File2)
			for _, dm := range childDirMoves {
				delete(unsyncedDirs, dm.oldDir)
			}
			for _, dm := range childDirMoves {
				unsyncedDirs[dm.newDir] = parent(dm.newDir)
			}

			srcFi := files[event.File]
			if dstFi := files[event.File2]; dstFi != nil && dstFi != srcFi {
				detachEntry(dstFi)
			}
			for _, cm := range childMoves {
				delete(files, cm.oldPath)
			}
			for _, cm := range childMoves {
				if existing := files[cm.newPath]; existing != nil && existing != cm.entry {
					detachEntry(existing)
				}
				cm.entry.path = cm.newPath
				cm.entry.parentDir = parent(cm.newPath)
				files[cm.newPath] = cm.entry
			}

			isDir := wasUnsyncedDir || len(childMoves) > 0 || len(childDirMoves) > 0
			if srcFi != nil {
				delete(files, event.File)
				srcFi.path = event.File2
				srcFi.parentDir = parent(event.File2)
				srcFi.dirEntrySynced = false
				if isDir {
					srcFi.inode.isDir = true
				}
				applyFileClassification(srcFi.inode, event.Cat, event.File2)
				files[event.File2] = srcFi
			} else {
				inode := newTrackInode(event.Cat, event.File2, isDir, event.ID)
				attachNewEntry(event.File2, inode, false)
			}

			for i := range pendingRenames {
				if pendingRenames[i].dir == event.File || strings.HasPrefix(pendingRenames[i].dir, srcPrefix) {
					pendingRenames[i].dir = event.File2 + strings.TrimPrefix(pendingRenames[i].dir, event.File)
				}
				if pendingRenames[i].srcDir == event.File || strings.HasPrefix(pendingRenames[i].srcDir, srcPrefix) {
					pendingRenames[i].srcDir = event.File2 + strings.TrimPrefix(pendingRenames[i].srcDir, event.File)
				}
			}

			srcDir := parent(event.File)
			dstDir := parent(event.File2)
			isCrit := isManifestBase(pathBase(event.File2)) || isOptionsPath(event.File2)
			pendingRenames = append(pendingRenames, renameRecord{
				from:       event.File,
				to:         event.File2,
				isCritical: isCrit,
				dir:        dstDir,
				srcDir:     srcDir,
				dstSynced:  false,
				srcSynced:  srcDir == dstDir,
				eventID:    event.ID,
				violating:  false,
			})
		case trace.KindRemove, trace.KindRemoveAll:
			if event.Err != "" {
				noOp = true
				break
			}
			// Record if a .log or .sst file is deleted while any MANIFEST file is dirty.
			if strings.HasSuffix(event.File, ".log") || strings.HasSuffix(event.File, ".sst") {
				for _, mf := range files {
					if mf.inode.isManifest && (mf.inode.dirtyBytes > 0 || !mf.dirEntrySynced) {
						fileRetiredWhileManifestDirty = true
						break
					}
				}
			}
			if removedFi := files[event.File]; removedFi != nil {
				detachEntry(removedFi)
			}
			delete(unsyncedDirs, event.File)
			if event.Kind == trace.KindRemoveAll {
				cleanFile := path.Clean(event.File)
				isRoot := event.File == "" || cleanFile == "."
				prefix := strings.TrimSuffix(cleanFile, "/") + "/"
				for childPath, childFi := range files {
					if isRoot || childPath == cleanFile || strings.HasPrefix(childPath, prefix) {
						if strings.HasSuffix(childPath, ".log") || strings.HasSuffix(childPath, ".sst") {
							for _, mf := range files {
								if mf.inode.isManifest && (mf.inode.dirtyBytes > 0 || !mf.dirEntrySynced) {
									fileRetiredWhileManifestDirty = true
									break
								}
							}
						}
						detachEntry(childFi)
					}
				}
				for dirPath := range unsyncedDirs {
					if isRoot || dirPath == cleanFile || strings.HasPrefix(dirPath, prefix) {
						delete(unsyncedDirs, dirPath)
					}
				}
				var keptRenames []renameRecord
				for _, r := range pendingRenames {
					if isRoot || r.dir == cleanFile || strings.HasPrefix(r.dir, prefix) {
						r.dstSynced = true
					}
					if isRoot || r.srcDir == cleanFile || strings.HasPrefix(r.srcDir, prefix) {
						r.srcSynced = true
					}
					if !(r.dstSynced && r.srcSynced) {
						keptRenames = append(keptRenames, r)
					}
				}
				pendingRenames = keptRenames
			}
		case trace.KindClose:
			var inode *trackInode
			if h := handles[event.Handle]; h != nil {
				inode = h.inode
				delete(handles, event.Handle)
			} else if event.File != "" {
				if fi := files[event.File]; fi != nil {
					inode = fi.inode
				}
			}
			if inode != nil {
				if inode.openHandles > 0 {
					inode.openHandles--
				}
				if inode.openHandles == 0 && inode.dirtyBytes > 0 {
					inode.closedWhileDirty = true
					inode.closedEvent = event.ID
				}
			}
		case trace.KindOpEnd:
			if trace.MutatingOperation(event.OpKind) && event.Err == "" {
				if len(opWrites[event.Op]) > 0 {
					ackOps[event.Op] = true
				}
				if currentOpHadSync {
					hasAckSyncOp = true
				}
			}
			if event.Op == currentOp {
				currentOp = 0
				currentOpHadSync = false
			}
		}

		if noOp {
			cuts[cutID] = cuts[prevCut]
			prevCut = cutID
			continue
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

		// Single pass over files for closed dirty WALs, manifest uncommitted state,
		// dangling SST/Manifest counts, closed WAL indicators, and unsynced dir entry counts.
		hasClosedDirtyWAL := false
		hasDirtyManifest := false
		var hasClosedDirtyLog bool
		var dirtyLogBytes uint64
		seenClosedWALInodes := make(map[*trackInode]bool)
		parentUnsyncedCount := make(map[string]int)

		for _, f := range files {
			in := f.inode
			_, parentUnsynced := unsyncedDirs[f.parentDir]
			if in.isWAL && in.closedWhileDirty {
				hasClosedDirtyWAL = true
				hasClosedDirtyLog = true
				if !seenClosedWALInodes[in] {
					seenClosedWALInodes[in] = true
					dirtyLogBytes += in.dirtyBytes
				}
			}
			if in.isManifest && (in.dirtyBytes > 0 || !f.dirEntrySynced || parentUnsynced) {
				hasDirtyManifest = true
			}
			if !f.dirEntrySynced {
				parentUnsyncedCount[f.parentDir]++
			}
		}

		if hasDirtyManifest {
			isManifestBoundary := (event.Kind == trace.KindOpBegin && len(activeFlushes) == 0 && len(activeCompactions) == 0) ||
				(event.Kind == trace.KindEngineEvent && (event.Note == "FlushEnd" || event.Note == "CompactionEnd"))
			for _, f := range files {
				in := f.inode
				_, parentUnsynced := unsyncedDirs[f.parentDir]
				dirSynced := f.dirEntrySynced && !parentUnsynced
				if !in.isManifest || (in.dirtyBytes == 0 && dirSynced) {
					continue
				}
				// Case 1: Initial MANIFEST has 0 synced bytes on disk (!in.hasSyncedData).
				if in.dirtyBytes > 0 && !in.hasSyncedData && (dirSynced || isManifestBoundary) {
					manifestDirtyUncommitted = true
					break
				}
				// Case 2: Unsynced MANIFEST directory entry across a lifecycle boundary before any synced prefix exists.
				if !dirSynced && isManifestBoundary && !in.hasSyncedData {
					manifestDirtyUncommitted = true
					break
				}
				// Case 3: Subsequent MANIFEST write where a valid synced prefix already exists on disk (in.hasSyncedData).
				if in.dirtyBytes > 0 && in.hasSyncedData {
					if fileRetiredWhileManifestDirty || (hasClosedDirtyWAL && walSyncedWhileManifestDirty) || in.closedWhileDirty {
						manifestDirtyUncommitted = true
						break
					}
				}
				if in.closedWhileDirty {
					manifestDirtyUncommitted = true
					break
				}
			}
		} else {
			manifestDirtyUncommitted = false
			fileRetiredWhileManifestDirty = false
			walSyncedWhileManifestDirty = false
		}

		// Calculate contract snapshot at cutID:
		// 1. Indicator 1: AckSyncMissing
		var unpersistedOps uint64
		var danglingWAL uint64
		for opID := range ackOps {
			writes := opWrites[opID]
			if len(writes) == 0 {
				delete(ackOps, opID)
				delete(opWrites, opID)
				continue
			}
			opHasDirty := false
			var opDirtyBytes uint64
			allDoneForever := true
			for _, w := range writes {
				entry := w.entry
				inode := w.inode
				if entry == nil || entry.removed || inode == nil {
					continue
				}
				_, parentUnsynced := unsyncedDirs[entry.parentDir]
				if !entry.dirEntrySynced || parentUnsynced {
					opHasDirty = true
					for _, ws := range w.spans {
						opDirtyBytes += uint64(ws.end - ws.start)
					}
					allDoneForever = false
				} else if len(inode.dirtySpans) > 0 {
					allDoneForever = false
					for _, ws := range w.spans {
						for _, ds := range inode.dirtySpans {
							overlapStart := maxInt64(ws.start, ds.start)
							overlapEnd := minInt64(ws.end, ds.end)
							if overlapEnd > overlapStart {
								opHasDirty = true
								opDirtyBytes += uint64(overlapEnd - overlapStart)
							}
						}
					}
				} else if inode.openHandles > 0 {
					allDoneForever = false
				}
			}
			if allDoneForever {
				delete(ackOps, opID)
				delete(opWrites, opID)
				continue
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
			in := fi.inode
			_, parentUnsynced := unsyncedDirs[fi.parentDir]
			if in.isSST && (in.dirtyBytes > 0 || len(in.dirtySpans) > 0 || !fi.dirEntrySynced || parentUnsynced) {
				if fi.committedInManifest {
					danglingSSTCount++
					var vol uint64
					if !fi.dirEntrySynced || parentUnsynced {
						if in.size > 0 {
							vol = uint64(in.size)
						} else {
							vol = in.dirtyBytes
						}
					} else {
						vol = in.dirtyBytes
						if vol == 0 && in.size > 0 {
							vol = uint64(in.size)
						}
					}
					danglingSSTBytes += vol
				}
			} else if in.isManifest && (in.dirtyBytes > 0 || !fi.dirEntrySynced || parentUnsynced) {
				if in.closedWhileDirty || manifestDirtyUncommitted {
					danglingSSTCount++
					var vol uint64
					if !fi.dirEntrySynced || parentUnsynced {
						if in.size > 0 {
							vol = uint64(in.size)
						} else {
							vol = in.dirtyBytes
						}
					} else {
						vol = in.dirtyBytes
						if vol == 0 && in.size > 0 {
							vol = uint64(in.size)
						}
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
				isOptionsOrWAL := isOptionsPath(r.to) || strings.HasSuffix(r.to, ".log")
				if isOptionsOrWAL {
					// WAL/options directory hazards require at least one acknowledged sync operation
					if hasAckSyncOp {
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

		// Compute UnsyncedDirEntryCount: max unsynced dir entries in any single parentDir.
		for dirPath := range unsyncedDirs {
			pdir := parent(dirPath)
			parentUnsyncedCount[pdir]++
		}
		maxUnsyncedDirEntries := 0
		for _, cnt := range parentUnsyncedCount {
			if cnt > maxUnsyncedDirEntries {
				maxUnsyncedDirEntries = cnt
			}
		}

		cuts[cutID] = ContractFeatures{
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
			HasClosedDirtyLog:       hasClosedDirtyLog,
			DirtyLogBytes:           dirtyLogBytes,
			UnsyncedDirEntryCount:   maxUnsyncedDirEntries,
		}
		prevCut = cutID
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

func (view *View) ensureContractFeatures() map[uint64]ContractFeatures {
	view.contractOnce.Do(func() {
		view.contractFeat = ComputeContractFeatures(view.Events)
	})
	return view.contractFeat
}

// ContractFeatures returns the contract features for cutoff cut from the view.
func (view *View) ContractFeatures(cut uint64) ContractFeatures {
	if view == nil {
		return ContractFeatures{}
	}
	return view.ensureContractFeatures()[cut]
}

// ContractScore returns the composite contract score for cutoff cut from the view.
func (view *View) ContractScore(cut uint64) float64 {
	return view.ContractFeatures(cut).ContractScore
}

// AllContractFeatures returns a cloned map of contract features for all cutoffs in the view.
func (view *View) AllContractFeatures() map[uint64]ContractFeatures {
	if view == nil {
		return nil
	}
	return maps.Clone(view.ensureContractFeatures())
}
