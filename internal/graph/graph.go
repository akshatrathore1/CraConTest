// Package graph builds the persistence dependency graph and the per-cutoff
// persistence-state view of a trace.
//
// The graph is a ranking heuristic, not a durability oracle. Its vertices are
// the trace's persistence events (trace.Event.IsMutation). Edges always go
// from a lower to a higher event ID and come from three sources: the order of
// events on one resource (a file or directory object, independent of its
// names), the namespace order of each directory, and producer-to-consumer
// pairs observed in engine annotations. Nothing is inferred from file names.
//
// For every cutoff k in 0..len(events) the view holds Features computed from
// events 1..k only: which resources hold unsynced bytes, which directory
// entries are not yet made durable by a directory sync, and labels used by
// ranking strategies.
package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// Background is the operation label of a cutoff outside any client operation.
const Background = "background"

// Features are the trace-derived labels of one cutoff.
type Features struct {
	// Desc counts distinct later persistence vertices reachable from the
	// anchor (the vertex at the cutoff, or the closest one before it).
	Desc uint64
	// CrossFile counts those reachable vertices on a different resource.
	CrossFile uint64
	// Semantic is one when a mutating client interval, a non-empty write
	// category or an engine annotation labels the anchor, otherwise zero.
	Semantic uint64
	// StateKey is the hex SHA-256 of the canonical persistence state.
	StateKey string
	// OpKind is the active client operation, the completed operation's kind
	// at an OpEnd cutoff, or Background. It is an interval label, not proof
	// that a background job belongs to that operation.
	OpKind string
	// Categories is the sorted set of write categories of dirty or pending
	// resources, inherited from their opening events.
	Categories []string
	// BeforeBoundary reports trace.BeforeBoundary at this cutoff.
	BeforeBoundary bool

	// Reverse Risk Horizon:
	// DirtyBytes is the total unpersisted dirty byte volume across all resources.
	DirtyBytes uint64
	// UncommittedSyncOps is the count of acknowledged mutating client operations since the last WAL barrier.
	UncommittedSyncOps uint64
	// RiskScore is the composite uncommitted risk exposure at this cutoff.
	RiskScore float64

	// Barrier Proximity:
	// BarrierProximity is 1 / (1 + delta) to the nearest subsequent barrier (1.0 if immediately preceding).
	BarrierProximity float64
	// NextBarrier is the event ID of the nearest subsequent barrier (0 if none).
	NextBarrier uint64

	// 5-Tier Discriminative Semantics:
	// SemanticTier is 0..4 (4: Manifest, 3: WAL commit, 2: Directory, 1: Memtable flush, 0: Routine data).
	SemanticTier int
	// SemanticWeight is the corresponding weight (10.0, 8.0, 6.0, 2.0, 0.0).
	SemanticWeight float64
}

// View is the read-only result of Build. It owns copies of its inputs.
type View struct {
	Events []trace.Event
	Cuts   map[uint64]Features

	successors map[uint64][]uint64
	vertices   map[uint64]bool
	vertexOp   map[uint64]uint64

	contractOnce sync.Once
	contractFeat map[uint64]ContractFeatures
}

// Successors returns the sorted direct successors of a persistence vertex.
func (view *View) Successors(id uint64) []uint64 {
	return append([]uint64(nil), view.successors[id]...)
}

// IsVertex reports whether the event is a persistence vertex.
func (view *View) IsVertex(id uint64) bool { return view.vertices[id] }

// WriteDOT exports the dependency graph in Graphviz DOT format for inspection.
// Vertices are labeled with event Kind, File, and Op (SRS CCT-REQ-022).
func (view *View) WriteDOT(w io.Writer) error {
	if view == nil {
		return errors.New("graph: nil view")
	}

	writeLine := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}

	if err := writeLine("digraph G {\n"); err != nil {
		return err
	}
	if err := writeLine("  rankdir=LR;\n"); err != nil {
		return err
	}
	if err := writeLine("  node [shape=box, fontname=\"sans-serif\", fontsize=10];\n"); err != nil {
		return err
	}
	if err := writeLine("  edge [fontname=\"sans-serif\", fontsize=9];\n"); err != nil {
		return err
	}

	vIDs := make([]uint64, 0, len(view.vertices))
	for id := range view.vertices {
		vIDs = append(vIDs, id)
	}
	sort.Slice(vIDs, func(i, j int) bool { return vIDs[i] < vIDs[j] })

	for _, id := range vIDs {
		var kind, file, file2 string
		op := view.vertexOp[id]
		if id > 0 && int(id) <= len(view.Events) {
			ev := view.Events[id-1]
			kind = string(ev.Kind)
			file = ev.File
			file2 = ev.File2
			if op == 0 {
				op = ev.Op
			}
		}
		label := fmt.Sprintf("%d: %s", id, kind)
		if file != "" && file2 != "" {
			label += fmt.Sprintf("\n%s -> %s", file, file2)
		} else if file != "" {
			label += fmt.Sprintf("\n%s", file)
		}
		if op > 0 {
			label += fmt.Sprintf("\nop=%d", op)
		}
		if err := writeLine("  n%d [label=%q];\n", id, label); err != nil {
			return err
		}
	}

	for _, id := range vIDs {
		succs := view.successors[id]
		sortedSuccs := append([]uint64(nil), succs...)
		sort.Slice(sortedSuccs, func(i, j int) bool { return sortedSuccs[i] < sortedSuccs[j] })
		for _, s := range sortedSuccs {
			if err := writeLine("  n%d -> n%d;\n", id, s); err != nil {
				return err
			}
		}
	}

	return writeLine("}\n")
}

type interval struct{ start, end int64 }

type resource struct {
	id          uint64
	directory   bool
	openHandles int
	aliases     map[string]bool
	dirty       []interval
	categories  map[string]bool
}

type pendingEntry struct {
	event    uint64
	dir      string
	dirID    uint64
	path     string
	kind     string
	resource uint64
}

type builder struct {
	resources      map[uint64]*resource
	dirtyResources map[uint64]bool
	nextID         uint64
	names          map[string]uint64
	handles        map[uint64]uint64
	pending        []pendingEntry
	unknown        []uint64

	lastOn     map[uint64]uint64
	deferred   map[uint64][]uint64
	successors map[uint64]map[uint64]bool
	vertexRes  map[uint64]uint64
	vertexKind map[uint64]string
	vertexCats map[uint64]bool
	vertexOp   map[uint64]uint64
	firstNamed map[uint64]uint64
	order      []uint64

	activeKind string
	activeOp   uint64
	stateDirty bool
}

// Build validates the trace and returns its graph view.
func Build(input []trace.Event) (*View, error) {
	if err := trace.Validate(input); err != nil {
		return nil, fmt.Errorf("graph: %w", err)
	}
	events := trace.Clone(input)
	b := newBuilder()
	type partial struct {
		stateKey       string
		categories     []string
		opKind         string
		anchor         uint64
		anchorTier     int
		anchorWeight   float64
		barrierTier    int
		barrierWeight  float64
		dirtyBytes     uint64
		uncommittedOps uint64
		riskScore      float64
	}
	parts := make([]partial, len(events)+1)
	prevKey, prevCats, err := b.stateKey()
	if err != nil {
		return nil, err
	}
	parts[0] = partial{stateKey: prevKey, categories: prevCats, opKind: Background}
	var anchor uint64
	var anchorTier int
	var anchorWeight float64
	var prevDirtyBytes uint64
	var prevDirtyRisk, prevPendingRisk, prevRiskScore float64
	// uncommittedMutatingOps counts acknowledged mutating client operations since
	// the last WAL durability barrier.
	var uncommittedMutatingOps uint64
	for _, event := range events {
		if event.IsBarrier() || (event.Kind == trace.KindRename && event.Err == "") {
			var br *resource
			if event.Kind == trace.KindRename {
				if id, ok := b.names[event.File]; ok {
					br = b.resources[id]
				}
			} else if id, ok := b.handles[event.Handle]; ok {
				br = b.resources[id]
			}
			if br != nil {
				bt, bw := ResourceTier(br.categories, br.aliases, br.directory)
				parts[event.ID-1].barrierTier = bt
				parts[event.ID-1].barrierWeight = bw
			}
		}

		prevOps := uncommittedMutatingOps
		if event.Kind == trace.KindOpEnd && trace.MutatingOperation(event.OpKind) && event.Err == "" {
			uncommittedMutatingOps++
		}
		b.stateDirty = false
		opKind, err := b.apply(event)
		if err != nil {
			return nil, err
		}
		if event.IsBarrier() && !event.Dir {
			if resID := b.vertexRes[event.ID]; resID != 0 {
				if r := b.resources[resID]; r != nil && isWALResource(r) {
					uncommittedMutatingOps = 0
				}
			}
		}
		if b.isVertex(event) {
			anchor = event.ID
		}
		if anchor != 0 {
			anchorTier, anchorWeight = TierData, 0.0
			if resID := b.vertexRes[anchor]; resID != 0 {
				if r := b.resources[resID]; r != nil {
					anchorTier, anchorWeight = ResourceTier(r.categories, r.aliases, r.directory)
				}
			}
		}
		var cats []string
		if b.stateDirty {
			prevKey, prevCats, err = b.stateKey()
			if err != nil {
				return nil, err
			}
			prevDirtyBytes, prevDirtyRisk, prevPendingRisk = computeRiskHorizonParts(b.resources, b.dirtyResources, b.pending)
			prevRiskScore = prevDirtyRisk + prevPendingRisk + 4.0*float64(uncommittedMutatingOps)
			cats = prevCats
		} else {
			if len(prevCats) > 0 {
				cats = append([]string(nil), prevCats...)
			} else {
				cats = []string{}
			}
			if uncommittedMutatingOps != prevOps {
				prevRiskScore = prevDirtyRisk + prevPendingRisk + 4.0*float64(uncommittedMutatingOps)
			}
		}
		parts[event.ID] = partial{
			stateKey:       prevKey,
			categories:     cats,
			opKind:         opKind,
			anchor:         anchor,
			anchorTier:     anchorTier,
			anchorWeight:   anchorWeight,
			dirtyBytes:     prevDirtyBytes,
			uncommittedOps: uncommittedMutatingOps,
			riskScore:      prevRiskScore,
		}
	}

	barriers := extractBarriers(events)
	desc, cross := b.reachability()
	view := &View{
		Events:     events,
		Cuts:       make(map[uint64]Features, len(events)+1),
		successors: make(map[uint64][]uint64, len(b.successors)),
		vertices:   make(map[uint64]bool, len(b.order)),
		vertexOp:   b.vertexOp,
	}
	for _, vertex := range b.order {
		view.vertices[vertex] = true
		list := make([]uint64, 0, len(b.successors[vertex]))
		for successor := range b.successors[vertex] {
			list = append(list, successor)
		}
		sort.Slice(list, func(left, right int) bool { return list[left] < list[right] })
		view.successors[vertex] = list
	}
	bIdx := 0
	for cut := uint64(0); cut <= uint64(len(events)); cut++ {
		part := parts[cut]
		for bIdx < len(barriers) && barriers[bIdx] <= cut {
			bIdx++
		}
		var prox float64
		var nextBarrier uint64
		if bIdx < len(barriers) {
			nextBarrier = barriers[bIdx]
			delta := (nextBarrier - 1) - cut
			prox = 1.0 / (1.0 + float64(delta))
		}
		tier, semWeight := cutoffTierFromCausal(events, cut, part.anchor, nextBarrier, part.anchorTier, part.anchorWeight, part.barrierTier, part.barrierWeight)
		features := Features{
			StateKey:           part.stateKey,
			OpKind:             part.opKind,
			Categories:         part.categories,
			BeforeBoundary:     trace.BeforeBoundary(events, cut),
			DirtyBytes:         part.dirtyBytes,
			UncommittedSyncOps: part.uncommittedOps,
			RiskScore:          part.riskScore,
			BarrierProximity:   prox,
			NextBarrier:        nextBarrier,
			SemanticTier:       tier,
			SemanticWeight:     semWeight,
		}
		if part.anchor != 0 {
			features.Desc = desc[part.anchor]
			features.CrossFile = cross[part.anchor]
			res := b.vertexRes[part.anchor]
			named, seen := b.firstNamed[res]
			if trace.MutatingOperation(b.vertexKind[part.anchor]) || b.vertexCats[part.anchor] ||
				(res != 0 && seen && named <= cut) {
				features.Semantic = 1
			}
		}
		view.Cuts[cut] = features
	}
	return view, nil
}

func newBuilder() *builder {
	return &builder{
		resources:      make(map[uint64]*resource),
		dirtyResources: make(map[uint64]bool),
		names:          make(map[string]uint64),
		handles:        make(map[uint64]uint64),
		lastOn:         make(map[uint64]uint64),
		deferred:       make(map[uint64][]uint64),
		successors:     make(map[uint64]map[uint64]bool),
		vertexRes:      make(map[uint64]uint64),
		vertexKind:     make(map[uint64]string),
		vertexCats:     make(map[uint64]bool),
		vertexOp:       make(map[uint64]uint64),
		firstNamed:     make(map[uint64]uint64),
	}
}

func (b *builder) isVertex(event trace.Event) bool { return event.IsMutation() }

func parent(name string) string {
	dir := path.Dir(name)
	if dir == "." || dir == "/" {
		return ""
	}
	return dir
}

func (b *builder) newResource(directory bool) *resource {
	b.nextID++
	r := &resource{id: b.nextID, directory: directory, aliases: make(map[string]bool), categories: make(map[string]bool)}
	b.resources[r.id] = r
	return r
}

// ensureDir returns the resource of a directory that is assumed to exist,
// creating an implicit resource (with no pending entry) the first time a
// pre-existing directory such as the root is referenced.
func (b *builder) ensureDir(name string) uint64 {
	if id, ok := b.names[name]; ok {
		return id
	}
	r := b.newResource(true)
	r.aliases[name] = true
	b.names[name] = r.id
	return r.id
}

func (b *builder) setName(name string, id uint64) {
	if old, ok := b.names[name]; ok && old != id {
		delete(b.resources[old].aliases, name)
		b.maybeClearDeadResource(old)
		b.stateDirty = true
	} else if !ok {
		b.stateDirty = true
	}
	b.names[name] = id
	b.resources[id].aliases[name] = true
}

func (b *builder) dropName(name string) {
	if old, ok := b.names[name]; ok {
		delete(b.resources[old].aliases, name)
		delete(b.names, name)
		b.maybeClearDeadResource(old)
		b.stateDirty = true
	}
}

func (b *builder) maybeClearDeadResource(id uint64) {
	r := b.resources[id]
	if r != nil && len(r.aliases) == 0 && r.openHandles == 0 && len(r.dirty) > 0 {
		r.dirty = nil
		delete(b.dirtyResources, id)
		b.stateDirty = true
	}
}

func (b *builder) bindHandle(handle, id uint64) {
	if handle == 0 || id == 0 {
		return
	}
	if r := b.resources[id]; r != nil {
		r.openHandles++
	}
	b.handles[handle] = id
}

func (b *builder) releaseHandle(handle uint64) {
	id, ok := b.handles[handle]
	if !ok {
		return
	}
	delete(b.handles, handle)
	if r := b.resources[id]; r != nil && r.openHandles > 0 {
		r.openHandles--
		b.maybeClearDeadResource(id)
	}
}

func (b *builder) addPending(event uint64, name, kind string, id uint64) {
	dir := parent(name)
	b.pending = append(b.pending, pendingEntry{event: event, dir: dir, dirID: b.ensureDir(dir), path: name, kind: kind, resource: id})
	b.stateDirty = true
}

func (b *builder) lookup(event trace.Event, name string) (uint64, error) {
	id, ok := b.names[name]
	if !ok {
		return 0, fmt.Errorf("graph: event %d: %s of %q, which the trace never created", event.ID, event.Kind, name)
	}
	return id, nil
}

func (b *builder) addEdge(from, to uint64) {
	if from == 0 || from >= to {
		return
	}
	if b.successors[from] == nil {
		b.successors[from] = make(map[uint64]bool)
	}
	b.successors[from][to] = true
}

// touch records vertex as the latest event on each resource, adding an edge
// from the previous event on that resource and resolving deferred
// engine-annotation edges whose consumer is this resource.
func (b *builder) touch(vertex uint64, ids ...uint64) {
	done := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		if id == 0 || done[id] {
			continue
		}
		done[id] = true
		b.addEdge(b.lastOn[id], vertex)
		for _, producer := range b.deferred[id] {
			b.addEdge(producer, vertex)
		}
		delete(b.deferred, id)
		b.lastOn[id] = vertex
	}
}

func (b *builder) markVertex(event trace.Event, primary uint64, others ...uint64) {
	b.order = append(b.order, event.ID)
	b.vertexRes[event.ID] = primary
	if b.activeOp > 0 {
		b.vertexOp[event.ID] = b.activeOp
	}
	if b.activeKind != "" {
		b.vertexKind[event.ID] = b.activeKind
	} else {
		b.vertexKind[event.ID] = Background
	}
	if primary != 0 && len(b.resources[primary].categories) > 0 {
		b.vertexCats[event.ID] = true
	}
	b.touch(event.ID, append([]uint64{primary}, others...)...)
}

func (b *builder) failedNamespace(event trace.Event) {
	b.unknown = append(b.unknown, event.ID)
	b.stateDirty = true
	// Order a failed call after earlier events on the parents the trace
	// actually knows; never invent a directory a failed call names.
	var parents []uint64
	for _, name := range []string{event.File, event.File2} {
		if name == "" {
			continue
		}
		if id, ok := b.names[parent(name)]; ok {
			parents = append(parents, id)
		}
	}
	if len(parents) == 0 {
		parents = append(parents, b.ensureDir(""))
	}
	b.markVertex(event, parents[0], parents[1:]...)
}

func (b *builder) addCategory(id uint64, category string) {
	if category != "" {
		r := b.resources[id]
		if !r.categories[category] {
			r.categories[category] = true
			b.stateDirty = true
		}
	}
}

// create makes a new file resource at name, replacing any previous one.
func (b *builder) create(event trace.Event, name string) uint64 {
	kind := "create"
	if _, exists := b.names[name]; exists {
		kind = "replace"
	}
	r := b.newResource(false)
	b.setName(name, r.id)
	b.addPending(event.ID, name, kind, r.id)
	return r.id
}

// renameNames moves name (and, for a directory, every name below it) to
// destination, keeping resource identity.
func (b *builder) renameNames(from, to string) {
	type move struct{ oldName, newName string }
	var moves []move
	for name := range b.names {
		if name == from {
			moves = append(moves, move{name, to})
		} else if strings.HasPrefix(name, from+"/") {
			moves = append(moves, move{name, to + strings.TrimPrefix(name, from)})
		}
	}
	sort.Slice(moves, func(left, right int) bool { return moves[left].oldName < moves[right].oldName })
	ids := make([]uint64, len(moves))
	for index, item := range moves {
		id := b.names[item.oldName]
		ids[index] = id
		delete(b.resources[id].aliases, item.oldName)
		delete(b.names, item.oldName)
	}
	for index, item := range moves {
		b.setName(item.newName, ids[index])
	}
	if len(moves) > 0 {
		b.stateDirty = true
	}
}

// apply processes one event and returns the cutoff's operation label.
func (b *builder) apply(event trace.Event) (string, error) {
	opKind := ""
	switch event.Kind {
	case trace.KindOpBegin:
		b.activeOp = event.Op
		b.activeKind = event.OpKind
	case trace.KindOpEnd:
		opKind = event.OpKind
		b.activeOp = 0
		b.activeKind = ""
	case trace.KindEngineEvent:
		b.engineEvent(event)
	case trace.KindCreate, trace.KindLock:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		id := b.create(event, event.File)
		b.addCategory(id, event.Cat)
		b.bindHandle(event.Handle, id)
		b.markVertex(event, id, b.ensureDir(parent(event.File)))
	case trace.KindOpenReadWrite:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		var id uint64
		others := []uint64{}
		if event.Created {
			id = b.create(event, event.File)
			others = append(others, b.ensureDir(parent(event.File)))
		} else {
			var err error
			if id, err = b.lookup(event, event.File); err != nil {
				return "", err
			}
		}
		b.addCategory(id, event.Cat)
		b.bindHandle(event.Handle, id)
		b.markVertex(event, id, others...)
	case trace.KindReuseForWrite:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		id, err := b.lookup(event, event.File)
		if err != nil {
			return "", err
		}
		if r := b.resources[id]; r != nil && len(r.dirty) > 0 {
			r.dirty = nil
			delete(b.dirtyResources, id)
			b.stateDirty = true
		}
		b.renameNames(event.File, event.File2)
		b.addPending(event.ID, event.File, "rename_from", id)
		b.addPending(event.ID, event.File2, "rename_to", id)
		b.addCategory(id, event.Cat)
		b.bindHandle(event.Handle, id)
		b.markVertex(event, id, b.ensureDir(parent(event.File)), b.ensureDir(parent(event.File2)))
	case trace.KindOpen, trace.KindOpenDir:
		if event.Err != "" {
			break
		}
		var id uint64
		if event.Dir {
			id = b.ensureDir(event.File)
		} else {
			var err error
			if id, err = b.lookup(event, event.File); err != nil {
				return "", err
			}
		}
		b.bindHandle(event.Handle, id)
	case trace.KindLink:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		id, err := b.lookup(event, event.File)
		if err != nil {
			return "", err
		}
		b.setName(event.File2, id)
		b.addPending(event.ID, event.File2, "link", id)
		b.markVertex(event, id, b.ensureDir(parent(event.File2)))
	case trace.KindRename:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		id, err := b.lookup(event, event.File)
		if err != nil {
			return "", err
		}
		b.renameNames(event.File, event.File2)
		b.addPending(event.ID, event.File, "rename_from", id)
		b.addPending(event.ID, event.File2, "rename_to", id)
		b.markVertex(event, id, b.ensureDir(parent(event.File)), b.ensureDir(parent(event.File2)))
	case trace.KindRemove:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		id, err := b.lookup(event, event.File)
		if err != nil {
			return "", err
		}
		b.dropName(event.File)
		b.addPending(event.ID, event.File, "remove", id)
		b.markVertex(event, id, b.ensureDir(parent(event.File)))
	case trace.KindRemoveAll:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		var removed []string
		isRoot := event.File == "" || event.File == "."
		prefix := strings.TrimSuffix(event.File, "/") + "/"
		for name := range b.names {
			if (isRoot && name != "") || (!isRoot && (name == event.File || strings.HasPrefix(name, prefix))) {
				removed = append(removed, name)
			}
		}
		sort.Strings(removed)
		type removalItem struct {
			name  string
			id    uint64
			dirID uint64
		}
		items := make([]removalItem, 0, len(removed))
		for _, name := range removed {
			items = append(items, removalItem{
				name:  name,
				id:    b.names[name],
				dirID: b.ensureDir(parent(name)),
			})
		}
		var parentID uint64
		if isRoot {
			parentID = b.ensureDir("")
		} else if len(items) > 0 {
			parentID = b.ensureDir(parent(event.File))
		} else if id, ok := b.names[parent(event.File)]; ok {
			parentID = id
		} else {
			parentID = b.ensureDir("")
		}
		touched := []uint64{parentID}
		for _, item := range items {
			b.dropName(item.name)
			b.pending = append(b.pending, pendingEntry{
				event:    event.ID,
				dir:      parent(item.name),
				dirID:    item.dirID,
				path:     item.name,
				kind:     "remove",
				resource: item.id,
			})
			b.stateDirty = true
			touched = append(touched, item.id, item.dirID)
		}
		b.markVertex(event, touched[0], touched[1:]...)
	case trace.KindMkdirAll:
		if event.Err != "" {
			b.failedNamespace(event)
			break
		}
		primary := b.ensureDir("")
		var touched []uint64
		if event.File != "" {
			parts := strings.Split(event.File, "/")
			for index := range parts {
				name := strings.Join(parts[:index+1], "/")
				id, exists := b.names[name]
				if !exists {
					r := b.newResource(true)
					b.setName(name, r.id)
					b.addPending(event.ID, name, "mkdir", r.id)
					id = r.id
					touched = append(touched, id, b.ensureDir(parent(name)))
				}
				primary = id
			}
		}
		b.markVertex(event, primary, touched...)
	case trace.KindWrite, trace.KindWriteAt:
		if event.Len <= 0 {
			break
		}
		id, ok := b.handles[event.Handle]
		if !ok || id == 0 {
			break
		}
		r := b.resources[id]
		if r == nil {
			break
		}
		r.dirty = addInterval(r.dirty, interval{event.Off, event.Off + int64(event.Len)})
		if len(r.dirty) > 0 {
			b.dirtyResources[id] = true
			b.stateDirty = true
		}
		b.markVertex(event, id)
	case trace.KindSync, trace.KindSyncData, trace.KindSyncTo:
		if event.Err != "" {
			return "", fmt.Errorf("graph: event %d: failed %s is not supported by the recording profile", event.ID, event.Kind)
		}
		if !event.IsBarrier() {
			break
		}
		id, ok := b.handles[event.Handle]
		if !ok || id == 0 {
			break
		}
		r := b.resources[id]
		if r == nil {
			break
		}
		if r.directory {
			// Allocate a new slice sized exactly to surviving entries so the old
			// backing array is not retained unnecessarily.
			count := 0
			for _, entry := range b.pending {
				if entry.dirID != id {
					count++
				}
			}
			if count != len(b.pending) {
				b.stateDirty = true
			}
			if count == 0 {
				b.pending = nil
			} else {
				kept := make([]pendingEntry, 0, count)
				for _, entry := range b.pending {
					if entry.dirID != id {
						kept = append(kept, entry)
					}
				}
				b.pending = kept
			}
		} else {
			if len(r.dirty) > 0 {
				r.dirty = nil
				delete(b.dirtyResources, id)
				b.stateDirty = true
			}
		}
		b.markVertex(event, id)
	case trace.KindClose, trace.KindUnlock:
		b.releaseHandle(event.Handle)
	case trace.KindPreallocate:
		if event.Size <= 0 {
			break
		}
		id, ok := b.handles[event.Handle]
		if event.Err != "" {
			b.unknown = append(b.unknown, event.ID)
			b.stateDirty = true
			if ok && id != 0 {
				b.markVertex(event, id)
			} else {
				b.markVertex(event, b.ensureDir(""))
			}
			break
		}
		if ok && id != 0 {
			b.markVertex(event, id)
		}
	case trace.KindRead:
	default:
		return "", fmt.Errorf("graph: event %d: unsupported kind %q", event.ID, event.Kind)
	}
	if opKind == "" {
		opKind = b.activeKind
	}
	if opKind == "" {
		opKind = Background
	}
	return opKind, nil
}

// engineEvent adds observed producer-to-consumer edges: from the latest
// vertex on each named input resource to the latest vertex on each named
// output resource, or to the next vertex on that output when the output has
// no later vertex yet. Paths the namespace does not know are ignored.
func (b *builder) engineEvent(event trace.Event) {
	resolve := func(names []string) []uint64 {
		var ids []uint64
		for _, name := range names {
			if id, ok := b.names[name]; ok {
				ids = append(ids, id)
				if _, seen := b.firstNamed[id]; !seen {
					b.firstNamed[id] = event.ID
				}
			}
		}
		return ids
	}
	inputs, outputs := resolve(event.Inputs), resolve(event.Outputs)
	for _, input := range inputs {
		producer := b.lastOn[input]
		if producer == 0 {
			continue
		}
		for _, output := range outputs {
			if output == input {
				continue
			}
			if consumer := b.lastOn[output]; consumer > producer {
				b.addEdge(producer, consumer)
			} else {
				b.deferred[output] = append(b.deferred[output], producer)
			}
		}
	}
}

func addInterval(list []interval, next interval) []interval {
	if next.end <= next.start {
		return list
	}
	merged := make([]interval, 0, len(list)+1)
	inserted := false
	for _, current := range list {
		switch {
		case current.end < next.start:
			merged = append(merged, current)
		case next.end < current.start:
			if !inserted {
				merged = append(merged, next)
				inserted = true
			}
			merged = append(merged, current)
		default:
			if current.start < next.start {
				next.start = current.start
			}
			if current.end > next.end {
				next.end = current.end
			}
		}
	}
	if !inserted {
		merged = append(merged, next)
	}
	return merged
}

type keyResource struct {
	ID      uint64     `json:"id"`
	Aliases []string   `json:"aliases"`
	Dirty   [][2]int64 `json:"dirty"`
}

type keyPending struct {
	Event    uint64 `json:"event"`
	Dir      string `json:"dir"`
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Resource uint64 `json:"resource"`
}

type keyState struct {
	Resources []keyResource `json:"resources"`
	Pending   []keyPending  `json:"pending"`
	Unknown   []uint64      `json:"unknown"`
}

// stateKey returns the hash of the current persistence state and the sorted
// categories of its dirty and pending resources.
func (b *builder) stateKey() (string, []string, error) {
	included := make(map[uint64]bool, len(b.dirtyResources)+len(b.pending))
	for id := range b.dirtyResources {
		if r := b.resources[id]; r != nil && len(r.dirty) > 0 {
			included[id] = true
		}
	}
	state := keyState{Resources: []keyResource{}, Pending: []keyPending{}, Unknown: append([]uint64{}, b.unknown...)}
	for _, entry := range b.pending {
		included[entry.resource] = true
		state.Pending = append(state.Pending, keyPending{entry.event, entry.dir, entry.path, entry.kind, entry.resource})
	}
	sort.Slice(state.Pending, func(left, right int) bool {
		l, r := state.Pending[left], state.Pending[right]
		if l.Event != r.Event {
			return l.Event < r.Event
		}
		if l.Dir != r.Dir {
			return l.Dir < r.Dir
		}
		if l.Path != r.Path {
			return l.Path < r.Path
		}
		return l.Kind < r.Kind
	})
	ids := make([]uint64, 0, len(included))
	for id := range included {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	categorySet := make(map[string]bool)
	for _, id := range ids {
		r := b.resources[id]
		entry := keyResource{ID: id, Aliases: []string{}, Dirty: [][2]int64{}}
		for alias := range r.aliases {
			entry.Aliases = append(entry.Aliases, alias)
		}
		sort.Strings(entry.Aliases)
		for _, span := range r.dirty {
			entry.Dirty = append(entry.Dirty, [2]int64{span.start, span.end})
		}
		state.Resources = append(state.Resources, entry)
		for category := range r.categories {
			categorySet[category] = true
		}
	}
	sort.Slice(state.Unknown, func(left, right int) bool { return state.Unknown[left] < state.Unknown[right] })
	encoded, err := json.Marshal(state)
	if err != nil {
		return "", nil, fmt.Errorf("graph: state key encoding: %w", err)
	}
	digest := sha256.Sum256(encoded)
	categories := make([]string, 0, len(categorySet))
	for category := range categorySet {
		categories = append(categories, category)
	}
	sort.Strings(categories)
	return hex.EncodeToString(digest[:]), categories, nil
}

// reachability computes, for every vertex, the exact number of distinct
// reachable later vertices and how many of them lie on another resource.
func (b *builder) reachability() (map[uint64]uint64, map[uint64]uint64) {
	count := len(b.order)
	desc := make(map[uint64]uint64, count)
	cross := make(map[uint64]uint64, count)
	if count == 0 {
		return desc, cross
	}
	index := make(map[uint64]int, count)
	for position, vertex := range b.order {
		index[vertex] = position
	}
	words := (count + 63) / 64
	flatReach := make([]uint64, count*words)
	masks := make(map[uint64][]uint64)
	for position, vertex := range b.order {
		res := b.vertexRes[vertex]
		if masks[res] == nil {
			masks[res] = make([]uint64, words)
		}
		masks[res][position/64] |= 1 << (position % 64)
	}
	for position := count - 1; position >= 0; position-- {
		vertex := b.order[position]
		set := flatReach[position*words : (position+1)*words]
		for successor := range b.successors[vertex] {
			next := index[successor]
			set[next/64] |= 1 << (next % 64)
			nextReach := flatReach[next*words : (next+1)*words]
			for word, value := range nextReach {
				set[word] |= value
			}
		}
		var total, same int
		mask := masks[b.vertexRes[vertex]]
		for word, value := range set {
			total += bits.OnesCount64(value)
			same += bits.OnesCount64(value & mask[word])
		}
		desc[vertex] = uint64(total)
		cross[vertex] = uint64(total - same)
	}
	return desc, cross
}
