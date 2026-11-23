// Package candidate defines crash candidates and the candidate universe.
//
// A candidate is the full tuple (cutoff event, retention percent, seed). The
// complete tuple is its identity: two candidates with equal event and percent
// but different seeds are distinct. A universe is the canonically ordered set
// of candidates that every strategy ranks and every outcome table covers.
package candidate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// Version is the schema version of universe documents.
const Version = 1

// seedDomain separates candidate seeds from any other use of SHA-256.
const seedDomain = "cracontest/candidate-seed/v1\x00"

// Candidate is one crash to test: replay events 1..Event, keep Percent of the
// unsynced data, and use Seed for the retention RNG.
type Candidate struct {
	Event   uint64 `json:"event"`
	Percent int    `json:"percent"`
	Seed    uint64 `json:"seed"`
}

// Document is the version-one wire format of a universe.
type Document struct {
	SchemaVersion int         `json:"schema_version"`
	Candidates    []Candidate `json:"candidates"`
}

// Less reports whether left precedes right in canonical order
// (Event, then Percent, then Seed).
func Less(left, right Candidate) bool {
	if left.Event != right.Event {
		return left.Event < right.Event
	}
	if left.Percent != right.Percent {
		return left.Percent < right.Percent
	}
	return left.Seed < right.Seed
}

// Sort orders candidates canonically in place.
func Sort(candidates []Candidate) {
	sort.Slice(candidates, func(left, right int) bool { return Less(candidates[left], candidates[right]) })
}

// DeriveSeed returns the deterministic retention seed for a tuple. It is the
// first eight bytes, read big-endian, of SHA-256 over the domain string, the
// big-endian base seed and event, and the percent as one byte.
func DeriveSeed(base, event uint64, percent int) (uint64, error) {
	if percent < 0 || percent > 100 {
		return 0, fmt.Errorf("candidate: percent %d outside 0..100", percent)
	}
	encoded := make([]byte, 0, len(seedDomain)+17)
	encoded = append(encoded, seedDomain...)
	encoded = binary.BigEndian.AppendUint64(encoded, base)
	encoded = binary.BigEndian.AppendUint64(encoded, event)
	encoded = append(encoded, byte(percent))
	digest := sha256.Sum256(encoded)
	return binary.BigEndian.Uint64(digest[:8]), nil
}

// Build returns the canonical universe for a trace: every cutoff chosen by
// trace.CutIDs combined with every policy, one derived seed per tuple. Any
// retention policy in 0..100 is supported.
func Build(events []trace.Event, base uint64, policies []int) ([]Candidate, error) {
	if len(policies) == 0 {
		return nil, errors.New("candidate: no retention policies")
	}
	seen := make(map[int]bool, len(policies))
	for _, policy := range policies {
		if policy < 0 || policy > 100 {
			return nil, fmt.Errorf("candidate: percent %d outside 0..100", policy)
		}
		if seen[policy] {
			return nil, fmt.Errorf("candidate: duplicate policy %d", policy)
		}
		seen[policy] = true
	}
	cuts, err := trace.CutIDs(events)
	if err != nil {
		return nil, fmt.Errorf("candidate: %w", err)
	}
	universe := make([]Candidate, 0, len(cuts)*len(policies))
	for _, cut := range cuts {
		for _, policy := range policies {
			seed, err := DeriveSeed(base, cut, policy)
			if err != nil {
				return nil, err
			}
			universe = append(universe, Candidate{Event: cut, Percent: policy, Seed: seed})
		}
	}
	Sort(universe)
	return universe, nil
}

// Validate checks a universe against its trace: the trace itself is valid,
// every cutoff lies in 0..len(events), every percent in 0..100, and no full
// tuple appears twice.
func Validate(events []trace.Event, universe []Candidate) error {
	if err := trace.Validate(events); err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	last := uint64(len(events))
	seen := make(map[Candidate]bool, len(universe))
	for index, candidate := range universe {
		if candidate.Event > last {
			return fmt.Errorf("candidate %d: cutoff %d beyond trace length %d", index, candidate.Event, last)
		}
		if err := checkPercent(candidate); err != nil {
			return fmt.Errorf("candidate %d: %w", index, err)
		}
		if seen[candidate] {
			return fmt.Errorf("candidate %d: duplicate tuple %+v", index, candidate)
		}
		seen[candidate] = true
	}
	return nil
}

func checkPercent(candidate Candidate) error {
	if candidate.Percent < 0 || candidate.Percent > 100 {
		return fmt.Errorf("percent %d outside 0..100", candidate.Percent)
	}
	return nil
}

// checkCanonical requires strictly increasing canonical order, which also
// rules out duplicates, and valid percents.
func checkCanonical(universe []Candidate) error {
	for index, candidate := range universe {
		if err := checkPercent(candidate); err != nil {
			return fmt.Errorf("candidate %d: %w", index, err)
		}
		if index > 0 && !Less(universe[index-1], candidate) {
			return fmt.Errorf("candidate %d: universe is not in strict canonical order", index)
		}
	}
	return nil
}

// Encode writes a version-one universe document. The universe must be in
// strict canonical order.
func Encode(writer io.Writer, universe []Candidate) error {
	if err := checkCanonical(universe); err != nil {
		return err
	}
	owned := append([]Candidate{}, universe...)
	return json.NewEncoder(writer).Encode(Document{SchemaVersion: Version, Candidates: owned})
}

// Decode reads a version-one universe document. It rejects unknown fields,
// other versions, a missing or null array, trailing data and non-canonical
// order. Cutoff range checks need the trace and belong to Validate.
func Decode(reader io.Reader) ([]Candidate, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var raw struct {
		SchemaVersion *int            `json:"schema_version"`
		Candidates    json.RawMessage `json:"candidates"`
	}
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	if raw.SchemaVersion == nil || *raw.SchemaVersion != Version {
		return nil, errors.New("decode universe: unsupported or missing schema_version")
	}
	if len(raw.Candidates) == 0 || bytes.Equal(bytes.TrimSpace(raw.Candidates), []byte("null")) {
		return nil, errors.New("decode universe: missing or null candidates array")
	}
	inner := json.NewDecoder(bytes.NewReader(raw.Candidates))
	inner.DisallowUnknownFields()
	var universe []Candidate
	if err := inner.Decode(&universe); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	if universe == nil {
		universe = []Candidate{}
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode universe: trailing data after document")
	}
	if err := checkCanonical(universe); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	return universe, nil
}

type canonicalFile struct {
	Path       string `json:"path"`
	DurableLen int64  `json:"durable_len"`
	CurrentLen int64  `json:"current_len,omitempty"`
}

type canonicalState struct {
	Percent int             `json:"percent"`
	Files   []canonicalFile `json:"files"`
	Pending []string        `json:"pending,omitempty"`
}

type fileEntry struct {
	path       string
	durableLen int64
	currentLen int64
	deleted    bool
	isDir      bool
	synced     bool
}

type pendingDirOp struct {
	dir  string
	desc string
}

type cutoffState struct {
	files   map[string]*fileEntry
	pending []pendingDirOp
}

func cloneFiles(src map[string]*fileEntry) map[string]*fileEntry {
	dst := make(map[string]*fileEntry, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}
	return dst
}

func clonePending(src []pendingDirOp) []pendingDirOp {
	if len(src) == 0 {
		return nil
	}
	dst := make([]pendingDirOp, len(src))
	copy(dst, src)
	return dst
}

func (s *cutoffState) Key(percent int) string {
	state := canonicalState{
		Percent: percent,
		Files:   []canonicalFile{},
	}
	if len(s.pending) > 0 {
		state.Pending = make([]string, 0, len(s.pending))
		for _, p := range s.pending {
			state.Pending = append(state.Pending, p.desc)
		}
		sort.Strings(state.Pending)
	}

	names := make([]string, 0, len(s.files))
	for name := range s.files {
		names = append(names, name)
	}
	sort.Strings(names)

	isPendingCreate := func(pending []pendingDirOp, p string) bool {
		for _, op := range pending {
			if op.desc == "mkdir:"+p ||
				op.desc == "create:"+p ||
				op.desc == "link:"+p ||
				op.desc == "rename_to:"+p ||
				strings.HasSuffix(op.desc, "->"+p) ||
				op.desc == "reuse_to:"+p {
				return true
			}
		}
		return false
	}

	for _, name := range names {
		f := s.files[name]
		if f.deleted {
			continue
		}
		if percent == 0 {
			// At 0% retention, unsynced directory entries and unsynced files do not survive crash.
			// A file or directory survives if its directory entry has been durably synced
			// (not in s.pending), or if the file itself has durable data / was synced.
			survives := false
			if f.isDir {
				survives = !isPendingCreate(s.pending, f.path)
			} else {
				if f.durableLen > 0 || f.synced {
					survives = true
				} else if !isPendingCreate(s.pending, f.path) {
					survives = true
				}
			}
			if survives {
				state.Files = append(state.Files, canonicalFile{
					Path:       f.path,
					DurableLen: f.durableLen,
				})
			}
		} else {
			state.Files = append(state.Files, canonicalFile{
				Path:       f.path,
				DurableLen: f.durableLen,
				CurrentLen: f.currentLen,
			})
		}
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Sprintf("fallback:%d:%d", state.Percent, len(state.Files))
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

type cutoffCacheEntry struct {
	length int
	hash   uint64
	states []*cutoffState
}

var (
	cutoffCacheMu sync.Mutex
	cutoffCache   = make([]cutoffCacheEntry, 0, 16)
)

func traceFingerprint(events []trace.Event) uint64 {
	h := uint64(14695981039346656037)
	const prime = 1099511628211
	for i := range events {
		h ^= uint64(events[i].ID)
		h *= prime
		for j := 0; j < len(events[i].Kind); j++ {
			h ^= uint64(events[i].Kind[j])
			h *= prime
		}
		for j := 0; j < len(events[i].File); j++ {
			h ^= uint64(events[i].File[j])
			h *= prime
		}
		for j := 0; j < len(events[i].File2); j++ {
			h ^= uint64(events[i].File2[j])
			h *= prime
		}
		for j := 0; j < len(events[i].Cat); j++ {
			h ^= uint64(events[i].Cat[j])
			h *= prime
		}
		for j := 0; j < len(events[i].Err); j++ {
			h ^= uint64(events[i].Err[j])
			h *= prime
		}
		for j := 0; j < len(events[i].Note); j++ {
			h ^= uint64(events[i].Note[j])
			h *= prime
		}
		for j := 0; j < len(events[i].OpKind); j++ {
			h ^= uint64(events[i].OpKind[j])
			h *= prime
		}
		h ^= uint64(events[i].Mode)
		h *= prime
		h ^= uint64(events[i].Off)
		h *= prime
		h ^= uint64(events[i].Len)
		h *= prime
		h ^= events[i].Handle
		h *= prime
		h ^= events[i].Op
		h *= prime
		h ^= events[i].Job
		h *= prime
		h ^= uint64(events[i].Size)
		h *= prime
		if events[i].Dir {
			h ^= 1
			h *= prime
		}
		if events[i].FullSync {
			h ^= 2
			h *= prime
		}
		if events[i].Created {
			h ^= 4
			h *= prime
		}
	}
	return h
}

// buildCutoffStates returns the persistence states for all cutoffs, using an in-memory
// cache to avoid repeated O(N) reconstruction on the same trace.
func buildCutoffStates(events []trace.Event) []*cutoffState {
	if len(events) == 0 {
		return buildCutoffStatesUncached(events)
	}

	length := len(events)
	fp := traceFingerprint(events)

	cutoffCacheMu.Lock()
	for i, entry := range cutoffCache {
		if entry.length == length && entry.hash == fp {
			res := entry.states
			if i > 0 {
				copy(cutoffCache[1:i+1], cutoffCache[0:i])
				cutoffCache[0] = entry
			}
			cutoffCacheMu.Unlock()
			return res
		}
	}
	cutoffCacheMu.Unlock()

	states := buildCutoffStatesUncached(events)

	cutoffCacheMu.Lock()
	newEntry := cutoffCacheEntry{
		length: length,
		hash:   fp,
		states: states,
	}
	if len(cutoffCache) < 16 {
		cutoffCache = append([]cutoffCacheEntry{newEntry}, cutoffCache...)
	} else {
		copy(cutoffCache[1:], cutoffCache[:15])
		cutoffCache[0] = newEntry
	}
	cutoffCacheMu.Unlock()

	return states
}

// buildCutoffStatesUncached performs a single forward pass over events, computing the
// snapshot of persistence state for every cutoff 0..len(events).
func buildCutoffStatesUncached(events []trace.Event) []*cutoffState {
	maxID := uint64(len(events))
	for _, event := range events {
		if event.ID > maxID {
			maxID = event.ID
		}
	}
	states := make([]*cutoffState, maxID+1)
	files := make(map[string]*fileEntry)
	handles := make(map[uint64]*fileEntry)
	var pending []pendingDirOp

	states[0] = &cutoffState{
		files:   cloneFiles(files),
		pending: nil,
	}

	for i, event := range events {
		switch event.Kind {
		case trace.KindMkdirAll:
			if event.Err != "" {
				break
			}
			files[event.File] = &fileEntry{path: event.File, isDir: true}
			d := path.Clean(path.Dir(event.File))
			pending = append(pending, pendingDirOp{dir: d, desc: "mkdir:" + event.File})
		case trace.KindCreate:
			if event.Err != "" {
				break
			}
			f := &fileEntry{path: event.File}
			files[event.File] = f
			handles[event.Handle] = f
			d := path.Clean(path.Dir(event.File))
			pending = append(pending, pendingDirOp{dir: d, desc: "create:" + event.File})
		case trace.KindReuseForWrite:
			if event.Err != "" {
				break
			}
			f := files[event.File]
			if f != nil {
				delete(files, event.File)
				f.path = event.File2
				f.currentLen = 0
				f.durableLen = 0
				f.synced = false
				f.isDir = false
				files[event.File2] = f
			} else {
				f = &fileEntry{path: event.File2}
				files[event.File2] = f
			}
			handles[event.Handle] = f
			dOld := path.Clean(path.Dir(event.File))
			dNew := path.Clean(path.Dir(event.File2))
			if dOld == dNew {
				pending = append(pending, pendingDirOp{dir: dOld, desc: "reuse:" + event.File + "->" + event.File2})
			} else {
				pending = append(pending, pendingDirOp{dir: dOld, desc: "reuse_from:" + event.File})
				pending = append(pending, pendingDirOp{dir: dNew, desc: "reuse_to:" + event.File2})
			}
		case trace.KindOpen, trace.KindOpenReadWrite, trace.KindOpenDir:
			if event.Err != "" {
				break
			}
			f := files[event.File]
			if f == nil {
				f = &fileEntry{path: event.File}
				files[event.File] = f
			}
			handles[event.Handle] = f
		case trace.KindWrite, trace.KindWriteAt:
			if event.Err != "" {
				break
			}
			f := handles[event.Handle]
			if f != nil && event.Len > 0 {
				end := event.Off + int64(event.Len)
				if end > f.currentLen {
					f.currentLen = end
				}
			}
		case trace.KindPreallocate:
			if event.Err != "" {
				break
			}
			f := handles[event.Handle]
			if f != nil && event.Size > 0 {
				end := event.Off + event.Size
				if end > f.currentLen {
					f.currentLen = end
				}
			}
		case trace.KindSync, trace.KindSyncData, trace.KindSyncTo:
			if event.Err != "" {
				break
			}
			if event.IsBarrier() {
				f := handles[event.Handle]
				if f != nil {
					f.synced = true
					f.durableLen = f.currentLen
				}
				if event.Dir {
					syncDir := event.File
					if syncDir == "" {
						if h := handles[event.Handle]; h != nil {
							syncDir = h.path
						}
					}
					if syncDir != "" {
						syncDir = path.Clean(syncDir)
						kept := make([]pendingDirOp, 0, len(pending))
						for _, p := range pending {
							if p.dir != syncDir {
								kept = append(kept, p)
							}
						}
						pending = kept
					}
				}
			}
		case trace.KindClose:
			delete(handles, event.Handle)
		case trace.KindRename:
			if event.Err != "" {
				break
			}
			f := files[event.File]
			if f != nil {
				delete(files, event.File)
				f.path = event.File2
				files[event.File2] = f
			}
			dOld := path.Clean(path.Dir(event.File))
			dNew := path.Clean(path.Dir(event.File2))
			if dOld == dNew {
				pending = append(pending, pendingDirOp{dir: dOld, desc: "rename:" + event.File + "->" + event.File2})
			} else {
				pending = append(pending, pendingDirOp{dir: dOld, desc: "rename_from:" + event.File})
				pending = append(pending, pendingDirOp{dir: dNew, desc: "rename_to:" + event.File2})
			}
		case trace.KindRemove, trace.KindRemoveAll:
			if event.Err != "" {
				break
			}
			f := files[event.File]
			if f != nil {
				f.deleted = true
				delete(files, event.File)
			}
			// For RemoveAll, also delete all child paths (subtree pruning).
			if event.Kind == trace.KindRemoveAll {
				isRoot := event.File == "" || event.File == "."
				prefix := strings.TrimSuffix(event.File, "/") + "/"
				for childPath := range files {
					if isRoot || strings.HasPrefix(childPath, prefix) {
						cf := files[childPath]
						if cf != nil {
							cf.deleted = true
						}
						delete(files, childPath)
					}
				}
				kept := make([]pendingDirOp, 0, len(pending))
				for _, p := range pending {
					if !isRoot && p.dir != event.File && !strings.HasPrefix(p.dir, prefix) {
						kept = append(kept, p)
					}
				}
				pending = kept
			}
			d := path.Clean(path.Dir(event.File))
			pending = append(pending, pendingDirOp{dir: d, desc: "remove:" + event.File})
		case trace.KindLink:
			if event.Err != "" {
				break
			}
			// Mirror the link in the files map: the destination path shares the
			// same size as the source, matching replay.go's filePaths alias logic.
			src := files[event.File]
			if src != nil {
				dst := &fileEntry{
					path:       event.File2,
					currentLen: src.currentLen,
					durableLen: src.durableLen,
					isDir:      src.isDir,
					synced:     src.synced,
				}
				files[event.File2] = dst
			}
			d := path.Clean(path.Dir(event.File2))
			pending = append(pending, pendingDirOp{dir: d, desc: "link:" + event.File2})
		}

		cs := &cutoffState{
			files:   cloneFiles(files),
			pending: clonePending(pending),
		}
		if event.ID > 0 {
			if event.ID <= maxID {
				states[event.ID] = cs
			}
		} else if uint64(i+1) <= maxID {
			states[i+1] = cs
		}
	}

	for idx := 1; idx <= int(maxID); idx++ {
		if states[idx] == nil {
			states[idx] = states[idx-1]
		}
	}

	return states
}

// StateHash returns the canonical persistence state key for a cutoff in a trace
// under a given retention policy.
func StateHash(events []trace.Event, cut uint64, percent int) string {
	states := buildCutoffStates(events)
	if cut >= uint64(len(states)) {
		cut = uint64(len(states) - 1)
	}
	return states[cut].Key(percent)
}

type collapseKey struct {
	event   uint64
	percent int
}

func collapseWithoutTrace(candidates []Candidate) []Candidate {
	ordered := append([]Candidate(nil), candidates...)
	Sort(ordered)
	seen := make(map[collapseKey]bool, len(ordered))
	result := make([]Candidate, 0, len(ordered))
	for _, c := range ordered {
		k := collapseKey{event: c.Event, percent: c.Percent}
		if seen[k] {
			continue
		}
		seen[k] = true
		result = append(result, c)
	}
	return result
}

func isCutoffMutation(events []trace.Event, cut uint64) bool {
	if cut == 0 || cut > uint64(len(events)) {
		return false
	}
	return events[cut-1].IsMutation()
}

func isBetterCandidateRepresentative(events []trace.Event, a, b Candidate) bool {
	bbA := trace.BeforeBoundary(events, a.Event)
	bbB := trace.BeforeBoundary(events, b.Event)
	if bbA != bbB {
		return bbA // boundary cutoffs preferred
	}

	mutA := isCutoffMutation(events, a.Event)
	mutB := isCutoffMutation(events, b.Event)
	if mutA != mutB {
		return mutA // prefer mutations over annotations
	}

	return Less(a, b)
}

// CollapseEquivalenceStates groups candidates by canonical persistence state
// (using StateHash) and returns one representative candidate per distinct state.
// Redundant OpEnd markers and redundant intermediate cutoffs are collapsed.
// Representatives prioritize cutoffs where trace.BeforeBoundary is true,
// followed by canonical order. The returned slice is sorted canonically.
func CollapseEquivalenceStates(candidates []Candidate, events ...[]trace.Event) []Candidate {
	var evs []trace.Event
	if len(events) > 0 && len(events[0]) > 0 {
		evs = events[0]
	}

	if len(evs) == 0 {
		return collapseWithoutTrace(candidates)
	}

	states := buildCutoffStates(evs)
	numStates := uint64(len(states))

	type classEntry struct {
		key        string
		candidates []Candidate
	}
	classMap := make(map[string]int)
	var classes []classEntry

	for _, c := range candidates {
		cut := c.Event
		if cut >= numStates {
			cut = numStates - 1
		}
		key := states[cut].Key(c.Percent)
		idx, found := classMap[key]
		if !found {
			idx = len(classes)
			classMap[key] = idx
			classes = append(classes, classEntry{key: key})
		}
		classes[idx].candidates = append(classes[idx].candidates, c)
	}

	representatives := make([]Candidate, 0, len(classes))
	for _, cls := range classes {
		best := cls.candidates[0]
		for j := 1; j < len(cls.candidates); j++ {
			cand := cls.candidates[j]
			if isBetterCandidateRepresentative(evs, cand, best) {
				best = cand
			}
		}
		representatives = append(representatives, best)
	}

	Sort(representatives)
	return representatives
}

// CollapseEquivalenceStatesWithTrace is a convenience wrapper passing an explicit trace.
func CollapseEquivalenceStatesWithTrace(events []trace.Event, candidates []Candidate) []Candidate {
	return CollapseEquivalenceStates(candidates, events)
}

// EquivalenceClasses groups candidates into equivalence classes by their canonical StateHash.
func EquivalenceClasses(candidates []Candidate, events ...[]trace.Event) map[string][]Candidate {
	var evs []trace.Event
	if len(events) > 0 && len(events[0]) > 0 {
		evs = events[0]
	}

	if len(evs) == 0 {
		m := make(map[string][]Candidate)
		for _, c := range candidates {
			key := fmt.Sprintf("cut_%d_p_%d", c.Event, c.Percent)
			m[key] = append(m[key], c)
		}
		return m
	}

	states := buildCutoffStates(evs)
	numStates := uint64(len(states))
	result := make(map[string][]Candidate)
	for _, c := range candidates {
		cut := c.Event
		if cut >= numStates {
			cut = numStates - 1
		}
		key := states[cut].Key(c.Percent)
		result[key] = append(result[key], c)
	}
	return result
}

// BuildCollapsed builds the universe and immediately collapses it down to
// distinct canonical persistence states.
func BuildCollapsed(events []trace.Event, base uint64, policies []int) ([]Candidate, error) {
	universe, err := Build(events, base, policies)
	if err != nil {
		return nil, err
	}
	return CollapseEquivalenceStates(universe, events), nil
}
