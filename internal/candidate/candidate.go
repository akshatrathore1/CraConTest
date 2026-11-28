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
	var rawElems []json.RawMessage
	if err := json.NewDecoder(bytes.NewReader(raw.Candidates)).Decode(&rawElems); err != nil {
		return nil, fmt.Errorf("decode universe: %w", err)
	}
	universe := make([]Candidate, 0, len(rawElems))
	for i, elem := range rawElems {
		if bytes.Equal(bytes.TrimSpace(elem), []byte("null")) {
			return nil, fmt.Errorf("decode universe: candidate %d is null", i)
		}
		elemDec := json.NewDecoder(bytes.NewReader(elem))
		elemDec.DisallowUnknownFields()
		var c Candidate
		if err := elemDec.Decode(&c); err != nil {
			return nil, fmt.Errorf("decode universe: candidate %d: %w", i, err)
		}
		universe = append(universe, c)
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

type span struct {
	start int64
	end   int64
}

func addSpan(list []span, next span) []span {
	if next.end <= next.start {
		return list
	}
	merged := make([]span, 0, len(list)+1)
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

func mixWriteDigest(prev uint64, off int64, length int, data []byte, writeSeq uint64) uint64 {
	h := prev
	if h == 0 {
		h = 14695981039346656037
	}
	const prime = 1099511628211
	mixU64 := func(v uint64) {
		for k := 0; k < 8; k++ {
			h ^= v & 0xff
			h *= prime
			v >>= 8
		}
	}
	mixU64(uint64(off))
	mixU64(uint64(length))
	mixU64(writeSeq)
	mixU64(uint64(len(data)))
	for _, b := range data {
		h ^= uint64(b)
		h *= prime
	}
	if h == 0 {
		h = 1
	}
	return h
}

type canonicalFile struct {
	Path              string     `json:"path"`
	IsDir             bool       `json:"is_dir,omitempty"`
	DurableOnly       bool       `json:"durable_only,omitempty"`
	Replaced          bool       `json:"replaced,omitempty"`
	DurableLen        int64      `json:"durable_len,omitempty"`
	DurableDigest     uint64     `json:"durable_digest,omitempty"`
	DurableWriteCount uint64     `json:"durable_write_count,omitempty"`
	DurableSynced     bool       `json:"durable_synced,omitempty"`
	CurrentLen        int64      `json:"current_len,omitempty"`
	LiveDigest        uint64     `json:"live_digest,omitempty"`
	LiveWriteCount    uint64     `json:"live_write_count,omitempty"`
	DirtySpans        [][2]int64 `json:"dirty_spans,omitempty"`
}

type canonicalState struct {
	Percent int             `json:"percent"`
	Files   []canonicalFile `json:"files"`
	Pending []string        `json:"pending,omitempty"`
}

type fileEntry struct {
	path              string
	durableLen        int64
	durableDigest     uint64
	durableWriteCount uint64
	currentLen        int64
	liveDigest        uint64
	writeCount        uint64
	dirtySpans        []span
	deleted           bool
	isDir             bool
	synced            bool
}

type handleEntry struct {
	path  string
	isDir bool
	file  *fileEntry
}

type pendingDirOp struct {
	dir    string
	op     string
	target string
	desc   string
}

type cutoffState struct {
	files       map[string]*fileEntry
	durable     map[string]*fileEntry
	createdDirs map[string]bool
	pending     []pendingDirOp
}

func cloneFilePair(srcFiles, srcDurable map[string]*fileEntry) (map[string]*fileEntry, map[string]*fileEntry) {
	dstFiles := make(map[string]*fileEntry, len(srcFiles))
	dstDurable := make(map[string]*fileEntry, len(srcDurable))
	cloned := make(map[*fileEntry]*fileEntry, len(srcFiles)+len(srcDurable))
	cloneOne := func(v *fileEntry) *fileEntry {
		if v == nil {
			return nil
		}
		if cp, ok := cloned[v]; ok {
			return cp
		}
		tmp := *v
		if len(v.dirtySpans) > 0 {
			tmp.dirtySpans = append([]span(nil), v.dirtySpans...)
		}
		cp := &tmp
		cloned[v] = cp
		return cp
	}
	for k, v := range srcFiles {
		if cp := cloneOne(v); cp != nil {
			dstFiles[k] = cp
		}
	}
	for k, v := range srcDurable {
		if cp := cloneOne(v); cp != nil {
			dstDurable[k] = cp
		}
	}
	return dstFiles, dstDurable
}

func cloneCreatedDirs(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		if v {
			dst[k] = true
		}
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

func cleanVFSPath(p string) string {
	if p == "" || p == "." || p == "/" {
		return ""
	}
	c := path.Clean(p)
	if c == "." || c == "/" {
		return ""
	}
	return strings.TrimPrefix(c, "/")
}

func parentDir(p string) string {
	p = cleanVFSPath(p)
	if p == "" {
		return ""
	}
	d := path.Dir(p)
	if d == "." || d == "/" {
		return ""
	}
	return d
}

func (s *cutoffState) isDurableReachable(name string) bool {
	if s.durable[name] == nil {
		return false
	}
	for anc := parentDir(name); anc != ""; anc = parentDir(anc) {
		if d := s.durable[anc]; d != nil {
			if !d.isDir {
				return false
			}
			continue
		}
		if s.files[anc] != nil || s.createdDirs[anc] {
			return false
		}
	}
	return true
}

func spansToPairs(spans []span) [][2]int64 {
	if len(spans) == 0 {
		return nil
	}
	out := make([][2]int64, len(spans))
	for i, sp := range spans {
		out[i] = [2]int64{sp.start, sp.end}
	}
	return out
}

func (s *cutoffState) Key(percent int) string {
	state := canonicalState{
		Percent: percent,
		Files:   []canonicalFile{},
	}

	if percent == 0 {
		if len(s.pending) > 0 {
			state.Pending = make([]string, 0, len(s.pending))
			for _, p := range s.pending {
				state.Pending = append(state.Pending, p.desc)
			}
			sort.Strings(state.Pending)
		}
		names := make([]string, 0, len(s.durable))
		for name := range s.durable {
			if s.isDurableReachable(name) {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			f := s.durable[name]
			if f == nil || f.deleted {
				continue
			}
			state.Files = append(state.Files, canonicalFile{
				Path:              name,
				IsDir:             f.isDir,
				DurableLen:        f.durableLen,
				DurableDigest:     f.durableDigest,
				DurableWriteCount: f.durableWriteCount,
				DurableSynced:     f.synced,
			})
		}
	} else if percent == 100 {
		// Under 100% retention all unsynced data and directory entries survive unconditionally,
		// so pending directory ops and durable checkpoints do not alter the post-crash filesystem.
		names := make([]string, 0, len(s.files))
		for name := range s.files {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			f := s.files[name]
			if f == nil || f.deleted {
				continue
			}
			state.Files = append(state.Files, canonicalFile{
				Path:           name,
				IsDir:          f.isDir,
				CurrentLen:     f.currentLen,
				LiveDigest:     f.liveDigest,
				LiveWriteCount: f.writeCount,
			})
		}
	} else {
		if len(s.pending) > 0 {
			state.Pending = make([]string, 0, len(s.pending))
			for _, p := range s.pending {
				state.Pending = append(state.Pending, p.desc)
			}
			sort.Strings(state.Pending)
		}
		nameSet := make(map[string]bool, len(s.files)+len(s.durable))
		for name, f := range s.files {
			if f != nil && !f.deleted {
				nameSet[name] = true
			}
		}
		for name, f := range s.durable {
			if f != nil && !f.deleted && s.isDurableReachable(name) {
				nameSet[name] = true
			}
		}
		names := make([]string, 0, len(nameSet))
		for name := range nameSet {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			lf := s.files[name]
			var df *fileEntry
			if s.isDurableReachable(name) {
				df = s.durable[name]
			}
			switch {
			case lf != nil && !lf.deleted && df != nil && !df.deleted:
				state.Files = append(state.Files, canonicalFile{
					Path:              name,
					IsDir:             lf.isDir,
					Replaced:          lf != df,
					DurableLen:        df.durableLen,
					DurableDigest:     df.durableDigest,
					DurableWriteCount: df.durableWriteCount,
					DurableSynced:     df.synced,
					CurrentLen:        lf.currentLen,
					LiveDigest:        lf.liveDigest,
					LiveWriteCount:    lf.writeCount,
					DirtySpans:        spansToPairs(lf.dirtySpans),
				})
			case lf != nil && !lf.deleted:
				state.Files = append(state.Files, canonicalFile{
					Path:              name,
					IsDir:             lf.isDir,
					DurableLen:        lf.durableLen,
					DurableDigest:     lf.durableDigest,
					DurableWriteCount: lf.durableWriteCount,
					DurableSynced:     lf.synced,
					CurrentLen:        lf.currentLen,
					LiveDigest:        lf.liveDigest,
					LiveWriteCount:    lf.writeCount,
					DirtySpans:        spansToPairs(lf.dirtySpans),
				})
			case df != nil && !df.deleted:
				state.Files = append(state.Files, canonicalFile{
					Path:              name,
					IsDir:             df.isDir,
					DurableOnly:       true,
					DurableLen:        df.durableLen,
					DurableDigest:     df.durableDigest,
					DurableWriteCount: df.durableWriteCount,
					DurableSynced:     df.synced,
				})
			}
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
	hash1  uint64
	hash2  uint64
	states []*cutoffState
}

var (
	cutoffCacheMu sync.Mutex
	cutoffCache   = make([]cutoffCacheEntry, 0, 16)
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
	mixBytes := func(b []byte) {
		mixU64(uint64(len(b)))
		for j := 0; j < len(b); j++ {
			v := uint64(b[j])
			h1 ^= v
			h1 *= prime1
			h2 ^= v
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
		mixBytes(ev.Data)
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

// buildCutoffStates returns the persistence states for all cutoffs, using an in-memory
// cache to avoid repeated O(N) reconstruction on the same trace.
func buildCutoffStates(events []trace.Event) []*cutoffState {
	if len(events) == 0 {
		return buildCutoffStatesUncached(events)
	}

	length := len(events)
	fp1, fp2 := traceFingerprint(events)

	cutoffCacheMu.Lock()
	for i, entry := range cutoffCache {
		if entry.length == length && entry.hash1 == fp1 && entry.hash2 == fp2 {
			res := append([]*cutoffState(nil), entry.states...)
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
		hash1:  fp1,
		hash2:  fp2,
		states: append([]*cutoffState(nil), states...),
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

func rewritePathPrefix(p, oldP, newP, oldPrefix, newPrefix string) (string, bool) {
	if p == oldP {
		return newP, true
	}
	if oldP != "" && strings.HasPrefix(p, oldPrefix) {
		return newPrefix + strings.TrimPrefix(p, oldPrefix), true
	}
	return p, false
}

// buildCutoffStatesUncached performs a single forward pass over events, computing the
// snapshot of persistence state for every cutoff 0..maxID.
func buildCutoffStatesUncached(events []trace.Event) []*cutoffState {
	const maxGapAllowance = 1 << 20
	maxID := uint64(len(events))
	for _, event := range events {
		if event.ID > maxID {
			if event.ID-uint64(len(events)) > maxGapAllowance {
				return nil
			}
			maxID = event.ID
		}
	}
	if maxID == ^uint64(0) || maxID-uint64(len(events)) > maxGapAllowance {
		return nil
	}
	states := make([]*cutoffState, maxID+1)
	files := make(map[string]*fileEntry)
	durable := make(map[string]*fileEntry)
	createdDirs := make(map[string]bool)
	handles := make(map[uint64]*handleEntry)
	var pending []pendingDirOp

	initFiles, initDurable := cloneFilePair(files, durable)
	states[0] = &cutoffState{
		files:   initFiles,
		durable: initDurable,
		pending: nil,
	}

	for i, event := range events {
		switch event.Kind {
		case trace.KindMkdirAll:
			if event.Err != "" {
				break
			}
			clean := cleanVFSPath(event.File)
			if clean != "" {
				parts := strings.Split(clean, "/")
				for idx := range parts {
					sub := strings.Join(parts[:idx+1], "/")
					if sub == "" || sub == "." {
						continue
					}
					if _, exists := files[sub]; !exists {
						files[sub] = &fileEntry{path: sub, isDir: true}
						createdDirs[sub] = true
						d := parentDir(sub)
						pending = append(pending, pendingDirOp{dir: d, op: "mkdir", target: sub, desc: "mkdir:" + sub})
					}
				}
			}
		case trace.KindCreate:
			if event.Err != "" {
				break
			}
			p := cleanVFSPath(event.File)
			f := &fileEntry{path: p}
			files[p] = f
			handles[event.Handle] = &handleEntry{path: p, file: f}
			d := parentDir(p)
			pending = append(pending, pendingDirOp{dir: d, op: "create", target: p, desc: "create:" + p})
		case trace.KindReuseForWrite:
			if event.Err != "" {
				break
			}
			oldP := cleanVFSPath(event.File)
			newP := cleanVFSPath(event.File2)
			delete(files, oldP)
			f := &fileEntry{path: newP}
			files[newP] = f
			handles[event.Handle] = &handleEntry{path: newP, file: f}
			dOld := parentDir(oldP)
			dNew := parentDir(newP)
			if dOld == dNew {
				pending = append(pending, pendingDirOp{dir: dOld, op: "reuse", target: newP, desc: "reuse:" + oldP + "->" + newP})
			} else {
				pending = append(pending, pendingDirOp{dir: dOld, op: "reuse_from", target: oldP, desc: "reuse_from:" + oldP})
				pending = append(pending, pendingDirOp{dir: dNew, op: "reuse_to", target: newP, desc: "reuse_to:" + newP})
			}
		case trace.KindOpen, trace.KindOpenReadWrite, trace.KindOpenDir:
			if event.Err != "" {
				break
			}
			p := cleanVFSPath(event.File)
			isDir := event.Dir || event.Kind == trace.KindOpenDir
			if event.Kind == trace.KindOpenReadWrite && event.Created {
				f := &fileEntry{path: p, isDir: isDir}
				files[p] = f
				if isDir && p != "" {
					createdDirs[p] = true
				}
				handles[event.Handle] = &handleEntry{path: p, isDir: isDir, file: f}
				d := parentDir(p)
				pending = append(pending, pendingDirOp{dir: d, op: "create", target: p, desc: "create:" + p})
			} else {
				// Read-only Open / OpenDir / existing OpenReadWrite must not mutate namespace state.
				handles[event.Handle] = &handleEntry{path: p, isDir: isDir, file: files[p]}
			}
		case trace.KindWrite, trace.KindWriteAt:
			if event.Err != "" {
				break
			}
			h := handles[event.Handle]
			if h != nil && event.Len > 0 {
				if h.file == nil && h.path != "" {
					f := files[h.path]
					if f == nil {
						f = &fileEntry{path: h.path}
						files[h.path] = f
					}
					h.file = f
				}
				if h.file != nil {
					f := h.file
					end := event.Off + int64(event.Len)
					if end > f.currentLen {
						f.currentLen = end
					}
					f.dirtySpans = addSpan(f.dirtySpans, span{start: event.Off, end: end})
					f.writeCount++
					f.liveDigest = mixWriteDigest(f.liveDigest, event.Off, event.Len, event.Data, f.writeCount)
				}
			}
		case trace.KindPreallocate:
			// MemFS Preallocate is a no-op that does not modify durable or current length.
		case trace.KindSync, trace.KindSyncData, trace.KindSyncTo:
			if event.Err != "" {
				break
			}
			if event.IsBarrier() {
				h := handles[event.Handle]
				if h != nil && h.file != nil && !h.isDir {
					f := h.file
					f.synced = true
					f.durableLen = f.currentLen
					f.durableDigest = f.liveDigest
					f.durableWriteCount = f.writeCount
					f.dirtySpans = nil
				}
				if event.Dir || (h != nil && h.isDir) {
					syncDir := cleanVFSPath(event.File)
					if event.File == "" && h != nil {
						syncDir = h.path
					}
					kept := make([]pendingDirOp, 0, len(pending))
					for _, p := range pending {
						if p.dir != syncDir {
							kept = append(kept, p)
						}
					}
					pending = kept

					// Preserve synced descendants if a durable directory was renamed into syncDir.
					movedSubtrees := make(map[string]map[string]*fileEntry)
					for lp, lf := range files {
						if parentDir(lp) != syncDir || lf == nil || !lf.isDir {
							continue
						}
						if durable[lp] == lf {
							continue
						}
						for oldDP, oldDF := range durable {
							if oldDF == lf && oldDP != lp {
								oldPrefix := oldDP + "/"
								subMap := make(map[string]*fileEntry)
								for subP, subF := range durable {
									if strings.HasPrefix(subP, oldPrefix) {
										subMap[strings.TrimPrefix(subP, oldPrefix)] = subF
									}
								}
								movedSubtrees[lp] = subMap
								break
							}
						}
					}

					// Prune stale durable direct children of syncDir (and subtrees of removed/replaced dirs).
					var staleDirs []string
					var staleFiles []string
					for dp, df := range durable {
						if parentDir(dp) == syncDir && files[dp] != df {
							staleFiles = append(staleFiles, dp)
							if df != nil && df.isDir {
								staleDirs = append(staleDirs, dp)
							}
						}
					}
					for _, dp := range staleFiles {
						delete(durable, dp)
					}
					for _, d := range staleDirs {
						prefix := d + "/"
						for sub := range durable {
							if strings.HasPrefix(sub, prefix) {
								delete(durable, sub)
							}
						}
					}
					// Persist all current live direct children of syncDir into durable.
					for lp, lf := range files {
						if parentDir(lp) == syncDir && lf != nil && !lf.deleted {
							durable[lp] = lf
							if subMap, ok := movedSubtrees[lp]; ok {
								for rel, subF := range subMap {
									durable[lp+"/"+rel] = subF
								}
							}
						}
					}
				}
			}
		case trace.KindClose:
			delete(handles, event.Handle)
		case trace.KindRename:
			if event.Err != "" {
				break
			}
			oldP := cleanVFSPath(event.File)
			newP := cleanVFSPath(event.File2)
			if f := files[oldP]; f != nil {
				delete(files, oldP)
				f.path = newP
				files[newP] = f
			}
			oldPrefix := oldP + "/"
			newPrefix := newP + "/"
			type childMove struct {
				oldPath string
				newPath string
				entry   *fileEntry
			}
			var moved []childMove
			for childPath, cf := range files {
				if oldP != "" && strings.HasPrefix(childPath, oldPrefix) {
					moved = append(moved, childMove{
						oldPath: childPath,
						newPath: newPrefix + strings.TrimPrefix(childPath, oldPrefix),
						entry:   cf,
					})
				}
			}
			for _, m := range moved {
				delete(files, m.oldPath)
				m.entry.path = m.newPath
				files[m.newPath] = m.entry
			}
			var movedDirs []string
			for d := range createdDirs {
				if d == oldP || (oldP != "" && strings.HasPrefix(d, oldPrefix)) {
					movedDirs = append(movedDirs, d)
				}
			}
			for _, d := range movedDirs {
				delete(createdDirs, d)
				if nd, ok := rewritePathPrefix(d, oldP, newP, oldPrefix, newPrefix); ok {
					createdDirs[nd] = true
				}
			}
			for _, h := range handles {
				if np, ok := rewritePathPrefix(h.path, oldP, newP, oldPrefix, newPrefix); ok {
					h.path = np
				}
			}
			for idx := range pending {
				if nd, ok := rewritePathPrefix(pending[idx].dir, oldP, newP, oldPrefix, newPrefix); ok {
					pending[idx].dir = nd
				}
				if nt, ok := rewritePathPrefix(pending[idx].target, oldP, newP, oldPrefix, newPrefix); ok {
					pending[idx].target = nt
					if pending[idx].op != "" && pending[idx].op != "rename" && pending[idx].op != "reuse" {
						pending[idx].desc = pending[idx].op + ":" + nt
					}
				}
			}
			dOld := parentDir(oldP)
			dNew := parentDir(newP)
			if dOld == dNew {
				pending = append(pending, pendingDirOp{dir: dOld, op: "rename", target: newP, desc: "rename:" + oldP + "->" + newP})
			} else {
				pending = append(pending, pendingDirOp{dir: dOld, op: "rename_from", target: oldP, desc: "rename_from:" + oldP})
				pending = append(pending, pendingDirOp{dir: dNew, op: "rename_to", target: newP, desc: "rename_to:" + newP})
			}
		case trace.KindRemove, trace.KindRemoveAll:
			if event.Err != "" {
				break
			}
			p := cleanVFSPath(event.File)
			if p != "" {
				delete(files, p)
			}
			isRoot := false
			// For RemoveAll, also delete all child paths (subtree pruning).
			if event.Kind == trace.KindRemoveAll {
				isRoot = p == ""
				prefix := p + "/"
				for childPath := range files {
					if isRoot || childPath == p || strings.HasPrefix(childPath, prefix) {
						delete(files, childPath)
					}
				}
				for d := range createdDirs {
					if isRoot || d == p || strings.HasPrefix(d, prefix) {
						delete(createdDirs, d)
					}
				}
				kept := make([]pendingDirOp, 0, len(pending))
				for _, po := range pending {
					if !isRoot && po.dir != p && !strings.HasPrefix(po.dir, prefix) {
						kept = append(kept, po)
					}
				}
				pending = kept
			}
			if !isRoot && p != "" {
				d := parentDir(p)
				pending = append(pending, pendingDirOp{dir: d, op: "remove", target: p, desc: "remove:" + p})
			}
		case trace.KindLink:
			if event.Err != "" {
				break
			}
			srcP := cleanVFSPath(event.File)
			dstP := cleanVFSPath(event.File2)
			// Share inode state across hard-linked aliases.
			if src := files[srcP]; src != nil {
				files[dstP] = src
			} else if src := durable[srcP]; src != nil {
				files[dstP] = src
			}
			d := parentDir(dstP)
			pending = append(pending, pendingDirOp{dir: d, op: "link", target: dstP, desc: "link:" + dstP})
		}

		snapFiles, snapDurable := cloneFilePair(files, durable)
		cs := &cutoffState{
			files:       snapFiles,
			durable:     snapDurable,
			createdDirs: cloneCreatedDirs(createdDirs),
			pending:     clonePending(pending),
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

// StateHash returns a retention-policy-aware canonical persistence state key
// for cutoff in events under percent. Note: pipeline ranking strategies use
// graph.View.Cuts[cut].StateKey (percent-agnostic DAG equivalence); StateHash
// provides standalone cutoff-state hashing including synthetic and gap-ID traces.
// Returns "" if the trace cannot be indexed or cut is out of range.
func StateHash(events []trace.Event, cut uint64, percent int) string {
	states := buildCutoffStates(events)
	if len(states) == 0 || cut >= uint64(len(states)) {
		return ""
	}
	return states[cut].Key(percent)
}

type collapseKey struct {
	event   uint64
	percent int
	seed    uint64
}

func seedForEquivalence(c Candidate) uint64 {
	if c.Percent > 0 && c.Percent < 100 {
		return c.Seed
	}
	return 0
}

func candidateEquivalenceKey(s *cutoffState, c Candidate) string {
	key := s.Key(c.Percent)
	if c.Percent > 0 && c.Percent < 100 {
		return fmt.Sprintf("%s:seed=%d", key, c.Seed)
	}
	return key
}

func collapseWithoutTrace(candidates []Candidate) []Candidate {
	ordered := append([]Candidate(nil), candidates...)
	Sort(ordered)
	seen := make(map[collapseKey]bool, len(ordered))
	result := make([]Candidate, 0, len(ordered))
	for _, c := range ordered {
		k := collapseKey{event: c.Event, percent: c.Percent, seed: seedForEquivalence(c)}
		if seen[k] {
			continue
		}
		seen[k] = true
		result = append(result, c)
	}
	return result
}

func findEventByID(events []trace.Event, id uint64) (trace.Event, bool) {
	if id == 0 || len(events) == 0 {
		return trace.Event{}, false
	}
	if int(id) <= len(events) && events[id-1].ID == id {
		return events[id-1], true
	}
	idx := sort.Search(len(events), func(i int) bool {
		return events[i].ID >= id
	})
	if idx < len(events) && events[idx].ID == id {
		return events[idx], true
	}
	for i := range events {
		if events[i].ID == id {
			return events[i], true
		}
	}
	return trace.Event{}, false
}

func beforeBoundaryByID(events []trace.Event, cut uint64) bool {
	next, ok := findEventByID(events, cut+1)
	if !ok {
		return false
	}
	return next.IsBarrier() || (next.Kind == trace.KindRename && next.Err == "")
}

func isCutoffMutation(events []trace.Event, cut uint64) bool {
	ev, ok := findEventByID(events, cut)
	if !ok {
		return false
	}
	return ev.IsMutation()
}

func isBetterCandidateRepresentative(events []trace.Event, a, b Candidate) bool {
	bbA := beforeBoundaryByID(events, a.Event)
	bbB := beforeBoundaryByID(events, b.Event)
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
// (using StateHash, preserving distinct seeds for partial 1..99% retention)
// and returns one representative candidate per distinct state.
// Redundant OpEnd markers and redundant intermediate cutoffs are collapsed.
// Representatives prioritize cutoffs immediately before sync/rename boundaries,
// followed by canonical order. Out-of-range cutoffs remain distinct rather than
// clamping to the final trace cutoff. The returned slice is sorted canonically.
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
	if numStates == 0 {
		return collapseWithoutTrace(candidates)
	}

	type classEntry struct {
		key        string
		candidates []Candidate
	}
	classMap := make(map[string]int)
	var classes []classEntry

	for _, c := range candidates {
		var key string
		if c.Event >= numStates {
			key = fmt.Sprintf("oob:cut=%d:p=%d:seed=%d", c.Event, c.Percent, c.Seed)
		} else {
			key = candidateEquivalenceKey(states[c.Event], c)
		}
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

// EquivalenceClasses groups candidates into equivalence classes by their canonical StateHash
// (preserving distinct seeds for partial 1..99% retention and keeping out-of-range cutoffs distinct).
func EquivalenceClasses(candidates []Candidate, events ...[]trace.Event) map[string][]Candidate {
	var evs []trace.Event
	if len(events) > 0 && len(events[0]) > 0 {
		evs = events[0]
	}

	fallbackClasses := func() map[string][]Candidate {
		m := make(map[string][]Candidate)
		for _, c := range candidates {
			var key string
			if c.Percent > 0 && c.Percent < 100 {
				key = fmt.Sprintf("cut_%d_p_%d_s_%d", c.Event, c.Percent, c.Seed)
			} else {
				key = fmt.Sprintf("cut_%d_p_%d", c.Event, c.Percent)
			}
			m[key] = append(m[key], c)
		}
		return m
	}

	if len(evs) == 0 {
		return fallbackClasses()
	}

	states := buildCutoffStates(evs)
	numStates := uint64(len(states))
	if numStates == 0 {
		return fallbackClasses()
	}
	result := make(map[string][]Candidate)
	for _, c := range candidates {
		var key string
		if c.Event >= numStates {
			key = fmt.Sprintf("oob:cut=%d:p=%d:seed=%d", c.Event, c.Percent, c.Seed)
		} else {
			key = candidateEquivalenceKey(states[c.Event], c)
		}
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
