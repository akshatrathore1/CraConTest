// Package vfsrec provides a recording virtual filesystem wrapper that logs
// mutating filesystem operations and engine lifecycle events into a trace.
package vfsrec

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// FS wraps a vfs.FS implementation and records mutating filesystem calls,
// operation markers, and engine events into a trace.
type FS struct {
	mu                       sync.Mutex
	inner                    vfs.FS
	nextHandle               uint64
	events                   []trace.Event
	pinnedLen                int
	hook                     func(id uint64)
	waitTimeout              time.Duration
	primaryDBDir             string
	primaryDBDirSet          bool
	inIngestSection          bool
	ingestDepth              int
	ingestGoroutineID        uint64
	ingestGoroutines         map[uint64]int
	ingestCond               *sync.Cond
	walWrites                uint64
	walCond                  *sync.Cond
	dbManifestCount          int
	pendingManifestDeletions int
	manifestCond             *sync.Cond
	listenerCreated          bool
}

// Compile-time interface checks.
var (
	_ vfs.FS = (*FS)(nil)
	_ interface {
		Begin(uint64, string)
		End(uint64, string, string)
	} = (*FS)(nil)
)

// New constructs a recording filesystem wrapper around inner.
func New(inner vfs.FS) *FS {
	fs := &FS{
		inner:            inner,
		ingestGoroutines: make(map[uint64]int),
	}
	fs.ingestCond = sync.NewCond(&fs.mu)
	fs.walCond = sync.NewCond(&fs.mu)
	fs.manifestCond = sync.NewCond(&fs.mu)
	fs.mu.Lock()
	fs.recountDBManifestsLocked()
	fs.mu.Unlock()
	return fs
}

func baseName(p string) string {
	if slash := strings.LastIndexByte(p, '/'); slash >= 0 {
		return p[slash+1:]
	}
	return p
}

func dirOf(p string) string {
	if slash := strings.LastIndexByte(p, '/'); slash >= 0 {
		return p[:slash]
	}
	return ""
}

func isInPrimaryDBDir(p, primaryDBDir string, primaryDBDirSet bool) bool {
	if primaryDBDirSet {
		if primaryDBDir == "" {
			return p != "" && strings.IndexByte(p, '/') < 0
		}
		prefix := primaryDBDir + "/"
		if !strings.HasPrefix(p, prefix) {
			return false
		}
		rest := p[len(prefix):]
		return rest != "" && strings.IndexByte(rest, '/') < 0
	}
	slash := strings.IndexByte(p, '/')
	if slash < 0 {
		return p != ""
	}
	if strings.IndexByte(p[slash+1:], '/') >= 0 {
		return false
	}
	dir := p[:slash]
	return dir == "db" || strings.HasPrefix(dir, "db_") || dir == "testdb"
}

func isManifestFileName(p string) bool {
	base := baseName(p)
	return !strings.HasPrefix(base, "marker.") && strings.HasPrefix(base, "MANIFEST-")
}

// isPrimaryDBDir reports whether path resides in the root directory ("") or a
// primary database directory ("db", "db_*", or "testdb"), excluding secondary
// or staging directories such as "checkpoint/", "ingest_staging/", or unrelated folders.
func isPrimaryDBDir(path string) bool {
	return isInPrimaryDBDir(path, "", false)
}

// isDBWALPath reports whether path is a primary-DB WAL segment ("*.log" in the
// root or primary DB directory, excluding "checkpoint/" and other non-primary directories).
func isDBWALPath(path string) bool {
	return isPrimaryDBDir(path) && strings.HasSuffix(path, ".log")
}

// isDBManifestPath reports whether path is a primary-DB MANIFEST file ("MANIFEST-*"
// in the root or primary DB directory, excluding "checkpoint/", atomic "marker." files,
// and other non-primary directories).
func isDBManifestPath(path string) bool {
	return isPrimaryDBDir(path) && isManifestFileName(path)
}

// cleanPath normalizes a directory path using forward slashes, collapsing
// redundant separators and relative "./" segments, treating "." and "" as "".
func cleanPath(dir string) string {
	dir = strings.ReplaceAll(dir, "\\", "/")
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return ""
	}
	cleaned := path.Clean(dir)
	cleaned = strings.Trim(cleaned, "/")
	if cleaned == "" || cleaned == "." {
		return ""
	}
	return cleaned
}

func (fs *FS) listDirLocked(dir string) ([]string, error) {
	if dir != "" {
		return fs.inner.List(dir)
	}
	entries, err := fs.inner.List("")
	if err == nil {
		return entries, nil
	}
	return fs.inner.List(".")
}

func (fs *FS) countManifestsInDirLocked(dir string) int {
	if fs.inner == nil {
		return 0
	}
	// Calling fs.inner.List under fs.mu is safe because fs.inner is the underlying VFS and never calls back into fs.
	entries, err := fs.listDirLocked(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		p := entry
		if dir != "" {
			p = dir + "/" + entry
		}
		if fs.isDBManifestPathLocked(p) {
			count++
		}
	}
	return count
}

func (fs *FS) recountDBManifestsLocked() {
	if fs.inner == nil {
		fs.dbManifestCount = 0
		return
	}
	if fs.primaryDBDirSet {
		fs.dbManifestCount = fs.countManifestsInDirLocked(fs.primaryDBDir)
		return
	}
	count := fs.countManifestsInDirLocked("")
	if entries, err := fs.listDirLocked(""); err == nil {
		for _, entry := range entries {
			if entry == "db" || strings.HasPrefix(entry, "db_") || entry == "testdb" {
				count += fs.countManifestsInDirLocked(entry)
			}
		}
	}
	fs.dbManifestCount = count
}

// SetPrimaryDBDir configures the primary database directory path for the recording FS.
// It normalizes dir with cleanPath (treating "." and "/" as explicit root-only and "" as reset
// to the default primary directory allowlist) and recounts live manifests under fs.mu.
func (fs *FS) SetPrimaryDBDir(dir string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if strings.TrimSpace(dir) == "" {
		fs.primaryDBDir = ""
		fs.primaryDBDirSet = false
	} else {
		fs.primaryDBDir = cleanPath(dir)
		fs.primaryDBDirSet = true
	}
	fs.recountDBManifestsLocked()
}

// isPrimaryDBDirLocked reports whether path is directly inside the configured primaryDBDir
// (or root directory only when SetPrimaryDBDir(".") was configured), falling back to the
// package-level isPrimaryDBDir allowlist if primaryDBDir has not been configured.
// Must be called with fs.mu held.
func (fs *FS) isPrimaryDBDirLocked(p string) bool {
	return isInPrimaryDBDir(p, fs.primaryDBDir, fs.primaryDBDirSet)
}

// isDBWALPathLocked reports whether path is a primary-DB WAL segment, called while holding fs.mu.
func (fs *FS) isDBWALPathLocked(path string) bool {
	return fs.isPrimaryDBDirLocked(path) && strings.HasSuffix(path, ".log")
}

// isDBManifestPathLocked reports whether path is a primary-DB MANIFEST file, called while holding fs.mu.
func (fs *FS) isDBManifestPathLocked(path string) bool {
	return fs.isPrimaryDBDirLocked(path) && isManifestFileName(path)
}

// WALWriteCount returns the number of recorded primary-DB WAL write/drop events.
func (fs *FS) WALWriteCount() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.walWrites
}

// NeedWALWriteBarrier reports whether WaitForWALWrite barriers should be
// issued after Sync mutations (op.Sync). For plain recording FSes the WAL write
// tracking is informational only and no barrier is needed; fault-injecting
// wrappers override this to return true.
func (fs *FS) NeedWALWriteBarrier() bool { return false }

// NotifyDroppedWALWrite increments the WAL write counter and wakes any waiter
// when a fault injector drops a primary-DB WAL write before it reaches vfsrec.FS.
func (fs *FS) NotifyDroppedWALWrite() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.walWrites++
	if fs.walCond != nil {
		fs.walCond.Broadcast()
	}
}

func defaultWaitTimeout() time.Duration {
	if env := os.Getenv("CRACONTEST_WAIT_TIMEOUT"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Second
}

// waitCondWithTimeoutLocked waits on cond until ready() returns true or the configured
// safety deadline expires. Must be called with fs.mu held; returns with fs.mu held.
func (fs *FS) waitCondWithTimeoutLocked(cond *sync.Cond, ready func() bool) (timedOut bool, timeout time.Duration) {
	timeout = fs.waitTimeout
	if timeout <= 0 {
		timeout = defaultWaitTimeout()
	}
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			fs.mu.Lock()
			timedOut = true
			cond.Broadcast()
			fs.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	for !ready() {
		if timedOut {
			return true, timeout
		}
		cond.Wait()
	}
	return false, timeout
}

// WaitForWALWrite blocks until WALWriteCount() >= target, panicking if a safety
// deadline expires before the expected WAL write is recorded.
func (fs *FS) WaitForWALWrite(target uint64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.walWrites >= target {
		return
	}
	if fs.walCond == nil {
		fs.walCond = sync.NewCond(&fs.mu)
	}
	if timedOut, timeout := fs.waitCondWithTimeoutLocked(fs.walCond, func() bool {
		return fs.walWrites >= target
	}); timedOut {
		panic(fmt.Sprintf("vfsrec: WaitForWALWrite exceeded %v deadline", timeout))
	}
}

// Begin records an OpBegin marker event for a client operation.
func (fs *FS) Begin(op uint64, kind string) {
	_ = fs.BeginEvent(op, kind)
}

// BeginEvent records an OpBegin marker event and returns the assigned event ID atomically.
func (fs *FS) BeginEvent(op uint64, kind string) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.appendEventLocked(trace.Event{
		Kind:   trace.KindOpBegin,
		Op:     op,
		OpKind: kind,
	})
	fs.pinnedLen = len(fs.events)
	return fs.events[len(fs.events)-1].ID
}

// End records an OpEnd marker event for a client operation.
func (fs *FS) End(op uint64, kind string, err string) {
	_ = fs.EndEvent(op, kind, err)
}

// EndEvent records an OpEnd marker event and returns the assigned event ID atomically.
func (fs *FS) EndEvent(op uint64, kind string, err string) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.appendEventLocked(trace.Event{
		Kind:   trace.KindOpEnd,
		Op:     op,
		OpKind: kind,
		Err:    err,
	})
	fs.pinnedLen = len(fs.events)
	return fs.events[len(fs.events)-1].ID
}

// CurrentEventID returns the ID of the most recently recorded trace event (0 if none).
func (fs *FS) CurrentEventID() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.events) == 0 {
		return 0
	}
	fs.pinnedLen = len(fs.events)
	return fs.events[len(fs.events)-1].ID
}

// Events returns a deep copy of all recorded trace events.
func (fs *FS) Events() []trace.Event {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return trace.Clone(fs.events)
}

// SetHook registers a test hook called with the mutex held immediately after
// an event is appended to the trace.
func (fs *FS) SetHook(hook func(id uint64)) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.hook = hook
}

func (fs *FS) appendEventLocked(event trace.Event) {
	event.ID = uint64(len(fs.events) + 1)
	fs.events = append(fs.events, event)
	if fs.hook != nil {
		fs.hook(event.ID)
	}
}

func (fs *FS) insertEventAtLocked(event trace.Event, insertIdx int) {
	fs.events = append(fs.events, trace.Event{})
	copy(fs.events[insertIdx+1:], fs.events[insertIdx:len(fs.events)-1])
	event.ID = uint64(insertIdx + 1)
	fs.events[insertIdx] = event
	for j := insertIdx + 1; j < len(fs.events); j++ {
		fs.events[j].ID = uint64(j + 1)
	}
	if fs.hook != nil {
		for j := insertIdx; j < len(fs.events); j++ {
			fs.hook(fs.events[j].ID)
		}
	}
}

// syncReorderScanWindow bounds the backward scan over trailing non-mutating
// events when placing a file Sync/SyncData/SyncTo adjacent to its preceding
// Write/WriteAt on the same handle.
const syncReorderScanWindow = 8

// appendSyncEventLocked repositions a Sync/SyncData/SyncTo barrier event adjacent
// to its preceding Write/WriteAt on the same handle when only unrelated Close
// or EngineEvent records intervene.
//
// Boundary Invariants:
//  1. Intervening Filter: Only KindEngineEvent and KindClose on unrelated handles
//     (e.Handle != event.Handle) may be crossed. Any other event (Open, OpenDir,
//     Read, Write, WriteAt, Preallocate, Sync, SyncData, SyncTo, Lock, Unlock,
//     OpBegin, OpEnd, or namespace operations) halts the scan immediately.
//  2. Same-Handle & Observed-ID Boundary: Reordering stops at any event on the
//     same handle and never crosses fs.pinnedLen (events whose IDs were already
//     returned by CurrentEventID/BeginEvent/EndEvent).
//  3. Bounded Scan Window: The backward scan checks at most syncReorderScanWindow (8)
//     events. If the preceding Write/WriteAt is farther back, the event is appended
//     to the trace tail without reordering.
//  4. Sequential ID Invariant: If inserted, trailing events are shifted and all IDs
//     from insertIdx onwards are re-indexed to strictly preserve the 1-based contiguous
//     trace invariant: events[k].ID == k+1 for all k in [0, len(fs.events)).
//     If a hook is registered, it is notified for the inserted event and all
//     re-indexed trailing events.
func (fs *FS) appendSyncEventLocked(event trace.Event) {
	insertIdx := -1
	n := len(fs.events)
	for i := n - 1; i >= fs.pinnedLen && (n-1-i) < syncReorderScanWindow; i-- {
		e := fs.events[i]
		if e.Kind != trace.KindEngineEvent && !(e.Kind == trace.KindClose && e.Handle != event.Handle) {
			break
		}
		if i > 0 {
			prev := fs.events[i-1]
			if (prev.Kind == trace.KindWrite || prev.Kind == trace.KindWriteAt) && prev.Handle == event.Handle {
				insertIdx = i
				break
			}
		}
	}

	if insertIdx < 0 {
		fs.appendEventLocked(event)
		return
	}
	fs.insertEventAtLocked(event, insertIdx)
}

func curGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n == 0 {
		return 0
	}
	return parseGoroutineID(string(buf[:n]))
}

func parseGoroutineID(s string) uint64 {
	idx := strings.Index(s, "goroutine ")
	if idx < 0 {
		return 0
	}
	s = strings.TrimLeft(s[idx+len("goroutine "):], " ")
	end := strings.IndexFunc(s, func(r rune) bool {
		return r < '0' || r > '9'
	})
	if end < 0 {
		end = len(s)
	}
	if end == 0 {
		return 0
	}
	id, err := strconv.ParseUint(s[:end], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// BeginIngestSection marks the start of an ingest section, pausing background flushes.
func (fs *FS) BeginIngestSection() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	gid := curGoroutineID()
	if fs.ingestGoroutines == nil {
		fs.ingestGoroutines = make(map[uint64]int)
	}
	fs.ingestDepth++
	fs.ingestGoroutines[gid]++
	fs.inIngestSection = true
	fs.ingestGoroutineID = gid
}

// EndIngestSection marks the end of an ingest section and unpauses background flushes.
func (fs *FS) EndIngestSection() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	gid := curGoroutineID()
	if fs.ingestDepth > 0 {
		fs.ingestDepth--
	}
	if fs.ingestGoroutines != nil && fs.ingestGoroutines[gid] > 0 {
		fs.ingestGoroutines[gid]--
		if fs.ingestGoroutines[gid] == 0 {
			delete(fs.ingestGoroutines, gid)
		}
	}
	if fs.ingestDepth == 0 || len(fs.ingestGoroutines) == 0 {
		fs.ingestDepth = 0
		clear(fs.ingestGoroutines)
		fs.inIngestSection = false
		fs.ingestGoroutineID = 0
		if fs.ingestCond != nil {
			fs.ingestCond.Broadcast()
		}
	}
}

// ErrUnrepresentablePath is returned, without calling the wrapped filesystem,
// for a name whose trace spelling would not name the same file.
var ErrUnrepresentablePath = errors.New("vfsrec: path cannot be recorded faithfully")

// canonical returns the trace spelling of a filesystem name. The recorded
// profile uses Pebble's in-memory filesystem, whose only separator is "/",
// which ignores leading separators and collapses repeated ones, and which
// does not resolve "." or ".." components. Only those equivalences are
// applied, so the trace path always names the file the call touched. Any
// other spelling (a "." or ".." component, a backslash, a drive letter or a
// trailing separator) has no faithful trace representation, and the call is
// refused with ErrUnrepresentablePath instead of being recorded under a
// different name. The whole name "." means the root, as it does in MemFS.
func canonical(name string) (string, error) {
	p := strings.TrimLeft(name, "/")
	if p == "" || p == "." {
		return "", nil
	}
	parts := strings.Split(p, "/")
	kept := parts[:0]
	for index, part := range parts {
		switch {
		case part == "" && index < len(parts)-1:
			continue // repeated separator
		case part == "", part == ".", part == "..", strings.ContainsAny(part, "\\:"):
			return "", fmt.Errorf("%w: %q", ErrUnrepresentablePath, name)
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "/"), nil
}

func canonicalNonRoot(name string) (string, error) {
	canon, err := canonical(name)
	if err != nil {
		return "", err
	}
	if canon == "" {
		return "", fmt.Errorf("%w: %q", ErrUnrepresentablePath, name)
	}
	return canon, nil
}

// Create creates the named file for reading and writing.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(name)
	if err != nil {
		return nil, err
	}
	innerFile, err := fs.inner.Create(name, category)
	event := trace.Event{
		Kind: trace.KindCreate,
		File: canon,
		Cat:  string(category),
	}
	if err != nil {
		event.Err = err.Error()
		fs.appendEventLocked(event)
		return nil, err
	}
	if fs.isDBManifestPathLocked(canon) {
		fs.recountDBManifestsLocked()
	}

	fs.nextHandle++
	event.Handle = fs.nextHandle
	fs.appendEventLocked(event)
	return &recFile{
		fs:       fs,
		inner:    innerFile,
		handle:   fs.nextHandle,
		label:    canon,
		isDir:    false,
		writable: true,
		cat:      category,
	}, nil
}

// Link creates newname as a hard link to oldname.
func (fs *FS) Link(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canonOld, err := canonicalNonRoot(oldname)
	if err != nil {
		return err
	}
	canonNew, err := canonicalNonRoot(newname)
	if err != nil {
		return err
	}
	err = fs.inner.Link(oldname, newname)
	event := trace.Event{
		Kind:  trace.KindLink,
		File:  canonOld,
		File2: canonNew,
	}
	if err != nil {
		event.Err = err.Error()
	} else if fs.isDBManifestPathLocked(canonNew) {
		fs.recountDBManifestsLocked()
	}
	fs.appendEventLocked(event)
	return err
}

// Open opens the named file for reading.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(name)
	if err != nil {
		return nil, err
	}
	innerFile, err := fs.inner.Open(name, opts...)
	event := trace.Event{
		Kind: trace.KindOpen,
		File: canon,
	}
	if err != nil {
		event.Err = err.Error()
		fs.appendEventLocked(event)
		return nil, err
	}

	fs.nextHandle++
	event.Handle = fs.nextHandle
	fs.appendEventLocked(event)
	return &recFile{
		fs:     fs,
		inner:  innerFile,
		handle: fs.nextHandle,
		label:  canon,
		isDir:  false,
	}, nil
}

// OpenReadWrite opens the named file for reading and writing, creating it if absent.
func (fs *FS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(name)
	if err != nil {
		return nil, err
	}
	_, statErr := fs.inner.Stat(name)
	wasAbsent := errors.Is(statErr, os.ErrNotExist) || os.IsNotExist(statErr)

	innerFile, err := fs.inner.OpenReadWrite(name, category, opts...)
	event := trace.Event{
		Kind: trace.KindOpenReadWrite,
		File: canon,
		Cat:  string(category),
	}
	if err != nil {
		event.Err = err.Error()
		fs.appendEventLocked(event)
		return nil, err
	}

	fs.nextHandle++
	event.Handle = fs.nextHandle
	if wasAbsent {
		event.Created = true
		if fs.isDBManifestPathLocked(canon) {
			fs.recountDBManifestsLocked()
		}
	}
	fs.appendEventLocked(event)
	return &recFile{
		fs:       fs,
		inner:    innerFile,
		handle:   fs.nextHandle,
		label:    canon,
		isDir:    false,
		writable: true,
		cat:      category,
	}, nil
}

// OpenDir opens the named directory for syncing.
func (fs *FS) OpenDir(name string) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonical(name)
	if err != nil {
		return nil, err
	}
	innerFile, err := fs.inner.OpenDir(name)
	event := trace.Event{
		Kind: trace.KindOpenDir,
		File: canon,
		Dir:  true,
	}
	if err != nil {
		event.Err = err.Error()
		fs.appendEventLocked(event)
		return nil, err
	}

	fs.nextHandle++
	event.Handle = fs.nextHandle
	fs.appendEventLocked(event)
	return &recFile{
		fs:     fs,
		inner:  innerFile,
		handle: fs.nextHandle,
		label:  canon,
		isDir:  true,
	}, nil
}

// Remove removes the named file or directory.
func (fs *FS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(name)
	if err != nil {
		return err
	}

	base := baseName(canon)
	isStagingPath := canon == "ingest_staging" || strings.HasPrefix(canon, "ingest_staging/")
	isIngestNamed := isStagingPath || strings.HasPrefix(base, "ingest_")
	isMarkerManifest := strings.HasPrefix(base, "marker.manifest.")
	isIngestOp := isIngestNamed
	if !isIngestOp && fs.inIngestSection {
		gid := curGoroutineID()
		if (fs.ingestGoroutines != nil && fs.ingestGoroutines[gid] > 0) || (fs.ingestGoroutineID != 0 && gid == fs.ingestGoroutineID) {
			isIngestOp = true
		}
	}

	if fs.inIngestSection && !isIngestOp && !fs.isDBManifestPathLocked(canon) && !isMarkerManifest {
		if fs.ingestCond == nil {
			fs.ingestCond = sync.NewCond(&fs.mu)
		}
		if timedOut, timeout := fs.waitCondWithTimeoutLocked(fs.ingestCond, func() bool {
			return !fs.inIngestSection
		}); timedOut {
			timeoutErr := fmt.Errorf("vfsrec: FS.Remove ingestCond wait exceeded %v deadline (ingest section never ended)", timeout)
			fs.appendEventLocked(trace.Event{
				Kind: trace.KindRemove,
				File: canon,
				Err:  timeoutErr.Error(),
			})
			return timeoutErr
		}
	}

	if isStagingPath && fs.pendingManifestDeletions > 0 && fs.manifestCond != nil {
		if timedOut, timeout := fs.waitCondWithTimeoutLocked(fs.manifestCond, func() bool {
			return fs.pendingManifestDeletions <= 0
		}); timedOut {
			timeoutErr := fmt.Errorf("vfsrec: FS.Remove manifestCond wait exceeded %v deadline", timeout)
			fs.appendEventLocked(trace.Event{
				Kind: trace.KindRemove,
				File: canon,
				Err:  timeoutErr.Error(),
			})
			return timeoutErr
		}
	}

	err = fs.inner.Remove(name)
	if fs.isDBManifestPathLocked(canon) {
		fs.recountDBManifestsLocked()
	}
	if err == nil && fs.listenerCreated && fs.isPrimaryDBDirLocked(canon) && isMarkerManifest && fs.countManifestsInDirLocked(dirOf(canon)) > 2 {
		fs.pendingManifestDeletions++
	}
	event := trace.Event{
		Kind: trace.KindRemove,
		File: canon,
	}
	if err != nil {
		event.Err = err.Error()
	}
	fs.appendEventLocked(event)
	return err
}

// RemoveAll removes the named file or directory and any children.
func (fs *FS) RemoveAll(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(name)
	if err != nil {
		return err
	}
	err = fs.inner.RemoveAll(name)
	event := trace.Event{
		Kind: trace.KindRemoveAll,
		File: canon,
	}
	if err != nil {
		event.Err = err.Error()
	}
	fs.recountDBManifestsLocked()
	fs.appendEventLocked(event)
	return err
}

// Rename renames a file, overwriting the destination if it exists.
func (fs *FS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canonOld, err := canonicalNonRoot(oldname)
	if err != nil {
		return err
	}
	canonNew, err := canonicalNonRoot(newname)
	if err != nil {
		return err
	}
	err = fs.inner.Rename(oldname, newname)
	event := trace.Event{
		Kind:  trace.KindRename,
		File:  canonOld,
		File2: canonNew,
	}
	if err != nil {
		event.Err = err.Error()
	}
	fs.recountDBManifestsLocked()
	fs.appendEventLocked(event)
	return err
}

// ReuseForWrite reuses an existing file for writing under a new name without truncation.
func (fs *FS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canonOld, err := canonicalNonRoot(oldname)
	if err != nil {
		return nil, err
	}
	canonNew, err := canonicalNonRoot(newname)
	if err != nil {
		return nil, err
	}
	innerFile, err := fs.inner.ReuseForWrite(oldname, newname, category)
	event := trace.Event{
		Kind:  trace.KindReuseForWrite,
		File:  canonOld,
		File2: canonNew,
		Cat:   string(category),
	}
	fs.recountDBManifestsLocked()
	if err != nil {
		event.Err = err.Error()
		fs.appendEventLocked(event)
		return nil, err
	}

	fs.nextHandle++
	event.Handle = fs.nextHandle
	fs.appendEventLocked(event)
	return &recFile{
		fs:       fs,
		inner:    innerFile,
		handle:   fs.nextHandle,
		label:    canonNew,
		isDir:    false,
		writable: true,
		cat:      category,
	}, nil
}

// MkdirAll creates a directory and all parent directories.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(dir)
	if err != nil {
		return err
	}
	err = fs.inner.MkdirAll(dir, perm)
	event := trace.Event{
		Kind: trace.KindMkdirAll,
		File: canon,
		Mode: uint32(perm),
	}
	if err != nil {
		event.Err = err.Error()
	}
	fs.appendEventLocked(event)
	return err
}

// Lock places an exclusive lock on the given file.
func (fs *FS) Lock(name string) (io.Closer, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonicalNonRoot(name)
	if err != nil {
		return nil, err
	}
	closer, err := fs.inner.Lock(name)
	event := trace.Event{
		Kind: trace.KindLock,
		File: canon,
	}
	if err != nil {
		event.Err = err.Error()
		fs.appendEventLocked(event)
		return nil, err
	}

	fs.nextHandle++
	event.Handle = fs.nextHandle
	fs.appendEventLocked(event)
	return &recCloser{
		fs:     fs,
		inner:  closer,
		handle: fs.nextHandle,
		label:  canon,
	}, nil
}

// List returns a listing of the directory contents.
func (fs *FS) List(dir string) ([]string, error) {
	return fs.inner.List(dir)
}

// Stat returns FileInfo describing the named file.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	return fs.inner.Stat(name)
}

// PathBase returns the last element of path.
func (fs *FS) PathBase(path string) string {
	return fs.inner.PathBase(path)
}

// PathJoin joins multiple path elements.
func (fs *FS) PathJoin(elem ...string) string {
	return fs.inner.PathJoin(elem...)
}

// PathDir returns all but the last element of path.
func (fs *FS) PathDir(path string) string {
	return fs.inner.PathDir(path)
}

// GetDiskUsage returns disk space statistics.
func (fs *FS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return fs.inner.GetDiskUsage(path)
}

// Unwrap returns the wrapped underlying filesystem.
func (fs *FS) Unwrap() vfs.FS {
	return fs.inner
}

type recCloser struct {
	fs     *FS
	inner  io.Closer
	handle uint64
	label  string
	closed bool
}

func (c *recCloser) Close() error {
	c.fs.mu.Lock()
	defer c.fs.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var err error
	if c.inner != nil {
		err = c.inner.Close()
	}
	event := trace.Event{
		Kind:   trace.KindUnlock,
		File:   c.label,
		Handle: c.handle,
	}
	if err != nil {
		event.Err = err.Error()
	}
	c.fs.appendEventLocked(event)
	return err
}
