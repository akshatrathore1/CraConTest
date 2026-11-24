// Package vfsrec provides a recording virtual filesystem wrapper that logs
// mutating filesystem operations and engine lifecycle events into a trace.
package vfsrec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
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
	nextEvent                uint64
	events                   []trace.Event
	hook                     func(id uint64)
	waitTimeout              time.Duration
	inIngestSection          bool
	ingestGoroutineID        uint64
	ingestGoroutines         map[uint64]int
	ingestCond               *sync.Cond
	walWrites                uint64
	walCond                  *sync.Cond
	dbManifestCount          int
	pendingManifestDeletions int
	manifestCond             *sync.Cond
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
	return fs
}

// isPrimaryDBDir reports whether path resides in the root directory ("") or a
// primary database directory ("db", "db_*", or "testdb"), excluding secondary
// or staging directories such as "checkpoint/", "ingest_staging/", or unrelated folders.
func isPrimaryDBDir(path string) bool {
	slash := strings.IndexByte(path, '/')
	if slash < 0 {
		return path != ""
	}
	if strings.IndexByte(path[slash+1:], '/') >= 0 {
		return false
	}
	dir := path[:slash]
	return dir == "db" || strings.HasPrefix(dir, "db_") || dir == "testdb"
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
	if !isPrimaryDBDir(path) {
		return false
	}
	base := path
	if slash := strings.LastIndexByte(path, '/'); slash >= 0 {
		base = path[slash+1:]
	}
	return !strings.HasPrefix(base, "marker.") && strings.HasPrefix(base, "MANIFEST-")
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
	fs.walWrites++
	if fs.walCond != nil {
		fs.walCond.Broadcast()
	}
	fs.mu.Unlock()
}

func defaultWaitTimeout() time.Duration {
	if env := os.Getenv("CRACONTEST_WAIT_TIMEOUT"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Second
}

// WaitForWALWrite blocks until WALWriteCount() >= target, panicking if a safety
// deadline expires before the expected WAL write is recorded.
func (fs *FS) WaitForWALWrite(target uint64) {
	fs.mu.Lock()
	if fs.walWrites >= target {
		fs.mu.Unlock()
		return
	}
	if fs.walCond == nil {
		fs.walCond = sync.NewCond(&fs.mu)
	}
	timeout := fs.waitTimeout
	if timeout <= 0 {
		timeout = defaultWaitTimeout()
	}
	var timedOut bool
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			fs.mu.Lock()
			timedOut = true
			fs.walCond.Broadcast()
			fs.mu.Unlock()
		case <-done:
		}
	}()
	for fs.walWrites < target {
		if timedOut {
			close(done)
			fs.mu.Unlock()
			panic(fmt.Sprintf("vfsrec: WaitForWALWrite exceeded %v deadline", timeout))
		}
		fs.walCond.Wait()
	}
	close(done)
	fs.mu.Unlock()
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
	return fs.events[len(fs.events)-1].ID
}

// CurrentEventID returns the ID of the most recently recorded trace event (0 if none).
func (fs *FS) CurrentEventID() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.events) == 0 {
		return 0
	}
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

// syncReorderScanWindow bounds the backward scan over trailing non-mutating
// events when placing a file Sync/SyncData/SyncTo adjacent to its preceding
// Write/WriteAt on the same handle.
const syncReorderScanWindow = 8

// appendSyncEventLocked repositions a Sync/SyncData/SyncTo barrier event adjacent
// to its preceding Write/WriteAt on the same handle when only non-mutating events
// (such as Read, Stat, Close on unrelated handles, or EngineEvent markers) intervene.
//
// Boundary Invariants:
//  1. Causal & Mutation Barrier: Reordering stops immediately (breaks scan) upon
//     encountering any mutating event (e.IsMutation() is true, excluding EngineEvent).
//     A sync barrier is never moved before another handle's mutating operation.
//  2. Same-Handle Boundary: Reordering stops immediately if another event on the
//     same handle is encountered (e.Handle == event.Handle). A sync is never moved
//     across earlier operations on the same file.
//  3. Concurrency & Transaction Boundary: Reordering stops immediately upon encountering
//     KindLock, KindUnlock, KindOpBegin, or KindOpEnd. Syncs are never reordered across
//     synchronization or client-visible transaction boundaries.
//  4. Bounded Scan Window: The backward scan checks at most syncReorderScanWindow (8)
//     events. If the preceding Write/WriteAt is farther back, the event is appended
//     to the trace tail without reordering.
//  5. Sequential ID Invariant: If inserted, trailing events are shifted and all IDs
//     from insertIdx onwards are re-indexed to strictly preserve the 1-based contiguous
//     trace invariant: events[k].ID == k+1 for all k in [0, len(fs.events)).
//     If a hook is registered, it is called with the final assigned event ID.
func (fs *FS) appendSyncEventLocked(event trace.Event) {
	insertIdx := -1
	n := len(fs.events)
	for i := n - 1; i >= 0 && (n-1-i) < syncReorderScanWindow; i-- {
		e := fs.events[i]
		if e.Handle == event.Handle || e.Kind == trace.KindLock || e.Kind == trace.KindUnlock || e.Kind == trace.KindOpBegin || e.Kind == trace.KindOpEnd {
			break
		}
		isNonMutating := !e.IsMutation() || e.Kind == trace.KindEngineEvent
		if !isNonMutating {
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

	// Insert event at insertIdx and shift trailing non-mutating events right by 1
	fs.events = append(fs.events, trace.Event{})
	copy(fs.events[insertIdx+1:], fs.events[insertIdx:n])
	event.ID = uint64(insertIdx + 1)
	fs.events[insertIdx] = event
	for j := insertIdx + 1; j < len(fs.events); j++ {
		fs.events[j].ID = uint64(j + 1)
	}
	fs.nextEvent = uint64(len(fs.events))

	if fs.hook != nil {
		fs.hook(event.ID)
	}
}

func curGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	if n == 0 {
		return 0
	}
	b := buf[:n]
	prefix := []byte("goroutine ")
	idx := bytes.Index(b, prefix)
	if idx < 0 {
		return 0
	}
	b = b[idx+len(prefix):]
	for len(b) > 0 && b[0] == ' ' {
		b = b[1:]
	}
	var id uint64
	for len(b) > 0 && b[0] >= '0' && b[0] <= '9' {
		id = id*10 + uint64(b[0]-'0')
		b = b[1:]
	}
	return id
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
	gid := curGoroutineID()
	if fs.ingestGoroutines == nil {
		fs.ingestGoroutines = make(map[uint64]int)
	}
	fs.ingestGoroutines[gid]++
	fs.inIngestSection = true
	fs.ingestGoroutineID = gid
	fs.mu.Unlock()
}

// EndIngestSection marks the end of an ingest section and unpauses background flushes.
func (fs *FS) EndIngestSection() {
	fs.mu.Lock()
	gid := curGoroutineID()
	if fs.ingestGoroutines != nil && fs.ingestGoroutines[gid] > 0 {
		fs.ingestGoroutines[gid]--
		if fs.ingestGoroutines[gid] == 0 {
			delete(fs.ingestGoroutines, gid)
		}
	}
	if len(fs.ingestGoroutines) == 0 {
		fs.inIngestSection = false
		fs.ingestGoroutineID = 0
		if fs.ingestCond != nil {
			fs.ingestCond.Broadcast()
		}
	}
	fs.mu.Unlock()
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

// Create creates the named file for reading and writing.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonical(name)
	if err != nil {
		return nil, err
	}
	if isDBManifestPath(canon) {
		fs.dbManifestCount++
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

	fs.nextHandle++
	event.Handle = fs.nextHandle
	fs.appendEventLocked(event)
	return &recFile{
		fs:     fs,
		inner:  innerFile,
		handle: fs.nextHandle,
		label:  canon,
		isDir:  false,
		cat:    category,
	}, nil
}

// Link creates newname as a hard link to oldname.
func (fs *FS) Link(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canonOld, err := canonical(oldname)
	if err != nil {
		return err
	}
	canonNew, err := canonical(newname)
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
	}
	fs.appendEventLocked(event)
	return err
}

// Open opens the named file for reading.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonical(name)
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

	canon, err := canonical(name)
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
	}
	fs.appendEventLocked(event)
	return &recFile{
		fs:     fs,
		inner:  innerFile,
		handle: fs.nextHandle,
		label:  canon,
		isDir:  false,
		cat:    category,
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

	canon, err := canonical(name)
	if err != nil {
		return err
	}

	isIngestOp := false
	if strings.Contains(canon, "ingest_staging") || strings.Contains(canon, "ingest_") {
		isIngestOp = true
	} else if fs.inIngestSection {
		gid := curGoroutineID()
		if (fs.ingestGoroutines != nil && fs.ingestGoroutines[gid] > 0) || (fs.ingestGoroutineID != 0 && gid == fs.ingestGoroutineID) {
			isIngestOp = true
		} else {
			// Defensively walk stack frames to detect ingest callers.
			var pcs [32]uintptr
			n := runtime.Callers(2, pcs[:])
			if n > 0 {
				frames := runtime.CallersFrames(pcs[:n])
				for {
					frame, more := frames.Next()
					fnLower := strings.ToLower(frame.Function)
					fileLower := strings.ToLower(frame.File)
					if strings.Contains(fnLower, "ingest") || strings.Contains(fileLower, "ingest") {
						isIngestOp = true
						break
					}
					if !more {
						break
					}
				}
			}
			// Safe fallback: if stack frames could not be matched, check file path.
			if !isIngestOp && strings.Contains(strings.ToLower(canon), "ingest") {
				isIngestOp = true
			}
		}
	}

	if fs.inIngestSection && !isIngestOp && !isDBManifestPath(canon) && !strings.Contains(canon, "ingest_staging") && !strings.Contains(canon, "ingest_") && !strings.Contains(canon, "marker.manifest") {
		if fs.ingestCond == nil {
			fs.ingestCond = sync.NewCond(&fs.mu)
		}
		timeout := fs.waitTimeout
		if timeout <= 0 {
			timeout = defaultWaitTimeout()
		}
		var timedOut bool
		done := make(chan struct{})
		go func() {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				fs.mu.Lock()
				timedOut = true
				fs.ingestCond.Broadcast()
				fs.mu.Unlock()
			case <-done:
			}
		}()
		for fs.inIngestSection {
			if timedOut {
				close(done)
				timeoutErr := fmt.Errorf("vfsrec: FS.Remove ingestCond wait exceeded %v deadline (ingest section never ended)", timeout)
				event := trace.Event{
					Kind: trace.KindRemove,
					File: canon,
					Err:  timeoutErr.Error(),
				}
				fs.appendEventLocked(event)
				return timeoutErr
			}
			fs.ingestCond.Wait()
		}
		close(done)
	}

	if strings.Contains(canon, "ingest_staging") && fs.pendingManifestDeletions > 0 && fs.manifestCond != nil {
		timeout := fs.waitTimeout
		if timeout <= 0 {
			timeout = defaultWaitTimeout()
		}
		var timedOut bool
		done := make(chan struct{})
		go func() {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				fs.mu.Lock()
				timedOut = true
				fs.manifestCond.Broadcast()
				fs.mu.Unlock()
			case <-done:
			}
		}()
		for fs.pendingManifestDeletions > 0 {
			if timedOut {
				close(done)
				timeoutErr := fmt.Errorf("vfsrec: FS.Remove manifestCond wait exceeded %v deadline", timeout)
				event := trace.Event{
					Kind: trace.KindRemove,
					File: canon,
					Err:  timeoutErr.Error(),
				}
				fs.appendEventLocked(event)
				return timeoutErr
			}
			fs.manifestCond.Wait()
		}
		close(done)
	}

	err = fs.inner.Remove(name)
	if err == nil && isDBManifestPath(canon) && fs.dbManifestCount > 0 {
		fs.dbManifestCount--
	}
	if err == nil && isPrimaryDBDir(canon) && strings.Contains(canon, "marker.manifest.") && fs.dbManifestCount > 2 {
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

	canon, err := canonical(name)
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
	fs.appendEventLocked(event)
	return err
}

// Rename renames a file, overwriting the destination if it exists.
func (fs *FS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canonOld, err := canonical(oldname)
	if err != nil {
		return err
	}
	canonNew, err := canonical(newname)
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
	fs.appendEventLocked(event)
	return err
}

// ReuseForWrite reuses an existing file for writing under a new name without truncation.
func (fs *FS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canonOld, err := canonical(oldname)
	if err != nil {
		return nil, err
	}
	canonNew, err := canonical(newname)
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
		label:  canonNew,
		isDir:  false,
		cat:    category,
	}, nil
}

// MkdirAll creates a directory and all parent directories.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	canon, err := canonical(dir)
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

	canon, err := canonical(name)
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
