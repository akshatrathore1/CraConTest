// Package vfsrec provides a recording virtual filesystem wrapper that logs
// mutating filesystem operations and engine lifecycle events into a trace.
package vfsrec

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// FS wraps a vfs.FS implementation and records mutating filesystem calls,
// operation markers, and engine events into a trace.
type FS struct {
	mu         sync.Mutex
	inner      vfs.FS
	nextHandle uint64
	events     []trace.Event
	hook       func(id uint64)
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
	return &FS{
		inner: inner,
	}
}

// Begin records an OpBegin marker event for a client operation.
func (fs *FS) Begin(op uint64, kind string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.appendEventLocked(trace.Event{
		Kind:   trace.KindOpBegin,
		Op:     op,
		OpKind: kind,
	})
}

// End records an OpEnd marker event for a client operation.
func (fs *FS) End(op uint64, kind string, err string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.appendEventLocked(trace.Event{
		Kind:   trace.KindOpEnd,
		Op:     op,
		OpKind: kind,
		Err:    err,
	})
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
	err = fs.inner.Remove(name)
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
