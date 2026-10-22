// Package faults implements seeded fault injection for filesystem operations.
//
// Decorator order is faults(vfsrec(memFS)). When a fault drops a sync, it returns
// nil without invoking the underlying recorder, ensuring that no barrier event
// appears in the trace.
package faults

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// Seeded fault IDs.
const (
	MUT0 = "MUT-0"
	MUT1 = "MUT-1"
)

// IDs returns the list of all supported fault identifiers.
func IDs() []string {
	return []string{MUT0, MUT1}
}

// FS wraps an inner vfs.FS and injects faults based on its configuration.
type FS struct {
	id    string
	inner vfs.FS
}

// Compile-time interface check.
var _ vfs.FS = (*FS)(nil)

// New constructs a fault-injected filesystem wrapper around inner.
// id must be "MUT-0" (null control, pass-through) or "MUT-1" (drops pebble-wal syncs).
func New(id string, inner vfs.FS) (vfs.FS, error) {
	if inner == nil {
		return nil, errors.New("faults: inner FS cannot be nil")
	}
	if id != MUT0 && id != MUT1 {
		return nil, fmt.Errorf("faults: unknown fault id %q (supported: %v)", id, IDs())
	}
	return &FS{
		id:    id,
		inner: inner,
	}, nil
}

// ID returns the active fault identifier.
func (fs *FS) ID() string {
	return fs.id
}

// Unwrap returns the underlying filesystem.
func (fs *FS) Unwrap() vfs.FS {
	return fs.inner
}

func (fs *FS) shouldDropSync(category vfs.DiskWriteCategory) bool {
	return fs.id == MUT1 && string(category) == "pebble-wal"
}

// Create implements vfs.FS.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.inner.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &file{inner: f, dropSync: fs.shouldDropSync(category)}, nil
}

// Link implements vfs.FS.
func (fs *FS) Link(oldname, newname string) error {
	return fs.inner.Link(oldname, newname)
}

// Open implements vfs.FS.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.inner.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &file{inner: f, dropSync: false}, nil
}

// OpenReadWrite implements vfs.FS.
func (fs *FS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.inner.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	return &file{inner: f, dropSync: fs.shouldDropSync(category)}, nil
}

// OpenDir implements vfs.FS.
func (fs *FS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.inner.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &file{inner: f, dropSync: false}, nil
}

// Remove implements vfs.FS.
func (fs *FS) Remove(name string) error {
	return fs.inner.Remove(name)
}

// RemoveAll implements vfs.FS.
func (fs *FS) RemoveAll(name string) error {
	return fs.inner.RemoveAll(name)
}

// Rename implements vfs.FS.
func (fs *FS) Rename(oldname, newname string) error {
	return fs.inner.Rename(oldname, newname)
}

// ReuseForWrite implements vfs.FS.
func (fs *FS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.inner.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return &file{inner: f, dropSync: fs.shouldDropSync(category)}, nil
}

// MkdirAll implements vfs.FS.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.inner.MkdirAll(dir, perm)
}

// Lock implements vfs.FS.
func (fs *FS) Lock(name string) (io.Closer, error) {
	return fs.inner.Lock(name)
}

// List implements vfs.FS.
func (fs *FS) List(dir string) ([]string, error) {
	return fs.inner.List(dir)
}

// Stat implements vfs.FS.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	return fs.inner.Stat(name)
}

// PathBase implements vfs.FS.
func (fs *FS) PathBase(path string) string {
	return fs.inner.PathBase(path)
}

// PathJoin implements vfs.FS.
func (fs *FS) PathJoin(elem ...string) string {
	return fs.inner.PathJoin(elem...)
}

// PathDir implements vfs.FS.
func (fs *FS) PathDir(path string) string {
	return fs.inner.PathDir(path)
}

// GetDiskUsage implements vfs.FS.
func (fs *FS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return fs.inner.GetDiskUsage(path)
}

type file struct {
	inner    vfs.File
	dropSync bool
}

var _ vfs.File = (*file)(nil)

func (f *file) Close() error {
	return f.inner.Close()
}

func (f *file) Read(p []byte) (int, error) {
	return f.inner.Read(p)
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	return f.inner.ReadAt(p, off)
}

func (f *file) Write(p []byte) (int, error) {
	return f.inner.Write(p)
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	return f.inner.WriteAt(p, off)
}

func (f *file) Stat() (vfs.FileInfo, error) {
	return f.inner.Stat()
}

func (f *file) Sync() error {
	if f.dropSync {
		return nil
	}
	return f.inner.Sync()
}

func (f *file) SyncData() error {
	if f.dropSync {
		return nil
	}
	return f.inner.SyncData()
}

func (f *file) SyncTo(length int64) (fullSync bool, err error) {
	if f.dropSync {
		return false, nil
	}
	return f.inner.SyncTo(length)
}

func (f *file) Preallocate(off, length int64) error {
	return f.inner.Preallocate(off, length)
}

func (f *file) Prefetch(off, length int64) error {
	return f.inner.Prefetch(off, length)
}

func (f *file) Fd() uintptr {
	return f.inner.Fd()
}
