package vfsrec

import (
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type recFile struct {
	mu       sync.Mutex
	fs       *FS
	inner    vfs.File
	handle   uint64
	label    string
	isDir    bool
	writable bool
	cat      vfs.DiskWriteCategory
	closed   bool
	cursor   int64
}

var _ vfs.File = (*recFile)(nil)

func (f *recFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	var err error
	if f.inner != nil {
		err = f.inner.Close()
	}
	event := trace.Event{
		Kind:   trace.KindClose,
		File:   f.label,
		Handle: f.handle,
		Dir:    f.isDir,
	}
	if err != nil {
		event.Err = err.Error()
	}
	f.fs.appendEventLocked(event)
	return err
}

func (f *recFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	off := f.cursor
	size := int64(len(p))
	n, err := f.inner.Read(p)
	if n > 0 {
		f.cursor += int64(n)
	} else if n < 0 {
		n = 0
	}

	event := trace.Event{
		Kind:   trace.KindRead,
		File:   f.label,
		Handle: f.handle,
		Off:    off,
		Len:    n,
		Size:   size,
	}
	if err != nil {
		event.Err = err.Error()
	}
	f.fs.appendEventLocked(event)
	return n, err
}

func (f *recFile) recordWriteLocked(kind trace.Kind, off int64, p []byte, n int, err error) (int, error) {
	if n < 0 {
		n = 0
	}
	event := trace.Event{
		Kind:   kind,
		File:   f.label,
		Handle: f.handle,
		Off:    off,
		Len:    n,
		Data:   p,
	}
	if err != nil {
		event.Err = err.Error()
	} else if n < len(p) {
		err = io.ErrShortWrite
		event.Err = err.Error()
	}
	f.fs.appendEventLocked(event)
	if err == nil && f.fs.isDBWALPathLocked(f.label) && (f.cat == "pebble-wal" || f.cat == vfs.WriteCategoryUnspecified || f.cat == "") {
		f.fs.walWrites++
		if f.fs.walCond != nil {
			f.fs.walCond.Broadcast()
		}
	}
	return n, err
}

func (f *recFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	dataCopy := bytes.Clone(p)
	off := f.cursor
	n, err := f.inner.Write(p)
	if n > 0 {
		f.cursor += int64(n)
	}
	return f.recordWriteLocked(trace.KindWrite, off, dataCopy, n, err)
}

func (f *recFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}
	if off < 0 {
		return 0, &os.PathError{Op: "writeat", Path: f.label, Err: os.ErrInvalid}
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	dataCopy := bytes.Clone(p)
	n, err := f.inner.WriteAt(p, off)
	return f.recordWriteLocked(trace.KindWriteAt, off, dataCopy, n, err)
}

func (f *recFile) appendSyncOrEventLocked(event trace.Event) {
	if !f.isDir && event.IsBarrier() {
		f.fs.appendSyncEventLocked(event)
	} else {
		f.fs.appendEventLocked(event)
	}
}

func (f *recFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	err := f.inner.Sync()
	event := trace.Event{
		Kind:   trace.KindSync,
		File:   f.label,
		Handle: f.handle,
		Dir:    f.isDir,
	}
	if err != nil {
		event.Err = err.Error()
	}
	f.appendSyncOrEventLocked(event)
	return err
}

func (f *recFile) SyncData() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}
	if f.isDir {
		return &os.PathError{Op: "syncdata", Path: f.label, Err: os.ErrInvalid}
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	err := f.inner.SyncData()
	event := trace.Event{
		Kind:   trace.KindSyncData,
		File:   f.label,
		Handle: f.handle,
		Dir:    f.isDir,
	}
	if err != nil {
		event.Err = err.Error()
	}
	f.appendSyncOrEventLocked(event)
	return err
}

func (f *recFile) SyncTo(length int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return false, os.ErrClosed
	}
	if f.isDir || length < 0 {
		return false, &os.PathError{Op: "syncto", Path: f.label, Err: os.ErrInvalid}
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	fullSync, err := f.inner.SyncTo(length)
	event := trace.Event{
		Kind:     trace.KindSyncTo,
		File:     f.label,
		Handle:   f.handle,
		Dir:      f.isDir,
		Size:     length,
		FullSync: fullSync,
	}
	if err != nil {
		event.Err = err.Error()
	}
	f.appendSyncOrEventLocked(event)
	return fullSync, err
}

func (f *recFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}
	return f.inner.ReadAt(p, off)
}

func (f *recFile) Stat() (vfs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, os.ErrClosed
	}
	return f.inner.Stat()
}

func (f *recFile) Preallocate(offset, length int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}
	if f.isDir || !f.writable || offset < 0 || length < 0 {
		return &os.PathError{Op: "preallocate", Path: f.label, Err: os.ErrInvalid}
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	err := f.inner.Preallocate(offset, length)
	event := trace.Event{
		Kind:   trace.KindPreallocate,
		File:   f.label,
		Handle: f.handle,
		Off:    offset,
		Size:   length,
	}
	if err != nil {
		event.Err = err.Error()
	}
	f.fs.appendEventLocked(event)
	return err
}

func (f *recFile) Prefetch(offset, length int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}
	return f.inner.Prefetch(offset, length)
}

func (f *recFile) Fd() uintptr {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed || f.inner == nil {
		return 0
	}
	return f.inner.Fd()
}
