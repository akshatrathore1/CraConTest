package vfsrec

import (
	"bytes"
	"io"
	"os"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type recFile struct {
	fs     *FS
	inner  vfs.File
	handle uint64
	label  string
	isDir  bool
	closed bool
	cursor int64
}

var _ vfs.File = (*recFile)(nil)

func (f *recFile) Close() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true

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
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	off := f.cursor
	size := int64(len(p))
	n, err := f.inner.Read(p)
	f.cursor += int64(n)

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

func (f *recFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	dataCopy := bytes.Clone(p)
	off := f.cursor
	n, err := f.inner.Write(p)
	f.cursor += int64(n)

	event := trace.Event{
		Kind:   trace.KindWrite,
		File:   f.label,
		Handle: f.handle,
		Off:    off,
		Len:    n,
		Data:   dataCopy,
	}
	if err != nil {
		event.Err = err.Error()
	} else if n < len(p) {
		err = io.ErrShortWrite
		event.Err = err.Error()
	}
	f.fs.appendEventLocked(event)
	return n, err
}

func (f *recFile) WriteAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	dataCopy := bytes.Clone(p)
	n, err := f.inner.WriteAt(p, off)

	event := trace.Event{
		Kind:   trace.KindWriteAt,
		File:   f.label,
		Handle: f.handle,
		Off:    off,
		Len:    n,
		Data:   dataCopy,
	}
	if err != nil {
		event.Err = err.Error()
	} else if n < len(p) {
		err = io.ErrShortWrite
		event.Err = err.Error()
	}
	f.fs.appendEventLocked(event)
	return n, err
}

func (f *recFile) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}

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
	f.fs.appendEventLocked(event)
	return err
}

func (f *recFile) SyncData() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return os.ErrClosed
	}

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
	f.fs.appendEventLocked(event)
	return err
}

func (f *recFile) SyncTo(length int64) (bool, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if f.closed {
		return false, os.ErrClosed
	}

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
	f.fs.appendEventLocked(event)
	return fullSync, err
}

func (f *recFile) ReadAt(p []byte, off int64) (int, error) {
	return f.inner.ReadAt(p, off)
}

func (f *recFile) Stat() (vfs.FileInfo, error) {
	return f.inner.Stat()
}

func (f *recFile) Preallocate(offset, length int64) error {
	return f.inner.Preallocate(offset, length)
}

func (f *recFile) Prefetch(offset, length int64) error {
	return f.inner.Prefetch(offset, length)
}

func (f *recFile) Fd() uintptr {
	return f.inner.Fd()
}
