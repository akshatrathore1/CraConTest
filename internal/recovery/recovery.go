// Package recovery provides crash-recovery utilities for Pebble databases
// running on in-memory filesystems. It handles panic-safe opening of Pebble
// databases after a simulated crash, with timeout protection and structural
// integrity checks.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// PanicLogger is an Options.Logger that converts Fatalf calls into panics
// instead of calling os.Exit(1), which is Pebble's default behavior.
// This allows the crash-recovery harness to catch fatal errors via recover().
type PanicLogger struct{}

// Infof logs informational messages (no-op in the recovery context).
func (PanicLogger) Infof(format string, args ...any) {}

// Errorf logs error messages (no-op in the recovery context).
func (PanicLogger) Errorf(format string, args ...any) {}

// Fatalf converts a fatal log into a panic so it can be caught by recover().
func (PanicLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble fatal: "+format, args...))
}

type cancelLogger struct {
	canceled *atomic.Bool
}

func (cancelLogger) Infof(format string, args ...any)  {}
func (cancelLogger) Errorf(format string, args ...any) {}
func (c cancelLogger) Fatalf(format string, args ...any) {
	if c.canceled != nil && c.canceled.Load() {
		runtime.Goexit()
	}
	panic(fmt.Sprintf("pebble fatal: "+format, args...))
}

// OpenResult holds the outcome of attempting to open a Pebble database
// after a simulated crash.
type OpenResult struct {
	// DB is the opened database, nil if opening failed.
	DB *pebble.DB

	// Err is non-nil if Open returned an error.
	Err error

	// Panic is non-empty if Open triggered a Fatalf panic.
	Panic string

	// Timeout is true if Open did not complete within the deadline.
	Timeout bool
}

// SafeOpen opens a Pebble database at dir on fs with a timeout and panic recovery.
// Any logger fatals are intercepted by PanicLogger and converted to panics,
// which SafeOpen recovers and reports in OpenResult.Panic.
// Because pebble.Open does not accept a context.Context, a call blocked inside an
// uncooperative VFS operation cannot be preempted by the Go runtime on timeout;
// the background drain goroutine acts as a bounded reaper (at most one per timed-out
// candidate) that closes the DB if pebble.Open eventually unblocks.
func SafeOpen(fs vfs.FS, dir string, timeout time.Duration) OpenResult {
	type result struct {
		db  *pebble.DB
		err error
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ch := make(chan result, 1)
	var mu sync.Mutex
	timedOut := false
	var canceled atomic.Bool

	go func() {
		defer func() {
			if r := recover(); r != nil {
				mu.Lock()
				defer mu.Unlock()
				if !timedOut && ctx.Err() == nil {
					ch <- result{nil, fmt.Errorf("panic: %v", r)}
				}
			}
		}()

		cancelableFS := &cancelFS{FS: fs, canceled: &canceled}
		opts := &pebble.Options{
			FS:                          cancelableFS,
			Logger:                      cancelLogger{canceled: &canceled},
			DisableAutomaticCompactions: true,
		}
		db, err := pebble.Open(dir, opts)
		mu.Lock()
		if timedOut || ctx.Err() != nil {
			mu.Unlock()
			// SafeOpen already timed out and returned.
			// Clean up the late-opened DB immediately so resources are never leaked.
			if db != nil {
				_ = db.Close()
			}
			return
		}
		ch <- result{db, err}
		mu.Unlock()
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			// Check if it was a panic-converted error.
			errStr := r.err.Error()
			if strings.HasPrefix(errStr, "panic: ") {
				return OpenResult{Panic: errStr[7:]}
			}
			return OpenResult{Err: r.err}
		}
		return OpenResult{DB: r.db}
	case <-ctx.Done():
		canceled.Store(true)
		mu.Lock()
		timedOut = true
		mu.Unlock()
		// In case ch was filled concurrently right before timedOut was set:
		select {
		case r := <-ch:
			if r.db != nil {
				_ = r.db.Close()
			}
		default:
		}
		return OpenResult{Timeout: true}
	}
}

// CheckIntegrity runs structural checks on an opened Pebble database:
// CheckLevels and a full forward+backward iteration scan.
func CheckIntegrity(db *pebble.DB) (err error) {
	if db == nil {
		return errors.New("check integrity: db is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during integrity check: %v", r)
		}
	}()

	if err := db.CheckLevels(nil); err != nil {
		return fmt.Errorf("CheckLevels: %w", err)
	}

	// Forward scan: collect all keys.
	forward, err := collectKeys(db.NewIter(nil))
	if err != nil {
		return fmt.Errorf("forward iteration: %w", err)
	}

	// Backward scan: collect all keys in reverse.
	backward, err := collectKeysReverse(db.NewIter(nil))
	if err != nil {
		return fmt.Errorf("backward iteration: %w", err)
	}

	// Compare.
	if len(forward) != len(backward) {
		return fmt.Errorf("key count mismatch: forward=%d backward=%d",
			len(forward), len(backward))
	}
	for i := range forward {
		if forward[i] != backward[len(backward)-1-i] {
			return fmt.Errorf("key mismatch at index %d: forward=%q backward=%q",
				i, forward[i], backward[len(backward)-1-i])
		}
	}

	return nil
}

// DumpContents returns all key-value pairs in the database as a map.
func DumpContents(db *pebble.DB) (result map[string]string, err error) {
	if db == nil {
		return nil, errors.New("dump contents: db is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic during dump contents: %v", r)
		}
	}()

	result = make(map[string]string)
	it, err := db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	defer it.Close()

	for valid := it.First(); valid; valid = it.Next() {
		val, err := it.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("value at key %q: %w", it.Key(), err)
		}
		result[string(it.Key())] = string(val)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("iterator: %w", err)
	}

	return result, nil
}

func collectKeys(it *pebble.Iterator, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var keys []string
	for valid := it.First(); valid; valid = it.Next() {
		if _, valErr := it.ValueAndErr(); valErr != nil {
			return nil, valErr
		}
		keys = append(keys, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

func collectKeysReverse(it *pebble.Iterator, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var keys []string
	for valid := it.Last(); valid; valid = it.Prev() {
		if _, valErr := it.ValueAndErr(); valErr != nil {
			return nil, valErr
		}
		keys = append(keys, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

// AllEntries returns an iterator over all key-value pairs in the database.
// This is a convenience wrapper for use with range-over-func.
// If an iterator creation or step error occurs it panics, because such errors
// indicate database corruption that must not be silently swallowed.
func AllEntries(db *pebble.DB) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		if db == nil {
			return
		}
		it, err := db.NewIter(nil)
		if err != nil {
			panic(fmt.Sprintf("recovery: AllEntries iterator creation failed: %v", err))
		}
		defer it.Close()
		for valid := it.First(); valid; valid = it.Next() {
			val, err := it.ValueAndErr()
			if err != nil {
				panic(fmt.Sprintf("recovery: AllEntries iterator step failed: %v", err))
			}
			if !yield(string(it.Key()), string(val)) {
				return
			}
		}
		if err := it.Error(); err != nil {
			panic(fmt.Sprintf("recovery: AllEntries iterator closed with error: %v", err))
		}
	}
}

type cancelFS struct {
	vfs.FS
	canceled *atomic.Bool
}

func (c *cancelFS) check() error {
	if c.canceled.Load() {
		return context.DeadlineExceeded
	}
	return nil
}

func (c *cancelFS) Unwrap() vfs.FS {
	return c.FS
}

func (c *cancelFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	f, err := c.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &cancelFile{File: f, canceled: c.canceled}, nil
}

func (c *cancelFS) Link(oldname, newname string) error {
	if err := c.check(); err != nil {
		return err
	}
	return c.FS.Link(oldname, newname)
}

func (c *cancelFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	f, err := c.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &cancelFile{File: f, canceled: c.canceled}, nil
}

func (c *cancelFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	f, err := c.FS.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	return &cancelFile{File: f, canceled: c.canceled}, nil
}

func (c *cancelFS) OpenDir(name string) (vfs.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	f, err := c.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &cancelFile{File: f, canceled: c.canceled}, nil
}

func (c *cancelFS) Remove(name string) error {
	if err := c.check(); err != nil {
		return err
	}
	return c.FS.Remove(name)
}

func (c *cancelFS) RemoveAll(name string) error {
	if err := c.check(); err != nil {
		return err
	}
	return c.FS.RemoveAll(name)
}

func (c *cancelFS) Rename(oldname, newname string) error {
	if err := c.check(); err != nil {
		return err
	}
	return c.FS.Rename(oldname, newname)
}

func (c *cancelFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	f, err := c.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return &cancelFile{File: f, canceled: c.canceled}, nil
}

func (c *cancelFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := c.check(); err != nil {
		return err
	}
	return c.FS.MkdirAll(dir, perm)
}

func (c *cancelFS) Lock(name string) (io.Closer, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	return c.FS.Lock(name)
}

func (c *cancelFS) List(dir string) ([]string, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	return c.FS.List(dir)
}

func (c *cancelFS) Stat(name string) (vfs.FileInfo, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	return c.FS.Stat(name)
}

type cancelFile struct {
	vfs.File
	canceled *atomic.Bool
}

func (f *cancelFile) Read(p []byte) (int, error) {
	if f.canceled.Load() {
		return 0, context.DeadlineExceeded
	}
	return f.File.Read(p)
}

func (f *cancelFile) ReadAt(p []byte, off int64) (int, error) {
	if f.canceled.Load() {
		return 0, context.DeadlineExceeded
	}
	return f.File.ReadAt(p, off)
}

func (f *cancelFile) Write(p []byte) (int, error) {
	if f.canceled.Load() {
		return 0, context.DeadlineExceeded
	}
	return f.File.Write(p)
}

func (f *cancelFile) WriteAt(p []byte, off int64) (int, error) {
	if f.canceled.Load() {
		return 0, context.DeadlineExceeded
	}
	return f.File.WriteAt(p, off)
}

func (f *cancelFile) Sync() error {
	if f.canceled.Load() {
		return context.DeadlineExceeded
	}
	return f.File.Sync()
}

func (f *cancelFile) SyncData() error {
	if f.canceled.Load() {
		return context.DeadlineExceeded
	}
	return f.File.SyncData()
}

func (f *cancelFile) SyncTo(length int64) (bool, error) {
	if f.canceled.Load() {
		return false, context.DeadlineExceeded
	}
	return f.File.SyncTo(length)
}
