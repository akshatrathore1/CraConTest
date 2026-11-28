// Package recovery provides crash-recovery utilities for Pebble databases
// running on in-memory filesystems. It handles panic-safe opening of Pebble
// databases after a simulated crash, with timeout protection and structural
// integrity checks.
package recovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// PanicLogger is an Options.Logger that converts Fatalf calls into panics
// instead of calling os.Exit(1), which is Pebble's default behavior.
// This allows the crash-recovery harness to catch fatal errors on the calling
// goroutine via recover(). Note that Fatalf calls originating on Pebble
// background goroutines cannot be caught by a caller-goroutine recover().
type PanicLogger struct{}

// PanicError indicates that CheckIntegrity or DumpContents recovered a panic
// (including PanicLogger.Fatalf on the calling goroutine).
type PanicError struct {
	Op    string
	Value any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic during %s: %v", e.Op, e.Value)
}

// Infof logs informational messages (no-op in the recovery context).
func (PanicLogger) Infof(format string, args ...any) {}

// Errorf logs error messages (no-op in the recovery context).
func (PanicLogger) Errorf(format string, args ...any) {}

// Fatalf converts a fatal log into a panic so it can be caught by recover().
func (PanicLogger) Fatalf(format string, args ...any) {
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
// uncooperative VFS operation cannot be preempted by the Go runtime on timeout.
// The open goroutine closes a DB that finishes after the deadline. It deliberately
// does not inject cancellation errors into the VFS: Pebble treats fsync errors as
// unrecoverable, and returning a synthetic deadline error from an in-flight sync can
// panic a background flush after SafeOpen has already returned.
func SafeOpen(fs vfs.FS, dir string, timeout time.Duration) OpenResult {
	if fs == nil {
		return OpenResult{Err: errors.New("safe open: fs is nil")}
	}
	if timeout <= 0 {
		return OpenResult{Err: fmt.Errorf("safe open: timeout %v must be > 0", timeout)}
	}

	type result struct {
		db       *pebble.DB
		err      error
		panicked bool
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ch := make(chan result, 1)
	var mu sync.Mutex
	timedOut := false

	go func() {
		defer func() {
			if r := recover(); r != nil {
				mu.Lock()
				defer mu.Unlock()
				if !timedOut && ctx.Err() == nil {
					ch <- result{err: fmt.Errorf("%v", r), panicked: true}
				}
			}
		}()

		opts := &pebble.Options{
			FS:                          fs,
			Logger:                      PanicLogger{},
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
		ch <- result{db: db, err: err}
		mu.Unlock()
	}()

	select {
	case r := <-ch:
		if r.panicked {
			msg := r.err.Error()
			if msg == "" {
				msg = "empty panic"
			}
			return OpenResult{Panic: msg}
		}
		if r.err != nil {
			return OpenResult{Err: r.err}
		}
		return OpenResult{DB: r.db}
	case <-ctx.Done():
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
// CheckLevels and a full forward+backward iteration scan comparing both keys
// and value digests. Caller-goroutine panics are surfaced as *PanicError.
func CheckIntegrity(db *pebble.DB) (err error) {
	if db == nil {
		return errors.New("check integrity: db is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Op: "integrity check", Value: r}
		}
	}()

	if err := db.CheckLevels(nil); err != nil {
		return fmt.Errorf("CheckLevels: %w", err)
	}

	// Forward scan: collect all keys and value digests.
	fwdIt, fwdErr := db.NewIter(nil)
	forward, err := collectEntries(fwdIt, fwdErr, false)
	if err != nil {
		return fmt.Errorf("forward iteration: %w", err)
	}

	// Backward scan: collect all keys and value digests in reverse.
	bwdIt, bwdErr := db.NewIter(nil)
	backward, err := collectEntries(bwdIt, bwdErr, true)
	if err != nil {
		return fmt.Errorf("backward iteration: %w", err)
	}

	// Compare both keys and values.
	if len(forward) != len(backward) {
		return fmt.Errorf("key count mismatch: forward=%d backward=%d",
			len(forward), len(backward))
	}
	for i := range forward {
		rev := backward[len(backward)-1-i]
		if forward[i].key != rev.key {
			return fmt.Errorf("key mismatch at index %d: forward=%q backward=%q",
				i, forward[i].key, rev.key)
		}
		if forward[i].valLen != rev.valLen || forward[i].valHash != rev.valHash {
			return fmt.Errorf("value mismatch at key %q (index %d): forward_len=%d backward_len=%d",
				forward[i].key, i, forward[i].valLen, rev.valLen)
		}
	}

	return nil
}

// DumpContents returns all key-value pairs in the database as a map.
// Caller-goroutine panics are surfaced as *PanicError.
func DumpContents(db *pebble.DB) (result map[string]string, err error) {
	if db == nil {
		return nil, errors.New("dump contents: db is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Op: "dump contents", Value: r}
		}
	}()

	result = make(map[string]string)
	it, err := db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = it.Close()
		}
	}()

	for valid := it.First(); valid; valid = it.Next() {
		key := string(it.Key())
		val, valErr := it.ValueAndErr()
		if valErr != nil {
			closeErr := it.Close()
			closed = true
			return nil, fmt.Errorf("value at key %q: %w", key, errors.Join(valErr, closeErr))
		}
		result[key] = string(val)
	}
	iterErr := it.Error()
	closeErr := it.Close()
	closed = true
	if err := errors.Join(iterErr, closeErr); err != nil {
		return nil, fmt.Errorf("iterator: %w", err)
	}

	return result, nil
}

type kvDigest struct {
	key     string
	valLen  int
	valHash [32]byte
}

func collectEntries(it *pebble.Iterator, err error, reverse bool) ([]kvDigest, error) {
	if err != nil {
		return nil, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = it.Close()
		}
	}()
	var entries []kvDigest
	valid := false
	if reverse {
		valid = it.Last()
	} else {
		valid = it.First()
	}
	for ; valid; valid = nextEntry(it, reverse) {
		val, valErr := it.ValueAndErr()
		if valErr != nil {
			closeErr := it.Close()
			closed = true
			return nil, errors.Join(valErr, closeErr)
		}
		entries = append(entries, kvDigest{
			key:     string(it.Key()),
			valLen:  len(val),
			valHash: sha256.Sum256(val),
		})
	}
	iterErr := it.Error()
	closeErr := it.Close()
	closed = true
	if err := errors.Join(iterErr, closeErr); err != nil {
		return nil, err
	}
	return entries, nil
}

func nextEntry(it *pebble.Iterator, reverse bool) bool {
	if reverse {
		return it.Prev()
	}
	return it.Next()
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
		closed := false
		defer func() {
			if !closed {
				_ = it.Close()
			}
		}()
		for valid := it.First(); valid; valid = it.Next() {
			val, err := it.ValueAndErr()
			if err != nil {
				panic(fmt.Sprintf("recovery: AllEntries iterator step failed: %v", err))
			}
			if !yield(string(it.Key()), string(val)) {
				break
			}
		}
		iterErr := it.Error()
		closeErr := it.Close()
		closed = true
		if err := errors.Join(iterErr, closeErr); err != nil {
			panic(fmt.Sprintf("recovery: AllEntries iterator closed with error: %v", err))
		}
	}
}
