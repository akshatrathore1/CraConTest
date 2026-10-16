// Package recovery provides crash-recovery utilities for Pebble databases
// running on in-memory filesystems. It handles panic-safe opening of Pebble
// databases after a simulated crash, with timeout protection and structural
// integrity checks.
package recovery

import (
	"context"
	"fmt"
	"iter"
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

// SafeOpen attempts to open a Pebble database on the given filesystem with
// panic recovery and timeout protection. The returned OpenResult describes
// what happened.
func SafeOpen(fs vfs.FS, dir string, timeout time.Duration) OpenResult {
	type result struct {
		db  *pebble.DB
		err error
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ch := make(chan result, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{nil, fmt.Errorf("panic: %v", r)}
			}
		}()

		opts := &pebble.Options{
			FS:     fs,
			Logger: PanicLogger{},
		}
		db, err := pebble.Open(dir, opts)
		ch <- result{db, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			// Check if it was a panic-converted error.
			errStr := r.err.Error()
			if len(errStr) > 7 && errStr[:7] == "panic: " {
				return OpenResult{Panic: errStr[7:]}
			}
			return OpenResult{Err: r.err}
		}
		return OpenResult{DB: r.db}
	case <-ctx.Done():
		return OpenResult{Timeout: true}
	}
}

// CheckIntegrity runs structural checks on an opened Pebble database:
// CheckLevels and a full forward+backward iteration scan.
func CheckIntegrity(db *pebble.DB) error {
	if err := db.CheckLevels(nil); err != nil {
		return fmt.Errorf("CheckLevels: %w", err)
	}

	// Forward scan: collect all keys.
	forward, err := collectKeys(db.NewIter(nil))
	if err != nil {
		return fmt.Errorf("forward iteration: %w", err)
	}

	// Backward scan: collect all keys in reverse.
	backward, err := collectKeysReverse(db.NewIter(&pebble.IterOptions{}))
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
func DumpContents(db *pebble.DB) (map[string]string, error) {
	result := make(map[string]string)
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
		keys = append(keys, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

// AllEntries returns an iterator over all key-value pairs in the database.
// This is a convenience wrapper for use with range-over-func.
func AllEntries(db *pebble.DB) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		it, err := db.NewIter(nil)
		if err != nil {
			return
		}
		defer it.Close()
		for valid := it.First(); valid; valid = it.Next() {
			val, _ := it.ValueAndErr()
			if !yield(string(it.Key()), string(val)) {
				return
			}
		}
	}
}
