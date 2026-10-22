package recovery_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/akshatrathore1/CraConTest/internal/recovery"
)

// TestRealDiskCleanClose verifies that 200 synced keys with a mid-run flush
// survive a clean Close and are fully recoverable with integrity checks
// on a real-disk filesystem (vfs.Default).
func TestRealDiskCleanClose(t *testing.T) {
	dir := t.TempDir()

	opts := &pebble.Options{
		FS:     vfs.Default,
		Logger: recovery.PanicLogger{},
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}

	for i := 0; i < 200; i++ {
		if i == 100 {
			if err := db.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
		}
		key := fmt.Sprintf("k-%04d", i)
		val := fmt.Sprintf("v-%04d", i)
		if err := db.Set([]byte(key), []byte(val), pebble.Sync); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery phase via SafeOpen.
	res := recovery.SafeOpen(vfs.Default, dir, 30*time.Second)
	if res.Err != nil {
		t.Fatalf("SafeOpen: %v", res.Err)
	}
	if res.Panic != "" {
		t.Fatalf("SafeOpen panic: %s", res.Panic)
	}
	if res.Timeout {
		t.Fatal("SafeOpen timed out")
	}
	if res.DB == nil {
		t.Fatal("SafeOpen returned nil DB")
	}
	defer res.DB.Close()

	if err := recovery.CheckIntegrity(res.DB); err != nil {
		t.Fatalf("CheckIntegrity: %v", err)
	}

	contents, err := recovery.DumpContents(res.DB)
	if err != nil {
		t.Fatalf("DumpContents: %v", err)
	}

	if len(contents) != 200 {
		t.Fatalf("expected 200 keys, got %d", len(contents))
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k-%04d", i)
		expected := fmt.Sprintf("v-%04d", i)
		got, ok := contents[key]
		if !ok {
			t.Fatalf("missing key %s", key)
		}
		if got != expected {
			t.Fatalf("key %s mismatch: got %q, want %q", key, got, expected)
		}
	}
}

// TestRealDiskChildProcessKill verifies crash recovery when a process writing
// synced keys exits abruptly without calling Close. The parent process recovers
// the database using SafeOpen and verifies full data integrity.
func TestRealDiskChildProcessKill(t *testing.T) {
	if os.Getenv("CRACONTEST_TEST_CHILD") == "1" {
		dir := os.Getenv("CRACONTEST_TEST_DIR")
		opts := &pebble.Options{
			FS:     vfs.Default,
			Logger: recovery.PanicLogger{},
		}

		db, err := pebble.Open(dir, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child open failed: %v\n", err)
			os.Exit(1)
		}

		for i := 0; i < 200; i++ {
			if i == 100 {
				if err := db.Flush(); err != nil {
					fmt.Fprintf(os.Stderr, "child flush failed: %v\n", err)
					os.Exit(2)
				}
			}
			key := fmt.Sprintf("k-%04d", i)
			val := fmt.Sprintf("v-%04d", i)
			if err := db.Set([]byte(key), []byte(val), pebble.Sync); err != nil {
				fmt.Fprintf(os.Stderr, "child set failed: %v\n", err)
				os.Exit(2)
			}
		}

		// Exit abruptly with code 3 without calling Close().
		os.Exit(3)
	}

	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=^TestRealDiskChildProcessKill$")
	cmd.Env = append(os.Environ(),
		"CRACONTEST_TEST_CHILD=1",
		"CRACONTEST_TEST_DIR="+dir,
	)

	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("expected child to exit with code 3, got error %v (output: %s)", err, string(out))
	}

	// Recovery phase via SafeOpen.
	res := recovery.SafeOpen(vfs.Default, dir, 30*time.Second)
	if res.Err != nil {
		t.Fatalf("SafeOpen: %v", res.Err)
	}
	if res.Panic != "" {
		t.Fatalf("SafeOpen panic: %s", res.Panic)
	}
	if res.Timeout {
		t.Fatal("SafeOpen timed out")
	}
	if res.DB == nil {
		t.Fatal("SafeOpen returned nil DB")
	}
	defer res.DB.Close()

	if err := recovery.CheckIntegrity(res.DB); err != nil {
		t.Fatalf("CheckIntegrity: %v", err)
	}

	contents, err := recovery.DumpContents(res.DB)
	if err != nil {
		t.Fatalf("DumpContents: %v", err)
	}

	if len(contents) != 200 {
		t.Fatalf("expected 200 keys, got %d", len(contents))
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k-%04d", i)
		expected := fmt.Sprintf("v-%04d", i)
		got, ok := contents[key]
		if !ok {
			t.Fatalf("missing key %s", key)
		}
		if got != expected {
			t.Fatalf("key %s mismatch: got %q, want %q", key, got, expected)
		}
	}
}
