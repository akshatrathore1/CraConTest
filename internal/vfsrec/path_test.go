package vfsrec_test

import (
	"errors"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Spellings that the in-memory filesystem treats as the same file are
// recorded under one canonical trace path that names that file.
func TestEquivalentSpellingsShareOneTracePath(t *testing.T) {
	mem := vfs.NewMem()
	rec := vfsrec.New(mem)
	if err := rec.MkdirAll("//db/", 0755); err == nil {
		t.Fatal("trailing separator must be refused: MemFS would treat it differently")
	}
	if err := rec.MkdirAll("db", 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"db/a", "/db/a", "db//a", "//db///a"} {
		f, err := rec.Create(name, vfs.WriteCategoryUnspecified)
		if err != nil {
			t.Fatalf("Create(%q): %v", name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	events := rec.Events()
	if err := trace.Validate(events); err != nil {
		t.Fatalf("trace invalid: %v", err)
	}
	for _, event := range events {
		if event.Kind == trace.KindCreate && event.File != "db/a" {
			t.Fatalf("Create recorded as %q, want db/a", event.File)
		}
	}
	if list, err := mem.List("db"); err != nil || len(list) != 1 || list[0] != "a" {
		t.Fatalf("inner FS should hold exactly db/a, got %v %v", list, err)
	}
}

// A spelling with no faithful trace form is refused before it reaches the
// wrapped filesystem, so the trace never names a file other than the one
// the call would have touched.
func TestUnrepresentablePathsAreRefused(t *testing.T) {
	for _, name := range []string{
		"db/../a",    // MemFS does not resolve ".."
		"db/./a",     // nor "."
		`db\a`,       // backslash is an ordinary byte in MemFS names
		"C:/db/a",    // drive letters have no meaning here
		"db/a/",      // trailing separator: an empty final name
		"db/nested/", // same, deeper
	} {
		mem := vfs.NewMem()
		if err := mem.MkdirAll("db/nested", 0755); err != nil {
			t.Fatal(err)
		}
		rec := vfsrec.New(mem)
		_, err := rec.Create(name, vfs.WriteCategoryUnspecified)
		if !errors.Is(err, vfsrec.ErrUnrepresentablePath) {
			t.Fatalf("Create(%q): got %v, want ErrUnrepresentablePath", name, err)
		}
		if err := rec.Rename("db/nested", name); !errors.Is(err, vfsrec.ErrUnrepresentablePath) {
			t.Fatalf("Rename(_, %q): got %v, want ErrUnrepresentablePath", name, err)
		}
		if len(rec.Events()) != 0 {
			t.Fatalf("%q: refused calls must not be recorded, got %+v", name, rec.Events())
		}
		if list, _ := mem.List("db"); len(list) != 1 || list[0] != "nested" {
			t.Fatalf("%q: refused call reached the inner FS: %v", name, list)
		}
	}
}
