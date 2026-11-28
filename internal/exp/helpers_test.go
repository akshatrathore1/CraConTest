package exp_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	fallback := filepath.Join("..", "..")
	if _, err := os.Stat(filepath.Join(fallback, "results")); err == nil {
		return fallback
	}
	t.Fatalf("could not locate repository root (go.mod)")
	return ""
}

func findResultsDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "results")
	fi, err := os.Stat(dir)
	if err != nil || !fi.IsDir() {
		t.Fatalf("results directory not found at %s: %v", dir, err)
	}
	return dir
}

func diffCSVLines(got, want []byte) string {
	norm := func(b []byte) []string {
		s := string(bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n")))
		return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	}
	gotLines := norm(got)
	wantLines := norm(want)
	minLen := len(gotLines)
	if len(wantLines) < minLen {
		minLen = len(wantLines)
	}
	for i := 0; i < minLen; i++ {
		if gotLines[i] != wantLines[i] {
			return fmt.Sprintf("first mismatch at line %d:\n  got:  %s\n  want: %s", i+1, gotLines[i], wantLines[i])
		}
	}
	if len(gotLines) != len(wantLines) {
		return fmt.Sprintf("line count mismatch: got %d lines, want %d lines", len(gotLines), len(wantLines))
	}
	return ""
}
