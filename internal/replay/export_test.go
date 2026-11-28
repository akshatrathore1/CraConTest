package replay

import (
	"bytes"
	"math/rand/v2"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// DeterministicPartialCloneForTesting exposes deterministicPartialCloneWithLinks for external tests.
func DeterministicPartialCloneForTesting(
	base0, full100 vfs.FS,
	percent int,
	rng *rand.Rand,
	fileSyncedBytes map[string][]byte,
) (vfs.FS, error) {
	return deterministicPartialCloneWithLinks(base0, full100, percent, rng, func(p string) []byte {
		return fileSyncedBytes[p]
	}, nil, nil)
}

// MemFSForTesting returns the underlying *vfs.MemFS for testing.
func (s *State) MemFSForTesting() *vfs.MemFS {
	return s.memFS
}

// FileSyncedBytesForTesting returns a copy of fileSyncedBytes by path for testing.
func (s *State) FileSyncedBytesForTesting() map[string][]byte {
	result := make(map[string][]byte, len(s.filePaths))
	for name, snapshot := range s.filePaths {
		if snapshot != nil {
			result[name] = bytes.Clone(snapshot.bytes)
		}
	}
	return result
}

// WrapHandleForTesting wraps an open file handle in s.handles for testing.
func (s *State) WrapHandleForTesting(handle uint64, wrap func(vfs.File) vfs.File) {
	if f, ok := s.handles[handle]; ok {
		s.handles[handle] = wrap(f)
	}
}

// AdvanceStepForTesting exposes applyEvent for testing.
func (s *State) AdvanceStepForTesting(event trace.Event) error {
	return s.applyEvent(event)
}
