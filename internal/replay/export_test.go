package replay

import (
	"math/rand/v2"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// DeterministicPartialCloneForTesting exposes deterministicPartialClone for external tests.
func DeterministicPartialCloneForTesting(
	base0, full100 vfs.FS,
	percent int,
	rng *rand.Rand,
	fileSyncedBytes map[string][]byte,
) (vfs.FS, error) {
	return deterministicPartialClone(base0, full100, percent, rng, fileSyncedBytes)
}

// MemFSForTesting returns the underlying *vfs.MemFS for testing.
func (s *State) MemFSForTesting() *vfs.MemFS {
	return s.memFS
}

// FileSyncedBytesForTesting returns the fileSyncedBytes map for testing.
func (s *State) FileSyncedBytesForTesting() map[string][]byte {
	return s.fileSyncedBytes
}

// WrapHandleForTesting wraps an open file handle in s.handles for testing.
func (s *State) WrapHandleForTesting(handle uint64, wrap func(vfs.File) vfs.File) {
	if f, ok := s.handles[handle]; ok {
		s.handles[handle] = wrap(f)
	}
}
