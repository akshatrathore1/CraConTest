package exp

import "github.com/cockroachdb/pebble/v2/vfs"

// SetSafeOpenFSWrapperForTesting installs a temporary VFS wrapper around cloned crash filesystems
// before SafeOpen is invoked in RunExplore, returning a cleanup function to restore the prior hook.
func SetSafeOpenFSWrapperForTesting(fn func(vfs.FS) vfs.FS) func() {
	beforeSafeOpenFSMu.Lock()
	prev := beforeSafeOpenFS
	beforeSafeOpenFS = fn
	beforeSafeOpenFSMu.Unlock()
	return func() {
		beforeSafeOpenFSMu.Lock()
		beforeSafeOpenFS = prev
		beforeSafeOpenFSMu.Unlock()
	}
}
