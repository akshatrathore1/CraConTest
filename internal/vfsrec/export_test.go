package vfsrec

import (
	"time"

	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// AppendEventForTesting appends an event holding the internal lock.
func (fs *FS) AppendEventForTesting(event trace.Event) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.appendEventLocked(event)
}

// AppendSyncEventForTesting appends a sync event holding the internal lock.
func (fs *FS) AppendSyncEventForTesting(event trace.Event) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.appendSyncEventLocked(event)
}

// IsDBWALPathForTesting reports whether path is considered a primary DB WAL path.
func IsDBWALPathForTesting(path string) bool {
	return isDBWALPath(path)
}

// IsDBManifestPathForTesting reports whether path is considered a primary DB MANIFEST path.
func IsDBManifestPathForTesting(path string) bool {
	return isDBManifestPath(path)
}

// SetWaitTimeoutForTesting sets fs.waitTimeout under fs.mu.
func (fs *FS) SetWaitTimeoutForTesting(d time.Duration) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.waitTimeout = d
}

// SetPendingManifestDeletionsForTesting sets fs.pendingManifestDeletions under fs.mu.
func (fs *FS) SetPendingManifestDeletionsForTesting(n int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.pendingManifestDeletions = n
}
