// Package trace defines the event schema for VFS operation traces.
//
// A trace is a sequence of events recording every mutating filesystem
// call made by Pebble during a workload run. Events are the primary
// input to the dependency graph builder and the replay engine.
package trace

// Kind identifies the type of a VFS operation or annotation event.
type Kind string

const (
	// Filesystem operations.
	KindCreate       Kind = "Create"
	KindLink         Kind = "Link"
	KindLock         Kind = "Lock"
	KindMkdirAll     Kind = "MkdirAll"
	KindOpen         Kind = "Open"
	KindOpenDir      Kind = "OpenDir"
	KindOpenReadWrite Kind = "OpenReadWrite"
	KindRemove       Kind = "Remove"
	KindRemoveAll    Kind = "RemoveAll"
	KindRename       Kind = "Rename"
	KindReuseForWrite Kind = "ReuseForWrite"

	// File operations.
	KindWrite    Kind = "Write"
	KindSync     Kind = "Sync"
	KindSyncData Kind = "SyncData"
	KindSyncTo   Kind = "SyncTo"
	KindClose    Kind = "Close"

	// Annotations.
	KindOpBegin     Kind = "OpBegin"
	KindOpEnd       Kind = "OpEnd"
	KindEngineEvent Kind = "EngineEvent"
)

// Event represents a single VFS operation or annotation in the trace.
type Event struct {
	// ID is a monotonically increasing identifier assigned at record time.
	ID uint64 `json:"id"`

	// Kind identifies the operation type.
	Kind Kind `json:"kind"`

	// File is the path of the file involved, relative to the VFS root.
	// Empty for annotations and directory-level operations that don't
	// target a specific file.
	File string `json:"file,omitempty"`

	// File2 is the second path for two-path operations (Rename, Link).
	File2 string `json:"file2,omitempty"`

	// Off is the byte offset for positioned writes.
	Off int64 `json:"off,omitempty"`

	// Len is the number of bytes written. The actual data is stored
	// separately if payload recording is enabled.
	Len int `json:"len,omitempty"`

	// Data holds the write payload (a copy of the bytes passed to Write).
	// Nil for non-write events.
	Data []byte `json:"data,omitempty"`

	// Cat is the DiskWriteCategory string passed to Create,
	// OpenReadWrite, or ReuseForWrite. Empty when the call does not
	// carry a category.
	Cat string `json:"cat,omitempty"`

	// Op is the history operation ID for OpBegin and OpEnd markers.
	// Zero for all other event kinds.
	Op uint64 `json:"op,omitempty"`

	// Note carries the listener event name for EngineEvent annotations
	// (e.g., "FlushBegin", "CompactionEnd", "WALCreated").
	Note string `json:"note,omitempty"`

	// Err records any error returned by the operation, as a string.
	// Empty on success.
	Err string `json:"err,omitempty"`
}
