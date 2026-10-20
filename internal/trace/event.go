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
	KindCreate        Kind = "Create"
	KindLink          Kind = "Link"
	KindLock          Kind = "Lock"
	KindMkdirAll      Kind = "MkdirAll"
	KindOpen          Kind = "Open"
	KindOpenDir       Kind = "OpenDir"
	KindOpenReadWrite Kind = "OpenReadWrite"
	KindRemove        Kind = "Remove"
	KindRemoveAll     Kind = "RemoveAll"
	KindRename        Kind = "Rename"
	KindReuseForWrite Kind = "ReuseForWrite"

	// File operations.
	KindWrite    Kind = "Write"
	KindSync     Kind = "Sync"
	KindSyncData Kind = "SyncData"
	KindSyncTo   Kind = "SyncTo"
	KindClose    Kind = "Close"
	KindRead     Kind = "Read"
	KindWriteAt  Kind = "WriteAt"
	KindUnlock   Kind = "Unlock"

	// Annotations.
	KindOpBegin     Kind = "OpBegin"
	KindOpEnd       Kind = "OpEnd"
	KindEngineEvent Kind = "EngineEvent"
)

// Event represents a single VFS operation or annotation in the trace.
type Event struct {
	// ID is a consecutive one-based identifier assigned at record time.
	ID uint64 `json:"id"`

	// Kind identifies the operation type.
	Kind Kind `json:"kind"`

	// File is the path of the file involved, relative to the VFS root.
	// Empty for annotations and directory-level operations that don't
	// target a specific file.
	File string `json:"file,omitempty"`

	// File2 is the destination path for Rename, Link and ReuseForWrite.
	File2 string `json:"file2,omitempty"`

	// Off is the input offset for Read, Write and WriteAt.
	Off int64 `json:"off,omitempty"`

	// Len is the returned byte count for Read, Write and WriteAt.
	Len int `json:"len,omitempty"`

	// Data holds the complete write input, copied before the underlying call.
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

	Handle   uint64   `json:"handle,omitempty"`
	Dir      bool     `json:"dir,omitempty"`
	Created  bool     `json:"created,omitempty"`
	Size     int64    `json:"size,omitempty"`
	Mode     uint32   `json:"mode,omitempty"`
	FullSync bool     `json:"full_sync,omitempty"`
	OpKind   string   `json:"op_kind,omitempty"`
	Job      uint64   `json:"job,omitempty"`
	Inputs   []string `json:"inputs,omitempty"`
	Outputs  []string `json:"outputs,omitempty"`
}
