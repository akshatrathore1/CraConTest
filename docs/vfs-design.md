# CraConTest — VFS Instrumentation Design

This document specifies how CraConTest wraps Pebble's virtual filesystem to
capture persistence events. It defines the events recorded, the data each event
carries, the trace output format, and how the trace feeds into the dependency
graph builder.

## Design Principles

1. **Pebble is not modified.** All instrumentation happens through the `vfs.FS`
   and `vfs.File` interfaces that Pebble already supports.

2. **The wrapper covers the entire interface.** Every method on `vfs.FS` and
   `vfs.File` is intercepted, even read-only methods,
   so the wrapper satisfies the interface and can log every mutation.

3. **Trace order equals filesystem order.** Mutations, opens, sequential
   reads, closes, locks and annotations use one ordering lock. The lock covers
   each underlying call, its result, ID allocation and append, not the entire
   client operation.

4. **Write data is copied before recording.** Pebble's `vfs.File.Write()` is
   allowed to modify the passed slice. The wrapper copies the buffer before
   appending to the trace.

5. **Both `Sync()` and `SyncData()` are intercepted.** Pebble routes WAL and
   SSTable syncs through `vfs.NewSyncingFile`, which dispatches via
   `SyncData()`. Manifest files and directory handles use `Sync()` directly.
   The wrapper must record both to correctly capture all persistence boundaries.

## Pebble's VFS Interface (v2.1.7)

### `vfs.FS` Methods

The 18 methods on `vfs.FS` that the wrapper must implement:

| Method | Mutating | DiskWriteCategory | Records |
|--------|----------|-------------------|---------|
| `Create(name, category)` | Yes | Yes | `Create` event |
| `Link(old, new)` | Yes | No | `Link` event |
| `Open(name, opts...)` | No | No | `Open` event and resource handle |
| `OpenReadWrite(name, category, opts...)` | Yes | Yes | `OpenReadWrite` event |
| `OpenDir(name)` | No | No | `OpenDir` event and directory handle |
| `Remove(name)` | Yes | No | `Remove` event |
| `RemoveAll(name)` | Yes | No | `RemoveAll` event |
| `Rename(old, new)` | Yes | No | `Rename` event |
| `ReuseForWrite(old, new, category)` | Yes | Yes | `ReuseForWrite` event |
| `MkdirAll(dir, perm)` | Yes | No | `MkdirAll` event |
| `Lock(name)` | Yes | No | `Lock` event and lock resource |
| `List(dir)` | No | No | — |
| `Stat(name)` | No | No | — |
| `PathBase(path)` | No | No | — |
| `PathJoin(elem...)` | No | No | — |
| `PathDir(path)` | No | No | — |
| `GetDiskUsage(path)` | No | No | — |
| `Unwrap()` | No | No | Returns inner FS |

Three methods carry a `DiskWriteCategory`: `Create`, `OpenReadWrite`, and
`ReuseForWrite`. The category is recorded on the opening event; consumers
inherit it through the handle rather than repeating it on write/sync events.

### `vfs.File` Methods

| Method | Mutating | Records |
|--------|----------|---------|
| `Write(p)` | Yes | `Write` event with copied data |
| `WriteAt(p, off)` | Yes | `WriteAt` event with copied data |
| `Sync()` | Yes | `Sync` event |
| `SyncData()` | Yes | `SyncData` event |
| `SyncTo(length)` | No* | `SyncTo` event (informational; no durability guarantee) |
| `Close()` | Yes | `Close` event |
| `Read(p)` | No | Requested size, returned count and input cursor |
| `ReadAt(p, off)` | No | — |
| `Stat()` | No | — |
| `Preallocate(off, len)` | No | — |
| `Prefetch(off, len)` | No | — |
| `Fd()` | No | — |

\* `SyncTo` is a no-op in MemFS (returns `(false, nil)`) and provides no
persistence guarantee.

## Event Schema

Each event in the trace is a structured record with the following fields:

```go
type Event struct {
   ID       uint64   `json:"id"`
   Kind     Kind     `json:"kind"`
   File     string   `json:"file,omitempty"`
   File2    string   `json:"file2,omitempty"`
   Off      int64    `json:"off,omitempty"`
   Len      int      `json:"len,omitempty"`
   Data     []byte   `json:"data,omitempty"`
   Cat      string   `json:"cat,omitempty"`
   Op       uint64   `json:"op,omitempty"`
   Note     string   `json:"note,omitempty"`
   Err      string   `json:"err,omitempty"`
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
```

IDs are consecutive and one-based. Cutoff `k` includes events `1..k`, or
`events[:k]`; zero is the pretrace cutoff. Paths are cleaned, slash-separated
and relative to the VFS root. An empty directory path names the root.

Successful opens and locks allocate consecutive nonzero handles. Separate
opens of one path have separate cursors. File events retain the opening label
after rename, link or unlink; replay acts on the resource handle, not by
reopening that label. `ReuseForWrite` labels its new handle with `File2`.
`Close` retires a file handle and `Unlock` retires a lock resource.

`Data` contains the complete write input copied before the call; `Len` is its
returned byte count. Partial writes replay only `Data[:Len]`. `Read` records
requested `Size`, returned `Len` and input `Off`, without payload. `Read` and
`Write` advance the handle cursor; `WriteAt` and `ReadAt` do not. `Size` on
`SyncTo` is the requested length and `FullSync` is the returned boolean.
Successful `Sync`/`SyncData`, or successful `SyncTo` with `FullSync=true`, is
a barrier. Pinned MemFS `SyncTo` returns false and is not a barrier.

Namespace errors are not necessarily no-ops: pinned MemFS `Rename` can remove
the source before failing to install the destination. Replay executes these
calls and checks error presence while preserving their side effects.

### Event Kinds

| Kind | Source | Description |
|------|--------|-------------|
| `Create` | `FS.Create` | File created (or replaced) |
| `Open` | `FS.Open` | Readable resource opened |
| `OpenDir` | `FS.OpenDir` | Directory resource opened |
| `Lock` | `FS.Lock` | Lock resource acquired |
| `OpenReadWrite` | `FS.OpenReadWrite` | File opened for read-write |
| `ReuseForWrite` | `FS.ReuseForWrite` | File recycled with rename |
| `Write` | `File.Write` | Sequential write with payload |
| `WriteAt` | `File.WriteAt` | Positional write with payload |
| `Read` | `File.Read` | Sequential read affecting the handle cursor |
| `Sync` | `File.Sync` | Full file sync (metadata + data) |
| `SyncData` | `File.SyncData` | Data-only sync |
| `SyncTo` | `File.SyncTo` | Prefix sync (informational) |
| `Close` | `File.Close` | File handle closed |
| `Unlock` | Lock resource close | Lock resource released |
| `Rename` | `FS.Rename` | Atomic file rename |
| `Remove` | `FS.Remove` | File or directory removal |
| `RemoveAll` | `FS.RemoveAll` | Recursive removal |
| `Link` | `FS.Link` | Hard link creation |
| `MkdirAll` | `FS.MkdirAll` | Directory creation |
| `OpBegin` | Harness | Start of a client API operation (Op field set) |
| `OpEnd` | Harness | End of a client API operation (Op field set) |
| `EngineEvent` | EventListener | Pebble engine event (Note field set) |

### Semantic Mapping Sources

The trace carries semantic information from three independent sources:

1. **Operation markers** (`OpBegin`/`OpEnd`): written by the test harness
   around each client API call (Set, Delete, DeleteRange, Batch, Flush,
   Compact). They label a time interval, not proven ownership of background
   writes. Only markers carry `Op` and `OpKind`; filesystem events have Op=0.

2. **DiskWriteCategory** (`Cat` field): supplied by Pebble to `Create`,
   `OpenReadWrite`, and `ReuseForWrite`. Values include `"pebble-wal"`,
   `"pebble-manifest"`, `"pebble-memtable-flush"`, `"pebble-compaction"`,
   and `"pebble-blob-file-rewrite"`. Consumers inherit these from the opening
   event through the resource handle.

3. **Engine events** (`EngineEvent`): from `Options.EventListener` callbacks.
   These annotate flush begin/end, compaction begin/end, WAL creation/deletion,
   manifest creation/deletion, and table creation/deletion with structured
   metadata.

Together, these three sources label every persistence event with its operation
context, file purpose, and engine lifecycle stage — without modifying Pebble.

## Trace Output Format

Traces use a versioned UTF-8 JSON document with an ordered, non-null event
array. The following is a complete small trace:

```json
{
   "schema_version": 1,
   "events": [
      {"id":1,"kind":"Create","file":"000001.log","handle":1,"cat":"pebble-wal"},
      {"id":2,"kind":"Write","file":"000001.log","handle":1,"len":4,"data":"YWJjZA=="},
      {"id":3,"kind":"SyncData","file":"000001.log","handle":1},
      {"id":4,"kind":"Close","file":"000001.log","handle":1}
   ]
}
```

Write payloads are base64-encoded in JSON. The `Data` field is omitted for
non-write events. The `Cat` field is omitted when empty. The `Op` field is
omitted when zero. The `Note` field is omitted when empty.
Readers reject unsupported versions, unknown fields, invalid records and
trailing documents. Empty traces contain `"events":[]`, not null.

## Recording Architecture

```
                    ┌──────────────────┐
                    │  Test Harness    │
                    │  (workload +     │
                    │   op markers)    │
                    └────────┬─────────┘
                             │ API calls + OpBegin/OpEnd
                             ▼
                    ┌──────────────────┐
                    │     Pebble       │
                    │  Options.FS =    │
                    │  recordingFS     │
                    │  EventListener → │──── EngineEvent
                    └────────┬─────────┘
                             │ FS/File calls with DiskWriteCategory
                             ▼
                    ┌──────────────────┐
                    │  vfsrec          │
                    │  (recording FS)  │
                    │  over vfs.NewMem │
                    └────────┬─────────┘
                             │ append to trace (under lock)
                             ▼
                    ┌──────────────────┐
                    │     Trace        │
                    │  (JSON document) │
                    └──────────────────┘
```

### Concurrency and Ordering

The recording wrapper serializes recorded calls and annotations under a
single mutex.
This ensures:

- Trace order is the exact order the underlying filesystem saw operations.
- No interleaving of concurrent writes to the trace.
- The cost is acceptable because the underlying `vfs.NewMem()` is itself
  mutex-protected.

### The Recording FS is Non-Crashable

During the RECORD phase, the wrapper sits on top of `vfs.NewMem()` (not
`vfs.NewCrashableMem()`). The crashable filesystem is used only during the
EXPLORE phase, when trace events are replayed into a fresh
`vfs.NewCrashableMem()` to construct crash states. This separation is
deliberate:

- `vfs.NewMem()` avoids the overhead of tracking synced vs. unsynced state
  during recording.
- It avoids the `cloneMu` lock contention present in `NewCrashableMem()`.
- The v2.1.7 `CrashClone` deadlock (concurrent mutations during clone) is
  not a concern because `CrashClone` is never called on the recording FS.

## How the Trace Feeds the Dependency Graph

The dependency graph builder (`internal/graph`) reads the trace and constructs
a directed acyclic graph of persistence dependencies:

1. **Same-file ordering**: writes to the same file are ordered by their trace
   position. A sync creates a persistence barrier within each file.

2. **Sync barriers**: a `Sync` or `SyncData` event marks a persistence boundary.
   All writes before the sync are durably ordered before all writes after it
   (on the same file).

3. **Directory sync edges**: a directory `Sync` ensures that file creations,
   renames, and links within that directory are durable. Without a directory
   sync, a new file might not appear after crash even if the file's data was
   synced.

4. **Rename edges**: a `Rename(old, new)` depends on the creation of the source
   file and on the directory containing the target.

5. **Cross-file dependencies**: listener input/output metadata supplies
   observed producer/consumer edges. The initial graph does not infer a
   parsed MANIFEST dependency from a filename alone.

6. **Semantic labels**: each event is labeled with its marker interval (from
   `OpBegin`/`OpEnd`), its inherited write category, and any
   associated engine event (from `EngineEvent` annotations). These labels
   enable semantic-aware ranking strategies.

### Persistence-State View

For each event index *k* in the trace, the graph provides a
**persistence-state view**: which files hold unsynced data, which directory
entries are pending (created but not directory-synced), and how many persistence
events are downstream from *k*. This view is the primary input to ranking
strategies.

## Revision History

| Date | Section | Correction |
|---|---|---|
| 2026-10-20 | Schema, ordering and format | Version-one handles, call results, cursor semantics and JSON envelope replace the path-only sketch and unversioned event stream. |
