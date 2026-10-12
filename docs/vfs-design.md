# CraConTest — VFS Instrumentation Design

This document specifies how CraConTest wraps Pebble's virtual filesystem to
capture persistence events. It defines the events recorded, the data each event
carries, the trace output format, and how the trace feeds into the dependency
graph builder.

## Design Principles

1. **Pebble is not modified.** All instrumentation happens through the `vfs.FS`
   and `vfs.File` interfaces that Pebble already supports.

2. **The wrapper covers the entire interface.** Every method on `vfs.FS` (18
   methods) and `vfs.File` (13 methods) is intercepted, even read-only methods,
   so the wrapper satisfies the interface and can log every mutation.

3. **Trace order equals filesystem order.** All mutating operations execute
   under a single lock, ensuring the trace records events in the exact order
   the filesystem processed them.

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
| `Open(name, opts...)` | No | No | — |
| `OpenReadWrite(name, category, opts...)` | Yes | Yes | `OpenReadWrite` event |
| `OpenDir(name)` | No | No | Notes the directory handle for later Sync |
| `Remove(name)` | Yes | No | `Remove` event |
| `RemoveAll(name)` | Yes | No | `RemoveAll` event |
| `Rename(old, new)` | Yes | No | `Rename` event |
| `ReuseForWrite(old, new, category)` | Yes | Yes | `ReuseForWrite` event |
| `MkdirAll(dir, perm)` | Yes | No | `MkdirAll` event |
| `Lock(name)` | Yes | No | — (coordination only) |
| `List(dir)` | No | No | — |
| `Stat(name)` | No | No | — |
| `PathBase(path)` | No | No | — |
| `PathJoin(elem...)` | No | No | — |
| `PathDir(path)` | No | No | — |
| `GetDiskUsage(path)` | No | No | — |
| `Unwrap()` | No | No | Returns inner FS |

Three methods carry a `DiskWriteCategory`: `Create`, `OpenReadWrite`, and
`ReuseForWrite`. The category is recorded and propagated to all subsequent
events on that file handle.

### `vfs.File` Methods

| Method | Mutating | Records |
|--------|----------|---------|
| `Write(p)` | Yes | `Write` event with copied data |
| `WriteAt(p, off)` | Yes | `WriteAt` event with copied data |
| `Sync()` | Yes | `Sync` event |
| `SyncData()` | Yes | `SyncData` event |
| `SyncTo(length)` | No* | `SyncTo` event (informational; no durability guarantee) |
| `Close()` | Yes | `Close` event |
| `Read(p)` | No | — |
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
    ID   uint64  // Monotonically increasing, assigned under lock
    Kind Kind    // Event type (see table below)
    File string  // Filesystem path affected
    Off  int64   // Write offset (for Write/WriteAt; -1 otherwise)
    Size int     // Bytes written (for Write/WriteAt; 0 otherwise)
    Data []byte  // Copied write payload (for Write/WriteAt; nil otherwise)
    Cat  string  // DiskWriteCategory from Create/OpenReadWrite/ReuseForWrite
    Op   uint64  // History operation ID (for OpBegin/OpEnd markers; 0 otherwise)
    Note string  // Annotation (for EngineEvent; empty otherwise)
}
```

### Event Kinds

| Kind | Source | Description |
|------|--------|-------------|
| `Create` | `FS.Create` | File created (or replaced) |
| `OpenReadWrite` | `FS.OpenReadWrite` | File opened for read-write |
| `ReuseForWrite` | `FS.ReuseForWrite` | File recycled with rename |
| `Write` | `File.Write` | Sequential write with payload |
| `WriteAt` | `File.WriteAt` | Positional write with payload |
| `Sync` | `File.Sync` | Full file sync (metadata + data) |
| `SyncData` | `File.SyncData` | Data-only sync |
| `SyncTo` | `File.SyncTo` | Prefix sync (informational) |
| `Close` | `File.Close` | File handle closed |
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
   Compact). These bracket the persistence events caused by each logical
   operation.

2. **DiskWriteCategory** (`Cat` field): supplied by Pebble to `Create`,
   `OpenReadWrite`, and `ReuseForWrite`. Values include `"pebble-wal"`,
   `"pebble-manifest"`, `"pebble-memtable-flush"`, `"pebble-compaction"`,
   and `"pebble-blob-file-rewrite"`. Inherited by all subsequent events on
   that file handle.

3. **Engine events** (`EngineEvent`): from `Options.EventListener` callbacks.
   These annotate flush begin/end, compaction begin/end, WAL creation/deletion,
   manifest creation/deletion, and table creation/deletion with structured
   metadata.

Together, these three sources label every persistence event with its operation
context, file purpose, and engine lifecycle stage — without modifying Pebble.

## Trace Output Format

Traces are serialized as newline-delimited JSON (one JSON object per event),
enabling streaming writes and incremental reads:

```json
{"id":1,"kind":"Create","file":"000001.log","cat":"pebble-wal"}
{"id":2,"kind":"Write","file":"000001.log","off":0,"size":128,"data":"base64...","cat":"pebble-wal"}
{"id":3,"kind":"SyncData","file":"000001.log","cat":"pebble-wal"}
{"id":4,"kind":"OpBegin","op":1}
{"id":5,"kind":"Write","file":"000001.log","off":128,"size":64,"data":"base64...","cat":"pebble-wal"}
{"id":6,"kind":"OpEnd","op":1}
{"id":7,"kind":"EngineEvent","note":"flush-begin"}
```

Write payloads are base64-encoded in JSON. The `Data` field is omitted for
non-write events. The `Cat` field is omitted when empty. The `Op` field is
omitted when zero. The `Note` field is omitted when empty.

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
                    │  (NDJSON file)   │
                    └──────────────────┘
```

### Concurrency and Ordering

The recording wrapper serializes all mutating operations under a single mutex.
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

5. **Cross-file dependencies**: a manifest update references SSTable files by
   number. The manifest sync depends on the SSTables being fully written and
   synced.

6. **Semantic labels**: each event is labeled with its operation context (from
   `OpBegin`/`OpEnd` markers), its write category (from `Cat`), and any
   associated engine event (from `EngineEvent` annotations). These labels
   enable semantic-aware ranking strategies.

### Persistence-State View

For each event index *k* in the trace, the graph provides a
**persistence-state view**: which files hold unsynced data, which directory
entries are pending (created but not directory-synced), and how many persistence
events are downstream from *k*. This view is the primary input to ranking
strategies.
