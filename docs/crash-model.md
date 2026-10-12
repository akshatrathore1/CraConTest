# CraConTest — Crash Model Specification

This document defines precisely what constitutes a "crash" in CraConTest, what
post-crash states are valid, what constitutes a violation, and what Pebble's
durability contract guarantees about synced versus unsynced data surviving a
crash. It is intended to be concrete enough for implementing the consistency
oracle.

## Scope

CraConTest models crashes at the **virtual filesystem (VFS) layer**, not at the
device or kernel level. A crash is a sudden halt after which only data that has
been explicitly synced is guaranteed to survive. The framework does not model
sector-level write tearing, device firmware reordering, or power-fail
capacitors. This is a controlled experiment that trades physical realism for
reproducibility and determinism.

## What is a Crash

A crash occurs at a specific point in a persistence event trace: after event *k*,
the system halts. All filesystem state up to and including event *k* is visible
to the crash model. Subsequent events never happened.

### Post-Crash Filesystem State

Given a crash at event *k*, the post-crash filesystem contains:

1. **Synced data**: all data that was written and then followed by a successful
   `Sync()` or `SyncData()` call on the same file, or a `Sync()` on the
   directory for directory entries. This data **must** be present.

2. **Unsynced data**: data written to a file but not yet synced. This data
   **may or may not** survive. The survival of each unsynced 4096-byte data
   block or directory entry is determined independently, with probability
   controlled by `CrashCloneCfg.UnsyncedDataPercent`.

3. **Never-written data**: data from events after *k*. This data **must not**
   be present.

### Pebble's CrashClone Implementation

CraConTest uses Pebble's `vfs.NewCrashableMem()` and `MemFS.CrashClone()` to
construct post-crash filesystem states. The implementation
(source: [`vfs/mem_fs.go`](https://github.com/cockroachdb/pebble/blob/v2.1.7/vfs/mem_fs.go))
works as follows:

- Each `memNode` maintains both `data` (current) and `syncedData` (last synced)
  byte slices for files, and both `children` (current) and `syncedChildren`
  (last synced) maps for directories.

- `File.Sync()` and `File.SyncData()` copy current data to synced data.
  `File.SyncTo()` returns `(false, nil)` and does **not** update synced state
  (no durability guarantee).

- `CrashClone` acquires an exclusive lock (`cloneMu.Lock()`) to freeze all
  filesystem mutations, then recursively clones the tree:
  - **Directories**: start from `syncedChildren`, then randomly include
    unsynced entries with probability `UnsyncedDataPercent`.
  - **Files**: start from `syncedData`, then iterate over unsynced 4096-byte
    blocks and randomly include each with probability `UnsyncedDataPercent`.
    Gaps between included unsynced blocks are zero-filled.

## Pebble's Durability Contract

Pebble provides the following persistence guarantees:

### Synced Operations

An operation whose data has been written to the WAL and whose WAL file has been
synced (`SyncData()` on the WAL file) is **durably committed**. After any crash,
the operation must be recoverable. Pebble's WAL writer uses
`vfs.NewSyncingFile` to wrap the WAL file, and the syncing file's `Sync()`
delegates to `File.SyncData()`.

### Unsynced Operations

An operation written to the WAL but not yet synced **may or may not** survive
a crash. However, the surviving operations must form a **prefix** of the write
order (the no-hole rule, defined below).

### Flushed Data

When a memtable is flushed to an SSTable:

1. The SSTable is written via `vfs.NewSyncingFile`, syncing via `SyncData()`.
2. A new manifest version edit is written and synced (`Sync()` on the manifest).
3. The data directory is synced (`Sync()` on the directory handle from `OpenDir`).

Each sync step must complete for the flush to be durable. A crash between any of
these steps may leave the data partially persisted.

## The Prefix (No-Hole) Rule

This is the central invariant of the consistency oracle.

### Statement

After a crash, the recovered database state must equal the state produced by
applying some **prefix** of the commit order. That prefix must include at least
all durably committed (synced) operations.

Formally: let operations be ordered by their commit sequence number. After
recovery, there exists an index *j* such that:

- All operations with sequence number ≤ *j* are present.
- No operation with sequence number > *j* is present.
- *j* ≥ the sequence number of the last synced operation.

### What This Means

- **No holes**: if operation 5 is recovered but operation 4 is not, this is a
  violation. The recovered state must be a contiguous prefix.
- **No resurrection**: if operation 8 was never acknowledged, it must not appear
  in the recovered state.
- **Batches are atomic**: a batch operation is either fully present or fully
  absent. A partially applied batch is a violation.

### Why the Independent Rule is Wrong

An alternative model treats each unsynced write independently: each may or may
not survive with no ordering constraint. This model is **incorrect** because it
accepts illegal recoveries. Consider:

```
Op 1: PUT(A, 1)   — written to WAL, not synced
Op 2: PUT(B, 2)   — written to WAL, not synced
Op 3: PUT(C, 3)   — written to WAL, synced
```

Under the independent model, a recovery with Op 1 missing but Op 2 present
would be accepted. Under the prefix rule, this is a violation: Op 2 is present
but an earlier operation (Op 1) is missing, creating a hole.

The prefix rule matches how Pebble's WAL replay actually works and how
RocksDB's crash tests validate recovery (see the RocksDB
["lost buffered writes" blog post](https://rocksdb.org/blog/2022/10/05/lost-buffered-write-recovery.html):
valid recovery means all recovered writes are older than all lost writes).

## WAL Replay and Recovery Semantics in Pebble

Pebble's recovery process, implemented in `open.go` (function `replayWAL`),
replays WAL files to reconstruct in-memory state:

### Earlier WALs vs. the Last WAL

- **Earlier WALs** (`strictWALTail = true`): these were closed cleanly. Any
  unexpected end-of-file or invalid chunk in a closed WAL is treated as data
  corruption, aborting `Open()`.

- **The last (active) WAL** (`strictWALTail = false`): may end abruptly due to
  an incomplete in-flight write at the time of crash. When
  `record.ErrUnexpectedEOF` is encountered in the last WAL, Pebble tolerates
  the incomplete tail and stops replaying — this is a normal torn-tail
  condition.

### Torn and Invalid Records

Invalid or zeroed chunks (from WAL preallocation or recycling) trigger a
read-ahead check in `record.Reader`. The WAL format includes sync-offset
headers in sync chunks: if a subsequent chunk has a `syncedOffset` greater than
the offset of the invalid data, this proves a **hole** in the durable log —
data was synced past the point of corruption, confirming real corruption rather
than a torn tail. This directly enforces the no-hole invariant at the WAL level.

### Sequence Number Monotonicity

During replay, batches with a sequence number less than or equal to the
previously replayed sequence number are skipped. This maintains monotonic prefix
ordering and deduplicates records across failover WAL segments.

## Oracle Classification Rules

The oracle classifies each crash-recovery outcome into exactly one class:

| Class | Definition |
|-------|-----------|
| **valid** | Recovered state matches some prefix of the commit order that includes all synced operations. |
| **violation** | A specific rule is violated: a synced write is missing, a hole exists, a batch is partial, or a never-written key appears. |
| **open-error** | `pebble.Open()` returns an error. Under the VFS crash model (only unsynced data is lost), an open failure is itself a violation. |
| **panic** | Recovery triggers a panic or fatal log (Pebble's default logger calls `os.Exit(1)`). |
| **hang** | Recovery does not complete within the configured timeout. |
| **oracle-error** | The oracle itself cannot determine the outcome (internal error). |

### Violation Sub-Types

| Violation | Description |
|-----------|-------------|
| Lost acknowledged synced write | A key whose write was acknowledged and synced is absent after recovery. |
| Hole in commit order | A later unsynced write is present but an earlier one is absent. |
| Partial batch | Some but not all keys from an atomic batch are present. |
| Unexpected resurrection | A key that was never written, or was deleted and synced, appears after recovery. |
| Structural inconsistency | `DB.CheckLevels()` fails, or forward and backward iteration disagree. |

## Durability Classes for the Reference Model

The oracle's reference model tracks three durability classes for each operation:

| Class | Meaning | After crash |
|-------|---------|-------------|
| `durably_committed` | Written and synced | **Must** survive |
| `pending` | Written to WAL, not synced | **May** survive, but only as part of a prefix |
| `volatile` | In memtable only, no WAL write | **Must not** survive |

## DiskWriteCategory Labels

Pebble labels file operations with a `DiskWriteCategory` string (type defined in
`vfs/disk_health.go`). The following categories are used internally by Pebble
v2.1.7:

| Category | Used For |
|----------|----------|
| `"pebble-wal"` | Write-ahead log files |
| `"pebble-manifest"` | MANIFEST (version set) files |
| `"pebble-memtable-flush"` | SSTable files produced by memtable flush |
| `"pebble-compaction"` | SSTable files produced by compaction |
| `"pebble-blob-file-rewrite"` | Blob file rewrites |
| `"unspecified"` | Default when no category applies |

These categories, together with the file type, allow the trace to label each
persistence event without modifying Pebble.

## Sync Dispatch in Pebble v2.1.7

Understanding how Pebble dispatches sync calls is critical for the VFS wrapper:

- **WAL files** and **SSTable files**: writes are wrapped in
  `vfs.NewSyncingFile`, which dispatches `Sync()` through `File.SyncData()`.
  The wrapper must intercept both `Sync()` and `SyncData()`.

- **Manifest files**: synced via `File.Sync()` directly.

- **Directory handles** (`OpenDir`): synced via `File.Sync()` to persist
  directory entries (new file names after flush or compaction).

The VFS recording layer and the fault injection layer must intercept both
`Sync()` and `SyncData()` on all files to correctly capture and manipulate
persistence boundaries.
