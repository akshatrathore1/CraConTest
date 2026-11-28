# Workload Specification Configurations

This directory contains deterministic workload configuration specifications for CraConTest experiment runs (`main.json` and `smoke.json`).

## Configurations

| Configuration | Seed | Operations | Keys | Value Size | Sync Every | Flush Every | Target Workload |
|---|---|---|---|---|---|---|---|
| `main.json` | 42 | 1,000 | 64 | 128 bytes | 10 mutations | 100 operations | Full 1,000-op exploration and acceptance evaluation |
| `smoke.json` | 42 | 100 | 16 | 64 bytes | 10 mutations | 50 operations | Fast end-to-end smoke verification (~0.5s) |

## Note on Operation Mix

Workload JSON files specify operational parameters (`seed`, `operations`, `key_count`, `value_bytes`, `sync_every`, and `flush_every`, plus optional `preset`, `dir`, `compact_at`, `checkpoint_at`, `max_manifest_file_size`, `ingest_every`, `ratchet_at`, and `barrier_ingest`). Missing fields inherit defaults from `workload.DefaultConfig()`.

The default **operation mix** is standardized in `internal/workload/workload.go` (`workload.Plan()`):
- **50% Set**: Single-key mutation (`pebble.DB.Set`)
- **20% Get**: Point lookup (`pebble.DB.Get`)
- **10% Delete**: Single-key tombstone (`pebble.DB.Delete`)
- **10% Batch**: Two-key atomic mutation batch (`pebble.Batch`)
- **5% RangeDelete**: Bounded range tombstone (`pebble.DB.DeleteRange`)
- **5% Iterate**: Range scan (`pebble.Iterator`)

Alternative operation mixes are selected at runtime via the `-preset` CLI flag (`ycsb-a`, `ycsb-b`, `ycsb-f`, `overwrite-heavy`, `delete-heavy`), as implemented in `workload.Plan()`.
