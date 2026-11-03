// Package workload provides a deterministic client workload generator and driver
// for Pebble databases, recording full mutation histories and marker events.
package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Workload preset identifiers for multi-distribution evaluation.
const (
	PresetYCSBA          = "ycsb-a"
	PresetYCSBB          = "ycsb-b"
	PresetYCSBF          = "ycsb-f"
	PresetOverwriteHeavy = "overwrite-heavy"
	PresetDeleteHeavy    = "delete-heavy"
)

// Config specifies the configuration for deterministic workload execution.
type Config struct {
	Preset              string `json:"preset,omitempty"`
	Seed                uint64 `json:"seed"`
	Operations          int    `json:"operations"`
	KeyCount            int    `json:"key_count"`
	ValueBytes          int    `json:"value_bytes"`
	SyncEvery           int    `json:"sync_every"`
	FlushEvery          int    `json:"flush_every"`
	CompactAt           int    `json:"compact_at,omitempty"`
	CheckpointAt        int    `json:"checkpoint_at,omitempty"`
	MaxManifestFileSize int64  `json:"max_manifest_file_size,omitempty"`
	IngestEvery         int    `json:"ingest_every,omitempty"`
	RatchetAt           int    `json:"ratchet_at,omitempty"`
	BarrierIngest       bool   `json:"barrier_ingest,omitempty"`
	FS                  vfs.FS `json:"-"`
}

// DefaultConfig returns the frozen initial driver configuration (Target C2).
func DefaultConfig() Config {
	return Config{
		Seed:       42,
		Operations: 1000,
		KeyCount:   64,
		ValueBytes: 128,
		SyncEvery:  10,
		FlushEvery: 100,
	}
}

// Validate verifies that cfg contains valid operational parameters.
func Validate(cfg Config) error {
	if cfg.Preset != "" {
		switch cfg.Preset {
		case PresetYCSBA, PresetYCSBB, PresetYCSBF, PresetOverwriteHeavy, PresetDeleteHeavy:
		default:
			return fmt.Errorf("unknown workload preset %q", cfg.Preset)
		}
	}
	if cfg.Operations <= 0 {
		return fmt.Errorf("operations must be > 0, got %d", cfg.Operations)
	}
	if cfg.Preset == "" {
		if cfg.KeyCount <= 0 {
			return fmt.Errorf("key_count must be > 0, got %d", cfg.KeyCount)
		}
		if cfg.ValueBytes <= 0 {
			return fmt.Errorf("value_bytes must be > 0, got %d", cfg.ValueBytes)
		}
	} else {
		if cfg.KeyCount < 0 {
			return fmt.Errorf("key_count must be >= 0, got %d", cfg.KeyCount)
		}
		if cfg.ValueBytes < 0 {
			return fmt.Errorf("value_bytes must be >= 0, got %d", cfg.ValueBytes)
		}
	}
	if cfg.SyncEvery < 0 {
		return fmt.Errorf("sync_every must be >= 0, got %d", cfg.SyncEvery)
	}
	if cfg.FlushEvery < 0 {
		return fmt.Errorf("flush_every must be >= 0, got %d", cfg.FlushEvery)
	}
	if cfg.CheckpointAt < 0 {
		return fmt.Errorf("checkpoint_at must be >= 0, got %d", cfg.CheckpointAt)
	}
	if cfg.IngestEvery < 0 {
		return fmt.Errorf("ingest_every must be >= 0, got %d", cfg.IngestEvery)
	}
	if cfg.RatchetAt < 0 {
		return fmt.Errorf("ratchet_at must be >= 0, got %d", cfg.RatchetAt)
	}
	return nil
}

// Marker defines the callback boundary used to log client operation boundaries.
// vfsrec.FS satisfies this interface without requiring workload to import vfsrec.
type Marker interface {
	Begin(op uint64, kind string)
	End(op uint64, kind string, err string)
}

// PlanOperation describes one planned operation in the workload.
type PlanOperation struct {
	Op        uint64
	Kind      string
	Mutating  bool
	Sync      bool
	Key       []byte
	Value     []byte
	EndKey    []byte
	Key2      []byte
	Value2    []byte
	IsFlushOp bool
	Mutations []history.Mutation
	MultiFile bool
}

func formatKey(index int) []byte {
	return []byte(fmt.Sprintf("key-%04d", index))
}

func makeValue(rng *rand.Rand, size int, tag string) []byte {
	buf := make([]byte, size)
	header := fmt.Sprintf("val-%s-", tag)
	copy(buf, header)
	for i := len(header); i < size; i++ {
		buf[i] = byte(rng.IntN(26) + 'a')
	}
	return buf
}

// Plan generates the deterministic sequence of operations for cfg.
func Plan(cfg Config) []PlanOperation {
	if err := Validate(cfg); err != nil {
		return nil
	}
	keyCount := cfg.KeyCount
	valueBytes := cfg.ValueBytes
	if valueBytes <= 0 {
		valueBytes = 64
	}
	checkpointAt := cfg.CheckpointAt
	switch cfg.Preset {
	case PresetYCSBA:
		if keyCount <= 0 {
			keyCount = 8
		}
	case PresetYCSBB:
		if keyCount <= 0 {
			keyCount = 12
		}
	case PresetYCSBF:
		if keyCount <= 0 {
			keyCount = 8
		}
	case PresetOverwriteHeavy:
		if keyCount <= 0 {
			keyCount = 4
		}
	case PresetDeleteHeavy:
		if keyCount <= 0 {
			keyCount = 8
		}
		if checkpointAt <= 0 {
			checkpointAt = 10
		}
	}

	rng := rand.New(rand.NewPCG(0, cfg.Seed))
	var plan []PlanOperation
	var opID uint64
	var mutatingCount int

	for i := 0; i < cfg.Operations; i++ {
		switch cfg.Preset {
		case PresetYCSBA:
			opID++
			roll := rng.IntN(100)
			if roll < 50 {
				mutatingCount++
				sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
				kIdx := rng.IntN(keyCount)
				k := formatKey(kIdx)
				v := makeValue(rng, valueBytes, fmt.Sprintf("%05d", opID))
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "set",
					Mutating: true,
					Sync:     sync,
					Key:      k,
					Value:    v,
				})
			} else {
				kIdx := rng.IntN(keyCount)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "get",
					Mutating: false,
					Key:      formatKey(kIdx),
				})
			}

		case PresetYCSBB:
			opID++
			if i%4 == 3 {
				mutatingCount++
				sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
				kIdx := rng.IntN(keyCount)
				k := formatKey(kIdx)
				v := makeValue(rng, valueBytes, fmt.Sprintf("%05d", opID))
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "set",
					Mutating: true,
					Sync:     sync,
					Key:      k,
					Value:    v,
				})
			} else {
				kIdx := rng.IntN(keyCount)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "get",
					Mutating: false,
					Key:      formatKey(kIdx),
				})
			}

		case PresetYCSBF:
			opID++
			mutatingCount++
			sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
			kIdx1 := rng.IntN(keyCount)
			kIdx2 := rng.IntN(keyCount)
			if kIdx1 == kIdx2 {
				kIdx2 = (kIdx1 + 1) % keyCount
			}
			k1 := formatKey(kIdx1)
			k2 := formatKey(kIdx2)
			v1 := makeValue(rng, valueBytes, fmt.Sprintf("%05da", opID))
			v2 := makeValue(rng, valueBytes, fmt.Sprintf("%05db", opID))
			plan = append(plan, PlanOperation{
				Op:       opID,
				Kind:     "batch",
				Mutating: true,
				Sync:     sync,
				Key:      k1,
				Value:    v1,
				Key2:     k2,
				Value2:   v2,
			})

		case PresetOverwriteHeavy:
			opID++
			mutatingCount++
			sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
			kIdx := rng.IntN(keyCount)
			k := formatKey(kIdx)
			v := makeValue(rng, valueBytes, fmt.Sprintf("%05d", opID))
			plan = append(plan, PlanOperation{
				Op:       opID,
				Kind:     "set",
				Mutating: true,
				Sync:     sync,
				Key:      k,
				Value:    v,
			})

		case PresetDeleteHeavy:
			opID++
			mutatingCount++
			sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
			switch i % 3 {
			case 0: // set
				kIdx := rng.IntN(keyCount)
				k := formatKey(kIdx)
				v := makeValue(rng, valueBytes, fmt.Sprintf("%05d", opID))
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "set",
					Mutating: true,
					Sync:     sync,
					Key:      k,
					Value:    v,
				})
			case 1: // delete
				kIdx := rng.IntN(keyCount)
				k := formatKey(kIdx)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "delete",
					Mutating: true,
					Sync:     sync,
					Key:      k,
				})
			case 2: // range_delete
				idx1 := rng.IntN(keyCount)
				idx2 := rng.IntN(keyCount)
				if idx1 == idx2 {
					idx2 = (idx1 + 1) % keyCount
				}
				if idx1 > idx2 {
					idx1, idx2 = idx2, idx1
				}
				k1 := formatKey(idx1)
				k2 := formatKey(idx2)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "range_delete",
					Mutating: true,
					Sync:     sync,
					Key:      k1,
					EndKey:   k2,
				})
			}

		default:
			roll := rng.IntN(100)
			opID++

			switch {
			case roll < 50: // 50% set
				mutatingCount++
				sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
				kIdx := rng.IntN(cfg.KeyCount)
				k := formatKey(kIdx)
				v := makeValue(rng, cfg.ValueBytes, fmt.Sprintf("%05d", opID))
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "set",
					Mutating: true,
					Sync:     sync,
					Key:      k,
					Value:    v,
				})

			case roll < 70: // 20% get
				kIdx := rng.IntN(cfg.KeyCount)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "get",
					Mutating: false,
					Key:      formatKey(kIdx),
				})

			case roll < 80: // 10% delete
				mutatingCount++
				sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
				kIdx := rng.IntN(cfg.KeyCount)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "delete",
					Mutating: true,
					Sync:     sync,
					Key:      formatKey(kIdx),
				})

			case roll < 90: // 10% batch of exactly two sets
				mutatingCount++
				sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
				kIdx1 := rng.IntN(cfg.KeyCount)
				kIdx2 := rng.IntN(cfg.KeyCount)
				k1 := formatKey(kIdx1)
				k2 := formatKey(kIdx2)
				v1 := makeValue(rng, cfg.ValueBytes, fmt.Sprintf("%05da", opID))
				v2 := makeValue(rng, cfg.ValueBytes, fmt.Sprintf("%05db", opID))
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "batch",
					Mutating: true,
					Sync:     sync,
					Key:      k1,
					Value:    v1,
					Key2:     k2,
					Value2:   v2,
				})

			case roll < 95: // 5% range_delete
				mutatingCount++
				sync := (cfg.SyncEvery > 0 && mutatingCount%cfg.SyncEvery == 0)
				idx1 := rng.IntN(cfg.KeyCount)
				idx2 := rng.IntN(cfg.KeyCount)
				if idx1 == idx2 {
					idx2 = (idx1 + 1) % cfg.KeyCount
				}
				if idx1 > idx2 {
					idx1, idx2 = idx2, idx1
				}
				k1 := formatKey(idx1)
				k2 := formatKey(idx2)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "range_delete",
					Mutating: true,
					Sync:     sync,
					Key:      k1,
					EndKey:   k2,
				})

			default: // 5% iterate
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "iterate",
					Mutating: false,
				})
			}
		}

		if cfg.FlushEvery > 0 && (i+1)%cfg.FlushEvery == 0 {
			opID++
			plan = append(plan, PlanOperation{
				Op:        opID,
				Kind:      "flush",
				Mutating:  false,
				IsFlushOp: true,
			})
		}

		if cfg.CompactAt > 0 && (i+1) == cfg.CompactAt {
			opID++
			plan = append(plan, PlanOperation{
				Op:       opID,
				Kind:     "compact",
				Mutating: false,
			})
		}

		if checkpointAt > 0 && (i+1)%checkpointAt == 0 {
			opID++
			plan = append(plan, PlanOperation{
				Op:       opID,
				Kind:     "checkpoint",
				Mutating: false,
			})
		}

		if cfg.IngestEvery > 0 && (i+1)%cfg.IngestEvery == 0 {
			opID++
			multiFile := (i/cfg.IngestEvery)%2 == 1
			var mutations []history.Mutation
			if !multiFile || keyCount < 4 {
				idx1 := 0
				idx2 := 1
				if keyCount >= 2 {
					idx1 = rng.IntN(keyCount - 1)
					idx2 = idx1 + 1 + rng.IntN(keyCount-idx1-1)
				}
				k1 := formatKey(idx1)
				v1 := makeValue(rng, valueBytes, fmt.Sprintf("%05da", opID))
				k2 := formatKey(idx2)
				v2 := makeValue(rng, valueBytes, fmt.Sprintf("%05db", opID))
				mutations = []history.Mutation{
					{Kind: "set", Key: k1, Value: v1},
					{Kind: "set", Key: k2, Value: v2},
				}
				multiFile = false
			} else {
				n := keyCount
				idx0 := rng.IntN(n - 3)
				idx1 := idx0 + 1 + rng.IntN(n-2-idx0-1)
				idx2 := idx1 + 1 + rng.IntN(n-1-idx1-1)
				idx3 := idx2 + 1 + rng.IntN(n-idx2-1)
				indices := []int{idx0, idx1, idx2, idx3}
				mutations = make([]history.Mutation, 4)
				for mIdx := 0; mIdx < 4; mIdx++ {
					mutations[mIdx] = history.Mutation{
						Kind:  "set",
						Key:   formatKey(indices[mIdx]),
						Value: makeValue(rng, valueBytes, fmt.Sprintf("%05d_%d", opID, mIdx)),
					}
				}
			}
			plan = append(plan, PlanOperation{
				Op:        opID,
				Kind:      "ingest",
				Mutating:  true,
				Sync:      true,
				Mutations: mutations,
				MultiFile: multiFile,
			})
		}

		if cfg.RatchetAt > 0 && (i+1) == cfg.RatchetAt {
			opID++
			plan = append(plan, PlanOperation{
				Op:       opID,
				Kind:     "ratchet",
				Mutating: false,
			})
		}
	}

	return plan
}

// Run executes the planned workload on db, calling marker methods around each
// operation, and returns the verified history ledger.
func Run(db *pebble.DB, marker Marker, cfg Config) ([]history.Entry, error) {
	if err := Validate(cfg); err != nil {
		return nil, fmt.Errorf("workload validate config: %w", err)
	}
	plan := Plan(cfg)
	historyEntries := make([]history.Entry, 0, len(plan))

	var targetFS vfs.FS
	if cfg.FS != nil {
		targetFS = cfg.FS
	} else if fsMarker, ok := marker.(vfs.FS); ok {
		targetFS = fsMarker
	}
	if targetFS == nil && cfg.IngestEvery > 0 {
		return nil, errors.New("workload: ingest requires a valid VFS")
	}

	var eventTracker uint64

	for _, op := range plan {
		eventTracker++
		beginEv := eventTracker
		if marker != nil {
			marker.Begin(op.Op, op.Kind)
		}

		var opErr error
		wo := pebble.NoSync
		if op.Sync {
			wo = pebble.Sync
		}

		switch op.Kind {
		case "set":
			opErr = db.Set(op.Key, op.Value, wo)

		case "get":
			v, closer, err := db.Get(op.Key)
			if err == nil {
				_ = closer.Close()
				_ = v
			} else if err != pebble.ErrNotFound {
				opErr = err
			}

		case "delete":
			opErr = db.Delete(op.Key, wo)

		case "batch":
			v1, closer1, err1 := db.Get(op.Key)
			if err1 == nil {
				_ = closer1.Close()
				_ = v1
			}
			v2, closer2, err2 := db.Get(op.Key2)
			if err2 == nil {
				_ = closer2.Close()
				_ = v2
			}
			b := db.NewBatch()
			if err := b.Set(op.Key, op.Value, nil); err != nil {
				opErr = err
			} else if err := b.Set(op.Key2, op.Value2, nil); err != nil {
				opErr = err
			} else {
				opErr = b.Commit(wo)
			}
			_ = b.Close()

		case "range_delete":
			opErr = db.DeleteRange(op.Key, op.EndKey, wo)

		case "iterate":
			it, err := db.NewIter(nil)
			if err == nil {
				count := 0
				for valid := it.First(); valid && count < 10; valid = it.Next() {
					count++
				}
				_ = it.Close()
			} else {
				opErr = err
			}

		case "flush":
			opErr = db.Flush()
			if opErr == nil {
				db.TestOnlyWaitForCleaning()
			}

		case "compact":
			opErr = db.Compact(context.Background(), nil, []byte("\xff"), false)
			if opErr == nil {
				db.TestOnlyWaitForCleaning()
			}

		case "checkpoint":
			_ = db.Checkpoint("checkpoint")

		case "ingest":
			opErr = runIngest(db, targetFS, op, cfg.BarrierIngest)

		case "ratchet":
			opErr = db.RatchetFormatMajorVersion(pebble.FormatNewest)
		}

		eventTracker++
		endEv := eventTracker
		errStr := ""
		if opErr != nil {
			errStr = opErr.Error()
		}
		if marker != nil {
			marker.End(op.Op, op.Kind, errStr)
		}

		// Build history entry with owned bytes.
		entry := history.Entry{
			Op:         op.Op,
			Kind:       op.Kind,
			Sync:       op.Sync,
			BeginEvent: beginEv,
			EndEvent:   endEv,
			Err:        errStr,
		}

		switch op.Kind {
		case "set":
			entry.Mutations = []history.Mutation{
				{Kind: "set", Key: bytes.Clone(op.Key), Value: bytes.Clone(op.Value)},
			}
		case "delete":
			entry.Mutations = []history.Mutation{
				{Kind: "delete", Key: bytes.Clone(op.Key)},
			}
		case "range_delete":
			entry.Mutations = []history.Mutation{
				{Kind: "range_delete", Key: bytes.Clone(op.Key), End: bytes.Clone(op.EndKey)},
			}
		case "batch":
			entry.Mutations = []history.Mutation{
				{Kind: "set", Key: bytes.Clone(op.Key), Value: bytes.Clone(op.Value)},
				{Kind: "set", Key: bytes.Clone(op.Key2), Value: bytes.Clone(op.Value2)},
			}
		case "ingest":
			entry.Mutations = make([]history.Mutation, len(op.Mutations))
			for j, m := range op.Mutations {
				entry.Mutations[j] = history.Mutation{
					Kind:  m.Kind,
					Key:   bytes.Clone(m.Key),
					Value: bytes.Clone(m.Value),
				}
			}
		}

		historyEntries = append(historyEntries, entry)
	}

	if err := history.Validate(historyEntries); err != nil {
		return nil, fmt.Errorf("workload: %w", err)
	}
	return historyEntries, nil
}

// Correlate maps history entries to the exact event IDs of matching OpBegin and OpEnd in events.
func Correlate(entries []history.Entry, events []trace.Event) ([]history.Entry, error) {
	begins := make(map[uint64]uint64)
	ends := make(map[uint64]uint64)

	for _, e := range events {
		if e.Kind == "OpBegin" && e.Op > 0 {
			begins[e.Op] = e.ID
		} else if e.Kind == "OpEnd" && e.Op > 0 {
			ends[e.Op] = e.ID
		}
	}

	result := history.Clone(entries)
	for i := range result {
		bID, ok := begins[result[i].Op]
		if ok {
			result[i].BeginEvent = bID
		}
		eID, ok := ends[result[i].Op]
		if ok {
			result[i].EndEvent = eID
		}
	}

	if err := history.Validate(result); err != nil {
		return nil, fmt.Errorf("correlate: %w", err)
	}
	return result, nil
}

func runIngest(db *pebble.DB, targetFS vfs.FS, op PlanOperation, barrierIngest bool) error {
	if targetFS == nil {
		return errors.New("workload: ingest requires a valid VFS")
	}
	stagingDir := "ingest_staging"
	if err := targetFS.MkdirAll(stagingDir, 0755); err != nil {
		return fmt.Errorf("mkdir staging: %w", err)
	}
	writeSST := func(path string, muts []history.Mutation) error {
		f, err := targetFS.Create(path, vfs.WriteCategoryUnspecified)
		if err != nil {
			return err
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
			TableFormat: db.TableFormat(),
		})
		for _, m := range muts {
			if err := w.Set(m.Key, m.Value); err != nil {
				_ = w.Close()
				return err
			}
		}
		return w.Close()
	}

	var paths []string
	if !op.MultiFile {
		path := fmt.Sprintf("%s/ingest_%05d.sst", stagingDir, op.Op)
		if err := writeSST(path, op.Mutations); err != nil {
			return err
		}
		paths = []string{path}
	} else {
		path1 := fmt.Sprintf("%s/ingest_%05d_a.sst", stagingDir, op.Op)
		path2 := fmt.Sprintf("%s/ingest_%05d_b.sst", stagingDir, op.Op)
		if err := writeSST(path1, op.Mutations[:2]); err != nil {
			return err
		}
		if err := writeSST(path2, op.Mutations[2:]); err != nil {
			return err
		}
		paths = []string{path1, path2}
	}

	if sDir, err := targetFS.OpenDir(stagingDir); err == nil {
		_ = sDir.Sync()
		_ = sDir.Close()
	}

	if barrierIngest {
		if err := db.LogData(nil, pebble.Sync); err != nil {
			return fmt.Errorf("barrier ingest: %w", err)
		}
	}

	return db.Ingest(context.Background(), paths)
}
