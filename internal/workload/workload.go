// Package workload provides a deterministic client workload generator and driver
// for Pebble databases, recording full mutation histories and marker events.
package workload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"time"

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

// Operation kinds.
const (
	OpSet         = "set"
	OpGet         = "get"
	OpDelete      = "delete"
	OpBatch       = "batch"
	OpRangeDelete = "range_delete"
	OpIterate     = "iterate"
	OpFlush       = "flush"
	OpCompact     = "compact"
	OpCheckpoint  = "checkpoint"
	OpIngest      = "ingest"
	OpRatchet     = "ratchet"
)

type walWriteSyncer interface {
	WALWriteCount() uint64
	WaitForWALWrite(target uint64)
	// NeedWALWriteBarrier reports whether WaitForWALWrite barriers are
	// meaningful for this FS. Only fault-injecting filesystems that may drop
	// WAL writes asynchronously need this barrier; plain recording FSes do not.
	NeedWALWriteBarrier() bool
}

// Config specifies the configuration for deterministic workload execution.
//
// Note on Operation Mix: When Preset is empty, the operation mix is standardized
// in workload.Plan() (and DefaultConfig()): 50% set, 20% get, 10% delete, 10% two-key
// batch, 5% range_delete, and 5% read/iteration. Config files (e.g. main.json, smoke.json)
// specify operational parameters (seed, operations, key_count, value_bytes, sync_every,
// flush_every), while the operation mix is governed by Plan() or selected via Preset
// (ycsb-a, ycsb-b, ycsb-f, overwrite-heavy, delete-heavy).
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
	if cfg.CompactAt < 0 {
		return fmt.Errorf("compact_at must be >= 0, got %d", cfg.CompactAt)
	}
	if cfg.CheckpointAt < 0 {
		return fmt.Errorf("checkpoint_at must be >= 0, got %d", cfg.CheckpointAt)
	}
	if cfg.MaxManifestFileSize < 0 {
		return fmt.Errorf("max_manifest_file_size must be >= 0, got %d", cfg.MaxManifestFileSize)
	}
	if cfg.IngestEvery < 0 {
		return fmt.Errorf("ingest_every must be >= 0, got %d", cfg.IngestEvery)
	}
	if cfg.RatchetAt < 0 {
		return fmt.Errorf("ratchet_at must be >= 0, got %d", cfg.RatchetAt)
	}
	hasRangeDeleteOrIngest := cfg.Preset == "" || cfg.Preset == PresetDeleteHeavy || cfg.IngestEvery > 0
	if hasRangeDeleteOrIngest {
		effectiveKeys := cfg.KeyCount
		if effectiveKeys <= 0 && cfg.Preset != "" {
			effectiveKeys = 64
			if cfg.Preset == PresetDeleteHeavy {
				effectiveKeys = 128
			}
		}
		if effectiveKeys < 2 {
			return fmt.Errorf("key_count must be >= 2 when range deletes or ingest operations are enabled, got %d", cfg.KeyCount)
		}
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
	return fmt.Appendf(nil, "key-%04d", index)
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
			if i%2 == 1 {
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
			} else {
				kIdx := rng.IntN(keyCount)
				plan = append(plan, PlanOperation{
					Op:       opID,
					Kind:     "get",
					Mutating: false,
					Key:      formatKey(kIdx),
				})
			}

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

func drainBackgroundWork(db *pebble.DB) error {
	if db == nil {
		return nil
	}
	timeout := 30 * time.Second
	if env := os.Getenv("CRACONTEST_WAIT_TIMEOUT"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			timeout = d
		}
	}
	deadline := time.Now().Add(timeout)
	waitClean := func() error {
		rem := time.Until(deadline)
		if rem <= 0 {
			return fmt.Errorf("workload: drainBackgroundWork exceeded %v deadline waiting for cleaning", timeout)
		}
		done := make(chan struct{})
		go func() {
			db.TestOnlyWaitForCleaning()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-time.After(rem):
			return fmt.Errorf("workload: drainBackgroundWork exceeded %v deadline waiting for cleaning", timeout)
		}
	}
	yields := 0
	for db.Metrics().MemTable.Count > 1 || db.Metrics().Flush.NumInProgress > 0 || db.Metrics().Compact.NumInProgress > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("workload: drainBackgroundWork exceeded %v deadline waiting for memtable/flush/compact", timeout)
		}
		yields++
		if yields > 50 {
			time.Sleep(100 * time.Microsecond)
		} else {
			runtime.Gosched()
		}
	}
	if err := waitClean(); err != nil {
		return err
	}
	yields = 0
	for db.Metrics().MemTable.Count > 1 || db.Metrics().Flush.NumInProgress > 0 || db.Metrics().Compact.NumInProgress > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("workload: drainBackgroundWork exceeded %v deadline waiting for memtable/flush/compact", timeout)
		}
		yields++
		if yields > 50 {
			time.Sleep(100 * time.Microsecond)
		} else {
			runtime.Gosched()
		}
	}
	if err := waitClean(); err != nil {
		return err
	}
	// 2 ms settling delay is preserved for v1.1.2-evaluation-freeze trace-timing parity because Pebble's internal flush/cleaner loops do not expose a post-drain condition variable.
	time.Sleep(2 * time.Millisecond)
	return nil
}

type primaryDirSetter interface {
	SetPrimaryDBDir(string)
}

func findPrimaryDirSetter(targets ...any) primaryDirSetter {
	for _, t := range targets {
		curr := t
		for curr != nil {
			if ps, ok := curr.(primaryDirSetter); ok {
				return ps
			}
			if u, ok := curr.(interface{ Unwrap() vfs.FS }); ok {
				curr = u.Unwrap()
			} else {
				break
			}
		}
	}
	return nil
}

type ingestSectionGate interface {
	BeginIngestSection()
	EndIngestSection()
}

func findEventProvider(targets ...any) interface{ CurrentEventID() uint64 } {
	for _, t := range targets {
		curr := t
		for curr != nil {
			if ep, ok := curr.(interface{ CurrentEventID() uint64 }); ok {
				return ep
			}
			if u, ok := curr.(interface{ Unwrap() vfs.FS }); ok {
				curr = u.Unwrap()
			} else {
				break
			}
		}
	}
	return nil
}

type eventAwareMarker interface {
	BeginEvent(op uint64, kind string) uint64
	EndEvent(op uint64, kind string, err string) uint64
}

func findEventAwareMarker(targets ...any) eventAwareMarker {
	for _, t := range targets {
		curr := t
		for curr != nil {
			if em, ok := curr.(eventAwareMarker); ok {
				return em
			}
			if u, ok := curr.(interface{ Unwrap() vfs.FS }); ok {
				curr = u.Unwrap()
			} else {
				break
			}
		}
	}
	return nil
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

	dir := "db"
	if setter := findPrimaryDirSetter(targetFS, cfg.FS); setter != nil {
		setter.SetPrimaryDBDir(dir)
	}

	var ingestGate ingestSectionGate
	if ig, ok := marker.(ingestSectionGate); ok {
		ingestGate = ig
	} else if ig, ok := targetFS.(ingestSectionGate); ok {
		ingestGate = ig
	}

	if err := drainBackgroundWork(db); err != nil {
		return nil, err
	}

	var walSync walWriteSyncer
	if ws, ok := cfg.FS.(walWriteSyncer); ok {
		walSync = ws
	} else if ws, ok := targetFS.(walWriteSyncer); ok {
		walSync = ws
	} else if ws, ok := marker.(walWriteSyncer); ok {
		walSync = ws
	}

	evMarker := findEventAwareMarker(marker, targetFS, cfg.FS)
	evProvider := findEventProvider(marker, targetFS, cfg.FS)

	var eventTracker uint64

	for _, op := range plan {
		var prevWAL uint64
		if walSync != nil {
			prevWAL = walSync.WALWriteCount()
		}
		var beginEv uint64
		if evMarker != nil {
			beginEv = evMarker.BeginEvent(op.Op, op.Kind)
		} else if marker != nil {
			marker.Begin(op.Op, op.Kind)
			if evProvider != nil {
				beginEv = evProvider.CurrentEventID()
			}
		}
		if beginEv == 0 {
			eventTracker++
			beginEv = eventTracker
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
			} else if !errors.Is(err1, pebble.ErrNotFound) {
				opErr = err1
			}
			v2, closer2, err2 := db.Get(op.Key2)
			if err2 == nil {
				_ = closer2.Close()
				_ = v2
			} else if opErr == nil && !errors.Is(err2, pebble.ErrNotFound) {
				opErr = err2
			}
			if opErr == nil {
				b := db.NewBatch()
				if err := b.Set(op.Key, op.Value, nil); err != nil {
					opErr = err
				} else if err := b.Set(op.Key2, op.Value2, nil); err != nil {
					opErr = err
				} else {
					opErr = b.Commit(wo)
				}
				_ = b.Close()
			}

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
			// In workloads with periodic checkpoints (e.g., PresetDeleteHeavy with 25 ops
			// and checkpointAt=10, triggering at op 12 and op 24), the second checkpoint
			// returns "checkpoint checkpoint: file already exists" because the target
			// directory "checkpoint" already exists from the first checkpoint. Since
			// results/workload_diversity_*.csv is frozen at f9202dfca579, this deterministic
			// behavior is preserved and documented.
			opErr = db.Checkpoint("checkpoint")

		case "ingest":
			opErr = runIngest(db, targetFS, ingestGate, op, cfg.BarrierIngest)

		case "ratchet":
			opErr = db.RatchetFormatMajorVersion(pebble.FormatNewest)
		}

		if walSync != nil && opErr == nil && op.Sync && walSync.NeedWALWriteBarrier() {
			switch op.Kind {
			case OpSet, OpDelete, OpBatch, OpRangeDelete:
				walSync.WaitForWALWrite(prevWAL + 1)
				// 2 ms settling delay is preserved for v1.1.2-evaluation-freeze trace-timing parity because Pebble's internal flush/cleaner loops do not expose a post-drain condition variable.
				time.Sleep(2 * time.Millisecond)
			}
		}

		if opErr == nil {
			if drainErr := drainBackgroundWork(db); drainErr != nil {
				opErr = drainErr
			}
		}

		errStr := ""
		if opErr != nil {
			errStr = opErr.Error()
		}
		var endEv uint64
		if evMarker != nil {
			endEv = evMarker.EndEvent(op.Op, op.Kind, errStr)
		} else if marker != nil {
			marker.End(op.Op, op.Kind, errStr)
			if evProvider != nil {
				endEv = evProvider.CurrentEventID()
			}
		}
		if endEv == 0 {
			eventTracker++
			endEv = eventTracker
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

func runIngest(db *pebble.DB, targetFS vfs.FS, ingestGate ingestSectionGate, op PlanOperation, barrierIngest bool) (err error) {
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
	if !op.MultiFile || len(op.Mutations) < 2 {
		path := fmt.Sprintf("%s/ingest_%05d.sst", stagingDir, op.Op)
		if err := writeSST(path, op.Mutations); err != nil {
			return err
		}
		paths = []string{path}
	} else {
		mid := len(op.Mutations) / 2
		path1 := fmt.Sprintf("%s/ingest_%05d_a.sst", stagingDir, op.Op)
		path2 := fmt.Sprintf("%s/ingest_%05d_b.sst", stagingDir, op.Op)
		if err := writeSST(path1, op.Mutations[:mid]); err != nil {
			return err
		}
		if err := writeSST(path2, op.Mutations[mid:]); err != nil {
			return err
		}
		paths = []string{path1, path2}
	}
	defer func() {
		if err != nil {
			for _, p := range paths {
				_ = targetFS.Remove(p)
			}
		}
	}()

	sDir, err := targetFS.OpenDir(stagingDir)
	if err != nil {
		return fmt.Errorf("open staging dir %s: %w", stagingDir, err)
	}
	if syncErr := sDir.Sync(); syncErr != nil {
		_ = sDir.Close()
		return fmt.Errorf("sync staging dir %s: %w", stagingDir, syncErr)
	}
	if closeErr := sDir.Close(); closeErr != nil {
		return fmt.Errorf("close staging dir %s: %w", stagingDir, closeErr)
	}

	if barrierIngest {
		if err := db.LogData(nil, pebble.Sync); err != nil {
			return fmt.Errorf("barrier ingest: %w", err)
		}
	}

	err = func() error {
		if ingestGate != nil {
			ingestGate.BeginIngestSection()
			defer ingestGate.EndIngestSection()
		}
		return db.Ingest(context.Background(), paths)
	}()
	if err == nil {
		if drainErr := drainBackgroundWork(db); drainErr != nil {
			err = drainErr
		}
	}
	return err
}
