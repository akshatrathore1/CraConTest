// Package workload provides a deterministic client workload generator and driver
// for Pebble databases, recording full mutation histories and marker events.
package workload

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2"
)

// Config specifies the configuration for deterministic workload execution.
type Config struct {
	Seed       uint64 `json:"seed"`
	Operations int    `json:"operations"`
	KeyCount   int    `json:"key_count"`
	ValueBytes int    `json:"value_bytes"`
	SyncEvery  int    `json:"sync_every"`
	FlushEvery int    `json:"flush_every"`
	CompactAt  int    `json:"compact_at,omitempty"`
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
	rng := rand.New(rand.NewPCG(0, cfg.Seed))
	var plan []PlanOperation
	var opID uint64
	var mutatingCount int

	for i := 0; i < cfg.Operations; i++ {
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
	}

	return plan
}

// Run executes the planned workload on db, calling marker methods around each
// operation, and returns the verified history ledger.
func Run(db *pebble.DB, marker Marker, cfg Config) ([]history.Entry, error) {
	plan := Plan(cfg)
	historyEntries := make([]history.Entry, 0, len(plan))

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
