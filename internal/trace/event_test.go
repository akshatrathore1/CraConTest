package trace_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func contractTrace() []trace.Event {
	return []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindOpenDir, File: "db", Handle: 1, Dir: true},
		{ID: 3, Kind: trace.KindCreate, File: "db/log", Handle: 2, Cat: "pebble-wal"},
		{ID: 4, Kind: trace.KindWrite, File: "db/log", Handle: 2, Data: []byte("abcd"), Len: 4},
		{ID: 5, Kind: trace.KindOpenReadWrite, File: "db/log", Handle: 3},
		{ID: 6, Kind: trace.KindRead, File: "db/log", Handle: 3, Size: 2, Len: 2},
		{ID: 7, Kind: trace.KindWriteAt, File: "db/log", Handle: 3, Off: 1, Data: []byte("Z"), Len: 1},
		{ID: 8, Kind: trace.KindWrite, File: "db/log", Handle: 3, Off: 2, Data: []byte("Q"), Len: 1},
		{ID: 9, Kind: trace.KindSyncData, File: "db/log", Handle: 2},
		{ID: 10, Kind: trace.KindSyncTo, File: "db/log", Handle: 2, Size: 4},
		{ID: 11, Kind: trace.KindLink, File: "db/log", File2: "db/link"},
		{ID: 12, Kind: trace.KindRename, File: "db/log", File2: "db/new"},
		{ID: 13, Kind: trace.KindRemove, File: "db/new"},
		{ID: 14, Kind: trace.KindWriteAt, File: "db/log", Handle: 3, Data: []byte("U"), Len: 1},
		{ID: 15, Kind: trace.KindClose, File: "db/log", Handle: 2},
		{ID: 16, Kind: trace.KindClose, File: "db/log", Handle: 3},
		{ID: 17, Kind: trace.KindSync, File: "db", Handle: 1, Dir: true},
		{ID: 18, Kind: trace.KindClose, File: "db", Handle: 1, Dir: true},
		{ID: 19, Kind: trace.KindLock, File: "db/LOCK", Handle: 4},
		{ID: 20, Kind: trace.KindUnlock, File: "db/LOCK", Handle: 4},
		{ID: 21, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"},
		{ID: 22, Kind: trace.KindOpEnd, Op: 1, OpKind: "set"},
		{ID: 23, Kind: trace.KindEngineEvent, Note: "CompactionEnd", Job: 1, Inputs: []string{"db/link"}, Outputs: []string{"db/004.sst"}},
	}
}

func TestTraceV1RoundTrip(test *testing.T) {
	events := contractTrace()
	var encoded bytes.Buffer
	if err := trace.Encode(&encoded, events); err != nil {
		test.Fatal(err)
	}
	decoded, err := trace.Decode(&encoded)
	if err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(events, decoded) {
		test.Fatal("trace round trip changed event values")
	}
	copyOfEvents := trace.Clone(events)
	events[3].Data[0] = 'X'
	events[22].Inputs[0] = "changed"
	if string(copyOfEvents[3].Data) != "abcd" || copyOfEvents[22].Inputs[0] != "db/link" {
		test.Fatal("trace snapshots alias mutable inputs")
	}
}

func TestTraceV1RejectsInvalidRecords(test *testing.T) {
	cases := map[string]func([]trace.Event){
		"ID gap":                    func(events []trace.Event) { events[3].ID = 99 },
		"unknown kind":              func(events []trace.Event) { events[3].Kind = "Future" },
		"missing handle":            func(events []trace.Event) { events[3].Handle = 0 },
		"reused handle":             func(events []trace.Event) { events[4].Handle = 2 },
		"excess write count":        func(events []trace.Event) { events[3].Len = 5 },
		"short write without error": func(events []trace.Event) { events[3].Len = 2 },
		"payload on sync":           func(events []trace.Event) { events[8].Data = []byte("x") },
		"wrong cursor":              func(events []trace.Event) { events[7].Off = 3 },
		"directory mismatch":        func(events []trace.Event) { events[16].Dir = false },
		"wrong resource close":      func(events []trace.Event) { events[19].Kind = trace.KindClose },
		"overlapping operation":     func(events []trace.Event) { events[21].Kind = trace.KindOpBegin },
		"path traversal":            func(events []trace.Event) { events[0].File = "../db" },
		"absolute path":             func(events []trace.Event) { events[0].File = "/db" },
		"full sync on write":        func(events []trace.Event) { events[3].FullSync = true },
		"background attribution":    func(events []trace.Event) { events[3].Op = 1 },
		"category on write":         func(events []trace.Event) { events[3].Cat = "pebble-wal" },
		"category on sync":          func(events []trace.Event) { events[8].Cat = "pebble-wal" },
	}
	for name, corrupt := range cases {
		test.Run(name, func(test *testing.T) {
			events := contractTrace()
			corrupt(events)
			if err := trace.Validate(events); err == nil {
				test.Fatal("invalid trace accepted")
			}
		})
	}
	for _, document := range []string{
		`{"schema_version":2,"events":[]}`,
		`{"schema_version":1}`,
		`{"schema_version":1,"events":null}`,
		`{"schema_version":1,"events":[],"unknown":1}`,
		`{"schema_version":1,"events":[]} {}`,
	} {
		if _, err := trace.Decode(strings.NewReader(document)); err == nil {
			test.Fatalf("invalid document accepted: %s", document)
		}
	}
}

func TestTraceV1BoundaryCuts(test *testing.T) {
	events := contractTrace()
	cuts, err := trace.CutIDs(events)
	if err != nil {
		test.Fatal(err)
	}
	for _, required := range []uint64{0, 8, 9, 11, 12, 16, 17, 22} {
		found := false
		for _, cut := range cuts {
			found = found || cut == required
		}
		if !found {
			test.Fatalf("required cutoff %d absent", required)
		}
	}
	if !trace.BeforeBoundary(events, 8) || !trace.BeforeBoundary(events, 11) ||
		trace.BeforeBoundary(events, 9) || events[9].IsBarrier() {
		test.Fatal("before-boundary or SyncTo semantics are incorrect")
	}
	failedSync := events[8]
	failedSync.Err = "sync failed"
	if failedSync.IsBarrier() {
		test.Fatal("failed sync is a barrier")
	}
	fullSync := events[9]
	fullSync.FullSync = true
	if !fullSync.IsBarrier() {
		test.Fatal("successful full SyncTo is not a barrier")
	}
}

func TestTraceV1OperationMarkers(test *testing.T) {
	for _, kind := range []string{"set", "delete", "range_delete", "batch", "get", "iterate", "flush", "compact"} {
		events := []trace.Event{{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: kind}, {ID: 2, Kind: trace.KindOpEnd, Op: 1, OpKind: kind}}
		if err := trace.Validate(events); err != nil {
			test.Fatal(err)
		}
	}
	for _, events := range [][]trace.Event{
		{{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "future"}},
		{{ID: 1, Kind: trace.KindOpBegin, Op: 2, OpKind: "set"}, {ID: 2, Kind: trace.KindOpEnd, Op: 2, OpKind: "set"}, {ID: 3, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"}},
	} {
		if err := trace.Validate(events); err == nil {
			test.Fatal("unknown or nonmonotonic operation accepted")
		}
	}
	if err := trace.Validate([]trace.Event{{ID: 1, Kind: trace.KindOpBegin, Op: 1, OpKind: "set"}}); err != nil {
		test.Fatal("an in-flight final operation must remain representable")
	}
}

func TestTraceV1PartialWrite(test *testing.T) {
	events := []trace.Event{
		{ID: 1, Kind: trace.KindCreate, File: "data", Handle: 1},
		{ID: 2, Kind: trace.KindWrite, File: "data", Handle: 1, Data: []byte("abcd"), Len: 2, Err: "short write"},
		{ID: 3, Kind: trace.KindWrite, File: "data", Handle: 1, Off: 2, Data: []byte("ef"), Len: 2},
		{ID: 4, Kind: trace.KindClose, File: "data", Handle: 1},
	}
	var encoded bytes.Buffer
	if err := trace.Encode(&encoded, events); err != nil {
		test.Fatal(err)
	}
	decoded, err := trace.Decode(&encoded)
	if err != nil {
		test.Fatal(err)
	}
	if decoded[1].Len != 2 || string(decoded[1].Data) != "abcd" || decoded[2].Off != 2 {
		test.Fatal("partial-write input/result/cursor semantics changed")
	}
	var empty bytes.Buffer
	if err := trace.Encode(&empty, nil); err != nil {
		test.Fatal(err)
	}
	if _, err := trace.Decode(&empty); err != nil {
		test.Fatal(err)
	}
	invalid := contractTrace()
	invalid[2].Dir = true
	if err := trace.Validate(invalid); err == nil {
		test.Fatal("Create returned a directory")
	}
}

func TestCrashCloneSeedReproducibility(test *testing.T) {
	filesystem := vfs.NewCrashableMem()
	for fileIndex := 0; fileIndex < 20; fileIndex++ {
		name := fmt.Sprintf("file-%02d", fileIndex)
		file, err := filesystem.Create(name, vfs.WriteCategoryUnspecified)
		if err != nil {
			test.Fatal(err)
		}
		payload := bytes.Repeat([]byte{byte(fileIndex + 1)}, 8192)
		if _, err := file.Write(payload); err != nil {
			test.Fatal(err)
		}
		if err := file.Close(); err != nil {
			test.Fatal(err)
		}
	}
	directory, err := filesystem.OpenDir("")
	if err != nil {
		test.Fatal(err)
	}
	if err := directory.Sync(); err != nil {
		test.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		test.Fatal(err)
	}
	currentDigest := filesystemDigest(test, filesystem)

	for _, percent := range []int{0, 50, 100} {
		test.Run(fmt.Sprintf("percent=%d", percent), func(test *testing.T) {
			digests := make(map[string]struct{})
			for attempt := 0; attempt < 32; attempt++ {
				clone := filesystem.CrashClone(vfs.CrashCloneCfg{
					UnsyncedDataPercent: percent,
					RNG:                 rand.New(rand.NewPCG(0, 42)),
				})
				digest := filesystemDigest(test, clone)
				digests[digest] = struct{}{}
				if percent == 100 && digest != currentDigest {
					test.Fatal("100-percent clone differs from current filesystem")
				}
			}
			if percent != 50 && len(digests) != 1 {
				test.Fatalf("endpoint produced %d images for the same source", len(digests))
			}
			test.Logf("same filesystem and PCG seed: %d distinct images in 32 clones", len(digests))
		})
	}
}

func filesystemDigest(test *testing.T, filesystem vfs.FS) string {
	test.Helper()
	names, err := filesystem.List("")
	if err != nil {
		test.Fatal(err)
	}
	sort.Strings(names)
	digest := sha256.New()
	for _, name := range names {
		file, err := filesystem.Open(name)
		if err != nil {
			test.Fatal(err)
		}
		data, err := io.ReadAll(file)
		if err != nil {
			test.Fatal(err)
		}
		if err := file.Close(); err != nil {
			test.Fatal(err)
		}
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(name)))
		digest.Write(length[:])
		digest.Write([]byte(name))
		binary.BigEndian.PutUint64(length[:], uint64(len(data)))
		digest.Write(length[:])
		digest.Write(data)
	}
	return fmt.Sprintf("%x", digest.Sum(nil))
}
