package candidate_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

func smallTrace() []trace.Event {
	return []trace.Event{
		{ID: 1, Kind: trace.KindMkdirAll, File: "db", Mode: 0755},
		{ID: 2, Kind: trace.KindCreate, File: "db/log", Handle: 1, Cat: "pebble-wal"},
		{ID: 3, Kind: trace.KindWrite, File: "db/log", Handle: 1, Data: []byte("ab"), Len: 2},
		{ID: 4, Kind: trace.KindSyncData, File: "db/log", Handle: 1},
		{ID: 5, Kind: trace.KindClose, File: "db/log", Handle: 1},
	}
}

func TestSeedVectors(test *testing.T) {
	vectors := []struct {
		base, event uint64
		percent     int
		want        uint64
	}{
		{0, 0, 0, 5324358737731858600},
		{0, 1, 0, 14675981572963742630},
		{1, 9, 100, 7177736550350473416},
		{42, 9, 50, 11532854960572963643},
	}
	for _, vector := range vectors {
		got, err := candidate.DeriveSeed(vector.base, vector.event, vector.percent)
		if err != nil || got != vector.want {
			test.Fatalf("DeriveSeed(%d,%d,%d) = %d, %v; want %d", vector.base, vector.event, vector.percent, got, err, vector.want)
		}
	}
	for _, percent := range []int{-1, 101} {
		if _, err := candidate.DeriveSeed(0, 0, percent); err == nil {
			test.Fatalf("percent %d accepted", percent)
		}
	}
}

func TestFullTupleIdentity(test *testing.T) {
	zero := candidate.Candidate{}
	other := candidate.Candidate{Seed: 1}
	identities := map[candidate.Candidate]bool{zero: true, other: true}
	if len(identities) != 2 {
		test.Fatal("equal event/percent with different seeds collapsed")
	}
	if err := candidate.Validate(smallTrace(), []candidate.Candidate{zero, other, {Event: 5, Percent: 100}}); err != nil {
		test.Fatalf("zero cutoff, zero seed and last endpoint rejected: %v", err)
	}
	encoded, err := json.Marshal(zero)
	if err != nil || string(encoded) != `{"event":0,"percent":0,"seed":0}` {
		test.Fatalf("wire shape changed: %s %v", encoded, err)
	}
}

func TestBuildDeterministicCanonical(test *testing.T) {
	events := smallTrace()
	first, err := candidate.Build(events, 42, []int{100, 0})
	if err != nil {
		test.Fatal(err)
	}
	second, err := candidate.Build(trace.Clone(events), 42, []int{0, 100})
	if err != nil {
		test.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		test.Fatal("Build is not deterministic or depends on policy order")
	}
	cuts, _ := trace.CutIDs(events)
	if len(first) != 2*len(cuts) {
		test.Fatalf("universe size %d, want %d", len(first), 2*len(cuts))
	}
	for index, item := range first {
		if index > 0 && !candidate.Less(first[index-1], item) {
			test.Fatal("universe not in strict canonical order")
		}
		seed, _ := candidate.DeriveSeed(42, item.Event, item.Percent)
		if item.Seed != seed {
			test.Fatal("seed not derived from (base, event, percent)")
		}
	}
	if first[0].Event != 0 || first[0].Percent != 0 || first[1].Percent != 100 {
		test.Fatal("pretrace cutoff missing for both endpoint policies")
	}
	if first[len(first)-1].Event > uint64(len(events)) {
		test.Fatal("cutoff beyond trace length")
	}
	first[0].Seed = 7
	if second[0].Seed == 7 {
		test.Fatal("Build results alias each other")
	}
}

func TestBuildRejects(test *testing.T) {
	events := smallTrace()
	for name, policies := range map[string][]int{
		"empty":        {},
		"intermediate": {0, 50},
		"duplicate":    {0, 0},
		"out of range": {101},
	} {
		if _, err := candidate.Build(events, 0, policies); err == nil {
			test.Fatalf("%s policies accepted", name)
		}
	}
	broken := smallTrace()
	broken[2].ID = 9
	if _, err := candidate.Build(broken, 0, []int{0}); err == nil {
		test.Fatal("invalid trace accepted")
	}
}

func TestValidateRejects(test *testing.T) {
	events := smallTrace()
	for name, universe := range map[string][]candidate.Candidate{
		"beyond trace": {{Event: 6}},
		"negative":     {{Percent: -1}},
		"over 100":     {{Percent: 101}},
		"duplicate":    {{Event: 1}, {Event: 1}},
	} {
		if err := candidate.Validate(events, universe); err == nil {
			test.Fatalf("%s universe accepted", name)
		}
	}
	if err := candidate.Validate(events, []candidate.Candidate{{Event: 3, Percent: 50}}); err != nil {
		test.Fatalf("representable intermediate tuple rejected by Validate: %v", err)
	}
	broken := smallTrace()
	broken[0].File = "/db"
	if err := candidate.Validate(broken, nil); err == nil {
		test.Fatal("invalid trace accepted by Validate")
	}
}

func TestWireRoundTrip(test *testing.T) {
	universe := []candidate.Candidate{
		{Event: 0, Percent: 0, Seed: 0},
		{Event: 0, Percent: 0, Seed: 1<<63 + 12345},
		{Event: 9, Percent: 50, Seed: 11532854960572963643},
		{Event: 9, Percent: 100, Seed: 7177736550350473416},
	}
	var buffer bytes.Buffer
	if err := candidate.Encode(&buffer, universe); err != nil {
		test.Fatal(err)
	}
	if !strings.Contains(buffer.String(), "9223372036854788153") {
		test.Fatalf("uint64 above 2^53 not written exactly: %s", buffer.String())
	}
	decoded, err := candidate.Decode(&buffer)
	if err != nil || !reflect.DeepEqual(decoded, universe) {
		test.Fatalf("round trip changed universe: %v %v", decoded, err)
	}
	buffer.Reset()
	if err := candidate.Encode(&buffer, nil); err != nil {
		test.Fatal(err)
	}
	if !strings.Contains(buffer.String(), `"candidates":[]`) {
		test.Fatalf("empty universe not encoded as []: %s", buffer.String())
	}
	empty, err := candidate.Decode(&buffer)
	if err != nil || empty == nil || len(empty) != 0 {
		test.Fatal("empty universe round trip failed")
	}
	if err := candidate.Encode(&buffer, []candidate.Candidate{universe[1], universe[0]}); err == nil {
		test.Fatal("non-canonical universe encoded")
	}
}

func TestDecodeRejects(test *testing.T) {
	for _, document := range []string{
		`{"schema_version":2,"candidates":[]}`,
		`{"candidates":[]}`,
		`{"schema_version":1}`,
		`{"schema_version":1,"candidates":null}`,
		`{"schema_version":1,"candidates":[],"extra":1}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":0,"seed":0,"x":1}]}`,
		`{"schema_version":1,"candidates":[]} {}`,
		`{"schema_version":1,"candidates":[{"event":2,"percent":0,"seed":0},{"event":1,"percent":0,"seed":0}]}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":0,"seed":0},{"event":1,"percent":0,"seed":0}]}`,
		`{"schema_version":1,"candidates":[{"event":1,"percent":101,"seed":0}]}`,
		`{"schema_version":1,"candidates":[{"event":1.5,"percent":0,"seed":0}]}`,
	} {
		if _, err := candidate.Decode(strings.NewReader(document)); err == nil {
			test.Fatalf("invalid document accepted: %s", document)
		}
	}
}
