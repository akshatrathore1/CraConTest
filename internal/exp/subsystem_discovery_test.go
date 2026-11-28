package exp

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// SubsystemDiscoveryWorkload defines the configuration for a discovery trial.
type SubsystemDiscoveryWorkload struct {
	Name                string
	Operations          int
	KeyCount            int
	ValueBytes          int
	SyncEvery           int
	FlushEvery          int
	CompactAt           int
	CheckpointAt        int
	MaxManifestFileSize int64
	IngestEvery         int
	RatchetAt           int
	Policies            []int
}

// DiscoveryRecord stores the empirical discovery results for a given workload and seed.
type DiscoveryRecord struct {
	WorkloadName        string
	Seed                uint64
	Operations          int
	IngestEvery         int
	RatchetAt           int
	CheckpointAt        int
	MaxManifestFileSize int64
	Policies            string
	UniverseSize        int
	Violations          int
	DurablyCommitted    int
	PrefixViolations    int
	StructuralViols     int
	OpenErrors          int
	Panics              int
	Hangs               int
	OracleErrors        int
	GitCommit           string
}

// validateSubsystemDiscoveryCSV verifies the committed discovery CSV artifact.
func validateSubsystemDiscoveryCSV(t *testing.T, csvPath string) {
	t.Helper()
	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("subsystem_discovery.csv not found at %s: %v (run with -update to regenerate)", csvPath, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		t.Fatalf("read header: %v", err)
	}

	colIdx := func(name string) int {
		for i, h := range header {
			if h == name {
				return i
			}
		}
		t.Fatalf("column %q not found in header %v", name, header)
		return -1
	}

	wIdx := colIdx("workload")
	univIdx := colIdx("universe_size")
	violIdx := colIdx("violations")
	dcIdx := colIdx("durably_committed")
	prefixIdx := colIdx("prefix_violations")
	structIdx := colIdx("structural_violations")
	oeIdx := colIdx("open_errors")
	panicIdx := colIdx("panics")
	hangIdx := colIdx("hangs")
	oracleErrIdx := -1
	for i, h := range header {
		if h == "oracle_errors" {
			oracleErrIdx = i
			break
		}
	}

	rows := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read row %d: %v", rows+1, err)
		}
		rows++
		if len(rec) != len(header) {
			t.Fatalf("row %d column count = %d, want %d", rows, len(rec), len(header))
		}
		if rec[wIdx] == "" {
			t.Fatalf("row %d: empty workload name", rows)
		}
		univ, err := strconv.Atoi(rec[univIdx])
		if err != nil || univ <= 0 {
			t.Fatalf("row %d: invalid universe_size %q", rows, rec[univIdx])
		}
		for _, idx := range []int{violIdx, prefixIdx, structIdx, oeIdx} {
			if v, err := strconv.Atoi(rec[idx]); err != nil || v < 0 {
				t.Fatalf("row %d: invalid count %q=%s", rows, header[idx], rec[idx])
			}
		}
		for _, idx := range []int{dcIdx, panicIdx, hangIdx} {
			v, err := strconv.Atoi(rec[idx])
			if err != nil || v != 0 {
				t.Fatalf("row %d: non-zero count %q=%s in %s (expected 0)", rows, header[idx], rec[idx], csvPath)
			}
		}
		if oracleErrIdx >= 0 {
			if v, err := strconv.Atoi(rec[oracleErrIdx]); err != nil || v != 0 {
				t.Fatalf("row %d: non-zero oracle_errors=%s (expected 0)", rows, rec[oracleErrIdx])
			}
		}
	}

	if rows != 9 {
		t.Fatalf("expected 9 data rows (3 workloads x 3 seeds) in %s, got %d", csvPath, rows)
	}
	t.Logf("Validated %d rows in %s with 0 durably_committed violations, 0 panics, 0 hangs", rows, csvPath)
}

// writeSubsystemDiscoveryCSV writes the empirical discovery records atomically.
func writeSubsystemDiscoveryCSV(records []DiscoveryRecord, csvPath string) error {
	return writeCSVAtomic(csvPath, func(w *csv.Writer) error {
		header := []string{
			"workload", "seed", "operations", "ingest_every", "ratchet_at",
			"checkpoint_at", "max_manifest_file_size", "policies", "universe_size",
			"violations", "durably_committed", "prefix_violations", "structural_violations",
			"open_errors", "panics", "hangs", "oracle_errors", "git_commit",
		}
		if err := w.Write(header); err != nil {
			return err
		}

		for _, rec := range records {
			row := []string{
				rec.WorkloadName,
				strconv.FormatUint(rec.Seed, 10),
				strconv.Itoa(rec.Operations),
				strconv.Itoa(rec.IngestEvery),
				strconv.Itoa(rec.RatchetAt),
				strconv.Itoa(rec.CheckpointAt),
				strconv.FormatInt(rec.MaxManifestFileSize, 10),
				rec.Policies,
				strconv.Itoa(rec.UniverseSize),
				strconv.Itoa(rec.Violations),
				strconv.Itoa(rec.DurablyCommitted),
				strconv.Itoa(rec.PrefixViolations),
				strconv.Itoa(rec.StructuralViols),
				strconv.Itoa(rec.OpenErrors),
				strconv.Itoa(rec.Panics),
				strconv.Itoa(rec.Hangs),
				strconv.Itoa(rec.OracleErrors),
				rec.GitCommit,
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
		return nil
	})
}

// runSubsystemTrial executes a full workload run under MUT-0 and explores all crash candidates.
func runSubsystemTrial(t *testing.T, w SubsystemDiscoveryWorkload, seed uint64) DiscoveryRecord {
	t.Helper()

	wCfg := workload.Config{
		Seed:                seed,
		Operations:          w.Operations,
		KeyCount:            w.KeyCount,
		ValueBytes:          w.ValueBytes,
		SyncEvery:           w.SyncEvery,
		FlushEvery:          w.FlushEvery,
		CompactAt:           w.CompactAt,
		CheckpointAt:        w.CheckpointAt,
		MaxManifestFileSize: w.MaxManifestFileSize,
		IngestEvery:         w.IngestEvery,
		RatchetAt:           w.RatchetAt,
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	fCfg := faults.DefaultConfig(faults.MUT0)
	faultFS, err := faults.NewWithConfig(fCfg, rec)
	if err != nil {
		t.Fatalf("%s seed %d: init fault MUT-0 failed: %v", w.Name, seed, err)
	}

	listener := rec.EventListener()
	opts := &pebble.Options{
		FS:                          faultFS,
		Logger:                      recovery.PanicLogger{},
		MemTableSize:                64 * 1024,
		EventListener:               &listener,
		DisableAutomaticCompactions: true,
		DisableTableStats:           true,
		L0CompactionThreshold:       1000,
		L0StopWritesThreshold:       1000,
	}
	if w.MaxManifestFileSize > 0 {
		opts.MaxManifestFileSize = w.MaxManifestFileSize
	}
	if w.RatchetAt > 0 {
		opts.FormatMajorVersion = pebble.FormatMinSupported
	}
	wCfg.FS = faultFS

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("%s seed %d: open pebble failed: %v", w.Name, seed, err)
	}

	rawHist, err := workload.Run(db, rec, wCfg)
	if err != nil {
		_, _ = safeCloseDB(db)
		t.Fatalf("%s seed %d: run workload failed: %v", w.Name, seed, err)
	}
	db.TestOnlyWaitForCleaning()
	if panicked, closeErr := safeCloseDB(db); panicked || closeErr != nil {
		t.Fatalf("%s seed %d: close pebble failed (panic=%v): %v", w.Name, seed, panicked, closeErr)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("%s seed %d: validate trace failed: %v", w.Name, seed, err)
	}

	correlated, err := workload.Correlate(rawHist, traceEvents)
	if err != nil {
		t.Fatalf("%s seed %d: correlate history failed: %v", w.Name, seed, err)
	}

	policies := w.Policies
	if len(policies) == 0 {
		policies = []int{0, 50, 100}
	}

	universe, err := candidate.Build(traceEvents, seed, policies)
	if err != nil {
		t.Fatalf("%s seed %d: build universe failed: %v", w.Name, seed, err)
	}

	expRes, err := RunExplore(RunnerConfig{
		Universe: universe,
		Events:   traceEvents,
		History:  correlated,
		Dir:      "db",
		Timeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("%s seed %d: run explore failed: %v", w.Name, seed, err)
	}
	if expRes.Summary.OracleErrors != 0 {
		t.Fatalf("%s seed %d: expected 0 oracle_errors, got %d", w.Name, seed, expRes.Summary.OracleErrors)
	}
	if err := ValidateTable(universe, NormalizeTable(expRes.Outcomes)); err != nil {
		t.Fatalf("%s seed %d: ValidateTable failed: %v", w.Name, seed, err)
	}

	var durablyCommitted, prefixViols, structuralViols int
	for _, outcome := range expRes.Outcomes {
		switch outcome.Class {
		case oracle.ClassViolation:
			switch outcome.Rule {
			case "durably_committed":
				durablyCommitted++
			case "prefix_consistency", "prefix":
				prefixViols++
			case "structural":
				structuralViols++
			}
			t.Logf("VIOLATION observed on %s seed %d: Candidate=%+v Rule=%s Sig=%s Detail=%s",
				w.Name, seed, outcome.Candidate, outcome.Rule, outcome.Signature, outcome.Detail)
		case oracle.ClassOpenError:
			t.Logf("OPEN_ERROR observed on %s seed %d: Candidate=%+v Sig=%s Detail=%s",
				w.Name, seed, outcome.Candidate, outcome.Signature, outcome.Detail)
		case oracle.ClassPanic:
			t.Logf("PANIC observed on %s seed %d: Candidate=%+v Sig=%s Detail=%s",
				w.Name, seed, outcome.Candidate, outcome.Signature, outcome.Detail)
		}
	}

	policiesStr := fmt.Sprintf("%v", policies)

	recRecord := DiscoveryRecord{
		WorkloadName:        w.Name,
		Seed:                seed,
		Operations:          w.Operations,
		IngestEvery:         w.IngestEvery,
		RatchetAt:           w.RatchetAt,
		CheckpointAt:        w.CheckpointAt,
		MaxManifestFileSize: w.MaxManifestFileSize,
		Policies:            policiesStr,
		UniverseSize:        len(universe),
		Violations:          expRes.Summary.Violations,
		DurablyCommitted:    durablyCommitted,
		PrefixViolations:    prefixViols,
		StructuralViols:     structuralViols,
		OpenErrors:          expRes.Summary.OpenErrors,
		Panics:              expRes.Summary.Panics,
		Hangs:               expRes.Summary.Hangs,
		OracleErrors:        expRes.Summary.OracleErrors,
		GitCommit:           CurrentGitCommit(),
	}

	// Verification Assertions for Requirement R3:
	if durablyCommitted != 0 {
		t.Errorf("%s seed %d: expected 0 durably_committed violations, got %d", w.Name, seed, durablyCommitted)
	}
	if expRes.Summary.Panics != 0 {
		t.Errorf("%s seed %d: expected 0 panics during recovery, got %d", w.Name, seed, expRes.Summary.Panics)
	}
	if expRes.Summary.Hangs != 0 {
		t.Errorf("%s seed %d: expected 0 hangs during recovery, got %d", w.Name, seed, expRes.Summary.Hangs)
	}

	// Verify ratchet-triggering workloads actually recorded ratchet ops and suffered 0 post-ratchet open/panic failures.
	if w.RatchetAt > 0 {
		var ratchetBegin uint64
		for _, entry := range correlated {
			if entry.Kind == "ratchet" {
				ratchetBegin = entry.BeginEvent
				break
			}
		}
		if ratchetBegin == 0 {
			t.Fatalf("%s seed %d: expected ratchet operation in correlated history", w.Name, seed)
		}
		if w.Name == "Workload B: Live Format-Version Ratcheting" {
			if expRes.Summary.OpenErrors != 0 {
				t.Errorf("%s seed %d: expected 0 open_errors in live ratchet run, got %d", w.Name, seed, expRes.Summary.OpenErrors)
			}
			for _, outcome := range expRes.Outcomes {
				if outcome.Candidate.Event >= ratchetBegin {
					if outcome.Class == oracle.ClassOpenError || outcome.Class == oracle.ClassPanic {
						t.Errorf("%s seed %d: crash during/after ratchet (event %d >= %d) caused %s: %s",
							w.Name, seed, outcome.Candidate.Event, ratchetBegin, outcome.Class, outcome.Detail)
					}
				}
			}
		}
	}

	if w.CheckpointAt > 0 {
		var ckptBegin uint64
		for _, entry := range correlated {
			if entry.Kind == "checkpoint" {
				ckptBegin = entry.BeginEvent
				break
			}
		}
		if ckptBegin == 0 {
			t.Fatalf("%s seed %d: expected checkpoint operation in correlated history", w.Name, seed)
		}
	}

	return recRecord
}

// TestSubsystemDiscovery executes Requirement R3 empirical discovery across:
// - Workload A: External SSTable Ingestion (IngestEvery: 25, Policies: 0, 50, 100, MUT-0)
// - Workload B: Live Format-Version Ratcheting (RatchetAt: 50, Policies: 0, 50, 100, MUT-0)
// - Workload C: Combined Lifecycle Stress (IngestEvery: 30, RatchetAt: 40, CheckpointAt: 60, MaxManifestFileSize: 1, Policies: 0, 50, 100, MUT-0)
// Across representative seeds (42, 100, 777).
// Generation of results/subsystem_discovery.csv is guarded by the package-level *updateArtifacts flag.
func TestSubsystemDiscovery(t *testing.T) {
	resultsDir := findResultsDirInternal(t)
	csvPath := filepath.Join(resultsDir, "subsystem_discovery.csv")

	if !*updateArtifacts {
		validateSubsystemDiscoveryCSV(t, csvPath)
		if !testing.Short() {
			liveWorkloads := []SubsystemDiscoveryWorkload{
				{
					Name:        "Workload A: External SSTable Ingestion",
					Operations:  45,
					KeyCount:    32,
					ValueBytes:  64,
					SyncEvery:   5,
					FlushEvery:  20,
					IngestEvery: 15,
					Policies:    []int{0, 50, 100},
				},
				{
					Name:       "Workload B: Live Format-Version Ratcheting",
					Operations: 55,
					KeyCount:   32,
					ValueBytes: 64,
					SyncEvery:  8,
					FlushEvery: 20,
					RatchetAt:  30,
					Policies:   []int{0, 50, 100},
				},
				{
					Name:                "Workload C: Combined Lifecycle Stress",
					Operations:          55,
					KeyCount:            32,
					ValueBytes:          64,
					SyncEvery:           8,
					FlushEvery:          20,
					IngestEvery:         20,
					RatchetAt:           25,
					CheckpointAt:        35,
					MaxManifestFileSize: 1,
					Policies:            []int{0, 50, 100},
				},
			}
			for _, lw := range liveWorkloads {
				_ = runSubsystemTrial(t, lw, 42)
			}
		}
		return
	}

	workloads := []SubsystemDiscoveryWorkload{
		{
			Name:        "Workload A: External SSTable Ingestion",
			Operations:  80,
			KeyCount:    64,
			ValueBytes:  128,
			SyncEvery:   10,
			FlushEvery:  40,
			IngestEvery: 25,
			Policies:    []int{0, 50, 100},
		},
		{
			Name:       "Workload B: Live Format-Version Ratcheting",
			Operations: 80,
			KeyCount:   64,
			ValueBytes: 128,
			SyncEvery:  10,
			FlushEvery: 40,
			RatchetAt:  50,
			Policies:   []int{0, 50, 100},
		},
		{
			Name:                "Workload C: Combined Lifecycle Stress",
			Operations:          80,
			KeyCount:            64,
			ValueBytes:          128,
			SyncEvery:           10,
			FlushEvery:          40,
			IngestEvery:         30,
			RatchetAt:           40,
			CheckpointAt:        60,
			MaxManifestFileSize: 1,
			Policies:            []int{0, 50, 100},
		},
	}

	seeds := []uint64{42, 100, 777}
	var records []DiscoveryRecord

	for _, w := range workloads {
		for _, seed := range seeds {
			rec := runSubsystemTrial(t, w, seed)
			records = append(records, rec)
			t.Logf("[%s seed %d] Universe=%d Violations=%d (durably_committed=%d, prefix=%d) OpenErrors=%d Panics=%d",
				rec.WorkloadName, rec.Seed, rec.UniverseSize, rec.Violations, rec.DurablyCommitted, rec.PrefixViolations, rec.OpenErrors, rec.Panics)
		}
	}

	// Guard CSV generation with *updateArtifacts flag
	if err := writeSubsystemDiscoveryCSV(records, csvPath); err != nil {
		t.Fatalf("failed to write %s: %v", csvPath, err)
	}
	t.Logf("Successfully wrote empirical discovery artifact -> %s (%d rows)", csvPath, len(records))
}
