package exp

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/faults"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/akshatrathore1/CraConTest/internal/vfsrec"
	"github.com/akshatrathore1/CraConTest/internal/workload"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type shapeSpec struct {
	id         int
	ops        int
	keyCount   int
	valueBytes int
	syncEvery  int
	flushEvery int
	compactAt  int
}

type nullControlRecord struct {
	shapeID      int
	seed         uint64
	operations   int
	keyCount     int
	valueBytes   int
	syncEvery    int
	flushEvery   int
	compactAt    int
	universeSize int
	violations   int
	openErrors   int
	panics       int
	hangs        int
	gitCommit    string
}

// validateNullControlCSV reads the committed CSV and checks it has 108 rows with all-zero failure counts.
func validateNullControlCSV(t *testing.T, csvPath string) {
	t.Helper()
	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("null_control_multishape.csv not found at %s: %v (run with -update to regenerate)", csvPath, err)
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
		t.Fatalf("column %q not found in header", name)
		return -1
	}
	viIdx := colIdx("violations")
	oeIdx := colIdx("open_errors")
	pIdx := colIdx("panics")
	hIdx := colIdx("hangs")
	univIdx := colIdx("universe_size")

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
		univ, err := strconv.Atoi(rec[univIdx])
		if err != nil || univ <= 0 {
			t.Fatalf("row %d: invalid universe_size %q", rows, rec[univIdx])
		}
		for _, idx := range []int{viIdx, oeIdx, pIdx, hIdx} {
			v, err := strconv.Atoi(rec[idx])
			if err != nil || v != 0 {
				t.Fatalf("row %d: non-zero failure count %q=%s (expected 0 false alarms)", rows, header[idx], rec[idx])
			}
		}
	}
	if rows != 108 {
		t.Fatalf("expected 108 data rows in null_control_multishape.csv, got %d (run with -update to regenerate)", rows)
	}
	t.Logf("Validated %d rows with 0 false alarms in %s", rows, csvPath)
}

func runNullControlShapeSeed(t *testing.T, shape shapeSpec, seed uint64, gitCommit string) nullControlRecord {
	t.Helper()
	t0 := time.Now()
	wCfg := workload.Config{
		Seed:       seed,
		Operations: shape.ops,
		KeyCount:   shape.keyCount,
		ValueBytes: shape.valueBytes,
		SyncEvery:  shape.syncEvery,
		FlushEvery: shape.flushEvery,
		CompactAt:  shape.compactAt,
	}

	mem := vfs.NewCrashableMem()
	rec := vfsrec.New(mem)
	fCfg := faults.DefaultConfig(faults.MUT0)
	faultFS, err := faults.NewWithConfig(fCfg, rec)
	if err != nil {
		t.Fatalf("shape %d seed %d: init fault MUT-0 failed: %v", shape.id, seed, err)
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
	wCfg.FS = faultFS

	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatalf("shape %d seed %d: open pebble failed: %v", shape.id, seed, err)
	}

	rawHist, err := workload.Run(db, rec, wCfg)
	if err != nil {
		_, _ = safeCloseDB(db)
		t.Fatalf("shape %d seed %d: run workload failed: %v", shape.id, seed, err)
	}
	db.TestOnlyWaitForCleaning()
	if panicked, closeErr := safeCloseDB(db); panicked || closeErr != nil {
		t.Fatalf("shape %d seed %d: close pebble failed (panic=%v): %v", shape.id, seed, panicked, closeErr)
	}

	traceEvents := rec.Events()
	if err := trace.Validate(traceEvents); err != nil {
		t.Fatalf("shape %d seed %d: validate trace failed: %v", shape.id, seed, err)
	}

	correlated, err := workload.Correlate(rawHist, traceEvents)
	if err != nil {
		t.Fatalf("shape %d seed %d: correlate history failed: %v", shape.id, seed, err)
	}

	universe, err := candidate.Build(traceEvents, seed, []int{0, 100})
	if err != nil {
		t.Fatalf("shape %d seed %d: build universe failed: %v", shape.id, seed, err)
	}

	expRes, err := RunExplore(RunnerConfig{
		Universe: universe,
		Events:   traceEvents,
		History:  correlated,
		Dir:      "db",
		Timeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("shape %d seed %d: run explore failed: %v", shape.id, seed, err)
	}

	normOutcomes := NormalizeTable(expRes.Outcomes)
	if err := ValidateTable(universe, normOutcomes); err != nil {
		t.Fatalf("shape %d seed %d: ValidateTable failed: %v", shape.id, seed, err)
	}

	v := expRes.Summary.Violations
	oe := expRes.Summary.OpenErrors
	p := expRes.Summary.Panics
	h := expRes.Summary.Hangs
	orErr := expRes.Summary.OracleErrors

	if v > 0 || oe > 0 || p > 0 || h > 0 || orErr > 0 {
		t.Fatalf("shape %d seed %d: null-control failure: v=%d oe=%d p=%d h=%d oracle_err=%d (expected 0)",
			shape.id, seed, v, oe, p, h, orErr)
	}

	t.Logf("[shape=%2d seed=%3d] ops=%3d keys=%2d sync=%2d flush=%2d compact=%2d univ=%4d v=%d oe=%d p=%d h=%d (%v)",
		shape.id, seed, shape.ops, shape.keyCount, shape.syncEvery, shape.flushEvery, shape.compactAt,
		len(universe), v, oe, p, h, time.Since(t0).Round(time.Millisecond))

	return nullControlRecord{
		shapeID:      shape.id,
		seed:         seed,
		operations:   shape.ops,
		keyCount:     shape.keyCount,
		valueBytes:   shape.valueBytes,
		syncEvery:    shape.syncEvery,
		flushEvery:   shape.flushEvery,
		compactAt:    shape.compactAt,
		universeSize: len(universe),
		violations:   v,
		openErrors:   oe,
		panics:       p,
		hangs:        h,
		gitCommit:    gitCommit,
	}
}

func TestNullControlMultiShape(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-shape null control benchmark in short mode")
	}

	resultsDir := findResultsDirInternal(t)
	csvPath := filepath.Join(resultsDir, "null_control_multishape.csv")

	if !*updateArtifacts {
		validateNullControlCSV(t, csvPath)
		gitCommit := CurrentGitCommit()
		liveShapes := []shapeSpec{
			{id: 1, ops: 40, keyCount: 8, valueBytes: 32, syncEvery: 4, flushEvery: 20, compactAt: 0},
			{id: 7, ops: 50, keyCount: 16, valueBytes: 64, syncEvery: 8, flushEvery: 20, compactAt: 25},
		}
		for _, sh := range liveShapes {
			runNullControlShapeSeed(t, sh, 42, gitCommit)
		}
		return
	}

	// Generation mode: run the full suite and write the CSV atomically.
	runNullControlSuite(t, csvPath)
}

func runNullControlSuite(t *testing.T, csvPath string) {
	t.Helper()

	shapes := []shapeSpec{
		{id: 1, ops: 80, keyCount: 8, valueBytes: 32, syncEvery: 4, flushEvery: 25, compactAt: 0},
		{id: 2, ops: 80, keyCount: 16, valueBytes: 64, syncEvery: 8, flushEvery: 40, compactAt: 0},
		{id: 3, ops: 120, keyCount: 8, valueBytes: 128, syncEvery: 4, flushEvery: 25, compactAt: 0},
		{id: 4, ops: 120, keyCount: 32, valueBytes: 64, syncEvery: 8, flushEvery: 40, compactAt: 0},
		{id: 5, ops: 120, keyCount: 16, valueBytes: 256, syncEvery: 12, flushEvery: 60, compactAt: 0},
		{id: 6, ops: 200, keyCount: 8, valueBytes: 32, syncEvery: 4, flushEvery: 40, compactAt: 0},
		{id: 7, ops: 200, keyCount: 64, valueBytes: 64, syncEvery: 8, flushEvery: 40, compactAt: 30},
		{id: 8, ops: 200, keyCount: 16, valueBytes: 128, syncEvery: 12, flushEvery: 60, compactAt: 0},
		{id: 9, ops: 300, keyCount: 8, valueBytes: 32, syncEvery: 4, flushEvery: 25, compactAt: 0},
		{id: 10, ops: 300, keyCount: 32, valueBytes: 64, syncEvery: 8, flushEvery: 40, compactAt: 0},
		{id: 11, ops: 300, keyCount: 16, valueBytes: 128, syncEvery: 12, flushEvery: 60, compactAt: 0},
		{id: 12, ops: 300, keyCount: 64, valueBytes: 256, syncEvery: 12, flushEvery: 60, compactAt: 60},
	}

	seeds := []uint64{42, 100, 200, 300, 400, 500, 600, 700, 800}

	gitCommit := CurrentGitCommit()
	var records []nullControlRecord
	for _, shape := range shapes {
		for _, seed := range seeds {
			records = append(records, runNullControlShapeSeed(t, shape, seed, gitCommit))
		}
	}

	if err := writeCSVAtomic(csvPath, func(w *csv.Writer) error {
		if err := w.Write([]string{
			"shape_id", "seed", "operations", "key_count", "value_bytes",
			"sync_every", "flush_every", "compact_at", "universe_size",
			"violations", "open_errors", "panics", "hangs", "git_commit",
		}); err != nil {
			return err
		}
		for _, r := range records {
			row := []string{
				strconv.Itoa(r.shapeID),
				strconv.FormatUint(r.seed, 10),
				strconv.Itoa(r.operations),
				strconv.Itoa(r.keyCount),
				strconv.Itoa(r.valueBytes),
				strconv.Itoa(r.syncEvery),
				strconv.Itoa(r.flushEvery),
				strconv.Itoa(r.compactAt),
				strconv.Itoa(r.universeSize),
				strconv.Itoa(r.violations),
				strconv.Itoa(r.openErrors),
				strconv.Itoa(r.panics),
				strconv.Itoa(r.hangs),
				r.gitCommit,
			}
			if err := w.Write(row); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write %s failed: %v", csvPath, err)
	}
	t.Logf("Successfully verified %d runs with 0 false alarms and wrote %s", len(records), csvPath)
}
