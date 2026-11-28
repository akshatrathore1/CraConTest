package exp

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/graph"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// JournalStage represents the execution lifecycle stage in the journal.
type JournalStage string

const (
	StageStarted  JournalStage = "started"
	StageFinished JournalStage = "finished"
)

// JournalRecord represents one append-only entry in the explore journal.
type JournalRecord struct {
	Stage       JournalStage        `json:"stage"`
	Candidate   candidate.Candidate `json:"candidate"`
	Outcome     *oracle.Outcome     `json:"outcome,omitempty"`
	Timings     *StageTimings       `json:"timings,omitempty"`
	Time        time.Time           `json:"time"`
	ContextHash string              `json:"context_hash,omitempty"`
}

// StageTimings records the elapsed time spent across exploration pipeline stages.
type StageTimings struct {
	Replay time.Duration `json:"replay_ns"`
	Clone  time.Duration `json:"clone_ns"`
	Open   time.Duration `json:"open_ns"`
	Dump   time.Duration `json:"dump_ns"`
	Oracle time.Duration `json:"oracle_ns"`
	Total  time.Duration `json:"total_ns"`
}

// RunnerConfig provides configuration for executing the explore runner.
type RunnerConfig struct {
	Universe    []candidate.Candidate
	Events      []trace.Event
	History     []history.Entry
	Dir         string        // Pebble DB directory name, defaults to "db"
	Timeout     time.Duration // SafeOpen timeout, defaults to 5s (pebble.Open cannot be preempted mid-VFS call; late completions close cleanly)
	JournalPath string        // optional file path for append-only execution journal
	Journal     io.Writer     // optional writer for journal records
	// RetryInterrupted re-executes StageStarted-only candidates on journal resume
	// instead of recording ClassHang (useful after external SIGINT/OOM/timeout).
	RetryInterrupted bool
	// Categories gives, for every cutoff, the write categories of the files
	// and directory entries pending at that cutoff (graph.Features.Categories).
	// Violation signatures include them. When nil, they are computed from
	// Events with graph.Build.
	Categories map[uint64][]string
}

// RunSummary summarizes overall explore execution statistics.
type RunSummary struct {
	TotalCandidates int           `json:"total_candidates"`
	Valid           int           `json:"valid"`
	Violations      int           `json:"violations"`
	OpenErrors      int           `json:"open_errors"`
	Panics          int           `json:"panics"`
	Hangs           int           `json:"hangs"`
	OracleErrors    int           `json:"oracle_errors"`
	TotalTime       time.Duration `json:"total_time_ns"`
}

// ExploreResult holds the complete outcomes and timings produced by RunExplore.
type ExploreResult struct {
	Outcomes []oracle.Outcome                     `json:"outcomes"`
	Timings  map[candidate.Candidate]StageTimings `json:"timings"`
	Summary  RunSummary                           `json:"summary"`
}

// ReadJournal parses an append-only journal stream and returns the latest finished outcomes.
func ReadJournal(reader io.Reader) (map[candidate.Candidate]oracle.Outcome, error) {
	finished, _, _, err := readJournal(reader, "")
	return finished, err
}

func readJournal(reader io.Reader, expectedContext string) (map[candidate.Candidate]oracle.Outcome, map[candidate.Candidate]bool, map[candidate.Candidate]StageTimings, error) {
	br := bufio.NewReaderSize(reader, 64*1024)
	finished := make(map[candidate.Candidate]oracle.Outcome)
	started := make(map[candidate.Candidate]bool)
	timings := make(map[candidate.Candidate]StageTimings)
	seenRecord := false
	firstContext := ""

	for {
		line, readErr := br.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, nil, nil, fmt.Errorf("read journal scan: %w", readErr)
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			var record JournalRecord
			decoder := json.NewDecoder(bytes.NewReader(trimmed))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&record); err != nil {
				return nil, nil, nil, fmt.Errorf("read journal: %w", err)
			}
			var extra any
			if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
				return nil, nil, nil, errors.New("read journal: trailing data in record")
			}
			if record.Candidate.Percent < 0 || record.Candidate.Percent > 100 {
				return nil, nil, nil, errors.New("read journal: candidate percent outside 0..100")
			}
			if expectedContext != "" {
				if record.ContextHash != expectedContext {
					return nil, nil, nil, errors.New("read journal: execution context differs or legacy journal is unbound; use a fresh run directory")
				}
			} else if !seenRecord {
				seenRecord = true
				firstContext = record.ContextHash
			} else if record.ContextHash != firstContext {
				return nil, nil, nil, errors.New("read journal: mixed execution contexts in journal")
			}
			switch record.Stage {
			case StageStarted:
				if record.Outcome != nil {
					return nil, nil, nil, errors.New("read journal: started record contains an outcome")
				}
				if _, present := finished[record.Candidate]; present {
					return nil, nil, nil, errors.New("read journal: started record after finished outcome")
				}
				started[record.Candidate] = true
			case StageFinished:
				if record.Outcome == nil {
					return nil, nil, nil, errors.New("read journal: finished record missing outcome")
				}
				if record.Outcome.Candidate != record.Candidate {
					return nil, nil, nil, errors.New("read journal: finished outcome candidate differs from record candidate")
				}
				if record.Outcome.Class == oracle.ClassOracleError {
					if _, present := finished[record.Candidate]; present {
						return nil, nil, nil, errors.New("read journal: conflicting finished outcomes for one candidate")
					}
					// Transient oracle errors remain retriable rather than cached as finished outcomes.
					delete(started, record.Candidate)
					delete(timings, record.Candidate)
					continue
				}
				if err := ValidateTable([]candidate.Candidate{record.Candidate}, []oracle.Outcome{*record.Outcome}); err != nil {
					return nil, nil, nil, fmt.Errorf("read journal: %w", err)
				}
				if previous, present := finished[record.Candidate]; present && previous != *record.Outcome {
					return nil, nil, nil, errors.New("read journal: conflicting finished outcomes for one candidate")
				}
				finished[record.Candidate] = *record.Outcome
				delete(started, record.Candidate)
				if record.Timings != nil {
					timings[record.Candidate] = *record.Timings
				}
			default:
				return nil, nil, nil, fmt.Errorf("read journal: invalid stage %q", record.Stage)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}

	return finished, started, timings, nil
}

type journalWriter struct {
	mu          sync.Mutex
	w           io.Writer
	contextHash string
}

func (jw *journalWriter) writeRecord(record JournalRecord) error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	record.ContextHash = jw.contextHash
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	written, err := jw.w.Write(data)
	if err == nil && written != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// RunExplore executes the candidate universe through the forward-pass replay pipeline:
// - Sorts candidates in canonical order (by Event, Percent, Seed).
// - Advances replay forward incrementally with replay.Advance.
// - Clones with crash retention policy.
// - In-process SafeOpen with panic logger and timeout.
// - CheckIntegrity and DumpContents on successfully opened DBs.
// - Consistency evaluation via oracle.Check.
// - Appends started and finished entries to the journal.
// - Returns validated outcomes with per-stage timings.
// beforeSafeOpenFS is an optional test hook wrapping the cloned filesystem before SafeOpen.
var (
	beforeSafeOpenFSMu sync.RWMutex
	beforeSafeOpenFS   func(vfs.FS) vfs.FS
)

func wrapSafeOpenFS(fs vfs.FS) vfs.FS {
	beforeSafeOpenFSMu.RLock()
	fn := beforeSafeOpenFS
	beforeSafeOpenFSMu.RUnlock()
	if fn != nil {
		return fn(fs)
	}
	return fs
}

func safeCloseDB(db *pebble.DB) (panicked bool, err error) {
	if db == nil {
		return false, nil
	}
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			err = fmt.Errorf("%v", r)
		}
	}()
	return false, db.Close()
}

func RunExplore(cfg RunnerConfig) (*ExploreResult, error) {
	if cfg.JournalPath != "" && cfg.Journal != nil {
		return nil, errors.New("runner: cannot specify both JournalPath and Journal")
	}
	if err := candidate.Validate(cfg.Events, cfg.Universe); err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}
	if len(cfg.Universe) == 0 {
		return &ExploreResult{
			Outcomes: []oracle.Outcome{},
			Timings:  make(map[candidate.Candidate]StageTimings),
		}, nil
	}

	dir := cfg.Dir
	if dir == "" {
		dir = "db"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	if err := trace.Validate(cfg.Events); err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}
	if err := history.Validate(cfg.History); err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}
	categories := cfg.Categories
	if categories == nil {
		view, err := graph.Build(cfg.Events)
		if err != nil {
			return nil, fmt.Errorf("runner: categories for signatures: %w", err)
		}
		categories = make(map[uint64][]string, len(view.Cuts))
		for cut, features := range view.Cuts {
			categories[cut] = features.Categories
		}
	}

	// Setup journal writer.
	var jw *journalWriter
	var jf *os.File
	existingOutcomes := make(map[candidate.Candidate]oracle.Outcome)
	existingStarted := make(map[candidate.Candidate]bool)
	existingTimings := make(map[candidate.Candidate]StageTimings)
	contextHash := ""
	if cfg.JournalPath != "" || cfg.Journal != nil {
		contextData, err := json.Marshal(struct {
			Domain     string
			Events     []trace.Event
			History    []history.Entry
			Dir        string
			Timeout    time.Duration
			Categories map[uint64][]string
		}{"cracontest/journal-context/v1", cfg.Events, cfg.History, dir, timeout, categories})
		if err != nil {
			return nil, fmt.Errorf("runner journal context: %w", err)
		}
		contextHash = fmt.Sprintf("%x", sha256.Sum256(contextData))
	}

	if cfg.JournalPath != "" {
		// Read existing journal if present.
		f, err := os.Open(cfg.JournalPath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		} else {
			pastOutcomes, pastStarted, pastTimings, readErr := readJournal(f, contextHash)
			closeErr := f.Close()
			if readErr != nil {
				return nil, fmt.Errorf("runner read journal: %w", readErr)
			}
			if closeErr != nil {
				return nil, fmt.Errorf("runner close journal reader: %w", closeErr)
			}
			existingOutcomes = pastOutcomes
			existingStarted = pastStarted
			existingTimings = pastTimings
		}

		openedJF, err := os.OpenFile(cfg.JournalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, fmt.Errorf("runner open journal: %w", err)
		}
		jf = openedJF
		defer func() {
			if jf != nil {
				_ = jf.Close()
			}
		}()
		jw = &journalWriter{w: jf, contextHash: contextHash}
	} else if cfg.Journal != nil {
		jw = &journalWriter{w: cfg.Journal, contextHash: contextHash}
	}

	// Canonical order required for forward replay.
	universe := append([]candidate.Candidate{}, cfg.Universe...)
	candidate.Sort(universe)

	replayer, err := replay.Build(cfg.Events, 0)
	if err != nil {
		return nil, fmt.Errorf("runner build replay: %w", err)
	}
	replayerOpen := true
	defer func() {
		if replayerOpen {
			_ = replayer.Close()
		}
	}()

	var currentCut uint64
	outcomes := make([]oracle.Outcome, 0, len(universe))
	timingsMap := make(map[candidate.Candidate]StageTimings, len(universe))
	summary := RunSummary{TotalCandidates: len(universe)}
	totalStart := time.Now()

	for _, cand := range universe {
		// Check if already finished from existing journal (retry infrastructure oracle_error outcomes).
		if pastOutcome, ok := existingOutcomes[cand]; ok && pastOutcome.Class != oracle.ClassOracleError {
			outcomes = append(outcomes, pastOutcome)
			if pastTiming, ok := existingTimings[cand]; ok {
				timingsMap[cand] = pastTiming
			}
			updateSummary(&summary, pastOutcome.Class)
			continue
		}

		// Record started-but-unfinished journal candidates as a hang upon resume unless RetryInterrupted is set.
		if existingStarted[cand] && !cfg.RetryInterrupted {
			hangOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassHang,
				Detail:    "resumed after interrupted candidate (prior run crashed or hung)",
			}
			var zeroTimings StageTimings
			if err := finishCandidate(jw, cand, &hangOutcome, &zeroTimings, time.Now(), &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		// Log started in journal.
		if jw != nil {
			if err := jw.writeRecord(JournalRecord{
				Stage:     StageStarted,
				Candidate: cand,
				Time:      time.Now(),
			}); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
		}

		candStart := time.Now()
		var stageTimings StageTimings

		cutCategories, haveCategories := categories[cand.Event]
		if !haveCategories {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOracleError,
				Rule:      "missing_features",
				Detail:    fmt.Sprintf("no write categories for cutoff %d", cand.Event),
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		// Stage 1: Replay forward (universe is canonically sorted by Event).
		startReplay := time.Now()
		if cand.Event < currentCut {
			return nil, fmt.Errorf("runner: non-monotonic candidate cutoff %d < %d", cand.Event, currentCut)
		}
		if cand.Event > currentCut {
			if err := replayer.Advance(cand.Event); err != nil {
				return nil, fmt.Errorf("runner advance to %d: %w", cand.Event, err)
			}
			currentCut = cand.Event
		}
		stageTimings.Replay = time.Since(startReplay)

		// Stage 2: Clone.
		startClone := time.Now()
		clonedFS, err := replayer.Clone(cand.Percent, cand.Seed)
		stageTimings.Clone = time.Since(startClone)
		if err != nil {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOracleError,
				Rule:      "clone_failure",
				Detail:    err.Error(),
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		// Stage 3: SafeOpen.
		startOpen := time.Now()
		openFS := wrapSafeOpenFS(clonedFS)
		openRes := recovery.SafeOpen(openFS, dir, timeout)
		stageTimings.Open = time.Since(startOpen)

		if openRes.Timeout {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassHang,
				Detail:    fmt.Sprintf("SafeOpen timeout after %v", timeout),
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		if openRes.Panic != "" {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassPanic,
				Detail:    openRes.Panic,
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		if openRes.Err != nil {
			detail := openRes.Err.Error()
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOpenError,
				Signature: FormatOpenErrorSignature(detail),
				Detail:    detail,
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		if openRes.DB == nil {
			detail := "SafeOpen returned nil DB"
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOpenError,
				Signature: FormatOpenErrorSignature(detail),
				Detail:    detail,
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		// Stage 4: CheckIntegrity and DumpContents.
		startDump := time.Now()
		var integrityErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					integrityErr = fmt.Errorf("panic: %v", r)
				}
			}()
			integrityErr = recovery.CheckIntegrity(openRes.DB)
		}()
		if integrityErr != nil {
			_, _ = safeCloseDB(openRes.DB)
			stageTimings.Dump = time.Since(startDump)
			var panicErr *recovery.PanicError
			var terminalOutcome oracle.Outcome
			if errors.As(integrityErr, &panicErr) {
				terminalOutcome = oracle.Outcome{
					Candidate: cand,
					Class:     oracle.ClassPanic,
					Detail:    integrityErr.Error(),
				}
			} else {
				terminalOutcome = oracle.Outcome{
					Candidate: cand,
					Class:     oracle.ClassViolation,
					Rule:      "structural",
					Signature: oracle.FormatSignature("structural", "unknown", cutCategories),
					Detail:    integrityErr.Error(),
				}
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		var dump map[string]string
		var dumpErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					dumpErr = fmt.Errorf("panic: %v", r)
				}
			}()
			dump, dumpErr = recovery.DumpContents(openRes.DB)
		}()
		closePanicked, closeErr := safeCloseDB(openRes.DB)
		stageTimings.Dump = time.Since(startDump)
		if dumpErr != nil {
			var panicErr *recovery.PanicError
			var terminalOutcome oracle.Outcome
			if errors.As(dumpErr, &panicErr) {
				terminalOutcome = oracle.Outcome{
					Candidate: cand,
					Class:     oracle.ClassPanic,
					Detail:    fmt.Sprintf("DumpContents: %v", dumpErr),
				}
			} else {
				terminalOutcome = oracle.Outcome{
					Candidate: cand,
					Class:     oracle.ClassViolation,
					Rule:      "structural",
					Signature: oracle.FormatSignature("structural", "unknown", cutCategories),
					Detail:    fmt.Sprintf("DumpContents: %v", dumpErr),
				}
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}
		if closeErr != nil {
			var terminalOutcome oracle.Outcome
			if closePanicked {
				terminalOutcome = oracle.Outcome{
					Candidate: cand,
					Class:     oracle.ClassPanic,
					Detail:    fmt.Sprintf("DB close panic: %v", closeErr),
				}
			} else {
				terminalOutcome = oracle.Outcome{
					Candidate: cand,
					Class:     oracle.ClassViolation,
					Rule:      "structural",
					Signature: oracle.FormatSignature("structural", "unknown", cutCategories),
					Detail:    fmt.Sprintf("Close: %v", closeErr),
				}
			}
			if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
				return nil, fmt.Errorf("runner write journal: %w", err)
			}
			continue
		}

		// Stage 5: Oracle consistency check.
		startOracle := time.Now()
		terminalOutcome := oracle.Check(oracle.Context{
			Candidate:  cand,
			Events:     cfg.Events,
			History:    cfg.History,
			Recovered:  dump,
			Categories: cutCategories,
		})
		stageTimings.Oracle = time.Since(startOracle)

		// Check checkpoint directory if it exists and has a manifest pointer visible.
		ckptOpenDur, ckptDumpDur := inspectCheckpointRecovery(openFS, timeout, cutCategories, &terminalOutcome)
		stageTimings.Open += ckptOpenDur
		stageTimings.Dump += ckptDumpDur

		if err := finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary); err != nil {
			return nil, fmt.Errorf("runner write journal: %w", err)
		}
	}

	replayerOpen = false
	if err := replayer.Close(); err != nil {
		return nil, fmt.Errorf("runner close replay: %w", err)
	}
	if jf != nil {
		closeFile := jf
		jf = nil
		if err := closeFile.Close(); err != nil {
			return nil, fmt.Errorf("runner close journal: %w", err)
		}
	}

	summary.TotalTime = time.Since(totalStart)
	return &ExploreResult{
		Outcomes: outcomes,
		Timings:  timingsMap,
		Summary:  summary,
	}, nil
}

func finishCandidate(
	jw *journalWriter,
	cand candidate.Candidate,
	outcome *oracle.Outcome,
	timings *StageTimings,
	candStart time.Time,
	outcomes *[]oracle.Outcome,
	timingsMap map[candidate.Candidate]StageTimings,
	summary *RunSummary,
) error {
	timings.Total = time.Since(candStart)
	outcome.Candidate = cand

	if jw != nil {
		if err := jw.writeRecord(JournalRecord{
			Stage:     StageFinished,
			Candidate: cand,
			Outcome:   outcome,
			Timings:   timings,
			Time:      time.Now(),
		}); err != nil {
			return err
		}
	}

	*outcomes = append(*outcomes, *outcome)
	timingsMap[cand] = *timings
	updateSummary(summary, outcome.Class)
	return nil
}

func updateSummary(summary *RunSummary, class oracle.Class) {
	switch class {
	case oracle.ClassValid:
		summary.Valid++
	case oracle.ClassViolation:
		summary.Violations++
	case oracle.ClassOpenError:
		summary.OpenErrors++
	case oracle.ClassPanic:
		summary.Panics++
	case oracle.ClassHang:
		summary.Hangs++
	case oracle.ClassOracleError:
		summary.OracleErrors++
	}
}

// InspectCheckpointRecovery checks the secondary "checkpoint" database if present and updates terminalOutcome.
func InspectCheckpointRecovery(clonedFS vfs.FS, timeout time.Duration, cutCategories []string, terminalOutcome *oracle.Outcome) {
	_, _ = inspectCheckpointRecovery(clonedFS, timeout, cutCategories, terminalOutcome)
}

func inspectCheckpointRecovery(clonedFS vfs.FS, timeout time.Duration, cutCategories []string, terminalOutcome *oracle.Outcome) (openDur, dumpDur time.Duration) {
	if terminalOutcome == nil || terminalOutcome.Class != oracle.ClassValid {
		return 0, 0
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ckptEntries, ckptErr := clonedFS.List("checkpoint")
	if ckptErr != nil || len(ckptEntries) == 0 {
		return 0, 0
	}
	hasPointer := false
	for _, e := range ckptEntries {
		if strings.Contains(e, "marker.manifest") || e == "CURRENT" {
			hasPointer = true
			break
		}
	}
	if !hasPointer {
		return 0, 0
	}
	startOpen := time.Now()
	ckptRes := recovery.SafeOpen(clonedFS, "checkpoint", timeout)
	openDur = time.Since(startOpen)

	dbOpen := ckptRes.DB != nil
	defer func() {
		if dbOpen {
			_, _ = safeCloseDB(ckptRes.DB)
		}
	}()

	if ckptRes.Timeout {
		terminalOutcome.Class = oracle.ClassHang
		terminalOutcome.Detail = "checkpoint: open timeout"
		terminalOutcome.Rule = ""
		terminalOutcome.Signature = ""
	} else if ckptRes.Panic != "" {
		terminalOutcome.Class = oracle.ClassPanic
		terminalOutcome.Detail = "checkpoint: " + ckptRes.Panic
		terminalOutcome.Rule = ""
		terminalOutcome.Signature = ""
	} else if ckptRes.Err != nil {
		terminalOutcome.Class = oracle.ClassOpenError
		terminalOutcome.Detail = "checkpoint: " + ckptRes.Err.Error()
		terminalOutcome.Rule = ""
		terminalOutcome.Signature = FormatOpenErrorSignature(terminalOutcome.Detail)
	} else if ckptRes.DB == nil {
		terminalOutcome.Class = oracle.ClassOpenError
		terminalOutcome.Detail = "checkpoint: SafeOpen returned nil DB"
		terminalOutcome.Rule = ""
		terminalOutcome.Signature = FormatOpenErrorSignature(terminalOutcome.Detail)
	} else {
		startDump := time.Now()
		var integrityErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					integrityErr = fmt.Errorf("%v", r)
				}
			}()
			integrityErr = recovery.CheckIntegrity(ckptRes.DB)
		}()
		if integrityErr != nil {
			terminalOutcome.Class = oracle.ClassViolation
			terminalOutcome.Rule = "structural"
			terminalOutcome.Detail = "checkpoint integrity: " + integrityErr.Error()
			terminalOutcome.Signature = oracle.FormatSignature("structural", "checkpoint", cutCategories)
		} else {
			var dumpErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						dumpErr = fmt.Errorf("%v", r)
					}
				}()
				_, dumpErr = recovery.DumpContents(ckptRes.DB)
			}()
			if dumpErr != nil {
				terminalOutcome.Class = oracle.ClassViolation
				terminalOutcome.Rule = "structural"
				terminalOutcome.Detail = "checkpoint content dump: " + dumpErr.Error()
				terminalOutcome.Signature = oracle.FormatSignature("structural", "checkpoint", cutCategories)
			} else {
				dbOpen = false
				closePanicked, closeErr := safeCloseDB(ckptRes.DB)
				if closeErr != nil {
					if closePanicked {
						terminalOutcome.Class = oracle.ClassPanic
						terminalOutcome.Detail = "checkpoint close panic: " + closeErr.Error()
						terminalOutcome.Rule = ""
						terminalOutcome.Signature = ""
					} else {
						terminalOutcome.Class = oracle.ClassViolation
						terminalOutcome.Rule = "structural"
						terminalOutcome.Detail = "checkpoint close: " + closeErr.Error()
						terminalOutcome.Signature = oracle.FormatSignature("structural", "checkpoint", cutCategories)
					}
				}
			}
		}
		dumpDur = time.Since(startDump)
	}
	return openDur, dumpDur
}
