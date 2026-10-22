package exp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/candidate"
	"github.com/akshatrathore1/CraConTest/internal/history"
	"github.com/akshatrathore1/CraConTest/internal/oracle"
	"github.com/akshatrathore1/CraConTest/internal/recovery"
	"github.com/akshatrathore1/CraConTest/internal/replay"
	"github.com/akshatrathore1/CraConTest/internal/trace"
)

// JournalStage represents the execution lifecycle stage in the journal.
type JournalStage string

const (
	StageStarted  JournalStage = "started"
	StageFinished JournalStage = "finished"
)

// JournalRecord represents one append-only entry in the explore journal.
type JournalRecord struct {
	Stage     JournalStage        `json:"stage"`
	Candidate candidate.Candidate `json:"candidate"`
	Outcome   *oracle.Outcome     `json:"outcome,omitempty"`
	Timings   *StageTimings       `json:"timings,omitempty"`
	Time      time.Time           `json:"time"`
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
	Timeout     time.Duration // SafeOpen timeout, defaults to 5s
	JournalPath string        // optional file path for append-only execution journal
	Journal     io.Writer     // optional writer for journal records
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
	scanner := bufio.NewScanner(reader)
	finished := make(map[candidate.Candidate]oracle.Outcome)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var record JournalRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("read journal: %w", err)
		}
		if record.Stage == StageFinished && record.Outcome != nil {
			finished[record.Candidate] = *record.Outcome
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read journal scan: %w", err)
	}
	return finished, nil
}

type journalWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (jw *journalWriter) writeRecord(record JournalRecord) error {
	jw.mu.Lock()
	defer jw.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = jw.w.Write(data)
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
func RunExplore(cfg RunnerConfig) (*ExploreResult, error) {
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

	// Setup journal writer.
	var jw *journalWriter
	existingOutcomes := make(map[candidate.Candidate]oracle.Outcome)

	if cfg.JournalPath != "" {
		// Read existing journal if present.
		if f, err := os.Open(cfg.JournalPath); err == nil {
			if pastOutcomes, err := ReadJournal(f); err == nil {
				existingOutcomes = pastOutcomes
			}
			_ = f.Close()
		}

		jf, err := os.OpenFile(cfg.JournalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("runner open journal: %w", err)
		}
		defer jf.Close()
		jw = &journalWriter{w: jf}
	} else if cfg.Journal != nil {
		jw = &journalWriter{w: cfg.Journal}
	}

	// Canonical order required for forward replay.
	universe := append([]candidate.Candidate{}, cfg.Universe...)
	candidate.Sort(universe)

	replayer, err := replay.Build(cfg.Events, 0)
	if err != nil {
		return nil, fmt.Errorf("runner build replay: %w", err)
	}
	defer replayer.Close()

	var currentCut uint64
	outcomes := make([]oracle.Outcome, 0, len(universe))
	timingsMap := make(map[candidate.Candidate]StageTimings, len(universe))
	summary := RunSummary{TotalCandidates: len(universe)}
	totalStart := time.Now()

	for _, cand := range universe {
		// Check if already finished from existing journal.
		if pastOutcome, ok := existingOutcomes[cand]; ok {
			outcomes = append(outcomes, pastOutcome)
			updateSummary(&summary, pastOutcome.Class)
			continue
		}

		// Log started in journal.
		if jw != nil {
			_ = jw.writeRecord(JournalRecord{
				Stage:     StageStarted,
				Candidate: cand,
				Time:      time.Now(),
			})
		}

		candStart := time.Now()
		var stageTimings StageTimings

		// Stage 1: Replay advance.
		startReplay := time.Now()
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
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		// Stage 3: SafeOpen.
		startOpen := time.Now()
		openRes := recovery.SafeOpen(clonedFS, dir, timeout)
		stageTimings.Open = time.Since(startOpen)

		if openRes.Timeout {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassHang,
				Detail:    fmt.Sprintf("SafeOpen timeout after %v", timeout),
			}
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		if openRes.Panic != "" {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassPanic,
				Detail:    openRes.Panic,
			}
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		if openRes.Err != nil {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOpenError,
				Detail:    openRes.Err.Error(),
			}
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		if openRes.DB == nil {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOpenError,
				Detail:    "SafeOpen returned nil DB",
			}
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		// Stage 4: CheckIntegrity and DumpContents.
		startDump := time.Now()
		if err := recovery.CheckIntegrity(openRes.DB); err != nil {
			_ = openRes.DB.Close()
			stageTimings.Dump = time.Since(startDump)
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassViolation,
				Rule:      "structural",
				Signature: oracle.FormatSignature("structural", "unknown", nil),
				Detail:    err.Error(),
			}
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		dump, err := recovery.DumpContents(openRes.DB)
		_ = openRes.DB.Close()
		stageTimings.Dump = time.Since(startDump)
		if err != nil {
			terminalOutcome := oracle.Outcome{
				Candidate: cand,
				Class:     oracle.ClassOpenError,
				Detail:    fmt.Sprintf("DumpContents: %v", err),
			}
			finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
			continue
		}

		// Stage 5: Oracle consistency check.
		startOracle := time.Now()
		terminalOutcome := oracle.Check(oracle.Context{
			Candidate: cand,
			Events:    cfg.Events,
			History:   cfg.History,
			Recovered: dump,
		})
		stageTimings.Oracle = time.Since(startOracle)

		finishCandidate(jw, cand, &terminalOutcome, &stageTimings, candStart, &outcomes, timingsMap, &summary)
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
) {
	timings.Total = time.Since(candStart)
	outcome.Candidate = cand

	if jw != nil {
		_ = jw.writeRecord(JournalRecord{
			Stage:     StageFinished,
			Candidate: cand,
			Outcome:   outcome,
			Timings:   timings,
			Time:      time.Now(),
		})
	}

	*outcomes = append(*outcomes, *outcome)
	timingsMap[cand] = *timings
	updateSummary(summary, outcome.Class)
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
