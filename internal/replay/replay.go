// Package replay reconstructs filesystem crash states by executing recorded
// VFS traces against an in-memory CrashableMem filesystem.
package replay

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"strings"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// State represents an active replay state holding an in-memory crashable filesystem,
// open file/lock handles, and trace progress.
type State struct {
	events      []trace.Event
	memFS       *vfs.MemFS
	handles     map[uint64]vfs.File
	locks       map[uint64]io.Closer
	currentCut  uint64
	handleFiles map[uint64]*fileSnapshot
	filePaths   map[string]*fileSnapshot
}

type fileSnapshot struct {
	bytes []byte
}

// Replay validates and executes the given trace events into a State replayed to completion.
func Replay(events []trace.Event) (*State, error) {
	return Build(events, uint64(len(events)))
}

// Build validates the trace events and initializes a State replayed up to cut.
func Build(events []trace.Event, cut uint64) (*State, error) {
	if err := trace.Validate(events); err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}
	if cut > uint64(len(events)) {
		return nil, fmt.Errorf("replay: cutoff %d exceeds event count %d", cut, len(events))
	}

	s := &State{
		events:      trace.Clone(events),
		memFS:       vfs.NewCrashableMem(),
		handles:     make(map[uint64]vfs.File),
		locks:       make(map[uint64]io.Closer),
		handleFiles: make(map[uint64]*fileSnapshot),
		filePaths:   make(map[string]*fileSnapshot),
	}

	if cut > 0 {
		if err := s.Advance(cut); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

// Advance advances replay progress from currentCut up to cut (forward-only).
func (s *State) Advance(cut uint64) error {
	if cut < s.currentCut {
		return fmt.Errorf("replay: cannot advance backwards from %d to %d", s.currentCut, cut)
	}
	if cut > uint64(len(s.events)) {
		return fmt.Errorf("replay: cutoff %d exceeds event count %d", cut, len(s.events))
	}
	if cut == s.currentCut {
		return nil
	}

	for _, event := range s.events[s.currentCut:cut] {
		if err := s.applyEvent(event); err != nil {
			return err
		}
	}
	s.currentCut = cut
	return nil
}

// Clone clones the filesystem using CrashClone with the given unsynced retention
// percentage (0..100) and RNG seed. Outstanding handles remain open during clone.
// For percent == 0 and percent == 100, it delegates directly to MemFS.CrashClone.
// For 0 < percent < 100, it executes deterministicPartialClone to guarantee 100%
// deterministic, seed-reproducible crash sampling regardless of map iteration order.
func (s *State) Clone(percent int, seed uint64) (vfs.FS, error) {
	if percent < 0 || percent > 100 {
		return nil, fmt.Errorf("replay: retention percent %d outside 0..100", percent)
	}

	if percent == 0 {
		return s.memFS.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0}), nil
	}
	if percent == 100 {
		return s.memFS.CrashClone(vfs.CrashCloneCfg{
			UnsyncedDataPercent: 100,
			RNG:                 rand.New(rand.NewPCG(0, seed)),
		}), nil
	}

	rng := rand.New(rand.NewPCG(0, seed))
	base0 := s.memFS.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 0})
	return deterministicPartialClone(base0, s.memFS, percent, rng, s.syncedBytesByPath())
}

// deterministicPartialClone constructs a 100% deterministic, seed-reproducible partial-retention
// crash image from base0 (exact synced state) and full100 (exact unsynced state).
// Traversal and RNG consumption are executed in strictly sorted lexicographical order.
func deterministicPartialClone(
	base0, full100 vfs.FS,
	percent int,
	rng *rand.Rand,
	fileSyncedBytes map[string][]byte,
) (vfs.FS, error) {
	detFS := vfs.NewCrashableMem()
	dirsToSync := []string{""}

	var traverse func(dir string) error
	traverse = func(dir string) error {
		syncedSet := make(map[string]bool)
		if entries, err := base0.List(dir); err == nil {
			for _, e := range entries {
				syncedSet[e] = true
			}
		}

		currSet := make(map[string]bool)
		if entries, err := full100.List(dir); err == nil {
			for _, e := range entries {
				currSet[e] = true
			}
		}

		nameMap := make(map[string]bool, len(syncedSet)+len(currSet))
		for e := range syncedSet {
			nameMap[e] = true
		}
		for e := range currSet {
			nameMap[e] = true
		}

		allNames := make([]string, 0, len(nameMap))
		for name := range nameMap {
			allNames = append(allNames, name)
		}
		sort.Strings(allNames)

		for _, name := range allNames {
			childPath := name
			if dir != "" {
				childPath = dir + "/" + name
			}

			var isDir bool
			if fi, err := full100.Stat(childPath); err == nil {
				isDir = fi.IsDir()
			} else if fi, err := base0.Stat(childPath); err == nil {
				isDir = fi.IsDir()
			}

			// Retention rule for directory entry childPath:
			// If syncedSet[name] is true: durably synced entry, retain unconditionally without consuming RNG.
			// If syncedSet[name] is false: unsynced entry present only in full100, consume one roll:
			// if rng.IntN(100) >= percent { continue } (skip entry and subtree).
			if !syncedSet[name] {
				if rng.IntN(100) >= percent {
					continue
				}
			}

			if isDir {
				if childPath != "" {
					if err := detFS.MkdirAll(childPath, 0755); err != nil {
						return err
					}
					dirsToSync = append(dirsToSync, childPath)
				}
				if err := traverse(childPath); err != nil {
					return err
				}
			} else {
				var syncedBytes []byte
				if syncedSet[name] {
					if f, err := base0.Open(childPath); err == nil {
						syncedBytes, _ = io.ReadAll(f)
						_ = f.Close()
					}
				} else if fileSyncedBytes != nil {
					syncedBytes = bytes.Clone(fileSyncedBytes[childPath])
				}

				currBytes := syncedBytes
				if currSet[name] {
					if f, err := full100.Open(childPath); err == nil {
						currBytes, _ = io.ReadAll(f)
						_ = f.Close()
					}
				}

				out := bytes.Clone(syncedBytes)
				// If the current file is shorter than the synced snapshot (e.g. rewrite
				// to a shorter length), truncation is an unsynced metadata change that
				// only persists if retained by the partial retention policy.
				if len(currBytes) < len(syncedBytes) {
					if rng.IntN(100) < percent {
						out = out[:len(currBytes)]
					}
				}
				if currSet[name] && !bytes.Equal(syncedBytes, currBytes) {
					for offset := 0; offset < len(currBytes); offset += 4096 {
						end := offset + 4096
						if end > len(currBytes) {
							end = len(currBytes)
						}
						differs := false
						if offset >= len(syncedBytes) || end > len(syncedBytes) {
							differs = true
						} else if !bytes.Equal(syncedBytes[offset:end], currBytes[offset:end]) {
							differs = true
						}
						if differs {
							if rng.IntN(100) < percent {
								if len(out) < end {
									out = append(out, make([]byte, end-len(out))...)
								}
								copy(out[offset:end], currBytes[offset:end])
							}
						}
					}
				}

				f, err := detFS.Create(childPath, vfs.WriteCategoryUnspecified)
				if err != nil {
					return err
				}
				if len(out) > 0 {
					if _, err := f.Write(out); err != nil {
						_ = f.Close()
						return err
					}
				}
				if err := f.Sync(); err != nil {
					_ = f.Close()
					return err
				}
				if err := f.Close(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := traverse(""); err != nil {
		return nil, err
	}

	// Sync all created directories in detFS (including "") so the constructed crash image is quiescent and ready for pebble.Open.
	sort.Strings(dirsToSync)
	for i, d := range dirsToSync {
		if i > 0 && d == dirsToSync[i-1] {
			continue
		}
		if df, err := detFS.OpenDir(d); err == nil {
			_ = df.Sync()
			_ = df.Close()
		}
	}

	return detFS, nil
}

// Close closes all outstanding file and lock handles. Safe to call multiple times.
func (s *State) Close() error {
	var errs []error

	for id, f := range s.handles {
		if err := f.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close handle %d: %w", id, err))
		} else {
			delete(s.handles, id)
			delete(s.handleFiles, id)
		}
	}

	for id, closer := range s.locks {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unlock handle %d: %w", id, err))
		} else {
			delete(s.locks, id)
		}
	}

	return errors.Join(errs...)
}

// CurrentCut returns the current cutoff position of the replayed state.
func (s *State) CurrentCut() uint64 {
	return s.currentCut
}

// FS returns the underlying memory filesystem for inspection or recovery tests.
func (s *State) FS() vfs.FS {
	return s.memFS
}

func (s *State) applyEvent(event trace.Event) error {
	switch event.Kind {
	case trace.KindOpBegin, trace.KindOpEnd, trace.KindEngineEvent:
		// Annotations carry no filesystem mutations.
		return nil

	case trace.KindCreate:
		f, err := s.memFS.Create(event.File, vfs.DiskWriteCategory(event.Cat))
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		if err == nil {
			s.handles[event.Handle] = f
			s.trackFile(event.Handle, event.File, true)
		}
		return nil

	case trace.KindOpen:
		f, err := s.memFS.Open(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		if err == nil {
			s.handles[event.Handle] = f
			if !event.Dir {
				s.trackFile(event.Handle, event.File, false)
			}
		}
		return nil

	case trace.KindOpenReadWrite:
		f, err := s.memFS.OpenReadWrite(event.File, vfs.DiskWriteCategory(event.Cat))
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		if err == nil {
			s.handles[event.Handle] = f
			if !event.Dir {
				s.trackFile(event.Handle, event.File, event.Created)
			}
		}
		return nil

	case trace.KindOpenDir:
		f, err := s.memFS.OpenDir(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		if err == nil {
			s.handles[event.Handle] = f
		}
		return nil

	case trace.KindReuseForWrite:
		f, err := s.memFS.ReuseForWrite(event.File, event.File2, vfs.DiskWriteCategory(event.Cat))
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s -> %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.File2, event.Err, err)
		}
		if err == nil {
			s.handles[event.Handle] = f
			s.renameFilePaths(event.File, event.File2)
			s.trackFile(event.Handle, event.File2, true)
		}
		return nil

	case trace.KindMkdirAll:
		err := s.memFS.MkdirAll(event.File, os.FileMode(event.Mode))
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		return nil

	case trace.KindRemove:
		err := s.memFS.Remove(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		if err == nil {
			delete(s.filePaths, event.File)
		}
		return nil

	case trace.KindRemoveAll:
		err := s.memFS.RemoveAll(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		for name := range s.filePaths {
			if name == event.File || strings.HasPrefix(name, event.File+"/") || event.File == "" {
				if _, statErr := s.memFS.Stat(name); statErr != nil {
					delete(s.filePaths, name)
				}
			}
		}
		return nil

	case trace.KindRename:
		err := s.memFS.Rename(event.File, event.File2)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s -> %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.File2, event.Err, err)
		}
		if err == nil {
			s.renameFilePaths(event.File, event.File2)
		} else if _, statErr := s.memFS.Stat(event.File2); statErr != nil {
			delete(s.filePaths, event.File2)
		}
		return nil

	case trace.KindLink:
		err := s.memFS.Link(event.File, event.File2)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s -> %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.File2, event.Err, err)
		}
		if err == nil {
			s.filePaths[event.File2] = s.filePaths[event.File]
		}
		return nil

	case trace.KindLock:
		closer, err := s.memFS.Lock(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		if err == nil {
			s.locks[event.Handle] = closer
		}
		return nil

	case trace.KindWrite:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		if event.Len < 0 || (event.Err == "" && event.Len > len(event.Data)) {
			return fmt.Errorf("replay: corrupted write event %d: len %d exceeds data length %d", event.ID, event.Len, len(event.Data))
		}
		writeLen := event.Len
		if writeLen > len(event.Data) {
			writeLen = len(event.Data)
		}
		data := bytes.Clone(event.Data[:writeLen])
		n, err := f.Write(data)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on write event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if event.Err == "" && n != event.Len {
			return fmt.Errorf("replay: short write on event %d: got %d, want %d", event.ID, n, event.Len)
		}
		return nil

	case trace.KindWriteAt:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		if event.Len < 0 || (event.Err == "" && event.Len > len(event.Data)) {
			return fmt.Errorf("replay: corrupted writeAt event %d: len %d exceeds data length %d", event.ID, event.Len, len(event.Data))
		}
		writeLen := event.Len
		if writeLen > len(event.Data) {
			writeLen = len(event.Data)
		}
		data := bytes.Clone(event.Data[:writeLen])
		n, err := f.WriteAt(data, event.Off)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on writeAt event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if event.Err == "" && n != event.Len {
			return fmt.Errorf("replay: short writeAt on event %d: got %d, want %d", event.ID, n, event.Len)
		}
		return nil

	case trace.KindPreallocate:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		err := f.Preallocate(event.Off, event.Size)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on preallocate event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		return nil

	case trace.KindRead:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		if event.Size > 0 {
			buf := make([]byte, event.Size)
			n, err := f.Read(buf)
			if (err != nil) != (event.Err != "") {
				return fmt.Errorf("replay: infrastructure error: mismatch on read event %d: trace err=%q, replay err=%v",
					event.ID, event.Err, err)
			}
			if event.Err == "" && n != event.Len {
				return fmt.Errorf("replay: read len mismatch on event %d: got %d, want %d", event.ID, n, event.Len)
			}
		}
		return nil

	case trace.KindSync:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		err := f.Sync()
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on sync event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if err == nil {
			return s.snapshotSyncedBytes(event.Handle)
		}
		return nil

	case trace.KindSyncData:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		err := f.SyncData()
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on syncData event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if err == nil {
			return s.snapshotSyncedBytes(event.Handle)
		}
		return nil

	case trace.KindSyncTo:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		fullSync, err := f.SyncTo(event.Size)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on syncTo event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if fullSync != event.FullSync {
			return fmt.Errorf("replay: syncTo fullSync mismatch on event %d: trace=%v, replay=%v",
				event.ID, event.FullSync, fullSync)
		}
		if err == nil {
			// Only snapshot when FullSync=true (the entire file is durable).
			// When FullSync=false, MemFS does not guarantee any bytes are
			// durable to a specific offset, so we do not update the snapshot.
			// This is consistent with ADR 2026-10-12-a which specifies that
			// SyncTo in MemFS returns (false, nil) and is not a persistence boundary.
			if event.FullSync {
				return s.snapshotSyncedBytes(event.Handle)
			}
		}
		return nil

	case trace.KindClose:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		err := f.Close()
		delete(s.handles, event.Handle)
		delete(s.handleFiles, event.Handle)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on close event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		return nil

	case trace.KindUnlock:
		closer, ok := s.locks[event.Handle]
		if !ok {
			return fmt.Errorf("replay: lock handle %d not open on event %d", event.Handle, event.ID)
		}
		err := closer.Close()
		delete(s.locks, event.Handle)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on unlock event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		return nil

	default:
		return fmt.Errorf("replay: unsupported event kind %q on event %d", event.Kind, event.ID)
	}
}

func (s *State) trackFile(handle uint64, name string, replace bool) {
	snapshot := s.filePaths[name]
	if replace || snapshot == nil {
		snapshot = &fileSnapshot{}
		s.filePaths[name] = snapshot
	}
	s.handleFiles[handle] = snapshot
}

func (s *State) renameFilePaths(source, destination string) {
	if source == destination {
		return
	}
	moved := make(map[string]*fileSnapshot)
	for name, snapshot := range s.filePaths {
		if name == source || strings.HasPrefix(name, source+"/") {
			moved[destination+strings.TrimPrefix(name, source)] = snapshot
			delete(s.filePaths, name)
		}
	}
	for name, snapshot := range moved {
		s.filePaths[name] = snapshot
	}
}

func (s *State) syncedBytesByPath() map[string][]byte {
	result := make(map[string][]byte, len(s.filePaths))
	for name, snapshot := range s.filePaths {
		if snapshot != nil {
			result[name] = bytes.Clone(snapshot.bytes)
		}
	}
	return result
}

// snapshotSyncedBytes snapshots durable bytes for the file associated with handle.
//
// Hard-link determinism: multiple filePaths entries can share the same *fileSnapshot
// (created by KindLink). To guarantee a reproducible pick among aliases, the
// lexicographically smallest path is chosen. If the file has been unlinked (removed
// from filePaths while the handle is still open), snapshot bytes are read directly
// from the open handle.
func (s *State) snapshotSyncedBytes(handle uint64) error {
	snapshot := s.handleFiles[handle]
	if snapshot == nil {
		return nil
	}

	// Collect all path aliases that share this snapshot, then sort for determinism.
	var names []string
	for name, current := range s.filePaths {
		if current == snapshot {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		// File was unlinked while the handle was still open (POSIX temp-file pattern).
		// Try reading directly from the open handle if available.
		if f, ok := s.handles[handle]; ok {
			if stat, err := f.Stat(); err == nil && stat.Size() > 0 {
				buf := make([]byte, stat.Size())
				if n, err := f.ReadAt(buf, 0); err == nil || errors.Is(err, io.EOF) {
					snapshot.bytes = bytes.Clone(buf[:n])
					return nil
				}
			}
		}
		return nil
	}
	sort.Strings(names)
	name := names[0]

	file, err := s.memFS.Open(name)
	if err != nil {
		return fmt.Errorf("replay: open synced file %q: %w", name, err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("replay: snapshot synced file %q: %w", name, err)
	}
	snapshot.bytes = data
	return nil
}
