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
	events        []trace.Event
	memFS         *vfs.MemFS
	handles       map[uint64]vfs.File
	locks         map[uint64]io.Closer
	currentCut    uint64
	err           error
	handleFiles   map[uint64]*fileSnapshot
	handleDirs    map[uint64]*dirSnapshot
	handleCursors map[uint64]int64
	filePaths     map[string]*fileSnapshot
	dirPaths      map[string]*dirSnapshot
}

type fileSnapshot struct {
	bytes     []byte
	dirtyFrom int64
	dirty     bool
}

func (snap *fileSnapshot) markDirty(offset int64) {
	if !snap.dirty || offset < snap.dirtyFrom {
		snap.dirtyFrom = offset
		snap.dirty = true
	}
}

type dirSnapshot struct {
	syncedEntries map[string]bool
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
		events:        trace.Clone(events),
		memFS:         vfs.NewCrashableMem(),
		handles:       make(map[uint64]vfs.File),
		locks:         make(map[uint64]io.Closer),
		handleFiles:   make(map[uint64]*fileSnapshot),
		handleDirs:    make(map[uint64]*dirSnapshot),
		handleCursors: make(map[uint64]int64),
		filePaths:     make(map[string]*fileSnapshot),
		dirPaths:      make(map[string]*dirSnapshot),
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
	if s.err != nil {
		return fmt.Errorf("replay: cannot advance after prior error: %w", s.err)
	}
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
			s.err = err
			return err
		}
	}
	s.currentCut = cut
	return nil
}

// Clone clones the filesystem using CrashClone with the given unsynced retention
// percentage (0..100) and RNG seed. Outstanding handles remain open during clone.
// For percent == 0 and percent == 100, it delegates directly to MemFS.CrashClone.
// For 0 < percent < 100, it executes deterministicPartialCloneWithLinks to guarantee 100%
// deterministic, seed-reproducible crash sampling regardless of map iteration order.
func (s *State) Clone(percent int, seed uint64) (vfs.FS, error) {
	if s.err != nil {
		return nil, fmt.Errorf("replay: cannot clone after prior error: %w", s.err)
	}
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
	return deterministicPartialCloneWithLinks(base0, s.memFS, percent, rng, s.syncedBytesForPath, s.linkGroups(), s.syncedDirEntries())
}

// deterministicPartialCloneWithLinks constructs a 100% deterministic, seed-reproducible
// partial-retention crash image from base0 (exact synced state) and full100 (exact unsynced state).
// Traversal and RNG consumption are executed in strictly sorted lexicographical order.
func deterministicPartialCloneWithLinks(
	base0, full100 vfs.FS,
	percent int,
	rng *rand.Rand,
	syncedLookup func(string) []byte,
	linkGroup map[string]string,
	syncedDirs map[string]map[string]bool,
) (vfs.FS, error) {
	detFS := vfs.NewCrashableMem()
	dirsToSync := []string{""}
	createdByGroup := make(map[string]string)

	var traverse func(dir string, inBase0, inFull100 bool) error
	traverse = func(dir string, inBase0, inFull100 bool) error {
		syncedSet := make(map[string]bool)
		if inBase0 {
			entries, err := base0.List(dir)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			} else {
				for _, e := range entries {
					syncedSet[e] = true
				}
			}
		}

		currSet := make(map[string]bool)
		if inFull100 {
			entries, err := full100.List(dir)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			} else {
				for _, e := range entries {
					currSet[e] = true
				}
			}
		}

		if !inBase0 && inFull100 && syncedDirs != nil {
			if sd := syncedDirs[dir]; sd != nil {
				for e := range sd {
					if currSet[e] {
						syncedSet[e] = true
					}
				}
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

			var fiBase, fiFull vfs.FileInfo
			if inBase0 && syncedSet[name] {
				var err error
				fiBase, err = base0.Stat(childPath)
				if err != nil {
					return err
				}
			}
			if inFull100 && currSet[name] {
				var err error
				fiFull, err = full100.Stat(childPath)
				if err != nil {
					return err
				}
			}

			// Retention rule for directory entry childPath:
			// - If syncedSet[name] is true: durably synced entry (matching MemFS.CrashClone's syncedChildren model),
			//   retain unconditionally without consuming RNG so Clone(0) <= Clone(pct) <= Clone(100) holds.
			// - If syncedSet[name] is false (unsynced creation/link): keep with probability percent%.
			if !syncedSet[name] {
				if rng.IntN(100) >= percent {
					continue
				}
			}

			var isDir, useBase, useFull bool
			switch {
			case fiBase != nil && fiFull != nil:
				if fiBase.IsDir() == fiFull.IsDir() {
					isDir = fiFull.IsDir()
					useBase = true
					useFull = true
				} else if rng.IntN(100) < percent {
					isDir = fiFull.IsDir()
					useBase = false
					useFull = true
				} else {
					isDir = fiBase.IsDir()
					useBase = true
					useFull = false
				}
			case fiFull != nil:
				isDir = fiFull.IsDir()
				useBase = false
				useFull = true
			case fiBase != nil:
				isDir = fiBase.IsDir()
				useBase = true
				useFull = false
			default:
				return fmt.Errorf("replay: missing stat for %q during partial clone", childPath)
			}

			if isDir {
				if childPath != "" {
					if err := detFS.MkdirAll(childPath, 0755); err != nil {
						return err
					}
					dirsToSync = append(dirsToSync, childPath)
				}
				if err := traverse(childPath, useBase, useFull); err != nil {
					return err
				}
			} else {
				if grp, ok := linkGroup[childPath]; ok {
					if existing, created := createdByGroup[grp]; created {
						if err := detFS.Link(existing, childPath); err != nil {
							return err
						}
						continue
					}
				}

				var syncedBytes []byte
				if useBase {
					b, err := readAllVFSFile(base0, childPath)
					if err != nil {
						return err
					}
					syncedBytes = b
				} else if syncedLookup != nil {
					syncedBytes = syncedLookup(childPath)
				}

				currBytes := syncedBytes
				if useFull {
					b, err := readAllVFSFile(full100, childPath)
					if err != nil {
						return err
					}
					currBytes = b
				}

				out := bytes.Clone(syncedBytes)
				// If the current file is shorter than the synced snapshot (e.g. rewrite
				// to a shorter length), truncation and rewritten blocks retain atomically
				// with probability percent%; otherwise keep the synced snapshot intact.
				if useFull && len(currBytes) < len(syncedBytes) {
					if rng.IntN(100) < percent {
						out = bytes.Clone(currBytes)
					}
				} else if useFull && !bytes.Equal(syncedBytes, currBytes) {
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
									if cap(out) < len(currBytes) {
										grown := make([]byte, end, len(currBytes))
										copy(grown, out)
										out = grown
									} else {
										out = out[:end]
									}
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
				if grp, ok := linkGroup[childPath]; ok {
					createdByGroup[grp] = childPath
				}
			}
		}
		return nil
	}

	if err := traverse("", true, true); err != nil {
		return nil, err
	}

	// Sync all created directories in detFS (including "") so the constructed crash image is quiescent and ready for pebble.Open.
	sort.Strings(dirsToSync)
	for i, d := range dirsToSync {
		if i > 0 && d == dirsToSync[i-1] {
			continue
		}
		df, err := detFS.OpenDir(d)
		if err != nil {
			return nil, err
		}
		if err := errors.Join(df.Sync(), df.Close()); err != nil {
			return nil, err
		}
	}

	return detFS, nil
}

func readAllVFSFile(fs vfs.FS, path string) ([]byte, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(f)
	closeErr := f.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	return data, nil
}

// Close closes all outstanding file and lock handles. Safe to call multiple times.
func (s *State) Close() error {
	var errs []error

	for id, f := range s.handles {
		err := f.Close()
		delete(s.handles, id)
		delete(s.handleFiles, id)
		delete(s.handleDirs, id)
		delete(s.handleCursors, id)
		if err != nil {
			errs = append(errs, fmt.Errorf("close handle %d: %w", id, err))
		}
	}

	for id, closer := range s.locks {
		err := closer.Close()
		delete(s.locks, id)
		if err != nil {
			errs = append(errs, fmt.Errorf("unlock handle %d: %w", id, err))
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
			s.handleCursors[event.Handle] = 0
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
			s.handleCursors[event.Handle] = 0
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
			s.handleCursors[event.Handle] = 0
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
			snap := s.dirPaths[event.File]
			if snap == nil {
				snap = &dirSnapshot{}
				s.dirPaths[event.File] = snap
			}
			s.handleDirs[event.Handle] = snap
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
			s.handleCursors[event.Handle] = 0
			s.renamePaths(event.File, event.File2)
			s.trackFile(event.Handle, event.File2, false)
		} else {
			s.pruneMissingPaths(event.File)
			s.pruneMissingPaths(event.File2)
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
			delete(s.dirPaths, event.File)
		}
		return nil

	case trace.KindRemoveAll:
		err := s.memFS.RemoveAll(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		s.pruneMissingPaths(event.File)
		return nil

	case trace.KindRename:
		err := s.memFS.Rename(event.File, event.File2)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s -> %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.File2, event.Err, err)
		}
		if err == nil {
			s.renamePaths(event.File, event.File2)
		} else {
			s.pruneMissingPaths(event.File)
			s.pruneMissingPaths(event.File2)
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
			s.filePaths[event.File] = &fileSnapshot{dirty: true}
			delete(s.dirPaths, event.File)
		}
		return nil

	case trace.KindWrite:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		return s.applyWrite(event, f.Write)

	case trace.KindWriteAt:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		return s.applyWrite(event, func(data []byte) (int, error) {
			return f.WriteAt(data, event.Off)
		})

	case trace.KindPreallocate:
		if event.Off < 0 || event.Size < 0 {
			return fmt.Errorf("replay: corrupted preallocate event %d: off=%d size=%d", event.ID, event.Off, event.Size)
		}
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
		if event.Off < 0 || event.Size < 0 || event.Len < 0 || int64(event.Len) > event.Size {
			return fmt.Errorf("replay: corrupted read event %d: off=%d len=%d size=%d", event.ID, event.Off, event.Len, event.Size)
		}
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		buf := make([]byte, event.Size)
		n, err := f.Read(buf)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on read event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if event.Err == "" && n != event.Len {
			return fmt.Errorf("replay: read len mismatch on event %d: got %d, want %d", event.ID, n, event.Len)
		}
		if n > 0 {
			s.handleCursors[event.Handle] += int64(n)
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
			if dirSnap := s.handleDirs[event.Handle]; dirSnap != nil {
				return s.snapshotSyncedDir(dirSnap)
			}
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
		if event.Size < 0 {
			return fmt.Errorf("replay: corrupted syncTo event %d: size=%d", event.ID, event.Size)
		}
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
		delete(s.handleDirs, event.Handle)
		delete(s.handleCursors, event.Handle)
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

func (s *State) applyWrite(event trace.Event, writeFn func([]byte) (int, error)) error {
	if event.Off < 0 || event.Len < 0 || event.Len > len(event.Data) || (event.Err == "" && event.Len != len(event.Data)) {
		return fmt.Errorf("replay: corrupted %s event %d: off=%d len=%d dataLen=%d",
			event.Kind, event.ID, event.Off, event.Len, len(event.Data))
	}
	snap := s.handleFiles[event.Handle]
	writeOff := event.Off
	if event.Kind == trace.KindWrite {
		writeOff = s.handleCursors[event.Handle]
	}
	data := bytes.Clone(event.Data[:event.Len])
	n, err := writeFn(data)
	if event.Err == "" {
		if err != nil {
			return fmt.Errorf("replay: infrastructure error: mismatch on %s event %d: trace err=%q, replay err=%v",
				event.Kind, event.ID, event.Err, err)
		}
		if n != event.Len {
			return fmt.Errorf("replay: short %s on event %d: got %d, want %d", event.Kind, event.ID, n, event.Len)
		}
	} else if n != event.Len {
		return fmt.Errorf("replay: partial %s len mismatch on event %d: got %d, want %d (replay err=%v)",
			event.Kind, event.ID, n, event.Len, err)
	}
	if event.Kind == trace.KindWrite && n > 0 {
		s.handleCursors[event.Handle] += int64(n)
	}
	if snap != nil && n > 0 {
		snap.markDirty(writeOff)
	}
	return nil
}

func (s *State) trackFile(handle uint64, name string, replace bool) {
	snapshot := s.filePaths[name]
	if replace || snapshot == nil {
		snapshot = &fileSnapshot{dirty: true}
		s.filePaths[name] = snapshot
	}
	if replace {
		deletePathPrefix(s.dirPaths, name)
	}
	s.handleFiles[handle] = snapshot
}

func deletePathPrefix[T any](m map[string]T, prefix string) {
	for name := range m {
		if name == prefix || strings.HasPrefix(name, prefix+"/") {
			delete(m, name)
		}
	}
}

func renameMapEntries[T any](m map[string]T, source, destination string) {
	if source == destination {
		return
	}
	moved := make(map[string]T)
	for name, val := range m {
		if name == source || strings.HasPrefix(name, source+"/") {
			moved[destination+strings.TrimPrefix(name, source)] = val
			delete(m, name)
		}
	}
	deletePathPrefix(m, destination)
	for name, val := range moved {
		m[name] = val
	}
}

func (s *State) renamePaths(source, destination string) {
	renameMapEntries(s.filePaths, source, destination)
	renameMapEntries(s.dirPaths, source, destination)
}

func (s *State) pruneMissingPaths(prefix string) {
	for name := range s.filePaths {
		if prefix == "" || name == prefix || strings.HasPrefix(name, prefix+"/") {
			if _, statErr := s.memFS.Stat(name); statErr != nil {
				delete(s.filePaths, name)
			}
		}
	}
	for name := range s.dirPaths {
		if prefix == "" || name == prefix || strings.HasPrefix(name, prefix+"/") {
			if _, statErr := s.memFS.Stat(name); statErr != nil {
				delete(s.dirPaths, name)
			}
		}
	}
}

func (s *State) linkGroups() map[string]string {
	bySnap := make(map[*fileSnapshot][]string)
	for name, snap := range s.filePaths {
		if snap != nil {
			bySnap[snap] = append(bySnap[snap], name)
		}
	}
	var groups map[string]string
	for _, names := range bySnap {
		if len(names) < 2 {
			continue
		}
		sort.Strings(names)
		if groups == nil {
			groups = make(map[string]string)
		}
		canonicalName := names[0]
		for _, n := range names {
			groups[n] = canonicalName
		}
	}
	return groups
}

func (s *State) syncedBytesForPath(name string) []byte {
	if snapshot := s.filePaths[name]; snapshot != nil {
		return snapshot.bytes
	}
	return nil
}

func (s *State) syncedDirEntries() map[string]map[string]bool {
	var out map[string]map[string]bool
	for dir, snap := range s.dirPaths {
		if snap != nil && snap.syncedEntries != nil {
			if out == nil {
				out = make(map[string]map[string]bool)
			}
			out[dir] = snap.syncedEntries
		}
	}
	return out
}

func (s *State) snapshotSyncedDir(snap *dirSnapshot) error {
	if snap == nil {
		return nil
	}
	var dirName string
	var found bool
	for name, current := range s.dirPaths {
		if current == snap {
			dirName = name
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	entries, err := s.memFS.List(dirName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("replay: list synced dir %q: %w", dirName, err)
	}
	synced := make(map[string]bool, len(entries))
	for _, e := range entries {
		synced[e] = true
	}
	snap.syncedEntries = synced
	return nil
}

func readFileRange(r io.ReaderAt, dst []byte, offset int64) error {
	pos := 0
	for pos < len(dst) {
		n, err := r.ReadAt(dst[pos:], offset+int64(pos))
		pos += n
		if err != nil {
			if errors.Is(err, io.EOF) && pos == len(dst) {
				return nil
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func readIntoSnapshot(snapshot *fileSnapshot, size int64, r io.ReaderAt) error {
	if size < 0 {
		return fmt.Errorf("negative size %d", size)
	}
	if size == 0 {
		snapshot.bytes = []byte{}
		snapshot.dirty = false
		snapshot.dirtyFrom = 0
		return nil
	}
	keepPrefix := int64(len(snapshot.bytes))
	if snapshot.dirtyFrom < keepPrefix {
		keepPrefix = snapshot.dirtyFrom
	}
	if keepPrefix < 0 {
		keepPrefix = 0
	}
	if keepPrefix > size {
		keepPrefix = size
	}
	buf := make([]byte, size)
	copy(buf[:keepPrefix], snapshot.bytes[:keepPrefix])
	if keepPrefix < size {
		if err := readFileRange(r, buf[keepPrefix:], keepPrefix); err != nil {
			return err
		}
	}
	snapshot.bytes = buf
	snapshot.dirty = false
	snapshot.dirtyFrom = 0
	return nil
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
	if snapshot == nil || !snapshot.dirty {
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
		// Read directly from the open handle (including zero-byte truncation).
		f, ok := s.handles[handle]
		if !ok {
			return nil
		}
		stat, err := f.Stat()
		if err != nil {
			return fmt.Errorf("replay: stat unlinked synced handle %d: %w", handle, err)
		}
		if err := readIntoSnapshot(snapshot, stat.Size(), f); err != nil {
			return fmt.Errorf("replay: snapshot unlinked synced handle %d: %w", handle, err)
		}
		return nil
	}
	sort.Strings(names)
	name := names[0]

	file, err := s.memFS.Open(name)
	if err != nil {
		return fmt.Errorf("replay: open synced file %q: %w", name, err)
	}
	stat, statErr := file.Stat()
	var readErr error
	if statErr == nil {
		readErr = readIntoSnapshot(snapshot, stat.Size(), file)
	}
	closeErr := file.Close()
	if err := errors.Join(statErr, readErr, closeErr); err != nil {
		return fmt.Errorf("replay: snapshot synced file %q: %w", name, err)
	}
	return nil
}
