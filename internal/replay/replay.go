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

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// State represents an active replay state holding an in-memory crashable filesystem,
// open file/lock handles, and trace progress.
type State struct {
	events     []trace.Event
	memFS      *vfs.MemFS
	handles    map[uint64]vfs.File
	locks      map[uint64]io.Closer
	currentCut uint64
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
		events:  trace.Clone(events),
		memFS:   vfs.NewCrashableMem(),
		handles: make(map[uint64]vfs.File),
		locks:   make(map[uint64]io.Closer),
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
// percentage (0 or 100) and RNG seed. Outstanding handles remain open during clone.
func (s *State) Clone(percent int, seed uint64) (vfs.FS, error) {
	if percent != 0 && percent != 100 {
		return nil, fmt.Errorf("replay: unsupported retention policy %d (only 0 and 100)", percent)
	}

	rng := rand.New(rand.NewPCG(0, seed))
	cloned := s.memFS.CrashClone(vfs.CrashCloneCfg{
		UnsyncedDataPercent: percent,
		RNG:                 rng,
	})
	return cloned, nil
}

// Close closes all outstanding file and lock handles. Safe to call multiple times.
func (s *State) Close() error {
	var errs []error

	for id, f := range s.handles {
		if err := f.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close handle %d: %w", id, err))
		}
		delete(s.handles, id)
	}

	for id, closer := range s.locks {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("unlock handle %d: %w", id, err))
		}
		delete(s.locks, id)
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
		return nil

	case trace.KindRemoveAll:
		err := s.memFS.RemoveAll(event.File)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.Err, err)
		}
		return nil

	case trace.KindRename:
		err := s.memFS.Rename(event.File, event.File2)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s -> %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.File2, event.Err, err)
		}
		return nil

	case trace.KindLink:
		err := s.memFS.Link(event.File, event.File2)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on event %d (%s %s -> %s): trace err=%q, replay err=%v",
				event.ID, event.Kind, event.File, event.File2, event.Err, err)
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
		data := bytes.Clone(event.Data[:event.Len])
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
		data := bytes.Clone(event.Data[:event.Len])
		n, err := f.WriteAt(data, event.Off)
		if (err != nil) != (event.Err != "") {
			return fmt.Errorf("replay: infrastructure error: mismatch on writeAt event %d: trace err=%q, replay err=%v",
				event.ID, event.Err, err)
		}
		if event.Err == "" && n != event.Len {
			return fmt.Errorf("replay: short writeAt on event %d: got %d, want %d", event.ID, n, event.Len)
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
		return nil

	case trace.KindClose:
		f, ok := s.handles[event.Handle]
		if !ok {
			return fmt.Errorf("replay: handle %d not open on event %d", event.Handle, event.ID)
		}
		err := f.Close()
		delete(s.handles, event.Handle)
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
