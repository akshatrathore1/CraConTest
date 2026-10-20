package trace

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
)

const Version = 1

type Document struct {
	SchemaVersion int     `json:"schema_version"`
	Events        []Event `json:"events"`
}

func Clone(events []Event) []Event {
	result := make([]Event, len(events))
	copy(result, events)
	for index := range result {
		result[index].Data = bytes.Clone(events[index].Data)
		result[index].Inputs = append([]string(nil), events[index].Inputs...)
		result[index].Outputs = append([]string(nil), events[index].Outputs...)
	}
	return result
}

func (event Event) IsAnnotation() bool {
	return event.Kind == KindOpBegin || event.Kind == KindOpEnd || event.Kind == KindEngineEvent
}

func (event Event) IsBarrier() bool {
	return event.Err == "" && (event.Kind == KindSync || event.Kind == KindSyncData ||
		(event.Kind == KindSyncTo && event.FullSync))
}

func (event Event) IsMutation() bool {
	switch event.Kind {
	case KindCreate, KindLink, KindLock, KindMkdirAll, KindOpenReadWrite,
		KindRemove, KindRemoveAll, KindRename, KindReuseForWrite:
		return true
	case KindWrite, KindWriteAt:
		return event.Len > 0
	default:
		return event.IsBarrier()
	}
}

func MutatingOperation(kind string) bool {
	return kind == "set" || kind == "delete" || kind == "range_delete" || kind == "batch"
}

func BeforeBoundary(events []Event, cut uint64) bool {
	if cut >= uint64(len(events)) {
		return false
	}
	next := events[cut]
	return next.IsBarrier() || (next.Kind == KindRename && next.Err == "")
}

func CutIDs(events []Event) ([]uint64, error) {
	if err := Validate(events); err != nil {
		return nil, err
	}
	selected := map[uint64]bool{0: true}
	for _, event := range events {
		if event.IsMutation() || (event.Kind == KindOpEnd && event.Err == "" && MutatingOperation(event.OpKind)) {
			selected[event.ID] = true
		}
		if event.IsBarrier() || (event.Kind == KindRename && event.Err == "") {
			selected[event.ID-1] = true
		}
	}
	result := make([]uint64, 0, len(selected))
	for cut := range selected {
		result = append(result, cut)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result, nil
}

func Encode(writer io.Writer, events []Event) error {
	if err := Validate(events); err != nil {
		return err
	}
	return json.NewEncoder(writer).Encode(Document{SchemaVersion: Version, Events: Clone(events)})
}

func Decode(reader io.Reader) ([]Event, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode trace: %w", err)
	}
	if document.SchemaVersion != Version || document.Events == nil {
		return nil, errors.New("unsupported trace version or missing events array")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after trace document")
	}
	if err := Validate(document.Events); err != nil {
		return nil, err
	}
	return document.Events, nil
}

type handleState struct {
	name      string
	directory bool
	lock      bool
	readable  bool
	writable  bool
	cursor    int64
}

func canonicalPath(name string) bool {
	return name == "" || (!path.IsAbs(name) && path.Clean(name) == name && name != "." &&
		name != ".." && !strings.HasPrefix(name, "../") && !strings.ContainsAny(name, "\\:"))
}

func Validate(events []Event) error {
	handles := make(map[uint64]handleState)
	var lastHandle, lastOperation, activeOperation uint64
	var activeKind string
	for index, event := range events {
		if event.ID != uint64(index+1) {
			return fmt.Errorf("event %d: IDs must be consecutive from one", index+1)
		}
		if !canonicalPath(event.File) || !canonicalPath(event.File2) {
			return fmt.Errorf("event %d: noncanonical VFS path", event.ID)
		}
		for _, name := range append(append([]string(nil), event.Inputs...), event.Outputs...) {
			if name == "" || !canonicalPath(name) {
				return fmt.Errorf("event %d: invalid engine file path", event.ID)
			}
		}
		write := event.Kind == KindWrite || event.Kind == KindWriteAt
		read := event.Kind == KindRead
		if event.Len < 0 || event.Off < 0 || event.Size < 0 ||
			(!write && len(event.Data) != 0) || (!write && !read && event.Len != 0) ||
			(!write && !read && event.Off != 0) ||
			(event.Kind != KindSyncTo && !read && event.Size != 0) ||
			(event.Kind != KindMkdirAll && event.Mode != 0) ||
			(event.Kind != KindSyncTo && event.FullSync) ||
			(event.Kind != KindCreate && event.Kind != KindOpenReadWrite && event.Kind != KindReuseForWrite && event.Cat != "") ||
			(event.Kind != KindOpenReadWrite && event.Created) {
			return fmt.Errorf("event %d: fields do not match operation", event.ID)
		}
		if write && (event.Len > len(event.Data) || (event.Err == "" && event.Len != len(event.Data))) {
			return fmt.Errorf("event %d: invalid write result", event.ID)
		}
		if read && int64(event.Len) > event.Size {
			return fmt.Errorf("event %d: read exceeds requested size", event.ID)
		}
		if event.Kind != KindLink && event.Kind != KindRename && event.Kind != KindReuseForWrite && event.File2 != "" {
			return fmt.Errorf("event %d: unexpected second path", event.ID)
		}
		if event.Kind != KindEngineEvent && (event.Job != 0 || len(event.Inputs) != 0 || len(event.Outputs) != 0 || event.Note != "") {
			return fmt.Errorf("event %d: unexpected engine annotation fields", event.ID)
		}
		if !event.IsAnnotation() && (event.Op != 0 || event.OpKind != "") {
			return fmt.Errorf("event %d: filesystem events cannot claim client attribution", event.ID)
		}
		switch event.Kind {
		case KindOpBegin, KindOpEnd, KindEngineEvent:
			if event.Handle != 0 || event.Dir || event.File != "" || event.Cat != "" {
				return fmt.Errorf("event %d: annotation contains handle fields", event.ID)
			}
			if event.Kind == KindEngineEvent {
				if event.Note == "" || event.Op != 0 || event.OpKind != "" {
					return fmt.Errorf("event %d: invalid engine annotation", event.ID)
				}
			} else if event.Op == 0 || (!MutatingOperation(event.OpKind) && event.OpKind != "get" &&
				event.OpKind != "iterate" && event.OpKind != "flush" && event.OpKind != "compact") {
				return fmt.Errorf("event %d: missing operation identity", event.ID)
			} else if event.Kind == KindOpBegin {
				if activeOperation != 0 || event.Op <= lastOperation || event.Err != "" {
					return fmt.Errorf("event %d: overlapping or repeated operation", event.ID)
				}
				activeOperation, activeKind = event.Op, event.OpKind
				lastOperation = event.Op
			} else {
				if activeOperation != event.Op || activeKind != event.OpKind {
					return fmt.Errorf("event %d: unmatched operation end", event.ID)
				}
				activeOperation, activeKind = 0, ""
			}
		case KindCreate, KindOpen, KindOpenReadWrite, KindOpenDir, KindReuseForWrite, KindLock:
			if event.Err != "" {
				if event.Handle != 0 || event.Created {
					return fmt.Errorf("event %d: failed open returned a handle", event.ID)
				}
				continue
			}
			if event.Handle != lastHandle+1 || (event.Kind == KindOpenDir && !event.Dir) ||
				((event.Kind == KindCreate || event.Kind == KindLock) && event.Dir) ||
				(!event.Dir && event.File == "") || (event.Kind == KindReuseForWrite && event.File2 == "") {
				return fmt.Errorf("event %d: invalid new handle", event.ID)
			}
			name := event.File
			if event.Kind == KindReuseForWrite {
				name = event.File2
			}
			handles[event.Handle] = handleState{name: name, directory: event.Dir, lock: event.Kind == KindLock,
				readable: event.Kind != KindReuseForWrite && event.Kind != KindLock && !event.Dir,
				writable: event.Kind == KindCreate || event.Kind == KindOpenReadWrite || event.Kind == KindReuseForWrite}
			lastHandle = event.Handle
		case KindRead, KindWrite, KindWriteAt, KindSync, KindSyncData, KindSyncTo, KindClose, KindUnlock:
			state, exists := handles[event.Handle]
			if !exists || event.File != state.name || event.Dir != state.directory ||
				(state.lock && event.Kind != KindUnlock) || (!state.lock && event.Kind == KindUnlock) {
				return fmt.Errorf("event %d: invalid handle reference", event.ID)
			}
			if write && (event.Err == "" || event.Len > 0) && (!state.writable || state.directory) {
				return fmt.Errorf("event %d: successful write to nonwritable handle", event.ID)
			}
			if read && (event.Err == "" || event.Len > 0) && !state.readable {
				return fmt.Errorf("event %d: successful read from nonreadable handle", event.ID)
			}
			if event.Kind == KindRead || event.Kind == KindWrite {
				if event.Off != state.cursor {
					return fmt.Errorf("event %d: sequential cursor mismatch", event.ID)
				}
				state.cursor += int64(event.Len)
				handles[event.Handle] = state
			}
			if event.Kind == KindClose || event.Kind == KindUnlock {
				delete(handles, event.Handle)
			}
		case KindLink, KindMkdirAll, KindRemove, KindRemoveAll, KindRename:
			if event.Handle != 0 || event.Dir {
				return fmt.Errorf("event %d: namespace event contains handle fields", event.ID)
			}
			if event.Err == "" && event.Kind != KindMkdirAll && (event.File == "" ||
				((event.Kind == KindLink || event.Kind == KindRename) && event.File2 == "")) {
				return fmt.Errorf("event %d: missing namespace path", event.ID)
			}
		default:
			return fmt.Errorf("event %d: unsupported kind %q", event.ID, event.Kind)
		}
	}
	return nil
}
