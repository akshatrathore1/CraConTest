package vfsrec

import (
	"sync"
	"time"

	"github.com/akshatrathore1/CraConTest/internal/trace"
	"github.com/cockroachdb/pebble/v2"
)

func (fs *FS) recordEngineEvent(note string, job uint64, inputs, outputs []string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	event := trace.Event{
		Kind:    trace.KindEngineEvent,
		Note:    note,
		Job:     job,
		Inputs:  inputs,
		Outputs: outputs,
	}
	fs.appendEventLocked(event)
}

func pathSlice(name string) []string {
	if name == "" {
		return nil
	}
	c, err := canonical(name)
	if err != nil || c == "" {
		// Leave out a path the trace cannot name faithfully.
		return nil
	}
	return []string{c}
}

func jobID(job int) uint64 {
	if job > 0 {
		return uint64(job)
	}
	return 0
}

// EventListener returns a pebble.EventListener whose callbacks append EngineEvent
// annotations to the trace.
func (fs *FS) EventListener() pebble.EventListener {
	return pebble.EventListener{
		BackgroundError: func(err error) {
			fs.recordEngineEvent("BackgroundError", 0, nil, nil)
		},
		BlobFileCreated: func(info pebble.BlobFileCreateInfo) {
			fs.recordEngineEvent("BlobFileCreated", jobID(info.JobID), nil, pathSlice(info.Path))
		},
		BlobFileDeleted: func(info pebble.BlobFileDeleteInfo) {
			fs.recordEngineEvent("BlobFileDeleted", jobID(info.JobID), pathSlice(info.Path), nil)
		},
		BlobFileRewriteBegin: func(info pebble.BlobFileRewriteInfo) {
			fs.recordEngineEvent("BlobFileRewriteBegin", jobID(info.JobID), nil, nil)
		},
		BlobFileRewriteEnd: func(info pebble.BlobFileRewriteInfo) {
			fs.recordEngineEvent("BlobFileRewriteEnd", jobID(info.JobID), nil, nil)
		},
		DataCorruption: func(info pebble.DataCorruptionInfo) {
			fs.recordEngineEvent("DataCorruption", 0, nil, nil)
		},
		CompactionBegin: func(info pebble.CompactionInfo) {
			fs.recordEngineEvent("CompactionBegin", jobID(info.JobID), nil, nil)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			fs.recordEngineEvent("CompactionEnd", jobID(info.JobID), nil, nil)
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			fs.recordEngineEvent("DiskSlow", 0, nil, nil)
		},
		FlushBegin: func(info pebble.FlushInfo) {
			fs.mu.Lock()
			if fs.inIngestSection {
				if fs.ingestCond == nil {
					fs.ingestCond = sync.NewCond(&fs.mu)
				}
				var timedOut bool
				done := make(chan struct{})
				go func() {
					select {
					case <-done:
					case <-time.After(500 * time.Millisecond):
						fs.mu.Lock()
						timedOut = true
						if fs.ingestCond != nil {
							fs.ingestCond.Broadcast()
						}
						fs.mu.Unlock()
					}
				}()
				for fs.inIngestSection && !timedOut {
					fs.ingestCond.Wait()
				}
				close(done)
			}
			if fs.pendingManifestDeletions > 0 {
				if fs.manifestCond == nil {
					fs.manifestCond = sync.NewCond(&fs.mu)
				}
				var timedOut bool
				done := make(chan struct{})
				go func() {
					select {
					case <-done:
					case <-time.After(200 * time.Millisecond):
						fs.mu.Lock()
						timedOut = true
						if fs.manifestCond != nil {
							fs.manifestCond.Broadcast()
						}
						fs.mu.Unlock()
					}
				}()
				for fs.pendingManifestDeletions > 0 && !timedOut {
					fs.manifestCond.Wait()
				}
				close(done)
			}
			fs.mu.Unlock()
			fs.recordEngineEvent("FlushBegin", jobID(info.JobID), nil, nil)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			fs.recordEngineEvent("FlushEnd", jobID(info.JobID), nil, nil)
		},
		DownloadBegin: func(info pebble.DownloadInfo) {
			fs.recordEngineEvent("DownloadBegin", jobID(info.JobID), nil, nil)
		},
		DownloadEnd: func(info pebble.DownloadInfo) {
			fs.recordEngineEvent("DownloadEnd", jobID(info.JobID), nil, nil)
		},
		FormatUpgrade: func(version pebble.FormatMajorVersion) {
			fs.recordEngineEvent("FormatUpgrade", 0, nil, nil)
		},
		ManifestCreated: func(info pebble.ManifestCreateInfo) {
			fs.mu.Lock()
			if info.JobID > 0 {
				fs.pendingManifestDeletions++
			}
			fs.mu.Unlock()
			fs.recordEngineEvent("ManifestCreated", jobID(info.JobID), nil, pathSlice(info.Path))
		},
		ManifestDeleted: func(info pebble.ManifestDeleteInfo) {
			fs.mu.Lock()
			if fs.pendingManifestDeletions > 0 {
				fs.pendingManifestDeletions--
				if fs.manifestCond != nil {
					fs.manifestCond.Broadcast()
				}
			}
			fs.mu.Unlock()
			fs.recordEngineEvent("ManifestDeleted", jobID(info.JobID), pathSlice(info.Path), nil)
		},
		TableCreated: func(info pebble.TableCreateInfo) {
			fs.recordEngineEvent("TableCreated", jobID(info.JobID), nil, pathSlice(info.Path))
		},
		TableDeleted: func(info pebble.TableDeleteInfo) {
			fs.recordEngineEvent("TableDeleted", jobID(info.JobID), pathSlice(info.Path), nil)
		},
		TableIngested: func(info pebble.TableIngestInfo) {
			fs.recordEngineEvent("TableIngested", jobID(info.JobID), nil, nil)
		},
		TableStatsLoaded: func(info pebble.TableStatsInfo) {
			fs.recordEngineEvent("TableStatsLoaded", 0, nil, nil)
		},
		TableValidated: func(info pebble.TableValidatedInfo) {
			fs.recordEngineEvent("TableValidated", 0, nil, nil)
		},
		WALCreated: func(info pebble.WALCreateInfo) {
			fs.recordEngineEvent("WALCreated", jobID(info.JobID), nil, pathSlice(info.Path))
		},
		WALDeleted: func(info pebble.WALDeleteInfo) {
			fs.recordEngineEvent("WALDeleted", jobID(info.JobID), pathSlice(info.Path), nil)
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			fs.recordEngineEvent("WriteStallBegin", 0, nil, nil)
		},
		WriteStallEnd: func() {
			fs.recordEngineEvent("WriteStallEnd", 0, nil, nil)
		},
		LowDiskSpace: func(info pebble.LowDiskSpaceInfo) {
			fs.recordEngineEvent("LowDiskSpace", 0, nil, nil)
		},
		PossibleAPIMisuse: func(info pebble.PossibleAPIMisuseInfo) {
			fs.recordEngineEvent("PossibleAPIMisuse", 0, nil, nil)
		},
	}
}
