// Package faults implements seeded fault injection for filesystem operations.
//
// Decorator order is faults(vfsrec(memFS)). When a fault drops a sync, it returns
// nil without invoking the underlying recorder, ensuring that no barrier event
// appears in the trace.
package faults

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// Seeded fault IDs.
const (
	MUT0 = "MUT-0" // Null pass-through control
	MUT1 = "MUT-1" // Dense WAL sync drop
	MUT2 = "MUT-2" // Dense Manifest sync drop
	MUT3 = "MUT-3" // Dense Directory sync drop
	MUT4 = "MUT-4" // Dense SST flush sync drop
	MUT5 = "MUT-5" // Single k-th WAL sync drop
	MUT6 = "MUT-6" // Single manifest-rotation sync drop
	MUT7 = "MUT-7" // RocksDB #10573: dir sync omitted after CURRENT rename
	MUT8 = "MUT-8" // RocksDB #10460: fsyncs omitted after trigger event
	MUT9 = "MUT-9" // Single k-th directory sync drop

	// Descriptive aliases
	MutWalK       = "MUT-WAL-K"
	MutManRot     = "MUT-MAN-ROT"
	MutRocks10573 = "MUT-ROCKS-10573"
	MutRocks10460 = "MUT-ROCKS-10460"
	MutDirK       = "MUT-DIR-K"
)

// IDs returns the list of all supported canonical fault identifiers.
func IDs() []string {
	return []string{MUT0, MUT1, MUT2, MUT3, MUT4, MUT5, MUT6, MUT7, MUT8, MUT9}
}

// Aliases returns all supported descriptive alias fault identifiers.
func Aliases() []string {
	return []string{MutWalK, MutManRot, MutRocks10573, MutRocks10460, MutDirK}
}

// NormalizeID converts a canonical ID or alias into its canonical form.
func NormalizeID(id string) (string, error) {
	switch id {
	case MUT0:
		return MUT0, nil
	case MUT1:
		return MUT1, nil
	case MUT2:
		return MUT2, nil
	case MUT3:
		return MUT3, nil
	case MUT4:
		return MUT4, nil
	case MUT5, MutWalK:
		return MUT5, nil
	case MUT6, MutManRot:
		return MUT6, nil
	case MUT7, MutRocks10573:
		return MUT7, nil
	case MUT8, MutRocks10460:
		return MUT8, nil
	case MUT9, MutDirK:
		return MUT9, nil
	default:
		return "", fmt.Errorf("faults: unknown fault id %q (supported: %v)", id, IDs())
	}
}

// IsSparse reports whether a fault model is a sparse/windowed fault model.
func IsSparse(id string) bool {
	norm, err := NormalizeID(id)
	if err != nil {
		return false
	}
	switch norm {
	case MUT2, MUT3, MUT4, MUT5, MUT6, MUT7, MUT8, MUT9:
		return true
	default:
		return false
	}
}

// Config defines execution parameters for fault injection.
type Config struct {
	ID          string // Canonical ID (MUT-0..MUT-9) or descriptive alias
	WALDropK    int    // Drop strictly the k-th WAL sync for MUT5 (default: 1)
	DirDropK    int    // Drop strictly the k-th directory sync for MUT9 (default: 1)
	TriggerName string // Lifecycle trigger event for MUT8 (default: "CompactionBegin")
	DropBudget  int    // Number of syncs to drop after trigger for MUT8 (default: 1)
}

// DefaultConfig returns calibrated defaults for a given fault identifier.
func DefaultConfig(id string) Config {
	norm, _ := NormalizeID(id)
	if norm == "" {
		norm = id
	}
	return Config{
		ID:          norm,
		WALDropK:    1,
		DirDropK:    1,
		TriggerName: "CompactionBegin",
		DropBudget:  1,
	}
}

// FS wraps an inner vfs.FS and injects faults based on its configuration.
type FS struct {
	cfg   Config
	inner vfs.FS
	mu    sync.Mutex

	// MUT-5 state
	walSyncCount   int
	walSyncDropped bool

	// MUT-6 state
	manifestCount     int
	manifestSyncCount int
	manSyncDropped    bool

	// MUT-7 state
	pendingDirSyncDrop bool
	dirSyncDropped     bool

	// MUT-8 state
	triggered        bool
	triggerDropCount int

	// MUT-9 state
	dirSyncCount    int
	kDirSyncDropped bool
}

// Compile-time interface check.
var _ vfs.FS = (*FS)(nil)

// New constructs a fault-injected filesystem wrapper around inner with default config.
// id may be any canonical ID (MUT-0..MUT-9) or supported alias.
func New(id string, inner vfs.FS) (vfs.FS, error) {
	return NewWithConfig(DefaultConfig(id), inner)
}

// NewWithConfig constructs a fault-injected filesystem wrapper around inner with custom config.
func NewWithConfig(cfg Config, inner vfs.FS) (vfs.FS, error) {
	if inner == nil {
		return nil, errors.New("faults: inner FS cannot be nil")
	}
	normID, err := NormalizeID(cfg.ID)
	if err != nil {
		return nil, err
	}
	cfg.ID = normID
	if cfg.WALDropK <= 0 {
		cfg.WALDropK = 1
	}
	if cfg.DirDropK <= 0 {
		cfg.DirDropK = 1
	}
	if cfg.DropBudget <= 0 {
		cfg.DropBudget = 1
	}
	if cfg.TriggerName == "" {
		cfg.TriggerName = "CompactionBegin"
	}
	return &FS{
		cfg:   cfg,
		inner: inner,
	}, nil
}

// ID returns the active canonical fault identifier.
func (fs *FS) ID() string {
	return fs.cfg.ID
}

// Config returns a copy of the active configuration.
func (fs *FS) Config() Config {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.cfg
}

// Unwrap returns the underlying filesystem.
func (fs *FS) Unwrap() vfs.FS {
	return fs.inner
}

// State inspection methods
func (fs *FS) WALSyncCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.walSyncCount
}

func (fs *FS) WALSyncDropped() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.walSyncDropped
}

func (fs *FS) ManifestSyncCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.manifestSyncCount
}

func (fs *FS) ManifestSyncDropped() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.manSyncDropped
}

func (fs *FS) DirSyncDropped() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.dirSyncDropped || fs.kDirSyncDropped
}

func (fs *FS) DirSyncCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.dirSyncCount
}

func (fs *FS) KDirSyncDropped() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.kDirSyncDropped
}

func (fs *FS) PendingDirSyncDrop() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.pendingDirSyncDrop
}

func (fs *FS) ArmDirSyncDrop() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.pendingDirSyncDrop = true
}

func (fs *FS) Trigger(name string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.cfg.TriggerName == "" || name == fs.cfg.TriggerName || name == "CompactionBegin" || name == "TableCreated" {
		fs.triggered = true
	}
}

func (fs *FS) SetTriggered(triggered bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.triggered = triggered
}

func (fs *FS) IsTriggered() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.triggered
}

func (fs *FS) TriggerDropCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.triggerDropCount
}

func (fs *FS) Reset() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.walSyncCount = 0
	fs.walSyncDropped = false
	fs.manifestCount = 0
	fs.manifestSyncCount = 0
	fs.manSyncDropped = false
	fs.pendingDirSyncDrop = false
	fs.dirSyncDropped = false
	fs.triggered = false
	fs.triggerDropCount = 0
	fs.dirSyncCount = 0
	fs.kDirSyncDropped = false
}

func (fs *FS) onFileCreate(name string, category vfs.DiskWriteCategory) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if category == "pebble-manifest" {
		fs.manifestCount++
	}
	if fs.cfg.ID == MUT8 {
		if category == "pebble-compaction" {
			fs.triggered = true
		}
	}
}

func isMetadataPointer(base, full string) bool {
	if base == "CURRENT" || strings.HasPrefix(base, "CURRENT") {
		return true
	}
	if strings.HasPrefix(base, "OPTIONS") {
		return true
	}
	if strings.Contains(base, "marker.manifest") || strings.Contains(full, "marker.manifest") {
		return true
	}
	if strings.Contains(full, "CURRENT") {
		return true
	}
	return false
}

func (fs *FS) shouldDropSync(f *file, op string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	switch fs.cfg.ID {
	case MUT0:
		return false

	case MUT1:
		return string(f.category) == "pebble-wal"

	case MUT2:
		return string(f.category) == "pebble-manifest"

	case MUT3:
		return f.isDir

	case MUT4:
		return (op == "SyncData" || op == "SyncTo") && string(f.category) == "pebble-memtable-flush"

	case MUT5:
		if string(f.category) == "pebble-wal" {
			fs.walSyncCount++
			if fs.walSyncCount == fs.cfg.WALDropK && !fs.walSyncDropped {
				fs.walSyncDropped = true
				return true
			}
		}
		return false

	case MUT6:
		if string(f.category) == "pebble-manifest" {
			fs.manifestSyncCount++
			if !fs.manSyncDropped {
				if fs.manifestCount > 1 || fs.manifestSyncCount >= 2 || fs.walSyncCount > 0 {
					fs.manSyncDropped = true
					return true
				}
			}
		}
		return false

	case MUT7:
		if f.isDir && fs.pendingDirSyncDrop {
			fs.pendingDirSyncDrop = false
			fs.dirSyncDropped = true
			return true
		}
		return false

	case MUT8:
		if fs.triggered && fs.triggerDropCount < fs.cfg.DropBudget {
			if string(f.category) == "pebble-compaction" || string(f.category) == "pebble-memtable-flush" {
				fs.triggerDropCount++
				return true
			}
		}
		return false

	case MUT9:
		if f.isDir {
			fs.dirSyncCount++
			if fs.dirSyncCount == fs.cfg.DirDropK && !fs.kDirSyncDropped {
				fs.kDirSyncDropped = true
				return true
			}
		}
		return false

	default:
		return false
	}
}

// Create implements vfs.FS.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.inner.Create(name, category)
	if err != nil {
		return nil, err
	}
	fs.onFileCreate(name, category)
	return &file{
		fs:       fs,
		inner:    f,
		category: category,
	}, nil
}

// Link implements vfs.FS.
func (fs *FS) Link(oldname, newname string) error {
	fs.mu.Lock()
	if fs.cfg.ID == MUT7 {
		baseNew := fs.inner.PathBase(newname)
		if isMetadataPointer(baseNew, newname) {
			fs.pendingDirSyncDrop = true
		}
	}
	fs.mu.Unlock()
	return fs.inner.Link(oldname, newname)
}

// Open implements vfs.FS.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.inner.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &file{
		fs:    fs,
		inner: f,
	}, nil
}

// OpenReadWrite implements vfs.FS.
func (fs *FS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.inner.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	fs.onFileCreate(name, category)
	return &file{
		fs:       fs,
		inner:    f,
		category: category,
	}, nil
}

// OpenDir implements vfs.FS.
func (fs *FS) OpenDir(name string) (vfs.File, error) {
	f, err := fs.inner.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &file{
		fs:    fs,
		inner: f,
		isDir: true,
	}, nil
}

// Remove implements vfs.FS.
func (fs *FS) Remove(name string) error {
	return fs.inner.Remove(name)
}

// RemoveAll implements vfs.FS.
func (fs *FS) RemoveAll(name string) error {
	return fs.inner.RemoveAll(name)
}

// Rename implements vfs.FS.
func (fs *FS) Rename(oldname, newname string) error {
	fs.mu.Lock()
	if fs.cfg.ID == MUT7 {
		baseNew := fs.inner.PathBase(newname)
		if isMetadataPointer(baseNew, newname) {
			fs.pendingDirSyncDrop = true
		}
	}
	fs.mu.Unlock()
	return fs.inner.Rename(oldname, newname)
}

// ReuseForWrite implements vfs.FS.
func (fs *FS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.inner.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	fs.onFileCreate(newname, category)
	return &file{
		fs:       fs,
		inner:    f,
		category: category,
	}, nil
}

// MkdirAll implements vfs.FS.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.inner.MkdirAll(dir, perm)
}

// Lock implements vfs.FS.
func (fs *FS) Lock(name string) (io.Closer, error) {
	return fs.inner.Lock(name)
}

// List implements vfs.FS.
func (fs *FS) List(dir string) ([]string, error) {
	return fs.inner.List(dir)
}

// Stat implements vfs.FS.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	return fs.inner.Stat(name)
}

// PathBase implements vfs.FS.
func (fs *FS) PathBase(path string) string {
	return fs.inner.PathBase(path)
}

// PathJoin implements vfs.FS.
func (fs *FS) PathJoin(elem ...string) string {
	return fs.inner.PathJoin(elem...)
}

// PathDir implements vfs.FS.
func (fs *FS) PathDir(path string) string {
	return fs.inner.PathDir(path)
}

// GetDiskUsage implements vfs.FS.
func (fs *FS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return fs.inner.GetDiskUsage(path)
}

type file struct {
	fs       *FS
	inner    vfs.File
	category vfs.DiskWriteCategory
	isDir    bool
}

var _ vfs.File = (*file)(nil)

func (f *file) Close() error {
	return f.inner.Close()
}

func (f *file) Read(p []byte) (int, error) {
	return f.inner.Read(p)
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	return f.inner.ReadAt(p, off)
}

func (f *file) Write(p []byte) (int, error) {
	return f.inner.Write(p)
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	return f.inner.WriteAt(p, off)
}

func (f *file) Stat() (vfs.FileInfo, error) {
	return f.inner.Stat()
}

func (f *file) Sync() error {
	if f.fs != nil && f.fs.shouldDropSync(f, "Sync") {
		return nil
	}
	return f.inner.Sync()
}

func (f *file) SyncData() error {
	if f.fs != nil && f.fs.shouldDropSync(f, "SyncData") {
		return nil
	}
	return f.inner.SyncData()
}

func (f *file) SyncTo(length int64) (fullSync bool, err error) {
	if f.fs != nil && f.fs.shouldDropSync(f, "SyncTo") {
		return false, nil
	}
	return f.inner.SyncTo(length)
}

func (f *file) Preallocate(off, length int64) error {
	return f.inner.Preallocate(off, length)
}

func (f *file) Prefetch(off, length int64) error {
	return f.inner.Prefetch(off, length)
}

func (f *file) Fd() uintptr {
	return f.inner.Fd()
}
