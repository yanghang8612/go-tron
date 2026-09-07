package pebbledb

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

var (
	ErrMaintenanceSpace       = errors.New("maintenance SST write would cross free-space reserve")
	ErrMaintenanceWriteBudget = errors.New("maintenance SST write budget exhausted")
)

// maintenanceFS only wraps new writable SSTs. WAL/MANIFEST writes are allowed
// their reserved headroom, because injecting their commit failures is unsafe.
// The mutex serializes our own admission and allocation, not other processes.
// Attempts are charged conservatively (4KiB rounded; preallocation and writes
// may count the same space twice), never refunded even after an SST is removed.
type maintenanceFS struct {
	vfs.FS
	path                    string
	mu                      sync.Mutex
	floor, budget, admitted uint64
	stopped                 error
}

func (fs *maintenanceFS) admittedBytes() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.admitted
}

func (fs *maintenanceFS) preflight() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.stopped != nil {
		return fs.stopped
	}
	usage, err := fs.FS.GetDiskUsage(fs.path)
	if err != nil {
		return err
	}
	remaining := fs.budget - fs.admitted
	if usage.AvailBytes < fs.floor || usage.AvailBytes-fs.floor < remaining {
		return fmt.Errorf("%w: available=%d reserve=%d remaining_budget=%d", ErrMaintenanceSpace, usage.AvailBytes, fs.floor, remaining)
	}
	return nil
}

// allocate must encompass the actual IO so our own concurrent allocations
// cannot all pass a check against the same available bytes. A failed admission
// remains sticky: vfs.syncingFile ignores a Preallocate error, so the following
// Write MUST also fail before touching the SST.
func (fs *maintenanceFS) allocate(n uint64, io func() error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.stopped != nil {
		return fs.stopped
	}
	if n > ^uint64(0)-4095 {
		fs.stopped = ErrMaintenanceWriteBudget
		return fs.stopped
	}
	n = (n + 4095) &^ 4095
	if n > fs.budget-fs.admitted {
		fs.stopped = ErrMaintenanceWriteBudget
		return fs.stopped
	}
	usage, err := fs.FS.GetDiskUsage(fs.path)
	if err != nil {
		fs.stopped = err
		return err
	}
	if usage.AvailBytes < fs.floor || n > usage.AvailBytes-fs.floor {
		fs.stopped = fmt.Errorf("%w: available=%d reserve=%d allocation=%d", ErrMaintenanceSpace, usage.AvailBytes, fs.floor, n)
		return fs.stopped
	}
	fs.admitted += n
	if err := io(); err != nil {
		fs.stopped = err
		return err
	}
	return nil
}

func (fs *maintenanceFS) wrap(name string, open func() (vfs.File, error)) (file vfs.File, err error) {
	if !strings.HasSuffix(name, ".sst") {
		return open()
	}
	err = fs.allocate(4096, func() error { file, err = open(); return err })
	if err != nil {
		return nil, err
	}
	return &maintenanceFile{File: file, fs: fs}, nil
}

func (fs *maintenanceFS) Create(name string) (vfs.File, error) {
	return fs.wrap(name, func() (vfs.File, error) { return fs.FS.Create(name) })
}

func (fs *maintenanceFS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(name, func() (vfs.File, error) { return fs.FS.OpenReadWrite(name, opts...) })
}

func (fs *maintenanceFS) ReuseForWrite(old, name string) (vfs.File, error) {
	return fs.wrap(name, func() (vfs.File, error) { return fs.FS.ReuseForWrite(old, name) })
}

type maintenanceFile struct {
	vfs.File
	fs *maintenanceFS
}

func (f *maintenanceFile) Write(p []byte) (n int, err error) {
	err = f.fs.allocate(uint64(len(p)), func() error { n, err = f.File.Write(p); return err })
	return n, err
}

func (f *maintenanceFile) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative SST write offset")
	}
	// Conservatively include a possible sparse gap, rather than treating the
	// payload length as a bound on the file's logical growth.
	err = f.fs.allocate(uint64(off)+uint64(len(p)), func() error { n, err = f.File.WriteAt(p, off); return err })
	return n, err
}

func (f *maintenanceFile) Preallocate(off, length int64) error {
	if off < 0 || length < 0 {
		return errors.New("negative SST preallocation")
	}
	return f.fs.allocate(uint64(length), func() error { return f.File.Preallocate(off, length) })
}
