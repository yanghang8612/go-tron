package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

type maintenanceTestFS struct {
	vfs.FS
	available uint64
}

func (fs *maintenanceTestFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{AvailBytes: fs.available, TotalBytes: fs.available}, nil
}

func maintenanceTestOptions() MaintenanceOptions {
	return MaintenanceOptions{MinFreeBytes: 1 << 20, MaxSSTWriteBytes: 16 << 20, TargetFileSizeBytes: 32 << 10}
}

func maintenanceTestPebble(t *testing.T, path string, readOnly bool) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{
		Comparer: exactPointComparer, ReadOnly: readOnly,
		DisableAutomaticCompactions: true, MaxConcurrentCompactions: func() int { return 1 },
		MemTableSize: 8 << 20, Levels: levelOptions(32 << 10), Logger: panicLogger{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func maintenanceSnapshot(t *testing.T, path string) map[string]string {
	t.Helper()
	db := maintenanceTestPebble(t, path, true)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	iter, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string)
	for ok := iter.First(); ok; ok = iter.Next() {
		out[string(iter.Key())] = string(bytes.Clone(iter.Value()))
	}
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		t.Fatal(err)
	}
	return out
}

func maintenanceFixture(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	db := maintenanceTestPebble(t, path, false)
	for _, key := range []string{"a-live", "n-endpoint", "z-live"} {
		if err := db.Set([]byte(key), bytes.Repeat([]byte(key), 1024), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 256; i++ {
		if err := db.Set([]byte(fmt.Sprintf("m-%04d", i)), bytes.Repeat([]byte{byte(i)}, 2048), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("m-%04d", i)), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// Keep byte-for-byte database file evidence, excluding the directory lock.
func maintenanceFiles(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "LOCK" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(path, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(b)
	}
	return out
}

func TestMaintenanceReadOnlyAndRefuseLive(t *testing.T) {
	path := maintenanceFixture(t)
	before := maintenanceFiles(t, path)
	m, err := openMaintenance(path, maintenanceTestOptions(), &maintenanceTestFS{vfs.Default, 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	info, err := m.Inspect([]byte("m"), []byte("n"))
	if err != nil || !info.Empty || info.EstimatedBytes == 0 || info.OverlappingTableBytes == 0 {
		t.Fatalf("inspect: %+v %v", info, err)
	}
	if has, err := m.Has([]byte("a-live")); err != nil || !has {
		t.Fatalf("Has: %v %v", has, err)
	}
	if has, err := m.Has([]byte("missing")); err != nil || has {
		t.Fatalf("Has missing: %v %v", has, err)
	}
	if _, err := m.CompactEmptyRange([]byte("a"), []byte("b")); !errors.Is(err, ErrMaintenanceLiveRange) {
		t.Fatalf("live range error: %v", err)
	}
	if other, err := pebble.Open(path, &pebble.Options{ReadOnly: true}); err == nil {
		other.Close()
		t.Fatal("maintenance lock did not exclude another open")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, maintenanceFiles(t, path)) {
		t.Fatal("read-only/refused operation changed database files")
	}
}

func TestMaintenanceRefusesLiveRangeKeys(t *testing.T) {
	path := t.TempDir()
	db, err := pebble.Open(path, &pebble.Options{Comparer: exactPointComparer, FormatMajorVersion: pebble.FormatRangeKeys, DisableAutomaticCompactions: true, Logger: panicLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RangeKeySet([]byte("a"), []byte("z"), []byte("suffix"), []byte("value"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := openMaintenance(path, maintenanceTestOptions(), &maintenanceTestFS{vfs.Default, 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CompactEmptyRange([]byte("m"), []byte("n")); !errors.Is(err, ErrMaintenanceLiveRange) {
		t.Fatalf("live range key accepted: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceCompactPreservesAllLiveKeys(t *testing.T) {
	path := maintenanceFixture(t)
	before := maintenanceSnapshot(t, path)
	m, err := openMaintenance(path, maintenanceTestOptions(), &maintenanceTestFS{vfs.Default, 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.CompactEmptyRange([]byte("m"), []byte("n"))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Before.Empty || !out.After.Empty || out.After.EstimatedBytes >= out.Before.EstimatedBytes || out.SSTBytesAdmitted == 0 {
		t.Fatalf("no verified reclamation: %+v", out)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, maintenanceSnapshot(t, path)) {
		t.Fatal("compaction changed live KV, including neighboring/end keys")
	}
}

func TestMaintenanceBudgetFailurePreservesAllLiveKeys(t *testing.T) {
	path := maintenanceFixture(t)
	before := maintenanceSnapshot(t, path)
	opts := maintenanceTestOptions()
	opts.MaxSSTWriteBytes = 4096 // Enough to create a file, but not write a block.
	m, err := openMaintenance(path, opts, &maintenanceTestFS{vfs.Default, 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.CompactEmptyRange([]byte("m"), []byte("n"))
	if !errors.Is(err, ErrMaintenanceWriteBudget) {
		t.Fatalf("want budget failure, got %+v %v", out, err)
	}
	if out.SSTBytesAdmitted > opts.MaxSSTWriteBytes {
		t.Fatal("exceeded budget")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, maintenanceSnapshot(t, path)) {
		t.Fatal("failed compaction changed live KV")
	}
}

func TestMaintenanceWALRecoveryAndCompactionPreserveAllLiveKeys(t *testing.T) {
	path := maintenanceFixture(t)
	db := maintenanceTestPebble(t, path, false)
	if err := db.Set([]byte("z-pending"), []byte("replayed"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("m-shadow"), []byte("deleted in WAL"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("m-shadow"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := maintenanceSnapshot(t, path)
	opts := maintenanceTestOptions()
	opts.AllowWALReplay = true
	m, err := openMaintenance(path, opts, &maintenanceTestFS{vfs.Default, 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.CompactEmptyRange([]byte("m"), []byte("n"))
	if err != nil || !out.Before.Empty || out.Before.MemTableCount == 0 || !out.After.Empty {
		t.Fatalf("replay and compaction: %+v %v", out, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, maintenanceSnapshot(t, path)) {
		t.Fatal("successful WAL replay/compaction changed live KV")
	}
}

func TestMaintenanceWALRecoveryFailureIsBoundedAndRecoverable(t *testing.T) {
	for _, kind := range []string{"point", "range-delete", "log-data", "large-batch", "empty"} {
		t.Run(kind, func(t *testing.T) {
			path := t.TempDir()
			db := maintenanceTestPebble(t, path, false)
			if kind == "large-batch" {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				// Keep a >32MiB batch unflushed at shutdown, then recover it
				// using maintenance's smaller 64MiB memtable so replay takes
				// Pebble's large-flushable-batch path.
				db, err = pebble.Open(path, &pebble.Options{Comparer: exactPointComparer, DisableAutomaticCompactions: true, MemTableSize: 128 << 20, Logger: panicLogger{}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if kind == "range-delete" {
				if err := db.Set([]byte("z-seed"), []byte("live"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
				if err := db.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch kind {
			case "point":
				err = db.Set([]byte("z-pending"), bytes.Repeat([]byte("x"), 8192), pebble.Sync)
			case "range-delete":
				err = db.DeleteRange([]byte("z"), []byte("zz"), pebble.Sync)
			case "log-data":
				err = db.LogData([]byte("barrier"), pebble.Sync)
			case "large-batch":
				b := db.NewBatch()
				if err = b.Set([]byte("z-large"), bytes.Repeat([]byte("x"), 40<<20), nil); err != nil {
					t.Fatal(err)
				}
				err = b.Commit(pebble.Sync)
				b.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := maintenanceSnapshot(t, path)
			opts := maintenanceTestOptions()
			opts.MaxSSTWriteBytes = 4096
			opts.AllowWALReplay = true
			m, err := openMaintenance(path, opts, &maintenanceTestFS{vfs.Default, 1 << 40})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := m.CompactEmptyRange([]byte("m"), []byte("n")); done <- err }()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("WAL recovery/compaction blocked instead of returning a space error")
			}
			if err != nil && !errors.Is(err, ErrMaintenanceWriteBudget) {
				t.Fatalf("unexpected recovery error: %v", err)
			}
			if kind == "point" && !errors.Is(err, ErrMaintenanceWriteBudget) {
				t.Fatal("point WAL recovery did not exercise allocation failure")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, maintenanceSnapshot(t, path)) {
				t.Fatal("failed recovery changed live KV")
			}
		})
	}
}

func TestMaintenanceWALPermissionAndSpacePreflight(t *testing.T) {
	path := t.TempDir()
	db := maintenanceTestPebble(t, path, false)
	if err := db.Set([]byte("z-pending"), []byte("value"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := maintenanceFiles(t, path)
	opts := maintenanceTestOptions()
	fs := &maintenanceTestFS{vfs.Default, 1 << 40}
	m, err := openMaintenance(path, opts, fs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CompactEmptyRange([]byte("m"), []byte("n")); !errors.Is(err, ErrMaintenanceWALReplay) {
		t.Fatalf("want replay permission error, got %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	opts.AllowWALReplay = true
	fs.available = opts.MinFreeBytes + MaintenanceControlReserveBytes + opts.MaxSSTWriteBytes - 1
	m, err = openMaintenance(path, opts, fs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CompactEmptyRange([]byte("m"), []byte("n")); !errors.Is(err, ErrMaintenanceSpace) {
		t.Fatalf("want preflight space error, got %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, maintenanceFiles(t, path)) {
		t.Fatal("refused WAL recovery changed files")
	}
}

func TestMaintenanceFSAllocationPathsAndStickyPreallocate(t *testing.T) {
	for _, op := range []string{"write", "write-at", "preallocate"} {
		t.Run(op, func(t *testing.T) {
			base := &maintenanceTestFS{vfs.NewMem(), 1 << 40}
			if err := base.MkdirAll("db", 0755); err != nil {
				t.Fatal(err)
			}
			fs := &maintenanceFS{FS: base, path: "db", floor: 8192, budget: 8192}
			f, err := fs.Create("db/000001.sst")
			if err != nil {
				t.Fatal(err)
			}
			switch op {
			case "write":
				_, err = f.Write(make([]byte, 4097))
			case "write-at":
				_, err = f.WriteAt([]byte{1}, 4096)
			case "preallocate":
				err = f.Preallocate(0, 4097)
			}
			if !errors.Is(err, ErrMaintenanceWriteBudget) {
				t.Fatalf("want budget error, got %v", err)
			}
			if _, err := f.Write([]byte{1}); !errors.Is(err, ErrMaintenanceWriteBudget) {
				t.Fatalf("ignored preallocation error was not sticky: %v", err)
			}
			if info, err := f.Stat(); err != nil || info.Size() != 0 {
				t.Fatalf("rejected allocation wrote data: %v %v", info, err)
			}
			f.Close()
			// Control-file commits remain possible after SST output was stopped.
			control, err := fs.Create("db/MANIFEST-000002")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := control.Write([]byte("commit")); err != nil {
				t.Fatal(err)
			}
			control.Close()
		})
	}
}

func TestMaintenanceFSRechecksFreeSpaceBeforeEachWrite(t *testing.T) {
	base := &maintenanceTestFS{vfs.NewMem(), 1 << 40}
	if err := base.MkdirAll("db", 0755); err != nil {
		t.Fatal(err)
	}
	fs := &maintenanceFS{FS: base, path: "db", floor: 8192, budget: 1 << 20}
	f, err := fs.Create("db/000001.sst")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	base.available = fs.floor + 4095 // Simulate a competing database allocation.
	if _, err := f.Write([]byte("second")); !errors.Is(err, ErrMaintenanceSpace) {
		t.Fatalf("want reserve error, got %v", err)
	}
	if info, err := f.Stat(); err != nil || info.Size() != 5 {
		t.Fatalf("rejected write touched file: %v %v", info, err)
	}
	f.Close()
}
