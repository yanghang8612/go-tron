package pebble

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// Only this picker method may be needed by the lightweight API. An accidental
// dependency on any other picker operation fails through the embedded nil.
type maintenanceMetricsPicker struct{ compactionPicker }

func (maintenanceMetricsPicker) estimatedCompactionDebt(extra uint64) uint64 {
	if extra != 0 {
		panic("maintenance metrics must use the current L0 size")
	}
	return 731
}

func TestMaintenanceMetricsDoesNotRequireCacheOrOtherMetricsSources(t *testing.T) {
	// This intentionally unopened receiver has no options/cache, table cache,
	// manifest, object provider, WAL metrics, or memtable objects. It provides
	// only the three real fields read by the API; no live mutex is manipulated.
	d := &DB{}
	d.mu.versions = &versionSet{picker: maintenanceMetricsPicker{}}
	d.mu.versions.metrics.Levels[0].Sublevels = 19
	d.mu.mem.queue = make(flushableList, 3)
	want := MaintenanceMetrics{L0Sublevels: 19, MemTableCount: 3, CompactionDebt: 731}
	if got := d.MaintenanceMetrics(); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = d.MaintenanceMetrics() }); allocs != 0 {
		t.Fatalf("lightweight metrics allocated: %g", allocs)
	}
}

func TestMaintenanceMetricsMatchesEngineTransitions(t *testing.T) {
	d, err := Open("maintenance-metrics", &Options{
		FS: vfs.NewMem(), DisableAutomaticCompactions: true,
		MemTableSize: 64 << 10, L0StopWritesThreshold: 64,
		LBaseMaxBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	check := func(stage string) MaintenanceMetrics {
		t.Helper()
		got := d.MaintenanceMetrics()
		full := d.Metrics()
		want := MaintenanceMetrics{full.Levels[0].Sublevels, full.MemTable.Count, full.Compact.EstimatedDebt}
		if got != want {
			t.Fatalf("%s: lightweight %+v differs from full %+v", stage, got, want)
		}
		return got
	}
	write := func(revision byte) {
		t.Helper()
		b := d.NewBatch()
		defer b.Close()
		for i := 0; i < 96; i++ {
			if err := b.Set([]byte(fmt.Sprintf("key-%04d", i)), bytes.Repeat([]byte{revision, byte(i)}, 128), nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Commit(NoSync); err != nil {
			t.Fatal(err)
		}
	}
	check("empty")
	write(0)
	if got := check("pending memtable"); got.MemTableCount < 1 {
		t.Fatalf("missing active memtable: %+v", got)
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	check("first flush")
	if err := d.Compact(nil, []byte{0xff}, false); err != nil {
		t.Fatal(err)
	}
	check("base compacted")
	for i := byte(1); i <= 4; i++ {
		write(i)
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		check(fmt.Sprintf("overlapping flush %d", i))
	}
	if got := check("L0 pressure"); got.L0Sublevels < 4 || got.CompactionDebt == 0 {
		t.Fatalf("fixture did not exercise L0 and nonzero debt: %+v", got)
	}
	if err := d.Compact(nil, []byte{0xff}, false); err != nil {
		t.Fatal(err)
	}
	if got := check("compacted again"); got.L0Sublevels != 0 {
		t.Fatalf("L0 not compacted: %+v", got)
	}
}

func TestMaintenanceMetricsConcurrentWritesAndFlush(t *testing.T) {
	d, err := Open("maintenance-concurrent", &Options{
		FS: vfs.NewMem(), DisableAutomaticCompactions: true, L0StopWritesThreshold: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	done := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = d.MaintenanceMetrics()
				}
			}
		}()
	}
	defer func() { close(done); readers.Wait() }()
	for i := 0; i < 8; i++ {
		if err := d.Set([]byte("key"), []byte{byte(i)}, NoSync); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
	}
}
