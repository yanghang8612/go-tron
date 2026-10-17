package pebbledb

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestMaintenancePressureUnavailable(t *testing.T) {
	for _, db := range []*Database{nil, {}, {closed: true}} {
		got := db.MaintenancePressure()
		if got.Available || !got.SampledAt.IsZero() {
			t.Fatalf("unavailable database reported pressure: %+v", got)
		}
	}
}

func TestMaintenancePressureReflectsEngineAndOptions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		opts          Options
		compact, stop int
	}{
		{name: "default", opts: Options{}, compact: 8, stop: 64},
		{name: "configured", opts: Options{L0CompactionThreshold: 3, L0StopWritesThreshold: 19}, compact: 3, stop: 19},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.opts.DisableAutomaticCompactions = true
			db, err := New(t.TempDir(), 16, 32, "test/maintenance-pressure/"+tc.name+"/", false, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			// Overlapping flushed keys create multiple real L0 sublevels. With
			// automatic compaction disabled, the comparison remains stable.
			for i := 0; i < 2; i++ {
				if err := db.Put([]byte("overlapping"), bytes.Repeat([]byte{byte(i)}, 4096)); err != nil {
					t.Fatal(err)
				}
				if err := db.db.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			before := time.Now()
			got := db.MaintenancePressure()
			after := time.Now()
			if !got.Available || got.SampledAt.Before(before) || got.SampledAt.After(after) {
				t.Fatalf("bad sample availability/time: %+v", got)
			}
			if got.L0CompactionThreshold != tc.compact || got.L0StopWritesThreshold != tc.stop || got.MemTableStopWritesThreshold != 8 {
				t.Fatalf("wrong opened thresholds: %+v", got)
			}
			engine := db.db.Metrics()
			if got.L0Sublevels < 2 || got.L0Sublevels != int(engine.Levels[0].Sublevels) || got.MemTableCount != engine.MemTable.Count || got.CompactionDebt != engine.Compact.EstimatedDebt {
				t.Fatalf("pressure differs from engine: %+v; L0=%d memtables=%d debt=%d", got, engine.Levels[0].Sublevels, engine.MemTable.Count, engine.Compact.EstimatedDebt)
			}
			if got.WriteStalled || got.StallCount != 0 || got.StallDuration != 0 {
				t.Fatalf("unexpected stall pressure: %+v", got)
			}
		})
	}
}

func TestMaintenancePressureObservesStallLifecycle(t *testing.T) {
	db, err := New(t.TempDir(), 16, 32, "test/maintenance-pressure/stall/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Exercise the same listener callbacks Pebble uses without forcing an
	// actual blocked writer (or a timing-dependent background compaction).
	db.onWriteStallBegin(pebble.WriteStallBeginInfo{Reason: "memtable count limit reached"})
	during := db.MaintenancePressure()
	if !during.Available || !during.WriteStalled || during.StallCount != 1 || during.StallDuration != 0 {
		t.Fatalf("active stall not observed: %+v", during)
	}
	db.writeDelayStartTime = time.Now().Add(-10 * time.Millisecond)
	db.onWriteStallEnd()
	after := db.MaintenancePressure()
	if after.WriteStalled || after.StallCount != 1 || after.StallDuration < 10*time.Millisecond {
		t.Fatalf("completed stall not observed: %+v", after)
	}
}

func TestMaintenancePressureFreshAcrossFlushAndCompaction(t *testing.T) {
	db, err := New(t.TempDir(), 16, 32, "test/maintenance-pressure/transitions/", false, Options{DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	check := func(stage string) {
		t.Helper()
		before := time.Now()
		got := db.MaintenancePressure()
		full := db.db.Metrics()
		if !got.Available || got.SampledAt.Before(before) || got.SampledAt.After(time.Now()) ||
			got.L0Sublevels != int(full.Levels[0].Sublevels) || got.MemTableCount != full.MemTable.Count || got.CompactionDebt != full.Compact.EstimatedDebt {
			t.Fatalf("%s: pressure %+v differs from current engine %+v", stage, got, db.db.MaintenanceMetrics())
		}
	}
	check("empty")
	if err := db.Put([]byte("overlapping"), bytes.Repeat([]byte{1}, 4096)); err != nil {
		t.Fatal(err)
	}
	check("pending memtable")
	if err := db.db.Flush(); err != nil {
		t.Fatal(err)
	}
	check("first flush")
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	check("base compacted")
	for i := byte(2); i < 5; i++ {
		if err := db.Put([]byte("overlapping"), bytes.Repeat([]byte{i}, 4096)); err != nil {
			t.Fatal(err)
		}
		if err := db.db.Flush(); err != nil {
			t.Fatal(err)
		}
		check("overlapping flush")
	}
	if got := db.MaintenancePressure(); got.L0Sublevels < 3 || got.CompactionDebt == 0 {
		t.Fatalf("fixture did not exercise real debt: %+v", got)
	}
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	check("compacted again")
}

func TestMaintenancePressureConcurrentClose(t *testing.T) {
	db, err := New(t.TempDir(), 16, 32, "test/maintenance-pressure/close/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var ready, done sync.WaitGroup
	const readers = 8
	ready.Add(readers)
	done.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer done.Done()
			_ = db.MaintenancePressure()
			ready.Done()
			for db.MaintenancePressure().Available {
			}
		}()
	}
	ready.Wait()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	done.Wait()
	if got := db.MaintenancePressure(); got.Available || !got.SampledAt.IsZero() {
		t.Fatalf("closed database reported pressure: %+v", got)
	}
}
