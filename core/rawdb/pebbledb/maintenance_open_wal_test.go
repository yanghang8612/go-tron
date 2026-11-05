package pebbledb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
)

// A real WAL seeded without flushing. Closing the clean engine leaves the WAL
// for the next normal writable Open replay; no crash/copy or production DB.
func seedMaintenanceReplayWAL(t *testing.T, path string) {
	t.Helper()
	d, e := pebble.Open(path, &pebble.Options{Comparer: exactPointComparer, DisableAutomaticCompactions: true, Logger: panicLogger{}})
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 32; i++ {
		if e = d.Set([]byte(fmt.Sprintf("wal-%03d", i)), bytes.Repeat([]byte{byte(i)}, 128), pebble.Sync); e != nil {
			t.Fatal(e)
		}
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestMaintenanceOpenRealWALCallbackSequence(t *testing.T) {
	path := t.TempDir() + "/db"
	seedMaintenanceReplayWAL(t, path)
	var mu sync.Mutex
	opening := true
	events := []map[string]any{}
	began := map[int]bool{}
	unpaired := 0
	listener := &pebble.EventListener{
		FlushBegin: func(i pebble.FlushInfo) {
			mu.Lock()
			defer mu.Unlock()
			began[i.JobID] = true
			events = append(events, map[string]any{"event": "FlushBegin", "job": i.JobID, "opening": opening})
		},
		FlushEnd: func(i pebble.FlushInfo) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, map[string]any{"event": "FlushEnd", "job": i.JobID, "opening": opening})
		},
		TableCreated: func(i pebble.TableCreateInfo) {
			mu.Lock()
			defer mu.Unlock()
			known := began[i.JobID]
			events = append(events, map[string]any{"event": "TableCreated", "reason": i.Reason, "job": i.JobID, "opening": opening, "matched_FlushBegin": known})
			if opening && i.Reason == "flushing" && !known {
				unpaired++
			}
		},
	}
	d, e := pebble.Open(path, &pebble.Options{Comparer: exactPointComparer, DisableAutomaticCompactions: true, Logger: panicLogger{}, EventListener: listener})
	if e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	opening = false
	count := unpaired
	log := append([]map[string]any(nil), events...)
	mu.Unlock()
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	if count == 0 {
		t.Fatalf("no real replayWAL unpaired event: %+v", log)
	}
	raw, e := json.Marshal(log)
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("actual_writable_Open_WAL_callbacks=%s; unmatched_open_flushing=%d", raw, count)
}
func TestMaintenanceOpenRealWALObserverAccounting(t *testing.T) {
	path := t.TempDir() + "/db"
	seedMaintenanceReplayWAL(t, path)
	opts := DefaultOptions()
	opts.DisableAutomaticCompactions = true
	d, e := New(path, 16, 16, t.Name()+"/", false, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	for i := 0; i < 32; i++ {
		v, e := d.Get([]byte(fmt.Sprintf("wal-%03d", i)))
		if e != nil || !bytes.Equal(v, bytes.Repeat([]byte{byte(i)}, 128)) {
			t.Fatalf("WAL value oracle%d %v", i, e)
		}
	}
	o := d.maintenanceWork
	o.publish(d.db.Metrics(), false)
	if invalid := workGauge(o, "maintenance/work/invalid_events"); invalid != 0 {
		t.Fatalf("legal actual Open/WAL unpaired TableCreated polluted invalid_events=%d", invalid)
	}
	if g := o.gauges["maintenance/work/startup_unscoped_table_created"]; g == nil || g.Snapshot().Value() == 0 {
		t.Fatal("actual Open/WAL unpaired tables not separately counted")
	}
	if workGauge(o, "flush/work/started_jobs") != 0 || workGauge(o, "flush/work/job_wall_nanos") != 0 || workGauge(o, "flush/work/started_input_memory_bytes") != 0 {
		t.Fatal("WAL startup table fabricated paired job accounting")
	}
}
