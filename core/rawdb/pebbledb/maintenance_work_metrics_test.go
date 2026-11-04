package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func workGauge(o *maintenanceWorkObserver, name string) int64 {
	return o.gauges[name].Snapshot().Value()
}
func TestMaintenanceWorkOpenPhasesErrorsAndSameNamespaceInstances(t *testing.T) {
	ns := t.Name() + "/"
	a, b := newMaintenanceWorkObserver(ns), newMaintenanceWorkObserver(ns)
	a.begin(7, false, 11)
	b.begin(7, false, 22)
	s := &pebble.Metrics{}
	s.Compact.Duration = 10
	a.publish(s, false)
	s.Compact.Duration = 20
	b.publish(s, false)
	if workGauge(a, "compact/work/active_jobs") != 2 || workGauge(a, "compact/work/active_input_sst_bytes") != 33 {
		t.Fatal("same-namespace contributions overwritten")
	}
	a.tableCreated(pebble.TableCreateInfo{JobID: 7, Reason: "compacting"})
	a.publish(nil, false)
	if workGauge(a, "compact/work/output_created_jobs") != 1 || workGauge(a, "compact/work/before_first_output_jobs") != 1 {
		t.Fatal("first output phase absent")
	}
	before := workGauge(a, "compact/work/busy_wall_nanos")
	time.Sleep(time.Millisecond)
	a.publish(nil, false)
	if workGauge(a, "compact/work/busy_wall_nanos") <= before {
		t.Fatal("open busy span absent")
	}
	a.end(7, false, errors.New("canceled"))
	a.publish(nil, true)
	if workGauge(a, "compact/work/active_jobs") != 1 || workGauge(a, "compact/work/active_input_sst_bytes") != 22 || workGauge(a, "maintenance/work/active_instances") != 1 {
		t.Fatal("closing one instance cleared another")
	}
	b.end(7, false, nil)
	s.Compact.Duration = 30
	b.publish(s, false)
	if workGauge(a, "compact/work/finished_jobs") != 2 || workGauge(a, "compact/work/failed_jobs") != 1 || workGauge(a, "compact/work/finished_input_sst_bytes") != 22 {
		t.Fatal("failure/cumulative accounting changed")
	}
	b.publish(nil, true)
}
func TestMaintenanceWorkFlushAndInvalidEvents(t *testing.T) {
	o := newMaintenanceWorkObserver(t.Name() + "/")
	o.begin(1, true, 64)
	o.publish(nil, false)
	one := workGauge(o, "flush/work/job_wall_nanos")
	time.Sleep(time.Millisecond)
	o.publish(nil, false)
	if workGauge(o, "flush/work/job_wall_nanos") <= one || workGauge(o, "flush/work/active_input_memory_bytes") != 64 {
		t.Fatal("open flush invisible")
	}
	o.tableCreated(pebble.TableCreateInfo{JobID: 1, Reason: "flushing"})
	o.end(1, true, errors.New("flush failed"))
	o.end(99, true, nil)
	o.begin(2, false, math.MaxUint64)
	o.publish(nil, false)
	if workGauge(o, "flush/work/active_jobs") != 0 || workGauge(o, "flush/work/failed_jobs") != 1 || workGauge(o, "maintenance/work/invalid_events") != 2 {
		t.Fatal("error/unsupported event silently became healthy zero")
	}
	o.end(2, false, nil)
	o.publish(nil, true)
}
func TestMaintenanceWorkRealFlushCompactionAndConcurrentScrape(t *testing.T) {
	tune := DefaultOptions()
	tune.DisableAutomaticCompactions = true
	tune.MaxConcurrentCompactions = 1
	tune.TargetFileSizeBytes = 32 << 10
	d, err := New(t.TempDir(), 16, 32, t.Name()+"/", false, tune)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for round := range 4 {
		for i := range 256 {
			if err := d.Put([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte{byte(round)}, 128)); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Compact([]byte{0}, []byte{255}); err != nil {
		t.Fatal(err)
	}
	for i := range 256 {
		got, err := d.Get([]byte(fmt.Sprintf("k%04d", i)))
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{3}, 128)) {
			t.Fatalf("oracle %d %v", i, err)
		}
	}
	stats := d.db.Metrics()
	d.maintenanceWork.publish(stats, false)
	o := d.maintenanceWork
	if workGauge(o, "flush/work/started_jobs") < 4 || workGauge(o, "flush/work/output_files_created") < 4 || workGauge(o, "compact/work/started_jobs") == 0 || workGauge(o, "compact/work/job_wall_nanos") <= 0 || workGauge(o, "maintenance/work/invalid_events") != 0 {
		t.Fatal("real callback/engine source mismatch")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(done)
		for i := range 100 {
			if err := d.Put([]byte(fmt.Sprintf("z%04d", i)), []byte("next")); err != nil {
				t.Error(err)
			}
		}
		if err := d.db.Flush(); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			d.maintenanceWork.publish(d.db.Metrics(), false)
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	wg.Wait()
	if workGauge(o, "maintenance/work/epoch_unix_nanos") == 0 || workGauge(o, "maintenance/work/refresh_unix_nanos") == 0 {
		t.Fatal("freshness absent")
	}
}

// Engine Duration can lose an open ingested-flush term while an unrelated
// normal compaction grows, leaving even a nonnegative engine delta misleading.
func TestMaintenanceWorkCompactionCounterIgnoresVanishingEngineTerm(t *testing.T) {
	o := newMaintenanceWorkObserver(t.Name() + "/")
	o.begin(1, false, 123)
	stats := &pebble.Metrics{}
	stats.Compact.Duration = time.Hour
	o.publish(stats, false)
	a := workGauge(o, "compact/work/job_wall_nanos")
	time.Sleep(time.Millisecond)
	stats.Compact.Duration = time.Hour + time.Minute
	o.publish(stats, false)
	b := workGauge(o, "compact/work/job_wall_nanos")
	if b <= a || b >= int64(time.Minute) {
		t.Fatal("tracker inherited inflated engine duration")
	}
	stats.Compact.Duration = time.Second
	o.end(1, false, nil)
	o.publish(stats, false)
	c := workGauge(o, "compact/work/job_wall_nanos")
	if c < b || c >= int64(time.Minute) {
		t.Fatal("vanishing engine term decreased/rebased counter")
	}
	// A masked positive engine delta is ignored too, with no active compaction.
	stats.Compact.Duration = 2 * time.Second
	o.publish(stats, false)
	if workGauge(o, "compact/work/job_wall_nanos") != c {
		t.Fatal("engine-only gain attributed as actual compaction")
	}
	o.publish(nil, true)
}

func TestMaintenanceWorkRealIngestedFlushLifecycle(t *testing.T) {
	fs := vfs.NewMem()
	o := newMaintenanceWorkObserver(t.Name() + "/")
	opened, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var onceOpen, onceEnd, onceRelease sync.Once
	// The test-only listener pauses a real ingest flush to inspect the tracker's
	// open span WITHOUT reentering DB.Metrics while the event holds DB.mu.
	opts := &pebble.Options{FS: fs, Comparer: exactPointComparer, FormatMajorVersion: pebble.FormatFlushableIngest, DisableAutomaticCompactions: true, Logger: panicLogger{}, EventListener: &pebble.EventListener{
		CompactionBegin: o.compactionBegin, CompactionEnd: func(info pebble.CompactionInfo) { o.end(info.JobID, false, info.Err) },
		FlushBegin: func(info pebble.FlushInfo) {
			o.begin(info.JobID, true, info.InputBytes)
			if info.Ingest {
				onceOpen.Do(func() { close(opened) })
				<-release
			}
		},
		FlushEnd: func(info pebble.FlushInfo) {
			o.end(info.JobID, true, info.Err)
			if info.Ingest {
				onceEnd.Do(func() { close(ended) })
			}
		}, TableCreated: o.tableCreated,
	}}
	db, err := pebble.Open("db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		onceRelease.Do(func() { close(release) })
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		o.publish(nil, true)
	}()
	if err := db.Set([]byte("k"), []byte("unflushed"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Create("input.sst")
	if err != nil {
		t.Fatal(err)
	}
	writer := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{Comparer: exactPointComparer, TableFormat: sstable.TableFormatPebblev2})
	if err := writer.Set([]byte("k"), []byte("ingested")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	// Force the queued flushable and allow either async ordering of Ingest.
	flushDone := make(chan error, 1)
	go func() {
		if err := db.Ingest([]string{"input.sst"}); err != nil {
			flushDone <- err
			return
		}
		flushDone <- db.Flush()
	}()
	select {
	case <-opened:
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not produce FlushInfo.Ingest")
	}
	o.publish(nil, false)
	before := workGauge(o, "flush/work/job_wall_nanos")
	time.Sleep(time.Millisecond)
	o.publish(nil, false)
	if workGauge(o, "flush/work/active_jobs") != 1 || workGauge(o, "flush/work/job_wall_nanos") <= before || workGauge(o, "compact/work/job_wall_nanos") != 0 {
		t.Fatal("ingest open span incorrectly disappeared/entered regular compaction sum")
	}
	onceRelease.Do(func() { close(release) })
	select {
	case <-ended:
	case <-time.After(3 * time.Second):
		t.Fatal("ingest did not finish")
	}
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	o.publish(db.Metrics(), false)
	closed := workGauge(o, "flush/work/job_wall_nanos")
	time.Sleep(time.Millisecond)
	o.publish(db.Metrics(), false)
	if workGauge(o, "flush/work/active_jobs") != 0 || workGauge(o, "flush/work/job_wall_nanos") < closed || workGauge(o, "compact/work/job_wall_nanos") != 0 || workGauge(o, "maintenance/work/invalid_events") != 0 {
		t.Fatal("ingested flush close accounting failed")
	}
	got, closer, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("ingested")) {
		t.Fatal("ingest changed data")
	}
	closer.Close()
}
