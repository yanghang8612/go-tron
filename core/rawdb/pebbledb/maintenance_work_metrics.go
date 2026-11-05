package pebbledb

import (
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

// Per-instance JobIDs and metadata are not DB handles. Callbacks may hold
// Pebble's mutex: they only copy sizes and update this small in-memory map.
// "input" is selected SST file size (flush: memtable footprint), NOT bytes
// already read. "output_created" is a TableCreated event, not an IO/CPU phase.
type maintenanceWorkJob struct {
	input         int64
	began         time.Time
	outputCreated bool
}
type maintenanceWorkBusy struct {
	done  time.Duration
	began time.Time
}
type maintenanceWorkObserver struct {
	mu                  sync.Mutex
	comp, flush         map[int]maintenanceWorkJob
	compBusy, flushBusy maintenanceWorkBusy
	values, published   map[string]int64
	gauges              map[string]*metrics.Gauge
	closed              bool
	opening             bool
}

var maintenanceWorkEpoch = time.Now().UnixNano()
var maintenanceWorkFields = []string{
	"maintenance/work/schema", "maintenance/work/epoch_unix_nanos", "maintenance/work/refresh_unix_nanos", "maintenance/work/active_instances", "maintenance/work/invalid_events", "maintenance/work/startup_unscoped_table_created",
	"compact/work/started_jobs", "compact/work/finished_jobs", "compact/work/failed_jobs", "compact/work/started_input_sst_bytes", "compact/work/finished_input_sst_bytes", "compact/work/active_jobs", "compact/work/active_input_sst_bytes", "compact/work/before_first_output_jobs", "compact/work/output_created_jobs", "compact/work/output_files_created", "compact/work/busy_wall_nanos", "compact/work/job_wall_nanos",
	"flush/work/started_jobs", "flush/work/finished_jobs", "flush/work/failed_jobs", "flush/work/started_input_memory_bytes", "flush/work/finished_input_memory_bytes", "flush/work/active_jobs", "flush/work/active_input_memory_bytes", "flush/work/before_first_output_jobs", "flush/work/output_created_jobs", "flush/work/output_files_created", "flush/work/busy_wall_nanos", "flush/work/job_wall_nanos",
	"flush/work/engine_active_jobs", "flush/work/engine_iterated_bytes", "flush/work/engine_work_wall_nanos", "flush/work/engine_idle_wall_nanos",
}

func newMaintenanceWorkObserver(namespace string) *maintenanceWorkObserver {
	o := &maintenanceWorkObserver{comp: make(map[int]maintenanceWorkJob), flush: make(map[int]maintenanceWorkJob), values: make(map[string]int64), published: make(map[string]int64), gauges: make(map[string]*metrics.Gauge)}
	for _, name := range maintenanceWorkFields {
		o.gauges[name] = metrics.GetOrRegisterGauge(namespace+name, nil)
	}
	// Epoch/schema are process constants, not zero-valued unsupported fallbacks.
	o.gauges["maintenance/work/schema"].Update(1)
	o.gauges["maintenance/work/epoch_unix_nanos"].Update(maintenanceWorkEpoch)
	o.values["maintenance/work/active_instances"] = 1
	return o
}

// observeOpen brackets only the actual Pebble.Open call. Callbacks and the
// window transition use the same mutex; no observer lock crosses an engine call.
// A failed/panicking constructor has no meter goroutine to join: retire just its
// observer contribution, never call partial Database.Close.
func (o *maintenanceWorkObserver) observeOpen(open func() (*pebble.DB, error)) (db *pebble.DB, err error) {
	o.mu.Lock()
	o.opening = true
	o.mu.Unlock()
	successful := false
	defer func() {
		o.mu.Lock()
		o.opening = false
		if !successful {
			o.values["maintenance/work/active_instances"] = 0
		}
		o.mu.Unlock()
		if !successful {
			o.publish(nil, true)
		}
	}()
	db, err = open()
	successful = err == nil && db != nil
	return db, err
}
func (o *maintenanceWorkObserver) begin(id int, flush bool, input uint64) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	jobs, busy, p, unit := o.comp, &o.compBusy, "compact/work/", "sst"
	if flush {
		jobs, busy, p, unit = o.flush, &o.flushBusy, "flush/work/", "memory"
	}
	if o.closed {
		o.values["maintenance/work/invalid_events"]++
		return
	}
	if _, exists := jobs[id]; exists {
		o.values["maintenance/work/invalid_events"]++
		return
	}
	if input > math.MaxInt64 {
		o.values["maintenance/work/invalid_events"]++
		input = 0
	}
	now := time.Now()
	if len(jobs) == 0 {
		busy.began = now
	}
	jobs[id] = maintenanceWorkJob{input: int64(input), began: now}
	o.values[p+"started_jobs"]++
	o.values[p+"started_input_"+unit+"_bytes"] += int64(input)
}
func (o *maintenanceWorkObserver) end(id int, flush bool, err error) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	jobs, busy, p, unit := o.comp, &o.compBusy, "compact/work/", "sst"
	if flush {
		jobs, busy, p, unit = o.flush, &o.flushBusy, "flush/work/", "memory"
	}
	job, exists := jobs[id]
	if !exists {
		o.values["maintenance/work/invalid_events"]++
		return
	}
	now := time.Now()
	delete(jobs, id)
	if len(jobs) == 0 {
		busy.done += now.Sub(busy.began)
	}
	o.values[p+"finished_jobs"]++
	if err != nil {
		o.values[p+"failed_jobs"]++
	} else {
		o.values[p+"finished_input_"+unit+"_bytes"] += job.input
	}
	o.values[p+"job_wall_nanos"] += now.Sub(job.began).Nanoseconds()
}
func (o *maintenanceWorkObserver) tableCreated(info pebble.TableCreateInfo) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	jobs, p := o.comp, "compact/work/"
	if info.Reason == "flushing" {
		jobs, p = o.flush, "flush/work/"
	} else if info.Reason != "compacting" {
		return
	}
	job, ok := jobs[info.JobID]
	if !ok {
		// WAL replay creates flush tables without FlushBegin/End. Count only
		// this known Open-window event; do not invent input, jobs or wall time.
		if !o.closed && o.opening && info.Reason == "flushing" {
			o.values["maintenance/work/startup_unscoped_table_created"]++
			return
		}
		o.values["maintenance/work/invalid_events"]++
		return
	}
	job.outputCreated = true
	jobs[info.JobID] = job
	o.values[p+"output_files_created"]++
}
func (o *maintenanceWorkObserver) compactionBegin(info pebble.CompactionInfo) {
	if o == nil {
		return
	}
	var input uint64
	for _, level := range info.Input {
		for _, table := range level.Tables {
			if table.Size > math.MaxUint64-input {
				input = math.MaxUint64
				break
			}
			input += table.Size
		}
	}
	o.begin(info.JobID, false, input)
}
func (o *maintenanceWorkObserver) publish(stats *pebble.Metrics, close bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	values := make(map[string]int64, len(maintenanceWorkFields))
	for k, v := range o.values {
		values[k] = v
	}
	if stats != nil {
		// Engine Compact.Duration temporarily includes ingestedFlushable work
		// that its completed compaction sum does not retain. Use only our actual
		// CompactionBegin/End lifecycle sum+open spans for the hard work counter.
		values["flush/work/engine_active_jobs"] = stats.Flush.NumInProgress
		values["flush/work/engine_iterated_bytes"] = stats.Flush.WriteThroughput.Bytes
		values["flush/work/engine_work_wall_nanos"] = stats.Flush.WriteThroughput.WorkDuration.Nanoseconds()
		values["flush/work/engine_idle_wall_nanos"] = stats.Flush.WriteThroughput.IdleDuration.Nanoseconds()
	} else {
		for _, name := range []string{"flush/work/engine_iterated_bytes", "flush/work/engine_work_wall_nanos", "flush/work/engine_idle_wall_nanos"} {
			values[name] = o.published[name]
		}
	}
	for _, group := range []struct {
		jobs    map[int]maintenanceWorkJob
		busy    maintenanceWorkBusy
		p, unit string
	}{{o.comp, o.compBusy, "compact/work/", "sst"}, {o.flush, o.flushBusy, "flush/work/", "memory"}} {
		values[group.p+"active_jobs"] = int64(len(group.jobs))
		wall := group.busy.done
		if len(group.jobs) > 0 {
			wall += now.Sub(group.busy.began)
		}
		values[group.p+"busy_wall_nanos"] = wall.Nanoseconds()
		for _, job := range group.jobs {
			values[group.p+"active_input_"+group.unit+"_bytes"] += job.input
			if job.outputCreated {
				values[group.p+"output_created_jobs"]++
			} else {
				values[group.p+"before_first_output_jobs"]++
			}
			values[group.p+"job_wall_nanos"] += now.Sub(job.began).Nanoseconds()
		}
	}
	if close || o.closed {
		// DB.Close has already joined its own jobs. Preserve cumulative work, remove
		// only this instance's live contribution; never clear another instance.
		if !o.closed && len(o.comp)+len(o.flush) > 0 {
			o.values["maintenance/work/invalid_events"]++
			values["maintenance/work/invalid_events"]++
		}
		for _, k := range []string{"maintenance/work/active_instances", "compact/work/active_jobs", "compact/work/active_input_sst_bytes", "compact/work/before_first_output_jobs", "compact/work/output_created_jobs", "flush/work/active_jobs", "flush/work/active_input_memory_bytes", "flush/work/before_first_output_jobs", "flush/work/output_created_jobs", "flush/work/engine_active_jobs"} {
			values[k] = 0
		}
		o.closed = true
	}
	for _, name := range maintenanceWorkFields {
		if name == "maintenance/work/schema" || name == "maintenance/work/epoch_unix_nanos" || name == "maintenance/work/refresh_unix_nanos" {
			continue
		}
		v := values[name]
		o.gauges[name].Inc(v - o.published[name])
		o.published[name] = v
	}
	o.gauges["maintenance/work/refresh_unix_nanos"].Update(now.UnixNano())
}
