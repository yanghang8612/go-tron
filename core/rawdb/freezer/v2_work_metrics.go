package freezer

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// These are process-local V2 writer observations, not consensus state, CPU time,
// device IO, RSS or progress within EncodeAll. Payload bytes include the encoded
// record-length prefixes; capacities describe only the explicitly owned buffers.
const v2WorkPrefix = "chain/freezer/v2/work/"

const (
	v2EncodeStartedFrames = iota
	v2EncodeStartedRaw
	v2EncodeCompletedFrames
	v2EncodeCompletedRaw
	v2EncodeCompletedCompressed
	v2EncodeAbortedFrames
	v2EncodingRaw
	v2EncodingRawCap
	v2EncodingDstCap
	v2WaitingFrames
	v2WaitingRaw
	v2WaitingRawCap
	v2ReadyFrames
	v2ReadyRaw
	v2ReadyCompressed
	v2ReadyCompressedCap
	v2PendingFrames
	v2PendingRawCap
	v2PendingDstCap
	v2IdleInputCap
	v2IdleOutputCap
	v2AbandonedFrames
	v2ReadFrames
	v2ReadRaw
	v2WriteFrames
	v2WriteRaw
	v2WriteCompressed
	v2WriteReturned
	v2SegmentsStarted
	v2SegmentsCompleted
	v2SegmentsFailed
	v2PrepareOwnedRows
	v2PrepareOwnedCap
	v2DictionaryFallbacks
	v2ManifestFailures
	v2FieldCount
)

var v2WorkFields = [v2FieldCount]string{
	"encode/started_frames", "encode/started_raw_bytes", "encode/completed_frames", "encode/completed_raw_bytes", "encode/completed_compressed_bytes", "encode/aborted_frames", "encode/active_raw_bytes", "encode/active_raw_capacity_bytes", "encode/active_destination_initial_capacity_bytes",
	"waiting/frames", "waiting/raw_bytes", "waiting/raw_capacity_bytes", "ready_to_write/frames", "ready_to_write/raw_bytes", "ready_to_write/compressed_bytes", "ready_to_write/compressed_capacity_bytes",
	"pending/frames", "pending/raw_capacity_bytes", "pending/destination_capacity_bytes", "idle_pool/input_capacity_bytes", "idle_pool/output_capacity_bytes", "abandoned/frames",
	"frame_read/completed_frames", "frame_read/completed_raw_bytes", "frame_write/completed_frames", "frame_write/completed_raw_bytes", "frame_write/completed_compressed_bytes", "frame_write/returned_compressed_bytes",
	"segment/started", "segment/completed", "segment/failed", "preparation/owned_rows", "preparation/owned_input_capacity_bytes", "dictionary_train/fallbacks", "manifest_publish/failed",
}

const (
	v2StageEncode = iota
	v2StageRead
	v2StageWrite
	v2StageTrain
	v2StageCheckpoint
	v2StageFinalize
	v2StagePrepareLoad
	v2StagePrepareTransform
	v2StageManifest
	v2StageCount
)

var v2StageNames = [v2StageCount]string{"encode", "frame_read", "frame_write", "dictionary_train", "checkpoint", "segment_finalize", "preparation_load", "preparation_transform", "manifest_publish"}

type v2WallStage struct{ active, starts, finished, wall, sumStarted, busy, busyStarted int64 }
type v2WorkObserver struct {
	mu        sync.Mutex
	refreshMu sync.Mutex
	origin    time.Time
	values    [v2FieldCount]int64
	stages    [v2StageCount]v2WallStage
	gauges    map[string]*metrics.Gauge
}

func newV2WorkObserver(registry metrics.Registry) *v2WorkObserver {
	o := &v2WorkObserver{origin: time.Now(), gauges: make(map[string]*metrics.Gauge)}
	for _, name := range v2WorkFields {
		o.gauges[name] = metrics.GetOrRegisterGauge(v2WorkPrefix+name, registry)
	}
	for _, stage := range v2StageNames {
		for _, suffix := range []string{"active", "started", "finished", "worker_wall_nanos", "busy_wall_nanos"} {
			name := stage + "/" + suffix
			o.gauges[name] = metrics.GetOrRegisterGauge(v2WorkPrefix+name, registry)
		}
	}
	for _, name := range []string{"schema", "refresh_unix_nanos", "epoch_unix_nanos", "segment/active"} {
		o.gauges[name] = metrics.GetOrRegisterGauge(v2WorkPrefix+name, registry)
	}
	return o
}

var v2Work = newV2WorkObserver(nil)

type v2WorkSpan struct {
	o       *v2WorkObserver
	stage   int
	started int64
}

func (o *v2WorkObserver) begin(stage int) v2WorkSpan {
	o.mu.Lock()
	now := time.Since(o.origin).Nanoseconds()
	s := &o.stages[stage]
	if s.active == 0 {
		s.busyStarted = now
	}
	s.active++
	s.starts++
	s.sumStarted += now
	o.mu.Unlock()
	return v2WorkSpan{o, stage, now}
}
func (s v2WorkSpan) end() {
	s.o.mu.Lock()
	now := time.Since(s.o.origin).Nanoseconds()
	v := &s.o.stages[s.stage]
	v.active--
	v.finished++
	v.wall += now - s.started
	v.sumStarted -= s.started
	if v.active == 0 {
		v.busy += now - v.busyStarted
	}
	s.o.mu.Unlock()
}
func (o *v2WorkObserver) add(field int, value int64) {
	o.mu.Lock()
	o.values[field] += value
	o.mu.Unlock()
}
func (o *v2WorkObserver) snapshot() map[string]int64 {
	result := make(map[string]int64, len(o.gauges))
	o.mu.Lock()
	now := time.Now()
	tick := now.Sub(o.origin).Nanoseconds()
	defer o.mu.Unlock()
	for i, name := range v2WorkFields {
		result[name] = o.values[i]
	}
	for i, name := range v2StageNames {
		s := o.stages[i]
		result[name+"/active"] = s.active
		result[name+"/started"] = s.starts
		result[name+"/finished"] = s.finished
		result[name+"/worker_wall_nanos"] = s.wall + s.active*tick - s.sumStarted
		busy := s.busy
		if s.active > 0 {
			busy += tick - s.busyStarted
		}
		result[name+"/busy_wall_nanos"] = busy
	}
	result["segment/active"] = o.values[v2SegmentsStarted] - o.values[v2SegmentsCompleted] - o.values[v2SegmentsFailed]
	result["schema"] = 1
	result["refresh_unix_nanos"] = now.UnixNano()
	result["epoch_unix_nanos"] = o.origin.UnixNano()
	return result
}

// RefreshV2WorkMetrics runs in the existing 3-second NodeCollector, so open
// spans advance even while no frame completes. Exported gauges have that lag.
// Registry readers may straddle refreshes; fields are not one atomic JSON tuple.
func RefreshV2WorkMetrics() {
	v2Work.refreshMu.Lock()
	defer v2Work.refreshMu.Unlock()
	for name, value := range v2Work.snapshot() {
		v2Work.gauges[name].Update(value)
	}
}

// Each job owns only its own contribution. Cancellation cleanup runs after all
// workers join and never resets another concurrent pipeline's gauges.
const (
	v2JobWaiting  = 1
	v2JobEncoding = 2
	v2JobReady    = 3
	v2JobWriting  = 4
	v2JobReleased = 5
)

func (o *v2WorkObserver) admit(job *v2FrameEncodeJob) {
	o.mu.Lock()
	defer o.mu.Unlock()
	job.workStage = v2JobWaiting
	job.workDstCap = int64(cap(job.dst))
	v := &o.values
	v[v2WaitingFrames]++
	v[v2WaitingRaw] += int64(len(job.raw.data))
	v[v2WaitingRawCap] += int64(cap(job.raw.data))
	v[v2PendingFrames]++
	v[v2PendingRawCap] += int64(cap(job.raw.data))
	v[v2PendingDstCap] += job.workDstCap
}
func (o *v2WorkObserver) startJob(job *v2FrameEncodeJob) {
	o.mu.Lock()
	defer o.mu.Unlock()
	job.workStage = v2JobEncoding
	o.values[v2WaitingFrames]--
	o.values[v2WaitingRaw] -= int64(len(job.raw.data))
	o.values[v2WaitingRawCap] -= int64(cap(job.raw.data))
}
func (o *v2WorkObserver) ready(job *v2FrameEncodeJob, frame v2EncodedFrame) {
	o.mu.Lock()
	defer o.mu.Unlock()
	job.workStage = v2JobReady
	job.workCompressed = int64(len(frame.compressed))
	cap := int64(cap(frame.compressed))
	o.values[v2PendingDstCap] += cap - job.workDstCap
	job.workDstCap = cap
	o.values[v2ReadyFrames]++
	o.values[v2ReadyRaw] += int64(len(job.raw.data))
	o.values[v2ReadyCompressed] += job.workCompressed
	o.values[v2ReadyCompressedCap] += cap
}
func (o *v2WorkObserver) writing(job *v2FrameEncodeJob) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.removeReady(job)
	job.workStage = v2JobWriting
}
func (o *v2WorkObserver) removeReady(job *v2FrameEncodeJob) {
	o.values[v2ReadyFrames]--
	o.values[v2ReadyRaw] -= int64(len(job.raw.data))
	o.values[v2ReadyCompressed] -= job.workCompressed
	o.values[v2ReadyCompressedCap] -= job.workDstCap
}
func (o *v2WorkObserver) release(job *v2FrameEncodeJob, abandoned bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if job.workStage == v2JobReleased {
		return
	}
	if job.workStage == v2JobWaiting {
		o.values[v2WaitingFrames]--
		o.values[v2WaitingRaw] -= int64(len(job.raw.data))
		o.values[v2WaitingRawCap] -= int64(cap(job.raw.data))
	}
	if job.workStage == v2JobReady {
		o.removeReady(job)
	}
	o.values[v2PendingFrames]--
	o.values[v2PendingRawCap] -= int64(cap(job.raw.data))
	o.values[v2PendingDstCap] -= job.workDstCap
	if abandoned {
		o.values[v2AbandonedFrames]++
	}
	job.workStage = v2JobReleased
}
