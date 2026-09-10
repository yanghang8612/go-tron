package snapshots

import (
	"math"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

// These are scheduling targets, not a promise that a legal complete block or
// an in-flight filesystem operation can be interrupted at a precise boundary.
const (
	historyLoadRetry        = 5 * time.Second
	historyCPUBurstRecovery = 250 * time.Millisecond
)

type historyLoadReason uint16

// These bit values are exposed by history/budget/reason_bits. Keep existing
// assignments stable when adding reasons. Pressure and recovery reasons are
// separate: level 1 can persist after every instantaneous pressure has cleared.
const (
	historyLoadReasonHard historyLoadReason = 1 << iota
	historyLoadReasonNewStall
	historyLoadReasonDevicePressure
	historyLoadReasonL0Pressure
	historyLoadReasonDebtGrowth
	historyLoadReasonEngineUnavailable
	historyLoadReasonEngineStale
	historyLoadReasonRecoverySamples
	historyLoadReasonPressureHysteresis
	historyLoadReasonDeviceUnknown
)

var historyLoadReasonMetrics = [...]struct {
	bit  historyLoadReason
	name string
}{
	{historyLoadReasonHard, "hard"},
	{historyLoadReasonNewStall, "new_stall"},
	{historyLoadReasonDevicePressure, "device_pressure"},
	{historyLoadReasonL0Pressure, "l0_pressure"},
	{historyLoadReasonDebtGrowth, "debt_growth"},
	{historyLoadReasonEngineUnavailable, "engine_unavailable"},
	{historyLoadReasonEngineStale, "engine_stale"},
	{historyLoadReasonRecoverySamples, "recovery_samples"},
	{historyLoadReasonPressureHysteresis, "pressure_hysteresis"},
	{historyLoadReasonDeviceUnknown, "device_unknown"},
}

type historyWorkSample struct {
	blocks, txnums, bytes uint64
	work                  time.Duration
}

// All controller state is owned by Runner.passMu. Engine pressure protects
// writes; device observations include other services and are used only to
// reduce discretionary work, never to assert spare hardware bandwidth.
type historyLoadState struct {
	sample               maintenance.StoragePressure
	lastAccepted         time.Time
	acceptedSequence     uint64 // Accepted engine observations, including resets.
	syncSeenAt           time.Time
	lastDebt             uint64
	lastStalls           uint64
	debtRises            int
	good                 int
	level                int // 0 unknown, 1 pressured, 2 recovering, 3 healthy
	hard                 bool
	reasons              historyLoadReason
	history              historyWorkSample
	event                historyWorkSample
	historyFailureBlocks uint64
	historyFailureTxNums uint64
	eventFailureBlocks   uint64
	metrics              map[string]*metrics.Gauge
	reasonEntries        [len(historyLoadReasonMetrics)]*metrics.Counter
}

func (r *Runner) initHistoryLoadMetrics() {
	if r.throughputCatchup() && r.cfg.HistoryLoadProbe != nil {
		// Startup/peer handoffs must not masquerade as settled idle time and
		// immediately launch a full-keyspace job on a large existing database.
		r.historyLoad.syncSeenAt = time.Now()
	}
	r.historyLoad.metrics = make(map[string]*metrics.Gauge)
	for _, name := range []string{"level", "hard", "deferred", "duty_ppm", "cpu_burst", "block_limit", "txnum_limit", "recovery_cost", "density_work", "density_metadata_work", "density_total_work", "density_measurement", "device_known", "device_busy_ppm", "device_queue_milli", "device_await", "compaction_debt", "merge_input_bytes", "merge_input_logical_bytes", "merge_input_records", "merge_sources", "merge_recovery",
		"reason_bits", "l0_sublevels", "l0_compaction_threshold", "l0_stop_writes_threshold", "debt_rises", "good", "sample_accepted", "accepted_sequence", "accepted_sample_unix_nano"} {
		r.historyLoad.metrics[name] = metrics.GetOrRegisterGauge(strings.TrimRight(r.cfg.MetricsNamespace, "/")+"/history/budget/"+name, nil)
	}
	for i, reason := range historyLoadReasonMetrics {
		r.historyLoad.reasonEntries[i] = metrics.GetOrRegisterCounter(strings.TrimRight(r.cfg.MetricsNamespace, "/")+"/history/budget/reason/"+reason.name+"/entered", nil)
	}
}

func (s *historyLoadState) metric(name string, value int64) {
	if gauge := s.metrics[name]; gauge != nil {
		gauge.Update(value)
	}
}

func (s *historyLoadState) recordReasons(reasons historyLoadReason) {
	// Count activations, not refresh calls or elapsed time. Reusing an engine
	// observation cannot inflate a persistent reason's count, but pressure
	// arriving between accepted observations remains immediately observable.
	entered := reasons &^ s.reasons
	for i, reason := range historyLoadReasonMetrics {
		if entered&reason.bit != 0 && s.reasonEntries[i] != nil {
			s.reasonEntries[i].Inc(1)
		}
	}
	s.reasons = reasons
	s.metric("reason_bits", int64(reasons))
}

func freshHistoryLoad(sampled, now time.Time) bool {
	return !sampled.IsZero() && now.Sub(sampled) >= -time.Second && now.Sub(sampled) <= 15*time.Second
}

func (r *Runner) refreshHistoryLoad(now time.Time) {
	if r.syncActive() {
		r.historyLoad.syncSeenAt = now
	}
	if r.cfg.HistoryLoadProbe == nil {
		return
	}
	s := &r.historyLoad
	p := r.cfg.HistoryLoadProbe()
	s.sample = p
	previousHard := s.hard
	s.hard = p.HardLimitReached(now)
	previousLevel := s.level
	var reasons historyLoadReason
	accepted := false
	if !p.Available || !freshHistoryLoad(p.SampledAt, now) {
		s.level, s.good, s.debtRises = 0, 0, 0
		if !p.Available {
			reasons |= historyLoadReasonEngineUnavailable
		} else {
			// Includes missing timestamps and observations too far in the future.
			reasons |= historyLoadReasonEngineStale
		}
	} else {
		newSample := s.lastAccepted.IsZero() || p.SampledAt.Sub(s.lastAccepted) >= 5*time.Second
		reset := p.SampledAt.Before(s.lastAccepted) || p.StallCount < s.lastStalls
		if reset {
			s.good, s.debtRises, s.lastAccepted = 0, 0, time.Time{}
			newSample = true
		}
		newStall := !s.lastAccepted.IsZero() && p.StallCount > s.lastStalls
		if newSample {
			if !s.lastAccepted.IsZero() && p.CompactionDebt > s.lastDebt {
				s.debtRises++
			} else {
				s.debtRises = 0
			}
		}
		deviceKnown := p.DeviceAvailable && freshHistoryLoad(p.DeviceSampledAt, now)
		devicePressure := deviceKnown && p.DeviceBusyPPM >= 950_000 && p.DeviceQueueMilli >= 2_000 && p.DeviceAwait >= 5*time.Millisecond
		l0Pressure := p.L0CompactionThreshold > 0 && p.L0Sublevels >= p.L0CompactionThreshold && p.L0Sublevels-p.L0CompactionThreshold >= p.L0CompactionThreshold
		debtPressure := s.debtRises >= 2 && p.CompactionDebt >= 2<<30
		soft := s.hard || newStall || devicePressure || l0Pressure || debtPressure
		low := (p.L0CompactionThreshold <= 0 || p.L0Sublevels < p.L0CompactionThreshold) &&
			(s.lastAccepted.IsZero() || p.CompactionDebt <= s.lastDebt)
		if soft {
			s.level, s.good = 1, 0
			if s.hard {
				reasons |= historyLoadReasonHard
			}
			if newStall {
				reasons |= historyLoadReasonNewStall
			}
			if devicePressure {
				reasons |= historyLoadReasonDevicePressure
			}
			if l0Pressure {
				reasons |= historyLoadReasonL0Pressure
			}
			if debtPressure {
				reasons |= historyLoadReasonDebtGrowth
			}
		} else {
			if newSample {
				if low {
					s.good = min(3, s.good+1)
				} else {
					s.good = 0
				}
			}
			// Neither one good point nor an unavailable device permits the
			// largest online budget. Leave pressure gradually with hysteresis.
			s.level = 2
			if previousLevel == 1 && s.good < 2 {
				s.level = 1
				reasons |= historyLoadReasonPressureHysteresis
			}
			if s.good >= 3 && deviceKnown {
				s.level = 3
			}
			if s.good < 3 {
				reasons |= historyLoadReasonRecoverySamples
			}
			if !deviceKnown {
				reasons |= historyLoadReasonDeviceUnknown
			}
		}
		if newSample {
			s.lastAccepted, s.lastDebt, s.lastStalls = p.SampledAt, p.CompactionDebt, p.StallCount
			s.acceptedSequence++
			accepted = true
		}
	}
	s.recordReasons(reasons)
	s.metric("level", int64(s.level))
	s.metric("hard", boolGauge(s.hard))
	s.metric("duty_ppm", int64(s.dutyPPM()))
	s.metric("cpu_burst", boolGauge(s.cpuBurstReady(now)))
	s.metric("device_known", boolGauge(p.DeviceAvailable && freshHistoryLoad(p.DeviceSampledAt, now)))
	s.metric("device_busy_ppm", coldSnapshotUintGauge(p.DeviceBusyPPM))
	s.metric("device_queue_milli", coldSnapshotUintGauge(p.DeviceQueueMilli))
	s.metric("device_await", int64(p.DeviceAwait))
	s.metric("compaction_debt", coldSnapshotUintGauge(p.CompactionDebt))
	s.metric("l0_sublevels", int64(p.L0Sublevels))
	s.metric("l0_compaction_threshold", int64(p.L0CompactionThreshold))
	s.metric("l0_stop_writes_threshold", int64(p.L0StopWritesThreshold))
	s.metric("debt_rises", int64(s.debtRises))
	s.metric("good", int64(s.good))
	s.metric("sample_accepted", boolGauge(accepted))
	s.metric("accepted_sequence", coldSnapshotUintGauge(s.acceptedSequence))
	acceptedAt := int64(0)
	if !s.lastAccepted.IsZero() {
		acceptedAt = s.lastAccepted.UnixNano()
	}
	s.metric("accepted_sample_unix_nano", acceptedAt)
	if previousLevel != s.level || previousHard != s.hard {
		// Healthy/recovering oscillation is ordinary batch tuning. Pressure,
		// unknown observations and hard-limit edges remain visible at Info.
		logBudget := coldSnapshotLog.Debug
		if previousLevel < 2 || s.level < 2 || previousHard != s.hard {
			logBudget = coldSnapshotLog.Info
		}
		logBudget("History storage budget changed", "level", s.level, "hard", s.hard,
			"dutyPPM", s.dutyPPM(), "l0Sublevels", p.L0Sublevels, "compactionDebt", p.CompactionDebt,
			"deviceKnown", p.DeviceAvailable, "deviceQueueMilli", p.DeviceQueueMilli, "deviceAwait", p.DeviceAwait,
			"reasonBits", uint16(reasons), "debtRises", s.debtRises, "good", s.good,
			"acceptedSequence", s.acceptedSequence, "acceptedSample", s.lastAccepted)
	}
}

func boolGauge(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func (s *historyLoadState) dutyPPM() uint64 {
	if s.cpuBurstReady(time.Now()) {
		if s.level == 3 {
			return 900_000
		}
		return 800_000
	}
	switch s.level {
	case 3:
		return 600_000
	case 2:
		return 333_333
	default:
		return 200_000
	}
}

// A high device busy percentage alone does not mean an SSD is saturated.
// Sustained low latency plus fresh engine headroom permits CPU pipelines to
// keep working instead of sleeping for multiples of their complete wall time.
// This remains an admission target, not a guarantee about in-flight I/O.
func (s *historyLoadState) cpuBurstReady(now time.Time) bool {
	p := s.sample
	return s.level >= 2 && !s.hard && p.Available && freshHistoryLoad(p.SampledAt, now) &&
		!p.HardLimitReached(now) && p.DeviceAvailable && freshHistoryLoad(p.DeviceSampledAt, now) &&
		p.DeviceAwait >= 0 && p.DeviceAwait <= 2*time.Millisecond
}

// Preserve a deliberately long configured pause. The normal three-second
// catch-up floor predates parallel CPU work and must not dominate small jobs.
func (r *Runner) historyLeaseCooldown() time.Duration {
	cooldown := r.cfg.CatchupHeavyWorkCooldown
	if r.throughputCatchup() && cooldown > 0 && cooldown <= 3*time.Second && r.historyLoad.cpuBurstReady(time.Now()) {
		return min(cooldown, historyCPUBurstRecovery)
	}
	return cooldown
}

func (s *historyLoadState) targets() (time.Duration, uint64) {
	switch s.level {
	case 3:
		return 8 * time.Second, 128 << 20
	case 2:
		return 4 * time.Second, 64 << 20
	default:
		return 2 * time.Second, 32 << 20
	}
}

// Clamp a density estimate without overflowing even for malformed counters.
func historyScaledLimit(count uint64, ratio float64) uint64 {
	v := float64(count) * ratio
	if math.IsNaN(v) || v < 1 {
		return 1
	}
	if v >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(v)
}

func (s *historyLoadState) batchLimit(configured, previous uint64, sample historyWorkSample) uint64 {
	if configured == 0 {
		return 0
	}
	if sample.work <= 0 || previous == 0 {
		return max(uint64(1), configured/4)
	}
	targetWork, targetBytes := s.targets()
	ratio := float64(targetWork) / float64(sample.work)
	if sample.bytes > 0 {
		ratio = min(ratio, float64(targetBytes)/float64(sample.bytes))
	}
	// Measured cheap rows do not establish storage headroom. Unknown samples
	// may shrink a known batch but must not use excluded metadata to grow it.
	if s.level == 0 || s.hard {
		ratio = min(ratio, 1)
	}
	// Shrink immediately on a density jump; enlarge only 25% per completed
	// batch. The block and txnum bounds apply together before reading history.
	capacityRatio := ratio
	ratio = min(ratio, 1.25)
	limit := historyScaledLimit(previous, ratio)
	// Integer 1/2/3-block slices must be able to recover after density falls.
	// A one-unit exception to 25% growth is allowed only if measured capacity
	// can actually hold the extra unit. Never overflow a uint64 counter.
	if limit == previous && previous < math.MaxUint64 && capacityRatio >= float64(previous+1)/float64(previous) {
		limit++
	}
	return min(configured, limit)
}

func (r *Runner) adaptiveHistoryBatchLimits(blocks, txnums uint64) (uint64, uint64) {
	if !r.throughputCatchup() || !r.historySyncBudgetActive() || r.cfg.HistoryLoadProbe == nil {
		return blocks, txnums
	}
	s := &r.historyLoad
	blocks = s.batchLimit(blocks, s.history.blocks, s.history)
	txnums = s.batchLimit(txnums, s.history.txnums, s.history)
	if s.historyFailureBlocks > 0 {
		blocks = min(blocks, s.historyFailureBlocks)
	}
	if s.historyFailureTxNums > 0 {
		txnums = min(txnums, s.historyFailureTxNums)
	}
	s.metric("block_limit", coldSnapshotUintGauge(blocks))
	s.metric("txnum_limit", coldSnapshotUintGauge(txnums))
	return blocks, txnums
}

func (r *Runner) adaptiveEventBatchLimit(blocks uint64) uint64 {
	if !r.throughputCatchup() || !r.historySyncBudgetActive() || r.cfg.HistoryLoadProbe == nil {
		return blocks
	}
	// Do not let an independent freezer-dependent event gap start with a
	// 65,536-block burst. Its own observed density then controls later ranges.
	blocks = min(blocks, r.cfg.BatchBlocks)
	limit := r.historyLoad.batchLimit(blocks, r.historyLoad.event.blocks, r.historyLoad.event)
	if r.historyLoad.eventFailureBlocks > 0 {
		limit = min(limit, r.historyLoad.eventFailureBlocks)
	}
	return limit
}

func (r *Runner) historyLoadPermitsMerge() bool {
	r.refreshHistoryLoad(time.Now())
	return !r.historyLoad.hard && (r.cfg.HistoryLoadProbe == nil || r.historyLoad.level >= 2)
}

func (r *Runner) historySyncBudgetActive() bool {
	return r.syncActive() || r.throughputCatchup() && r.cfg.HistoryLoadProbe != nil &&
		!r.historyLoad.syncSeenAt.IsZero() && time.Since(r.historyLoad.syncSeenAt) < time.Minute
}

func (r *Runner) deferHistoryForLoad(result *PassResult) bool {
	if !r.historyLoad.hard {
		return false
	}
	result.HistoryDeferred, result.HistoryLoadDeferred = true, true
	result.extendHistoryRetryDeadline(time.Now(), historyLoadRetry)
	if gauge := r.historyLoad.metrics["deferred"]; gauge != nil {
		gauge.Inc(1)
	}
	return true
}

func (r *Runner) recordHistoryWork(result *PassResult) {
	if !result.HistoryBuildAttempted {
		return
	}
	if !result.Built {
		// Keep failed admission sizes separate from measured successful work.
		// Repeated attempts must shrink rather than retry the same oversized
		// range forever. A pre-admission rejection does not reach this path.
		r.historyLoad.historyFailureBlocks = max(uint64(1), result.HistoryBatchBlocks/2)
		r.historyLoad.historyFailureTxNums = max(uint64(1), result.HistoryBatchTxNums/2)
		return
	}
	r.historyLoad.historyFailureBlocks, r.historyLoad.historyFailureTxNums = 0, 0
	work, metadata, measurement := historyDensityWork(result)
	r.historyLoad.history = historyWorkSample{blocks: result.HistoryBatchBlocks, txnums: result.HistoryBatchTxNums,
		bytes: segmentRefsSize(result.Segments), work: work}
	r.historyLoad.metric("density_work", int64(work))
	r.historyLoad.metric("density_metadata_work", int64(metadata))
	r.historyLoad.metric("density_total_work", int64(historyWorkDurationSum(result.BuildDuration, result.BeforeMergeDuration)))
	r.historyLoad.metric("density_measurement", measurement)
}

func (r *Runner) recordEventWork(result *PassResult) {
	if !result.HistoryEventAttempted {
		return
	}
	if !result.DerivedSidecarCatchup {
		r.historyLoad.eventFailureBlocks = max(uint64(1), result.historyEventBatchBlocks/2)
		return
	}
	r.historyLoad.eventFailureBlocks = 0
	r.historyLoad.event = historyWorkSample{blocks: result.historyEventBatchBlocks,
		bytes: segmentRefsSize(result.Segments), work: result.DerivedSidecarDuration + result.BeforeMergeDuration}
}

func saturatingHistoryBudgetSum(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func (r *Runner) recordCompactionBudget(result HistoryCompactionResult) {
	if !result.Merged {
		return
	}
	s := &r.historyLoad
	s.metric("merge_input_bytes", coldSnapshotUintGauge(result.InputBytes))
	s.metric("merge_input_logical_bytes", coldSnapshotUintGauge(result.InputLogicalBytes))
	s.metric("merge_input_records", coldSnapshotUintGauge(result.InputRecords))
	s.metric("merge_sources", coldSnapshotUintGauge(result.InputSources))
	s.metric("merge_recovery", int64(result.Recovery))
	coldSnapshotLog.Info("History bounded merge completed", "inputBytes", result.InputBytes,
		"logicalBytes", result.InputLogicalBytes, "records", result.InputRecords,
		"sources", result.InputSources, "recovery", result.Recovery)
}

// The earliest independent wake runs another admission check. Waking for a
// merge never grants history permission before its own not-before deadline.
func (r PassResult) MaintenanceRetryRemaining(now time.Time) time.Duration {
	after := r.HistoryRetryRemaining(now)
	if !r.Compaction.RetryDeadline.IsZero() {
		mergeAfter := max(time.Nanosecond, r.Compaction.RetryDeadline.Sub(now))
		if after <= 0 || mergeAfter < after {
			after = mergeAfter
		}
	}
	return after
}
