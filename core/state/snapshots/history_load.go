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
	historyLoadRetry          = 5 * time.Second
	historyCPUBurstRecovery   = 250 * time.Millisecond
	historyDebtTrendPoints    = 7
	historyDebtTrendSpan      = 30 * time.Second
	historyDebtTrendMaxGap    = 90 * time.Second
	historyDebtGrowthMinBytes = 256 << 20
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

type historyDebtPoint struct {
	at   time.Time
	debt uint64
}

type historyDebtWindow struct {
	points   int
	span     time.Duration
	net      int64
	positive int
}

type historyDebtTrend struct {
	recent   historyDebtWindow
	growth   historyDebtWindow
	mature   bool
	pressure bool
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
	debtTrend            [historyDebtTrendPoints]historyDebtPoint
	debtTrendCount       int
	good                 int
	level                int  // 0 unknown, 1 pressured, 2 recovering, 3 healthy
	debtModerate         bool // Fresh healthy device evidence permits the debt-only middle tier.
	hard                 bool
	reasons              historyLoadReason
	history              historyWorkSample
	event                historyWorkSample
	historyFailureBlocks uint64
	historyFailureTxNums uint64
	eventFailureBlocks   uint64
	metrics              map[string]*metrics.Gauge
	reasonEntries        [len(historyLoadReasonMetrics)]*metrics.Counter
	middleHistoryTargets *metrics.Counter
	middleEventTargets   *metrics.Counter
}

func (r *Runner) initHistoryLoadMetrics() {
	if r.throughputCatchup() && r.cfg.HistoryLoadProbe != nil {
		// Startup/peer handoffs must not masquerade as settled idle time and
		// immediately launch a full-keyspace job on a large existing database.
		r.historyLoad.syncSeenAt = time.Now()
	}
	// Counts range decisions receiving the larger middle target, including work
	// later rejected by a lease/deadline. These are not completed job counters.
	namespace := strings.TrimRight(r.cfg.MetricsNamespace, "/") + "/history/budget/"
	r.historyLoad.middleHistoryTargets = metrics.GetOrRegisterCounter(namespace+"middle/history/selected", nil)
	r.historyLoad.middleEventTargets = metrics.GetOrRegisterCounter(namespace+"middle/event/selected", nil)
	r.historyLoad.metrics = make(map[string]*metrics.Gauge)
	for _, name := range []string{"level", "hard", "deferred", "duty_ppm", "cpu_burst", "block_limit", "txnum_limit", "recovery_cost", "density_work", "density_metadata_work", "density_gc_work", "density_total_work", "density_measurement", "device_known", "device_busy_ppm", "device_queue_milli", "device_await", "compaction_debt", "merge_input_bytes", "merge_input_logical_bytes", "merge_input_records", "merge_sources", "merge_recovery",
		"reason_bits", "debt_tier", "debt_recovery", "l0_sublevels", "l0_compaction_threshold", "l0_stop_writes_threshold", "debt_rises", "debt_trend_points", "debt_trend_span_ns", "debt_trend_net_bytes", "debt_trend_positive_steps", "debt_trend_growth_points", "debt_trend_growth_span_ns", "debt_trend_growth_net_bytes", "debt_trend_growth_positive_steps", "good", "sample_accepted", "accepted_sequence", "accepted_sample_unix_nano"} {
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

func (s *historyLoadState) appendDebtTrend(at time.Time, debt uint64) {
	if s.debtTrendCount == len(s.debtTrend) {
		copy(s.debtTrend[:], s.debtTrend[1:])
		s.debtTrendCount--
	}
	s.debtTrend[s.debtTrendCount] = historyDebtPoint{at: at, debt: debt}
	s.debtTrendCount++
}

func (s *historyLoadState) debtWindow(anchor int) historyDebtWindow {
	first := s.debtTrend[anchor]
	last := s.debtTrend[s.debtTrendCount-1]
	w := historyDebtWindow{points: s.debtTrendCount - anchor, span: last.at.Sub(first.at)}
	if last.debt >= first.debt {
		w.net = coldSnapshotUintGauge(last.debt - first.debt)
	} else {
		w.net = -coldSnapshotUintGauge(first.debt - last.debt)
	}
	for i := anchor + 1; i < s.debtTrendCount; i++ {
		if s.debtTrend[i].debt > s.debtTrend[i-1].debt {
			w.positive++
		}
	}
	return w
}

// First establish the direction of the newest geometrically mature suffix.
// Only a positive recent direction may extend backwards to find a second
// independent rise. A distant high cannot hide new growth after a drop, and
// an old rise cannot keep pressure active after the recent window goes flat.
func (s *historyLoadState) debtTrendPressure() historyDebtTrend {
	if s.debtTrendCount == 0 {
		return historyDebtTrend{}
	}
	trend := historyDebtTrend{recent: s.debtWindow(0)}
	last := s.debtTrend[s.debtTrendCount-1]
	for i := s.debtTrendCount - 3; i >= 0; i-- {
		if last.at.Sub(s.debtTrend[i].at) >= historyDebtTrendSpan {
			trend.recent, trend.mature = s.debtWindow(i), true
			break
		}
	}
	if !trend.mature || !s.debtGrowthSignificant(trend.recent) || last.debt < 2<<30 {
		return trend
	}
	for i := s.debtTrendCount - trend.recent.points; i >= 0; i-- {
		candidate := s.debtWindow(i)
		if candidate.positive >= 2 && candidate.net > 0 {
			trend.growth, trend.pressure = candidate, true
			break
		}
	}
	return trend
}

// Small positive compaction sawteeth alone do not establish saturation.
// The relative floor scales to large stores; this does not assert
// spare disk bandwidth or replace independent hard/L0/stall/device guards.
func (s *historyLoadState) debtGrowthSignificant(w historyDebtWindow) bool {
	if w.points == 0 || w.net <= 0 {
		return false
	}
	baseline := s.debtTrend[s.debtTrendCount-w.points].debt
	threshold := max(uint64(historyDebtGrowthMinBytes), baseline/100)
	return uint64(w.net) >= threshold
}

// Severity is scheduling policy, not evidence that the device is saturated.
// Compare only the recent window; an extended suffix cannot amplify it.
func (s *historyLoadState) debtGrowthSevere(w historyDebtWindow) bool {
	if w.points == 0 || w.net <= 0 {
		return false
	}
	threshold := max(uint64(historyDebtGrowthMinBytes), s.debtTrend[s.debtTrendCount-w.points].debt/100)
	if threshold > ^uint64(0)/4 {
		return false
	}
	return uint64(w.net) >= threshold*4
}

func (r *Runner) refreshHistoryLoad(now time.Time) {
	r.refreshHistoryLoadFromProbe(now, r.cfg.HistoryLoadProbe)
}

func (r *Runner) refreshHistoryLoadFromProbe(now time.Time, probe func() maintenance.StoragePressure) {
	if r.syncActive() {
		r.historyLoad.syncSeenAt = now
	}
	if probe == nil {
		return
	}
	s := &r.historyLoad
	p := probe()
	s.sample = p
	previousModerate := s.debtModerate
	s.debtModerate = false
	previousHard := s.hard
	s.hard = p.HardLimitReached(now)
	previousLevel := s.level
	var reasons historyLoadReason
	accepted := false
	if !p.Available || !freshHistoryLoad(p.SampledAt, now) {
		s.level, s.good, s.debtRises, s.debtTrendCount = 0, 0, 0, 0
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
			s.good, s.debtRises, s.debtTrendCount, s.lastAccepted = 0, 0, 0, time.Time{}
			newSample = true
		}
		newStall := !s.lastAccepted.IsZero() && p.StallCount > s.lastStalls
		deviceKnown := p.DeviceAvailable && freshHistoryLoad(p.DeviceSampledAt, now)
		if newSample {
			comparable := !s.lastAccepted.IsZero() && p.SampledAt.Sub(s.lastAccepted) <= historyDebtTrendMaxGap
			if !comparable {
				s.debtTrendCount = 0
			}
			if comparable && p.CompactionDebt > s.lastDebt {
				s.debtRises++
			} else {
				s.debtRises = 0
			}
			s.appendDebtTrend(p.SampledAt, p.CompactionDebt)
		}
		devicePressure := deviceKnown && p.DeviceBusyPPM >= 950_000 && p.DeviceQueueMilli >= 2_000 && p.DeviceAwait >= 5*time.Millisecond
		l0Pressure := p.L0CompactionThreshold > 0 && p.L0Sublevels >= p.L0CompactionThreshold && p.L0Sublevels-p.L0CompactionThreshold >= p.L0CompactionThreshold
		trend := s.debtTrendPressure()
		debtPressure := trend.pressure
		soft := s.hard || newStall || devicePressure || l0Pressure || debtPressure
		middleEligible := !reset && !s.debtGrowthSevere(trend.recent) &&
			!s.hard && !newStall && !devicePressure && !l0Pressure &&
			p.L0CompactionThreshold > 0 && p.L0Sublevels < p.L0CompactionThreshold &&
			deviceKnown && p.DeviceAwait >= 0 && p.DeviceAwait <= 2*time.Millisecond
		s.debtModerate = (newSample || previousModerate) && debtPressure && middleEligible
		// Mature falling or sub-deadband debt can support recovery despite
		// individual rises. Significant recent growth still resets good samples.
		trendQuiet := trend.mature && !s.debtGrowthSignificant(trend.recent)
		low := (p.L0CompactionThreshold <= 0 || p.L0Sublevels < p.L0CompactionThreshold) &&
			(s.lastAccepted.IsZero() || p.CompactionDebt <= s.lastDebt || trendQuiet)
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
				// Debt-only pressure already admitted 60% duty. Its first healthy
				// recovery point must not perversely drop to 20% while waiting for
				// the second good sample. Carry only existing, comparable admission;
				// lost/reset/stale protection cannot be reacquired by reusing a probe.
				s.debtModerate = previousModerate && middleEligible &&
					!s.lastAccepted.IsZero() && p.SampledAt.Sub(s.lastAccepted) <= historyDebtTrendMaxGap
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
	debtTier := int64(0)
	if reasons&historyLoadReasonDebtGrowth != 0 {
		debtTier = 2
		if s.debtModerateReady(now) {
			debtTier = 1
		}
	}
	s.metric("debt_tier", debtTier)
	s.metric("debt_recovery", boolGauge(reasons&historyLoadReasonPressureHysteresis != 0 && s.debtModerateReady(now)))
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
	trend := s.debtTrendPressure()
	s.metric("debt_trend_points", int64(trend.recent.points))
	s.metric("debt_trend_span_ns", int64(trend.recent.span))
	s.metric("debt_trend_net_bytes", trend.recent.net)
	s.metric("debt_trend_positive_steps", int64(trend.recent.positive))
	s.metric("debt_trend_growth_points", int64(trend.growth.points))
	s.metric("debt_trend_growth_span_ns", int64(trend.growth.span))
	s.metric("debt_trend_growth_net_bytes", trend.growth.net)
	s.metric("debt_trend_growth_positive_steps", int64(trend.growth.positive))
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
	if s.debtModerateReady(time.Now()) {
		return 600_000
	}
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
	return (s.level >= 2 || s.debtModerateReady(now)) && !s.hard && p.Available && freshHistoryLoad(p.SampledAt, now) &&
		!p.HardLimitReached(now) && p.DeviceAvailable && freshHistoryLoad(p.DeviceSampledAt, now) &&
		p.DeviceAwait >= 0 && p.DeviceAwait <= 2*time.Millisecond
}

// Recheck freshness at use time: a previously healthy observation never grants
// the middle tier after engine/device evidence expires.
func (s *historyLoadState) debtModerateReady(now time.Time) bool {
	p := s.sample
	return s.debtModerate && s.level == 1 && !s.hard && p.Available && freshHistoryLoad(p.SampledAt, now) &&
		!p.HardLimitReached(now) && p.DeviceAvailable && freshHistoryLoad(p.DeviceSampledAt, now) &&
		p.DeviceAwait >= 0 && p.DeviceAwait <= 2*time.Millisecond &&
		p.L0CompactionThreshold > 0 && p.L0Sublevels < p.L0CompactionThreshold
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
	targetWork, targetBytes := s.targets()
	return s.batchLimitWithTargets(configured, previous, sample, targetWork, targetBytes)
}

func (s *historyLoadState) batchLimitWithTargets(configured, previous uint64, sample historyWorkSample, targetWork time.Duration, targetBytes uint64) uint64 {
	if configured == 0 {
		return 0
	}
	if sample.work <= 0 || previous == 0 {
		return max(uint64(1), configured/4)
	}
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

// Middle debt already permits 60% duty, but a larger single batch additionally
// needs the node's current independent CPU/runtime/memory admission. Missing or
// rejected resources retain the original liveness target, rather than rejecting
// history altogether. This is a scheduling target, not a memory reservation.
func (r *Runner) historyBatchTargets(now time.Time) (time.Duration, uint64) {
	if r.cfg.HeavyWorkGate != nil && r.historyLoad.debtModerateReady(now) && r.cfg.BusyHistoryBuildReady != nil && r.cfg.BusyHistoryBuildReady() && r.historyLoad.debtModerateReady(time.Now()) {
		return 4 * time.Second, 64 << 20
	}
	return r.historyLoad.targets()
}

func (r *Runner) adaptiveHistoryBatchLimits(blocks, txnums uint64) (uint64, uint64) {
	if !r.throughputCatchup() || !r.historySyncBudgetActive() || r.cfg.HistoryLoadProbe == nil {
		return blocks, txnums
	}
	s := &r.historyLoad
	targetWork, targetBytes := r.historyBatchTargets(time.Now())
	if s.level == 1 && targetWork == 4*time.Second && targetBytes == 64<<20 && s.middleHistoryTargets != nil {
		s.middleHistoryTargets.Inc(1)
	}
	blocks = s.batchLimitWithTargets(blocks, s.history.blocks, s.history, targetWork, targetBytes)
	txnums = s.batchLimitWithTargets(txnums, s.history.txnums, s.history, targetWork, targetBytes)
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
	targetWork, targetBytes := r.historyBatchTargets(time.Now())
	if r.historyLoad.level == 1 && targetWork == 4*time.Second && targetBytes == 64<<20 && r.historyLoad.middleEventTargets != nil {
		r.historyLoad.middleEventTargets.Inc(1)
	}
	limit := r.historyLoad.batchLimitWithTargets(blocks, r.historyLoad.event.blocks, r.historyLoad.event, targetWork, targetBytes)
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
	gcWork := time.Duration(0)
	if measurement == 1 {
		gcWork = result.BeforeMergeHistoryGCDuration
	}
	r.historyLoad.metric("density_gc_work", int64(gcWork))
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
