package freezer

import (
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

const (
	transactionIndexPressureRecheck = 15 * time.Second
	transactionIndexHardRecheck     = 30 * time.Second
	transactionIndexSampleSpacing   = 5 * time.Second
)

// Numeric values are exported by txindex/budget/{admission,completion}/reason.
// Keep these stable so a retained metrics sample can be explained later.
type transactionIndexHealthReason int64

const (
	txIndexHealthUnobserved       transactionIndexHealthReason = iota // 0: no probe evaluated
	txIndexHealthHealthy                                              // 1: healthy, including recovery evidence
	txIndexHealthEngineUnknown                                        // 2: unavailable engine
	txIndexHealthEngineStale                                          // 3: stale/future engine sample
	txIndexHealthDeviceUnknown                                        // 4: unavailable device or invalid await
	txIndexHealthDeviceStale                                          // 5: stale/future device sample
	txIndexHealthWriteStalled                                         // 6: writes currently stalled (hard)
	txIndexHealthMemTableHard                                         // 7: near memtable stop threshold
	txIndexHealthL0Hard                                               // 8: near L0 stop threshold
	txIndexHealthNewStall                                             // 9: recently observed stall counter growth
	txIndexHealthL0Soft                                               // 10: twice the L0 compaction threshold
	txIndexHealthDebtGrowth                                           // 11: repeated debt growth at/above 2 GiB
	txIndexHealthDeviceLatency                                        // 12: device await exceeds 2 ms
	txIndexHealthRecovering                                           // 13: waiting for two distinct healthy observations
	txIndexHealthWorkError                                            // 14: the admitted batch failed/canceled
	txIndexHealthCandidateUnknown                                     // 15: cannot establish bounded work eligibility
)

func (reason transactionIndexHealthReason) unknown() bool {
	return reason >= txIndexHealthEngineUnknown && reason <= txIndexHealthDeviceStale
}

func (reason transactionIndexHealthReason) hard() bool {
	return reason >= txIndexHealthWriteStalled && reason <= txIndexHealthL0Hard
}

func (reason transactionIndexHealthReason) soft() bool {
	return reason >= txIndexHealthNewStall && reason <= txIndexHealthRecovering
}

type transactionIndexPressureState struct {
	lastTrendSample time.Time
	lastDebt        uint64
	debtRises       int
	lastSeenSample  time.Time
	lastStalls      uint64
	stallObservedAt time.Time
	recovering      bool
	goodSamples     int
	goodEngine      time.Time
	goodDevice      time.Time
}

// assess does not count repeated observations as new evidence. Engine trends
// are sampled at most once per five seconds; recovery also requires a newer
// device observation, rather than two calls sharing one cached disk sample.
func (s *transactionIndexPressureState) assess(p maintenance.StoragePressure, now time.Time) transactionIndexHealthReason {
	reason := s.classify(p, now)
	if reason != txIndexHealthHealthy {
		s.recovering = true
		s.goodSamples = 0
		s.goodEngine, s.goodDevice = time.Time{}, time.Time{}
		return reason
	}
	if !s.recovering {
		return reason
	}
	if s.goodSamples == 0 || p.SampledAt.Sub(s.goodEngine) >= transactionIndexSampleSpacing && p.DeviceSampledAt.After(s.goodDevice) {
		s.goodSamples++
		s.goodEngine, s.goodDevice = p.SampledAt, p.DeviceSampledAt
	}
	if s.goodSamples < 2 {
		return txIndexHealthRecovering
	}
	s.recovering = false
	return txIndexHealthHealthy
}

func (s *transactionIndexPressureState) classify(p maintenance.StoragePressure, now time.Time) transactionIndexHealthReason {
	if !p.Available {
		return txIndexHealthEngineUnknown
	}
	if !freshTransactionIndexSample(p.SampledAt, now) {
		return txIndexHealthEngineStale
	}
	// A reset of the clock or cumulative counters invalidates trend evidence.
	// It must not count as the second healthy recovery observation.
	if p.SampledAt.Before(s.lastSeenSample) || p.StallCount < s.lastStalls {
		*s = transactionIndexPressureState{recovering: true}
	}
	if p.SampledAt.After(s.lastSeenSample) {
		if !s.lastSeenSample.IsZero() && p.StallCount > s.lastStalls {
			s.stallObservedAt = p.SampledAt
		}
		s.lastSeenSample, s.lastStalls = p.SampledAt, p.StallCount
	}
	if s.lastTrendSample.IsZero() || p.SampledAt.Sub(s.lastTrendSample) >= transactionIndexSampleSpacing {
		if !s.lastTrendSample.IsZero() && p.CompactionDebt > s.lastDebt {
			s.debtRises = min(2, s.debtRises+1)
		} else {
			s.debtRises = 0
		}
		s.lastTrendSample, s.lastDebt = p.SampledAt, p.CompactionDebt
	}
	// Preserve the shared gate's hard-limit semantics, including thresholds
	// with their configured headroom, before considering missing device data.
	if p.HardLimitReached(now) {
		if p.WriteStalled {
			return txIndexHealthWriteStalled
		}
		if p.MemTableStopWritesThreshold > 0 && p.MemTableCount >= int64(max(1, p.MemTableStopWritesThreshold-1)) {
			return txIndexHealthMemTableHard
		}
		return txIndexHealthL0Hard
	}
	if !p.DeviceAvailable || p.DeviceAwait < 0 {
		return txIndexHealthDeviceUnknown
	}
	if !freshTransactionIndexSample(p.DeviceSampledAt, now) {
		return txIndexHealthDeviceStale
	}
	if !s.stallObservedAt.IsZero() && p.SampledAt.Sub(s.stallObservedAt) < transactionIndexSampleSpacing {
		return txIndexHealthNewStall
	}
	if p.L0CompactionThreshold > 0 && p.L0Sublevels >= p.L0CompactionThreshold && p.L0Sublevels-p.L0CompactionThreshold >= p.L0CompactionThreshold {
		return txIndexHealthL0Soft
	}
	if s.debtRises >= 2 && p.CompactionDebt >= 2<<30 {
		return txIndexHealthDebtGrowth
	}
	if p.DeviceAwait > 2*time.Millisecond {
		return txIndexHealthDeviceLatency
	}
	return txIndexHealthHealthy
}

// wait/reason values describe the next scheduled attempt, not a gate lease.
type transactionIndexWaitReason int64

const (
	txIndexWaitNone         transactionIndexWaitReason = iota // 0: no scheduled attempt
	txIndexWaitWork                                           // 1: complete-work duty recovery
	txIndexWaitSoft                                           // 2: soft pressure / stable evidence recheck
	txIndexWaitConservative                                   // 3: unknown observation / conservative interval
	txIndexWaitHard                                           // 4: hard pressure recheck
	txIndexWaitError                                          // 5: actual batch error backoff
	txIndexWaitStartup                                        // 6: configured startup delay
	txIndexWaitGate                                           // 7: shared gate active/cooling/reserved
)

func transactionIndexTimestamp(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func (r *Runner) recordTransactionIndexHealth(stage string, reason transactionIndexHealthReason, p maintenance.StoragePressure, now time.Time) {
	r.transactionIndexBudgetMetric(stage+"/healthy", boolGaugeValue(reason == txIndexHealthHealthy))
	r.transactionIndexBudgetMetric(stage+"/reason", int64(reason))
	r.transactionIndexBudgetMetric(stage+"/checked_at_unix_nano", now.UnixNano())
	r.transactionIndexBudgetMetric(stage+"/engine_sampled_at_unix_nano", transactionIndexTimestamp(p.SampledAt))
	r.transactionIndexBudgetMetric(stage+"/device_sampled_at_unix_nano", transactionIndexTimestamp(p.DeviceSampledAt))
	if stage == "admission" {
		// Compatibility gauge: this is explicitly the last admission check.
		r.transactionIndexBudgetMetric("healthy", boolGaugeValue(reason == txIndexHealthHealthy))
	}
}

// Remaining is a value at sampled_at, not a perpetually running countdown.
// The absolute deadline lets observers calculate the remaining wait at read
// time even while this runner is inside another long maintenance job.
func (r *Runner) recordTransactionIndexWait(now time.Time, reason transactionIndexWaitReason) {
	b := &r.txIndexBudget
	r.transactionIndexBudgetMetric("wait/reason", int64(reason))
	r.transactionIndexBudgetMetric("wait/deadline_unix_nano", transactionIndexTimestamp(b.retry))
	r.transactionIndexBudgetMetric("wait/remaining_at_sample", int64(max(0, b.retry.Sub(now))))
	r.transactionIndexBudgetMetric("wait/sampled_at_unix_nano", now.UnixNano())
	r.transactionIndexBudgetMetric("work_not_before_unix_nano", transactionIndexTimestamp(b.notBefore))
	r.transactionIndexBudgetMetric("conservative_until_unix_nano", transactionIndexTimestamp(b.conservativeUntil))
	r.transactionIndexBudgetMetric("pressure_recheck_unix_nano", transactionIndexTimestamp(b.pressureRecheck))
	failed := r.lastTxIndexMaintenanceError.Load()
	var deadline time.Time
	if failed > 0 && r.cfg.HeavyMaintenanceErrorBackoff > 0 {
		deadline = time.Unix(0, failed).Add(r.cfg.HeavyMaintenanceErrorBackoff)
	}
	r.transactionIndexBudgetMetric("error_not_before_unix_nano", transactionIndexTimestamp(deadline))
}
