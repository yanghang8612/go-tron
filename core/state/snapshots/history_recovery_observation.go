package snapshots

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

func (r *Runner) historyRecoveryObservationEnabled() bool {
	return r.cfg.Enabled && r.throughputCatchup() && r.cfg.HistoryLoadProbe != nil &&
		r.cfg.HistoryRecoveryLoadProbe != nil
}

// A completed recovery can finish earlier after fresh, comparable observations
// confirm the higher sustained duty. The original deadline remains installed
// until the recalculated time has actually elapsed.
type historyRecoveryWindow struct {
	forced          bool
	completed       time.Time
	cost            time.Duration
	deadline, floor int64
	good            int
}

// Recovery observation does not replace the busy-resource early-wake mode.
// Called under passMu only after a successful inner pass; outer failure cancels
// its generation and pending outer completion prevents observation.
func (r *Runner) armHistoryRecoveryObservation(result *PassResult, now time.Time) {
	if result.HistoryBusyResourceDeferred || !r.historyRecoveryObservationEnabled() ||
		!r.cfg.DeferHistoryBuildWhileSyncing || r.historyNotBefore.Load() <= now.UnixNano() {
		return
	}
	source, ok := r.chain.(syncRemainingSource)
	if !ok {
		return
	}
	if remaining, active := source.SyncRemainingBlocks(); !active || remaining <= r.cfg.HistoryWindow {
		return
	}
	r.armHistoryObservation(result, now, true)
	r.recoveryWindow.good = 0
	result.HistoryRecoveryObservation = true
}

// Called with passMu held, a live generation, no pending outer completion and
// the original deadline still in the future.
func (r *Runner) observeHistoryRecoveryResources(ctx context.Context, id uint64, now time.Time) (bool, time.Duration) {
	r.historyRecoveryMetrics.checks.Inc(1)
	defer func() {
		r.nextBusyHistoryObservation = time.Now().Add(BusyHistoryObservationInterval)
		r.historyRecoveryMetrics.duration.Update(int64(coldSnapshotPhaseDuration(now)))
	}()
	p := r.cfg.HistoryRecoveryLoadProbe()
	if ctx != nil && ctx.Err() != nil || r.ctx != nil && r.ctx.Err() != nil {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	if r.busyHistoryObservationPending.Load() != id {
		return false, 0
	}
	checkedAt := time.Now()
	sequence := r.historyLoad.acceptedSequence
	previousAt, previousStalls := r.historyLoad.lastAccepted, r.historyLoad.lastStalls
	previousDevice := r.historyLoad.sample.DeviceSampledAt
	// The dedicated production probe has already read engine metadata and a
	// cached device snapshot; do not fall through to the ordinary I/O probe.
	r.refreshHistoryLoadFromProbe(checkedAt, func() maintenance.StoragePressure { return p })
	accepted := r.historyLoad.acceptedSequence != sequence
	if accepted {
		r.historyRecoveryMetrics.accepted.Inc(1)
	}
	w := &r.recoveryWindow
	if w.deadline > checkedAt.UnixNano() && r.historyNotBefore.Load() == w.deadline {
		healthy := r.historyLoad.level >= 2 && r.historyLoad.cpuBurstReady(checkedAt) &&
			r.historyLoad.reasons&^historyLoadReasonRecoverySamples == 0 &&
			p.L0CompactionThreshold > 0 && p.L0Sublevels < p.L0CompactionThreshold
		comparable := !previousAt.IsZero() && p.SampledAt.After(previousAt) &&
			p.SampledAt.Sub(previousAt) <= historyDebtTrendMaxGap && p.StallCount == previousStalls
		switch {
		case !healthy || accepted && !comparable:
			w.good = 0
		case accepted && p.SampledAt.After(w.completed) && p.DeviceSampledAt.After(w.completed) &&
			p.DeviceSampledAt.After(previousDevice):
			w.good = min(2, w.good+1)
			earlier := max(w.floor, historyRecoveryDeadline(w.completed, r.throughputRecoveryAt(w.cost, false, checkedAt)).UnixNano())
			// Consume the observation before changing admission. Cancellation
			// or a newer generation can win this race without an early wake.
			if w.good >= 2 && r.historyLoad.level == 3 && earlier <= checkedAt.UnixNano() &&
				r.cfg.HeavyWorkGate.CooldownRemaining() <= 0 &&
				r.busyHistoryObservationPending.CompareAndSwap(id, 0) &&
				r.historyNotBefore.CompareAndSwap(w.deadline, earlier) {
				// The forced-busy fallback derives another deadline from
				// the same completion. Keep it in step without changing a
				// different attempt's cooldown or the recorded work metrics.
				if w.forced && r.lastForcedBusyAttemptAt.Load() == w.completed.UnixNano() {
					r.lastForcedAttemptRecovery.Store(earlier - w.completed.UnixNano())
				}
				r.historyRecoveryMetrics.wakeups.Inc(1)
				return true, 0
			}
		}
	}
	if r.historyNotBefore.Load() <= checkedAt.UnixNano() {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	return false, BusyHistoryObservationInterval
}

type historyRecoveryObservationMetrics struct {
	checks, accepted, wakeups *metrics.Counter
	duration                  *metrics.Gauge
}

func newHistoryRecoveryObservationMetrics(namespace string) historyRecoveryObservationMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace) + "history/recovery_observation/"
	return historyRecoveryObservationMetrics{
		checks:   metrics.GetOrRegisterCounter(namespace+"checks", nil),
		accepted: metrics.GetOrRegisterCounter(namespace+"accepted_samples", nil),
		wakeups:  metrics.GetOrRegisterCounter(namespace+"wakeups", nil),
		duration: metrics.GetOrRegisterGauge(namespace+"last_duration", nil),
	}
}
