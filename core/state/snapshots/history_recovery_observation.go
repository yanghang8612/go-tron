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

// The existing deadline is the only opportunity this mode observes. It neither
// installs recovery nor replaces the original busy-resource early-wake mode.
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
	result.HistoryRecoveryObservation = true
}

// Called with passMu held, a live generation, no pending outer completion and
// the original deadline still in the future. Returning only a retry duration
// makes the recovery branch incapable of asking the lifecycle for a full pass.
func (r *Runner) observeHistoryRecoveryResources(ctx context.Context, id uint64, now time.Time) time.Duration {
	r.historyRecoveryMetrics.checks.Inc(1)
	defer func() {
		r.nextBusyHistoryObservation = time.Now().Add(BusyHistoryObservationInterval)
		r.historyRecoveryMetrics.duration.Update(int64(coldSnapshotPhaseDuration(now)))
	}()
	p := r.cfg.HistoryRecoveryLoadProbe()
	if ctx != nil && ctx.Err() != nil || r.ctx != nil && r.ctx.Err() != nil {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return 0
	}
	if r.busyHistoryObservationPending.Load() != id {
		return 0
	}
	sequence := r.historyLoad.acceptedSequence
	// The dedicated production probe has already read engine metadata and a
	// cached device snapshot; do not fall through to the ordinary I/O probe.
	r.refreshHistoryLoadFromProbe(now, func() maintenance.StoragePressure { return p })
	if r.historyLoad.acceptedSequence != sequence {
		r.historyRecoveryMetrics.accepted.Inc(1)
	}
	if r.historyNotBefore.Load() <= time.Now().UnixNano() {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return 0
	}
	return BusyHistoryObservationInterval
}

type historyRecoveryObservationMetrics struct {
	checks, accepted *metrics.Counter
	duration         *metrics.Gauge
}

func newHistoryRecoveryObservationMetrics(namespace string) historyRecoveryObservationMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace) + "history/recovery_observation/"
	return historyRecoveryObservationMetrics{
		checks:   metrics.GetOrRegisterCounter(namespace+"checks", nil),
		accepted: metrics.GetOrRegisterCounter(namespace+"accepted_samples", nil),
		duration: metrics.GetOrRegisterGauge(namespace+"last_duration", nil),
	}
}
