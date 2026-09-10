package snapshots

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// BusyHistoryObservationInterval bounds resource rechecks independently of
// maintenance admission and its complete-work/failure recovery deadlines.
const BusyHistoryObservationInterval = 5 * time.Second

func (r *Runner) busyHistoryObservationEnabled() bool {
	return r.throughputCatchup() && r.cfg.HeavyWorkGate != nil &&
		r.cfg.HistoryLoadProbe != nil && r.cfg.BusyHistoryBuildReady != nil
}

// The caller has already established active deep sync and soft < lag <= busy.
// Resource headroom only opens this earlier opportunity; the existing range,
// deadline, shared lease and complete-maintenance recovery checks still apply.
// In particular cpuBurstReady describes storage, so it cannot replace the
// separate CPU/runtime/memory observation supplied by the node.
func (r *Runner) busyHistoryBuildReady(now time.Time) bool {
	return r.busyHistoryObservationEnabled() && r.historyLoad.cpuBurstReady(now) && r.cfg.BusyHistoryBuildReady()
}

// Called only under passMu after the complete soft/busy eligibility check.
func (r *Runner) armBusyHistoryObservation(result *PassResult, now time.Time) {
	if !r.busyHistoryObservationEnabled() {
		return
	}
	r.busyHistoryObservationSerial++
	if r.busyHistoryObservationSerial == 0 {
		r.busyHistoryObservationSerial++
	}
	result.historyBusyObservationID = r.busyHistoryObservationSerial
	result.HistoryBusyResourceDeferred = true
	r.nextBusyHistoryObservation = now.Add(BusyHistoryObservationInterval)
	r.busyHistoryObservationPending.Store(result.historyBusyObservationID)
}

// CancelBusyHistoryObservation invalidates a resource observation opportunity.
// Full lifecycle callers must cancel before preflight and on outer failure,
// including failures which occur before the Runner itself is entered.
func (r *Runner) CancelBusyHistoryObservation() {
	if r != nil {
		r.busyHistoryObservationPending.Store(0)
	}
}

func (r *Runner) cancelResultBusyHistoryObservation(result *PassResult) {
	if result.historyBusyObservationID != 0 {
		r.busyHistoryObservationPending.CompareAndSwap(result.historyBusyObservationID, 0)
		result.HistoryBusyResourceDeferred = false
	}
}

// ObserveBusyHistoryResources only refreshes bounded load/resource observations.
// It never acquires a maintenance lease, reads history/manifest/catalog files,
// records a pass or density, or changes a maintenance deadline. A true wake is
// consumed once and only requests a normal full pass; that pass must revalidate
// its current watermarks, space, storage and shared-gate admission.
func (r *Runner) ObserveBusyHistoryResources(ctx context.Context) (wake bool, retryAfter time.Duration) {
	if r == nil {
		return false, 0
	}
	id := r.busyHistoryObservationPending.Load()
	if id == 0 {
		return false, 0
	}
	canceled := func() bool { return ctx != nil && ctx.Err() != nil || r.ctx != nil && r.ctx.Err() != nil }
	if canceled() {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	if !r.passMu.TryLock() {
		return false, BusyHistoryObservationInterval
	}
	defer r.passMu.Unlock()
	if r.busyHistoryObservationPending.Load() != id {
		return false, 0
	}
	if canceled() {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	if r.pendingMaintenanceID != 0 {
		return false, BusyHistoryObservationInterval
	}
	source, ok := r.chain.(syncRemainingSource)
	if !r.busyHistoryObservationEnabled() || !r.cfg.DeferHistoryBuildWhileSyncing || !ok {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	if remaining, active := source.SyncRemainingBlocks(); !active || remaining <= r.cfg.HistoryWindow {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	now := time.Now()
	if now.Before(r.nextBusyHistoryObservation) {
		return false, r.nextBusyHistoryObservation.Sub(now)
	}
	started := now
	r.busyHistoryObservationMetrics.checks.Inc(1)
	defer func() {
		r.nextBusyHistoryObservation = time.Now().Add(BusyHistoryObservationInterval)
		r.busyHistoryObservationMetrics.duration.Update(int64(coldSnapshotPhaseDuration(started)))
	}()
	r.refreshHistoryLoad(now)
	ready := r.busyHistoryBuildReady(time.Now())
	if canceled() {
		r.busyHistoryObservationPending.CompareAndSwap(id, 0)
		return false, 0
	}
	if r.busyHistoryObservationPending.Load() != id {
		return false, 0
	}
	// Reading the current deadline also observes a later outer completion.
	// The gate cooldown is only inspected: an occupied lease is rechecked by
	// the eventual full pass, never probed by acquiring/releasing it here.
	if !ready || r.historyBuildRetryAfter(time.Now(), false, true) > 0 || r.cfg.HeavyWorkGate.CooldownRemaining() > 0 {
		return false, BusyHistoryObservationInterval
	}
	if r.busyHistoryObservationPending.CompareAndSwap(id, 0) {
		r.busyHistoryObservationMetrics.wakeups.Inc(1)
		return true, 0
	}
	return false, 0
}

type busyHistoryObservationMetrics struct {
	checks, wakeups *metrics.Counter
	duration        *metrics.Gauge
}

func newBusyHistoryObservationMetrics(namespace string) busyHistoryObservationMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace) + "history/busy_observation/"
	return busyHistoryObservationMetrics{
		checks:   metrics.GetOrRegisterCounter(namespace+"checks", nil),
		wakeups:  metrics.GetOrRegisterCounter(namespace+"wakeups", nil),
		duration: metrics.GetOrRegisterGauge(namespace+"last_duration", nil),
	}
}
