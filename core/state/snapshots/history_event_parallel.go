package snapshots

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// The range is already admitted under the common maintenance lease. This
// decision permits exactly two builders, not additional independent leases or
// larger worker pools. Idle derived builders keep their original ordering.
func (r *Runner) parallelHistoryEventReady(events, derived bool, now time.Time) bool {
	return events && !derived && r.cfg.EventLogVersion == EventLogSegmentV4Version &&
		r.cfg.HeavyWorkGate != nil && r.throughputCatchup() && r.historySyncBudgetActive() &&
		runtime.GOMAXPROCS(0) >= 8 && r.historyLoad.cpuBurstReady(now) &&
		r.cfg.ParallelHistoryEventReady != nil && r.cfg.ParallelHistoryEventReady()
}

type coldSnapshotBuildOutput struct {
	refs     []SegmentRef
	duration time.Duration
	err      error
}

func buildColdSnapshotFiles(build func() ([]SegmentRef, error)) coldSnapshotBuildOutput {
	started := time.Now()
	refs, err := build()
	return coldSnapshotBuildOutput{refs: refs, duration: coldSnapshotPhaseDuration(started), err: err}
}

// The existing hooks cannot interrupt all source scans, so cancellation is a
// join boundary, not a promise to preempt either builder. Every return waits for
// both workers; the caller then retains its lease through publication. Outputs
// are immutable files only. They do not become active until the parent publishes
// the combined manifest. Failed attempts may leave content-addressed orphans,
// as serial builders already do; deleting them here could remove an older
// manifest's identical file and is therefore deliberately forbidden.
func buildHistoryEventFiles(ctx context.Context, history, event func() ([]SegmentRef, error)) (h, e coldSnapshotBuildOutput, err error) {
	if err := ctx.Err(); err != nil {
		return h, e, err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		h = buildColdSnapshotFiles(history)
	}()
	go func() {
		defer wg.Done()
		e = buildColdSnapshotFiles(event)
	}()
	wg.Wait()
	return h, e, errors.Join(h.err, e.err, ctx.Err())
}

type historyEventBuildMetrics struct {
	attempts, builds                 *metrics.Counter
	lastPaired, wall, history, event *metrics.Gauge
}

func newHistoryEventBuildMetrics(namespace string) historyEventBuildMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace) + "history/event_parallel/"
	return historyEventBuildMetrics{
		attempts:   metrics.GetOrRegisterCounter(namespace+"attempts", nil),
		builds:     metrics.GetOrRegisterCounter(namespace+"builds", nil),
		lastPaired: metrics.GetOrRegisterGauge(namespace+"last/paired", nil),
		wall:       metrics.GetOrRegisterGauge(namespace+"last/build_wall", nil),
		history:    metrics.GetOrRegisterGauge(namespace+"last/history", nil),
		event:      metrics.GetOrRegisterGauge(namespace+"last/event", nil),
	}
}

// Last values describe the last admitted history attempt, including failures;
// polling/deferred passes do not erase them. Phase durations may overlap. The
// build-wall metric is the complete measured inner pass, also used by density;
// ordinary full-lifecycle recovery remains independently measured outside it.
func (m historyEventBuildMetrics) record(result PassResult) {
	if !result.HistoryBuildAttempted || m.lastPaired == nil {
		return
	}
	m.lastPaired.Update(boolGauge(result.HistoryEventParallel))
	m.wall.Update(int64(result.BuildDuration))
	m.history.Update(int64(result.HistoryDuration))
	m.event.Update(int64(result.EventLogDuration))
	if result.HistoryEventParallel {
		m.attempts.Inc(1)
		if result.Built {
			m.builds.Inc(1)
		}
	}
}
