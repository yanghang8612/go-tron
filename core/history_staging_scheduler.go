package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type historyStagingAdmission uint8

const (
	historyStagingIdle historyStagingAdmission = iota
	historyStagingBusy
	historyStagingEngineUnknown
	historyStagingEngineStale
	historyStagingEngineHard
	historyStagingDeviceUnknown
	historyStagingDeviceStale
	historyStagingDeviceBusy
	historyStagingDeviceQueue
	historyStagingDeviceAwait
	historyStagingMemoryUnknown
	historyStagingMemoryStale
	historyStagingMemoryOOM
	historyStagingMemoryLow
)

var historyStagingAdmissionNames = [...]string{"idle", "busy", "engine_unknown", "engine_stale", "engine_hard", "device_unknown", "device_stale", "device_busy", "device_queue", "device_await", "memory_unknown", "memory_stale", "memory_oom", "memory_low"}

// ErrHistoryStagingResourceDeferred ends a live pass to release its own
// buffers and pinned snapshots. It is an expected deferral, not corruption.
var ErrHistoryStagingResourceDeferred = errors.New("history staging mover: runtime resource deferred")

type HistoryStagingMemory struct {
	Available     bool
	SampledAt     time.Time
	HeadroomBytes uint64
	UnderOOM      bool
}

func (m *HistoryStagingMover) memoryAdmission(now time.Time) historyStagingAdmission {
	if m.cfg.MemoryProbe == nil {
		return historyStagingIdle
	}
	p := m.cfg.MemoryProbe()
	if p.UnderOOM {
		return historyStagingMemoryOOM
	}
	if !p.Available {
		return historyStagingMemoryUnknown
	}
	if p.SampledAt.IsZero() || now.Sub(p.SampledAt) < -time.Second || now.Sub(p.SampledAt) > 15*time.Second {
		return historyStagingMemoryStale
	}
	if p.HeadroomBytes < 2<<30 {
		return historyStagingMemoryLow
	}
	return historyStagingIdle
}

func historyStagingResourceError(mode historyStagingAdmission) error {
	return fmt.Errorf("%w: %s", ErrHistoryStagingResourceDeferred, historyStagingAdmissionNames[mode])
}

var (
	historyStagingMoverQuantumBytes  = metrics.NewRegisteredCounter("core/history_staging/mover/quantum_work_bytes", nil)
	historyStagingMoverYields        = metrics.NewRegisteredCounter("core/history_staging/mover/quantum_yields", nil)
	historyStagingMoverWaitNanos     = metrics.NewRegisteredCounter("core/history_staging/mover/quantum_wait_nanos", nil)
	historyStagingMoverHeldNanos     = metrics.NewRegisteredCounter("core/history_staging/mover/quantum_held_nanos", nil)
	historyStagingMoverAdmissionMode = metrics.NewRegisteredGauge("core/history_staging/mover/admission_mode", nil)
)

func classifyHistoryStagingPressure(p maintenance.StoragePressure, now time.Time) historyStagingAdmission {
	if !p.Available {
		return historyStagingEngineUnknown
	}
	if p.SampledAt.IsZero() || now.Sub(p.SampledAt) < -time.Second || now.Sub(p.SampledAt) > 15*time.Second {
		return historyStagingEngineStale
	}
	if p.HardLimitReached(now) {
		return historyStagingEngineHard
	}
	if !p.DeviceAvailable {
		return historyStagingDeviceUnknown
	}
	if p.DeviceSampledAt.IsZero() || now.Sub(p.DeviceSampledAt) < -time.Second || now.Sub(p.DeviceSampledAt) > 15*time.Second {
		return historyStagingDeviceStale
	}
	if p.DeviceBusyPPM >= 900_000 {
		return historyStagingDeviceBusy
	}
	if p.DeviceQueueMilli < 1_000 && p.DeviceAwait < 20*time.Millisecond {
		return historyStagingIdle
	}
	if p.DeviceQueueMilli >= 8_000 {
		return historyStagingDeviceQueue
	}
	if p.DeviceAwait >= 5*time.Millisecond {
		return historyStagingDeviceAwait
	}
	return historyStagingBusy
}

// A continuously busy parallel device is not necessarily latency saturated.
// Only the cooperative mover gets this stricter low-latency busy lane; index
// GC still uses the original classifier. All freshness and engine hard limits
// are evaluated before this exception, and quanta/recovery remain unchanged.
func classifyHistoryStagingMoverPressure(p maintenance.StoragePressure, now time.Time) historyStagingAdmission {
	mode := classifyHistoryStagingPressure(p, now)
	if mode == historyStagingDeviceBusy && p.DeviceBusyPPM <= 1_000_000 &&
		p.DeviceQueueMilli < 8_000 && p.DeviceAwait >= 0 && p.DeviceAwait < 2*time.Millisecond {
		return historyStagingBusy
	}
	return mode
}

func (m *HistoryStagingMover) admission(now time.Time) historyStagingAdmission {
	if reason := m.memoryAdmission(now); reason >= historyStagingMemoryUnknown {
		metrics.GetOrRegisterCounter("core/history_staging/mover/resource_denied/"+historyStagingAdmissionNames[reason], nil).Inc(1)
		historyStagingMoverAdmissionMode.Update(int64(reason))
		return reason
	}
	mode := historyStagingIdle
	for _, engine := range []struct {
		name  string
		probe func() maintenance.StoragePressure
	}{{"hot", m.cfg.HotPressure}, {"stage", m.cfg.StagePressure}} {
		reason := classifyHistoryStagingMoverPressure(engine.probe(), now)
		if reason > historyStagingBusy {
			metrics.GetOrRegisterCounter("core/history_staging/mover/denied/"+engine.name+"/"+historyStagingAdmissionNames[reason], nil).Inc(1)
			if engine.name == "hot" {
				historyStagingMoverSkippedHot.Inc(1)
			} else {
				historyStagingMoverSkippedStage.Inc(1)
			}
			historyStagingMoverAdmissionMode.Update(int64(reason))
			return reason
		}
		mode = max(mode, reason)
	}
	historyStagingMoverAdmissionMode.Update(int64(mode))
	return mode
}

func historyStagingWait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// One live copy retains its pinned view, iterator, digest and pending batch
// while yielding. Restart still uses the existing durable claim protocol.
// Quanta are cooperative: one row/block or an fsync can exceed 100ms.
type historyStagingQuantum struct {
	m        *HistoryStagingMover
	ctx      context.Context
	release  func()
	started  time.Time
	bytes    uint64
	now      func() time.Time
	wait     func(context.Context, time.Duration) error
	yielding bool
	terminal error
}

func (q *historyStagingQuantum) close() {
	if q.release != nil {
		historyStagingMoverHeldNanos.Inc(max(0, q.now().Sub(q.started)).Nanoseconds())
		q.release()
		q.release = nil
	}
}

func (q *historyStagingQuantum) checkpoint(work uint64) error {
	return q.checkpointWork(work, false)
}

func (q *historyStagingQuantum) recheck() error {
	return q.checkpointWork(0, true)
}

func (q *historyStagingQuantum) checkpointWork(work uint64, force bool) (err error) {
	defer func() {
		if err != nil && q.release == nil {
			q.terminal = err
		}
	}()
	if q.terminal != nil {
		return q.terminal
	}
	if q.release == nil {
		return fmt.Errorf("history staging mover: quantum lost maintenance lease")
	}
	if err := q.ctx.Err(); err != nil {
		return err
	}
	q.bytes += work
	historyStagingMoverQuantumBytes.Inc(int64(work))
	now := q.now()
	boundary := q.bytes >= 32<<20 || now.Sub(q.started) >= 100*time.Millisecond
	if !force && !boundary {
		return nil
	}
	mode := q.m.admission(now)
	if mode >= historyStagingMemoryUnknown {
		q.close()
		return historyStagingResourceError(mode)
	}
	if mode <= historyStagingBusy && !boundary {
		return q.m.checkFreeSpace()
	}
	held := max(0, now.Sub(q.started))
	q.yielding = true
	q.close()
	q.yielding = false
	historyStagingMoverYields.Inc(1)
	waitStarted := q.now()
	defer func() { historyStagingMoverWaitNanos.Inc(max(0, q.now().Sub(waitStarted)).Nanoseconds()) }()
	// Do not reserve during recovery: other maintenance gets a real turn.
	if err := q.wait(q.ctx, max(time.Second, 4*held)); err != nil {
		return err
	}
	for {
		if err := q.ctx.Err(); err != nil {
			q.m.cancelReservation()
			return err
		}
		mode := q.m.admission(q.now())
		if mode >= historyStagingMemoryUnknown {
			return historyStagingResourceError(mode)
		}
		if mode <= historyStagingBusy {
			if release, ok := q.m.acquireHeavy(true); ok {
				q.release, q.started, q.bytes = release, q.now(), 0
				mode = q.m.admission(q.now())
				if mode >= historyStagingMemoryUnknown {
					q.close()
					return historyStagingResourceError(mode)
				}
				if mode > historyStagingBusy {
					q.close()
					q.m.cancelReservation()
					continue
				}
				if err := q.m.checkFreeSpace(); err != nil {
					q.close()
					return err
				}
				return nil
			}
		} else {
			q.m.cancelReservation()
		}
		if err := q.wait(q.ctx, 250*time.Millisecond); err != nil {
			q.m.cancelReservation()
			return err
		}
	}
}

func (m *HistoryStagingMover) checkFreeSpace() error {
	if m.cfg.Limits.FreeBytes == nil {
		return nil
	} // pressure-only unit tests
	free, err := m.cfg.Limits.FreeBytes()
	if err != nil {
		return fmt.Errorf("history staging mover free-space probe: %w", err)
	}
	if free < m.cfg.Limits.MinFreeBytes {
		return fmt.Errorf("history staging mover free-space floor: have %d require %d", free, m.cfg.Limits.MinFreeBytes)
	}
	return nil
}

func (m *HistoryStagingMover) cancelReservation() {
	if m.reservation != nil {
		m.reservation.Cancel()
		m.reservation = nil
	}
}

func historyStagingRecovery(held, _ time.Duration) time.Duration {
	return max(time.Second, 4*held)
}

// Gates with no configured recovery retain their final-release behavior;
// every actual cooperative yield still applies measured recovery.
func (m *HistoryStagingMover) releaseRecovery(held, defaultCooldown time.Duration) time.Duration {
	if defaultCooldown == 0 && (m.quantum == nil || !m.quantum.yielding) {
		return 0
	}
	return historyStagingRecovery(held, defaultCooldown)
}

func (m *HistoryStagingMover) acquireHeavy(candidate bool) (func(), bool) {
	now := time.Now()
	if m.reservation != nil && !m.reservation.Active() {
		m.reservation = nil
	}
	if m.reservation != nil {
		release, ok := m.reservation.TryAcquireWithReleaseCooldown(m.releaseRecovery)
		if ok {
			m.reservation = nil
		}
		return release, ok
	}
	if release, ok := m.cfg.HeavyWorkGate.TryAcquireWithReleaseCooldown(m.releaseRecovery); ok {
		return release, true
	}
	// A failed reservation gets at least 20s with no new reservation from us.
	// This bounds interference with other ready maintenance contenders.
	if candidate && (m.lastReservation.IsZero() || now.Sub(m.lastReservation) >= 30*time.Second) {
		m.reservation = m.cfg.HeavyWorkGate.ReserveNext(10 * time.Second)
		m.lastReservation = now
	}
	return nil, false
}

func (m *HistoryStagingMover) workLimits() rawdb.HistoryStagingLimits {
	limits := m.cfg.Limits
	if m.quantum != nil {
		limits.Checkpoint = m.quantum.checkpoint
	}
	return limits
}

// This bounded metadata hint may reserve a turn, but never authorizes a move.
// Full canonical, hash-bound stage, settled prefix and claim proofs still run.
func (m *HistoryStagingMover) candidateHint(ctx context.Context) (bool, error) {
	if m.bc == nil || m.bc.HistoryStagingManager() == nil {
		return false, nil
	}
	bc := m.bc
	// Approximate cached/stage observations are hints only. Do not stall
	// canonical import to scan maintenance metadata or decode claim proofs.
	if bc.closed.Load() || bc.historyStagingReplayEpoch.Load() != 0 {
		return false, nil
	}
	solid := bc.cachedDynProps().LatestSolidifiedBlockNum()
	if solid <= 0 || uint64(solid) <= m.cfg.HistoryWindow {
		return false, nil
	}
	finish, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageFinish)
	if err != nil || !ok || !finish.HasBlockHash {
		return false, err
	}
	indexed, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if err != nil || !ok || !indexed.HasBlockHash {
		return false, err
	}
	head := bc.CurrentBlock()
	if head == nil {
		return false, nil
	}
	eligible := min(head.Number(), uint64(solid)-m.cfg.HistoryWindow, finish.BlockNum, indexed.BlockNum)
	lastCompleteBucket := historyStagingLastCompleteBucket(eligible)
	var coldThrough uint64
	if cold, ok := bc.stateCodeColdHistory.(*snapshots.Manager); ok && cold != nil {
		if manifest := cold.Manifest(); manifest != nil {
			coldThrough = manifest.VisibleTxEnd
		}
	}
	ready := func(route rawdb.HistoryStagingRoute) (bool, error) {
		if route.Bucket == 0 {
			return false, nil
		}
		_, last, err := rawdb.StateHistoryChunkBucketBounds(route.Bucket)
		if err != nil {
			return false, err
		}
		switch route.Owner {
		case rawdb.HistoryStagingOwnerSource:
			return route.Bucket >= m.nextBucket && route.Bucket <= lastCompleteBucket, nil
		case rawdb.HistoryStagingOwnerTarget:
			if !route.SourceCleared {
				return true, nil
			}
			if coldThrough != 0 {
				row, present, err := rawdb.ReadStateTxRange(bc.db, last)
				if err != nil {
					return false, err
				}
				return present && row.EndTxNum <= coldThrough, nil
			}
		case rawdb.HistoryStagingOwnerCold:
			// A direct SOURCE→COLD handoff has no target payload to retire.
			return route.SourceCleared && !route.TargetCleared, nil
		}
		return false, nil
	}
	budget := uint64(256)
	// Revalidate a cached candidate even after its reservation expires;
	// an unrelated empty page must not revoke its still-eligible next turn.
	if m.hintBucket != 0 {
		route, present, err := bc.HistoryStagingManager().ReadRoute(m.hintBucket)
		if err != nil {
			return false, err
		}
		if present {
			if ok, err := ready(route); err != nil || ok {
				return ok, err
			}
		}
		m.cancelReservation()
		m.hintBucket = 0
		budget--
	}
	routes, next, complete, err := bc.HistoryStagingManager().ScanRoutes(ctx, m.hintCursor, budget)
	if err != nil {
		return false, err
	}
	if complete {
		m.hintCursor = nil
	} else {
		m.hintCursor = append(m.hintCursor[:0], next...)
	}
	for _, route := range routes {
		ok, err := ready(route)
		if err != nil {
			return false, err
		}
		if ok {
			m.hintBucket = route.Bucket
			return true, nil
		}
	}
	return false, nil
}
