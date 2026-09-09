package freezer

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

const transactionIndexRecovery = 3 * time.Second

type transactionIndexBoundedContextKey struct{}
type transactionIndexQuietContextKey struct{}

type transactionIndexBudget struct {
	mu                sync.Mutex
	retry, notBefore  time.Time // retry may inspect pressure before work is allowed
	conservativeUntil time.Time // unknown/hard completion does not become a soft retry
	pressureRecheck   time.Time
	pressure          transactionIndexPressureState
	clock             func() time.Time
	reservation       *maintenance.HeavyWorkReservation
	totalWork         time.Duration
	lastProgressLog   time.Time
	suppressedLogs    uint64
}

func (b *transactionIndexBudget) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}
	return time.Now()
}

func (r *Runner) resetTransactionIndexBudget() {
	b := &r.txIndexBudget
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cancelReservation()
	b.retry = time.Time{}
	b.pressureRecheck = time.Time{}
	r.recordTransactionIndexWait(b.now(), txIndexWaitNone)
}

func (r *Runner) transactionIndexRetryDeadline() time.Time {
	b := &r.txIndexBudget
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.retry
}

func transactionIndexRetryDelay(deadline, now time.Time) time.Duration {
	if remaining := deadline.Sub(now); remaining > 0 {
		return remaining
	}
	// A concurrent explicit caller may own the index mutex while the lifecycle
	// sees its previous deadline. Do not turn this into a millisecond spin or
	// overwrite the recovery deadline that caller will install on completion.
	return transactionIndexRecovery
}

func (b *transactionIndexBudget) cancelReservation() {
	b.reservation.Cancel()
	b.reservation = nil
}

func freshTransactionIndexSample(sample, now time.Time) bool {
	age := now.Sub(sample)
	return !sample.IsZero() && age >= -time.Second && age <= 15*time.Second
}

func (b *transactionIndexBudget) healthy(p maintenance.StoragePressure, now time.Time) bool {
	return b.pressure.assess(p, now) == txIndexHealthHealthy
}

func (r *Runner) transactionIndexBudgetMetric(name string, value int64) {
	metrics.GetOrRegisterGauge(normalizeMetricNamespace(r.cfg.MetricsNamespace)+"txindex/budget/"+name, nil).Update(value)
}

// candidate only reads coverage and a single durable cursor. It never scans
// bodies, acquires the freezer write lock, or reserves work whose prerequisites
// depend on history/event publication.
func (r *Runner) transactionIndexCandidate() (debt uint64, valid bool) {
	v2, ok := r.freezer.(V2Compactor)
	if !ok {
		return 0, false
	}
	index, ok := r.freezer.(TransactionIndexCompactor)
	if !ok {
		return 0, false
	}
	coverage, end := index.TransactionIndexCoverage(), v2.V2Coverage()
	pruned, initialized, err := r.transactionIndexPruneProgress(coverage)
	if err != nil || !initialized || coverage > end || pruned > coverage {
		return 0, false
	}
	return end - pruned, true
}

func (r *Runner) deferTransactionIndexBudget(now time.Time, wait time.Duration, reason heavyMaintenanceDeferral, waitReason transactionIndexWaitReason) {
	r.txIndexBudget.retry = now.Add(max(transactionIndexRecovery, wait))
	r.recordTransactionIndexWait(now, waitReason)
	r.recordHeavyMaintenanceDeferred(heavyMaintenanceTxIndex, reason)
}

func (r *Runner) deferTransactionIndexPressure(now time.Time, delay time.Duration, reason transactionIndexWaitReason) {
	b := &r.txIndexBudget
	// Explicit requests must not keep moving an already scheduled observation
	// farther into the future. These are probe opportunities, not work permits.
	if !b.pressureRecheck.After(now) {
		b.pressureRecheck = now.Add(delay)
	}
	r.deferTransactionIndexBudget(now, b.pressureRecheck.Sub(now), heavyMaintenanceDeferredResource, reason)
}

func (r *Runner) beginTransactionIndexBudget() (*heavyMaintenanceLease, bool) {
	b := &r.txIndexBudget
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if failed := r.lastTxIndexMaintenanceError.Load(); failed > 0 && r.cfg.HeavyMaintenanceErrorBackoff > 0 {
		if remaining := time.Unix(0, failed).Add(r.cfg.HeavyMaintenanceErrorBackoff).Sub(now); remaining > 0 {
			b.cancelReservation()
			r.deferTransactionIndexBudget(now, max(remaining, b.notBefore.Sub(now)), heavyMaintenanceDeferredErrorBackoff, txIndexWaitError)
			return nil, false
		}
	}
	if remaining := r.startedAt.Add(r.cfg.HeavyMaintenanceStartupDelay).Sub(now); remaining > 0 {
		b.cancelReservation()
		r.deferTransactionIndexBudget(now, remaining, heavyMaintenanceDeferredCatchup, txIndexWaitStartup)
		return nil, false
	}
	debt, valid := r.transactionIndexCandidate()
	if valid && debt == 0 {
		b.cancelReservation()
		b.retry = time.Time{}
		b.pressureRecheck = time.Time{}
		r.recordTransactionIndexWait(now, txIndexWaitNone)
		return nil, false
	}
	p := r.cfg.TransactionIndexLoadProbe()
	checkedAt := b.now()
	reason := b.pressure.assess(p, checkedAt)
	healthy := reason == txIndexHealthHealthy
	r.recordTransactionIndexHealth("admission", reason, p, checkedAt)
	if !healthy || !valid {
		b.cancelReservation()
	}
	if reason.hard() {
		r.deferTransactionIndexPressure(now, transactionIndexHardRecheck, txIndexWaitHard)
		return nil, false
	}
	if reason.unknown() || !valid {
		if last := r.lastTxIndexCatchupMaintenance.Load(); last > 0 {
			b.conservativeUntil = maxTime(b.conservativeUntil, time.Unix(0, last).Add(r.cfg.CatchupMaintenanceInterval))
		}
	}
	if reason.soft() {
		r.deferTransactionIndexPressure(now, transactionIndexPressureRecheck, txIndexWaitSoft)
		return nil, false
	}
	deadline := maxTime(b.notBefore, b.conservativeUntil)
	if now.Before(deadline) {
		b.cancelReservation()
		if reason.unknown() {
			r.deferTransactionIndexPressure(now, transactionIndexHardRecheck, txIndexWaitConservative)
		} else {
			b.pressureRecheck = time.Time{}
			waitReason := txIndexWaitWork
			if b.conservativeUntil.After(b.notBefore) {
				waitReason = txIndexWaitConservative
			}
			r.deferTransactionIndexBudget(now, deadline.Sub(now), heavyMaintenanceDeferredCatchup, waitReason)
		}
		return nil, false
	}
	b.pressureRecheck = time.Time{}
	var release func()
	var ok bool
	// Default to conservative recovery until the complete batch has finished
	// and its pressure sample has been checked again. The gate measures this
	// lease directly, so the first unusually large leaf cannot be underestimated.
	healthyAtRelease := false
	recoveryPolicy := func(held, defaultCooldown time.Duration) time.Duration {
		if healthyAtRelease {
			return min(transactionIndexRecovery, held)
		}
		return defaultCooldown
	}
	if healthy && valid {
		if b.reservation.Active() {
			release, ok = b.reservation.TryAcquireWithReleaseCooldown(recoveryPolicy)
		} else {
			b.cancelReservation()
			release, ok = r.cfg.HeavyWorkGate.TryAcquireWithReleaseCooldown(recoveryPolicy)
		}
	} else {
		release, ok = r.cfg.HeavyWorkGate.TryAcquire()
	}
	if !ok {
		cooldown := r.cfg.HeavyWorkGate.CooldownRemaining()
		wait := cooldown
		if healthy && valid && cooldown > 7*time.Second {
			// Do not let a ten-second reservation expire before a longer
			// existing recovery deadline. Recheck once near that deadline.
			b.cancelReservation()
			wait = cooldown - 6*time.Second
		} else if healthy && valid && !b.reservation.Active() {
			b.reservation = r.cfg.HeavyWorkGate.ReserveNext(10 * time.Second)
		}
		r.deferTransactionIndexBudget(now, wait, heavyMaintenanceDeferredResource, txIndexWaitGate)
		return nil, false
	}
	b.cancelReservation()
	r.lastTxIndexCatchupMaintenance.Store(now.UnixNano())
	r.txIndexMaintenanceAdmitted.Add(1)
	r.updateMetrics()
	b.retry = time.Time{}
	r.recordTransactionIndexWait(now, txIndexWaitNone)
	// One leaf owns the gate. Debt affects its duty target, never its atomic
	// range or the amount of work that can run without giving other jobs a turn.
	duty := uint64(100_000)
	if healthy && valid && debt >= 8*defaultV2SegmentBlocks {
		duty = 200_000
	}
	r.transactionIndexBudgetMetric("duty_ppm", int64(duty))
	ctx := context.WithValue(r.pauseCtx, transactionIndexBoundedContextKey{}, true)
	ctx = context.WithValue(ctx, transactionIndexQuietContextKey{}, healthy && valid)
	var once sync.Once
	return &heavyMaintenanceLease{ctx: ctx, release: func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			// Even an exceptional completion probe must not leak the gate token.
			defer release()
			completed := b.now()
			endPressure := maintenance.StoragePressure{}
			endReason := txIndexHealthWorkError
			failed := r.lastTxIndexMaintenanceError.Load()
			if failed == 0 {
				endPressure = r.cfg.TransactionIndexLoadProbe()
				completed = b.now()
				endReason = b.pressure.assess(endPressure, completed)
				if !valid {
					endReason = txIndexHealthCandidateUnknown
				}
			}
			// Charge the final health probe too, and assess freshness after it
			// returns rather than against a timestamp from before a slow read.
			work := max(0, completed.Sub(now))
			b.totalWork += work
			b.notBefore = completed.Add(transactionIndexWorkRecovery(work, duty))
			healthyAtRelease = healthy && valid && endReason == txIndexHealthHealthy
			r.recordTransactionIndexHealth("completion", endReason, endPressure, completed)
			if reason.unknown() || !valid || endReason.unknown() || endReason.hard() || failed > 0 {
				b.conservativeUntil = maxTime(b.conservativeUntil, now.Add(r.cfg.CatchupMaintenanceInterval))
			}
			b.retry = maxTime(b.notBefore, b.conservativeUntil)
			waitReason := txIndexWaitWork
			if b.conservativeUntil.After(b.notBefore) {
				waitReason = txIndexWaitConservative
			}
			switch {
			case failed > 0 && r.cfg.HeavyMaintenanceErrorBackoff > 0:
				b.retry = maxTime(b.retry, time.Unix(0, failed).Add(r.cfg.HeavyMaintenanceErrorBackoff))
				waitReason = txIndexWaitError
			case endReason.soft():
				b.pressureRecheck = completed.Add(transactionIndexPressureRecheck)
				b.retry, waitReason = b.pressureRecheck, txIndexWaitSoft
			case endReason.unknown() || endReason.hard():
				b.pressureRecheck = completed.Add(transactionIndexHardRecheck)
				b.retry, waitReason = b.pressureRecheck, txIndexWaitConservative
				if endReason.hard() {
					waitReason = txIndexWaitHard
				}
			default:
				b.pressureRecheck = time.Time{}
			}
			r.transactionIndexBudgetMetric("last_work_duration", int64(work))
			r.transactionIndexBudgetMetric("total_work_duration", int64(b.totalWork))
			// Preserve the old duration gauge as the effective work floor. The
			// separately named wait metrics describe the earlier probe timer.
			r.transactionIndexBudgetMetric("recovery_duration", int64(max(0, maxTime(b.notBefore, b.conservativeUntil).Sub(completed))))
			r.recordTransactionIndexWait(completed, waitReason)
		})
	}}, true
}

func quietTransactionIndexContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	quiet, _ := ctx.Value(transactionIndexQuietContextKey{}).(bool)
	return quiet
}

func logTransactionIndexDetail(ctx context.Context, message string, args ...any) {
	if quietTransactionIndexContext(ctx) {
		log.Debug(message, args...)
	} else {
		log.Info(message, args...)
	}
}

func (r *Runner) transactionIndexProgressLogDecision(quiet, caughtUp bool, now time.Time) (bool, uint64) {
	b := &r.txIndexBudget
	b.mu.Lock()
	defer b.mu.Unlock()
	if quiet && !caughtUp && !b.lastProgressLog.IsZero() && now.Sub(b.lastProgressLog) >= 0 && now.Sub(b.lastProgressLog) < 30*time.Second {
		b.suppressedLogs++
		return false, 0
	}
	suppressed := b.suppressedLogs
	b.suppressedLogs = 0
	if quiet {
		b.lastProgressLog = now
	} else {
		b.lastProgressLog = time.Time{}
	}
	return true, suppressed
}

// All successful entry points share one progress window. Failures remain at
// their existing unthrottled call sites; idle maintenance keeps its detail.
func (r *Runner) logTransactionIndexProgress(ctx context.Context) {
	quiet := quietTransactionIndexContext(ctx)
	debt, valid := r.transactionIndexCandidate()
	if emit, suppressed := r.transactionIndexProgressLogDecision(quiet, valid && debt == 0, time.Now()); emit {
		if quiet {
			log.Info("Freezer: transaction-index maintenance complete", "debtBlocks", debt,
				"archivedRows", r.txIndexRowsArchived.Load(), "prunedRows", r.txIndexRowsPruned.Load(), "suppressedPasses", suppressed)
		} else {
			log.Info("Freezer: transaction-index maintenance complete")
		}
	}
}

func transactionIndexWorkRecovery(work time.Duration, duty uint64) time.Duration {
	factor := uint64(1_000_000)/duty - 1
	if uint64(max(work, 0)) > uint64(math.MaxInt64)/factor {
		return time.Duration(math.MaxInt64)
	}
	return max(transactionIndexRecovery, work*time.Duration(factor))
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
