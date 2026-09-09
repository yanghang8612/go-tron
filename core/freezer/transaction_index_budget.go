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
	mu               sync.Mutex
	retry, notBefore time.Time
	lastSample       time.Time
	lastDebt         uint64
	lastStalls       uint64
	debtRises        int
	reservation      *maintenance.HeavyWorkReservation
	totalWork        time.Duration
	lastProgressLog  time.Time
	suppressedLogs   uint64
}

func (r *Runner) resetTransactionIndexBudget() {
	b := &r.txIndexBudget
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cancelReservation()
	b.retry = time.Time{}
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

// Require both engine and device headroom. A busy SSD with low latency is not
// treated as saturated; stalls, L0 pressure and sustained compaction debt growth
// use the same conservative signals as history maintenance.
func (b *transactionIndexBudget) healthy(p maintenance.StoragePressure, now time.Time) bool {
	if !p.Available || !freshTransactionIndexSample(p.SampledAt, now) {
		return false
	}
	newStall := !b.lastSample.IsZero() && p.StallCount > b.lastStalls
	if b.lastSample.IsZero() || p.SampledAt.Sub(b.lastSample) >= 5*time.Second || p.SampledAt.Before(b.lastSample) {
		if !b.lastSample.IsZero() && p.CompactionDebt > b.lastDebt {
			b.debtRises++
		} else {
			b.debtRises = 0
		}
		b.lastSample, b.lastStalls, b.lastDebt = p.SampledAt, p.StallCount, p.CompactionDebt
	}
	return !newStall && !p.HardLimitReached(now) &&
		!(p.L0CompactionThreshold > 0 && p.L0Sublevels >= p.L0CompactionThreshold && p.L0Sublevels-p.L0CompactionThreshold >= p.L0CompactionThreshold) &&
		!(b.debtRises >= 2 && p.CompactionDebt >= 2<<30) &&
		p.DeviceAvailable && freshTransactionIndexSample(p.DeviceSampledAt, now) && p.DeviceAwait >= 0 && p.DeviceAwait <= 2*time.Millisecond
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

func (r *Runner) deferTransactionIndexBudget(now time.Time, wait time.Duration, reason heavyMaintenanceDeferral) {
	r.txIndexBudget.retry = now.Add(max(transactionIndexRecovery, wait))
	r.recordHeavyMaintenanceDeferred(heavyMaintenanceTxIndex, reason)
}

func (r *Runner) beginTransactionIndexBudget() (*heavyMaintenanceLease, bool) {
	b := &r.txIndexBudget
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if failed := r.lastTxIndexMaintenanceError.Load(); failed > 0 && r.cfg.HeavyMaintenanceErrorBackoff > 0 {
		if remaining := time.Unix(0, failed).Add(r.cfg.HeavyMaintenanceErrorBackoff).Sub(now); remaining > 0 {
			b.cancelReservation()
			r.deferTransactionIndexBudget(now, remaining, heavyMaintenanceDeferredErrorBackoff)
			return nil, false
		}
	}
	if remaining := r.startedAt.Add(r.cfg.HeavyMaintenanceStartupDelay).Sub(now); remaining > 0 {
		b.cancelReservation()
		r.deferTransactionIndexBudget(now, remaining, heavyMaintenanceDeferredCatchup)
		return nil, false
	}
	debt, valid := r.transactionIndexCandidate()
	if valid && debt == 0 {
		b.cancelReservation()
		b.retry = time.Time{}
		return nil, false
	}
	p := r.cfg.TransactionIndexLoadProbe()
	healthy := b.healthy(p, now)
	if !healthy || !valid {
		b.cancelReservation()
	}
	r.transactionIndexBudgetMetric("healthy", boolGaugeValue(healthy))
	if p.HardLimitReached(now) {
		r.deferTransactionIndexBudget(now, 30*time.Second, heavyMaintenanceDeferredResource)
		return nil, false
	}
	deadline := b.notBefore
	if !healthy {
		if last := r.lastTxIndexCatchupMaintenance.Load(); last > 0 {
			deadline = maxTime(deadline, time.Unix(0, last).Add(r.cfg.CatchupMaintenanceInterval))
		}
	}
	if now.Before(deadline) {
		b.cancelReservation()
		r.deferTransactionIndexBudget(now, deadline.Sub(now), heavyMaintenanceDeferredCatchup)
		return nil, false
	}
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
		r.deferTransactionIndexBudget(now, wait, heavyMaintenanceDeferredResource)
		return nil, false
	}
	b.cancelReservation()
	r.lastTxIndexCatchupMaintenance.Store(now.UnixNano())
	r.txIndexMaintenanceAdmitted.Add(1)
	r.updateMetrics()
	// One leaf owns the gate. Debt affects its duty target, never its atomic
	// range or the amount of work that can run without giving other jobs a turn.
	duty := uint64(100_000)
	if healthy && valid && debt >= 8*defaultV2SegmentBlocks {
		duty = 200_000
	}
	r.transactionIndexBudgetMetric("duty_ppm", int64(duty))
	ctx := context.WithValue(r.pauseCtx, transactionIndexBoundedContextKey{}, true)
	ctx = context.WithValue(ctx, transactionIndexQuietContextKey{}, healthy && valid)
	return &heavyMaintenanceLease{ctx: ctx, release: func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		completed := time.Now()
		work := completed.Sub(now)
		b.totalWork += work
		wait := transactionIndexWorkRecovery(work, duty)
		healthyAtRelease = healthy && valid && r.lastTxIndexMaintenanceError.Load() == 0 && b.healthy(r.cfg.TransactionIndexLoadProbe(), completed)
		if !healthyAtRelease {
			wait = max(wait, now.Add(r.cfg.CatchupMaintenanceInterval).Sub(completed))
		}
		b.notBefore = completed.Add(wait)
		b.retry = b.notBefore
		r.transactionIndexBudgetMetric("last_work_duration", int64(work))
		r.transactionIndexBudgetMetric("total_work_duration", int64(b.totalWork))
		r.transactionIndexBudgetMetric("recovery_duration", int64(wait))
		release()
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
