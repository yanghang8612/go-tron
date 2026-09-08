package snapshots

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

type HistoryCatchupMode string

var ErrHistoryMaintenancePending = errors.New("snapshots: previous history maintenance has not completed")

const (
	HistoryCatchupBalanced   HistoryCatchupMode = "balanced"
	HistoryCatchupThroughput HistoryCatchupMode = "throughput"
)

func ParseHistoryCatchupMode(value string) (HistoryCatchupMode, error) {
	switch HistoryCatchupMode(value) {
	case "", HistoryCatchupBalanced:
		return HistoryCatchupBalanced, nil
	case HistoryCatchupThroughput:
		return HistoryCatchupThroughput, nil
	default:
		return "", fmt.Errorf("invalid history catchup mode %q (want balanced or throughput)", value)
	}
}

func (r *Runner) throughputCatchup() bool {
	return r != nil && r.cfg.HistoryCatchupMode == HistoryCatchupThroughput
}

func (r *Runner) throughputRecovery(work time.Duration, failed bool) time.Duration {
	// Target history duty (not total device duty). An oversized complete block
	// retains its real recovery cost even when it cannot be split further.
	// Only optional merge cost is excluded; clipping expensive ordinary work
	// would silently raise online history duty when density increases.
	duty := r.historyLoad.dutyPPM()
	budget := float64(max(time.Duration(0), work)) * float64(1_000_000-duty) / float64(duty)
	recovery := time.Duration(math.MaxInt64)
	if budget < float64(math.MaxInt64) {
		recovery = time.Duration(budget)
	}
	minimum := max(3*time.Second, r.cfg.CatchupHeavyWorkCooldown)
	if r.historyLoad.cpuBurstReady(time.Now()) && r.cfg.CatchupHeavyWorkCooldown <= 3*time.Second {
		minimum = historyCPUBurstRecovery
	}
	recovery = max(recovery, minimum)
	if failed {
		recovery = max(recovery, time.Minute, r.cfg.CatchupBuildMinInterval)
	}
	return recovery
}

// Saturate the Unix-nanosecond deadline as well as the duration multiplication.
// Otherwise a very large duration can wrap a deadline into the past.
func historyRecoveryDeadline(now time.Time, after time.Duration) time.Time {
	ns := now.UnixNano()
	if after > 0 && ns > math.MaxInt64-int64(after) {
		return time.Unix(0, math.MaxInt64)
	}
	return now.Add(after)
}

func (r *Runner) applyThroughputRecovery(result *PassResult, started, completed time.Time, passErr error) {
	if !r.throughputCatchup() || (!result.HistoryBuildAttempted && !result.HistoryEventAttempted) {
		return
	}
	work := max(time.Duration(0), completed.Sub(started))
	result.HistoryMaintenanceDuration = work
	// Optional merges have an independent gated admission/recovery budget.
	result.HistoryRecoveryCost = max(time.Duration(0), work-result.CompactionDuration)
	r.historyLoad.metric("recovery_cost", int64(result.HistoryRecoveryCost))
	if !result.HistoryForcedBusy && !result.historyWasSyncing && !r.syncActive() && passErr == nil {
		return
	}
	recovery := r.throughputRecovery(result.HistoryRecoveryCost, passErr != nil)
	result.HistoryMinRecovery = recovery
	deadline := historyRecoveryDeadline(completed, recovery)
	r.extendHistoryNotBefore(deadline)
	deadline = time.Unix(0, r.historyNotBefore.Load())
	if deadline.After(result.HistoryRetryDeadline) {
		result.HistoryRetryDeadline = deadline
	}
	result.refreshHistoryRetry(completed)
}

func (r *Runner) extendHistoryNotBefore(deadline time.Time) {
	for old := r.historyNotBefore.Load(); deadline.UnixNano() > old; old = r.historyNotBefore.Load() {
		if r.historyNotBefore.CompareAndSwap(old, deadline.UnixNano()) {
			return
		}
	}
}

// OnePassWithDeferredMaintenanceContext is for an ordered lifecycle which has
// additional work after the builder returns. The caller must call
// CompleteHistoryMaintenance exactly once, including on error. An inner
// provisional deadline protects the handoff; complete success rates wait for
// the outer completion. Ordinary standalone calls finalize automatically.
func (r *Runner) OnePassWithDeferredMaintenanceContext(ctx context.Context, beforeMerge func(context.Context, PassResult) error) (PassResult, error) {
	return r.onePassWithMaintenanceContext(ctx, beforeMerge, true)
}

// CompleteHistoryMaintenance accounts the complete outer wall time once. It
// does not repeat publication/build counters and never shortens a deadline.
func (r *Runner) CompleteHistoryMaintenance(result *PassResult, started time.Time, passErr error) {
	if r == nil || result == nil {
		return
	}
	r.passMu.Lock()
	defer r.passMu.Unlock()
	r.completeHistoryMaintenance(result, started, time.Now(), passErr)
}

func (r *Runner) completeHistoryMaintenance(result *PassResult, started, completed time.Time, passErr error) {
	if !r.throughputCatchup() || !result.historyCompletionPending {
		return
	}
	result.historyCompletionPending = false
	if result.historyMaintenanceID <= r.completedMaintenanceID {
		return
	}
	if r.pendingMaintenanceID != 0 && r.pendingMaintenanceID != result.historyMaintenanceID {
		return
	}
	r.pendingMaintenanceID = 0
	r.completedMaintenanceID = result.historyMaintenanceID
	r.applyThroughputRecovery(result, started, completed, passErr)
	// Completed construction remains a density observation even when a later
	// prune/catalog operation fails. Successful frontier accounting stays below.
	r.recordHistoryWork(result)
	r.recordEventWork(result)
	r.lastMaintenanceDuration.Store(int64(result.HistoryMaintenanceDuration))
	if result.Built {
		r.lastHistoryBuildAt.Store(completed.UnixNano())
	}
	if result.HistoryForcedBusy {
		r.lastForcedBusyAttemptAt.Store(completed.UnixNano())
		r.lastForcedAttemptRecovery.Store(int64(result.HistoryMinRecovery))
		r.lastForcedRecovery.Store(int64(result.HistoryMinRecovery))
		r.lastForcedDutyPPM.Store(coldSnapshotDutyPPM(result.HistoryMaintenanceDuration, result.HistoryMinRecovery))
		if result.Built && passErr == nil {
			lag := uint64(0)
			if result.EligibleCutoffBlock > result.PublishedBlock {
				lag = result.EligibleCutoffBlock - result.PublishedBlock
			}
			r.recordSuccessfulForcedBuild(result.HistoryBatchBlocks, lag, completed)
		} else {
			// A failed outer pass can already have published history. Do not
			// attribute that uncounted progress to the next successful batch.
			r.lastSuccessfulForcedAt.Store(0)
			r.lastSuccessfulForcedLag.Store(0)
			r.lastForcedCompletionInterval.Store(0)
			r.lastForcedGrossRateMilli.Store(0)
			r.lastForcedNetCatchupRateMilli.Store(0)
		}
	}
	r.updateMetrics()
	ctx := []any{
		"publishedBlock", result.PublishedBlock, "blocks", result.HistoryBatchBlocks,
		"work", result.HistoryMaintenanceDuration, "recovery", result.HistoryMinRecovery,
		"recoveryCost", result.HistoryRecoveryCost, "mergeWork", result.CompactionDuration, "budgetLevel", r.historyLoad.level,
		"retryAt", result.HistoryRetryDeadline, "failed", passErr != nil,
	}
	if errors.Is(passErr, context.Canceled) || errors.Is(passErr, context.DeadlineExceeded) {
		coldSnapshotLog.Info("History throughput maintenance canceled", append(ctx, "err", passErr)...)
	} else if passErr != nil {
		coldSnapshotLog.Warn("History throughput maintenance failed", append(ctx, "err", passErr)...)
	} else {
		// The sampled publication already reports normal progress at Info.
		coldSnapshotLog.Debug("History throughput maintenance completed", ctx...)
	}
}
