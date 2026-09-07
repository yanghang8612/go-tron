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
	recovery := time.Duration(math.MaxInt64)
	if work <= time.Duration(math.MaxInt64/forcedBusyRecoveryWorkMultiplier) {
		recovery = max(time.Duration(0), work) * time.Duration(forcedBusyRecoveryWorkMultiplier)
	}
	recovery = max(recovery, 3*time.Second, r.cfg.CatchupHeavyWorkCooldown)
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
	if !r.throughputCatchup() || !result.HistoryBuildAttempted {
		return
	}
	work := max(time.Duration(0), completed.Sub(started))
	result.HistoryMaintenanceDuration = work
	if !result.HistoryForcedBusy && passErr == nil {
		return
	}
	recovery := r.throughputRecovery(work, passErr != nil)
	result.HistoryMinRecovery = recovery
	deadline := historyRecoveryDeadline(completed, recovery)
	for old := r.historyNotBefore.Load(); deadline.UnixNano() > old; old = r.historyNotBefore.Load() {
		if r.historyNotBefore.CompareAndSwap(old, deadline.UnixNano()) {
			break
		}
	}
	deadline = time.Unix(0, r.historyNotBefore.Load())
	if deadline.After(result.HistoryRetryDeadline) {
		result.HistoryRetryDeadline = deadline
	}
	result.refreshHistoryRetry(completed)
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
	coldSnapshotLog.Info("History throughput maintenance completed",
		"publishedBlock", result.PublishedBlock, "blocks", result.HistoryBatchBlocks,
		"work", result.HistoryMaintenanceDuration, "recovery", result.HistoryMinRecovery,
		"retryAt", result.HistoryRetryDeadline, "failed", passErr != nil)
}
