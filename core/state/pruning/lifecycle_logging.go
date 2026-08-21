package pruning

import (
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

const snapshotLifecycleFailureSummaryInterval = 10 * time.Minute

var (
	snapshotLifecyclePassFailureCounter       = metrics.NewRegisteredCounter("state/lifecycle/pass_failures", nil)
	snapshotLifecycleFailureSuppressedCounter = metrics.NewRegisteredCounter("state/lifecycle/failure_logs_suppressed", nil)
	snapshotLifecycleRecoveryCounter          = metrics.NewRegisteredCounter("state/lifecycle/recoveries", nil)
	snapshotLifecycleFailureActiveGauge       = metrics.NewRegisteredGauge("state/lifecycle/failure_active", nil)
)

type snapshotLifecycleFailureLogState struct {
	key              string
	err              string
	firstFailure     time.Time
	lastSummary      time.Time
	attempts         uint64
	reportedAttempts uint64
}

type snapshotLifecycleFailureLogDecision struct {
	log        bool
	first      bool
	changed    bool
	previous   string
	err        string
	attempts   uint64
	suppressed uint64
	failedFor  time.Duration
}

func snapshotLifecycleFailureKey(err error) string {
	var coverage *snapshots.HistoryCoverageError
	if errors.As(err, &coverage) {
		return "history-coverage:" + string(coverage.Dataset)
	}
	return err.Error()
}

func (s *snapshotLifecycleFailureLogState) observeFailure(now time.Time, err error) snapshotLifecycleFailureLogDecision {
	key := snapshotLifecycleFailureKey(err)
	message := err.Error()
	if s.key == "" || s.key != key {
		previous := s.err
		*s = snapshotLifecycleFailureLogState{
			key:              key,
			err:              message,
			firstFailure:     now,
			lastSummary:      now,
			attempts:         1,
			reportedAttempts: 1,
		}
		return snapshotLifecycleFailureLogDecision{
			log:      true,
			first:    previous == "",
			changed:  previous != "",
			previous: previous,
			err:      message,
			attempts: 1,
		}
	}

	s.err = message
	s.attempts++
	if now.Sub(s.lastSummary) < snapshotLifecycleFailureSummaryInterval {
		return snapshotLifecycleFailureLogDecision{}
	}
	suppressed := s.attempts - s.reportedAttempts - 1
	decision := snapshotLifecycleFailureLogDecision{
		log:        true,
		err:        s.err,
		attempts:   s.attempts,
		suppressed: suppressed,
		failedFor:  now.Sub(s.firstFailure),
	}
	s.lastSummary = now
	s.reportedAttempts = s.attempts
	return decision
}

func (s *snapshotLifecycleFailureLogState) observeRecovery(now time.Time) snapshotLifecycleFailureLogDecision {
	if s.key == "" {
		return snapshotLifecycleFailureLogDecision{}
	}
	decision := snapshotLifecycleFailureLogDecision{
		log:       true,
		err:       s.err,
		attempts:  s.attempts,
		failedFor: now.Sub(s.firstFailure),
	}
	*s = snapshotLifecycleFailureLogState{}
	return decision
}

func (l *SnapshotLifecycle) logPassFailure(reason string, err error, now time.Time) {
	if l == nil || err == nil {
		return
	}
	snapshotLifecyclePassFailureCounter.Inc(1)
	snapshotLifecycleFailureActiveGauge.Update(1)

	l.failureLogMu.Lock()
	decision := l.failureLog.observeFailure(now, err)
	l.failureLogMu.Unlock()
	if !decision.log {
		snapshotLifecycleFailureSuppressedCounter.Inc(1)
		return
	}

	ctx := snapshotLifecycleFailureContext(reason, err)
	if decision.changed {
		ctx = append(ctx, "previousErr", decision.previous)
	}
	if decision.first || decision.changed {
		lifecycleLog.Warn("Domain state snapshot/prune pass failed", ctx...)
		return
	}
	ctx = append(ctx,
		"failures", decision.attempts,
		"suppressedSinceLastLog", decision.suppressed,
		"failedFor", decision.failedFor.Round(time.Second),
		"nextSummaryAfter", snapshotLifecycleFailureSummaryInterval)
	lifecycleLog.Warn("Domain state snapshot/prune pass still failing", ctx...)
}

func (l *SnapshotLifecycle) logPassRecovery(now time.Time) {
	if l == nil {
		return
	}
	l.failureLogMu.Lock()
	decision := l.failureLog.observeRecovery(now)
	l.failureLogMu.Unlock()
	if !decision.log {
		return
	}
	snapshotLifecycleFailureActiveGauge.Update(0)
	snapshotLifecycleRecoveryCounter.Inc(1)
	lifecycleLog.Info("Domain state snapshot/prune pass recovered",
		"previousErr", decision.err,
		"failures", decision.attempts,
		"failedFor", decision.failedFor.Round(time.Second))
}

func snapshotLifecycleFailureContext(reason string, err error) []any {
	ctx := []any{"reason", reason, "err", err}
	var coverage *snapshots.HistoryCoverageError
	if errors.As(err, &coverage) {
		ctx = append(ctx,
			"failureKind", "history_coverage_gap",
			"dataset", coverage.Dataset,
			"historyProgress", coverage.Progress,
			"visibleCoverage", coverage.VisibleEnd,
			"coverageGap", coverage.Gap())
	}
	return ctx
}
