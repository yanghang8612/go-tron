package net

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const stalledFetchRecoverySummaryInterval = 10 * time.Minute

var (
	stalledFetchRecoveryAttemptCounter    = metrics.NewRegisteredCounter("sync/stalled_fetch/recovery_attempts", nil)
	stalledFetchRecoverySuppressedCounter = metrics.NewRegisteredCounter("sync/stalled_fetch/logs_suppressed", nil)
	stalledFetchRecoveryCounter           = metrics.NewRegisteredCounter("sync/stalled_fetch/recoveries", nil)
)

type stalledFetchRecoveryLogState struct {
	head             uint64
	started          time.Time
	lastSummary      time.Time
	attempts         uint64
	reportedAttempts uint64
}

type stalledFetchRecoveryLogDecision struct {
	first      bool
	summary    bool
	head       uint64
	attempts   uint64
	suppressed uint64
	stalledFor time.Duration
}

func (s *stalledFetchRecoveryLogState) observe(head uint64, now time.Time) stalledFetchRecoveryLogDecision {
	if s.attempts == 0 || s.head != head {
		*s = stalledFetchRecoveryLogState{
			head:             head,
			started:          now,
			lastSummary:      now,
			attempts:         1,
			reportedAttempts: 1,
		}
		return stalledFetchRecoveryLogDecision{first: true, head: head, attempts: 1}
	}
	s.attempts++
	if now.Sub(s.lastSummary) < stalledFetchRecoverySummaryInterval {
		return stalledFetchRecoveryLogDecision{head: head, attempts: s.attempts}
	}
	suppressed := s.attempts - s.reportedAttempts - 1
	decision := stalledFetchRecoveryLogDecision{
		summary:    true,
		head:       head,
		attempts:   s.attempts,
		suppressed: suppressed,
		stalledFor: now.Sub(s.started),
	}
	s.lastSummary = now
	s.reportedAttempts = s.attempts
	return decision
}

func (ss *SyncService) logStalledFetchRecovery(before, after uint64, stillSyncing bool, now time.Time) {
	stalledFetchRecoveryAttemptCounter.Inc(1)
	if after > before || !stillSyncing {
		ss.stalledFetchLogMu.Lock()
		attempts := ss.stalledFetchLog.attempts + 1
		ss.stalledFetchLog = stalledFetchRecoveryLogState{}
		ss.stalledFetchLogMu.Unlock()
		stalledFetchRecoveryCounter.Inc(1)
		syncLog.Info("Stalled sync fetch recovered",
			"fromHead", before,
			"toHead", after,
			"syncing", stillSyncing,
			"attempts", attempts)
		return
	}

	ss.stalledFetchLogMu.Lock()
	decision := ss.stalledFetchLog.observe(after, now)
	ss.stalledFetchLogMu.Unlock()
	switch {
	case decision.first:
		syncLog.Info("Re-kicking stalled sync fetch",
			"head", decision.head,
			"attempt", decision.attempts,
			"warningAfter", stalledFetchRecoverySummaryInterval)
	case decision.summary:
		syncLog.Warn("Sync fetch remains stalled after recovery attempts",
			"head", decision.head,
			"attempts", decision.attempts,
			"suppressedSinceLastLog", decision.suppressed,
			"stalledFor", decision.stalledFor.Round(time.Second),
			"nextSummaryAfter", stalledFetchRecoverySummaryInterval)
	default:
		stalledFetchRecoverySuppressedCounter.Inc(1)
	}
}
