package freezer

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

const directV2RetryInterval = 3 * time.Second

// This hint exists only after a complete, receipt-covered direct segment lost
// real gate admission. It is not a permit, a reservation, or a success loop.
func (r *Runner) deferDirectV2Admission() {
	now := time.Now()
	after := r.cfg.HeavyWorkGate.CooldownRemaining()
	r.directV2RetryMu.Lock()
	defer r.directV2RetryMu.Unlock()
	if r.checkStopping() != nil {
		r.clearDirectV2RetryLocked()
		return
	}
	reason := int64(1) // Active owner, reservation, or pressure: no known deadline.
	if after > 0 {
		reason = 2
	} else {
		after = directV2RetryInterval
	}
	deadline := now.Add(after).UnixNano()
	r.directV2RetryAt.Store(deadline)
	r.directV2RetryMetric("deadline_unix_ns", deadline)
	r.directV2RetryMetric("reason", reason)
	r.directV2RetryMetric("last_rejected_unix_ns", now.UnixNano())
	metrics.GetOrRegisterCounter(normalizeMetricNamespace(r.cfg.MetricsNamespace)+"v2/retry/scheduled", nil).Inc(1)
	if r.checkStopping() != nil {
		r.clearDirectV2RetryLocked()
	}
}

func (r *Runner) clearDirectV2Retry() {
	r.directV2RetryMu.Lock()
	defer r.directV2RetryMu.Unlock()
	r.clearDirectV2RetryLocked()
}

func (r *Runner) clearDirectV2RetryLocked() {
	r.directV2RetryAt.Store(0)
	r.directV2RetryMetric("deadline_unix_ns", 0)
	r.directV2RetryMetric("reason", 0)
}

func (r *Runner) directV2RetryDeadline() time.Time {
	if at := r.directV2RetryAt.Load(); at > 0 {
		return time.Unix(0, at)
	}
	return time.Time{}
}

func directV2RetryDelay(deadline, now time.Time) time.Duration {
	if remaining := deadline.Sub(now); remaining > 0 {
		return remaining
	}
	return directV2RetryInterval
}

// Consume before trying workMu. A concurrent explicit OnePass may already own
// the runner; that no-op must not keep rearming an expired hint. CAS also keeps
// a concurrently replaced future hint from being cleared or executed early.
func (r *Runner) takeDirectV2Retry(now time.Time) bool {
	r.directV2RetryMu.Lock()
	defer r.directV2RetryMu.Unlock()
	at := r.directV2RetryAt.Load()
	if at <= 0 || at > now.UnixNano() || !r.directV2RetryAt.CompareAndSwap(at, 0) {
		return false
	}
	r.directV2RetryMetric("deadline_unix_ns", 0)
	r.directV2RetryMetric("reason", 0)
	r.directV2RetryMetric("last_attempt_unix_ns", now.UnixNano())
	r.directV2RetryMetric("last_lateness_ns", max(int64(0), now.UnixNano()-at))
	metrics.GetOrRegisterCounter(normalizeMetricNamespace(r.cfg.MetricsNamespace)+"v2/retry/attempts", nil).Inc(1)
	return true
}

func (r *Runner) directV2RetryMetric(name string, value int64) {
	metrics.GetOrRegisterGauge(normalizeMetricNamespace(r.cfg.MetricsNamespace)+"v2/retry/"+name, nil).Update(value)
}
