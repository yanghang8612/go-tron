package snapshots

import "time"

// An active lease has no completion timestamp. Keep a bounded wake opportunity
// instead of dropping back to the coarse lifecycle tick; actual admission still
// belongs to the shared gate and the persisted history recovery deadline.
func (r *Runner) deferHistoryForHeavyWork(result *PassResult) {
	result.HistoryDeferred, result.HistoryGateDeferred = true, true
	now := time.Now()
	after := r.cfg.HeavyWorkGate.CooldownRemaining()
	if after <= 0 {
		after = 3 * time.Second
	}
	result.extendHistoryRetryDeadline(now, after)
	if deadline := r.historyNotBefore.Load(); deadline > result.HistoryRetryDeadline.UnixNano() {
		result.HistoryRetryDeadline = time.Unix(0, deadline)
		result.refreshHistoryRetry(now)
	}
}
