package snapshots

import "time"

// The caller has already established active deep sync and soft < lag <= busy.
// Resource headroom only opens this earlier opportunity; the existing range,
// deadline, shared lease and complete-maintenance recovery checks still apply.
// In particular cpuBurstReady describes storage, so it cannot replace the
// separate CPU/runtime/memory observation supplied by the node.
func (r *Runner) busyHistoryBuildReady(now time.Time) bool {
	return r.throughputCatchup() && r.cfg.HeavyWorkGate != nil &&
		r.cfg.HistoryLoadProbe != nil && r.historyLoad.cpuBurstReady(now) &&
		r.cfg.BusyHistoryBuildReady != nil && r.cfg.BusyHistoryBuildReady()
}
