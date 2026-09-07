package snapshots

import (
	"context"
	"fmt"
	"time"
)

// HistoryPressure is a measured scheduling signal. HotHistoryBytes may be an
// SST-range estimate including obsolete versions. FreeBytes must describe the
// volume where new snapshot and ETL output is written. Separate availability
// bits distinguish a measured zero from an unavailable measurement.
type HistoryPressure struct {
	HotHistoryBytes          uint64
	FreeBytes                uint64
	HotHistoryBytesAvailable bool
	FreeBytesAvailable       bool
}

func (r *Runner) readHistoryPressure(ctx context.Context) (HistoryPressure, error) {
	if !r.cfg.Enabled || r.cfg.HistoryPressureProbe == nil {
		return HistoryPressure{}, nil
	}
	p, err := r.cfg.HistoryPressureProbe(ctx)
	if err != nil {
		return p, fmt.Errorf("snapshots: history pressure probe: %w", err)
	}
	return p, ctx.Err()
}

func (r *Runner) historySpaceDeferred(p HistoryPressure) bool {
	return p.FreeBytesAvailable && r.cfg.MinHistoryBuildFreeBytes > 0 && p.FreeBytes < r.cfg.MinHistoryBuildFreeBytes
}

func (r *Runner) historyPressureActive(p HistoryPressure) bool {
	return p.HotHistoryBytesAvailable && r.cfg.HistoryPressureHotBytes > 0 && p.HotHistoryBytes >= r.cfg.HistoryPressureHotBytes ||
		p.FreeBytesAvailable && r.cfg.HistoryPressureFreeBytes > 0 && p.FreeBytes <= r.cfg.HistoryPressureFreeBytes
}

func pressureHistoryBatchLimit(configured uint64) uint64 {
	if configured == 0 {
		return 0
	}
	return max(uint64(1), configured/maxForcedBusyBatchDivisor)
}

// Under measured storage pressure a successful bounded busy batch receives
// roughly one work interval of importer recovery, capped at half the ordinary
// interval. The shared heavy-work lease/cooldown is never bypassed. Failures use
// the old full recovery; this policy cannot spin on failed builds.
func (r *Runner) pressureHistoryRecovery(work time.Duration) time.Duration {
	if work <= 0 {
		return r.forcedBusyHistoryRecovery(0)
	}
	ceiling := r.cfg.CatchupBuildMinInterval / 2
	floor := r.cfg.CatchupHeavyWorkCooldown
	if ceiling < floor {
		ceiling = floor
	}
	if ceiling <= 0 {
		return 0
	}
	return min(ceiling, max(floor, work))
}
