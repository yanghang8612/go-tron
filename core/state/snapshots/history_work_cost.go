package snapshots

import (
	"math"
	"time"
)

// historyDensityWork excludes directly timed metadata and the independent
// shared-chunk GC phase from the current history batch-size estimate. Current
// batch authentication, scans, compression, hot-row deletes and commits remain
// chargeable. Independent GC still performs every proof and retirement; its
// bucket count does not scale with the current history range. An unmeasured
// callback retains its whole cost; the measured regions must be disjoint.
// The separate complete-lifecycle recovery calculation must not use this value.
//
// density_measurement: 0 = no excluded timing (conservative original estimate),
// 1 = validated measured regions, 2 = inconsistent timing (conservative fallback).
func historyDensityWork(result *PassResult) (work, metadata time.Duration, measurement int64) {
	if result == nil {
		return 0, 0, 0
	}
	work = historyWorkDurationSum(result.BuildDuration, result.BeforeMergeDuration)
	for _, phase := range []struct{ total, metadata, gc time.Duration }{
		{result.BuildDuration, result.HistoryMetadataDuration, 0},
		{result.BeforeMergeDuration, result.BeforeMergeMetadataDuration, result.BeforeMergeHistoryGCDuration},
	} {
		if phase.total < 0 || phase.metadata < 0 || phase.gc < 0 || phase.metadata > phase.total || phase.gc > phase.total-phase.metadata {
			if work == 0 {
				// An impossible zero/negative observation provides no row-rate
				// estimate. Treat it as over budget rather than resetting to a
				// potentially much larger configured/4 initial batch.
				work = time.Duration(math.MaxInt64)
			}
			return work, 0, 2
		}
	}
	metadata = historyWorkDurationSum(result.HistoryMetadataDuration, result.BeforeMergeMetadataDuration)
	if metadata == 0 && result.BeforeMergeHistoryGCDuration == 0 {
		return work, 0, 0
	}
	// Subtract before adding to avoid two saturated totals hiding real row work.
	work = historyWorkDurationSum(result.BuildDuration-result.HistoryMetadataDuration,
		result.BeforeMergeDuration-result.BeforeMergeMetadataDuration-result.BeforeMergeHistoryGCDuration)
	// A timer-resolution-sized batch is still an observation; zero would reset
	// the controller to its initial configured/4 batch and bypass bounded growth.
	return max(time.Nanosecond, work), metadata, 1
}

func historyWorkDurationSum(values ...time.Duration) time.Duration {
	var sum time.Duration
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if value > time.Duration(math.MaxInt64)-sum {
			return time.Duration(math.MaxInt64)
		}
		sum += value
	}
	return sum
}
