package snapshots

import (
	"math"
	"time"
)

// historyDensityWork removes only directly timed metadata regions from the
// batch-size estimate. Everything else remains chargeable, including content
// authentication, scans, compression, deletes and batch commits. This is not a
// fixed/per-row regression model: an unmeasured callback retains its whole cost.
// The separate complete-lifecycle recovery calculation must not use this value.
//
// density_measurement: 0 = no metadata timing (conservative original estimate),
// 1 = validated measured metadata, 2 = inconsistent timing (conservative fallback).
func historyDensityWork(result *PassResult) (work, metadata time.Duration, measurement int64) {
	if result == nil {
		return 0, 0, 0
	}
	work = historyWorkDurationSum(result.BuildDuration, result.BeforeMergeDuration)
	for _, phase := range []struct{ total, metadata time.Duration }{
		{result.BuildDuration, result.HistoryMetadataDuration},
		{result.BeforeMergeDuration, result.BeforeMergeMetadataDuration},
	} {
		if phase.total < 0 || phase.metadata < 0 || phase.metadata > phase.total {
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
	if metadata == 0 {
		return work, 0, 0
	}
	// Subtract before adding to avoid two saturated totals hiding real row work.
	work = historyWorkDurationSum(result.BuildDuration-result.HistoryMetadataDuration,
		result.BeforeMergeDuration-result.BeforeMergeMetadataDuration)
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
