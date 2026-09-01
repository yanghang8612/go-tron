package pebbledb

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestPointReadMetricsObserveIteratorStats(t *testing.T) {
	m := newPointReadMetrics("test/point_read_metrics/")
	stats := pebble.IteratorStats{}
	stats.ForwardSeekCount[pebble.InterfaceCall] = 7
	stats.ForwardSeekCount[pebble.InternalIterCall] = 19
	stats.InternalStats.BlockBytes = 8192
	stats.InternalStats.BlockBytesInCache = 6144
	stats.InternalStats.BlockReadDuration = 3 * time.Millisecond
	stats.InternalStats.PointCount = 5
	m.observe(stats, pebble.IteratorMetrics{ReadAmp: 4})

	for name, gotWant := range map[string][2]int64{
		"cursors":            {m.cursors.Snapshot().Count(), 1},
		"seek calls":         {m.seekCalls.Snapshot().Count(), 7},
		"internal seeks":     {m.internalSeeks.Snapshot().Count(), 19},
		"block bytes":        {m.blockBytes.Snapshot().Count(), 8192},
		"cached block bytes": {m.blockBytesCached.Snapshot().Count(), 6144},
		"block read nanos":   {m.blockReadNanos.Snapshot().Count(), int64(3 * time.Millisecond)},
		"point count":        {m.pointCount.Snapshot().Count(), 5},
		"read amp sum":       {m.readAmpSum.Snapshot().Count(), 4},
	} {
		if gotWant[0] != gotWant[1] {
			t.Fatalf("%s = %d, want %d", name, gotWant[0], gotWant[1])
		}
	}
}
