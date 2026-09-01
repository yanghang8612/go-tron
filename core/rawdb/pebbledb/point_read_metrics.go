package pebbledb

import (
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

// pointReadMetrics attributes Pebble's iterator-level block work to the
// commitment parent cursors. BlockBytes includes bytes satisfied by Pebble's
// block cache; subtract BlockBytesInCache to obtain bytes that required a VFS
// fetch. The VFS counters in physical_read_metrics.go separately report the
// actual SST ReadAt call boundary.
type pointReadMetrics struct {
	cursors          *metrics.Counter
	seekCalls        *metrics.Counter
	internalSeeks    *metrics.Counter
	blockBytes       *metrics.Counter
	blockBytesCached *metrics.Counter
	blockReadNanos   *metrics.Counter
	pointCount       *metrics.Counter
	readAmpSum       *metrics.Counter
}

// Commitment parent cursors are a process-wide subsystem, matching the
// blockbuffer metric namespace. Keeping the recorder package-global avoids
// adding one pointer to every reserved cursor (normally 33 per fold).
var commitmentPointReadMetrics = newPointReadMetrics("")

func newPointReadMetrics(namespace string) *pointReadMetrics {
	prefix := namespace + "blockbuffer/commitment_parent/pebble/"
	return &pointReadMetrics{
		cursors:          metrics.GetOrRegisterCounter(prefix+"cursors", nil),
		seekCalls:        metrics.GetOrRegisterCounter(prefix+"seek_calls", nil),
		internalSeeks:    metrics.GetOrRegisterCounter(prefix+"internal_seek_calls", nil),
		blockBytes:       metrics.GetOrRegisterCounter(prefix+"block_bytes", nil),
		blockBytesCached: metrics.GetOrRegisterCounter(prefix+"block_bytes_cached", nil),
		blockReadNanos:   metrics.GetOrRegisterCounter(prefix+"block_read_nanos", nil),
		pointCount:       metrics.GetOrRegisterCounter(prefix+"point_count", nil),
		readAmpSum:       metrics.GetOrRegisterCounter(prefix+"read_amp_sum", nil),
	}
}

func (m *pointReadMetrics) observe(stats pebble.IteratorStats, iteratorMetrics pebble.IteratorMetrics) {
	if m == nil {
		return
	}
	m.cursors.Inc(1)
	m.seekCalls.Inc(int64(stats.ForwardSeekCount[pebble.InterfaceCall]))
	m.internalSeeks.Inc(int64(stats.ForwardSeekCount[pebble.InternalIterCall]))
	m.blockBytes.Inc(int64(stats.InternalStats.BlockBytes))
	m.blockBytesCached.Inc(int64(stats.InternalStats.BlockBytesInCache))
	m.blockReadNanos.Inc(stats.InternalStats.BlockReadDuration.Nanoseconds())
	m.pointCount.Inc(int64(stats.InternalStats.PointCount))
	m.readAmpSum.Inc(int64(iteratorMetrics.ReadAmp))
}
