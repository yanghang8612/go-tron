package pebbledb

import (
	"bytes"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

// viewGet searches levels lazily and stops at the first MVCC-visible value.
// A merging iterator seeks every overlapping level even when an upper table
// already contains the answer. Keep the original snapshot sequence, prefix
// restriction and callback-scoped ownership; Get's closer must survive through
// the callback, including its error/panic paths. Unbounded sorted state-prefetch
// cursors continue using the reusable iterator.
func (c *pointReadCursor) viewGet(key []byte, fn func([]byte) error) (found bool, err error) {
	if !bytes.HasPrefix(key, c.prefix) {
		return false, nil
	}
	started := time.Now()
	// The MVCC snapshot/lifecycle lease is eager, but an unused cursor need
	// not allocate or pin a second readState. Include first construction in the
	// existing lookup wall measurement.
	var value []byte
	var readErr error
	if c.getCursor == nil {
		c.getCursor, readErr = c.getSnapshot.NewGetCursor()
	}
	if readErr == nil {
		value, readErr = c.getCursor.Get(key)
	}
	c.getNanos += uint64(time.Since(started))
	c.getCalls++
	// Only the exact sentinel means absence. Joined operation/cleanup errors
	// may contain ErrNotFound alongside a real failure and must propagate.
	if readErr == pebble.ErrNotFound {
		return false, nil
	}
	if readErr != nil {
		c.getErrors++
		return false, readErr
	}
	c.getHits++
	defer func() {
		if closeErr := c.getCursor.ReleaseValue(); closeErr != nil {
			c.getErrors++
			if err == nil {
				err = closeErr
			}
		}
	}()
	return true, fn(value)
}

// Pebble v1.1.5 Get does not expose internal block stats (getIter's upstream
// TODO). Do not put synthetic zero samples in the iterator's counters. These
// separate metrics measure logical Get calls and lookup wall time, excluding
// callback time; SST ReadAt remains observable through physicalReadFS.
type pointGetMetrics struct {
	cursors, calls, hits, errors, nanos *metrics.Counter
}

var commitmentPointGetMetrics = newPointGetMetrics("blockbuffer/commitment_parent/pebble/get/")

func newPointGetMetrics(prefix string) *pointGetMetrics {
	return &pointGetMetrics{
		cursors: metrics.GetOrRegisterCounter(prefix+"cursors", nil),
		calls:   metrics.GetOrRegisterCounter(prefix+"calls", nil),
		hits:    metrics.GetOrRegisterCounter(prefix+"hits", nil),
		errors:  metrics.GetOrRegisterCounter(prefix+"errors", nil),
		nanos:   metrics.GetOrRegisterCounter(prefix+"nanos", nil),
	}
}

func (m *pointGetMetrics) observe(c *pointReadCursor) {
	m.cursors.Inc(1)
	m.calls.Inc(int64(c.getCalls))
	m.hits.Inc(int64(c.getHits))
	m.errors.Inc(int64(c.getErrors))
	m.nanos.Inc(int64(c.getNanos))
}

// Cursor reuse counts are new measurements, not zero-valued historical Get stats.
// Each cursor records locally; only Close publishes to the process counters.
type getCursorMetrics struct {
	requests, opens, reuses, fallbacks, evictions, peakSum *metrics.Counter
}

var commitmentGetCursorMetrics = &getCursorMetrics{
	requests:  metrics.GetOrRegisterCounter("blockbuffer/commitment_parent/pebble/get_cursor/table_requests", nil),
	opens:     metrics.GetOrRegisterCounter("blockbuffer/commitment_parent/pebble/get_cursor/table_iterator_opens", nil),
	reuses:    metrics.GetOrRegisterCounter("blockbuffer/commitment_parent/pebble/get_cursor/table_reuses", nil),
	fallbacks: metrics.GetOrRegisterCounter("blockbuffer/commitment_parent/pebble/get_cursor/table_fallbacks", nil),
	evictions: metrics.GetOrRegisterCounter("blockbuffer/commitment_parent/pebble/get_cursor/table_evictions", nil),
	peakSum:   metrics.GetOrRegisterCounter("blockbuffer/commitment_parent/pebble/get_cursor/retained_peak_sum", nil),
}

func (m *getCursorMetrics) observe(s pebble.GetCursorStats) {
	m.requests.Inc(int64(s.TableRequests))
	m.opens.Inc(int64(s.TableOpens))
	m.reuses.Inc(int64(s.TableReuses))
	m.fallbacks.Inc(int64(s.TableFallbacks))
	m.evictions.Inc(int64(s.TableEvictions))
	m.peakSum.Inc(int64(s.MaxRetained))
}
