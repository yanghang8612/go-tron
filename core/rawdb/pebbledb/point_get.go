package pebbledb

import (
	"bytes"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

// viewGet searches levels lazily and stops at the first MVCC-visible value.
// A merging iterator seeks every overlapping level even when an upper table
// already contains the answer. Keep the original snapshot sequence, prefix
// restriction and callback-scoped ownership; Get's closer must survive through
// the callback, including its error/panic paths. Unbounded state-prefetch
// cursors discard a failed inner cursor so the next View can retry at the same
// snapshot; bounded commitment cursors retain their existing poisoned state.
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
		// Unbounded cursors currently serve state prefetch, whose key mix
		// includes misses. Commitment prefixes keep the default L6 read policy.
		c.getCursor, readErr = c.getSnapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{UseL6Filters: c.unbounded})
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
		if c.unbounded {
			readErr = c.discardUnboundedGet(readErr)
		}
		return false, readErr
	}
	c.getHits++
	defer func() {
		if closeErr := c.getCursor.ReleaseValue(); closeErr != nil {
			c.getErrors++
			if c.unbounded {
				closeErr = c.discardUnboundedGet(closeErr)
				err = errors.Join(err, closeErr)
			} else if err == nil {
				err = closeErr
			}
		}
	}()
	return true, fn(value)
}

// closeGetCursor detaches and publishes each inner cursor exactly once. An
// unbounded logical cursor may own several inners after recoverable failures;
// its wrapper-level counters are still published only at wrapper Close.
func (c *pointReadCursor) closeGetCursor() error {
	inner := c.getCursor
	if inner == nil {
		return nil
	}
	c.getCursor = nil
	err := inner.Close()
	if c.unbounded {
		statePrefetchGetCursorMetrics.observe(inner.Stats())
		statePrefetchGetCursorInners.Inc(1)
	} else {
		commitmentGetCursorMetrics.observe(inner.Stats())
	}
	return err
}

func (c *pointReadCursor) discardUnboundedGet(err error) error {
	err = errors.Join(err, c.closeGetCursor())
	c.lastUnboundedError = err
	return err
}

// Pebble v1.1.5 Get does not expose internal block stats (getIter's upstream
// TODO). Do not put synthetic zero samples in the iterator's counters. These
// separate metrics measure logical Get calls and lookup wall time, excluding
// callback time; SST ReadAt remains observable through physicalReadFS.
type pointGetMetrics struct {
	cursors, calls, hits, errors, nanos *metrics.Counter
}

var commitmentPointGetMetrics = newPointGetMetrics("blockbuffer/commitment_parent/pebble/get/")
var statePrefetchPointGetMetrics = newPointGetMetrics("blockbuffer/state_prefetch/pebble/get/")

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

var (
	commitmentGetCursorMetrics = newGetCursorMetrics("blockbuffer/commitment_parent/pebble/get_cursor/", "retained_peak_sum")
	// Recreated inner cursors contribute separately to the peak sum. These are
	// handle counts, not pinned-byte or block-cache capacity measurements.
	statePrefetchGetCursorMetrics = newGetCursorMetrics("blockbuffer/state_prefetch/pebble/get_cursor/", "inner_retained_peak_sum")
	statePrefetchGetCursorInners  = metrics.GetOrRegisterCounter("blockbuffer/state_prefetch/pebble/get_cursor/inner_cursors", nil)
)

func newGetCursorMetrics(prefix, peakName string) *getCursorMetrics {
	return &getCursorMetrics{
		requests:  metrics.GetOrRegisterCounter(prefix+"table_requests", nil),
		opens:     metrics.GetOrRegisterCounter(prefix+"table_iterator_opens", nil),
		reuses:    metrics.GetOrRegisterCounter(prefix+"table_reuses", nil),
		fallbacks: metrics.GetOrRegisterCounter(prefix+"table_fallbacks", nil),
		evictions: metrics.GetOrRegisterCounter(prefix+"table_evictions", nil),
		peakSum:   metrics.GetOrRegisterCounter(prefix+peakName, nil),
	}
}

func (m *getCursorMetrics) observe(s pebble.GetCursorStats) {
	m.requests.Inc(int64(s.TableRequests))
	m.opens.Inc(int64(s.TableOpens))
	m.reuses.Inc(int64(s.TableReuses))
	m.fallbacks.Inc(int64(s.TableFallbacks))
	m.evictions.Inc(int64(s.TableEvictions))
	m.peakSum.Inc(int64(s.MaxRetained))
}
