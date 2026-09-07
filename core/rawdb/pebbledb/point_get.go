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
// the callback, including its error/panic paths. Unbounded sorted state-prefetch
// cursors continue using the reusable iterator.
func (c *pointReadCursor) viewGet(key []byte, fn func([]byte) error) (found bool, err error) {
	if !bytes.HasPrefix(key, c.prefix) {
		return false, nil
	}
	started := time.Now()
	value, closer, readErr := c.getSnapshot.Get(key)
	c.getNanos += uint64(time.Since(started))
	c.getCalls++
	if errors.Is(readErr, pebble.ErrNotFound) {
		return false, nil
	}
	if readErr != nil {
		c.getErrors++
		return false, readErr
	}
	c.getHits++
	defer func() {
		if closeErr := closer.Close(); closeErr != nil {
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
