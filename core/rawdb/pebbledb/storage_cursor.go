package pebbledb

import (
	"errors"
	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/core/pointread"
)

var storageCursorMetrics = newGetCursorMetrics("blockbuffer/storage_read/pebble/get_cursor/", "retained_peak_sum")

// A foreground storage cursor has its own metrics and owner. It must never be
// counted as a prefetch cursor, nor be reused with a moving database sequence.
type storagePointCursor struct{ inner *pebble.GetCursor }

func (s *keyValueSnapshot) NewPointReadCursor() (pointread.Cursor, error) {
	if s == nil || s.snapshot == nil {
		return nil, pebble.ErrClosed
	}
	// Keep ordinary DB.Get's filter policy. This candidate changes iterator
	// lifetime only, not L6 filter selection or table lookup semantics.
	// Only canonical storage opts into 16 pairs; other cursor roles retain 4.
	// The first four pairs stay inline. A compatible miss needing a fifth
	// distinct retained table allocates fixed overflow for the remaining 12.
	// Neither array moves, including while inline wrappers are leased.
	c, err := s.snapshot.NewGetCursorWithOptions(pebble.GetCursorOptions{MaxRetainedTables: 16})
	if err != nil {
		return nil, err
	}
	return &storagePointCursor{inner: c}, nil
}

func (c *storagePointCursor) View(key []byte, fn func([]byte) error) (found bool, err error) {
	if c == nil || c.inner == nil {
		return false, pebble.ErrClosed
	}
	value, err := c.inner.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() {
		err = errors.Join(err, c.inner.ReleaseValue())
	}()
	return true, fn(value)
}

func (c *storagePointCursor) Close() error {
	if c == nil || c.inner == nil {
		return nil
	}
	inner := c.inner
	c.inner = nil
	err := inner.Close()
	storageCursorMetrics.observe(inner.Stats())
	return err
}
