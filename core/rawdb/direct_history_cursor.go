package rawdb

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/pointread"
)

// Open both cursors on the already pinned dual-engine snapshot. Routing stays
// identical to directHistorySnapshot.Get, including non-storage/history keys.
// The canonical Pebble factories each retain at most 16 iterator pairs (32
// across both engines / 64 retained point-plus-range component handles).
// Each uses inline four pairs, allocating fixed overflow for the remaining 12
// only on a compatible miss needing a fifth distinct retained SST. Neither
// backing moves. These retained bounds exclude transient fallback resources
// and do not bound pinned bytes.
func (s *directHistorySnapshot) NewPointReadCursor() (pointread.Cursor, error) {
	hotFactory, hotOK := s.hot.(pointread.KeyValueCursorSnapshot)
	historyFactory, historyOK := s.history.(pointread.KeyValueCursorSnapshot)
	if !hotOK || !historyOK {
		return nil, pointread.ErrPointReadCursorUnsupported
	}
	hot, err := hotFactory.NewPointReadCursor()
	if err != nil {
		return nil, err
	}
	history, err := historyFactory.NewPointReadCursor()
	if err != nil {
		return nil, errors.Join(err, hot.Close())
	}
	return &directHistoryKeyValueCursor{hot: hot, history: history}, nil
}

type directHistoryKeyValueCursor struct{ hot, history pointread.Cursor }

func (c *directHistoryKeyValueCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	if IsDirectHistoryKey(key) {
		return c.history.View(key, fn)
	}
	return c.hot.View(key, fn)
}

func (c *directHistoryKeyValueCursor) Close() error {
	return errors.Join(c.hot.Close(), c.history.Close())
}
