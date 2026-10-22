package rawdb

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/pointread"
)

// Open both cursors on the already pinned dual-engine snapshot. Routing stays
// identical to directHistorySnapshot.Get, including non-storage/history keys.
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
