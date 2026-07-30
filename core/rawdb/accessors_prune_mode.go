package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// WriteHistoryPruneMode records the operator-selected history/state retention
// mode for this datadir. The value is intentionally not part of any consensus
// state root; it protects local storage layout assumptions across restarts.
func WriteHistoryPruneMode(db ethdb.KeyValueWriter, mode string) error {
	if db == nil {
		return errors.New("rawdb: nil history prune mode writer")
	}
	if mode == "" {
		return errors.New("rawdb: empty history prune mode")
	}
	return db.Put(historyPruneModeKey, []byte(mode))
}

// ReadHistoryPruneMode returns the mode previously stored by
// WriteHistoryPruneMode. Missing rows are reported as ok=false so upgraded
// datadirs can be locked on first run with the effective operator mode.
func ReadHistoryPruneMode(db ethdb.KeyValueReader) (mode string, ok bool, err error) {
	if db == nil {
		return "", false, nil
	}
	exists, err := db.Has(historyPruneModeKey)
	if err != nil {
		return "", false, fmt.Errorf("rawdb: read history prune mode presence: %w", err)
	}
	if !exists {
		return "", false, nil
	}
	raw, err := db.Get(historyPruneModeKey)
	if err != nil {
		return "", false, fmt.Errorf("rawdb: read history prune mode: %w", err)
	}
	if len(raw) == 0 {
		return "", true, errors.New("rawdb: empty persisted history prune mode")
	}
	return string(raw), true, nil
}
