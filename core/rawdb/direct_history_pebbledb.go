package rawdb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

// NewDirectHistoryPebbleDB opens the history partition used by normal block
// processing. Its independent cache budget and metrics namespace keep the two
// engines observable, while its write budget supports foreground history writes.
func NewDirectHistoryPebbleDB(path string, cache, handles int, readOnly bool) (ethdb.KeyValueStore, error) {
	if path == "" || cache <= 0 || handles <= 0 {
		return nil, errors.New("rawdb: invalid direct history database options")
	}
	tune := pebbledb.DefaultOptions()
	tune.MemTableSizeBytes = 128 << 20
	tune.MaxConcurrentCompactions = 2
	if !readOnly {
		var err error
		tune, err = withDiskSpaceObservation(tune)
		if err != nil {
			return nil, err
		}
	}
	return pebbledb.New(path, cache, handles, "history/", readOnly, tune)
}
