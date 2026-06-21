package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

func readPresentValue(db ethdb.KeyValueReader, key []byte, context string) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database while reading %s", context)
	}
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read %s presence: %w", context, err)
	}
	if !exists {
		return nil, false, nil
	}
	value, err := db.Get(key)
	if err != nil {
		return nil, false, fmt.Errorf("rawdb: read %s: %w", context, err)
	}
	return append([]byte(nil), value...), true, nil
}
