package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

func readPresentValue(db ethdb.KeyValueReader, key []byte, context string) ([]byte, bool, error) {
	exists, err := readKeyPresence(db, key, context)
	if err != nil {
		return nil, false, err
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

func readKeyPresence(db ethdb.KeyValueReader, key []byte, context string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("rawdb: nil database while reading %s", context)
	}
	exists, err := db.Has(key)
	if err != nil {
		return false, fmt.Errorf("rawdb: read %s presence: %w", context, err)
	}
	return exists, nil
}

func readValueThenVerifyMiss(db ethdb.KeyValueReader, key []byte, context string, read func([]byte) ([]byte, error)) ([]byte, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database while reading %s", context)
	}
	if read == nil {
		read = db.Get
	}
	value, err := read(key)
	if err == nil {
		return value, true, nil
	}
	exists, hasErr := db.Has(key)
	if hasErr != nil {
		return nil, false, fmt.Errorf("rawdb: read %s presence after get error: %w", context, hasErr)
	}
	if !exists {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("rawdb: read %s: %w", context, err)
}
