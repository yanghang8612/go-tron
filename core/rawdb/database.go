package rawdb

import (
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
)

func NewPebbleDB(path string, cache int, handles int) (ethdb.KeyValueStore, error) {
	return pebble.New(path, cache, handles, "", false)
}

func NewMemoryDatabase() ethdb.KeyValueStore {
	return memorydb.New()
}

// WrapKeyValueStore wraps an ethdb.KeyValueStore into a full ethdb.Database.
func WrapKeyValueStore(db ethdb.KeyValueStore) ethdb.Database {
	return ethrawdb.NewDatabase(db)
}
