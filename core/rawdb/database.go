package rawdb

import (
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
