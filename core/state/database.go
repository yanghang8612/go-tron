package state

import (
	"sync"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
)

const trieNodeBatchPreallocSize = ethdb.IdealBatchSize

// DatabaseConfig tunes the in-process trie database wrapper.
type DatabaseConfig struct {
	// CleanTrieCacheSizeBytes enables geth hashdb's clean-node cache. It caches
	// decoded-once trie node blobs above Pebble so consecutive block commits do
	// not have to re-read hot account/KV trie nodes from compacting SSTables.
	CleanTrieCacheSizeBytes int
}

// Database wraps access to tries.
type Database struct {
	disk              ethdb.Database
	trieDB            *triedb.Database
	trieNodeBatchPool sync.Pool
}

// NewDatabase creates a state database.
func NewDatabase(diskdb ethdb.Database) *Database {
	return NewDatabaseWithConfig(diskdb, DatabaseConfig{})
}

// NewDatabaseWithConfig creates a state database with explicit trie cache
// settings. The default remains hash-based trie storage for java-tron wire and
// state-root compatibility.
func NewDatabaseWithConfig(diskdb ethdb.Database, cfg DatabaseConfig) *Database {
	if cfg.CleanTrieCacheSizeBytes < 0 {
		cfg.CleanTrieCacheSizeBytes = 0
	}
	trieDBCfg := &triedb.Config{
		HashDB: &hashdb.Config{CleanCacheSize: cfg.CleanTrieCacheSizeBytes},
	}
	trieDB := triedb.NewDatabase(diskdb, trieDBCfg)
	db := &Database{
		disk:   diskdb,
		trieDB: trieDB,
	}
	db.trieNodeBatchPool.New = func() any {
		return diskdb.NewBatchWithSize(trieNodeBatchPreallocSize)
	}
	return db
}

// OpenTrie opens the main account trie at the given root.
func (db *Database) OpenTrie(root ethcommon.Hash) (*trie.Trie, error) {
	return trie.New(trie.TrieID(root), db.trieDB)
}

// TrieDB returns the underlying trie database for committing nodes.
func (db *Database) TrieDB() *triedb.Database {
	return db.trieDB
}

// DiskDB returns the underlying disk database.
func (db *Database) DiskDB() ethdb.Database {
	return db.disk
}

// Close releases in-process trie caches. The underlying disk database remains
// owned by the caller.
func (db *Database) Close() error {
	if db == nil || db.trieDB == nil {
		return nil
	}
	return db.trieDB.Close()
}

func (db *Database) newTrieNodeBatch() ethdb.Batch {
	return db.trieNodeBatchPool.Get().(ethdb.Batch)
}

func (db *Database) releaseTrieNodeBatch(batch ethdb.Batch) {
	if batch == nil {
		return
	}
	batch.Reset()
	db.trieNodeBatchPool.Put(batch)
}
