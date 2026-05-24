package state

import (
	"sync"

	"github.com/VictoriaMetrics/fastcache"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
)

const trieNodeBatchPreallocSize = ethdb.IdealBatchSize

// DatabaseConfig tunes the in-process trie database wrapper.
type DatabaseConfig struct {
	// CleanTrieCacheSizeBytes enables the in-process trie-node cache. It keeps
	// raw legacy trie node blobs above Pebble, including nodes just produced by
	// block commits, so consecutive blocks do not have to re-read hot account/KV
	// trie nodes from compacting SSTables.
	CleanTrieCacheSizeBytes int
}

// Database wraps access to tries.
type Database struct {
	disk              ethdb.Database
	trieDisk          ethdb.Database
	trieDB            *triedb.Database
	trieNodeCache     *fastcache.Cache
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
	trieDisk := diskdb
	var trieNodeCache *fastcache.Cache
	if cfg.CleanTrieCacheSizeBytes > 0 {
		trieNodeCache = fastcache.New(cfg.CleanTrieCacheSizeBytes)
		trieDisk = &trieNodeCacheDB{
			Database: diskdb,
			cache:    trieNodeCache,
		}
	}
	trieDBCfg := &triedb.Config{
		// go-tron keeps its own read-through/write-through node cache around
		// the trie database so nodes produced by our manual batched writer are
		// visible to the next block without a Pebble read. Leaving hashdb's
		// separate clean cache disabled avoids holding the same blobs twice.
		HashDB: &hashdb.Config{CleanCacheSize: 0},
	}
	trieDB := triedb.NewDatabase(trieDisk, trieDBCfg)
	db := &Database{
		disk:          diskdb,
		trieDisk:      trieDisk,
		trieDB:        trieDB,
		trieNodeCache: trieNodeCache,
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
	err := db.trieDB.Close()
	if db.trieNodeCache != nil {
		db.trieNodeCache.Reset()
	}
	return err
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

func (db *Database) cacheTrieNode(hash ethcommon.Hash, blob []byte) {
	if db == nil || db.trieNodeCache == nil || len(blob) == 0 {
		return
	}
	db.trieNodeCache.Set(hash[:], blob)
}
