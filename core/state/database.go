package state

import (
	"sync"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

const trieNodeBatchPreallocSize = ethdb.IdealBatchSize

// Database wraps access to tries.
type Database struct {
	disk              ethdb.Database
	trieDB            *triedb.Database
	trieNodeBatchPool sync.Pool
}

// NewDatabase creates a state database.
func NewDatabase(diskdb ethdb.Database) *Database {
	trieDB := triedb.NewDatabase(diskdb, nil) // hash-based defaults
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
