package state

import (
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// Database wraps access to tries.
type Database struct {
	disk   ethdb.Database
	trieDB *triedb.Database
}

// NewDatabase creates a state database.
func NewDatabase(diskdb ethdb.Database) *Database {
	trieDB := triedb.NewDatabase(diskdb, nil) // hash-based defaults
	return &Database{
		disk:   diskdb,
		trieDB: trieDB,
	}
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
