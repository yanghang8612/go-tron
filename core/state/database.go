package state

import (
	"github.com/VictoriaMetrics/fastcache"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
)

// DatabaseConfig tunes the in-process trie database wrapper.
type DatabaseConfig struct {
	// CleanTrieCacheSizeBytes enables the in-process trie-node cache for
	// java-tron accountStateRoot trie reads/writes. Internal full state no
	// longer uses a trie-backed root.
	CleanTrieCacheSizeBytes int

	// StagedCommitment selects the Erigon-style staged commitment engine
	// (prefix-keyed BranchData) over the legacy incremental binary-radix tree.
	// It is bound once at database construction for a given DB and must not be
	// flipped on a DB that already holds commitment rows (fresh-DB-only; see
	// docs/superpowers/specs/2026-05-25-erigon-state-architecture-gap.md #6).
	// StateDB.latestCommitmentStore branches on it at commit time; the default
	// (false) keeps the legacy binary-radix engine as the production path.
	StagedCommitment bool
}

// Database wraps state storage plus the independent java-tron accountStateRoot trie.
type Database struct {
	disk             ethdb.Database
	trieDisk         ethdb.Database
	trieDB           *triedb.Database
	trieNodeCache    *fastcache.Cache
	stagedCommitment bool
}

// NewDatabase creates a state database.
func NewDatabase(diskdb ethdb.Database) *Database {
	return NewDatabaseWithConfig(diskdb, DatabaseConfig{})
}

// NewDatabaseWithConfig creates a state database with explicit trie cache
// settings for the independent java-tron accountStateRoot trie.
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
		// The wrapper above provides the cache; leaving hashdb's separate clean
		// cache disabled avoids holding the same blobs twice.
		HashDB: &hashdb.Config{CleanCacheSize: 0},
	}
	trieDB := triedb.NewDatabase(trieDisk, trieDBCfg)
	db := &Database{
		disk:             diskdb,
		trieDisk:         trieDisk,
		trieDB:           trieDB,
		trieNodeCache:    trieNodeCache,
		stagedCommitment: cfg.StagedCommitment,
	}
	return db
}

// StagedCommitment reports whether this database selects the Erigon-style
// staged commitment engine. Bound at construction; see DatabaseConfig.
func (db *Database) StagedCommitment() bool {
	return db.stagedCommitment
}

// OpenTrie opens the independent java-tron accountStateRoot trie at root.
func (db *Database) OpenTrie(root ethcommon.Hash) (*trie.Trie, error) {
	return trie.New(trie.TrieID(root), db.trieDB)
}

// TrieDB returns the underlying java-tron accountStateRoot trie database.
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
