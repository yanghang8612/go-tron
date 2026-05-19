package rawdb

import (
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"

	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

// NewPebbleDB opens (or creates) a Pebble-backed key-value store at path,
// using cache MiB of read cache and handles open-file slots.
//
// Tuning is delegated to core/rawdb/pebbledb, whose DefaultOptions() applies a
// go-tron-specific deviation from go-ethereum's upstream Pebble defaults:
//
//   - MemTableSize is sized independently from the cache (64 MiB by default,
//     up from go-eth's cache/8 ≈ 32 MiB at cache=256 MiB) so the WAL/memtable
//     absorbs more sync write traffic before flushing to L0.
//   - L0CompactionThreshold is restored to Pebble's upstream 4 (go-eth uses 2
//     to cap compaction debt; that pegged background-compaction CPU under our
//     sync workload — see the h≈1.96M profile that motivated this change).
//   - L0StopWritesThreshold is raised to 24 (Pebble default 12) so transient
//     L0 bursts don't stall foreground writers when MaxConcurrentCompactions
//     can drain them.
//
// Everything else — async writes (pebble.NoSync), MaxConcurrentCompactions=NumCPU,
// MemTableStopWritesThreshold=8, the per-level TargetFileSize ramp, bloom
// filters, and the metrics surface — matches the upstream go-ethereum wrapper.
func NewPebbleDB(path string, cache int, handles int) (ethdb.KeyValueStore, error) {
	return pebbledb.New(path, cache, handles, "", false, pebbledb.DefaultOptions())
}

func NewMemoryDatabase() ethdb.KeyValueStore {
	return memorydb.New()
}

// NewMemoryChainDB returns a `*ChainDB` backed by an in-memory KV store and
// a `NoopAncient` reader. Slice 2's accessor migration changed
// `core/rawdb` chain readers from `ethdb.KeyValueReader` to `*ChainDB`; this
// helper lets every existing test that previously called
// `NewMemoryDatabase()` keep its byte-identical behavior by simply switching
// to `NewMemoryChainDB()`. With the noop ancient, `AncientCount` is always
// zero so every read falls through to the embedded KV store.
func NewMemoryChainDB() *ChainDB {
	return NewChainDB(memorydb.New(), NoopAncient{})
}

// WrapKeyValueStore wraps an ethdb.KeyValueStore into a full ethdb.Database.
func WrapKeyValueStore(db ethdb.KeyValueStore) ethdb.Database {
	return ethrawdb.NewDatabase(db)
}
