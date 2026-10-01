package rawdb

import (
	"errors"
	"path/filepath"
	"strings"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"

	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
)

// PebbleOptions exposes the Pebble tuning knobs accepted by NewPebbleDBWithOptions.
type PebbleOptions = pebbledb.Options

// NewPebbleDB opens (or creates) a Pebble-backed key-value store at path,
// using cache MiB of read cache and handles open-file slots.
//
// Tuning is delegated to core/rawdb/pebbledb, whose DefaultOptions() applies a
// go-tron-specific deviation from go-ethereum's upstream Pebble defaults:
//
//   - MemTableSize is sized independently from the cache (256 MiB by default,
//     up from go-eth's cache/8 ≈ 32 MiB at cache=256 MiB) so the WAL/memtable
//     absorbs more sync write traffic before flushing to L0.
//   - L0CompactionThreshold is relaxed to 8 (go-eth uses 2 to cap compaction
//     debt; that pegged background-compaction CPU under our sync workload).
//   - L0StopWritesThreshold is raised to 64 (Pebble default 12) so transient
//     L0 bursts don't stall foreground writers when MaxConcurrentCompactions
//     can drain them.
//
// Everything else — async writes (pebble.NoSync), MaxConcurrentCompactions=NumCPU,
// MemTableStopWritesThreshold=8, the per-level TargetFileSize ramp, bloom
// filters, and the metrics surface — matches the upstream go-ethereum wrapper.
func NewPebbleDB(path string, cache int, handles int) (ethdb.KeyValueStore, error) {
	return NewPebbleDBWithOptions(path, cache, handles, pebbledb.DefaultOptions())
}

// DefaultPebbleOptions returns the production defaults used by NewPebbleDB.
func DefaultPebbleOptions() PebbleOptions {
	return pebbledb.DefaultOptions()
}

// NewPebbleDBWithOptions opens a Pebble database with explicit cache, handle,
// and low-level Pebble tuning values.
func NewPebbleDBWithOptions(path string, cache int, handles int, tune PebbleOptions) (ethdb.KeyValueStore, error) {
	var err error
	tune, err = withDiskSpaceObservation(tune)
	if err != nil {
		return nil, err
	}
	return pebbledb.New(path, cache, handles, "", false, tune)
}

// NewPebbleDBReadOnly opens an existing Pebble database without permitting
// mutations. It is intended for offline diagnostic commands such as db
// inspect; Pebble still requires the node process to release the database lock.
func NewPebbleDBReadOnly(path string, cache int, handles int) (ethdb.KeyValueStore, error) {
	return pebbledb.New(path, cache, handles, "", true, pebbledb.DefaultOptions())
}

// NewHistoryStagingPebbleDB opens the separate history-payload store with its
// own metrics namespace and a deliberately smaller write/compaction budget.
// cache is part of the process-wide Pebble cache allowance, not an additional
// copy of the main DB budget.
func NewHistoryStagingPebbleDB(path string, cache int, handles int, readOnly bool) (ethdb.KeyValueStore, error) {
	if path == "" || cache <= 0 || handles <= 0 {
		return nil, errors.New("rawdb: invalid history staging database options")
	}
	tune := pebbledb.DefaultOptions()
	tune.MemTableSizeBytes = 32 << 20
	tune.MaxConcurrentCompactions = 1
	if !readOnly {
		var err error
		tune, err = withDiskSpaceObservation(tune)
		if err != nil {
			return nil, err
		}
	}
	return pebbledb.New(path, cache, handles, "history-staging/", readOnly, tune)
}

// ValidateHistoryStagingPaths prevents a store from containing another store
// or the cold directory. Callers still need OS locks and stable path identity.
func ValidateHistoryStagingPaths(source, target, cold string) error {
	paths := []string{source, target, cold}
	for i := range paths {
		if paths[i] == "" {
			return errors.New("rawdb: empty history staging path")
		}
		abs, err := filepath.Abs(paths[i])
		if err != nil {
			return err
		}
		paths[i] = filepath.Clean(abs)
	}
	for i := range paths {
		for j := i + 1; j < len(paths); j++ {
			if paths[i] == paths[j] || strings.HasPrefix(paths[i], paths[j]+string(filepath.Separator)) || strings.HasPrefix(paths[j], paths[i]+string(filepath.Separator)) {
				return errors.New("rawdb: history staging paths overlap")
			}
		}
	}
	return nil
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
	return &stateHistorySnapshotDatabase{Database: ethrawdb.NewDatabase(db), source: db}
}
