package rawdb

import (
	"bytes"
	"slices"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type StateCommitmentUpdate struct {
	Key    []byte
	Value  []byte
	Delete bool
}

func NewStateCommitmentPut(key, value []byte) StateCommitmentUpdate {
	return StateCommitmentUpdate{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
}

func NewStateCommitmentDelete(key []byte) StateCommitmentUpdate {
	return StateCommitmentUpdate{
		Key:    append([]byte(nil), key...),
		Delete: true,
	}
}

// NewStateCommitmentPutOwned constructs an update without cloning its inputs.
// The caller transfers exclusive ownership of key and value and must not mutate
// them for the update's lifetime (including a deferred async fold). Hot
// latest-domain paths use this only with freshly allocated physical keys and
// encoded values.
func NewStateCommitmentPutOwned(key, value []byte) StateCommitmentUpdate {
	return StateCommitmentUpdate{Key: key, Value: value}
}

// NewStateCommitmentDeleteOwned is the delete counterpart of
// NewStateCommitmentPutOwned.
func NewStateCommitmentDeleteOwned(key []byte) StateCommitmentUpdate {
	return StateCommitmentUpdate{Key: key, Delete: true}
}

func StateAccountLatestCommitmentKey(owner common.Address) []byte {
	return stateAccountLatestKey(owner)
}

// StateAccountLatestCommitmentKeySize is the fixed encoded size of an
// account-latest physical key. It lets commit paths reserve one contiguous
// arena for all keys instead of allocating each key separately.
func StateAccountLatestCommitmentKeySize() int {
	return len(stateAccountLatestPrefix) + common.AccountIDLength
}

// AppendStateAccountLatestCommitmentKey appends an account-latest physical key
// to dst. When dst has reserved capacity this performs no allocation.
func AppendStateAccountLatestCommitmentKey(dst []byte, owner common.Address) []byte {
	accountID := owner.AccountID()
	dst = append(dst, stateAccountLatestPrefix...)
	return append(dst, accountID[:]...)
}

func StateKVLatestCommitmentKey(owner common.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) []byte {
	return stateKVLatestKey(owner, generation, domain, logicalKey)
}

func StateKVGenerationCommitmentKey(owner common.Address) []byte {
	return stateKVGenerationKey(owner)
}

// IterateLatestDomainCommitmentSources iterates every row in the three
// latest-domain source tables (account-latest, KV-generation, KV-latest) in a
// deterministic prefix order and calls fn with the physical (key, value) of
// each row. The physical key is exactly what NewStateCommitmentPut expects as a
// commitment key. Iteration stops when fn returns (false, nil) or an error. It
// exists so callers outside rawdb can bootstrap a commitment engine from the
// latest-domain rows without duplicating the unexported prefix literals.
func IterateLatestDomainCommitmentSources(db ethdb.Iteratee, fn func(key, value []byte) (bool, error)) error {
	for _, prefix := range [][]byte{stateAccountLatestPrefix, stateKVGenerationPrefix, stateKVLatestPrefix} {
		it := db.NewIterator(prefix, nil)
		for it.Next() {
			cont, err := fn(it.Key(), it.Value())
			if err != nil {
				it.Release()
				return err
			}
			if !cont {
				it.Release()
				return nil
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// CoalesceStateCommitmentUpdates deduplicates updates per key (last-writer-wins)
// and returns them sorted by key. Both callers
// (DomainCommitmentState.latestUpdatesFromTouches and the unwind builder)
// pass owned, immutable Key and Value buffers. This re-uses those buffers rather
// than cloning them. The downstream commitment fold borrows both synchronously;
// branch persistence encodes/copies every retained key before the fold returns.
// The returned slice may therefore share backing arrays with the input.
func CoalesceStateCommitmentUpdates(updates []StateCommitmentUpdate) []StateCommitmentUpdate {
	if len(updates) == 0 {
		return nil
	}
	// The production latest-touch collector already emits non-empty, unique
	// keys in strict byte order. Preserve that slice directly: rebuilding a map
	// here used to allocate one string per update plus map buckets and two result
	// slices, only for buildOps to rediscover the same ordering one layer later.
	strictlySorted := true
	for i := range updates {
		if len(updates[i].Key) == 0 || (i > 0 && bytes.Compare(updates[i-1].Key, updates[i].Key) >= 0) {
			strictlySorted = false
			break
		}
	}
	if strictlySorted {
		return updates
	}
	byKey := make(map[string]StateCommitmentUpdate, len(updates))
	for _, update := range updates {
		if len(update.Key) == 0 {
			continue
		}
		byKey[string(update.Key)] = update
	}
	if len(byKey) == 0 {
		return nil
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	// slices.Sort on []string orders lexicographically by byte content — the same
	// total order as bytes.Compare on the underlying key bytes — without the
	// reflect-backed sort.Slice closure and its per-comparison []byte(string)
	// conversions, which together dominated this function's allocation.
	slices.Sort(keys)
	out := make([]StateCommitmentUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}
