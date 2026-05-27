package rawdb

import (
	"bytes"
	"sort"

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

func StateAccountLatestCommitmentKey(owner common.Address) []byte {
	return stateAccountLatestKey(owner)
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

func CoalesceStateCommitmentUpdates(updates []StateCommitmentUpdate) []StateCommitmentUpdate {
	if len(updates) == 0 {
		return nil
	}
	byKey := make(map[string]StateCommitmentUpdate, len(updates))
	for _, update := range updates {
		if len(update.Key) == 0 {
			continue
		}
		byKey[string(update.Key)] = cloneStateCommitmentUpdate(update)
	}
	if len(byKey) == 0 {
		return nil
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	out := make([]StateCommitmentUpdate, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}

func cloneStateCommitmentUpdate(update StateCommitmentUpdate) StateCommitmentUpdate {
	return StateCommitmentUpdate{
		Key:    append([]byte(nil), update.Key...),
		Value:  append([]byte(nil), update.Value...),
		Delete: update.Delete,
	}
}
