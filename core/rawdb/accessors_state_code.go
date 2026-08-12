package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type StateCodeRow struct {
	Hash common.Hash
	Code []byte
}

type stateCodeReadViewContext struct {
	hash        common.Hash
	owned       []byte
	shareStable bool
	callback    func([]byte, bool) error
}

var stateCodeReadViewContextPool = sync.Pool{
	New: func() any {
		ctx := new(stateCodeReadViewContext)
		ctx.callback = ctx.consume
		return ctx
	},
}

func (ctx *stateCodeReadViewContext) consume(code []byte, stable bool) error {
	if ctx.shareStable && stable {
		ctx.owned = code
		return nil
	}
	ctx.owned = append([]byte(nil), code...)
	return nil
}

// WriteStateCode persists immutable contract bytecode by content hash.
func WriteStateCode(db ethdb.KeyValueWriter, hash common.Hash, code []byte) error {
	if hash == (common.Hash{}) || len(code) == 0 {
		return nil
	}
	if common.Keccak256(code) != hash {
		return errors.New("state code: hash does not match code bytes")
	}
	return db.Put(stateCodeKey(hash), append([]byte(nil), code...))
}

// ReadStateCode loads immutable contract bytecode by content hash.
func ReadStateCode(db ethdb.KeyValueReader, hash common.Hash) []byte {
	return readStateCode(db, hash, false)
}

// ReadStateCodeImmutable loads immutable contract bytecode by content hash for
// trusted internal consumers. A stable layered/cache value may be returned
// directly; a callback-scoped durable value is still copied before its view is
// released. The caller may retain the result but must not mutate it.
//
// ReadStateCode deliberately keeps its stronger caller-owned contract. This
// variant exists for StateDB, whose state objects already treat bytecode as
// immutable, so every new block does not copy the same cache-resident contract
// code again.
func ReadStateCodeImmutable(db ethdb.KeyValueReader, hash common.Hash) []byte {
	return readStateCode(db, hash, true)
}

// ReadStateCodeStrict distinguishes a missing row from a backend read error
// while retaining the public reader's owned-byte contract.
func ReadStateCodeStrict(db ethdb.KeyValueReader, hash common.Hash) ([]byte, bool, error) {
	if hash == (common.Hash{}) {
		return nil, false, nil
	}
	data, ok, err := readValueThenVerifyMiss(db, stateCodeKey(hash), fmt.Sprintf("state code %x", hash), nil)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append([]byte(nil), data...), true, nil
}

// PrefetchStateCode admits immutable bytecode directly into a capable
// reader's bounded cache without changing the ordinary owned-byte API.
func PrefetchStateCode(db ethdb.KeyValueReader, hash common.Hash) ([]byte, bool, error) {
	if hash == (common.Hash{}) {
		return nil, false, nil
	}
	return prefetchStatePresentNoCopy(db, stateCodeKey(hash), fmt.Sprintf("state code %x", hash))
}

func readStateCode(db ethdb.KeyValueReader, hash common.Hash, shareStable bool) []byte {
	if hash == (common.Hash{}) {
		return nil
	}
	if viewer, ok := db.(cachedNoCopyKeyPartsViewer); ok {
		ctx := stateCodeReadViewContextPool.Get().(*stateCodeReadViewContext)
		// The viewer may lend a Pebble block slice or a cache/layer slice.
		// State objects retain bytecode across calls. Public reads take one owned
		// copy; trusted immutable reads may retain values marked stable by viewer.
		ctx.hash = hash
		ctx.shareStable = shareStable
		found, err := viewer.ViewNoCopyCachedKeyParts(stateCodePrefix, ctx.hash[:], ctx.callback)
		owned := ctx.owned
		ctx.hash = common.Hash{}
		ctx.owned = nil
		ctx.shareStable = false
		stateCodeReadViewContextPool.Put(ctx)
		if err != nil || !found {
			return nil
		}
		return owned
	}
	data, err := db.Get(stateCodeKey(hash))
	if err != nil {
		return nil
	}
	// KeyValueReader.Get returns caller-owned bytes in every production reader
	// (Pebble, blockbuffer and memorydb). Preserve that ownership instead of
	// copying immutable content-addressed bytecode a second time here.
	return data
}

func DeleteStateCode(db ethdb.KeyValueWriter, hash common.Hash) error {
	if hash == (common.Hash{}) {
		return nil
	}
	return db.Delete(stateCodeKey(hash))
}

func DecodeStateCodeKey(key []byte) (common.Hash, bool) {
	if len(key) != len(stateCodePrefix)+common.HashLength || !bytes.HasPrefix(key, stateCodePrefix) {
		return common.Hash{}, false
	}
	return common.BytesToHash(key[len(stateCodePrefix):]), true
}

func IterateStateCode(db ethdb.Iteratee, fn func(StateCodeRow) (bool, error)) error {
	if db == nil || fn == nil {
		return nil
	}
	it := db.NewIterator(stateCodePrefix, nil)
	defer it.Release()
	for it.Next() {
		hash, ok := DecodeStateCodeKey(it.Key())
		if !ok {
			continue
		}
		cont, err := fn(StateCodeRow{
			Hash: hash,
			Code: append([]byte(nil), it.Value()...),
		})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

// IterateStateCodeHashes walks immutable code keys without copying bytecode.
// It is intended for lifecycle decisions that only need the content hash.
func IterateStateCodeHashes(db ethdb.Iteratee, fn func(common.Hash) (bool, error)) error {
	if db == nil || fn == nil {
		return nil
	}
	it := db.NewIterator(stateCodePrefix, nil)
	defer it.Release()
	for it.Next() {
		hash, ok := DecodeStateCodeKey(it.Key())
		if !ok {
			continue
		}
		cont, err := fn(hash)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}
