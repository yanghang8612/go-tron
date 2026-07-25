package rawdb

import (
	"bytes"
	"errors"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type StateCodeRow struct {
	Hash common.Hash
	Code []byte
}

type stateCodeReadViewContext struct {
	hash     common.Hash
	owned    []byte
	callback func([]byte, bool) error
}

var stateCodeReadViewContextPool = sync.Pool{
	New: func() any {
		ctx := new(stateCodeReadViewContext)
		ctx.callback = ctx.consume
		return ctx
	},
}

func (ctx *stateCodeReadViewContext) consume(code []byte, _ bool) error {
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
	if hash == (common.Hash{}) {
		return nil
	}
	if viewer, ok := db.(cachedNoCopyKeyPartsViewer); ok {
		ctx := stateCodeReadViewContextPool.Get().(*stateCodeReadViewContext)
		// The viewer may lend a Pebble block slice or a cache/layer slice.
		// State objects retain bytecode across calls, so the bound callback takes
		// exactly one caller-owned copy while the scoped view is valid.
		ctx.hash = hash
		found, err := viewer.ViewNoCopyCachedKeyParts(stateCodePrefix, ctx.hash[:], ctx.callback)
		owned := ctx.owned
		ctx.hash = common.Hash{}
		ctx.owned = nil
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
