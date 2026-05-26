package rawdb

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type StateCodeRow struct {
	Hash common.Hash
	Code []byte
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
	data, err := db.Get(stateCodeKey(hash))
	if err != nil {
		return nil
	}
	return append([]byte(nil), data...)
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
