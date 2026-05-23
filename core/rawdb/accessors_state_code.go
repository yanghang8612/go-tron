package rawdb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

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
