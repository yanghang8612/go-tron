package rawdb

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/trondb"
)

func WriteHeadBlockHash(db trondb.KeyValueWriter, hash common.Hash) {
	db.Put(headBlockKey, hash.Bytes())
}

func ReadHeadBlockHash(db trondb.KeyValueReader) common.Hash {
	data, err := db.Get(headBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func WriteHeadSolidBlockHash(db trondb.KeyValueWriter, hash common.Hash) {
	db.Put(headSolidBlockKey, hash.Bytes())
}

func ReadHeadSolidBlockHash(db trondb.KeyValueReader) common.Hash {
	data, err := db.Get(headSolidBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func WriteDynamicProperty(db trondb.KeyValueWriter, name string, value []byte) {
	db.Put(dynPropKey(name), value)
}

func ReadDynamicProperty(db trondb.KeyValueReader, name string) []byte {
	data, err := db.Get(dynPropKey(name))
	if err != nil {
		return nil
	}
	return data
}
