package rawdb

import (
	"encoding/binary"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/trondb"
)

func WriteBlock(db trondb.KeyValueWriter, block *types.Block) {
	data, err := block.Marshal()
	if err != nil {
		return
	}
	db.Put(blockKey(block.Number()), data)

	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, block.Number())
	db.Put(blockHashKey(block.Hash().Bytes()), num)
}

func ReadBlock(db trondb.KeyValueReader, number uint64) *types.Block {
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil
	}
	return block
}

func ReadBlockNumber(db trondb.KeyValueReader, hash common.Hash) *uint64 {
	data, err := db.Get(blockHashKey(hash.Bytes()))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
