package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func WriteBlock(db ethdb.KeyValueWriter, block *types.Block) error {
	data, err := block.Marshal()
	if err != nil {
		return err
	}
	if err := db.Put(blockKey(block.Number()), data); err != nil {
		return err
	}
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, block.Number())
	return db.Put(blockHashKey(block.Hash().Bytes()), num)
}

func ReadBlock(db ethdb.KeyValueReader, number uint64) *types.Block {
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

func ReadBlockNumber(db ethdb.KeyValueReader, hash common.Hash) *uint64 {
	data, err := db.Get(blockHashKey(hash.Bytes()))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
