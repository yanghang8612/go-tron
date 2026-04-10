package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// WriteTransactionInfo stores a single TransactionInfo indexed by txID.
func WriteTransactionInfo(db ethdb.KeyValueWriter, txID []byte, info *corepb.TransactionInfo) {
	data, err := proto.Marshal(info)
	if err != nil {
		return
	}
	db.Put(txInfoKey(txID), data)
}

// ReadTransactionInfo retrieves a TransactionInfo by txID.
func ReadTransactionInfo(db ethdb.KeyValueReader, txID []byte) *corepb.TransactionInfo {
	data, err := db.Get(txInfoKey(txID))
	if err != nil {
		return nil
	}
	info := &corepb.TransactionInfo{}
	if err := proto.Unmarshal(data, info); err != nil {
		return nil
	}
	return info
}

// WriteTransactionInfosByBlock stores all TransactionInfos for a block.
func WriteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64, infos []*corepb.TransactionInfo) {
	ret := &corepb.TransactionRet{
		BlockNumber:     int64(blockNum),
		Transactioninfo: infos,
	}
	if len(infos) > 0 {
		ret.BlockTimeStamp = infos[0].BlockTimeStamp
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		return
	}
	db.Put(txInfoBlockKey(blockNum), data)
}

// ReadTransactionInfosByBlock retrieves all TransactionInfos for a block number.
func ReadTransactionInfosByBlock(db ethdb.KeyValueReader, blockNum uint64) []*corepb.TransactionInfo {
	data, err := db.Get(txInfoBlockKey(blockNum))
	if err != nil {
		return nil
	}
	ret := &corepb.TransactionRet{}
	if err := proto.Unmarshal(data, ret); err != nil {
		return nil
	}
	return ret.Transactioninfo
}

// WriteTransactionIndex stores a tx-hash to block-number mapping.
func WriteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64) {
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, blockNum)
	db.Put(txKey(txHash), num)
}

// ReadTransactionIndex retrieves the block number for a tx hash.
func ReadTransactionIndex(db ethdb.KeyValueReader, txHash []byte) *uint64 {
	data, err := db.Get(txKey(txHash))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
