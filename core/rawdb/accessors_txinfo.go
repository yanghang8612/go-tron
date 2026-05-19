package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// ancientTxInfos names the freezer table holding marshalled
// `corepb.TransactionRet` blobs keyed by block number (the same payload
// `tib-<num>` stores in Pebble).
const ancientTxInfos = "tx_infos"

// WriteTransactionInfo stores a single TransactionInfo indexed by txID.
func WriteTransactionInfo(db ethdb.KeyValueWriter, txID []byte, info *corepb.TransactionInfo) {
	data, err := proto.Marshal(info)
	if err != nil {
		return
	}
	db.Put(txInfoKey(txID), data)
}

// ReadTransactionInfo retrieves a TransactionInfo by txID. The per-tx index
// stays hot per the slice-1 freezer spec, so this accessor reads only from
// Pebble; the `*ChainDB` parameter exists for signature uniformity.
func ReadTransactionInfo(db *ChainDB, txID []byte) *corepb.TransactionInfo {
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

// ReadTransactionInfosByBlock retrieves all TransactionInfos for a block
// number. Consults the freezer first when the requested block is below
// the ancient cutoff; falls back to `tib-<num>` in Pebble otherwise.
func ReadTransactionInfosByBlock(db *ChainDB, blockNum uint64) []*corepb.TransactionInfo {
	if data, ok := readAncient(db, ancientTxInfos, blockNum); ok {
		ret := &corepb.TransactionRet{}
		if err := proto.Unmarshal(data, ret); err != nil {
			return nil
		}
		return ret.Transactioninfo
	}
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

// ReadTransactionIndex retrieves the block number for a tx hash. The tx
// reverse index stays hot per the slice-1 freezer spec, so this accessor
// reads only from Pebble.
func ReadTransactionIndex(db *ChainDB, txHash []byte) *uint64 {
	data, err := db.Get(txKey(txHash))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
