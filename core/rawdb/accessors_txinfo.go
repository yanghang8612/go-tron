package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// ancientTxInfos names the freezer table holding marshalled
// `corepb.TransactionRet` blobs keyed by block number (the same payload
// `tib-<num>` stores in Pebble).
const ancientTxInfos = AncientTxInfosTable

// WriteTransactionInfo stores a single TransactionInfo indexed by txID.
func WriteTransactionInfo(db ethdb.KeyValueWriter, txID []byte, info *corepb.TransactionInfo) error {
	data, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return db.Put(txInfoKey(txID), data)
}

// ReadTransactionInfo retrieves a TransactionInfo by txID. The hot per-tx
// `ti-<txid>` row is preferred; on a miss, a ChainDB with a cold chain-index
// sidecar can resolve txID -> block number and scan that block's TransactionRet
// payload from ancient or hot per-block storage.
func ReadTransactionInfo(db *ChainDB, txID []byte) *corepb.TransactionInfo {
	data, err := db.Get(txInfoKey(txID))
	if err == nil {
		info := &corepb.TransactionInfo{}
		if err := proto.Unmarshal(data, info); err != nil {
			return nil
		}
		if !transactionInfoIDMatches(info, txID) {
			return nil
		}
		return info
	}
	blockNum := ReadTransactionIndex(db, txID)
	if blockNum == nil {
		return nil
	}
	infos := ReadTransactionInfosByBlock(db, *blockNum)
	if lookup, ok := readColdTransactionIndexByHash(db, txID); ok && lookup.BlockNum == *blockNum {
		if int(lookup.TxIndex) < len(infos) {
			info := infos[lookup.TxIndex]
			if info != nil && (len(info.Id) == 0 || bytes.Equal(info.Id, txID)) {
				return info
			}
		}
	}
	for _, info := range infos {
		if info == nil {
			continue
		}
		if bytes.Equal(info.Id, txID) {
			return info
		}
	}
	return nil
}

func transactionInfoIDMatches(info *corepb.TransactionInfo, txID []byte) bool {
	if info == nil {
		return false
	}
	if len(info.Id) == 0 {
		return true
	}
	return len(info.Id) == common.HashLength && bytes.Equal(info.Id, txID)
}

func readColdTransactionIndexByHash(db *ChainDB, txHash []byte) (ChainIndexTxLookup, bool) {
	var zero ChainIndexTxLookup
	if db == nil || db.chainIndex == nil || len(txHash) != common.HashLength {
		return zero, false
	}
	reader, ok := db.chainIndex.(ChainIndexTxPositionReader)
	if !ok {
		return zero, false
	}
	var hash common.Hash
	copy(hash[:], txHash)
	lookup, ok, err := reader.TransactionIndexByHash(hash)
	if err != nil || !ok {
		return zero, false
	}
	return lookup, true
}

// WriteTransactionInfosByBlock stores all TransactionInfos for a block.
func WriteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64, infos []*corepb.TransactionInfo) error {
	for txIndex, info := range infos {
		if info == nil {
			return fmt.Errorf("rawdb: nil transaction info at block %d index %d", blockNum, txIndex)
		}
	}
	ret := &corepb.TransactionRet{
		BlockNumber:     int64(blockNum),
		Transactioninfo: infos,
	}
	if len(infos) > 0 {
		ret.BlockTimeStamp = infos[0].BlockTimeStamp
	}
	data, err := proto.Marshal(ret)
	if err != nil {
		return err
	}
	return db.Put(txInfoBlockKey(blockNum), data)
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
func WriteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64) error {
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, blockNum)
	return db.Put(txKey(txHash), num)
}

// ReadTransactionIndex retrieves the block number for a tx hash. The hot
// `tx-<hash>` row is preferred; on a miss, an attached cold chain-index sidecar
// can resolve historical tx hashes without keeping every old reverse index in
// Pebble.
func ReadTransactionIndex(db *ChainDB, txHash []byte) *uint64 {
	data, err := db.Get(txKey(txHash))
	if err == nil && len(data) == 8 {
		num := binary.BigEndian.Uint64(data)
		return &num
	}
	if db != nil && db.chainIndex != nil && len(txHash) == common.HashLength {
		var hash common.Hash
		copy(hash[:], txHash)
		num, ok, err := db.chainIndex.TransactionBlockNumberByHash(hash)
		if err == nil && ok {
			return &num
		}
	}
	return nil
}

// DeleteTransactionInfo removes the per-tx TransactionInfo row for txID.
func DeleteTransactionInfo(db ethdb.KeyValueWriter, txID []byte) error {
	return db.Delete(txInfoKey(txID))
}

// DeleteTransactionInfosByBlock removes the per-block TransactionRet row for blockNum.
func DeleteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(txInfoBlockKey(blockNum))
}

// DeleteTransactionIndex removes the tx-hash→block-number reverse index row.
func DeleteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte) error {
	return db.Delete(txKey(txHash))
}
