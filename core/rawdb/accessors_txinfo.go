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
	if err := validateTransactionInfoIDForKey(txID, info, "write transaction info"); err != nil {
		return err
	}
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
			if transactionInfoMatchesIndexedLookup(info, txID, *blockNum) {
				return info
			}
		}
	}
	for _, info := range infos {
		if transactionInfoMatchesIndexedLookup(info, txID, *blockNum) && len(info.Id) != 0 {
			return info
		}
	}
	return nil
}

func transactionInfoIDMatches(info *corepb.TransactionInfo, txID []byte) bool {
	return validateTransactionInfoIDForKey(txID, info, "read transaction info") == nil
}

func transactionInfoMatchesIndexedLookup(info *corepb.TransactionInfo, txID []byte, blockNum uint64) bool {
	if !transactionInfoIDMatches(info, txID) {
		return false
	}
	return transactionInfoBlockNumberMatches(info.BlockNumber, blockNum)
}

func validateTransactionInfoIDForKey(txID []byte, info *corepb.TransactionInfo, context string) error {
	if info == nil {
		return fmt.Errorf("rawdb: nil transaction info during %s", context)
	}
	if len(info.Id) == 0 {
		return nil
	}
	if len(info.Id) != common.HashLength {
		return fmt.Errorf("rawdb: transaction info id length %d during %s", len(info.Id), context)
	}
	if !bytes.Equal(info.Id, txID) {
		return fmt.Errorf("rawdb: transaction info id %x does not match key %x during %s", info.Id, txID, context)
	}
	return nil
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
	infos, ok, err := readTransactionInfosByBlockStrict(db, blockNum)
	if err != nil || !ok {
		return nil
	}
	return infos
}

func readTransactionInfosByBlockStrict(db *ChainDB, blockNum uint64) ([]*corepb.TransactionInfo, bool, error) {
	if data, ok := readAncient(db, ancientTxInfos, blockNum); ok {
		infos, err := decodeTransactionRetForBlock(data, blockNum)
		return infos, true, err
	}
	data, err := db.Get(txInfoBlockKey(blockNum))
	if err != nil {
		return nil, false, nil
	}
	infos, err := decodeTransactionRetForBlock(data, blockNum)
	return infos, true, err
}

func decodeTransactionRetForBlock(data []byte, blockNum uint64) ([]*corepb.TransactionInfo, error) {
	ret := &corepb.TransactionRet{}
	if err := proto.Unmarshal(data, ret); err != nil {
		return nil, err
	}
	if !transactionInfoBlockNumberMatches(ret.BlockNumber, blockNum) {
		return nil, fmt.Errorf("rawdb: transaction ret block number %d does not match key block %d", ret.BlockNumber, blockNum)
	}
	for txIndex, info := range ret.Transactioninfo {
		if info == nil {
			return nil, fmt.Errorf("rawdb: nil transaction info at block %d index %d", blockNum, txIndex)
		}
		if !transactionInfoBlockNumberMatches(info.BlockNumber, blockNum) {
			return nil, fmt.Errorf("rawdb: transaction info block number %d at block %d index %d", info.BlockNumber, blockNum, txIndex)
		}
	}
	return ret.Transactioninfo, nil
}

func transactionInfoBlockNumberMatches(got int64, want uint64) bool {
	if got == 0 {
		return true
	}
	if got < 0 {
		return false
	}
	return uint64(got) == want
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
