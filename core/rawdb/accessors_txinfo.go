package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
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

// HasHotTransactionInfo reports whether the legacy per-transaction
// `ti-<txid>` row exists in the hot key-value store. It deliberately does not
// consult per-block TransactionRet rows or cold indexes: callers use it only
// to migrate or prune the redundant physical row without suppressing the
// receipt-by-ID fallback path.
func HasHotTransactionInfo(db ethdb.KeyValueReader, txID []byte) (bool, error) {
	if err := validateTransactionHashKey(txID, "check hot transaction info"); err != nil {
		return false, err
	}
	if db == nil {
		return false, fmt.Errorf("rawdb: nil database during check hot transaction info")
	}
	return db.Has(txInfoKey(txID))
}

// ReadTransactionInfo retrieves a TransactionInfo by txID. The hot per-tx
// `ti-<txid>` row is preferred; on a miss, a ChainDB with a cold chain-index
// sidecar can resolve txID -> block number and scan that block's TransactionRet
// payload from ancient or hot per-block storage.
func ReadTransactionInfo(db *ChainDB, txID []byte) *corepb.TransactionInfo {
	info, ok, err := ReadTransactionInfoStrict(db, txID)
	if err != nil || !ok {
		return nil
	}
	return info
}

// ReadTransactionInfoStrict retrieves a TransactionInfo by txID and surfaces
// malformed hot rows, corrupt per-block TransactionRet payloads, and
// hash/index mismatches as data errors.
func ReadTransactionInfoStrict(db *ChainDB, txID []byte) (*corepb.TransactionInfo, bool, error) {
	if err := validateTransactionHashKey(txID, "read transaction info"); err != nil {
		return nil, false, err
	}
	data, ok, err := readValueThenVerifyMiss(db, txInfoKey(txID), fmt.Sprintf("transaction info %x", txID), nil)
	if err != nil {
		return nil, false, err
	}
	if ok {
		info := &corepb.TransactionInfo{}
		if err := proto.Unmarshal(data, info); err != nil {
			return nil, true, err
		}
		if err := validateTransactionInfoIDForKey(txID, info, "read transaction info"); err != nil {
			return info, true, err
		}
		return info, true, nil
	}
	blockNum, hasIndex, err := readHotTransactionIndexStrict(db, txID)
	if err != nil {
		return nil, hasIndex, err
	}
	hasHotIndex := hasIndex
	if !hasIndex {
		blockNum, hasIndex, err = ReadTransactionIndexStrict(db, txID)
		if err != nil || !hasIndex {
			return nil, hasIndex, err
		}
	}
	infos, hasInfos, err := ReadTransactionInfosByBlockStrict(db, blockNum)
	if err != nil || !hasInfos {
		return nil, hasInfos, err
	}
	if hasHotIndex {
		if info, ok, err := transactionInfoByReadableBlockPosition(db, txID, blockNum, infos, "read transaction info"); err != nil || ok {
			return info, ok, err
		}
		if info, ok, err := transactionInfoByExplicitIDPosition(txID, blockNum, infos, "read transaction info by explicit id"); err != nil || ok {
			return info, ok, err
		}
	}
	lookup, ok, err := readColdTransactionIndexByHash(db, txID)
	if err != nil {
		return nil, false, err
	}
	if ok && lookup.BlockNum == blockNum {
		matchesBlock, err := coldTransactionPositionMatchesReadableBlock(db, txID, lookup)
		if err != nil {
			return nil, true, err
		}
		if !matchesBlock {
			return nil, true, fmt.Errorf("rawdb: cold transaction index position block %d tx %d does not match transaction %x", lookup.BlockNum, lookup.TxIndex, txID)
		}
		if int(lookup.TxIndex) < len(infos) {
			info := infos[lookup.TxIndex]
			if len(info.GetId()) == 0 && transactionInfoBlockNumberMatches(info.BlockNumber, blockNum) {
				return info, true, nil
			}
			if err := validateTransactionInfoIDForKey(txID, info, "read transaction info by cold position"); err != nil {
				return info, true, err
			}
			if transactionInfoBlockNumberMatches(info.BlockNumber, blockNum) {
				return info, true, nil
			}
			return info, true, fmt.Errorf("rawdb: transaction info block number %d does not match indexed block %d during read transaction info by cold position", info.BlockNumber, blockNum)
		}
		return nil, true, fmt.Errorf("rawdb: cold transaction index position %d outside transaction info coverage %d for block %d", lookup.TxIndex, len(infos), blockNum)
	}
	if info, ok, err := transactionInfoByReadableBlockPosition(db, txID, blockNum, infos, "read transaction info"); err != nil || ok {
		return info, ok, err
	}
	return transactionInfoByExplicitIDPosition(txID, blockNum, infos, "read transaction info by explicit id")
}

func transactionInfoByExplicitIDPosition(txID []byte, blockNum uint64, infos []*corepb.TransactionInfo, context string) (*corepb.TransactionInfo, bool, error) {
	for _, info := range infos {
		if info == nil || len(info.Id) == 0 {
			continue
		}
		if !bytes.Equal(info.Id, txID) {
			continue
		}
		if err := validateTransactionInfoIDForKey(txID, info, context); err != nil {
			return info, true, err
		}
		if transactionInfoBlockNumberMatches(info.BlockNumber, blockNum) {
			return info, true, nil
		}
		return info, true, fmt.Errorf("rawdb: transaction info block number %d does not match indexed block %d during %s", info.BlockNumber, blockNum, context)
	}
	return nil, false, nil
}

func validateTransactionInfoIDForKey(txID []byte, info *corepb.TransactionInfo, context string) error {
	if info == nil {
		return fmt.Errorf("rawdb: nil transaction info during %s", context)
	}
	if err := validateTransactionHashKey(txID, context); err != nil {
		return err
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

func readColdTransactionIndexByHash(db *ChainDB, txHash []byte) (ChainIndexTxLookup, bool, error) {
	var zero ChainIndexTxLookup
	if db == nil || db.chainIndex == nil || len(txHash) != common.HashLength {
		return zero, false, nil
	}
	reader, ok := db.chainIndex.(ChainIndexTxPositionReader)
	if !ok {
		return zero, false, nil
	}
	var hash common.Hash
	copy(hash[:], txHash)
	lookup, ok, err := reader.TransactionIndexByHash(hash)
	if err != nil {
		return zero, false, err
	}
	if !ok {
		return zero, false, nil
	}
	return lookup, true, nil
}

func coldTransactionPositionMatchesReadableBlock(db *ChainDB, txID []byte, lookup ChainIndexTxLookup) (bool, error) {
	block, ok, err := ReadBlockStrict(db, lookup.BlockNum)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	txs := block.Transactions()
	txIndex := int(lookup.TxIndex)
	if txIndex < 0 || txIndex >= len(txs) {
		return false, nil
	}
	tx := txs[txIndex]
	if tx == nil {
		return false, nil
	}
	txHash := tx.Hash()
	return bytes.Equal(txHash[:], txID), nil
}

func transactionIndexInReadableBlock(db *ChainDB, txID []byte, blockNum uint64) (uint32, bool, error) {
	block, ok, err := ReadBlockStrict(db, blockNum)
	if err != nil {
		return 0, true, err
	}
	if !ok {
		return 0, false, nil
	}
	for txIndex, tx := range block.Transactions() {
		if tx == nil {
			continue
		}
		txHash := tx.Hash()
		if !bytes.Equal(txHash[:], txID) {
			continue
		}
		if uint64(txIndex) > uint64(^uint32(0)) {
			return 0, true, fmt.Errorf("rawdb: transaction index %d exceeds uint32 for block %d", txIndex, blockNum)
		}
		return uint32(txIndex), true, nil
	}
	return 0, true, fmt.Errorf("rawdb: transaction index points to block %d but transaction %x is not in the readable block body", blockNum, txID)
}

func transactionInfoByReadableBlockPosition(db *ChainDB, txID []byte, blockNum uint64, infos []*corepb.TransactionInfo, context string) (*corepb.TransactionInfo, bool, error) {
	block, ok, err := ReadBlockStrict(db, blockNum)
	if err != nil {
		return nil, true, err
	}
	if !ok {
		return nil, false, nil
	}
	txs := block.Transactions()
	if err := ValidateTransactionInfosForBlock(blockNum, txs, infos, context); err != nil {
		return nil, true, err
	}
	for txIndex, tx := range txs {
		if tx == nil {
			continue
		}
		hash := tx.Hash()
		if !bytes.Equal(hash[:], txID) {
			continue
		}
		return infos[txIndex], true, nil
	}
	return nil, true, fmt.Errorf("rawdb: transaction index points to block %d but transaction %x is not in the readable block body", blockNum, txID)
}

// WriteTransactionInfosByBlock stores all TransactionInfos for a block.
func WriteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64, infos []*corepb.TransactionInfo) error {
	if err := validateTransactionInfosForKey(blockNum, infos, "write transaction infos by block"); err != nil {
		return err
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
	infos, ok, err := ReadTransactionInfosByBlockStrict(db, blockNum)
	if err != nil || !ok {
		return nil
	}
	return infos
}

// ReadTransactionInfosByBlockStrict retrieves the per-block TransactionRet row
// and reports whether a source row existed. Unlike ReadTransactionInfosByBlock,
// malformed or block-number-mismatched payloads are returned as errors so
// rebuild/snapshot publishers can fail loudly instead of treating corrupt
// coverage as an ordinary miss.
func ReadTransactionInfosByBlockStrict(db *ChainDB, blockNum uint64) ([]*corepb.TransactionInfo, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("rawdb: nil database during read transaction infos by block")
	}
	if data, ok, err := readAncientTransactionInfosStrict(db, blockNum); err != nil || ok {
		if err != nil {
			return nil, ok, err
		}
		infos, err := decodeTransactionRetForBlock(data, blockNum)
		return infos, true, err
	}
	data, ok, err := readValueThenVerifyMiss(db, txInfoBlockKey(blockNum), fmt.Sprintf("transaction infos for block %d", blockNum), nil)
	if err != nil || !ok {
		return nil, ok, err
	}
	infos, err := decodeTransactionRetForBlock(data, blockNum)
	return infos, true, err
}

func readAncientTransactionInfosStrict(db *ChainDB, blockNum uint64) ([]byte, bool, error) {
	if db == nil || db.AncientReader == nil {
		return nil, false, nil
	}
	data, err := db.Ancient(ancientTxInfos, blockNum)
	if err != nil {
		if errors.Is(err, ErrNotInAncient) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return data, true, nil
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
		if err := validateTransactionInfoForBlockKey(blockNum, txIndex, info, "read transaction infos by block"); err != nil {
			return nil, err
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

func validateTransactionInfosForKey(blockNum uint64, infos []*corepb.TransactionInfo, context string) error {
	for txIndex, info := range infos {
		if err := validateTransactionInfoForBlockKey(blockNum, txIndex, info, context); err != nil {
			return err
		}
	}
	return nil
}

func validateTransactionInfoForBlockKey(blockNum uint64, txIndex int, info *corepb.TransactionInfo, context string) error {
	if info == nil {
		return fmt.Errorf("rawdb: nil transaction info at block %d index %d during %s", blockNum, txIndex, context)
	}
	if !transactionInfoBlockNumberMatches(info.BlockNumber, blockNum) {
		return fmt.Errorf("rawdb: transaction info block number %d at block %d index %d during %s", info.BlockNumber, blockNum, txIndex, context)
	}
	if len(info.Id) != 0 && len(info.Id) != common.HashLength {
		return fmt.Errorf("rawdb: transaction info id length %d at block %d index %d during %s", len(info.Id), blockNum, txIndex, context)
	}
	return nil
}

// WriteTransactionIndex stores a tx-hash to block-number mapping.
func WriteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64) error {
	if err := validateTransactionHashKey(txHash, "write transaction index"); err != nil {
		return err
	}
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, blockNum)
	return db.Put(txKey(txHash), num)
}

// ReadTransactionIndex retrieves the block number for a tx hash. The hot
// `tx-<hash>` row is preferred; on a miss, an attached cold chain-index sidecar
// can resolve historical tx hashes without keeping every old reverse index in
// Pebble.
func ReadTransactionIndex(db *ChainDB, txHash []byte) *uint64 {
	num, ok, err := ReadTransactionIndexStrict(db, txHash)
	if err != nil || !ok {
		return nil
	}
	return &num
}

// ReadTransactionIndexStrict retrieves the block number for a tx hash and
// surfaces malformed hot rows or cold sidecar lookup errors. Boundary checks
// use this to decide solid/PBFT visibility from the lookup index before reading
// full transaction or receipt payloads.
func ReadTransactionIndexStrict(db *ChainDB, txHash []byte) (uint64, bool, error) {
	if err := validateTransactionHashKey(txHash, "read transaction index"); err != nil {
		return 0, false, err
	}
	if num, ok, err := readHotTransactionIndexStrict(db, txHash); err != nil || ok {
		return num, ok, err
	}
	if db != nil && db.chainIndex != nil && len(txHash) == common.HashLength {
		var hash common.Hash
		copy(hash[:], txHash)
		num, ok, err := db.chainIndex.TransactionBlockNumberByHash(hash)
		if err != nil || !ok {
			return 0, ok, err
		}
		return num, true, nil
	}
	return 0, false, nil
}

func readHotTransactionIndexStrict(db ethdb.KeyValueReader, txHash []byte) (uint64, bool, error) {
	if db == nil {
		return 0, false, fmt.Errorf("rawdb: nil database during read transaction index")
	}
	key := txKey(txHash)
	exists, err := db.Has(key)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return 0, false, nil
	}
	data, err := db.Get(key)
	if err != nil {
		return 0, false, err
	}
	if len(data) != 8 {
		return 0, true, fmt.Errorf("rawdb: transaction index %x has length %d, want 8", txHash, len(data))
	}
	return binary.BigEndian.Uint64(data), true, nil
}

// DeleteTransactionInfo removes the per-tx TransactionInfo row for txID.
func DeleteTransactionInfo(db ethdb.KeyValueWriter, txID []byte) error {
	if err := validateTransactionHashKey(txID, "delete transaction info"); err != nil {
		return err
	}
	return db.Delete(txInfoKey(txID))
}

// DeleteTransactionInfosByBlock removes the per-block TransactionRet row for blockNum.
func DeleteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(txInfoBlockKey(blockNum))
}

// DeleteTransactionIndex removes the tx-hash→block-number reverse index row.
func DeleteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte) error {
	if err := validateTransactionHashKey(txHash, "delete transaction index"); err != nil {
		return err
	}
	return db.Delete(txKey(txHash))
}

func validateTransactionHashKey(hash []byte, context string) error {
	if !validTransactionHashKey(hash) {
		return fmt.Errorf("rawdb: transaction hash length %d during %s", len(hash), context)
	}
	return nil
}

func validTransactionHashKey(hash []byte) bool {
	return len(hash) == common.HashLength
}
