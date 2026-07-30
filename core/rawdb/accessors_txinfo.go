package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ancientTxInfos names the freezer table holding marshalled
// `corepb.TransactionRet` blobs keyed by block number (the same payload
// `tib-<num>` stores in Pebble).
const ancientTxInfos = "tx_infos"

var storedReplayAncientTxInfosCounter = metrics.NewRegisteredCounter("core/stored_replay/ancient_tx_infos", nil)

// HasAncientTransactionInfos reports whether the freezer already owns the
// canonical block-level TransactionRet row. Stored replay uses this to rebuild
// the reset tx-hash location index without writing a duplicate hot tib-* value
// that readers would always bypass in favor of the ancient row.
func HasAncientTransactionInfos(db *ChainDB, blockNum uint64) bool {
	if db == nil || db.AncientReader == nil {
		return false
	}
	ok, err := db.HasAncient(ancientTxInfos, blockNum)
	if err == nil && ok {
		storedReplayAncientTxInfosCounter.Inc(1)
		return true
	}
	return false
}

const (
	transactionLocationMarker      = uint64(1) << 63
	transactionLocationOrdinalBits = 16
	transactionLocationMaxBlock    = (uint64(1) << (63 - transactionLocationOrdinalBits)) - 1
)

type transactionLocation struct {
	blockNumber uint64
	ordinal     uint16
	hasOrdinal  bool
}

type ancientTransactionIndexReader interface {
	TransactionIndexCandidates(hash [32]byte) ([]uint64, error)
	TransactionIndexCoverage() uint64
}

// WriteTransactionInfo stores a legacy, individually indexed TransactionInfo.
// New block commits no longer call this function: tx-* locates the block and
// tib-*/ancient tx_infos owns the canonical TransactionRet payload. It remains
// exported for old-database compatibility and focused migration tests.
func WriteTransactionInfo(db ethdb.KeyValueWriter, txID []byte, info *corepb.TransactionInfo) error {
	data, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return db.Put(txInfoKey(txID), data)
}

// ReadTransactionInfo retrieves a TransactionInfo by txID without requiring a
// duplicate ti-<txID> payload. The compact tx-* reverse index resolves the
// block number, then the canonical per-block TransactionRet is read from the
// hot tib-* row or ancient tx_infos table and searched by ID.
//
// A direct ti-* lookup remains as the final fallback so binaries can be rolled
// out before operators prune legacy rows, and so partially migrated databases
// with a missing tx-* row remain readable.
func ReadTransactionInfo(db *ChainDB, txID []byte) *corepb.TransactionInfo {
	if db == nil {
		return nil
	}
	if location := readTransactionLocation(db, txID); location != nil {
		if info := readTransactionInfoFromBlock(db, *location, txID); info != nil {
			return info
		}
	}
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

func readTransactionInfoFromBlock(db *ChainDB, location transactionLocation, txID []byte) *corepb.TransactionInfo {
	if data, ok := readAncient(db, ancientTxInfos, location.blockNumber); ok {
		return findTransactionInfoAtLocation(db, data, location, txID)
	}
	var info *corepb.TransactionInfo
	_, _ = viewRawValue(db.KeyValueStore, txInfoBlockKey(location.blockNumber), func(data []byte) error {
		info = findTransactionInfoAtLocation(db, data, location, txID)
		return nil
	})
	return info
}

func findTransactionInfoAtLocation(db *ChainDB, data []byte, location transactionLocation, txID []byte) *corepb.TransactionInfo {
	if location.hasOrdinal {
		info := transactionInfoAtOrdinal(data, uint64(location.ordinal))
		if info != nil && (len(info.Id) == 0 || bytes.Equal(info.Id, txID)) {
			if len(info.Id) == 0 {
				info.Id = append([]byte(nil), txID...)
			}
			return info
		}
		// A mismatched packed locator should not hide an otherwise valid legacy
		// row. The ID scan is corruption/partial-upgrade fallback, not the hot
		// path for newly written indexes.
		return findTransactionInfoInRet(data, txID)
	}
	if info := findTransactionInfoInRet(data, txID); info != nil {
		return info
	}
	ordinal, ok := transactionOrdinalInBlock(db, location.blockNumber, txID)
	if !ok {
		return nil
	}
	info := transactionInfoAtOrdinal(data, ordinal)
	if info == nil || (len(info.Id) > 0 && !bytes.Equal(info.Id, txID)) {
		return nil
	}
	if len(info.Id) == 0 {
		info.Id = append([]byte(nil), txID...)
	}
	return info
}

func transactionOrdinalInBlock(db *ChainDB, blockNum uint64, txID []byte) (uint64, bool) {
	block := ReadBlock(db, blockNum)
	if block == nil {
		return 0, false
	}
	for i, tx := range block.Transactions() {
		hash := tx.Hash()
		if bytes.Equal(hash[:], txID) {
			return uint64(i), true
		}
	}
	return 0, false
}

// findTransactionInfoInRet scans the TransactionRet wire envelope and only
// unmarshals the matching nested TransactionInfo. A mainnet block contains
// about 54 transactions on average, so avoiding a full repeated-message
// unmarshal keeps single-transaction wallet lookups inexpensive.
func findTransactionInfoInRet(data, txID []byte) *corepb.TransactionInfo {
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil
		}
		data = data[tagLen:]
		if number == 3 && wireType == protowire.BytesType {
			payload, fieldLen := protowire.ConsumeBytes(data)
			if fieldLen < 0 {
				return nil
			}
			if transactionInfoWireIDEqual(payload, txID) {
				info := &corepb.TransactionInfo{}
				if err := proto.Unmarshal(payload, info); err != nil {
					return nil
				}
				return info
			}
			data = data[fieldLen:]
			continue
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil
		}
		data = data[fieldLen:]
	}
	return nil
}

func transactionInfoAtOrdinal(data []byte, wanted uint64) *corepb.TransactionInfo {
	var ordinal uint64
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil
		}
		data = data[tagLen:]
		if number == 3 && wireType == protowire.BytesType {
			payload, fieldLen := protowire.ConsumeBytes(data)
			if fieldLen < 0 {
				return nil
			}
			if ordinal == wanted {
				info := &corepb.TransactionInfo{}
				if err := proto.Unmarshal(payload, info); err != nil {
					return nil
				}
				return info
			}
			ordinal++
			data = data[fieldLen:]
			continue
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return nil
		}
		data = data[fieldLen:]
	}
	return nil
}

func transactionInfoWireIDEqual(data, txID []byte) bool {
	for len(data) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return false
		}
		data = data[tagLen:]
		if number == 1 && wireType == protowire.BytesType {
			id, fieldLen := protowire.ConsumeBytes(data)
			return fieldLen >= 0 && bytes.Equal(id, txID)
		}
		fieldLen := protowire.ConsumeFieldValue(number, wireType, data)
		if fieldLen < 0 {
			return false
		}
		data = data[fieldLen:]
	}
	return false
}

// WriteTransactionInfosByBlock stores all TransactionInfos for a block.
func WriteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64, infos []*corepb.TransactionInfo) error {
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
		return decodeTransactionInfosByBlock(db, blockNum, data)
	}
	data, err := db.Get(txInfoBlockKey(blockNum))
	if err != nil {
		return nil
	}
	return decodeTransactionInfosByBlock(db, blockNum, data)
}

func decodeTransactionInfosByBlock(db *ChainDB, blockNum uint64, data []byte) []*corepb.TransactionInfo {
	ret := &corepb.TransactionRet{}
	if err := proto.Unmarshal(data, ret); err != nil {
		return nil
	}
	needsIDs := false
	for _, info := range ret.Transactioninfo {
		if info != nil && len(info.Id) == 0 {
			needsIDs = true
			break
		}
	}
	if !needsIDs {
		return ret.Transactioninfo
	}
	block := ReadBlock(db, blockNum)
	if block == nil || len(block.Transactions()) != len(ret.Transactioninfo) {
		return ret.Transactioninfo
	}
	for i, tx := range block.Transactions() {
		if ret.Transactioninfo[i] != nil && len(ret.Transactioninfo[i].Id) == 0 {
			hash := tx.Hash()
			ret.Transactioninfo[i].Id = append([]byte(nil), hash[:]...)
		}
	}
	return ret.Transactioninfo
}

// WriteTransactionIndex stores a tx-hash to block-number mapping.
func WriteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64) error {
	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, blockNum)
	return db.Put(txKey(txHash), num)
}

// WriteTransactionLocation stores the block number and transaction ordinal in
// the existing 8-byte tx-* value. It is exported for offline upgrades of
// legacy reverse indexes; normal block commits call it indirectly.
func WriteTransactionLocation(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64, ordinal int) error {
	packed, err := EncodeTransactionLocation(blockNum, ordinal)
	if err != nil {
		return err
	}
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], packed)
	return db.Put(txKey(txHash), value[:])
}

// EncodeTransactionLocation returns the persisted block/ordinal value used by
// both hot tx-* rows and immutable transaction-index runs.
func EncodeTransactionLocation(blockNum uint64, ordinal int) (uint64, error) {
	if blockNum > transactionLocationMaxBlock {
		return 0, fmt.Errorf("transaction location block %d exceeds %d", blockNum, transactionLocationMaxBlock)
	}
	if ordinal < 0 || ordinal > int(^uint16(0)) {
		return 0, fmt.Errorf("transaction location ordinal %d exceeds %d", ordinal, ^uint16(0))
	}
	return transactionLocationMarker | blockNum<<transactionLocationOrdinalBits | uint64(ordinal), nil
}

// WriteEncodedTransactionLocation restores one validated tx-* row without
// decoding and repacking its location. It is used by the offline cold-index
// migration to preserve the unarchived hot tail after a namespace DeleteRange.
func WriteEncodedTransactionLocation(db ethdb.KeyValueWriter, txHash []byte, location uint64) error {
	if len(txHash) != 32 {
		return fmt.Errorf("transaction hash length %d, want 32", len(txHash))
	}
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], location)
	return db.Put(txKey(txHash), value[:])
}

func readTransactionLocation(db *ChainDB, txHash []byte) *transactionLocation {
	if db == nil {
		return nil
	}
	data, err := db.Get(txKey(txHash))
	if err == nil && len(data) == 8 {
		location := decodeTransactionLocation(binary.BigEndian.Uint64(data))
		return &location
	}
	if len(txHash) != 32 || db.AncientReader == nil {
		return nil
	}
	reader, ok := db.AncientReader.(ancientTransactionIndexReader)
	if !ok {
		return nil
	}
	var hash [32]byte
	copy(hash[:], txHash)
	candidates, err := reader.TransactionIndexCandidates(hash)
	if err != nil {
		return nil
	}
	for _, encoded := range candidates {
		location := decodeTransactionLocation(encoded)
		if location.blockNumber >= reader.TransactionIndexCoverage() {
			continue
		}
		if transactionLocationMatches(db, location, hash) {
			return &location
		}
	}
	return nil
}

func decodeTransactionLocation(value uint64) transactionLocation {
	if value&transactionLocationMarker == 0 {
		return transactionLocation{blockNumber: value}
	}
	return transactionLocation{
		blockNumber: (value &^ transactionLocationMarker) >> transactionLocationOrdinalBits,
		ordinal:     uint16(value),
		hasOrdinal:  true,
	}
}

func transactionIndexLocationBlock(value uint64) uint64 {
	return decodeTransactionLocation(value).blockNumber
}

// TransactionIndexLocationBlock returns the block component of the persisted
// eight-byte tx-* location without exposing the marker/ordinal bit layout.
func TransactionIndexLocationBlock(value uint64) uint64 {
	return transactionIndexLocationBlock(value)
}

func transactionLocationMatches(db *ChainDB, location transactionLocation, hash [32]byte) bool {
	block := ReadBlock(db, location.blockNumber)
	if block == nil {
		return false
	}
	txs := block.Transactions()
	if location.hasOrdinal {
		ordinal := int(location.ordinal)
		return ordinal < len(txs) && txs[ordinal].Hash() == hash
	}
	for _, tx := range txs {
		if tx.Hash() == hash {
			return true
		}
	}
	return false
}

// HasAncientTransactionIndex reports whether a manifest-selected immutable
// transaction-index run covers blockNum. Stored replay uses it to avoid
// recreating historical tx-* rows that were safely removed after publication.
func HasAncientTransactionIndex(db *ChainDB, blockNum uint64) bool {
	if db == nil || db.AncientReader == nil {
		return false
	}
	reader, ok := db.AncientReader.(ancientTransactionIndexReader)
	return ok && blockNum < reader.TransactionIndexCoverage()
}

// ReadTransactionIndex retrieves the block number for a tx hash. Recent rows
// resolve from Pebble; V2-covered history falls through to the immutable cold
// index and verifies the full hash against the canonical block body.
func ReadTransactionIndex(db *ChainDB, txHash []byte) *uint64 {
	location := readTransactionLocation(db, txHash)
	if location == nil {
		return nil
	}
	num := location.blockNumber
	return &num
}

// DeleteTransactionInfo removes the per-tx TransactionInfo row for txID.
func DeleteTransactionInfo(db ethdb.KeyValueWriter, txID []byte) error {
	return db.Delete(txInfoKey(txID))
}

// HasLegacyTransactionInfos reports whether the deprecated ti-* keyspace still
// contains at least one live row.
func HasLegacyTransactionInfos(db ethdb.Iteratee) (bool, error) {
	it := db.NewIterator(txInfoPrefix, nil)
	defer it.Release()
	found := it.Next()
	if err := it.Error(); err != nil {
		return false, err
	}
	return found, nil
}

// DeleteLegacyTransactionInfos hides the complete deprecated ti-* keyspace
// with one range tombstone. It deliberately does not compact: physical
// reclamation can require hundreds of GiB of I/O and is an explicit operator
// choice exposed by CompactLegacyTransactionInfos.
func DeleteLegacyTransactionInfos(db ethdb.KeyValueRangeDeleter) error {
	start, limit := LegacyTransactionInfoRangeBounds()
	return db.DeleteRange(start, limit)
}

// CompactLegacyTransactionInfos physically reclaims obsolete ti-* SST data.
func CompactLegacyTransactionInfos(db ethdb.Compacter) error {
	start, limit := LegacyTransactionInfoRangeBounds()
	return db.Compact(start, limit)
}

// LegacyTransactionInfoRangeBounds returns the half-open ti-* key range. The
// upper bound is ti. and therefore cannot overlap the tib-* block rows.
func LegacyTransactionInfoRangeBounds() (start, limit []byte) {
	return append([]byte(nil), txInfoPrefix...), prefixUpperBound(txInfoPrefix)
}

// DeleteTransactionInfosByBlock removes the per-block TransactionRet row for blockNum.
func DeleteTransactionInfosByBlock(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(txInfoBlockKey(blockNum))
}

// DeleteTransactionIndex removes the tx-hash→block-number reverse index row.
func DeleteTransactionIndex(db ethdb.KeyValueWriter, txHash []byte) error {
	return db.Delete(txKey(txHash))
}

// TransactionIndexRangeBounds returns the half-open tx-* key range for
// offline compaction after selected historical rows have been point-deleted.
func TransactionIndexRangeBounds() (start, limit []byte) {
	return append([]byte(nil), txPrefix...), prefixUpperBound(txPrefix)
}

// CompactTransactionIndexes rewrites the complete tx-* range so point-deleted
// historical rows release their SST space immediately.
func CompactTransactionIndexes(db ethdb.Compacter) error {
	start, limit := TransactionIndexRangeBounds()
	return db.Compact(start, limit)
}
