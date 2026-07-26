package rawdb

import (
	"bytes"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ancientTxInfos names the freezer table holding marshalled
// `corepb.TransactionRet` blobs keyed by block number (the same payload
// `tib-<num>` stores in Pebble).
const ancientTxInfos = "tx_infos"

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
	if blockNum := ReadTransactionIndex(db, txID); blockNum != nil {
		if info := readTransactionInfoFromBlock(db, *blockNum, txID); info != nil {
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

func readTransactionInfoFromBlock(db *ChainDB, blockNum uint64, txID []byte) *corepb.TransactionInfo {
	if data, ok := readAncient(db, ancientTxInfos, blockNum); ok {
		return findTransactionInfoInRet(data, txID)
	}
	var info *corepb.TransactionInfo
	_, _ = viewRawValue(db.KeyValueStore, txInfoBlockKey(blockNum), func(data []byte) error {
		info = findTransactionInfoInRet(data, txID)
		return nil
	})
	return info
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
