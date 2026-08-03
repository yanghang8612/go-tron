package rawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// TransactionIndexKeyspaceBounds returns the half-open tx-* key range.
func TransactionIndexKeyspaceBounds() (start, limit []byte) {
	return append([]byte(nil), txPrefix...), prefixUpperBound(txPrefix)
}

func TransactionIndexRangeBounds() (start, limit []byte) {
	return TransactionIndexKeyspaceBounds()
}

func EncodeTransactionLocation(blockNum uint64, ordinal int) (uint64, error) {
	if blockNum > transactionLocationMaxBlock {
		return 0, fmt.Errorf("transaction location block %d exceeds %d", blockNum, transactionLocationMaxBlock)
	}
	if ordinal < 0 || ordinal > int(^uint16(0)) {
		return 0, fmt.Errorf("transaction location ordinal %d exceeds %d", ordinal, ^uint16(0))
	}
	return transactionLocationMarker | blockNum<<transactionLocationOrdinalBits | uint64(ordinal), nil
}

func WriteTransactionLocation(db ethdb.KeyValueWriter, txHash []byte, blockNum uint64, ordinal int) error {
	location, err := EncodeTransactionLocation(blockNum, ordinal)
	if err != nil {
		return err
	}
	return WriteEncodedTransactionLocation(db, txHash, location)
}

func WriteEncodedTransactionLocation(db ethdb.KeyValueWriter, txHash []byte, location uint64) error {
	if len(txHash) != 32 {
		return fmt.Errorf("transaction hash length %d, want 32", len(txHash))
	}
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], location)
	return db.Put(txKey(txHash), value[:])
}

// CompactTransactionIndexes rewrites tx-* after covered point deletes.
func CompactTransactionIndexes(db ethdb.Compacter) error {
	start, limit := TransactionIndexKeyspaceBounds()
	return db.Compact(start, limit)
}
