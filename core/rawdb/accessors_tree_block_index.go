package rawdb

import "github.com/ethereum/go-ethereum/ethdb"

// WriteTreeBlockIndex stores the merkle-root identifier for a given block
// number. Mirrors java-tron TreeBlockIndexStore.put. Payload is treated
// as opaque — it's whatever MerkleContainer serialised.
func WriteTreeBlockIndex(db ethdb.KeyValueWriter, blockNum int64, treeKey []byte) error {
	return db.Put(treeBlockIndexKey(blockNum), treeKey)
}

// ReadTreeBlockIndex returns the merkle-root identifier or nil if absent.
func ReadTreeBlockIndex(db ethdb.KeyValueReader, blockNum int64) []byte {
	data, err := db.Get(treeBlockIndexKey(blockNum))
	if err != nil || len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

// DeleteTreeBlockIndex removes the entry.
func DeleteTreeBlockIndex(db ethdb.KeyValueWriter, blockNum int64) error {
	return db.Delete(treeBlockIndexKey(blockNum))
}
