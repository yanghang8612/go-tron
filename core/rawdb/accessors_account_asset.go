package rawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// WriteAccountAsset stores the asset balance for (owner, tokenID).
// Mirrors java-tron AccountAssetStore.put.
func WriteAccountAsset(db ethdb.KeyValueWriter, owner []byte, tokenID int64, balance int64) error {
	if len(owner) == 0 {
		return fmt.Errorf("account asset: empty owner")
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(balance))
	return db.Put(accountAssetKey(owner, tokenID), buf[:])
}

// ReadAccountAsset returns the balance for (owner, tokenID) or 0 if absent.
// Mirrors AccountAssetStore.getBalance.
func ReadAccountAsset(db ethdb.KeyValueReader, owner []byte, tokenID int64) int64 {
	data, err := db.Get(accountAssetKey(owner, tokenID))
	if err != nil || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// DeleteAccountAsset clears the (owner, tokenID) entry.
func DeleteAccountAsset(db ethdb.KeyValueWriter, owner []byte, tokenID int64) error {
	return db.Delete(accountAssetKey(owner, tokenID))
}
