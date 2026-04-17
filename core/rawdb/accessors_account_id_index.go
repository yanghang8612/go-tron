package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// WriteAccountIdIndex associates an account ID with an owner address.
// Mirrors java-tron AccountIdIndexStore.put.
func WriteAccountIdIndex(db ethdb.KeyValueWriter, accountID []byte, owner []byte) error {
	if len(accountID) == 0 {
		return fmt.Errorf("account id index: empty accountID")
	}
	if len(owner) == 0 {
		return fmt.Errorf("account id index: empty owner")
	}
	return db.Put(accountIdIndexKey(accountID), owner)
}

// ReadAccountIdIndex returns the owner address registered for accountID, or
// nil if none. Mirrors AccountIdIndexStore.get.
func ReadAccountIdIndex(db ethdb.KeyValueReader, accountID []byte) []byte {
	data, err := db.Get(accountIdIndexKey(accountID))
	if err != nil || len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

// HasAccountIdIndex reports whether accountID is registered.
// Mirrors AccountIdIndexStore.has, used by SetAccountIdActuator's
// uniqueness precheck.
func HasAccountIdIndex(db ethdb.KeyValueReader, accountID []byte) bool {
	ok, _ := db.Has(accountIdIndexKey(accountID))
	return ok
}

// DeleteAccountIdIndex removes the mapping.
func DeleteAccountIdIndex(db ethdb.KeyValueWriter, accountID []byte) error {
	return db.Delete(accountIdIndexKey(accountID))
}
