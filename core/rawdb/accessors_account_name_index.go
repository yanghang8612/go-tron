package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

func WriteAccountNameIndex(db ethdb.KeyValueWriter, accountName []byte, owner []byte) error {
	if len(owner) == 0 {
		return fmt.Errorf("account name index: empty owner")
	}
	return db.Put(accountNameIndexKey(accountName), owner)
}

func ReadAccountNameIndex(db ethdb.KeyValueReader, accountName []byte) []byte {
	data, err := db.Get(accountNameIndexKey(accountName))
	if err != nil || len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func HasAccountNameIndex(db ethdb.KeyValueReader, accountName []byte) bool {
	ok, _ := db.Has(accountNameIndexKey(accountName))
	return ok
}

func DeleteAccountNameIndex(db ethdb.KeyValueWriter, accountName []byte) error {
	return db.Delete(accountNameIndexKey(accountName))
}
