package rawdb

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

func ReadPagedDelegationLayout(db ethdb.KeyValueReader) (bool, error) {
	value, exists, err := readPresentValue(db, pagedDelegationLayoutKey, "paged delegation layout")
	if err != nil || !exists {
		return false, err
	}
	if bytes.Equal(value, []byte{0}) {
		return false, fmt.Errorf("uncertified imported delegation layout; sync from genesis")
	}
	if !bytes.Equal(value, []byte{1}) {
		return false, fmt.Errorf("unsupported paged delegation layout marker %x", value)
	}
	return true, nil
}

func MarkPagedDelegationLayout(db ethdb.KeyValueWriter) error {
	return db.Put(pagedDelegationLayoutKey, []byte{1})
}

// InvalidatePagedDelegationLayout leaves a fixed rejection marker, including
// when an interrupted import has not yet installed a canonical head.
func InvalidatePagedDelegationLayout(db ethdb.KeyValueWriter) error {
	if err := db.Put(pagedDelegationLayoutKey, []byte{0}); err != nil {
		return err
	}
	// Imports may write history into a separate database. Persist rejection
	// before those independent writes can become durable.
	if syncer, ok := db.(interface{ SyncKeyValue() error }); ok {
		return syncer.SyncKeyValue()
	}
	return nil
}
