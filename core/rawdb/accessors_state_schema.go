package rawdb

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

// CurrentStateSchemaVersion identifies the mutually incompatible physical
// state layout written by this binary. Version 3 uses slim account envelopes
// and stores the six TRC10 maps, Owner/Witness/Active permissions, votes, and
// Stake V2 lists, TRC10 frozen supply, and AccountResource in account-local KV
// domains.
const CurrentStateSchemaVersion uint64 = 3

func ReadStateSchemaVersion(db ethdb.KeyValueReader) (uint64, bool, error) {
	ok, err := db.Has(stateSchemaVersionKey)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	data, err := db.Get(stateSchemaVersionKey)
	if err != nil {
		return 0, false, err
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("rawdb: state schema version length %d, want 8", len(data))
	}
	return binary.BigEndian.Uint64(data), true, nil
}

func DeleteStateSchemaVersion(db ethdb.KeyValueWriter) error {
	return db.Delete(stateSchemaVersionKey)
}

func WriteStateSchemaVersion(db ethdb.KeyValueWriter, version uint64) error {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], version)
	return db.Put(stateSchemaVersionKey, data[:])
}
