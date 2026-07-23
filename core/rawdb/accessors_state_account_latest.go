package rawdb

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type StateAccountLatestRow struct {
	Owner common.Address
	Value []byte
}

func WriteStateAccountLatest(db ethdb.KeyValueWriter, owner common.Address, value []byte) error {
	// KeyValueWriter implementations own/copy Put inputs (Pebble batches,
	// memorydb and blockbuffer all do). Avoid a redundant accessor-level value
	// clone before the writer performs its required ownership copy.
	return WriteStateAccountLatestByKey(db, stateAccountLatestKey(owner), value)
}

// WriteStateAccountLatestByKey writes an account-latest row using a physical
// key already constructed by StateAccountLatestCommitmentKey or its append
// variant. Commit paths use it to build all write keys in one arena.
func WriteStateAccountLatestByKey(db ethdb.KeyValueWriter, physicalKey, value []byte) error {
	return db.Put(physicalKey, value)
}

func ReadStateAccountLatest(db ethdb.KeyValueReader, owner common.Address) ([]byte, bool, error) {
	value, err := readStateNoCopyCached(db, stateAccountLatestKey(owner))
	if err != nil {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func DeleteStateAccountLatest(db ethdb.KeyValueWriter, owner common.Address) error {
	return DeleteStateAccountLatestByKey(db, stateAccountLatestKey(owner))
}

// DeleteStateAccountLatestByKey is the delete counterpart of
// WriteStateAccountLatestByKey.
func DeleteStateAccountLatestByKey(db ethdb.KeyValueWriter, physicalKey []byte) error {
	return db.Delete(physicalKey)
}

func IterateStateAccountLatest(db ethdb.Iteratee, ownerPrefix []byte, fn func(StateAccountLatestRow) (bool, error)) error {
	prefix := stateAccountLatestLogicalPrefix(ownerPrefix)
	headerLen := len(stateAccountLatestPrefix)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) != headerLen+common.AccountIDLength || !bytes.HasPrefix(key, prefix) {
			continue
		}
		var id common.AccountID
		copy(id[:], key[headerLen:])
		cont, err := fn(StateAccountLatestRow{
			Owner: id.Address(common.AddressPrefixMainnet),
			Value: append([]byte(nil), it.Value()...),
		})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	if err := it.Error(); err != nil {
		return fmt.Errorf("iterate state account latest: %w", err)
	}
	return nil
}
