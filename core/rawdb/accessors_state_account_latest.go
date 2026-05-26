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
	return db.Put(stateAccountLatestKey(owner), append([]byte(nil), value...))
}

func ReadStateAccountLatest(db ethdb.KeyValueReader, owner common.Address) ([]byte, bool, error) {
	value, err := db.Get(stateAccountLatestKey(owner))
	if err != nil {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func DeleteStateAccountLatest(db ethdb.KeyValueWriter, owner common.Address) error {
	return db.Delete(stateAccountLatestKey(owner))
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
