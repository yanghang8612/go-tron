package rawdb

import (
	"bytes"

	"github.com/ethereum/go-ethereum/ethdb"
)

// WriteStateCommitmentDomain stores one opaque commitment-domain row.
func WriteStateCommitmentDomain(db ethdb.KeyValueWriter, logicalKey, value []byte) error {
	return db.Put(stateCommitmentDomainKey(logicalKey), append([]byte(nil), value...))
}

// ReadStateCommitmentDomain loads one opaque commitment-domain row.
func ReadStateCommitmentDomain(db ethdb.KeyValueReader, logicalKey []byte) ([]byte, bool, error) {
	value, err := db.Get(stateCommitmentDomainKey(logicalKey))
	if err != nil {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

// DeleteStateCommitmentDomain deletes one opaque commitment-domain row.
func DeleteStateCommitmentDomain(db ethdb.KeyValueWriter, logicalKey []byte) error {
	return db.Delete(stateCommitmentDomainKey(logicalKey))
}

// IterateStateCommitmentDomain iterates rows whose logical keys match
// logicalPrefix. The callback receives logical keys with the physical prefix
// removed.
func IterateStateCommitmentDomain(db ethdb.Iteratee, logicalPrefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	prefix := stateCommitmentDomainLogicalPrefix(logicalPrefix)
	headerLen := len(stateCommitmentDomainPrefix)
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		key := it.Key()
		if len(key) < headerLen || !bytes.HasPrefix(key, prefix) {
			continue
		}
		cont, err := fn(append([]byte(nil), key[headerLen:]...), append([]byte(nil), it.Value()...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}
