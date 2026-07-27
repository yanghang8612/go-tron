package rawdb

import (
	"bytes"

	"github.com/ethereum/go-ethereum/ethdb"
)

// WriteStateCommitmentDomain stores one opaque commitment-domain row.
func WriteStateCommitmentDomain(db ethdb.KeyValueWriter, logicalKey, value []byte) error {
	ownedValue := append([]byte(nil), value...)
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.PutKeyParts(stateCommitmentDomainPrefix, logicalKey, ownedValue)
	}
	return db.Put(stateCommitmentDomainKey(logicalKey), ownedValue)
}

// ReadStateCommitmentDomain loads one opaque commitment-domain row.
func ReadStateCommitmentDomain(db ethdb.KeyValueReader, logicalKey []byte) ([]byte, bool, error) {
	value, ok, err := readStateCommitmentDomainNoCopy(db, logicalKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append([]byte(nil), value...), true, nil
}

// readStateCommitmentDomainNoCopy is the immediate-consumption form used by
// typed decoders that copy bytes into values before their next DB operation.
// Capable layered readers join the schema prefix and logical suffix directly,
// avoiding an intermediate physical-key allocation.
func readStateCommitmentDomainNoCopy(db ethdb.KeyValueReader, logicalKey []byte) ([]byte, bool, error) {
	var (
		value []byte
		err   error
	)
	if reader, ok := db.(cachedNoCopyKeyPartsReader); ok {
		value, err = reader.GetNoCopyCachedKeyParts(stateCommitmentDomainPrefix, logicalKey)
	} else {
		value, err = db.Get(stateCommitmentDomainKey(logicalKey))
	}
	if err != nil {
		return nil, false, nil
	}
	return value, true, nil
}

// DeleteStateCommitmentDomain deletes one opaque commitment-domain row.
func DeleteStateCommitmentDomain(db ethdb.KeyValueWriter, logicalKey []byte) error {
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.DeleteKeyParts(stateCommitmentDomainPrefix, logicalKey)
	}
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
