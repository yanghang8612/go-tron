package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
)

// WriteCommitmentBranch persists an encoded BranchData row for the given
// hex-trie prefix.  The encoded bytes are opaque at the rawdb layer.
func WriteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	return db.Put(commitmentBranchKey(prefix), encoded)
}

// ReadCommitmentBranch retrieves the encoded BranchData for prefix.
// Returns (nil, false, nil) when the row is absent.
func ReadCommitmentBranch(db ethdb.KeyValueReader, prefix []byte) ([]byte, bool, error) {
	raw, err := db.Get(commitmentBranchKey(prefix))
	if err != nil {
		// go-ethereum memorydb / pebble both return an error on missing keys.
		return nil, false, nil
	}
	return append([]byte(nil), raw...), true, nil
}

// DeleteCommitmentBranch removes the branch row for prefix.
func DeleteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte) error {
	return db.Delete(commitmentBranchKey(prefix))
}

// IterateCommitmentBranches iterates every branch row in the commitment
// keyspace and calls fn with (logicalPrefix, encodedBranchData).  logicalPrefix
// is the hex-trie prefix as passed to WriteCommitmentBranch (i.e. the physical
// key with stateCommitmentBranchPrefix stripped).  Iteration stops when fn
// returns (false, nil) or an error.
func IterateCommitmentBranches(db ethdb.Iteratee, fn func(prefix, encoded []byte) (bool, error)) error {
	schemaLen := len(stateCommitmentBranchPrefix)
	it := db.NewIterator(stateCommitmentBranchPrefix, nil)
	defer it.Release()
	for it.Next() {
		physKey := it.Key()
		if len(physKey) < schemaLen {
			continue
		}
		logicalPrefix := append([]byte(nil), physKey[schemaLen:]...)
		encoded := append([]byte(nil), it.Value()...)
		cont, err := fn(logicalPrefix, encoded)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

// WriteCommitmentEngineState persists the rewindable staged-engine state blob.
// The bytes are opaque; the staged engine will define their structure.
func WriteCommitmentEngineState(db ethdb.KeyValueWriter, encoded []byte) error {
	return db.Put(stateCommitmentEngineStateKey, encoded)
}

// ReadCommitmentEngineState retrieves the staged-engine state blob.
// Returns (nil, false, nil) when absent.
func ReadCommitmentEngineState(db ethdb.KeyValueReader) ([]byte, bool, error) {
	raw, err := db.Get(stateCommitmentEngineStateKey)
	if err != nil {
		return nil, false, nil
	}
	return append([]byte(nil), raw...), true, nil
}

// commitmentBranchKey builds the physical DB key for a branch row.
func commitmentBranchKey(prefix []byte) []byte {
	k := make([]byte, len(stateCommitmentBranchPrefix)+len(prefix))
	copy(k, stateCommitmentBranchPrefix)
	copy(k[len(stateCommitmentBranchPrefix):], prefix)
	return k
}
