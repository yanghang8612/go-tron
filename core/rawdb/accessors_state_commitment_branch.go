package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
)

// branchKeyStackBufLen sizes the stack array used by commitmentBranchKeyInto.
// Wall-format: stateCommitmentBranchPrefix (27 bytes) + a hex-trie prefix up to
// pathLen=64 nibbles. 128 covers every real key with headroom; oversize prefixes
// fall back to heap.
const branchKeyStackBufLen = 128

// WriteCommitmentBranch persists an encoded BranchData row for the given
// hex-trie prefix.  The encoded bytes are opaque at the rawdb layer.
//
// pebble batches and direct Put both copy the key bytes into their internal
// buffer during the call, so passing a stack-backed key slice is safe.
func WriteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	var buf [branchKeyStackBufLen]byte
	return db.Put(commitmentBranchKeyInto(buf[:0], prefix), encoded)
}

// ReadCommitmentBranch retrieves the encoded BranchData for prefix.
// Returns (nil, false, nil) when the row is absent.
//
// The returned slice is a defensive copy of the underlying KV value, so callers
// may retain it past subsequent DB reads. Callers that decode the value inline
// and discard the bytes immediately should prefer ReadCommitmentBranchNoCopy
// — it avoids the copy and is the bulk-sync hot path.
func ReadCommitmentBranch(db ethdb.KeyValueReader, prefix []byte) ([]byte, bool, error) {
	raw, ok, err := ReadCommitmentBranchNoCopy(db, prefix)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append([]byte(nil), raw...), true, nil
}

// ReadCommitmentBranchNoCopy is ReadCommitmentBranch without the trailing copy.
// The returned slice aliases the KV implementation's storage and is only valid
// until the next operation on db. The commitment fold's GetBranch consumes the
// bytes immediately (decodes and copies the leaf-key field) before any further
// DB access, so it can use this variant to skip the per-Get heap copy.
// noCopyKeyValueReader is an optional fast path. A reader whose Get would
// otherwise defensively copy the value (blockbuffer.Buffer on synchronous
// commit, blockbuffer.LayerView on async commit) can expose GetNoCopy to hand
// back its internal slice, so the fold decodes straight from buffer storage and
// skips the ~1.5 KB copy per branch read. Backends without it (pebble, memorydb
// — whose Get already aliases until the next op) use Get.
type noCopyKeyValueReader interface {
	GetNoCopy(key []byte) ([]byte, error)
}

// cachedNoCopyKeyValueReader is implemented by blockbuffer.Buffer and
// blockbuffer.LayerView when a bounded durable-base cache is configured. The
// method remains optional so direct Pebble/memorydb users keep their existing
// semantics and rawdb does not depend on blockbuffer.
type cachedNoCopyKeyValueReader interface {
	GetNoCopyCached(key []byte) ([]byte, error)
}

func ReadCommitmentBranchNoCopy(db ethdb.KeyValueReader, prefix []byte) ([]byte, bool, error) {
	var buf [branchKeyStackBufLen]byte
	key := commitmentBranchKeyInto(buf[:0], prefix)
	if cached, ok := db.(cachedNoCopyKeyValueReader); ok {
		raw, err := cached.GetNoCopyCached(key)
		if err != nil {
			return nil, false, nil
		}
		return raw, true, nil
	}
	if nc, ok := db.(noCopyKeyValueReader); ok {
		raw, err := nc.GetNoCopy(key)
		if err != nil {
			return nil, false, nil
		}
		return raw, true, nil
	}
	raw, err := db.Get(key)
	if err != nil {
		// go-ethereum memorydb / pebble both return an error on missing keys.
		return nil, false, nil
	}
	return raw, true, nil
}

// DeleteCommitmentBranch removes the branch row for prefix.
func DeleteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte) error {
	var buf [branchKeyStackBufLen]byte
	return db.Delete(commitmentBranchKeyInto(buf[:0], prefix))
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

// commitmentBranchKey builds the physical DB key for a branch row. The result
// always escapes to the heap; the Write/Read/Delete accessors above bypass it
// in favour of commitmentBranchKeyInto with a stack buffer.
func commitmentBranchKey(prefix []byte) []byte {
	return commitmentBranchKeyInto(make([]byte, 0, len(stateCommitmentBranchPrefix)+len(prefix)), prefix)
}

// commitmentBranchKeyInto appends the physical key into dst and returns the
// resulting slice. Caller-owned buffer; the returned slice is only safe to use
// while dst's backing array is alive (typically the same stack frame).
func commitmentBranchKeyInto(dst, prefix []byte) []byte {
	dst = append(dst, stateCommitmentBranchPrefix...)
	dst = append(dst, prefix...)
	return dst
}
