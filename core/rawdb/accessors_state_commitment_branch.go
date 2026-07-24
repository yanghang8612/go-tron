package rawdb

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
)

type cachedNoCopyKeyPartsReader interface {
	GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error)
}

type cachedNoCopyKeyPartsViewer interface {
	ViewNoCopyCachedKeyParts(first, second []byte, fn func(value []byte, stable bool) error) error
}

// keyPartsWriter is an optional writer fast path for layered stores whose
// native key is a string. It lets them join the fixed schema prefix and trie
// path directly into their owned key instead of allocating an intermediate
// []byte that is immediately copied again by Put.
type keyPartsWriter interface {
	PutKeyParts(first, second, value []byte) error
	DeleteKeyParts(first, second []byte) error
}

// keyPartsOwnedValueWriter is the narrow layered-store extension used when a
// caller has just encoded a value and can transfer its backing bytes. Ordinary
// Put/PutKeyParts retain their defensive-copy contract; this method may retain
// value directly, and the caller must not mutate it after the call.
type keyPartsOwnedValueWriter interface {
	PutKeyPartsOwnedValue(first, second, value []byte) error
}

type keyPartsStringOwnedValueWriter interface {
	PutKeyPartsStringOwnedValue(first []byte, second string, value []byte) error
}

type keyPartsStringsOwnedValuesWriter interface {
	PutKeyPartsStringsOwnedValues(first []byte, seconds []string, values [][]byte) error
}

// SupportsCommitmentBranchOwnedValue reports whether db can retain a freshly
// encoded branch value directly. Callers use this to choose between allocating
// the final immutable encoding and reusing a scratch buffer for copying stores.
func SupportsCommitmentBranchOwnedValue(db ethdb.KeyValueWriter) bool {
	_, ok := db.(keyPartsOwnedValueWriter)
	return ok
}

// WriteCommitmentBranch persists an encoded BranchData row for the given
// hex-trie prefix.  The encoded bytes are opaque at the rawdb layer.
//
// Generic writers receive a key allocated at its exact encoded length. A
// fixed-size local array looks cheaper here, but passing its slice through
// ethdb.KeyValueWriter makes the whole array escape; commitment keys are
// usually much shorter than the previous 128-byte scratch object. Layered
// writers can implement keyPartsWriter and avoid that intermediate key.
func WriteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.PutKeyParts(stateCommitmentBranchPrefix, prefix, encoded)
	}
	return db.Put(commitmentBranchKey(prefix), encoded)
}

// WriteCommitmentBranchOwned is WriteCommitmentBranch for a freshly allocated
// immutable encoding whose ownership the caller transfers to db. A capable
// layered writer retains encoded directly; all other writers fall back to the
// normal copying path. The caller must not mutate encoded after this call.
func WriteCommitmentBranchOwned(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	if writer, ok := db.(keyPartsOwnedValueWriter); ok {
		return writer.PutKeyPartsOwnedValue(stateCommitmentBranchPrefix, prefix, encoded)
	}
	return WriteCommitmentBranch(db, prefix, encoded)
}

// WriteCommitmentBranchOwnedString is the batch-flush form of
// WriteCommitmentBranchOwned. Layered writers can join the already-immutable
// string prefix directly into their map key; generic writers retain the normal
// []byte API and copy semantics through the fallback.
func WriteCommitmentBranchOwnedString(db ethdb.KeyValueWriter, prefix string, encoded []byte) error {
	if writer, ok := db.(keyPartsStringOwnedValueWriter); ok {
		return writer.PutKeyPartsStringOwnedValue(stateCommitmentBranchPrefix, prefix, encoded)
	}
	return WriteCommitmentBranchOwned(db, []byte(prefix), encoded)
}

// WriteCommitmentBranchesOwnedStrings is the sibling-fold batch form of
// WriteCommitmentBranchOwnedString. A layered writer can pack every physical
// key into one immutable arena instead of allocating one backing string per
// branch. Values are already disjoint slices of the fold's immutable encoding
// arena and may be retained directly.
func WriteCommitmentBranchesOwnedStrings(db ethdb.KeyValueWriter, prefixes []string, encoded [][]byte) error {
	if len(prefixes) != len(encoded) {
		return errors.New("rawdb: commitment branch batch length mismatch")
	}
	if writer, ok := db.(keyPartsStringsOwnedValuesWriter); ok {
		return writer.PutKeyPartsStringsOwnedValues(stateCommitmentBranchPrefix, prefixes, encoded)
	}
	for i, prefix := range prefixes {
		if err := WriteCommitmentBranchOwnedString(db, prefix, encoded[i]); err != nil {
			return err
		}
	}
	return nil
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
func ReadCommitmentBranchNoCopy(db ethdb.KeyValueReader, prefix []byte) ([]byte, bool, error) {
	var (
		raw []byte
		err error
	)
	if reader, ok := db.(cachedNoCopyKeyPartsReader); ok {
		raw, err = reader.GetNoCopyCachedKeyParts(stateCommitmentBranchPrefix, prefix)
	} else {
		raw, err = readStateNoCopyCached(db, commitmentBranchKey(prefix))
	}
	if err != nil {
		// go-ethereum memorydb / pebble both return an error on missing keys.
		return nil, false, nil
	}
	return raw, true, nil
}

// ViewCommitmentBranchNoCopy invokes fn with the encoded branch and reports
// whether the row exists. stable is true when fn may retain slices that alias
// encoded (immutable overlay/cache or an owned Get result); false identifies a
// callback-scoped durable-base view. The callback form lets the commitment
// decoder consume a cold Pebble value before its closer is released instead of
// allocating a full encoded-value copy solely for lifetime extension.
func ViewCommitmentBranchNoCopy(db ethdb.KeyValueReader, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	if viewer, ok := db.(cachedNoCopyKeyPartsViewer); ok {
		called := false
		var callbackErr error
		err := viewer.ViewNoCopyCachedKeyParts(stateCommitmentBranchPrefix, prefix, func(encoded []byte, stable bool) error {
			called = true
			callbackErr = fn(encoded, stable)
			return callbackErr
		})
		if called {
			if callbackErr != nil {
				return true, callbackErr
			}
			return true, err
		}
		if err != nil {
			// Match ReadCommitmentBranchNoCopy's long-standing missing-row
			// contract: storage backends surface absence as an error.
			return false, nil
		}
		return false, nil
	}

	encoded, ok, err := ReadCommitmentBranchNoCopy(db, prefix)
	if err != nil || !ok {
		return ok, err
	}
	return true, fn(encoded, true)
}

// DeleteCommitmentBranch removes the branch row for prefix.
func DeleteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte) error {
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.DeleteKeyParts(stateCommitmentBranchPrefix, prefix)
	}
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

// commitmentBranchKey builds the physical DB key for a branch row with an
// exact capacity. The result necessarily escapes through the ethdb interface,
// so minimizing the allocation is more effective than using an oversized
// local array that the compiler also moves to the heap.
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
