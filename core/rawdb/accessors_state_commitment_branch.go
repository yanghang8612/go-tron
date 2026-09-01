package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type cachedNoCopyKeyPartsReader interface {
	GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error)
}

type cachedNoCopyKeyPartsViewer interface {
	ViewNoCopyCachedKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error)
}

type commitmentParentKeyPartsViewer interface {
	ViewCommitmentParentKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error)
}

// CommitmentBranchKeyspace identifies one physical staged-branch namespace.
// Its zero value and LegacyCommitmentBranchKeyspace both address the original
// complete hot table. A non-zero delta generation is never constructed from
// arbitrary bytes, keeping schema ownership inside rawdb.
type CommitmentBranchKeyspace struct {
	physicalPrefix []byte
}

// CommitmentBranchIterator exposes logical branch prefixes in sorted order
// while keeping the physical schema/generation prefix encapsulated by rawdb.
// Key and Value follow ethdb.Iterator lifetime rules and are invalidated by the
// next call to Next.
type CommitmentBranchIterator struct {
	inner     ethdb.Iterator
	schemaLen int
	err       error
}

func LegacyCommitmentBranchKeyspace() CommitmentBranchKeyspace {
	return CommitmentBranchKeyspace{physicalPrefix: stateCommitmentBranchPrefix}
}

func NewCommitmentBranchDeltaKeyspace(generation uint64) (CommitmentBranchKeyspace, error) {
	if generation == 0 {
		return CommitmentBranchKeyspace{}, errors.New("rawdb: zero commitment branch delta generation")
	}
	prefix := make([]byte, len(stateCommitmentBranchDeltaPrefix)+8)
	copy(prefix, stateCommitmentBranchDeltaPrefix)
	binary.BigEndian.PutUint64(prefix[len(stateCommitmentBranchDeltaPrefix):], generation)
	return CommitmentBranchKeyspace{physicalPrefix: prefix}, nil
}

func (s CommitmentBranchKeyspace) prefix() []byte {
	if len(s.physicalPrefix) == 0 {
		return stateCommitmentBranchPrefix
	}
	return s.physicalPrefix
}

func (s CommitmentBranchKeyspace) key(prefix []byte) []byte {
	schema := s.prefix()
	key := make([]byte, len(schema)+len(prefix))
	copy(key, schema)
	copy(key[len(schema):], prefix)
	return key
}

func (s CommitmentBranchKeyspace) NewIterator(db ethdb.Iteratee) *CommitmentBranchIterator {
	schema := s.prefix()
	return &CommitmentBranchIterator{inner: db.NewIterator(schema, nil), schemaLen: len(schema)}
}

// HasRows reports whether the logical branch namespace contains any physical
// row. Lifecycle cleanup uses this guard to avoid emitting a fresh Pebble range
// tombstone on every periodic rotation after the legacy namespace is empty.
func (s CommitmentBranchKeyspace) HasRows(db ethdb.Iteratee) (bool, error) {
	it := s.NewIterator(db)
	hasRows := it.Next()
	err := it.Error()
	it.Release()
	return hasRows, err
}

func (it *CommitmentBranchIterator) Next() bool {
	if it == nil || it.inner == nil || it.err != nil || !it.inner.Next() {
		return false
	}
	if len(it.inner.Key()) < it.schemaLen {
		it.err = errors.New("rawdb: commitment branch iterator returned a short physical key")
		return false
	}
	return true
}

func (it *CommitmentBranchIterator) Key() []byte {
	if it == nil || it.inner == nil || len(it.inner.Key()) < it.schemaLen {
		return nil
	}
	return it.inner.Key()[it.schemaLen:]
}

func (it *CommitmentBranchIterator) Value() []byte {
	if it == nil || it.inner == nil {
		return nil
	}
	return it.inner.Value()
}

func (it *CommitmentBranchIterator) Error() error {
	if it == nil {
		return nil
	}
	if it.err != nil {
		return it.err
	}
	if it.inner == nil {
		return nil
	}
	return it.inner.Error()
}

func (it *CommitmentBranchIterator) Release() {
	if it != nil && it.inner != nil {
		it.inner.Release()
		it.inner = nil
	}
}

// NewCommitmentParentView returns an optional fold-scoped parent reader. Stores
// that do not expose the capability keep the ordinary per-key path.
func NewCommitmentParentView(db ethdb.KeyValueReader) (pointread.CommitmentParentView, error) {
	viewer, ok := db.(pointread.CommitmentParentViewer)
	if !ok {
		return nil, nil
	}
	return viewer.NewCommitmentParentView()
}

// ViewCommitmentParentBranchInView is the fold-scoped counterpart of
// ViewCommitmentParentBranchNoCopy.
func ViewCommitmentParentBranchInView(view pointread.CommitmentParentView, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	return LegacyCommitmentBranchKeyspace().ViewParentInView(view, prefix, fn)
}

func (s CommitmentBranchKeyspace) ViewParentInView(view pointread.CommitmentParentView, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	if view == nil {
		return false, errors.New("rawdb: nil commitment parent view")
	}
	return view.GetKeyParts(s.prefix(), prefix, fn)
}

// NewCommitmentParentReadSession asks a capable layered reader for a stable,
// cursor-backed parent-state view. nil means the backend does not support the
// optimization and callers should retain the ordinary point-read view.
func NewCommitmentParentReadSession(db ethdb.KeyValueReader, readers int) (pointread.CommitmentParentSession, error) {
	if sessioner, ok := db.(pointread.CommitmentParentSessioner); ok {
		return sessioner.NewCommitmentParentReadSession(readers)
	}
	return nil, nil
}

// ViewCommitmentParentBranchInSession reads one logical branch prefix through
// a previously captured parent-state session.
func ViewCommitmentParentBranchInSession(session pointread.CommitmentParentSession, reader int, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	return LegacyCommitmentBranchKeyspace().ViewParentInSession(session, reader, prefix, fn)
}

func (s CommitmentBranchKeyspace) ViewParentInSession(session pointread.CommitmentParentSession, reader int, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	if session == nil {
		return false, errors.New("rawdb: nil commitment parent session")
	}
	return session.ViewKeyParts(reader, s.prefix(), prefix, fn)
}

// PrefetchParentInSession schedules one exact physical branch lookup through a
// capable parent session. Callers retain the logical trie prefix while rawdb
// owns the active legacy/delta schema prefix.
func (s CommitmentBranchKeyspace) PrefetchParentInSession(session pointread.CommitmentParentPrefetchSession, reader int, prefix []byte) (bool, error) {
	if session == nil {
		return false, errors.New("rawdb: nil commitment parent prefetch session")
	}
	return session.PrefetchKeyParts(reader, s.prefix(), prefix)
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

type keyPartsStringsOwnedValuesBatchWriter interface {
	PutKeyPartsStringsOwnedValuesWithBatchCount(first []byte, seconds []string, values [][]byte, batchCount int) error
}

// keyPartsStringsOwnedValuesArenaBatchWriter is the layered commitment writer
// fast path for a caller whose encoded values occupy the prefix of one owned
// arena and whose spare capacity is exactly large enough for the joined
// physical keys. The writer appends those keys into the same immutable backing
// allocation and retains both key strings and value slices until layer drop.
// Generic writers deliberately omit this interface and retain the ordinary
// ownership/copy contracts below.
type keyPartsStringsOwnedValuesArenaBatchWriter interface {
	PutKeyPartsStringsOwnedValuesInArenaWithBatchCount(first []byte, seconds []string, values [][]byte, arena []byte, batchCount int) error
}

// SupportsCommitmentBranchOwnedValue reports whether db can retain a freshly
// encoded branch value directly. Callers use this to choose between allocating
// the final immutable encoding and reusing a scratch buffer for copying stores.
func SupportsCommitmentBranchOwnedValue(db ethdb.KeyValueWriter) bool {
	_, ok := db.(keyPartsOwnedValueWriter)
	return ok
}

// SupportsCommitmentBranchOwnedBatchArena reports whether db can retain the
// encoded values and their joined physical keys from one shared immutable
// allocation. Callers must use this narrower capability independently of
// SupportsCommitmentBranchOwnedValue: an older layered writer may support
// ownership transfer without supporting the combined batch layout.
func SupportsCommitmentBranchOwnedBatchArena(db ethdb.KeyValueWriter) bool {
	_, ok := db.(keyPartsStringsOwnedValuesArenaBatchWriter)
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
	return LegacyCommitmentBranchKeyspace().Write(db, prefix, encoded)
}

func (s CommitmentBranchKeyspace) Write(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.PutKeyParts(s.prefix(), prefix, encoded)
	}
	return db.Put(s.key(prefix), encoded)
}

// WriteCommitmentBranchOwned is WriteCommitmentBranch for a freshly allocated
// immutable encoding whose ownership the caller transfers to db. A capable
// layered writer retains encoded directly; all other writers fall back to the
// normal copying path. The caller must not mutate encoded after this call.
func WriteCommitmentBranchOwned(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	return LegacyCommitmentBranchKeyspace().WriteOwned(db, prefix, encoded)
}

func (s CommitmentBranchKeyspace) WriteOwned(db ethdb.KeyValueWriter, prefix []byte, encoded []byte) error {
	if writer, ok := db.(keyPartsOwnedValueWriter); ok {
		return writer.PutKeyPartsOwnedValue(s.prefix(), prefix, encoded)
	}
	return s.Write(db, prefix, encoded)
}

// WriteCommitmentBranchOwnedString is the batch-flush form of
// WriteCommitmentBranchOwned. Layered writers can join the already-immutable
// string prefix directly into their map key; generic writers retain the normal
// []byte API and copy semantics through the fallback.
func WriteCommitmentBranchOwnedString(db ethdb.KeyValueWriter, prefix string, encoded []byte) error {
	return LegacyCommitmentBranchKeyspace().WriteOwnedString(db, prefix, encoded)
}

func (s CommitmentBranchKeyspace) WriteOwnedString(db ethdb.KeyValueWriter, prefix string, encoded []byte) error {
	if writer, ok := db.(keyPartsStringOwnedValueWriter); ok {
		return writer.PutKeyPartsStringOwnedValue(s.prefix(), prefix, encoded)
	}
	return s.WriteOwned(db, []byte(prefix), encoded)
}

// WriteCommitmentBranchesOwnedStrings is the sibling-fold batch form of
// WriteCommitmentBranchOwnedString. A layered writer can pack every physical
// key into one immutable arena instead of allocating one backing string per
// branch. Values are already disjoint slices of the fold's immutable encoding
// arena and may be retained directly.
func WriteCommitmentBranchesOwnedStrings(db ethdb.KeyValueWriter, prefixes []string, encoded [][]byte) error {
	return WriteCommitmentBranchesOwnedStringsWithBatchCount(db, prefixes, encoded, 1)
}

// WriteCommitmentBranchesOwnedStringsWithBatchCount carries the number of
// active root-sibling batches to layered writers so their first map allocation
// can reserve for the real fold fan-out rather than the maximum of 16.
func WriteCommitmentBranchesOwnedStringsWithBatchCount(db ethdb.KeyValueWriter, prefixes []string, encoded [][]byte, batchCount int) error {
	return LegacyCommitmentBranchKeyspace().WriteOwnedStringsWithBatchCount(db, prefixes, encoded, batchCount)
}

func (s CommitmentBranchKeyspace) WriteOwnedStringsWithBatchCount(db ethdb.KeyValueWriter, prefixes []string, encoded [][]byte, batchCount int) error {
	if len(prefixes) != len(encoded) {
		return errors.New("rawdb: commitment branch batch length mismatch")
	}
	if writer, ok := db.(keyPartsStringsOwnedValuesBatchWriter); ok {
		return writer.PutKeyPartsStringsOwnedValuesWithBatchCount(s.prefix(), prefixes, encoded, batchCount)
	}
	if writer, ok := db.(keyPartsStringsOwnedValuesWriter); ok {
		return writer.PutKeyPartsStringsOwnedValues(s.prefix(), prefixes, encoded)
	}
	for i, prefix := range prefixes {
		if err := s.WriteOwnedString(db, prefix, encoded[i]); err != nil {
			return err
		}
	}
	return nil
}

// PhysicalKeyBytes returns the exact number of bytes occupied by the joined
// physical keys for prefixes. It lets an ownership-taking caller reserve one
// shared values+keys arena without exposing the keyspace's internal prefix.
func (s CommitmentBranchKeyspace) PhysicalKeyBytes(prefixes []string) int {
	total := len(s.prefix()) * len(prefixes)
	for _, prefix := range prefixes {
		total += len(prefix)
	}
	return total
}

// WriteOwnedStringsInArenaWithBatchCount is
// WriteOwnedStringsWithBatchCount for encoded values occupying arena's current
// length. A capable layered writer appends every joined physical key into the
// spare capacity of that same allocation. The caller must not mutate arena or
// encoded after this call. Unsupported writers ignore the spare capacity and
// retain the existing batch path.
func (s CommitmentBranchKeyspace) WriteOwnedStringsInArenaWithBatchCount(db ethdb.KeyValueWriter, prefixes []string, encoded [][]byte, arena []byte, batchCount int) error {
	if len(prefixes) != len(encoded) {
		return errors.New("rawdb: commitment branch batch length mismatch")
	}
	if writer, ok := db.(keyPartsStringsOwnedValuesArenaBatchWriter); ok {
		return writer.PutKeyPartsStringsOwnedValuesInArenaWithBatchCount(s.prefix(), prefixes, encoded, arena, batchCount)
	}
	return s.WriteOwnedStringsWithBatchCount(db, prefixes, encoded, batchCount)
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
	return LegacyCommitmentBranchKeyspace().ReadNoCopy(db, prefix)
}

func (s CommitmentBranchKeyspace) ReadNoCopy(db ethdb.KeyValueReader, prefix []byte) ([]byte, bool, error) {
	if reader, ok := db.(cachedNoCopyKeyPartsReader); ok {
		raw, err := reader.GetNoCopyCachedKeyParts(s.prefix(), prefix)
		if err != nil {
			return verifyStateReadMiss(db, s.key(prefix), fmt.Sprintf("commitment branch %x", prefix), err)
		}
		return raw, true, nil
	}
	key := s.key(prefix)
	return readValueThenVerifyMiss(db, key, fmt.Sprintf("commitment branch %x", prefix), func(key []byte) ([]byte, error) {
		return readStateNoCopyCached(db, key)
	})
}

// ViewCommitmentBranchNoCopy invokes fn with the encoded branch and reports
// whether the row exists. stable is true when fn may retain slices that alias
// encoded (immutable overlay/cache or an owned Get result); false identifies a
// callback-scoped durable-base view. The callback form lets the commitment
// decoder consume a cold Pebble value before its closer is released instead of
// allocating a full encoded-value copy solely for lifetime extension.
func ViewCommitmentBranchNoCopy(db ethdb.KeyValueReader, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	return LegacyCommitmentBranchKeyspace().ViewNoCopy(db, prefix, fn)
}

func (s CommitmentBranchKeyspace) ViewNoCopy(db ethdb.KeyValueReader, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	if viewer, ok := db.(cachedNoCopyKeyPartsViewer); ok {
		return viewer.ViewNoCopyCachedKeyParts(s.prefix(), prefix, fn)
	}

	encoded, ok, err := s.ReadNoCopy(db, prefix)
	if err != nil || !ok {
		return ok, err
	}
	return true, fn(encoded, true)
}

// ViewCommitmentParentBranchNoCopy is the async-fold counterpart of
// ViewCommitmentBranchNoCopy. A capable layer-bound reader skips the committing
// block's own layer, whose commitment branch namespace is empty at fold start,
// and resolves the parent state directly. Generic readers retain the ordinary
// lookup semantics.
func ViewCommitmentParentBranchNoCopy(db ethdb.KeyValueReader, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	return LegacyCommitmentBranchKeyspace().ViewParentNoCopy(db, prefix, fn)
}

func (s CommitmentBranchKeyspace) ViewParentNoCopy(db ethdb.KeyValueReader, prefix []byte, fn func(encoded []byte, stable bool) error) (bool, error) {
	if viewer, ok := db.(commitmentParentKeyPartsViewer); ok {
		return viewer.ViewCommitmentParentKeyParts(s.prefix(), prefix, fn)
	}
	return s.ViewNoCopy(db, prefix, fn)
}

// DeleteCommitmentBranch removes the branch row for prefix.
func DeleteCommitmentBranch(db ethdb.KeyValueWriter, prefix []byte) error {
	return LegacyCommitmentBranchKeyspace().Delete(db, prefix)
}

func (s CommitmentBranchKeyspace) Delete(db ethdb.KeyValueWriter, prefix []byte) error {
	if writer, ok := db.(keyPartsWriter); ok {
		return writer.DeleteKeyParts(s.prefix(), prefix)
	}
	return db.Delete(s.key(prefix))
}

type commitmentBranchStore interface {
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

// DeleteCommitmentBranches removes the staged commitment branch keyspace.
// Pebble uses one range tombstone; generic stores fall back to bounded point
// deletion so fresh-database rebuilds do not retain the retired branch state.
func DeleteCommitmentBranches(db commitmentBranchStore) error {
	return LegacyCommitmentBranchKeyspace().DeleteAll(db)
}

func (s CommitmentBranchKeyspace) DeleteAll(db commitmentBranchStore) error {
	prefix := s.prefix()
	if deleter, ok := db.(ethdb.KeyValueRangeDeleter); ok {
		if err := deleter.DeleteRange(prefix, prefixUpperBound(prefix)); err == nil {
			return nil
		} else if !errors.Is(err, ethdb.ErrTooManyKeys) {
			return err
		}
	}
	return deleteCommitmentBranchesByPointScan(db, prefix)
}

// DeleteCommitmentBranchDeltaGenerationsExcept reclaims every complete delta
// namespace except keepGeneration. It is idempotent crash-window cleanup after
// a new immutable base marker has made older generations unreachable.
func DeleteCommitmentBranchDeltaGenerationsExcept(db commitmentBranchStore, keepGeneration uint64) error {
	it := db.NewIterator(stateCommitmentBranchDeltaPrefix, nil)
	var generations []uint64
	var last uint64
	for it.Next() {
		key := it.Key()
		if len(key) < len(stateCommitmentBranchDeltaPrefix)+8 {
			it.Release()
			return fmt.Errorf("rawdb: short commitment branch delta key length %d", len(key))
		}
		generation := binary.BigEndian.Uint64(key[len(stateCommitmentBranchDeltaPrefix) : len(stateCommitmentBranchDeltaPrefix)+8])
		if generation == keepGeneration || (len(generations) > 0 && generation == last) {
			continue
		}
		generations = append(generations, generation)
		last = generation
	}
	err := it.Error()
	it.Release()
	if err != nil {
		return err
	}
	for _, generation := range generations {
		keyspace, err := NewCommitmentBranchDeltaKeyspace(generation)
		if err != nil {
			return err
		}
		if err := keyspace.DeleteAll(db); err != nil {
			return err
		}
	}
	return nil
}

func deleteCommitmentBranchesByPointScan(db commitmentBranchStore, prefix []byte) error {
	for {
		it := db.NewIterator(prefix, nil)
		keys := make([][]byte, 0, resetScanBatch)
		for it.Next() {
			keys = append(keys, append([]byte(nil), it.Key()...))
			if len(keys) >= resetScanBatch {
				break
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := deleteCommitmentBranchKeys(db, keys); err != nil {
			return err
		}
		if len(keys) < resetScanBatch {
			return nil
		}
	}
}

func deleteCommitmentBranchKeys(db commitmentBranchStore, keys [][]byte) error {
	if batcher, ok := db.(ethdb.Batcher); ok {
		batch := batcher.NewBatch()
		for _, key := range keys {
			if err := batch.Delete(key); err != nil {
				return err
			}
		}
		return batch.Write()
	}
	for _, key := range keys {
		if err := db.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// IterateCommitmentBranches iterates every branch row in the commitment
// keyspace and calls fn with (logicalPrefix, encodedBranchData).  logicalPrefix
// is the hex-trie prefix as passed to WriteCommitmentBranch (i.e. the physical
// key with stateCommitmentBranchPrefix stripped).  Iteration stops when fn
// returns (false, nil) or an error.
func IterateCommitmentBranches(db ethdb.Iteratee, fn func(prefix, encoded []byte) (bool, error)) error {
	return LegacyCommitmentBranchKeyspace().Iterate(db, fn)
}

func (s CommitmentBranchKeyspace) Iterate(db ethdb.Iteratee, fn func(prefix, encoded []byte) (bool, error)) error {
	schema := s.prefix()
	schemaLen := len(schema)
	it := db.NewIterator(schema, nil)
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
	return readPresentValue(db, stateCommitmentEngineStateKey, "commitment engine state")
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
