package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// ErrStateHistoryReadViewUnpinned prevents resolving a shared pack through a
// different sequence from the one that supplied its references.
var ErrStateHistoryReadViewUnpinned = errors.New("rawdb: shared history pack requires a pinned key/value view")

// StateHistoryReadView is the common read surface for hot history. A false
// marker is permitted only for legacy self-contained packs. It is not evidence
// that concurrent writes are safe for a higher-level historical query.
type StateHistoryReadView interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
	pointread.PinnedKeyValueView
}

// AcquireStateHistoryReadView borrows an explicitly pinned view, otherwise
// acquires one snapshot from an optional factory. The returned release closes
// only a snapshot owned by this call. Nested history accessors reuse the view;
// they must not unwrap it or open a newer snapshot. Callers performing several
// scans (cold builders, for example) should acquire once around all scans.
//
// Old reader-only/iterator-only stores remain usable for self-contained packs.
// Shared-pack decoders reject these unpinned adapters before any chunk lookup.
func AcquireStateHistoryReadView(source any) (StateHistoryReadView, func() error, error) {
	if view, ok := source.(StateHistoryReadView); ok && view.IsPinnedKeyValueView() {
		return view, closeBorrowedStateHistoryView, nil
	}
	if view, ok := source.(*stateHistoryReadView); ok {
		return view, closeBorrowedStateHistoryView, nil
	}
	if factory, ok := source.(pointread.KeyValueSnapshotter); ok {
		snapshot, err := factory.NewKeyValueSnapshot()
		if err != nil && !errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
			return nil, nil, err
		}
		if err == nil {
			if snapshot == nil {
				return nil, nil, errors.New("rawdb: history snapshot factory returned nil")
			}
			if view, ok := snapshot.(StateHistoryReadView); ok && view.IsPinnedKeyValueView() {
				return view, snapshot.Close, nil
			}
			return &stateHistoryReadView{reader: snapshot, iteratee: snapshot, pinned: true}, snapshot.Close, nil
		}
	}
	reader, _ := source.(ethdb.KeyValueReader)
	iteratee, _ := source.(ethdb.Iteratee)
	if reader == nil && iteratee == nil {
		return nil, nil, errors.New("rawdb: unavailable history reader")
	}
	return &stateHistoryReadView{reader: reader, iteratee: iteratee}, closeBorrowedStateHistoryView, nil
}

func closeBorrowedStateHistoryView() error { return nil }

// ethrawdb.NewDatabase embeds the narrow KeyValueStore and would otherwise
// discard the optional snapshot factory of the underlying engine.
type stateHistorySnapshotDatabase struct {
	ethdb.Database
	source ethdb.KeyValueStore
}

func (db *stateHistorySnapshotDatabase) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	if factory, ok := db.source.(pointread.KeyValueSnapshotter); ok {
		return factory.NewKeyValueSnapshot()
	}
	return nil, pointread.ErrKeyValueSnapshotUnsupported
}

func (db *stateHistorySnapshotDatabase) SupportsKeyValueSnapshots() bool {
	return db != nil && pointread.SupportsKeyValueSnapshots(db.source)
}

type stateHistoryReadView struct {
	reader   ethdb.KeyValueReader
	iteratee ethdb.Iteratee
	pinned   bool
}

func (v *stateHistoryReadView) IsPinnedKeyValueView() bool { return v != nil && v.pinned }
func (v *stateHistoryReadView) Get(key []byte) ([]byte, error) {
	if v.reader == nil {
		return nil, errors.New("rawdb: history view does not support point reads")
	}
	return v.reader.Get(key)
}
func (v *stateHistoryReadView) Has(key []byte) (bool, error) {
	if v.reader == nil {
		return false, errors.New("rawdb: history view does not support point reads")
	}
	return v.reader.Has(key)
}

func (v *stateHistoryReadView) GetWithPresence(key []byte) ([]byte, bool, error) {
	if v.reader == nil {
		return nil, false, errors.New("rawdb: history view does not support point reads")
	}
	return readPresentValue(v.reader, key, "history view")
}
func (v *stateHistoryReadView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if v.iteratee == nil {
		return &stateHistoryErrorIterator{err: errors.New("rawdb: history view does not support iteration")}
	}
	return v.iteratee.NewIterator(prefix, start)
}

type stateHistoryErrorIterator struct{ err error }

func (*stateHistoryErrorIterator) Next() bool     { return false }
func (i *stateHistoryErrorIterator) Error() error { return i.err }
func (*stateHistoryErrorIterator) Key() []byte    { return nil }
func (*stateHistoryErrorIterator) Value() []byte  { return nil }
func (*stateHistoryErrorIterator) Release()       {}

// materializeStateHistorySharedPack is the only decoder bridge allowed to
// fetch referenced chunks. Recognition precedes legacy fallbacks; malformed
// shared envelopes must never be treated as absent or standalone rows.
func materializeStateHistorySharedPack(data []byte, blockNum uint64, readers []ethdb.KeyValueReader) ([]byte, error) {
	if !isStateHistorySharedPack(data) {
		return data, nil
	}
	if len(readers) != 1 {
		return nil, ErrStateHistoryReadViewUnpinned
	}
	marker, ok := readers[0].(pointread.PinnedKeyValueView)
	if !ok || !marker.IsPinnedKeyValueView() {
		return nil, ErrStateHistoryReadViewUnpinned
	}
	raw, err := decodeStateHistorySharedPack(readers[0], data, blockNum)
	if err != nil {
		return nil, fmt.Errorf("rawdb: resolve shared history block %d: %w", blockNum, err)
	}
	return raw, nil
}
