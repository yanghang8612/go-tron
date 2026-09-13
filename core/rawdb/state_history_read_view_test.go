package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb/pebbledb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type historyViewTestFactory struct {
	ethdb.KeyValueStore
	opened, closed int
	factoryErr     error
	afterCapture   func()
}

// A live source has the structural Get/Iterator/Close methods of a snapshot,
// but its explicit marker is false and must not suppress factory acquisition.
func (*historyViewTestFactory) IsPinnedKeyValueView() bool { return false }

func (f *historyViewTestFactory) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	if f.factoryErr != nil {
		return nil, f.factoryErr
	}
	copyDB := NewMemoryDatabase()
	it := f.KeyValueStore.NewIterator(nil, nil)
	for it.Next() {
		if err := copyDB.Put(it.Key(), it.Value()); err != nil {
			it.Release()
			_ = copyDB.Close()
			return nil, err
		}
	}
	err := it.Error()
	it.Release()
	if err != nil {
		_ = copyDB.Close()
		return nil, err
	}
	f.opened++
	if f.afterCapture != nil {
		f.afterCapture()
	}
	return &historyViewTestSnapshot{KeyValueStore: copyDB, factory: f}, nil
}

// Deliberately lacks a marker: the factory contract itself proves that this
// result is stable, and the acquisition helper must mark its borrowed adapter.
type historyViewTestSnapshot struct {
	ethdb.KeyValueStore
	factory *historyViewTestFactory
}

func (s *historyViewTestSnapshot) Close() error {
	s.factory.closed++
	return s.KeyValueStore.Close()
}

func newHistoryReadViewFixture(t *testing.T) (*historyViewTestFactory, common.Address) {
	t.Helper()
	f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
	t.Cleanup(func() { _ = f.KeyValueStore.Close() })
	owner := common.Address{common.AddressPrefixMainnet, 0x55}
	for block := uint64(10); block <= 11; block++ {
		change := &StateDomainChange{BlockNum: block, Seq: 1, TxNum: block * 10,
			FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.SystemDelegation,
			Key: []byte("shared-key"), PrevExists: true, Prev: []byte{byte(block)}}
		if err := WriteStateDomainChangeBlockRows(f.KeyValueStore, []*StateDomainChange{change}); err != nil {
			t.Fatal(err)
		}
		if err := WriteStateDomainChangePostingIndex(f.KeyValueStore, change); err != nil {
			t.Fatal(err)
		}
		if err := WriteStateTxRange(f.KeyValueStore, block, common.Hash{byte(block)}, block*10, block*10); err != nil {
			t.Fatal(err)
		}
	}
	return f, owner
}

func TestStateHistoryReadViewQueryPinsOnceAcrossNestedReads(t *testing.T) {
	tests := []struct {
		name string
		want int
		run  func(*historyViewTestFactory, common.Address, func(*StateDomainChange) (bool, error)) error
	}{
		{"point", 1, func(f *historyViewTestFactory, _ common.Address, visit func(*StateDomainChange) (bool, error)) error {
			row, exists, err := ReadStateDomainChange(f, 10, 1)
			if err == nil && exists {
				_, err = visit(row)
			}
			return err
		}},
		{"block", 1, func(f *historyViewTestFactory, _ common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChanges(f, 10, visit)
		}},
		{"block-range", 2, func(f *historyViewTestFactory, _ common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChangesByBlockRange(f, 10, 11, visit)
		}},
		{"tx-range", 2, func(f *historyViewTestFactory, _ common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChangesByTxRange(f, 100, 110, visit)
		}},
		{"block-tx-range", 2, func(f *historyViewTestFactory, _ common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChangesByBlockTxRange(f, 10, 11, 100, 110, visit)
		}},
		{"borrowed", 2, func(f *historyViewTestFactory, _ common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChangesByBlockTxRangeBorrowed(f, 10, 11, 100, 110, visit)
		}},
		{"key", 2, func(f *historyViewTestFactory, owner common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChangesByKeyBlockRange(f, 9, 11, 99, 110, StateFlatDomainKVLatest, owner, 0, kvdomains.SystemDelegation, []byte("shared-key"), visit)
		}},
		{"prefix", 2, func(f *historyViewTestFactory, owner common.Address, visit func(*StateDomainChange) (bool, error)) error {
			return IterateStateDomainChangesByPrefixBlockRange(f, 9, 11, 99, 110, owner, 0, kvdomains.SystemDelegation, []byte("shared"), visit)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, owner := newHistoryReadViewFixture(t)
			// Both packs disappear from the live DB after capture. Every nested
			// posting lookup and pack decode must still see the captured version.
			f.afterCapture = func() {
				for block := uint64(10); block <= 11; block++ {
					if err := f.KeyValueStore.Delete(stateChangeSetKey(block, 0)); err != nil {
						t.Fatal(err)
					}
				}
			}
			rows := 0
			err := test.run(f, owner, func(row *StateDomainChange) (bool, error) {
				if !bytes.Equal(row.Prev, []byte{byte(row.BlockNum)}) {
					t.Fatalf("wrong previous value: %#v", row)
				}
				rows++
				return true, nil
			})
			if err != nil || rows != test.want || f.opened != 1 || f.closed != 1 {
				t.Fatalf("rows=%d want=%d opened=%d closed=%d err=%v", rows, test.want, f.opened, f.closed, err)
			}
		})
	}
}

func TestStateHistoryReadViewBorrowedOwnershipAndErrors(t *testing.T) {
	f, _ := newHistoryReadViewFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	if !view.IsPinnedKeyValueView() {
		t.Fatal("factory result was not marked pinned")
	}
	fail := errors.New("callback stopped")
	if err := IterateStateDomainChanges(view, 10, func(*StateDomainChange) (bool, error) { return false, fail }); !errors.Is(err, fail) {
		t.Fatalf("callback failure = %v", err)
	}
	if f.closed != 0 || f.opened != 1 {
		t.Fatalf("nested reader took ownership: opened=%d closed=%d", f.opened, f.closed)
	}
	if err := release(); err != nil || f.closed != 1 {
		t.Fatalf("release: closed=%d err=%v", f.closed, err)
	}
	f.factoryErr = fail
	if _, _, err := AcquireStateHistoryReadView(f); !errors.Is(err, fail) {
		t.Fatalf("factory failure was hidden: %v", err)
	}
}

func TestStateHistoryReadViewLegacyUnsupportedAndIteratee(t *testing.T) {
	f, _ := newHistoryReadViewFixture(t)
	f.factoryErr = pointread.ErrKeyValueSnapshotUnsupported
	for _, source := range []ethdb.Iteratee{f, struct{ ethdb.Iteratee }{f.KeyValueStore}, NewChainDB(f.KeyValueStore, NoopAncient{})} {
		rows := 0
		if err := IterateStateDomainChanges(source, 10, func(*StateDomainChange) (bool, error) { rows++; return true, nil }); err != nil || rows != 1 {
			t.Fatalf("legacy reader %T rows=%d err=%v", source, rows, err)
		}
	}
	if f.opened != 0 || f.closed != 0 {
		t.Fatal("unsupported factory unexpectedly acquired a snapshot")
	}
	for _, wrapped := range []any{NewChainDB(f.KeyValueStore, NoopAncient{}), WrapKeyValueStore(f.KeyValueStore)} {
		if pointread.SupportsKeyValueSnapshots(wrapped) {
			t.Fatalf("%T advertises snapshots over memorydb", wrapped)
		}
	}
}

func TestStateHistoryReadViewPebbleWrappersAndMarker(t *testing.T) {
	db, err := pebbledb.New(t.TempDir(), 16, 16, "test/history-read-view/", false, pebbledb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, source := range []any{db, NewChainDB(db, NoopAncient{}), WrapKeyValueStore(db)} {
		if !pointread.SupportsKeyValueSnapshots(source) {
			t.Fatalf("%T lost Pebble's snapshot capability", source)
		}
		if err := db.Put([]byte("coherent-key"), []byte("before")); err != nil {
			t.Fatal(err)
		}
		view, release, err := AcquireStateHistoryReadView(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Delete([]byte("coherent-key")); err != nil {
			t.Fatal(err)
		}
		got, readErr := view.Get([]byte("coherent-key"))
		it := view.NewIterator([]byte("coherent-key"), nil)
		iterOK := it.Next() && bytes.Equal(it.Value(), []byte("before"))
		iterErr := it.Error()
		it.Release()
		closeErr := release()
		if readErr != nil || iterErr != nil || closeErr != nil || !bytes.Equal(got, []byte("before")) || !iterOK {
			t.Fatalf("%T fixed view value=%q iter=%v errors=%v/%v/%v", source, got, iterOK, readErr, iterErr, closeErr)
		}
	}
}

// Small one-chunk packs exercise the production decoder without depending on
// writer admission thresholds or a global writer flag. The bytes use the exact
// v3 envelope and chunk encoder; no decoder hooks are installed.
func writeHistoryReadViewSharedFixture(t *testing.T, db ethdb.KeyValueStore, block uint64) {
	t.Helper()
	stored, err := db.Get(stateChangeSetKey(block, 0))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := decodeStateDomainChangeBlockStorage(stored)
	if err != nil {
		t.Fatal(err)
	}
	var payload persistedStateDomainChangeBlock
	if err := rlp.DecodeBytes(raw, &payload); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	pack := append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...)
	pack = append(pack, stateDomainChangeBlockSharedVersion)
	pack = binary.AppendUvarint(pack, block)
	pack = binary.AppendUvarint(pack, uint64(len(raw)))
	pack = append(pack, digest[:]...)
	pack = binary.AppendUvarint(pack, 1)
	pack = binary.AppendUvarint(pack, uint64(len(raw)))
	pack = append(pack, digest[:]...)
	bucket := stateHistoryChunkBucket(block)
	for _, entry := range []struct{ key, value []byte }{
		{stateHistoryChunkKey(bucket, digest), encodeStateHistorySharedChunk(raw)},
		{stateHistoryChunkBucketKey(bucket), []byte{1, 0}},
		{stateChangeSetKey(block, 0), pack},
	} {
		if err := db.Put(entry.key, entry.value); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStateHistoryReadViewSharedPackFailuresNeverBecomeLegacyOrMissing(t *testing.T) {
	for _, mode := range []string{"point", "owning", "borrowed", "physical", "physical-borrowed", "bulk-delete"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := newHistoryReadViewFixture(t)
			writeHistoryReadViewSharedFixture(t, f.KeyValueStore, 10)
			visit := func(*StateDomainChange) (bool, error) { return true, nil }
			run := func() error {
				switch mode {
				case "point":
					_, exists, err := ReadStateDomainChange(f, 10, 1)
					if exists {
						t.Fatal("invalid shared pack returned a row")
					}
					return err
				case "owning":
					return IterateStateDomainChanges(f, 10, visit)
				case "borrowed":
					return IterateStateDomainChangesByBlockTxRangeBorrowed(f, 10, 10, 100, 100, visit)
				case "physical":
					return iteratePhysicalStateDomainChanges(f, 10, visit)
				case "physical-borrowed":
					return iteratePhysicalStateDomainChangesBorrowed(f, 10, visit)
				default:
					return DeleteStateDomainChangeBlocks(f, []uint64{10})
				}
			}
			f.factoryErr = pointread.ErrKeyValueSnapshotUnsupported
			if err := run(); !errors.Is(err, ErrStateHistoryReadViewUnpinned) {
				t.Fatalf("unpinned reference read = %v", err)
			}
			f.factoryErr = nil
			prefix := stateHistoryChunkBucketPrefix(stateHistoryChunkBucket(10))
			if err := f.KeyValueStore.DeleteRange(prefix, prefixUpperBound(prefix)); err != nil {
				t.Fatal(err)
			}
			if err := run(); err == nil || errors.Is(err, ErrStateDomainChangeBorrowedLegacyRows) {
				t.Fatalf("missing referenced chunk was ignored/reclassified: %v", err)
			}
			if f.opened != f.closed {
				t.Fatalf("error leaked snapshots: opened=%d closed=%d", f.opened, f.closed)
			}
		})
	}
}

func TestStateHistoryReadViewSharedRepairAndOwnedBorrowedEquality(t *testing.T) {
	f, owner := newHistoryReadViewFixture(t)
	writeHistoryReadViewSharedFixture(t, f.KeyValueStore, 10)
	var owned, borrowed []byte
	if err := IterateStateDomainChanges(f, 10, func(c *StateDomainChange) (bool, error) { owned = append(owned, c.Prev...); return true, nil }); err != nil {
		t.Fatal(err)
	}
	if err := IterateStateDomainChangesByBlockTxRangeBorrowed(f, 10, 10, 100, 100, func(c *StateDomainChange) (bool, error) { borrowed = append(borrowed, c.Prev...); return true, nil }); err != nil || !bytes.Equal(owned, borrowed) {
		t.Fatalf("shared owning=%x borrowed=%x err=%v", owned, borrowed, err)
	}
	repair := &StateDomainChange{BlockNum: 10, Seq: 1, TxNum: 100, FlatDomain: StateFlatDomainKVLatest,
		Owner: owner, Domain: kvdomains.SystemDelegation, Key: []byte("shared-key"), PrevExists: true, Prev: []byte("repair")}
	if err := WriteStateDomainChangeRow(f.KeyValueStore, repair); err != nil {
		t.Fatal(err)
	}
	row, exists, err := ReadStateDomainChange(f, 10, 1)
	if err != nil || !exists || !bytes.Equal(row.Prev, repair.Prev) {
		t.Fatalf("point repair: row=%v exists=%v err=%v", row, exists, err)
	}
	rows := 0
	if err := IterateStateDomainChanges(f, 10, func(c *StateDomainChange) (bool, error) {
		rows++
		if !bytes.Equal(c.Prev, repair.Prev) {
			t.Fatalf("logical repair previous value = %q", c.Prev)
		}
		return true, nil
	}); err != nil || rows != 1 {
		t.Fatalf("logical repair rows=%d err=%v", rows, err)
	}
	if err := IterateStateDomainChangesByBlockTxRangeBorrowed(f, 10, 10, 100, 100, func(*StateDomainChange) (bool, error) { return true, nil }); !errors.Is(err, ErrStateDomainChangeBorrowedLegacyRows) {
		t.Fatalf("repair requires owning fallback: %v", err)
	}
}

func TestStateHistoryReadViewPebbleSharedPackSurvivesRetirement(t *testing.T) {
	db, err := pebbledb.New(t.TempDir(), 16, 16, "test/history-read-retire/", false, pebbledb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	f, _ := newHistoryReadViewFixture(t)
	it := f.KeyValueStore.NewIterator(nil, nil)
	for it.Next() {
		if err := db.Put(it.Key(), it.Value()); err != nil {
			t.Fatal(err)
		}
	}
	iterErr := it.Error()
	it.Release()
	if iterErr != nil {
		t.Fatal(iterErr)
	}
	writeHistoryReadViewSharedFixture(t, db, 10)
	view, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	// All pack keys are deleted first. Retirement atomically removes chunks
	// and marks the bucket; the older view must still resolve its old pack.
	if err := db.DeleteRange(stateChangeSetBlockPrefix(0), stateChangeSetBlockPrefix(1024)); err != nil {
		t.Fatal(err)
	}
	result, err := RetireStateHistoryChunkBucket(context.Background(), db, stateHistoryChunkBucket(10))
	if err != nil || !result.Retired {
		t.Fatalf("retirement=%+v err=%v", result, err)
	}
	row, exists, err := ReadStateDomainChange(view, 10, 1)
	if err != nil || !exists || !bytes.Equal(row.Prev, []byte{10}) {
		t.Fatalf("retained view row=%v exists=%v err=%v", row, exists, err)
	}
	if row, exists, err := ReadStateDomainChange(db, 10, 1); err != nil || exists || row != nil {
		t.Fatalf("current view row=%v exists=%v err=%v", row, exists, err)
	}
}
