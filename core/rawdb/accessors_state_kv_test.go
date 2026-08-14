package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type discardPointDeleteStore struct {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

func (discardPointDeleteStore) Put([]byte, []byte) error { return nil }
func (discardPointDeleteStore) Delete([]byte) error      { return nil }

type noBatchStateKVStore struct{ db ethdb.KeyValueStore }

func (s noBatchStateKVStore) Has(key []byte) (bool, error)   { return s.db.Has(key) }
func (s noBatchStateKVStore) Get(key []byte) ([]byte, error) { return s.db.Get(key) }
func (s noBatchStateKVStore) Put(key, value []byte) error    { return s.db.Put(key, value) }
func (s noBatchStateKVStore) Delete(key []byte) error        { return s.db.Delete(key) }
func (s noBatchStateKVStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	return s.db.NewIterator(prefix, start)
}

type cancelingStateKVIteratee struct {
	ethdb.Iteratee
	cancel    context.CancelFunc
	remaining int
}

func (db *cancelingStateKVIteratee) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &cancelingStateKVIterator{Iterator: db.Iteratee.NewIterator(prefix, start), parent: db}
}

type cancelingStateKVIterator struct {
	ethdb.Iterator
	parent *cancelingStateKVIteratee
}

func (it *cancelingStateKVIterator) Next() bool {
	ok := it.Iterator.Next()
	if ok && it.parent.remaining > 0 {
		it.parent.remaining--
		if it.parent.remaining == 0 {
			it.parent.cancel()
		}
	}
	return ok
}

func TestIterateStateKVLatestDomainRowsContextCancelsWhileSkippingOtherDomains(t *testing.T) {
	db := NewMemoryDatabase()
	for i := byte(1); i <= 8; i++ {
		owner := common.BytesToAddress([]byte{common.AddressPrefixMainnet, i})
		if err := WriteStateKVLatest(db, owner, 0, kvdomains.SystemAsset, []byte{i}, []byte("value")); err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	wrapped := &cancelingStateKVIteratee{Iteratee: db, cancel: cancel, remaining: 3}
	called := false
	err := IterateStateKVLatestDomainRowsContext(ctx, wrapped, kvdomains.ContractStorage, func(StateKVLatestRow) (bool, error) {
		called = true
		return true, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("iteration error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("target-domain callback called for non-matching rows")
	}
}

func BenchmarkDeleteStateKVPrefixByPointScan(b *testing.B) {
	const rows = 100_000
	base, err := NewPebbleDB(b.TempDir(), 64, 64)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = base.Close() })
	prefix := []byte("point-scan/")
	batch := base.NewBatchWithSize(rows * (len(prefix) + 8))
	for i := uint64(0); i < rows; i++ {
		key := make([]byte, len(prefix)+8)
		copy(key, prefix)
		binary.BigEndian.PutUint64(key[len(prefix):], i)
		if err := batch.Put(key, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}
	batch.Reset()
	db := discardPointDeleteStore{KeyValueReader: base, Iteratee: base}
	b.Run("owned-iterator-keys", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it := db.NewIterator(prefix, nil)
			for it.Next() {
				if err := db.Delete(append([]byte(nil), it.Key()...)); err != nil {
					it.Release()
					b.Fatal(err)
				}
			}
			if err := it.Error(); err != nil {
				it.Release()
				b.Fatal(err)
			}
			it.Release()
		}
	})
	b.Run("borrowed-iterator-keys", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := deleteStateKVPrefixByPointScan(db, prefix); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestStateKVLatestReadWriteEmptyAndAccountID(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x11)
	alias := stateKVTestAddress(0xa0, 0x11)

	if err := WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, []byte("slot"), []byte{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateKVLatest(db, alias, 7, kvdomains.ContractStorage, []byte("slot"))
	if err != nil || !ok || len(got) != 0 {
		t.Fatalf("read via alias = %x ok=%v err=%v, want empty,true,nil", got, ok, err)
	}
}

func TestStateKVLatestIterateScopesAndSorts(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x22)
	other := stateKVTestAddress(0x41, 0x23)
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemDelegation, []byte("aa/2"), []byte("2"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemDelegation, []byte("aa/1"), []byte("1"))
	mustWriteStateKVLatest(t, db, owner, 1, kvdomains.SystemDelegation, []byte("aa/new-gen"), []byte("x"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, []byte("aa/wrong-domain"), []byte("x"))
	mustWriteStateKVLatest(t, db, other, 0, kvdomains.SystemDelegation, []byte("aa/wrong-owner"), []byte("x"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemDelegation, []byte("bb/1"), []byte("x"))

	var keys []string
	err := IterateStateKVLatest(db, owner, 0, kvdomains.SystemDelegation, []byte("aa/"), func(k, v []byte) (bool, error) {
		keys = append(keys, string(k)+"="+string(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aa/1=1", "aa/2=2"}
	if !sameStrings(keys, want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
}

func TestStateKVLatestDeletePrefix(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x33)
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("book/1"), []byte("1"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("book/2"), []byte("2"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("price/1"), []byte("p"))
	mustWriteStateKVLatest(t, db, owner, 1, kvdomains.SystemMarket, []byte("book/old"), []byte("old"))

	if err := DeleteStateKVLatestPrefix(db, owner, 0, kvdomains.SystemMarket, []byte("book/")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemMarket, []byte("book/1")); err != nil || ok {
		t.Fatalf("book/1 after delete ok=%v err=%v", ok, err)
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemMarket, []byte("price/1")); err != nil || !ok || string(got) != "p" {
		t.Fatalf("price/1 = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 1, kvdomains.SystemMarket, []byte("book/old")); err != nil || !ok || string(got) != "old" {
		t.Fatalf("new generation = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateKVLatestDeletePrefixPointScanMutatesLiveStore(t *testing.T) {
	base := NewMemoryDatabase()
	db := noBatchStateKVStore{db: base}
	owner := stateKVTestAddress(0x41, 0x34)
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("book/1"), []byte("1"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("book/2"), []byte("2"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("price/1"), []byte("p"))

	if err := DeleteStateKVLatestPrefix(db, owner, 0, kvdomains.SystemMarket, []byte("book/")); err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{[]byte("book/1"), []byte("book/2")} {
		if _, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemMarket, key); err != nil || ok {
			t.Fatalf("%s after point scan delete ok=%v err=%v", key, ok, err)
		}
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemMarket, []byte("price/1")); err != nil || !ok || string(got) != "p" {
		t.Fatalf("unrelated price row = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateKVGenerationRoundTrip(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x44)
	if _, ok, err := ReadStateKVGeneration(db, owner); err != nil || ok {
		t.Fatalf("missing generation ok=%v err=%v", ok, err)
	}
	if err := WriteStateKVGeneration(db, owner, 12); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateKVGeneration(db, owner)
	if err != nil || !ok || got != 12 {
		t.Fatalf("generation = %d ok=%v err=%v, want 12,true,nil", got, ok, err)
	}
}

func mustWriteStateKVLatest(t *testing.T, db stateKVLatestStore, owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) {
	t.Helper()
	if err := WriteStateKVLatest(db, owner, generation, domain, key, value); err != nil {
		t.Fatal(err)
	}
}

func stateKVTestAddress(prefix, tail byte) common.Address {
	var addr common.Address
	addr[0] = prefix
	addr[20] = tail
	return addr
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal([]byte(a[i]), []byte(b[i])) {
			return false
		}
	}
	return true
}
