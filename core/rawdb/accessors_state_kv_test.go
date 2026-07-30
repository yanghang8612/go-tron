package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

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
