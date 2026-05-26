package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestMemoryStoreDomainIterateSortsScopesAndCopies(t *testing.T) {
	owner := testAddress(0x41)
	other := testAddress(0x42)
	store := NewMemoryStore()
	if err := store.DomainPut(owner, kvdomains.SystemDelegation, []byte("aa/2"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := store.DomainPut(owner, kvdomains.SystemDelegation, []byte("aa/1"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.DomainPut(owner, kvdomains.SystemDelegation, []byte("bb/1"), []byte("skip")); err != nil {
		t.Fatal(err)
	}
	if err := store.DomainPut(owner, kvdomains.SystemReward, []byte("aa/reward"), []byte("skip")); err != nil {
		t.Fatal(err)
	}
	if err := store.DomainPut(other, kvdomains.SystemDelegation, []byte("aa/other"), []byte("skip")); err != nil {
		t.Fatal(err)
	}

	var rows []string
	if err := store.DomainIterate(owner, kvdomains.SystemDelegation, []byte("aa/"), func(key, value []byte) (bool, error) {
		rows = append(rows, string(key)+"="+string(value))
		key[0] = 'x'
		value[0] = 'x'
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !sameFlatRows(rows, []string{"aa/1=one", "aa/2=two"}) {
		t.Fatalf("rows = %v", rows)
	}
	assertMemoryValue(t, store, owner, kvdomains.SystemDelegation, "aa/1", "one")
}

func assertMemoryValue(t *testing.T, store *MemoryStore, owner common.Address, domain kvdomains.KVDomain, key, want string) {
	t.Helper()
	got, ok, err := store.GetLatest(owner, domain, []byte(key))
	if err != nil || !ok || string(got) != want {
		t.Fatalf("%s = %q ok=%v err=%v, want %q,true,nil", key, got, ok, err, want)
	}
}
