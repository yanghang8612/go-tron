package state

import (
	"errors"
	"testing"

	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestDomainStateAdaptsAccountKV(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x77)
	dom := sdb.Domains()

	if err := dom.DomainPut(owner, kvdomains.ContractABI, []byte("abi"), []byte("data")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := sdb.GetAccountKV(owner, kvdomains.ContractABI, []byte("abi"))
	if err != nil || !ok || string(got) != "data" {
		t.Fatalf("StateDB account KV = %q ok=%v err=%v", got, ok, err)
	}

	if err := dom.DomainDel(owner, kvdomains.ContractABI, []byte("abi")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err = dom.GetLatest(owner, kvdomains.ContractABI, []byte("abi")); err != nil || ok {
		t.Fatalf("domain delete still visible: ok=%v err=%v", ok, err)
	}
}

func TestDomainOverlayFlushesToStateDBAdapter(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(0x78)
	if err := sdb.SetAccountKV(owner, kvdomains.SystemDelegation, []byte("parent"), []byte("p")); err != nil {
		t.Fatal(err)
	}
	overlay := statedomains.NewOverlay(sdb.Domains())

	got, ok, err := overlay.GetLatest(owner, kvdomains.SystemDelegation, []byte("parent"))
	if err != nil || !ok || string(got) != "p" {
		t.Fatalf("overlay parent read-through = %q ok=%v err=%v", got, ok, err)
	}
	if err := overlay.DomainPut(owner, kvdomains.SystemDelegation, []byte("child"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := overlay.FlushTo(sdb.Domains()); err != nil {
		t.Fatal(err)
	}

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err = reopened.GetAccountKV(owner, kvdomains.SystemDelegation, []byte("child"))
	if err != nil || !ok || string(got) != "c" {
		t.Fatalf("flushed domain value = %q ok=%v err=%v", got, ok, err)
	}
}

func TestDomainStatePrefixDeleteUnsupportedUntilLatestIndex(t *testing.T) {
	sdb := newTestStateDB(t)
	err := sdb.Domains().DomainDelPrefix(testAddr(0x79), kvdomains.SystemDelegation, []byte("prefix"))
	if !errors.Is(err, statedomains.ErrPrefixDeleteUnsupported) {
		t.Fatalf("DomainDelPrefix err = %v", err)
	}
}
