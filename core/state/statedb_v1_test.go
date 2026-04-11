package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestFreezeV1Bandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Bandwidth(addr, 500, 3_000_000)

	obj := sdb.getStateObject(addr)
	if obj == nil {
		t.Fatal("account not found")
	}
	if got := obj.account.TotalFrozenBandwidth(); got != 500 {
		t.Fatalf("frozen bandwidth: want 500, got %d", got)
	}
	list := obj.account.FrozenBandwidthList()
	if len(list) != 1 {
		t.Fatalf("frozen list length: want 1, got %d", len(list))
	}
	if list[0].FrozenBalance != 500 || list[0].ExpireTime != 3_000_000 {
		t.Fatalf("frozen entry: want {500, 3000000}, got {%d, %d}", list[0].FrozenBalance, list[0].ExpireTime)
	}
}

func TestUnfreezeV1Bandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(2)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Bandwidth(addr, 300, 2_000_000)
	sdb.FreezeV1Bandwidth(addr, 200, 5_000_000)

	refunded := sdb.UnfreezeV1Bandwidth(addr, 3_000_000)
	if refunded != 300 {
		t.Fatalf("refunded: want 300, got %d", refunded)
	}
	obj := sdb.getStateObject(addr)
	if got := obj.account.TotalFrozenBandwidth(); got != 200 {
		t.Fatalf("remaining frozen: want 200, got %d", got)
	}
}

func TestFreezeV1Energy(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(3)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Energy(addr, 400, 3_000_000)
	obj := sdb.getStateObject(addr)
	if got := obj.account.FrozenEnergyAmount(); got != 400 {
		t.Fatalf("frozen energy: want 400, got %d", got)
	}

	sdb.FreezeV1Energy(addr, 100, 4_000_000)
	obj = sdb.getStateObject(addr)
	if got := obj.account.FrozenEnergyAmount(); got != 500 {
		t.Fatalf("accumulated energy: want 500, got %d", got)
	}
	if got := obj.account.FrozenEnergyExpireTime(); got != 4_000_000 {
		t.Fatalf("expire time: want 4000000, got %d", got)
	}
}

func TestUnfreezeV1Energy_NotExpired(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(4)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Energy(addr, 400, 5_000_000)

	refunded := sdb.UnfreezeV1Energy(addr, 3_000_000)
	if refunded != 0 {
		t.Fatalf("refunded: want 0, got %d", refunded)
	}

	refunded = sdb.UnfreezeV1Energy(addr, 5_000_000)
	if refunded != 400 {
		t.Fatalf("refunded: want 400, got %d", refunded)
	}
}

func TestFreezeV1DelegatedBandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(5)
	receiver := testAddr(6)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(receiver, corepb.AccountType_Normal)

	sdb.FreezeV1DelegatedBandwidth(owner, receiver, 300)

	ownerObj := sdb.getStateObject(owner)
	if got := ownerObj.account.DelegatedFrozenBandwidth(); got != 300 {
		t.Fatalf("owner delegated: want 300, got %d", got)
	}
	recvObj := sdb.getStateObject(receiver)
	if got := recvObj.account.AcquiredDelegatedFrozenBandwidth(); got != 300 {
		t.Fatalf("receiver acquired: want 300, got %d", got)
	}
}

func TestUnfreezeV1DelegatedBandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(7)
	receiver := testAddr(8)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(receiver, corepb.AccountType_Normal)

	sdb.FreezeV1DelegatedBandwidth(owner, receiver, 300)
	sdb.UnfreezeV1DelegatedBandwidth(owner, receiver, 300)

	ownerObj := sdb.getStateObject(owner)
	if got := ownerObj.account.DelegatedFrozenBandwidth(); got != 0 {
		t.Fatalf("owner delegated: want 0, got %d", got)
	}
	recvObj := sdb.getStateObject(receiver)
	if got := recvObj.account.AcquiredDelegatedFrozenBandwidth(); got != 0 {
		t.Fatalf("receiver acquired: want 0, got %d", got)
	}
}
