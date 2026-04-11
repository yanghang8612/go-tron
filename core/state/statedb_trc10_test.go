package state

import (
	"testing"
)

func TestGetTRC10Balance_Empty(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 0 {
		t.Fatalf("expected 0 for new account, got %d", got)
	}
}

func TestSetGetTRC10Balance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.SetTRC10Balance(addr, 1_000_001, 500_000)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 500_000 {
		t.Fatalf("expected 500000, got %d", got)
	}
}

func TestAddSubTRC10Balance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.AddTRC10Balance(addr, 1_000_001, 1_000_000)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 1_000_000 {
		t.Fatalf("add: expected 1000000, got %d", got)
	}
	if err := sdb.SubTRC10Balance(addr, 1_000_001, 300_000); err != nil {
		t.Fatalf("sub failed: %v", err)
	}
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 700_000 {
		t.Fatalf("after sub: expected 700000, got %d", got)
	}
}

func TestSubTRC10Balance_Insufficient(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.SetTRC10Balance(addr, 1_000_001, 100)
	err := sdb.SubTRC10Balance(addr, 1_000_001, 200)
	if err == nil {
		t.Fatal("expected ErrInsufficientBalance")
	}
}

func TestFrozenClaimed(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	if sdb.IsFrozenClaimed(addr, 1_000_001, 0) {
		t.Fatal("should not be claimed initially")
	}
	sdb.SetFrozenClaimed(addr, 1_000_001, 0)
	if !sdb.IsFrozenClaimed(addr, 1_000_001, 0) {
		t.Fatal("should be claimed after SetFrozenClaimed")
	}
	if sdb.IsFrozenClaimed(addr, 1_000_001, 1) {
		t.Fatal("index 1 should not be claimed independently")
	}
}

func TestTRC10BalanceIndependentSlots(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.SetTRC10Balance(addr, 1_000_001, 100)
	sdb.SetTRC10Balance(addr, 1_000_002, 200)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 100 {
		t.Fatalf("token 1000001: expected 100, got %d", got)
	}
	if got := sdb.GetTRC10Balance(addr, 1_000_002); got != 200 {
		t.Fatalf("token 1000002: expected 200, got %d", got)
	}
}
