package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountSurvivesCommitReopen(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x22)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 9999)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	obj := reopened.getStateObject(addr)
	if obj == nil {
		t.Fatal("account not found after reopen")
	}
	if got := obj.account.Balance(); got != 9999 {
		t.Fatalf("balance = %d, want 9999", got)
	}
	if obj.accountKVRoot != EmptyKVRoot {
		t.Fatalf("accountKVRoot = %x, want EmptyKVRoot", obj.accountKVRoot)
	}
	if obj.accountKVGeneration != 0 {
		t.Fatalf("accountKVGeneration = %d, want 0", obj.accountKVGeneration)
	}
}
