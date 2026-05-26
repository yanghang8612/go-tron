package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestCommitWritesV2Envelope(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x11)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1234)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	raw, ok, err := rawdb.ReadStateAccountLatest(reopened.accountKVIndex(), addr)
	if err != nil || !ok {
		t.Fatalf("account latest: ok=%v err=%v", ok, err)
	}
	v, err := DecodeStateAccountV2(raw)
	if err != nil {
		t.Fatalf("account latest value is not a StateAccountV2 envelope: %v", err)
	}
	if v.Version != StateAccountVersion {
		t.Fatalf("version = %d", v.Version)
	}
}
