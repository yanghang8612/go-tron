package state

import (
	"testing"

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
	raw, err := reopened.trie.Get(trieKey(addr))
	if err != nil || raw == nil {
		t.Fatalf("trie.Get: data=%v err=%v", raw, err)
	}
	v, err := DecodeStateAccountV2(raw)
	if err != nil {
		t.Fatalf("trie value is not a StateAccountV2 envelope: %v", err)
	}
	if v.Version != StateAccountVersion {
		t.Fatalf("version = %d", v.Version)
	}
}
