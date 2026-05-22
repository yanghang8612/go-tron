package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestSystemKVRoundTrip(t *testing.T) {
	sdb := newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemDynamicProperty, []byte("p"), []byte("42")); err != nil {
		t.Fatalf("put: %v", err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	v, ok, err := reopened.SystemKVGet(kvdomains.SystemDynamicProperty, []byte("p"))
	if err != nil || !ok || string(v) != "42" {
		t.Fatalf("get = %q,%v,%v want 42,true,nil", v, ok, err)
	}
	if !sdb.AccountExists(tcommon.SystemAccountAddress) {
		t.Fatal("system account should exist after a system KV write")
	}
}
