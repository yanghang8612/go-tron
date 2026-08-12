package state

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAppendAccountLatestObjectPreparedOwnsProtoInFinalArena(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x10)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 987654)
	obj := sdb.getStateObject(addr)
	wantProto, err := obj.account.MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	obj.accountProto = nil
	encodedSize, protoSize, exists, err := accountLatestObjectEncodedSize(obj)
	if err != nil || !exists {
		t.Fatalf("size: exists=%v err=%v", exists, err)
	}
	if protoSize != len(wantProto) {
		t.Fatalf("proto size = %d, want %d", protoSize, len(wantProto))
	}
	got, exists, err := appendAccountLatestObjectPrepared(make([]byte, 0, encodedSize), obj, true, protoSize)
	if err != nil || !exists {
		t.Fatalf("append: exists=%v err=%v", exists, err)
	}
	want := appendStateAccountV2Fields(nil, StateAccountVersion, wantProto, EmptyKVRoot, obj.accountKVGeneration, obj.codeHash)
	if !bytes.Equal(got, want) {
		t.Fatalf("prepared envelope = %x, want %x", got, want)
	}
	if !bytes.Equal(obj.accountProto, wantProto) || cap(obj.accountProto) != len(obj.accountProto) {
		t.Fatalf("cached proto = %x len/cap=%d/%d", obj.accountProto, len(obj.accountProto), cap(obj.accountProto))
	}
	protoOffset := bytes.Index(got, wantProto)
	if protoOffset < 0 || len(wantProto) == 0 || &got[protoOffset] != &obj.accountProto[0] {
		t.Fatal("cached account proto does not share the final envelope arena")
	}
}

func TestAppendAccountLatestObjectPreparedMinimumV4Core(t *testing.T) {
	account := types.NewAccountFromPB(&corepb.Account{})
	account.Proto().ProtoReflect().SetUnknown([]byte{0x08})
	obj := &stateObject{account: account, accountKVRoot: EmptyKVRoot}
	wantProto, err := account.MarshalStorageCoreV4()
	if err != nil {
		t.Fatal(err)
	}
	encodedSize, protoSize, exists, err := accountLatestObjectEncodedSize(obj)
	if err != nil || !exists || protoSize != len(wantProto) {
		t.Fatalf("size fallback = encoded:%d proto:%d exists:%v cache:%x err:%v", encodedSize, protoSize, exists, obj.accountProto, err)
	}
	got, exists, err := appendAccountLatestObjectPrepared(make([]byte, 0, encodedSize), obj, true, protoSize)
	if err != nil || !exists {
		t.Fatalf("append fallback: exists=%v err=%v", exists, err)
	}
	want := appendStateAccountV2Fields(nil, StateAccountVersion, wantProto, EmptyKVRoot, 0, obj.codeHash)
	if !bytes.Equal(got, want) {
		t.Fatalf("one-byte envelope = %x, want %x", got, want)
	}
}

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

func TestCorruptRootedAccountPoisonsStateAndBlocksCommit(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0x12)
	if err := rawdb.WriteStateAccountLatest(sdb.accountKVIndex(), addr, []byte{0x80}); err != nil {
		t.Fatal(err)
	}

	if sdb.AccountExists(addr) {
		t.Fatal("corrupt rooted account was reported as existing")
	}
	if err := sdb.Error(); err == nil || !strings.Contains(err.Error(), "decode rooted account envelope") {
		t.Fatalf("state error = %v, want rooted account decode failure", err)
	}

	// Even a caller that mistakes the semantic nil for absence cannot publish an
	// empty replacement over the corrupt durable account.
	sdb.CreateAccountWithTime(addr, corepb.AccountType_Normal, 123)
	if _, err := sdb.Commit(); err == nil || !strings.Contains(err.Error(), "prior read failure") {
		t.Fatalf("commit error = %v, want fail-closed state error", err)
	}
}
