package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestStateAccountLatestReadWriteDelete(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x55)
	alias := stateKVTestAddress(0xa0, 0x55)
	value := []byte("account-envelope")

	if _, ok, err := ReadStateAccountLatest(db, owner); err != nil || ok {
		t.Fatalf("pre-read = ok:%v err:%v", ok, err)
	}
	if err := WriteStateAccountLatest(db, owner, value); err != nil {
		t.Fatalf("write account latest: %v", err)
	}
	value[0] = 'x'
	got, ok, err := ReadStateAccountLatest(db, alias)
	if err != nil || !ok || !bytes.Equal(got, []byte("account-envelope")) {
		t.Fatalf("read via alias = %q ok=%v err=%v", got, ok, err)
	}
	got[0] = 'x'
	reread, ok, err := ReadStateAccountLatest(db, owner)
	if err != nil || !ok || !bytes.Equal(reread, []byte("account-envelope")) {
		t.Fatalf("reread after mutating result = %q ok=%v err=%v", reread, ok, err)
	}
	if err := DeleteStateAccountLatest(db, owner); err != nil {
		t.Fatalf("delete account latest: %v", err)
	}
	if _, ok, err := ReadStateAccountLatest(db, owner); err != nil || ok {
		t.Fatalf("read after delete = ok:%v err:%v", ok, err)
	}
}

func TestStateAccountLatestIterate(t *testing.T) {
	db := NewMemoryDatabase()
	owner1 := stateKVTestAddress(0x41, 0x61)
	owner2 := stateKVTestAddress(0x41, 0x62)
	if err := WriteStateAccountLatest(db, owner2, []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(db, owner1, []byte("a")); err != nil {
		t.Fatal(err)
	}

	var rows []string
	err := IterateStateAccountLatest(db, nil, func(row StateAccountLatestRow) (bool, error) {
		rows = append(rows, row.Owner.Hex()+"="+string(row.Value))
		row.Value[0] = 'x'
		return true, nil
	})
	if err != nil {
		t.Fatalf("iterate account latest: %v", err)
	}
	want := []string{owner1.Hex() + "=a", owner2.Hex() + "=b"}
	if !sameStrings(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	got, ok, err := ReadStateAccountLatest(db, owner1)
	if err != nil || !ok || !bytes.Equal(got, []byte("a")) {
		t.Fatalf("read after mutating callback value = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateAccountLatestIteratePrefix(t *testing.T) {
	db := NewMemoryDatabase()
	var owner common.Address
	owner[0] = common.AddressPrefixMainnet
	owner[1] = 0xaa
	owner[20] = 0x01
	other := owner
	other[1] = 0xbb
	if err := WriteStateAccountLatest(db, owner, []byte("match")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateAccountLatest(db, other, []byte("skip")); err != nil {
		t.Fatal(err)
	}

	var rows []string
	prefix := owner.AccountID()
	err := IterateStateAccountLatest(db, prefix[:1], func(row StateAccountLatestRow) (bool, error) {
		rows = append(rows, row.Owner.Hex()+"="+string(row.Value))
		return true, nil
	})
	if err != nil {
		t.Fatalf("iterate account latest prefix: %v", err)
	}
	if !sameStrings(rows, []string{owner.Hex() + "=match"}) {
		t.Fatalf("rows = %v", rows)
	}
}

func TestResetMutableStateDeletesStateAccountLatest(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x71)
	if err := WriteStateAccountLatest(db, owner, []byte("account")); err != nil {
		t.Fatal(err)
	}
	if err := ResetMutableState(db); err != nil {
		t.Fatalf("reset mutable state: %v", err)
	}
	if got, ok, err := ReadStateAccountLatest(db, owner); err != nil || ok || got != nil {
		t.Fatalf("account latest after reset = %q ok=%v err=%v, want nil,false,nil", got, ok, err)
	}
}
