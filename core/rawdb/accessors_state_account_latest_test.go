package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type accountLatestOwnedValueProbe struct {
	ownedCalls int
	putCalls   int
	key        []byte
	value      []byte
}

type accountLatestOwnedKeyValueProbe struct {
	accountLatestOwnedValueProbe
	ownedKeyValueCalls int
}

func (p *accountLatestOwnedKeyValueProbe) PutOwnedKeyValue(key, value []byte) error {
	p.ownedKeyValueCalls++
	p.key = key
	p.value = value
	return nil
}

func (p *accountLatestOwnedValueProbe) Put(key, value []byte) error {
	p.putCalls++
	p.key = append(p.key[:0], key...)
	p.value = append(p.value[:0], value...)
	return nil
}

func (*accountLatestOwnedValueProbe) Delete([]byte) error { return nil }

func (p *accountLatestOwnedValueProbe) PutOwnedValue(key, value []byte) error {
	p.ownedCalls++
	p.key = key
	p.value = value
	return nil
}

func TestWriteStateAccountLatestOwnedByKeyPrefersOwnedWriter(t *testing.T) {
	probe := new(accountLatestOwnedValueProbe)
	key := []byte("physical-account-key")
	value := []byte("fresh-account-envelope")
	if err := WriteStateAccountLatestOwnedByKey(probe, key, value); err != nil {
		t.Fatal(err)
	}
	if probe.ownedCalls != 1 || probe.putCalls != 0 {
		t.Fatalf("writer calls owned/Put = %d/%d, want 1/0", probe.ownedCalls, probe.putCalls)
	}
	if &probe.key[0] != &key[0] || &probe.value[0] != &value[0] {
		t.Fatal("owned writer did not receive the original key/value slices")
	}
}

func TestWriteStateAccountLatestOwnedByKeyPrefersOwnedKeyValueWriter(t *testing.T) {
	probe := new(accountLatestOwnedKeyValueProbe)
	key := []byte("owned-physical-account-key")
	value := []byte("owned-account-envelope")
	if err := WriteStateAccountLatestOwnedByKey(probe, key, value); err != nil {
		t.Fatal(err)
	}
	if probe.ownedKeyValueCalls != 1 || probe.ownedCalls != 0 || probe.putCalls != 0 {
		t.Fatalf("writer calls key-value/value/Put = %d/%d/%d, want 1/0/0", probe.ownedKeyValueCalls, probe.ownedCalls, probe.putCalls)
	}
	if &probe.key[0] != &key[0] || &probe.value[0] != &value[0] {
		t.Fatal("owned key/value writer did not receive the original slices")
	}
}

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

type accountLatestAliasingReadProbe struct {
	ethdb.KeyValueReader
	value []byte
}

func (p *accountLatestAliasingReadProbe) GetNoCopyCached([]byte) ([]byte, error) {
	return p.value, nil
}

func TestReadStateAccountLatestNoCopyAliasesReaderValue(t *testing.T) {
	owner := stateKVTestAddress(0x41, 0x56)
	backing := []byte("account-envelope")
	probe := &accountLatestAliasingReadProbe{value: backing}
	borrowed, ok, err := ReadStateAccountLatestNoCopy(probe, owner)
	if err != nil || !ok || !bytes.Equal(borrowed, backing) {
		t.Fatalf("borrowed read = (%q,%v,%v)", borrowed, ok, err)
	}
	if &borrowed[0] != &backing[0] {
		t.Fatal("no-copy account read copied the reader value")
	}
	owned, ok, err := ReadStateAccountLatest(probe, owner)
	if err != nil || !ok || !bytes.Equal(owned, backing) {
		t.Fatalf("owned read = (%q,%v,%v)", owned, ok, err)
	}
	if &owned[0] == &backing[0] {
		t.Fatal("ordinary account read exposed the reader value")
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
