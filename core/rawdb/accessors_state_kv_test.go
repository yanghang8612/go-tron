package rawdb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type cachedStateReadProbe struct {
	ethdb.KeyValueReader
	cachedReads     int
	structuredReads int
}

type ownedStateKVWriteProbe struct {
	putCalled  bool
	owner      common.AccountID
	generation uint64
	domain     uint16
	logicalKey []byte
	value      []byte
}

func (p *ownedStateKVWriteProbe) Put(_, _ []byte) error {
	p.putCalled = true
	return nil
}

func (*ownedStateKVWriteProbe) Delete([]byte) error { return nil }

func (p *ownedStateKVWriteProbe) PutStateKVLatestOwnedValue(_ []byte, owner common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	p.owner = owner
	p.generation = generation
	p.domain = domain
	p.logicalKey = append([]byte(nil), logicalKey...)
	p.value = value
	return nil
}

func (p *cachedStateReadProbe) GetNoCopyCached(key []byte) ([]byte, error) {
	p.cachedReads++
	return p.KeyValueReader.Get(key)
}

func (p *cachedStateReadProbe) GetNoCopyCachedStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) ([]byte, error) {
	p.structuredReads++
	if !bytes.Equal(prefix, stateKVLatestPrefix) {
		return nil, errors.New("unexpected state latest prefix")
	}
	owner := accountID.Address(common.AddressPrefixMainnet)
	return p.KeyValueReader.Get(stateKVLatestKey(owner, generation, kvdomains.KVDomain(domain), logicalKey))
}

func TestStateKVLatestReadWriteEmptyAndAccountID(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x11)
	alias := stateKVTestAddress(0xa0, 0x11)

	if err := WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, []byte("slot"), []byte{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateKVLatest(db, alias, 7, kvdomains.ContractStorage, []byte("slot"))
	if err != nil || !ok || len(got) != 0 {
		t.Fatalf("read via alias = %x ok=%v err=%v, want empty,true,nil", got, ok, err)
	}
}

func TestAppendStateKVLatestValueReusesDestination(t *testing.T) {
	dst := make([]byte, 0, 64)
	encoded := AppendStateKVLatestValue(dst, []byte("first"))
	firstPtr := &encoded[0]
	encoded = AppendStateKVLatestValue(encoded[:0], []byte("replacement"))
	if &encoded[0] != firstPtr {
		t.Fatal("latest value encoder did not reuse destination capacity")
	}
	decoded, err := DecodeStateKVLatestValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded) != "replacement" {
		t.Fatalf("decoded value = %q, want replacement", decoded)
	}
}

func TestWriteStateKVLatestEncodedOwnedUsesStructuredWriter(t *testing.T) {
	owner := stateKVTestAddress(0x41, 0x31)
	logicalKey := []byte("owned-slot")
	encoded := EncodeStateKVLatestValue([]byte("owned-value"))
	probe := new(ownedStateKVWriteProbe)
	if err := WriteStateKVLatestEncodedOwned(probe, owner, 9, kvdomains.ContractStorage, logicalKey, encoded); err != nil {
		t.Fatal(err)
	}
	if probe.putCalled {
		t.Fatal("owned structured writer fell back to Put")
	}
	if probe.owner != owner.AccountID() || probe.generation != 9 || probe.domain != uint16(kvdomains.ContractStorage) || string(probe.logicalKey) != "owned-slot" {
		t.Fatalf("structured write metadata = owner:%x generation:%d domain:%x key:%q", probe.owner, probe.generation, probe.domain, probe.logicalKey)
	}
	if &probe.value[0] != &encoded[0] {
		t.Fatal("structured owned writer copied encoded value")
	}
}

func TestStateKVLatestIterateScopesAndSorts(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x22)
	other := stateKVTestAddress(0x41, 0x23)
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemDelegation, []byte("aa/2"), []byte("2"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemDelegation, []byte("aa/1"), []byte("1"))
	mustWriteStateKVLatest(t, db, owner, 1, kvdomains.SystemDelegation, []byte("aa/new-gen"), []byte("x"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemReward, []byte("aa/wrong-domain"), []byte("x"))
	mustWriteStateKVLatest(t, db, other, 0, kvdomains.SystemDelegation, []byte("aa/wrong-owner"), []byte("x"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemDelegation, []byte("bb/1"), []byte("x"))

	var keys []string
	err := IterateStateKVLatest(db, owner, 0, kvdomains.SystemDelegation, []byte("aa/"), func(k, v []byte) (bool, error) {
		keys = append(keys, string(k)+"="+string(v))
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"aa/1=1", "aa/2=2"}
	if !sameStrings(keys, want) {
		t.Fatalf("keys = %v, want %v", keys, want)
	}
}

func TestStateKVLatestDeletePrefix(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x33)
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("book/1"), []byte("1"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("book/2"), []byte("2"))
	mustWriteStateKVLatest(t, db, owner, 0, kvdomains.SystemMarket, []byte("price/1"), []byte("p"))
	mustWriteStateKVLatest(t, db, owner, 1, kvdomains.SystemMarket, []byte("book/old"), []byte("old"))

	if err := DeleteStateKVLatestPrefix(db, owner, 0, kvdomains.SystemMarket, []byte("book/")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemMarket, []byte("book/1")); err != nil || ok {
		t.Fatalf("book/1 after delete ok=%v err=%v", ok, err)
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 0, kvdomains.SystemMarket, []byte("price/1")); err != nil || !ok || string(got) != "p" {
		t.Fatalf("price/1 = %q ok=%v err=%v", got, ok, err)
	}
	if got, ok, err := ReadStateKVLatest(db, owner, 1, kvdomains.SystemMarket, []byte("book/old")); err != nil || !ok || string(got) != "old" {
		t.Fatalf("new generation = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateKVGenerationRoundTrip(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x44)
	if _, ok, err := ReadStateKVGeneration(db, owner); err != nil || ok {
		t.Fatalf("missing generation ok=%v err=%v", ok, err)
	}
	if err := WriteStateKVGeneration(db, owner, 12); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateKVGeneration(db, owner)
	if err != nil || !ok || got != 12 {
		t.Fatalf("generation = %d ok=%v err=%v, want 12,true,nil", got, ok, err)
	}
}

func TestFlatLatestReadsPreferCachedNoCopyReader(t *testing.T) {
	db := NewMemoryDatabase()
	owner := stateKVTestAddress(0x41, 0x55)
	if err := WriteStateAccountLatest(db, owner, []byte("account")); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVGeneration(db, owner, 9); err != nil {
		t.Fatal(err)
	}
	if err := WriteStateKVLatest(db, owner, 9, kvdomains.ContractStorage, []byte("slot"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	probe := &cachedStateReadProbe{KeyValueReader: db}

	account, ok, err := ReadStateAccountLatest(probe, owner)
	if err != nil || !ok || string(account) != "account" {
		t.Fatalf("account latest = %q ok=%v err=%v", account, ok, err)
	}
	generation, ok, err := ReadStateKVGeneration(probe, owner)
	if err != nil || !ok || generation != 9 {
		t.Fatalf("generation = %d ok=%v err=%v", generation, ok, err)
	}
	value, ok, err := ReadStateKVLatest(probe, owner, 9, kvdomains.ContractStorage, []byte("slot"))
	if err != nil || !ok || string(value) != "value" {
		t.Fatalf("kv latest = %q ok=%v err=%v", value, ok, err)
	}
	if probe.cachedReads != 2 || probe.structuredReads != 1 {
		t.Fatalf("cached reads = %d structured = %d, want 2/1", probe.cachedReads, probe.structuredReads)
	}

	account[0] = 'X'
	value[0] = 'X'
	accountAgain, _, _ := ReadStateAccountLatest(probe, owner)
	valueAgain, _, _ := ReadStateKVLatest(probe, owner, 9, kvdomains.ContractStorage, []byte("slot"))
	if string(accountAgain) != "account" || string(valueAgain) != "value" {
		t.Fatalf("cached no-copy backing storage was mutated: account=%q value=%q", accountAgain, valueAgain)
	}
	if probe.cachedReads != 3 || probe.structuredReads != 2 {
		t.Fatalf("cached reads after replay = %d structured = %d, want 3/2", probe.cachedReads, probe.structuredReads)
	}
}

func mustWriteStateKVLatest(t *testing.T, db stateKVLatestStore, owner common.Address, generation uint64, domain kvdomains.KVDomain, key, value []byte) {
	t.Helper()
	if err := WriteStateKVLatest(db, owner, generation, domain, key, value); err != nil {
		t.Fatal(err)
	}
}

func stateKVTestAddress(prefix, tail byte) common.Address {
	var addr common.Address
	addr[0] = prefix
	addr[20] = tail
	return addr
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal([]byte(a[i]), []byte(b[i])) {
			return false
		}
	}
	return true
}
