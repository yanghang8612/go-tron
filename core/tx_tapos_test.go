package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func testVMKVTaposStore(t testing.TB) (vmKVStore, []byte, []byte) {
	t.Helper()
	buf := blockbuffer.New(nil)
	buf.BeginBlock(tcommon.Hash{0x91}, 0x1234)
	blockHash := tcommon.Hash{8: 0x41, 9: 0x42, 10: 0x43, 11: 0x44, 12: 0x45, 13: 0x46, 14: 0x47, 15: 0x48}
	if err := rawdb.WriteTaposRef(buf, 0x1234, blockHash); err != nil {
		t.Fatal(err)
	}
	return vmKVStore{BufferedKVStore: buf}, []byte{0x12, 0x34}, blockHash[8:16]
}

func TestVMKVStoreForwardsTaposSplitRead(t *testing.T) {
	store, ref, want := testVMKVTaposStore(t)
	if got := rawdb.ReadTaposRefNoCopy(store, ref); !bytes.Equal(got, want) {
		t.Fatalf("wrapped TAPOS read = %x, want %x", got, want)
	}
}

func TestVMKVStoreSplitReadFallback(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	first, second := []byte("prefix/"), []byte("suffix")
	want := []byte("value")
	key := append(append([]byte(nil), first...), second...)
	if err := db.Put(key, want); err != nil {
		t.Fatal(err)
	}
	store := vmKVStore{BufferedKVStore: db}
	if got, err := store.GetNoCopyCachedKeyParts(first, second); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("fallback split read = (%q,%v), want (%q,nil)", got, err, want)
	}
}

type vmKVStateCacheForwardProbe struct {
	ethdb.KeyValueStore
	accountReads int
	kvReads      int
	missing      error
}

func (p *vmKVStateCacheForwardProbe) GetNoCopyCachedStateAccountLatest([]byte, tcommon.AccountID) ([]byte, error) {
	p.accountReads++
	return []byte("account-envelope"), nil
}

func (p *vmKVStateCacheForwardProbe) GetNoCopyCachedStateKVLatest([]byte, tcommon.AccountID, uint64, uint16, []byte) ([]byte, error) {
	p.kvReads++
	return rawdb.EncodeStateKVLatestValue([]byte("reward-value")), nil
}

func (p *vmKVStateCacheForwardProbe) IsKeyNotFound(err error) bool {
	return errors.Is(err, p.missing)
}

func TestVMKVStoreForwardsStateCacheReads(t *testing.T) {
	probe := &vmKVStateCacheForwardProbe{KeyValueStore: rawdb.NewMemoryDatabase(), missing: errors.New("missing")}
	store := vmKVStore{BufferedKVStore: probe}
	owner := tcommon.BytesToAddress([]byte{0x41, 0x77})

	account, ok, err := rawdb.ReadStateAccountLatestNoCopy(store, owner)
	if err != nil || !ok || !bytes.Equal(account, []byte("account-envelope")) {
		t.Fatalf("account latest = %q/%v/%v", account, ok, err)
	}
	value, ok, err := rawdb.ReadStateKVLatestNoCopy(store, owner, 3, kvdomains.SystemReward, []byte("reward"))
	if err != nil || !ok || !bytes.Equal(value, []byte("reward-value")) {
		t.Fatalf("state kv latest = %q/%v/%v", value, ok, err)
	}
	if probe.accountReads != 1 || probe.kvReads != 1 {
		t.Fatalf("forwarded reads account=%d kv=%d, want 1/1", probe.accountReads, probe.kvReads)
	}
	if !store.IsKeyNotFound(probe.missing) {
		t.Fatal("missing-key classifier was not forwarded")
	}
}

func BenchmarkVMKVStoreReadTaposRefNoCopy(b *testing.B) {
	store, ref, _ := testVMKVTaposStore(b)
	// processBlock boxes vmKVStore into BufferedKVStore once per block before
	// transaction iteration. Pre-box it here so the benchmark measures each
	// TAPOS read rather than a concrete-value-to-interface conversion.
	var reader ethdb.KeyValueReader = store
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if got := rawdb.ReadTaposRefNoCopy(reader, ref); len(got) != 8 {
			b.Fatalf("wrapped TAPOS read = %x", got)
		}
	}
}
