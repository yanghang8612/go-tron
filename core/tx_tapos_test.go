package core

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
