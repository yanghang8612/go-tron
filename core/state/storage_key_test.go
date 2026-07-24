package state

import (
	"bytes"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

var storageRowKeyBenchmarkSink tcommon.Hash

func BenchmarkStateDBStorageRowKey(b *testing.B) {
	addr := tcommon.BytesToAddress(bytes.Repeat([]byte{0x41}, tcommon.AddressLength))
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash{}, NewDatabase(diskdb))
	if err != nil {
		b.Fatal(err)
	}
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, &contractpb.SmartContract{
		ContractAddress: addr.Bytes(),
		TrxHash:         bytes.Repeat([]byte{0x7f}, tcommon.HashLength),
	})
	slots := make([]tcommon.Hash, 1024)
	for i := range slots {
		slots[i][30] = byte(i >> 8)
		slots[i][31] = byte(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storageRowKeyBenchmarkSink = sdb.storageRowKey(addr, slots[i&1023])
	}
}

func TestStateObjectStorageRowKeyMatchesJavaLayout(t *testing.T) {
	addr := testAddr(0x93)
	slot := tcommon.BytesToHash([]byte("slot"))
	tests := []struct {
		name string
		meta *contractpb.SmartContract
	}{
		{name: "legacy_nil"},
		{name: "legacy_creation_hash", meta: &contractpb.SmartContract{TrxHash: bytes.Repeat([]byte{0x31}, tcommon.HashLength)}},
		{name: "version_one", meta: &contractpb.SmartContract{Version: 1, TrxHash: bytes.Repeat([]byte{0x32}, tcommon.HashLength)}},
		{name: "oversized_creation_hash", meta: &contractpb.SmartContract{TrxHash: bytes.Repeat([]byte{0x33}, tcommon.HashLength+7)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := newStateObject(addr, nil)
			got := obj.storageRowKey(slot, tt.meta)
			want := javaStorageRowKey(addr, slot, tt.meta)
			if got != want {
				t.Fatalf("cached row key = %x, want %x", got, want)
			}
			if !obj.storageKeyLayoutCached {
				t.Fatal("storage key layout was not cached")
			}
		})
	}
}

func TestStateDBStorageRowKeyCacheInvalidation(t *testing.T) {
	addr := testAddr(0x94)
	slot := tcommon.BytesToHash([]byte("slot"))
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash{}, NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	first := &contractpb.SmartContract{TrxHash: bytes.Repeat([]byte{0x41}, tcommon.HashLength)}
	sdb.SetContract(addr, first)
	firstKey := sdb.storageRowKey(addr, slot)
	obj := sdb.getStateObject(addr)
	if !obj.storageKeyLayoutCached {
		t.Fatal("initial storage key layout was not cached")
	}

	snapshot := sdb.Snapshot()
	second := &contractpb.SmartContract{Version: 1, TrxHash: bytes.Repeat([]byte{0x42}, tcommon.HashLength)}
	sdb.SetContract(addr, second)
	if obj.storageKeyLayoutCached {
		t.Fatal("SetContract retained a stale storage key layout")
	}
	secondKey := sdb.storageRowKey(addr, slot)
	if want := javaStorageRowKey(addr, slot, second); secondKey != want {
		t.Fatalf("updated row key = %x, want %x", secondKey, want)
	}
	if secondKey == firstKey {
		t.Fatal("metadata layout change did not change the row key")
	}

	sdb.RevertToSnapshot(snapshot)
	if obj.storageKeyLayoutCached {
		t.Fatal("metadata revert retained a stale storage key layout")
	}
	if got := sdb.storageRowKey(addr, slot); got != firstKey {
		t.Fatalf("reverted row key = %x, want %x", got, firstKey)
	}
}

func TestStorageRowKeyFromFlatLatestUsesTypedMetadataReader(t *testing.T) {
	addr := testAddr(0x91)
	slot := tcommon.BytesToHash([]byte("slot"))
	meta := &contractpb.SmartContract{
		Version: 1,
		TrxHash: []byte{
			0x01, 0x02, 0x03, 0x04,
			0x05, 0x06, 0x07, 0x08,
			0x09, 0x0a, 0x0b, 0x0c,
			0x0d, 0x0e, 0x0f, 0x10,
		},
	}
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	reader := &storageKeyLatestReader{
		t:          t,
		owner:      addr,
		generation: 9,
		value:      metaBytes,
	}

	got, err := storageRowKeyFromFlatLatest(reader, addr, 9, slot)
	if err != nil {
		t.Fatalf("storage row key from typed latest reader: %v", err)
	}
	want := javaStorageRowKey(addr, slot, meta)
	if got != want {
		t.Fatalf("row key = %x, want %x", got, want)
	}
	if reader.calls != 1 {
		t.Fatalf("metadata reader calls = %d, want 1", reader.calls)
	}
}

func TestStorageRowKeyFromFlatLatestReturnsReaderError(t *testing.T) {
	wantErr := errors.New("metadata unavailable")
	reader := &storageKeyLatestReader{err: wantErr}
	if _, err := storageRowKeyFromFlatLatest(reader, testAddr(0x92), 1, tcommon.Hash{0x01}); !errors.Is(err, wantErr) {
		t.Fatalf("storage row key error = %v, want %v", err, wantErr)
	}
}

type storageKeyLatestReader struct {
	t          *testing.T
	owner      tcommon.Address
	generation uint64
	value      []byte
	err        error
	calls      int
}

func (r *storageKeyLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	r.calls++
	if r.err != nil {
		return nil, false, r.err
	}
	r.t.Helper()
	if owner != r.owner {
		r.t.Fatalf("owner = %s, want %s", owner.Hex(), r.owner.Hex())
	}
	if generation != r.generation {
		r.t.Fatalf("generation = %d, want %d", generation, r.generation)
	}
	if domain != kvdomains.ContractMetadata {
		r.t.Fatalf("domain = %#04x, want ContractMetadata", uint16(domain))
	}
	if string(key) != string(contractMetaKVKey) {
		r.t.Fatalf("key = %q, want %q", key, contractMetaKVKey)
	}
	return append([]byte(nil), r.value...), len(r.value) > 0, nil
}
