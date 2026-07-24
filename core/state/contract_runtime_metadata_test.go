package state

import (
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var (
	contractRuntimeBenchmarkMeta ContractRuntimeMetadata
	contractRuntimeBenchmarkPB   *contractpb.SmartContract
)

func contractRuntimeFixture(t testing.TB) (tcommon.Address, []byte, *contractpb.SmartContract) {
	t.Helper()
	addr := testAddr(0x71)
	abi := &contractpb.SmartContract_ABI{Entrys: make([]*contractpb.SmartContract_ABI_Entry, 64)}
	for i := range abi.Entrys {
		abi.Entrys[i] = &contractpb.SmartContract_ABI_Entry{Name: fmt.Sprintf("method_%d", i)}
	}
	meta := &contractpb.SmartContract{
		OriginAddress:              testAddr(0x72).Bytes(),
		ContractAddress:            addr.Bytes(),
		Abi:                        abi,
		Bytecode:                   make([]byte, 4096),
		ConsumeUserResourcePercent: 37,
		OriginEnergyLimit:          9_000_000,
		TrxHash:                    mutationTestHash(0x73).Bytes(),
		Version:                    1,
	}
	data, err := proto.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	return addr, data, meta
}

func TestDecodeContractRuntimeMetadataMatchesProtobuf(t *testing.T) {
	addr, data, wantPB := contractRuntimeFixture(t)
	got, err := decodeContractRuntimeMetadata(addr, data)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := contractRuntimeMetadataFromProto(addr, wantPB)
	if got != want {
		t.Fatalf("runtime metadata = %+v, want %+v", got, want)
	}
}

func TestDecodeContractRuntimeMetadataMatchesWireEdgeCases(t *testing.T) {
	addr := testAddr(0x75)
	meta := &contractpb.SmartContract{
		OriginAddress:              make([]byte, tcommon.AddressLength+4),
		ConsumeUserResourcePercent: -7,
		OriginEnergyLimit:          -9,
		TrxHash:                    make([]byte, tcommon.HashLength+8),
		Version:                    -1,
	}
	for i := range meta.OriginAddress {
		meta.OriginAddress[i] = byte(i + 1)
	}
	for i := range meta.TrxHash {
		meta.TrxHash[i] = byte(i + 2)
	}
	data, err := proto.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	// Protobuf's singular-field rule is last-value-wins. Exercise duplicate
	// scalar tags explicitly so the wire scanner cannot silently diverge.
	data = protowire.AppendTag(data, 11, protowire.VarintType)
	data = protowire.AppendVarint(data, 1)
	var decoded contractpb.SmartContract
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := decodeContractRuntimeMetadata(addr, data)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := contractRuntimeMetadataFromProto(addr, &decoded)
	if got != want {
		t.Fatalf("runtime edge metadata = %+v, want %+v", got, want)
	}
}

func TestContractRuntimeUsesWireCacheWithoutMaterializingABI(t *testing.T) {
	addr, _, meta := contractRuntimeFixture(t)
	sdb := newTestStateDB(t)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.SetContract(addr, meta)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	got, ok := reloaded.ContractRuntime(addr)
	if !ok {
		t.Fatal("runtime metadata missing")
	}
	want, _ := contractRuntimeMetadataFromProto(addr, meta)
	if got != want {
		t.Fatalf("runtime metadata = %+v, want %+v", got, want)
	}
	obj := reloaded.getStateObject(addr)
	if obj.contractMeta != nil {
		t.Fatal("runtime metadata materialized full SmartContract")
	}
	if !obj.contractRuntimeLoaded || !obj.contractRuntimeExists {
		t.Fatalf("runtime cache state loaded=%v exists=%v", obj.contractRuntimeLoaded, obj.contractRuntimeExists)
	}
	if gotKey, wantKey := reloaded.storageRowKey(addr, mutationTestHash(0x74)), javaStorageRowKey(addr, mutationTestHash(0x74), meta); gotKey != wantKey {
		t.Fatalf("runtime storage row key = %x, want %x", gotKey, wantKey)
	}
	if obj.contractMeta != nil {
		t.Fatal("storage row key materialized full SmartContract")
	}

	snapshot := reloaded.Snapshot()
	changed := proto.Clone(meta).(*contractpb.SmartContract)
	changed.Version = 0
	changed.ConsumeUserResourcePercent = 88
	changedBytes, err := proto.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.SetAccountKV(addr, kvdomains.ContractMetadata, contractMetaKVKey, changedBytes); err != nil {
		t.Fatal(err)
	}
	changedRuntime, ok := reloaded.ContractRuntime(addr)
	if !ok || changedRuntime.Version != 0 || changedRuntime.ConsumeUserResourcePercent != 88 {
		t.Fatalf("runtime metadata after generic write = %+v ok=%v", changedRuntime, ok)
	}
	reloaded.RevertToSnapshot(snapshot)
	reverted, ok := reloaded.ContractRuntime(addr)
	if !ok || reverted != want {
		t.Fatalf("runtime metadata after revert = %+v ok=%v, want %+v", reverted, ok, want)
	}
}

func BenchmarkDecodeContractRuntimeMetadata(b *testing.B) {
	addr, data, _ := contractRuntimeFixture(b)
	b.Run("protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			meta := new(contractpb.SmartContract)
			if err := proto.Unmarshal(data, meta); err != nil {
				b.Fatal(err)
			}
			contractRuntimeBenchmarkPB = meta
		}
	})
	b.Run("runtime", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			meta, err := decodeContractRuntimeMetadata(addr, data)
			if err != nil {
				b.Fatal(err)
			}
			contractRuntimeBenchmarkMeta = meta
		}
	})
}
