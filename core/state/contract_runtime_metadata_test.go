package state

import (
	"bytes"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var contractRuntimeBenchmarkMeta ContractRuntimeMetadata

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

func TestDecodeContractRuntimeMetadataMatchesNative(t *testing.T) {
	addr, _, fixture := contractRuntimeFixture(t)
	for _, input := range []*contractpb.SmartContract{
		{}, fixture,
		{OriginAddress: bytes.Repeat([]byte{0x75}, tcommon.AddressLength+4),
			TrxHash: bytes.Repeat([]byte{0x76}, tcommon.HashLength+8),
			Version: -1, ConsumeUserResourcePercent: -7, OriginEnergyLimit: -9},
	} {
		data, err := statecodec.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeContractRuntimeMetadata(addr, data)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := contractRuntimeMetadataFromProto(addr, input)
		clear(data) // The state cache must not retain the codec's borrowed slices.
		if got != want {
			t.Fatalf("native runtime metadata = %+v, want %+v", got, want)
		}
	}
}

func TestContractRuntimeUsesNativeCacheWithoutMaterializingABI(t *testing.T) {
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

	for _, encoding := range []struct {
		name    string
		marshal func(proto.Message) ([]byte, error)
	}{{"native", statecodec.Marshal}, {"protobuf", proto.Marshal}} {
		t.Run(encoding.name, func(t *testing.T) {
			snapshot := reloaded.Snapshot()
			changed := proto.Clone(meta).(*contractpb.SmartContract)
			changed.Version = 0
			changed.ConsumeUserResourcePercent = 88
			changedBytes, err := encoding.marshal(changed)
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
		})
	}
}

func BenchmarkDecodeContractRuntimeMetadata(b *testing.B) {
	for _, entries := range []int{0, 8, 64} {
		addr, input := contractRuntimeBenchmarkFixture(b, entries)
		for _, native := range []bool{true, false} {
			encoding, marshal, unmarshal := "protobuf", proto.Marshal, proto.Unmarshal
			if native {
				encoding, marshal, unmarshal = "native", statecodec.Marshal, statecodec.Unmarshal
			}
			data, err := marshal(input)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%s/entries=%d", encoding, entries), func(b *testing.B) {
				b.Run("full", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(data)))
					for b.Loop() {
						meta := new(contractpb.SmartContract)
						if err := unmarshal(data, meta); err != nil {
							b.Fatal(err)
						}
						contractRuntimeBenchmarkMeta, _ = contractRuntimeMetadataFromProto(addr, meta)
					}
				})
				b.Run("runtime", func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(data)))
					for b.Loop() {
						meta, err := decodeContractRuntimeMetadata(addr, data)
						if err != nil {
							b.Fatal(err)
						}
						contractRuntimeBenchmarkMeta = meta
					}
				})
			})
		}
	}
}

func contractRuntimeBenchmarkFixture(t testing.TB, entries int) (tcommon.Address, *contractpb.SmartContract) {
	t.Helper()
	addr, _, input := contractRuntimeFixture(t)
	if entries == 0 {
		return addr, &contractpb.SmartContract{}
	}
	input.Abi.Entrys = input.Abi.Entrys[:entries]
	for _, entry := range input.Abi.Entrys {
		entry.Type = contractpb.SmartContract_ABI_Entry_Function
		entry.StateMutability = contractpb.SmartContract_ABI_Entry_View
		entry.Inputs = []*contractpb.SmartContract_ABI_Entry_Param{{Name: "to", Type: "address"}, {Name: "amount", Type: "uint256"}}
		entry.Outputs = []*contractpb.SmartContract_ABI_Entry_Param{{Name: "success", Type: "bool"}}
	}
	return addr, input
}

// Each iteration opens a fresh StateDB over the committed native rows, so it
// includes first-read account/KV work and never measures the runtime cache hit.
// This uses the in-memory database and does not represent production disk I/O.
func BenchmarkContractRuntimeNativeFirstRead(b *testing.B) {
	for _, entries := range []int{0, 8, 64} {
		addr, input := contractRuntimeBenchmarkFixture(b, entries)
		sdb := newTestStateDB(b)
		sdb.CreateAccount(addr, corepb.AccountType_Contract)
		sdb.SetContract(addr, input)
		root, err := sdb.Commit()
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			for _, full := range []bool{true, false} {
				name := "runtime"
				if full {
					name = "GetContract"
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						fresh, err := New(root, sdb.db)
						if err != nil {
							b.Fatal(err)
						}
						var ok bool
						if full {
							contractRuntimeBenchmarkMeta, ok = contractRuntimeMetadataFromProto(addr, fresh.GetContract(addr))
						} else {
							contractRuntimeBenchmarkMeta, ok = fresh.ContractRuntime(addr)
						}
						if !ok {
							b.Fatal("committed native metadata missing")
						}
					}
				})
			}
		})
	}
}
