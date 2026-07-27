package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

type dynamicPropertyOwnedWriterProbe struct {
	putCalled bool
	key       string
	value     []byte
}

func (p *dynamicPropertyOwnedWriterProbe) Put([]byte, []byte) error {
	p.putCalled = true
	return nil
}

func (*dynamicPropertyOwnedWriterProbe) Delete([]byte) error { return nil }

func (p *dynamicPropertyOwnedWriterProbe) PutStringOwnedValue(key string, value []byte) error {
	p.key = key
	p.value = value
	return nil
}

func TestTotalTransactionCount(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	// Initial read returns 0.
	if n := ReadTotalTransactionCount(db); n != 0 {
		t.Fatalf("initial count: want 0, got %d", n)
	}

	WriteTotalTransactionCount(db, 42)
	if n := ReadTotalTransactionCount(db); n != 42 {
		t.Fatalf("after write 42: want 42, got %d", n)
	}

	// Overwrite with a larger value.
	WriteTotalTransactionCount(db, 1_000_000)
	if n := ReadTotalTransactionCount(db); n != 1_000_000 {
		t.Fatalf("after write 1000000: want 1000000, got %d", n)
	}

	// Increment simulation.
	prev := ReadTotalTransactionCount(db)
	WriteTotalTransactionCount(db, prev+5)
	if n := ReadTotalTransactionCount(db); n != 1_000_005 {
		t.Fatalf("after +5: want 1000005, got %d", n)
	}
}

func TestWriteDynamicPropertyOwnedTransfersDerivedKeyAndValue(t *testing.T) {
	probe := new(dynamicPropertyOwnedWriterProbe)
	value := []byte("derived-value")
	WriteDynamicPropertyOwned(probe, "latest_block_header_number", value)
	if probe.putCalled {
		t.Fatal("dynamic property used defensive Put instead of owned string write")
	}
	if probe.key != dynPropLatestBlockNumberKeyString {
		t.Fatalf("dynamic property key = %q, want %q", probe.key, dynPropLatestBlockNumberKeyString)
	}
	if !bytes.Equal(probe.value, value) || &probe.value[0] != &value[0] {
		t.Fatal("dynamic property value was copied instead of transferred")
	}
}

var benchmarkDynamicPropertyKey []byte

func BenchmarkDerivedDynamicPropertyKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkDynamicPropertyKey = dynPropKey("latest_block_header_number")
	}
}

func TestWitnessCapsuleStateKeyInto(t *testing.T) {
	addr := common.BytesToAddress(bytes.Repeat([]byte{0x42}, common.AddressLength))
	var storage [WitnessCapsuleStateKeyLength]byte
	got := WitnessCapsuleStateKeyInto(&storage, addr)
	want := witnessKey(addr[:])
	if !bytes.Equal(got, want) {
		t.Fatalf("witness key = %x, want %x", got, want)
	}
	if &got[0] != &storage[0] {
		t.Fatal("witness key does not alias caller storage")
	}
}

var benchmarkWitnessCapsuleStateKey []byte

func BenchmarkWitnessCapsuleStateKey(b *testing.B) {
	addr := common.Address{0x41, 1}
	b.ReportAllocs()
	for range b.N {
		benchmarkWitnessCapsuleStateKey = WitnessCapsuleStateKey(addr)
	}
}

func BenchmarkWitnessCapsuleStateKeyInto(b *testing.B) {
	addr := common.Address{0x41, 1}
	var storage [WitnessCapsuleStateKeyLength]byte
	b.ReportAllocs()
	for range b.N {
		benchmarkWitnessCapsuleStateKey = WitnessCapsuleStateKeyInto(&storage, addr)
	}
}
