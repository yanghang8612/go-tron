package rawdb

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func benchmarkBlockWithPayload(number uint64) *types.Block {
	pb := newBlockProto(number, int64(number*3000))
	pb.Transactions = make([]*corepb.Transaction, 100)
	for i := range pb.Transactions {
		pb.Transactions[i] = &corepb.Transaction{
			RawData: &corepb.TransactionRaw{Data: bytes.Repeat([]byte{byte(i)}, 1024)},
		}
	}
	return types.NewBlockFromPB(pb)
}

func BenchmarkReadBlockHashByNumberLegacy(b *testing.B) {
	db := NewMemoryChainDB()
	block := benchmarkBlockWithPayload(42)
	if err := WriteBlock(db, block); err != nil {
		b.Fatal(err)
	}
	want := block.Hash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := ReadBlockKV(db, block.Number())
		if got == nil || got.Hash() != want {
			b.Fatal("block hash mismatch")
		}
	}
}

func BenchmarkReadBlockHashByNumberLegacyRawFallback(b *testing.B) {
	db := NewMemoryChainDB()
	block := benchmarkBlockWithPayload(42)
	if err := WriteBlock(db, block); err != nil {
		b.Fatal(err)
	}
	if err := db.Delete(blockNumberHashKey(block.Number())); err != nil {
		b.Fatal(err)
	}
	want := block.Hash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, ok := ReadBlockHashKV(db, block.Number())
		if !ok || got != want {
			b.Fatal("block hash mismatch")
		}
	}
}

func BenchmarkReadBlockHashByNumberIndex(b *testing.B) {
	db := NewMemoryChainDB()
	block := benchmarkBlockWithPayload(42)
	if err := WriteBlock(db, block); err != nil {
		b.Fatal(err)
	}
	want := block.Hash()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, ok := ReadBlockHashKV(db, block.Number())
		if !ok || got != want {
			b.Fatal("block hash mismatch")
		}
	}
}
