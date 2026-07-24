package types

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var benchmarkBlockHash common.Hash
var benchmarkBlockBytes []byte

func blockHashRawTestBlock(txCount, dataSize int) *Block {
	txs := make([]*corepb.Transaction, txCount)
	for i := range txs {
		txs[i] = &corepb.Transaction{
			RawData: &corepb.TransactionRaw{
				RefBlockBytes: []byte{byte(i >> 8), byte(i)},
				Data:          bytes.Repeat([]byte{byte(i)}, dataSize),
				Timestamp:     int64(i + 1),
			},
			Signature: [][]byte{bytes.Repeat([]byte{byte(i + 1)}, 65)},
		}
	}
	raw := &corepb.BlockHeaderRaw{
		Number:           12_345_678,
		Timestamp:        1_700_000_000_000,
		TxTrieRoot:       bytes.Repeat([]byte{0x11}, common.HashLength),
		ParentHash:       bytes.Repeat([]byte{0x22}, common.HashLength),
		WitnessAddress:   append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x33}, common.AccountIDLength)...),
		Version:          34,
		AccountStateRoot: bytes.Repeat([]byte{0x44}, common.HashLength),
	}
	rawUnknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	rawUnknown = protowire.AppendBytes(rawUnknown, []byte("raw-unknown"))
	raw.ProtoReflect().SetUnknown(rawUnknown)
	return NewBlockFromPB(&corepb.Block{
		Transactions: txs,
		BlockHeader: &corepb.BlockHeader{
			RawData:          raw,
			WitnessSignature: bytes.Repeat([]byte{0x55}, 65),
		},
	})
}

func BenchmarkBlockHashFromRaw(b *testing.B) {
	block := blockHashRawTestBlock(200, 256)
	data, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("full-unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			decoded, err := UnmarshalBlock(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkBlockHash = decoded.Hash()
		}
	})
	b.Run("raw-header-scan", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkBlockHash, err = BlockHashFromRaw(data)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBlockMarshalReusable(b *testing.B) {
	block := blockHashRawTestBlock(200, 256)
	raw, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			benchmarkBlockBytes, err = block.Marshal()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("owned-scratch", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			block.AdoptMarshalScratch(raw)
			benchmarkBlockBytes, err = block.MarshalReusable()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestNewBlock(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    100,
				Timestamp: 1000000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.Number() != 100 {
		t.Fatalf("expected number 100, got %d", b.Number())
	}
	if b.Timestamp() != 1000000 {
		t.Fatalf("expected timestamp 1000000, got %d", b.Timestamp())
	}
}

func TestBlockHash(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	h := b.Hash()
	if h.IsEmpty() {
		t.Fatal("hash should not be empty")
	}
	h2 := b.Hash()
	if h != h2 {
		t.Fatal("hash not deterministic")
	}
}

func TestBlockHashFromRawMatchesBlockHash(t *testing.T) {
	block := blockHashRawTestBlock(8, 64)
	data, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := BlockHashFromRaw(data)
	if err != nil {
		t.Fatalf("BlockHashFromRaw: %v", err)
	}
	if want := block.Hash(); got != want {
		t.Fatalf("raw block hash = %x, want %x", got, want)
	}
}

func TestBlockHashFromRawRejectsMalformedOrMissingHeader(t *testing.T) {
	wrongWire := protowire.AppendTag(nil, 2, protowire.VarintType)
	wrongWire = protowire.AppendVarint(wrongWire, 1)
	missingRaw, err := proto.Marshal(&corepb.Block{BlockHeader: &corepb.BlockHeader{}})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"empty":       nil,
		"truncated":   {0x12, 0x80},
		"wrong-wire":  wrongWire,
		"missing-raw": missingRaw,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BlockHashFromRaw(data); err == nil {
				t.Fatal("expected malformed/missing header error")
			}
		})
	}
}

func TestBlockSerialize(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    42,
				Timestamp: 9000,
			},
		},
	}
	b := NewBlockFromPB(pb)
	data, err := b.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := UnmarshalBlock(data)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Number() != 42 {
		t.Fatalf("expected 42, got %d", b2.Number())
	}
}

func TestBlockID(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number: 5,
			},
		},
	}
	b := NewBlockFromPB(pb)
	id := b.ID()
	num := id.Number()
	if num != 5 {
		t.Fatalf("expected block number 5 from ID, got %d", num)
	}
}

func TestBlockParentHash(t *testing.T) {
	parent := common.HexToHash("aabbccdd")
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				ParentHash: parent.Bytes(),
			},
		},
	}
	b := NewBlockFromPB(pb)
	if b.ParentHash() != parent {
		t.Fatal("parent hash mismatch")
	}
}

func TestBlockProtoRoundTrip(t *testing.T) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         999,
				Timestamp:      123456789,
				WitnessAddress: []byte{0x41, 0x01, 0x02},
				Version:        34,
			},
		},
	}
	b := NewBlockFromPB(pb)
	pb2 := b.Proto()
	if !proto.Equal(pb, pb2) {
		t.Fatal("proto round trip not equal")
	}
}

func TestBlock_SetWitnessSignature(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	sig := make([]byte, 65)
	sig[0] = 0xAA
	block.SetWitnessSignature(sig)

	if got := block.WitnessSignature(); len(got) != 65 || got[0] != 0xAA {
		t.Fatalf("unexpected signature: %x", got)
	}
}

func TestBlock_SetAccountStateRoot(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1},
		},
	})

	var root common.Hash
	root[0] = 0xBB
	block.SetAccountStateRoot(root)

	if block.AccountStateRoot() != root {
		t.Fatalf("expected root %x, got %x", root, block.AccountStateRoot())
	}
}

func TestBlock_ResetHash(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	hash1 := block.Hash()

	block.Proto().BlockHeader.RawData.Timestamp = 6000
	if block.Hash() != hash1 {
		t.Fatal("hash should be cached")
	}

	block.ResetHash()
	hash2 := block.Hash()
	if hash2 == hash1 {
		t.Fatal("hash should change after ResetHash + modified RawData")
	}
}

// TestBlock_TransactionsAreStable verifies Transactions() memoizes the wrapped
// slice and returns the SAME *Transaction instances every call. This identity
// is what lets the parallel pre-pass warm a tx's signers memo and have the
// serial execution path (which re-fetches via Transactions()) read the warm
// result.
func TestBlock_TransactionsAreStable(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000}},
		Transactions: []*corepb.Transaction{
			{RawData: &corepb.TransactionRaw{Timestamp: 1}},
			{RawData: &corepb.TransactionRaw{Timestamp: 2}},
		},
	})
	a := block.Transactions()
	b := block.Transactions()
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("len: a=%d b=%d, want 2", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("Transactions()[%d] not stable: %p vs %p", i, a[i], b[i])
		}
		if a[i].Proto() != block.Proto().Transactions[i] {
			t.Fatalf("Transactions()[%d] wraps the wrong protobuf", i)
		}
	}
	if a[0] == a[1] {
		t.Fatal("distinct protobuf transactions share one wrapper")
	}
	if a[0].Hash() == a[1].Hash() {
		t.Fatal("distinct transaction wrappers share a hash cache")
	}
}

// TestBlock_CachedRecoveredWitness verifies the witness-recovery memo: the
// supplied recover func runs exactly once, the cached (addr, err) is returned
// thereafter, and SetWitnessSignature / ResetHash invalidate it so a re-signed
// block re-derives.
func TestBlock_CachedRecoveredWitness(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000}},
	})
	var calls int
	want := common.Address{0x41, 0x07}
	rec := func(*Block) (common.Address, error) { calls++; return want, nil }

	if got, _ := block.CachedRecoveredWitness(rec); got != want {
		t.Fatalf("addr = %x, want %x", got, want)
	}
	if got, _ := block.CachedRecoveredWitness(rec); got != want {
		t.Fatalf("cached addr = %x, want %x", got, want)
	}
	if calls != 1 {
		t.Fatalf("recover called %d times, want 1 (memoized)", calls)
	}

	// SetWitnessSignature must invalidate the memo (re-sign re-derives).
	block.SetWitnessSignature(make([]byte, 65))
	if _, _ = block.CachedRecoveredWitness(rec); calls != 2 {
		t.Fatalf("recover called %d times after SetWitnessSignature, want 2", calls)
	}

	// ResetHash must invalidate too.
	block.ResetHash()
	if _, _ = block.CachedRecoveredWitness(rec); calls != 3 {
		t.Fatalf("recover called %d times after ResetHash, want 3", calls)
	}
}
