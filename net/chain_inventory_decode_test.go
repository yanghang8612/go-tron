package net

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var (
	chainInventoryDecodeBenchmarkIDs    []decodedChainInventoryID
	chainInventoryDecodeBenchmarkRemain int64
	chainInventoryDecodeBenchmarkHash   tcommon.Hash
)

func chainInventoryFixture(t testing.TB, count int) []byte {
	t.Helper()
	ids := make([]*corepb.ChainInventory_BlockId, count)
	for index := range ids {
		hash := tcommon.Hash{0xa1, byte(index), byte(index >> 8), byte(index >> 16)}
		ids[index] = &corepb.ChainInventory_BlockId{Hash: hash[:], Number: int64(index + 1)}
	}
	payload, err := proto.Marshal(&corepb.ChainInventory{Ids: ids, RemainNum: 1234})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func generatedChainInventoryValues(payload []byte) ([]decodedChainInventoryID, int64, error) {
	var inventory corepb.ChainInventory
	if err := proto.Unmarshal(payload, &inventory); err != nil {
		return nil, 0, err
	}
	ids := make([]decodedChainInventoryID, len(inventory.Ids))
	for index, id := range inventory.Ids {
		ids[index] = decodedChainInventoryID{hash: tcommon.BytesToHash(id.Hash), number: id.Number}
	}
	return ids, inventory.RemainNum, nil
}

func assertChainInventoryDecodeMatchesGenerated(t *testing.T, payload []byte) {
	t.Helper()
	wantIDs, wantRemain, wantErr := generatedChainInventoryValues(payload)
	gotIDs, gotRemain, gotOK := decodeChainInventory(payload)
	if gotOK != (wantErr == nil) {
		t.Fatalf("decode status: optimized=%v generated=%v payload=%x", gotOK, wantErr, payload)
	}
	if !gotOK {
		return
	}
	if gotRemain != wantRemain || len(gotIDs) != len(wantIDs) {
		t.Fatalf("inventory shape: optimized=(%d,%d) generated=(%d,%d)", len(gotIDs), gotRemain, len(wantIDs), wantRemain)
	}
	for index := range gotIDs {
		if gotIDs[index] != wantIDs[index] {
			t.Fatalf("id %d: optimized=%+v generated=%+v", index, gotIDs[index], wantIDs[index])
		}
	}
}

func TestDecodeChainInventoryMatchesGenerated(t *testing.T) {
	payload := chainInventoryFixture(t, 2000)
	payload = protowire.AppendTag(payload, 99, protowire.BytesType)
	payload = protowire.AppendBytes(payload, []byte("future"))
	assertChainInventoryDecodeMatchesGenerated(t, payload)
}

func TestDecodeChainInventoryDuplicateAndGroupFields(t *testing.T) {
	child := protowire.AppendTag(nil, 1, protowire.BytesType)
	child = protowire.AppendBytes(child, []byte{1, 2, 3})
	child = protowire.AppendTag(child, 1, protowire.BytesType)
	lastHash := tcommon.Hash{0xaa, 0xbb}
	child = protowire.AppendBytes(child, lastHash[:])
	child = protowire.AppendTag(child, 2, protowire.VarintType)
	child = protowire.AppendVarint(child, 7)
	child = protowire.AppendTag(child, 2, protowire.VarintType)
	child = protowire.AppendVarint(child, 9)
	payload := protowire.AppendTag(nil, 1, protowire.BytesType)
	payload = protowire.AppendBytes(payload, child)
	payload = protowire.AppendTag(payload, 2, protowire.VarintType)
	payload = protowire.AppendVarint(payload, 3)
	payload = protowire.AppendTag(payload, 2, protowire.VarintType)
	payload = protowire.AppendVarint(payload, 4)
	assertChainInventoryDecodeMatchesGenerated(t, payload)

	withGroup := append([]byte(nil), payload...)
	withGroup = protowire.AppendTag(withGroup, 100, protowire.StartGroupType)
	withGroup = protowire.AppendTag(withGroup, 1, protowire.VarintType)
	withGroup = protowire.AppendVarint(withGroup, 1)
	withGroup = protowire.AppendTag(withGroup, 100, protowire.EndGroupType)
	assertChainInventoryDecodeMatchesGenerated(t, withGroup)
}

func TestDecodeChainInventoryRejectsMalformed(t *testing.T) {
	assertChainInventoryDecodeMatchesGenerated(t, []byte{0x80})
}

func FuzzDecodeChainInventoryMatchesGenerated(f *testing.F) {
	f.Add(chainInventoryFixture(f, 3))
	f.Add([]byte(nil))
	f.Add([]byte{0x80})
	f.Fuzz(func(t *testing.T, payload []byte) {
		assertChainInventoryDecodeMatchesGenerated(t, payload)
	})
}

func BenchmarkDecodeChainInventory(b *testing.B) {
	payload := chainInventoryFixture(b, 2000)
	b.Run("Generated", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var inventory corepb.ChainInventory
			if err := proto.Unmarshal(payload, &inventory); err != nil {
				b.Fatal(err)
			}
			for _, id := range inventory.Ids {
				chainInventoryDecodeBenchmarkHash = tcommon.BytesToHash(id.Hash)
			}
			chainInventoryDecodeBenchmarkRemain = inventory.RemainNum
		}
	})
	b.Run("Contiguous", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var ok bool
			chainInventoryDecodeBenchmarkIDs, chainInventoryDecodeBenchmarkRemain, ok = decodeChainInventory(payload)
			if !ok {
				b.Fatal("decode failed")
			}
			for _, id := range chainInventoryDecodeBenchmarkIDs {
				chainInventoryDecodeBenchmarkHash = id.hash
			}
		}
	})
}
