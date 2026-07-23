package rawdb

import (
	"encoding/binary"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWriteReadBlock(t *testing.T) {
	chaindb := NewMemoryChainDB()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    42,
				Timestamp: 126000,
			},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(chaindb, block)

	got := ReadBlock(chaindb, block.Number())
	if got == nil {
		t.Fatal("block not found")
	}
	if got.Number() != 42 {
		t.Fatalf("expected 42, got %d", got.Number())
	}
	gotHash, ok := ReadBlockHash(chaindb, block.Number())
	if !ok || gotHash != block.Hash() {
		t.Fatalf("number->hash = %x,%v want %x,true", gotHash, ok, block.Hash())
	}
}

func TestReadBlockHashKVCompactIndexWithoutBody(t *testing.T) {
	db := NewMemoryDatabase()
	var want common.Hash
	binary.BigEndian.PutUint64(want[:8], 42)
	copy(want[8:], []byte("compact-index"))
	if err := db.Put(blockNumberHashKey(42), want[:]); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadBlockHashKV(db, 42)
	if !ok || got != want {
		t.Fatalf("ReadBlockHashKV = %x,%v want %x,true", got, ok, want)
	}
}

func TestReadBlockHashKVRejectsOverwrittenRingSlot(t *testing.T) {
	db := NewMemoryDatabase()
	newer := uint64(42) + blockNumberHashSlots
	var want common.Hash
	binary.BigEndian.PutUint64(want[:8], newer)
	copy(want[8:], []byte("newer-ring-value"))
	if err := db.Put(blockNumberHashKey(newer), want[:]); err != nil {
		t.Fatal(err)
	}
	if got, ok := ReadBlockHashKV(db, 42); ok || got != (common.Hash{}) {
		t.Fatalf("overwritten slot answered old number: %x,%v", got, ok)
	}
	if got, ok := ReadBlockHashKV(db, newer); !ok || got != want {
		t.Fatalf("new slot value = %x,%v want %x,true", got, ok, want)
	}
}

func TestReadBlockHashKVLegacyBodyFallback(t *testing.T) {
	db := NewMemoryDatabase()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 42, Timestamp: 126000}},
	})
	data, err := block.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a database created before the compact number→hash index.
	if err := db.Put(blockKey(block.Number()), data); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadBlockHashKV(db, block.Number())
	if !ok || got != block.Hash() {
		t.Fatalf("legacy ReadBlockHashKV = %x,%v want %x,true", got, ok, block.Hash())
	}
}

func TestWriteReadBlockByHash(t *testing.T) {
	chaindb := NewMemoryChainDB()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 10},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(chaindb, block)

	num := ReadBlockNumber(chaindb, block.Hash())
	if num == nil {
		t.Fatal("hash->number mapping not found")
	}
	if *num != 10 {
		t.Fatalf("expected 10, got %d", *num)
	}
}

func TestWriteBlockOverwritesRecentHashForCanonicalNumber(t *testing.T) {
	db := NewMemoryChainDB()
	first := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 10, Timestamp: 30_000}},
	})
	second := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 10, Timestamp: 30_001}},
	})
	if first.Hash() == second.Hash() {
		t.Fatal("test blocks unexpectedly have the same hash")
	}
	if err := WriteBlock(db, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteBlock(db, second); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadBlockHashKV(db, 10)
	if !ok || got != second.Hash() {
		t.Fatalf("canonical replacement hash = %x,%v want %x,true", got, ok, second.Hash())
	}
}

func TestHeadBlock(t *testing.T) {
	db := NewMemoryDatabase()
	WriteHeadBlockHash(db, common.HexToHash("aabb"))
	h := ReadHeadBlockHash(db)
	if h != common.HexToHash("aabb") {
		t.Fatal("head block hash mismatch")
	}
}

func TestWriteReadAccount(t *testing.T) {
	db := NewMemoryDatabase()
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(1000000)

	WriteAccount(db, addr, acc)
	got := ReadAccount(db, addr)
	if got == nil {
		t.Fatal("account not found")
	}
	if got.Balance() != 1000000 {
		t.Fatalf("expected 1000000, got %d", got.Balance())
	}
}
