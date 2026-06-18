package rawdb

import (
	"bytes"
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

func TestHeadBlock(t *testing.T) {
	db := NewMemoryDatabase()
	WriteHeadBlockHash(db, common.HexToHash("aabb"))
	h := ReadHeadBlockHash(db)
	if h != common.HexToHash("aabb") {
		t.Fatal("head block hash mismatch")
	}
}

func TestHashBoundaryRowsRejectMalformedValues(t *testing.T) {
	db := NewMemoryDatabase()
	if err := db.Put(headBlockKey, []byte{0xaa}); err != nil {
		t.Fatalf("put malformed head block hash: %v", err)
	}
	if got := ReadHeadBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("ReadHeadBlockHash malformed row = %x, want zero", got)
	}
	if err := db.Put(headSolidBlockKey, bytes.Repeat([]byte{0xbb}, common.HashLength-1)); err != nil {
		t.Fatalf("put malformed solid head block hash: %v", err)
	}
	if got := ReadHeadSolidBlockHash(db); got != (common.Hash{}) {
		t.Fatalf("ReadHeadSolidBlockHash malformed row = %x, want zero", got)
	}
	if err := db.Put(genesisStateRootKey, bytes.Repeat([]byte{0xcc}, common.HashLength+1)); err != nil {
		t.Fatalf("put malformed genesis state root: %v", err)
	}
	if got := ReadGenesisStateRoot(db); got != (common.Hash{}) {
		t.Fatalf("ReadGenesisStateRoot malformed row = %x, want zero", got)
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
