package rawdb

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSyncStagedBlockReadWriteDelete(t *testing.T) {
	db := NewMemoryDatabase()
	block := testSyncStagedBlock(3, common.Hash{0x02})

	if _, ok, err := ReadSyncStagedBlock(db, 3); err != nil || ok {
		t.Fatalf("empty staged block ok=%v err=%v", ok, err)
	}
	if err := WriteSyncStagedBlock(db, block); err != nil {
		t.Fatalf("write sync staged block: %v", err)
	}
	got, ok, err := ReadSyncStagedBlock(db, 3)
	if err != nil || !ok || got.Hash() != block.Hash() {
		t.Fatalf("read sync staged block = %v ok=%v err=%v, want %x", got, ok, err, block.Hash())
	}
	if err := DeleteSyncStagedBlock(db, 3); err != nil {
		t.Fatalf("delete sync staged block: %v", err)
	}
	if _, ok, err := ReadSyncStagedBlock(db, 3); err != nil || ok {
		t.Fatalf("deleted staged block ok=%v err=%v", ok, err)
	}
}

func TestDeleteSyncStagedBlocksThrough(t *testing.T) {
	db := NewMemoryDatabase()
	for n := uint64(1); n <= 4; n++ {
		if err := WriteSyncStagedBlock(db, testSyncStagedBlock(n, common.Hash{byte(n - 1)})); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
	}
	deleted, err := DeleteSyncStagedBlocksThrough(db, 2)
	if err != nil {
		t.Fatalf("delete staged blocks through: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted staged blocks = %d, want 2", deleted)
	}
	for n := uint64(1); n <= 4; n++ {
		_, ok, err := ReadSyncStagedBlock(db, n)
		if err != nil {
			t.Fatalf("read staged block %d: %v", n, err)
		}
		if n <= 2 && ok {
			t.Fatalf("staged block %d survived cleanup", n)
		}
		if n > 2 && !ok {
			t.Fatalf("staged block %d was deleted unexpectedly", n)
		}
	}
}

func TestDeleteSyncStagedBlocksFrom(t *testing.T) {
	db := NewMemoryDatabase()
	for n := uint64(1); n <= 4; n++ {
		if err := WriteSyncStagedBlock(db, testSyncStagedBlock(n, common.Hash{byte(n - 1)})); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
	}
	deleted, err := DeleteSyncStagedBlocksFrom(db, 3)
	if err != nil {
		t.Fatalf("delete staged blocks from: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted staged blocks = %d, want 2", deleted)
	}
	for n := uint64(1); n <= 4; n++ {
		_, ok, err := ReadSyncStagedBlock(db, n)
		if err != nil {
			t.Fatalf("read staged block %d: %v", n, err)
		}
		if n < 3 && !ok {
			t.Fatalf("staged block %d was deleted unexpectedly", n)
		}
		if n >= 3 && ok {
			t.Fatalf("staged block %d survived cleanup", n)
		}
	}
}

func TestDeleteAllSyncStagedBlocks(t *testing.T) {
	db := NewMemoryDatabase()
	for n := uint64(1); n <= 3; n++ {
		if err := WriteSyncStagedBlock(db, testSyncStagedBlock(n, common.Hash{byte(n - 1)})); err != nil {
			t.Fatalf("write staged block %d: %v", n, err)
		}
	}
	deleted, err := DeleteAllSyncStagedBlocks(db)
	if err != nil {
		t.Fatalf("delete all staged blocks: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("deleted staged blocks = %d, want 3", deleted)
	}
	for n := uint64(1); n <= 3; n++ {
		if _, ok, err := ReadSyncStagedBlock(db, n); err != nil || ok {
			t.Fatalf("staged block %d after delete all ok=%v err=%v", n, ok, err)
		}
	}
}

func testSyncStagedBlock(number uint64, parent common.Hash) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     int64(number),
				Timestamp:  int64(number) * 3000,
				ParentHash: parent.Bytes(),
			},
			WitnessSignature: make([]byte, 65),
		},
	})
}
