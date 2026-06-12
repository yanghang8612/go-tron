package rawdb

import (
	"bytes"
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

func TestSyncStagedBlockRawIterate(t *testing.T) {
	db := NewMemoryDatabase()
	for _, n := range []uint64{4, 2, 3} {
		block := testSyncStagedBlock(n, common.Hash{byte(n - 1)})
		raw, err := block.Marshal()
		if err != nil {
			t.Fatalf("marshal block %d: %v", n, err)
		}
		if err := WriteSyncStagedBlockRaw(db, block, raw); err != nil {
			t.Fatalf("write raw staged block %d: %v", n, err)
		}
	}
	row, ok, err := ReadSyncStagedBlockRaw(db, 2)
	if err != nil || !ok || row.Number != 2 || row.Hash != testSyncStagedBlock(2, common.Hash{0x01}).Hash() {
		t.Fatalf("ReadSyncStagedBlockRaw = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	if len(row.Raw) == 0 {
		t.Fatal("ReadSyncStagedBlockRaw returned empty raw bytes")
	}
	row.Raw[0] ^= 0xff
	row2, ok, err := ReadSyncStagedBlockRaw(db, 2)
	if err != nil || !ok {
		t.Fatalf("second ReadSyncStagedBlockRaw ok=%v err=%v", ok, err)
	}
	if bytes.Equal(row.Raw, row2.Raw) {
		t.Fatal("ReadSyncStagedBlockRaw returned aliased bytes")
	}

	var got []uint64
	err = IterateSyncStagedBlocksFrom(db, 3, func(row SyncStagedBlockRow) (bool, error) {
		got = append(got, row.Number)
		if row.Hash == (common.Hash{}) || len(row.Raw) == 0 {
			t.Fatalf("iter row missing hash/raw: %+v", row)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("IterateSyncStagedBlocksFrom: %v", err)
	}
	if want := []uint64{3, 4}; !equalUint64s(got, want) {
		t.Fatalf("iterated staged blocks = %v, want %v", got, want)
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

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
