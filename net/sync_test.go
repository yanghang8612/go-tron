package net

import (
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestBuildChainSummary(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	summary := ss.BuildChainSummary()
	// With only genesis, summary should have 1 block ID
	if len(summary) != 1 {
		t.Fatalf("expected 1 entry in chain summary, got %d", len(summary))
	}
	if summary[0].Number() != 0 {
		t.Fatalf("expected genesis in summary, got block #%d", summary[0].Number())
	}
}

func TestSyncServiceStopConsumesInboundBlocks(t *testing.T) {
	ss := NewSyncService(makeTestChain(t), nil)
	ss.Stop()
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1},
		},
	})
	if !ss.HandleBlock(nil, block, nil) {
		t.Fatal("stopped sync service should consume inbound blocks during shutdown")
	}
}

func TestSyncServiceRestoresInventoryTargetProgress(t *testing.T) {
	bc := makeTestChain(t)
	if err := rawdb.WriteStageProgress(bc.DB(), rawdb.StageSyncInventory, 500); err != nil {
		t.Fatalf("write sync inventory progress: %v", err)
	}
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	got := ss.targetHeadNum
	ss.mu.Unlock()

	if got != 500 {
		t.Fatalf("restored targetHeadNum = %d, want persisted inventory target 500", got)
	}
}

func TestSyncServiceIgnoresStaleInventoryTargetProgress(t *testing.T) {
	bc := makeChainWithBlocks(t, 10)
	if err := rawdb.WriteStageProgress(bc.DB(), rawdb.StageSyncInventory, 7); err != nil {
		t.Fatalf("write stale sync inventory progress: %v", err)
	}
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	got := ss.targetHeadNum
	ss.mu.Unlock()

	if got != 10 {
		t.Fatalf("targetHeadNum with stale persisted inventory = %d, want current head 10", got)
	}
}

func TestSyncServiceRestoresStagedBodiesOnSessionStart(t *testing.T) {
	bc := makeTestChain(t)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write sync staged block %d: %v", block.Number(), err)
		}
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	ss.mu.Unlock()
	if buffered != 2 || target != 2 {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 2/2", buffered, target)
	}

	ss.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after staged body restore = %v, want block2 %x", got, block2.Hash())
	}
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after import ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
}

func TestSyncServiceResetDeletesStagedBodies(t *testing.T) {
	bc := makeTestChain(t)
	block := stubBlock(1, bc.CurrentBlock().Hash())
	if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
		t.Fatalf("write sync staged block: %v", err)
	}
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.doReset()
	ss.mu.Unlock()

	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
		t.Fatalf("staged block after reset ok=%v err=%v, want deleted", ok, err)
	}
}

func TestBuildChainSummaryMultipleBlocks(t *testing.T) {
	bc := makeTestChain(t)

	// Insert 10 blocks
	for i := uint64(1); i <= 10; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlockWithoutVerify(block); err != nil {
			t.Fatal(err)
		}
	}

	ss := NewSyncService(bc, nil)
	summary := ss.BuildChainSummary()

	// Ascending order — java-tron's SyncBlockChainMsgHandler.check enforces
	// summary[last].num >= peer.lastSyncBlockId.num, so the head must be
	// last and genesis must be first.
	if summary[0].Number() != 0 {
		t.Fatalf("first summary entry should be genesis (#0), got #%d", summary[0].Number())
	}
	last := summary[len(summary)-1]
	if last.Number() != 10 {
		t.Fatalf("last summary entry should be head (#10), got #%d", last.Number())
	}
}

func TestFindCommonBlock(t *testing.T) {
	bc := makeTestChain(t)

	// Insert 5 blocks
	for i := uint64(1); i <= 5; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		bc.InsertBlockWithoutVerify(block)
	}

	ss := NewSyncService(bc, nil)

	// Build a summary from blocks we know
	block3 := bc.GetBlockByNumber(3)
	block0 := bc.GetBlockByNumber(0)

	peerSummary := []types.BlockID{block3.ID(), block0.ID()}
	commonNum := ss.FindCommonBlock(peerSummary)

	if commonNum != 3 {
		t.Fatalf("expected common block #3, got #%d", commonNum)
	}
}

func TestFindCommonBlockNoMatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	// Summary with unknown blocks
	fakeID := types.BlockID{Hash: tcommon.Hash{0xFF}, Num: 100}
	commonNum := ss.FindCommonBlock([]types.BlockID{fakeID})

	if commonNum != 0 {
		t.Fatalf("expected common block #0 (genesis fallback), got #%d", commonNum)
	}
}
