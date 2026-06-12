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

func TestSyncServiceDropsGappedStagedBodyTailOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block2 := stubBlock(2, bc.CurrentBlock().Hash())
	block4 := stubBlock(4, block2.Hash())
	for _, block := range []*types.Block{block2, block4} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write sync staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodies, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	ss.mu.Unlock()
	if buffered != 1 || target != 2 {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 1/2", buffered, target)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || !ok {
		t.Fatalf("contiguous staged block after gap cleanup ok=%v err=%v, want present", ok, err)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block4.Number()); err != nil || ok {
		t.Fatalf("stale staged tail after gap cleanup ok=%v err=%v, want deleted", ok, err)
	}
	row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies)
	if err != nil || !ok {
		t.Fatalf("sync bodies progress after gap cleanup ok=%v err=%v, want rewound", ok, err)
	}
	if row.BlockNum != block2.Number() || !row.HasBlockHash || row.BlockHash != block2.Hash() {
		t.Fatalf("sync bodies progress after gap cleanup = num %d hash %x hasHash=%v, want block2 %x",
			row.BlockNum, row.BlockHash, row.HasBlockHash, block2.Hash())
	}
}

func TestSyncServiceDropsMissingFirstStagedBodyProgressOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block2 := stubBlock(2, bc.CurrentBlock().Hash())
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodies, block2.Number(), block2.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	ss.mu.Unlock()
	if buffered != 0 || target != 1 {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 0/1", buffered, target)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies); err != nil || ok {
		t.Fatalf("sync bodies progress after missing first body = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceTracksContiguousStagedBodiesReady(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block2 := stubBlock(2, bc.CurrentBlock().Hash())
	block3 := stubBlock(3, block2.Hash())
	ss := NewSyncService(bc, nil)

	ss.stageSyncBody(block3, nil)
	row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies)
	if err != nil || !ok || row.BlockNum != block3.Number() || !row.HasBlockHash || row.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodies after out-of-order block3 = %+v ok=%v err=%v, want block3", row, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after gapped block3 = %+v ok=%v err=%v, want absent", row, ok, err)
	}

	ss.stageSyncBody(block2, nil)
	row, ok, err = rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies)
	if err != nil || !ok || row.BlockNum != block3.Number() || row.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodies after later block2 = %+v ok=%v err=%v, want monotonic block3", row, ok, err)
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block3.Number() || !ready.HasBlockHash || ready.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodiesReady after block2 fills gap = %+v ok=%v err=%v, want contiguous block3", ready, ok, err)
	}
}

func TestSyncServiceKeepsCanonicalSyncImportProgressOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 2)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	for _, stage := range syncPipelineProgressStages() {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	assertSyncPipelineProgress(t, bc.DB(), block1)
}

func TestSyncServiceDeletesStaleSyncPipelineProgressOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 2)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	for _, stage := range syncPipelineProgressStages() {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), tcommon.Hash{0xee}); err != nil {
			t.Fatalf("write mismatched %s progress: %v", stage, err)
		}
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	for _, stage := range syncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("stale %s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
	for _, stage := range syncPipelineProgressStages() {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, 5, tcommon.Hash{0x05}); err != nil {
			t.Fatalf("write ahead %s progress: %v", stage, err)
		}
	}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	for _, stage := range syncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("ahead %s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestSyncServiceResetDeletesStagedBodies(t *testing.T) {
	bc := makeTestChain(t)
	block := stubBlock(1, bc.CurrentBlock().Hash())
	if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
		t.Fatalf("write sync staged block: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodies, block.Number(), block.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, block.Number(), block.Hash()); err != nil {
		t.Fatalf("write sync bodies ready progress: %v", err)
	}
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.doReset()
	ss.mu.Unlock()

	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
		t.Fatalf("staged block after reset ok=%v err=%v, want deleted", ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies); err != nil || ok {
		t.Fatalf("sync bodies after reset = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("sync bodies ready after reset = %+v ok=%v err=%v, want deleted", row, ok, err)
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
