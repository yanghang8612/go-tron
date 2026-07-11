package net

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
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
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after restored staged import = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceRestoresHalfDownloadedSessionOnStart(t *testing.T) {
	bc := makeTestChain(t)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block, rawOf(t, block))
		if result.StageError != nil || result.ProgressWriteError != nil {
			t.Fatalf("stage block %d result = %+v", block.Number(), result)
		}
	}
	if err := rawdb.WriteStageProgress(bc.DB(), rawdb.StageSyncInventory, 5); err != nil {
		t.Fatalf("write sync inventory target: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	remain := ss.estimatedRemainLocked()
	path1 := ss.blockPath[block1.Number()]
	path2 := ss.blockPath[block2.Number()]
	ss.mu.Unlock()

	if buffered != 2 || target != 5 || remain != 5 {
		t.Fatalf("restored half download buffered=%d target=%d remain=%d, want 2/5/5", buffered, target, remain)
	}
	if path1 != block1.Hash() || path2 != block2.Hash() {
		t.Fatalf("restored block path = %x/%x, want block1/block2 %x/%x", path1, path2, block1.Hash(), block2.Hash())
	}
	if got := bc.CurrentBlock().Number(); got != 0 {
		t.Fatalf("head after half-download restore = %d, want genesis before local import", got)
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block2.Number() || !ready.HasBlockHash || ready.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodiesReady after half-download restore = %+v ok=%v err=%v, want block2", ready, ok, err)
	}
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("%s progress after half-download restore = %+v ok=%v err=%v, want absent before execution", stage, row, ok, err)
		}
	}

	ss.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after half-download drain = %v, want block2 %x", got, block2.Hash())
	}
	ss.mu.Lock()
	target = ss.targetHeadNum
	remain = ss.estimatedRemainLocked()
	syncing := ss.syncing
	buffered = len(ss.blockBuffer)
	ss.mu.Unlock()
	if !syncing || target != 5 || remain != 3 || buffered != 0 {
		t.Fatalf("post-drain session syncing=%v target=%d remain=%d buffered=%d, want true/5/3/0", syncing, target, remain, buffered)
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after half-download drain ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after half-download drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceStartupCleanupRefreshesReadyAfterImportedBodyDelete(t *testing.T) {
	bc := makeChainWithBlocks(t, 2)
	imported := bc.GetBlockByNumber(2)
	if imported == nil {
		t.Fatal("missing imported block2")
	}
	block3 := stubBlock(3, imported.Hash())
	block4 := stubBlock(4, block3.Hash())
	for _, block := range []*types.Block{imported, block3, block4} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write sync staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, imported.Number(), imported.Hash()); err != nil {
		t.Fatalf("write stale SyncBodiesReady: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	path3 := ss.blockPath[block3.Number()]
	path4 := ss.blockPath[block4.Number()]
	ss.mu.Unlock()

	if buffered != 2 || target != block4.Number() {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 2/%d", buffered, target, block4.Number())
	}
	if path3 != block3.Hash() || path4 != block4.Hash() {
		t.Fatalf("restored block path = %x/%x, want block3/block4 %x/%x", path3, path4, block3.Hash(), block4.Hash())
	}
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), imported.Number()); err != nil || ok {
		t.Fatalf("imported staged block2 after startup ok=%v err=%v, want deleted", ok, err)
	}
	for _, block := range []*types.Block{block3, block4} {
		if row, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || !ok || row.Hash() != block.Hash() {
			t.Fatalf("restored staged block %d = %v ok=%v err=%v, want present", block.Number(), row, ok, err)
		}
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block4.Number() || !ready.HasBlockHash || ready.BlockHash != block4.Hash() {
		t.Fatalf("SyncBodiesReady after imported cleanup = %+v ok=%v err=%v, want block4", ready, ok, err)
	}
}

func TestSyncServiceRepairsUnboundBodiesReadyOnSessionStart(t *testing.T) {
	bc := makeTestChain(t)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write sync staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgress(bc.DB(), rawdb.StageSyncBodiesReady, block2.Number()); err != nil {
		t.Fatalf("write unbound SyncBodiesReady: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	ss.mu.Unlock()

	if buffered != 2 || target != block2.Number() {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 2/%d", buffered, target, block2.Number())
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block2.Number() || !ready.HasBlockHash || ready.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodiesReady after unbound repair = %+v ok=%v err=%v, want hash-bound block2", ready, ok, err)
	}

	ss.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after repaired ready drain = %v, want block2 %x", got, block2.Hash())
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after repaired drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceRepairsBodiesReadyHashMismatchOnSessionStart(t *testing.T) {
	bc := makeTestChain(t)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write sync staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, block2.Number(), tcommon.Hash{0xff}); err != nil {
		t.Fatalf("write mismatched SyncBodiesReady: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	path1 := ss.blockPath[block1.Number()]
	path2 := ss.blockPath[block2.Number()]
	ss.mu.Unlock()

	if buffered != 2 || target != block2.Number() {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 2/%d", buffered, target, block2.Number())
	}
	if path1 != block1.Hash() || path2 != block2.Hash() {
		t.Fatalf("restored block path = %x/%x, want block1/block2 %x/%x", path1, path2, block1.Hash(), block2.Hash())
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block2.Number() || !ready.HasBlockHash || ready.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodiesReady after hash-mismatch repair = %+v ok=%v err=%v, want hash-bound block2", ready, ok, err)
	}

	ss.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after hash-mismatch ready restart drain = %v, want block2 %x", got, block2.Hash())
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after hash-mismatch ready drain = %+v ok=%v err=%v, want deleted", row, ok, err)
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

func TestSyncServicePrunesGappedStagedTailBeyondImportChunkOnSessionStart(t *testing.T) {
	bc := makeTestChain(t)
	prev := bc.CurrentBlock().Hash()
	var lastContiguous *types.Block
	for n := 1; n <= maxSyncImportBatch; n++ {
		block := stubBlock(int64(n), prev)
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write sync staged block %d: %v", n, err)
		}
		prev = block.Hash()
		lastContiguous = block
	}
	gapped := stubBlock(int64(maxSyncImportBatch+2), prev)
	if err := rawdb.WriteSyncStagedBlock(bc.DB(), gapped); err != nil {
		t.Fatalf("write gapped sync staged block: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodies, gapped.Number(), gapped.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	ss.mu.Unlock()

	if buffered != maxSyncImportBatch {
		t.Fatalf("restored staged bodies buffered=%d, want import chunk %d", buffered, maxSyncImportBatch)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), gapped.Number()); err != nil || ok {
		t.Fatalf("gapped staged tail after startup cleanup ok=%v err=%v, want deleted", ok, err)
	}
	row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies)
	if err != nil || !ok {
		t.Fatalf("sync bodies progress after distant gap cleanup ok=%v err=%v, want rewound", ok, err)
	}
	if row.BlockNum != lastContiguous.Number() || !row.HasBlockHash || row.BlockHash != lastContiguous.Hash() {
		t.Fatalf("sync bodies progress after distant gap cleanup = num %d hash %x hasHash=%v, want block %d %x",
			row.BlockNum, row.BlockHash, row.HasBlockHash, lastContiguous.Number(), lastContiguous.Hash())
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

func TestSyncServiceRefreshesReadyWhenLaterGapFills(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block2 := stubBlock(2, bc.CurrentBlock().Hash())
	block3 := stubBlock(3, block2.Hash())
	block4 := stubBlock(4, block3.Hash())
	block5 := stubBlock(5, block4.Hash())
	for _, block := range []*types.Block{block2, block3} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, block3.Number(), block3.Hash()); err != nil {
		t.Fatalf("write ready progress: %v", err)
	}
	ss := NewSyncService(bc, nil)

	ss.stageSyncBody(block5, nil)
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block3.Number() || ready.BlockHash != block3.Hash() {
		t.Fatalf("SyncBodiesReady after gap block5 = %+v ok=%v err=%v, want block3", ready, ok, err)
	}

	ss.stageSyncBody(block4, nil)
	ready, ok, err = rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block5.Number() || ready.BlockHash != block5.Hash() {
		t.Fatalf("SyncBodiesReady after block4 fills gap = %+v ok=%v err=%v, want block5", ready, ok, err)
	}
}

func TestSyncServiceKeepsCanonicalSyncImportProgressOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 2)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	for _, stage := range syncdl.SyncPipelineProgressStages() {
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
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), tcommon.Hash{0xee}); err != nil {
			t.Fatalf("write mismatched %s progress: %v", stage, err)
		}
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("stale %s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, 5, tcommon.Hash{0x05}); err != nil {
			t.Fatalf("write ahead %s progress: %v", stage, err)
		}
	}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("ahead %s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestSyncServiceDeletesMismatchedSyncPipelineOrderOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 2)
	block1 := bc.GetBlockByNumber(1)
	block2 := bc.GetBlockByNumber(2)
	if block1 == nil || block2 == nil {
		t.Fatal("missing test blocks")
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncImport, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write import progress: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block2.Number(), block2.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("sync import progress after startup = %+v ok=%v err=%v, want block1 kept", row, ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestSyncServiceDeletesOrphanedDownstreamSyncPipelineOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncFinish, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write orphaned finish progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestSyncServiceDeletesExecutionWithoutImportOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncExecution, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write orphaned execution progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestSyncServiceCompletesCurrentHeadAfterExecutionForkHashMismatchOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncImport, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write import progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncExecution, block1.Number(), tcommon.Hash{0xee}); err != nil {
		t.Fatalf("write forked execution progress: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want block1 completed", stage, row, ok, err)
		}
	}
}

func TestSyncServiceCompletesCurrentHeadAfterCommitmentForkHashMismatchOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncCommitment, block1.Number(), tcommon.Hash{0xee}); err != nil {
		t.Fatalf("write forked commitment progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncFinish, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write finish progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want block1 completed", stage, row, ok, err)
		}
	}
}

func TestSyncServiceCompletesCurrentHeadAfterFinishForkHashMismatchOnSessionStart(t *testing.T) {
	bc := makeChainWithBlocks(t, 1)
	block1 := bc.GetBlockByNumber(1)
	if block1 == nil {
		t.Fatal("missing block1")
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution, rawdb.StageSyncCommitment} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncFinish, block1.Number(), tcommon.Hash{0xee}); err != nil {
		t.Fatalf("write forked finish progress: %v", err)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want block1 completed", stage, row, ok, err)
		}
	}
}

func TestSyncServiceRestartsHalfExecutedPipelineWithNextStagedBody(t *testing.T) {
	bc := makeTestChain(t)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("insert block1: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}
	block2 := stubBlock(2, block1.Hash())
	result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block2, rawOf(t, block2))
	if result.StageError != nil || result.ProgressWriteError != nil {
		t.Fatalf("stage block2 result = %+v", result)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.syncing = true
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	path2 := ss.blockPath[block2.Number()]
	ss.mu.Unlock()

	if buffered != 1 || target != block2.Number() {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 1/%d", buffered, target, block2.Number())
	}
	if path2 != block2.Hash() {
		t.Fatalf("restored block path for block2 = %x, want %x", path2, block2.Hash())
	}
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want block1 completed", stage, row, ok, err)
		}
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block2.Number() || !ready.HasBlockHash || ready.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodiesReady after half-execution restart = %+v ok=%v err=%v, want block2", ready, ok, err)
	}

	ss.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after half-execution restart drain = %v, want block2 %x", got, block2.Hash())
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || ok {
		t.Fatalf("staged block2 after half-execution drain ok=%v err=%v, want deleted", ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after half-execution drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceRestartsHalfCommittedPipelineWithNextStagedBody(t *testing.T) {
	bc := makeTestChain(t)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("insert block1: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution, rawdb.StageSyncCommitment} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}
	block2 := stubBlock(2, block1.Hash())
	result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block2, rawOf(t, block2))
	if result.StageError != nil || result.ProgressWriteError != nil {
		t.Fatalf("stage block2 result = %+v", result)
	}

	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.syncing = true
	buffered := len(ss.blockBuffer)
	target := ss.targetHeadNum
	path2 := ss.blockPath[block2.Number()]
	ss.mu.Unlock()

	if buffered != 1 || target != block2.Number() {
		t.Fatalf("restored staged bodies buffered=%d target=%d, want 1/%d", buffered, target, block2.Number())
	}
	if path2 != block2.Hash() {
		t.Fatalf("restored block path for block2 = %x, want %x", path2, block2.Hash())
	}
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress after startup = %+v ok=%v err=%v, want block1 completed", stage, row, ok, err)
		}
	}

	ss.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after half-commitment restart drain = %v, want block2 %x", got, block2.Hash())
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || ok {
		t.Fatalf("staged block2 after half-commitment drain ok=%v err=%v, want deleted", ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after half-commitment drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceContinuesDrainAfterResumePhasePublish(t *testing.T) {
	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "4")
	bc := makeTestChain(t)
	bc.SetAsyncCommit(true)

	ss := NewSyncService(bc, nil)
	if err := ss.SetImportBatchSize(1); err != nil {
		t.Fatalf("set import batch size: %v", err)
	}

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block, rawOf(t, block))
		if result.StageError != nil || result.ProgressWriteError != nil {
			t.Fatalf("stage block %d result = %+v", block.Number(), result)
		}
	}

	var hookOnce sync.Once
	hooked := make(chan struct{})
	release := make(chan struct{})
	core.SetCommitFoldHookForTest(func(blockNum uint64) error {
		if blockNum == block1.Number() {
			hookOnce.Do(func() {
				close(hooked)
				<-release
			})
		}
		return nil
	})
	t.Cleanup(func() {
		core.SetCommitFoldHookForTest(nil)
		select {
		case <-release:
		default:
			close(release)
		}
	})

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	buffered := len(ss.blockBuffer)
	ss.mu.Unlock()
	if buffered != 2 {
		t.Fatalf("restored staged bodies buffered=%d, want 2", buffered)
	}

	done := make(chan struct{})
	go func() {
		ss.drainBufferedBlocks()
		close(done)
	}()

	select {
	case <-hooked:
	case <-time.After(3 * time.Second):
		t.Fatal("commit fold hook was not reached")
	}
	// The first commitment fold is deliberately blocked. A depth-4 session must
	// still execute the next local chunk and enqueue its fold before the barrier
	// is released; otherwise every pending commitment suffix serializes the
	// staged drain back to one local import chunk at a time.
	if !waitUntil(3*time.Second, func() bool {
		row, ok, err := rawdb.ReadStageProgressRow(bc.BufferedDB(), rawdb.StageExecution)
		return err == nil && ok && row.HasBlockHash && row.BlockNum == block2.Number() && row.BlockHash == block2.Hash()
	}) {
		row, ok, err := rawdb.ReadStageProgressRow(bc.BufferedDB(), rawdb.StageExecution)
		t.Fatalf("next chunk did not execute while first commitment was blocked: row=%+v ok=%v err=%v", row, ok, err)
	}
	close(release)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish after releasing commit fold hook")
	}

	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after resume-phase drain = %v, want block2 %x; stages=%s", got, block2.Hash(), syncPipelineProgressDebug(t, bc.DB()))
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after resume-phase drain ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after resume-phase drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceDepthTwoSessionExecutesNextChunkBeforeCommitmentSettles(t *testing.T) {
	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "2")
	bc := makeTestChain(t)
	bc.SetAsyncCommit(true)

	ss := NewSyncService(bc, nil)
	if err := ss.SetImportBatchSize(1); err != nil {
		t.Fatalf("set import batch size: %v", err)
	}

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block, rawOf(t, block))
		if result.StageError != nil || result.ProgressWriteError != nil {
			t.Fatalf("stage block %d result = %+v", block.Number(), result)
		}
	}

	var hookOnce sync.Once
	hooked := make(chan struct{})
	release := make(chan struct{})
	core.SetCommitFoldHookForTest(func(blockNum uint64) error {
		if blockNum == block1.Number() {
			hookOnce.Do(func() {
				close(hooked)
				<-release
			})
		}
		return nil
	})
	t.Cleanup(func() {
		core.SetCommitFoldHookForTest(nil)
		select {
		case <-release:
		default:
			close(release)
		}
	})

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()

	done := make(chan struct{})
	go func() {
		ss.drainBufferedBlocks()
		close(done)
	}()

	select {
	case <-hooked:
	case <-time.After(3 * time.Second):
		t.Fatal("commit fold hook was not reached")
	}
	if !waitUntil(3*time.Second, func() bool {
		row, ok, err := rawdb.ReadStageProgressRow(bc.BufferedDB(), rawdb.StageExecution)
		return err == nil && ok && row.HasBlockHash && row.BlockNum == block2.Number() && row.BlockHash == block2.Hash()
	}) {
		row, ok, err := rawdb.ReadStageProgressRow(bc.BufferedDB(), rawdb.StageExecution)
		t.Fatalf("depth-2 session did not execute next chunk before first commitment settled: row=%+v ok=%v err=%v", row, ok, err)
	}
	close(release)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish after releasing commit fold hook")
	}
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after depth-2 session drain = %v, want block2 %x; stages=%s", got, block2.Hash(), syncPipelineProgressDebug(t, bc.DB()))
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after depth-2 drain ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after depth-2 drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceDepthTwoSessionPublishesCommittedPrefixBeforeLaterFailure(t *testing.T) {
	testSyncServiceAsyncSessionPublishesCommittedPrefixBeforeLaterFailure(t, "2")
}

func TestSyncServiceDeepAsyncPublishesCommittedPrefixBeforeLaterFailure(t *testing.T) {
	testSyncServiceAsyncSessionPublishesCommittedPrefixBeforeLaterFailure(t, "4")
}

func testSyncServiceAsyncSessionPublishesCommittedPrefixBeforeLaterFailure(t *testing.T, depth string) {
	t.Helper()
	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", depth)
	bc := makeTestChain(t)
	bc.SetAsyncCommit(true)

	ss := NewSyncService(bc, nil)
	if err := ss.SetImportBatchSize(1); err != nil {
		t.Fatalf("set import batch size: %v", err)
	}
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	// This body extends block1 but declares the wrong java-tron account root.
	// The first chunk can commit, while canonical execution must pause at the
	// second after its execution stage has been observed.
	block2 := stubBlock(2, block1.Hash())
	block2.Proto().BlockHeader.RawData.AccountStateRoot = tcommon.Hash{0xff}.Bytes()
	block2 = types.NewBlockFromPB(block2.Proto())
	for _, block := range []*types.Block{block1, block2} {
		result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block, rawOf(t, block))
		if result.StageError != nil || result.ProgressWriteError != nil {
			t.Fatalf("stage block %d result = %+v", block.Number(), result)
		}
	}

	var hookOnce sync.Once
	hooked := make(chan struct{})
	release := make(chan struct{})
	core.SetCommitFoldHookForTest(func(blockNum uint64) error {
		if blockNum == block1.Number() {
			hookOnce.Do(func() {
				close(hooked)
				<-release
			})
		}
		return nil
	})
	t.Cleanup(func() {
		core.SetCommitFoldHookForTest(nil)
		select {
		case <-release:
		default:
			close(release)
		}
	})

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	done := make(chan struct{})
	go func() {
		ss.drainBufferedBlocks()
		close(done)
	}()
	select {
	case <-hooked:
	case <-time.After(3 * time.Second):
		t.Fatal("commit fold hook was not reached")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish after later-block failure")
	}

	if got := bc.CurrentBlock(); got == nil || got.Hash() != block1.Hash() {
		t.Fatalf("head after failed second chunk = %v, want block1 %x", got, block1.Hash())
	}
	if !ss.IsPaused() {
		t.Fatal("sync did not pause after the second chunk failed")
	}
	assertSyncPipelineProgress(t, bc.DB(), block1)
}

func TestSyncServicePublishesResumePhaseProgressAfterBarrier(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	block := stubBlock(1, bc.CurrentBlock().Hash())
	for _, stage := range []rawdb.StageID{rawdb.StageCommitment, rawdb.StageFinish} {
		if err := rawdb.WriteStageProgressWithHash(bc.DB(), stage, block.Number(), block.Hash()); err != nil {
			t.Fatalf("write canonical %s progress: %v", stage, err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncExecution, block.Number(), block.Hash()); err != nil {
		t.Fatalf("write sync execution progress: %v", err)
	}
	phases := []syncdl.ImportStagePhasePlan{
		{
			Phase:          syncdl.ImportStagePhaseCommitment,
			CanonicalStage: rawdb.StageCommitment,
			SyncStage:      rawdb.StageSyncCommitment,
			Tasks:          []syncdl.ImportStageTask{syncdl.ImportCommitmentStageTask(block.Number(), block.Hash())},
		},
		{
			Phase:          syncdl.ImportStagePhaseFinish,
			CanonicalStage: rawdb.StageFinish,
			SyncStage:      rawdb.StageSyncFinish,
			Tasks:          []syncdl.ImportStageTask{syncdl.ImportFinishStageTask(block.Number(), block.Hash())},
		},
	}

	result := ss.publishImportResumePhaseProgress(phases, true, false)
	publish := result.Publish.Publish
	if !result.Finalization.Publish || !publish.Applied || publish.Rows != 2 || publish.WriteError != nil || !publish.Plan.OK {
		t.Fatalf("resume publish result = %+v, want two rows applied", result)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block.Number() || row.BlockHash != block.Hash() || !row.HasBlockHash {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block1 hash-bound", stage, row, ok, err)
		}
	}
}

func TestSyncServiceResumePhasePublishRejectsUnsafeRows(t *testing.T) {
	tests := map[string]struct {
		setup           func(t *testing.T, db ethdb.KeyValueStore, block *types.Block)
		wantStatus      syncdl.ImportResumePhasePublishStatus
		wantExistingRow bool
	}{
		"canonical mismatch": {
			setup: func(t *testing.T, db ethdb.KeyValueStore, block *types.Block) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageCommitment, block.Number(), tcommon.Hash{0xee}); err != nil {
					t.Fatalf("write mismatched canonical commitment: %v", err)
				}
			},
			wantStatus: syncdl.ImportResumePhasePublishCanonicalMismatch,
		},
		"sync ahead": {
			setup: func(t *testing.T, db ethdb.KeyValueStore, block *types.Block) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageCommitment, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write canonical commitment: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncCommitment, block.Number()+1, tcommon.Hash{0x02}); err != nil {
					t.Fatalf("write ahead sync commitment: %v", err)
				}
			},
			wantStatus:      syncdl.ImportResumePhasePublishSyncAhead,
			wantExistingRow: true,
		},
		"upstream missing": {
			setup: func(t *testing.T, db ethdb.KeyValueStore, block *types.Block) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageCommitment, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write canonical commitment: %v", err)
				}
			},
			wantStatus: syncdl.ImportResumePhasePublishUpstreamMissing,
		},
		"upstream unbound": {
			setup: func(t *testing.T, db ethdb.KeyValueStore, block *types.Block) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageCommitment, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write canonical commitment: %v", err)
				}
				if err := rawdb.WriteStageProgress(db, rawdb.StageSyncExecution, block.Number()); err != nil {
					t.Fatalf("write unbound sync execution: %v", err)
				}
			},
			wantStatus: syncdl.ImportResumePhasePublishUpstreamUnbound,
		},
		"upstream behind": {
			setup: func(t *testing.T, db ethdb.KeyValueStore, block *types.Block) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageCommitment, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write canonical commitment: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, block.Number()-1, tcommon.Hash{0x01}); err != nil {
					t.Fatalf("write behind sync execution: %v", err)
				}
			},
			wantStatus: syncdl.ImportResumePhasePublishUpstreamBehind,
		},
		"upstream hash mismatch": {
			setup: func(t *testing.T, db ethdb.KeyValueStore, block *types.Block) {
				t.Helper()
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageCommitment, block.Number(), block.Hash()); err != nil {
					t.Fatalf("write canonical commitment: %v", err)
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, block.Number(), tcommon.Hash{0xee}); err != nil {
					t.Fatalf("write mismatched sync execution: %v", err)
				}
			},
			wantStatus: syncdl.ImportResumePhasePublishUpstreamHashMismatch,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			bc := makeTestChain(t)
			ss := NewSyncService(bc, nil)
			block := stubBlock(1, bc.CurrentBlock().Hash())
			test.setup(t, bc.DB(), block)
			phase := syncdl.ImportStagePhasePlan{
				Phase:          syncdl.ImportStagePhaseCommitment,
				CanonicalStage: rawdb.StageCommitment,
				SyncStage:      rawdb.StageSyncCommitment,
				Tasks:          []syncdl.ImportStageTask{syncdl.ImportCommitmentStageTask(block.Number(), block.Hash())},
			}

			result := ss.publishImportResumePhaseProgress([]syncdl.ImportStagePhasePlan{phase}, true, false)
			publish := result.Publish.Publish
			if !result.Finalization.Publish || publish.Applied || publish.Rows != 0 || publish.WriteError != nil || publish.Plan.OK ||
				len(publish.Plan.Decisions) != 1 || publish.Plan.Decisions[0].Status != test.wantStatus {
				t.Fatalf("resume publish result = %+v, want blocked with status %s", result, test.wantStatus)
			}
			if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncCommitment); err != nil {
				t.Fatalf("read sync commitment after rejected publish: %v", err)
			} else if !test.wantExistingRow && ok {
				t.Fatalf("sync commitment after %s rejection = %+v, want absent", name, row)
			} else if test.wantExistingRow && (!ok || row.BlockNum != block.Number()+1 || row.BlockHash != (tcommon.Hash{0x02})) {
				t.Fatalf("sync commitment after %s rejection = %+v ok=%v, want existing ahead row retained", name, row, ok)
			}
		})
	}
}

func TestSyncServiceResumePhasePublishFinalizationSkipsWhenPaused(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	block := stubBlock(1, bc.CurrentBlock().Hash())
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageCommitment, block.Number(), block.Hash()); err != nil {
		t.Fatalf("write canonical commitment: %v", err)
	}
	phase := syncdl.ImportStagePhasePlan{
		Phase:          syncdl.ImportStagePhaseCommitment,
		CanonicalStage: rawdb.StageCommitment,
		SyncStage:      rawdb.StageSyncCommitment,
		Tasks:          []syncdl.ImportStageTask{syncdl.ImportCommitmentStageTask(block.Number(), block.Hash())},
	}

	result := ss.publishImportResumePhaseProgress([]syncdl.ImportStagePhasePlan{phase}, true, true)
	if result.Finalization.Publish ||
		result.Finalization.SkipReason != syncdl.ImportResumePhasePublishFinalizationPaused ||
		result.Publish.Publish.Applied {
		t.Fatalf("paused finalization result = %+v, want no publish", result)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncCommitment); err != nil || ok {
		t.Fatalf("sync commitment after paused finalization = %+v ok=%v err=%v, want absent", row, ok, err)
	}
}

func TestSyncServicePausesWhenResumePhasePublishFails(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	block := stubBlock(7, bc.CurrentBlock().Hash())

	ss.applyImportResumePhaseDrainContinuation(syncdl.ImportResumePhasePublishFinalizationRunApplyResult{
		Finalization: syncdl.ImportResumePhasePublishFinalizationPlan{
			Publish: true,
			Phases: []syncdl.ImportStagePhasePlan{
				{
					Phase:          syncdl.ImportStagePhaseCommitment,
					CanonicalStage: rawdb.StageCommitment,
					SyncStage:      rawdb.StageSyncCommitment,
					Tasks:          []syncdl.ImportStageTask{syncdl.ImportCommitmentStageTask(block.Number(), block.Hash())},
				},
			},
		},
		Publish: syncdl.ImportResumePhasePublishRunApplyResult{
			PublishPlan: syncdl.ImportResumePhasePublishPlan{
				Decisions: []syncdl.ImportResumePhasePublishDecision{
					{
						SyncStage:   rawdb.StageSyncCommitment,
						TargetBlock: block.Number(),
						TargetHash:  block.Hash(),
						Status:      syncdl.ImportResumePhasePublishCanonicalMissing,
					},
				},
			},
		},
	}, nil)

	paused, atNum, _, err := ss.PausedStatus()
	if !paused || atNum != block.Number() || err == nil {
		t.Fatalf("pause status = paused:%v at:%d err:%v, want resume-publish pause at block %d", paused, atNum, err, block.Number())
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

func TestSyncServiceCompleteHookRunsAfterSessionReset(t *testing.T) {
	bc := makeTestChain(t)
	block := stubBlock(1, bc.CurrentBlock().Hash())
	if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
		t.Fatalf("write sync staged block: %v", err)
	}
	ss := NewSyncService(bc, nil)
	called := false
	ss.AddSyncCompleteHook(func() {
		ss.mu.Lock()
		syncing := ss.syncing
		ss.mu.Unlock()
		if syncing {
			t.Error("sync complete hook ran before the session reset")
		}
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Errorf("staged block during complete hook ok=%v err=%v, want deleted", ok, err)
		}
		called = true
	})

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.mu.Unlock()
	ss.finishSync()

	if !called {
		t.Fatal("sync complete hook was not called")
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

func syncPipelineProgressDebug(t *testing.T, db ethdb.KeyValueReader) string {
	t.Helper()
	out := ""
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		switch {
		case err != nil:
			out += fmt.Sprintf("%s=err:%v ", stage, err)
		case !ok:
			out += fmt.Sprintf("%s=missing ", stage)
		default:
			out += fmt.Sprintf("%s=%d/%x ", stage, row.BlockNum, row.BlockHash)
		}
	}
	return out
}
