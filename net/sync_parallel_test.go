package net

import (
	gnet "net"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestMultiPeerChainInventorySplitsFetchBatches(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peerA, closeA := testPeer(t, "sync-a")
	defer closeA()
	peerB, closeB := testPeer(t, "sync-b")
	defer closeB()

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.addPeerStateLocked(peerA)
	ss.addPeerStateLocked(peerB)
	ss.mu.Unlock()

	payload := testChainInventoryPayload(t, 1, 250, 1000)
	ss.HandleChainInventory(peerA, payload)
	ss.HandleChainInventory(peerB, payload)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	psA := ss.peers[peerA.ID()]
	psB := ss.peers[peerB.ID()]
	if psA == nil || psB == nil {
		t.Fatalf("missing peer state: a=%v b=%v", psA, psB)
	}
	if psA.inflight != maxFetchBatch {
		t.Fatalf("peer A inflight=%d, want %d", psA.inflight, maxFetchBatch)
	}
	if psB.inflight != maxFetchBatch {
		t.Fatalf("peer B inflight=%d, want %d", psB.inflight, maxFetchBatch)
	}
	if len(ss.requested) != 2*maxFetchBatch {
		t.Fatalf("global requested=%d, want %d", len(ss.requested), 2*maxFetchBatch)
	}
	assertPendingRange(t, "peer A", psA.pending, 1, 100)
	assertPendingRange(t, "peer B", psB.pending, 101, 200)
	for h := range psA.pending {
		if _, dup := psB.pending[h]; dup {
			t.Fatalf("same block requested from both peers: %x", h)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncInventory); err != nil || !ok || row.BlockNum != 1250 || row.HasBlockHash {
		t.Fatalf("sync inventory stage = %+v ok=%v err=%v, want target block 1250 without hash", row, ok, err)
	}
}

func TestMultiPeerSyncBuffersOutOfOrderBlocks(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peerA, closeA := testPeer(t, "ordered-a")
	defer closeA()
	peerB, closeB := testPeer(t, "ordered-b")
	defer closeB()

	parent := bc.CurrentBlock().Hash()
	block1 := stubBlock(1, parent)
	block2 := stubBlock(2, block1.Hash())

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	psA, _ := ss.addPeerStateLocked(peerA)
	psB, _ := ss.addPeerStateLocked(peerB)
	markPendingLocked(ss, psA, block1.ID())
	markPendingLocked(ss, psB, block2.ID())
	ss.mu.Unlock()

	if !ss.HandleBlock(peerB, block2, nil) {
		t.Fatal("block 2 should be consumed by sync")
	}
	if got := bc.CurrentBlock().Number(); got != 0 {
		t.Fatalf("out-of-order block should stay buffered, head=%d", got)
	}
	if staged, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || !ok || staged.Hash() != block2.Hash() {
		t.Fatalf("sync staged body for block2 = %v ok=%v err=%v, want %x", staged, ok, err, block2.Hash())
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies); err != nil || !ok || row.BlockNum != block2.Number() || !row.HasBlockHash || row.BlockHash != block2.Hash() {
		t.Fatalf("sync bodies stage = %+v ok=%v err=%v, want block2", row, ok, err)
	}

	if !ss.HandleBlock(peerA, block1, nil) {
		t.Fatal("block 1 should be consumed by sync")
	}
	if got := bc.CurrentBlock().Number(); got != 2 {
		t.Fatalf("buffered chain did not drain in order, head=%d", got)
	}
	if got := ss.stats.CurrentSnapshot().TotalBlocks; got != 2 {
		t.Fatalf("sync stats total blocks after buffered range drain = %d, want 2", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != block2.Number() || !row.HasBlockHash || row.BlockHash != block2.Hash() {
		t.Fatalf("sync import stage = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("imported sync staged body #%d ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("sync bodies ready after imported range = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
	ss.mu.Lock()
	buffered := len(ss.blockBuffer)
	ss.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("buffered range not fully drained: %d blocks remain", buffered)
	}
}

func TestMultiPeerSyncPausesAtFailedBlockInBufferedRange(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "range-fail")
	defer closePeer()

	parent := bc.CurrentBlock().Hash()
	block1 := stubBlock(1, parent)
	block2 := stubBlock(2, tcommon.Hash{0xee})

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.blockBuffer[1] = syncdl.BufferedBlock{Raw: rawOf(t, block1), Num: 1, Hash: block1.Hash(), Peer: peer}
	ss.blockBuffer[2] = syncdl.BufferedBlock{Raw: rawOf(t, block2), Num: 2, Hash: block2.Hash(), Peer: peer}
	ss.bufferedHash[block1.Hash()] = struct{}{}
	ss.bufferedHash[block2.Hash()] = struct{}{}
	ss.mu.Unlock()

	ss.drainBufferedBlocksOnce()

	if got := bc.CurrentBlock().Number(); got != 1 {
		t.Fatalf("head after partial buffered range failure = %d, want 1", got)
	}
	paused, atNum, _, err := ss.PausedStatus()
	if !paused || atNum != 2 || err == nil {
		t.Fatalf("paused=%v at=%d err=%v, want paused at block 2", paused, atNum, err)
	}
	if got := ss.stats.CurrentSnapshot().TotalBlocks; got != 1 {
		t.Fatalf("sync stats total blocks after partial range = %d, want 1", got)
	}
	assertSyncPipelineProgress(t, bc.DB(), block1)
}

func TestSyncStageProgressCollectorKeepsLatestAppliedStage(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	collector := syncdl.NewStageProgressCollector()
	for _, stage := range rawdb.CanonicalExecutionStages() {
		collector.Observe(stage, block1.Number(), block1.Hash())
	}
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())

	collector.WriteSchedule(syncdl.NewImportStageSchedule(block1.Number(), block1.Hash()), func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		ss.writeStageProgress(stage, blockNum, blockHash, true)
	})

	assertSyncPipelineProgress(t, bc.DB(), block1)
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("sync import after capped collector write = %+v ok=%v err=%v, want block1", row, ok, err)
	}
}

func TestRecordImportedBatchKeepsAppliedStagePrefixAfterPartialExecution(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	batch := syncdl.BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []syncdl.BufferedBlock{
			{Raw: rawOf(t, block1), Num: block1.Number(), Hash: block1.Hash()},
			{Raw: rawOf(t, block2), Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := syncdl.NewStageProgressCollector()
	for _, stage := range rawdb.CanonicalExecutionStages() {
		collector.Observe(stage, block1.Number(), block1.Hash())
	}
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())

	execution := syncdl.PlanImportBatchExecution(batch)
	plan := execution.ProgressPlan(batch, 1, collector)
	ss.recordImportedBatch(plan, time.Millisecond)

	assertSyncPipelineProgress(t, bc.DB(), block1)
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block1.Number()); err != nil || ok {
		t.Fatalf("staged block1 after partial import ok=%v err=%v, want deleted", ok, err)
	}
	if staged, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || !ok || staged.Hash() != block2.Hash() {
		t.Fatalf("staged block2 after partial import = %v ok=%v err=%v, want retained", staged, ok, err)
	}
}

func TestSyncServiceRestoresHalfExecutedSessionOnStart(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	batch := syncdl.BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []syncdl.BufferedBlock{
			{Raw: rawOf(t, block1), Num: block1.Number(), Hash: block1.Hash()},
			{Raw: rawOf(t, block2), Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := syncdl.NewStageProgressCollector()
	if err := bc.InsertBlocksWithStageHook([]*types.Block{block1}, collector.Observe); err != nil {
		t.Fatalf("insert partially executed block1: %v", err)
	}
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())

	execution := syncdl.PlanImportBatchExecution(batch)
	plan := execution.ProgressPlan(batch, 1, collector)
	ss.recordImportedBatch(plan, time.Millisecond)

	assertSyncPipelineProgress(t, bc.DB(), block1)
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block1.Number()); err != nil || ok {
		t.Fatalf("staged block1 after half execution ok=%v err=%v, want deleted", ok, err)
	}
	if staged, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || !ok || staged.Hash() != block2.Hash() {
		t.Fatalf("staged block2 after half execution = %v ok=%v err=%v, want retained", staged, ok, err)
	}

	restarted := NewSyncService(bc, nil)
	restarted.mu.Lock()
	restarted.initSessionLocked(time.Now())
	buffered := len(restarted.blockBuffer)
	target := restarted.targetHeadNum
	path2 := restarted.blockPath[block2.Number()]
	restarted.mu.Unlock()

	if buffered != 1 || target != block2.Number() {
		t.Fatalf("restart restored buffered=%d target=%d, want 1/%d", buffered, target, block2.Number())
	}
	if path2 != block2.Hash() {
		t.Fatalf("restart block path for block2 = %x, want %x", path2, block2.Hash())
	}
	ready, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block2.Number() || !ready.HasBlockHash || ready.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodiesReady after half-executed restart = %+v ok=%v err=%v, want block2", ready, ok, err)
	}

	restarted.drainBufferedBlocks()
	if got := bc.CurrentBlock(); got == nil || got.Hash() != block2.Hash() {
		t.Fatalf("head after half-executed restart drain = %v, want block2 %x", got, block2.Hash())
	}
	assertSyncPipelineProgress(t, bc.DB(), block2)
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block2.Number()); err != nil || ok {
		t.Fatalf("staged block2 after restart drain ok=%v err=%v, want deleted", ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || ok {
		t.Fatalf("SyncBodiesReady after half-executed restart drain = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestSyncServiceStartupPrunesStagedBodyGap(t *testing.T) {
	bc := makeTestChain(t)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	block3 := stubBlock(3, block2.Hash())
	for _, block := range []*types.Block{block1, block3} {
		result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block, nil)
		if result.StageError != nil || result.ProgressWriteError != nil {
			t.Fatalf("write staged block %d: stage=%v progress=%v", block.Number(), result.StageError, result.ProgressWriteError)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies); err != nil || !ok || row.BlockNum != block3.Number() {
		t.Fatalf("precondition SyncBodies = %+v ok=%v err=%v, want block3", row, ok, err)
	}

	restarted := NewSyncService(bc, nil)
	restarted.mu.Lock()
	restarted.initSessionLocked(time.Now())
	buffered := len(restarted.blockBuffer)
	path1 := restarted.blockPath[block1.Number()]
	_, path3 := restarted.blockPath[block3.Number()]
	restarted.mu.Unlock()

	if buffered != 1 {
		t.Fatalf("restored buffered blocks = %d, want only contiguous block1", buffered)
	}
	if path1 != block1.Hash() {
		t.Fatalf("restored path for block1 = %x, want %x", path1, block1.Hash())
	}
	if path3 {
		t.Fatalf("stale path for gapped block3 was restored")
	}
	if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block3.Number()); err != nil || ok {
		t.Fatalf("gapped staged block3 ok=%v err=%v, want pruned", ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodies); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("SyncBodies after gap prune = %+v ok=%v err=%v, want block1", row, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("SyncBodiesReady after gap prune = %+v ok=%v err=%v, want block1", row, ok, err)
	}
}

func TestSyncServiceDrainRepairsBodiesReadyHashMismatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	if result := rawdb.WriteSyncStagedBlockRawAndProgress(bc.DB(), block1, nil); result.StageError != nil || result.ProgressWriteError != nil {
		t.Fatalf("write staged block1: stage=%v progress=%v", result.StageError, result.ProgressWriteError)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.DB(), rawdb.StageSyncBodiesReady, block1.Number(), tcommon.Hash{0xff}); err != nil {
		t.Fatalf("write mismatched SyncBodiesReady: %v", err)
	}

	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	remaining := len(ss.blockBuffer)
	ss.mu.Unlock()

	if len(batch.Buffered) != 1 || batch.Buffered[0].Num != block1.Number() || batch.Buffered[0].Hash != block1.Hash() {
		t.Fatalf("popped batch = %+v, want repaired block1", batch.Buffered)
	}
	if remaining != 0 {
		t.Fatalf("remaining buffered blocks = %d, want 0", remaining)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncBodiesReady); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("SyncBodiesReady after hash-mismatch drain = %+v ok=%v err=%v, want repaired block1", row, ok, err)
	}
}

func TestRecordImportedBatchBlocksDownstreamStagesAfterExecutionMismatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	batch := syncdl.BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []syncdl.BufferedBlock{
			{Raw: rawOf(t, block1), Num: block1.Number(), Hash: block1.Hash()},
			{Raw: rawOf(t, block2), Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := syncdl.NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageCommitment, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	execution := syncdl.PlanImportBatchExecution(batch)
	plan := execution.ProgressPlan(batch, 2, collector)
	ss.recordImportedBatch(plan, time.Millisecond)

	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
		t.Fatalf("sync import progress = %+v ok=%v err=%v, want block2", row, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncExecution); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("sync execution progress = %+v ok=%v err=%v, want block1", row, ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want blocked", stage, row, ok, err)
		}
	}
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after import ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
}

func TestRecordImportedBatchBlocksDownstreamStagesAfterCommitmentMismatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(bc.DB(), block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	batch := syncdl.BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []syncdl.BufferedBlock{
			{Raw: rawOf(t, block1), Num: block1.Number(), Hash: block1.Hash()},
			{Raw: rawOf(t, block2), Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := syncdl.NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageExecution, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageCommitment, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	execution := syncdl.PlanImportBatchExecution(batch)
	plan := execution.ProgressPlan(batch, 2, collector)
	ss.recordImportedBatch(plan, time.Millisecond)

	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block2", stage, row, ok, err)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageSyncCommitment); err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
		t.Fatalf("sync commitment progress = %+v ok=%v err=%v, want block1", row, ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want blocked", stage, row, ok, err)
		}
	}
	for _, block := range []*types.Block{block1, block2} {
		if _, ok, err := rawdb.ReadSyncStagedBlock(bc.DB(), block.Number()); err != nil || ok {
			t.Fatalf("staged block %d after import ok=%v err=%v, want deleted", block.Number(), ok, err)
		}
	}
}

func TestMultiPeerSyncRejectsConflictingSameHeightInventories(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peerA, closeA := testPeer(t, "fork-a")
	defer closeA()
	peerB, closeB := testPeer(t, "fork-b")
	defer closeB()

	parent := bc.CurrentBlock().Hash()
	blockA1 := forkedStubBlock(1, parent, 0xa1)
	blockA2 := forkedStubBlock(2, blockA1.Hash(), 0xa2)
	blockB1 := forkedStubBlock(1, parent, 0xb1)
	blockB2 := forkedStubBlock(2, blockB1.Hash(), 0xb2)
	if blockA1.Hash() == blockB1.Hash() || blockA2.Hash() == blockB2.Hash() {
		t.Fatal("test setup expected distinct fork hashes at the same heights")
	}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.addPeerStateLocked(peerA)
	ss.addPeerStateLocked(peerB)
	ss.mu.Unlock()

	ss.HandleChainInventory(peerA, testChainInventoryFromBlocks(t, blockA1, blockA2))
	ss.HandleChainInventory(peerB, testChainInventoryFromBlocks(t, blockB1, blockB2))

	ss.mu.Lock()
	defer ss.mu.Unlock()
	psA := ss.peers[peerA.ID()]
	psB := ss.peers[peerB.ID()]
	if psA == nil || psB == nil {
		t.Fatalf("missing peer state: a=%v b=%v", psA, psB)
	}
	if psA.inflight != 2 {
		t.Fatalf("peer A inflight=%d, want 2", psA.inflight)
	}
	if psB.inflight != 0 || len(psB.fetchList) != 0 {
		t.Fatalf("peer B conflicting fork was not filtered: inflight=%d fetchList=%d", psB.inflight, len(psB.fetchList))
	}
	if len(ss.requested) != 2 {
		t.Fatalf("requested=%d, want only peer A's two blocks", len(ss.requested))
	}
	if ss.blockPath[1] != blockA1.Hash() || ss.blockPath[2] != blockA2.Hash() {
		t.Fatalf("sync path changed away from peer A fork")
	}
	if _, ok := ss.requested[blockB1.Hash()]; ok {
		t.Fatal("conflicting block #1 from peer B was requested")
	}
	if _, ok := ss.requested[blockB2.Hash()]; ok {
		t.Fatal("conflicting block #2 from peer B was requested")
	}
}

func TestMultiPeerSyncDoesNotRepollBeforeInventoryTipProcessed(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "window-peer")
	defer closePeer()

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = 10
	out := ss.fillFetchSlotsLocked(time.Now())
	waiting := !ps.chainRequested
	ss.mu.Unlock()

	if len(out) != 0 || !waiting {
		t.Fatalf("sent early sync request before local head caught up: out=%d waiting=%v", len(out), waiting)
	}

	caughtUp := makeChainWithBlocks(t, 10)
	ss = NewSyncService(caughtUp, nil)
	peer, closePeer = testPeer(t, "caught-up-peer")
	defer closePeer()

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ = ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = 10
	out = ss.fillFetchSlotsLocked(time.Now())
	requested := ps.chainRequested
	ss.mu.Unlock()

	if len(out) != 1 || !out[0].chain || !requested {
		t.Fatalf("did not request next window after local head caught up: out=%d chain=%v requested=%v",
			len(out), len(out) == 1 && out[0].chain, requested)
	}
}

func TestJoinAvailablePeersFallsBackToHandshakedPeers(t *testing.T) {
	bc := makeTestChain(t)
	handler := NewTronHandler(bc, nil, nil)
	ss := NewSyncService(bc, handler)

	peerA, closeA := testPeer(t, "fallback-a")
	defer closeA()
	peerB, closeB := testPeer(t, "fallback-b")
	defer closeB()
	handler.peers[peerA.ID()] = &peerState{peer: peerA, connState: peerStateHandshaked, rl: p2p.NewRateLimiter()}
	handler.peers[peerB.ID()] = &peerState{peer: peerB, connState: peerStateHandshaked, rl: p2p.NewRateLimiter()}

	ss.joinAvailablePeers()

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.peers[peerA.ID()] == nil || ss.peers[peerB.ID()] == nil {
		t.Fatalf("handshaked fallback peers were not joined: %v", ss.peers)
	}
}

func TestSyncCandidatesSkipPeersBelowRequiredRange(t *testing.T) {
	bc := makeChainWithBlocks(t, 10)
	handler := NewTronHandler(bc, nil, nil)

	full, closeFull := testPeer(t, "range-full")
	defer closeFull()
	edge, closeEdge := testPeer(t, "range-edge")
	defer closeEdge()
	pruned, closePruned := testPeer(t, "range-pruned")
	defer closePruned()

	handler.peers[full.ID()] = &peerState{
		peer:           full,
		connState:      peerStateHandshaked,
		rl:             p2p.NewRateLimiter(),
		headNum:        30,
		lowestBlockNum: 0,
	}
	handler.peers[edge.ID()] = &peerState{
		peer:           edge,
		connState:      peerStateHandshaked,
		rl:             p2p.NewRateLimiter(),
		headNum:        20,
		lowestBlockNum: 11,
	}
	handler.peers[pruned.ID()] = &peerState{
		peer:           pruned,
		connState:      peerStateHandshaked,
		rl:             p2p.NewRateLimiter(),
		headNum:        100,
		lowestBlockNum: 12,
	}

	candidates := handler.SyncCandidates(nil, 10)
	seen := map[string]bool{}
	for _, peer := range candidates {
		seen[peer.ID()] = true
	}
	if !seen[full.ID()] || !seen[edge.ID()] {
		t.Fatalf("expected eligible peers in candidates, got %v", seen)
	}
	if seen[pruned.ID()] {
		t.Fatalf("peer below required sync range should be skipped")
	}
	if best := handler.BestSyncCandidate(nil); best == nil || best.ID() != full.ID() {
		t.Fatalf("best sync candidate = %v, want %s", best, full.ID())
	}
}

func TestStartSyncSkipsPeerBelowRequiredRange(t *testing.T) {
	bc := makeChainWithBlocks(t, 10)
	handler := NewTronHandler(bc, nil, nil)
	ss := NewSyncService(bc, handler)

	pruned, closePruned := testPeer(t, "start-pruned")
	defer closePruned()
	handler.peers[pruned.ID()] = &peerState{
		peer:           pruned,
		connState:      peerStateHandshaked,
		rl:             p2p.NewRateLimiter(),
		headNum:        20,
		lowestBlockNum: 12,
	}

	ss.StartSync(pruned)
	if ss.IsSyncing() {
		t.Fatal("sync started from peer whose lowest block is above the required next block")
	}

	eligible, closeEligible := testPeer(t, "start-eligible")
	defer closeEligible()
	handler.peers[eligible.ID()] = &peerState{
		peer:           eligible,
		connState:      peerStateHandshaked,
		rl:             p2p.NewRateLimiter(),
		headNum:        20,
		lowestBlockNum: 11,
	}

	ss.StartSync(eligible)
	if !ss.IsSyncing() {
		t.Fatal("sync did not start from peer covering the required next block")
	}
}

func TestShouldJoinAvailablePeersThrottle(t *testing.T) {
	bc := makeTestChain(t)
	handler := NewTronHandler(bc, nil, nil)
	ss := NewSyncService(bc, handler)
	now := time.Unix(100, 0)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.syncing = true
	progress := ss.sessionProgressLocked()
	if !ss.shouldJoinAvailablePeersLocked(now, progress) {
		t.Fatal("first join attempt should be allowed")
	}
	if ss.shouldJoinAvailablePeersLocked(now.Add(peerJoinAttemptInterval/2), progress) {
		t.Fatal("join attempt should be throttled")
	}
	if !ss.shouldJoinAvailablePeersLocked(now.Add(peerJoinAttemptInterval), progress) {
		t.Fatal("join attempt should be allowed after throttle interval")
	}
}

func testPeer(t *testing.T, id string) (*p2p.Peer, func()) {
	t.Helper()
	c1, c2 := gnet.Pipe()
	return p2p.NewPeer(c1, id, false, nil), func() {
		_ = c1.Close()
		_ = c2.Close()
	}
}

func testChainInventoryPayload(t *testing.T, start, count int64, remain int64) []byte {
	t.Helper()
	ids := make([]*corepb.ChainInventory_BlockId, 0, count)
	for n := start; n < start+count; n++ {
		hash := tcommon.Hash{0xa1, byte(n), byte(n >> 8), byte(n >> 16)}
		ids = append(ids, &corepb.ChainInventory_BlockId{
			Hash:   hash[:],
			Number: n,
		})
	}
	payload, err := proto.Marshal(&corepb.ChainInventory{Ids: ids, RemainNum: remain})
	if err != nil {
		t.Fatalf("marshal chain inventory: %v", err)
	}
	return payload
}

func testChainInventoryFromBlocks(t *testing.T, blocks ...*types.Block) []byte {
	t.Helper()
	ids := make([]*corepb.ChainInventory_BlockId, 0, len(blocks))
	for _, block := range blocks {
		bid := block.ID()
		ids = append(ids, &corepb.ChainInventory_BlockId{
			Hash:   bid.Hash[:],
			Number: int64(bid.Num),
		})
	}
	payload, err := proto.Marshal(&corepb.ChainInventory{Ids: ids})
	if err != nil {
		t.Fatalf("marshal chain inventory: %v", err)
	}
	return payload
}

func forkedStubBlock(num int64, parent tcommon.Hash, salt byte) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         num,
				Timestamp:      num * 3000,
				ParentHash:     parent[:],
				WitnessAddress: []byte{salt},
			},
			WitnessSignature: make([]byte, 65),
		},
	})
}

func assertPendingRange(t *testing.T, label string, pending map[tcommon.Hash]uint64, min, max uint64) {
	t.Helper()
	if len(pending) != int(max-min+1) {
		t.Fatalf("%s pending=%d, want %d", label, len(pending), max-min+1)
	}
	for _, num := range pending {
		if num < min || num > max {
			t.Fatalf("%s requested block #%d outside [%d,%d]", label, num, min, max)
		}
	}
}

func markPendingLocked(ss *SyncService, ps *syncPeerState, bid types.BlockID) {
	ss.reserveBlockPathLocked(bid)
	ps.inflight = 1
	ps.pending = map[tcommon.Hash]uint64{bid.Hash: bid.Num}
	ps.pendingIDs = map[tcommon.Hash]types.BlockID{bid.Hash: bid}
	ps.requestedHashes[bid.Hash] = struct{}{}
	ss.requested[bid.Hash] = ps.peer.ID()
}
