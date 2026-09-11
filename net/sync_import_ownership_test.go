package net

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// ownDecodedTestBatch stops at the real gap between removing a decoded prefix
// from the raw buffer and executing it. No block has reached the chain yet.
func ownDecodedTestBatch(t *testing.T, ss *SyncService, blocks ...*types.Block) syncdl.BufferedBatch {
	t.Helper()
	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	batch := syncdl.BufferedBatch{}
	for _, block := range blocks {
		raw := rawOf(t, block)
		entry := syncdl.BufferedBlock{Raw: raw, Num: block.Number(), Hash: block.Hash()}
		ss.blockBuffer[entry.Num] = entry
		ss.bufferedHash[entry.Hash] = struct{}{}
		ss.blockPath[entry.Num] = entry.Hash
		ss.bufferedBytes += int64(len(raw))
		batch.Buffered = append(batch.Buffered, entry)
		if err := rawdb.WriteSyncStagedBlock(ss.chain.DB(), block); err != nil {
			ss.mu.Unlock()
			t.Fatal(err)
		}
	}
	ss.mu.Unlock()
	decode := syncdl.DecodeBufferedBatch(&batch)
	if decode.Err != nil {
		t.Fatal(decode.Err)
	}
	if _, err := ss.commitDecodedBufferedBatch(&batch, decode, time.Now()); err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestDecodedImportOwnershipDeduplicatesBeforeExecution(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	ownDecodedTestBatch(t, ss, block1, block2)
	if bc.CurrentBlock().Number() != 0 || bc.HasBlockInKhaosDB(block2.Hash()) {
		t.Fatal("fixture must stop before the decoded prefix reaches the chain")
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.blockBuffer) != 0 || ss.bufferedBytes != 0 {
		t.Fatal("decoded prefix should release raw buffer storage")
	}
	if !ss.hasBlockOrRequestLocked(block2.ID()) {
		t.Error("fetch scheduler forgot the decoded, not-yet-executed block")
	}
	if !(syncInventoryCandidateFactReader{service: ss}).HasBufferedInventoryBlock(block2.ID()) {
		t.Error("inventory filter forgot the decoded, not-yet-executed block")
	}
	plan := syncdl.PlanFetchedBlockBufferFromReader(block2.ID(), syncFetchedBlockBufferFactReader{service: ss})
	if plan.Action != syncdl.FetchedBlockBufferIgnore {
		t.Errorf("late requested duplicate action = %v, want ignore", plan.Action)
	}
}

func TestDecodedImportOwnershipAllowsDifferentHashAtSameHeight(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	block1 := stubBlock(1, bc.CurrentBlock().Hash())
	block2 := stubBlock(2, block1.Hash())
	ownDecodedTestBatch(t, ss, block1, block2)
	fork := stubBlock(2, tcommon.Hash{0xfa})
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.hasBlockOrRequestLocked(fork.ID()) {
		t.Error("import ownership must not reject a different hash at the same height")
	}
	plan := syncdl.PlanFetchedBlockBufferFromReader(fork.ID(), syncFetchedBlockBufferFactReader{service: ss})
	if plan.Action != syncdl.FetchedBlockBufferStage {
		t.Errorf("competing hash action = %v, want stage", plan.Action)
	}
}

func TestImportOwnershipSurvivesLateBodyUntilCommitBarrier(t *testing.T) {
	for _, depth := range []int{2, 4} {
		t.Run(fmt.Sprintf("depth-%d", depth), func(t *testing.T) {
			t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", fmt.Sprint(depth))
			bc := makeTestChain(t)
			bc.SetAsyncCommit(depth > 0)
			ss := NewSyncService(bc, nil)
			peer, closePeer := testPeer(t, "late-body")
			defer closePeer()
			ss.mu.Lock()
			ss.initSessionLocked(time.Now())
			last := seedBufferedSyncRange(t, ss, bc.CurrentBlock().Hash(), 1, 2)
			ss.targetHeadNum.Store(3)
			ps, _ := ss.addPeerStateLocked(peer)
			ps.chainRequested = true // Keep receipt refill from sending inventory.
			ss.mu.Unlock()

			hooked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			core.SetCommitFoldHookForTest(func(number uint64) error {
				if number == 1 {
					once.Do(func() { close(hooked); <-release })
				}
				return nil
			})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
				<-done
				core.SetCommitFoldHookForTest(nil)
			})
			go func() { ss.drainBufferedBlocks(); close(done) }()
			select {
			case <-hooked:
			case <-time.After(5 * time.Second):
				t.Fatal("commit barrier hook not reached")
			}
			if depth > 2 && !waitUntil(5*time.Second, func() bool {
				row, ok, err := rawdb.ReadStageProgressRow(bc.BufferedDB(), rawdb.StageExecution)
				return err == nil && ok && row.BlockNum == 2
			}) {
				t.Fatal("deep async execution did not advance beyond blocked commitment")
			}
			// Model a request already sent by another peer just before this drain
			// acquired its prefix, then arriving twice while canonical head lags.
			for i := 0; i < 2; i++ {
				ss.mu.Lock()
				if len(ss.importingHash) != 2 {
					ss.mu.Unlock()
					t.Fatal("entire uncommitted prefix must keep hash ownership")
				}
				markPendingLocked(ss, ps, last.ID())
				ss.mu.Unlock()
				if !ss.HandleRawBlock(peer, rawOf(t, last)) {
					t.Fatal("late requested duplicate was not consumed")
				}
				ss.mu.Lock()
				_, buffered := ss.blockBuffer[last.Number()]
				_, pending := ps.pending[last.Hash()]
				ss.mu.Unlock()
				if buffered || pending {
					t.Fatalf("late duplicate buffered=%v pending=%v, want ignored and settled", buffered, pending)
				}
			}
			close(release)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("drain did not settle after commitment was released")
			}
			ss.mu.Lock()
			owned, buffered := len(ss.importingHash), len(ss.blockBuffer)
			ss.mu.Unlock()
			if owned != 0 || buffered != 0 || bc.CurrentBlock().Hash() != last.Hash() {
				t.Fatalf("settled ownership=%d buffer=%d head=%d, want 0/0/2", owned, buffered, bc.CurrentBlock().Number())
			}
		})
	}
}

func TestImportOwnershipReleasedAfterCommitFailure(t *testing.T) {
	t.Setenv("GTRON_ASYNC_COMMIT_DEPTH", "2")
	bc := makeTestChain(t)
	bc.SetAsyncCommit(true)
	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	seedBufferedSyncRange(t, ss, bc.CurrentBlock().Hash(), 1, 3)
	ss.targetHeadNum.Store(3)
	ss.mu.Unlock()
	wantErr := errors.New("injected commitment failure")
	core.SetCommitFoldHookForTest(func(number uint64) error {
		if number == 2 {
			return wantErr
		}
		return nil
	})
	t.Cleanup(func() { core.SetCommitFoldHookForTest(nil) })
	ss.drainBufferedBlocks()
	if !ss.IsPaused() || bc.CurrentBlock().Number() != 1 {
		t.Fatalf("paused=%v head=%d, want failed import paused at committed prefix #1", ss.IsPaused(), bc.CurrentBlock().Number())
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.importingHash) != 0 {
		t.Fatal("failed drain retained hash ownership after its final barrier")
	}
}

func TestImportOwnershipSurvivesPeerResetAndPreservesRecovery(t *testing.T) {
	for _, disposition := range []string{"committed", "failed", "competing"} {
		t.Run(disposition, func(t *testing.T) {
			bc := makeTestChain(t)
			ss := NewSyncService(bc, nil)
			peer, closePeer := testPeer(t, "reset-owner")
			defer closePeer()
			ss.mu.Lock()
			ss.initSessionLocked(time.Now())
			ss.addPeerStateLocked(peer)
			ss.mu.Unlock()
			block := stubBlock(1, bc.CurrentBlock().Hash())
			ownDecodedTestBatch(t, ss, block)
			// Last-peer failover resets network maps, but the old drain is
			// still executing and must continue suppressing duplicate requests.
			ss.PeerDisconnected(peer)
			ss.mu.Lock()
			ss.initSessionLocked(time.Now())
			if !ss.hasBlockOrRequestLocked(block.ID()) {
				ss.mu.Unlock()
				t.Fatal("peer reset released hash ownership before the import barrier")
			}
			ss.mu.Unlock()

			restored := block
			if disposition == "competing" {
				pb := proto.Clone(block.Proto()).(*corepb.Block)
				pb.BlockHeader.RawData.Timestamp++
				restored = types.NewBlockFromPB(pb)
			}
			if err := rawdb.WriteSyncStagedBlock(bc.DB(), restored); err != nil {
				t.Fatal(err)
			}
			ss.mu.Lock()
			ss.restoreSyncStagedBodiesLocked(1, 1, false)
			ss.mu.Unlock()
			if disposition != "failed" {
				if err := bc.InsertBlock(block); err != nil {
					t.Fatal(err)
				}
			}
			ss.releaseImportingBlocks()
			ss.mu.Lock()
			defer ss.mu.Unlock()
			if len(ss.importingHash) != 0 {
				t.Fatal("settled old drain retained import ownership")
			}
			entry, buffered := ss.blockBuffer[1]
			if disposition == "committed" {
				if buffered || ss.bufferedBytes != 0 || len(ss.bufferedHash) != 0 || len(ss.blockPath) != 0 {
					t.Fatal("restored canonical duplicate was not released with matching buffer bookkeeping")
				}
			} else if !buffered || entry.Hash != restored.Hash() || ss.bufferedBytes != int64(len(entry.Raw)) {
				t.Fatal("old drain deleted the recovery body or a different hash restored by the new session")
			}
			row, ok, err := rawdb.ReadSyncStagedBlockRaw(bc.DB(), 1)
			if err != nil || !ok || row.Hash != restored.Hash() {
				t.Fatal("releasing in-memory import ownership must not delete durable recovery rows")
			}
		})
	}
}

func TestImportOwnershipReleasePreservesReorganizedBody(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	genesis := bc.CurrentBlock()
	a1 := stubBlock(1, genesis.Hash())
	a2 := stubBlock(2, a1.Hash())
	ownDecodedTestBatch(t, ss, a1, a2)
	if err := bc.InsertBlocks([]*types.Block{a1, a2}); err != nil {
		t.Fatal(err)
	}
	// A restart can restore A's body before another valid branch becomes
	// canonical. Cleanup must verify the hash, not just number <= head.
	ss.mu.Lock()
	ss.restoreSyncStagedBodiesLocked(1, 2, false)
	ss.mu.Unlock()
	parent := genesis
	for number := int64(1); number <= 3; number++ {
		pb := stubBlock(number, parent.Hash()).Proto()
		pb.BlockHeader.RawData.Timestamp++
		parent = types.NewBlockFromPB(pb)
		if err := bc.InsertBlock(parent); err != nil {
			t.Fatalf("insert competing block %d: %v", number, err)
		}
	}
	if bc.CurrentBlock().Hash() != parent.Hash() {
		t.Fatal("longer competing branch did not reorganize canonical state")
	}
	ss.releaseImportingBlocks()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.importingHash) != 0 {
		t.Fatal("completed drain retained old-branch hash ownership after reorg")
	}
	if restored, ok := ss.blockBuffer[1]; !ok || restored.Hash != a1.Hash() {
		t.Fatal("height-only cleanup deleted a noncanonical recovery body after reorg")
	}
}

func TestDecodedImportOwnershipDoesNotTransferRejectedBatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	seedBufferedSyncRange(t, ss, bc.CurrentBlock().Hash(), 1, 2)
	batch := ss.runStagedBodyDrainLocked(time.Now()).Batch
	ss.mu.Unlock()
	decode := syncdl.DecodeBufferedBatch(&batch)
	if decode.Err != nil || len(batch.Blocks) != 2 {
		t.Fatalf("decode result=%+v blocks=%d", decode, len(batch.Blocks))
	}
	// A peer/session reset can replace a selected entry while off-lock decode
	// is underway. Failure to verify any suffix must leave the whole prefix
	// untouched, with no importer acquiring partial ownership.
	ss.mu.Lock()
	changed := ss.blockBuffer[2]
	changed.Hash = tcommon.Hash{0xba}
	ss.blockBuffer[2] = changed
	ss.mu.Unlock()
	if _, err := ss.commitDecodedBufferedBatch(&batch, decode, time.Now()); err == nil {
		t.Fatal("changed buffered suffix should reject decode commit")
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.importingHash) != 0 || len(ss.blockBuffer) != 2 || ss.syncedTipNum != 0 {
		t.Fatal("rejected decode commit transferred a partial prefix")
	}
}
