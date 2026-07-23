package net

import (
	"errors"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestSyncServiceStatusSnapshot(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	pauseErr := errors.New("state root mismatch")

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.targetHeadNum = 100
	ss.syncedTipNum = 5
	ss.inflight = 3
	ss.bufferedBytes = 4096
	ss.blockBuffer[6] = bufferedSyncBlock{}
	ss.requested[tcommon.Hash{1}] = "peer-a"
	ss.retryList = []types.BlockID{{Num: 7}, {Num: 8}}
	ss.retainedDecodedBlocks = 2
	ss.retainedDecodedBytes = 2048
	ss.peers["peer-a"] = &syncPeerState{}
	ss.mu.Unlock()
	ss.pause.Enter(6, pauseErr)

	status := ss.Status()
	if !status.Active || !status.Paused || status.PauseBlock != 6 || !errors.Is(status.PauseError, pauseErr) || status.PauseTime.IsZero() {
		t.Fatalf("pause status = %+v", status)
	}
	if status.TargetHead != 100 || status.AppliedTip != 5 || status.Remaining != 100 {
		t.Fatalf("progress status = %+v", status)
	}
	if status.SyncPeerCount != 1 || status.Inflight != 3 || status.BufferedBlocks != 1 || status.BufferedBytes != 4096 || status.RequestedBlocks != 1 || status.RetryBlocks != 2 {
		t.Fatalf("queue status = %+v", status)
	}
	if status.RetainedDecodedBlocks != 2 || status.RetainedDecodedBytes != 2048 {
		t.Fatalf("retained decode status = %+v", status)
	}
}

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
