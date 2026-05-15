package net

import (
	gnet "net"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// nopHandler is the minimum p2p.Handler that lets a Peer.Start() / Stop()
// roundtrip without nil-derefing in peer.disconnect(). Tests use it when
// they need writeLoop to actually flush frames to the pipe.
type nopHandler struct{}

func (nopHandler) OnPeerConnected(*p2p.Peer)            {}
func (nopHandler) OnPeerDisconnected(*p2p.Peer)         {}
func (nopHandler) OnMessage(*p2p.Peer, byte, []byte)    {}

// stubBlock builds a minimal block at the given number/parent. The block
// won't actually insert (parent hash won't match a real chain head) — the
// stall tests don't care about insertion, only about the SyncService
// bookkeeping that happens before InsertBlock.
func stubBlock(num int64, parent tcommon.Hash) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     num,
				Timestamp:  num * 3000,
				ParentHash: parent[:],
			},
			WitnessSignature: make([]byte, 65),
		},
	})
}

// TestPartialBatchRearmsFetchTimer is the regression for the
// inflight>0-but-timer-stopped stall: when a peer delivers part of a
// batch and then goes silent, the fetch timer must re-arm so
// onFetchTimeout eventually fires and the sync state machine recovers
// via tryFindSyncPeer. Before the fix HandleBlock unconditionally
// stopped the timer without re-arming, leaving inflight>0 forever.
func TestPartialBatchRearmsFetchTimer(t *testing.T) {
	old := syncFetchTimeout
	syncFetchTimeout = 50 * time.Millisecond
	defer func() { syncFetchTimeout = old }()

	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "partial-peer", false, nil)

	// Simulate: a batch of 2 blocks was requested. Set up the state the
	// way fetchNextBatch would have left it.
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.inflight = 2
	ss.armFetchTimer()
	ss.mu.Unlock()

	// Peer delivers only block 1 of the batch and then goes silent. The
	// HandleBlock path used to stop the timer without re-arming — leaving
	// inflight=1 and no timer.
	parent := bc.CurrentBlock().Hash()
	consumed := ss.HandleBlock(peer, stubBlock(1, parent))
	if !consumed {
		t.Fatal("HandleBlock should have consumed the block while syncing")
	}

	// After one block we still have inflight=1. The timer must be re-armed
	// so the stall is detectable.
	ss.mu.Lock()
	infl := ss.inflight
	timer := ss.fetchTimer
	ss.mu.Unlock()
	if infl != 1 {
		t.Fatalf("inflight after 1/2 blocks: got %d, want 1", infl)
	}
	if timer == nil {
		t.Fatal("partial batch left fetchTimer nil — peer-silent stall would never recover")
	}

	// Wait past the timeout. onFetchTimeout should fire and clear syncing.
	time.Sleep(200 * time.Millisecond)
	if ss.IsSyncing() {
		t.Fatal("sync should have aborted after fetch timeout on partial batch")
	}
}

// TestLastBlockInsertFailureRecovers checks the second stall shape: when
// the final block of a batch fails to insert, the original code returned
// early before the batchDone check, leaving syncing=true forever (and the
// watchdog short-circuits on IsSyncing()). The fix runs the batchDone path
// regardless of insert outcome — fetchNextBatch with an empty fetchList
// sends SYNC_BLOCK_CHAIN, polling for the missing range from our true
// canonical head.
func TestLastBlockInsertFailureRecovers(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	// Start a real peer so peer.Send actually flushes to the pipe. Without
	// Start the message would sit in the write buffer and never reach c2.
	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "fail-insert-peer", false, nopHandler{})
	peer.Start()
	defer peer.Stop()

	// Drain outbound frames on c2 so the writeLoop doesn't block. Just
	// signal on the channel that at least one frame arrived — we don't
	// need to parse content.
	gotFrame := make(chan struct{}, 8)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := c2.Read(buf)
			if err != nil {
				return
			}
			if n > 0 {
				select {
				case gotFrame <- struct{}{}:
				default:
				}
			}
		}
	}()

	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.inflight = 1
	ss.fetchList = nil // empty → fetchNextBatch will sendSyncBlockChain
	ss.mu.Unlock()

	// A bogus block — wrong parent hash so InsertBlock fails (KhaosDB
	// rejects unknown parent).
	badBlock := stubBlock(99, tcommon.Hash{1, 2, 3})
	consumed := ss.HandleBlock(peer, badBlock)
	if !consumed {
		t.Fatal("HandleBlock should have consumed the block")
	}

	// fetchNextBatch with an empty fetchList writes a SYNC_BLOCK_CHAIN
	// frame. If HandleBlock returned early on insert failure (pre-fix
	// behaviour) the pipe stays silent and the select hits its deadline.
	select {
	case <-gotFrame:
		// Good: recovery cycle started.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HandleBlock did not trigger fetchNextBatch on the batch's last-block insert failure (stall shape)")
	}
}
