package net

import (
	gnet "net"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// nopHandler is the minimum p2p.Handler that lets a Peer.Start() / Stop()
// roundtrip without nil-derefing in peer.disconnect(). Tests use it when
// they need writeLoop to actually flush frames to the pipe.
type nopHandler struct{}

func (nopHandler) OnPeerConnected(*p2p.Peer)         {}
func (nopHandler) OnPeerDisconnected(*p2p.Peer)      {}
func (nopHandler) OnMessage(*p2p.Peer, byte, []byte) {}

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
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	ss.fetchTimeout = 50 * time.Millisecond

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "partial-peer", false, nil)

	parent := bc.CurrentBlock().Hash()
	first := stubBlock(1, parent)
	second := stubBlock(2, first.Hash())

	// Simulate: a batch of 2 blocks was requested. Set up the state the
	// way fetchNextBatch would have left it.
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.inflight = 2
	ss.pending = map[tcommon.Hash]uint64{
		first.Hash():  first.Number(),
		second.Hash(): second.Number(),
	}
	ss.armFetchTimer()
	ss.mu.Unlock()

	// Peer delivers only block 1 of the batch and then goes silent. The
	// HandleBlock path used to stop the timer without re-arming — leaving
	// inflight=1 and no timer.
	consumed := ss.HandleBlock(peer, first, nil)
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

func TestUnrequestedSyncPeerBlockDoesNotPauseOrDrainBatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "unexpected-sync-peer", false, nil)

	parent := bc.CurrentBlock().Hash()
	requested := stubBlock(1, parent)
	unrequested := stubBlock(99, tcommon.Hash{1, 2, 3})

	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.inflight = 1
	ss.pending = map[tcommon.Hash]uint64{requested.Hash(): requested.Number()}
	ss.mu.Unlock()

	consumed := ss.HandleBlock(peer, unrequested, nil)
	if !consumed {
		t.Fatal("unexpected block from sync peer should be consumed and dropped")
	}
	if ss.IsPaused() {
		t.Fatal("unrequested block should not trigger sticky pause")
	}
	if !ss.IsSyncing() {
		t.Fatal("unrequested block should not reset active sync")
	}
	ss.mu.Lock()
	inflight := ss.inflight
	_, stillPending := ss.pending[requested.Hash()]
	ss.mu.Unlock()
	if inflight != 1 || !stillPending {
		t.Fatalf("unrequested block drained requested batch: inflight=%d stillPending=%v", inflight, stillPending)
	}
	if got := bc.CurrentBlock().Number(); got != 0 {
		t.Fatalf("unrequested block was inserted, head=%d", got)
	}
}

// TestChainInventorySkipsKhaosDBOrphans verifies HandleChainInventory's
// dedup filter drops block IDs we already buffer as orphans in KhaosDB.
// Without the filter step a subsequent FETCH_INV_DATA would re-request
// blocks java-tron has already sent us, triggering its syncBlockIdCache
// check → BAD_PROTOCOL disconnect → loss of every peer. The orphans
// themselves do not need refetching: once the gap parent arrives KhaosDB
// promoteUnlinked cascades them into miniStore and InsertBlock's
// switchFork applies the stretch in topological order.
func TestChainInventorySkipsKhaosDBOrphans(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	// Plant orphans into KhaosDB.miniUnlinkedStore by InsertBlock'ing
	// blocks with bogus parents. KhaosDB.Push sees the missing parent and
	// stashes them in the unlinked store; InsertBlock returns
	// ErrUnlinkedBlock which we ignore.
	const orphanCount = 11
	orphanHashes := make([]tcommon.Hash, 0, orphanCount)
	for i := 0; i < orphanCount; i++ {
		b := stubBlock(int64(200+i), tcommon.Hash{0xde, 0xad, byte(i)})
		_ = bc.InsertBlock(b)
		orphanHashes = append(orphanHashes, b.Hash())
	}
	for _, h := range orphanHashes {
		if !bc.HasBlockInKhaosDB(h) {
			t.Fatalf("orphan %x missing from KhaosDB after InsertBlock", h)
		}
	}

	// Wire a peer the SyncService will accept as syncPeer, draining
	// outbound frames so fetchNextBatch's FETCH_INV_DATA write doesn't
	// block on the pipe.
	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "inv-orphan-peer", false, nopHandler{})
	peer.Start()
	defer peer.Stop()
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := c2.Read(buf); err != nil {
				return
			}
		}
	}()
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.mu.Unlock()

	// Build an inventory with exactly maxFetchBatch (100) gap entries
	// followed by the orphan IDs. After the filter drops the orphans,
	// fetchList has 100 gap entries → fetchNextBatch pops all of them in a
	// single batch → fetchList ends empty. Without the fix the orphans
	// stay in fetchList (101..111 entries total; first 100 popped, 11 left
	// behind) — that's the failing-assertion shape.
	ids := make([]*corepb.ChainInventory_BlockId, 0, maxFetchBatch+orphanCount)
	for n := int64(101); n <= 100+maxFetchBatch; n++ {
		gapHash := tcommon.Hash{0x9a, byte(n), byte(n >> 8)}
		ids = append(ids, &corepb.ChainInventory_BlockId{
			Hash:   gapHash[:],
			Number: n,
		})
	}
	for i, h := range orphanHashes {
		ids = append(ids, &corepb.ChainInventory_BlockId{
			Hash:   h[:],
			Number: int64(200 + i),
		})
	}
	payload, err := proto.Marshal(&corepb.ChainInventory{Ids: ids, RemainNum: 1000})
	if err != nil {
		t.Fatalf("marshal inv: %v", err)
	}

	ss.HandleChainInventory(peer, payload)

	ss.mu.Lock()
	leaked := append([]types.BlockID(nil), ss.fetchList...)
	ss.mu.Unlock()
	if len(leaked) != 0 {
		nums := make([]uint64, 0, len(leaked))
		for _, b := range leaked {
			nums = append(nums, b.Num)
		}
		t.Fatalf("orphans leaked into fetchList after HandleChainInventory: %v (would trigger BAD_PROTOCOL on next FETCH_INV_DATA)", nums)
	}
}

func TestChainInventoryFetchesStaleFutureDiskBlocks(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	defer ss.Stop()

	stale := stubBlock(1, bc.CurrentBlock().Hash())
	if err := rawdb.WriteBlock(bc.DB(), stale); err != nil {
		t.Fatalf("write stale block body: %v", err)
	}
	if got := bc.GetBlockByNumber(1); got != nil {
		t.Fatal("stale block body above current head should not be canonical")
	}

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "stale-future-peer", false, nopHandler{})
	peer.Start()
	defer peer.Stop()
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := c2.Read(buf); err != nil {
				return
			}
		}
	}()
	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.mu.Unlock()

	payload, err := proto.Marshal(&corepb.ChainInventory{
		Ids: []*corepb.ChainInventory_BlockId{{
			Hash:   stale.Hash().Bytes(),
			Number: int64(stale.Number()),
		}},
	})
	if err != nil {
		t.Fatalf("marshal inv: %v", err)
	}

	ss.HandleChainInventory(peer, payload)

	ss.mu.Lock()
	inflight := ss.inflight
	ss.mu.Unlock()
	if inflight != 1 {
		t.Fatalf("stale future disk block was not requested: inflight=%d, want 1", inflight)
	}
}

// TestInsertFailurePausesSync covers the failed-insert path: when InsertBlock
// returns an error, sync must stop sticky (paused) without disconnecting the
// peer. The previous "retry via fetchNextBatch" recovery path was removed —
// retrying the same block from the same/another peer in gtron almost always
// rediscovers the same bug locally (gtron is a re-impl; our state is the more
// likely culprit than a malicious peer), so retrying burns peer budget for
// no recovery. Pausing keeps the peer connection so an operator can diagnose.
// Clears only on process restart.
func TestInsertFailurePausesSync(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "fail-insert-peer", false, nopHandler{})
	peer.Start()
	defer peer.Stop()

	// Drain outbound frames so peer.writeLoop doesn't block. We assert
	// later that NO frame is sent on the failure path.
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

	// Bogus next block — wrong parent hash so InsertBlock fails when the
	// ordered sync buffer attempts to apply it.
	badBlock := stubBlock(1, tcommon.Hash{1, 2, 3})

	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.inflight = 1
	ss.fetchList = nil
	ss.pending = map[tcommon.Hash]uint64{badBlock.Hash(): badBlock.Number()}
	ss.mu.Unlock()

	consumed := ss.HandleBlock(peer, badBlock, nil)
	if !consumed {
		t.Fatal("HandleBlock should have consumed the block while syncing")
	}
	ss.waitForDrain()

	// Sync must be paused, not syncing, and not have sent any outbound
	// frame (no SYNC_BLOCK_CHAIN retry, no FETCH_INV_DATA).
	if !ss.IsPaused() {
		t.Fatal("InsertBlock failure should have paused sync")
	}
	if ss.IsSyncing() {
		t.Fatal("InsertBlock failure should have cleared syncing")
	}
	select {
	case <-gotFrame:
		t.Fatal("paused sync must not send any outbound frame")
	case <-time.After(200 * time.Millisecond):
	}

	paused, atNum, _, err := ss.PausedStatus()
	if !paused || atNum != 1 || err == nil {
		t.Fatalf("PausedStatus mismatch: paused=%v atNum=%d err=%v", paused, atNum, err)
	}

	// Once paused, StartSync must be a no-op even if a fresh sync candidate
	// shows up.
	ss.StartSync(peer)
	if ss.IsSyncing() {
		t.Fatal("StartSync should short-circuit while paused")
	}
}
