package net

import (
	gnet "net"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/p2p"
)

// TestRecoverStalledFetchReRequestsInventory is the regression for the
// async-commit depth>2 lost wakeup: a syncing session whose peers have drained
// their fetch lists and parked on "waiting for local head" must re-request the
// next inventory window when the watchdog calls RecoverStalledFetch (the head
// has caught up by then). Before the fix the watchdog short-circuited on
// IsSyncing() and nothing re-armed the scheduler, so the session wedged forever
// with the peers still connected.
func TestRecoverStalledFetchReRequestsInventory(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "stalled-peer", false, nil)

	// A session marked syncing whose only peer is idle: it drained its fetch
	// list and is neither requesting (chainRequested=false) nor in flight —
	// the state the depth>2 lost wakeup leaves behind once the committed head
	// catches up but no event re-evaluates the scheduler.
	ss.mu.Lock()
	ss.syncing = true
	ps, added := ss.addPeerStateLocked(peer)
	ss.mu.Unlock()
	if !added || ps == nil {
		t.Fatal("failed to add peer to sync session")
	}

	ss.RecoverStalledFetch()

	ss.mu.Lock()
	rearmed := ps.chainRequested
	ss.mu.Unlock()
	if !rearmed {
		t.Fatal("RecoverStalledFetch did not re-request inventory for a parked peer")
	}
}

// TestRecoverStalledFetchNoopWhenNotSyncing confirms the watchdog re-kick is a
// no-op outside an active session: it must never start one.
func TestRecoverStalledFetchNoopWhenNotSyncing(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	ss.RecoverStalledFetch()

	if ss.IsSyncing() {
		t.Fatal("RecoverStalledFetch must not start a sync session when idle")
	}
}

// TestFillFetchSlotsRearmsOnWaitingForLocalHead covers the depth>2 lost-wakeup
// root cause: a peer that drained its fetch list while its last inventory tip is
// still ahead of our (commit-worker-lagged) head must arm a re-check timer, so
// it re-requests the next window once the head catches up instead of idling
// until the coarse watchdog poll.
func TestFillFetchSlotsRearmsOnWaitingForLocalHead(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "waiting-peer", false, nil)

	ss.mu.Lock()
	ss.syncing = true
	ps, _ := ss.addPeerStateLocked(peer)
	// Drained fetch list, last inventory tip ahead of our head: the peer must
	// wait for the local head to catch up before re-requesting.
	ps.lastInventoryNum = bc.CurrentBlock().Number() + 5
	out := ss.fillFetchSlotsLocked(time.Now())
	rearmed := ps.fetchDelayTimer != nil
	requested := ps.chainRequested
	// Stop the timer so it can't fire after the test.
	ss.syncing = false
	if ps.fetchDelayTimer != nil {
		ps.fetchDelayTimer.Stop()
	}
	ss.mu.Unlock()

	if len(out) != 0 {
		t.Fatalf("parked peer produced %d outbound requests, want 0", len(out))
	}
	if requested {
		t.Fatal("parked peer re-requested inventory while head was behind the inventory tip")
	}
	if !rearmed {
		t.Fatal("parked peer armed no re-check timer — recovery would wait for the watchdog")
	}
}

// TestPopBufferedSyncBatchUsesDrainCursor is the regression for the depth>2
// drain-cursor stall and stale-buffer leak: when the committed head lags the
// applied tip, the pop must discard stale entries behind the cursor (including
// every accounting index) and still drain the next contiguous run.
func TestPopBufferedSyncBatchUsesDrainCursor(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.initSessionLocked(time.Now())
	head := bc.CurrentBlock().Number()
	// Simulate async-commit depth>2 lag: 50 blocks applied (cursor at head+50)
	// while the committed head is still `head`.
	ss.syncedTipNum = head + 50
	// A stale entry at CurrentBlock+1 that can never be reached from the applied
	// cursor, plus the real next run past that cursor.
	stale := stubBlock(int64(head+1), bc.CurrentBlock().Hash())
	staleRaw := []byte("stale raw block bytes")
	ss.blockBuffer[head+1] = bufferedSyncBlock{raw: staleRaw, hash: stale.Hash(), num: head + 1}
	ss.bufferedHash[stale.Hash()] = struct{}{}
	ss.blockPath[head+1] = stale.Hash()
	ss.bufferedBytes = int64(len(staleRaw))
	for n := head + 51; n <= head+53; n++ {
		b := stubBlock(int64(n), tcommon.Hash{byte(n), byte(n >> 8)})
		ss.blockBuffer[n] = bufferedSyncBlock{hash: b.Hash(), num: n}
		ss.bufferedHash[b.Hash()] = struct{}{}
		ss.blockPath[n] = b.Hash()
	}

	batch := ss.popBufferedSyncBatchLocked(time.Now())

	if len(batch.buffered) != 3 {
		t.Fatalf("popped %d blocks, want 3 from the drain cursor", len(batch.buffered))
	}
	if batch.buffered[0].num != head+51 {
		t.Fatalf("first popped = #%d, want #%d (cursor+1, not CurrentBlock+1)", batch.buffered[0].num, head+51)
	}
	if _, ok := ss.blockBuffer[head+1]; ok {
		t.Fatal("stale entry behind the applied cursor was retained")
	}
	if _, ok := ss.bufferedHash[stale.Hash()]; ok {
		t.Fatal("stale bufferedHash entry was retained")
	}
	if _, ok := ss.blockPath[head+1]; ok {
		t.Fatal("stale blockPath reservation was retained")
	}
	if ss.bufferedBytes != 0 {
		t.Fatalf("bufferedBytes=%d after stale cleanup and contiguous pop, want 0", ss.bufferedBytes)
	}
	if ss.syncedTipNum != head+53 {
		t.Fatalf("drain cursor = %d, want %d (advanced past the popped run)", ss.syncedTipNum, head+53)
	}
}

// TestChainInventorySkipsAppliedAheadOfCommitted verifies request admission
// during async commit lag. Heights owned by the active insert session must not
// be queued or reserved again merely because CurrentBlock has not published
// them; the first height after the effective tip remains fetchable.
func TestChainInventorySkipsAppliedAheadOfCommitted(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	peer, closePeer := testPeer(t, "applied-inventory")
	defer closePeer()

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ss.syncedTipNum = 5 // CurrentBlock is still genesis (#0).
	ss.mu.Unlock()

	ss.HandleChainInventory(peer, testChainInventoryPayload(t, 1, 6, 0))

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ps.pending) != 1 {
		t.Fatalf("pending=%v, want only the block after the effective tip", ps.pending)
	}
	for _, num := range ps.pending {
		if num != 6 {
			t.Fatalf("requested stale block #%d while effective tip is #5", num)
		}
	}
	for num := uint64(1); num <= 5; num++ {
		if _, ok := ss.blockPath[num]; ok {
			t.Fatalf("stale inventory reserved blockPath[%d]", num)
		}
	}
}

// TestHandleBlockDropsAppliedRawWithoutBuffering covers requests that were
// already in flight when the applied cursor advanced. Their responses still
// complete peer accounting, but must not copy raw bytes or recreate any buffer
// index behind the cursor, even when that height range was already pruned.
func TestHandleBlockDropsAppliedRawWithoutBuffering(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	peer, closePeer := testPeer(t, "applied-response")
	defer closePeer()

	old := stubBlock(3, tcommon.Hash{0xaa})
	oldRaw := make([]byte, 1<<20)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ss.syncedTipNum = 5 // Simulate applied #1..#5 with CurrentBlock still #0.
	ss.bufferPrunedTipNum = 5
	ss.targetHeadNum = 6
	ps.chainRequested = true // Keep the synthetic session alive for inspection.
	markPendingLocked(ss, ps, old.ID())
	ss.mu.Unlock()

	if !ss.HandleBlock(peer, old, oldRaw) {
		t.Fatal("HandleBlock should consume the stale requested response")
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.blockBuffer) != 0 || len(ss.bufferedHash) != 0 {
		t.Fatalf("stale raw state retained: buffer=%d hashes=%d", len(ss.blockBuffer), len(ss.bufferedHash))
	}
	if ss.bufferedBytes != 0 {
		t.Fatalf("bufferedBytes=%d after stale response, want 0", ss.bufferedBytes)
	}
	if _, ok := ss.blockPath[old.Number()]; ok {
		t.Fatal("stale response retained its blockPath reservation")
	}
	if _, ok := ss.requested[old.Hash()]; ok {
		t.Fatal("stale response retained global requested state")
	}
	if ps.inflight != 0 || len(ps.pending) != 0 || len(ps.pendingIDs) != 0 {
		t.Fatalf("peer request accounting not completed: inflight=%d pending=%d ids=%d",
			ps.inflight, len(ps.pending), len(ps.pendingIDs))
	}
}

// BenchmarkHandleBlockDropsAppliedRaw measures the leak-sensitive receive path.
// The 1 MiB raw payload is intentionally much larger than a typical block: its
// size must not affect allocations once the height is behind the applied tip.
func BenchmarkHandleBlockDropsAppliedRaw(b *testing.B) {
	bc := makeTestChain(b)
	ss := NewSyncService(bc, nil)
	peer, closePeer := testPeer(b, "applied-response-bench")
	defer closePeer()

	block := stubBlock(1, bc.CurrentBlock().Hash())
	bid := block.ID()
	raw := make([]byte, 1<<20)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ss.syncedTipNum = 1
	ss.bufferPrunedTipNum = 1
	ss.targetHeadNum = 2
	ps.chainRequested = true
	ss.mu.Unlock()

	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ss.mu.Lock()
		ss.blockPath[bid.Num] = bid.Hash
		ps.inflight = 1
		ps.pending[bid.Hash] = bid.Num
		ps.pendingIDs[bid.Hash] = bid
		ps.requestedHashes[bid.Hash] = bid.Num
		ss.requested[bid.Hash] = peer.ID()
		ss.mu.Unlock()

		ss.HandleBlock(peer, block, raw)
	}
}

// TestPopBufferedSyncBatchTracksCommittedHeadWhenNotAhead confirms the cursor
// degrades to CurrentBlock+1 when async commit is off (the cursor is not ahead),
// so that path is unchanged.
func TestPopBufferedSyncBatchTracksCommittedHeadWhenNotAhead(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.initSessionLocked(time.Now()) // syncedTipNum == CurrentBlock after init
	head := bc.CurrentBlock().Number()
	b1 := stubBlock(int64(head+1), bc.CurrentBlock().Hash())
	ss.blockBuffer[head+1] = bufferedSyncBlock{hash: b1.Hash(), num: head + 1}
	ss.bufferedHash[b1.Hash()] = struct{}{}

	batch := ss.popBufferedSyncBatchLocked(time.Now())

	if len(batch.buffered) != 1 || batch.buffered[0].num != head+1 {
		t.Fatalf("async-off: expected to pop #%d, got %d blocks", head+1, len(batch.buffered))
	}
}
