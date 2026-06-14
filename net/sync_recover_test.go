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
// drain-cursor stall: when the committed head lags the applied tip (the cursor),
// popBufferedSyncBatchLocked must pop the next contiguous run from the cursor,
// not from CurrentBlock+1 (which names an already-imported, deleted entry and
// would break the drain after one batch).
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
	// A stale entry at CurrentBlock+1 that a committed-head cursor would pop
	// first, plus the real next run past the drain cursor.
	stale := stubBlock(int64(head+1), bc.CurrentBlock().Hash())
	ss.blockBuffer[head+1] = bufferedSyncBlock{hash: stale.Hash(), num: head + 1}
	ss.bufferedHash[stale.Hash()] = struct{}{}
	for n := head + 51; n <= head+53; n++ {
		b := stubBlock(int64(n), tcommon.Hash{byte(n), byte(n >> 8)})
		ss.blockBuffer[n] = bufferedSyncBlock{hash: b.Hash(), num: n}
		ss.bufferedHash[b.Hash()] = struct{}{}
	}

	batch := ss.popBufferedSyncBatchLocked(time.Now())

	if len(batch.buffered) != 3 {
		t.Fatalf("popped %d blocks, want 3 from the drain cursor", len(batch.buffered))
	}
	if batch.buffered[0].num != head+51 {
		t.Fatalf("first popped = #%d, want #%d (cursor+1, not CurrentBlock+1)", batch.buffered[0].num, head+51)
	}
	if _, ok := ss.blockBuffer[head+1]; !ok {
		t.Fatal("stale entry at CurrentBlock+1 was popped — cursor ignored the async-commit lag")
	}
	if ss.syncedTipNum != head+53 {
		t.Fatalf("drain cursor = %d, want %d (advanced past the popped run)", ss.syncedTipNum, head+53)
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
