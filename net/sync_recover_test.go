package net

import (
	gnet "net"
	"testing"
	"time"

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
