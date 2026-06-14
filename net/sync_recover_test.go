package net

import (
	gnet "net"
	"testing"

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
