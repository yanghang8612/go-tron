package net

import (
	gnet "net"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
)

// TestPeerDisconnectedAbortsSyncState verifies that PeerDisconnected resets
// all sync fields when the dying peer is the active syncPeer.
func TestPeerDisconnectedAbortsSyncState(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	// NewPeer with nil handler is safe here: we never Start() the peer,
	// so no goroutines run and the handler is never called.
	peer := p2p.NewPeer(c1, "test-peer", false, nil)

	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.mu.Unlock()

	ss.PeerDisconnected(peer)

	ss.mu.Lock()
	syncing := ss.syncing
	syncPeer := ss.syncPeer
	ss.mu.Unlock()

	if syncing {
		t.Fatal("expected syncing=false after peer disconnect")
	}
	if syncPeer != nil {
		t.Fatal("expected syncPeer=nil after peer disconnect")
	}
}

// TestPeerDisconnectedIgnoresNonSyncPeer checks that disconnecting an
// unrelated peer has no effect on an active sync.
func TestPeerDisconnectedIgnoresNonSyncPeer(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	syncPeer := p2p.NewPeer(c1, "sync-peer", false, nil)

	d1, d2 := gnet.Pipe()
	defer d1.Close()
	defer d2.Close()
	otherPeer := p2p.NewPeer(d1, "other-peer", false, nil)

	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = syncPeer
	ss.mu.Unlock()

	ss.PeerDisconnected(otherPeer) // should be a no-op

	if !ss.IsSyncing() {
		t.Fatal("sync should still be active after unrelated peer disconnects")
	}
}

// TestFetchTimeoutAbortsSyncState verifies that when the fetch timer fires
// (simulated with a very short timeout) the sync state is cleared.
func TestFetchTimeoutAbortsSyncState(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	ss.fetchTimeout = 50 * time.Millisecond

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	peer := p2p.NewPeer(c1, "stalled-peer", false, nil)

	ss.mu.Lock()
	ss.syncing = true
	ss.syncPeer = peer
	ss.armFetchTimer() // starts 50ms countdown
	ss.mu.Unlock()

	// Wait for timeout to fire
	time.Sleep(200 * time.Millisecond)

	if ss.IsSyncing() {
		t.Fatal("expected sync to be aborted after fetch timeout")
	}
}

func TestFetchTimeoutWithStalledRetriesRestartsSession(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	c1, c2 := gnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	stale := p2p.NewPeer(c1, "stale-peer", false, nil)

	d1, d2 := gnet.Pipe()
	defer d1.Close()
	defer d2.Close()
	remaining := p2p.NewPeer(d1, "done-peer", false, nil)

	missing := types.BlockID{Hash: tcommon.Hash{0xaa, 0xbb}, Num: 50}

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	staleState, _ := ss.addPeerStateLocked(stale)
	remainingState, _ := ss.addPeerStateLocked(remaining)
	staleState.inflight = 1
	staleState.fetchSeq = 7
	staleState.pending = map[tcommon.Hash]uint64{missing.Hash: missing.Num}
	staleState.pendingIDs = map[tcommon.Hash]types.BlockID{missing.Hash: missing}
	staleState.requestedHashes[missing.Hash] = missing.Num
	ss.requested[missing.Hash] = stale.ID()
	remainingState.done = true
	ss.mu.Unlock()

	ss.onFetchTimeout(7, stale.ID())

	if ss.IsSyncing() {
		t.Fatal("stalled retry list should reset the sync session instead of leaving syncing=true")
	}
}

// TestPeerRemovalDoesNotRetryAppliedRequest covers an in-flight response that
// becomes obsolete while CurrentBlock still lags. Failover must neither retry
// it nor retain its path reservation; later peers start strictly after the
// effective sync tip.
func TestPeerRemovalDoesNotRetryAppliedRequest(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	stale, closeStale := testPeer(t, "applied-stale-peer")
	defer closeStale()
	remaining, closeRemaining := testPeer(t, "applied-remaining-peer")
	defer closeRemaining()

	applied := runaheadBid(3)
	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	staleState, _ := ss.addPeerStateLocked(stale)
	remainingState, _ := ss.addPeerStateLocked(remaining)
	remainingState.chainRequested = true
	markPendingLocked(ss, staleState, applied)
	ss.syncedTipNum = 5
	ss.bufferPrunedTipNum = 5
	ss.removePeerStateLocked(stale.ID(), true)
	_, pathKept := ss.blockPath[applied.Num]
	_, requested := ss.requested[applied.Hash]
	retries := len(ss.retryList)
	retryList := append([]types.BlockID(nil), ss.retryList...)
	ss.mu.Unlock()

	if retries != 0 {
		t.Fatalf("applied request was queued for retry: %v", retryList)
	}
	if pathKept {
		t.Fatal("applied request retained its blockPath reservation after peer removal")
	}
	if requested {
		t.Fatal("applied request retained global requested state after peer removal")
	}
}

// TestSyncPeerDisconnectFailover verifies that when the active sync peer
// disconnects, the sync service aborts the stalled sync and immediately
// retries with an alternate peer that has the same blocks.
func TestSyncPeerDisconnectFailover(t *testing.T) {
	// A and C both have 20 blocks; B starts from genesis.
	bcA := makeChainWithBlocks(t, 20)
	bcB := makeTestChain(t)
	bcC := makeChainWithBlocks(t, 20)

	poolA := txpool.New()
	poolB := txpool.New()
	poolC := txpool.New()

	bcastA := NewBroadcastService(nil)
	bcastB := NewBroadcastService(nil)
	bcastC := NewBroadcastService(nil)

	hA := NewTronHandler(bcA, poolA, bcastA)
	hB := NewTronHandler(bcB, poolB, bcastB)
	hC := NewTronHandler(bcC, poolC, bcastC)

	syncA := NewSyncService(bcA, hA)
	syncB := NewSyncService(bcB, hB)
	syncC := NewSyncService(bcC, hC)
	hA.SetSyncService(syncA)
	hB.SetSyncService(syncB)
	hC.SetSyncService(syncC)

	srvA := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, hA)
	srvB := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, hB)
	srvC := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, hC)
	hA.SetServer(srvA)
	hB.SetServer(srvB)
	hC.SetServer(srvC)
	bcastA.SetPeersFunc(hA.HandshakedPeers)
	bcastB.SetPeersFunc(hB.HandshakedPeers)
	bcastC.SetPeersFunc(hC.HandshakedPeers)

	srvA.Start()
	srvB.Start()
	defer srvB.Stop()
	srvC.Start()
	defer srvC.Stop()

	// B connects to A and C
	srvB.AddPeer(srvA.ListenAddr())
	srvB.AddPeer(srvC.ListenAddr())

	// Wait for both handshakes
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hB.HandshakedPeerCount() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if hB.HandshakedPeerCount() < 2 {
		t.Fatalf("B should have 2 handshaked peers, got %d", hB.HandshakedPeerCount())
	}

	// Wait for sync to start (B's syncPeer should be set)
	time.Sleep(100 * time.Millisecond)

	// Kill A — simulates the active sync peer disconnecting mid-sync.
	// No defer here: we stop A exactly once explicitly.
	srvA.Stop()

	// B should failover to C and complete sync within 5 seconds
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bcB.CurrentBlock().Number() >= 20 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if bcB.CurrentBlock().Number() != 20 {
		t.Fatalf("B should have synced to block #20 after failover, got #%d", bcB.CurrentBlock().Number())
	}
}

// TestSyncFetchTimeoutFailover verifies that when the sync peer stops
// responding (simulated via a very short timeout), sync aborts and retries.
func TestSyncFetchTimeoutFailover(t *testing.T) {
	// A has 20 blocks; B has 0. C also has 20 blocks as a fallback.
	bcA := makeChainWithBlocks(t, 20)
	bcB := makeTestChain(t)
	bcC := makeChainWithBlocks(t, 20)

	poolA := txpool.New()
	poolB := txpool.New()
	poolC := txpool.New()

	bcastA := NewBroadcastService(nil)
	bcastB := NewBroadcastService(nil)
	bcastC := NewBroadcastService(nil)

	hA := NewTronHandler(bcA, poolA, bcastA)
	hB := NewTronHandler(bcB, poolB, bcastB)
	hC := NewTronHandler(bcC, poolC, bcastC)

	syncA := NewSyncService(bcA, hA)
	syncB := NewSyncService(bcB, hB)
	syncC := NewSyncService(bcC, hC)
	// Override the fetch timeout so the test doesn't take 30 seconds. Set
	// per-instance before any peer connects so the fetch-timer goroutine
	// never races this assignment.
	syncA.fetchTimeout = 300 * time.Millisecond
	syncB.fetchTimeout = 300 * time.Millisecond
	syncC.fetchTimeout = 300 * time.Millisecond
	hA.SetSyncService(syncA)
	hB.SetSyncService(syncB)
	hC.SetSyncService(syncC)

	srvA := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, hA)
	srvB := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, hB)
	srvC := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, hC)
	hA.SetServer(srvA)
	hB.SetServer(srvB)
	hC.SetServer(srvC)
	bcastA.SetPeersFunc(hA.HandshakedPeers)
	bcastB.SetPeersFunc(hB.HandshakedPeers)
	bcastC.SetPeersFunc(hC.HandshakedPeers)

	srvA.Start()
	defer srvA.Stop()
	srvB.Start()
	defer srvB.Stop()
	srvC.Start()
	defer srvC.Stop()

	srvB.AddPeer(srvA.ListenAddr())
	srvB.AddPeer(srvC.ListenAddr())

	// B should complete sync (either from A or C, with at most one timeout cycle)
	// within 5 seconds even if one peer is slow at any point.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bcB.CurrentBlock().Number() >= 20 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if bcB.CurrentBlock().Number() != 20 {
		t.Fatalf("B should have synced to block #20, got #%d", bcB.CurrentBlock().Number())
	}
}
