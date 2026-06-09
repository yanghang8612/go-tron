package net

import (
	gnet "net"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
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

	if !ss.HandleBlock(peerA, block1, nil) {
		t.Fatal("block 1 should be consumed by sync")
	}
	if got := bc.CurrentBlock().Number(); got != 2 {
		t.Fatalf("buffered chain did not drain in order, head=%d", got)
	}
	if got := ss.stats.CurrentSnapshot().TotalBlocks; got != 2 {
		t.Fatalf("sync stats total blocks after buffered range drain = %d, want 2", got)
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
	ss.blockBuffer[1] = bufferedSyncBlock{raw: rawOf(t, block1), num: 1, hash: block1.Hash(), peer: peer}
	ss.blockBuffer[2] = bufferedSyncBlock{raw: rawOf(t, block2), num: 2, hash: block2.Hash(), peer: peer}
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
	if !ss.shouldJoinAvailablePeersLocked(now) {
		t.Fatal("first join attempt should be allowed")
	}
	if ss.shouldJoinAvailablePeersLocked(now.Add(peerJoinAttemptInterval / 2)) {
		t.Fatal("join attempt should be throttled")
	}
	if !ss.shouldJoinAvailablePeersLocked(now.Add(peerJoinAttemptInterval)) {
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
