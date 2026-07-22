package net

import (
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// runaheadBid builds a BlockID whose hash is derived from the number with a
// prefix distinct from testChainInventoryPayload's (0xa1), so bids seeded
// directly never collide with ids delivered through an inventory payload.
func runaheadBid(num uint64) types.BlockID {
	return types.BlockID{
		Hash: tcommon.Hash{0xba, byte(num), byte(num >> 8), byte(num >> 16), byte(num >> 24)},
		Num:  num,
	}
}

// TestNextFetchBatchSkipsBidsBeyondRunaheadBlockCap pins the count half of the
// sync runahead budget: bids further than MaxBufferedRunaheadBlocks past the
// local head must not be requested (the live Nile node was observed holding a
// ~2.8M-block, 2.5 GB raw runahead — the GC-spiral driver). Over-budget bids
// stay queued in fetchList — never dropped — and acquire no reservation side
// effects, so they become fetchable once the head advances.
func TestNextFetchBatchSkipsBidsBeyondRunaheadBlockCap(t *testing.T) {
	bc := makeTestChain(t) // head = genesis (#0)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "runahead-count")
	defer closePeer()

	within1, within2 := runaheadBid(1), runaheadBid(2)
	beyond1 := runaheadBid(maxBufferedRunaheadBlocks + 1)
	beyond2 := runaheadBid(maxBufferedRunaheadBlocks + 2)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = maxBufferedRunaheadBlocks + 2
	ps.fetchList = []types.BlockID{within1, within2, beyond1, beyond2}
	out := ss.fillFetchSlotsLocked(time.Now())
	kept := append([]types.BlockID(nil), ps.fetchList...)
	_, beyondRequested := ss.requested[beyond1.Hash]
	_, beyondPath := ss.blockPath[beyond1.Num]
	ss.mu.Unlock()

	if len(out) != 1 || out[0].chain {
		t.Fatalf("expected one block fetch request, got %+v", out)
	}
	if n := len(out[0].blocks); n != 2 {
		t.Fatalf("batch should hold only the %d within-cap bids, got %d: %v", 2, n, out[0].blocks)
	}
	for i, want := range []types.BlockID{within1, within2} {
		if out[0].blocks[i] != want {
			t.Fatalf("batch[%d] = %+v, want %+v", i, out[0].blocks[i], want)
		}
	}
	if len(kept) != 2 || kept[0] != beyond1 || kept[1] != beyond2 {
		t.Fatalf("over-cap bids must stay queued for when the head advances, fetchList=%v", kept)
	}
	if beyondRequested {
		t.Fatal("over-cap bid must not be marked requested")
	}
	if beyondPath {
		t.Fatal("over-cap bid must not reserve a block path")
	}
}

// TestNextFetchBatchOverByteBudgetKeepsOnlyNearHeadStrip pins the byte half of
// the budget: once the raw buffer holds MaxBufferedRunaheadBytes, only bids
// inside the AlwaysFetchRunaheadBlocks strip may be requested — the strip keeps
// the contiguous drain fed (hole refills right ahead of the head) while
// far-ahead fetching pauses until the buffer drains.
func TestNextFetchBatchOverByteBudgetKeepsOnlyNearHeadStrip(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "runahead-bytes")
	defer closePeer()

	nearEdge := runaheadBid(alwaysFetchRunaheadBlocks) // == head+strip: admissible
	farEdge := runaheadBid(alwaysFetchRunaheadBlocks + 1)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = alwaysFetchRunaheadBlocks + 1
	ps.fetchList = []types.BlockID{nearEdge, farEdge}
	ss.bufferedBytes = maxBufferedRunaheadBytes
	out := ss.fillFetchSlotsLocked(time.Now())
	kept := append([]types.BlockID(nil), ps.fetchList...)
	ss.mu.Unlock()

	if len(out) != 1 || out[0].chain {
		t.Fatalf("expected one block fetch request, got %+v", out)
	}
	if len(out[0].blocks) != 1 || out[0].blocks[0] != nearEdge {
		t.Fatalf("only the near-head strip bid should be requested over byte budget, got %v", out[0].blocks)
	}
	if len(kept) != 1 || kept[0] != farEdge {
		t.Fatalf("far bid must stay queued while over byte budget, fetchList=%v", kept)
	}
}

// TestFetchResumesBeyondStripAfterBufferDrains pins recovery: a bid parked by
// the byte budget becomes fetchable again once the buffer drains below the
// budget — the cap is backpressure, not a drop.
func TestFetchResumesBeyondStripAfterBufferDrains(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "runahead-resume")
	defer closePeer()

	far := runaheadBid(alwaysFetchRunaheadBlocks + 5)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = far.Num
	ps.fetchList = []types.BlockID{far}
	ss.bufferedBytes = maxBufferedRunaheadBytes
	out := ss.fillFetchSlotsLocked(time.Now())
	ss.mu.Unlock()
	if len(out) != 0 {
		t.Fatalf("no request expected while over byte budget, got %+v", out)
	}

	ss.mu.Lock()
	ss.bufferedBytes = 0 // buffer drained
	out = ss.fillFetchSlotsLocked(time.Now().Add(minFetchRequestInterval))
	ss.mu.Unlock()
	if len(out) != 1 || out[0].chain || len(out[0].blocks) != 1 || out[0].blocks[0] != far {
		t.Fatalf("parked bid should be requested after the buffer drains, got %+v", out)
	}
}

// TestRunaheadBudgetUsesEffectiveTip verifies async-commit progress opens the
// same forward window as committed progress. Otherwise a lagging CurrentBlock
// parks otherwise-valid work behind the runahead cap and can stall the pipeline.
func TestRunaheadBudgetUsesEffectiveTip(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)
	peer, closePeer := testPeer(t, "runahead-effective-tip")
	defer closePeer()

	effectiveTip := uint64(100)
	edge := runaheadBid(effectiveTip + maxBufferedRunaheadBlocks)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ss.syncedTipNum = effectiveTip
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = edge.Num
	ps.fetchList = []types.BlockID{edge}
	out := ss.fillFetchSlotsLocked(time.Now())
	ss.mu.Unlock()

	if len(out) != 1 || out[0].chain || len(out[0].blocks) != 1 || out[0].blocks[0] != edge {
		t.Fatalf("edge bid relative to effective tip should be requested, got %+v", out)
	}
}

// TestBufferedBytesTrackBufferLifecycle pins the bufferedBytes accounting the
// byte budget relies on: HandleBlock adds the stored raw length exactly once
// (conflicting duplicates at the same number add nothing), the drain pop
// subtracts it, and session teardown zeroes it.
func TestBufferedBytesTrackBufferLifecycle(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "bytes-track")
	defer closePeer()

	// Block #2 with head at #0: stays buffered (gap at #1), inspectable.
	blk := blockWithTxs(2, tcommon.Hash{0xab}, 2)
	raw := rawOf(t, blk)
	conflict := forkedStubBlock(2, tcommon.Hash{0xab}, 0x7)
	conflictRaw := rawOf(t, conflict)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	markPendingLocked(ss, ps, blk.ID())
	ss.mu.Unlock()

	if !ss.HandleBlock(peer, blk, raw) {
		t.Fatal("HandleBlock should consume the expected sync block")
	}
	ss.mu.Lock()
	got := ss.bufferedBytes
	ss.mu.Unlock()
	if got != int64(len(raw)) {
		t.Fatalf("bufferedBytes=%d after buffering, want %d", got, len(raw))
	}

	// A conflicting block at the same number is dropped: no double count.
	ss.mu.Lock()
	markPendingLocked(ss, ps, conflict.ID())
	ss.mu.Unlock()
	if !ss.HandleBlock(peer, conflict, conflictRaw) {
		t.Fatal("HandleBlock should consume the conflicting sync block")
	}
	ss.mu.Lock()
	got = ss.bufferedBytes
	ss.mu.Unlock()
	if got != int64(len(raw)) {
		t.Fatalf("bufferedBytes=%d after conflicting duplicate, want %d (unchanged)", got, len(raw))
	}

	// Popping the entry returns its bytes to the budget.
	ss.mu.Lock()
	ss.syncedTipNum = 1 // point the drain cursor at the buffered #2
	batch := ss.popBufferedSyncBatchLocked(time.Now())
	got = ss.bufferedBytes
	ss.mu.Unlock()
	if len(batch.buffered) != 1 {
		t.Fatalf("expected to pop 1 entry, got %d", len(batch.buffered))
	}
	if got != 0 {
		t.Fatalf("bufferedBytes=%d after pop, want 0", got)
	}

	// Session teardown and re-init both reset the counter.
	ss.mu.Lock()
	ss.bufferedBytes = 123
	ss.mu.Unlock()
	ss.doReset()
	ss.mu.Lock()
	afterReset := ss.bufferedBytes
	ss.bufferedBytes = 456
	ss.initSessionLocked(time.Now())
	afterInit := ss.bufferedBytes
	ss.mu.Unlock()
	if afterReset != 0 {
		t.Fatalf("bufferedBytes=%d after doReset, want 0", afterReset)
	}
	if afterInit != 0 {
		t.Fatalf("bufferedBytes=%d after initSessionLocked, want 0", afterInit)
	}
}

// TestRetryAssignmentRespectsRunaheadBudget pins that the retry path obeys the
// same budget as the primary fetch path: an over-cap retry bid stays in the
// shared retryList (available to any peer later) instead of being assigned.
func TestRetryAssignmentRespectsRunaheadBudget(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "runahead-retry")
	defer closePeer()

	beyond := runaheadBid(maxBufferedRunaheadBlocks + 1)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = beyond.Num // peer could serve it; only the budget says no
	ss.retryList = []types.BlockID{beyond}
	out := ss.fillFetchSlotsLocked(time.Now())
	retryLen := len(ss.retryList)
	fetchLen := len(ps.fetchList)
	ss.mu.Unlock()

	if len(out) != 0 {
		t.Fatalf("no request expected for an over-cap retry bid, got %+v", out)
	}
	if retryLen != 1 || fetchLen != 0 {
		t.Fatalf("over-cap retry bid must stay in retryList (retry=%d fetch=%d)", retryLen, fetchLen)
	}
}

// TestRequestedHashesPrunedBelowMinFetchNumOnInventory pins the requestedHashes
// bound. java-tron's FetchInvDataMsgHandler.check rejects any sync fetch below
// minBlockNum = lastSyncBlockId − 2×SYNC_FETCH_BATCH_NUM *before* its duplicate
// check, and its per-peer dup cache holds at most 2×SYNC_FETCH_BATCH_NUM
// entries — so hashes below our mirrored ps.minFetchNum can never be legally
// re-fetched from this peer and remembering them is pure leak (1.81 GB live on
// the Nile node). Entries at or above minFetchNum must survive.
func TestRequestedHashesPrunedBelowMinFetchNumOnInventory(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "prune-window")
	defer closePeer()

	bidOld := runaheadBid(1)
	bidEdgeOut := runaheadBid(2000) // < minFetchNum(2001): pruned
	bidEdgeIn := runaheadBid(2001)  // == minFetchNum: kept (java rejects only below)
	bidIn := runaheadBid(2500)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	for _, bid := range []types.BlockID{bidOld, bidEdgeOut, bidEdgeIn, bidIn} {
		markPendingLocked(ss, ps, bid)
	}
	ss.mu.Unlock()

	// Inventory whose last id (#6001) moves minFetchNum to 6001−2×2000 = 2001.
	ss.HandleChainInventory(peer, testChainInventoryPayload(t, 6001, 1, 0))

	ss.mu.Lock()
	minFetch := ps.minFetchNum
	total := len(ps.requestedHashes)
	_, oldKept := ps.requestedHashes[bidOld.Hash]
	_, edgeOutKept := ps.requestedHashes[bidEdgeOut.Hash]
	_, edgeInKept := ps.requestedHashes[bidEdgeIn.Hash]
	_, inKept := ps.requestedHashes[bidIn.Hash]
	ss.mu.Unlock()

	if minFetch != 2001 {
		t.Fatalf("setup: minFetchNum=%d, want 2001", minFetch)
	}
	if oldKept || edgeOutKept {
		t.Fatalf("hashes below minFetchNum must be pruned (old=%v edgeOut=%v)", oldKept, edgeOutKept)
	}
	if !edgeInKept || !inKept {
		t.Fatalf("in-window hashes must survive the prune (edgeIn=%v in=%v)", edgeInKept, inKept)
	}
	if total != 2 {
		t.Fatalf("requestedHashes holds %d entries, want 2", total)
	}
}

// TestRequestedHashesKeptWhileWindowUnestablished guards the prune threshold:
// before the peer's window clears 2×MaxChainInventorySize the mirrored
// minFetchNum stays 0 and nothing may be pruned.
func TestRequestedHashesKeptWhileWindowUnestablished(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "prune-early")
	defer closePeer()

	bid := runaheadBid(1)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	markPendingLocked(ss, ps, bid)
	ss.mu.Unlock()

	// last id #3000 ≤ 2×2000: window not established, minFetchNum stays 0.
	ss.HandleChainInventory(peer, testChainInventoryPayload(t, 3000, 1, 0))

	ss.mu.Lock()
	minFetch := ps.minFetchNum
	_, kept := ps.requestedHashes[bid.Hash]
	ss.mu.Unlock()

	if minFetch != 0 {
		t.Fatalf("setup: minFetchNum=%d, want 0", minFetch)
	}
	if !kept {
		t.Fatal("entry pruned while the fetch window is unestablished")
	}
}

// TestGrantedFetchRecordsPrunableHashNums pins that the grant path records the
// bid's block number (not a zero) so a later prune keeps exactly the entries
// java-tron's window still enforces: a hash granted at num == the eventual
// minFetchNum must survive.
func TestGrantedFetchRecordsPrunableHashNums(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	peer, closePeer := testPeer(t, "prune-grant")
	defer closePeer()

	edge := runaheadBid(5001)

	ss.mu.Lock()
	ss.initSessionLocked(time.Now())
	ps, _ := ss.addPeerStateLocked(peer)
	ps.lastInventoryNum = edge.Num
	ps.fetchList = []types.BlockID{edge}
	out := ss.fillFetchSlotsLocked(time.Now())
	ss.mu.Unlock()
	if len(out) != 1 || len(out[0].blocks) != 1 {
		t.Fatalf("setup: expected the edge bid granted, got %+v", out)
	}

	// Inventory at #9001 moves minFetchNum to exactly 5001 == the granted num.
	ss.HandleChainInventory(peer, testChainInventoryPayload(t, 9001, 1, 0))

	ss.mu.Lock()
	minFetch := ps.minFetchNum
	_, kept := ps.requestedHashes[edge.Hash]
	ss.mu.Unlock()

	if minFetch != edge.Num {
		t.Fatalf("setup: minFetchNum=%d, want %d", minFetch, edge.Num)
	}
	if !kept {
		t.Fatal("granted hash at num == minFetchNum was pruned — grant must record the real block number")
	}
}
