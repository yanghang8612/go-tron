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

	if !ss.HandleBlock(peerB, block2) {
		t.Fatal("block 2 should be consumed by sync")
	}
	if got := bc.CurrentBlock().Number(); got != 0 {
		t.Fatalf("out-of-order block should stay buffered, head=%d", got)
	}

	if !ss.HandleBlock(peerA, block1) {
		t.Fatal("block 1 should be consumed by sync")
	}
	if got := bc.CurrentBlock().Number(); got != 2 {
		t.Fatalf("buffered chain did not drain in order, head=%d", got)
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
	ps.inflight = 1
	ps.pending = map[tcommon.Hash]uint64{bid.Hash: bid.Num}
	ps.pendingIDs = map[tcommon.Hash]types.BlockID{bid.Hash: bid}
	ps.requestedHashes[bid.Hash] = struct{}{}
	ss.requested[bid.Hash] = ps.peer.ID()
}
