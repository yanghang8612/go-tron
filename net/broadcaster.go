package net

import (
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const seenCacheSize = 10000

// BroadcastService manages inventory-based gossip for new blocks and transactions.
type BroadcastService struct {
	getPeers func() []*p2p.Peer

	mu   sync.Mutex
	seen map[tcommon.Hash]struct{}
}

// NewBroadcastService creates a new broadcast service.
// getPeers returns the list of handshaked peers to broadcast to.
func NewBroadcastService(getPeers func() []*p2p.Peer) *BroadcastService {
	return &BroadcastService{
		getPeers: getPeers,
		seen:     make(map[tcommon.Hash]struct{}),
	}
}

// BroadcastBlock sends an INVENTORY message for a new block to all peers.
func (bs *BroadcastService) BroadcastBlock(block *types.Block) {
	hash := block.Hash()
	if bs.markSeen(hash) {
		return // already broadcast
	}

	inv := &corepb.Inventory{
		Type: corepb.Inventory_BLOCK,
		Ids:  [][]byte{hash[:]},
	}
	data, _ := proto.Marshal(inv)

	for _, peer := range bs.getPeers() {
		peer.Send(p2p.MsgInventory, data)
	}
}

// BroadcastTx sends an INVENTORY message for a new transaction to all peers.
func (bs *BroadcastService) BroadcastTx(tx *types.Transaction) {
	hash := tx.Hash()
	if bs.markSeen(hash) {
		return
	}

	inv := &corepb.Inventory{
		Type: corepb.Inventory_TRX,
		Ids:  [][]byte{hash[:]},
	}
	data, _ := proto.Marshal(inv)

	for _, peer := range bs.getPeers() {
		peer.Send(p2p.MsgInventory, data)
	}
}

// markSeen returns true if already seen, false if new (and marks it).
func (bs *BroadcastService) markSeen(hash tcommon.Hash) bool {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if _, exists := bs.seen[hash]; exists {
		return true
	}
	// Evict oldest if cache is full (simple: clear entire cache)
	if len(bs.seen) >= seenCacheSize {
		bs.seen = make(map[tcommon.Hash]struct{})
	}
	bs.seen[hash] = struct{}{}
	return false
}
