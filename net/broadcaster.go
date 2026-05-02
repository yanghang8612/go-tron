package net

import (
	"sync"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	seenCacheSize         = 10_000
	maxSpreadSize         = 1_000
	spreadInterval        = 30 * time.Millisecond
	blockProducedInterval = 3 * time.Second // items older than this are dropped at flush
)

type spreadItem struct {
	invType corepb.Inventory_InventoryType
	hash    tcommon.Hash
	origin  *p2p.Peer // nil if locally produced; skip this peer at flush
	addedAt time.Time
}

// BroadcastService manages inventory-based gossip for new blocks and transactions.
// The design mirrors java-tron's AdvService: items are queued in a global spread
// list, then flushed every 30 ms as per-peer batched INV messages.  Blocks bypass
// the ticker and trigger an immediate async flush because they are time-critical.
type BroadcastService struct {
	getPeers func() []*p2p.Peer

	mu       sync.Mutex
	seenA    map[tcommon.Hash]struct{} // current seen-generation
	seenB    map[tcommon.Hash]struct{} // previous seen-generation (two-gen LRU)
	toSpread []spreadItem

	quit     chan struct{}
	stopOnce sync.Once
}

// NewBroadcastService creates a new broadcast service.
func NewBroadcastService(getPeers func() []*p2p.Peer) *BroadcastService {
	return &BroadcastService{
		getPeers: getPeers,
		seenA:    make(map[tcommon.Hash]struct{}),
		seenB:    make(map[tcommon.Hash]struct{}),
		quit:     make(chan struct{}),
	}
}

// SetPeersFunc sets the function used to get handshaked peers.
func (bs *BroadcastService) SetPeersFunc(fn func() []*p2p.Peer) {
	bs.getPeers = fn
}

// Start launches the spread goroutine.
func (bs *BroadcastService) Start() {
	go bs.spreadLoop()
}

// Stop shuts down the broadcast service.
func (bs *BroadcastService) Stop() {
	bs.stopOnce.Do(func() { close(bs.quit) })
}

// BroadcastBlock queues a block produced locally for immediate spread to all peers.
func (bs *BroadcastService) BroadcastBlock(block *types.Block) {
	if bs.enqueue(corepb.Inventory_BLOCK, block.Hash(), nil) {
		go bs.flush()
	}
}

// BroadcastBlockFrom queues a relayed block, excluding the originating peer.
func (bs *BroadcastService) BroadcastBlockFrom(block *types.Block, origin *p2p.Peer) {
	if bs.enqueue(corepb.Inventory_BLOCK, block.Hash(), origin) {
		go bs.flush()
	}
}

// BroadcastTx queues a transaction for the next spread flush (satisfies TxBroadcaster).
// The inventory key is `tx.Hash()` (= SHA-256 of RawData = txID), matching
// java-tron's `TransactionMessage.getMessageId()` override which returns
// the rawData hash (not the full-tx hash that the base Message class
// produces). Mismatch triggers NO_SUCH_MESSAGE and disconnect.
func (bs *BroadcastService) BroadcastTx(tx *types.Transaction) {
	bs.enqueue(corepb.Inventory_TRX, tx.Hash(), nil)
}

// BroadcastTxFrom queues a relayed transaction, excluding the originating peer.
func (bs *BroadcastService) BroadcastTxFrom(tx *types.Transaction, origin *p2p.Peer) {
	bs.enqueue(corepb.Inventory_TRX, tx.Hash(), origin)
}

func (bs *BroadcastService) enqueue(invType corepb.Inventory_InventoryType, hash tcommon.Hash, origin *p2p.Peer) bool {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.markSeenLocked(hash) {
		return false
	}
	if len(bs.toSpread) >= maxSpreadSize {
		return false
	}
	bs.toSpread = append(bs.toSpread, spreadItem{
		invType: invType,
		hash:    hash,
		origin:  origin,
		addedAt: time.Now(),
	})
	return true
}

func (bs *BroadcastService) spreadLoop() {
	ticker := time.NewTicker(spreadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bs.flush()
		case <-bs.quit:
			return
		}
	}
}

func (bs *BroadcastService) flush() {
	bs.mu.Lock()
	if len(bs.toSpread) == 0 {
		bs.mu.Unlock()
		return
	}
	items := bs.toSpread
	bs.toSpread = nil
	bs.mu.Unlock()

	if bs.getPeers == nil {
		return
	}
	peers := bs.getPeers()
	if len(peers) == 0 {
		return
	}

	now := time.Now()

	// Build per-peer, per-type batches (mirrors java-tron InvSender).
	type batchKey struct {
		peer    *p2p.Peer
		invType corepb.Inventory_InventoryType
	}
	batches := make(map[batchKey][][]byte)

	for _, item := range items {
		if item.invType == corepb.Inventory_BLOCK && now.Sub(item.addedAt) > blockProducedInterval {
			continue // block advertisement expired
		}
		h := item.hash
		for _, peer := range peers {
			if peer == item.origin {
				continue // don't echo back to sender
			}
			k := batchKey{peer: peer, invType: item.invType}
			batches[k] = append(batches[k], h[:])
		}
	}

	for k, ids := range batches {
		inv := &corepb.Inventory{Type: k.invType, Ids: ids}
		if data, err := proto.Marshal(inv); err == nil {
			k.peer.Send(p2p.MsgInventory, data)
		}
	}
}

// markSeenLocked returns true if already seen, false if new (and marks it).
// Uses two-generation rotation to bound memory without full-clear eviction.
// Must be called with bs.mu held.
func (bs *BroadcastService) markSeenLocked(hash tcommon.Hash) bool {
	if _, ok := bs.seenA[hash]; ok {
		return true
	}
	if _, ok := bs.seenB[hash]; ok {
		return true
	}
	if len(bs.seenA) >= seenCacheSize {
		bs.seenB = bs.seenA
		bs.seenA = make(map[tcommon.Hash]struct{})
	}
	bs.seenA[hash] = struct{}{}
	return false
}
