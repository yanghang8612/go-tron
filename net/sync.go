package net

import (
	"log"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	maxChainInventorySize = 2000
	maxFetchBatch         = 100
)

// SyncService handles the block sync protocol.
type SyncService struct {
	chain   *core.BlockChain
	handler *TronHandler

	mu        sync.Mutex
	syncing   bool
	syncPeer  *p2p.Peer
	fetchList []types.BlockID // blocks to fetch from peer
	remainNum int64
}

// NewSyncService creates a new sync service.
func NewSyncService(chain *core.BlockChain, handler *TronHandler) *SyncService {
	return &SyncService{
		chain:   chain,
		handler: handler,
	}
}

// IsSyncing returns whether sync is in progress.
func (ss *SyncService) IsSyncing() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.syncing
}

// BuildChainSummary creates an exponentially-spaced list of block IDs
// from our chain, used in SYNC_BLOCK_CHAIN messages.
func (ss *SyncService) BuildChainSummary() []types.BlockID {
	head := ss.chain.CurrentBlock()
	headNum := head.Number()

	var summary []types.BlockID
	step := uint64(1)
	num := headNum

	for {
		block := ss.chain.GetBlockByNumber(num)
		if block != nil {
			summary = append(summary, block.ID())
		}
		if num == 0 {
			break
		}
		if num < step {
			num = 0
		} else {
			num -= step
		}
		// Double step each time for exponential backoff
		step *= 2
	}

	return summary
}

// FindCommonBlock finds the highest block in peerSummary that exists in our chain.
func (ss *SyncService) FindCommonBlock(peerSummary []types.BlockID) uint64 {
	for _, bid := range peerSummary {
		block := ss.chain.GetBlockByNumber(bid.Number())
		if block != nil && block.ID().Hash == bid.Hash {
			return bid.Number()
		}
	}
	return 0 // fallback to genesis
}

// StartSync initiates sync with a peer that has a higher head block.
func (ss *SyncService) StartSync(peer *p2p.Peer) {
	ss.mu.Lock()
	if ss.syncing {
		ss.mu.Unlock()
		return
	}
	ss.syncing = true
	ss.syncPeer = peer
	ss.mu.Unlock()

	log.Printf("Starting sync with peer %s", peer.ID())
	ss.sendSyncBlockChain(peer)
}

func (ss *SyncService) sendSyncBlockChain(peer *p2p.Peer) {
	summary := ss.BuildChainSummary()
	var ids []*corepb.BlockInventory_BlockId
	for _, bid := range summary {
		ids = append(ids, &corepb.BlockInventory_BlockId{
			Hash:   bid.Hash[:],
			Number: int64(bid.Num),
		})
	}
	msg := &corepb.BlockInventory{
		Ids:  ids,
		Type: corepb.BlockInventory_SYNC,
	}
	data, _ := proto.Marshal(msg)
	peer.Send(p2p.MsgSyncBlockChain, data)
}

// HandleSyncBlockChain processes SYNC_BLOCK_CHAIN from a peer.
// Responds with CHAIN_INVENTORY containing missing block IDs.
func (ss *SyncService) HandleSyncBlockChain(peer *p2p.Peer, payload []byte) {
	var inv corepb.BlockInventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}

	// Convert to BlockIDs
	var peerSummary []types.BlockID
	for _, bid := range inv.Ids {
		peerSummary = append(peerSummary, types.BlockID{
			Hash: tcommon.BytesToHash(bid.Hash),
			Num:  uint64(bid.Number),
		})
	}

	// Find common block
	commonNum := ss.FindCommonBlock(peerSummary)
	headNum := ss.chain.CurrentBlock().Number()

	// Build chain inventory: sequential blocks after common
	var responseIDs []*corepb.ChainInventory_BlockId
	count := 0
	for num := commonNum + 1; num <= headNum && count < maxChainInventorySize; num++ {
		block := ss.chain.GetBlockByNumber(num)
		if block == nil {
			break
		}
		bid := block.ID()
		responseIDs = append(responseIDs, &corepb.ChainInventory_BlockId{
			Hash:   bid.Hash[:],
			Number: int64(bid.Num),
		})
		count++
	}

	remainNum := int64(0)
	if commonNum+uint64(count) < headNum {
		remainNum = int64(headNum) - int64(commonNum) - int64(count)
	}

	resp := &corepb.ChainInventory{
		Ids:       responseIDs,
		RemainNum: remainNum,
	}
	data, _ := proto.Marshal(resp)
	peer.Send(p2p.MsgChainInventory, data)
}

// HandleChainInventory processes CHAIN_INVENTORY from the sync peer.
// Stores the block IDs to fetch, then starts fetching.
func (ss *SyncService) HandleChainInventory(peer *p2p.Peer, payload []byte) {
	ss.mu.Lock()
	if peer != ss.syncPeer {
		ss.mu.Unlock()
		return
	}
	ss.mu.Unlock()

	var inv corepb.ChainInventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}

	ss.mu.Lock()
	ss.fetchList = nil
	for _, bid := range inv.Ids {
		ss.fetchList = append(ss.fetchList, types.BlockID{
			Hash: tcommon.BytesToHash(bid.Hash),
			Num:  uint64(bid.Number),
		})
	}
	ss.remainNum = inv.RemainNum
	ss.mu.Unlock()

	if len(inv.Ids) == 0 {
		ss.finishSync()
		return
	}

	log.Printf("Chain inventory: %d blocks to fetch, %d remaining", len(inv.Ids), inv.RemainNum)
	ss.fetchNextBatch()
}

func (ss *SyncService) fetchNextBatch() {
	ss.mu.Lock()
	if len(ss.fetchList) == 0 {
		peer := ss.syncPeer
		remainNum := ss.remainNum
		ss.mu.Unlock()
		// If more remain, request next chain inventory
		if remainNum > 0 {
			ss.sendSyncBlockChain(peer)
		} else {
			ss.finishSync()
		}
		return
	}

	batch := ss.fetchList
	if len(batch) > maxFetchBatch {
		batch = batch[:maxFetchBatch]
	}
	ss.fetchList = ss.fetchList[len(batch):]
	peer := ss.syncPeer
	ss.mu.Unlock()

	var ids [][]byte
	for _, bid := range batch {
		h := bid.Hash
		ids = append(ids, h[:])
	}
	fetch := &corepb.Inventory{
		Type: corepb.Inventory_BLOCK,
		Ids:  ids,
	}
	data, _ := proto.Marshal(fetch)
	peer.Send(p2p.MsgFetchInvData, data)
}

// HandleBlock processes a received block during sync.
// Returns true if the block was consumed by sync, false if it should be handled as a broadcast.
func (ss *SyncService) HandleBlock(peer *p2p.Peer, block *types.Block) bool {
	ss.mu.Lock()
	if !ss.syncing || peer != ss.syncPeer {
		ss.mu.Unlock()
		return false
	}
	ss.mu.Unlock()

	if err := ss.chain.InsertBlock(block); err != nil {
		log.Printf("Sync: failed to insert block #%d: %v", block.Number(), err)
		// Try without verify for sync (peer already validated)
		if err2 := ss.chain.InsertBlockWithoutVerify(block); err2 != nil {
			log.Printf("Sync: also failed InsertBlockWithoutVerify #%d: %v", block.Number(), err2)
			return true
		}
	}

	log.Printf("Synced block #%d", block.Number())

	// Check if we need more blocks
	ss.fetchNextBatch()
	return true
}

func (ss *SyncService) finishSync() {
	ss.mu.Lock()
	ss.syncing = false
	ss.syncPeer = nil
	ss.fetchList = nil
	ss.remainNum = 0
	ss.mu.Unlock()
	log.Printf("Sync complete (head=#%d)", ss.chain.CurrentBlock().Number())
}
