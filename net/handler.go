package net

import (
	"log"
	"sync"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// peerState tracks per-peer protocol state.
type peerState struct {
	peer        *p2p.Peer
	handshaked  bool
	headBlockID tcommon.Hash
	headNum     uint64
}

// TronHandler implements p2p.Handler for the TRON protocol.
type TronHandler struct {
	chain  *core.BlockChain
	pool   *txpool.TxPool
	server *p2p.Server

	mu    sync.RWMutex
	peers map[string]*peerState // peer id → state

	syncService *SyncService
	broadcaster *BroadcastService

	quit chan struct{}
}

// NewTronHandler creates a new TronHandler.
func NewTronHandler(chain *core.BlockChain, pool *txpool.TxPool, broadcaster *BroadcastService) *TronHandler {
	return &TronHandler{
		chain:       chain,
		pool:        pool,
		broadcaster: broadcaster,
		peers:       make(map[string]*peerState),
		quit:        make(chan struct{}),
	}
}

// SetServer sets the P2P server reference (for sending messages).
func (h *TronHandler) SetServer(srv *p2p.Server) {
	h.server = srv
}

// SetSyncService sets the sync service reference.
func (h *TronHandler) SetSyncService(ss *SyncService) {
	h.syncService = ss
}

// HandshakedPeerCount returns the number of handshaked peers.
func (h *TronHandler) HandshakedPeerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, ps := range h.peers {
		if ps.handshaked {
			count++
		}
	}
	return count
}

// HandshakedPeers returns all handshaked peers.
func (h *TronHandler) HandshakedPeers() []*p2p.Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var result []*p2p.Peer
	for _, ps := range h.peers {
		if ps.handshaked {
			result = append(result, ps.peer)
		}
	}
	return result
}

// OnPeerConnected is called when a new TCP connection is established.
func (h *TronHandler) OnPeerConnected(peer *p2p.Peer) {
	h.mu.Lock()
	h.peers[peer.ID()] = &peerState{peer: peer}
	h.mu.Unlock()

	// Send hello
	hello := h.buildHello()
	data, err := proto.Marshal(hello)
	if err != nil {
		log.Printf("Failed to marshal hello: %v", err)
		peer.Stop()
		return
	}
	peer.Send(p2p.MsgHello, data)
}

// OnPeerDisconnected is called when a peer connection is lost.
func (h *TronHandler) OnPeerDisconnected(peer *p2p.Peer) {
	h.mu.Lock()
	delete(h.peers, peer.ID())
	h.mu.Unlock()
}

// OnMessage routes incoming messages by type.
func (h *TronHandler) OnMessage(peer *p2p.Peer, code byte, payload []byte) {
	switch code {
	case p2p.MsgHello:
		h.handleHello(peer, payload)
	case p2p.MsgDisconnect:
		h.handleDisconnect(peer, payload)
	case p2p.MsgPing:
		peer.Send(p2p.MsgPong, nil)
	case p2p.MsgPong:
		// keep-alive acknowledged
	default:
		// Protocol messages — only process if handshaked
		h.mu.RLock()
		ps := h.peers[peer.ID()]
		h.mu.RUnlock()
		if ps == nil || !ps.handshaked {
			return
		}
		h.handleProtocolMessage(peer, code, payload)
	}
}

// StartKeepAlive starts a goroutine that pings peers every 30 seconds.
func (h *TronHandler) StartKeepAlive() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, peer := range h.HandshakedPeers() {
					peer.Send(p2p.MsgPing, nil)
				}
			case <-h.quit:
				return
			}
		}
	}()
}

// Stop signals the handler to shut down.
func (h *TronHandler) Stop() {
	select {
	case <-h.quit:
	default:
		close(h.quit)
	}
}

func (h *TronHandler) buildHello() *corepb.HelloMessage {
	head := h.chain.CurrentBlock()
	genesis := h.chain.GetBlockByNumber(0)
	genesisID := genesis.ID()
	headID := head.ID()

	return &corepb.HelloMessage{
		Version:   p2p.ProtocolVersion,
		Timestamp: time.Now().UnixMilli(),
		GenesisBlockId: &corepb.HelloMessage_BlockId{
			Hash:   genesisID.Hash[:],
			Number: int64(genesisID.Num),
		},
		SolidBlockId: &corepb.HelloMessage_BlockId{
			Hash:   headID.Hash[:],
			Number: int64(headID.Num),
		},
		HeadBlockId: &corepb.HelloMessage_BlockId{
			Hash:   headID.Hash[:],
			Number: int64(headID.Num),
		},
	}
}

func (h *TronHandler) handleHello(peer *p2p.Peer, payload []byte) {
	var hello corepb.HelloMessage
	if err := proto.Unmarshal(payload, &hello); err != nil {
		log.Printf("Peer %s: bad hello: %v", peer.ID(), err)
		h.disconnectPeer(peer, corepb.ReasonCode_BAD_PROTOCOL)
		return
	}

	// Validate genesis
	genesis := h.chain.GetBlockByNumber(0)
	genesisID := genesis.ID()
	if hello.GenesisBlockId == nil ||
		tcommon.BytesToHash(hello.GenesisBlockId.Hash) != genesisID.Hash {
		log.Printf("Peer %s: genesis mismatch", peer.ID())
		h.disconnectPeer(peer, corepb.ReasonCode_INCOMPATIBLE_CHAIN)
		return
	}

	// Mark handshaked
	h.mu.Lock()
	ps := h.peers[peer.ID()]
	if ps == nil {
		h.mu.Unlock()
		return
	}
	ps.handshaked = true
	if hello.HeadBlockId != nil {
		ps.headNum = uint64(hello.HeadBlockId.Number)
		ps.headBlockID = tcommon.BytesToHash(hello.HeadBlockId.Hash)
	}
	h.mu.Unlock()

	log.Printf("Peer %s handshaked (head=#%d)", peer.ID(), ps.headNum)

	// Trigger sync if peer has more blocks
	if h.syncService != nil && ps.headNum > h.chain.CurrentBlock().Number() {
		h.syncService.StartSync(peer)
	}
}

func (h *TronHandler) handleDisconnect(peer *p2p.Peer, payload []byte) {
	var msg corepb.DisconnectMessage
	if err := proto.Unmarshal(payload, &msg); err == nil {
		log.Printf("Peer %s disconnected: %v", peer.ID(), msg.Reason)
	}
	// Close the connection — readLoop will exit and call disconnect().
	// Don't call peer.Stop() here: we're inside readLoop, Stop() would deadlock.
	peer.Close()
}

func (h *TronHandler) disconnectPeer(peer *p2p.Peer, reason corepb.ReasonCode) {
	msg := &corepb.DisconnectMessage{Reason: reason}
	data, _ := proto.Marshal(msg)
	peer.Send(p2p.MsgDisconnect, data)
	go func() {
		time.Sleep(100 * time.Millisecond)
		peer.Close()
	}()
}

func (h *TronHandler) handleProtocolMessage(peer *p2p.Peer, code byte, payload []byte) {
	switch code {
	case p2p.MsgSyncBlockChain:
		if h.syncService != nil {
			h.syncService.HandleSyncBlockChain(peer, payload)
		}
	case p2p.MsgChainInventory:
		if h.syncService != nil {
			h.syncService.HandleChainInventory(peer, payload)
		}
	case p2p.MsgFetchInvData:
		h.handleFetchInvData(peer, payload)
	case p2p.MsgBlock:
		h.handleBlock(peer, payload)
	case p2p.MsgTx:
		h.handleTx(peer, payload)
	case p2p.MsgInventory:
		h.handleInventory(peer, payload)
	}
}

func (h *TronHandler) handleFetchInvData(peer *p2p.Peer, payload []byte) {
	var inv corepb.Inventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}
	switch inv.Type {
	case corepb.Inventory_BLOCK:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			block := h.chain.GetBlockByHash(hash)
			if block != nil {
				data, err := proto.Marshal(block.Proto())
				if err == nil {
					peer.Send(p2p.MsgBlock, data)
				}
			}
		}
	case corepb.Inventory_TRX:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			tx := h.pool.Get(hash)
			if tx != nil {
				data, err := proto.Marshal(tx.Proto())
				if err == nil {
					peer.Send(p2p.MsgTx, data)
				}
			}
		}
	}
}

func (h *TronHandler) handleBlock(peer *p2p.Peer, payload []byte) {
	var pbBlock corepb.Block
	if err := proto.Unmarshal(payload, &pbBlock); err != nil {
		return
	}
	block := types.NewBlockFromPB(&pbBlock)

	// If sync service handles it (sequential sync), defer to it
	if h.syncService != nil && h.syncService.HandleBlock(peer, block) {
		return
	}

	// Otherwise it's a new block broadcast — try to insert
	if err := h.chain.InsertBlock(block); err != nil {
		return
	}
	log.Printf("Received block #%d from peer %s", block.Number(), peer.ID())

	// Relay to other peers
	if h.broadcaster != nil {
		h.broadcaster.BroadcastBlock(block)
	}
}

func (h *TronHandler) handleTx(peer *p2p.Peer, payload []byte) {
	var pbTx corepb.Transaction
	if err := proto.Unmarshal(payload, &pbTx); err != nil {
		return
	}
	tx := types.NewTransactionFromPB(&pbTx)
	if err := h.pool.Add(tx); err != nil {
		return
	}
	// Relay inventory to other peers
	if h.broadcaster != nil {
		h.broadcaster.BroadcastTx(tx)
	}
}

func (h *TronHandler) handleInventory(peer *p2p.Peer, payload []byte) {
	var inv corepb.Inventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}

	// Filter out items we already have, request the rest
	var needed [][]byte
	switch inv.Type {
	case corepb.Inventory_BLOCK:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			if h.chain.GetBlockByHash(hash) == nil {
				needed = append(needed, id)
			}
		}
	case corepb.Inventory_TRX:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			if h.pool.Get(hash) == nil {
				needed = append(needed, id)
			}
		}
	}

	if len(needed) > 0 {
		fetch := &corepb.Inventory{Type: inv.Type, Ids: needed}
		data, _ := proto.Marshal(fetch)
		peer.Send(p2p.MsgFetchInvData, data)
	}
}
