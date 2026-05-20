package net

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// peerConnState is the lifecycle state of a peer connection.
type peerConnState uint8

const (
	peerStateInit       peerConnState = iota // connected, no hello yet
	peerStateHandshaked                      // hello exchanged successfully
	peerStateBad                             // hard failure (e.g. genesis mismatch)
)

// peerState tracks per-peer protocol state.
type peerState struct {
	peer        *p2p.Peer
	connState   peerConnState
	rl          *p2p.RateLimiter
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

	syncService  *SyncService
	broadcaster  *BroadcastService
	pbftHandler  *PbftHandler
	pbftDataSync *PbftDataSyncHandler

	// cheatDetector watches advertised blocks for double-signing by a
	// witness (same height + same witness + different hash). Mirrors
	// java-tron `WitnessProductBlockService` and is exposed via
	// `CheatDetector()` for the NodeInfo RPC. Detection is monitoring
	// only — no consensus or witness state is mutated.
	cheatDetector *dpos.CheatDetector

	quit chan struct{}
}

// NewTronHandler creates a new TronHandler.
func NewTronHandler(chain *core.BlockChain, pool *txpool.TxPool, broadcaster *BroadcastService) *TronHandler {
	return &TronHandler{
		chain:         chain,
		pool:          pool,
		broadcaster:   broadcaster,
		peers:         make(map[string]*peerState),
		quit:          make(chan struct{}),
		pbftHandler:   NewPbftHandler(chain, chain.DB(), nil, nil),
		pbftDataSync:  NewPbftDataSyncHandler(chain, chain.DB()),
		cheatDetector: dpos.NewCheatDetector(),
	}
}

// CheatDetector returns the witness double-sign detector. Exposed for the
// NodeInfo RPC and operator tooling.
func (h *TronHandler) CheatDetector() *dpos.CheatDetector {
	return h.cheatDetector
}

// SetServer sets the P2P server reference (for sending messages).
func (h *TronHandler) SetServer(srv *p2p.Server) {
	h.server = srv
	h.pbftHandler.server = srv
}

// SetSyncService sets the sync service reference.
func (h *TronHandler) SetSyncService(ss *SyncService) {
	h.syncService = ss
	h.pbftHandler.sync = ss
}

// PbftHandler returns the PBFT message handler (for Lifecycle registration and hook wiring).
func (h *TronHandler) PbftHandler() *PbftHandler {
	return h.pbftHandler
}

// PbftDataSync returns the PBFT data sync handler (for Lifecycle registration and hook wiring).
func (h *TronHandler) PbftDataSync() *PbftDataSyncHandler {
	return h.pbftDataSync
}

// HandshakedPeerCount returns the number of handshaked peers.
func (h *TronHandler) HandshakedPeerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, ps := range h.peers {
		if ps.connState == peerStateHandshaked {
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
		if ps.connState == peerStateHandshaked {
			result = append(result, ps.peer)
		}
	}
	return result
}

// BestSyncCandidate returns the handshaked peer with the highest head that is
// ahead of our own chain head, excluding the given peer. Returns nil when no
// suitable candidate exists.
func (h *TronHandler) BestSyncCandidate(exclude *p2p.Peer) *p2p.Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ourHead := h.chain.CurrentBlock().Number()
	var best *peerState
	for _, ps := range h.peers {
		if ps.connState != peerStateHandshaked {
			continue
		}
		if exclude != nil && ps.peer == exclude {
			continue
		}
		if ps.headNum <= ourHead {
			continue
		}
		if best == nil || ps.headNum > best.headNum {
			best = ps
		}
	}
	if best == nil {
		return nil
	}
	return best.peer
}

// SyncCandidates returns handshaked peers ahead of our head, sorted by their
// advertised head descending and excluding peers whose IDs are present in
// exclude. It is used by SyncService to fan out java-tron-compatible 100-block
// fetch batches across several peers.
func (h *TronHandler) SyncCandidates(exclude map[string]struct{}, limit int) []*p2p.Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ourHead := h.chain.CurrentBlock().Number()
	states := make([]*peerState, 0, len(h.peers))
	for _, ps := range h.peers {
		if ps.connState != peerStateHandshaked {
			continue
		}
		if exclude != nil {
			if _, skip := exclude[ps.peer.ID()]; skip {
				continue
			}
		}
		if ps.headNum <= ourHead {
			continue
		}
		states = append(states, ps)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].headNum > states[j].headNum
	})
	if limit > 0 && len(states) > limit {
		states = states[:limit]
	}
	peers := make([]*p2p.Peer, 0, len(states))
	for _, ps := range states {
		peers = append(peers, ps.peer)
	}
	return peers
}

// OnPeerConnected is called when a new TCP connection is established.
func (h *TronHandler) OnPeerConnected(peer *p2p.Peer) {
	h.mu.Lock()
	h.peers[peer.ID()] = &peerState{peer: peer, rl: p2p.NewRateLimiter()}
	h.mu.Unlock()

	// Send hello
	hello := h.buildHello()
	data, err := proto.Marshal(hello)
	if err != nil {
		log.Error("Failed to marshal hello", "peer", peer.ID(), "err", err)
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
	// h.mu released before notifying syncService to avoid lock-order inversion
	// (syncService.tryFindSyncPeer re-acquires h.mu via BestSyncCandidate).
	if h.syncService != nil {
		h.syncService.PeerDisconnected(peer)
	}
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
		if ps == nil || ps.connState != peerStateHandshaked {
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

	// HelloMessage.Version is java-tron's `p2p.version` (network
	// discriminator), distinct from the libp2p-handshake protocol version.
	// Pull it from the configured server so custom-chain bootstrap
	// (--genesis with `p2p_version: 0`) handshakes against java-tron.
	version := p2p.ProtocolVersion
	if h.server != nil {
		version = h.server.NetworkID()
	}
	return &corepb.HelloMessage{
		Version:   version,
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
		log.Warn("Bad hello", "peer", peer.ID(), "err", err)
		h.disconnectPeer(peer, corepb.ReasonCode_BAD_PROTOCOL)
		return
	}

	// Validate genesis
	genesis := h.chain.GetBlockByNumber(0)
	genesisID := genesis.ID()
	if hello.GenesisBlockId == nil ||
		tcommon.BytesToHash(hello.GenesisBlockId.Hash) != genesisID.Hash {
		log.Warn("Genesis mismatch", "peer", peer.ID())
		h.mu.Lock()
		if ps := h.peers[peer.ID()]; ps != nil {
			ps.connState = peerStateBad
		}
		h.mu.Unlock()
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
	ps.connState = peerStateHandshaked
	if hello.HeadBlockId != nil {
		ps.headNum = uint64(hello.HeadBlockId.Number)
		ps.headBlockID = tcommon.BytesToHash(hello.HeadBlockId.Hash)
	}
	h.mu.Unlock()

	localHead := h.chain.CurrentBlock().Number()
	log.Info("Peer handshaked",
		"peer", peer.ID(),
		"peerHead", ps.headNum,
		"localHead", localHead,
		"lag", int64(ps.headNum)-int64(localHead))

	// Trigger sync if peer has more blocks
	if h.syncService != nil && ps.headNum > localHead {
		h.syncService.StartSync(peer)
	}
}

func (h *TronHandler) handleDisconnect(peer *p2p.Peer, payload []byte) {
	var msg corepb.DisconnectMessage
	if err := proto.Unmarshal(payload, &msg); err == nil {
		log.Info("Peer disconnected", "peer", peer.ID(), "reason", msg.Reason.String())
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
	// Rate-limit check: drop throttled messages rather than crashing or disconnecting.
	h.mu.RLock()
	ps := h.peers[peer.ID()]
	h.mu.RUnlock()
	if ps != nil && ps.rl != nil && !ps.rl.Allow(code) {
		log.Warn("Peer rate limited",
			"peer", peer.ID(),
			"msg", p2p.MsgName(code),
			"code", fmt.Sprintf("0x%02x", code))
		return
	}

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
	case p2p.MsgTrxs:
		h.handleTrxs(peer, payload)
	case p2p.MsgInventory:
		h.handleInventory(peer, payload)
	case p2p.MsgPbftMsg:
		h.pbftHandler.HandlePbftMsg(peer, payload)
	case p2p.MsgPbftCommitMsg:
		h.pbftDataSync.HandleCommitMsg(peer, payload)
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
			// Block IDs have block number in first 8 bytes (big-endian).
			// Try by number first (more reliable), fallback to hash.
			var block *types.Block
			if len(id) >= 8 {
				num := binary.BigEndian.Uint64(id[:8])
				block = h.chain.GetBlockByNumber(num)
			}
			if block == nil {
				block = h.chain.GetBlockByHash(tcommon.BytesToHash(id))
			}
			if block != nil {
				data, err := proto.Marshal(block.Proto())
				if err == nil {
					peer.Send(p2p.MsgBlock, data)
				}
			}
		}
	case corepb.Inventory_TRX:
		for _, id := range inv.Ids {
			tx := h.pool.Get(tcommon.BytesToHash(id))
			if tx != nil {
				// java-tron's P2pEventHandlerImpl only dispatches TRXS
				// (0x03, batched). Single TRX (0x01) falls through to
				// NO_SUCH_MESSAGE; wrap in Transactions even when sending
				// just one.
				batch := &corepb.Transactions{Transactions: []*corepb.Transaction{tx.Proto()}}
				data, err := proto.Marshal(batch)
				if err == nil {
					peer.Send(p2p.MsgTrxs, data)
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

	// During initial sync, drop adv-broadcast blocks. Inserting them moves
	// KhaosDB's head far ahead of our actual canonical tip; the next eviction
	// pass (effectiveHead - 1024) then erases the parents of the in-progress
	// sync range, and every subsequent sync block fails with ErrUnlinkedBlock.
	// java-tron's BlockMsgHandler is forgiving about this because its KhaosDB
	// is much larger; gtron's tighter window can't tolerate the gap. Once
	// sync completes (IsSyncing()==false), we accept adv blocks normally.
	if h.syncService != nil && (h.syncService.IsSyncing() || h.syncService.IsPaused()) {
		return
	}

	// Otherwise it's a new block broadcast — try to insert
	if err := h.chain.InsertBlock(block); err != nil {
		log.Warn("Block insert failed", "number", block.Number(), "peer", peer.ID(), "err", err)
		return
	}
	log.Debug("Block received", "number", block.Number(), "peer", peer.ID())

	// Witness cheat detection runs only on the advertised-block path,
	// matching java-tron `BlockMsgHandler.processAdvBlock` line 153 where
	// `witnessProductBlockService.validWitnessProductTwoBlock(block)` is
	// invoked after `tronNetDelegate.processBlock` succeeds. The sync
	// path returns above, the producer path inserts directly through
	// BlockChain without coming through here, so this is the single
	// adv-only hook.
	if h.cheatDetector != nil {
		h.cheatDetector.CheckBlock(block)
	}

	// Relay to other peers
	if h.broadcaster != nil {
		h.broadcaster.BroadcastBlockFrom(block, peer)
	}
}

func (h *TronHandler) handleTx(peer *p2p.Peer, payload []byte) {
	var pbTx corepb.Transaction
	if err := proto.Unmarshal(payload, &pbTx); err != nil {
		return
	}
	tx := types.NewTransactionFromPB(&pbTx)
	// Drop peer-gossiped txs that fail signature/permission. Without this a
	// malicious peer can flood the pool with malformed txs that the producer
	// only finds out about when it tries to include them. Mirrors java-tron
	// Manager.pushTransaction's validateSignature gate.
	if err := h.chain.ValidateTransaction(tx); err != nil {
		return
	}
	if err := h.pool.Add(tx); err != nil {
		return
	}
	// Relay inventory to other peers
	if h.broadcaster != nil {
		h.broadcaster.BroadcastTxFrom(tx, peer)
	}
}

// handleTrxs accepts a TRXS (0x03) batched transactions payload from a peer.
// java-tron sends every tx — solo or batch — as TRXS, so this is the
// primary inbound tx ingestion path on a cross-impl link.
func (h *TronHandler) handleTrxs(peer *p2p.Peer, payload []byte) {
	var batch corepb.Transactions
	if err := proto.Unmarshal(payload, &batch); err != nil {
		return
	}
	for _, pbTx := range batch.Transactions {
		tx := types.NewTransactionFromPB(pbTx)
		if err := h.chain.ValidateTransaction(tx); err != nil {
			continue
		}
		if err := h.pool.Add(tx); err != nil {
			continue
		}
		if h.broadcaster != nil {
			h.broadcaster.BroadcastTxFrom(tx, peer)
		}
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
			if h.pool.Get(tcommon.BytesToHash(id)) == nil {
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
