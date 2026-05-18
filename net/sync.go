package net

import (
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
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

// syncFetchTimeout is how long to wait for a block response before failing over
// to another peer. Tests may override this.
var syncFetchTimeout = 30 * time.Second

// statsReportInterval is the cadence at which sync emits "Imported chain
// segment" summary lines. Exposed as a var so tests can shrink it. Mirrors
// geth's blockchain_insert.go:statsReportLimit.
var statsReportInterval = 8 * time.Second

// syncStats accumulates per-window throughput counters for the sync summary
// line. Reset at every emit so the figures represent rolling-window rates.
type syncStats struct {
	startTime   time.Time     // window start
	blocks      int           // blocks applied in window
	txs         int           // tx count applied in window
	execElapsed time.Duration // accumulated InsertBlock wall time
	totalStart  time.Time     // session start (for "Sync complete" line)
	totalBlocks int           // session-wide block count
}

// SyncService handles the block sync protocol.
type SyncService struct {
	chain   *core.BlockChain
	handler *TronHandler

	mu         sync.Mutex
	syncing    bool
	syncPeer   *p2p.Peer
	fetchList  []types.BlockID // blocks to fetch from peer
	remainNum  int64
	inflight   int // blocks requested but not yet received in the current batch
	pending    map[tcommon.Hash]uint64
	fetchSeq   uint64      // incremented on each fetch batch and on block receipt
	fetchTimer *time.Timer // fires if no block arrives within syncFetchTimeout

	// Sticky pause set on any InsertBlock failure during sync. Once set,
	// StartSync / checkIsolation / tryFindSyncPeer all short-circuit; the
	// SyncBlockChain handler still serves outbound peers. The peer that
	// delivered the bad block is NOT disconnected — gtron is the more
	// likely culprit than a peer (re-impl racing toward parity), so we keep
	// the connection so the operator can diagnose without losing peer
	// state. Cleared only by process restart.
	paused       bool
	pausedAtNum  uint64
	pausedAtTime time.Time
	pausedErr    error

	// stats accumulates per-window throughput counters used for the
	// "Imported chain segment" summary line. Guarded by ss.mu.
	stats syncStats

	quit     chan struct{}
	stopOnce sync.Once
}

// NewSyncService creates a new sync service.
func NewSyncService(chain *core.BlockChain, handler *TronHandler) *SyncService {
	return &SyncService{
		chain:   chain,
		handler: handler,
		quit:    make(chan struct{}),
	}
}

// Start launches the isolation watchdog goroutine.
func (ss *SyncService) Start() {
	go ss.watchdog()
}

// Stop shuts down the sync service and cancels any in-progress sync.
func (ss *SyncService) Stop() {
	ss.stopOnce.Do(func() { close(ss.quit) })
	ss.mu.Lock()
	ss.doReset()
	ss.mu.Unlock()
}

// watchdog fires every 30 s and triggers a sync if the chain appears isolated.
func (ss *SyncService) watchdog() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ss.checkIsolation()
		case <-ss.quit:
			return
		}
	}
}

// checkIsolation starts a sync if we are not already syncing and the chain
// head has not advanced in over 30 s. Tries `BestSyncCandidate` first
// (peer with strictly-higher advertised head) and falls back to any
// handshaked peer — java-tron's `AdvService` does not advertise new
// blocks via INVENTORY until it considers our peer "ready", so the
// peer's cached headNum can lag arbitrarily behind reality. Polling
// `BuildChainSummary` against any peer lets java-tron re-evaluate.
func (ss *SyncService) checkIsolation() {
	if ss.IsSyncing() || ss.IsPaused() || ss.chain == nil || ss.handler == nil {
		return
	}
	if time.Since(ss.chain.LastInsertTime()) < 30*time.Second {
		return
	}
	candidate := ss.handler.BestSyncCandidate(nil)
	if candidate == nil {
		// Fall back: any handshaked peer. java-tron will respond with an
		// empty CHAIN_INVENTORY if we're already at head, so this is cheap.
		if peers := ss.handler.HandshakedPeers(); len(peers) > 0 {
			candidate = peers[0]
		}
	}
	if candidate != nil {
		log.Info("Polling peer (chain stalled)",
			"peer", candidate.ID(),
			"head", ss.chain.CurrentBlock().Number(),
			"stalledFor", ethcommon.PrettyDuration(time.Since(ss.chain.LastInsertTime())))
		ss.StartSync(candidate)
	}
}

// IsSyncing returns whether sync is in progress.
func (ss *SyncService) IsSyncing() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.syncing
}

// IsPaused reports whether sync has been stopped by a prior InsertBlock failure.
// While paused, no new sync starts and the watchdog skips its kick — but peers
// stay connected and inbound SYNC_BLOCK_CHAIN requests are still served.
func (ss *SyncService) IsPaused() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.paused
}

// PausedStatus returns the pause flag along with the block number, time, and
// error captured when the pause was triggered. Intended for status reporting.
func (ss *SyncService) PausedStatus() (paused bool, atNum uint64, at time.Time, err error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.paused, ss.pausedAtNum, ss.pausedAtTime, ss.pausedErr
}

// BuildChainSummary creates an exponentially-spaced list of block IDs
// from our chain, used in SYNC_BLOCK_CHAIN messages. The result is in
// ascending order (oldest first, newest last) — matching java-tron's
// `SyncService.getBlockChainSummary` convention. java-tron's
// `SyncBlockChainMsgHandler.check` enforces
// `summary[last].num >= peer.lastSyncBlockId.num`, so the summary must
// end at our current head; sending it head-first triggers BAD_MESSAGE
// after the first inventory exchange.
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

	// Reverse to ascending order: java-tron expects oldest first.
	for i, j := 0, len(summary)-1; i < j; i, j = i+1, j-1 {
		summary[i], summary[j] = summary[j], summary[i]
	}
	return summary
}

// FindCommonBlock finds the highest block in peerSummary that exists in our chain.
func (ss *SyncService) FindCommonBlock(peerSummary []types.BlockID) uint64 {
	headNum := ss.chain.CurrentBlock().Number()
	for _, bid := range peerSummary {
		if bid.Number() > headNum {
			continue
		}
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
	if ss.paused {
		ss.mu.Unlock()
		return
	}
	if ss.syncing {
		ss.mu.Unlock()
		return
	}
	ss.syncing = true
	ss.syncPeer = peer
	now := time.Now()
	ss.stats = syncStats{startTime: now, totalStart: now}
	ss.mu.Unlock()

	log.Info("Sync started",
		"peer", peer.ID(),
		"localHead", ss.chain.CurrentBlock().Number())
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

	// Drop any ids we already hold. java-tron tracks every block id it has
	// sent us in `syncBlockIdCache` and treats a repeat FETCH_INV_DATA for
	// the same id as a protocol violation (BAD_PROTOCOL → peer drop). Two
	// classes of repeats need to be filtered:
	//
	//   1. The un-fork point id, which java-tron's getLostBlockIds always
	//      returns as the first id of CHAIN_INVENTORY — on every batch
	//      after the first this is a block we already committed.
	//   2. Blocks we received past a parent gap and parked in KhaosDB's
	//      miniUnlinkedStore. They are not on the canonical chain (the
	//      rawdb check below would miss them) but we already hold them; if
	//      their gap parent later arrives, KhaosDB.promoteUnlinked cascades
	//      them into miniStore and InsertBlock's switchFork applies the
	//      stretch in topological order, so refetching is never needed.
	ss.mu.Lock()
	ss.fetchList = ss.fetchList[:0]
	headNum := ss.chain.CurrentBlock().Number()
	for _, bid := range inv.Ids {
		num := uint64(bid.Number)
		hash := tcommon.BytesToHash(bid.Hash)
		if num <= headNum {
			if existing := ss.chain.GetBlockByNumber(num); existing != nil && existing.Hash() == hash {
				continue
			}
		}
		if ss.chain.HasBlockInKhaosDB(hash) {
			continue
		}
		ss.fetchList = append(ss.fetchList, types.BlockID{Hash: hash, Num: num})
	}
	ss.remainNum = inv.RemainNum
	ss.mu.Unlock()

	if len(inv.Ids) == 0 {
		ss.finishSync()
		return
	}

	// java-tron sets `needSyncFromUs = false` on its peer record only when
	// our summary's last block matches its head (lostBlockIds.size == 1).
	// While needSyncFromUs is true, java-tron's InventoryMsgHandler drops
	// every inbound INV — so our outbound TRX advertisements never reach
	// the producer's mempool. Detect "we are at head" here (response is a
	// single id we already have) and finish; otherwise continue fetching.
	if len(ss.fetchList) == 0 && len(inv.Ids) == 1 && inv.RemainNum == 0 {
		ss.finishSync()
		return
	}

	log.Debug("Chain inventory received",
		"blocks", len(inv.Ids), "remain", inv.RemainNum, "peer", peer.ID())
	ss.fetchNextBatch()
}

func (ss *SyncService) fetchNextBatch() {
	ss.mu.Lock()
	if len(ss.fetchList) == 0 {
		peer := ss.syncPeer
		ss.mu.Unlock()
		// Always re-poll, even when remainNum == 0. java-tron may have
		// produced new blocks while we were applying the previous batch;
		// we need to keep sending SYNC_BLOCK_CHAIN until the response
		// shrinks to 1 block (handled in HandleChainInventory). That
		// transition flips java-tron's `needSyncFromUs` flag to false
		// and lets our subsequent INV broadcasts through.
		ss.sendSyncBlockChain(peer)
		return
	}

	batch := ss.fetchList
	if len(batch) > maxFetchBatch {
		batch = batch[:maxFetchBatch]
	}
	ss.fetchList = ss.fetchList[len(batch):]
	ss.inflight = len(batch)
	ss.pending = make(map[tcommon.Hash]uint64, len(batch))
	for _, bid := range batch {
		ss.pending[bid.Hash] = bid.Num
	}
	peer := ss.syncPeer
	ss.armFetchTimer()
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
	log.Trace("Fetch sent", "blocks", len(batch), "peer", peer.ID())
}

// HandleBlock processes a received block during sync.
// Returns true if the block was consumed by sync, false if it should be handled as a broadcast.
func (ss *SyncService) HandleBlock(peer *p2p.Peer, block *types.Block) bool {
	ss.mu.Lock()
	if !ss.syncing || peer != ss.syncPeer {
		ss.mu.Unlock()
		return false
	}
	blockHash := block.Hash()
	blockNum := block.Number()
	expectedNum, ok := ss.pending[blockHash]
	if !ok || expectedNum != blockNum {
		ss.mu.Unlock()
		return true
	}
	delete(ss.pending, blockHash)
	// Bump seq so any in-flight timer callback short-circuits. We stop the
	// armed timer below but the callback may already be running on another
	// goroutine and waiting on ss.mu; the seq check inside onFetchTimeout
	// rejects it.
	ss.fetchSeq++
	if ss.inflight > 0 {
		ss.inflight--
	}
	batchDone := ss.inflight == 0
	if ss.fetchTimer != nil {
		ss.fetchTimer.Stop()
		ss.fetchTimer = nil
	}
	// Re-arm the fetch timeout if blocks are still in flight. Without
	// this a peer that delivers part of a batch and then stalls (network
	// blip, JVM GC pause, deliberate misbehaviour) leaves the sync state
	// machine wedged forever: batchDone stays false → fetchNextBatch
	// never runs → onFetchTimeout never fires → the watchdog's
	// IsSyncing() short-circuit keeps it from intervening either.
	if !batchDone {
		ss.armFetchTimer()
	}
	ss.mu.Unlock()

	insertStart := time.Now()
	insertErr := ss.chain.InsertBlock(block)
	insertElapsed := time.Since(insertStart)
	if insertErr != nil {
		// Stop sync without disconnecting the peer. Recovery (re-attempting
		// the same block from another peer) would just rediscover the same
		// failure when the root cause is gtron-side — far more likely than
		// peer-side given gtron is a re-impl racing toward java-tron parity.
		// Keeping the peer connected preserves diagnostic state. Cleared
		// only by process restart.
		log.Error("Sync paused",
			"number", block.Number(),
			"peer", peer.ID(),
			"err", insertErr,
			"hint", "restart to resume")
		ss.mu.Lock()
		ss.paused = true
		ss.pausedAtNum = block.Number()
		ss.pausedAtTime = time.Now()
		ss.pausedErr = insertErr
		ss.doReset()
		ss.mu.Unlock()
		return true
	}

	// Update sync stats and possibly emit the throttled "Imported chain
	// segment" summary. The window is reset on every emit so each line
	// represents a fresh rolling-window rate.
	ss.mu.Lock()
	ss.stats.blocks++
	ss.stats.totalBlocks++
	ss.stats.txs += len(block.Transactions())
	ss.stats.execElapsed += insertElapsed
	emit := time.Since(ss.stats.startTime) >= statsReportInterval ||
		(batchDone && len(ss.fetchList) == 0 && ss.remainNum == 0)
	var snap syncStats
	if emit {
		snap = ss.stats
		ss.stats.startTime = time.Now()
		ss.stats.blocks = 0
		ss.stats.txs = 0
		ss.stats.execElapsed = 0
	}
	remain := int64(len(ss.fetchList)) + ss.remainNum
	syncPeer := ss.syncPeer
	ss.mu.Unlock()

	if emit {
		ss.reportSegment(snap, block.Number(), remain, syncPeer)
	}

	// Only request the next batch when the current one is fully drained;
	// otherwise we'd overlap FETCH_INV_DATA requests and lose the one-batch
	// backpressure java-tron keeps with syncBlockRequested/isSyncIdle.
	if batchDone {
		ss.fetchNextBatch()
	}
	return true
}

// reportSegment emits the throttled "Imported chain segment" summary. Called
// without ss.mu held.
func (ss *SyncService) reportSegment(s syncStats, head uint64, remain int64, peer *p2p.Peer) {
	elapsed := time.Since(s.startTime)
	if elapsed <= 0 {
		elapsed = 1
	}
	blocksPerSec := float64(s.blocks) * float64(time.Second) / float64(elapsed)
	txsPerSec := float64(s.txs) * float64(time.Second) / float64(elapsed)

	ctx := []any{
		"blocks", s.blocks,
		"txs", s.txs,
		"elapsed", ethcommon.PrettyDuration(elapsed),
		"blocks/s", round2(blocksPerSec),
		"txs/s", round2(txsPerSec),
		"head", head,
		"remain", remain,
	}
	if blocksPerSec > 0 && remain > 0 {
		etaSec := float64(remain) / blocksPerSec
		ctx = append(ctx, "eta", ethcommon.PrettyDuration(time.Duration(etaSec*float64(time.Second))))
	}
	if peer != nil {
		ctx = append(ctx, "peer", peer.ID())
	}
	log.Info("Imported chain segment", ctx...)
}

func round2(f float64) float64 {
	// Trim to 2 decimals for log readability without depending on a printf
	// format directive (slog handlers print floats with full precision).
	return float64(int64(f*100+0.5)) / 100
}

// doReset clears all sync state. Must be called with ss.mu held.
func (ss *SyncService) doReset() {
	ss.syncing = false
	ss.syncPeer = nil
	ss.fetchList = nil
	ss.remainNum = 0
	ss.inflight = 0
	ss.pending = nil
	ss.fetchSeq++
	if ss.fetchTimer != nil {
		ss.fetchTimer.Stop()
		ss.fetchTimer = nil
	}
}

// armFetchTimer arms the fetch-response timeout. Must be called with ss.mu held.
func (ss *SyncService) armFetchTimer() {
	if ss.fetchTimer != nil {
		ss.fetchTimer.Stop()
	}
	seq := ss.fetchSeq
	stalePeer := ss.syncPeer
	ss.fetchTimer = time.AfterFunc(syncFetchTimeout, func() {
		ss.onFetchTimeout(seq, stalePeer)
	})
}

func (ss *SyncService) onFetchTimeout(seq uint64, stalePeer *p2p.Peer) {
	ss.mu.Lock()
	if !ss.syncing || ss.fetchSeq != seq || ss.syncPeer != stalePeer {
		ss.mu.Unlock()
		return
	}
	inflight := ss.inflight
	ss.doReset()
	ss.mu.Unlock()
	log.Warn("Fetch timeout, failing over",
		"peer", stalePeer.ID(),
		"timeout", ethcommon.PrettyDuration(syncFetchTimeout),
		"inflight", inflight)
	ss.tryFindSyncPeer(stalePeer)
}

// PeerDisconnected is called by the handler when a peer goes away. If that
// peer is the active sync peer, the sync is aborted and we immediately try
// to find a replacement.
func (ss *SyncService) PeerDisconnected(peer *p2p.Peer) {
	ss.mu.Lock()
	if !ss.syncing || ss.syncPeer != peer {
		ss.mu.Unlock()
		return
	}
	ss.doReset()
	ss.mu.Unlock()
	log.Warn("Sync peer disconnected", "peer", peer.ID())
	ss.tryFindSyncPeer(peer)
}

// tryFindSyncPeer picks the best available peer (excluding the failed one) and
// starts a new sync if one exists.
func (ss *SyncService) tryFindSyncPeer(exclude *p2p.Peer) {
	if ss.handler == nil {
		return
	}
	if p := ss.handler.BestSyncCandidate(exclude); p != nil {
		ss.StartSync(p)
	}
}

func (ss *SyncService) finishSync() {
	ss.mu.Lock()
	totalBlocks := ss.stats.totalBlocks
	totalStart := ss.stats.totalStart
	ss.doReset()
	ss.mu.Unlock()

	totalElapsed := time.Since(totalStart)
	ctx := []any{
		"head", ss.chain.CurrentBlock().Number(),
		"totalBlocks", totalBlocks,
		"totalElapsed", ethcommon.PrettyDuration(totalElapsed),
	}
	if totalElapsed > 0 && totalBlocks > 0 {
		rate := float64(totalBlocks) * float64(time.Second) / float64(totalElapsed)
		ctx = append(ctx, "avgBlocks/s", round2(rate))
	}
	log.Info("Sync complete", ctx...)
}
