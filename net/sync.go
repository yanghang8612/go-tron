package net

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Slice 1 of the SyncService refactor moved these tunables into
// net/sync/constants.go. The lowercase aliases here keep call sites and
// tests under net/ untouched until Slice 6 deletes net/sync.go entirely;
// at that point every remaining reference moves to tsync.* directly.
const (
	maxChainInventorySize   = tsync.MaxChainInventorySize
	maxFetchBatch           = tsync.MaxFetchBatch
	maxSyncImportBatch      = tsync.MaxImportBatch
	maxParallelSyncPeers    = tsync.MaxParallelSyncPeers
	minFetchRequestInterval = tsync.MinFetchRequestInterval
	peerJoinAttemptInterval = 2 * time.Second
)

type syncPeerState struct {
	peer *p2p.Peer

	fetchList []types.BlockID
	remainNum int64

	inflight   int
	pending    map[tcommon.Hash]uint64
	pendingIDs map[tcommon.Hash]types.BlockID

	// requestedHashes mirrors java-tron's syncBlockIdCache rule: never ask the
	// same peer for the same block hash twice, even after a timeout.
	requestedHashes map[tcommon.Hash]struct{}

	lastInventoryNum uint64
	minFetchNum      uint64

	fetchSeq        uint64
	fetchTimer      *time.Timer
	fetchDelayTimer *time.Timer
	nextFetchAt     time.Time
	chainRequested  bool
	done            bool
}

type outboundSyncRequest struct {
	peer   *p2p.Peer
	blocks []types.BlockID
	chain  bool
}

// SyncService handles the block sync protocol.
type SyncService struct {
	chain   *core.BlockChain
	handler *TronHandler

	drainMu    sync.Mutex
	drainCond  *sync.Cond
	draining   bool
	drainAgain bool
	stopping   atomic.Bool

	mu         sync.Mutex
	syncing    bool
	syncPeer   *p2p.Peer
	fetchList  []types.BlockID // blocks to fetch from peer
	remainNum  int64
	inflight   int // blocks requested but not yet received in the current batch
	pending    map[tcommon.Hash]uint64
	fetchSeq   uint64      // incremented on each fetch batch and on block receipt
	fetchTimer *time.Timer // fires if no block arrives within fetchTimeout

	// fetchTimeout is this service's block-fetch deadline, copied from
	// tsync.SyncFetchTimeout at construction. The timer goroutine reads it
	// (armPeerFetchTimerLocked / onFetchTimeout) without ss.mu held, so it
	// must stay a per-instance value: tests override it before sync starts
	// rather than mutating the shared package global from a defer.
	fetchTimeout time.Duration

	peers         map[string]*syncPeerState
	requested     map[tcommon.Hash]string
	retryList     []types.BlockID
	blockBuffer   map[uint64]syncdl.BufferedBlock
	bufferedHash  map[tcommon.Hash]struct{}
	blockPath     syncdl.BlockPath
	targetHeadNum uint64

	// Sticky pause set on any InsertBlock failure during sync. Once set,
	// StartSync / checkIsolation / tryFindSyncPeer all short-circuit; the
	// SyncBlockChain handler still serves outbound peers. The peer that
	// delivered the bad block is NOT disconnected — gtron is the more
	// likely culprit than a peer (re-impl racing toward parity), so we keep
	// the connection so the operator can diagnose without losing peer
	// state. Cleared only by process restart. The gate owns its own
	// mutex; lock order is always ss.mu (outer) → pause.mu (inner) when
	// both are held. Read sites (onPeerFetchReady, drainBufferedBlocks,
	// shouldFinishLocked) hold ss.mu and then call Paused(); Enter is
	// called outside ss.mu so write paths never nest.
	pause *tsync.PauseGate

	// stats accumulates per-window throughput counters used for the
	// "Imported chain segment" summary line. Owns its own mutex; lock
	// order is ss.mu (outer) → stats.mu (inner) when both are held.
	// onApplyStats is the only writer that does NOT also hold ss.mu —
	// stats.mu serializes its own state so the off-sync producer path
	// is safe.
	stats *tsync.Stats

	importBatchSize int

	// watchdog runs the periodic isolation check. Owns its own goroutine
	// and ticker; Start/Stop fan-out launches and joins it.
	watchdog *tsync.Watchdog

	bufferWait syncdl.BufferWaitTracker

	lastPeerJoinAttempt time.Time
}

// chainStatusAdapter adapts *core.BlockChain to tsync.ChainStatus by adding
// a CurrentBlockNum accessor that unwraps CurrentBlock().Number() — keeps
// net/sync free of core/types imports.
type chainStatusAdapter struct{ chain *core.BlockChain }

func (a chainStatusAdapter) LastInsertTime() time.Time { return a.chain.LastInsertTime() }
func (a chainStatusAdapter) CurrentBlockNum() uint64   { return a.chain.CurrentBlock().Number() }

// NewSyncService creates a new sync service.
func NewSyncService(chain *core.BlockChain, handler *TronHandler) *SyncService {
	ss := &SyncService{
		chain:           chain,
		handler:         handler,
		pause:           tsync.NewPauseGate(),
		stats:           tsync.NewStats(),
		fetchTimeout:    tsync.SyncFetchTimeout,
		importBatchSize: maxSyncImportBatch,
	}
	ss.drainCond = sync.NewCond(&ss.drainMu)
	ss.watchdog = tsync.NewWatchdog(
		chainStatusAdapter{chain: chain},
		watchdogPeerSource{handler: handler},
		ss.pause,
		ss,
		watchdogLog,
	)
	// Subscribe to per-block phase breakdowns so the throttled "Imported chain
	// segment" line can show validate/execute/maintenance/stateCommit/dpUpdate/
	// persist/hooks alongside the existing execElapsed total.
	chain.AddApplyStatsHook(ss.onApplyStats)
	return ss
}

// SetImportBatchSize changes the local staged-body import chunk. It never
// changes the java-tron-compatible FETCH_INV_DATA request size; it only bounds
// how many already staged bodies are decoded/executed in one local range pass.
func (ss *SyncService) SetImportBatchSize(size int) error {
	if ss == nil {
		return fmt.Errorf("nil sync service")
	}
	if size <= 0 {
		return fmt.Errorf("sync import batch must be >= 1")
	}
	if size > maxFetchBatch {
		return fmt.Errorf("sync import batch %d exceeds fetch batch %d", size, maxFetchBatch)
	}
	ss.mu.Lock()
	ss.importBatchSize = size
	ss.mu.Unlock()
	return nil
}

// watchdogPeerSource adapts a possibly-nil *TronHandler to tsync.PeerSource;
// when handler is nil (unit-test scaffold) the adapter reports no peers so
// checkIsolation short-circuits without dereferencing.
type watchdogPeerSource struct{ handler *TronHandler }

func (w watchdogPeerSource) BestSyncCandidate(exclude *p2p.Peer) *p2p.Peer {
	if w.handler == nil {
		return nil
	}
	return w.handler.BestSyncCandidate(exclude)
}

func (w watchdogPeerSource) HandshakedPeers() []*p2p.Peer {
	if w.handler == nil {
		return nil
	}
	return w.handler.HandshakedPeers()
}

// watchdogLog mirrors the pre-refactor "Polling peer (chain stalled)" Info
// line emitted from checkIsolation. Routed through the net package logger so
// the module=net tag stays consistent across all sync log lines.
func watchdogLog(peer *p2p.Peer, head uint64, stalledFor time.Duration) {
	syncLog.Info("Polling peer (chain stalled)",
		"peer", peer.ID(),
		"head", head,
		"stalledFor", ethcommon.PrettyDuration(stalledFor))
}

// onApplyStats folds one block's per-phase wall-clock breakdown into the
// rolling window. Fires synchronously from applyBlock on the importing
// goroutine — during sync that is drainBufferedBlocks; during normal
// operation it is the broadcast/producer path. Stats owns its own mutex
// so no ss.mu acquisition here; this matters because the producer path
// may invoke applyBlock from a goroutine that already holds the producer
// lock, and we don't want to deadlock with any future ss.mu holder.
func (ss *SyncService) onApplyStats(_ *types.Block, s core.ApplyStats) {
	ss.stats.AddApplyBlock(s)
}

// Start launches the isolation watchdog goroutine.
func (ss *SyncService) Start() {
	ss.stopping.Store(false)
	if ss.watchdog != nil {
		ss.watchdog.Start()
	}
}

// Stop shuts down the sync service, cancels any in-progress sync, and waits
// for the active drain to leave InsertBlocks before shutdown continues.
func (ss *SyncService) Stop() {
	ss.stopping.Store(true)
	if ss.watchdog != nil {
		ss.watchdog.Stop()
	}
	ss.mu.Lock()
	ss.doReset()
	ss.mu.Unlock()
	ss.waitForDrain()
}

// IsSyncing returns whether sync is in progress.
func (ss *SyncService) IsSyncing() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.syncing
}

// SyncRemainingBlocks reports the current sync backlog when a sync session is
// active. The value is advisory and intended for background workers that should
// avoid competing with deep catch-up imports.
func (ss *SyncService) SyncRemainingBlocks() (int64, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if !ss.syncing || ss.pause.Paused() {
		return 0, false
	}
	remaining := ss.estimatedRemainLocked()
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// IsPaused reports whether sync has been stopped by a prior InsertBlock failure.
// While paused, no new sync starts and the watchdog skips its kick — but peers
// stay connected and inbound SYNC_BLOCK_CHAIN requests are still served.
func (ss *SyncService) IsPaused() bool {
	return ss.pause.Paused()
}

// PausedStatus returns the pause flag along with the block number, time, and
// error captured when the pause was triggered. Intended for status reporting.
func (ss *SyncService) PausedStatus() (paused bool, atNum uint64, at time.Time, err error) {
	return ss.pause.Status()
}

// BuildChainSummary returns the exponentially-spaced summary of our
// chain used in SYNC_BLOCK_CHAIN messages. Slice 1 of the SyncService
// refactor moved the implementation to net/sync/downloader; the wrapper
// stays on SyncService so external tests / call sites under net/ keep
// using the method form until slice 4 migrates them.
func (ss *SyncService) BuildChainSummary() []types.BlockID {
	return syncdl.BuildChainSummary(ss.chain)
}

// FindCommonBlock finds the highest block in peerSummary that exists in our
// chain. Wrapper for slice-1 compatibility; see syncdl.FindCommonBlock.
func (ss *SyncService) FindCommonBlock(peerSummary []types.BlockID) uint64 {
	return syncdl.FindCommonBlock(ss.chain, peerSummary)
}

// StartSync initiates sync with a peer that has a higher head block.
func (ss *SyncService) StartSync(peer *p2p.Peer) {
	if peer == nil {
		return
	}
	if ss.stopping.Load() {
		return
	}
	if ss.pause.Paused() {
		return
	}
	needFrom := ss.chain.CurrentBlock().Number() + 1
	if ss.handler != nil {
		ok, lowest, head := ss.handler.syncPeerCanServe(peer, needFrom)
		if !ok {
			syncLog.Info("Skipping sync peer outside available range",
				"peer", peer.ID(),
				"needFrom", needFrom,
				"peerLowest", lowest,
				"peerHead", head)
			return
		}
	}
	now := time.Now()
	ss.mu.Lock()
	started := false
	if !ss.syncing {
		ss.initSessionLocked(now)
		started = true
	}
	ps, added := ss.addPeerStateLocked(peer)
	if !added {
		ss.mu.Unlock()
		return
	}
	ps.chainRequested = true
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()

	if started {
		syncLog.Info("Sync started",
			"peer", peer.ID(),
			"localHead", ss.chain.CurrentBlock().Number())
	} else {
		syncLog.Debug("Sync peer joined", "peer", peer.ID())
	}
	ss.sendSyncBlockChain(peer)
	ss.joinAvailablePeers()
}

func (ss *SyncService) initSessionLocked(now time.Time) {
	ss.syncing = true
	ss.syncPeer = nil
	ss.fetchList = nil
	ss.remainNum = 0
	ss.inflight = 0
	ss.pending = nil
	ss.fetchSeq = 0
	ss.fetchTimer = nil
	ss.peers = make(map[string]*syncPeerState)
	ss.requested = make(map[tcommon.Hash]string)
	ss.retryList = nil
	ss.blockBuffer = make(map[uint64]syncdl.BufferedBlock)
	ss.bufferedHash = make(map[tcommon.Hash]struct{})
	ss.blockPath = syncdl.NewBlockPath()
	headBlock := ss.chain.CurrentBlock()
	if headBlock == nil {
		headBlock = ss.chain.GetBlockByNumber(0)
	}
	var head uint64
	if headBlock != nil {
		head = headBlock.Number()
	}
	startup := syncdl.PlanSessionStartup(syncdl.SessionStartupInput{
		Head:         head,
		RestoreLimit: maxFetchBatch,
	})
	ss.repairSyncPipelineProgress(headBlock)
	ss.targetHeadNum = ss.restoreSyncInventoryTarget(startup.InventoryFloor)
	ss.deleteImportedSyncBodiesThrough(startup.DeleteImportedThrough)
	ss.restoreSyncStagedBodiesLocked(startup.RestoreStagedBodiesFrom, startup.RestoreLimit, startup.PruneStaleTail)
	ss.writeSyncBodiesReadyProgress()
	ss.stats.InitSession(now)
	ss.bufferWait.Reset()
	if startup.ResetPeerJoinThrottle {
		ss.lastPeerJoinAttempt = time.Time{}
	}
}

func (ss *SyncService) ensureSessionMapsLocked() {
	if ss.peers == nil {
		ss.peers = make(map[string]*syncPeerState)
	}
	if ss.requested == nil {
		ss.requested = make(map[tcommon.Hash]string)
	}
	if ss.blockBuffer == nil {
		ss.blockBuffer = make(map[uint64]syncdl.BufferedBlock)
	}
	if ss.bufferedHash == nil {
		ss.bufferedHash = make(map[tcommon.Hash]struct{})
	}
	if ss.blockPath == nil {
		ss.blockPath = syncdl.NewBlockPath()
	}
}

func (ss *SyncService) restoreSyncInventoryTarget(head uint64) uint64 {
	if ss == nil || ss.chain == nil {
		return head
	}
	restore := rawdb.RestoreSyncInventoryTarget(ss.chain.DB(), head)
	if restore.ReadError != nil {
		syncLog.Warn("Read sync inventory stage progress failed", "err", restore.ReadError)
	}
	return restore.Target
}

func (ss *SyncService) repairSyncPipelineProgress(head *types.Block) {
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		ss.repairSyncStageProgress(head, stage)
	}
}

func (ss *SyncService) repairSyncStageProgress(head *types.Block, stage rawdb.StageID) {
	if ss == nil || ss.chain == nil || head == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	repair := syncdl.RepairSyncStageProgress(db, stage, head.Number(), func(number uint64) (tcommon.Hash, bool) {
		block := ss.chain.GetBlockByNumber(number)
		if block == nil {
			return tcommon.Hash{}, false
		}
		return block.Hash(), true
	})
	switch repair.Status {
	case syncdl.SyncStageProgressReadError:
		syncLog.Warn("Read sync stage progress failed", "stage", stage, "err", repair.ReadError)
		return
	case syncdl.SyncStageProgressDeleteError:
		syncLog.Warn("Delete stale sync stage progress failed", "stage", stage, "block", repair.Row.BlockNum, "hash", repair.Row.BlockHash, "err", repair.DeleteError)
		return
	case syncdl.SyncStageProgressDeleted:
		syncLog.Debug("Deleted stale sync stage progress", "stage", stage, "block", repair.Row.BlockNum, "hash", repair.Row.BlockHash, "head", head.Number(), "headHash", head.Hash())
		return
	}
}

func (ss *SyncService) restoreSyncStagedBodiesLocked(start uint64, limit int, pruneStaleTail bool) {
	if ss == nil || ss.chain == nil || limit <= 0 {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	result := syncdl.RestoreStagedBodies(start, limit, ss.targetHeadNum, ss.blockBuffer, ss.bufferedHash, &ss.blockPath, func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error {
		return rawdb.IterateSyncStagedBlocksFrom(db, start, fn)
	})
	ss.targetHeadNum = result.TargetHead
	if result.ReadError != nil {
		syncLog.Warn("Read sync staged block range failed", "from", result.NextExpected, "err", result.ReadError)
	}
	if pruneStaleTail && result.NeedPruneTail {
		ss.deleteStaleSyncBodiesFrom(result.PruneFrom, result.LastRestoredNum, result.LastRestoredHash, result.HaveLastRestored)
	}
}

func (ss *SyncService) addPeerStateLocked(peer *p2p.Peer) (*syncPeerState, bool) {
	if peer == nil {
		return nil, false
	}
	ss.ensureSessionMapsLocked()
	if ps := ss.peers[peer.ID()]; ps != nil {
		return ps, false
	}
	ps := &syncPeerState{
		peer:            peer,
		pending:         make(map[tcommon.Hash]uint64),
		pendingIDs:      make(map[tcommon.Hash]types.BlockID),
		requestedHashes: make(map[tcommon.Hash]struct{}),
	}
	ss.peers[peer.ID()] = ps
	if ss.syncPeer == nil {
		ss.syncPeer = peer
	}
	return ps, true
}

func (ss *SyncService) ensurePeerStateLocked(peer *p2p.Peer) *syncPeerState {
	if peer == nil {
		return nil
	}
	ss.ensureSessionMapsLocked()
	if ps := ss.peers[peer.ID()]; ps != nil {
		return ps
	}
	ps, _ := ss.addPeerStateLocked(peer)
	if peer == ss.syncPeer {
		ps.fetchList = append(ps.fetchList, ss.fetchList...)
		ps.remainNum = ss.remainNum
		ps.inflight = ss.inflight
		if ss.pending != nil {
			ps.pending = ss.pending
			for h, n := range ss.pending {
				bid := types.BlockID{Hash: h, Num: n}
				ps.pendingIDs[h] = bid
				ss.requested[h] = peer.ID()
			}
		}
		ps.fetchSeq = ss.fetchSeq
		ps.fetchTimer = ss.fetchTimer
	}
	return ps
}

func (ss *SyncService) mirrorLegacyLocked() {
	if ss.syncPeer == nil {
		ss.fetchList = nil
		ss.remainNum = 0
		ss.inflight = 0
		ss.pending = nil
		ss.fetchSeq = 0
		ss.fetchTimer = nil
		return
	}
	ps := ss.peers[ss.syncPeer.ID()]
	if ps == nil {
		ss.fetchList = nil
		ss.remainNum = 0
		ss.inflight = 0
		ss.pending = nil
		ss.fetchTimer = nil
		return
	}
	ss.fetchList = ps.fetchList
	ss.remainNum = ps.remainNum
	ss.inflight = ps.inflight
	ss.pending = ps.pending
	ss.fetchSeq = ps.fetchSeq
	ss.fetchTimer = ps.fetchTimer
}

func (ss *SyncService) joinAvailablePeers() {
	if ss.handler == nil {
		return
	}
	needFrom := ss.chain.CurrentBlock().Number() + 1
	ss.mu.Lock()
	need := syncdl.PeerJoinCapacity(len(ss.peers), maxParallelSyncPeers)
	exclude := make(map[string]struct{}, len(ss.peers))
	for id := range ss.peers {
		exclude[id] = struct{}{}
	}
	ss.mu.Unlock()
	if need <= 0 {
		return
	}
	candidates := ss.handler.SyncCandidates(exclude, need)
	for _, peer := range candidates {
		if peer != nil {
			exclude[peer.ID()] = struct{}{}
		}
	}
	if len(candidates) < need {
		for _, peer := range ss.handler.HandshakedPeers() {
			if peer == nil {
				continue
			}
			if _, skip := exclude[peer.ID()]; skip {
				continue
			}
			if ok, _, _ := ss.handler.syncPeerCanServe(peer, needFrom); !ok {
				continue
			}
			candidates = append(candidates, peer)
			exclude[peer.ID()] = struct{}{}
			if len(candidates) >= need {
				break
			}
		}
	}
	for _, peer := range candidates {
		ss.StartSync(peer)
	}
}

func (ss *SyncService) shouldJoinAvailablePeersLocked(now time.Time) bool {
	plan := syncdl.PlanPeerJoinAttempt(syncdl.PeerJoinAttemptInput{
		HandlerAvailable: ss.handler != nil,
		Syncing:          ss.syncing,
		Paused:           ss.pause.Paused(),
		CurrentPeers:     len(ss.peers),
		MaxPeers:         maxParallelSyncPeers,
		LastAttempt:      ss.lastPeerJoinAttempt,
		Now:              now,
		MinInterval:      peerJoinAttemptInterval,
	})
	if !plan.Allowed {
		return false
	}
	ss.lastPeerJoinAttempt = now
	return true
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
	var stageInventoryTarget uint64

	ss.mu.Lock()
	if !ss.syncing {
		ss.mu.Unlock()
		return
	}
	ps := ss.peers[peer.ID()]
	if ps == nil && peer == ss.syncPeer {
		ps = ss.ensurePeerStateLocked(peer)
	}
	if ps == nil {
		ss.mu.Unlock()
		return
	}
	ps.chainRequested = false
	headNum := ss.chain.CurrentBlock().Number()
	for _, bid := range inv.Ids {
		num := uint64(bid.Number)
		hash := tcommon.BytesToHash(bid.Hash)
		facts := syncdl.InventoryCandidateFacts{}
		if num <= headNum {
			if existing := ss.chain.GetBlockByNumber(num); existing != nil && existing.Hash() == hash {
				facts.KnownOrRequested = true
			}
		}
		if !facts.KnownOrRequested && ss.chain.HasBlockInKhaosDB(hash) {
			facts.KnownOrRequested = true
		}
		if !facts.KnownOrRequested {
			_, facts.KnownOrRequested = ss.bufferedHash[hash]
		}
		if !facts.KnownOrRequested {
			_, facts.KnownOrRequested = ss.requested[hash]
		}
		if !facts.KnownOrRequested {
			_, facts.PeerRequested = ps.requestedHashes[hash]
		}
		bid := types.BlockID{Hash: hash, Num: num}
		if !facts.KnownOrRequested && !facts.PeerRequested {
			facts.ReservedPath = ss.reserveBlockPathLocked(bid)
		}
		if syncdl.AcceptInventoryCandidate(facts) {
			ps.fetchList = append(ps.fetchList, bid)
		}
	}
	ps.remainNum = inv.RemainNum
	if len(inv.Ids) > 0 {
		last := inv.Ids[len(inv.Ids)-1]
		if last.Number > 0 {
			target := syncdl.ObserveInventoryTarget(ss.targetHeadNum, uint64(last.Number), inv.RemainNum, maxChainInventorySize)
			ps.lastInventoryNum = target.Window.Max
			ps.minFetchNum = target.Window.Min
			ss.targetHeadNum = target.Target
			stageInventoryTarget = target.StageTarget
		}
	}

	// java-tron sets `needSyncFromUs = false` on its peer record only when
	// our summary's last block matches its head (lostBlockIds.size == 1).
	// While needSyncFromUs is true, java-tron's InventoryMsgHandler drops
	// every inbound INV — so our outbound TRX advertisements never reach
	// the producer's mempool. Detect "we are at head" here (response is a
	// single id we already have) and finish; otherwise continue fetching.
	if syncdl.ShouldMarkInventoryDone(len(inv.Ids), len(ps.fetchList), inv.RemainNum) {
		ps.done = true
	}

	syncLog.Debug("Chain inventory received",
		"blocks", len(inv.Ids), "queued", len(ps.fetchList), "remain", inv.RemainNum, "peer", peer.ID())
	out := ss.fillFetchSlotsLocked(time.Now())
	restart := len(out) == 0 && ss.shouldRestartForStalledRetriesLocked()
	complete := false
	if restart {
		ss.doReset()
	} else {
		complete = ss.shouldFinishLocked()
		ss.mirrorLegacyLocked()
	}
	ss.mu.Unlock()

	if stageInventoryTarget > 0 {
		ss.writeStageProgress(rawdb.StageSyncInventory, stageInventoryTarget, tcommon.Hash{}, false)
	}

	ss.sendOutboundRequests(out)
	if restart {
		ss.tryFindSyncPeer(nil)
		return
	}
	if complete {
		ss.finishSync()
	}
}

func (ss *SyncService) fetchNextBatch() {
	ss.mu.Lock()
	if ss.syncPeer != nil {
		ss.ensurePeerStateLocked(ss.syncPeer)
	}
	out := ss.fillFetchSlotsLocked(time.Now())
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()
	ss.sendOutboundRequests(out)
}

func (ss *SyncService) fillFetchSlotsLocked(now time.Time) []outboundSyncRequest {
	ss.ensureSessionMapsLocked()
	var out []outboundSyncRequest
	for _, ps := range ss.peers {
		if ps == nil || ps.peer == nil || ps.done || ps.chainRequested || ps.inflight > 0 {
			continue
		}
		ss.assignRetryLocked(ps)
		batch := ss.nextFetchBatchLocked(ps)
		currentHead := uint64(0)
		if len(batch) == 0 {
			currentHead = ss.chain.CurrentBlock().Number()
		}
		fetchWait := time.Duration(0)
		if len(batch) > 0 {
			fetchWait = time.Until(ps.nextFetchAt)
		}
		plan := syncdl.PlanReadyPeerFetch(syncdl.ReadyPeerFetchInput{
			BatchLen:     len(batch),
			FetchWait:    fetchWait,
			Done:         ps.done,
			InventoryTip: ps.lastInventoryNum,
			CurrentHead:  currentHead,
		})
		switch plan.Action {
		case syncdl.PeerFetchWaitLocalHead:
			// java-tron rejects a follow-up SYNC_BLOCK_CHAIN if the
			// summary tail is below the last inventory tip it sent us
			// on this peer (lastSyncNum > lastNum). Wait until the
			// canonical head catches up before asking this peer for
			// the next 2000-block window.
			syncLog.Trace("Sync peer waiting for local head",
				"peer", ps.peer.ID(),
				"head", currentHead,
				"inventoryTip", ps.lastInventoryNum)
			continue
		case syncdl.PeerFetchRequestInventory:
			// Always re-poll once a peer's local queue drains. java-tron may
			// have produced new blocks while we were applying the previous
			// batch; the one-id inventory response is what marks sync done.
			ps.chainRequested = true
			out = append(out, outboundSyncRequest{peer: ps.peer, chain: true})
			continue
		case syncdl.PeerFetchDelay:
			ps.fetchList = append(batch, ps.fetchList...)
			ss.armPeerDelayTimerLocked(ps, plan.Wait)
			continue
		case syncdl.PeerFetchSend:
		default:
			continue
		}
		request := syncdl.NewFetchRequestState(batch)
		ps.inflight = request.Inflight
		ps.pending = request.Pending
		ps.pendingIDs = request.PendingIDs
		for _, hash := range request.RequestedHashes {
			ps.requestedHashes[hash] = struct{}{}
			ss.requested[hash] = ps.peer.ID()
		}
		ps.nextFetchAt = now.Add(minFetchRequestInterval)
		ss.armPeerFetchTimerLocked(ps)
		out = append(out, outboundSyncRequest{peer: ps.peer, blocks: batch})
	}
	return out
}

func (ss *SyncService) assignRetryLocked(ps *syncPeerState) {
	if len(ss.retryList) == 0 {
		return
	}
	window := syncdl.FetchWindow{Min: ps.minFetchNum, Max: ps.lastInventoryNum}
	assigned, keep := syncdl.AssignRetryCandidates(ss.retryList, func(bid types.BlockID) syncdl.RetryDecision {
		facts := syncdl.RetryCandidateFacts{KnownOrRequested: ss.hasBlockOrRequestLocked(bid)}
		if !facts.KnownOrRequested {
			facts.InWindow = window.Contains(bid)
		}
		if facts.InWindow {
			_, facts.PeerRequested = ps.requestedHashes[bid.Hash]
		}
		if facts.InWindow && !facts.PeerRequested {
			facts.ReservedPath = ss.reserveBlockPathLocked(bid)
		}
		return syncdl.ClassifyRetryCandidate(facts)
	})
	ps.fetchList = append(ps.fetchList, assigned...)
	ss.retryList = keep
}

func (ss *SyncService) nextFetchBatchLocked(ps *syncPeerState) []types.BlockID {
	if len(ps.fetchList) == 0 {
		return nil
	}
	batch, remaining := syncdl.PopFetchBatch(ps.fetchList, maxFetchBatch, func(bid types.BlockID) bool {
		facts := syncdl.FetchCandidateFacts{KnownOrRequested: ss.hasBlockOrRequestLocked(bid)}
		if !facts.KnownOrRequested {
			facts.ReservedPath = ss.reserveBlockPathLocked(bid)
		}
		if facts.ReservedPath {
			_, facts.PeerRequested = ps.requestedHashes[bid.Hash]
		}
		return syncdl.AcceptFetchCandidate(facts)
	})
	ps.fetchList = remaining
	return batch
}

func (ss *SyncService) hasBlockOrRequestLocked(bid types.BlockID) bool {
	if ss.blockPath.Conflicts(bid) {
		return true
	}
	if _, ok := ss.requested[bid.Hash]; ok {
		return true
	}
	if _, ok := ss.bufferedHash[bid.Hash]; ok {
		return true
	}
	headNum := ss.chain.CurrentBlock().Number()
	if bid.Num <= headNum {
		if existing := ss.chain.GetBlockByNumber(bid.Num); existing != nil && existing.Hash() == bid.Hash {
			return true
		}
	}
	return ss.chain.HasBlockInKhaosDB(bid.Hash)
}

func (ss *SyncService) reserveBlockPathLocked(bid types.BlockID) bool {
	var ok bool
	ss.blockPath, ok = ss.blockPath.Reserve(bid)
	return ok
}

func (ss *SyncService) sendOutboundRequests(out []outboundSyncRequest) {
	for _, req := range out {
		if req.peer == nil {
			continue
		}
		if req.chain {
			ss.sendSyncBlockChain(req.peer)
			continue
		}
		ss.sendFetchBlocks(req.peer, req.blocks)
	}
}

func (ss *SyncService) sendFetchBlocks(peer *p2p.Peer, batch []types.BlockID) {
	if len(batch) == 0 {
		return
	}
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
	syncLog.Trace("Fetch sent", "blocks", len(batch), "peer", peer.ID())
}

func (ss *SyncService) armPeerDelayTimerLocked(ps *syncPeerState, wait time.Duration) {
	if ps.fetchDelayTimer != nil {
		ps.fetchDelayTimer.Stop()
	}
	peerID := ps.peer.ID()
	ps.fetchDelayTimer = time.AfterFunc(wait, func() {
		ss.onPeerFetchReady(peerID)
	})
}

func (ss *SyncService) onPeerFetchReady(peerID string) {
	ss.mu.Lock()
	if !ss.syncing || ss.pause.Paused() {
		ss.mu.Unlock()
		return
	}
	if ps := ss.peers[peerID]; ps != nil {
		ps.fetchDelayTimer = nil
	}
	out := ss.fillFetchSlotsLocked(time.Now())
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()
	ss.sendOutboundRequests(out)
}

// HandleBlock processes a received block during sync.
// Returns true if the block was consumed by sync, false if it should be handled
// as a broadcast. `raw` is the block's exact wire bytes (the decode source);
// the buffer stores those rather than the decoded block. Callers without the
// original bytes may pass nil — they are re-marshaled from `block`.
func (ss *SyncService) HandleBlock(peer *p2p.Peer, block *types.Block, raw []byte) bool {
	if ss.stopping.Load() {
		return true
	}
	ss.mu.Lock()
	if !ss.syncing {
		ss.mu.Unlock()
		return false
	}
	ps := ss.peers[peer.ID()]
	if ps == nil && peer == ss.syncPeer {
		ps = ss.ensurePeerStateLocked(peer)
	}
	if ps == nil {
		ss.mu.Unlock()
		return false
	}
	blockHash := block.Hash()
	blockNum := block.Number()
	ack := syncdl.AcknowledgeFetchReceipt(syncdl.FetchReceiptState{
		Inflight:   ps.inflight,
		Pending:    ps.pending,
		PendingIDs: ps.pendingIDs,
	}, blockHash, blockNum)
	if !ack.Accepted {
		ss.mu.Unlock()
		return true
	}
	delete(ss.requested, blockHash)
	// Bump seq so any in-flight timer callback short-circuits. We stop the
	// armed timer below but the callback may already be running on another
	// goroutine and waiting on ss.mu; the seq check inside onFetchTimeout
	// rejects it.
	ps.fetchSeq++
	ps.inflight = ack.Inflight
	batchDone := ack.BatchDone
	if ps.fetchTimer != nil {
		ps.fetchTimer.Stop()
		ps.fetchTimer = nil
	}
	// Re-arm the fetch timeout if blocks are still in flight. Without
	// this a peer that delivers part of a batch and then stalls (network
	// blip, JVM GC pause, deliberate misbehaviour) leaves the sync state
	// machine wedged forever: batchDone stays false → fetchNextBatch
	// never runs → onFetchTimeout never fires → the watchdog's
	// IsSyncing() short-circuit keeps it from intervening either.
	if !batchDone {
		ss.armPeerFetchTimerLocked(ps)
	}
	if blockNum > ss.chain.CurrentBlock().Number() {
		bid := types.BlockID{Hash: blockHash, Num: blockNum}
		if existing, ok := ss.blockBuffer[blockNum]; ok {
			if existing.Hash != blockHash {
				syncLog.Debug("Dropping conflicting buffered sync block",
					"number", blockNum, "hash", blockHash, "kept", existing.Hash, "peer", peer.ID())
			}
		} else if _, ok := ss.bufferedHash[blockHash]; !ok && ss.reserveBlockPathLocked(bid) {
			ss.stageSyncBody(block, raw)
			ss.blockBuffer[blockNum] = syncdl.NewBufferedBlock(peer, block, raw)
			ss.bufferedHash[blockHash] = struct{}{}
		}
	}
	var out []outboundSyncRequest
	if batchDone {
		out = ss.fillFetchSlotsLocked(time.Now())
	}
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()

	ss.drainBufferedBlocks()
	if len(out) > 0 && ss.IsSyncing() && !ss.IsPaused() {
		ss.sendOutboundRequests(out)
	}
	return true
}

func (ss *SyncService) drainBufferedBlocks() {
	ss.drainMu.Lock()
	if ss.draining {
		ss.drainAgain = true
		ss.drainMu.Unlock()
		return
	}
	ss.draining = true
	ss.drainMu.Unlock()

	for {
		ss.drainBufferedBlocksOnce()
		ss.drainMu.Lock()
		if !ss.drainAgain {
			ss.draining = false
			if ss.drainCond != nil {
				ss.drainCond.Broadcast()
			}
			ss.drainMu.Unlock()
			return
		}
		ss.drainAgain = false
		ss.drainMu.Unlock()
	}
}

func (ss *SyncService) drainBufferedBlocksOnce() {
	var out []outboundSyncRequest
	for {
		now := time.Now()
		ss.mu.Lock()
		if !ss.syncing || ss.pause.Paused() {
			ss.mu.Unlock()
			break
		}
		batch := ss.popBufferedSyncBatchLocked(now)
		if len(batch.Buffered) == 0 {
			next := ss.chain.CurrentBlock().Number() + 1
			ss.bufferWait.Begin(next, now)
			out = append(out, ss.fillFetchSlotsLocked(now)...)
			complete := ss.shouldFinishLocked()
			joinPeers := !complete && ss.shouldJoinAvailablePeersLocked(now)
			ss.mirrorLegacyLocked()
			ss.mu.Unlock()
			if joinPeers {
				ss.joinAvailablePeers()
			}
			if complete {
				ss.finishSync()
			}
			break
		}
		ss.mu.Unlock()
		// Decode off-lock — see decodeBatchBlocks. Keeps the heavy proto work
		// off the central sync mutex so receiving peers aren't stalled.
		ss.decodeBatchBlocks(&batch)
		if len(batch.Blocks) == 0 {
			// Every popped block failed to decode (can't happen for validated
			// wire bytes). The entries were already removed at pop, so loop to
			// re-pop the next run or hit the gap.
			continue
		}
		for _, wait := range batch.BufferWaits {
			ss.stats.AddBufferWait(wait)
		}

		stageProgress := syncdl.NewStageProgressCollector()
		insertStart := time.Now()
		insertErr := ss.chain.InsertBlocksWithStageHook(batch.Blocks, stageProgress.Observe)
		insertElapsed := time.Since(insertStart)
		applied := len(batch.Blocks)
		if insertErr != nil {
			failure := syncdl.ResolveImportFailure(batch, insertErr)
			if failure.OK {
				applied = failure.Applied
				ss.recordImportedBatch(batch, applied, insertElapsed, stageProgress)
				ss.pauseSync(failure.Failed.Peer, failure.FailedNum, insertErr)
			} else {
				ss.pauseSync(nil, 0, insertErr)
			}
			break
		}
		ss.recordImportedBatch(batch, applied, insertElapsed, stageProgress)
	}
	ss.sendOutboundRequests(out)
}

func (ss *SyncService) waitForDrain() {
	ss.drainMu.Lock()
	if ss.draining {
		ss.drainAgain = true
	}
	for ss.draining {
		ss.drainCond.Wait()
	}
	ss.drainMu.Unlock()
}

func (ss *SyncService) importBatchLimitLocked() int {
	if ss == nil || ss.importBatchSize <= 0 {
		return maxSyncImportBatch
	}
	if ss.importBatchSize > maxFetchBatch {
		return maxFetchBatch
	}
	return ss.importBatchSize
}

func (ss *SyncService) popBufferedSyncBatchLocked(now time.Time) syncdl.BufferedBatch {
	next := ss.chain.CurrentBlock().Number() + 1
	readyLimit, hasReadyLimit := ss.syncBodiesReadyDrainLimit(next)
	restoreLimit, ok := syncdl.StagedBodyDrainLimit(next, ss.importBatchLimitLocked(), readyLimit, hasReadyLimit)
	if !ok {
		return syncdl.BufferedBatch{}
	}
	ss.restoreSyncStagedBodiesLocked(next, restoreLimit, false)
	return syncdl.PopBufferedBatch(ss.blockBuffer, ss.bufferedHash, ss.blockPath, &ss.bufferWait, next, restoreLimit, now)
}

func (ss *SyncService) syncBodiesReadyDrainLimit(next uint64) (uint64, bool) {
	if ss == nil || ss.chain == nil {
		return 0, false
	}
	db := ss.chain.DB()
	if db == nil {
		return 0, false
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil {
		syncLog.Warn("Read sync bodies ready stage progress failed", "err", err)
		return 0, false
	}
	if !ok {
		return 0, false
	}
	var (
		staged   rawdb.SyncStagedBlockRow
		stagedOK bool
		readErr  error
	)
	if row.HasBlockHash && row.BlockNum >= next {
		staged, stagedOK, readErr = rawdb.ReadSyncStagedBlockRaw(db, row.BlockNum)
	}
	limit := syncdl.ValidateStagedBodyReadyDrainLimit(next, row, true, staged, stagedOK, readErr)
	switch limit.Status {
	case syncdl.StagedBodyReadyLimitValid:
		return limit.Limit, true
	case syncdl.StagedBodyReadyLimitUnbound:
		syncLog.Warn("Ignoring unbound sync bodies ready stage progress", "block", row.BlockNum)
		return 0, false
	case syncdl.StagedBodyReadyLimitStale:
		ss.writeSyncBodiesReadyProgress()
		return 0, false
	case syncdl.StagedBodyReadyLimitReadError:
		syncLog.Warn("Read staged block for sync bodies ready limit failed", "block", row.BlockNum, "hash", row.BlockHash, "err", limit.ReadError)
		return 0, false
	case syncdl.StagedBodyReadyLimitStagedMissing:
		syncLog.Warn("Ignoring sync bodies ready stage without matching staged block", "block", row.BlockNum, "hash", row.BlockHash)
		return 0, false
	case syncdl.StagedBodyReadyLimitHashMismatch:
		syncLog.Warn("Ignoring sync bodies ready stage hash mismatch", "block", row.BlockNum, "hash", row.BlockHash, "stagedHash", limit.StagedHash)
		return 0, false
	default:
		return 0, false
	}
}

// decodeBatchBlocks decodes the popped raw blocks into batch.Blocks. It runs
// OFF ss.mu — a full proto decode per block (up to the configured local import
// chunk, and largest in exactly the full-block era this raw buffer targets) is
// far too heavy to hold the sync lock across, and InsertBlocks already runs
// off-lock. A decode error (can't happen for bytes that already decoded at
// receive) truncates the batch; the dropped suffix was removed from the buffer
// at pop, so it is simply re-fetched.
func (ss *SyncService) decodeBatchBlocks(batch *syncdl.BufferedBatch) {
	dropped, err := batch.DecodeBlocks()
	if err != nil {
		syncLog.Error("Dropping undecodable buffered sync block",
			"number", dropped.Num, "hash", dropped.Hash, "err", err)
	}
}

func (ss *SyncService) recordImportedBatch(batch syncdl.BufferedBatch, applied int, totalElapsed time.Duration, stageProgress *syncdl.StageProgressCollector) {
	summary := syncdl.SummarizeAppliedBatch(batch, applied)
	if !summary.OK {
		return
	}
	ss.deleteImportedSyncBodies(batch, summary.Applied)
	if summary.HasStage {
		ss.writeStageProgressRows(stageProgress.Rows(summary.Last.Num))
	}
	// RecordBlocks atomically (under stats.mu) appends the whole range's
	// counters and decides whether the window has elapsed. applyBlock hooks
	// have already contributed phase stats for the same applied range, so
	// recording the range as one unit keeps block counts and phase totals
	// aligned in the emitted sync summary.
	snap, emit := ss.stats.RecordBlocks(
		summary.Applied,
		summary.TxCount,
		totalElapsed,
		time.Now(),
		tsync.StatsReportInterval,
	)

	ss.mu.Lock()
	var diag syncdl.Diagnostics
	if emit {
		diag = ss.snapshotDiagnosticsLocked()
	}
	remain := ss.estimatedRemainLocked()
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()

	if emit {
		ss.reportSegment(snap, diag, summary.Last.Num, remain, summary.Last.Peer)
	}
}

func (ss *SyncService) writeStageProgress(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash, hasHash bool) {
	if ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	var err error
	if hasHash {
		err = rawdb.WriteStageProgressWithHash(db, stage, blockNum, blockHash)
	} else {
		err = rawdb.WriteStageProgress(db, stage, blockNum)
	}
	if err != nil {
		syncLog.Warn("Persist sync stage progress failed", "stage", stage, "block", blockNum, "err", err)
	}
}

func (ss *SyncService) writeStageProgressRows(rows []rawdb.StageProgress) {
	if len(rows) == 0 || ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	if err := rawdb.WriteStageProgressRows(db, rows); err != nil {
		syncLog.Warn("Persist sync stage progress rows failed", "rows", len(rows), "err", err)
	}
}

func (ss *SyncService) stageSyncBody(block *types.Block, raw []byte) {
	if ss == nil || ss.chain == nil || block == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	result := rawdb.WriteSyncStagedBlockRawAndProgress(db, block, raw)
	if result.StageError != nil {
		syncLog.Warn("Persist sync staged block failed", "number", block.Number(), "hash", block.Hash(), "err", result.StageError)
		return
	}
	if result.ProgressReadError != nil {
		syncLog.Warn("Read sync bodies stage progress failed", "err", result.ProgressReadError)
	}
	if result.ProgressWriteError != nil {
		syncLog.Warn("Persist sync stage progress failed", "stage", rawdb.StageSyncBodies, "block", result.Number, "err", result.ProgressWriteError)
	}
	ss.writeSyncBodiesReadyProgress()
}

func (ss *SyncService) writeSyncBodiesReadyProgress() {
	if ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	head := ss.chain.CurrentBlock()
	if head == nil {
		return
	}
	refresh := syncdl.RefreshStagedBodyReadyProgress(db, head.Number()+1, ss.targetHeadNum)
	if refresh.Frontier.Error != nil {
		syncLog.Warn("Read sync staged block for ready progress failed", "number", refresh.Frontier.ErrorAt, "err", refresh.Frontier.Error)
	}
	if refresh.DeleteError != nil {
		syncLog.Warn("Delete sync bodies ready stage progress failed", "err", refresh.DeleteError)
	}
	if refresh.WriteError != nil {
		syncLog.Warn("Persist sync bodies ready stage progress failed", "block", refresh.Frontier.Number, "hash", refresh.Frontier.Hash, "err", refresh.WriteError)
	}
}

func (ss *SyncService) deleteImportedSyncBodies(batch syncdl.BufferedBatch, applied int) {
	if ss == nil || ss.chain == nil || applied <= 0 {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	result := rawdb.DeleteSyncStagedBlockBatch(db, syncdl.AppliedStagedBlockDeletes(batch, applied))
	for _, deleteErr := range result.Errors {
		syncLog.Warn("Delete sync staged block failed", "number", deleteErr.Number, "hash", deleteErr.Hash, "err", deleteErr.Err)
	}
	ss.writeSyncBodiesReadyProgress()
}

func (ss *SyncService) deleteImportedSyncBodiesThrough(head uint64) {
	if ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	if _, err := rawdb.DeleteSyncStagedBlocksThrough(db, head); err != nil {
		syncLog.Warn("Delete imported sync staged blocks failed", "head", head, "err", err)
	}
	ss.writeSyncBodiesReadyProgress()
}

func (ss *SyncService) deleteStaleSyncBodiesFrom(blockNum uint64, lastRestoredNum uint64, lastRestoredHash tcommon.Hash, haveLastRestored bool) {
	if ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	result, err := rawdb.PruneSyncStagedBlocksFrom(db, blockNum, lastRestoredNum, lastRestoredHash, haveLastRestored)
	if err != nil {
		syncLog.Warn("Prune stale sync staged blocks failed", "from", blockNum, "err", err)
		return
	}
	ss.writeSyncBodiesReadyProgress()
	if result.Deleted > 0 {
		if result.RewoundProgress {
			syncLog.Debug("Deleted stale sync staged block tail", "from", blockNum, "count", result.Deleted, "rewoundTo", result.RewindBlock)
			return
		}
		syncLog.Debug("Deleted stale sync staged block tail", "from", blockNum, "count", result.Deleted)
	}
}

func (ss *SyncService) deleteAllSyncBodies() {
	if ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	result := rawdb.ResetSyncStagedBodies(db)
	if result.StagedDeleteError != nil {
		syncLog.Warn("Delete sync staged blocks failed", "err", result.StagedDeleteError)
	}
	if result.BodiesProgressError != nil {
		syncLog.Warn("Delete sync bodies stage progress failed", "err", result.BodiesProgressError)
	}
	if result.BodiesReadyProgressError != nil {
		syncLog.Warn("Delete sync bodies ready stage progress failed", "err", result.BodiesReadyProgressError)
	}
}

func (ss *SyncService) pauseSync(peer *p2p.Peer, num uint64, err error) {
	peerID := "<nil>"
	if peer != nil {
		peerID = peer.ID()
	}
	syncLog.Error("Sync paused",
		"number", num,
		"peer", peerID,
		"err", err,
		"hint", tsync.PauseHint(err))
	// Latch the gate outside ss.mu: lock order is ss.mu (outer) →
	// pause.mu (inner) elsewhere, and Enter is sticky so the brief
	// window between Enter and the doReset() that follows is fine —
	// new sync attempts will already short-circuit on the gate while
	// callers blocked on ss.mu wait their turn.
	ss.pause.Enter(num, err)
	ss.mu.Lock()
	ss.doReset()
	ss.mu.Unlock()
}

func (ss *SyncService) estimatedRemainLocked() int64 {
	return ss.sessionProgressLocked().EstimatedRemaining()
}

func (ss *SyncService) shouldFinishLocked() bool {
	return ss.sessionProgressLocked().ShouldFinish()
}

func (ss *SyncService) shouldRestartForStalledRetriesLocked() bool {
	return ss.sessionProgressLocked().ShouldRestartForStalledRetries()
}

func (ss *SyncService) sessionProgressLocked() syncdl.SessionProgress {
	progress := syncdl.SessionProgress{
		Syncing:        ss.syncing,
		Paused:         ss.pause.Paused(),
		TargetHead:     ss.targetHeadNum,
		RetryListLen:   len(ss.retryList),
		BlockBufferLen: len(ss.blockBuffer),
	}
	if ss.chain != nil && ss.chain.CurrentBlock() != nil {
		progress.CurrentHead = ss.chain.CurrentBlock().Number()
	}
	if len(ss.peers) > 0 {
		progress.Peers = make([]syncdl.PeerProgress, 0, len(ss.peers))
	}
	for _, ps := range ss.peers {
		if ps == nil {
			continue
		}
		progress.Peers = append(progress.Peers, syncdl.PeerProgress{
			FetchListLen:   len(ps.fetchList),
			Inflight:       ps.inflight,
			RemainNum:      ps.remainNum,
			ChainRequested: ps.chainRequested,
			Done:           ps.done,
		})
	}
	return progress
}

func (ss *SyncService) snapshotDiagnosticsLocked() syncdl.Diagnostics {
	peers := make([]syncdl.PeerDiagnostics, 0, len(ss.peers))
	for id, ps := range ss.peers {
		if ps == nil {
			continue
		}
		peers = append(peers, syncdl.PeerDiagnostics{
			ID:             id,
			Inflight:       ps.inflight,
			FetchListLen:   len(ps.fetchList),
			PendingLen:     len(ps.pending),
			RemainNum:      ps.remainNum,
			ChainRequested: ps.chainRequested,
			Done:           ps.done,
		})
	}
	return syncdl.NewDiagnostics(len(ss.blockBuffer), len(ss.requested), len(ss.retryList), peers)
}

// reportSegment emits the throttled "Imported chain segment" summary. Called
// without ss.mu held.
func (ss *SyncService) reportSegment(s tsync.Snapshot, diag syncdl.Diagnostics, head uint64, remain int64, peer *p2p.Peer) {
	elapsed := time.Since(s.StartTime)
	if elapsed <= 0 {
		elapsed = 1
	}
	blocksPerSec := float64(s.Blocks) * float64(time.Second) / float64(elapsed)
	txsPerSec := float64(s.Txs) * float64(time.Second) / float64(elapsed)

	ctx := []any{
		"blocks", s.Blocks,
		"txs", s.Txs,
		"elapsed", ethcommon.PrettyDuration(elapsed),
		"execElapsed", ethcommon.PrettyDuration(s.ExecElapsed),
		"applyElapsed", ethcommon.PrettyDuration(s.ApplyStats.Total()),
		"blocks/s", round2(blocksPerSec),
		"txs/s", round2(txsPerSec),
		"head", head,
		"remain", remain,
	}
	if phase, elapsed := slowestApplyPhase(s.ApplyStats); phase != "" {
		ctx = append(ctx, "slowPhase", phase, "slowElapsed", ethcommon.PrettyDuration(elapsed))
	}
	if phase, elapsed := slowestStateCommitPhase(s.ApplyStats); phase != "" {
		ctx = append(ctx, "slowStateCommitPhase", phase, "slowStateCommitElapsed", ethcommon.PrettyDuration(elapsed))
	}
	topMutations := s.ApplyStats.StateCommitDetail.Mutations.TopKindsString(3)
	if topMutations == "" {
		topMutations = "none"
	}
	ctx = append(ctx, "stateMutTop", topMutations)
	topKVDomains := s.ApplyStats.StateCommitDetail.Mutations.TopKVDomainsString(3)
	if topKVDomains == "" {
		topKVDomains = "none"
	}
	ctx = append(ctx, "stateMutKVTop", topKVDomains)
	if blocksPerSec > 0 && remain > 0 {
		etaSec := float64(remain) / blocksPerSec
		ctx = append(ctx, "eta", ethcommon.PrettyDuration(time.Duration(etaSec*float64(time.Second))))
	}
	if peer != nil {
		ctx = append(ctx, "peer", peer.ID())
	}
	syncLog.Info("Imported chain segment", ctx...)

	detail := []any{
		"blocks", s.Blocks,
		"head", head,
		"bufferWaitElapsed", ethcommon.PrettyDuration(s.BufferWaitElapsed),
		"validate", ethcommon.PrettyDuration(s.ApplyStats.Validate),
		"execute", ethcommon.PrettyDuration(s.ApplyStats.Execute),
		"maintenance", ethcommon.PrettyDuration(s.ApplyStats.Maintenance),
		"stateCommit", ethcommon.PrettyDuration(s.ApplyStats.StateCommit),
		"stateCommitMeasured", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.Total()),
		"stateCommitPrepare", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.Prepare),
		"stateCommitFlatWrite", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.FlatWrite),
		"stateCommitFlatFlush", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.FlatFlush),
		"stateCommitKVCompute", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.KVCompute),
		"stateCommitKVNodes", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.KVNodeWrite),
		"stateCommitAccountTrieUpdate", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.AccountTrieUpdate),
		"stateCommitAccountTrieMarshal", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.AccountTrieMarshal),
		"stateCommitAccountTrieGeneration", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.AccountTrieGeneration),
		"stateCommitAccountTrieWrite", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.AccountTrieWrite),
		"stateCommitFinalize", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.Finalize),
		"stateCommitAccountTrieCommit", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.AccountTrieCommit),
		"stateCommitTrieNodes", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.TrieNodeWrite),
		"stateCommitTrieFlush", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.TrieNodeFlush),
		"stateCommitReopen", ethcommon.PrettyDuration(s.ApplyStats.StateCommitDetail.Reopen),
		"stateCommitAccounts", s.ApplyStats.StateCommitDetail.Accounts,
		"stateCommitKVAccounts", s.ApplyStats.StateCommitDetail.KVAccounts,
		"stateCommitKVItems", s.ApplyStats.StateCommitDetail.KVItems,
		"stateCommitDeferredKVAccounts", s.ApplyStats.StateCommitDetail.DeferredKVAccounts,
		"stateCommitDeferredKVItems", s.ApplyStats.StateCommitDetail.DeferredKVItems,
		"stateCommitRebuiltKVAccounts", s.ApplyStats.StateCommitDetail.RebuiltKVAccounts,
		"stateCommitRebuiltKVItems", s.ApplyStats.StateCommitDetail.RebuiltKVItems,
		"stateMutAccountCreates", s.ApplyStats.StateCommitDetail.Mutations.AccountCreates,
		"stateMutAccountUpdates", s.ApplyStats.StateCommitDetail.Mutations.AccountUpdates,
		"stateMutAccountDeletes", s.ApplyStats.StateCommitDetail.Mutations.AccountDeletes,
		"stateMutCodeUpdates", s.ApplyStats.StateCommitDetail.Mutations.CodeUpdates,
		"stateMutCodeDeletes", s.ApplyStats.StateCommitDetail.Mutations.CodeDeletes,
		"stateMutContractMetaUpdates", s.ApplyStats.StateCommitDetail.Mutations.ContractMetaUpdates,
		"stateMutContractMetaDeletes", s.ApplyStats.StateCommitDetail.Mutations.ContractMetaDeletes,
		"stateMutStoragePuts", s.ApplyStats.StateCommitDetail.Mutations.StoragePuts,
		"stateMutStorageDeletes", s.ApplyStats.StateCommitDetail.Mutations.StorageDeletes,
		"stateMutStorageNoops", s.ApplyStats.StateCommitDetail.Mutations.StorageNoops,
		"stateMutKVPuts", s.ApplyStats.StateCommitDetail.Mutations.KVPutItems,
		"stateMutKVDeletes", s.ApplyStats.StateCommitDetail.Mutations.KVDeleteItems,
		"stateMutKVNoops", s.ApplyStats.StateCommitDetail.Mutations.KVNoopItems,
		"stateMutTop", s.ApplyStats.StateCommitDetail.Mutations.TopKindsString(10),
		"stateMutKVTop", s.ApplyStats.StateCommitDetail.Mutations.TopKVDomainsString(10),
		"dpUpdate", ethcommon.PrettyDuration(s.ApplyStats.DPUpdate),
		"persist", ethcommon.PrettyDuration(s.ApplyStats.Persist),
		"hooks", ethcommon.PrettyDuration(s.ApplyStats.Hooks),
		"blockBuffer", diag.BlockBufferLen,
		"requested", diag.RequestedLen,
		"retryList", diag.RetryListLen,
	}
	if diag.PeerState != "" {
		detail = append(detail, "peerState", diag.PeerState)
	}
	syncLog.Debug("Imported chain segment details", detail...)
}

func round2(f float64) float64 {
	// Trim to 2 decimals for log readability without depending on a printf
	// format directive (slog handlers print floats with full precision).
	return float64(int64(f*100+0.5)) / 100
}

func slowestApplyPhase(s core.ApplyStats) (string, time.Duration) {
	phase := ""
	var max time.Duration
	for _, p := range []struct {
		name string
		d    time.Duration
	}{
		{"validate", s.Validate},
		{"execute", s.Execute},
		{"maintenance", s.Maintenance},
		{"stateCommit", s.StateCommit},
		{"dpUpdate", s.DPUpdate},
		{"persist", s.Persist},
		{"hooks", s.Hooks},
	} {
		if p.d > max {
			phase = p.name
			max = p.d
		}
	}
	return phase, max
}

func slowestStateCommitPhase(s core.ApplyStats) (string, time.Duration) {
	phase := ""
	var max time.Duration
	d := s.StateCommitDetail
	type phaseDuration struct {
		name string
		d    time.Duration
	}
	phases := []phaseDuration{
		{"prepare", d.Prepare},
		{"flatWrite", d.FlatWrite},
		{"flatFlush", d.FlatFlush},
		{"kvCompute", d.KVCompute},
		{"kvNodes", d.KVNodeWrite},
		{"finalize", d.Finalize},
		{"accountTrieCommit", d.AccountTrieCommit},
		{"trieNodes", d.TrieNodeWrite},
		{"trieFlush", d.TrieNodeFlush},
		{"reopen", d.Reopen},
	}
	if d.AccountTrieMarshal+d.AccountTrieGeneration+d.AccountTrieWrite > 0 {
		phases = append(phases,
			phaseDuration{"accountTrieMarshal", d.AccountTrieMarshal},
			phaseDuration{"accountTrieGeneration", d.AccountTrieGeneration},
			phaseDuration{"accountTrieWrite", d.AccountTrieWrite},
		)
	} else {
		phases = append(phases, phaseDuration{"accountTrieUpdate", d.AccountTrieUpdate})
	}
	for _, p := range phases {
		if p.d > max {
			phase = p.name
			max = p.d
		}
	}
	return phase, max
}

// doReset clears all sync state. Must be called with ss.mu held.
func (ss *SyncService) doReset() {
	for _, ps := range ss.peers {
		if ps.fetchTimer != nil {
			ps.fetchTimer.Stop()
			ps.fetchTimer = nil
		}
		if ps.fetchDelayTimer != nil {
			ps.fetchDelayTimer.Stop()
			ps.fetchDelayTimer = nil
		}
	}
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
	ss.peers = nil
	ss.requested = nil
	ss.retryList = nil
	ss.blockBuffer = nil
	ss.bufferedHash = nil
	ss.blockPath = nil
	ss.targetHeadNum = 0
	ss.bufferWait.Reset()
	ss.deleteAllSyncBodies()
}

// armFetchTimer arms the fetch-response timeout. Must be called with ss.mu held.
func (ss *SyncService) armFetchTimer() {
	ps := ss.ensurePeerStateLocked(ss.syncPeer)
	if ps == nil {
		return
	}
	ss.armPeerFetchTimerLocked(ps)
	ss.mirrorLegacyLocked()
}

func (ss *SyncService) armPeerFetchTimerLocked(ps *syncPeerState) {
	if ps.fetchTimer != nil {
		ps.fetchTimer.Stop()
	}
	ps.fetchSeq++
	seq := ps.fetchSeq
	peerID := ps.peer.ID()
	ps.fetchTimer = time.AfterFunc(ss.fetchTimeout, func() {
		ss.onFetchTimeout(seq, peerID)
	})
}

func (ss *SyncService) onFetchTimeout(seq uint64, peerID string) {
	ss.mu.Lock()
	ps := ss.peers[peerID]
	if !ss.syncing || ps == nil || ps.fetchSeq != seq {
		ss.mu.Unlock()
		return
	}
	stalePeer := ps.peer
	inflight := ps.inflight
	ss.removePeerStateLocked(peerID, true)
	var out []outboundSyncRequest
	remainingPeers := len(ss.peers)
	if remainingPeers > 0 {
		out = ss.fillFetchSlotsLocked(time.Now())
	}
	stalledRetries := false
	if remainingPeers > 0 && len(out) == 0 {
		stalledRetries = ss.shouldRestartForStalledRetriesLocked()
	}
	plan := syncdl.PlanPeerFailover(syncdl.PeerFailoverInput{
		RemainingPeers:   remainingPeers,
		OutboundRequests: len(out),
		StalledRetries:   stalledRetries,
	})
	if plan.Reset {
		ss.doReset()
	} else if plan.Mirror {
		ss.mirrorLegacyLocked()
	}
	ss.mu.Unlock()
	syncLog.Warn("Fetch timeout, failing over",
		"peer", stalePeer.ID(),
		"timeout", ethcommon.PrettyDuration(ss.fetchTimeout),
		"inflight", inflight)
	if len(out) > 0 {
		ss.sendOutboundRequests(out)
		return
	}
	if plan.TryFindPeer {
		ss.tryFindSyncPeer(stalePeer)
	}
}

// PeerDisconnected is called by the handler when a peer goes away. If that
// peer is the active sync peer, the sync is aborted and we immediately try
// to find a replacement.
func (ss *SyncService) PeerDisconnected(peer *p2p.Peer) {
	if peer == nil {
		return
	}
	ss.mu.Lock()
	if !ss.syncing {
		ss.mu.Unlock()
		return
	}
	if ss.syncPeer != nil && ss.syncPeer.ID() == peer.ID() {
		ss.ensurePeerStateLocked(peer)
	}
	if _, ok := ss.peers[peer.ID()]; !ok {
		ss.mu.Unlock()
		return
	}
	ss.removePeerStateLocked(peer.ID(), true)
	var out []outboundSyncRequest
	remainingPeers := len(ss.peers)
	if remainingPeers > 0 {
		out = ss.fillFetchSlotsLocked(time.Now())
	}
	stalledRetries := false
	if remainingPeers > 0 && len(out) == 0 {
		stalledRetries = ss.shouldRestartForStalledRetriesLocked()
	}
	plan := syncdl.PlanPeerFailover(syncdl.PeerFailoverInput{
		RemainingPeers:   remainingPeers,
		OutboundRequests: len(out),
		StalledRetries:   stalledRetries,
	})
	if plan.Reset {
		ss.doReset()
	} else if plan.Mirror {
		ss.mirrorLegacyLocked()
	}
	ss.mu.Unlock()
	syncLog.Info("Sync peer disconnected", "peer", peer.ID())
	if len(out) > 0 {
		ss.sendOutboundRequests(out)
	}
	if plan.TryFindPeer {
		ss.tryFindSyncPeer(peer)
	}
}

func (ss *SyncService) removePeerStateLocked(peerID string, retry bool) {
	ps := ss.peers[peerID]
	if ps == nil {
		return
	}
	if ps.fetchTimer != nil {
		ps.fetchTimer.Stop()
		ps.fetchTimer = nil
	}
	if ps.fetchDelayTimer != nil {
		ps.fetchDelayTimer.Stop()
		ps.fetchDelayTimer = nil
	}
	if retry {
		for h := range ps.pendingIDs {
			delete(ss.requested, h)
		}
		ss.retryList = syncdl.AppendDisconnectedPeerRetries(ss.retryList, ps.pendingIDs, ps.fetchList, func(bid types.BlockID) bool {
			return !ss.hasBlockOrRequestLocked(bid)
		})
	}
	delete(ss.peers, peerID)
	if ss.syncPeer != nil && ss.syncPeer.ID() == peerID {
		ss.syncPeer = nil
		for _, next := range ss.peers {
			ss.syncPeer = next.peer
			break
		}
	}
	ss.mirrorLegacyLocked()
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
	totalBlocks := ss.stats.TotalBlocks()
	totalStart := ss.stats.TotalStart()
	if totalStart.IsZero() {
		totalStart = time.Now()
	}
	ss.mu.Lock()
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
	syncLog.Info("Sync complete", ctx...)
}
