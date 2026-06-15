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
	preGate := syncdl.PlanSyncStartGate(syncdl.SyncStartGateInput{
		PeerPresent: peer != nil,
		Stopping:    ss.stopping.Load(),
		Paused:      ss.pause.Paused(),
	})
	if !preGate.Allowed {
		return
	}
	needFrom := ss.chain.CurrentBlock().Number() + 1
	availabilityChecked := false
	peerCanServe := true
	var lowest, peerHead uint64
	if ss.handler != nil {
		availabilityChecked = true
		ok, serviceLowest, serviceHead := ss.handler.syncPeerCanServe(peer, needFrom)
		peerCanServe = ok
		lowest = serviceLowest
		peerHead = serviceHead
	}
	gate := syncdl.PlanSyncStartGate(syncdl.SyncStartGateInput{
		PeerPresent:         peer != nil,
		Stopping:            ss.stopping.Load(),
		Paused:              ss.pause.Paused(),
		AvailabilityChecked: availabilityChecked,
		PeerCanServe:        peerCanServe,
	})
	if !gate.Allowed {
		if gate.SkipReason == syncdl.SyncStartSkipPeerUnavailable {
			syncLog.Info("Skipping sync peer outside available range",
				"peer", peer.ID(),
				"needFrom", needFrom,
				"peerLowest", lowest,
				"peerHead", peerHead)
		}
		return
	}
	now := time.Now()
	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	attach := syncdl.PlanSyncPeerAttach(syncdl.SyncPeerAttachInput{
		Syncing:           ss.syncing,
		PeerAlreadyJoined: ss.syncing && ss.peers[peer.ID()] != nil,
	})
	if !attach.Attach {
		ss.mu.Unlock()
		return
	}
	if attach.InitSession {
		ss.initSessionLocked(now)
	}
	ps, added := ss.addPeerStateLocked(peer)
	if !added {
		ss.mu.Unlock()
		return
	}
	if attach.MarkChainRequested {
		ps.chainRequested = true
	}
	if attach.MirrorLegacy {
		ss.mirrorLegacyLocked()
	}
	ss.mu.Unlock()

	if attach.LogStarted {
		syncLog.Info("Sync started",
			"peer", peer.ID(),
			"localHead", ss.chain.CurrentBlock().Number())
	} else {
		syncLog.Debug("Sync peer joined", "peer", peer.ID())
	}
	if attach.SendChainSummary {
		ss.sendSyncBlockChain(peer)
	}
	if attach.JoinAvailablePeers {
		ss.joinAvailablePeers()
	}
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
	ss.applySessionStartupPlan(headBlock, startup)
	ss.stats.InitSession(now)
	ss.bufferWait.Reset()
	if startup.ResetPeerJoinThrottle {
		ss.lastPeerJoinAttempt = time.Time{}
	}
}

func (ss *SyncService) applySessionStartupPlan(headBlock *types.Block, startup syncdl.SessionStartupPlan) {
	result := syncdl.ApplySessionStartupPlan(startup, syncSessionStartupApplier{service: ss, headBlock: headBlock})
	ss.logSyncStartupRepairSummary(result)
}

type syncSessionStartupApplier struct {
	service   *SyncService
	headBlock *types.Block
}

func (a syncSessionStartupApplier) RepairSyncPipeline() syncdl.SyncPipelineProgressRepairResult {
	return a.service.repairSyncPipelineProgress(a.headBlock)
}

func (a syncSessionStartupApplier) RestoreInventoryTarget(inventoryFloor uint64) {
	a.service.targetHeadNum = a.service.restoreSyncInventoryTarget(inventoryFloor)
}

func (a syncSessionStartupApplier) DeleteImportedBodies(through uint64) {
	a.service.deleteImportedSyncBodiesThrough(through)
}

func (a syncSessionStartupApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) syncdl.StagedBodyRestoreResult {
	return a.service.restoreSyncStagedBodiesLocked(from, limit, pruneStaleTail)
}

func (a syncSessionStartupApplier) RefreshBodiesReady() {
	a.service.writeSyncBodiesReadyProgress()
}

func (a syncSessionStartupApplier) RepairSyncPipelineProgressOrder() syncdl.SyncPipelineProgressOrderRepairResult {
	return a.service.repairSyncPipelineProgressOrder()
}

func (a syncSessionStartupApplier) CheckSyncPipelineProgressOrder() syncdl.SyncPipelineProgressOrderCheckResult {
	return a.service.checkSyncPipelineProgressOrder()
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

func (ss *SyncService) repairSyncPipelineProgress(head *types.Block) syncdl.SyncPipelineProgressRepairResult {
	if ss == nil || ss.chain == nil || head == nil {
		return syncdl.SyncPipelineProgressRepairResult{}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.SyncPipelineProgressRepairResult{}
	}
	result := syncdl.RepairSyncPipelineProgressWithResult(db, head.Number(), func(number uint64) (tcommon.Hash, bool) {
		block := ss.chain.GetBlockByNumber(number)
		if block == nil {
			return tcommon.Hash{}, false
		}
		return block.Hash(), true
	})
	for _, repair := range result.Repairs {
		ss.logSyncStageProgressRepair(head, repair)
	}
	return result
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
	ss.logSyncStageProgressRepair(head, repair)
}

func (ss *SyncService) logSyncStageProgressRepair(head *types.Block, repair syncdl.SyncStageProgressRepair) {
	if head == nil {
		return
	}
	switch repair.Status {
	case syncdl.SyncStageProgressReadError:
		syncLog.Warn("Read sync stage progress failed", "stage", repair.Stage, "err", repair.ReadError)
		return
	case syncdl.SyncStageProgressDeleteError:
		syncLog.Warn("Delete stale sync stage progress failed", "stage", repair.Stage, "block", repair.Row.BlockNum, "hash", repair.Row.BlockHash, "err", repair.DeleteError)
		return
	case syncdl.SyncStageProgressDeleted:
		syncLog.Debug("Deleted stale sync stage progress", "stage", repair.Stage, "block", repair.Row.BlockNum, "hash", repair.Row.BlockHash, "head", head.Number(), "headHash", head.Hash())
		return
	}
}

func (ss *SyncService) checkSyncPipelineProgressOrder() syncdl.SyncPipelineProgressOrderCheckResult {
	if ss == nil || ss.chain == nil {
		return syncdl.SyncPipelineProgressOrderCheckResult{}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.SyncPipelineProgressOrderCheckResult{}
	}
	result := syncdl.CheckSyncPipelineProgressOrderFromDB(db, syncdl.SyncPipelineProgressOrderOptions{})
	for _, readErr := range result.ReadErrors {
		syncLog.Warn("Read sync pipeline stage progress failed", "stage", readErr.Stage, "err", readErr.Err)
	}
	return result
}

func (ss *SyncService) repairSyncPipelineProgressOrder() syncdl.SyncPipelineProgressOrderRepairResult {
	if ss == nil || ss.chain == nil {
		return syncdl.SyncPipelineProgressOrderRepairResult{}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.SyncPipelineProgressOrderRepairResult{}
	}
	result := syncdl.RepairSyncPipelineProgressOrderFromDB(db, syncdl.SyncPipelineProgressOrderOptions{})
	for _, readErr := range result.Before.ReadErrors {
		syncLog.Warn("Read sync pipeline stage progress failed during order repair", "stage", readErr.Stage, "err", readErr.Err)
	}
	for _, repair := range result.Repairs {
		if repair.WriteError != nil {
			syncLog.Warn("Write sync pipeline order repair failed", "stage", repair.Stage, "block", repair.Row.BlockNum, "err", repair.WriteError)
			continue
		}
		if repair.DeleteError != nil {
			syncLog.Warn("Delete sync pipeline order violation failed", "stage", repair.Stage, "block", repair.Row.BlockNum, "err", repair.DeleteError)
			continue
		}
		if repair.Updated {
			syncLog.Debug("Updated sync pipeline order violation",
				"stage", repair.Stage,
				"block", repair.Row.BlockNum,
				"hash", repair.Row.BlockHash,
				"issue", repair.Issue.String())
			continue
		}
		syncLog.Debug("Deleted sync pipeline order violation",
			"stage", repair.Stage,
			"block", repair.Row.BlockNum,
			"hash", repair.Row.BlockHash,
			"issue", repair.Issue.String())
	}
	return result
}

func (ss *SyncService) logSyncStartupRepairSummary(result syncdl.SessionStartupApplyResult) {
	if !result.HasSyncPipelineRepair && !result.HasStagedBodyRestore && !result.HasSyncPipelineOrderRepair && !result.HasSyncPipelineOrder {
		return
	}
	repair := result.SyncPipelineRepairResult
	restore := result.StagedBodyRestore
	orderRepair := result.SyncPipelineOrderRepair
	cursor := result.SyncPipelineCursor
	orderIssueCount := len(result.SyncPipelineOrderIssues)
	orderReadErrorCount := len(result.SyncPipelineOrderErrors)
	var firstOrderIssue string
	if orderIssueCount > 0 {
		firstOrderIssue = result.SyncPipelineOrderIssues[0].String()
	}
	var firstOrderReadErrorStage rawdb.StageID
	if orderReadErrorCount > 0 {
		firstOrderReadErrorStage = result.SyncPipelineOrderErrors[0].Stage
	}
	syncLog.Info("Sync startup repair summary",
		"syncStartupRepairComplete", repair.Complete,
		"syncStartupRepairKept", repair.Kept,
		"syncStartupRepairMissing", repair.Missing,
		"syncStartupRepairDeleted", repair.Deleted,
		"syncStartupRepairHasBlocked", repair.HasBlocked,
		"syncStartupRepairFirstBlocked", repair.FirstBlockedStage,
		"syncStartupRepairInterrupted", repair.Interrupted,
		"syncStartupRepairErrorStage", repair.ErrorStage,
		"syncStartupRepairRows", len(repair.Repairs),
		"syncStartupPipelineOrderChecked", result.HasSyncPipelineOrder,
		"syncStartupPipelineOrderIssues", orderIssueCount,
		"syncStartupPipelineOrderFirstIssue", firstOrderIssue,
		"syncStartupPipelineOrderReadErrors", orderReadErrorCount,
		"syncStartupPipelineOrderFirstReadErrorStage", firstOrderReadErrorStage,
		"syncStartupPipelineOrderRepairChecked", result.HasSyncPipelineOrderRepair,
		"syncStartupPipelineOrderRepairComplete", orderRepair.Complete,
		"syncStartupPipelineOrderRepairDeleted", orderRepair.Deleted,
		"syncStartupPipelineOrderRepairUpdated", orderRepair.Updated,
		"syncStartupPipelineOrderRepairInterrupted", orderRepair.Interrupted,
		"syncStartupPipelineOrderRepairErrorStage", orderRepair.ErrorStage,
		"syncStartupPipelineOrderRepairRows", len(orderRepair.Repairs),
		"syncStartupPipelineCursorChecked", result.HasSyncPipelineCursor,
		"syncStartupPipelineCursorComplete", cursor.Complete,
		"syncStartupPipelineCursorRows", cursor.StageRows,
		"syncStartupPipelineCursorHasLast", cursor.HasLast,
		"syncStartupPipelineCursorLastStage", cursor.LastStage,
		"syncStartupPipelineCursorLastBlock", cursor.LastBlock,
		"syncStartupPipelineCursorLastHasHash", cursor.LastHasHash,
		"syncStartupPipelineCursorHasNext", cursor.HasNext,
		"syncStartupPipelineCursorNextStage", cursor.NextStage,
		"syncStartupPipelineCursorBlocked", cursor.HasBlocked,
		"syncStartupPipelineCursorInterrupted", cursor.Interrupted,
		"syncStartupPipelineCursorErrorStage", cursor.ErrorStage,
		"syncStartupStagedRestored", restore.Restored,
		"syncStartupStagedTargetHead", restore.TargetHead,
		"syncStartupStagedNextExpected", restore.NextExpected,
		"syncStartupStagedNeedPruneTail", restore.NeedPruneTail,
		"syncStartupStagedPruneFrom", restore.PruneFrom,
		"syncStartupStagedHaveLastRestored", restore.HaveLastRestored,
		"syncStartupStagedLastRestored", restore.LastRestoredNum,
	)
}

func (ss *SyncService) restoreSyncStagedBodiesLocked(start uint64, limit int, pruneStaleTail bool) syncdl.StagedBodyRestoreResult {
	if ss == nil || ss.chain == nil || limit <= 0 {
		return syncdl.StagedBodyRestoreResult{NextExpected: start}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.StagedBodyRestoreResult{NextExpected: start}
	}
	result := syncdl.RestoreStagedBodies(start, limit, ss.targetHeadNum, ss.blockBuffer, ss.bufferedHash, &ss.blockPath, func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error {
		return rawdb.IterateSyncStagedBlocksFrom(db, start, fn)
	})
	syncdl.ApplyStagedBodyRestoreSettlementPlan(
		syncdl.PlanStagedBodyRestoreSettlement(result, pruneStaleTail),
		syncStagedBodyRestoreSettlementApplier{service: ss},
	)
	if result.ReadError != nil {
		syncLog.Warn("Read sync staged block range failed", "from", result.NextExpected, "err", result.ReadError)
	}
	return result
}

type syncStagedBodyRestoreSettlementApplier struct {
	service *SyncService
}

func (a syncStagedBodyRestoreSettlementApplier) SetStagedBodyRestoreTargetHead(targetHead uint64) {
	a.service.targetHeadNum = targetHead
}

func (a syncStagedBodyRestoreSettlementApplier) PruneStaleStagedBodyTail(from uint64, lastRestoredNum uint64, lastRestoredHash tcommon.Hash, haveLastRestored bool) {
	a.service.deleteStaleSyncBodiesFrom(from, lastRestoredNum, lastRestoredHash, haveLastRestored)
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
	existing := make([]string, 0, len(ss.peers))
	for id := range ss.peers {
		exclude[id] = struct{}{}
		existing = append(existing, id)
	}
	ss.mu.Unlock()
	if need <= 0 {
		return
	}
	candidateByID := make(map[string]*p2p.Peer)
	primaryPeers := ss.handler.SyncCandidates(exclude, need)
	primary := make([]string, 0, len(primaryPeers))
	for _, peer := range primaryPeers {
		if peer == nil {
			continue
		}
		id := peer.ID()
		primary = append(primary, id)
		candidateByID[id] = peer
	}
	var fallback []syncdl.PeerJoinFallbackCandidate
	if len(primary) < need {
		for _, peer := range ss.handler.HandshakedPeers() {
			if peer == nil {
				continue
			}
			id := peer.ID()
			if _, skip := exclude[id]; skip {
				continue
			}
			ok, _, _ := ss.handler.syncPeerCanServe(peer, needFrom)
			fallback = append(fallback, syncdl.PeerJoinFallbackCandidate{
				ID:       id,
				CanServe: ok,
			})
			if _, exists := candidateByID[id]; !exists {
				candidateByID[id] = peer
			}
		}
	}
	selection := syncdl.PlanPeerJoinSelection(syncdl.PeerJoinSelectionInput{
		Need:     need,
		Existing: existing,
		Primary:  primary,
		Fallback: fallback,
	})
	for _, id := range selection.Selected {
		if peer := candidateByID[id]; peer != nil {
			ss.StartSync(peer)
		}
	}
}

func (ss *SyncService) shouldJoinAvailablePeersLocked(now time.Time, progress syncdl.SessionProgress) bool {
	plan := syncdl.PlanPeerJoinAttempt(syncdl.PeerJoinAttemptInput{
		HandlerAvailable: ss.handler != nil,
		Progress:         progress,
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

	responsePlan := syncdl.PlanChainInventoryResponse(syncdl.ChainInventoryResponseInput{
		CommonBlock:    commonNum,
		HeadBlock:      headNum,
		InventoryLimit: maxChainInventorySize,
		ReadBlockID: func(num uint64) (types.BlockID, bool) {
			block := ss.chain.GetBlockByNumber(num)
			if block == nil {
				return types.BlockID{}, false
			}
			return block.ID(), true
		},
	})
	responseIDs := make([]*corepb.ChainInventory_BlockId, 0, len(responsePlan.IDs))
	for _, bid := range responsePlan.IDs {
		responseIDs = append(responseIDs, &corepb.ChainInventory_BlockId{
			Hash:   bid.Hash[:],
			Number: int64(bid.Num),
		})
	}

	resp := &corepb.ChainInventory{
		Ids:       responseIDs,
		RemainNum: responsePlan.RemainNum,
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
	ids := make([]types.BlockID, 0, len(inv.Ids))
	for _, bid := range inv.Ids {
		ids = append(ids, types.BlockID{
			Hash: tcommon.BytesToHash(bid.Hash),
			Num:  uint64(bid.Number),
		})
	}
	candidates := syncdl.BuildInventoryCandidates(ids, syncInventoryCandidateFactReader{
		service:   ss,
		peerState: ps,
		headNum:   ss.chain.CurrentBlock().Number(),
	})

	sessionApplier := &syncChainInventorySessionRunApplier{
		service:         ss,
		peerState:       ps,
		now:             time.Now(),
		inventoryBlocks: len(inv.Ids),
		remainNum:       inv.RemainNum,
		peerID:          peer.ID(),
	}
	inventorySession := syncdl.ApplyChainInventorySessionRun(syncdl.ChainInventorySessionRunInput{
		Inventory: syncdl.ChainInventoryInput{
			CurrentTarget:  ss.targetHeadNum,
			ExistingQueued: len(ps.fetchList),
			RemainNum:      inv.RemainNum,
			InventoryLimit: maxChainInventorySize,
			Candidates:     candidates,
		},
	}, sessionApplier)
	ss.mu.Unlock()

	syncdl.ApplyChainInventoryPostLockPlan(inventorySession.Inventory.PostLock, syncChainInventoryPostLockApplier{service: ss})
	syncdl.ApplyPostInventoryRunPostLockPlan(inventorySession.PostInventory.Plan, syncFetchRefillDispatchApplier{service: ss, out: sessionApplier.out}, sessionApplier)
}

type syncInventoryCandidateFactReader struct {
	service   *SyncService
	peerState *syncPeerState
	headNum   uint64
}

func (r syncInventoryCandidateFactReader) HasCanonicalInventoryBlock(id types.BlockID) bool {
	if r.service == nil || r.service.chain == nil || id.Num > r.headNum {
		return false
	}
	existing := r.service.chain.GetBlockByNumber(id.Num)
	return existing != nil && existing.Hash() == id.Hash
}

func (r syncInventoryCandidateFactReader) HasKhaosInventoryBlock(id types.BlockID) bool {
	if r.service == nil || r.service.chain == nil {
		return false
	}
	return r.service.chain.HasBlockInKhaosDB(id.Hash)
}

func (r syncInventoryCandidateFactReader) HasBufferedInventoryBlock(id types.BlockID) bool {
	if r.service == nil {
		return false
	}
	_, ok := r.service.bufferedHash[id.Hash]
	return ok
}

func (r syncInventoryCandidateFactReader) HasRequestedInventoryBlock(id types.BlockID) bool {
	if r.service == nil {
		return false
	}
	_, ok := r.service.requested[id.Hash]
	return ok
}

func (r syncInventoryCandidateFactReader) PeerRequestedInventoryBlock(id types.BlockID) bool {
	if r.peerState == nil {
		return false
	}
	_, ok := r.peerState.requestedHashes[id.Hash]
	return ok
}

func (r syncInventoryCandidateFactReader) ReserveInventoryBlockPath(id types.BlockID) bool {
	if r.service == nil {
		return false
	}
	return r.service.reserveBlockPathLocked(id)
}

type syncChainInventorySessionRunApplier struct {
	service         *SyncService
	peerState       *syncPeerState
	now             time.Time
	inventoryBlocks int
	remainNum       int64
	peerID          string
	out             []outboundSyncRequest
}

func (a *syncChainInventorySessionRunApplier) AppendAcceptedInventory(ids []types.BlockID) {
	(&syncChainInventoryApplier{service: a.service, peerState: a.peerState}).AppendAcceptedInventory(ids)
}

func (a *syncChainInventorySessionRunApplier) UpdateInventoryProgress(remainNum int64, target syncdl.InventoryTargetUpdate, hasTarget bool, stageTarget uint64, hasStageTarget bool) {
	(&syncChainInventoryApplier{service: a.service, peerState: a.peerState}).UpdateInventoryProgress(remainNum, target, hasTarget, stageTarget, hasStageTarget)
}

func (a *syncChainInventorySessionRunApplier) MarkInventoryDone() {
	(&syncChainInventoryApplier{service: a.service, peerState: a.peerState}).MarkInventoryDone()
}

func (a *syncChainInventorySessionRunApplier) ResetSyncUnderLock() {
	(syncPostInventorySettlementApplier{service: a.service}).ResetSyncUnderLock()
}

func (a *syncChainInventorySessionRunApplier) MirrorLegacyUnderLock() {
	(syncPostInventorySettlementApplier{service: a.service}).MirrorLegacyUnderLock()
}

func (a *syncChainInventorySessionRunApplier) TryFindSyncPeer() {
	(syncPostInventorySettlementApplier{service: a.service}).TryFindSyncPeer()
}

func (a *syncChainInventorySessionRunApplier) FinishSync() {
	(syncPostInventorySettlementApplier{service: a.service}).FinishSync()
}

func (a *syncChainInventorySessionRunApplier) ChainInventoryApplied() {
	syncLog.Debug("Chain inventory received",
		"blocks", a.inventoryBlocks, "queued", len(a.peerState.fetchList), "remain", a.remainNum, "peer", a.peerID)
}

func (a *syncChainInventorySessionRunApplier) RefillFetchSlotsAfterInventory() int {
	a.out = a.service.fillFetchSlotsLocked(a.now)
	return len(a.out)
}

func (a *syncChainInventorySessionRunApplier) PostInventoryRunProgress() syncdl.SessionProgress {
	return a.service.sessionProgressLocked()
}

func (ss *SyncService) fetchNextBatch() {
	ss.mu.Lock()
	if ss.syncPeer != nil {
		ss.ensurePeerStateLocked(ss.syncPeer)
	}
	out := ss.fillFetchSlotsLocked(time.Now())
	progress := ss.sessionProgressLocked()
	refill := syncdl.ApplyFetchRefillRun(syncdl.FetchRefillRunInput{
		OutboundRequests: len(out),
		Progress:         progress,
	}, syncFetchRefillRunApplier{service: ss})
	ss.mu.Unlock()
	syncdl.ApplyFetchRefillRunPostLockPlan(refill.Plan, syncFetchRefillDispatchApplier{service: ss, out: out})
}

func (ss *SyncService) fillFetchSlotsLocked(now time.Time) []outboundSyncRequest {
	ss.ensureSessionMapsLocked()
	var out []outboundSyncRequest
	for _, ps := range ss.peers {
		eligibility := syncdl.FetchSlotEligibilityInput{}
		if ps != nil {
			eligibility.PeerPresent = ps.peer != nil
			eligibility.Done = ps.done
			eligibility.ChainRequested = ps.chainRequested
			eligibility.Inflight = ps.inflight
		}
		applier := &syncFetchSlotRefillApplier{service: ss, peerState: ps, now: now}
		applyResult := syncdl.ApplyFetchSlotRefill(syncdl.FetchSlotRefillInput{Eligibility: eligibility}, applier)
		if !applyResult.Plan.Eligible {
			continue
		}
		if applyResult.RequestInventory {
			out = append(out, outboundSyncRequest{peer: ps.peer, chain: true})
		}
		if applyResult.SendFetch {
			out = append(out, outboundSyncRequest{peer: ps.peer, blocks: applyResult.SlotPlan.Batch})
		}
	}
	return out
}

func (ss *SyncService) assignRetryLocked(ps *syncPeerState) {
	if len(ss.retryList) == 0 {
		return
	}
	window := syncdl.FetchWindow{Min: ps.minFetchNum, Max: ps.lastInventoryNum}
	plan := syncdl.PlanRetryAssignment(ss.retryList, func(bid types.BlockID) syncdl.RetryCandidateFacts {
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
		return facts
	})
	syncdl.ApplyRetryAssignmentPlan(plan, syncRetryAssignmentApplier{service: ss, peerState: ps})
}

func (ss *SyncService) nextFetchBatchLocked(ps *syncPeerState) []types.BlockID {
	if len(ps.fetchList) == 0 {
		return nil
	}
	plan := syncdl.PlanNextFetchBatch(ps.fetchList, maxFetchBatch, func(bid types.BlockID) syncdl.FetchCandidateFacts {
		facts := syncdl.FetchCandidateFacts{KnownOrRequested: ss.hasBlockOrRequestLocked(bid)}
		if !facts.KnownOrRequested {
			facts.ReservedPath = ss.reserveBlockPathLocked(bid)
		}
		if facts.ReservedPath {
			_, facts.PeerRequested = ps.requestedHashes[bid.Hash]
		}
		return facts
	})
	syncdl.ApplyNextFetchBatchPlan(plan, syncNextFetchBatchApplier{peerState: ps})
	return plan.Batch
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
	progress := ss.sessionProgressLocked()
	refill := syncdl.ApplyFetchRefillRun(syncdl.FetchRefillRunInput{
		OutboundRequests: len(out),
		Progress:         progress,
	}, syncFetchRefillRunApplier{service: ss})
	ss.mu.Unlock()
	syncdl.ApplyFetchRefillRunPostLockPlan(refill.Plan, syncFetchRefillDispatchApplier{service: ss, out: out})
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
	settlementApplier := &syncFetchReceiptSettlementApplier{service: ss, peerState: ps, blockHash: blockHash}
	receiptApply := syncdl.ApplyFetchReceiptRun(syncdl.FetchReceiptRunInput{Receipt: ack}, settlementApplier)
	receiptRun := receiptApply.Plan
	bid := types.BlockID{Hash: blockHash, Num: blockNum}
	bufferFacts := syncdl.FetchedBlockBufferFacts{
		ID:          bid,
		CurrentHead: ss.chain.CurrentBlock().Number(),
	}
	if blockNum > bufferFacts.CurrentHead {
		if existing, ok := ss.blockBuffer[blockNum]; ok {
			bufferFacts.ExistingBuffered = true
			bufferFacts.ExistingBufferedHash = existing.Hash
		} else {
			_, bufferFacts.HashBuffered = ss.bufferedHash[blockHash]
			if !bufferFacts.HashBuffered {
				bufferFacts.ReservedPath = ss.reserveBlockPathLocked(bid)
			}
		}
		bufferPlan := syncdl.PlanFetchedBlockBuffer(bufferFacts)
		receiptRun = syncdl.PlanFetchReceiptRunBuffer(receiptRun, bufferPlan)
		syncdl.ApplyFetchReceiptRunLockedBufferPlan(receiptRun, syncFetchedBlockBufferApplier{service: ss, peer: peer, block: block, raw: raw})
	}
	postBufferResult := syncdl.ApplyFetchReceiptRunLockedPostBufferPlan(receiptRun, settlementApplier)
	ss.mu.Unlock()

	syncdl.ApplyFetchReceiptRunAfterUnlockPlan(receiptRun, settlementApplier)
	receiptRun = syncdl.PlanFetchReceiptRunAfterDrain(receiptRun, syncdl.FetchReceiptRunAfterDrainInput{
		OutboundRequests: postBufferResult.LockedPostBuffer.OutboundRequests,
		Progress: syncdl.SessionProgress{
			Syncing: ss.IsSyncing(),
			Paused:  ss.IsPaused(),
		},
	})
	syncdl.ApplyFetchReceiptRunDispatchPlan(receiptRun, syncFetchReceiptDispatchApplier{service: ss, out: settlementApplier.out})
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
drainLoop:
	for {
		now := time.Now()
		ss.mu.Lock()
		drainSession := syncdl.ApplyLocalDrainSessionRun(syncdl.LocalDrainSessionRunInput{
			Progress: ss.sessionProgressLocked(),
			Next:     ss.chain.CurrentBlock().Number() + 1,
			Max:      ss.importBatchLimitLocked(),
		}, syncLocalDrainSessionRunApplier{service: ss, now: now})
		switch {
		case drainSession.StopLoop:
			ss.mu.Unlock()
			break drainLoop
		case drainSession.EmptyDrain:
			prepareApplier := &syncEmptyDrainPreparationApplier{service: ss, now: now}
			prepareResult := syncdl.ApplyEmptyDrainPreparationLockedRunPlan(syncdl.EmptyDrainPreparationInput{
				Progress: ss.sessionProgressLocked(),
			}, prepareApplier, syncEmptyDrainJoinGate{service: ss, now: now}, syncEmptyDrainRunApplier{service: ss})
			out = append(out, prepareApplier.out...)
			emptyDrain := prepareResult.Run
			ss.mu.Unlock()
			syncdl.ApplyEmptyDrainRunAfterUnlockPlan(emptyDrain, syncIdleDrainApplier{service: ss}, syncFetchRefillDispatchApplier{service: ss, out: out})
			break drainLoop
		case drainSession.ImportBatch:
		default:
			ss.mu.Unlock()
			break drainLoop
		}
		ss.mu.Unlock()
		batch := drainSession.Batch
		importRun := syncdl.ApplyImportBatchRun(batch, syncImportBatchRunApplier{service: ss})
		loop := importRun.DrainLoopApply
		if loop.StopLoop {
			break drainLoop
		}
		if loop.ContinueLoop {
			continue drainLoop
		}
		continue drainLoop
	}
}

type syncLocalDrainSessionRunApplier struct {
	service *SyncService
	now     time.Time
}

func (a syncLocalDrainSessionRunApplier) ReadAndApplyStagedBodyDrain(next uint64, max int) syncdl.StagedBodyDrainRunResult {
	var result syncdl.StagedBodyDrainRunResult
	if db := a.service.chain.DB(); db != nil {
		result = syncdl.ReadAndApplyStagedBodyDrainPlan(db, next, max, syncStagedBodyDrainApplier{service: a.service, now: a.now})
	} else {
		result = syncdl.ReadAndApplyStagedBodyDrainPlan(nil, next, max, syncStagedBodyDrainApplier{service: a.service, now: a.now})
	}
	a.service.logStagedBodyReadyDrainLimit(result.Read.Ready)
	return result
}

func (a syncLocalDrainSessionRunApplier) LocalDrainRunProgress() syncdl.SessionProgress {
	return a.service.sessionProgressLocked()
}

type syncEmptyDrainPreparationApplier struct {
	service *SyncService
	now     time.Time
	out     []outboundSyncRequest
}

func (a *syncEmptyDrainPreparationApplier) BeginBufferWait(next uint64) {
	a.service.bufferWait.Begin(next, a.now)
}

func (a *syncEmptyDrainPreparationApplier) RefillFetchSlots() int {
	a.out = append(a.out, a.service.fillFetchSlotsLocked(a.now)...)
	return len(a.out)
}

func (a *syncEmptyDrainPreparationApplier) EmptyDrainRunProgress() syncdl.SessionProgress {
	return a.service.sessionProgressLocked()
}

type syncEmptyDrainJoinGate struct {
	service *SyncService
	now     time.Time
}

func (g syncEmptyDrainJoinGate) CheckJoinAvailablePeers(progress syncdl.SessionProgress) bool {
	return g.service.shouldJoinAvailablePeersLocked(g.now, progress)
}

type syncEmptyDrainRunApplier struct {
	service *SyncService
}

func (a syncEmptyDrainRunApplier) MirrorLegacyUnderLock() {
	a.service.mirrorLegacyLocked()
}

type syncFetchRefillRunApplier struct {
	service *SyncService
}

func (a syncFetchRefillRunApplier) MirrorLegacyUnderLock() {
	a.service.mirrorLegacyLocked()
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
	if ss == nil {
		return syncdl.ImportBatchLimit(0)
	}
	return syncdl.ImportBatchLimit(ss.importBatchSize)
}

func (ss *SyncService) popBufferedSyncBatchLocked(now time.Time) syncdl.BufferedBatch {
	return ss.runStagedBodyDrainLocked(now).Batch
}

func (ss *SyncService) runStagedBodyDrainLocked(now time.Time) syncdl.StagedBodyDrainRunResult {
	next := ss.chain.CurrentBlock().Number() + 1
	max := ss.importBatchLimitLocked()
	return syncLocalDrainSessionRunApplier{service: ss, now: now}.ReadAndApplyStagedBodyDrain(next, max)
}

func (ss *SyncService) logStagedBodyReadyDrainLimit(ready syncdl.StagedBodyReadyLimit) {
	switch ready.Status {
	case syncdl.StagedBodyReadyLimitProgressReadError:
		syncLog.Warn("Read sync bodies ready stage progress failed", "err", ready.StageError)
	case syncdl.StagedBodyReadyLimitUnbound:
		syncLog.Warn("Ignoring unbound sync bodies ready stage progress", "block", ready.StageRow.BlockNum)
	case syncdl.StagedBodyReadyLimitReadError:
		syncLog.Warn("Read staged block for sync bodies ready limit failed", "block", ready.StageRow.BlockNum, "hash", ready.StageRow.BlockHash, "err", ready.ReadError)
	case syncdl.StagedBodyReadyLimitStagedMissing:
		syncLog.Warn("Ignoring sync bodies ready stage without matching staged block", "block", ready.StageRow.BlockNum, "hash", ready.StageRow.BlockHash)
	case syncdl.StagedBodyReadyLimitHashMismatch:
		syncLog.Warn("Ignoring sync bodies ready stage hash mismatch", "block", ready.StageRow.BlockNum, "hash", ready.StageRow.BlockHash, "stagedHash", ready.StagedHash)
	}
}

type syncStagedBodyDrainApplier struct {
	service *SyncService
	now     time.Time
}

func (a syncStagedBodyDrainApplier) RefreshSyncBodiesReady() syncdl.StagedBodyReadyProgressRefresh {
	return a.service.writeSyncBodiesReadyProgress()
}

func (a syncStagedBodyDrainApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) syncdl.StagedBodyRestoreResult {
	return a.service.restoreSyncStagedBodiesLocked(from, limit, pruneStaleTail)
}

func (a syncStagedBodyDrainApplier) PopBufferedBatch(next uint64, limit int) syncdl.BufferedBatch {
	return syncdl.PopBufferedBatch(a.service.blockBuffer, a.service.bufferedHash, a.service.blockPath, &a.service.bufferWait, next, limit, a.now)
}

type syncIdleDrainApplier struct {
	service *SyncService
}

func (a syncIdleDrainApplier) FinishSync() {
	a.service.finishSync()
}

func (a syncIdleDrainApplier) JoinAvailablePeers() {
	a.service.joinAvailablePeers()
}

type syncFetchSlotApplier struct {
	service     *SyncService
	peerState   *syncPeerState
	currentHead uint64
}

type syncFetchSlotRefillApplier struct {
	service     *SyncService
	peerState   *syncPeerState
	now         time.Time
	currentHead uint64
}

func (a *syncFetchSlotRefillApplier) AssignRetry() {
	a.service.assignRetryLocked(a.peerState)
}

func (a *syncFetchSlotRefillApplier) NextFetchBatch() []types.BlockID {
	return a.service.nextFetchBatchLocked(a.peerState)
}

func (a *syncFetchSlotRefillApplier) FetchSlotInput(batch []types.BlockID) syncdl.FetchSlotInput {
	currentHead := uint64(0)
	if len(batch) == 0 {
		currentHead = a.service.chain.CurrentBlock().Number()
	}
	a.currentHead = currentHead
	fetchWait := time.Duration(0)
	if len(batch) > 0 {
		fetchWait = time.Until(a.peerState.nextFetchAt)
	}
	return syncdl.FetchSlotInput{
		Batch:        batch,
		FetchWait:    fetchWait,
		Done:         a.peerState.done,
		InventoryTip: a.peerState.lastInventoryNum,
		CurrentHead:  currentHead,
		Now:          a.now,
		MinInterval:  minFetchRequestInterval,
	}
}

func (a *syncFetchSlotRefillApplier) WaitLocalHead(plan syncdl.FetchSlotPlan) {
	a.fetchSlotApplier().WaitLocalHead(plan)
}

func (a *syncFetchSlotRefillApplier) RequestInventory(plan syncdl.FetchSlotPlan) {
	a.fetchSlotApplier().RequestInventory(plan)
}

func (a *syncFetchSlotRefillApplier) DelayFetch(plan syncdl.FetchSlotPlan) {
	a.fetchSlotApplier().DelayFetch(plan)
}

func (a *syncFetchSlotRefillApplier) SendFetch(plan syncdl.FetchSlotPlan) {
	a.fetchSlotApplier().SendFetch(plan)
}

func (a *syncFetchSlotRefillApplier) fetchSlotApplier() *syncFetchSlotApplier {
	return &syncFetchSlotApplier{
		service:     a.service,
		peerState:   a.peerState,
		currentHead: a.currentHead,
	}
}

func (a *syncFetchSlotApplier) WaitLocalHead(_ syncdl.FetchSlotPlan) {
	// java-tron rejects a follow-up SYNC_BLOCK_CHAIN if the summary tail is
	// below the last inventory tip it sent us on this peer (lastSyncNum >
	// lastNum). Wait until the canonical head catches up before asking this
	// peer for the next 2000-block window.
	syncLog.Trace("Sync peer waiting for local head",
		"peer", a.peerState.peer.ID(),
		"head", a.currentHead,
		"inventoryTip", a.peerState.lastInventoryNum)
}

func (a *syncFetchSlotApplier) RequestInventory(_ syncdl.FetchSlotPlan) {
	// Always re-poll once a peer's local queue drains. java-tron may have
	// produced new blocks while we were applying the previous batch; the
	// one-id inventory response is what marks sync done.
	a.peerState.chainRequested = true
}

func (a *syncFetchSlotApplier) DelayFetch(plan syncdl.FetchSlotPlan) {
	a.peerState.fetchList = append(plan.Batch, a.peerState.fetchList...)
	a.service.armPeerDelayTimerLocked(a.peerState, plan.Wait)
}

func (a *syncFetchSlotApplier) SendFetch(plan syncdl.FetchSlotPlan) {
	request := plan.Request
	a.peerState.inflight = request.Inflight
	a.peerState.pending = request.Pending
	a.peerState.pendingIDs = request.PendingIDs
	for _, hash := range request.RequestedHashes {
		a.peerState.requestedHashes[hash] = struct{}{}
		a.service.requested[hash] = a.peerState.peer.ID()
	}
	a.peerState.nextFetchAt = plan.NextFetchAt
	a.service.armPeerFetchTimerLocked(a.peerState)
}

type syncChainInventoryApplier struct {
	service   *SyncService
	peerState *syncPeerState
}

func (a *syncChainInventoryApplier) AppendAcceptedInventory(ids []types.BlockID) {
	a.peerState.fetchList = append(a.peerState.fetchList, ids...)
}

func (a *syncChainInventoryApplier) UpdateInventoryProgress(remainNum int64, target syncdl.InventoryTargetUpdate, hasTarget bool, stageTarget uint64, hasStageTarget bool) {
	a.peerState.remainNum = remainNum
	if hasTarget {
		a.peerState.lastInventoryNum = target.Window.Max
		a.peerState.minFetchNum = target.Window.Min
		a.service.targetHeadNum = target.Target
	}
}

func (a *syncChainInventoryApplier) MarkInventoryDone() {
	// java-tron sets `needSyncFromUs = false` on its peer record only when our
	// summary's last block matches its head (lostBlockIds.size == 1). While
	// needSyncFromUs is true, java-tron's InventoryMsgHandler drops every
	// inbound INV, so outbound TRX advertisements never reach the producer's
	// mempool. Mark done only for downloader's one-id completion signal.
	a.peerState.done = true
}

type syncChainInventoryPostLockApplier struct {
	service *SyncService
}

func (a syncChainInventoryPostLockApplier) WriteInventoryStageProgress(stage rawdb.StageID, target uint64) {
	a.service.writeStageProgress(stage, target, tcommon.Hash{}, false)
}

type syncRetryAssignmentApplier struct {
	service   *SyncService
	peerState *syncPeerState
}

func (a syncRetryAssignmentApplier) AppendAssignedRetries(ids []types.BlockID) {
	a.peerState.fetchList = append(a.peerState.fetchList, ids...)
}

func (a syncRetryAssignmentApplier) ReplaceRetryList(ids []types.BlockID) {
	a.service.retryList = ids
}

type syncNextFetchBatchApplier struct {
	peerState *syncPeerState
}

func (a syncNextFetchBatchApplier) ReplaceFetchList(ids []types.BlockID) {
	a.peerState.fetchList = ids
}

type syncPostInventorySettlementApplier struct {
	service *SyncService
}

func (a syncPostInventorySettlementApplier) ResetSyncUnderLock() {
	a.service.doReset()
}

func (a syncPostInventorySettlementApplier) MirrorLegacyUnderLock() {
	a.service.mirrorLegacyLocked()
}

func (a syncPostInventorySettlementApplier) TryFindSyncPeer() {
	a.service.tryFindSyncPeer(nil)
}

func (a syncPostInventorySettlementApplier) FinishSync() {
	a.service.finishSync()
}

type syncPeerFailoverApplier struct {
	service *SyncService
	exclude *p2p.Peer
	out     []outboundSyncRequest
}

func (a syncPeerFailoverApplier) ResetSyncUnderLock() {
	a.service.doReset()
}

func (a syncPeerFailoverApplier) MirrorLegacyUnderLock() {
	a.service.mirrorLegacyLocked()
}

func (a syncPeerFailoverApplier) SendOutboundRequests() {
	a.service.sendOutboundRequests(a.out)
}

func (a syncPeerFailoverApplier) TryFindSyncPeer() {
	a.service.tryFindSyncPeer(a.exclude)
}

type syncFetchReceiptSettlementApplier struct {
	service   *SyncService
	peerState *syncPeerState
	blockHash tcommon.Hash
	out       []outboundSyncRequest
}

func (a *syncFetchReceiptSettlementApplier) DeleteRequestedHash() {
	delete(a.service.requested, a.blockHash)
}

func (a *syncFetchReceiptSettlementApplier) AdvanceFetchSeq() {
	// Bump seq so any in-flight timer callback short-circuits. We stop the
	// armed timer below but the callback may already be running on another
	// goroutine and waiting on ss.mu; the seq check inside onFetchTimeout
	// rejects it.
	a.peerState.fetchSeq++
}

func (a *syncFetchReceiptSettlementApplier) UpdateInflight(inflight int) {
	a.peerState.inflight = inflight
}

func (a *syncFetchReceiptSettlementApplier) StopFetchTimer() {
	if a.peerState.fetchTimer != nil {
		a.peerState.fetchTimer.Stop()
		a.peerState.fetchTimer = nil
	}
}

func (a *syncFetchReceiptSettlementApplier) RearmFetchTimer() {
	// Re-arm the fetch timeout if blocks are still in flight. Without this a
	// peer that delivers part of a batch and then stalls leaves the sync state
	// machine wedged until external intervention.
	a.service.armPeerFetchTimerLocked(a.peerState)
}

func (a *syncFetchReceiptSettlementApplier) FillFetchSlots() int {
	a.out = a.service.fillFetchSlotsLocked(time.Now())
	return len(a.out)
}

func (a *syncFetchReceiptSettlementApplier) MirrorLegacyLocked() {
	a.service.mirrorLegacyLocked()
}

func (a *syncFetchReceiptSettlementApplier) DrainBuffered() {
	a.service.drainBufferedBlocks()
}

type syncFetchReceiptDispatchApplier struct {
	service *SyncService
	out     []outboundSyncRequest
}

func (a syncFetchReceiptDispatchApplier) SendOutboundRequests() {
	a.service.sendOutboundRequests(a.out)
}

type syncFetchRefillDispatchApplier struct {
	service *SyncService
	out     []outboundSyncRequest
}

func (a syncFetchRefillDispatchApplier) SendOutboundRequests() {
	a.service.sendOutboundRequests(a.out)
}

type syncFetchedBlockBufferApplier struct {
	service *SyncService
	peer    *p2p.Peer
	block   *types.Block
	raw     []byte
}

func (a syncFetchedBlockBufferApplier) DropConflictingFetchedBlock(plan syncdl.FetchedBlockBufferPlan) {
	syncLog.Debug("Dropping conflicting buffered sync block",
		"number", plan.ID.Num, "hash", plan.ID.Hash, "kept", plan.Kept, "peer", a.peer.ID())
}

func (a syncFetchedBlockBufferApplier) StageFetchedBlock(plan syncdl.FetchedBlockBufferPlan) {
	a.service.stageSyncBody(a.block, a.raw)
	a.service.blockBuffer[plan.ID.Num] = syncdl.NewBufferedBlock(a.peer, a.block, a.raw)
	a.service.bufferedHash[plan.ID.Hash] = struct{}{}
}

// logDecodeBatchResult logs decode failures from the off-lock raw-buffer decode
// step. A non-empty decoded prefix is still imported by the caller.
func (ss *SyncService) logDecodeBatchResult(result syncdl.BufferedBatchDecodeResult) {
	if result.Err != nil {
		syncLog.Error("Dropping undecodable buffered sync block",
			"number", result.Dropped.Num, "hash", result.Dropped.Hash, "err", result.Err)
	}
}

type syncImportBatchRunApplier struct {
	service *SyncService
}

func (a syncImportBatchRunApplier) LogDecodeBatchResult(result syncdl.BufferedBatchDecodeResult) {
	a.service.logDecodeBatchResult(result)
}

func (a syncImportBatchRunApplier) RecordBufferWait(wait time.Duration) {
	a.service.stats.AddBufferWait(wait)
}

func (a syncImportBatchRunApplier) ExecuteImportBatch(attempt syncdl.ImportBatchExecutionAttempt) (time.Duration, error) {
	result := syncdl.RunImportBatchExecutionAttemptWithStageHook(attempt, a.service.chain, time.Now)
	return result.Elapsed, result.Err
}

func (a syncImportBatchRunApplier) ApplyImportedBatchRecord(plan syncdl.ImportedBatchRecordPlan) syncdl.ImportedBatchRecordApplyResult {
	return a.service.applyImportedBatchRecord(plan)
}

func (a syncImportBatchRunApplier) PauseImport(peer *p2p.Peer, blockNum uint64, err error) {
	a.service.pauseSync(peer, blockNum, err)
}

func (ss *SyncService) applyImportedBatchRecord(plan syncdl.ImportedBatchRecordPlan) syncdl.ImportedBatchRecordApplyResult {
	recordResult := syncdl.ApplyImportedBatchRecordPlan(plan, syncImportedBatchRecordApplier{service: ss})
	ss.logImportedBatchRecordApplyResult(recordResult)
	return recordResult
}

type syncImportedBatchRecordApplier struct {
	service *SyncService
}

func (a syncImportedBatchRecordApplier) ApplyImportedBatchProgress(plan syncdl.ImportedBatchProgressPlan) syncdl.ImportedBatchProgressApplyResult {
	return syncdl.ApplyImportedBatchProgressPlan(plan, syncImportedBatchProgressApplier{service: a.service})
}

func (a syncImportedBatchRecordApplier) RecordImportedBatchStats(blocks int, txs int, elapsed time.Duration) syncdl.ImportedBatchStatsRecordResult {
	// RecordBlocks atomically (under stats.mu) appends the whole range's
	// counters and decides whether the window has elapsed. applyBlock hooks
	// have already contributed phase stats for the same applied range, so
	// recording the range as one unit keeps block counts and phase totals
	// aligned in the emitted sync summary.
	snap, emit := a.service.stats.RecordBlocks(
		blocks,
		txs,
		elapsed,
		time.Now(),
		tsync.StatsReportInterval,
	)
	return syncdl.ImportedBatchStatsRecordResult{Snapshot: snap, Emit: emit}
}

func (a syncImportedBatchRecordApplier) PrepareImportedBatchReport(plan syncdl.ImportedBatchProgressPlan, emit bool) syncdl.ImportedBatchReportPreparation {
	a.service.mu.Lock()
	var diag syncdl.Diagnostics
	if emit {
		diag = a.service.snapshotDiagnosticsLocked().WithImportedBatchProgressPlan(plan)
	}
	remain := a.service.estimatedRemainLocked()
	a.service.mirrorLegacyLocked()
	a.service.mu.Unlock()
	return syncdl.ImportedBatchReportPreparation{Diagnostics: diag, Remaining: remain}
}

func (a syncImportedBatchRecordApplier) ReportImportedBatchSegment(report syncdl.ImportedBatchRecordReport) {
	a.service.reportSegment(report.Snapshot, report.Diagnostics, report.Head, report.Remaining, report.Peer)
}

type syncImportedBatchProgressApplier struct {
	service *SyncService
}

func (a syncImportedBatchProgressApplier) WriteImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) rawdb.SyncImportProgressWriteResult {
	return a.service.writeImportedSyncProgress(deletes, rows)
}

func (a syncImportedBatchProgressApplier) RefreshSyncBodiesReady() syncdl.StagedBodyReadyProgressRefresh {
	return a.service.writeSyncBodiesReadyProgress()
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

func (ss *SyncService) writeImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) rawdb.SyncImportProgressWriteResult {
	if (len(deletes) == 0 && len(rows) == 0) || ss == nil || ss.chain == nil {
		return rawdb.SyncImportProgressWriteResult{}
	}
	db := ss.chain.DB()
	if db == nil {
		return rawdb.SyncImportProgressWriteResult{}
	}
	return rawdb.WriteSyncImportProgressBatch(db, deletes, rows)
}

func (ss *SyncService) logImportedBatchProgressApplyResult(result syncdl.ImportedBatchProgressApplyResult) {
	for _, unknown := range result.UnknownSteps {
		syncLog.Warn("Unknown imported batch progress step", "step", unknown)
	}
	if !result.HasWriteResult {
		return
	}
	for _, deleteErr := range result.WriteResult.DeleteErrors {
		syncLog.Warn("Delete sync staged block failed", "number", deleteErr.Number, "hash", deleteErr.Hash, "err", deleteErr.Err)
	}
	if result.WriteResult.ProgressError != nil {
		syncLog.Warn("Persist sync stage progress rows failed", "rows", result.WriteProgressRows, "err", result.WriteResult.ProgressError)
	}
}

func (ss *SyncService) logImportedBatchRecordApplyResult(result syncdl.ImportedBatchRecordApplyResult) {
	for _, unknown := range result.UnknownSteps {
		syncLog.Warn("Unknown imported batch record step", "step", unknown)
	}
	ss.logImportedBatchProgressApplyResult(result.ProgressApply)
}

func (ss *SyncService) stageSyncBody(block *types.Block, raw []byte) {
	if ss == nil || ss.chain == nil || block == nil {
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
	result := syncdl.AcceptStagedBody(db, block, raw, head.Number()+1, ss.targetHeadNum)
	if result.Write.StageError != nil {
		syncLog.Warn("Persist sync staged block failed", "number", block.Number(), "hash", block.Hash(), "err", result.Write.StageError)
		return
	}
	if result.Write.ProgressReadError != nil {
		syncLog.Warn("Read sync bodies stage progress failed", "err", result.Write.ProgressReadError)
	}
	if result.Write.ProgressWriteError != nil {
		syncLog.Warn("Persist sync stage progress failed", "stage", rawdb.StageSyncBodies, "block", result.Write.Number, "err", result.Write.ProgressWriteError)
	}
	if result.Ready.Refreshed {
		ss.logSyncBodiesReadyRefresh(result.Ready.Refresh)
	}
}

func (ss *SyncService) writeSyncBodiesReadyProgress() syncdl.StagedBodyReadyProgressRefresh {
	if ss == nil || ss.chain == nil {
		return syncdl.StagedBodyReadyProgressRefresh{}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.StagedBodyReadyProgressRefresh{}
	}
	head := ss.chain.CurrentBlock()
	if head == nil {
		return syncdl.StagedBodyReadyProgressRefresh{}
	}
	refresh := syncdl.RefreshStagedBodyReadyProgress(db, head.Number()+1, ss.targetHeadNum)
	ss.logSyncBodiesReadyRefresh(refresh)
	return refresh
}

func (ss *SyncService) logSyncBodiesReadyRefresh(refresh syncdl.StagedBodyReadyProgressRefresh) {
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

func (ss *SyncService) deleteImportedSyncBodiesThrough(head uint64) {
	if ss == nil || ss.chain == nil {
		return
	}
	db := ss.chain.DB()
	if db == nil {
		return
	}
	current := ss.chain.CurrentBlock()
	if current == nil {
		return
	}
	result := syncdl.DeleteImportedStagedBodiesThrough(db, head, current.Number()+1, ss.targetHeadNum)
	if result.DeleteError != nil {
		syncLog.Warn("Delete imported sync staged blocks failed", "head", head, "err", result.DeleteError)
		return
	}
	ss.logSyncBodiesReadyRefresh(result.Ready)
}

func (ss *SyncService) deleteStaleSyncBodiesFrom(blockNum uint64, lastRestoredNum uint64, lastRestoredHash tcommon.Hash, haveLastRestored bool) {
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
	result := syncdl.PruneStaleStagedBodyTail(db, blockNum, lastRestoredNum, lastRestoredHash, haveLastRestored, head.Number()+1, ss.targetHeadNum)
	if result.PruneError != nil {
		syncLog.Warn("Prune stale sync staged blocks failed", "from", blockNum, "err", result.PruneError)
		return
	}
	ss.logSyncBodiesReadyRefresh(result.Ready)
	if result.Prune.Deleted > 0 {
		if result.Prune.RewoundProgress {
			syncLog.Debug("Deleted stale sync staged block tail", "from", blockNum, "count", result.Prune.Deleted, "rewoundTo", result.Prune.RewindBlock)
			return
		}
		syncLog.Debug("Deleted stale sync staged block tail", "from", blockNum, "count", result.Prune.Deleted)
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
	ctx = diag.AppendImportPlanLogFields(ctx)
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
	detail = diag.AppendImportPlanLogFields(detail)
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
	progress := ss.sessionProgressLocked()
	plan := syncdl.PlanPeerFailover(syncdl.PeerFailoverInput{
		OutboundRequests: len(out),
		Progress:         progress,
	})
	failoverApplier := syncPeerFailoverApplier{service: ss, exclude: stalePeer, out: out}
	syncdl.ApplyPeerFailoverLockedPlan(plan, failoverApplier)
	ss.mu.Unlock()
	syncLog.Warn("Fetch timeout, failing over",
		"peer", stalePeer.ID(),
		"timeout", ethcommon.PrettyDuration(ss.fetchTimeout),
		"inflight", inflight)
	syncdl.ApplyPeerFailoverDispatchPlan(plan, failoverApplier)
	syncdl.ApplyPeerFailoverAfterDispatchPlan(plan, failoverApplier)
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
	progress := ss.sessionProgressLocked()
	plan := syncdl.PlanPeerFailover(syncdl.PeerFailoverInput{
		OutboundRequests: len(out),
		Progress:         progress,
	})
	failoverApplier := syncPeerFailoverApplier{service: ss, exclude: peer, out: out}
	syncdl.ApplyPeerFailoverLockedPlan(plan, failoverApplier)
	ss.mu.Unlock()
	syncLog.Info("Sync peer disconnected", "peer", peer.ID())
	syncdl.ApplyPeerFailoverDispatchPlan(plan, failoverApplier)
	syncdl.ApplyPeerFailoverAfterDispatchPlan(plan, failoverApplier)
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
