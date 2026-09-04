package net

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/types"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Slice 1 of the SyncService refactor moved these tunables into
// net/sync/constants.go. The lowercase aliases here keep call sites and
// tests under net/ untouched until Slice 6 deletes net/sync.go entirely;
// at that point every remaining reference moves to tsync.* directly.
const (
	maxChainInventorySize       = tsync.MaxChainInventorySize
	maxFetchBatch               = tsync.MaxFetchBatch
	maxSyncImportBatch          = tsync.MaxImportBatch
	maxStagedImportBatch        = tsync.MaxStagedImportBatch
	maxParallelSyncPeers        = tsync.MaxParallelSyncPeers
	minFetchRequestInterval     = tsync.MinFetchRequestInterval
	maxBufferedRunaheadBlocks   = tsync.MaxBufferedRunaheadBlocks
	maxBufferedRunaheadBytes    = tsync.MaxBufferedRunaheadBytes
	resumeBufferedRunaheadBytes = tsync.ResumeBufferedRunaheadBytes
	alwaysFetchRunaheadBlocks   = tsync.AlwaysFetchRunaheadBlocks
	peerJoinAttemptInterval     = 2 * time.Second
	peerRangeSummaryInterval    = time.Minute
	syncProgressMinETACoverage  = 80.0
	syncTargetFutureAllowance   = time.Hour
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
	requestedHashes map[tcommon.Hash]uint64

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

	stopAtHeight     atomic.Uint64
	stopAtConfigured atomic.Bool

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
	bufferedBytes int64
	// fetchBackpressured is a service-wide hysteresis gate. The older per-ID
	// byte check still admitted a near-tip strip independently on every peer;
	// under dense historical blocks that allowed dozens of peers to keep a
	// multi-gigabyte raw buffer full. Once the high-water mark is crossed, only
	// one peer at a time may refill that anti-starvation strip until the buffer
	// drains below the low-water mark.
	fetchBackpressured bool
	targetHeadNum      atomic.Uint64
	// syncedTipNum is the drain cursor: the highest block this session has
	// popped for import. Under async-commit depth>2 the committed CurrentBlock
	// lags the applied tip by up to the pipeline depth, so popping from
	// CurrentBlock+1 would re-target an already-imported (and deleted) buffer
	// entry and break the drain after every batch. Tracking the cursor lets the
	// drain pop the whole buffered run in one pass. Equals CurrentBlock when
	// async commit is off (the production default), so that path is unchanged.
	syncedTipNum uint64

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

	// stats accumulates per-window throughput counters used for the compact
	// "Sync import progress" line. Owns its own mutex; lock
	// order is ss.mu (outer) → stats.mu (inner) when both are held.
	// onApplyStats is the only writer that does NOT also hold ss.mu —
	// stats.mu serializes its own state so the off-sync producer path
	// is safe.
	stats *tsync.Stats

	importBatchSize int

	// watchdog runs the periodic isolation check. Owns its own goroutine
	// and ticker; Start/Stop fan-out launches and joins it.
	watchdog *tsync.Watchdog

	progressMu       sync.Mutex
	progressStop     chan struct{}
	progressWG       sync.WaitGroup
	derivedMu        sync.Mutex
	derivedRunning   bool
	derivedPending   bool
	derivedWG        sync.WaitGroup
	inventoryStageMu sync.Mutex

	stalledFetchLogMu sync.Mutex
	stalledFetchLog   stalledFetchRecoveryLogState

	bufferWait syncdl.BufferWaitTracker

	lastPeerJoinAttempt time.Time

	peerRangeRejects     atomic.Uint64
	lastPeerRangeSummary atomic.Int64

	// completeHooks run after a successful sync session has been reset. Hooks
	// must return promptly; they are intended for non-blocking stage wakeups.
	completeHooks []func()
}

// transactionLookupStageBatchBlocks bounds one derived-index catch-up pass.
// The stage runs after canonical import settlement, so a large restored
// downloader buffer cannot hold the chain writer lock for an unbounded ETL
// sort/load.
const (
	transactionLookupStageBatchBlocks = 4096
	stateHistoryIndexStageMinBlocks   = 256
	stateHistoryIndexStageBatchBlocks = 4096
	// A deep async InsertSession normally spans every locally available chunk.
	// Bound it so derived stages cannot lag indefinitely while peers keep the
	// downloader continuously supplied. This still reuses one executor across
	// dozens of 100-block chunks and matches the maximum ETL pass size.
	syncDerivedStageBarrierBlocks = 4096
)

var (
	transactionLookupStagePassesCounter       = metrics.NewRegisteredCounter("sync/stage/tx_lookup/passes", nil)
	transactionLookupStageBlocksCounter       = metrics.NewRegisteredCounter("sync/stage/tx_lookup/blocks", nil)
	transactionLookupStageAncientCounter      = metrics.NewRegisteredCounter("sync/stage/tx_lookup/ancient_blocks", nil)
	transactionLookupStageHotCounter          = metrics.NewRegisteredCounter("sync/stage/tx_lookup/hot_iterator_blocks", nil)
	transactionLookupStageTransactionsCounter = metrics.NewRegisteredCounter("sync/stage/tx_lookup/transactions", nil)
	transactionLookupStageInterruptedCounter  = metrics.NewRegisteredCounter("sync/stage/tx_lookup/interrupted", nil)
	transactionLookupStageNanosCounter        = metrics.NewRegisteredCounter("sync/stage/tx_lookup/nanos", nil)
	stateHistoryIndexStagePassesCounter       = metrics.NewRegisteredCounter("sync/stage/state_history_index/passes", nil)
	stateHistoryIndexStageBlocksCounter       = metrics.NewRegisteredCounter("sync/stage/state_history_index/blocks", nil)
	stateHistoryIndexStageChangesCounter      = metrics.NewRegisteredCounter("sync/stage/state_history_index/changes", nil)
	stateHistoryIndexStageAppliedCounter      = metrics.NewRegisteredCounter("sync/stage/state_history_index/etl_applied", nil)
	stateHistoryIndexStageInputBytesCounter   = metrics.NewRegisteredCounter("sync/stage/state_history_index/etl_input_bytes", nil)
	stateHistoryIndexStageBatchWritesCounter  = metrics.NewRegisteredCounter("sync/stage/state_history_index/etl_batch_writes", nil)
	stateHistoryIndexStageInterruptedCounter  = metrics.NewRegisteredCounter("sync/stage/state_history_index/interrupted", nil)
	stateHistoryIndexStageNanosCounter        = metrics.NewRegisteredCounter("sync/stage/state_history_index/nanos", nil)
	signatureLookaheadBatchesCounter          = metrics.NewRegisteredCounter("sync/signature_lookahead/batches", nil)
	signatureLookaheadDecodeNanosCounter      = metrics.NewRegisteredCounter("sync/signature_lookahead/decode_nanos", nil)
	signatureLookaheadReusedCounter           = metrics.NewRegisteredCounter("sync/signature_lookahead/reused", nil)
	signatureLookaheadDiscardedCounter        = metrics.NewRegisteredCounter("sync/signature_lookahead/discarded", nil)
	signatureLookaheadMismatchedCounter       = metrics.NewRegisteredCounter("sync/signature_lookahead/mismatched", nil)
	signatureLookaheadReadyCounter            = metrics.NewRegisteredCounter("sync/signature_lookahead/ready_at_import", nil)
	signatureLookaheadPendingCounter          = metrics.NewRegisteredCounter("sync/signature_lookahead/pending_at_import", nil)
	signatureLookaheadLeadNanosCounter        = metrics.NewRegisteredCounter("sync/signature_lookahead/lead_nanos", nil)
	signatureLookaheadOverlapNanosCounter     = metrics.NewRegisteredCounter("sync/signature_lookahead/overlap_after_import_start_nanos", nil)
)

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

// AddSyncCompleteHook registers a callback invoked after every completed sync
// session has cleared its staged-body and in-memory tracking state. The hook
// runs without ss.mu held and must return promptly.
func (ss *SyncService) AddSyncCompleteHook(hook func()) {
	if ss == nil || hook == nil {
		return
	}
	ss.mu.Lock()
	ss.completeHooks = append(ss.completeHooks, hook)
	ss.mu.Unlock()
}

func (ss *SyncService) notifySyncComplete() {
	ss.mu.Lock()
	hooks := append([]func(){}, ss.completeHooks...)
	ss.mu.Unlock()
	for _, hook := range hooks {
		hook()
	}
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
	if size > maxStagedImportBatch {
		return fmt.Errorf("sync import batch %d exceeds staged import batch cap %d", size, maxStagedImportBatch)
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

// onApplyStats folds one block's execution telemetry into the rolling window.
// Fires synchronously from applyBlock on the importing
// goroutine — during sync that is drainBufferedBlocks; during normal
// operation it is the broadcast/producer path. Stats owns its own mutex
// so no ss.mu acquisition here; this matters because the producer path
// may invoke applyBlock from a goroutine that already holds the producer
// lock, and we don't want to deadlock with any future ss.mu holder.
func (ss *SyncService) onApplyStats(block *types.Block, s core.ApplyStats) {
	txs := 0
	if block != nil {
		txs = len(block.Transactions())
	}
	ss.stats.AddApplyBlockWithTxs(txs, s)
}

// Start launches the isolation watchdog goroutine.
func (ss *SyncService) Start() {
	ss.stopping.Store(false)
	ss.pauseIfStopHeightReached()
	if ss.watchdog != nil {
		ss.watchdog.Start()
	}
	ss.startProgressReporter()
}

// Stop shuts down the sync service, cancels any in-progress sync, and waits
// for the active drain to leave InsertBlocks before shutdown continues.
func (ss *SyncService) Stop() {
	ss.stopping.Store(true)
	ss.stopProgressReporter()
	if ss.watchdog != nil {
		ss.watchdog.Stop()
	}
	ss.mu.Lock()
	// Quiesce the session before joining the drain, but retain staged bodies.
	// The active drain may already own an off-lock batch and still needs those
	// rows to atomically publish its imported progress. Deleting them here races
	// that publication and leaves the sync-stage cursor behind the canonical
	// head even though the block commit itself succeeded.
	syncdl.ApplySessionResetPlan(syncdl.PlanSessionQuiesce(), syncSessionResetApplier{service: ss})
	ss.mu.Unlock()
	ss.waitForDrain()
	ss.derivedWG.Wait()
	// No new bodies can enter after stopping is set, and the sole drain owner is
	// now gone, so durable staged-body cleanup can no longer invalidate an
	// in-flight imported-progress proof.
	ss.deleteAllSyncBodies()
}

type syncProgressWindow struct {
	label string
	from  time.Time
}

func nextSyncProgressBoundary(now time.Time) time.Time {
	minute := now.Minute() - now.Minute()%5
	boundary := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), minute, 0, 0, now.Location())
	if !boundary.After(now) {
		boundary = boundary.Add(5 * time.Minute)
	}
	return boundary
}

func dueSyncProgressWindows(boundary time.Time) []syncProgressWindow {
	windows := []syncProgressWindow{{label: "5m", from: boundary.Add(-5 * time.Minute)}}
	if boundary.Minute()%30 == 0 {
		windows = append(windows, syncProgressWindow{label: "30m", from: boundary.Add(-30 * time.Minute)})
	}
	if boundary.Minute() == 0 {
		windows = append(windows, syncProgressWindow{label: "1h", from: boundary.Add(-time.Hour)})
	}
	if boundary.Hour() == 0 && boundary.Minute() == 0 {
		previousDay := boundary.AddDate(0, 0, -1)
		windows = append(windows, syncProgressWindow{label: "1d", from: previousDay})
	}
	return windows
}

func (ss *SyncService) startProgressReporter() {
	ss.progressMu.Lock()
	if ss.progressStop != nil {
		ss.progressMu.Unlock()
		return
	}
	stop := make(chan struct{})
	ss.progressStop = stop
	ss.progressWG.Add(1)
	ss.progressMu.Unlock()
	go func() {
		defer ss.progressWG.Done()
		for {
			boundary := nextSyncProgressBoundary(time.Now())
			timer := time.NewTimer(time.Until(boundary))
			select {
			case <-timer.C:
				ss.reportCalendarProgress(boundary)
			case <-stop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
	}()
}

func (ss *SyncService) stopProgressReporter() {
	ss.progressMu.Lock()
	stop := ss.progressStop
	ss.progressStop = nil
	if stop != nil {
		close(stop)
	}
	ss.progressMu.Unlock()
	if stop != nil {
		ss.progressWG.Wait()
	}
}

func (ss *SyncService) reportCalendarProgress(boundary time.Time) {
	status := ss.Status()
	if !status.Active {
		return
	}
	target := status.TargetHead
	if target < status.AppliedTip {
		target = status.AppliedTip
	}
	progress := 100.0
	if target > 0 {
		progress = float64(status.AppliedTip) * 100 / float64(target)
	}
	for _, window := range dueSyncProgressWindows(boundary) {
		speed := ss.stats.CalendarSpeedSummary(window.from, boundary)
		coverage := 0.0
		if speed.Window > 0 {
			coverage = float64(speed.Coverage) * 100 / float64(speed.Window)
		}
		warming := coverage < syncProgressMinETACoverage
		ctx := []any{
			"window", window.label,
			"from", window.from.Format(time.RFC3339),
			"to", boundary.Format(time.RFC3339),
			"coveragePct", round2(coverage),
			"warming", warming,
			"head", status.AppliedTip,
			"target", target,
			"chainProgressPct", round2(progress),
			"remaining", status.Remaining,
			"windowBlocks", round2(speed.Blocks),
			"avgBlocksPerSec", round2(speed.Average),
			"minBlocksPerSec", round2(speed.Minimum),
			"maxBlocksPerSec", round2(speed.Maximum),
		}
		if !warming && speed.Average > 0 && status.Remaining > 0 {
			etaSec := float64(status.Remaining) / speed.Average
			ctx = append(ctx, "eta", ethcommon.PrettyDuration(time.Duration(etaSec*float64(time.Second))))
		}
		syncLog.Info("Sync progress", ctx...)
	}
}

// IsSyncing returns whether sync is in progress.
func (ss *SyncService) IsSyncing() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.syncing
}

// RecoverStalledFetch re-kicks the fetch scheduler of an active sync session
// whose head has not advanced for a full watchdog StallThreshold. The trigger
// is the async-commit depth>2 lost wakeup: the last fillFetchSlots ran against
// a commit-worker-lagged CurrentBlock and parked every peer on "waiting for
// local head", leaving no in-flight fetch or armed timer to re-evaluate once
// the committed head caught up. Re-running the drain finishes any deep-pipeline
// session (advancing the committed head to the applied tip) and re-fills the
// fetch slots against that now-accurate head, re-requesting the next inventory
// window. Called only by the watchdog goroutine — never the commit worker — so
// re-entering the drain here cannot wedge the commit queue. No-op when not
// syncing or paused.
func (ss *SyncService) RecoverStalledFetch() {
	if ss.stopping.Load() {
		return
	}
	ss.mu.Lock()
	syncing := ss.syncing
	ss.mu.Unlock()
	if !syncing || ss.pause.Paused() {
		return
	}
	if ss.StallRecoveryBlocked() {
		return
	}
	before := ss.chain.CurrentBlock().Number()
	ss.drainBufferedBlocks()
	after := ss.chain.CurrentBlock().Number()
	ss.logStalledFetchRecovery(before, after, ss.IsSyncing(), time.Now())
}

// StallRecoveryBlocked lets the watchdog distinguish a known long-running
// commitment rebuild from an unresponsive fetch scheduler. The rebuild owns
// its own start/progress/completion logs, so polling it every 30 seconds would
// add noise and resource contention without advancing sync.
func (ss *SyncService) StallRecoveryBlocked() bool {
	blocked := statedomains.CommitmentRebuildActive()
	if blocked {
		ss.stalledFetchLogMu.Lock()
		ss.stalledFetchLog = stalledFetchRecoveryLogState{}
		ss.stalledFetchLogMu.Unlock()
	}
	return blocked
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

// SyncStatus is a point-in-time downloader snapshot for operational APIs.
type SyncStatus struct {
	Active                bool
	Paused                bool
	SyncPeerCount         int
	TargetHead            uint64
	AppliedTip            uint64
	SessionBlocks         int
	SessionTransactions   int
	Remaining             int64
	Inflight              int
	BufferedBlocks        int
	BufferedBytes         int64
	FetchBackpressured    bool
	RequestedBlocks       int
	RetryBlocks           int
	RetainedDecodedBlocks int
	RetainedDecodedBytes  int64
	PauseBlock            uint64
	PauseTime             time.Time
	PauseError            error
	LastPeerFailure       string
	LastPeerFailureTime   time.Time
}

func (ss *SyncService) Status() SyncStatus {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	paused, pauseBlock, pauseTime, pauseErr := ss.pause.Status()
	stats := ss.stats.CurrentSnapshot()
	return SyncStatus{
		Active:              ss.syncing,
		Paused:              paused,
		SyncPeerCount:       len(ss.peers),
		TargetHead:          ss.targetHeadNum.Load(),
		AppliedTip:          ss.syncedTipNum,
		SessionBlocks:       stats.TotalBlocks,
		SessionTransactions: stats.TotalTxs,
		Remaining:           ss.estimatedRemainLocked(),
		Inflight:            ss.inflight,
		BufferedBlocks:      len(ss.blockBuffer),
		BufferedBytes:       ss.bufferedBytes,
		FetchBackpressured:  ss.fetchBackpressured,
		RequestedBlocks:     len(ss.requested),
		RetryBlocks:         len(ss.retryList),
		PauseBlock:          pauseBlock,
		PauseTime:           pauseTime,
		PauseError:          pauseErr,
	}
}

// ErrSyncStopHeightReached identifies a planned operator audit boundary.
var ErrSyncStopHeightReached = errors.New("configured sync stop height reached")

// SetStopAtHeight configures an inclusive sync boundary. The configured block
// is committed, blocks above it are left staged/buffered, and sync then enters
// the sticky pause state for inspection.
func (ss *SyncService) SetStopAtHeight(height uint64) {
	ss.stopAtHeight.Store(height)
	ss.stopAtConfigured.Store(true)
	ss.pauseIfStopHeightReached()
}

func (ss *SyncService) configuredStopHeight() (uint64, bool) {
	if !ss.stopAtConfigured.Load() {
		return 0, false
	}
	return ss.stopAtHeight.Load(), true
}

func (ss *SyncService) pauseIfStopHeightReached() bool {
	height, configured := ss.configuredStopHeight()
	if !configured || ss.chain == nil || ss.chain.CurrentBlock() == nil || ss.chain.CurrentBlock().Number() < height {
		return false
	}
	ss.pauseAtStopHeight(height)
	return true
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
	if ss.pauseIfStopHeightReached() {
		return
	}
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
			ss.reportUnavailableSyncPeer(peer, needFrom, lowest, peerHead)
		}
		return
	}
	now := time.Now()
	ss.mu.Lock()
	ss.ensureSessionMapsLocked()
	if ss.syncing && ss.peers[peer.ID()] == nil && len(ss.peers) >= maxParallelSyncPeers {
		ss.mu.Unlock()
		return
	}
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

func (ss *SyncService) reportUnavailableSyncPeer(peer *p2p.Peer, needFrom, peerLowest, peerHead uint64) {
	if ss == nil || peer == nil {
		return
	}
	syncLog.Debug("Sync peer outside available range",
		"peer", peer.ID(),
		"needFrom", needFrom,
		"peerLowest", peerLowest,
		"peerHead", peerHead)
	ss.peerRangeRejects.Add(1)
	now := time.Now()
	last := ss.lastPeerRangeSummary.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < peerRangeSummaryInterval {
		return
	}
	if !ss.lastPeerRangeSummary.CompareAndSwap(last, now.UnixNano()) {
		return
	}
	rejected := ss.peerRangeRejects.Swap(0)
	syncLog.Info("Historical sync peers unavailable",
		"rejectedSinceLastReport", rejected,
		"needFrom", needFrom,
		"samplePeerLowest", peerLowest,
		"samplePeerHead", peerHead)
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
	ss.bufferedBytes = 0
	ss.fetchBackpressured = false
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
	ss.syncedTipNum = head
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

func (a syncSessionStartupApplier) CompleteCurrentHeadSyncPipeline(repair syncdl.SyncPipelineProgressRepairResult) syncdl.SyncPipelineProgressHeadCompletion {
	var result syncdl.SyncPipelineProgressHeadCompletion
	if a.service == nil || a.service.chain == nil || a.headBlock == nil {
		return result
	}
	plan := syncdl.PlanSyncPipelineProgressHeadCompletion(repair, a.headBlock.Number(), a.headBlock.Hash())
	db := a.service.chain.DB()
	if db == nil {
		return syncdl.ApplySyncPipelineProgressHeadCompletionPlan(plan, nil)
	}
	if repair.Kept == 0 && !plan.HasHeadPrefix {
		evidence, errorStage, err := readSyncPipelineProgressHeadRecoveryEvidence(db)
		if err != nil {
			result.Plan = plan
			result.ErrorStage = errorStage
			result.WriteError = fmt.Errorf("read sync pipeline current-head recovery evidence %s: %w", errorStage, err)
			return result
		}
		plan = syncdl.PlanSyncPipelineProgressHeadCompletionWithEvidence(repair, a.headBlock.Number(), a.headBlock.Hash(), evidence)
	}
	result = syncdl.ApplySyncPipelineProgressHeadCompletionPlan(plan, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) error {
		return rawdb.WriteStageProgressWithHash(db, stage, blockNum, blockHash)
	})
	if result.WriteError != nil {
		syncLog.Warn("Complete sync pipeline current head failed", "stage", result.ErrorStage, "head", plan.Head, "hash", plan.HeadHash, "err", result.WriteError)
	}
	return result
}

func readSyncPipelineProgressHeadRecoveryEvidence(db ethdb.KeyValueStore) (syncdl.SyncPipelineProgressHeadRecoveryEvidence, rawdb.StageID, error) {
	var evidence syncdl.SyncPipelineProgressHeadRecoveryEvidence
	if db == nil {
		return evidence, "", nil
	}
	var err error
	evidence.CanonicalFinish, evidence.HasCanonicalFinish, err = rawdb.ReadStageProgressRow(db, rawdb.StageFinish)
	if err != nil {
		return evidence, rawdb.StageFinish, err
	}
	evidence.TxLookup, evidence.HasTxLookup, err = rawdb.ReadStageProgressRow(db, rawdb.StageTxLookup)
	if err != nil {
		return evidence, rawdb.StageTxLookup, err
	}
	evidence.SyncInventory, evidence.HasSyncInventory, err = rawdb.ReadStageProgressRow(db, rawdb.StageSyncInventory)
	if err != nil {
		return evidence, rawdb.StageSyncInventory, err
	}
	return evidence, "", nil
}

func (a syncSessionStartupApplier) RestoreInventoryTarget(inventoryFloor uint64) {
	a.service.targetHeadNum.Store(a.service.restoreSyncInventoryTarget(inventoryFloor))
}

func (a syncSessionStartupApplier) DeleteImportedBodies(through uint64) syncdl.ImportedStagedBodyCleanup {
	return a.service.deleteImportedSyncBodiesThrough(through)
}

func (a syncSessionStartupApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) syncdl.StagedBodyRestoreResult {
	return a.service.restoreSyncStagedBodiesLocked(from, limit, pruneStaleTail)
}

func (a syncSessionStartupApplier) RefreshBodiesReady() syncdl.StagedBodyReadyProgressRefresh {
	return a.service.writeSyncBodiesReadyProgress()
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
	target := restore.Target
	if maxTarget := ss.maxPlausibleSyncTarget(time.Now()); maxTarget > 0 && target > maxTarget {
		syncLog.Warn("Discarding implausible persisted sync inventory target", "target", target, "maxTarget", maxTarget, "head", head)
		target = head
		if err := rawdb.WriteStageProgress(ss.chain.DB(), rawdb.StageSyncInventory, head); err != nil {
			syncLog.Warn("Repair implausible sync inventory target failed", "head", head, "err", err)
		}
	}
	return target
}

// maxPlausibleSyncTarget bounds untrusted peer height hints by projecting from
// the current canonical block's timestamp at TRON's three-second slot interval.
// This works during historical catch-up even though TRON's configured genesis
// timestamp is zero. A one-hour allowance is deliberately generous for clock
// skew and near-future inventory propagation.
func (ss *SyncService) maxPlausibleSyncTarget(now time.Time) uint64 {
	if ss == nil || ss.chain == nil {
		return 0
	}
	head := ss.chain.CurrentBlock()
	if head == nil || head.Timestamp() < 0 || params.BlockProducedInterval <= 0 {
		return 0
	}
	projectTo := now.UnixMilli() + syncTargetFutureAllowance.Milliseconds()
	if head.Timestamp() >= projectTo {
		return head.Number()
	}
	elapsed := uint64(projectTo - head.Timestamp())
	additional := elapsed / uint64(params.BlockProducedInterval)
	if additional > ^uint64(0)-head.Number() {
		return ^uint64(0)
	}
	return head.Number() + additional
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
	if !result.HasSyncPipelineRepair && !result.HasImportedBodyCleanup && !result.HasStagedBodyRestore && !result.HasBodiesReadyRefresh && !result.HasSyncPipelineOrderRepair && !result.HasSyncPipelineOrder {
		return
	}
	repair := result.SyncPipelineRepairResult
	headCompletion := result.SyncPipelineHeadCompletion
	cleanup := result.ImportedBodyCleanup
	restore := result.StagedBodyRestore
	readyRefresh := result.BodiesReadyRefresh
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
	syncLog.Info("Sync startup repair summary", syncStartupRepairSummaryContext(result)...)
	if !syncLog.DebugEnabled() {
		return
	}
	syncLog.Debug("Sync startup repair diagnostics",
		"syncStartupRepairComplete", repair.Complete,
		"syncStartupRepairKept", repair.Kept,
		"syncStartupRepairMissing", repair.Missing,
		"syncStartupRepairDeleted", repair.Deleted,
		"syncStartupRepairHasBlocked", repair.HasBlocked,
		"syncStartupRepairFirstBlocked", repair.FirstBlockedStage,
		"syncStartupRepairInterrupted", repair.Interrupted,
		"syncStartupRepairErrorStage", repair.ErrorStage,
		"syncStartupRepairRows", len(repair.Repairs),
		"syncStartupHeadCompletionChecked", result.HasSyncPipelineHead,
		"syncStartupHeadCompletionHasPrefix", headCompletion.Plan.HasHeadPrefix,
		"syncStartupHeadCompletionRecovered", headCompletion.Plan.RecoveredFromHeadEvidence,
		"syncStartupHeadCompletionLastStage", headCompletion.Plan.LastStage,
		"syncStartupHeadCompletionLastBlock", headCompletion.Plan.LastBlock,
		"syncStartupHeadCompletionFillStages", len(headCompletion.Plan.FillStages),
		"syncStartupHeadCompletionWritten", headCompletion.Written,
		"syncStartupHeadCompletionComplete", headCompletion.Complete,
		"syncStartupHeadCompletionErrorStage", headCompletion.ErrorStage,
		"syncStartupImportedCleanupChecked", result.HasImportedBodyCleanup,
		"syncStartupImportedCleanupDeleted", cleanup.Deleted,
		"syncStartupImportedCleanupFailed", cleanup.Failed(),
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
		"syncStartupReadyRefreshChecked", result.HasBodiesReadyRefresh,
		"syncStartupReadyRefreshUpdated", readyRefresh.Updated,
		"syncStartupReadyRefreshDeleted", readyRefresh.Deleted,
		"syncStartupReadyRefreshFailed", readyRefresh.Failed(),
		"syncStartupReadyRefreshFrontier", readyRefresh.Frontier.Number,
		"syncStartupReadyRefreshNextMissing", readyRefresh.Frontier.NextMissing,
		"syncStartupInterrupted", result.Interrupted,
		"syncStartupErrorStep", result.ErrorStep,
	)
}

func syncStartupRepairSummaryContext(result syncdl.SessionStartupApplyResult) []any {
	repair := result.SyncPipelineRepairResult
	ctx := []any{
		"repairComplete", repair.Complete,
		"repairRows", len(repair.Repairs),
	}
	if repair.Kept > 0 {
		ctx = append(ctx, "kept", repair.Kept)
	}
	if repair.Missing > 0 {
		ctx = append(ctx, "missing", repair.Missing)
	}
	if repair.Deleted > 0 {
		ctx = append(ctx, "deleted", repair.Deleted)
	}
	if repair.HasBlocked {
		ctx = append(ctx, "blockedStage", repair.FirstBlockedStage)
	}
	if repair.Interrupted {
		ctx = append(ctx, "repairInterrupted", true)
	}
	if repair.ErrorStage != "" {
		ctx = append(ctx, "repairErrorStage", repair.ErrorStage)
	}
	if result.HasSyncPipelineHead {
		head := result.SyncPipelineHeadCompletion
		if head.Complete || head.Written > 0 || head.ErrorStage != "" {
			ctx = append(ctx, "headComplete", head.Complete, "headStagesWritten", head.Written)
			if head.Plan.RecoveredFromHeadEvidence {
				ctx = append(ctx, "headRecovered", true)
			}
			if head.ErrorStage != "" {
				ctx = append(ctx, "headErrorStage", head.ErrorStage)
			}
		}
	}
	if result.HasImportedBodyCleanup {
		cleanup := result.ImportedBodyCleanup
		if cleanup.Deleted > 0 || cleanup.Failed() {
			ctx = append(ctx, "importedBodiesDeleted", cleanup.Deleted, "importedCleanupFailed", cleanup.Failed())
		}
	}
	if issues := len(result.SyncPipelineOrderIssues); issues > 0 {
		ctx = append(ctx, "orderIssues", issues, "firstOrderIssue", result.SyncPipelineOrderIssues[0].String())
	}
	if readErrors := len(result.SyncPipelineOrderErrors); readErrors > 0 {
		ctx = append(ctx, "orderReadErrors", readErrors, "firstOrderReadErrorStage", result.SyncPipelineOrderErrors[0].Stage)
	}
	if result.HasSyncPipelineOrderRepair {
		order := result.SyncPipelineOrderRepair
		if order.Deleted > 0 || order.Updated > 0 || order.Interrupted || order.ErrorStage != "" {
			ctx = append(ctx, "orderDeleted", order.Deleted, "orderUpdated", order.Updated)
			if order.Interrupted {
				ctx = append(ctx, "orderInterrupted", true)
			}
			if order.ErrorStage != "" {
				ctx = append(ctx, "orderErrorStage", order.ErrorStage)
			}
		}
	}
	if result.HasSyncPipelineCursor {
		cursor := result.SyncPipelineCursor
		ctx = append(ctx, "cursorRows", cursor.StageRows, "cursorComplete", cursor.Complete)
		if cursor.HasLast {
			ctx = append(ctx, "cursorLastStage", cursor.LastStage, "cursorLastBlock", cursor.LastBlock)
		}
		if cursor.HasNext {
			ctx = append(ctx, "cursorNextStage", cursor.NextStage)
		}
		if cursor.HasBlocked {
			ctx = append(ctx, "cursorBlocked", true)
		}
		if cursor.Interrupted {
			ctx = append(ctx, "cursorInterrupted", true)
		}
		if cursor.ErrorStage != "" {
			ctx = append(ctx, "cursorErrorStage", cursor.ErrorStage)
		}
	}
	if result.HasStagedBodyRestore {
		restore := result.StagedBodyRestore
		if restore.Restored > 0 || restore.NeedPruneTail {
			ctx = append(ctx,
				"stagedBodiesRestored", restore.Restored,
				"stagedTargetHead", restore.TargetHead,
				"stagedNextExpected", restore.NextExpected)
			if restore.NeedPruneTail {
				ctx = append(ctx, "stagedPruneFrom", restore.PruneFrom)
			}
		}
	}
	if result.HasBodiesReadyRefresh {
		ready := result.BodiesReadyRefresh
		if ready.Updated || ready.Deleted || ready.Failed() {
			ctx = append(ctx,
				"bodiesReadyUpdated", ready.Updated,
				"bodiesReadyDeleted", ready.Deleted,
				"bodiesReadyFailed", ready.Failed(),
				"bodiesReadyFrontier", ready.Frontier.Number,
				"bodiesReadyNextMissing", ready.Frontier.NextMissing)
		}
	}
	if result.Interrupted {
		ctx = append(ctx, "interrupted", true, "errorStep", result.ErrorStep)
	}
	return ctx
}

func (ss *SyncService) restoreSyncStagedBodiesLocked(start uint64, limit int, pruneStaleTail bool) syncdl.StagedBodyRestoreResult {
	if ss == nil || ss.chain == nil || limit <= 0 {
		return syncdl.StagedBodyRestoreResult{NextExpected: start}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.StagedBodyRestoreResult{NextExpected: start}
	}
	result := syncdl.RestoreStagedBodies(start, limit, ss.targetHeadNum.Load(), ss.blockBuffer, ss.bufferedHash, &ss.blockPath, func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error {
		return rawdb.IterateSyncStagedBlocksFrom(db, start, fn)
	})
	ss.bufferedBytes += result.RestoredBytes
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
	a.service.targetHeadNum.Store(targetHead)
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
		requestedHashes: make(map[tcommon.Hash]uint64),
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
	stopHeight, stopConfigured := ss.configuredStopHeight()
	maxTarget := ss.maxPlausibleSyncTarget(time.Now())
	for _, bid := range inv.Ids {
		if bid.Number <= 0 || (stopConfigured && uint64(bid.Number) > stopHeight) || (maxTarget > 0 && uint64(bid.Number) > maxTarget) {
			continue
		}
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
			CurrentTarget:  ss.targetHeadNum.Load(),
			MaxTarget:      maxTarget,
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
		"blocks", a.inventoryBlocks, "queued", len(a.peerState.fetchList), "remaining", a.remainNum, "peer", a.peerID)
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
	effectiveTipNum := ss.effectiveSyncTipLocked()
	backpressured := ss.updateFetchBackpressureLocked()
	for _, ps := range ss.peers {
		eligibility := syncdl.FetchSlotEligibilityInput{}
		if ps != nil {
			eligibility.PeerPresent = ps.peer != nil
			eligibility.Done = ps.done
			eligibility.ChainRequested = ps.chainRequested
			eligibility.Inflight = ps.inflight
		}
		applier := &syncFetchSlotRefillApplier{service: ss, peerState: ps, now: now, effectiveTipNum: effectiveTipNum}
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
		// At high water keep exactly one peer feeding the small contiguous
		// anti-starvation strip. Already-requested bodies remain accepted and the
		// local drain continues; this only stops new fan-out.
		if backpressured && applyResult.SendFetch {
			break
		}
	}
	return out
}

func (ss *SyncService) updateFetchBackpressureLocked() bool {
	if ss.fetchBackpressured {
		if ss.bufferedBytes <= resumeBufferedRunaheadBytes {
			ss.fetchBackpressured = false
		}
	} else if ss.bufferedBytes >= maxBufferedRunaheadBytes {
		ss.fetchBackpressured = true
	}
	return ss.fetchBackpressured
}

func (ss *SyncService) withinRunaheadBudgetLocked(bid types.BlockID, effectiveTipNum uint64) bool {
	if bid.Num > effectiveTipNum+maxBufferedRunaheadBlocks {
		return false
	}
	if ss.fetchBackpressured && bid.Num > effectiveTipNum+alwaysFetchRunaheadBlocks {
		return false
	}
	return true
}

func (ss *SyncService) effectiveSyncTipLocked() uint64 {
	var tip uint64
	if ss != nil && ss.chain != nil {
		if head := ss.chain.CurrentBlock(); head != nil {
			tip = head.Number()
		}
	}
	if ss.syncedTipNum > tip {
		tip = ss.syncedTipNum
	}
	return tip
}

func (ss *SyncService) assignRetryLocked(ps *syncPeerState, effectiveTipNum uint64) {
	if len(ss.retryList) == 0 {
		return
	}
	window := syncdl.FetchWindow{Min: ps.minFetchNum, Max: ps.lastInventoryNum}
	plan := syncdl.PlanRetryAssignment(ss.retryList, func(bid types.BlockID) syncdl.RetryCandidateFacts {
		if stopHeight, configured := ss.configuredStopHeight(); configured && bid.Num > stopHeight {
			return syncdl.RetryCandidateFacts{KnownOrRequested: true}
		}
		facts := syncdl.RetryCandidateFacts{KnownOrRequested: ss.hasBlockOrRequestLocked(bid)}
		if facts.KnownOrRequested {
			return facts
		}
		if !ss.withinRunaheadBudgetLocked(bid, effectiveTipNum) {
			return facts
		}
		facts.InWindow = window.Contains(bid)
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

func (ss *SyncService) nextFetchBatchLocked(ps *syncPeerState, effectiveTipNum uint64) []types.BlockID {
	if len(ps.fetchList) == 0 {
		return nil
	}
	batch, remaining := syncdl.DrainNextFetchBatch(ps.fetchList, maxFetchBatch, func(bid types.BlockID) syncdl.FetchCandidateFacts {
		if stopHeight, configured := ss.configuredStopHeight(); configured && bid.Num > stopHeight {
			return syncdl.FetchCandidateFacts{KnownOrRequested: true}
		}
		if !ss.withinRunaheadBudgetLocked(bid, effectiveTipNum) {
			return syncdl.FetchCandidateFacts{Deferred: true}
		}
		facts := syncdl.FetchCandidateFacts{KnownOrRequested: ss.hasBlockOrRequestLocked(bid)}
		if !facts.KnownOrRequested {
			facts.ReservedPath = ss.reserveBlockPathLocked(bid)
		}
		if facts.ReservedPath {
			_, facts.PeerRequested = ps.requestedHashes[bid.Hash]
		}
		return facts
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
	if block == nil {
		return false
	}
	return ss.handleFetchedBlock(peer, block.ID(), block, raw)
}

func (ss *SyncService) handleFetchedBlock(peer *p2p.Peer, id types.BlockID, block *types.Block, raw []byte) bool {
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
	receiptApplier := &syncFetchReceiptSessionRunApplier{
		syncFetchReceiptSettlementApplier: &syncFetchReceiptSettlementApplier{
			service:   ss,
			peerState: ps,
			blockHash: id.Hash,
		},
		peer:  peer,
		id:    id,
		block: block,
		raw:   raw,
	}
	receiptRun := syncdl.ApplyFetchReceiptSessionLockedRunFromState(syncdl.FetchReceiptSessionLockedRunInput{
		State: syncdl.FetchReceiptState{
			Inflight:   ps.inflight,
			Pending:    ps.pending,
			PendingIDs: ps.pendingIDs,
		},
		Hash: id.Hash,
		Num:  id.Num,
	}, receiptApplier)
	if !receiptRun.Plan.Settlement.Accepted {
		ss.mu.Unlock()
		return true
	}
	ss.mu.Unlock()

	if receiptRun.Buffer.StageFailed {
		ss.pauseSync(peer, id.Num, receiptRun.Buffer.StageAcceptance.FailureError())
		return true
	}

	syncdl.ApplyFetchReceiptSessionAfterUnlockPlan(receiptRun.Plan, receiptRun.OutboundRequests, receiptApplier, syncFetchReceiptDispatchApplier{service: ss, out: receiptApplier.out})
	return true
}

// HandleRawBlock is the wire receive entrypoint used by TronHandler. Sync
// admission and staging need only the BlockID, so decode the small header and
// leave the pointer-rich transaction tree in raw form until the body reaches
// the contiguous import frontier. The importer still performs the same one
// authoritative full decode before any consensus validation or execution.
func (ss *SyncService) HandleRawBlock(peer *p2p.Peer, raw []byte) bool {
	id, err := types.BlockIDFromRaw(raw)
	if err != nil {
		if ss.stopping.Load() {
			return true
		}
		ss.mu.Lock()
		syncing := ss.syncing
		ss.mu.Unlock()
		return syncing
	}
	return ss.handleFetchedBlock(peer, id, nil, raw)
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

// syncSignatureLookahead owns one decoded-but-uncommitted staged-body batch.
// Decoding may overlap the current import. Signature preprocessing starts only
// after the current batch's run finishes, bounding the process to one recovery
// worker pool while still overlapping the remainder of ordered execution.
type syncSignatureLookahead struct {
	batch          syncdl.BufferedBatch
	done           chan struct{}
	selected       bool
	decode         syncdl.BufferedBatchDecodeResult
	prewarm        *core.SignaturePrewarmRun
	prewarmStarted time.Time
}

func (ss *SyncService) startSignatureLookahead(previous *core.SignaturePrewarmRun) *syncSignatureLookahead {
	if ss == nil || ss.chain == nil || ss.stopping.Load() {
		return nil
	}
	ahead := &syncSignatureLookahead{done: make(chan struct{})}
	go func() {
		defer close(ahead.done)
		// Selection can restore staged bodies from disk. Keep it off the current
		// batch's import start path as well as keeping protobuf decode off-lock.
		ss.mu.Lock()
		if !ss.syncing || ss.pause.Paused() || ss.stopping.Load() {
			ss.mu.Unlock()
			return
		}
		ahead.batch = ss.runStagedBodyDrainLocked(time.Now()).Batch
		ss.mu.Unlock()
		if len(ahead.batch.Buffered) == 0 {
			return
		}
		ahead.selected = true
		signatureLookaheadBatchesCounter.Inc(1)
		started := time.Now()
		ahead.decode = syncdl.DecodeBufferedBatch(&ahead.batch)
		signatureLookaheadDecodeNanosCounter.Inc(time.Since(started).Nanoseconds())
		if len(ahead.batch.Blocks) == 0 {
			return
		}
		// Do not overlap two full signature worker pools. The current run can
		// still overlap its own ordered execution; once it is done, the next
		// run overlaps the remaining state/commit work of the current batch.
		previous.Wait()
		ahead.prewarmStarted = time.Now()
		ahead.prewarm = ss.chain.StartSignaturePrewarm(ahead.batch.Blocks)
	}()
	return ahead
}

func sameBufferedBatch(left, right syncdl.BufferedBatch) bool {
	if len(left.Buffered) != len(right.Buffered) {
		return false
	}
	for i := range left.Buffered {
		l, r := left.Buffered[i], right.Buffered[i]
		if l.Num != r.Num || l.Hash != r.Hash || len(l.Raw) != len(r.Raw) {
			return false
		}
	}
	return len(left.Buffered) > 0
}

// take returns the lookahead result only when a fresh lock-held drain plan still
// selects the same immutable buffered rows. A changed/removed batch is joined
// and discarded; the caller then decodes the fresh selection normally.
func (a *syncSignatureLookahead) take(batch syncdl.BufferedBatch) (syncdl.BufferedBatch, syncdl.BufferedBatchDecodeResult, *core.SignaturePrewarmRun, time.Time, bool) {
	if a == nil {
		return syncdl.BufferedBatch{}, syncdl.BufferedBatchDecodeResult{}, nil, time.Time{}, false
	}
	<-a.done
	if !a.selected {
		return syncdl.BufferedBatch{}, syncdl.BufferedBatchDecodeResult{}, nil, time.Time{}, false
	}
	if !sameBufferedBatch(a.batch, batch) {
		a.prewarm.Wait()
		signatureLookaheadMismatchedCounter.Inc(1)
		return syncdl.BufferedBatch{}, syncdl.BufferedBatchDecodeResult{}, nil, time.Time{}, false
	}
	signatureLookaheadReusedCounter.Inc(1)
	return a.batch, a.decode, a.prewarm, a.prewarmStarted, true
}

func (a *syncSignatureLookahead) discard() {
	if a == nil {
		return
	}
	<-a.done
	a.prewarm.Wait()
	if a.selected {
		signatureLookaheadDiscardedCounter.Inc(1)
	}
}

func (ss *SyncService) drainBufferedBlocksOnce() {
	var out []outboundSyncRequest
	// One drain may consume several small local import chunks. Reuse one
	// canonical executor across all of them so synchronous sync avoids reopening
	// StateDB/CommitScope at every chunk boundary. The synchronous session flushes
	// latest-domain writes after each chunk to retain ordinary InsertBlocks
	// visibility; async sessions keep their shared scope until the final drain
	// barrier. Deep async additionally keeps a buffered worker pipeline full. The
	// deep session is created up front to retain its existing empty-drain settlement
	// barrier; synchronous and depth-2 sessions start only after a real batch is
	// available.
	depth := ss.chain.PipelinedCommitDepth()
	var sess *core.InsertSession
	if depth > 2 {
		sess = ss.chain.BeginSyncInsertSession()
	}
	var lastPeer *p2p.Peer
	var resumePhases []syncdl.ImportStagePhasePlan
	paused := false
	pauseBlock := uint64(0)
	sessionAppliedBlocks := uint64(0)
	rotatedForDerivedStages := false
	var lookahead *syncSignatureLookahead
	defer func() { lookahead.discard() }()
drainLoop:
	for {
		now := time.Now()
		ss.mu.Lock()
		drainSession := syncdl.ApplyLocalDrainSessionRun(syncdl.LocalDrainSessionRunInput{
			Progress: ss.sessionProgressLocked(),
			Next:     ss.nextDrainBlockLocked(),
			Max:      ss.importBatchLimitLocked(),
		}, syncLocalDrainSessionRunApplier{service: ss, now: now})
		switch {
		case drainSession.StopLoop:
			ss.mu.Unlock()
			break drainLoop
		case drainSession.EmptyDrain:
			if sess != nil {
				ss.mu.Unlock()
				ferr := sess.Finish()
				commitBarrier := ss.applyImportDrainCommitBarrier(ferr, paused, lastPeer)
				paused = commitBarrier.Paused
				sess = nil
				if ferr != nil {
					break drainLoop
				}
				ss.mu.Lock()
			}
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
		var (
			decode         syncdl.BufferedBatchDecodeResult
			prewarm        *core.SignaturePrewarmRun
			prewarmStarted time.Time
			prewarmedAhead bool
		)
		if lookahead != nil {
			ahead := lookahead
			lookahead = nil
			if aheadBatch, aheadDecode, aheadPrewarm, aheadStarted, ok := ahead.take(batch); ok {
				batch = aheadBatch
				decode = aheadDecode
				prewarm = aheadPrewarm
				prewarmStarted = aheadStarted
				prewarmedAhead = true
			}
		}
		if !prewarmedAhead {
			decode = syncdl.DecodeBufferedBatch(&batch)
			if len(batch.Blocks) > 0 {
				prewarmStarted = time.Now()
				prewarm = ss.chain.StartSignaturePrewarm(batch.Blocks)
			}
		}
		ss.logDecodeBatchResult(decode)
		badPeer, commitErr := ss.commitDecodedBufferedBatch(&batch, decode, time.Now())
		if commitErr != nil {
			prewarm.Wait()
			failedBlock := decode.Dropped.Num
			if failedBlock == 0 && len(batch.Buffered) > 0 {
				failedBlock = batch.Buffered[0].Num
			}
			ss.pauseSync(badPeer, failedBlock, commitErr)
			paused = true
			pauseBlock = failedBlock
			break drainLoop
		}
		if decode.Err != nil && badPeer != nil {
			badPeer.RecordDisconnectCause(fmt.Sprintf("invalid sync block body #%d: %v", decode.Dropped.Num, decode.Err))
			go badPeer.Close()
		}
		if len(batch.Blocks) == 0 {
			prewarm.Wait()
			continue drainLoop
		}
		lookahead = ss.startSignatureLookahead(prewarm)
		if sess == nil {
			sess = ss.chain.BeginSyncInsertSession()
		}
		importRun := syncdl.ApplyImportBatchRun(batch, syncImportBatchRunApplier{
			service:                ss,
			session:                sess,
			flushLatestAfterInsert: depth == 0,
			prewarm:                prewarm,
			prewarmStarted:         prewarmStarted,
			prewarmedAhead:         prewarmedAhead,
		})
		// ExecuteImportBatch normally transfers the handle to core, which joins it
		// before returning. Keep an unconditional caller-side join for downloader
		// plans that stop earlier (for example, a second metadata validation
		// rejecting the batch before Execute); no worker may outlive batch ownership.
		prewarm.Wait()
		if applied := importRun.Run.Outcome.Applied; applied > 0 {
			sessionAppliedBlocks += uint64(applied)
		}
		rotateForDerivedStages := sess != nil && !paused && sessionAppliedBlocks >= syncDerivedStageBarrierBlocks
		importLoop := syncdl.PlanImportBatchDrainLoopFinalization(importRun)
		if importLoop.HasLastPeer {
			lastPeer = importLoop.LastPeer
		}
		if importLoop.Pause {
			paused = true
			pauseBlock = importRun.Run.Outcome.PauseNum
		}
		if importLoop.YieldResumePhase {
			resumePhases = importLoop.ResumePhases
			ss.logImportResumePhaseYield(importLoop.ResumePhasePlan)
			// An async session can continue feeding later chunks while the worker
			// finishes this chunk's commitment/finish suffix. The worker commits
			// FIFO, so the final pending suffix at the session barrier proves every
			// earlier chunk too; stopping here would drain the worker after every
			// local chunk and defeat the cross-chunk session. Depth 2 still uses an
			// unbuffered rendezvous queue, so this does not widen its in-flight cap.
			if depth > 0 && !rotateForDerivedStages {
				continue drainLoop
			}
		} else if depth > 0 && importRun.Run.HasRecord && !importRun.Run.RecordProgressFailed() {
			// A later chunk that completed every phase can supersede an older
			// pending suffix: FIFO commitment means its canonical boundary also
			// proves the older one has finished.
			resumePhases = nil
		}
		if rotateForDerivedStages {
			rotatedForDerivedStages = true
			break drainLoop
		}
		if importLoop.StopLoop {
			break drainLoop
		}
		if importLoop.ContinueLoop {
			continue drainLoop
		}
		continue drainLoop
	}
	commitBarrier := syncdl.PlanImportDrainCommitBarrier(syncdl.ImportDrainCommitBarrierInput{Paused: paused})
	if sess != nil {
		commitBarrier = ss.applyImportDrainCommitBarrier(sess.Finish(), paused, lastPeer)
		paused = commitBarrier.Paused
	}
	if commitBarrier.FinishOK && !paused && ss.pauseIfStopHeightReached() {
		paused = true
	}
	// A later chunk may have paused the drain after an earlier chunk's async
	// commitment suffix became pending. Preserve that already-finished prefix
	// when every suffix task is strictly before the failed block, but never
	// re-arm the drain from a paused session.
	publishPausedPrefix := paused && syncdl.ImportResumePhaseSuffixPrecedesBlock(resumePhases, pauseBlock)
	resumePublish := ss.publishImportResumePhaseProgress(resumePhases, commitBarrier.FinishOK, paused && !publishPausedPrefix)
	if commitBarrier.FinishOK && !paused && sessionAppliedBlocks > 0 {
		ss.scheduleDerivedSyncStages()
	}
	if !publishPausedPrefix {
		ss.applyImportResumePhaseDrainContinuation(resumePublish, lastPeer)
	}
	if rotatedForDerivedStages && !paused {
		ss.requestDrainAgain()
	}
}

func (ss *SyncService) applyImportResumePhaseDrainContinuation(run syncdl.ImportResumePhasePublishFinalizationRunApplyResult, lastPeer *p2p.Peer) {
	resumeContinuation := syncdl.PlanImportResumePhaseDrainContinuation(run)
	if resumeContinuation.Pause {
		ss.pauseSync(lastPeer, resumeContinuation.PauseBlock, resumeContinuation.Err)
		return
	}
	if resumeContinuation.DrainAgain {
		ss.requestDrainAgain()
	}
}

func (ss *SyncService) applyImportDrainCommitBarrier(finishErr error, paused bool, lastPeer *p2p.Peer) syncdl.ImportDrainCommitBarrierPlan {
	pauseBlock := uint64(0)
	if finishErr != nil && !paused {
		pauseBlock = ss.chain.CurrentBlock().Number() + 1
	}
	commitBarrier := syncdl.PlanImportDrainCommitBarrier(syncdl.ImportDrainCommitBarrierInput{
		FinishErr:  finishErr,
		Paused:     paused,
		LastPeer:   lastPeer,
		PauseBlock: pauseBlock,
	})
	if commitBarrier.Pause {
		ss.pauseSync(commitBarrier.PausePeer, commitBarrier.PauseBlock, commitBarrier.Err)
	}
	return commitBarrier
}

func (ss *SyncService) logImportResumePhaseYield(plan syncdl.ImportStagePhasePlan) {
	taskCount := len(plan.Tasks)
	if taskCount == 0 {
		syncLog.Debug("Yielding sync import drain to staged scheduler",
			"syncPhase", plan.Phase,
			"syncPhaseCanonicalStage", plan.CanonicalStage,
			"syncPhaseStage", plan.SyncStage,
			"syncPhaseTasks", taskCount)
		return
	}
	first := plan.Tasks[0]
	last := plan.Tasks[taskCount-1]
	syncLog.Debug("Yielding sync import drain to staged scheduler",
		"syncPhase", plan.Phase,
		"syncPhaseCanonicalStage", plan.CanonicalStage,
		"syncPhaseStage", plan.SyncStage,
		"syncPhaseTasks", taskCount,
		"syncPhaseFromBlock", first.BlockNum,
		"syncPhaseFromHash", first.BlockHash,
		"syncPhaseToBlock", last.BlockNum,
		"syncPhaseToHash", last.BlockHash)
}

func (ss *SyncService) publishImportResumePhaseProgress(phases []syncdl.ImportStagePhasePlan, finishOK bool, paused bool) syncdl.ImportResumePhasePublishFinalizationRunApplyResult {
	run := syncdl.ApplyImportResumePhasePublishFinalizationRun(syncdl.ImportResumePhasePublishFinalizationInput{
		Phases:   phases,
		FinishOK: finishOK,
		Paused:   paused,
	}, syncImportResumePhasePublishApplier{service: ss})
	ss.logImportResumePhasePublishResult(run.Publish.Publish)
	return run
}

func (ss *SyncService) requestDrainAgain() {
	ss.drainMu.Lock()
	ss.drainAgain = true
	ss.drainMu.Unlock()
}

func (ss *SyncService) logImportResumePhasePublishResult(result syncdl.ImportResumePhasePublishApplyResult) {
	if len(result.Plan.Phases) == 0 {
		return
	}
	if !result.Plan.OK {
		for _, decision := range result.Plan.Decisions {
			if decision.Status == syncdl.ImportResumePhasePublishReady {
				continue
			}
			syncLog.Warn("Sync import resume phase not publishable",
				"syncPhase", decision.Phase,
				"syncPhaseCanonicalStage", decision.CanonicalStage,
				"syncPhaseStage", decision.SyncStage,
				"syncPhaseBlock", decision.TargetBlock,
				"syncPhaseHash", decision.TargetHash,
				"syncPhaseStatus", decision.Status.String(),
				"canonicalBlock", decision.CanonicalRow.BlockNum,
				"canonicalHash", decision.CanonicalRow.BlockHash,
				"canonicalHasHash", decision.CanonicalRow.HasBlockHash,
				"canonicalAhead", decision.CanonicalAhead,
				"canonicalProofHash", decision.CanonicalHash,
				"canonicalProofHasHash", decision.HasCanonicalHash,
				"targetCanonicalHash", decision.TargetCanonicalHash,
				"targetCanonicalHasHash", decision.HasTargetCanonicalHash,
				"syncStageBlock", decision.SyncRow.BlockNum,
				"syncStageHash", decision.SyncRow.BlockHash,
				"syncStageHasHash", decision.SyncRow.HasBlockHash,
				"syncUpstreamStage", decision.UpstreamStage,
				"syncUpstreamBlock", decision.UpstreamRow.BlockNum,
				"syncUpstreamHash", decision.UpstreamRow.BlockHash,
				"syncUpstreamHasHash", decision.UpstreamRow.HasBlockHash,
				"err", decision.Err)
			return
		}
		return
	}
	if result.WriteError != nil {
		syncLog.Warn("Persist sync import resume phase failed", "rows", result.Rows, "err", result.WriteError)
		return
	}
	if len(result.Plan.RecoveryPhases) > 0 {
		phase := result.Plan.Phases[0]
		target := phase.Tasks[len(phase.Tasks)-1]
		canonicalAhead := false
		canonicalBlock := target.BlockNum
		for _, decision := range result.Plan.Decisions {
			if decision.CanonicalAhead {
				canonicalAhead = true
				if decision.CanonicalRow.BlockNum > canonicalBlock {
					canonicalBlock = decision.CanonicalRow.BlockNum
				}
			}
		}
		syncLog.Info("Recovered missing sync import phase prefix",
			"resumePhase", phase.Phase,
			"resumeStage", phase.SyncStage,
			"block", target.BlockNum,
			"hash", target.BlockHash,
			"canonicalAhead", canonicalAhead,
			"canonicalBlock", canonicalBlock,
			"recoveredPhases", len(result.Plan.RecoveryPhases),
			"rows", result.Rows)
		return
	}
	syncLog.Debug("Published sync import resume phase",
		"rows", result.Rows,
		"recoveredPhases", len(result.Plan.RecoveryPhases))
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
	peeked := ss.runStagedBodyDrainLocked(now).Batch
	if len(peeked.Buffered) == 0 {
		return peeked
	}
	batch := syncdl.PopBufferedBatch(ss.blockBuffer, ss.bufferedHash, ss.blockPath, &ss.bufferWait, peeked.Buffered[0].Num, len(peeked.Buffered), now)
	for i := range batch.Buffered {
		ss.bufferedBytes -= int64(len(batch.Buffered[i].Raw))
	}
	if ss.bufferedBytes < 0 {
		ss.bufferedBytes = 0
	}
	if n := len(batch.Buffered); n > 0 && batch.Buffered[n-1].Num > ss.syncedTipNum {
		ss.syncedTipNum = batch.Buffered[n-1].Num
	}
	return batch
}

func (ss *SyncService) runStagedBodyDrainLocked(now time.Time) syncdl.StagedBodyDrainRunResult {
	next := ss.nextDrainBlockLocked()
	max := ss.importBatchLimitLocked()
	if stopHeight, configured := ss.configuredStopHeight(); configured {
		if next > stopHeight {
			max = 0
		} else if span := stopHeight - next + 1; span < uint64(max) {
			max = int(span)
		}
	}
	return syncLocalDrainSessionRunApplier{service: ss, now: now}.ReadAndApplyStagedBodyDrain(next, max)
}

func (ss *SyncService) nextDrainBlockLocked() uint64 {
	current := uint64(0)
	if ss != nil && ss.chain != nil {
		if head := ss.chain.CurrentBlock(); head != nil {
			current = head.Number()
		}
	}
	if ss.syncedTipNum > current {
		return ss.syncedTipNum + 1
	}
	return current + 1
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
	case syncdl.StagedBodyReadyLimitNumberMismatch:
		syncLog.Warn("Ignoring sync bodies ready stage block-number mismatch", "block", ready.StageRow.BlockNum, "hash", ready.StageRow.BlockHash, "stagedBlock", ready.StagedRow.Number, "stagedHash", ready.StagedHash)
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
	return syncdl.PeekBufferedBatch(a.service.blockBuffer, next, limit)
}

// commitDecodedBufferedBatch completes the second half of staged-body drain.
// The caller decoded batch without ss.mu. Under the lock we verify ownership,
// durably remove only a malformed row, release the decoded prefix, and leave
// the untouched suffix reserved in the buffer. The malformed ID is put back
// on the global retry queue after its path reservation is released.
func (ss *SyncService) commitDecodedBufferedBatch(batch *syncdl.BufferedBatch, decode syncdl.BufferedBatchDecodeResult, now time.Time) (*p2p.Peer, error) {
	if ss == nil || batch == nil {
		return nil, fmt.Errorf("sync: cannot commit nil decoded buffer batch")
	}
	prefix := len(batch.Blocks)
	if prefix > len(batch.Buffered) {
		return nil, fmt.Errorf("sync: decoded prefix %d exceeds buffered batch %d", prefix, len(batch.Buffered))
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	verifyOwned := func(buffered syncdl.BufferedBlock) error {
		owned, ok := ss.blockBuffer[buffered.Num]
		if !ok {
			return fmt.Errorf("sync: buffered block #%d disappeared before decode commit", buffered.Num)
		}
		if owned.Hash != buffered.Hash || len(owned.Raw) != len(buffered.Raw) {
			return fmt.Errorf("sync: buffered block #%d changed before decode commit", buffered.Num)
		}
		return nil
	}
	for i := 0; i < prefix; i++ {
		if err := verifyOwned(batch.Buffered[i]); err != nil {
			return nil, err
		}
	}

	var badPeer *p2p.Peer
	if decode.Err != nil {
		if prefix >= len(batch.Buffered) || batch.Buffered[prefix].Num != decode.Dropped.Num || batch.Buffered[prefix].Hash != decode.Dropped.Hash {
			return nil, fmt.Errorf("sync: decoded failure is not the buffered prefix boundary: %w", decode.Err)
		}
		if err := verifyOwned(decode.Dropped); err != nil {
			return nil, err
		}
		if ss.chain == nil || ss.chain.DB() == nil {
			return decode.Dropped.Peer, fmt.Errorf("sync: cannot delete malformed staged body #%d without database", decode.Dropped.Num)
		}
		if err := rawdb.DeleteSyncStagedBlock(ss.chain.DB(), decode.Dropped.Num); err != nil {
			return decode.Dropped.Peer, fmt.Errorf("sync: delete malformed staged body #%d: %w", decode.Dropped.Num, err)
		}
		refresh := ss.writeSyncBodiesReadyProgress()
		if refresh.Failed() {
			refreshErr := refresh.Frontier.Error
			if refreshErr == nil {
				refreshErr = refresh.WriteError
			}
			if refreshErr == nil {
				refreshErr = refresh.DeleteError
			}
			return decode.Dropped.Peer, fmt.Errorf("sync: refresh bodies-ready after malformed staged body #%d: %w", decode.Dropped.Num, refreshErr)
		}
		badPeer = decode.Dropped.Peer
	}

	release := func(buffered syncdl.BufferedBlock, recordWait bool) {
		wait := ss.bufferWait.End(buffered.Num, now)
		delete(ss.blockBuffer, buffered.Num)
		ss.blockPath.Release(buffered.Num)
		delete(ss.bufferedHash, buffered.Hash)
		ss.bufferedBytes -= int64(len(buffered.Raw))
		if recordWait {
			batch.BufferWaits = append(batch.BufferWaits, wait)
		}
	}
	for i := 0; i < prefix; i++ {
		release(batch.Buffered[i], true)
	}
	if prefix > 0 {
		if popped := batch.Buffered[prefix-1].Num; popped > ss.syncedTipNum {
			ss.syncedTipNum = popped
		}
	}
	if decode.Err != nil {
		release(decode.Dropped, false)
		bid := types.BlockID{Hash: decode.Dropped.Hash, Num: decode.Dropped.Num}
		queued := false
		for _, retry := range ss.retryList {
			if retry.Hash == bid.Hash {
				queued = true
				break
			}
		}
		if !queued {
			ss.retryList = append(ss.retryList, bid)
		}
	}
	if ss.bufferedBytes < 0 {
		ss.bufferedBytes = 0
	}
	batch.Buffered = batch.Buffered[:prefix]
	return badPeer, nil
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
	service         *SyncService
	peerState       *syncPeerState
	now             time.Time
	currentHead     uint64
	effectiveTipNum uint64
}

func (a *syncFetchSlotRefillApplier) AssignRetry() {
	a.service.assignRetryLocked(a.peerState, a.effectiveTipNum)
}

func (a *syncFetchSlotRefillApplier) NextFetchBatch() []types.BlockID {
	return a.service.nextFetchBatchLocked(a.peerState, a.effectiveTipNum)
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
	if a.peerState.fetchDelayTimer == nil {
		a.service.armPeerDelayTimerLocked(a.peerState, minFetchRequestInterval)
	}
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
	for _, bid := range plan.Batch {
		a.peerState.requestedHashes[bid.Hash] = bid.Num
		a.service.requested[bid.Hash] = a.peerState.peer.ID()
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
		a.service.targetHeadNum.Store(target.Target)
		if a.peerState.minFetchNum > 0 {
			for hash, num := range a.peerState.requestedHashes {
				if num < a.peerState.minFetchNum {
					delete(a.peerState.requestedHashes, hash)
				}
			}
		}
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
	if a.service == nil || a.service.chain == nil || target == 0 {
		return
	}
	db := a.service.chain.DB()
	if db == nil {
		return
	}
	a.service.inventoryStageMu.Lock()
	defer a.service.inventoryStageMu.Unlock()
	if current, ok, err := rawdb.ReadStageProgress(db, stage); err != nil {
		syncLog.Warn("Read sync inventory stage before monotonic update failed", "stage", stage, "err", err)
		return
	} else if ok && current >= target {
		return
	}
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

type syncFetchReceiptSessionRunApplier struct {
	*syncFetchReceiptSettlementApplier
	peer  *p2p.Peer
	id    types.BlockID
	block *types.Block
	raw   []byte
}

func (a *syncFetchReceiptSessionRunApplier) PlanFetchedBlockBuffer(_ syncdl.FetchReceiptRunPlan) syncdl.FetchedBlockBufferPlan {
	if a == nil || a.service == nil {
		return syncdl.FetchedBlockBufferPlan{}
	}
	return syncdl.PlanFetchedBlockBufferFromReader(a.id, syncFetchedBlockBufferFactReader{service: a.service})
}

func (a *syncFetchReceiptSessionRunApplier) FetchReceiptRunProgress() syncdl.SessionProgress {
	if a == nil || a.service == nil {
		return syncdl.SessionProgress{}
	}
	return syncdl.SessionProgress{
		Syncing: a.service.IsSyncing(),
		Paused:  a.service.IsPaused(),
	}
}

func (a *syncFetchReceiptSessionRunApplier) DropConflictingFetchedBlock(plan syncdl.FetchedBlockBufferPlan) {
	syncFetchedBlockBufferApplier{service: a.service, peer: a.peer, id: a.id, block: a.block, raw: a.raw}.DropConflictingFetchedBlock(plan)
}

func (a *syncFetchReceiptSessionRunApplier) StageFetchedBlock(plan syncdl.FetchedBlockBufferPlan) syncdl.StagedBodyAcceptance {
	return syncFetchedBlockBufferApplier{service: a.service, peer: a.peer, id: a.id, block: a.block, raw: a.raw}.StageFetchedBlock(plan)
}

type syncFetchedBlockBufferFactReader struct {
	service *SyncService
}

func (r syncFetchedBlockBufferFactReader) CurrentFetchedBlockHead() (uint64, bool) {
	if r.service == nil || r.service.chain == nil {
		return 0, false
	}
	head := r.service.chain.CurrentBlock()
	if head == nil {
		return 0, false
	}
	return head.Number(), true
}

func (r syncFetchedBlockBufferFactReader) ExistingFetchedBlock(number uint64) (syncdl.BufferedBlock, bool) {
	if r.service == nil {
		return syncdl.BufferedBlock{}, false
	}
	block, ok := r.service.blockBuffer[number]
	return block, ok
}

func (r syncFetchedBlockBufferFactReader) HasFetchedBlockHash(hash tcommon.Hash) bool {
	if r.service == nil {
		return false
	}
	_, ok := r.service.bufferedHash[hash]
	return ok
}

func (r syncFetchedBlockBufferFactReader) ReserveFetchedBlockPath(id types.BlockID) bool {
	if r.service == nil {
		return false
	}
	return r.service.reserveBlockPathLocked(id)
}

type syncFetchedBlockBufferApplier struct {
	service *SyncService
	peer    *p2p.Peer
	id      types.BlockID
	block   *types.Block
	raw     []byte
}

func (a syncFetchedBlockBufferApplier) DropConflictingFetchedBlock(plan syncdl.FetchedBlockBufferPlan) {
	syncLog.Debug("Dropping conflicting buffered sync block",
		"number", plan.ID.Num, "hash", plan.ID.Hash, "kept", plan.Kept, "peer", a.peer.ID())
}

func (a syncFetchedBlockBufferApplier) StageFetchedBlock(plan syncdl.FetchedBlockBufferPlan) syncdl.StagedBodyAcceptance {
	result := a.service.stageSyncBodyID(a.id, a.block, a.raw)
	if result.Failed() {
		return result
	}
	entry := syncdl.BufferedBlock{
		Raw:  syncdl.RawBlockBytes(a.block, a.raw),
		Hash: a.id.Hash,
		Num:  a.id.Num,
		Peer: a.peer,
	}
	a.service.blockBuffer[plan.ID.Num] = entry
	a.service.bufferedHash[plan.ID.Hash] = struct{}{}
	a.service.bufferedBytes += int64(len(entry.Raw))
	return result
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
	service                *SyncService
	session                *core.InsertSession
	flushLatestAfterInsert bool
	prewarm                *core.SignaturePrewarmRun
	prewarmStarted         time.Time
	prewarmedAhead         bool
}

// syncImportStageHookExecutor selects the sync-only insertion surface. It
// keeps the downloader's small stage-hook interface while ensuring ordinary
// producer/gossip insertion cannot accidentally defer tx lookup rows.
type syncImportStageHookExecutor struct {
	chain   *core.BlockChain
	session *core.InsertSession
	prewarm *core.SignaturePrewarmRun
}

func (e syncImportStageHookExecutor) InsertBlocksWithStageHook(blocks []*types.Block, hook core.StageProgressHook) error {
	if e.session != nil {
		return e.session.InsertBlocksWithStageHookPrewarmed(blocks, hook, e.prewarm)
	}
	if e.chain == nil {
		e.prewarm.Wait()
		return fmt.Errorf("sync: nil chain import executor")
	}
	return e.chain.InsertSyncBlocksWithStageHookPrewarmed(blocks, hook, e.prewarm)
}

func (a syncImportBatchRunApplier) LogDecodeBatchResult(result syncdl.BufferedBatchDecodeResult) {
	a.service.logDecodeBatchResult(result)
}

func (a syncImportBatchRunApplier) PrepareDecodedBlocks(batch syncdl.BufferedBatch) {
	if a.session == nil {
		return
	}
	for i, block := range batch.Blocks {
		encodedBytes := 0
		if i < len(batch.Buffered) {
			encodedBytes = len(batch.Buffered[i].Raw)
		}
		a.session.PrepareDecodedBlock(block, encodedBytes)
	}
}

func (a syncImportBatchRunApplier) RecordBufferWait(wait time.Duration) {
	a.service.stats.AddBufferWait(wait)
}

func (a syncImportBatchRunApplier) ExecuteImportBatch(attempt syncdl.ImportBatchExecutionAttempt) (time.Duration, error) {
	importStarted := time.Now()
	if a.prewarmedAhead && a.prewarm != nil {
		signatureLookaheadLeadNanosCounter.Inc(importStarted.Sub(a.prewarmStarted).Nanoseconds())
		if a.prewarm.Ready() {
			signatureLookaheadReadyCounter.Inc(1)
		} else {
			signatureLookaheadPendingCounter.Inc(1)
		}
	}
	var executor syncdl.ImportBatchStageHookExecutor = syncImportStageHookExecutor{
		chain:   a.service.chain,
		session: a.session,
		prewarm: a.prewarm,
	}
	result := syncdl.RunImportBatchExecutionAttemptWithStageHook(attempt, executor, time.Now)
	if a.prewarmedAhead && a.prewarm != nil {
		// Canonical insertion joins the run before returning. This records how
		// long preprocessing remained in flight after ordered import began; it
		// is an upper bound on possible validation-side waiting, not a claim that
		// the serial path blocked for the whole interval.
		finished := a.prewarmStarted.Add(a.prewarm.WallTime())
		if finished.After(importStarted) {
			signatureLookaheadOverlapNanosCounter.Inc(finished.Sub(importStarted).Nanoseconds())
		}
	}
	if a.session != nil && a.flushLatestAfterInsert {
		if flushErr := a.session.FlushLatest(); flushErr != nil {
			if result.Err != nil {
				return result.Elapsed, fmt.Errorf("%w; flush import session latest: %v", result.Err, flushErr)
			}
			return result.Elapsed, fmt.Errorf("flush import session latest: %w", flushErr)
		}
	}
	return result.Elapsed, result.Err
}

func (ss *SyncService) advanceTransactionLookupStage() {
	if ss == nil || ss.chain == nil || ss.stopping.Load() {
		return
	}
	started := time.Now()
	result, err := ss.chain.AdvanceTransactionLookupStageInterruptible(transactionLookupStageBatchBlocks, ss.stopping.Load)
	transactionLookupStageNanosCounter.Inc(time.Since(started).Nanoseconds())
	if err != nil {
		if errors.Is(err, rawdb.ErrTransactionLookupRebuildInterrupted) && ss.stopping.Load() {
			transactionLookupStageInterruptedCounter.Inc(1)
			return
		}
		// TxLookup is derived from durable canonical bodies. Keep consensus import
		// live on a transient ETL failure; the watermark remains unchanged and a
		// later drain wake retries the same range.
		syncLog.Warn("Advance transaction lookup stage failed", "err", err)
		return
	}
	if result.Advanced {
		transactionLookupStagePassesCounter.Inc(1)
		if result.Rebuilt != nil {
			transactionLookupStageBlocksCounter.Inc(int64(result.Rebuilt.BlocksScanned))
			transactionLookupStageAncientCounter.Inc(int64(result.Rebuilt.AncientBlocksScanned))
			transactionLookupStageHotCounter.Inc(int64(result.Rebuilt.HotBlocksScanned))
			transactionLookupStageTransactionsCounter.Inc(int64(result.Rebuilt.TransactionsIndexed))
		}
	}
}

func (ss *SyncService) advanceStateHistoryIndexStage() {
	ss.advanceStateHistoryIndexStageWithMinimum(stateHistoryIndexStageMinBlocks, false)
}

// scheduleDerivedSyncStages coalesces import-barrier wakes onto one background
// worker. Both stages use immutable source snapshots and serialize their own
// watermark publication, so canonical import no longer pays their ETL wall
// time or chain-write-lock hold time.
func (ss *SyncService) scheduleDerivedSyncStages() {
	if ss == nil || ss.chain == nil || ss.stopping.Load() {
		return
	}
	ss.derivedMu.Lock()
	if ss.derivedRunning {
		ss.derivedPending = true
		ss.derivedMu.Unlock()
		return
	}
	ss.derivedRunning = true
	ss.derivedWG.Add(1)
	ss.derivedMu.Unlock()

	go func() {
		defer ss.derivedWG.Done()
		for {
			ss.advanceStateHistoryIndexStage()
			ss.advanceTransactionLookupStage()

			ss.derivedMu.Lock()
			if ss.derivedPending && !ss.stopping.Load() {
				ss.derivedPending = false
				ss.derivedMu.Unlock()
				continue
			}
			ss.derivedPending = false
			ss.derivedRunning = false
			ss.derivedMu.Unlock()
			return
		}
	}()
}

func (ss *SyncService) advanceStateHistoryIndexStageWithMinimum(minBlocks uint64, requestAgain bool) {
	if ss == nil || ss.chain == nil || ss.stopping.Load() {
		return
	}
	started := time.Now()
	result, err := ss.chain.AdvanceStateHistoryIndexStageBatchedInterruptible(minBlocks, stateHistoryIndexStageBatchBlocks, ss.stopping.Load)
	stateHistoryIndexStageNanosCounter.Inc(time.Since(started).Nanoseconds())
	if err != nil {
		if errors.Is(err, rawdb.ErrStateHistoryIndexRebuildInterrupted) && ss.stopping.Load() {
			stateHistoryIndexStageInterruptedCounter.Inc(1)
			return
		}
		// The inverse index is derived from authoritative changesets. Preserve
		// canonical import on transient ETL failure; bounded archive reads scan
		// the unindexed tail until a later drain retries this watermark.
		syncLog.Warn("Advance state history index stage failed", "err", err)
		return
	}
	if result.Advanced {
		stateHistoryIndexStagePassesCounter.Inc(1)
		if result.Rebuilt != nil {
			stateHistoryIndexStageBlocksCounter.Inc(int64(result.Rebuilt.BlocksScanned))
			stateHistoryIndexStageChangesCounter.Inc(int64(result.Rebuilt.ChangesScanned))
			stateHistoryIndexStageAppliedCounter.Inc(int64(result.Rebuilt.ETL.Applied))
			stateHistoryIndexStageInputBytesCounter.Inc(int64(result.Rebuilt.ETL.InputBytes))
			stateHistoryIndexStageBatchWritesCounter.Inc(int64(result.Rebuilt.ETL.BatchWrites))
		}
		if requestAgain {
			ss.requestDrainAgain()
		}
	}
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

func (a syncImportedBatchRecordApplier) RecordImportedBatchStats(blocks int, txs int, txKinds map[string]int, elapsed time.Duration) syncdl.ImportedBatchStatsRecordResult {
	a.service.stats.AddTxKinds(txKinds)
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

type syncImportResumePhasePublishApplier struct {
	service *SyncService
}

func (a syncImportedBatchProgressApplier) WriteImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) rawdb.SyncImportProgressWriteResult {
	return a.service.writeImportedSyncProgress(deletes, rows)
}

// WriteImportedSyncProgressAndReady coalesces imported-body deletion and stage
// progress with the downstream ready state. A synchronous batch whose head is
// already published can write its next frontier; async batches instead clear
// SyncBodiesReady in the same batch because the commit worker may still be
// publishing that frontier. The local drain can safely continue without a ready
// row, and the next body ingress or restart reconstructs it.
func (a syncImportedBatchProgressApplier) WriteImportedSyncProgressAndReady(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) (rawdb.SyncImportProgressWriteResult, syncdl.StagedBodyReadyProgressRefresh, bool) {
	if a.service == nil || a.service.chain == nil || len(deletes) == 0 {
		return rawdb.SyncImportProgressWriteResult{}, syncdl.StagedBodyReadyProgressRefresh{}, false
	}
	db := a.service.chain.ChainDB()
	if db == nil {
		return rawdb.SyncImportProgressWriteResult{}, syncdl.StagedBodyReadyProgressRefresh{}, false
	}
	if _, ok := any(db).(ethdb.Batcher); !ok {
		return rawdb.SyncImportProgressWriteResult{}, syncdl.StagedBodyReadyProgressRefresh{}, false
	}
	if a.service.chain.PipelinedCommitDepth() > 0 {
		return rawdb.WriteSyncImportProgressAndReadyBatch(db, deletes, rows, nil), syncdl.StagedBodyReadyProgressRefresh{Deleted: true}, true
	}
	head := a.service.chain.CurrentBlock()
	if head == nil {
		return rawdb.SyncImportProgressWriteResult{}, syncdl.StagedBodyReadyProgressRefresh{}, false
	}
	lastDeleted := deletes[len(deletes)-1]
	if head.Number() != lastDeleted.Number || head.Hash() != lastDeleted.Hash {
		return rawdb.SyncImportProgressWriteResult{}, syncdl.StagedBodyReadyProgressRefresh{}, false
	}
	ready := syncdl.PlanStagedBodyReadyProgress(db, head.Number()+1, a.service.targetHeadNum.Load())
	readyRow, readyOK := ready.ReadyStageProgress()
	if !readyOK {
		return rawdb.SyncImportProgressWriteResult{}, syncdl.StagedBodyReadyProgressRefresh{}, false
	}
	return rawdb.WriteSyncImportProgressAndReadyBatch(db, deletes, rows, readyRow), ready, true
}

func (a syncImportedBatchProgressApplier) RefreshSyncBodiesReady() syncdl.StagedBodyReadyProgressRefresh {
	return a.service.writeSyncBodiesReadyProgress()
}

func (a syncImportResumePhasePublishApplier) ReadStageProgress(stage rawdb.StageID) (rawdb.StageProgress, bool, error) {
	if a.service == nil || a.service.chain == nil {
		return rawdb.StageProgress{}, false, fmt.Errorf("sync: cannot read resume phase progress without service or chain")
	}
	db := a.service.chain.BufferedDB()
	if db == nil {
		return rawdb.StageProgress{}, false, fmt.Errorf("sync: cannot read resume phase progress without database")
	}
	return rawdb.ReadStageProgressRow(db, stage)
}

func (a syncImportResumePhasePublishApplier) ReadCanonicalHash(number uint64) (tcommon.Hash, bool) {
	if a.service == nil || a.service.chain == nil {
		return tcommon.Hash{}, false
	}
	id, ok := a.service.chain.BlockIDByNumber(number)
	if !ok {
		return tcommon.Hash{}, false
	}
	return id.Hash, true
}

func (a syncImportResumePhasePublishApplier) WriteResumePhaseProgress(rows []rawdb.StageProgress) error {
	if a.service == nil || a.service.chain == nil {
		return fmt.Errorf("sync: cannot publish resume phase progress without service or chain")
	}
	db := a.service.chain.DB()
	if db == nil {
		return fmt.Errorf("sync: cannot publish resume phase progress without database")
	}
	return rawdb.WriteStageProgressRows(db, rows)
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
	db := ss.chain.ChainDB()
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

func (ss *SyncService) stageSyncBody(block *types.Block, raw []byte) syncdl.StagedBodyAcceptance {
	if block == nil {
		return syncdl.StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
			StageError: fmt.Errorf("sync: cannot stage fetched block without service, chain, or block"),
		}}
	}
	return ss.stageSyncBodyID(block.ID(), block, raw)
}

func (ss *SyncService) stageSyncBodyID(id types.BlockID, block *types.Block, raw []byte) syncdl.StagedBodyAcceptance {
	if ss == nil || ss.chain == nil || (block == nil && len(raw) == 0) {
		return syncdl.StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
			StageError: fmt.Errorf("sync: cannot stage fetched block without service, chain, or body"),
		}}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
			StageError: fmt.Errorf("sync: cannot stage fetched block without database"),
		}}
	}
	head := ss.chain.CurrentBlock()
	if head == nil {
		return syncdl.StagedBodyAcceptance{Write: rawdb.SyncStagedBlockWriteResult{
			StageError: fmt.Errorf("sync: cannot stage fetched block without current head"),
		}}
	}
	var result syncdl.StagedBodyAcceptance
	if block != nil {
		result = syncdl.AcceptStagedBody(db, block, raw, head.Number()+1, ss.targetHeadNum.Load())
	} else {
		result = syncdl.AcceptStagedBodyRaw(db, id, raw, head.Number()+1, ss.targetHeadNum.Load())
	}
	if result.Write.StageError != nil {
		syncLog.Warn("Persist sync staged block failed", "number", id.Num, "hash", id.Hash, "err", result.Write.StageError)
		return result
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
	return result
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
	refresh := syncdl.RefreshStagedBodyReadyProgress(db, head.Number()+1, ss.targetHeadNum.Load())
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

func (ss *SyncService) deleteImportedSyncBodiesThrough(head uint64) syncdl.ImportedStagedBodyCleanup {
	if ss == nil || ss.chain == nil {
		return syncdl.ImportedStagedBodyCleanup{}
	}
	db := ss.chain.DB()
	if db == nil {
		return syncdl.ImportedStagedBodyCleanup{}
	}
	current := ss.chain.CurrentBlock()
	if current == nil {
		return syncdl.ImportedStagedBodyCleanup{}
	}
	result := syncdl.DeleteImportedStagedBodiesThrough(db, head, current.Number()+1, ss.targetHeadNum.Load())
	if result.DeleteError != nil {
		syncLog.Warn("Delete imported sync staged blocks failed", "head", head, "err", result.DeleteError)
		return result
	}
	ss.logSyncBodiesReadyRefresh(result.Ready)
	return result
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
	result := syncdl.PruneStaleStagedBodyTail(db, blockNum, lastRestoredNum, lastRestoredHash, haveLastRestored, head.Number()+1, ss.targetHeadNum.Load())
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

func (ss *SyncService) pauseAtStopHeight(height uint64) {
	err := fmt.Errorf("%w: %d", ErrSyncStopHeightReached, height)
	ss.pause.Enter(height, err)
	ss.mu.Lock()
	ss.doReset()
	ss.mu.Unlock()
	syncLog.Info("Sync stopped at configured height", "height", height)
}

func (ss *SyncService) estimatedRemainLocked() int64 {
	return ss.sessionProgressLocked().EstimatedRemaining()
}

func (ss *SyncService) sessionProgressLocked() syncdl.SessionProgress {
	progress := syncdl.SessionProgress{
		Syncing:        ss.syncing,
		Paused:         ss.pause.Paused(),
		TargetHead:     ss.targetHeadNum.Load(),
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

// reportSegment emits the throttled "Sync import progress" summary. Called
// without ss.mu held.
func (ss *SyncService) reportSegment(s tsync.Snapshot, diag syncdl.Diagnostics, head uint64, remain int64, peer *p2p.Peer) {
	now := time.Now()
	elapsed := now.Sub(s.StartTime)
	if elapsed <= 0 {
		elapsed = 1
	}
	obs := newSyncImportWindowObservation(s, elapsed)
	energyPerSec := formatCompactEnergy(obs.EnergyPerSec)
	updateSyncImportWindowMetrics(now, elapsed, s, obs, diag)
	ss.stats.RecordSpeed(now, s.Blocks, elapsed)
	ctx := []any{
		"window", ethcommon.PrettyDuration(elapsed),
		"head", head,
		"blocks", s.Blocks,
		"txs", s.Txs,
		"blocksPerSec", round2(obs.BlocksPerSec),
		"txsPerSec", round2(obs.TxsPerSec),
		"txsPerBlock", round2(obs.TxsPerBlock),
		"energyPerSec", energyPerSec,
		"energyPerBlock", formatCompactEnergy(obs.EnergyPerBlock),
		"energyPerTx", formatCompactEnergy(obs.EnergyPerTx),
		"vmTxsPerBlock", round2(obs.VMTransactionsPerBlock),
		"nativeTxsPerBlock", round2(obs.NativeTransactionsPerBlock),
		"vmTxSharePct", round2(obs.VMTransactionShare * 100),
		"rawEnergyPerSec", formatCompactEnergy(obs.RawEnergyPerSec),
		"rawEnergyPerVMTx", formatCompactEnergy(obs.RawEnergyPerVMTransaction),
		"billedToRawEnergyRatio", round2(obs.BilledToRawEnergyRatio),
		"vmMsPerVMTx", round2(obs.VMExecutionMillisPerVMTransaction),
		"vmNsPerRawEnergy", round2(obs.VMExecutionNanosPerRawEnergy),
		"execBusyPct", round2(obs.ExecBusyRatio * 100),
		"bufferWaitPct", round2(obs.BufferWaitRatio * 100),
		"applySamples", s.ApplyBlocks,
		"applyCoveragePct", round2(obs.ApplyCoverageRatio * 100),
		"importMsPerBlock", round2(obs.ImportMillisPerBlock),
		"applyMsPerBlock", round2(obs.ApplyMillisPerBlock),
		"importOverheadMsPerBlock", round2(obs.ImportOverheadMillisPerBlock),
		"outsideTxMsPerBlock", round2(obs.OutsideTxMillisPerBlock),
		"executeFixedMsPerBlock", round2(obs.ExecuteFixedMillisPerBlock),
		"transactionMsPerTx", round2(obs.TransactionMillisPerTx),
		"rewardsMsPerBlock", round2(obs.RewardsMillisPerBlock),
		"blockStatsMsPerBlock", round2(obs.BlockStatsMillisPerBlock),
		"stateCommitMsPerBlock", round2(obs.StateCommitMillisPerBlock),
		"commitmentUpdatesPerBlock", round2(obs.CommitmentUpdatesPerBlock),
		"stateCommitNsPerUpdate", round2(obs.StateCommitNanosPerCommitmentUpdate),
		"persistMsPerBlock", round2(obs.PersistMillisPerBlock),
		"persistMetadataBytesPerBlock", round2(obs.PersistMetadataBytesPerBlock),
		"persistMetadataBytesPerTx", round2(obs.PersistMetadataBytesPerTx),
		"remaining", remain,
		"peers", diag.PeerCount,
		"activePeers", diag.ActivePeerCount,
		"inflight", diag.Inflight,
		"buffered", diag.BlockBufferLen,
		"requested", diag.RequestedLen,
		"retries", diag.RetryListLen,
	}
	syncLog.Info("Sync import progress", ctx...)

	// Detailed execution diagnostics are intentionally opt-in. Besides keeping
	// the normal operator log compact, this guard avoids building a large field
	// slice on every reporting interval when net/sync debug logging is disabled.
	if !syncLog.DebugEnabled() {
		return
	}
	txTop := tsync.TopTxKindsString(s.TxKinds, 5)
	if txTop == "" {
		txTop = "none"
	}
	stateMutTop := s.ApplyStats.StateCommitDetail.Mutations.TopKindsString(3)
	if stateMutTop == "" {
		stateMutTop = "none"
	}
	stateMutKVTop := s.ApplyStats.StateCommitDetail.Mutations.TopKVDomainsString(3)
	if stateMutKVTop == "" {
		stateMutKVTop = "none"
	}
	detail := []any{
		"blocks", s.Blocks,
		"txs", s.Txs,
		"head", head,
		"elapsed", ethcommon.PrettyDuration(elapsed),
		"execElapsed", ethcommon.PrettyDuration(s.ExecElapsed),
		"applyElapsed", ethcommon.PrettyDuration(s.ApplyStats.Total()),
		"bufferWaitElapsed", ethcommon.PrettyDuration(s.BufferWaitElapsed),
		"validate", ethcommon.PrettyDuration(s.ApplyStats.Validate),
		"execute", ethcommon.PrettyDuration(s.ApplyStats.Execute),
		"transactionExecute", ethcommon.PrettyDuration(s.ApplyStats.TransactionExecute),
		"accountStateRoot", ethcommon.PrettyDuration(s.ApplyStats.AccountStateRoot),
		"adaptiveEnergy", ethcommon.PrettyDuration(s.ApplyStats.AdaptiveEnergy),
		"rewards", ethcommon.PrettyDuration(s.ApplyStats.Rewards),
		"shieldedFinalize", ethcommon.PrettyDuration(s.ApplyStats.ShieldedFinalize),
		"witnessFlush", ethcommon.PrettyDuration(s.ApplyStats.WitnessFlush),
		"blockStatistics", ethcommon.PrettyDuration(s.ApplyStats.BlockStatistics),
		"energy", formatCompactEnergy(float64(s.ApplyStats.EnergyUsageTotal)),
		"energyPerSec", energyPerSec,
		"vmTransactions", s.ApplyStats.VMTransactions,
		"nativeTransactions", s.ApplyStats.NativeTransactions,
		"vmExecution", ethcommon.PrettyDuration(s.ApplyStats.VMExecution),
		"vmRawEnergy", formatCompactEnergy(float64(s.ApplyStats.VMRawEnergyUsage)),
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
		"stateCommitmentUpdates", s.ApplyStats.StateCommitDetail.CommitmentUpdates,
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
		"dpUpdate", ethcommon.PrettyDuration(s.ApplyStats.DPUpdate),
		"persist", ethcommon.PrettyDuration(s.ApplyStats.Persist),
		"persistMetadataRecords", s.ApplyStats.PersistDetail.MetadataRecords,
		"persistMetadataBytes", s.ApplyStats.PersistDetail.MetadataBytes,
		"persistTransactionLookupRows", s.ApplyStats.PersistDetail.TransactionLookupRows,
		"persistTraceAccounts", s.ApplyStats.PersistDetail.TraceAccounts,
		"hooks", ethcommon.PrettyDuration(s.ApplyStats.Hooks),
		"blockBuffer", diag.BlockBufferLen,
		"requested", diag.RequestedLen,
		"retryList", diag.RetryListLen,
	}
	if phase, phaseElapsed := slowestApplyPhase(s.ApplyStats); phase != "" {
		detail = append(detail, "slowPhase", phase, "slowElapsed", ethcommon.PrettyDuration(phaseElapsed))
	}
	if phase, phaseElapsed := slowestStateCommitPhase(s.ApplyStats); phase != "" {
		detail = append(detail, "slowStateCommitPhase", phase, "slowStateCommitElapsed", ethcommon.PrettyDuration(phaseElapsed))
	}
	topMutations := s.ApplyStats.StateCommitDetail.Mutations.TopKindsString(10)
	if topMutations == "" {
		topMutations = "none"
	}
	topKVDomains := s.ApplyStats.StateCommitDetail.Mutations.TopKVDomainsString(10)
	if topKVDomains == "" {
		topKVDomains = "none"
	}
	detail = append(detail, "stateMutTop", topMutations, "stateMutKVTop", topKVDomains, "txTop", txTop)
	detail = diag.AppendImportPlanLogFields(detail)
	if peer != nil {
		detail = append(detail, "peer", peer.ID())
	}
	if diag.PeerState != "" {
		detail = append(detail, "peerState", diag.PeerState)
	}
	syncLog.Debug("Sync import diagnostics", detail...)
}

func syncEnergyPerSec(total int64, elapsed time.Duration) float64 {
	if total <= 0 || elapsed <= 0 {
		return 0
	}
	rate := float64(total) * float64(time.Second) / float64(elapsed)
	return round2(rate)
}

func formatSyncEnergyPerSec(total int64, elapsed time.Duration) string {
	return formatCompactEnergy(syncEnergyPerSec(total, elapsed))
}

func round2(f float64) float64 {
	// Trim to 2 decimals for log readability without depending on a printf
	// format directive (slog handlers print floats with full precision).
	return float64(int64(f*100+0.5)) / 100
}

func formatCompactEnergy(value float64) string {
	abs := value
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", value/1_000_000_000)
	case abs >= 1_000_000:
		return fmt.Sprintf("%.2fM", value/1_000_000)
	case abs >= 1_000:
		return fmt.Sprintf("%.2fk", value/1_000)
	default:
		return fmt.Sprintf("%g", round2(value))
	}
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
	syncdl.ApplySessionResetPlan(syncdl.PlanSessionReset(), syncSessionResetApplier{service: ss})
}

type syncSessionResetApplier struct {
	service *SyncService
}

func (a syncSessionResetApplier) StopPeerTimers() {
	for _, ps := range a.service.peers {
		if ps.fetchTimer != nil {
			ps.fetchTimer.Stop()
			ps.fetchTimer = nil
		}
		if ps.fetchDelayTimer != nil {
			ps.fetchDelayTimer.Stop()
			ps.fetchDelayTimer = nil
		}
	}
}

func (a syncSessionResetApplier) DeactivateSession() {
	if a.service.stats != nil {
		a.service.stats.EndSession(time.Now())
	}
	a.service.syncing = false
}

func (a syncSessionResetApplier) ClearLegacyFetchState() {
	a.service.syncPeer = nil
	a.service.fetchList = nil
	a.service.remainNum = 0
	a.service.inflight = 0
	a.service.pending = nil
}

func (a syncSessionResetApplier) AdvanceFetchSequence() {
	a.service.fetchSeq++
}

func (a syncSessionResetApplier) StopLegacyFetchTimer() {
	if a.service.fetchTimer != nil {
		a.service.fetchTimer.Stop()
		a.service.fetchTimer = nil
	}
}

func (a syncSessionResetApplier) ClearPeerState() {
	a.service.peers = nil
	a.service.requested = nil
	a.service.retryList = nil
}

func (a syncSessionResetApplier) ClearBlockTracking() {
	a.service.blockBuffer = nil
	a.service.bufferedHash = nil
	a.service.blockPath = nil
	a.service.bufferedBytes = 0
	a.service.fetchBackpressured = false
	a.service.syncedTipNum = 0
}

func (a syncSessionResetApplier) ClearTarget() {
	a.service.targetHeadNum.Store(0)
}

func (a syncSessionResetApplier) ResetBufferWait() {
	a.service.bufferWait.Reset()
}

func (a syncSessionResetApplier) DeleteStagedBodies() {
	a.service.deleteAllSyncBodies()
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
	// Drain a final sub-batch suffix before changing over to ordinary gossip
	// import. Inline stage progress refuses to jump a deferred gap.
	ss.derivedWG.Wait()
	ss.advanceStateHistoryIndexStageWithMinimum(1, false)
	ss.advanceTransactionLookupStage()
	totalBlocks := ss.stats.TotalBlocks()
	totalStart := ss.stats.TotalStart()
	if totalStart.IsZero() {
		totalStart = time.Now()
	}
	ss.mu.Lock()
	ss.doReset()
	ss.mu.Unlock()
	ss.notifySyncComplete()

	totalElapsed := time.Since(totalStart)
	ctx := []any{
		"head", ss.chain.CurrentBlock().Number(),
		"totalBlocks", totalBlocks,
		"totalElapsed", ethcommon.PrettyDuration(totalElapsed),
	}
	if totalElapsed > 0 && totalBlocks > 0 {
		rate := float64(totalBlocks) * float64(time.Second) / float64(totalElapsed)
		ctx = append(ctx, "avgBlocksPerSec", round2(rate))
	}
	syncLog.Info("Sync complete", ctx...)
}
