package net

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
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
	maxChainInventorySize     = tsync.MaxChainInventorySize
	maxFetchBatch             = tsync.MaxFetchBatch
	maxParallelSyncPeers      = tsync.MaxParallelSyncPeers
	minFetchRequestInterval   = tsync.MinFetchRequestInterval
	maxBufferedRunaheadBlocks = tsync.MaxBufferedRunaheadBlocks
	maxBufferedRunaheadBytes  = tsync.MaxBufferedRunaheadBytes
	alwaysFetchRunaheadBlocks = tsync.AlwaysFetchRunaheadBlocks
	peerJoinAttemptInterval   = 2 * time.Second

	// Retain up to one complete near-tip fetch batch already decoded by the peer
	// receive path so the drain does not immediately protobuf-decode it again.
	// Both caps are global to the SyncService and include the active drain batch.
	// The 64 MiB raw-byte charge bounds the corresponding pointer-rich protobuf
	// graph to a few hundred MiB even at the ~5x expansion observed in production;
	// all farther runahead remains raw-only, avoiding the former unbounded 12 GiB
	// / 161 M-object GC spiral while using the memory now available to remove the
	// second decode from the contiguous execution path.
	maxRetainedDecodedBlocks = maxFetchBatch
	maxRetainedDecodedBytes  = 64 << 20
)

type syncDiagnostics struct {
	blockBufferLen       int
	requestedLen         int
	retryListLen         int
	retainedDecoded      int
	retainedDecodedBytes int64
	peerState            string
}

type syncPeerState struct {
	peer *p2p.Peer

	fetchList []types.BlockID
	remainNum int64

	inflight   int
	pending    map[tcommon.Hash]uint64
	pendingIDs map[tcommon.Hash]types.BlockID

	// requestedHashes mirrors java-tron's syncBlockIdCache rule: never ask the
	// same peer for the same block hash twice, even after a timeout. Values
	// carry the block number so entries below minFetchNum can be pruned —
	// java rejects fetches under that floor (FetchInvDataMsgHandler's
	// minBlockNum check) before its duplicate check, and its dup cache holds
	// at most 2×SYNC_FETCH_BATCH_NUM entries, so a pruned hash can never be
	// legally re-fetched from this peer. Without the prune the map grows by
	// one entry per block for the whole session (1.81 GB live observed on
	// the Nile node mid re-sync).
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

// bufferedSyncBlock holds an out-of-order sync block awaiting contiguous
// drain. Raw wire bytes remain the authoritative, compact representation. A
// strictly bounded near-tip subset may also retain the block already decoded
// by the peer receive path, avoiding an immediate second protobuf decode. The
// bounds are essential: retaining every decoded block once ballooned the GC
// mark set to ≈12 GB / 161 M live objects on Nile (~70% CPU in GC).
type bufferedSyncBlock struct {
	raw     []byte
	decoded *types.Block
	hash    tcommon.Hash
	num     uint64
	peer    *p2p.Peer
}

// bufferRawBlockBytes takes ownership of the block's wire bytes for the sync
// buffer. The p2p codec allocates a fresh frame/unwrap payload per message and
// invokes the handler synchronously, so a consumed sync block can transfer that
// slice without another full-block copy. Callers must not mutate raw after
// HandleBlock consumes it. Tests/non-wire paths may pass nil to marshal a copy.
func bufferRawBlockBytes(block *types.Block, raw []byte) []byte {
	if len(raw) == 0 {
		b, _ := block.Marshal()
		return b
	}
	return raw
}

type bufferedSyncBatch struct {
	blocks      []*types.Block
	buffered    []bufferedSyncBlock
	bufferWaits []time.Duration
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

	// stopAtHeight is an operator-supplied audit boundary. When configured,
	// the downloader never requests or imports a block above the boundary and
	// engages the same sticky pause gate once the boundary is committed. The
	// two atomics allow the setting to be installed before Start (the normal
	// CLI path) or while a sync is active without adding another lock order.
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

	peers        map[string]*syncPeerState
	requested    map[tcommon.Hash]string
	retryList    []types.BlockID
	blockBuffer  map[uint64]bufferedSyncBlock
	bufferedHash map[tcommon.Hash]struct{}
	blockPath    map[uint64]tcommon.Hash
	// bufferedBytes tracks the raw wire bytes currently held in blockBuffer.
	// It gates far-ahead fetching against MaxBufferedRunaheadBytes so the
	// buffer's heap footprint stays bounded at full-block eras.
	bufferedBytes int64
	// retainedDecoded* accounts for decoded block pointers in blockBuffer plus
	// the single active drain batch. It is intentionally not reset blindly when
	// a sync session ends: an import already running off-lock may outlive reset,
	// and its pointers remain charged until releaseDecodedBatch runs.
	retainedDecodedBlocks int
	retainedDecodedBytes  int64
	targetHeadNum         uint64
	// syncedTipNum is the drain cursor: the highest block this session has
	// popped for import. Under async-commit depth>2 the committed CurrentBlock
	// lags the applied tip by up to the pipeline depth, so popping from
	// CurrentBlock+1 would re-target an already-imported (and deleted) buffer
	// entry and break the drain after every batch. Tracking the cursor lets the
	// drain pop the whole buffered run in one pass. Equals CurrentBlock when
	// async commit is off (the production default), so that path is unchanged.
	syncedTipNum uint64
	// bufferPrunedTipNum is the highest effective sync tip through which stale
	// blockBuffer/blockPath entries have been defensively removed. HandleBlock
	// never admits entries at or behind the effective tip, so each height range
	// needs scanning at most once even while CurrentBlock lags async execution.
	bufferPrunedTipNum uint64

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

	// watchdog runs the periodic isolation check. Owns its own goroutine
	// and ticker; Start/Stop fan-out launches and joins it.
	watchdog *tsync.Watchdog

	bufferWaitStart time.Time
	bufferWaitNum   uint64

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
		chain:        chain,
		handler:      handler,
		pause:        tsync.NewPauseGate(),
		stats:        tsync.NewStats(),
		fetchTimeout: tsync.SyncFetchTimeout,
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
	ss.pauseIfStopHeightReached()
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
	syncLog.Warn("Re-kicking stalled sync fetch", "head", ss.chain.CurrentBlock().Number())
	ss.drainBufferedBlocks()
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

// SyncStatus is a point-in-time downloader snapshot for operational APIs. It
// exposes counts rather than internal maps/slices so callers cannot mutate the
// sync state and collecting it remains bounded regardless of backlog size.
type SyncStatus struct {
	Active                bool
	Paused                bool
	SyncPeerCount         int
	TargetHead            uint64
	AppliedTip            uint64
	Remaining             int64
	Inflight              int
	BufferedBlocks        int
	BufferedBytes         int64
	RequestedBlocks       int
	RetryBlocks           int
	RetainedDecodedBlocks int
	RetainedDecodedBytes  int64
	PauseBlock            uint64
	PauseTime             time.Time
	PauseError            error
}

// Status returns one lock-consistent downloader snapshot. The lock order is
// the established ss.mu → pause.mu order used by the sync state machine.
func (ss *SyncService) Status() SyncStatus {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	paused, pauseBlock, pauseTime, pauseErr := ss.pause.Status()
	return SyncStatus{
		Active:                ss.syncing,
		Paused:                paused,
		SyncPeerCount:         len(ss.peers),
		TargetHead:            ss.targetHeadNum,
		AppliedTip:            ss.syncedTipNum,
		Remaining:             ss.estimatedRemainLocked(),
		Inflight:              ss.inflight,
		BufferedBlocks:        len(ss.blockBuffer),
		BufferedBytes:         ss.bufferedBytes,
		RequestedBlocks:       len(ss.requested),
		RetryBlocks:           len(ss.retryList),
		RetainedDecodedBlocks: ss.retainedDecodedBlocks,
		RetainedDecodedBytes:  ss.retainedDecodedBytes,
		PauseBlock:            pauseBlock,
		PauseTime:             pauseTime,
		PauseError:            pauseErr,
	}
}

// ErrSyncStopHeightReached is recorded in PausedStatus when an operator
// configured audit boundary has been reached. It is intentionally distinct
// from an InsertBlock error so status consumers can tell a planned pause from
// a consensus/execution failure with errors.Is.
var ErrSyncStopHeightReached = errors.New("configured sync stop height reached")

// SetStopAtHeight configures an inclusive sync boundary. Block height is
// imported, blocks above height are never requested, and sync then remains
// paused until process restart. Reusing the sticky pause gate also prevents
// broadcast blocks from advancing the chain while an operator inspects it.
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
	if !configured || ss.chain.CurrentBlock().Number() < height {
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
	if peer == nil {
		return
	}
	if ss.stopping.Load() {
		return
	}
	if ss.pauseIfStopHeightReached() {
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
	ss.releaseBufferedDecodedLocked()
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
	ss.blockBuffer = make(map[uint64]bufferedSyncBlock)
	ss.bufferedHash = make(map[tcommon.Hash]struct{})
	ss.blockPath = make(map[uint64]tcommon.Hash)
	ss.bufferedBytes = 0
	ss.targetHeadNum = ss.chain.CurrentBlock().Number()
	ss.syncedTipNum = ss.targetHeadNum
	ss.bufferPrunedTipNum = ss.targetHeadNum
	ss.stats.InitSession(now)
	ss.bufferWaitStart = time.Time{}
	ss.bufferWaitNum = 0
	ss.lastPeerJoinAttempt = time.Time{}
}

func (ss *SyncService) ensureSessionMapsLocked() {
	if ss.peers == nil {
		ss.peers = make(map[string]*syncPeerState)
	}
	if ss.requested == nil {
		ss.requested = make(map[tcommon.Hash]string)
	}
	if ss.blockBuffer == nil {
		ss.blockBuffer = make(map[uint64]bufferedSyncBlock)
	}
	if ss.bufferedHash == nil {
		ss.bufferedHash = make(map[tcommon.Hash]struct{})
	}
	if ss.blockPath == nil {
		ss.blockPath = make(map[uint64]tcommon.Hash)
	}
}

// effectiveSyncTipLocked returns the highest height already owned by this sync
// session. CurrentBlock is the durable/published tip; syncedTipNum may be ahead
// while a deep InsertSession has popped blocks for import but its async commit
// worker has not published them yet. Admission, request de-duplication and
// runahead budgeting must use the maximum or stale responses can be copied back
// into blockBuffer behind the drain cursor and remain there forever.
func (ss *SyncService) effectiveSyncTipLocked() uint64 {
	tip := ss.chain.CurrentBlock().Number()
	if ss.syncedTipNum > tip {
		tip = ss.syncedTipNum
	}
	return tip
}

// detachBufferedBlockLocked removes one raw entry and its indexes. When the
// block moves into the active drain batch, keepDecoded remains true so its
// decoded pointer stays charged against the global retention caps until the
// insert finishes. Stale/discard paths pass false and release it immediately.
func (ss *SyncService) detachBufferedBlockLocked(num uint64, keepDecoded bool) (bufferedSyncBlock, bool) {
	buffered, ok := ss.blockBuffer[num]
	if !ok {
		return bufferedSyncBlock{}, false
	}
	delete(ss.blockBuffer, num)
	delete(ss.bufferedHash, buffered.hash)
	delete(ss.blockPath, num)
	if n := int64(len(buffered.raw)); n >= ss.bufferedBytes {
		ss.bufferedBytes = 0
	} else {
		ss.bufferedBytes -= n
	}
	if !keepDecoded {
		ss.releaseRetainedDecodedLocked(&buffered)
	}
	return buffered, true
}

func (ss *SyncService) removeBufferedBlockLocked(num uint64) (bufferedSyncBlock, bool) {
	return ss.detachBufferedBlockLocked(num, false)
}

func (ss *SyncService) popBufferedBlockLocked(num uint64) (bufferedSyncBlock, bool) {
	return ss.detachBufferedBlockLocked(num, true)
}

func (ss *SyncService) retainDecodedBlockLocked(block *types.Block, blockNum, effectiveTip uint64, rawBytes int) bool {
	if block == nil || blockNum > effectiveTip+alwaysFetchRunaheadBlocks {
		return false
	}
	n := int64(rawBytes)
	if ss.retainedDecodedBlocks >= maxRetainedDecodedBlocks ||
		n > maxRetainedDecodedBytes-ss.retainedDecodedBytes {
		return false
	}
	ss.retainedDecodedBlocks++
	ss.retainedDecodedBytes += n
	return true
}

func (ss *SyncService) releaseRetainedDecodedLocked(buffered *bufferedSyncBlock) {
	if buffered == nil || buffered.decoded == nil {
		return
	}
	buffered.decoded = nil
	if ss.retainedDecodedBlocks > 0 {
		ss.retainedDecodedBlocks--
	}
	n := int64(len(buffered.raw))
	if n >= ss.retainedDecodedBytes {
		ss.retainedDecodedBytes = 0
	} else {
		ss.retainedDecodedBytes -= n
	}
}

func (ss *SyncService) releaseBufferedDecodedLocked() {
	for num, buffered := range ss.blockBuffer {
		ss.releaseRetainedDecodedLocked(&buffered)
		ss.blockBuffer[num] = buffered
	}
}

// pruneStaleSyncStateLocked drops buffer entries and path reservations at or
// behind tip. Such entries cannot be reached by the contiguous drain, which
// starts at effectiveSyncTipLocked()+1. The monotonic watermark avoids a full
// buffer scan on every received block; HandleBlock's admission gate guarantees
// no entry can later appear below an already-pruned tip.
func (ss *SyncService) pruneStaleSyncStateLocked(tip uint64) {
	if tip <= ss.bufferPrunedTipNum {
		return
	}
	for num := range ss.blockBuffer {
		if num <= tip {
			ss.removeBufferedBlockLocked(num)
		}
	}
	for num := range ss.blockPath {
		if num <= tip {
			delete(ss.blockPath, num)
		}
	}
	ss.bufferPrunedTipNum = tip
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
	need := maxParallelSyncPeers - len(ss.peers)
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
	if ss.handler == nil || !ss.syncing || ss.pause.Paused() || len(ss.peers) >= maxParallelSyncPeers {
		return false
	}
	if !ss.lastPeerJoinAttempt.IsZero() && now.Sub(ss.lastPeerJoinAttempt) < peerJoinAttemptInterval {
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
	peerSummary := make([]types.BlockID, 0, len(inv.Ids))
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
	responseCap := headNum - commonNum
	if responseCap > maxChainInventorySize {
		responseCap = maxChainInventorySize
	}
	responseIDs := make([]*corepb.ChainInventory_BlockId, 0, int(responseCap))
	count := 0
	for num := commonNum + 1; num <= headNum && count < maxChainInventorySize; num++ {
		bid, ok := ss.chain.BlockIDByNumber(num)
		if !ok {
			break
		}
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
	committedHeadNum := ss.chain.CurrentBlock().Number()
	effectiveTipNum := ss.effectiveSyncTipLocked()
	ss.pruneStaleSyncStateLocked(effectiveTipNum)
	stopHeight, stopConfigured := ss.configuredStopHeight()
	for _, bid := range inv.Ids {
		num := uint64(bid.Number)
		if stopConfigured && num > stopHeight {
			continue
		}
		hash := tcommon.BytesToHash(bid.Hash)
		// Preserve the existing committed-chain fork check, but never requeue a
		// height already handed to the active async insert session. It is not
		// necessarily visible through CurrentBlock/GetBlockByNumber yet.
		if num > committedHeadNum && num <= effectiveTipNum {
			delete(ss.blockPath, num)
			continue
		}
		if num <= committedHeadNum {
			if existing, ok := ss.chain.BlockIDByNumber(num); ok && existing.Hash == hash {
				continue
			}
		}
		if ss.chain.HasBlockInKhaosDB(hash) {
			continue
		}
		if _, ok := ss.bufferedHash[hash]; ok {
			continue
		}
		if _, ok := ss.requested[hash]; ok {
			continue
		}
		if _, ok := ps.requestedHashes[hash]; ok {
			continue
		}
		bid := types.BlockID{Hash: hash, Num: num}
		if !ss.reserveBlockPathLocked(bid) {
			continue
		}
		ps.fetchList = append(ps.fetchList, bid)
	}
	ps.remainNum = inv.RemainNum
	if len(inv.Ids) > 0 {
		last := inv.Ids[len(inv.Ids)-1]
		if last.Number > 0 {
			ps.lastInventoryNum = uint64(last.Number)
			if ps.lastInventoryNum > 2*maxChainInventorySize {
				ps.minFetchNum = ps.lastInventoryNum - 2*maxChainInventorySize
			} else {
				ps.minFetchNum = 0
			}
			// Prune the never-re-ask set below the window floor. java-tron
			// rejects any sync fetch under minBlockNum before it even
			// consults its per-peer duplicate cache (which itself holds at
			// most 2×SYNC_FETCH_BATCH_NUM entries), and canFetch never
			// assigns bids under minFetchNum — so entries below the floor
			// can never be re-fetched from this peer and remembering them
			// is pure growth: one entry per synced block for the whole
			// session (1.81 GB live on the Nile node). Keeping
			// [minFetchNum, lastInventoryNum] retains a superset of what
			// the remote's dup cache can still enforce.
			if ps.minFetchNum > 0 {
				for h, num := range ps.requestedHashes {
					if num < ps.minFetchNum {
						delete(ps.requestedHashes, h)
					}
				}
			}
			target := uint64(last.Number)
			if inv.RemainNum > 0 {
				target += uint64(inv.RemainNum)
			}
			if stopConfigured && target > stopHeight {
				target = stopHeight
			}
			if target > ss.targetHeadNum {
				ss.targetHeadNum = target
			}
		}
	}

	// java-tron sets `needSyncFromUs = false` on its peer record only when
	// our summary's last block matches its head (lostBlockIds.size == 1).
	// While needSyncFromUs is true, java-tron's InventoryMsgHandler drops
	// every inbound INV — so our outbound TRX advertisements never reach
	// the producer's mempool. Detect "we are at head" here (response is a
	// single id we already have) and finish; otherwise continue fetching.
	if len(inv.Ids) == 0 || (len(ps.fetchList) == 0 && len(inv.Ids) == 1 && inv.RemainNum == 0) {
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
	committedHeadNum := ss.chain.CurrentBlock().Number()
	effectiveTipNum := ss.effectiveSyncTipLocked()
	ss.pruneStaleSyncStateLocked(effectiveTipNum)
	for _, ps := range ss.peers {
		if ps == nil || ps.peer == nil || ps.done || ps.chainRequested || ps.inflight > 0 {
			continue
		}
		ss.assignRetryLocked(ps, effectiveTipNum)
		batch := ss.nextFetchBatchLocked(ps, effectiveTipNum)
		if len(batch) == 0 {
			if !ps.done {
				if ps.lastInventoryNum > committedHeadNum {
					// java-tron rejects a follow-up SYNC_BLOCK_CHAIN if the
					// summary tail is below the last inventory tip it sent us
					// on this peer (lastSyncNum > lastNum). Wait until the
					// canonical head catches up before asking this peer for
					// the next 2000-block window.
					//
					// Re-arm a short delay so this peer re-evaluates once the
					// head advances. Under async-commit depth>2 the head is
					// published by the commit worker after the foreground
					// applies, so an otherwise-idle scheduler would wait for the
					// coarse watchdog poll (lost wakeup) — the fetch/HandleBlock
					// paths only re-check while a block is in flight.
					if ps.fetchDelayTimer == nil {
						ss.armPeerDelayTimerLocked(ps, minFetchRequestInterval)
					}
					syncLog.Trace("Sync peer waiting for local head",
						"peer", ps.peer.ID(),
						"head", committedHeadNum,
						"effectiveTip", effectiveTipNum,
						"inventoryTip", ps.lastInventoryNum)
					continue
				}
				// Always re-poll once a peer's local queue drains. java-tron may
				// have produced new blocks while we were applying the previous
				// batch; the one-id inventory response is what marks sync done.
				ps.chainRequested = true
				out = append(out, outboundSyncRequest{peer: ps.peer, chain: true})
			}
			continue
		}
		if wait := time.Until(ps.nextFetchAt); wait > 0 {
			ps.fetchList = append(batch, ps.fetchList...)
			ss.armPeerDelayTimerLocked(ps, wait)
			continue
		}
		ps.inflight = len(batch)
		ps.pending = make(map[tcommon.Hash]uint64, len(batch))
		ps.pendingIDs = make(map[tcommon.Hash]types.BlockID, len(batch))
		for _, bid := range batch {
			ps.pending[bid.Hash] = bid.Num
			ps.pendingIDs[bid.Hash] = bid
			ps.requestedHashes[bid.Hash] = bid.Num
			ss.requested[bid.Hash] = ps.peer.ID()
		}
		ps.nextFetchAt = now.Add(minFetchRequestInterval)
		ss.armPeerFetchTimerLocked(ps)
		out = append(out, outboundSyncRequest{peer: ps.peer, blocks: batch})
	}
	return out
}

// withinRunaheadBudgetLocked reports whether bid may be requested now. Two
// independent budgets bound the fetch runahead past the effective sync tip:
//
//   - MaxBufferedRunaheadBlocks caps the number span outright;
//   - once the raw sync buffer holds MaxBufferedRunaheadBytes, only the
//     near-head AlwaysFetchRunaheadBlocks strip stays fetchable, so the
//     contiguous drain keeps getting the blocks right ahead of the head
//     while far-ahead fetching pauses.
//
// Over-budget bids stay queued (fetchList / retryList) and become fetchable
// again as the head advances or the buffer drains — backpressure, never a
// drop, so nothing is re-downloaded. This is the local complement of the
// remote window java-tron already enforces on us (FetchInvDataMsgHandler
// rejects fetches outside lastSyncBlockId − 2×SYNC_FETCH_BATCH_NUM ..
// lastSyncBlockId); without it the buffer's heap footprint is unbounded
// (a ~2.8M-block, 2.5 GB runahead was observed live on the Nile node).
func (ss *SyncService) withinRunaheadBudgetLocked(bid types.BlockID, effectiveTipNum uint64) bool {
	if bid.Num > effectiveTipNum+maxBufferedRunaheadBlocks {
		return false
	}
	if ss.bufferedBytes >= maxBufferedRunaheadBytes && bid.Num > effectiveTipNum+alwaysFetchRunaheadBlocks {
		return false
	}
	return true
}

func (ss *SyncService) assignRetryLocked(ps *syncPeerState, effectiveTipNum uint64) {
	if len(ss.retryList) == 0 {
		return
	}
	keep := ss.retryList[:0]
	for _, bid := range ss.retryList {
		if ss.hasBlockOrRequestLocked(bid) {
			continue
		}
		if !ss.withinRunaheadBudgetLocked(bid, effectiveTipNum) {
			keep = append(keep, bid)
			continue
		}
		if !ps.canFetch(bid) {
			keep = append(keep, bid)
			continue
		}
		if _, ok := ps.requestedHashes[bid.Hash]; ok {
			keep = append(keep, bid)
			continue
		}
		if !ss.reserveBlockPathLocked(bid) {
			continue
		}
		ps.fetchList = append(ps.fetchList, bid)
	}
	ss.retryList = keep
}

func (ps *syncPeerState) canFetch(bid types.BlockID) bool {
	if ps.lastInventoryNum == 0 {
		return false
	}
	return bid.Num >= ps.minFetchNum && bid.Num <= ps.lastInventoryNum
}

func (ss *SyncService) nextFetchBatchLocked(ps *syncPeerState, effectiveTipNum uint64) []types.BlockID {
	if len(ps.fetchList) == 0 {
		return nil
	}
	batch := make([]types.BlockID, 0, maxFetchBatch)
	remaining := ps.fetchList[:0]
	for _, bid := range ps.fetchList {
		// Budget first: it is the cheapest check and must run before
		// reserveBlockPathLocked so a parked bid acquires no reservation
		// side effects while it waits for the head to advance.
		if !ss.withinRunaheadBudgetLocked(bid, effectiveTipNum) {
			remaining = append(remaining, bid)
			continue
		}
		if ss.hasBlockOrRequestLocked(bid) {
			continue
		}
		if !ss.reserveBlockPathLocked(bid) {
			continue
		}
		if _, ok := ps.requestedHashes[bid.Hash]; ok {
			continue
		}
		if len(batch) < maxFetchBatch {
			batch = append(batch, bid)
			continue
		}
		remaining = append(remaining, bid)
	}
	ps.fetchList = remaining
	return batch
}

func (ss *SyncService) hasBlockOrRequestLocked(bid types.BlockID) bool {
	committedHeadNum := ss.chain.CurrentBlock().Number()
	effectiveTipNum := ss.effectiveSyncTipLocked()
	if bid.Num > committedHeadNum && bid.Num <= effectiveTipNum {
		// Any reservation at an async-applied height is obsolete, including a
		// conflicting one left by a request that was assigned before the cursor
		// advanced.
		delete(ss.blockPath, bid.Num)
		return true
	}
	if ss.blockPathConflictsLocked(bid) {
		return true
	}
	if _, ok := ss.requested[bid.Hash]; ok {
		return true
	}
	if _, ok := ss.bufferedHash[bid.Hash]; ok {
		return true
	}
	if bid.Num <= committedHeadNum {
		if existing, ok := ss.chain.BlockIDByNumber(bid.Num); ok && existing.Hash == bid.Hash {
			ss.releaseBlockPathLocked(bid)
			return true
		}
	}
	if ss.chain.HasBlockInKhaosDB(bid.Hash) {
		ss.releaseBlockPathLocked(bid)
		return true
	}
	return false
}

func (ss *SyncService) blockPathConflictsLocked(bid types.BlockID) bool {
	if ss.blockPath == nil {
		return false
	}
	hash, ok := ss.blockPath[bid.Num]
	return ok && hash != bid.Hash
}

func (ss *SyncService) reserveBlockPathLocked(bid types.BlockID) bool {
	if ss.blockPathConflictsLocked(bid) {
		return false
	}
	if ss.blockPath == nil {
		ss.blockPath = make(map[uint64]tcommon.Hash)
	}
	ss.blockPath[bid.Num] = bid.Hash
	return true
}

func (ss *SyncService) releaseBlockPathLocked(bid types.BlockID) {
	if hash, ok := ss.blockPath[bid.Num]; ok && hash == bid.Hash {
		delete(ss.blockPath, bid.Num)
	}
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
// original bytes may pass nil — they are re-marshaled from `block`. When this
// method consumes a block, ownership of non-empty raw transfers to the sync
// buffer and the caller must not mutate it afterward.
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
	expectedNum, ok := ps.pending[blockHash]
	if !ok || expectedNum != blockNum {
		ss.mu.Unlock()
		return true
	}
	delete(ps.pending, blockHash)
	delete(ps.pendingIDs, blockHash)
	delete(ss.requested, blockHash)
	// Bump seq so any in-flight timer callback short-circuits. We stop the
	// armed timer below but the callback may already be running on another
	// goroutine and waiting on ss.mu; the seq check inside onFetchTimeout
	// rejects it.
	ps.fetchSeq++
	if ps.inflight > 0 {
		ps.inflight--
	}
	batchDone := ps.inflight == 0
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
	effectiveTipNum := ss.effectiveSyncTipLocked()
	ss.pruneStaleSyncStateLocked(effectiveTipNum)
	if blockNum > effectiveTipNum {
		bid := types.BlockID{Hash: blockHash, Num: blockNum}
		if existing, ok := ss.blockBuffer[blockNum]; ok {
			if existing.hash != blockHash {
				syncLog.Debug("Dropping conflicting buffered sync block",
					"number", blockNum, "hash", blockHash, "kept", existing.hash, "peer", peer.ID())
			}
		} else if _, ok := ss.bufferedHash[blockHash]; !ok && ss.reserveBlockPathLocked(bid) {
			entry := bufferedSyncBlock{
				raw:  bufferRawBlockBytes(block, raw),
				hash: blockHash,
				num:  blockNum,
				peer: peer,
			}
			if ss.retainDecodedBlockLocked(block, blockNum, effectiveTipNum, len(entry.raw)) {
				entry.decoded = block
			}
			ss.blockBuffer[blockNum] = entry
			ss.bufferedHash[blockHash] = struct{}{}
			ss.bufferedBytes += int64(len(entry.raw))
		}
	} else {
		// A request can already be on the wire when another peer advances the
		// applied cursor. Complete its request bookkeeping above, and release
		// the now-stale path without disturbing a different fork reservation.
		ss.releaseBlockPathLocked(types.BlockID{Hash: blockHash, Num: blockNum})
	}
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()

	// Keep the peer read loop free to receive the rest of the requested range.
	// Importing a block can take tens of milliseconds; doing it inline here
	// prevents a lone peer from filling blockBuffer while the current block is
	// executing, which in turn forces every deep InsertSession to finish as soon
	// as the buffer runs dry. A single scheduled drain worker preserves ordered
	// insertion while allowing receive and execution to overlap.
	ss.scheduleDrainBufferedBlocks()
	return true
}

// scheduleDrainBufferedBlocks starts one asynchronous drain worker, or asks the
// active worker to take another pass. The drainMu handoff closes both lost-wake
// windows: an arrival before the worker's final check sets drainAgain, while an
// arrival after it clears draining starts a new worker.
func (ss *SyncService) scheduleDrainBufferedBlocks() {
	ss.drainMu.Lock()
	if ss.draining {
		ss.drainAgain = true
		ss.drainMu.Unlock()
		return
	}
	ss.draining = true
	ss.drainMu.Unlock()

	go ss.runDrainBufferedBlocks()
}

// drainBufferedBlocks runs a drain synchronously for lifecycle callers such as
// the watchdog. Peer receive paths use scheduleDrainBufferedBlocks instead.
func (ss *SyncService) drainBufferedBlocks() {
	ss.drainMu.Lock()
	if ss.draining {
		ss.drainAgain = true
		ss.drainMu.Unlock()
		return
	}
	ss.draining = true
	ss.drainMu.Unlock()
	ss.runDrainBufferedBlocks()
}

func (ss *SyncService) runDrainBufferedBlocks() {
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
	// Deep async-commit pipeline (depth > 2): span ONE InsertSession across all
	// batches so the commit worker is drained only at the end of the drain (not
	// at every ≤100-block batch boundary) — the cross-batch barrier amortization.
	// sess == nil for the synchronous / depth-2 path, where the loop below is
	// byte-identical to before (plain InsertBlocks per batch).
	var sess *core.InsertSession
	if ss.chain.PipelinedCommitDepth() > 2 {
		sess = ss.chain.BeginInsertSession()
	}
	var lastPeer *p2p.Peer
	paused := false
	for {
		now := time.Now()
		ss.mu.Lock()
		if !ss.syncing || ss.pause.Paused() {
			ss.mu.Unlock()
			break
		}
		batch := ss.popBufferedSyncBatchLocked(now)
		if len(batch.buffered) == 0 {
			if sess != nil {
				// Drain the commit worker before the completion check: under deep
				// pipelining CurrentBlock lags the applied tip, and shouldFinish /
				// fillFetchSlots must see every applied block as committed. Release
				// ss.mu first — Finish takes chainmu, always acquired outside ss.mu.
				ss.mu.Unlock()
				if ferr := sess.Finish(); ferr != nil && !paused {
					ss.pauseSync(lastPeer, ss.chain.CurrentBlock().Number()+1, ferr)
					paused = true
				}
				sess = nil
				ss.mu.Lock()
			}
			next := ss.chain.CurrentBlock().Number() + 1
			ss.beginBufferWaitLocked(next, now)
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
		if len(batch.blocks) == 0 {
			// Every popped block failed to decode (can't happen for validated
			// wire bytes). The entries were already removed at pop, so loop to
			// re-pop the next run or hit the gap.
			ss.releaseDecodedBatch(&batch)
			continue
		}
		for _, wait := range batch.bufferWaits {
			ss.stats.AddBufferWait(wait)
		}
		if n := len(batch.buffered); n > 0 {
			lastPeer = batch.buffered[n-1].peer
		}

		insertStart := time.Now()
		var insertErr error
		if sess != nil {
			insertErr = sess.Insert(batch.blocks)
		} else {
			insertErr = ss.chain.InsertBlocks(batch.blocks)
		}
		insertElapsed := time.Since(insertStart)
		applied := len(batch.blocks)
		if insertErr != nil {
			failed := 0
			var rangeErr *core.InsertBlocksError
			if errors.As(insertErr, &rangeErr) && rangeErr.Index >= 0 && rangeErr.Index < len(batch.buffered) {
				failed = rangeErr.Index
			}
			applied = failed
			ss.recordImportedBatch(batch, applied, insertElapsed)
			ss.releaseDecodedBatch(&batch)
			failedNum := batch.buffered[failed].num
			if failedNum == 0 && rangeErr != nil {
				failedNum = rangeErr.BlockNumber
			}
			ss.pauseSync(batch.buffered[failed].peer, failedNum, insertErr)
			paused = true
			break
		}
		ss.recordImportedBatch(batch, applied, insertElapsed)
		ss.releaseDecodedBatch(&batch)
		if stopHeight, configured := ss.configuredStopHeight(); configured && batch.buffered[applied-1].num >= stopHeight {
			// A deep InsertSession publishes CurrentBlock only when Finish drains
			// its commit worker. Settle it before latching the audit pause so the
			// database head is guaranteed to be exactly the requested height.
			if sess != nil {
				if ferr := sess.Finish(); ferr != nil {
					ss.pauseSync(lastPeer, stopHeight, ferr)
					paused = true
					sess = nil
					break
				}
				sess = nil
			}
			ss.pauseAtStopHeight(stopHeight)
			paused = true
			break
		}
		// Refill a peer only after at least one contiguous batch has applied
		// successfully. This lets receive run ahead of execution across fetch
		// batches without sending more requests after a bad block. In the common
		// single-peer case, the read loop fills blockBuffer while this worker is
		// inserting; once the previous request completes, this check immediately
		// starts the next request instead of waiting for the buffer to empty.
		ss.mu.Lock()
		var batchOut []outboundSyncRequest
		if ss.syncing && !ss.pause.Paused() {
			batchOut = ss.fillFetchSlotsLocked(time.Now())
			ss.mirrorLegacyLocked()
		}
		ss.mu.Unlock()
		ss.sendOutboundRequests(batchOut)
	}
	// Settle the session on any loop-exit path (not-syncing / paused / error
	// break). The empty-batch path already finished it above (sess == nil there).
	if sess != nil {
		if ferr := sess.Finish(); ferr != nil && !paused {
			ss.pauseSync(lastPeer, ss.chain.CurrentBlock().Number()+1, ferr)
		}
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

func (ss *SyncService) popBufferedSyncBatchLocked(now time.Time) bufferedSyncBatch {
	// Start from the drain cursor, not the committed head: under async-commit
	// depth>2 CurrentBlock lags the applied tip, and CurrentBlock+1 may name a
	// block we already imported and deleted from the buffer — which would break
	// the drain after a single batch. syncedTipNum tracks what we've popped and
	// equals CurrentBlock when async commit is off, keeping that path unchanged.
	next := ss.effectiveSyncTipLocked()
	ss.pruneStaleSyncStateLocked(next)
	next++
	var batch bufferedSyncBatch
	for len(batch.buffered) < maxFetchBatch {
		if stopHeight, configured := ss.configuredStopHeight(); configured && next > stopHeight {
			break
		}
		buffered, ok := ss.popBufferedBlockLocked(next)
		if !ok {
			break
		}
		batch.bufferWaits = append(batch.bufferWaits, ss.endBufferWaitLocked(next, now))
		batch.buffered = append(batch.buffered, buffered)
		next++
	}
	if popped := next - 1; popped > ss.syncedTipNum {
		ss.syncedTipNum = popped
	}
	if ss.syncedTipNum > ss.bufferPrunedTipNum {
		// The newly covered range consists exactly of entries removed above;
		// future late responses are rejected by HandleBlock admission.
		ss.bufferPrunedTipNum = ss.syncedTipNum
	}
	return batch
}

// decodeBatchBlocks materializes the popped blocks. A bounded near-tip subset
// reuses the object already decoded by the peer receive path; raw-only entries
// are protobuf-decoded here. It runs OFF ss.mu because a full decode per block
// is far too heavy for the central sync lock. A decode error (can't happen for
// bytes that already decoded at receive) truncates the batch.
func (ss *SyncService) decodeBatchBlocks(batch *bufferedSyncBatch) {
	batch.blocks = make([]*types.Block, 0, len(batch.buffered))
	for i := range batch.buffered {
		if batch.buffered[i].decoded != nil {
			batch.buffered[i].decoded.AdoptMarshalScratch(batch.buffered[i].raw)
			batch.blocks = append(batch.blocks, batch.buffered[i].decoded)
			continue
		}
		blk, err := types.UnmarshalBlockOwned(batch.buffered[i].raw)
		if err != nil {
			syncLog.Error("Dropping undecodable buffered sync block",
				"number", batch.buffered[i].num, "hash", batch.buffered[i].hash, "err", err)
			return
		}
		batch.blocks = append(batch.blocks, blk)
	}
}

// releaseDecodedBatch drops retained receive-path objects after their insert
// attempt and returns their charge to the global cap. Newly decoded raw-only
// blocks are also cleared so the next receive/refill pass cannot overlap their
// object graph unnecessarily.
func (ss *SyncService) releaseDecodedBatch(batch *bufferedSyncBatch) {
	ss.mu.Lock()
	for i := range batch.buffered {
		ss.releaseRetainedDecodedLocked(&batch.buffered[i])
	}
	ss.mu.Unlock()
	for i := range batch.blocks {
		batch.blocks[i] = nil
	}
}

func (ss *SyncService) recordImportedBatch(batch bufferedSyncBatch, applied int, totalElapsed time.Duration) {
	if applied <= 0 {
		return
	}
	var txs int
	// txKinds tallies the applied range's transactions by contract type for the
	// "txTop" composition field of the segment summary. ContractType() is a
	// cheap field read, so this shares the existing tx-count pass at no real
	// cost. Folded into the window before RecordBlocks so the snapshot it may
	// emit includes this range's breakdown.
	txKinds := make(map[string]int)
	for i := 0; i < applied && i < len(batch.blocks); i++ {
		if block := batch.blocks[i]; block != nil {
			txList := block.Transactions()
			txs += len(txList)
			for _, tx := range txList {
				txKinds[tx.ContractType().String()]++
			}
		}
	}
	ss.stats.AddTxKinds(txKinds)
	// RecordBlocks atomically (under stats.mu) appends the whole range's
	// counters and decides whether the window has elapsed. applyBlock hooks
	// have already contributed phase stats for the same applied range, so
	// recording the range as one unit keeps block counts and phase totals
	// aligned in the emitted sync summary.
	snap, emit := ss.stats.RecordBlocks(
		applied,
		txs,
		totalElapsed,
		time.Now(),
		tsync.StatsReportInterval,
	)

	ss.mu.Lock()
	var diag syncDiagnostics
	if emit {
		diag = ss.snapshotDiagnosticsLocked()
	}
	remain := ss.estimatedRemainLocked()
	ss.mirrorLegacyLocked()
	ss.mu.Unlock()

	if emit {
		last := batch.buffered[applied-1]
		ss.reportSegment(snap, diag, last.num, remain, last.peer)
	}
}

func (ss *SyncService) beginBufferWaitLocked(next uint64, now time.Time) {
	if ss.bufferWaitStart.IsZero() || ss.bufferWaitNum != next {
		ss.bufferWaitStart = now
		ss.bufferWaitNum = next
	}
}

func (ss *SyncService) endBufferWaitLocked(next uint64, now time.Time) time.Duration {
	if ss.bufferWaitStart.IsZero() || ss.bufferWaitNum != next {
		ss.bufferWaitStart = time.Time{}
		ss.bufferWaitNum = 0
		return 0
	}
	elapsed := now.Sub(ss.bufferWaitStart)
	ss.bufferWaitStart = time.Time{}
	ss.bufferWaitNum = 0
	if elapsed < 0 {
		return 0
	}
	return elapsed
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
		"hint", "restart to resume")
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
	syncLog.Info("Sync stopped at configured height",
		"height", height,
		"hint", "stop the node before opening its database with db-compare; remove --sync.stop-at to resume")
}

func (ss *SyncService) estimatedRemainLocked() int64 {
	head := ss.chain.CurrentBlock().Number()
	if ss.targetHeadNum > head {
		return int64(ss.targetHeadNum - head)
	}
	remain := len(ss.retryList) + len(ss.blockBuffer)
	for _, ps := range ss.peers {
		remain += len(ps.fetchList) + ps.inflight
		if ps.remainNum > 0 {
			remain += int(ps.remainNum)
		}
	}
	return int64(remain)
}

func (ss *SyncService) shouldFinishLocked() bool {
	if !ss.syncing || ss.pause.Paused() {
		return false
	}
	if len(ss.retryList) != 0 || len(ss.blockBuffer) != 0 {
		return false
	}
	for _, ps := range ss.peers {
		if ps.chainRequested || ps.inflight != 0 || len(ps.fetchList) != 0 {
			return false
		}
		if !ps.done {
			return false
		}
	}
	return ss.targetHeadNum == 0 || ss.chain.CurrentBlock().Number() >= ss.targetHeadNum
}

func (ss *SyncService) shouldRestartForStalledRetriesLocked() bool {
	if !ss.syncing || ss.pause.Paused() || len(ss.retryList) == 0 || len(ss.blockBuffer) != 0 {
		return false
	}
	for _, ps := range ss.peers {
		if ps == nil {
			continue
		}
		if ps.chainRequested || ps.inflight != 0 || len(ps.fetchList) != 0 {
			return false
		}
	}
	return true
}

func (ss *SyncService) snapshotDiagnosticsLocked() syncDiagnostics {
	diag := syncDiagnostics{
		blockBufferLen:       len(ss.blockBuffer),
		requestedLen:         len(ss.requested),
		retryListLen:         len(ss.retryList),
		retainedDecoded:      ss.retainedDecodedBlocks,
		retainedDecodedBytes: ss.retainedDecodedBytes,
	}
	if len(ss.peers) == 0 {
		return diag
	}
	ids := make([]string, 0, len(ss.peers))
	for id := range ss.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		ps := ss.peers[id]
		if ps == nil {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s{inflight=%d fetchList=%d pending=%d remain=%d chainRequested=%t done=%t}",
			id, ps.inflight, len(ps.fetchList), len(ps.pending), ps.remainNum, ps.chainRequested, ps.done))
	}
	diag.peerState = strings.Join(parts, ";")
	return diag
}

// reportSegment emits the throttled "Imported chain segment" summary. Called
// without ss.mu held.
func (ss *SyncService) reportSegment(s tsync.Snapshot, diag syncDiagnostics, head uint64, remain int64, peer *p2p.Peer) {
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
	txTop := tsync.TopTxKindsString(s.TxKinds, 5)
	if txTop == "" {
		txTop = "none"
	}
	ctx = append(ctx, "txTop", txTop)
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
		"blockBuffer", diag.blockBufferLen,
		"retainedDecoded", diag.retainedDecoded,
		"retainedDecodedBytes", diag.retainedDecodedBytes,
		"requested", diag.requestedLen,
		"retryList", diag.retryListLen,
	}
	if diag.peerState != "" {
		detail = append(detail, "peerState", diag.peerState)
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
	ss.releaseBufferedDecodedLocked()
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
	ss.bufferedBytes = 0
	ss.targetHeadNum = 0
	ss.syncedTipNum = 0
	ss.bufferPrunedTipNum = 0
	ss.bufferWaitStart = time.Time{}
	ss.bufferWaitNum = 0
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
	restart := false
	if len(ss.peers) == 0 {
		ss.doReset()
		restart = true
	} else {
		out = ss.fillFetchSlotsLocked(time.Now())
		restart = len(out) == 0 && ss.shouldRestartForStalledRetriesLocked()
		if restart {
			ss.doReset()
		} else {
			ss.mirrorLegacyLocked()
		}
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
	if restart || !ss.IsSyncing() {
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
	restart := false
	empty := len(ss.peers) == 0
	if empty {
		ss.doReset()
		restart = true
	} else {
		out = ss.fillFetchSlotsLocked(time.Now())
		restart = len(out) == 0 && ss.shouldRestartForStalledRetriesLocked()
		if restart {
			ss.doReset()
		} else {
			ss.mirrorLegacyLocked()
		}
	}
	ss.mu.Unlock()
	syncLog.Info("Sync peer disconnected", "peer", peer.ID())
	if len(out) > 0 {
		ss.sendOutboundRequests(out)
	}
	if restart || empty {
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
		for h, bid := range ps.pendingIDs {
			delete(ss.requested, h)
			if !ss.hasBlockOrRequestLocked(bid) {
				ss.retryList = append(ss.retryList, bid)
			}
		}
		for _, bid := range ps.fetchList {
			if !ss.hasBlockOrRequestLocked(bid) {
				ss.retryList = append(ss.retryList, bid)
			}
		}
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
