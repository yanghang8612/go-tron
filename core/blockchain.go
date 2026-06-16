package core

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var log = gtronlog.NewModule("core/chain")

var (
	ErrKnownBlock       = errors.New("block already known")
	ErrInvalidParent    = errors.New("parent block not found")
	ErrInvalidNumber    = errors.New("invalid block number")
	ErrBlockChainClosed = errors.New("blockchain closed")
)

// InsertBlocksError reports the first block that failed inside InsertBlocks.
// Blocks before Index were applied successfully; Index and later blocks were
// not applied.
type InsertBlocksError struct {
	Index       int
	BlockNumber uint64
	Err         error
}

func (e *InsertBlocksError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("insert block range index %d block %d: %v", e.Index, e.BlockNumber, e.Err)
}

func (e *InsertBlocksError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ApplyStats reports per-phase wall-clock time spent inside applyBlock.
//
// Subscribers should treat ApplyStats as read-only. The fields are exported so
// callers (sync summary line, future metrics surface) can aggregate without
// reaching back into core internals.
//
//   - Validate: header verification (signature recovery, scheduled-witness
//     match, post-fork timestamp alignment) plus parent linkage.
//   - Execute: transaction execution + reward + BLOCK_FILLED_SLOTS update.
//     Includes the in-memory state mutations; does NOT include the flat
//     commitment update (that lives in StateCommit).
//   - Maintenance: doMaintenance work on cycle boundaries (proposals, vote
//     tally, active-set rotation, reward VI). Zero on non-maintenance blocks.
//   - StateCommit: statedb.Commit — flat latest-domain writes plus
//     CommitmentDomain root update. StateCommitDetail splits this phase into
//     the latest-state and commitment subphases for sync diagnostics.
//   - DPUpdate: dynamic-properties writes (latest_block_header_*,
//     solidified, fork-vote tally) into the buffer.
//   - Persist: WriteBlock + WriteTaposRef + tx info persist + the final
//     buffer flushBufferUpToSolidified that lands committed layers on disk.
//   - Hooks: post-apply callback fan-out (PBFT, broadcaster, etc.).
type ApplyStats struct {
	Validate          time.Duration
	Execute           time.Duration
	Maintenance       time.Duration
	StateCommit       time.Duration
	StateCommitDetail state.CommitStats
	DPUpdate          time.Duration
	Persist           time.Duration
	Hooks             time.Duration
}

// Total returns the sum of every phase.
func (s ApplyStats) Total() time.Duration {
	return s.Validate + s.Execute + s.Maintenance + s.StateCommit + s.DPUpdate + s.Persist + s.Hooks
}

// applyStats is the in-flight accumulator used by applyBlock. The mark cursor
// is advanced at phase boundaries; the snapshot is published to subscribers
// only on the success path.
type applyStats struct {
	last time.Time
	ApplyStats
}

// mark accumulates the elapsed time since the previous mark into *phase and
// advances the cursor. Accumulation (rather than assignment) lets a phase be
// split across non-contiguous code blocks — e.g. persist runs both before
// and after the hook callbacks.
func (s *applyStats) mark(phase *time.Duration) {
	now := time.Now()
	*phase += now.Sub(s.last)
	s.last = now
}

// BlockChain manages the canonical chain and provides block insertion.
//
// db vs chaindb split (freezer slice-2): every WRITE goes through bc.db
// directly — writes never touch ancient. READS of chain data that has
// ancient fall-through (ReadBlock, ReadBlockStateRoot, tx-info accessors)
// go through bc.chaindb so frozen blocks resolve transparently. Reads of
// non-frozen state (DP, genesis state root, witness store, etc.) stay on
// bc.db because their accessors take ethdb.KeyValueReader, not *ChainDB.
// New code adding a rawdb.Read* call must pick bc.chaindb iff the
// accessor's signature is *ChainDB-typed (i.e. has ancient fall-through).
type BlockChain struct {
	db      ethdb.KeyValueStore
	chaindb *rawdb.ChainDB // composite (db + freezer reader); slice-2 freezer plumbing
	stateDB *state.Database
	config  *params.ChainConfig

	stateCodeColdHistory       state.StateCodeColdHistoryAtOrBefore
	stateCommitmentColdHistory state.StateCommitmentColdHistory
	stateOpenHook              func(tcommon.Hash)
	stateCommitScopeHook       func()
	stateTxRangeSeedHook       func(uint64)

	currentBlock   atomic.Pointer[types.Block]
	chainmu        sync.Mutex // serializes block insertion
	lastInsertNano atomic.Int64
	closed         atomic.Bool

	genesisBlock      *types.Block
	genesisWitnesses  []consensus.GenesisWitnessInfo
	activeWitnesses   atomic.Value // []tcommon.Address
	dynPropsCache     atomic.Value // *state.DynamicProperties; canonical head snapshot
	standbyPayCache   *standbyWitnessPaySet
	rewardAcctCache   map[tcommon.Address]*state.AccountSnapshot
	systemAcctCache   *state.AccountSnapshot
	rewardAcctSeen    map[tcommon.Address]struct{}
	rewardAcctAddrs   []tcommon.Address
	witnessBlockCache map[tcommon.Address]int64
	forkStatsCache    map[int32][]byte
	cycleRewards      *cycleRewardAccumulator
	// proposalCache skips re-reading already-resolved proposals during the
	// per-maintenance ProcessProposals scan. Node-local; reset on reorg /
	// failed apply. See proposalScanCache.
	proposalCache *proposalScanCache
	// versionPassCache skips the per-tx fork-stats read + vote tally for an SR
	// fork version that has already activated. Node-local; reset on reorg /
	// failed apply (same discipline as proposalCache). See forks.VersionPassCache.
	versionPassCache *forks.VersionPassCache
	fc               *forks.ForkController

	// engine validates block headers (signature, witness scheduling, timestamp
	// alignment) when applyBlock runs. Wired post-construction via SetEngine
	// because dpos.New(bc) requires bc to exist first. nil ⇒ header
	// verification is skipped — used only by tests that build unsigned blocks
	// to exercise the state-machine path in isolation. Every production
	// callsite must call SetEngine before the first InsertBlock.
	engine consensus.Engine

	khaosDB *KhaosDB

	// buffer holds legacy rawdb-shaped mirror writes from applyBlock that must
	// be rewindable on switchFork. Layered per applyBlock; switchFork drops
	// orphan-branch layers. Rooted state is authoritative for consensus state,
	// while BufferedDB keeps legacy readers aligned with unflushed mirrors.
	buffer *blockbuffer.Buffer

	// Async-flush plumbing. applyBlock posts the new solidified cutoff to
	// flushQueue via a non-blocking send; a worker goroutine drains the
	// channel and runs the disk flush off the chainmu critical path. The
	// buffer's internal RWMutex keeps PendingBlocks/Get/Has safe to call
	// concurrently with the worker (single-writer contract still holds:
	// only the worker and an inline fallback ever call FlushUpTo, and
	// Close serialises against both).
	//
	// flushPending counts in-flight cutoffs so callers (Close, tests,
	// switchFork, external observers via WaitForFlushSettled) can wait for
	// the queue to drain. The cond-var design tolerates concurrent post
	// (from applyBlock) and wait (from anywhere), which sync.WaitGroup
	// forbids. flushWorkerWg tracks the worker goroutine's lifetime so
	// Close can join it after closing the channel.
	//
	// flushErr is set fail-fast when an async flush returns an error; the
	// next applyBlock surfaces it before doing any work. Mirrors today's
	// sync error severity — a write failure at this layer corrupts the
	// chain regardless of timing.
	flushQueue    chan uint64
	flushPending  *flushBarrier
	flushWorkerWg sync.WaitGroup
	flushClosed   bool
	flushErr      atomic.Pointer[error]

	// Async-commit plumbing (default OFF — see SetAsyncCommit). When enabled,
	// applyBlockWithPlan runs only the foreground half (exec + maintenance +
	// latest-domain capture into the in-memory scope) and hands the commitment
	// fold + ordered publish tail (root write, head advance, hooks, CommitBlock,
	// solidified flush) to a single serial commit worker, so the ~55% commit
	// cost overlaps the next block's execution. The buffer's multi-active-layer
	// support (SetMaxInflight(2)) lets the worker write block N's layer while the
	// foreground writes block N+1's. Mirrors the flush worker lifecycle: a queue,
	// a cond-var barrier (post/wait), a fail-fast error pointer, and a WaitGroup.
	//
	// With asyncCommit false (the default) the guard in applyBlockWithPlan is not
	// taken and the synchronous commit path runs unchanged — byte-identical.
	asyncCommit    bool
	commitDepth    int // resolved at NewBlockChain (GTRON_ASYNC_COMMIT_DEPTH), ≥2
	commitQueue    chan *commitJob
	commitPending  *flushBarrier
	commitWorkerWg sync.WaitGroup
	commitClosed   bool
	commitErr      atomic.Pointer[error]

	blockHookMu sync.Mutex
	blockHooks  []func(*types.Block) // called after each successful InsertBlock

	maintHookMu sync.Mutex
	maintHooks  []func(*types.Block, []tcommon.Address) // fired after a maintenance block

	applyStatsHookMu sync.Mutex
	applyStatsHooks  []func(*types.Block, ApplyStats) // fired after each successful applyBlock with per-phase wall-clock breakdown
}

// SetEngine wires the consensus engine used for header verification in
// applyBlock. Must be called once, after NewBlockChain and before any
// InsertBlock — typically `bc.SetEngine(dpos.New(bc))` in cmd/gtron. Tests
// that bypass header verification (unsigned block builders, fork-rewind
// state-machine fixtures) simply omit the call.
func (bc *BlockChain) SetEngine(eng consensus.Engine) {
	bc.engine = eng
}

// headerSigPrewarmer returns the consensus engine as a headerSignaturePrewarmer
// for the parallel signature pre-pass, or nil when no engine is wired (test
// path) or the engine doesn't implement header-signature prewarming. When nil,
// the pre-pass warms only transaction senders; header recovery (also skipped
// without an engine) happens inline. Mirrors the bc.engine != nil guard that
// gates VerifyHeaderWithDynProps.
func (bc *BlockChain) headerSigPrewarmer() headerSignaturePrewarmer {
	if bc.engine == nil {
		return nil
	}
	pw, _ := bc.engine.(headerSignaturePrewarmer)
	return pw
}

// AddBlockHook registers a callback called after each successfully inserted block.
func (bc *BlockChain) AddBlockHook(fn func(*types.Block)) {
	bc.blockHookMu.Lock()
	bc.blockHooks = append(bc.blockHooks, fn)
	bc.blockHookMu.Unlock()
}

// SetStateCodeColdHistory wires content-addressed CodeDomain snapshots into
// live StateDB code reads after hot state-code rows have been pruned.
func (bc *BlockChain) SetStateCodeColdHistory(source state.StateCodeColdHistoryAtOrBefore) {
	bc.stateCodeColdHistory = source
}

// SetStateCommitmentColdHistory wires CommitmentDomain snapshots into commit
// repair so pruned hot branch nodes can be restored before incremental updates.
func (bc *BlockChain) SetStateCommitmentColdHistory(source state.StateCommitmentColdHistory) {
	bc.stateCommitmentColdHistory = source
}

// AddApplyStatsHook registers a callback invoked after each successful
// applyBlock with the per-phase wall-clock breakdown. Subscribers must treat
// the ApplyStats value as read-only and return quickly; callbacks run on the
// applyBlock goroutine, holding bc.chainmu. Used by the sync summary line and
// the metrics surface.
func (bc *BlockChain) AddApplyStatsHook(fn func(*types.Block, ApplyStats)) {
	bc.applyStatsHookMu.Lock()
	bc.applyStatsHooks = append(bc.applyStatsHooks, fn)
	bc.applyStatsHookMu.Unlock()
}

// AddMaintenanceHook registers a callback fired after each successfully
// inserted block whose timestamp crossed the maintenance boundary. The new
// active-witness set (post-rotation) is passed alongside the block. Mirrors
// java-tron MaintenanceManager.applyBlock's `pbftManager.srPrePrepare` trigger
// (consensus/src/main/java/org/tron/consensus/dpos/MaintenanceManager.java:72).
func (bc *BlockChain) AddMaintenanceHook(fn func(*types.Block, []tcommon.Address)) {
	bc.maintHookMu.Lock()
	bc.maintHooks = append(bc.maintHooks, fn)
	bc.maintHookMu.Unlock()
}

// flushBarrier counts in-flight async flushes and supports concurrent
// post (Add) + wait (Wait), which sync.WaitGroup explicitly forbids
// ("Add called concurrently with Wait" panics). postFlush runs on the
// chainmu-holding applyBlock path; WaitForFlushSettled is exported for
// external observers who must not need to coordinate against the writer.
// The cond-var design lets both proceed independently without races.
type flushBarrier struct {
	mu      sync.Mutex
	cond    *sync.Cond
	pending int
}

func newFlushBarrier() *flushBarrier {
	b := &flushBarrier{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *flushBarrier) post() {
	b.mu.Lock()
	b.pending++
	b.mu.Unlock()
}

func (b *flushBarrier) done() {
	b.mu.Lock()
	b.pending--
	if b.pending == 0 {
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}

func (b *flushBarrier) wait() {
	b.mu.Lock()
	for b.pending > 0 {
		b.cond.Wait()
	}
	b.mu.Unlock()
}

// flushQueueCap caps the in-flight async-flush cutoffs the worker will
// buffer ahead of itself. Steady state is one post per applyBlock; the
// queue exists only to smooth out micro-bursts (e.g. a sync replay that
// applies several blocks before the worker schedules in). When the queue
// is full, applyBlock falls back to an inline flush — backpressure
// guarantees a flush is never lost.
const flushQueueCap = 8

// NewBlockChain creates a new BlockChain, loading head from DB.
func NewBlockChain(db ethdb.KeyValueStore, stateDB *state.Database, config *params.ChainConfig) (*BlockChain, error) {
	return NewBlockChainWithAncient(db, stateDB, config, rawdb.NoopAncient{})
}

// NewBlockChainWithAncient creates a BlockChain with an explicit ancient
// reader. Production startup passes the freezer reader here so block and
// transaction-info accessors can transparently fall through to frozen data.
func NewBlockChainWithAncient(db ethdb.KeyValueStore, stateDB *state.Database, config *params.ChainConfig, ancient rawdb.AncientReader) (*BlockChain, error) {
	buffer := blockbuffer.New(db)
	if ancient == nil {
		ancient = rawdb.NoopAncient{}
	}
	chaindb := rawdb.NewChainDB(db, ancient)
	// Resolve the async-commit pipeline depth ONCE here and size the commit queue
	// to depth-2 (the backpressure bound). The commit worker, started below in
	// this constructor, ranges this exact channel for its lifetime — sizing it
	// here (not in the later SetAsyncCommit) keeps the worker from ever being
	// orphaned on a re-made channel. depth-2 == 0 ⇒ the unbuffered rendezvous.
	commitDepth := resolveCommitPipelineDepth()
	bc := &BlockChain{
		db:                db,
		chaindb:           chaindb,
		stateDB:           stateDB,
		config:            config,
		fc:                forks.NewForkController(buffer),
		buffer:            buffer,
		flushQueue:        make(chan uint64, flushQueueCap),
		flushPending:      newFlushBarrier(),
		commitDepth:       commitDepth,
		commitQueue:       make(chan *commitJob, commitDepth-2),
		commitPending:     newFlushBarrier(),
		rewardAcctCache:   make(map[tcommon.Address]*state.AccountSnapshot),
		rewardAcctSeen:    make(map[tcommon.Address]struct{}),
		rewardAcctAddrs:   make([]tcommon.Address, 0, 128),
		witnessBlockCache: make(map[tcommon.Address]int64),
		forkStatsCache:    make(map[int32][]byte, len(forks.KnownVersions)),
		proposalCache:     newProposalScanCache(),
		versionPassCache:  forks.NewVersionPassCache(),
	}
	var err error
	bc.cycleRewards, err = newCycleRewardAccumulator(buffer)
	if err != nil {
		return nil, fmt.Errorf("load cycle reward pending accumulator: %w", err)
	}
	bc.lastInsertNano.Store(time.Now().UnixNano())

	// Load genesis
	bc.genesisBlock = rawdb.ReadBlock(chaindb, 0)
	if bc.genesisBlock == nil {
		return nil, errors.New("genesis block not found in database")
	}

	for _, gw := range rawdb.ReadGenesisWitnesses(db) {
		bc.genesisWitnesses = append(bc.genesisWitnesses, consensus.GenesisWitnessInfo{
			Address:   gw.Address,
			VoteCount: gw.VoteCount,
		})
	}

	// Trust the persisted head. Every per-block write (LastBlock pointer,
	// applied dynamic properties, commitment root, account-KV, commitment
	// branches) lands in the same blockbuffer layer and is flushed to disk
	// atomically per layer, so the on-disk image is always self-consistent at
	// a block boundary — whether the node was closed cleanly (full buffer
	// flush to head) or crashed (last async flush to a solidified block).
	// Block bodies written ahead of the flushed head are harmless orphans that
	// re-sync re-applies. No startup state rebuild is required.
	head := loadStoredHeadBlock(chaindb, bc.genesisBlock)
	bc.currentBlock.Store(head)

	// Seed the dynprops cache now that the head is known: rooted keys load from
	// the system-KV at the head root, derived keys from the buffer.
	bc.storeDynPropsCache(state.LoadDynamicProperties(buffer, bc.sysKVAt(bc.HeadStateRoot())))

	// Initialize KhaosDB with the current head.
	bc.khaosDB = NewKhaosDB()
	bc.khaosDB.Start(bc.currentBlock.Load())

	// Load the active witness list from the rooted system-KV at the head root.
	// Genesis seeds it (genesisBlockAndStateRoot selects from the genesis
	// witnesses) and every maintenance block re-roots it, so any head that has
	// run genesis carries it — no derive-from-index fallback is needed.
	if sysKV := bc.sysKVAt(bc.HeadStateRoot()); sysKV != nil {
		if witnesses := sysKV.ReadActiveWitnesses(); len(witnesses) > 0 {
			bc.activeWitnesses.Store(witnesses)
		}
	}

	// Only start the async flush worker once construction can no longer
	// fail: an early error-return above would otherwise leak the worker
	// goroutine (no BlockChain handle is exposed to the caller, so nothing
	// can drive Close to drain it). Mirrors the standard Go pattern of
	// deferring resource start until the constructor is guaranteed to
	// succeed.
	bc.startFlushWorker()
	bc.startCommitWorker()

	return bc, nil
}

func loadStoredHeadBlock(chaindb *rawdb.ChainDB, genesis *types.Block) *types.Block {
	headHash := rawdb.ReadHeadBlockHash(chaindb)
	if headHash == (tcommon.Hash{}) {
		return genesis
	}
	num := rawdb.ReadBlockNumber(chaindb, headHash)
	if num == nil {
		return genesis
	}
	block := rawdb.ReadBlock(chaindb, *num)
	if block == nil {
		return genesis
	}
	return block
}

func syncKeyValueStore(db ethdb.KeyValueStore) error {
	type keyValueSyncer interface {
		SyncKeyValue() error
	}
	if syncer, ok := db.(keyValueSyncer); ok {
		return syncer.SyncKeyValue()
	}
	return nil
}

// CurrentBlock returns the head of the canonical chain.
func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock.Load()
}

// GetBlockByNumber retrieves a block by its number.
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	if current := bc.CurrentBlock(); current != nil && number > current.Number() {
		return nil
	}
	return rawdb.ReadBlock(bc.chaindb, number)
}

// GetBlockByHash retrieves a block by its hash.
func (bc *BlockChain) GetBlockByHash(hash tcommon.Hash) *types.Block {
	num := rawdb.ReadBlockNumber(bc.chaindb, hash)
	if num == nil {
		return nil
	}
	if current := bc.CurrentBlock(); current != nil && *num > current.Number() {
		return nil
	}
	return rawdb.ReadBlock(bc.chaindb, *num)
}

// HasBlockInKhaosDB reports whether KhaosDB holds the block in either the
// linked store or the unlinked-orphan store.
func (bc *BlockChain) HasBlockInKhaosDB(hash tcommon.Hash) bool {
	return bc.khaosDB.ContainsBlock(hash)
}

// GenesisTimestamp returns the genesis block timestamp.
func (bc *BlockChain) GenesisTimestamp() int64 {
	return bc.genesisBlock.Timestamp()
}

// Config returns the chain config.
func (bc *BlockChain) Config() *params.ChainConfig {
	return bc.config
}

func (bc *BlockChain) stateCommitOptions(_ *types.Block, _ bool) state.CommitOptions {
	return state.CommitOptions{}
}

// ForkController returns the chain's fork controller.
func (bc *BlockChain) ForkController() *forks.ForkController {
	if statedb := bc.sysKVAt(bc.HeadStateRoot()); statedb != nil {
		return forks.NewForkControllerFromState(statedb)
	}
	return bc.fc
}

// InsertBlockWithoutVerify inserts a block without consensus verification.
func (bc *BlockChain) InsertBlockWithoutVerify(block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return ErrBlockChainClosed
	}

	current := bc.CurrentBlock()

	if block.Number() != current.Number()+1 {
		return ErrInvalidNumber
	}
	if block.ParentHash() != current.Hash() {
		return ErrInvalidParent
	}

	if err := rawdb.WriteBlock(bc.db, block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())

	bc.currentBlock.Store(block)
	bc.lastInsertNano.Store(time.Now().UnixNano())

	return nil
}

// InsertBlock inserts a block with full state processing.
// It accepts blocks on competing forks: if the incoming block makes the KhaosDB
// head longer than the current canonical tip and on a different branch, switchFork
// is invoked to rewind and replay state on top of the lowest common ancestor.
// This mirrors java-tron Manager.pushBlock.
//
// Visibility guarantees on a successful return:
//
//   - State for the inserted block is in the BlockChain's buffer overlay:
//     reads through bc.DynProps(), bc.BufferedDPInt64(), bc.BufferedDB() and
//     any accessor that consults bc.buffer see the applied state.
//   - bc.CurrentBlock() has advanced to the inserted block.
//   - Block bytes (rawdb.WriteBlock + WriteTaposRef) and tx-info records
//     are persisted to disk.
//
// NOT guaranteed at return time:
//
//   - The buffer flush at the new solidified line runs on a background
//     worker (postFlush → flushBufferUpToSolidified). Disk-side counters
//     written into the buffer — dynamic properties, witness statistics,
//     fork-vote tallies — are visible through bc.buffer but may not yet
//     be on disk. On mainnet (27 SRs) the solidified line itself lags
//     head by ≥19 blocks, so any direct-disk reader was already observing
//     stale data; the async flush adds a few extra milliseconds on top.
//
// Callers that need synchronous disk-side visibility — typically tests
// or external observers reading bc.DB() directly — must call
// bc.WaitForFlushSettled() before reading. Production code reading
// through the buffer overlay is unaffected.
func (bc *BlockChain) InsertBlock(block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return ErrBlockChainClosed
	}

	return bc.insertBlockLocked(block)
}

// InsertBlocks applies a fetched canonical range through the same staged block
// pipeline while holding the chain lock once for the whole range. Each block
// still commits independently today; this entry point is the range-shaped
// surface sync can move onto while execution is collapsed toward shared domain
// transactions.
func (bc *BlockChain) InsertBlocks(blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return ErrBlockChainClosed
	}

	return bc.insertBlocksLocked(blocks)
}

// insertBlocksLocked applies a contiguous range through insertBlockLocked.
// Callers must hold bc.chainmu.
func (bc *BlockChain) insertBlocksLocked(blocks []*types.Block) (err error) {
	// Parallel signature pre-verification: warm every tx's sender recovery and
	// every block's witness-signature recovery ahead of serial execution, off
	// the critical path. Pure cache-warming — the serial path (envelope
	// validation, header verification) still owns every accept/reject decision
	// and reads an identical recovered value, computing inline on any miss.
	prewarmBlockSignatures(blocks, bc.headerSigPrewarmer())

	executor := newCanonicalRangeExecutor(bc, true)
	if bc.asyncCommit {
		// Async commit: settle the range at its boundary in one ordered defer so
		// the persistent state matches the synchronous path exactly. The
		// per-block flush lags by one (the in-flight layer's scope rows are not
		// flushable yet), so the final block would otherwise be left in the
		// buffer; here we drain, flush the scope into the committed layers, and
		// then flush buffer layers up to the head's solidified height — for a
		// single-SR chain (solidified == head) that flushes the final layer too,
		// converging to the synchronous on-disk image so a subsequent reorg
		// discards/rewinds identical state. For multi-SR the extra flush is a
		// no-op (already flushed ≤ solidified; the unsolidified tail stays
		// rewindable in the buffer, same as sync). Also bounds the worker's lag
		// to one range and guarantees the next range reads a fresh dynPropsCache.
		defer func() {
			bc.WaitForCommitSettled()
			if errPtr := bc.commitErr.Load(); errPtr != nil && err == nil {
				err = fmt.Errorf("async commit failed: %w", *errPtr)
			}
			if closeErr := executor.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			if err == nil {
				if solidified := bc.cachedDynProps().LatestSolidifiedBlockNum(); solidified > 0 {
					if flushErr := bc.postFlush(solidified); flushErr != nil {
						err = flushErr
					}
				}
			}
		}()
	} else {
		defer func() {
			if closeErr := executor.Close(); closeErr != nil {
				if err == nil {
					err = closeErr
				} else {
					err = fmt.Errorf("%w; close range executor: %v", err, closeErr)
				}
			}
		}()
	}
	for i, block := range blocks {
		if err := bc.insertBlockLockedWithExecutor(block, executor); err != nil {
			var blockNum uint64
			if block != nil {
				blockNum = block.Number()
			}
			return &InsertBlocksError{Index: i, BlockNumber: blockNum, Err: err}
		}
	}
	return nil
}

// insertBlockLocked is InsertBlock's body. Callers must hold bc.chainmu.
func (bc *BlockChain) insertBlockLocked(block *types.Block) error {
	executor := newCanonicalRangeExecutor(bc, false)
	defer executor.Close()
	return bc.insertBlockLockedWithExecutor(block, executor)
}

// insertBlockLockedWithExecutor is insertBlockLocked with a range executor that
// owns reusable StateDB, CommitScope, and StateTxRange allocation. Callers must
// hold bc.chainmu.
func (bc *BlockChain) insertBlockLockedWithExecutor(block *types.Block, executor *canonicalRangeExecutor) error {
	if block == nil {
		return errors.New("block is nil")
	}
	if bc.closed.Load() {
		return ErrBlockChainClosed
	}
	// Fork-detection runs against the range-local tip, not bc.CurrentBlock().
	// With async commit off they are identical (executor.tip() defaults to
	// bc.CurrentBlock() and the foreground advances currentBlock synchronously);
	// with async commit on the published currentBlock lags the executed tip, so
	// the duplicate / nothing-to-apply / parent-mismatch checks below must use
	// the tip the executor actually built on.
	current := bc.CurrentBlock()
	if executor != nil {
		current = executor.tip()
	}

	// Duplicate check: already committed on the canonical chain.
	if block.Number() <= current.Number() && bc.khaosDB.ContainsInMiniStore(block.Hash()) {
		return nil
	}

	// Push to KhaosDB — validates parent linkage and block number.
	// Returns the current global KhaosDB head (highest block seen across all branches).
	newHead, err := bc.khaosDB.Push(block)
	if err != nil {
		return err
	}

	// If the global KhaosDB head didn't surpass canonical, nothing to apply.
	if newHead.Number() <= current.Number() {
		return nil
	}

	// The global head advanced. If it doesn't extend the canonical tip → fork.
	if newHead.ParentHash() != current.Hash() {
		// Fully settle a deferred (session/deep) executor before the rewind so
		// switchFork sees exactly the precondition the per-call path gives it: a
		// quiesced worker and an empty range executor whose blocks are committed +
		// flushed to the same on-disk image the synchronous reorg rewinds. A
		// cross-batch InsertSession reuses ONE executor across forward batches and
		// defers the drain/flush to Finish; if a reorg interrupts mid-session that
		// executor still holds applied-but-unflushed latest-domain rows (open
		// scope) and an advanced range tip, which would leak (non-deterministically
		// with the worker's flush progress) into the re-applied branch. Replicate
		// insertBlocksLocked's async settle here: drain → Close (flush scope into
		// the committed layers) → postFlush(solidified) → Reset. No-op when the
		// executor never applied a block in this range (commit == nil) — the common
		// case where the competing branch was all nothing-to-apply until its
		// heavier tip, so the per-call path is byte-identical.
		if bc.asyncCommit && executor != nil && executor.commit != nil {
			bc.WaitForCommitSettled()
			if errPtr := bc.commitErr.Load(); errPtr != nil {
				return fmt.Errorf("async commit failed before fork: %w", *errPtr)
			}
			if err := executor.Close(); err != nil {
				return fmt.Errorf("settle executor scope before fork: %w", err)
			}
			if solidified := bc.cachedDynProps().LatestSolidifiedBlockNum(); solidified > 0 {
				if err := bc.postFlush(solidified); err != nil {
					return fmt.Errorf("flush before fork: %w", err)
				}
			}
			executor.Reset()
		}
		if err := bc.switchFork(newHead); err != nil {
			bc.khaosDB.RemoveBlk(block.Hash())
			return fmt.Errorf("switchFork: %w", err)
		}
		if executor != nil {
			executor.Reset()
		}
		return nil
	}

	// Normal linear extension: the pushed block IS the new global head.
	if executor == nil {
		executor = newCanonicalRangeExecutor(bc, false)
		defer executor.Close()
	}
	if err := executor.Apply(block); err != nil {
		bc.khaosDB.RemoveBlk(block.Hash())
		if abortErr := executor.Abort(); abortErr != nil {
			return fmt.Errorf("%w; abort range executor: %v", err, abortErr)
		}
		return err
	}
	return nil
}

// applyBlock executes, commits, and persists a single block on top of the
// current canonical tip (bc.CurrentBlock()). It updates currentBlock on success.
// Callers must hold bc.chainmu.
func (bc *BlockChain) applyBlock(block *types.Block) (retErr error) {
	executor := newCanonicalRangeExecutor(bc, false)
	defer executor.Close()
	return executor.Apply(block)
}

// headerParentChainReader pins CurrentBlock() to a specific parent block for
// header verification. Under async commit the published bc.CurrentBlock() is
// advanced off the critical path by the serial commit worker, so it lags the
// executor's range-local tip by up to one block (see range_executor.go tip()).
// Verifying a block's number / parent-hash / slot linkage against that lagging
// head spuriously rejects the 2nd+ block of an InsertBlocks range with
// ErrInvalidBlockNumber. The other ChainReader reads VerifyHeaderWithDynProps
// performs — GenesisTimestamp (immutable) and ActiveWitnesses (changes only at a
// maintenance boundary, advanced synchronously in the foreground) — are not
// worker-lagged, so they delegate to the embedded BlockChain unchanged.
type headerParentChainReader struct {
	consensus.ChainReader
	parent *types.Block
}

func (r headerParentChainReader) CurrentBlock() *types.Block { return r.parent }

// applyBlockWithPlan executes, commits, and persists one linear-extension
// block from a range-owned execution plan. If plan.state is non-nil, it must
// already represent the current canonical head's post-state; on success the
// same object represents the new head and can be reused by the next block in a
// canonicalRangeExecutor.
func (bc *BlockChain) applyBlockWithPlan(block *types.Block, plan *canonicalBlockExecution) (retErr error) {
	// The parent this block builds on is the range-local tip captured in the
	// plan, NOT bc.CurrentBlock() (which lags under async commit). Falls back to
	// the live head for callers that did not plan a parent; with async commit
	// off the two are identical.
	current := plan.parent
	if current == nil {
		current = bc.CurrentBlock()
	}
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("async buffer flush failed: %w", *errPtr)
	}
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return fmt.Errorf("async commit failed: %w", *errPtr)
	}
	historyEnabled := bc.config != nil && bc.config.HistoryEnabled
	if err := plan.Validate(block, historyEnabled); err != nil {
		return err
	}

	// When this block runs maintenance, ProcessProposals records terminal
	// proposal marks against the state it produces. If the apply then fails,
	// that state is abandoned, so the marks may no longer match committed
	// canonical state — drop the whole cache (it rebuilds lazily next boundary).
	maintenanceProcessed := false
	defer func() {
		if retErr != nil {
			// A pass memoized while executing this (now-abandoned) block stays
			// valid — the gate reads the committed-parent bitmap, which the
			// revert restores — but reset anyway to mirror proposalCache's
			// failed-apply discipline and stay robust to future mid-block
			// fork-stats writers. Cheap: the cache rebuilds lazily.
			bc.versionPassCache.Reset()
			if maintenanceProcessed {
				bc.proposalCache.reset()
			}
		}
	}()

	stats := applyStats{last: time.Now()}
	applyStart := stats.last
	defer func() {
		if retErr != nil {
			return
		}
		total := time.Since(applyStart)
		log.Trace("Block applied",
			"number", block.Number(),
			"hash", block.Hash(),
			"txs", len(block.Transactions()),
			"validate", ethcommon.PrettyDuration(stats.Validate),
			"execute", ethcommon.PrettyDuration(stats.Execute),
			"maintenance", ethcommon.PrettyDuration(stats.Maintenance),
			"stateCommit", ethcommon.PrettyDuration(stats.StateCommit),
			"stateCommitKVCompute", ethcommon.PrettyDuration(stats.StateCommitDetail.KVCompute),
			"stateCommitKVNodes", ethcommon.PrettyDuration(stats.StateCommitDetail.KVNodeWrite),
			"stateCommitDeferredKVItems", stats.StateCommitDetail.DeferredKVItems,
			"stateCommitRebuiltKVItems", stats.StateCommitDetail.RebuiltKVItems,
			"stateCommitAccountMarshal", ethcommon.PrettyDuration(stats.StateCommitDetail.AccountTrieMarshal),
			"stateCommitAccountTrieWrite", ethcommon.PrettyDuration(stats.StateCommitDetail.AccountTrieWrite),
			"stateCommitTrieCommit", ethcommon.PrettyDuration(stats.StateCommitDetail.AccountTrieCommit),
			"stateCommitTrieNodes", ethcommon.PrettyDuration(stats.StateCommitDetail.TrieNodeWrite+stats.StateCommitDetail.TrieNodeFlush),
			"dpUpdate", ethcommon.PrettyDuration(stats.DPUpdate),
			"persist", ethcommon.PrettyDuration(stats.Persist),
			"hooks", ethcommon.PrettyDuration(stats.Hooks),
			"total", ethcommon.PrettyDuration(total),
		)
		log.Debug("Block applied",
			"number", block.Number(),
			"txs", len(block.Transactions()),
			"elapsed", ethcommon.PrettyDuration(total),
		)
		// Publish the per-phase breakdown to subscribers (sync summary line,
		// metrics surface). Snapshot the hook slice under the mutex; invoke
		// without holding it so a slow subscriber can't wedge applyBlock.
		bc.applyStatsHookMu.Lock()
		hooks := bc.applyStatsHooks
		bc.applyStatsHookMu.Unlock()
		if len(hooks) > 0 {
			snap := stats.ApplyStats
			for _, h := range hooks {
				h(block, snap)
			}
		}
	}()

	// Open StateDB from parent's state root. State roots live in a side
	// store keyed by block hash, not on the block proto, so blocks coming
	// in from java-tron (which has empty account_state_root) round-trip
	// without losing wire-format identity. Genesis falls back to the
	// dedicated post-genesis-state-root key.
	//
	// This block (parentRoot + statedb + dp load) is hoisted above
	// VerifyHeader so the buffer-overlay dp can be threaded into header
	// verification, removing the redundant LoadDynamicProperties that
	// chain.DynProps() used to perform inside VerifyHeader.
	statedb := plan.state
	stagePipeline := plan.pipeline
	if err := bc.prepareOpenState(statedb); err != nil {
		return fmt.Errorf("prepare reusable state: %w", err)
	}
	bc.preloadSystemAccount(statedb)
	// HistoryEnabled on the live block path now means flat temporal domain
	// capture. The legacy SHI journal flag stays off here; StateDomainChange
	// rows below carry the per-block/per-tx history used by archive reads.

	// Load dynamic properties through the buffer so that DP keys written by
	// pending (not-yet-flushed) layers are visible to this applyBlock — e.g.
	// `current_cycle_number` advanced by an unflushed maintenance boundary
	// must be readable here. Slice 2 of the fork-rewind fix.
	//
	// Loading here (before BeginBlock) is safe: BeginBlock just stacks a new
	// empty layer; no DP writes happen until ProcessBlock runs. The load also
	// feeds VerifyHeaderWithDynProps below, replacing the redundant
	// chain.DynProps() scan that the legacy VerifyHeader entry point would
	// otherwise perform on the same buffer.
	// Async commit threads the previous block's finalized dynamic properties
	// directly into this block (decision-b): the commit worker publishes
	// dynPropsCache lazily, so reading the cache here could observe a stale,
	// not-yet-published value from several blocks back. With async off,
	// parentDynProps is nil and this reads the head cache exactly as before —
	// byte-identical.
	var dynProps *state.DynamicProperties
	if plan.parentDynProps != nil {
		dynProps = plan.parentDynProps.Copy()
	} else {
		dynProps = bc.cachedDynProps()
	}

	// Header verification (signature recovery, scheduled-witness match, and
	// post-fork timestamp alignment) runs here rather than at the top of
	// InsertBlock because:
	//   - applyBlock is the single chokepoint for state application from both
	//     linear extension and switchFork's re-apply loop;
	//   - the parent-linkage and "ts > parent.ts" checks must run against the
	//     block's true parent — the range-local executor tip captured in
	//     `current` (= plan.parent), NOT bc.CurrentBlock(). Under async commit the
	//     serial commit worker publishes bc.CurrentBlock() off the critical path,
	//     so it lags `current` by up to one block; verifying the 2nd+ block of a
	//     range against that stale head would reject it with ErrInvalidBlockNumber
	//     (the block-101 sync stall). headerParentChainReader pins CurrentBlock()
	//     to `current` — with async off the two are identical, so it is a no-op
	//     there and during a fork rewind;
	//   - bad blocks may briefly enter the KhaosDB mini-store but never reach
	//     state — KhaosDB's size bound caps the DoS surface, and the orphan is
	//     pruned by the caller on the returned error.
	// Skipped when bc.engine is nil (test path; see SetEngine).
	if bc.engine != nil {
		if err := bc.engine.VerifyHeaderWithDynProps(headerParentChainReader{bc, current}, block, dynProps); err != nil {
			return err
		}
	}
	stats.mark(&stats.Validate)

	// Open a fresh buffer layer for this block. The layer holds legacy
	// rawdb-shaped mirror writes so that switchFork can drop orphan-branch
	// layers via DiscardBlock. On any error path the active layer is discarded;
	// on success it is promoted via CommitBlock.
	bc.buffer.BeginBlock(block.Hash(), block.Number())
	if bc.cycleRewards == nil {
		bc.cycleRewards = newEmptyCycleRewardAccumulator()
	}
	rewardSnap := bc.cycleRewards.Snapshot()
	statedb.SetCycleRewardSink(bc.cycleRewards)
	defer func() {
		statedb.SetCycleRewardSink(nil)
		if retErr != nil {
			// Async commit: a foreground failure (e.g. exec of a speculative
			// block) can race in-flight commits of earlier blocks. Quiesce the
			// worker first so currentBlock/HeadStateRoot reflect every committed
			// block and their layers are promoted; only this failed block's layer
			// then remains in flight for DiscardActive to drop, and the witness
			// reload reads the correct (caught-up) head root.
			if bc.asyncCommit {
				bc.WaitForCommitSettled()
			}
			if bc.cycleRewards != nil {
				bc.cycleRewards.Restore(rewardSnap)
			}
			bc.buffer.DiscardActive()
			bc.clearSystemAccountCache()
			bc.clearWitnessBlockCache()
			bc.clearForkStatsCache()
			// SetActiveWitnesses may have mutated the in-memory atomic before
			// the failure. Its rooted write was on the now-discarded statedb
			// (never committed), so reload the atomic from the system-KV at the
			// head root — still the parent here, the state this block never
			// advanced past.
			bc.reloadActiveWitnesses(bc.HeadStateRoot())
		}
	}()
	if err := stagePipeline.Advance(rawdb.StageHeaders, rawdb.StageBodies); err != nil {
		return err
	}

	// Sapling commitment-tree lifecycle: java-tron resets CURRENT_TREE from
	// LAST_TREE before every block, then saves CURRENT_TREE as best after the
	// tx loop. Default pure-Go builds don't have the Pedersen backend needed
	// to compute roots; fail clearly once the chain can observe shielded txs
	// instead of silently producing an unusable anchor store.
	//
	// Gate the Reset/Save pair on whether shielded txs are actually possible
	// for this block — either the chain has activated AllowShieldedTransaction
	// or the block carries a shielded transfer. Pre-activation the work is
	// pure waste: GetBest() returns the empty tree (LAST_TREE has never been
	// written), Reset writes a marshalled-empty proto whose len==0 so the next
	// ReadLastMerkleTree treats it as absent again, and SaveCurrentAsBest's
	// fast-path therefore never fires — so every block was paying a cgocall
	// into librustzcash to hash an empty tree. Profile on a Nile soak showed
	// this loop burning ~20% of CPU at h≈890k. Once a proposal activates
	// shielded, the gate flips on and the regular path resumes.
	//
	// The gate is shared across both call sites here and after ProcessBlock
	// via shouldMaintainShieldedMerkleTree; drift between them would silently
	// desynchronise the LAST_TREE / MerkleTreeIndexStore density invariant.
	shieldedActive := shouldMaintainShieldedMerkleTree(dynProps, block)
	shieldedMerkleAvailable := zksnark.Available()
	if !shieldedMerkleAvailable && shieldedActive {
		return fmt.Errorf("shielded merkle tree backend unavailable: %w", zksnark.ErrPedersenUnimplemented)
	}
	if shieldedMerkleAvailable && shieldedActive {
		if err := zksnark.NewMerkleContainer(statedb).ResetCurrent(); err != nil {
			return fmt.Errorf("reset shielded merkle tree: %w", err)
		}
	}

	// Capture old-head timestamp BEFORE ProcessBlock; needed by ApplyBlockStatistics
	// to compute slot offset against the chain head as it stood pre-insert
	// (matches java-tron StatisticManager.applyBlock semantics).
	previousHeadTimestamp := current.Timestamp()
	// state_flag is 1 iff the previous applied block triggered maintenance.
	// java-tron's `lastHeadBlockIsMaintenance` reads this flag; recomputing
	// it from `previousHeadTimestamp >= NextMaintenanceTime` would always
	// be false (NextMaintenanceTime was just advanced past the prev block)
	// and miss the +MAINTENANCE_SKIP_SLOTS adjustment, over-counting
	// totalMissed once per maintenance cycle.
	prevIsMaintenance := dynProps.StateFlag() == 1

	// Process block (execute transactions, pay reward — does not commit).
	// Mutable state writes go through StateDB typed stores; bc.buffer is still
	// passed for non-rooted chain/runtime data visible during execution (TAPOS,
	// genesis witness metadata, and similar compatibility reads).
	blockRoot := block.AccountStateRoot()
	var txInfos []*corepb.TransactionInfo
	var javaAccountStateRoot tcommon.Hash
	var err error
	energyLimitForkBlockNum := bc.config.EnergyLimitForkBlockNum()
	var standbyPaySet *standbyWitnessPaySet
	if dynProps.ChangeDelegation() && dynProps.Witness127PayPerBlock() > 0 {
		standbyPaySet = bc.cachedStandbyPaySet(statedb, dynProps.CurrentCycleNumber(), dynProps.ConsensusLogicOptimization())
	}
	rewardAcctAddrs := bc.rewardAccountAddresses(block.WitnessAddress(), standbyPaySet)
	bc.preloadRewardAccounts(statedb, rewardAcctAddrs)
	defer func() {
		if retErr != nil && len(rewardAcctAddrs) > 0 {
			bc.clearRewardAccountCache()
		}
	}()
	var domainChangeStage *state.DomainChangeStage
	if historyEnabled {
		domainChangeStage, err = plan.BeginDomainChangeStage(bc.buffer)
		if err != nil {
			return fmt.Errorf("begin domain change stage: %w", err)
		}
	}
	if blockRoot != (tcommon.Hash{}) {
		parentRoot := current.AccountStateRoot()
		txInfos, javaAccountStateRoot, err = processBlock(statedb, dynProps, block, bc.vmKV(bc.buffer), bc.ActiveWitnesses(), bc.GenesisTimestamp(), energyLimitForkBlockNum, bc.engine != nil, bc.effectiveGenesisHash(), &parentRoot, standbyPaySet, domainChangeStage, bc.versionPassCache)
	} else {
		txInfos, _, err = processBlock(statedb, dynProps, block, bc.vmKV(bc.buffer), bc.ActiveWitnesses(), bc.GenesisTimestamp(), energyLimitForkBlockNum, bc.engine != nil, bc.effectiveGenesisHash(), nil, standbyPaySet, domainChangeStage, bc.versionPassCache)
	}
	if err != nil {
		return fmt.Errorf("process block: %w", err)
	}

	// Promote CURRENT_TREE to LAST_TREE + index by root + blockNum after
	// every block, matching java-tron Manager.processBlock. This keeps the
	// MerkleTreeIndexStore-equivalent dense for wallet/voucher lookups even
	// across blocks that do not append receive commitments.
	// Mirrors java-tron Manager.processBlock → MerkleContainer.saveCurrentMerkleTreeAsBestMerkleTree.
	//
	// Gated on shieldedActive for the same reason as ResetCurrent above —
	// pre-activation this loop computes the root of the empty tree (a cgocall
	// into librustzcash) every block and immediately discards it. Reuse the
	// local computed via shouldMaintainShieldedMerkleTree above; do not
	// re-inline the AllowShieldedTransaction / blockContainsShieldedTransfer
	// disjunction here — drift between the two call sites would silently
	// break the java-tron LAST_TREE / MerkleTreeIndexStore density invariant.
	if shieldedMerkleAvailable && shieldedActive {
		if err := zksnark.NewMerkleContainer(statedb).SaveCurrentAsBest(int64(block.Number())); err != nil {
			return fmt.Errorf("save shielded merkle tree: %w", err)
		}
	}

	// Drain the in-memory witness deltas (VoteCount from VoteWitness /
	// UnfreezeBalance / contract URL changes) into bc.buffer BEFORE
	// ApplyBlockStatistics. ApplyBlockStatistics reads the witness record
	// from the buffer to bump TotalProduced/TotalMissed, then writes it
	// back — so the merge order here ensures both the actuator-driven
	// VoteCount and the consensus-driven counter updates land together
	// instead of one overwriting the other. (D-2.c root-cause fix.)
	statedb.FlushWitnesses()

	// Update witness production counters + BLOCK_FILLED_SLOTS rolling window
	// (mirrors java-tron StatisticManager.applyBlock). The per-witness
	// counter writes go through bc.buffer so switchFork can rewind them on
	// reorgs (slice 1 of the fork-rewind fix). The BLOCK_FILLED_SLOTS ring is
	// updated on dynProps in-memory and, like every other consensus dynamic
	// property, lands in the rooted SystemDynamicProperty KV via
	// dynProps.FlushRooted below (before state Commit) — so it enters the
	// internal full-state root and rewinds with it on reorgs. This supersedes
	// the old fork-rewind "slice 2 / move the writer onto bc.buffer" plan
	// (docs/superpowers/specs/2026-04-30-fork-rewind-fix-design.md): the rooted
	// refactor rewinds these keys via the state root, not a buffer layer.
	dpos.ApplyBlockStatistics(statedb, dynProps, block, previousHeadTimestamp,
		bc.ActiveWitnesses(), bc.GenesisTimestamp(), prevIsMaintenance)
	stats.mark(&stats.Execute)
	if err := stagePipeline.Advance(rawdb.StageExecution); err != nil {
		return err
	}

	// Run maintenance if at boundary (before commit so allowances are included).
	wasMaintenanceBlock := false
	var maintNewWitnesses []tcommon.Address
	atBoundary := dynProps.NextMaintenanceTime() > 0 && block.Timestamp() >= dynProps.NextMaintenanceTime()
	if atBoundary {
		// java-tron parity (MaintenanceManager.applyBlock lines 62-75): when
		// the first block crosses the genesis-seeded boundary, advance
		// next_maintenance_time but skip doMaintenance entirely. Block #1
		// must NOT pay legacy standby allowance, rotate the active set, run
		// proposal processing, or accumulate cycle 0 VI — Nile's deployed
		// chain depends on the GR set staying intact through block #1 and
		// the first real maintenance running on block #2+ at the advanced
		// grid. The state flag is still set per `flag` so the next block's
		// missed-slot math sees this as a maintenance block.
		if block.Number() == 1 {
			interval := dynProps.MaintenanceTimeInterval()
			nextMaint := dpos.CalcNextMaintenanceTime(block.Timestamp(), dynProps.NextMaintenanceTime(), interval)
			dynProps.SetNextMaintenanceTime(nextMaint)
			wasMaintenanceBlock = true
		} else {
			bc.loadWitnessesIntoState(statedb)
			// Process expired proposals first — applies their parameter changes
			// to DP (or marks them CANCELED). Mirrors java-tron MaintenanceManager
			// → ConsensusService.applyBlock order: processProposals → updateWitness
			// (vote tally + active set rotation) → reward. Without this, every
			// proposal stays PENDING forever and downstream actuator / VM fork
			// gates never activate — observed empirically on a Nile soak at
			// h=860k where 4 proposals had 27 SR approvals each but `state =
			// PENDING` and `allow_creation_of_contracts = 0` (2026-05-09).
			// Per-proposal records and governance side-effects live in rooted
			// StateDB domains.
			if err := ProcessProposals(bc.buffer, statedb, dynProps, bc.ActiveWitnesses(), dynProps.NextMaintenanceTime(), bc.forkControllerForState(statedb), bc.proposalCache); err != nil {
				return fmt.Errorf("process proposals: %w", err)
			}
			// ProcessProposals may have recorded terminal marks against state
			// that this block's apply could still abandon on a later error.
			// Drop them if so (handled by the deferred reset below).
			maintenanceProcessed = true
			adapter := &chainHeaderAdapter{
				statedb:          statedb,
				dynProps:         dynProps,
				genesisWitnesses: bc.genesisWitnesses,
			}
			allWitnesses := bc.gatherWitnessVotes(statedb)
			dpos.TryRemoveThePowerOfTheGr(adapter, allWitnesses)
			// tryRemoveThePowerOfTheGr mutates witness VoteCount before the
			// java-tron reward-VI step. The earlier FlushWitnesses ran before
			// maintenance, so drain this mutation now.
			statedb.FlushWitnesses()

			// java-tron accumulates reward VI before VotesStore old/new deltas
			// are folded into WitnessStore, then snapshots cycle vote counts
			// after those deltas are applied.
			if bc.cycleRewards != nil {
				if err := bc.cycleRewards.FlushCycleToState(statedb, dynProps.CurrentCycleNumber()); err != nil {
					return fmt.Errorf("flush current-cycle rewards: %w", err)
				}
			}
			applyRewardVI(bc.buffer, statedb, dynProps)
			hasPendingVotes := applyPendingVotes(statedb)
			statedb.FlushWitnesses()
			maintNewWitnesses = bc.ActiveWitnesses()
			if hasPendingVotes {
				allWitnesses = bc.gatherWitnessVotes(statedb)

				sortOpt := dynProps.ConsensusLogicOptimization()
				sorted := dpos.SortWitnessesByVotesWithOptimization(allWitnesses, sortOpt)
				if !dynProps.ChangeDelegation() {
					dpos.DistributeLegacyStandby(adapter, sorted)
				}
				newActive := dpos.SelectActiveWitnessesWithOptimization(allWitnesses, sortOpt)
				// java-tron MaintenanceManager flips is_jobs after reward
				// distribution, before the active set is swapped — bc.ActiveWitnesses()
				// still holds the outgoing set here.
				flipWitnessIsJobs(statedb, bc.ActiveWitnesses(), newActive)
				if err := bc.SetActiveWitnesses(statedb, newActive); err != nil {
					return err
				}
				maintNewWitnesses = newActive
			}

			applyRewardCycleSnapshot(bc.buffer, statedb, dynProps)
			nextMaint := dpos.CalcNextMaintenanceTime(block.Timestamp(), dynProps.NextMaintenanceTime(), dynProps.MaintenanceTimeInterval())
			dynProps.SetNextMaintenanceTime(nextMaint)
			bc.invalidateStandbyPayCache()
			wasMaintenanceBlock = true
		}
	}
	// Record whether this block triggered maintenance; the next block will
	// read this via `dynProps.StateFlag()` to decide whether to add the
	// MAINTENANCE_SKIP_SLOTS offset when computing missed slots. java-tron
	// sets the state flag from `flag` regardless of blockNum (line 76), so
	// a block-#1 boundary still flips the flag even though doMaintenance is
	// skipped.
	if wasMaintenanceBlock {
		dynProps.SetStateFlag(1)
		// java Manager.processBlock calls forkController.reset() BEFORE
		// updateDynamicProperties, so reset's pass() check reads the PREVIOUS
		// block's timestamp (latest_block_header_timestamp is updated to this
		// block only at line 1159 below). Passing block.Timestamp() here used the
		// CURRENT block's time, so at the maintenance boundary that first crosses
		// a version's aligned hard-fork time, gtron KEPT a vote bitmap java CLEARS
		// (pass(currentTs)=true vs pass(prevTs)=false) — a ~1-cycle pass(version)
		// divergence at activation boundaries (e.g. exchange's pass(33)).
		bc.forkControllerForState(statedb).Reset(dynProps.LatestBlockHeaderTimestamp(), dynProps.MaintenanceTimeInterval(), len(bc.ActiveWitnesses()))
	} else {
		dynProps.SetStateFlag(0)
	}
	stats.mark(&stats.Maintenance)

	// Verify java-tron's header accountStateRoot, if present, before committing
	// the internal StateDB root. The two roots intentionally use different
	// domains; only the adapter knows how to interpret the wire root.
	if err := defaultStateRootAdapter.ValidateJavaAccountStateRoot(blockRoot, javaAccountStateRoot); err != nil {
		return err
	}

	// Update dynamic properties and fork-vote state before Commit. Non-derived
	// dynamic properties and fork votes enter the full internal state root;
	// head-pointer DP keys plus replay-derived TAPOS/metric rows are staged
	// into the active buffer below and intentionally stay outside the root.
	dynProps.SetLatestBlockHeaderNumber(int64(block.Number()))
	dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dynProps.SetLatestBlockHeaderHash(block.Hash())
	bc.updateSolidifiedBlock(statedb, block.WitnessAddress(), int64(block.Number()), dynProps)
	bc.updateFork(statedb, block)
	if err := rawdb.WriteTaposRef(bc.buffer, block.Number(), block.Hash()); err != nil {
		return fmt.Errorf("stage tapos ref: %w", err)
	}
	// Stage the block body alongside the TAPOS row so BLOCKHASH lookups from
	// the NEXT blocks of an insert range resolve through the buffer. The
	// durable b-<num> row is written by writeBlockMetadataBatch, but under
	// async commit that batch runs on the commit worker, so block N+1's
	// execution raced it and read blockhash(N) as 0 while java-tron always
	// serves the parent hash — Nile 10,552,292 stalled exactly here (OneSwap
	// derives limit-order ids from blockhash(block.number-1) ^ tx.origin, so
	// the zero hash silently diverged the order book at placement). Layered
	// staging keeps the row fork-rewindable, like the TAPOS ref above.
	if err := rawdb.WriteBlock(bc.buffer, block); err != nil {
		return fmt.Errorf("stage block body: %w", err)
	}
	if n := len(block.Transactions()); n > 0 {
		count := rawdb.ReadTotalTransactionCount(bc.buffer)
		rawdb.WriteTotalTransactionCount(bc.buffer, count+int64(n))
	}

	// Stage non-derived dynamic properties into the system account's KV BEFORE
	// Commit so they enter the internal full-state root (and thus rewind with it).
	if err := dynProps.FlushRooted(statedb); err != nil {
		return fmt.Errorf("flush rooted dynamic properties: %w", err)
	}
	if domainChangeStage != nil {
		if err := domainChangeStage.FlushFinal(); err != nil {
			return fmt.Errorf("flush block-final domain changes: %w", err)
		}
	}

	// dynProps is now finalized for this block. Carry it forward so the next
	// block in an async range threads it directly (decision-b) instead of
	// reading the lazily-published dynPropsCache.
	plan.finalDynProps = dynProps

	// Commit state (includes both tx execution and maintenance changes).
	commitOpts := bc.stateCommitOptions(block, wasMaintenanceBlock)

	// Async commit: hand the fold + ordered publish tail to the serial commit
	// worker so it overlaps the next block's execution. The foreground has
	// finished every shared step (exec, maintenance, rooted-DP flush, TAPOS/tx
	// count) and the latest-domain rows are written into the in-memory scope;
	// only the commitment fold and the publish tail remain, and they are
	// severable (see commitAsync).
	//
	// Gated on plan.commit != nil — i.e. the shared-commit RANGE path
	// (InsertBlocks / fork re-apply), which reuses one executor so the previous
	// block's finalized dynProps can be threaded forward (decision-b). The
	// single-block path (producer, gossip, restart) builds a fresh executor per
	// block with no DP to thread, and is out of the async scope (bulk sync only);
	// it falls through to the synchronous commit below. With async off this guard
	// is skipped entirely and the synchronous commit runs unchanged —
	// byte-identical.
	if bc.asyncCommit && plan.commit != nil {
		return bc.commitAsync(block, plan, statedb, dynProps, &stats, commitOpts, wasMaintenanceBlock, maintNewWitnesses, rewardAcctAddrs, txInfos)
	}

	commitResult, err := plan.CommitState(bc.buffer, block, commitOpts, bc.config.StateCommitmentCheckpoints)
	if err != nil {
		return err
	}
	newRoot := commitResult.Root
	stats.StateCommitDetail = commitResult.Stats
	bc.updateSystemAccountCache(statedb)
	bc.updateRewardAccountCache(statedb, rewardAcctAddrs)

	stats.mark(&stats.StateCommit)

	// Mirror the derived runtime DP keys to the flat dp- store. dynProps.Flush
	// writes ONLY the four derived keys (latest_block_header_number/timestamp/
	// hash and latest_solidified_block_num) for startup, crash recovery, and
	// diagnostic point reads. Every non-derived DP key — block_filled_slots,
	// burn_trx_amount, total_create_witness_cost, maintenance-touched keys, etc.
	// — was already staged into the rooted SystemDynamicProperty KV by
	// FlushRooted above.
	dynProps.Flush(bc.buffer)
	if bc.cycleRewards != nil {
		if err := bc.cycleRewards.Write(bc.buffer); err != nil {
			return fmt.Errorf("stage cycle reward pending accumulator: %w", err)
		}
	}

	stats.mark(&stats.DPUpdate)

	// Record this block in the TAPOS recent-block ring so future txs can
	// reference it. java-tron's Manager.updateRecentBlock runs unconditionally
	// at the head of pushBlockInner; doing it here (after the block is fully
	// committed) preserves the same observable ordering: the next block's
	// txs see this block's slot, and a fork-rewind that discards this block
	// will write a different value into the same slot when the alternate
	// branch's block #N applies — overwrite, not delete, matches java's
	// ring semantics.
	if err := bc.writeBlockMetadataBatch(block, newRoot, txInfos); err != nil {
		return err
	}
	rawdb.WriteHeadBlockHash(bc.buffer, block.Hash())

	// Publish the new head only after all metadata needed by readers
	// (block body, out-of-band state root, TAPOS, tx infos, tx indexes) has
	// landed in one durable batch.
	bc.currentBlock.Store(block)
	bc.lastInsertNano.Store(time.Now().UnixNano())

	bc.storeDynPropsCache(dynProps)
	stats.mark(&stats.Persist)

	// Fire maintenance hooks first so the SRL PBFT message goes out before
	// the block PREPREPARE — matches java-tron MaintenanceManager.applyBlock
	// ordering (srPrePrepare at line 72, blockPrePrepare at line 81). Block
	// #1 advances the grid but doesn't run doMaintenance, so java skips
	// srPrePrepare too (line 70 guard `if (blockNum != 1)`).
	if wasMaintenanceBlock && block.Number() != 1 {
		bc.maintHookMu.Lock()
		mhooks := bc.maintHooks
		bc.maintHookMu.Unlock()
		for _, h := range mhooks {
			h(block, maintNewWitnesses)
		}
	}

	bc.blockHookMu.Lock()
	hooks := bc.blockHooks
	bc.blockHookMu.Unlock()
	for _, h := range hooks {
		h(block)
	}
	stats.mark(&stats.Hooks)
	if err := stagePipeline.Advance(rawdb.StageFinish); err != nil {
		return err
	}

	// Promote the buffer layer to the layered stack. Slice 1 introduced the
	// layered stack; slice 2 adds the flush-at-solidified policy below.
	bc.buffer.CommitBlock()

	// Hand every layer at or below the new solidified-block number to the
	// async flusher. Layers above solidified stay in memory and remain
	// rewindable via switchFork's DiscardBlock. Mirrors java-tron's
	// invariant that Manager.eraseBlock can never pop past solidified.
	// Cap the flush cutoff at the newest COMMITTED layer. In normal forward
	// application solidified <= the just-committed block, so this is a no-op.
	// But switchFork re-applies a branch while dynProps can still report the
	// pre-reorg solidified (a high-water mark that may exceed the freshly
	// re-applied block); an uncapped cutoff lets the async flush worker — running
	// block N-1's postFlush(solidified) — drop block N's just-committed layer
	// before block N's own FlushLatestUpTo flushes its scope op into it, orphaning
	// that op → "batch target layer is no longer pending". Capping at the newest
	// committed layer keeps each postFlush from dropping the current block's
	// layer. (Same flush-cutoff invariant the depth>2 path enforces via
	// NewestCommittedNumber.)
	cutoff := dynProps.LatestSolidifiedBlockNum()
	if nc, ok := bc.buffer.NewestCommittedNumber(); ok && int64(nc) < cutoff {
		cutoff = int64(nc)
	}
	if err := plan.FlushLatestUpTo(cutoff); err != nil {
		return err
	}
	if err := bc.postFlush(cutoff); err != nil {
		return err
	}
	stats.mark(&stats.Persist)
	return nil
}

type stateTxRangeAllocator struct {
	enabled        bool
	parentEndTxNum uint64
}

func (bc *BlockChain) newStateTxRangeAllocator(parentBlockNum uint64) (*stateTxRangeAllocator, error) {
	if bc.config == nil || !bc.config.HistoryEnabled {
		return &stateTxRangeAllocator{}, nil
	}
	if bc.stateTxRangeSeedHook != nil {
		bc.stateTxRangeSeedHook(parentBlockNum)
	}
	parentEndTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(bc.buffer, parentBlockNum)
	if err != nil {
		return nil, fmt.Errorf("read state tx range seed at block %d: %w", parentBlockNum, err)
	}
	return &stateTxRangeAllocator{
		enabled:        true,
		parentEndTxNum: parentEndTxNum,
	}, nil
}

func (a *stateTxRangeAllocator) next(block *types.Block) (*rawdb.StateTxRange, error) {
	if a == nil || !a.enabled || block == nil {
		return nil, nil
	}
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(a.parentEndTxNum, uint64(len(block.Transactions())))
	if err != nil {
		return nil, err
	}
	a.parentEndTxNum = endTxNum
	return &rawdb.StateTxRange{
		BlockNum:   block.Number(),
		BlockHash:  block.Hash(),
		BeginTxNum: beginTxNum,
		EndTxNum:   endTxNum,
	}, nil
}

func (bc *BlockChain) writeBlockMetadataBatch(block *types.Block, stateRoot tcommon.Hash, txInfos []*corepb.TransactionInfo) error {
	batch := bc.db.NewBatch()

	// The root is persisted out-of-band — we do NOT mutate
	// `block.AccountStateRoot()` because the block proto's content must
	// round-trip byte-identical to what the wire delivered.
	if err := rawdb.WriteBlockStateRoot(batch, block.Hash(), stateRoot); err != nil {
		return fmt.Errorf("write block state root: %w", err)
	}
	if err := rawdb.WriteBlock(batch, block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	if err := rawdb.WriteTaposRef(batch, block.Number(), block.Hash()); err != nil {
		return fmt.Errorf("write tapos ref: %w", err)
	}
	for _, info := range txInfos {
		if err := rawdb.WriteTransactionInfo(batch, info.Id, info); err != nil {
			return fmt.Errorf("write tx info: %w", err)
		}
	}
	if err := rawdb.WriteTransactionInfosByBlock(batch, block.Number(), txInfos); err != nil {
		return fmt.Errorf("write block tx infos: %w", err)
	}
	for _, tx := range block.Transactions() {
		h := tx.Hash()
		if err := rawdb.WriteTransactionIndex(batch, h[:], block.Number()); err != nil {
			return fmt.Errorf("write tx index: %w", err)
		}
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write block metadata batch: %w", err)
	}
	return nil
}

// flushBufferUpToSolidified drains every committed buffer layer whose block
// number is <= solidified to bc.db (oldest-first), then drops those layers
// from the buffer. Higher layers remain in memory and rewindable on
// switchFork. Slice 2 of the fork-rewind fix.
//
// On a single-SR chain (and many small testnets) solidified == head, so
// every block flushes immediately. On mainnet (27 SRs) solidified lags
// head by at least 19 blocks, giving switchFork plenty of headroom.
func (bc *BlockChain) flushBufferUpToSolidified(solidified int64) error {
	if solidified <= 0 {
		return nil
	}
	return bc.buffer.FlushUpTo(uint64(solidified), bc.db)
}

// startFlushWorker spawns the background goroutine that drains flushQueue
// and runs flushBufferUpToSolidified off the chainmu critical path. Called
// once at the end of NewBlockChain — strictly after every error-returning
// step, so a constructor failure can never leak this goroutine. Close
// drives a graceful shutdown by closing the channel and joining
// flushWorkerWg.
func (bc *BlockChain) startFlushWorker() {
	bc.flushWorkerWg.Add(1)
	go func() {
		defer bc.flushWorkerWg.Done()
		for cutoff := range bc.flushQueue {
			bc.runFlushCutoff(cutoff)
		}
	}()
}

// runFlushCutoff is the body of one flush iteration shared by the worker
// loop and the inline fallback. It runs the flush, records a fail-fast
// error (first one wins), and decrements the pending WaitGroup.
//
// Errors are not panicked or logged-and-swallowed here: the next applyBlock
// surfaces flushErr at its top, matching the severity of an inline error.
// We still log so operators see the failure when it happens, not only when
// the next block tries to advance.
func (bc *BlockChain) runFlushCutoff(cutoff uint64) {
	defer bc.flushPending.done()
	if err := bc.flushBufferUpToSolidified(int64(cutoff)); err != nil {
		wrapped := fmt.Errorf("flush buffer up to solidified: %w", err)
		// First failure wins. A later flush attempt that also fails (e.g.
		// because the underlying store is gone) doesn't displace the
		// original — the original is the actionable signal for operators
		// and applyBlock callers.
		bc.flushErr.CompareAndSwap(nil, &wrapped)
		log.Error("Async buffer flush failed", "cutoff", cutoff, "err", err)
	}
}

// postFlush hands the cutoff to the async worker. The non-blocking send
// is the fast path — empty channel, single sync.WaitGroup.Add(1). If the
// queue is full (worker backlog under sustained load) the call falls back
// to a synchronous flush so we never lose a cutoff.
//
// Callers hold chainmu (applyBlock only). The fail-fast check at the top
// of applyBlock catches errors raised on either path; for the inline-
// fallback path we additionally surface the error to the caller so the
// current applyBlock unwinds (same observable behaviour as the pre-async
// inline flush).
func (bc *BlockChain) postFlush(solidified int64) error {
	if solidified <= 0 {
		return nil
	}
	cutoff := uint64(solidified)
	bc.flushPending.post()
	if bc.flushClosed || bc.flushQueue == nil {
		bc.runFlushCutoff(cutoff)
		if errPtr := bc.flushErr.Load(); errPtr != nil {
			return *errPtr
		}
		return nil
	}
	select {
	case bc.flushQueue <- cutoff:
		return nil
	default:
		// Queue full: drop back to sync flush. runFlushCutoff balances
		// the Add(1) above via its deferred Done().
		bc.runFlushCutoff(cutoff)
		if errPtr := bc.flushErr.Load(); errPtr != nil {
			return *errPtr
		}
		return nil
	}
}

// WaitForFlushSettled blocks until every in-flight async-flush cutoff
// posted by applyBlock has finished draining to disk.
//
// Call this only when you need synchronous disk-side visibility — typically
// tests, or production observers that read bc.DB() directly (e.g. a CLI
// dump, an external indexer that opened the same Pebble store read-only,
// or a graceful-shutdown path that needs the on-disk image to reflect
// every applied block before swapping in a new database handle).
//
// Production code reading through bc.buffer / bc.DynProps() /
// bc.BufferedDPInt64() / other accessors that consult the buffer overlay
// does NOT need this — the overlay serves the latest applied state
// regardless of flush state. See the InsertBlock godoc for the precise
// return-time guarantees.
//
// Cheap to call when the queue is empty (one mutex acquire on a zero
// counter). Close drives the same drain plus a final synchronous flush,
// so a Close caller does not need to call WaitForFlushSettled first.
func (bc *BlockChain) WaitForFlushSettled() {
	bc.flushPending.wait()
}

// Close performs a graceful shutdown of the BlockChain: it drains the async
// flush worker, flushes every pending buffer layer to disk, and persists the
// head pointer. Because the head pointer, applied dynamic properties, and the
// commitment root all live in the same buffer layer and flush atomically per
// layer, the on-disk image is self-consistent at the head block after Close —
// and equally self-consistent at the last async-flushed block after a crash,
// so startup can trust the persisted head without any state rebuild.
func (bc *BlockChain) Close() error {
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Swap(true) {
		return nil
	}
	// Drain the commit worker first: each completed commit posts a solidified
	// flush, so commits must settle before we wait on the flush queue.
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	bc.stopCommitWorkerLocked()
	bc.stopFlushWorkerLocked()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return fmt.Errorf("close: async commit failed: %w", *errPtr)
	}
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("close: async buffer flush failed: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return fmt.Errorf("close: flush pending buffer: %w", err)
	}
	bc.buffer.Discard()
	if err := bc.stateDB.Close(); err != nil {
		return fmt.Errorf("close: state trie database: %w", err)
	}
	if head := bc.CurrentBlock(); head != nil {
		rawdb.WriteHeadBlockHash(bc.db, head.Hash())
		if err := syncKeyValueStore(bc.db); err != nil {
			return fmt.Errorf("close: sync head: %w", err)
		}
		log.Info("Clean shutdown",
			"head", head.Number(), "root", rawdb.ReadBlockStateRoot(bc.chaindb, head.Hash()))
	}
	return nil
}

// stopFlushWorkerLocked closes the async worker channel and joins the worker.
// Callers must hold chainmu and must have waited for pending flushes first, so
// no producer can be racing a send into the channel.
func (bc *BlockChain) stopFlushWorkerLocked() {
	if bc.flushQueue != nil && !bc.flushClosed {
		close(bc.flushQueue)
		bc.flushClosed = true
	}
	bc.flushWorkerWg.Wait()
}

// switchFork rewinds the canonical chain to the LCA of newHead and the current
// tip, then re-applies the new branch on top of LCA state.
// Callers must hold bc.chainmu.
func (bc *BlockChain) switchFork(newHead *types.Block) error {
	// Drain the async commit worker before anything else: an in-flight commit
	// still owns an uncommitted buffer layer (it folds + publishes block N),
	// and currentBlock/HeadStateRoot may still be lagging the executed tip.
	// Waiting here quiesces the worker so every executed block is committed and
	// its layer promoted before the rewind reads the tip and discards orphan
	// layers. Mirrors the flush-drain rationale below. Safe (no deadlock): the
	// worker takes only the buffer's internal mu and the barriers, never chainmu.
	bc.WaitForCommitSettled()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return fmt.Errorf("async commit failed before fork rewind: %w", *errPtr)
	}
	// Drain any in-flight async flushes before rewinding buffer layers.
	// Without this, the worker may still be holding solidified-but-not-yet-
	// flushed layers in bc.buffer when DiscardBlock runs; DiscardBlock would
	// pop them, silently losing finalised state — violating the "forks must
	// not pop past solidified" invariant the synchronous flush used to
	// enforce by emptying those layers out of the buffer before applyBlock
	// returned. We wait on the chainmu-holding caller's path; the flush
	// worker holds only the buffer's internal mu, so this can't deadlock.
	bc.flushPending.wait()

	current := bc.CurrentBlock()
	if current == nil {
		return errors.New("current block is nil")
	}
	currentHash := current.Hash()
	if err := verifyCanonicalStagePipelineHead(bc.buffer, current.Number(), currentHash); err != nil {
		return fmt.Errorf("verify canonical stage head before fork rewind: %w", err)
	}
	newBranch, oldBranch, err := bc.khaosDB.GetBranch(newHead.Hash(), currentHash)
	if err != nil {
		// Can't find LCA: discard the entire new branch from KhaosDB.
		tmp := newHead
		for tmp != nil {
			bc.khaosDB.RemoveBlk(tmp.Hash())
			tmp = bc.khaosDB.GetBlock(tmp.ParentHash())
		}
		return err
	}

	// Determine LCA block hash.
	var lcaHash tcommon.Hash
	if len(oldBranch) == 0 {
		// newHead is a direct descendant of currentHash (shouldn't reach switchFork,
		// but handle defensively).
		lcaHash = currentHash
	} else {
		lcaHash = oldBranch[len(oldBranch)-1].ParentHash()
	}

	// Drop the buffer layers belonging to orphan-branch blocks. These were
	// laid down by earlier applyBlock calls (linear extensions) and contain
	// the rawdb-direct writes (slice 1: witness statistics) that must be
	// rewound before re-applying the new branch. DiscardBlock is a no-op
	// for hashes not present in the buffer, which covers the deeper shared
	// prefix above the LCA.
	for _, kb := range oldBranch {
		bc.buffer.DiscardBlock(kb.block.Hash())
	}

	// Rewind consensus caches to the LCA state. currentBlock is still the
	// pre-switch head here, so compute the LCA root explicitly rather than
	// relying on HeadStateRoot(). Both the active witness list (Phase 3c) and
	// the rooted dynprops live in the system-KV at the LCA root.
	lcaRoot := rawdb.ReadBlockStateRoot(bc.chaindb, lcaHash)
	if lcaRoot == (tcommon.Hash{}) {
		if n := rawdb.ReadBlockNumber(bc.chaindb, lcaHash); n != nil && *n == 0 {
			lcaRoot = rawdb.ReadGenesisStateRoot(bc.db)
		}
	}
	// An orphan-branch maintenance block may have called SetActiveWitnesses,
	// mutating the in-memory atomic. Its rooted write was dropped with the
	// orphan branch's abandoned state — reload the atomic from the system-KV at
	// the LCA root so the active set rewinds with the rest of consensus state
	// before the new branch is re-applied. (Without this the active set stays
	// stale even though witness is_jobs and DP correctly rewound.)
	bc.reloadActiveWitnesses(lcaRoot)
	bc.reloadDynPropsCache(lcaRoot)
	if err := bc.reloadCycleRewardsFromBuffer(); err != nil {
		return fmt.Errorf("reload cycle reward pending accumulator: %w", err)
	}
	bc.invalidateStandbyPayCache()
	bc.clearSystemAccountCache()
	bc.clearRewardAccountCache()
	bc.clearWitnessBlockCache()
	bc.clearForkStatsCache()
	// A rewound proposal may revert from terminal back to PENDING on the new
	// branch; drop the node-local terminal-skip cache so the re-applied branch
	// re-reads proposal state from scratch.
	bc.proposalCache.reset()
	// Likewise a reorg can rewind below a version's activation block, reverting
	// it to pending on the new branch; drop the fork-pass memo so the re-applied
	// branch re-evaluates each version from live fork-stats. See forks.VersionPassCache.
	bc.versionPassCache.Reset()

	var lcaBlock *types.Block
	numPtr := rawdb.ReadBlockNumber(bc.chaindb, lcaHash)
	if numPtr != nil {
		lcaBlock = rawdb.ReadBlock(bc.chaindb, *numPtr)
	}
	if lcaBlock == nil {
		return fmt.Errorf("LCA block %x not found in DB", lcaHash)
	}

	// Rewind currentBlock to LCA so that applyBlock reads the correct parent root.
	bc.currentBlock.Store(lcaBlock)
	if err := rewindCanonicalStagePipeline(bc.db, lcaBlock.Number(), lcaBlock.Hash()); err != nil {
		return fmt.Errorf("rewind canonical stage progress to LCA %d: %w", lcaBlock.Number(), err)
	}

	// Apply new branch blocks in order LCA+1 → newHead.
	reversed := make([]*types.Block, len(newBranch))
	for i, kb := range newBranch {
		reversed[len(newBranch)-1-i] = kb.block
	}
	forkExecutor := newCanonicalRangeExecutor(bc, true)
	if len(reversed) > 0 {
		for _, b := range reversed {
			if err := forkExecutor.Apply(b); err != nil {
				// Remove orphaned new-branch blocks from KhaosDB.
				for _, kb := range newBranch {
					bc.khaosDB.RemoveBlk(kb.block.Hash())
				}
				if bc.asyncCommit {
					bc.WaitForCommitSettled()
					// A concurrent worker commit failure may be the root cause of
					// the foreground apply error; surface it so the caller sees the
					// real reason rather than a downstream symptom.
					if errPtr := bc.commitErr.Load(); errPtr != nil {
						forkExecutor.Reset()
						return fmt.Errorf("apply fork block %d: %w; async commit failed: %v", b.Number(), err, *errPtr)
					}
				}
				forkExecutor.Reset()
				return fmt.Errorf("apply fork block %d: %w", b.Number(), err)
			}
		}
	}
	// Drain the commit worker before closing the fork executor's scope, so the
	// re-applied branch is fully committed (head/root caught up) and the scope
	// FlushLatest below sees a quiesced buffer. switchFork must return a settled
	// canonical state, matching the synchronous re-apply it replaces.
	if bc.asyncCommit {
		bc.WaitForCommitSettled()
		if errPtr := bc.commitErr.Load(); errPtr != nil {
			forkExecutor.Reset()
			return fmt.Errorf("async commit failed during fork re-apply: %w", *errPtr)
		}
	}
	return forkExecutor.Close()
}

// LastInsertTime returns when the last block was successfully inserted.
func (bc *BlockChain) LastInsertTime() time.Time {
	return time.Unix(0, bc.lastInsertNano.Load())
}

// StateDB returns the state database for reading state.
func (bc *BlockChain) StateDB() *state.Database {
	return bc.stateDB
}

// StateRootAtBlock returns the post-apply state root for the block at the
// given number, or the zero hash if either the block or its state root is
// missing. Used by the solid / PBFT HTTP variants to open StateDB at the
// solid / PBFT-confirmed head rather than the live head — without this,
// /walletsolidity/getaccount returns live (possibly-reorgable) balances,
// which is the bug the audit's "Solidity API isolation" P1 called out.
func (bc *BlockChain) StateRootAtBlock(num uint64) tcommon.Hash {
	block := bc.GetBlockByNumber(num)
	if block == nil {
		return tcommon.Hash{}
	}
	if root := rawdb.ReadBlockStateRoot(bc.chaindb, block.Hash()); root != (tcommon.Hash{}) {
		return root
	}
	if num == 0 {
		return rawdb.ReadGenesisStateRoot(bc.db)
	}
	// Backwards-compat fallback for chain databases written before
	// blockStateRootPrefix existed; matches HeadStateRoot's behaviour.
	return block.AccountStateRoot()
}

// HeadStateRoot returns the post-apply state root of the canonical head
// block. The block proto itself no longer carries `account_state_root`
// (java-tron parity), so callers that want to open a StateDB at head
// must use this helper rather than `block.AccountStateRoot()`.
func (bc *BlockChain) HeadStateRoot() tcommon.Hash {
	head := bc.CurrentBlock()
	if root := rawdb.ReadBlockStateRoot(bc.chaindb, head.Hash()); root != (tcommon.Hash{}) {
		return root
	}
	if head.Number() == 0 {
		return rawdb.ReadGenesisStateRoot(bc.db)
	}
	// Backwards-compat fallback for chain databases written before
	// blockStateRootPrefix existed.
	return head.AccountStateRoot()
}

// DB returns the underlying key-value store.
func (bc *BlockChain) DB() ethdb.KeyValueStore {
	return bc.db
}

// ChainDB returns the composite chain database (hot KV store + ancient
// reader). Callers that need to read chain accessors migrated by the
// slice-2 freezer work (`rawdb.ReadBlock`, `rawdb.ReadBlockNumber`,
// `rawdb.ReadTransactionInfo*`, `rawdb.ReadBlockStateRoot`) should use
// this handle so frozen blocks fall through to the ancient store.
//
// Reads on the freezer pass under no chain mutex — the freezer is
// append-only and threadsafe. Slice-2 ships with a `NoopAncient` reader,
// so behavior is byte-identical to a plain KV until slice 3 attaches a
// real `*freezer.Freezer`.
func (bc *BlockChain) ChainDB() *rawdb.ChainDB {
	return bc.chaindb
}

// BufferedDB returns a read-only view that consults the in-memory applyBlock
// buffer first, then falls through to the disk store. Legacy rawdb-shaped
// mirrors can remain buffered until their block is solidified, so non-rooted
// compatibility readers use this view to observe the canonical head.
func (bc *BlockChain) BufferedDB() ethdb.KeyValueReader {
	return bc.buffer
}

// BufferedDPInt64 reads a single DynamicProperties int64 key through the
// in-memory buffer overlay so DP changes from prior blocks not yet flushed
// to disk are visible. Falls back to state.DefaultDPInt64 when the key is
// absent in both buffer and disk, mirroring the per-key branch in
// state.LoadDynamicProperties.
//
// Hot callers (PBFT BlockHook) use this in place of a bare
// rawdb.ReadDynamicProperty(bc.db, ...) so they see the just-applied block's
// DP writes. It reads the in-memory head snapshot (refreshed each block under
// chainmu), which is as fresh as the buffer AND carries the rooted keys that
// Phase 3b moved out of the flat dp- store into the system-account KV — a
// dp- point read would now miss every rooted key (allow_pbft,
// next_maintenance_time, …) and silently return the default. The stored
// snapshot is immutable after storage, so the no-copy Get is race-free.
func (bc *BlockChain) BufferedDPInt64(name string) int64 {
	if v := bc.dynPropsCache.Load(); v != nil {
		if dp, ok := v.(*state.DynamicProperties); ok && dp != nil {
			if val, found := dp.Get(name); found {
				return val
			}
		}
	}
	def, _ := state.DefaultDPInt64(name)
	return def
}

// ActiveWitnesses returns the current active witness list.
func (bc *BlockChain) ActiveWitnesses() []tcommon.Address {
	v := bc.activeWitnesses.Load()
	if v == nil {
		return nil
	}
	return v.([]tcommon.Address)
}

// SetActiveWitnesses updates the active witness list in memory and stages it
// into the rooted system-KV on statedb (Phase 3c). java-tron keeps the active
// set in a revoking store (WitnessScheduleStore extends TronStoreWithRevoking),
// so it must rewind with the rest of consensus state on a fork rewind across a
// maintenance boundary; rooting it into the block's state root gives that
// rewindability plus historical-height recovery. The in-memory atomic is the
// fast read path for ActiveWitnesses(); switchFork and the applyBlock error
// defer reload it from the system-KV at the rewound root.
//
// The sole production caller is applyBlock's maintenance branch, with statedb
// open at the block being applied (the write is committed by applyBlock's
// statedb.Commit, alongside the rooted dynprops).
func (bc *BlockChain) SetActiveWitnesses(statedb *state.StateDB, witnesses []tcommon.Address) error {
	bc.activeWitnesses.Store(witnesses)
	return statedb.WriteActiveWitnesses(witnesses)
}

// SetActiveWitnessesForTest overwrites the in-memory active-witness atomic only
// (no rooted write). TEST-ONLY: lets tests stage an active set without applying
// a real block, mirroring SetDynPropsCacheForTest.
func (bc *BlockChain) SetActiveWitnessesForTest(witnesses []tcommon.Address) {
	bc.activeWitnesses.Store(witnesses)
}

// reloadActiveWitnesses refreshes the in-memory active-witness atomic from the
// rooted system-KV at the given state root. Called after a rewind (switchFork's
// DiscardBlock loop or the applyBlock error defer) so the atomic — which an
// orphan-branch SetActiveWitnesses mutated — falls back to the rewound state. A
// nil sysKV (zero root) or empty result leaves the atomic untouched.
func (bc *BlockChain) reloadActiveWitnesses(rootAt tcommon.Hash) {
	sysKV := bc.sysKVAt(rootAt)
	if sysKV == nil {
		return
	}
	if reloaded := sysKV.ReadActiveWitnesses(); reloaded != nil {
		bc.activeWitnesses.Store(reloaded)
	}
}

// sysKVAt opens a StateDB at the given state root so rooted dynamic properties
// can be read from the system account's KV. Returns nil for the zero root or if
// the state can't be opened — callers then fall back to rooted defaults.
func (bc *BlockChain) sysKVAt(root tcommon.Hash) *state.StateDB {
	if root == (tcommon.Hash{}) {
		return nil
	}
	sysKV, err := bc.openState(root)
	if err != nil {
		return nil
	}
	return sysKV
}

func (bc *BlockChain) openState(root tcommon.Hash) (*state.StateDB, error) {
	if bc.stateOpenHook != nil {
		bc.stateOpenHook(root)
	}
	statedb, err := state.New(root, bc.stateDB)
	if err != nil {
		return nil, err
	}
	if err := bc.prepareOpenState(statedb); err != nil {
		return nil, err
	}
	return statedb, nil
}

func (bc *BlockChain) openCurrentState() (*state.StateDB, error) {
	current := bc.CurrentBlock()
	root := rawdb.ReadBlockStateRoot(bc.chaindb, current.Hash())
	if root == (tcommon.Hash{}) && current.Number() == 0 {
		root = rawdb.ReadGenesisStateRoot(bc.db)
	}
	if root == (tcommon.Hash{}) {
		// Backwards-compat fallback for chain databases written before
		// blockStateRootPrefix existed.
		root = current.AccountStateRoot()
	}
	return bc.openState(root)
}

func (bc *BlockChain) prepareOpenState(statedb *state.StateDB) error {
	if err := bc.configureStateCodeColdHistory(statedb); err != nil {
		return err
	}
	statedb.SetAccountKVIndexStore(bc.buffer)
	statedb.SetAccountKVIndexReads(true)
	return nil
}

func (bc *BlockChain) configureStateCodeColdHistory(statedb *state.StateDB) error {
	if statedb == nil || (bc.stateCodeColdHistory == nil && bc.stateCommitmentColdHistory == nil) {
		return nil
	}
	head := bc.CurrentBlock()
	if head == nil {
		return nil
	}
	txNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(bc.db, head.Number())
	if err != nil {
		return err
	}
	if bc.stateCodeColdHistory != nil {
		statedb.SetCodeColdHistory(bc.stateCodeColdHistory, txNum)
	}
	if bc.stateCommitmentColdHistory != nil {
		statedb.SetCommitmentColdHistory(bc.stateCommitmentColdHistory, txNum)
	}
	return nil
}

func (bc *BlockChain) cachedDynProps() *state.DynamicProperties {
	if v := bc.dynPropsCache.Load(); v != nil {
		if dp, ok := v.(*state.DynamicProperties); ok && dp != nil {
			return dp.Copy()
		}
	}
	return state.LoadDynamicProperties(bc.buffer, bc.sysKVAt(bc.HeadStateRoot()))
}

func (bc *BlockChain) storeDynPropsCache(dp *state.DynamicProperties) {
	if dp != nil {
		bc.dynPropsCache.Store(dp)
	}
}

// SetDynPropsCacheForTest overwrites the in-memory dynamic-properties head
// snapshot. TEST-ONLY: production refreshes the cache through applyBlock; this
// lets participation / maintenance tests stage a specific DP value (including
// rooted keys that no longer live in flat dp-) without applying real blocks.
func (bc *BlockChain) SetDynPropsCacheForTest(dp *state.DynamicProperties) {
	bc.storeDynPropsCache(dp)
}

// reloadDynPropsCache rebuilds the head dynprops snapshot after a fork rewind.
// rootAt is the LCA state root: derived keys come from the (already rewound)
// buffer, rooted keys from the system-KV at that root. It must be passed
// explicitly because the caller rewinds currentBlock AFTER this runs, so
// HeadStateRoot() would still point at the pre-switch head.
func (bc *BlockChain) reloadDynPropsCache(rootAt tcommon.Hash) {
	bc.storeDynPropsCache(state.LoadDynamicProperties(bc.buffer, bc.sysKVAt(rootAt)))
}

func (bc *BlockChain) reloadCycleRewardsFromBuffer() error {
	acc, err := newCycleRewardAccumulator(bc.buffer)
	if err != nil {
		return err
	}
	bc.cycleRewards = acc
	return nil
}

func (bc *BlockChain) effectiveGenesisHash() tcommon.Hash {
	if bc.config != nil && bc.config.GenesisHash != (tcommon.Hash{}) {
		return bc.config.GenesisHash
	}
	if bc.genesisBlock != nil {
		return bc.genesisBlock.Hash()
	}
	return tcommon.Hash{}
}

func (bc *BlockChain) cachedStandbyPaySet(statedb *state.StateDB, cycle int64, sortOpt bool) *standbyWitnessPaySet {
	if bc.standbyPayCache == nil || bc.standbyPayCache.cycle != cycle {
		bc.standbyPayCache = buildStandbyWitnessPaySet(bc.buffer, statedb, cycle, sortOpt)
	}
	return bc.standbyPayCache
}

func (bc *BlockChain) invalidateStandbyPayCache() {
	bc.standbyPayCache = nil
}

func (bc *BlockChain) rewardAccountAddresses(blockWitness tcommon.Address, standby *standbyWitnessPaySet) []tcommon.Address {
	for addr := range bc.rewardAcctSeen {
		delete(bc.rewardAcctSeen, addr)
	}
	addrs := bc.rewardAcctAddrs[:0]
	if blockWitness != (tcommon.Address{}) {
		bc.rewardAcctSeen[blockWitness] = struct{}{}
		addrs = append(addrs, blockWitness)
	}
	if standby != nil {
		for _, w := range standby.witnesses {
			if _, ok := bc.rewardAcctSeen[w.addr]; ok {
				continue
			}
			bc.rewardAcctSeen[w.addr] = struct{}{}
			addrs = append(addrs, w.addr)
		}
	}
	bc.rewardAcctAddrs = addrs
	return addrs
}

func (bc *BlockChain) preloadRewardAccounts(statedb *state.StateDB, addrs []tcommon.Address) {
	if len(addrs) == 0 || len(bc.rewardAcctCache) == 0 {
		return
	}
	for _, addr := range addrs {
		if snapshot := bc.rewardAcctCache[addr]; snapshot != nil {
			statedb.LoadAccountSnapshotReference(snapshot)
		}
	}
}

func (bc *BlockChain) updateRewardAccountCache(statedb *state.StateDB, addrs []tcommon.Address) {
	if len(addrs) == 0 {
		bc.clearRewardAccountCache()
		return
	}
	for addr := range bc.rewardAcctCache {
		if _, ok := bc.rewardAcctSeen[addr]; !ok {
			delete(bc.rewardAcctCache, addr)
		}
	}
	for _, addr := range addrs {
		if snapshot := statedb.AccountSnapshotReference(addr); snapshot != nil {
			bc.rewardAcctCache[addr] = snapshot
		} else {
			delete(bc.rewardAcctCache, addr)
		}
	}
}

func (bc *BlockChain) clearRewardAccountCache() {
	bc.rewardAcctCache = make(map[tcommon.Address]*state.AccountSnapshot)
}

func (bc *BlockChain) preloadSystemAccount(statedb *state.StateDB) {
	if bc.systemAcctCache != nil {
		statedb.LoadAccountSnapshotReference(bc.systemAcctCache)
	}
}

// updateSystemAccountCache keeps the hot system-account envelope in memory
// across successful linear block applications. Rooted reward, dynamic-property,
// fork and schedule domains all hang off this account's KV root.
func (bc *BlockChain) updateSystemAccountCache(statedb *state.StateDB) {
	bc.systemAcctCache = statedb.AccountSnapshotReference(tcommon.SystemAccountAddress)
}

func (bc *BlockChain) clearSystemAccountCache() {
	bc.systemAcctCache = nil
}

func (bc *BlockChain) cachedWitnessLatestBlock(statedb *state.StateDB, addr tcommon.Address) int64 {
	if bc.witnessBlockCache == nil {
		bc.witnessBlockCache = make(map[tcommon.Address]int64)
	}
	if num, ok := bc.witnessBlockCache[addr]; ok {
		return num
	}
	num := statedb.ReadWitnessLatestBlock(addr)
	bc.witnessBlockCache[addr] = num
	return num
}

func (bc *BlockChain) clearWitnessBlockCache() {
	bc.witnessBlockCache = make(map[tcommon.Address]int64)
}

// loadWitnessesIntoState warms witness records for maintenance-only code that
// mutates vote counts through StateDB. Ordinary replay does lazy native-domain
// lookups instead, avoiding a full witness scan on every block.
func (bc *BlockChain) loadWitnessesIntoState(statedb *state.StateDB) {
	witnessAddrs := statedb.ReadWitnessIndex()
	for _, addr := range witnessAddrs {
		_ = statedb.GetWitness(addr)
	}
}

func (bc *BlockChain) updateFork(statedb *state.StateDB, block *types.Block) {
	active := bc.ActiveWitnesses()
	slot := -1
	for i, witness := range active {
		if witness == block.WitnessAddress() {
			slot = i
			break
		}
	}
	if slot < 0 {
		return
	}
	bc.forkControllerForState(statedb).Update(block.Version(), slot, len(active))
}

// NextMaintenanceTime returns the next scheduled maintenance time from dynamic
// properties. Reads the in-memory head snapshot (next_maintenance_time is a
// rooted key; the cache is the freshest committed-head view and avoids a
// system-KV trie open per call).
func (bc *BlockChain) NextMaintenanceTime() int64 {
	return bc.cachedDynProps().NextMaintenanceTime()
}

// ValidateTransaction runs the tx-envelope checks (signature recovery,
// permission lookup, weight summation, operation-bitmask) against the
// current head state. Callers that admit user-supplied transactions —
// HTTP/JSON-RPC backend, peer-tx gossip handler — invoke this before
// pool.Add so a malformed tx never enters the txpool.
//
// Gated on bc.engine for symmetry with the applyBlock-side check: tests
// that don't wire an engine accept unsigned txs into the pool without
// fuss. Production binaries wire the engine and get full validation.
func (bc *BlockChain) ValidateTransaction(tx *types.Transaction) error {
	if tx.ContractType() == corepb.Transaction_Contract_ShieldedTransferContract && !zksnark.Available() {
		return fmt.Errorf("shielded merkle tree backend unavailable: %w", zksnark.ErrPedersenUnimplemented)
	}
	if bc.engine == nil {
		return nil
	}
	if err := ValidateTxCommon(tx, bc.CurrentBlock().Timestamp()); err != nil {
		return err
	}
	statedb, err := bc.openState(bc.HeadStateRoot())
	if err != nil {
		return fmt.Errorf("open head state for tx validation: %w", err)
	}
	// VERSION_4_7_1 (value 27) swaps the multi-sig dedup key from raw
	// signature bytes to recovered address. For pool admission we use the
	// chain head's prev-block time + maintenance interval to evaluate it.
	dynProps := bc.DynProps()
	multiSigByAddress := forks.PassVersionFromStore(statedb, 27,
		dynProps.LatestBlockHeaderTimestamp(), dynProps.MaintenanceTimeInterval())
	if err := ValidateTxEnvelope(tx, statedb, multiSigByAddress); err != nil {
		return err
	}
	// TAPOS reads the recent-block ring from rawdb (no state opening
	// needed). Pool admission must reject txs that reference an unknown or
	// reorg-evicted block — relaying them would only fail at the producer
	// or peer's applyBlock step anyway.
	return ValidateTAPOS(tx, bc.buffer)
}

// DynProps returns a snapshot of the current dynamic properties. Reads the
// in-memory head snapshot (a Copy), which applyBlock refreshes each block
// under chainmu — strictly fresher than the solidified-flush boundary and
// avoiding a system-KV trie open for the rooted keys on every RPC/admission
// call.
func (bc *BlockChain) DynProps() *state.DynamicProperties {
	return bc.cachedDynProps()
}

// blockContainsShieldedTransfer reports whether any tx in the block is a
// ShieldedTransferContract. Used by applyBlock to gate the Sapling
// commitment-tree reset/save lifecycle so blocks without shielded receives
// don't churn CURRENT_TREE / LAST_TREE.
func blockContainsShieldedTransfer(block *types.Block) bool {
	for _, tx := range block.Transactions() {
		if tx.ContractType() == corepb.Transaction_Contract_ShieldedTransferContract {
			return true
		}
	}
	return false
}

// shouldMaintainShieldedMerkleTree reports whether this block needs the
// Sapling commitment-tree lifecycle (ResetCurrent before tx execution,
// SaveCurrentAsBest after). Mirrors java-tron's implicit gate — chain has
// activated AllowShieldedTransaction, or the block carries a shielded
// transfer. Centralised so the call sites in applyBlock cannot drift: a
// mismatched pair would silently desync LAST_TREE / MerkleTreeIndexStore
// from java-tron's invariant of one entry per post-activation block.
func shouldMaintainShieldedMerkleTree(dp *state.DynamicProperties, block *types.Block) bool {
	return dp.AllowShieldedTransaction() || blockContainsShieldedTransfer(block)
}

// chainHeaderAdapter adapts StateDB + DynProps to consensus.ChainHeaderWriter.
type chainHeaderAdapter struct {
	statedb          *state.StateDB
	dynProps         *state.DynamicProperties
	genesisWitnesses []consensus.GenesisWitnessInfo
}

func (a *chainHeaderAdapter) GetWitness(addr tcommon.Address) *types.Witness {
	return a.statedb.GetWitness(addr)
}

func (a *chainHeaderAdapter) PutWitness(w *types.Witness) {
	a.statedb.PutWitness(w.Address(), w.URL())
	a.statedb.AddWitnessVoteCount(w.Address(), w.VoteCount())
}

func (a *chainHeaderAdapter) AddWitnessVoteCount(addr tcommon.Address, delta int64) {
	a.statedb.AddWitnessVoteCount(addr, delta)
}

func (a *chainHeaderAdapter) AddAllowance(addr tcommon.Address, amount int64) {
	a.statedb.AddAllowance(addr, amount)
}

func (a *chainHeaderAdapter) NextMaintenanceTime() int64 {
	return a.dynProps.NextMaintenanceTime()
}

func (a *chainHeaderAdapter) SetNextMaintenanceTime(t int64) {
	a.dynProps.SetNextMaintenanceTime(t)
}

func (a *chainHeaderAdapter) WitnessPayPerBlock() int64 {
	return a.dynProps.WitnessPayPerBlock()
}

func (a *chainHeaderAdapter) WitnessStandbyAllowance() int64 {
	return a.dynProps.WitnessStandbyAllowance()
}

func (a *chainHeaderAdapter) ChangeDelegation() bool {
	return a.dynProps.ChangeDelegation()
}

func (a *chainHeaderAdapter) MaintenanceTimeInterval() int64 {
	return a.dynProps.MaintenanceTimeInterval()
}

func (a *chainHeaderAdapter) GenesisWitnesses() []consensus.GenesisWitnessInfo {
	return a.genesisWitnesses
}

func (a *chainHeaderAdapter) RemoveThePowerOfTheGr() int64 {
	return a.dynProps.RemoveThePowerOfTheGr()
}

func (a *chainHeaderAdapter) SetRemoveThePowerOfTheGr(v int64) {
	a.dynProps.SetRemoveThePowerOfTheGr(v)
}

// updateSolidifiedBlock updates the per-witness latest-block cache and
// recomputes the solidified block number using the java-tron algorithm:
//
//	sort all active witnesses by their latest produced block number and
//	take nums[floor(N * 0.3)] as the new solidified block (SOLIDIFIED_THRESHOLD = 0.7).
//
// For a single-SR chain (N=1) this means floor(1*0.3)=0, so the solidified
// number always equals that SR's latest block (i.e. the current head).
// Mirrors java-tron Manager.updateSolidifiedBlock().
func (bc *BlockChain) updateSolidifiedBlock(statedb *state.StateDB, producerAddr tcommon.Address, blockNum int64, dynProps *state.DynamicProperties) {
	if bc.witnessBlockCache == nil {
		bc.witnessBlockCache = make(map[tcommon.Address]int64)
	}
	bc.witnessBlockCache[producerAddr] = blockNum

	active := bc.ActiveWitnesses()
	n := len(active)
	if n == 0 {
		return
	}

	nums := make([]int64, 0, n)
	for _, addr := range active {
		nums = append(nums, bc.cachedWitnessLatestBlock(statedb, addr))
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	pos := int(float64(n) * 0.3) // SOLIDIFIED_THRESHOLD = 0.7 → position = floor(N*0.3)
	if pos >= n {
		pos = n - 1
	}
	solidified := nums[pos]
	if solidified >= dynProps.LatestSolidifiedBlockNum() {
		dynProps.SetLatestSolidifiedBlockNum(solidified)
	}
}

// flipWitnessIsJobs mirrors java-tron MaintenanceManager.applyBlock: when the
// active witness set rotates at a maintenance boundary, clear is_jobs on every
// outgoing member and set it on every incoming member. java-tron guards this
// on order-independent set inequality of currentWits vs newWits, so an
// unchanged cycle rewrites nothing.
func flipWitnessIsJobs(statedb *state.StateDB, oldActive, newActive []tcommon.Address) {
	if sameAddressSet(oldActive, newActive) {
		return
	}
	for _, addr := range oldActive {
		setWitnessIsJobs(statedb, addr, false)
	}
	for _, addr := range newActive {
		setWitnessIsJobs(statedb, addr, true)
	}
}

func setWitnessIsJobs(statedb *state.StateDB, addr tcommon.Address, v bool) {
	w := statedb.GetWitness(addr)
	if w == nil {
		return
	}
	w.SetIsJobs(v)
	_ = statedb.SetWitnessCapsule(w)
}

func sameAddressSet(a, b []tcommon.Address) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[tcommon.Address]struct{}, len(a))
	for _, x := range a {
		set[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := set[x]; !ok {
			return false
		}
	}
	return true
}

// gatherWitnessVotes collects all witnesses and their vote counts from the
// rooted witness index and rooted witness capsules on the same *StateDB the
// actuator appends to, so witnesses created earlier in this block are visible
// at maintenance.
func (bc *BlockChain) gatherWitnessVotes(statedb *state.StateDB) []dpos.WitnessVote {
	addrs := statedb.ReadWitnessIndex()
	var result []dpos.WitnessVote
	for _, addr := range addrs {
		w := statedb.GetWitness(addr)
		if w != nil {
			result = append(result, dpos.WitnessVote{
				Address: w.Address(),
				Votes:   w.VoteCount(),
			})
		}
	}
	return result
}

// vmKVStore augments a block-processing KV view (chain buffer, build buffer,
// or a validation layer) with the ancient-aware block-hash lookup the VM
// needs (rawdb.BlockHashReader). The slice-3 freezer prunes hot b-<num> rows
// past (solidified - margin), which sits inside BLOCKHASH's 256-block window
// and eventually covers genesis (CHAINID); the fall-through below keeps both
// opcodes resolving after pruning. Plain KV reads and writes pass through to
// the wrapped view unchanged.
type vmKVStore struct {
	actuator.BufferedKVStore
	chaindb *rawdb.ChainDB
}

func (s vmKVStore) BlockHashByNumber(number uint64) (tcommon.Hash, bool) {
	// Hot path: the wrapped view (buffer layers fall through to Pebble),
	// covering everything the freezer has not pruned, including blocks of
	// an in-flight insert batch that only exist in the buffer.
	if blk := rawdb.ReadBlockKV(s.BufferedKVStore, number); blk != nil {
		return blk.Hash(), true
	}
	// Frozen path: ancient-first ReadBlock (the hot row is already gone).
	if blk := rawdb.ReadBlock(s.chaindb, number); blk != nil {
		return blk.Hash(), true
	}
	return tcommon.Hash{}, false
}

// vmKV wraps a processing view for handoff to the actuator/VM layer.
func (bc *BlockChain) vmKV(view actuator.BufferedKVStore) vmKVStore {
	return vmKVStore{BufferedKVStore: view, chaindb: bc.chaindb}
}
