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
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/core/zksnark"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var log = gtronlog.NewModule("core/chain")

var (
	ErrKnownBlock    = errors.New("block already known")
	ErrInvalidParent = errors.New("parent block not found")
	ErrInvalidNumber = errors.New("invalid block number")
)

// ApplyStats reports per-phase wall-clock time spent inside applyBlock.
//
// Subscribers should treat ApplyStats as read-only. The fields are exported so
// callers (sync summary line, future metrics surface) can aggregate without
// reaching back into core internals.
//
//   - Validate: header verification (signature recovery, scheduled-witness
//     match, post-fork timestamp alignment) plus parent linkage.
//   - Execute: transaction execution + reward + BLOCK_FILLED_SLOTS update.
//     Includes the in-memory state mutations; does NOT include the trie
//     commit (that lives in StateCommit).
//   - Maintenance: doMaintenance work on cycle boundaries (proposals, vote
//     tally, active-set rotation, reward VI). Zero on non-maintenance blocks.
//   - StateCommit: statedb.Commit — trie.Update for every dirty account +
//     TrieDB.Update/Commit for hash-based trie node writes. Empirically the
//     dominant phase as state grows.
//   - DPUpdate: dynamic-properties writes (latest_block_header_*,
//     solidified, fork-vote tally) into the buffer.
//   - Persist: WriteBlock + WriteTaposRef + tx info persist + the final
//     buffer flushBufferUpToSolidified that lands committed layers on disk.
//   - Hooks: post-apply callback fan-out (PBFT, broadcaster, etc.).
type ApplyStats struct {
	Validate    time.Duration
	Execute     time.Duration
	Maintenance time.Duration
	StateCommit time.Duration
	DPUpdate    time.Duration
	Persist     time.Duration
	Hooks       time.Duration
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

	currentBlock   atomic.Pointer[types.Block]
	chainmu        sync.Mutex // serializes block insertion
	lastInsertNano atomic.Int64

	genesisBlock     *types.Block
	genesisWitnesses []consensus.GenesisWitnessInfo
	activeWitnesses  atomic.Value // []tcommon.Address
	dynPropsCache    atomic.Value // *state.DynamicProperties; canonical head snapshot
	standbyPayCache  *standbyWitnessPaySet
	rewardAcctCache  map[tcommon.Address]*types.Account
	rewardAcctSeen   map[tcommon.Address]struct{}
	rewardAcctAddrs  []tcommon.Address
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

// AddBlockHook registers a callback called after each successfully inserted block.
func (bc *BlockChain) AddBlockHook(fn func(*types.Block)) {
	bc.blockHookMu.Lock()
	bc.blockHooks = append(bc.blockHooks, fn)
	bc.blockHookMu.Unlock()
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
	bc := &BlockChain{
		db:              db,
		chaindb:         chaindb,
		stateDB:         stateDB,
		config:          config,
		fc:              forks.NewForkController(buffer),
		buffer:          buffer,
		flushQueue:      make(chan uint64, flushQueueCap),
		flushPending:    newFlushBarrier(),
		rewardAcctCache: make(map[tcommon.Address]*types.Account),
		rewardAcctSeen:  make(map[tcommon.Address]struct{}),
		rewardAcctAddrs: make([]tcommon.Address, 0, 128),
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

	head := loadStoredHeadBlock(chaindb, bc.genesisBlock)
	head = recoverHeadToAppliedState(db, chaindb, head, bc.genesisBlock)
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

func recoverHeadToAppliedState(db ethdb.KeyValueStore, chaindb *rawdb.ChainDB, head, genesis *types.Block) *types.Block {
	if head == nil {
		return genesis
	}
	// Reads only latest_block_header_number (a derived key in flat dp-), so no
	// system-KV reader is needed.
	dynProps := state.LoadDynamicProperties(db, nil)
	appliedNum := dynProps.LatestBlockHeaderNumber()
	if appliedNum < 0 || uint64(appliedNum) >= head.Number() {
		return head
	}

	recovered := rawdb.ReadBlock(chaindb, uint64(appliedNum))
	if recovered == nil {
		log.Warn("Head recovery: block missing, keeping disk head",
			"diskHead", head.Number(), "appliedState", appliedNum)
		return head
	}
	if appliedHash := dynProps.LatestBlockHeaderHash(); appliedHash != (tcommon.Hash{}) && recovered.Hash() != appliedHash {
		log.Warn("Head recovery: hash mismatch, keeping disk head",
			"diskHead", head.Number(), "appliedState", appliedNum,
			"dpHash", appliedHash, "blockHash", recovered.Hash())
		return head
	}

	rawdb.WriteHeadBlockHash(db, recovered.Hash())
	log.Info("Head recovered to applied state",
		"from", head.Number(), "to", recovered.Number())
	return recovered
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

	current := bc.CurrentBlock()

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
		if err := bc.switchFork(newHead); err != nil {
			bc.khaosDB.RemoveBlk(block.Hash())
			return fmt.Errorf("switchFork: %w", err)
		}
		return nil
	}

	// Normal linear extension: the pushed block IS the new global head.
	if err := bc.applyBlock(block); err != nil {
		bc.khaosDB.RemoveBlk(block.Hash())
		return err
	}
	return nil
}

// applyBlock executes, commits, and persists a single block on top of the
// current canonical tip (bc.CurrentBlock()). It updates currentBlock on success.
// Callers must hold bc.chainmu.
func (bc *BlockChain) applyBlock(block *types.Block) (retErr error) {
	current := bc.CurrentBlock()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("async buffer flush failed: %w", *errPtr)
	}

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
	parentRoot := rawdb.ReadBlockStateRoot(bc.chaindb, current.Hash())
	if parentRoot == (tcommon.Hash{}) && current.Number() == 0 {
		parentRoot = rawdb.ReadGenesisStateRoot(bc.db)
	}
	if parentRoot == (tcommon.Hash{}) {
		// Backwards-compat fallback for chain databases written before
		// blockStateRootPrefix existed.
		parentRoot = current.AccountStateRoot()
	}
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}
	// Flip the SHI capture flag for this block. Without this the StateDB's
	// AccumulateHistory short-circuits and no rows land in bc.buffer.
	if bc.config.HistoryEnabled {
		statedb.SetHistoryEnabled(true)
	}

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
	dynProps := bc.cachedDynProps()

	// Header verification (signature recovery, scheduled-witness match, and
	// post-fork timestamp alignment) runs here rather than at the top of
	// InsertBlock because:
	//   - applyBlock is the single chokepoint for state application from both
	//     linear extension and switchFork's re-apply loop;
	//   - bc.CurrentBlock() == block's actual parent at this point (the
	//     re-apply loop advances current sequentially), so VerifyHeader's
	//     parent-linkage and "ts > parent.ts" checks line up correctly even
	//     during a fork rewind;
	//   - bad blocks may briefly enter the KhaosDB mini-store but never reach
	//     state — KhaosDB's size bound caps the DoS surface, and the orphan is
	//     pruned by the caller on the returned error.
	// Skipped when bc.engine is nil (test path; see SetEngine).
	if bc.engine != nil {
		if err := bc.engine.VerifyHeaderWithDynProps(bc, block, dynProps); err != nil {
			return err
		}
	}
	stats.mark(&stats.Validate)

	// Open a fresh buffer layer for this block. The layer holds legacy
	// rawdb-shaped mirror writes so that switchFork can drop orphan-branch
	// layers via DiscardBlock. On any error path the active layer is discarded;
	// on success it is promoted via CommitBlock.
	bc.buffer.BeginBlock(block.Hash())
	statedb.SetAccountKVIndexStore(bc.buffer)
	defer func() {
		if retErr != nil {
			bc.buffer.DiscardActive()
			// SetActiveWitnesses may have mutated the in-memory atomic before
			// the failure. Its rooted write was on the now-discarded statedb
			// (never committed), so reload the atomic from the system-KV at the
			// head root — still the parent here, the state this block never
			// advanced past.
			bc.reloadActiveWitnesses(bc.HeadStateRoot())
		}
	}()

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
	energyLimitForkBlockNum := bc.config.EnergyLimitForkBlockNum()
	var standbyPaySet *standbyWitnessPaySet
	if dynProps.ChangeDelegation() && dynProps.Witness127PayPerBlock() > 0 {
		standbyPaySet = bc.cachedStandbyPaySet(statedb, dynProps.CurrentCycleNumber())
	}
	rewardAcctAddrs := bc.rewardAccountAddresses(block.WitnessAddress(), standbyPaySet)
	bc.preloadRewardAccounts(statedb, rewardAcctAddrs)
	defer func() {
		if retErr != nil && len(rewardAcctAddrs) > 0 {
			bc.clearRewardAccountCache()
		}
	}()
	if blockRoot != (tcommon.Hash{}) {
		parentRoot := current.AccountStateRoot()
		txInfos, javaAccountStateRoot, err = processBlock(statedb, dynProps, block, bc.buffer, bc.ActiveWitnesses(), bc.GenesisTimestamp(), energyLimitForkBlockNum, bc.engine != nil, bc.effectiveGenesisHash(), &parentRoot, standbyPaySet)
	} else {
		txInfos, _, err = processBlock(statedb, dynProps, block, bc.buffer, bc.ActiveWitnesses(), bc.GenesisTimestamp(), energyLimitForkBlockNum, bc.engine != nil, bc.effectiveGenesisHash(), nil, standbyPaySet)
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
	statedb.FlushWitnesses(bc.buffer)

	// Update witness production counters + BLOCK_FILLED_SLOTS rolling window
	// (mirrors java-tron StatisticManager.applyBlock). The per-witness
	// counter writes go through bc.buffer so switchFork can rewind them on
	// reorgs (slice 1 of the fork-rewind fix). The BLOCK_FILLED_SLOTS ring
	// is updated on dynProps in-memory and lands via dynProps.Flush(bc.db)
	// below — not yet retrofitted onto the buffer (see slice 2 backlog in
	// docs/superpowers/specs/2026-04-30-fork-rewind-fix-design.md).
	dpos.ApplyBlockStatistics(statedb, dynProps, block, previousHeadTimestamp,
		bc.ActiveWitnesses(), bc.GenesisTimestamp(), prevIsMaintenance)
	stats.mark(&stats.Execute)

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
			if err := ProcessProposals(bc.buffer, statedb, dynProps, bc.ActiveWitnesses(), block.Timestamp(), forks.NewForkControllerFromState(statedb)); err != nil {
				return fmt.Errorf("process proposals: %w", err)
			}
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
			statedb.FlushWitnesses(bc.buffer)

			// java-tron accumulates reward VI before VotesStore old/new deltas
			// are folded into WitnessStore, then snapshots cycle vote counts
			// after those deltas are applied.
			applyRewardVI(bc.buffer, statedb, dynProps)
			hasPendingVotes := applyPendingVotes(statedb)
			statedb.FlushWitnesses(bc.buffer)
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
		forks.NewForkControllerFromState(statedb).Reset(block.Timestamp(), dynProps.MaintenanceTimeInterval(), len(bc.ActiveWitnesses()))
	} else {
		dynProps.SetStateFlag(0)
	}
	stats.mark(&stats.Maintenance)

	// Verify state root if the block carries one (java-tron sets this on
	// post-fork blocks via the AccountStateCallBack hook) before committing
	// the full StateDB. Commit writes flat contract storage/code alongside
	// trie nodes; doing it after this check keeps a rejected java-tron block
	// from contaminating the parent state used by the next retry.
	if blockRoot != (tcommon.Hash{}) && blockRoot != javaAccountStateRoot {
		return fmt.Errorf("state root mismatch: block=%x computed=%x", blockRoot, javaAccountStateRoot)
	}

	// State History Index (SHI) capture. Walks the per-block journal and
	// flushes pre-block account / slot / code / contract-meta deltas to
	// bc.buffer so switchFork's DiscardBlock rewinds them along with the
	// other buffered writes for this layer. Must run BEFORE statedb.Commit,
	// which truncates the journal; the belt-and-braces config gate here
	// skips the function call entirely on non-archive operators (StateDB
	// also short-circuits internally, this guard just avoids the call cost).
	if bc.config.HistoryEnabled {
		if err := statedb.AccumulateHistory(bc.buffer, block.Number(), block.Hash()); err != nil {
			return fmt.Errorf("accumulate state history: %w", err)
		}
	}

	// Update dynamic properties and fork-vote state before Commit so every
	// mutable consensus store touched by this block is included in the full
	// internal state root. Replay-derived TAPOS/metric rows are staged into
	// the active buffer below, but intentionally stay outside the state root.
	dynProps.SetLatestBlockHeaderNumber(int64(block.Number()))
	dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dynProps.SetLatestBlockHeaderHash(block.Hash())
	bc.updateSolidifiedBlock(statedb, block.WitnessAddress(), int64(block.Number()), dynProps)
	bc.updateFork(statedb, block)
	if err := rawdb.WriteTaposRef(bc.buffer, block.Number(), block.Hash()); err != nil {
		return fmt.Errorf("stage tapos ref: %w", err)
	}
	if n := len(block.Transactions()); n > 0 {
		count := rawdb.ReadTotalTransactionCount(bc.buffer)
		rawdb.WriteTotalTransactionCount(bc.buffer, count+int64(n))
	}

	// Stage dynamic properties into the system account's KV BEFORE Commit so
	// they enter the internal full-state root (and thus rewind with it).
	if err := dynProps.FlushRooted(statedb); err != nil {
		return fmt.Errorf("flush rooted dynamic properties: %w", err)
	}
	statedb.SetDomainChangeSetWriter(bc.buffer, block.Number(), block.Hash())

	// Commit state (includes both tx execution and maintenance changes).
	newRoot, err := statedb.Commit()
	if err != nil {
		return fmt.Errorf("commit state: %w", err)
	}
	bc.updateRewardAccountCache(statedb, rewardAcctAddrs)

	// The root is persisted out-of-band — we do NOT mutate
	// `block.AccountStateRoot()` because the block proto's content must
	// round-trip byte-identical to what the wire delivered.
	rawdb.WriteBlockStateRoot(bc.db, block.Hash(), newRoot)
	stats.mark(&stats.StateCommit)

	// Land DP changes into the active buffer layer (slice 2). This includes
	// block_filled_slots (from ApplyBlockStatistics), latest_block_header_*,
	// latest_solidified_block_num, burn_trx_amount (from burnFee actuators),
	// total_create_witness_cost (from witness create), maintenance-touched
	// keys, etc. — every dirty DP key.
	dynProps.Flush(bc.buffer)

	stats.mark(&stats.DPUpdate)

	// Persist block.
	if err := rawdb.WriteBlock(bc.db, block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	rawdb.WriteHeadBlockHash(bc.buffer, block.Hash())

	// Record this block in the TAPOS recent-block ring so future txs can
	// reference it. java-tron's Manager.updateRecentBlock runs unconditionally
	// at the head of pushBlockInner; doing it here (after the block is fully
	// committed) preserves the same observable ordering: the next block's
	// txs see this block's slot, and a fork-rewind that discards this block
	// will write a different value into the same slot when the alternate
	// branch's block #N applies — overwrite, not delete, matches java's
	// ring semantics.
	if err := rawdb.WriteTaposRef(bc.db, block.Number(), block.Hash()); err != nil {
		return fmt.Errorf("write tapos ref: %w", err)
	}

	// Advance currentBlock before writing tx infos so that any caller
	// unblocked by WriteTransactionInfo sees the new state root.
	bc.currentBlock.Store(block)
	bc.lastInsertNano.Store(time.Now().UnixNano())

	// Persist transaction infos and indexes.
	for _, info := range txInfos {
		rawdb.WriteTransactionInfo(bc.db, info.Id, info)
	}
	rawdb.WriteTransactionInfosByBlock(bc.db, block.Number(), txInfos)
	for _, tx := range block.Transactions() {
		h := tx.Hash()
		rawdb.WriteTransactionIndex(bc.db, h[:], block.Number())
	}

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

	// Promote the buffer layer to the layered stack. Slice 1 introduced the
	// layered stack; slice 2 adds the flush-at-solidified policy below.
	bc.buffer.CommitBlock()

	// Hand every layer at or below the new solidified-block number to the
	// async flusher. Layers above solidified stay in memory and remain
	// rewindable via switchFork's DiscardBlock. Mirrors java-tron's
	// invariant that Manager.eraseBlock can never pop past solidified.
	if err := bc.postFlush(dynProps.LatestSolidifiedBlockNum()); err != nil {
		return err
	}
	stats.mark(&stats.Persist)
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
	numberOf := func(h tcommon.Hash) (uint64, bool) {
		p := rawdb.ReadBlockNumber(bc.chaindb, h)
		if p == nil {
			return 0, false
		}
		return *p, true
	}
	return bc.buffer.FlushUpTo(uint64(solidified), numberOf, bc.db)
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

// Close performs a graceful shutdown of the BlockChain: it acquires
// chainmu, flushes every buffer layer at or below the current solidified
// block to disk, and drops layers above solidified.
//
// We deliberately do NOT flush past solidified — the layers above the
// solidified line could in principle still be reorged out from under us
// on the next start (java-tron's `Manager.eraseBlock` invariant: cannot
// pop past solidified, but may pop the in-memory window above it). After
// restart, `NewBlockChain` reloads from `rawdb.ReadHeadBlockHash` whose
// most-recent fully-flushed image is at the solidified line, and the
// node re-syncs the post-solidified blocks from peers. This matches
// java-tron's behavior on a clean shutdown — `revokingStore` sessions
// above solidified are dropped (they were never persisted).
//
// Trade-off accepted: a clean shutdown loses up to `head - solidified`
// blocks of post-applyBlock counters. On mainnet (27 SRs) this is ~19
// blocks; recovery is automatic via re-sync. The alternative — flushing
// everything — would persist non-solidified state that a post-restart
// reorg could no longer rewind, which is the worse failure mode.
//
// Callers should invoke Close before closing the underlying KeyValueStore.
// Slice 3 of the fork-rewind fix.
func (bc *BlockChain) Close() error {
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	bc.WaitForFlushSettled()
	bc.stopFlushWorkerLocked()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("close: async buffer flush failed: %w", *errPtr)
	}
	// Reads only latest_solidified_block_num (a derived key in flat dp-), so no
	// system-KV reader is needed.
	dynProps := state.LoadDynamicProperties(bc.buffer, nil)
	if err := bc.flushBufferUpToSolidified(dynProps.LatestSolidifiedBlockNum()); err != nil {
		return fmt.Errorf("close: flush up to solidified: %w", err)
	}
	// Drop any leftover layers above solidified — they would otherwise
	// retain memory and (more importantly) be silently dropped on the
	// next Buffer mutation. Discard explicitly so reads after Close fall
	// straight through to disk.
	bc.buffer.Discard()
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
	// Drain any in-flight async flushes before rewinding buffer layers.
	// Without this, the worker may still be holding solidified-but-not-yet-
	// flushed layers in bc.buffer when DiscardBlock runs; DiscardBlock would
	// pop them, silently losing finalised state — violating the "forks must
	// not pop past solidified" invariant the synchronous flush used to
	// enforce by emptying those layers out of the buffer before applyBlock
	// returned. We wait on the chainmu-holding caller's path; the flush
	// worker holds only the buffer's internal mu, so this can't deadlock.
	bc.flushPending.wait()

	currentHash := bc.CurrentBlock().Hash()
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
	bc.invalidateStandbyPayCache()
	bc.clearRewardAccountCache()

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

	// Apply new branch blocks in order LCA+1 → newHead.
	reversed := make([]*types.Block, len(newBranch))
	for i, kb := range newBranch {
		reversed[len(newBranch)-1-i] = kb.block
	}
	for _, b := range reversed {
		if err := bc.applyBlock(b); err != nil {
			// Remove orphaned new-branch blocks from KhaosDB.
			for _, kb := range newBranch {
				bc.khaosDB.RemoveBlk(kb.block.Hash())
			}
			return fmt.Errorf("apply fork block %d: %w", b.Number(), err)
		}
	}
	return nil
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
	sysKV, err := state.New(root, bc.stateDB)
	if err != nil {
		return nil
	}
	return sysKV
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

func (bc *BlockChain) effectiveGenesisHash() tcommon.Hash {
	if bc.config != nil && bc.config.GenesisHash != (tcommon.Hash{}) {
		return bc.config.GenesisHash
	}
	if bc.genesisBlock != nil {
		return bc.genesisBlock.Hash()
	}
	return tcommon.Hash{}
}

func (bc *BlockChain) cachedStandbyPaySet(statedb *state.StateDB, cycle int64) *standbyWitnessPaySet {
	if bc.standbyPayCache == nil || bc.standbyPayCache.cycle != cycle {
		bc.standbyPayCache = buildStandbyWitnessPaySet(bc.buffer, statedb, cycle)
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
		if acc := bc.rewardAcctCache[addr]; acc != nil {
			statedb.LoadAccountReference(acc)
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
		if acc := statedb.AccountReference(addr); acc != nil {
			bc.rewardAcctCache[addr] = acc
		} else {
			delete(bc.rewardAcctCache, addr)
		}
	}
}

func (bc *BlockChain) clearRewardAccountCache() {
	bc.rewardAcctCache = make(map[tcommon.Address]*types.Account)
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
	forks.NewForkControllerFromState(statedb).Update(block.Version(), slot, len(active))
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
	statedb, err := state.New(bc.HeadStateRoot(), bc.stateDB)
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

// updateSolidifiedBlock updates the per-witness latest-block cursor and
// recomputes the solidified block number using the java-tron algorithm:
//
//	sort all active witnesses by their latest produced block number and
//	take nums[floor(N * 0.3)] as the new solidified block (SOLIDIFIED_THRESHOLD = 0.7).
//
// For a single-SR chain (N=1) this means floor(1*0.3)=0, so the solidified
// number always equals that SR's latest block (i.e. the current head).
// Mirrors java-tron Manager.updateSolidifiedBlock().
func (bc *BlockChain) updateSolidifiedBlock(statedb *state.StateDB, producerAddr tcommon.Address, blockNum int64, dynProps *state.DynamicProperties) {
	// The rooted cursor is canonical for rewind and historical restart.
	_ = statedb.WriteWitnessLatestBlock(producerAddr, blockNum)

	active := bc.ActiveWitnesses()
	n := len(active)
	if n == 0 {
		return
	}

	nums := make([]int64, 0, n)
	for _, addr := range active {
		nums = append(nums, statedb.ReadWitnessLatestBlock(addr))
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
