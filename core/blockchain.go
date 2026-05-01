package core

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

var (
	ErrKnownBlock    = errors.New("block already known")
	ErrInvalidParent = errors.New("parent block not found")
	ErrInvalidNumber = errors.New("invalid block number")
)

// BlockChain manages the canonical chain and provides block insertion.
type BlockChain struct {
	db      ethdb.KeyValueStore
	stateDB *state.Database
	config  *params.ChainConfig

	currentBlock   atomic.Pointer[types.Block]
	chainmu        sync.Mutex // serializes block insertion
	lastInsertNano atomic.Int64

	genesisBlock    *types.Block
	activeWitnesses atomic.Value // []tcommon.Address
	fc              *forks.ForkController

	khaosDB *KhaosDB

	// buffer holds rawdb-direct writes from applyBlock that must be
	// rewindable on switchFork (slice 1: witness statistics only). Layered
	// per applyBlock; switchFork drops orphan-branch layers. Slice 1 does
	// not flush to disk; reads must consult the buffer (see BufferedDB).
	buffer *blockbuffer.Buffer

	blockHookMu sync.Mutex
	blockHooks  []func(*types.Block) // called after each successful InsertBlock

	maintHookMu sync.Mutex
	maintHooks  []func(*types.Block, []tcommon.Address) // fired after a maintenance block
}

// AddBlockHook registers a callback called after each successfully inserted block.
func (bc *BlockChain) AddBlockHook(fn func(*types.Block)) {
	bc.blockHookMu.Lock()
	bc.blockHooks = append(bc.blockHooks, fn)
	bc.blockHookMu.Unlock()
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

// NewBlockChain creates a new BlockChain, loading head from DB.
func NewBlockChain(db ethdb.KeyValueStore, stateDB *state.Database, config *params.ChainConfig) (*BlockChain, error) {
	bc := &BlockChain{
		db:      db,
		stateDB: stateDB,
		config:  config,
		fc:      forks.NewForkController(db),
		buffer:  blockbuffer.New(db),
	}
	bc.lastInsertNano.Store(time.Now().UnixNano())

	// Load genesis
	bc.genesisBlock = rawdb.ReadBlock(db, 0)
	if bc.genesisBlock == nil {
		return nil, errors.New("genesis block not found in database")
	}

	// Load head block
	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		bc.currentBlock.Store(bc.genesisBlock)
	} else {
		num := rawdb.ReadBlockNumber(db, headHash)
		if num == nil {
			bc.currentBlock.Store(bc.genesisBlock)
		} else {
			block := rawdb.ReadBlock(db, *num)
			if block == nil {
				bc.currentBlock.Store(bc.genesisBlock)
			} else {
				bc.currentBlock.Store(block)
			}
		}
	}

	// Initialize KhaosDB with the current head.
	bc.khaosDB = NewKhaosDB()
	bc.khaosDB.Start(bc.currentBlock.Load())

	// Load active witnesses from DB; if empty, derive from genesis witnesses
	witnesses := rawdb.ReadActiveWitnesses(db)
	if len(witnesses) == 0 {
		var allWitnesses []dpos.WitnessVote
		witnessAddrs := rawdb.ReadWitnessIndex(db)
		for _, addr := range witnessAddrs {
			w := rawdb.ReadWitness(db, addr)
			if w != nil {
				allWitnesses = append(allWitnesses, dpos.WitnessVote{
					Address: w.Address(),
					Votes:   w.VoteCount(),
				})
			}
		}
		if len(allWitnesses) > 0 {
			witnesses = dpos.SelectActiveWitnesses(allWitnesses)
			rawdb.WriteActiveWitnesses(db, witnesses)
		}
	}
	if len(witnesses) > 0 {
		bc.activeWitnesses.Store(witnesses)
	}

	return bc, nil
}

// CurrentBlock returns the head of the canonical chain.
func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock.Load()
}

// GetBlockByNumber retrieves a block by its number.
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	return rawdb.ReadBlock(bc.db, number)
}

// GetBlockByHash retrieves a block by its hash.
func (bc *BlockChain) GetBlockByHash(hash tcommon.Hash) *types.Block {
	num := rawdb.ReadBlockNumber(bc.db, hash)
	if num == nil {
		return nil
	}
	return rawdb.ReadBlock(bc.db, *num)
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

	// Open a fresh buffer layer for this block. The layer holds rawdb-direct
	// writes (slice 1: witness statistics only) so that switchFork can drop
	// the orphan-branch layers via DiscardBlock. On any error path the
	// active layer is discarded; on success it is promoted via CommitBlock.
	bc.buffer.BeginBlock(block.Hash())
	defer func() {
		if retErr != nil {
			bc.buffer.DiscardActive()
		}
	}()

	// Open StateDB from parent's state root. State roots live in a side
	// store keyed by block hash, not on the block proto, so blocks coming
	// in from java-tron (which has empty account_state_root) round-trip
	// without losing wire-format identity. Genesis falls back to the
	// dedicated post-genesis-state-root key.
	parentRoot := rawdb.ReadBlockStateRoot(bc.db, current.Hash())
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

	// Load dynamic properties through the buffer so that DP keys written by
	// pending (not-yet-flushed) layers are visible to this applyBlock — e.g.
	// `current_cycle_number` advanced by an unflushed maintenance boundary
	// must be readable here. Slice 2 of the fork-rewind fix.
	dynProps := state.LoadDynamicProperties(bc.buffer)

	// Load witnesses into statedb for maintenance access.
	witnessAddrs := rawdb.ReadWitnessIndex(bc.db)
	for _, addr := range witnessAddrs {
		if statedb.GetWitness(addr) == nil {
			w := rawdb.ReadWitness(bc.db, addr)
			if w != nil {
				statedb.PutWitness(addr, w.URL())
				statedb.AddWitnessVoteCount(addr, w.VoteCount())
			}
		}
	}

	// Capture old-head timestamp BEFORE ProcessBlock; needed by ApplyBlockStatistics
	// to compute slot offset against the chain head as it stood pre-insert
	// (matches java-tron StatisticManager.applyBlock semantics).
	previousHeadTimestamp := current.Timestamp()
	prevIsMaintenance := dynProps.NextMaintenanceTime() > 0 &&
		previousHeadTimestamp >= dynProps.NextMaintenanceTime()

	// Process block (execute transactions, pay reward — does not commit).
	// The buffer is passed so per-block actuator-side rawdb-direct writes
	// (WriteAssetIssue, WriteExchange, WriteProposal, WriteContractState
	// from VMActuator dynamic-energy, WriteNullifier, etc.) and the
	// `payBlockReward → AddCycleReward` write (gated on change_delegation)
	// land in the active buffer layer. `switchFork` rewinds them on orphan
	// discard. Slice 3 of the fork-rewind fix.
	txInfos, err := ProcessBlock(statedb, dynProps, block, bc.buffer, bc.ActiveWitnesses(), bc.GenesisTimestamp())
	if err != nil {
		return fmt.Errorf("process block: %w", err)
	}

	// Update witness production counters + BLOCK_FILLED_SLOTS rolling window
	// (mirrors java-tron StatisticManager.applyBlock). The per-witness
	// counter writes go through bc.buffer so switchFork can rewind them on
	// reorgs (slice 1 of the fork-rewind fix). The BLOCK_FILLED_SLOTS ring
	// is updated on dynProps in-memory and lands via dynProps.Flush(bc.db)
	// below — not yet retrofitted onto the buffer (see slice 2 backlog in
	// docs/superpowers/specs/2026-04-30-fork-rewind-fix-design.md).
	dpos.ApplyBlockStatistics(bc.buffer, dynProps, block, previousHeadTimestamp,
		bc.ActiveWitnesses(), bc.GenesisTimestamp(), prevIsMaintenance)

	// Run maintenance if at boundary (before commit so allowances are included).
	wasMaintenanceBlock := false
	var maintNewWitnesses []tcommon.Address
	if dynProps.NextMaintenanceTime() > 0 && block.Timestamp() >= dynProps.NextMaintenanceTime() {
		allWitnesses := bc.gatherWitnessVotes(statedb)
		dpos.DoMaintenance(&chainHeaderAdapter{statedb: statedb, dynProps: dynProps}, block.Timestamp(), allWitnesses)
		// Cycle brokerage / vote / VI writes go through bc.buffer so they
		// rewind on switchFork (slice 2). Reads from rawdb inside
		// applyRewardMaintenance also consult the buffer, so cross-block
		// accumulators see in-flight values.
		applyRewardMaintenance(bc.buffer, statedb, dynProps)
		newActive := dpos.SelectActiveWitnesses(allWitnesses)
		bc.SetActiveWitnesses(newActive)
		wasMaintenanceBlock = true
		maintNewWitnesses = newActive
	}

	// Commit state (includes both tx execution and maintenance changes).
	newRoot, err := statedb.Commit()
	if err != nil {
		return fmt.Errorf("commit state: %w", err)
	}

	// Verify state root if the block carries one (java-tron sets this on
	// post-fork blocks via the AccountStateCallBack hook); otherwise just
	// trust our computed root. The root is persisted out-of-band — we do
	// NOT mutate `block.AccountStateRoot()` because the block proto's
	// content must round-trip byte-identical to what the wire delivered.
	blockRoot := block.AccountStateRoot()
	if blockRoot != (tcommon.Hash{}) && blockRoot != newRoot {
		return fmt.Errorf("state root mismatch: block=%x computed=%x", blockRoot, newRoot)
	}
	rawdb.WriteBlockStateRoot(bc.db, block.Hash(), newRoot)

	// Update dynamic properties.
	dynProps.SetLatestBlockHeaderNumber(int64(block.Number()))
	dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dynProps.SetLatestBlockHeaderHash(block.Hash())

	// Update solidified block number (mirrors java-tron Manager.updateSolidifiedBlock).
	// Routes WriteWitnessLatestBlock + the per-witness ReadWitnessLatestBlock
	// loop through bc.buffer so the solidified compute reflects in-flight
	// updates (slice 2).
	bc.updateSolidifiedBlock(block.WitnessAddress(), int64(block.Number()), dynProps)

	// Land DP changes into the active buffer layer (slice 2). This includes
	// block_filled_slots (from ApplyBlockStatistics), latest_block_header_*,
	// latest_solidified_block_num, burn_trx_amount (from burnFee actuators),
	// total_create_witness_cost (from witness create), maintenance-touched
	// keys, etc. — every dirty DP key.
	dynProps.Flush(bc.buffer)

	// Persist block.
	if err := rawdb.WriteBlock(bc.db, block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())

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

	// Increment the non-consensus total transaction counter through bc.buffer
	// so switchFork rewinds the increment on orphan blocks (slice 2). The
	// matching read also consults the buffer — otherwise a buffered
	// increment would be overwritten by the disk's stale value on the next
	// block.
	if n := len(block.Transactions()); n > 0 {
		count := rawdb.ReadTotalTransactionCount(bc.buffer)
		rawdb.WriteTotalTransactionCount(bc.buffer, count+int64(n))
	}

	// Fire maintenance hooks first so the SRL PBFT message goes out before
	// the block PREPREPARE — matches java-tron MaintenanceManager.applyBlock
	// ordering (srPrePrepare at line 72, blockPrePrepare at line 81).
	if wasMaintenanceBlock {
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

	// Promote the buffer layer to the layered stack. Slice 1 introduced the
	// layered stack; slice 2 adds the flush-at-solidified policy below.
	bc.buffer.CommitBlock()

	// Flush every layer at or below the new solidified-block number to
	// disk, oldest-first. Layers above solidified stay in memory and remain
	// rewindable via switchFork's DiscardBlock. Mirrors java-tron's
	// invariant that Manager.eraseBlock can never pop past solidified.
	if err := bc.flushBufferUpToSolidified(dynProps.LatestSolidifiedBlockNum()); err != nil {
		return fmt.Errorf("flush buffer up to solidified: %w", err)
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
	numberOf := func(h tcommon.Hash) (uint64, bool) {
		p := rawdb.ReadBlockNumber(bc.db, h)
		if p == nil {
			return 0, false
		}
		return *p, true
	}
	return bc.buffer.FlushUpTo(uint64(solidified), numberOf, bc.db)
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
	dynProps := state.LoadDynamicProperties(bc.buffer)
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

// switchFork rewinds the canonical chain to the LCA of newHead and the current
// tip, then re-applies the new branch on top of LCA state.
// Callers must hold bc.chainmu.
func (bc *BlockChain) switchFork(newHead *types.Block) error {
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

	var lcaBlock *types.Block
	numPtr := rawdb.ReadBlockNumber(bc.db, lcaHash)
	if numPtr != nil {
		lcaBlock = rawdb.ReadBlock(bc.db, *numPtr)
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

// DB returns the underlying key-value store.
func (bc *BlockChain) DB() ethdb.KeyValueStore {
	return bc.db
}

// BufferedDB returns a read-only view that consults the in-memory
// applyBlock buffer first, then falls through to the disk store. Reads of
// rawdb fields that are written through the buffer (slice 1: witness
// statistics counters) must go through this view to see the current state
// — disk reads alone return stale values until slice 2 wires a flush
// policy.
func (bc *BlockChain) BufferedDB() ethdb.KeyValueReader {
	return bc.buffer
}

// ActiveWitnesses returns the current active witness list.
func (bc *BlockChain) ActiveWitnesses() []tcommon.Address {
	v := bc.activeWitnesses.Load()
	if v == nil {
		return nil
	}
	return v.([]tcommon.Address)
}

// SetActiveWitnesses updates the active witness list in memory and persists it to the DB.
func (bc *BlockChain) SetActiveWitnesses(witnesses []tcommon.Address) {
	bc.activeWitnesses.Store(witnesses)
	rawdb.WriteActiveWitnesses(bc.db, witnesses)
}

// NextMaintenanceTime returns the next scheduled maintenance time from dynamic properties.
// Reads through bc.buffer so unflushed maintenance updates are visible (slice 2).
func (bc *BlockChain) NextMaintenanceTime() int64 {
	dynProps := state.LoadDynamicProperties(bc.buffer)
	return dynProps.NextMaintenanceTime()
}

// DynProps loads and returns a snapshot of the current dynamic properties.
// Reads through bc.buffer so unflushed DP writes (slice 2: every dirty DP
// key including counters, fee totals, latest_solidified_block_num) are
// visible to RPC and other external readers without waiting for the
// solidified-flush boundary.
func (bc *BlockChain) DynProps() *state.DynamicProperties {
	return state.LoadDynamicProperties(bc.buffer)
}

// chainHeaderAdapter adapts StateDB + DynProps to consensus.ChainHeaderWriter.
type chainHeaderAdapter struct {
	statedb  *state.StateDB
	dynProps *state.DynamicProperties
}

func (a *chainHeaderAdapter) GetWitness(addr tcommon.Address) *types.Witness {
	return a.statedb.GetWitness(addr)
}

func (a *chainHeaderAdapter) PutWitness(w *types.Witness) {
	a.statedb.PutWitness(w.Address(), w.URL())
	a.statedb.AddWitnessVoteCount(w.Address(), w.VoteCount())
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

// updateSolidifiedBlock updates the per-witness latest-block cursor and
// recomputes the solidified block number using the java-tron algorithm:
//
//	sort all active witnesses by their latest produced block number and
//	take nums[floor(N * 0.3)] as the new solidified block (SOLIDIFIED_THRESHOLD = 0.7).
//
// For a single-SR chain (N=1) this means floor(1*0.3)=0, so the solidified
// number always equals that SR's latest block (i.e. the current head).
// Mirrors java-tron Manager.updateSolidifiedBlock().
func (bc *BlockChain) updateSolidifiedBlock(producerAddr tcommon.Address, blockNum int64, dynProps *state.DynamicProperties) {
	// Persist this witness's latest produced block number through bc.buffer
	// so it rewinds on switchFork (slice 2). The N-way read below also
	// consults the buffer — otherwise the solidified compute would use a
	// stale on-disk value for the just-written witness.
	rawdb.WriteWitnessLatestBlock(bc.buffer, producerAddr, blockNum)

	active := bc.ActiveWitnesses()
	n := len(active)
	if n == 0 {
		return
	}

	nums := make([]int64, 0, n)
	for _, addr := range active {
		nums = append(nums, rawdb.ReadWitnessLatestBlock(bc.buffer, addr))
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

// gatherWitnessVotes collects all witnesses and their vote counts from statedb (falling back to rawdb).
func (bc *BlockChain) gatherWitnessVotes(statedb *state.StateDB) []dpos.WitnessVote {
	addrs := rawdb.ReadWitnessIndex(bc.db)
	var result []dpos.WitnessVote
	for _, addr := range addrs {
		w := statedb.GetWitness(addr)
		if w == nil {
			w = rawdb.ReadWitness(bc.db, addr)
		}
		if w != nil {
			result = append(result, dpos.WitnessVote{
				Address: w.Address(),
				Votes:   w.VoteCount(),
			})
		}
	}
	return result
}
