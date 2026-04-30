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
}

// AddBlockHook registers a callback called after each successfully inserted block.
func (bc *BlockChain) AddBlockHook(fn func(*types.Block)) {
	bc.blockHookMu.Lock()
	bc.blockHooks = append(bc.blockHooks, fn)
	bc.blockHookMu.Unlock()
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

	// Open StateDB from parent's state root.
	parentRoot := current.AccountStateRoot()
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}

	// Load dynamic properties.
	dynProps := state.LoadDynamicProperties(bc.db)

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
	txInfos, err := ProcessBlock(statedb, dynProps, block, bc.db, bc.ActiveWitnesses(), bc.GenesisTimestamp())
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
	if dynProps.NextMaintenanceTime() > 0 && block.Timestamp() >= dynProps.NextMaintenanceTime() {
		allWitnesses := bc.gatherWitnessVotes(statedb)
		dpos.DoMaintenance(&chainHeaderAdapter{statedb: statedb, dynProps: dynProps}, block.Timestamp(), allWitnesses)
		applyRewardMaintenance(bc.db, statedb, dynProps)
		newActive := dpos.SelectActiveWitnesses(allWitnesses)
		bc.SetActiveWitnesses(newActive)
	}

	// Commit state (includes both tx execution and maintenance changes).
	newRoot, err := statedb.Commit()
	if err != nil {
		return fmt.Errorf("commit state: %w", err)
	}

	// Verify state root if the block has one set; otherwise stamp the computed root.
	blockRoot := block.AccountStateRoot()
	if blockRoot != (tcommon.Hash{}) {
		if blockRoot != newRoot {
			return fmt.Errorf("state root mismatch: block=%x computed=%x", blockRoot, newRoot)
		}
	} else {
		block.SetAccountStateRoot(newRoot)
	}

	// Update dynamic properties.
	dynProps.SetLatestBlockHeaderNumber(int64(block.Number()))
	dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dynProps.SetLatestBlockHeaderHash(block.Hash())

	// Update solidified block number (mirrors java-tron Manager.updateSolidifiedBlock).
	bc.updateSolidifiedBlock(block.WitnessAddress(), int64(block.Number()), dynProps)

	dynProps.Flush(bc.db)

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

	// Increment the non-consensus total transaction counter.
	if n := len(block.Transactions()); n > 0 {
		count := rawdb.ReadTotalTransactionCount(bc.db)
		rawdb.WriteTotalTransactionCount(bc.db, count+int64(n))
	}

	bc.blockHookMu.Lock()
	hooks := bc.blockHooks
	bc.blockHookMu.Unlock()
	for _, h := range hooks {
		h(block)
	}

	// Promote the buffer layer to the layered stack. Slice 1 does not flush
	// to disk; reads via bc.BufferedDB() consult the buffer first.
	bc.buffer.CommitBlock()
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
func (bc *BlockChain) NextMaintenanceTime() int64 {
	dynProps := state.LoadDynamicProperties(bc.db)
	return dynProps.NextMaintenanceTime()
}

// DynProps loads and returns a snapshot of the current dynamic properties.
func (bc *BlockChain) DynProps() *state.DynamicProperties {
	return state.LoadDynamicProperties(bc.db)
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
	// Persist this witness's latest produced block number.
	rawdb.WriteWitnessLatestBlock(bc.db, producerAddr, blockNum)

	active := bc.ActiveWitnesses()
	n := len(active)
	if n == 0 {
		return
	}

	nums := make([]int64, 0, n)
	for _, addr := range active {
		nums = append(nums, rawdb.ReadWitnessLatestBlock(bc.db, addr))
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
