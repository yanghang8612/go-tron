package core

import (
	"encoding/binary"
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
type BlockChain struct {
	db      ethdb.KeyValueStore
	stateDB *state.Database
	config  *params.ChainConfig

	currentBlock   atomic.Pointer[types.Block]
	chainmu        sync.Mutex // serializes block insertion
	lastInsertNano atomic.Int64

	genesisBlock     *types.Block
	genesisWitnesses []consensus.GenesisWitnessInfo
	activeWitnesses  atomic.Value // []tcommon.Address
	fc               *forks.ForkController

	// engine validates block headers (signature, witness scheduling, timestamp
	// alignment) when applyBlock runs. Wired post-construction via SetEngine
	// because dpos.New(bc) requires bc to exist first. nil ⇒ header
	// verification is skipped — used only by tests that build unsigned blocks
	// to exercise the state-machine path in isolation. Every production
	// callsite must call SetEngine before the first InsertBlock.
	engine consensus.Engine

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

// NewBlockChain creates a new BlockChain, loading head from DB.
func NewBlockChain(db ethdb.KeyValueStore, stateDB *state.Database, config *params.ChainConfig) (*BlockChain, error) {
	buffer := blockbuffer.New(db)
	bc := &BlockChain{
		db:      db,
		stateDB: stateDB,
		config:  config,
		fc:      forks.NewForkController(buffer),
		buffer:  buffer,
	}
	bc.lastInsertNano.Store(time.Now().UnixNano())

	// Load genesis
	bc.genesisBlock = rawdb.ReadBlock(db, 0)
	if bc.genesisBlock == nil {
		return nil, errors.New("genesis block not found in database")
	}

	for _, gw := range rawdb.ReadGenesisWitnesses(db) {
		bc.genesisWitnesses = append(bc.genesisWitnesses, consensus.GenesisWitnessInfo{
			Address:   gw.Address,
			VoteCount: gw.VoteCount,
		})
	}

	head := loadStoredHeadBlock(db, bc.genesisBlock)
	head = recoverHeadToAppliedState(db, head, bc.genesisBlock)
	bc.currentBlock.Store(head)

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
			dynProps := state.LoadDynamicProperties(db)
			witnesses = dpos.SelectActiveWitnessesWithOptimization(allWitnesses, dynProps.ConsensusLogicOptimization())
			rawdb.WriteActiveWitnesses(db, witnesses)
		}
	}
	if len(witnesses) > 0 {
		bc.activeWitnesses.Store(witnesses)
	}

	return bc, nil
}

func loadStoredHeadBlock(db ethdb.KeyValueReader, genesis *types.Block) *types.Block {
	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		return genesis
	}
	num := rawdb.ReadBlockNumber(db, headHash)
	if num == nil {
		return genesis
	}
	block := rawdb.ReadBlock(db, *num)
	if block == nil {
		return genesis
	}
	return block
}

func recoverHeadToAppliedState(db ethdb.KeyValueStore, head, genesis *types.Block) *types.Block {
	if head == nil {
		return genesis
	}
	dynProps := state.LoadDynamicProperties(db)
	appliedNum := dynProps.LatestBlockHeaderNumber()
	if appliedNum < 0 || uint64(appliedNum) >= head.Number() {
		return head
	}

	recovered := rawdb.ReadBlock(db, uint64(appliedNum))
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
	return rawdb.ReadBlock(bc.db, number)
}

// GetBlockByHash retrieves a block by its hash.
func (bc *BlockChain) GetBlockByHash(hash tcommon.Hash) *types.Block {
	num := rawdb.ReadBlockNumber(bc.db, hash)
	if num == nil {
		return nil
	}
	if current := bc.CurrentBlock(); current != nil && *num > current.Number() {
		return nil
	}
	return rawdb.ReadBlock(bc.db, *num)
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
	dynProps := state.LoadDynamicProperties(bc.buffer)

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

	// Open a fresh buffer layer for this block. The layer holds rawdb-direct
	// writes (slice 1: witness statistics only) so that switchFork can drop
	// the orphan-branch layers via DiscardBlock. On any error path the
	// active layer is discarded; on success it is promoted via CommitBlock.
	bc.buffer.BeginBlock(block.Hash())
	defer func() {
		if retErr != nil {
			bc.buffer.DiscardActive()
			// SetActiveWitnesses may have mutated the in-memory atomic before
			// the failure. The buffered disk write was just discarded with the
			// layer, so reload the atomic from the rewound buffer to keep it
			// consistent with the state this block never reached.
			bc.reloadActiveWitnesses()
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
		if err := zksnark.NewMerkleContainer(bc.buffer).ResetCurrent(); err != nil {
			return fmt.Errorf("reset shielded merkle tree: %w", err)
		}
	}

	// Load witnesses into statedb for maintenance access. Reads go through
	// bc.buffer so that VoteCount/URL deltas persisted by previous blocks
	// (via statedb.FlushWitnesses below) are visible even when those blocks
	// haven't been flushed to bc.db yet — same layered-read consistency the
	// DP load above relies on.
	//
	// LoadWitness (vs PutWitness+AddWitnessVoteCount) deliberately does NOT
	// mark the addresses dirty: this is a hydration of in-memory cache from
	// rawdb, not a mutation. Actuators that actually change VoteCount or URL
	// downstream mark dirty via PutWitness / SetWitnessURL /
	// AddWitnessVoteCount, so FlushWitnesses only persists the genuine deltas.
	witnessAddrs := rawdb.ReadWitnessIndex(bc.buffer)
	for _, addr := range witnessAddrs {
		if statedb.GetWitness(addr) == nil {
			statedb.LoadWitness(rawdb.ReadWitness(bc.buffer, addr))
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
	// The buffer is passed so per-block actuator-side rawdb-direct writes
	// (WriteAssetIssue, WriteExchange, WriteProposal, WriteContractState
	// from VMActuator dynamic-energy, WriteNullifier, etc.) and the
	// `payBlockReward → AddCycleReward` write (gated on change_delegation)
	// land in the active buffer layer. `switchFork` rewinds them on orphan
	// discard. Slice 3 of the fork-rewind fix.
	blockRoot := block.AccountStateRoot()
	var txInfos []*corepb.TransactionInfo
	var javaAccountStateRoot tcommon.Hash
	energyLimitForkBlockNum := bc.config.EnergyLimitForkBlockNum()
	if blockRoot != (tcommon.Hash{}) {
		txInfos, javaAccountStateRoot, err = ProcessBlockWithJavaAccountStateRootAndEnergyFork(statedb, dynProps, block, bc.buffer, bc.ActiveWitnesses(), bc.GenesisTimestamp(), energyLimitForkBlockNum, bc.engine != nil, current.AccountStateRoot())
	} else {
		txInfos, err = ProcessBlockWithEnergyFork(statedb, dynProps, block, bc.buffer, bc.ActiveWitnesses(), bc.GenesisTimestamp(), energyLimitForkBlockNum, bc.engine != nil)
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
		if err := zksnark.NewMerkleContainer(bc.buffer).SaveCurrentAsBest(int64(block.Number())); err != nil {
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
	dpos.ApplyBlockStatistics(bc.buffer, dynProps, block, previousHeadTimestamp,
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
			// Process expired proposals first — applies their parameter changes
			// to DP (or marks them CANCELED). Mirrors java-tron MaintenanceManager
			// → ConsensusService.applyBlock order: processProposals → updateWitness
			// (vote tally + active set rotation) → reward. Without this, every
			// proposal stays PENDING forever and downstream actuator / VM fork
			// gates never activate — observed empirically on a Nile soak at
			// h=860k where 4 proposals had 27 SR approvals each but `state =
			// PENDING` and `allow_creation_of_contracts = 0` (2026-05-09).
			// Routes through bc.buffer per fork-rewind slice 3 so per-proposal
			// state writes rewind on switchFork.
			if err := ProcessProposals(bc.buffer, dynProps, bc.ActiveWitnesses(), block.Timestamp(), bc.fc, statedb); err != nil {
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
			hasPendingVotes := applyPendingVotes(bc.buffer, statedb)
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
				flipWitnessIsJobs(bc.buffer, bc.ActiveWitnesses(), newActive)
				bc.SetActiveWitnesses(newActive)
				maintNewWitnesses = newActive
			}

			applyRewardCycleSnapshot(bc.buffer, statedb, dynProps)
			nextMaint := dpos.CalcNextMaintenanceTime(block.Timestamp(), dynProps.NextMaintenanceTime(), dynProps.MaintenanceTimeInterval())
			dynProps.SetNextMaintenanceTime(nextMaint)
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
		bc.fc.Reset(block.Timestamp(), dynProps.MaintenanceTimeInterval(), len(bc.ActiveWitnesses()))
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

	// Commit state (includes both tx execution and maintenance changes).
	newRoot, err := statedb.Commit()
	if err != nil {
		return fmt.Errorf("commit state: %w", err)
	}

	// The root is persisted out-of-band — we do NOT mutate
	// `block.AccountStateRoot()` because the block proto's content must
	// round-trip byte-identical to what the wire delivered.
	rawdb.WriteBlockStateRoot(bc.db, block.Hash(), newRoot)
	stats.mark(&stats.StateCommit)

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

	// java-tron's Manager.applyBlock persists the block after processing and
	// then calls updateFork(block). At that point the current block's
	// transactions have already been validated against the previous fork
	// bitmap, while the next block observes this producer's version vote.
	bc.updateFork(block)
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

	// Increment the non-consensus total transaction counter through bc.buffer
	// so switchFork rewinds the increment on orphan blocks (slice 2). The
	// matching read also consults the buffer — otherwise a buffered
	// increment would be overwritten by the disk's stale value on the next
	// block.
	if n := len(block.Transactions()); n > 0 {
		count := rawdb.ReadTotalTransactionCount(bc.buffer)
		rawdb.WriteTotalTransactionCount(bc.buffer, count+int64(n))
	}
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

	// Flush every layer at or below the new solidified-block number to
	// disk, oldest-first. Layers above solidified stay in memory and remain
	// rewindable via switchFork's DiscardBlock. Mirrors java-tron's
	// invariant that Manager.eraseBlock can never pop past solidified.
	if err := bc.flushBufferUpToSolidified(dynProps.LatestSolidifiedBlockNum()); err != nil {
		return fmt.Errorf("flush buffer up to solidified: %w", err)
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

	// An orphan-branch maintenance block may have called SetActiveWitnesses,
	// mutating the in-memory atomic. Its buffered disk write was just dropped
	// with the orphan layer above — reload the atomic from the rewound buffer
	// so the active set rewinds with the rest of consensus state before the
	// new branch is re-applied. (Without this the active set stays stale even
	// though witness is_jobs and DP correctly rewound.)
	bc.reloadActiveWitnesses()

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
	if root := rawdb.ReadBlockStateRoot(bc.db, block.Hash()); root != (tcommon.Hash{}) {
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
	if root := rawdb.ReadBlockStateRoot(bc.db, head.Hash()); root != (tcommon.Hash{}) {
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

// BufferedDB returns a read-only view that consults the in-memory
// applyBlock buffer first, then falls through to the disk store. Reads of
// rawdb fields that are written through the buffer (slice 1: witness
// statistics counters) must go through this view to see the current state
// — disk reads alone return stale values until slice 2 wires a flush
// policy.
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
// rawdb.ReadDynamicProperty(bc.db, ...) so they see the just-applied
// block's DP writes — the buffer is flushed only up to the solidified
// boundary, which on mainnet 27-SR DPoS lags head by ~19 blocks. A
// maintenance-boundary write of next_maintenance_time lands in the buffer
// immediately; a disk-only reader would compute the old epoch and silently
// miss SRL commit results cached under the new one.
func (bc *BlockChain) BufferedDPInt64(name string) int64 {
	data := rawdb.ReadDynamicProperty(bc.buffer, name)
	if len(data) == 8 {
		return int64(binary.BigEndian.Uint64(data))
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

// SetActiveWitnesses updates the active witness list in memory and persists it
// through bc.buffer (the active applyBlock layer) — NOT straight to bc.db.
// java-tron keeps the active set in a revoking store (WitnessScheduleStore
// extends TronStoreWithRevoking), so it must rewind with the rest of consensus
// state on a fork rewind across a maintenance boundary. Routing the write
// through the buffer puts it in the same atomically-buffered, switchFork-
// rewindable set as the witness is_jobs flips (see flipWitnessIsJobs). The
// in-memory atomic is the fast read path for ActiveWitnesses(); switchFork and
// the applyBlock error defer reload it from the buffer after a rewind.
//
// Must be called inside an open buffer layer (Buffer.Put panics otherwise) —
// the sole production caller is applyBlock, after BeginBlock.
func (bc *BlockChain) SetActiveWitnesses(witnesses []tcommon.Address) {
	bc.activeWitnesses.Store(witnesses)
	rawdb.WriteActiveWitnesses(bc.buffer, witnesses)
}

// reloadActiveWitnesses refreshes the in-memory active-witness atomic from the
// buffer-backed view. Called after a rewind (switchFork's DiscardBlock loop or
// the applyBlock error defer) so the atomic — which an orphan-branch
// SetActiveWitnesses mutated — falls back to the rewound state. A nil result
// (no value buffered or on disk) leaves the atomic untouched.
func (bc *BlockChain) reloadActiveWitnesses() {
	if reloaded := rawdb.ReadActiveWitnesses(bc.buffer); reloaded != nil {
		bc.activeWitnesses.Store(reloaded)
	}
}

func (bc *BlockChain) updateFork(block *types.Block) {
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
	bc.fc.Update(block.Version(), slot, len(active))
}

// NextMaintenanceTime returns the next scheduled maintenance time from dynamic properties.
// Reads through bc.buffer so unflushed maintenance updates are visible (slice 2).
func (bc *BlockChain) NextMaintenanceTime() int64 {
	dynProps := state.LoadDynamicProperties(bc.buffer)
	return dynProps.NextMaintenanceTime()
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
	multiSigByAddress := forks.PassVersion(bc.buffer, 27,
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

// DynProps loads and returns a snapshot of the current dynamic properties.
// Reads through bc.buffer so unflushed DP writes (slice 2: every dirty DP
// key including counters, fee totals, latest_solidified_block_num) are
// visible to RPC and other external readers without waiting for the
// solidified-flush boundary.
func (bc *BlockChain) DynProps() *state.DynamicProperties {
	return state.LoadDynamicProperties(bc.buffer)
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

// witnessKV is the read+write capability flipWitnessIsJobs needs; both
// *blockbuffer.Buffer and a plain ethdb store satisfy it.
type witnessKV interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// flipWitnessIsJobs mirrors java-tron MaintenanceManager.applyBlock: when the
// active witness set rotates at a maintenance boundary, clear is_jobs on every
// outgoing member and set it on every incoming member. java-tron guards this
// on order-independent set inequality of currentWits vs newWits, so an
// unchanged cycle rewrites nothing. Writes go direct to the block buffer via
// rawdb (not through statedb) because statedb.FlushWitnesses only merges
// VoteCount and URL onto the stored record — is_jobs would be dropped.
func flipWitnessIsJobs(db witnessKV, oldActive, newActive []tcommon.Address) {
	if sameAddressSet(oldActive, newActive) {
		return
	}
	for _, addr := range oldActive {
		setWitnessIsJobs(db, addr, false)
	}
	for _, addr := range newActive {
		setWitnessIsJobs(db, addr, true)
	}
}

func setWitnessIsJobs(db witnessKV, addr tcommon.Address, v bool) {
	w := rawdb.ReadWitness(db, addr)
	if w == nil {
		return
	}
	w.SetIsJobs(v)
	rawdb.WriteWitness(db, addr, w)
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

// gatherWitnessVotes collects all witnesses and their vote counts from statedb (falling back to rawdb).
// Reads from bc.buffer so witnesses created earlier in the current block
// (WitnessCreateActuator writes to bc.buffer) are visible at maintenance.
func (bc *BlockChain) gatherWitnessVotes(statedb *state.StateDB) []dpos.WitnessVote {
	addrs := rawdb.ReadWitnessIndex(bc.buffer)
	var result []dpos.WitnessVote
	for _, addr := range addrs {
		w := statedb.GetWitness(addr)
		if w == nil {
			w = rawdb.ReadWitness(bc.buffer, addr)
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
