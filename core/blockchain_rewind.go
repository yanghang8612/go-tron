package core

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

const restartSyncReplayBatchSize = 1000

var (
	storedReplayPrefetchBatchesCounter   = metrics.NewRegisteredCounter("core/stored_replay/prefetch_batches", nil)
	storedReplayPrefetchWaitNanosCounter = metrics.NewRegisteredCounter("core/stored_replay/prefetch_wait/nanos", nil)
	storedReplayPreparedTxCounter        = metrics.NewRegisteredCounter("core/stored_replay/prepared_transactions", nil)
)

// RestartSyncProgress is emitted by RestartSyncFromHeight after each major
// phase and replayed block. Block is meaningful for replay/done phases.
type RestartSyncProgress struct {
	Phase  string
	Block  uint64
	Target uint64
}

// ReplayStoredBlocksToHeight advances materialized state from the current head
// using canonical block bytes that are already present in the hot database or
// freezer. It is an offline startup operation for resuming an interrupted
// reset+replay: callers must invoke it before P2P, producer, PBFT, and regular
// API lifecycles start. Unlike RestartSyncFromHeight it never resets state and
// never fetches from peers.
func (bc *BlockChain) ReplayStoredBlocksToHeight(height uint64, progressFn func(RestartSyncProgress)) error {
	return bc.replayStoredBlocksToHeight(height, restartSyncReplayBatchSize, progressFn)
}

func (bc *BlockChain) replayStoredBlocksToHeight(height uint64, batchSize uint64, progressFn func(RestartSyncProgress)) error {
	if bc == nil {
		return errors.New("stored replay: nil blockchain")
	}
	if batchSize == 0 {
		return errors.New("stored replay: zero batch size")
	}
	current := bc.CurrentBlock()
	if current == nil {
		return errors.New("stored replay: current block is nil")
	}
	if height < current.Number() {
		return fmt.Errorf("stored replay: target height %d is below current head %d", height, current.Number())
	}
	target := rawdb.ReadBlock(bc.chaindb, height)
	if target == nil {
		return fmt.Errorf("stored replay: canonical block %d not found", height)
	}
	emit := func(phase string, block uint64) {
		if progressFn != nil {
			progressFn(RestartSyncProgress{Phase: phase, Block: block, Target: height})
		}
	}
	if height == current.Number() {
		emit("done", height)
		return nil
	}

	type loadedBatch struct {
		start  uint64
		end    uint64
		blocks []*types.Block
		err    error
	}
	load := func(start uint64, parent *types.Block) loadedBatch {
		end := start + batchSize - 1
		if end < start || end > height {
			end = height
		}
		result := loadedBatch{start: start, end: end, blocks: make([]*types.Block, 0, end-start+1)}
		for n := start; n <= end; n++ {
			block := rawdb.ReadStoredBlockForReplay(bc.chaindb, n)
			if block == nil {
				result.err = fmt.Errorf("stored replay: block %d not found", n)
				return result
			}
			if len(result.blocks) > 0 {
				parent = result.blocks[len(result.blocks)-1]
			}
			if block.Number() != parent.Number()+1 {
				result.err = fmt.Errorf("stored replay: block %d has number %d, want %d", n, block.Number(), parent.Number()+1)
				return result
			}
			if block.ParentHash() != parent.Hash() {
				result.err = fmt.Errorf("stored replay: block %d parent mismatch: have %x want %x", n, block.ParentHash(), parent.Hash())
				return result
			}
			// Build the immutable transaction wrappers and their two execution-hot
			// memos while this batch is still on the prefetch goroutine. Hash and
			// contract decoding are pure sync.Once computations; canonical execution
			// later observes the same wrapper instances, values, and cached errors.
			transactions := block.Transactions()
			for _, tx := range transactions {
				_ = tx.Hash()
				_, _ = tx.DecodedContract()
			}
			storedReplayPreparedTxCounter.Inc(int64(len(transactions)))
			result.blocks = append(result.blocks, block)
		}
		return result
	}

	batch := load(current.Number()+1, current)
	for {
		if batch.err != nil {
			return batch.err
		}
		var prefetched <-chan loadedBatch
		if batch.end < height {
			prefetch := make(chan loadedBatch, 1)
			nextStart := batch.end + 1
			nextParent := batch.blocks[len(batch.blocks)-1]
			storedReplayPrefetchBatchesCounter.Inc(1)
			go func() {
				prefetch <- load(nextStart, nextParent)
			}()
			prefetched = prefetch
		}

		if err := bc.insertStoredBlocks(batch.blocks); err != nil {
			if prefetched != nil {
				<-prefetched
			}
			var rangeErr *InsertBlocksError
			if errors.As(err, &rangeErr) && rangeErr.BlockNumber != 0 {
				return fmt.Errorf("stored replay: apply block %d: %w", rangeErr.BlockNumber, err)
			}
			return fmt.Errorf("stored replay: apply range %d-%d: %w", batch.start, batch.end, err)
		}
		emit("replay", batch.end)
		if prefetched == nil {
			break
		}
		waitStart := time.Now()
		batch = <-prefetched
		storedReplayPrefetchWaitNanosCounter.Inc(time.Since(waitStart).Nanoseconds())
	}

	emit("flush", height)
	bc.WaitForFlushSettled()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("stored replay: async flush failed: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return fmt.Errorf("stored replay: flush replay buffer: %w", err)
	}
	bc.buffer.Discard()
	final := bc.CurrentBlock()
	if final == nil || final.Number() != height || final.Hash() != target.Hash() {
		if final == nil {
			return errors.New("stored replay: final head is nil")
		}
		return fmt.Errorf("stored replay: final head mismatch: got #%d %x want #%d %x", final.Number(), final.Hash(), height, target.Hash())
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return fmt.Errorf("stored replay: sync completion: %w", err)
	}
	emit("done", height)
	return nil
}

// insertStoredBlocks applies an offline replay batch while marking its existing
// canonical bodies as immutable preserved input. It mirrors InsertBlocks'
// locking and error surface; only metadata persistence differs.
func (bc *BlockChain) insertStoredBlocks(blocks []*types.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return ErrBlockChainClosed
	}
	return bc.insertBlocksLockedMode(blocks, true)
}

// RestartSyncFromHeight rewinds the local materialized state to height and
// leaves the chain ready to request height+1 from peers.
//
// This is an operator-initiated offline startup operation, driven only by the
// explicit --sync.restart-from flag. Call it before P2P, producer, PBFT, and
// API hooks are registered; it intentionally replays canonical blocks through
// the same staged range importer used by sync and therefore would otherwise
// re-fire apply hooks.
//
// Fast incremental path: when changesets covering (height, currentHead] are
// present, the chain is rewound via inverse-delta commitment unwind
// (domains.UnwindCommitment) rather than reset+genesis+replay.
// The gate is canIncrementalUnwind; false forces the conservative path.
//
// Known limitation (incremental path only): TAPOS ring slots in
// (height, height+65536] survive, whereas the reset+replay path removes them
// via ResetMutableState. They are self-healing: the ring is a 65536-slot
// overwrite ring, so within ~65536 blocks all stale slots are replaced.
// This matches the acceptable behaviour documented in the gap-6 slice-2 spec.
func (bc *BlockChain) RestartSyncFromHeight(height uint64, genesis *params.Genesis, ancient rawdb.AncientWriter, progressFn func(RestartSyncProgress)) error {
	if bc == nil {
		return errors.New("restart sync: nil blockchain")
	}
	if genesis == nil || genesis.Config == nil {
		return errors.New("restart sync: genesis with chain config is required")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.commitPending != nil {
		bc.WaitForCommitSettled()
	}
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return fmt.Errorf("restart sync: pending async commit failed: %w", *errPtr)
	}

	current := bc.CurrentBlock()
	if current == nil {
		return errors.New("restart sync: current block is nil")
	}
	if height > current.Number() {
		return fmt.Errorf("restart sync: target height %d exceeds current head %d", height, current.Number())
	}
	target, err := readRestartSyncBlock(bc.chaindb, height, fmt.Sprintf("canonical block %d not found", height))
	if err != nil {
		return fmt.Errorf("restart sync: %w", err)
	}

	emit := func(phase string, block uint64) {
		if progressFn != nil {
			progressFn(RestartSyncProgress{Phase: phase, Block: block, Target: height})
		}
	}

	emit("reset", 0)
	bc.WaitForFlushSettled()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("restart sync: pending async flush failed: %w", *errPtr)
	}

	materializedHead := current.Number()

	// Fast incremental path: skip reset+replay when changesets cover the full
	// (height, materializedHead] window.
	if bc.canIncrementalUnwind(height, materializedHead) {
		if err := bc.incrementalUnwindTo(target, materializedHead, ancient, emit); err != nil {
			return fmt.Errorf("restart sync: incremental unwind to %d: %w", height, err)
		}
		if err := syncKeyValueStore(bc.db); err != nil {
			return fmt.Errorf("restart sync: sync rewind completion: %w", err)
		}
		emit("done", height)
		return nil
	}

	// Conservative reset+replay path (always correct; taken when history is off
	// or changeset window is incomplete).
	bc.buffer.Discard()
	if ancient != nil {
		if _, err := ancient.TruncateHead(height + 1); err != nil {
			return fmt.Errorf("restart sync: truncate ancient head to %d: %w", height+1, err)
		}
		if err := ancient.Sync(); err != nil {
			return fmt.Errorf("restart sync: sync ancient truncate: %w", err)
		}
	}
	if err := rawdb.ResetMutableState(bc.db); err != nil {
		return fmt.Errorf("restart sync: reset mutable state: %w", err)
	}

	emit("genesis", 0)
	genesisBlock, genesisRoot, genesisDP, err := genesisBlockAndStateRoot(genesis, bc.stateDB)
	if err != nil {
		return fmt.Errorf("restart sync: rebuild genesis state: %w", err)
	}
	if bc.genesisBlock != nil && genesisBlock.Hash() != bc.genesisBlock.Hash() {
		return errors.New("restart sync: genesis hash mismatch after rebuild")
	}
	if err := writeGenesisMaterializedState(bc.db, genesis, genesisBlock, genesisRoot, genesisDP); err != nil {
		return fmt.Errorf("restart sync: write genesis state: %w", err)
	}
	if err := bc.resetRuntimeStateLocked(genesisBlock, genesisRoot); err != nil {
		return err
	}

	for start := uint64(1); start <= height; {
		end := start + restartSyncReplayBatchSize - 1
		if end < start || end > height {
			end = height
		}
		blocks := make([]*types.Block, 0, end-start+1)
		for n := start; n <= end; n++ {
			block, err := readRestartSyncBlock(bc.chaindb, n, fmt.Sprintf("block %d not found during replay", n))
			if err != nil {
				return fmt.Errorf("restart sync: %w", err)
			}
			if len(blocks) == 0 {
				parent := bc.CurrentBlock()
				if block.Number() != parent.Number()+1 {
					return fmt.Errorf("restart sync: block %d has number %d, want %d", n, block.Number(), parent.Number()+1)
				}
				if block.ParentHash() != parent.Hash() {
					return fmt.Errorf("restart sync: block %d parent mismatch: have %x want %x", n, block.ParentHash(), parent.Hash())
				}
			} else {
				parent := blocks[len(blocks)-1]
				if block.Number() != parent.Number()+1 {
					return fmt.Errorf("restart sync: block %d has number %d, want %d", n, block.Number(), parent.Number()+1)
				}
				if block.ParentHash() != parent.Hash() {
					return fmt.Errorf("restart sync: block %d parent mismatch: have %x want %x", n, block.ParentHash(), parent.Hash())
				}
			}
			blocks = append(blocks, block)
		}
		if err := bc.insertBlocksLocked(blocks, nil); err != nil {
			var rangeErr *InsertBlocksError
			if errors.As(err, &rangeErr) {
				for i := 0; i < rangeErr.Index && i < len(blocks); i++ {
					emit("replay", blocks[i].Number())
				}
				if rangeErr.BlockNumber != 0 {
					return fmt.Errorf("restart sync: replay block %d: %w", rangeErr.BlockNumber, err)
				}
			}
			return fmt.Errorf("restart sync: replay block range %d-%d: %w", start, end, err)
		}
		for _, block := range blocks {
			emit("replay", block.Number())
		}
		start = end + 1
	}

	emit("flush", height)
	bc.WaitForFlushSettled()
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("restart sync: async flush failed during replay: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return fmt.Errorf("restart sync: flush replay buffer: %w", err)
	}
	bc.buffer.Discard()

	final := bc.CurrentBlock()
	if final == nil || final.Number() != height || final.Hash() != target.Hash() {
		if final == nil {
			return errors.New("restart sync: final head is nil")
		}
		return fmt.Errorf("restart sync: final head mismatch: got #%d %x want #%d %x", final.Number(), final.Hash(), height, target.Hash())
	}
	rawdb.WriteHeadBlockHash(bc.db, final.Hash())
	if err := rewindCanonicalStagePipeline(bc.db, height, final.Hash()); err != nil {
		return fmt.Errorf("restart sync: rewind canonical stage progress: %w", err)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageTxLookup, height, final.Hash()); err != nil {
		return fmt.Errorf("restart sync: write tx lookup stage progress: %w", err)
	}
	if bc.config != nil && bc.config.HistoryEnabled {
		if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageStateHistoryIndex, height, final.Hash()); err != nil {
			return fmt.Errorf("restart sync: write state history index stage progress: %w", err)
		}
	}
	if err := bc.resetRuntimeStateLocked(final, bc.HeadStateRoot()); err != nil {
		return err
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return fmt.Errorf("restart sync: sync rewind completion: %w", err)
	}
	emit("done", height)
	return nil
}

// canIncrementalUnwind reports whether RestartSyncFromHeight can rewind to
// height by inverse-delta commitment unwind instead of reset+replay.
//
// Requires changeset coverage for every block in (height, currentHead] (i.e.
// the window has not been pruned). The proxy: if height+1's StateTxRange row is
// present, pruning has not yet reached height+1, and — since pruning proceeds
// oldest-first — the entire window [height+1, currentHead] is covered. Normal
// imports only write these rows when HistoryEnabled is true, but the gate checks
// the data rather than the current config so restart can use an existing history
// window if the flag changed between runs.
//
// The current-cycle reward accumulator is intentionally non-rooted and not part
// of commitment changesets. A same-cycle rewind reverses its per-block voter
// rewards explicitly; a range crossing a maintenance boundary falls back to
// reset+replay because maintenance flushes and resets the accumulator.
//
// This is a pure optimization gate: false forces the always-correct reset+replay.
func (bc *BlockChain) canIncrementalUnwind(height, currentHead uint64) bool {
	if height >= currentHead {
		return false // nothing to unwind, or invalid
	}
	if !bc.canIncrementalUnwindCycleRewards(height, currentHead) {
		return false
	}
	// Use height+1's StateTxRange as a coverage proxy. Pruning deletes both the
	// tx-range and its corresponding changeset rows together, so if the tx-range
	// for height+1 is present the changeset window is intact.
	_, ok, err := rawdb.ReadStateTxRange(bc.db, height+1)
	return err == nil && ok
}

// canIncrementalUnwindCycleRewards reports whether the non-rooted pending
// reward accumulator can be reversed alongside the rooted commitment. A
// maintenance boundary flushes and resets the accumulator, so only a range
// wholly contained in one cycle is safe to unwind arithmetically.
func (bc *BlockChain) canIncrementalUnwindCycleRewards(height, currentHead uint64) bool {
	if bc.cycleRewards == nil || len(bc.cycleRewards.rewards) == 0 {
		return true
	}
	target := rawdb.ReadBlock(bc.chaindb, height)
	current := rawdb.ReadBlock(bc.chaindb, currentHead)
	if target == nil || current == nil {
		return false
	}
	targetRoot := rawdb.ReadBlockStateRoot(bc.chaindb, target.Hash())
	currentRoot := rawdb.ReadBlockStateRoot(bc.chaindb, current.Hash())
	if targetRoot == (tcommon.Hash{}) || currentRoot == (tcommon.Hash{}) {
		return false
	}
	targetState, err := bc.openState(targetRoot)
	if err != nil {
		return false
	}
	currentState, err := bc.openState(currentRoot)
	if err != nil {
		return false
	}
	targetCycle := state.LoadDynamicProperties(bc.db, targetState).CurrentCycleNumber()
	currentCycle := state.LoadDynamicProperties(bc.db, currentState).CurrentCycleNumber()
	return targetCycle == currentCycle && currentCycle == bc.cycleRewards.cycle
}

// cycleRewardsAfterIncrementalUnwind reconstructs the pending accumulator at
// target height by subtracting the exact voter portions paid by each removed
// block. All inputs are read from that block's post-state: proposal parameters,
// witness votes, and brokerage therefore match the values used by applyBlock.
// The caller's same-cycle preflight rules out maintenance flush/reset semantics.
// This function is read-only so any unsupported or inconsistent range fails
// before incrementalUnwindTo mutates durable state.
func (bc *BlockChain) cycleRewardsAfterIncrementalUnwind(height, currentHead uint64) (cycleRewardAccumulatorSnapshot, error) {
	if bc.cycleRewards == nil || len(bc.cycleRewards.rewards) == 0 {
		return cycleRewardAccumulatorSnapshot{}, nil
	}

	acc := &cycleRewardAccumulator{
		cycle:   bc.cycleRewards.cycle,
		rewards: copyCycleRewardMap(bc.cycleRewards.rewards),
	}
	for n := currentHead; n > height; n-- {
		block := rawdb.ReadBlock(bc.chaindb, n)
		if block == nil {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d missing", n)
		}
		root := rawdb.ReadBlockStateRoot(bc.chaindb, block.Hash())
		if root == (tcommon.Hash{}) {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d state root missing", n)
		}
		statedb, err := bc.openState(root)
		if err != nil {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: open block %d state: %w", n, err)
		}
		dp := state.LoadDynamicProperties(bc.db, statedb)
		cycle := dp.CurrentCycleNumber()
		if cycle != acc.cycle {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d cycle %d differs from pending cycle %d", n, cycle, acc.cycle)
		}
		if !dp.ChangeDelegation() {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d has pending rewards with change_delegation disabled", n)
		}

		witness := block.WitnessAddress()
		if witness == (tcommon.Address{}) {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d has no witness", n)
		}
		if err := subtractPendingVoterReward(acc, statedb, cycle, witness, dp.WitnessPayPerBlock()); err != nil {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d producer reward: %w", n, err)
		}

		set := buildStandbyWitnessPaySet(bc.buffer, statedb, cycle, dp.ConsensusLogicOptimization())
		if err := subtractPendingStandbyRewards(acc, set, dp.Witness127PayPerBlock()); err != nil {
			return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d standby reward: %w", n, err)
		}

		if dp.AllowTransactionFeePool() {
			infos := rawdb.ReadTransactionInfosByBlock(bc.chaindb, n)
			if len(infos) != len(block.Transactions()) {
				return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d transaction infos=%d transactions=%d", n, len(infos), len(block.Transactions()))
			}
			var packingFee int64
			for _, info := range infos {
				if info != nil {
					packingFee += info.GetPackingFee()
				}
			}
			if err := subtractPendingVoterReward(acc, statedb, cycle, witness, packingFee); err != nil {
				return cycleRewardAccumulatorSnapshot{}, fmt.Errorf("cycle reward unwind: block %d fee reward: %w", n, err)
			}
		}
	}
	return acc.Snapshot(), nil
}

func subtractPendingVoterReward(acc *cycleRewardAccumulator, statedb *state.StateDB, cycle int64, addr tcommon.Address, amount int64) error {
	if amount <= 0 {
		return nil
	}
	brokerage := statedb.ReadCycleBrokerage(cycle, addr.Bytes())
	brokerageAmount := tcommon.JavaDoubleToInt64((float64(brokerage) / 100.0) * float64(amount))
	return subtractPendingCycleReward(acc, addr, amount-brokerageAmount)
}

func subtractPendingStandbyRewards(acc *cycleRewardAccumulator, set *standbyWitnessPaySet, totalPay int64) error {
	if totalPay <= 0 || set == nil || set.voteSum < 1 {
		return nil
	}
	eachVotePay := float64(totalPay) / float64(set.voteSum)
	for _, witness := range set.witnesses {
		pay := tcommon.JavaDoubleToInt64(float64(witness.votes) * eachVotePay)
		if pay <= 0 {
			continue
		}
		brokerageAmount := tcommon.JavaDoubleToInt64((float64(witness.brokerage) / 100.0) * float64(pay))
		if err := subtractPendingCycleReward(acc, witness.addr, pay-brokerageAmount); err != nil {
			return err
		}
	}
	return nil
}

func subtractPendingCycleReward(acc *cycleRewardAccumulator, addr tcommon.Address, amount int64) error {
	if amount <= 0 {
		return nil
	}
	current, ok := acc.rewards[addr]
	if !ok || current < amount {
		return fmt.Errorf("address %x pending reward %d is below removed amount %d", addr, current, amount)
	}
	if current == amount {
		delete(acc.rewards, addr)
	} else {
		acc.rewards[addr] = current - amount
	}
	return nil
}

// incrementalUnwindTo rewinds the chain to target.Number() from currentHead via
// inverse-delta commitment unwind. It is called only when canIncrementalUnwind
// returned true and leaves the chain in byte-equivalent end state to the
// reset+replay path for all namespaces EXCEPT the TAPOS ring (see function
// comment on RestartSyncFromHeight).
func (bc *BlockChain) incrementalUnwindTo(target *types.Block, currentHead uint64, ancient rawdb.AncientWriter, emit func(string, uint64)) error {
	height := target.Number()
	rewoundCycleRewards, err := bc.cycleRewardsAfterIncrementalUnwind(height, currentHead)
	if err != nil {
		return err
	}

	// 1. Flush all buffered layers to disk so UnwindCommitment operates on one
	//    consistent store. WaitForFlushSettled was called by the caller; the
	//    buffer.Discard in the caller has NOT been called yet on the incremental
	//    path, so we do it here after flushing.
	if err := bc.buffer.Flush(bc.db); err != nil {
		return fmt.Errorf("flush buffer before unwind: %w", err)
	}
	bc.buffer.Discard()

	// 2. Truncate ancient store if present (same as reset+replay path).
	if ancient != nil {
		if _, err := ancient.TruncateHead(height + 1); err != nil {
			return fmt.Errorf("truncate ancient head to %d: %w", height+1, err)
		}
		if err := ancient.Sync(); err != nil {
			return fmt.Errorf("sync ancient truncate: %w", err)
		}
	}

	// 3. Collect the orphaned blocks (height, currentHead] — needed for flat
	//    delete and total-tx-count subtraction. Iterate descending for safety.
	orphans := make([]*types.Block, 0, currentHead-height)
	for n := currentHead; n > height; n-- {
		b, err := readRestartSyncBlock(bc.chaindb, n, fmt.Sprintf("block %d missing during unwind", n))
		if err != nil {
			return err
		}
		orphans = append(orphans, b)
	}

	// 4. Inverse-delta unwind of latest tables + staged commitment branches.
	emit("unwind", height)
	expectedRoot, err := readRestartSyncStateRoot(bc.chaindb, target)
	if err != nil {
		return err
	}
	store, err := domains.NewStagedCommitmentStoreWithRepair(bc.db, domains.CommitmentSnapshotRepair{
		Source: bc.stateCommitmentColdHistory,
	}, false)
	if err != nil {
		return fmt.Errorf("open commitment store for unwind %d->%d: %w", currentHead, height, err)
	}
	defer func() { _ = domains.CloseLatestCommitmentStore(store) }()
	if _, err := domains.UnwindCommitment(bc.db, store, currentHead, height, expectedRoot); err != nil {
		return fmt.Errorf("commitment unwind %d->%d: %w", currentHead, height, err)
	}

	// 5. Delete flat block-keyed data + changesets for orphan blocks, and
	//    accumulate the tx count that will be subtracted from the global counter.
	var removedTxs int64
	batch := bc.db.NewBatch()
	for _, b := range orphans {
		removedTxs += int64(len(b.Transactions()))

		// Out-of-band state root (bsr-)
		rawdb.DeleteBlockStateRoot(batch, b.Hash())
		// PBFT sign data (psd-)
		if err := rawdb.DeleteBlockSignData(batch, int64(b.Number())); err != nil {
			return fmt.Errorf("delete block sign data %d: %w", b.Number(), err)
		}
		// Balance trace (btrace-)
		if err := rawdb.DeleteBlockBalanceTrace(batch, int64(b.Number())); err != nil {
			return fmt.Errorf("delete balance trace %d: %w", b.Number(), err)
		}
		// Per-block tx-info-by-block (tib-)
		if err := rawdb.DeleteTransactionInfosByBlock(batch, b.Number()); err != nil {
			return fmt.Errorf("delete tx infos by block %d: %w", b.Number(), err)
		}
		// Per-tx tx-info (ti-) and tx-index (tx-) for each transaction
		for _, tx := range b.Transactions() {
			h := tx.Hash()
			if err := rawdb.DeleteTransactionInfo(batch, h[:]); err != nil {
				return fmt.Errorf("delete tx info %x: %w", h, err)
			}
			if err := rawdb.DeleteTransactionIndex(batch, h[:]); err != nil {
				return fmt.Errorf("delete tx index %x: %w", h, err)
			}
		}
	}
	if err := rewoundCycleRewards.Write(batch); err != nil {
		return fmt.Errorf("write rewound cycle reward accumulator: %w", err)
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("write flat-delete batch: %w", err)
	}

	// 6. Delete changeset + tx-range rows for orphan blocks through the registered
	//    StateDomainChange history-domain hooks (not direct rawdb), so the rewind
	//    path dispatches through the DomainRegistry like the pruner does
	//    (erigon gap #9). These are outside the batch above because the registered
	//    block deleter uses its own iteration loop which does not compose with a
	//    batch.
	histCfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok || histCfg.DeleteHotHistoryBlock == nil || histCfg.DeleteHotHistoryTxRange == nil {
		return errors.New("incremental unwind: StateDomainChange history-domain delete hooks unavailable")
	}
	for _, b := range orphans {
		if err := histCfg.DeleteHotHistoryBlock(bc.db, b.Number()); err != nil {
			return fmt.Errorf("delete state domain changes %d: %w", b.Number(), err)
		}
		if err := histCfg.DeleteHotHistoryTxRange(bc.db, b.Number()); err != nil {
			return fmt.Errorf("delete state tx range %d: %w", b.Number(), err)
		}
	}

	// 7. Accumulators: subtract removed tx count (NOT zero — the reset+replay
	//    path re-accumulates to height's true value; we must match it).
	if removedTxs > 0 {
		cur := rawdb.ReadTotalTransactionCount(bc.db)
		rawdb.WriteTotalTransactionCount(bc.db, cur-removedTxs)
	}

	// 8. latestPbftBlockNum: delete singleton (matches resetMutableSingletons
	//    handling in ResetMutableState / reset+replay path).
	if err := rawdb.DeleteLatestPbftBlockNum(bc.db); err != nil {
		return fmt.Errorf("delete latest pbft block num: %w", err)
	}

	// 9. Head pointer + derived DP mirror + canonical stage rewind + runtime
	//    cache reload.
	//    rewindCanonicalStagePipeline and resetRuntimeStateLocked are the same
	//    final-sequence calls used by the reset+replay path.
	emit("flush", height)
	rawdb.WriteHeadBlockHash(bc.db, target.Hash())
	if err := bc.rewriteDerivedDynPropsAtHead(target, expectedRoot); err != nil {
		return fmt.Errorf("rewrite derived dynamic properties: %w", err)
	}
	if err := rewindCanonicalStagePipeline(bc.db, height, target.Hash()); err != nil {
		return fmt.Errorf("rewind canonical stage progress: %w", err)
	}
	if err := bc.rewindTransactionLookupStageLocked(height, target.Hash()); err != nil {
		return fmt.Errorf("rewind tx lookup stage progress: %w", err)
	}
	if err := bc.rewindStateHistoryIndexStageLocked(height, target.Hash()); err != nil {
		return fmt.Errorf("rewind state history index stage progress: %w", err)
	}
	if err := bc.resetRuntimeStateLocked(target, expectedRoot); err != nil {
		return err
	}
	return nil
}

func readRestartSyncBlock(chain *rawdb.ChainDB, number uint64, missing string) (*types.Block, error) {
	block, ok, err := rawdb.ReadBlockStrict(chain, number)
	if err != nil {
		return nil, fmt.Errorf("read block %d: %w", number, err)
	}
	if !ok {
		return nil, errors.New(missing)
	}
	return block, nil
}

func readRestartSyncStateRoot(chain *rawdb.ChainDB, block *types.Block) (tcommon.Hash, error) {
	root, _, err := rawdb.ReadBlockStateRootStrict(chain, block.Hash())
	if err != nil {
		return tcommon.Hash{}, fmt.Errorf("read state root for block %d (%x): %w", block.Number(), block.Hash(), err)
	}
	return root, nil
}

func (bc *BlockChain) rewriteDerivedDynPropsAtHead(head *types.Block, root tcommon.Hash) error {
	if head == nil {
		return errors.New("head is nil")
	}
	statedb, err := bc.openState(root)
	if err != nil {
		return fmt.Errorf("open head state %x: %w", root, err)
	}
	dp := state.LoadDynamicProperties(bc.db, statedb)
	dp.SetLatestBlockHeaderNumber(int64(head.Number()))
	dp.SetLatestBlockHeaderTimestamp(head.Timestamp())
	dp.SetLatestBlockHeaderHash(head.Hash())

	// latest_solidified_block_num is a derived runtime mirror. During normal
	// block execution it is monotonic, but an offline rewind must be allowed to
	// lower it. Recompute the current active-witness threshold from the rewound
	// rooted witness cursors. This is conservative across maintenance rotations:
	// it cannot exceed the exact historical value, and it recovers naturally as
	// new blocks advance the witness cursors after restart.
	dp.SetLatestSolidifiedBlockNum(solidifiedBlockFromRootedWitnessState(statedb))
	dp.Flush(bc.db)
	return nil
}

func solidifiedBlockFromRootedWitnessState(statedb *state.StateDB) int64 {
	if statedb == nil {
		return 0
	}
	active := statedb.ReadActiveWitnesses()
	if len(active) == 0 {
		return 0
	}
	nums := make([]int64, 0, len(active))
	for _, addr := range active {
		nums = append(nums, statedb.ReadWitnessLatestBlock(addr))
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	pos := int(float64(len(nums)) * 0.3)
	if pos >= len(nums) {
		pos = len(nums) - 1
	}
	return nums[pos]
}

func (bc *BlockChain) resetRuntimeStateLocked(head *types.Block, root tcommon.Hash) error {
	bc.invalidateOrderedCommitPipeline()
	bc.genesisWitnesses = bc.genesisWitnesses[:0]
	for _, gw := range rawdb.ReadGenesisWitnesses(bc.db) {
		bc.genesisWitnesses = append(bc.genesisWitnesses, consensus.GenesisWitnessInfo{
			Address:   gw.Address,
			VoteCount: gw.VoteCount,
		})
	}
	bc.currentBlock.Store(head)
	bc.archiveHead.Store(head)
	bc.lastInsertNano.Store(time.Now().UnixNano())
	bc.khaosDB = NewKhaosDB()
	bc.khaosDB.Start(head)
	bc.activeWitnesses.Store([]tcommon.Address(nil))
	bc.reloadActiveWitnesses(root)
	bc.storeDynPropsCache(state.LoadDynamicProperties(bc.buffer, bc.sysKVAt(root)))
	if err := bc.reloadCycleRewardsFromBuffer(); err != nil {
		return fmt.Errorf("reload cycle reward pending accumulator: %w", err)
	}
	bc.fc = forks.NewForkController(bc.buffer)
	bc.invalidateStandbyPayCache()
	bc.clearSystemAccountCache()
	bc.clearRewardAccountCache()
	bc.clearWitnessBlockCache()
	bc.clearForkStatsCache()
	return nil
}
