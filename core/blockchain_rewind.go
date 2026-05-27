package core

import (
	"errors"
	"fmt"
	"time"

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

const restartSyncReplayBatchSize = 100

// RestartSyncProgress is emitted by RestartSyncFromHeight after each major
// phase and replayed block. Block is meaningful for replay/done phases.
type RestartSyncProgress struct {
	Phase  string
	Block  uint64
	Target uint64
}

// RestartSyncFromHeight rewinds the local materialized state to height and
// leaves the chain ready to request height+1 from peers.
//
// This is an offline startup operation. Call it before P2P, producer, PBFT, and
// API hooks are registered; it intentionally replays canonical blocks through
// the same staged range importer used by sync and therefore would otherwise
// re-fire apply hooks.
//
// Fast incremental path: when HistoryEnabled is true and changesets covering
// (height, currentHead] are present, the chain is rewound via inverse-delta
// commitment unwind (domains.UnwindCommitment) rather than reset+genesis+replay.
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

	current := bc.CurrentBlock()
	if current == nil {
		return errors.New("restart sync: current block is nil")
	}
	if height > current.Number() {
		return fmt.Errorf("restart sync: target height %d exceeds current head %d", height, current.Number())
	}
	target := rawdb.ReadBlock(bc.chaindb, height)
	if target == nil {
		return fmt.Errorf("restart sync: canonical block %d not found", height)
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

	// Fast incremental path: skip reset+replay when history is on and changesets
	// cover the full (height, currentHead] window.
	if bc.canIncrementalUnwind(height, current.Number()) {
		if err := bc.incrementalUnwindTo(target, current.Number(), ancient, emit); err != nil {
			return fmt.Errorf("restart sync: incremental unwind to %d: %w", height, err)
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
	bc.resetRuntimeStateLocked(genesisBlock, genesisRoot)

	for start := uint64(1); start <= height; {
		end := start + restartSyncReplayBatchSize - 1
		if end < start || end > height {
			end = height
		}
		blocks := make([]*types.Block, 0, end-start+1)
		for n := start; n <= end; n++ {
			block := rawdb.ReadBlock(bc.chaindb, n)
			if block == nil {
				return fmt.Errorf("restart sync: block %d not found during replay", n)
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
		if err := bc.insertBlocksLocked(blocks); err != nil {
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
	bc.resetRuntimeStateLocked(final, bc.HeadStateRoot())
	emit("done", height)
	return nil
}

// canIncrementalUnwind reports whether RestartSyncFromHeight can rewind to
// height by inverse-delta commitment unwind instead of reset+replay.
//
// Requires HistoryEnabled (so changesets were captured) AND changeset coverage
// for every block in (height, currentHead] (i.e. the window has not been
// pruned). The proxy: if height+1's StateTxRange row is present, pruning has
// not yet reached height+1, and — since pruning proceeds oldest-first — the
// entire window [height+1, currentHead] is covered.
//
// This is a pure optimization gate: false forces the always-correct reset+replay.
func (bc *BlockChain) canIncrementalUnwind(height, currentHead uint64) bool {
	if bc.config == nil || !bc.config.HistoryEnabled {
		return false
	}
	if height >= currentHead {
		return false // nothing to unwind, or invalid
	}
	// Use height+1's StateTxRange as a coverage proxy. Pruning deletes both the
	// tx-range and its corresponding changeset rows together, so if the tx-range
	// for height+1 is present the changeset window is intact.
	_, ok, err := rawdb.ReadStateTxRange(bc.db, height+1)
	return err == nil && ok
}

// incrementalUnwindTo rewinds the chain to target.Number() from currentHead via
// inverse-delta commitment unwind. It is called only when canIncrementalUnwind
// returned true and leaves the chain in byte-equivalent end state to the
// reset+replay path for all namespaces EXCEPT the TAPOS ring (see function
// comment on RestartSyncFromHeight).
func (bc *BlockChain) incrementalUnwindTo(target *types.Block, currentHead uint64, ancient rawdb.AncientWriter, emit func(string, uint64)) error {
	height := target.Number()

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
		b := rawdb.ReadBlock(bc.chaindb, n)
		if b == nil {
			return fmt.Errorf("block %d missing during unwind", n)
		}
		orphans = append(orphans, b)
	}

	// 4. Inverse-delta unwind of latest tables + staged commitment branches.
	emit("unwind", height)
	expectedRoot := rawdb.ReadBlockStateRoot(bc.chaindb, target.Hash())
	store := domains.NewStagedCommitmentStore(bc.db)
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

	// 9. Head pointer + canonical stage rewind + runtime cache reload.
	//    rewindCanonicalStagePipeline and resetRuntimeStateLocked are the same
	//    final-sequence calls used by the reset+replay path.
	emit("flush", height)
	rawdb.WriteHeadBlockHash(bc.db, target.Hash())
	if err := rewindCanonicalStagePipeline(bc.db, height, target.Hash()); err != nil {
		return fmt.Errorf("rewind canonical stage progress: %w", err)
	}
	bc.resetRuntimeStateLocked(target, expectedRoot)
	return nil
}

func (bc *BlockChain) resetRuntimeStateLocked(head *types.Block, root tcommon.Hash) {
	bc.genesisWitnesses = bc.genesisWitnesses[:0]
	for _, gw := range rawdb.ReadGenesisWitnesses(bc.db) {
		bc.genesisWitnesses = append(bc.genesisWitnesses, consensus.GenesisWitnessInfo{
			Address:   gw.Address,
			VoteCount: gw.VoteCount,
		})
	}
	bc.currentBlock.Store(head)
	bc.lastInsertNano.Store(time.Now().UnixNano())
	bc.khaosDB = NewKhaosDB()
	bc.khaosDB.Start(head)
	bc.activeWitnesses.Store([]tcommon.Address(nil))
	bc.reloadActiveWitnesses(root)
	bc.storeDynPropsCache(state.LoadDynamicProperties(bc.buffer, bc.sysKVAt(root)))
	bc.fc = forks.NewForkController(bc.buffer)
	bc.invalidateStandbyPayCache()
	bc.clearSystemAccountCache()
	bc.clearRewardAccountCache()
	bc.clearWitnessBlockCache()
	bc.clearForkStatsCache()
}
