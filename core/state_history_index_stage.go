package core

import (
	"errors"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// Keep posting runs compact so duplicate latest-key/block candidates collapse
// before stable-cache pressure grows, matching Erigon's bounded ETL collectors.
const stateHistoryIndexETLDefaultBufferLimit = 8 << 20

// StateHistoryIndexStageResult reports one bounded posting-256 pass.
type StateHistoryIndexStageResult struct {
	Advanced bool
	Rebuilt  *rawdb.RebuildStateHistoryIndexResult
}

// SetStateHistoryIndexETLOptions configures the sorted collector before sync.
func (bc *BlockChain) SetStateHistoryIndexETLOptions(opts etl.Options) {
	if bc == nil {
		return
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	bc.stateHistoryIndexETLOptions = opts
}

// EnsureStateHistoryIndexStage initializes the derived watermark at genesis.
// The posting index has one fresh-sync format and is built forward from block 1.
func (bc *BlockChain) EnsureStateHistoryIndexStage() error {
	if bc == nil {
		return fmt.Errorf("state history index stage: nil blockchain")
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	return bc.ensureStateHistoryIndexStageLocked()
}

func (bc *BlockChain) ensureStateHistoryIndexStageLocked() error {
	if bc == nil || bc.db == nil || bc.chaindb == nil || bc.buffer == nil {
		return fmt.Errorf("state history index stage: unavailable database")
	}
	head := bc.CurrentBlock()
	if head == nil {
		return fmt.Errorf("state history index stage: nil canonical head")
	}
	row, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if err != nil {
		return fmt.Errorf("state history index stage: read progress: %w", err)
	}
	if ok {
		if row.BlockNum > head.Number() {
			if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageStateHistoryIndex, head.Number(), head.Hash()); err != nil {
				return fmt.Errorf("state history index stage: clamp progress to canonical head: %w", err)
			}
			return nil
		}
		if _, _, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageStateHistoryIndex, bc.readCanonicalHashStrict); err != nil {
			return fmt.Errorf("state history index stage: verify progress: %w", err)
		}
		return nil
	}

	baseline := uint64(0)
	hash, found, err := bc.readCanonicalHashStrict(baseline)
	if err != nil {
		return fmt.Errorf("state history index stage: read baseline hash %d: %w", baseline, err)
	}
	if !found {
		return fmt.Errorf("state history index stage: missing baseline block %d", baseline)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageStateHistoryIndex, baseline, hash); err != nil {
		return fmt.Errorf("state history index stage: write baseline: %w", err)
	}
	return nil
}

// AdvanceStateHistoryIndexStage materializes a bounded solidified prefix of
// hash -> block posting rows. Pebble-backed chains pin an MVCC source
// snapshot under chainmu, run the expensive ETL outside it, then reacquire the
// lock to validate and publish the watermark. The un-solidified tail remains
// changeset-only and is served by bounded direct scans.
func (bc *BlockChain) AdvanceStateHistoryIndexStage(maxBlocks uint64) (StateHistoryIndexStageResult, error) {
	return bc.advanceStateHistoryIndexStageInterruptible(1, maxBlocks, nil)
}

func (bc *BlockChain) AdvanceStateHistoryIndexStageInterruptible(maxBlocks uint64, interrupted func() bool) (StateHistoryIndexStageResult, error) {
	return bc.advanceStateHistoryIndexStageInterruptible(1, maxBlocks, interrupted)
}

// AdvanceStateHistoryIndexStageBatchedInterruptible waits until at least
// minBlocks are available, then rebuilds at most maxBlocks. Bulk sync uses a
// non-trivial minimum to amortize collector creation, sorting, and batch sync;
// the completion boundary calls the ordinary method to drain a final suffix.
func (bc *BlockChain) AdvanceStateHistoryIndexStageBatchedInterruptible(minBlocks, maxBlocks uint64, interrupted func() bool) (StateHistoryIndexStageResult, error) {
	return bc.advanceStateHistoryIndexStageInterruptible(minBlocks, maxBlocks, interrupted)
}

func (bc *BlockChain) advanceStateHistoryIndexStageInterruptible(minBlocks, maxBlocks uint64, interrupted func() bool) (StateHistoryIndexStageResult, error) {
	if bc == nil {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: nil blockchain")
	}
	bc.stateHistoryIndexMu.Lock()
	defer bc.stateHistoryIndexMu.Unlock()

	bc.chainmu.Lock()
	if bc.closed.Load() {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, ErrBlockChainClosed
	}
	if bc.config == nil || !bc.config.HistoryEnabled {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, nil
	}
	if err := bc.ensureStateHistoryIndexStageLocked(); err != nil {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, err
	}

	finishBlock, finishOK, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.buffer, rawdb.StageFinish, bc.readCanonicalHashStrict)
	if err != nil {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: verify finish progress: %w", err)
	}
	if !finishOK {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: missing finish progress")
	}
	targetBlock := finishBlock
	if dynProps := bc.cachedDynProps(); dynProps != nil {
		solidified := dynProps.LatestSolidifiedBlockNum()
		if solidified < 0 {
			solidified = 0
		}
		if uint64(solidified) < targetBlock {
			targetBlock = uint64(solidified)
		}
	}
	indexedRow, indexedOK, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if err == nil && indexedOK {
		_, _, err = rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageStateHistoryIndex, bc.readCanonicalHashStrict)
	}
	if err != nil {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: verify index progress: %w", err)
	}
	if !indexedOK {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: missing index progress")
	}
	indexedBlock := indexedRow.BlockNum
	if indexedBlock >= targetBlock {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, nil
	}
	if available := targetBlock - indexedBlock; minBlocks > 1 && available < minBlocks {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, nil
	}
	fromBlock := indexedBlock + 1
	toBlock := targetBlock
	if maxBlocks > 0 && toBlock-fromBlock+1 > maxBlocks {
		toBlock = fromBlock + maxBlocks - 1
	}

	etlOptions := bc.stateHistoryIndexETLOptions
	if etlOptions.BufferLimit <= 0 {
		etlOptions.BufferLimit = stateHistoryIndexETLDefaultBufferLimit
	}

	// Capture only committed layers through the planned boundary. Changesets and
	// tx-range rows are append-only, so a durable base already flushed beyond
	// toBlock is harmless; excluding in-flight/newer overlay layers makes the
	// source topology immutable while canonical import continues.
	snapshot, snapshotErr := bc.buffer.NewReadSnapshotThrough(toBlock)
	if errors.Is(snapshotErr, blockbuffer.ErrReadSnapshotUnsupported) {
		// memorydb and minimal test stores cannot pin MVCC. Preserve the original
		// fully-locked path for those backends.
		rebuilt, rebuildErr := rawdb.RebuildStateHistoryIndexInterruptible(bc.buffer, bc.db, fromBlock, toBlock, etlOptions, bc.readCanonicalHashStrict, interrupted)
		if rebuildErr != nil {
			bc.chainmu.Unlock()
			return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: rebuild [%d,%d]: %w", fromBlock, toBlock, rebuildErr)
		}
		result, publishErr := bc.publishStateHistoryIndexStageLocked(indexedRow, toBlock, rebuilt)
		bc.chainmu.Unlock()
		return result, publishErr
	}
	if snapshotErr != nil {
		bc.chainmu.Unlock()
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: snapshot through %d: %w", toBlock, snapshotErr)
	}
	bc.chainmu.Unlock()

	rebuilt, err := rawdb.RebuildStateHistoryIndexInterruptible(snapshot, bc.db, fromBlock, toBlock, etlOptions, bc.readCanonicalHashStrict, interrupted)
	closeErr := snapshot.Close()
	if err != nil {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: rebuild [%d,%d]: %w", fromBlock, toBlock, err)
	}
	if closeErr != nil {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: close snapshot: %w", closeErr)
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return StateHistoryIndexStageResult{}, ErrBlockChainClosed
	}
	return bc.publishStateHistoryIndexStageLocked(indexedRow, toBlock, rebuilt)
}

func (bc *BlockChain) publishStateHistoryIndexStageLocked(indexedRow rawdb.StageProgress, toBlock uint64, rebuilt *rawdb.RebuildStateHistoryIndexResult) (StateHistoryIndexStageResult, error) {
	currentRow, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if err != nil {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: re-read progress: %w", err)
	}
	if !ok {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: progress disappeared during rebuild")
	}
	if currentRow.BlockNum != indexedRow.BlockNum || currentRow.HasBlockHash != indexedRow.HasBlockHash || currentRow.BlockHash != indexedRow.BlockHash {
		if currentRow.HasBlockHash && currentRow.BlockNum >= toBlock {
			if _, _, verifyErr := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageStateHistoryIndex, bc.readCanonicalHashStrict); verifyErr == nil {
				return StateHistoryIndexStageResult{Rebuilt: rebuilt}, nil
			}
		}
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: progress changed during rebuild from %d/%x to %d/%x", indexedRow.BlockNum, indexedRow.BlockHash, currentRow.BlockNum, currentRow.BlockHash)
	}
	hash, ok, err := bc.readCanonicalHashStrict(toBlock)
	if err != nil {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: read canonical hash at %d: %w", toBlock, err)
	}
	if !ok {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: canonical block %d disappeared", toBlock)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageStateHistoryIndex, toBlock, hash); err != nil {
		return StateHistoryIndexStageResult{}, fmt.Errorf("state history index stage: write progress: %w", err)
	}
	return StateHistoryIndexStageResult{Advanced: true, Rebuilt: rebuilt}, nil
}

func (bc *BlockChain) rewindStateHistoryIndexStageLocked(blockNum uint64, hash tcommon.Hash) error {
	row, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageStateHistoryIndex)
	if err != nil || !ok {
		return err
	}
	if row.BlockNum < blockNum {
		return nil
	}
	if row.BlockNum == blockNum && row.HasBlockHash && row.BlockHash == hash {
		return nil
	}
	return rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageStateHistoryIndex, blockNum, hash)
}
