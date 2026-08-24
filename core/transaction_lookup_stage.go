package core

import (
	"errors"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

// Keep each in-memory tx-hash sort comfortably below the 64 MiB generic ETL
// default. TxLookup batches can contain around a million transactions; smaller
// runs bound both stop latency and stable-cache pressure before the k-way load.
const transactionLookupETLDefaultBufferLimit = 8 << 20

// TransactionLookupStageResult reports one bounded TxLookup stage pass.
// Advanced is false when the derived index already covers the verified Finish
// boundary. Rebuilt is nil in that case.
type TransactionLookupStageResult struct {
	Advanced bool
	Rebuilt  *rawdb.RebuildTransactionLookupResult
}

// SetTransactionLookupETLOptions configures the sorted ETL collector used by
// the recoverable TxLookup stage. It is intended to run during node setup,
// before sync begins; zero values retain the collector's defaults.
func (bc *BlockChain) SetTransactionLookupETLOptions(opts etl.Options) {
	if bc == nil {
		return
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	bc.transactionLookupETLOptions = opts
}

// EnsureTransactionLookupStage initializes the derived tx lookup watermark for
// a database written before the staged sync path existed. Older versions wrote
// every tx- row synchronously, so the stored canonical head is a safe baseline.
// Once initialized, the row is never advanced by this method: lagging progress
// is recovered by AdvanceTransactionLookupStage.
func (bc *BlockChain) EnsureTransactionLookupStage() error {
	if bc == nil {
		return fmt.Errorf("tx lookup stage: nil blockchain")
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	return bc.ensureTransactionLookupStageLocked()
}

func (bc *BlockChain) ensureTransactionLookupStageLocked() error {
	if bc == nil || bc.db == nil || bc.chaindb == nil {
		return fmt.Errorf("tx lookup stage: unavailable database")
	}
	row, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageTxLookup)
	if err != nil {
		return fmt.Errorf("tx lookup stage: read progress: %w", err)
	}
	head := bc.CurrentBlock()
	if head == nil {
		return fmt.Errorf("tx lookup stage: nil canonical head")
	}
	if ok {
		// Metadata and derived rows may be durable ahead of the stored head after
		// a crash: block import writes those rows before the buffered head layer
		// is flushed. This is already a supported recovery shape for block/tx
		// metadata. Clamp the derived watermark to the durable head so the next
		// pass deterministically rebuilds any not-yet-published suffix.
		if row.BlockNum > head.Number() {
			if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageTxLookup, head.Number(), head.Hash()); err != nil {
				return fmt.Errorf("tx lookup stage: clamp progress to canonical head: %w", err)
			}
			return nil
		}
		_, _, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageTxLookup, bc.readCanonicalHashStrict)
		if err != nil {
			return fmt.Errorf("tx lookup stage: verify progress: %w", err)
		}
		return nil
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageTxLookup, head.Number(), head.Hash()); err != nil {
		return fmt.Errorf("tx lookup stage: write baseline: %w", err)
	}
	return nil
}

// AdvanceTransactionLookupStage rebuilds a bounded prefix of the recoverable
// tx-hash lookup index from canonical block bodies. The authoritative per-block
// TransactionRet rows make individual `ti-` materialization unnecessary:
// receipt-by-ID reads resolve through this lookup then tib-.
// Pebble-backed chains pin an MVCC body snapshot under chainmu, run the
// expensive decode/sort/load outside it, then reacquire the lock to validate
// and publish the watermark. A failed ETL load leaves the previous watermark
// intact; rerunning is idempotent.
func (bc *BlockChain) AdvanceTransactionLookupStage(maxBlocks uint64) (TransactionLookupStageResult, error) {
	return bc.AdvanceTransactionLookupStageInterruptible(maxBlocks, nil)
}

// AdvanceTransactionLookupStageInterruptible is the sync-service form of the
// bounded stage pass. interrupted is polled between block rows; cancellation
// abandons the ETL collector before any lookup rows or stage watermark are
// published.
func (bc *BlockChain) AdvanceTransactionLookupStageInterruptible(maxBlocks uint64, interrupted func() bool) (TransactionLookupStageResult, error) {
	if bc == nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: nil blockchain")
	}
	bc.transactionLookupStageMu.Lock()
	defer bc.transactionLookupStageMu.Unlock()

	bc.chainmu.Lock()
	if bc.closed.Load() {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, ErrBlockChainClosed
	}
	if err := bc.ensureTransactionLookupStageLocked(); err != nil {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, err
	}

	finishBlock, finishOK, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.buffer, rawdb.StageFinish, bc.readCanonicalHashStrict)
	if err != nil {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: verify finish progress: %w", err)
	}
	if !finishOK {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: missing finish progress")
	}
	lookupRow, lookupOK, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageTxLookup)
	if err == nil && lookupOK {
		_, _, err = rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageTxLookup, bc.readCanonicalHashStrict)
	}
	if err != nil {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: verify lookup progress: %w", err)
	}
	if !lookupOK {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: missing lookup progress")
	}
	lookupBlock := lookupRow.BlockNum
	if lookupBlock > finishBlock {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: progress %d ahead of finish %d", lookupBlock, finishBlock)
	}
	if lookupBlock == finishBlock {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, nil
	}
	fromBlock := lookupBlock + 1
	toBlock := finishBlock
	if maxBlocks > 0 && toBlock-fromBlock+1 > maxBlocks {
		toBlock = fromBlock + maxBlocks - 1
	}

	etlOptions := bc.transactionLookupETLOptions
	if etlOptions.BufferLimit <= 0 {
		etlOptions.BufferLimit = transactionLookupETLDefaultBufferLimit
	}

	snapshot, snapshotErr := bc.buffer.NewReadSnapshotThrough(toBlock)
	if errors.Is(snapshotErr, blockbuffer.ErrReadSnapshotUnsupported) {
		rebuilt, rebuildErr := rawdb.RebuildTransactionLookupFromBlocksInterruptible(bc.chaindb, bc.db, fromBlock, toBlock, etlOptions, interrupted)
		if rebuildErr != nil {
			bc.chainmu.Unlock()
			return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: rebuild [%d,%d]: %w", fromBlock, toBlock, rebuildErr)
		}
		result, publishErr := bc.publishTransactionLookupStageLocked(lookupRow, toBlock, rebuilt)
		bc.chainmu.Unlock()
		return result, publishErr
	}
	if snapshotErr != nil {
		bc.chainmu.Unlock()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: snapshot through %d: %w", toBlock, snapshotErr)
	}
	view := rawdb.NewChainDBReadView(snapshot, bc.chaindb.AncientReader)
	bc.chainmu.Unlock()

	collected, err := rawdb.CollectTransactionLookupFromBlocksInterruptible(view, fromBlock, toBlock, etlOptions, interrupted)
	closeErr := snapshot.Close()
	if err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: collect [%d,%d]: %w", fromBlock, toBlock, err)
	}
	if closeErr != nil {
		collected.Close()
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: close snapshot: %w", closeErr)
	}
	defer collected.Close()

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return TransactionLookupStageResult{}, ErrBlockChainClosed
	}
	if err := bc.validateTransactionLookupStagePublishLocked(lookupRow, toBlock, collected.Result.ToBlockHash); err != nil {
		return TransactionLookupStageResult{}, err
	}
	if err := collected.LoadInterruptible(bc.db, interrupted); err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: load [%d,%d]: %w", fromBlock, toBlock, err)
	}
	return bc.publishTransactionLookupStageLocked(lookupRow, toBlock, collected.Result)
}

func (bc *BlockChain) validateTransactionLookupStagePublishLocked(lookupRow rawdb.StageProgress, toBlock uint64, expectedHash tcommon.Hash) error {
	currentRow, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageTxLookup)
	if err != nil {
		return fmt.Errorf("tx lookup stage: re-read progress before load: %w", err)
	}
	if !ok {
		return fmt.Errorf("tx lookup stage: progress disappeared before load")
	}
	if currentRow.BlockNum != lookupRow.BlockNum || currentRow.HasBlockHash != lookupRow.HasBlockHash || currentRow.BlockHash != lookupRow.BlockHash {
		return fmt.Errorf("tx lookup stage: progress changed before load from %d/%x to %d/%x", lookupRow.BlockNum, lookupRow.BlockHash, currentRow.BlockNum, currentRow.BlockHash)
	}
	if canonicalHash, ok, err := bc.readCanonicalHashStrict(toBlock); err != nil {
		return fmt.Errorf("tx lookup stage: read canonical hash at %d before load: %w", toBlock, err)
	} else if !ok {
		return fmt.Errorf("tx lookup stage: canonical block %d disappeared before load", toBlock)
	} else if canonicalHash != expectedHash {
		return fmt.Errorf("tx lookup stage: canonical block %d changed before load from %x to %x", toBlock, expectedHash, canonicalHash)
	}
	return nil
}

func (bc *BlockChain) publishTransactionLookupStageLocked(lookupRow rawdb.StageProgress, toBlock uint64, rebuilt *rawdb.RebuildTransactionLookupResult) (TransactionLookupStageResult, error) {
	currentRow, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageTxLookup)
	if err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: re-read progress: %w", err)
	}
	if !ok {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: progress disappeared during rebuild")
	}
	if currentRow.BlockNum != lookupRow.BlockNum || currentRow.HasBlockHash != lookupRow.HasBlockHash || currentRow.BlockHash != lookupRow.BlockHash {
		if currentRow.HasBlockHash && currentRow.BlockNum >= toBlock {
			if _, _, verifyErr := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageTxLookup, bc.readCanonicalHashStrict); verifyErr == nil {
				return TransactionLookupStageResult{Rebuilt: rebuilt}, nil
			}
		}
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: progress changed during rebuild from %d/%x to %d/%x", lookupRow.BlockNum, lookupRow.BlockHash, currentRow.BlockNum, currentRow.BlockHash)
	}
	hash, ok, err := bc.readCanonicalHashStrict(toBlock)
	if err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: read canonical hash at %d: %w", toBlock, err)
	}
	if !ok {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: canonical block %d disappeared", toBlock)
	}
	if err := rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageTxLookup, toBlock, hash); err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: write progress: %w", err)
	}
	return TransactionLookupStageResult{Advanced: true, Rebuilt: rebuilt}, nil
}

func (bc *BlockChain) rewindTransactionLookupStageLocked(blockNum uint64, hash tcommon.Hash) error {
	row, ok, err := rawdb.ReadStageProgressRow(bc.db, rawdb.StageTxLookup)
	if err != nil || !ok {
		return err
	}
	if row.BlockNum < blockNum {
		return nil
	}
	if row.BlockNum == blockNum && row.HasBlockHash && row.BlockHash == hash {
		return nil
	}
	return rawdb.WriteStageProgressWithHash(bc.db, rawdb.StageTxLookup, blockNum, hash)
}

func (bc *BlockChain) readCanonicalHashStrict(number uint64) (tcommon.Hash, bool, error) {
	return rawdb.ReadBlockHashByNumberStrict(bc.chaindb, number)
}
