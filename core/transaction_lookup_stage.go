package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

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
// It holds chainmu while
// loading and publishing the ETL result so a fork cannot race a stage watermark
// past a different canonical branch. A failed ETL load leaves the previous
// watermark intact; rerunning is idempotent.
func (bc *BlockChain) AdvanceTransactionLookupStage(maxBlocks uint64) (TransactionLookupStageResult, error) {
	if bc == nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: nil blockchain")
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if bc.closed.Load() {
		return TransactionLookupStageResult{}, ErrBlockChainClosed
	}
	if err := bc.ensureTransactionLookupStageLocked(); err != nil {
		return TransactionLookupStageResult{}, err
	}

	finishBlock, finishOK, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.buffer, rawdb.StageFinish, bc.readCanonicalHashStrict)
	if err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: verify finish progress: %w", err)
	}
	if !finishOK {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: missing finish progress")
	}
	lookupBlock, _, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, rawdb.StageTxLookup, bc.readCanonicalHashStrict)
	if err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: verify lookup progress: %w", err)
	}
	if lookupBlock > finishBlock {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: progress %d ahead of finish %d", lookupBlock, finishBlock)
	}
	if lookupBlock == finishBlock {
		return TransactionLookupStageResult{}, nil
	}
	fromBlock := lookupBlock + 1
	toBlock := finishBlock
	if maxBlocks > 0 && toBlock-fromBlock+1 > maxBlocks {
		toBlock = fromBlock + maxBlocks - 1
	}

	rebuilt, err := rawdb.RebuildTransactionLookupFromBlocks(bc.chaindb, bc.db, fromBlock, toBlock, bc.transactionLookupETLOptions)
	if err != nil {
		return TransactionLookupStageResult{}, fmt.Errorf("tx lookup stage: rebuild [%d,%d]: %w", fromBlock, toBlock, err)
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
