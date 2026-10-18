package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	"io"
)

// materializeForkLCA repairs only the part already flushed past the LCA. The
// ordinary unflushed fork continues to use immutable-layer discard.
// Caller holds chainmu and has drained the commit/flush workers.
func (bc *BlockChain) materializeForkLCA(target *types.Block) error {
	durable, present, err := rawdb.ReadStateCommitmentHead(bc.db)
	if err != nil {
		return err
	}
	if !present {
		return rawdb.ErrStateCommitmentHead
	}
	if durable.BlockNum <= target.Number() {
		return nil
	}
	root, err := readRestartSyncStateRoot(bc.chaindb, target)
	if err != nil {
		return err
	}
	if target.Number() == 0 {
		root = rawdb.ReadGenesisStateRoot(bc.db)
	}
	savedKhaos := bc.khaosDB
	defer func() { bc.khaosDB = savedKhaos }()
	if bc.config != nil && bc.config.HistoryEnabled && bc.canIncrementalUnwind(target.Number(), bc.CurrentBlock().Number()) {
		return bc.incrementalUnwindTo(target, bc.CurrentBlock().Number(), nil, func(string, uint64) {})
	}
	return bc.replayForkLCALocked(target, root)
}

// replayForkLCALocked is the rare missing-history or cross-maintenance path. It reuses the normal in-place
// reset/stored replay pipeline, guarded by a synced unpublished marker. It does
// not clone a full database, persist historical accumulators, or emit hooks for
// the already canonical prefix. Any uncertain storage result poisons the node.
func (bc *BlockChain) replayForkLCALocked(target *types.Block, expectedRoot [32]byte) (retErr error) {
	// Fresh production uses direct history. Legacy staging ownership cannot be
	// reset live; reject before a marker or any destructive mutation.
	if bc.historyStaging.Load() != nil {
		return errors.New("fork replay with legacy history staging is unsupported; current cut unchanged")
	}
	wireHash, payload, present, err := rawdb.ReadStateGenesisRecipe(bc.db)
	if err != nil {
		return err
	}
	if !present {
		return errors.New("fresh genesis replay recipe missing")
	}
	var genesis params.Genesis
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&genesis); err != nil {
		return err
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return errors.New("genesis recipe has trailing JSON")
	}
	if genesis.Config == nil {
		return errors.New("genesis recipe lacks config")
	}
	wire, err := GenesisWireBlock(&genesis)
	if err != nil {
		return err
	}
	if wire.Hash() != wireHash || wireHash != bc.genesisBlock.Hash() {
		return errors.New("genesis recipe wire identity mismatch")
	}
	if _, err = preflightRestartSyncBlocks(bc.chaindb, target.Number(), target.Hash()); err != nil {
		return err
	}
	// Only the small genesis construction is off-store. The full chain state is
	// reconstructed in-place using the existing bounded stored-replay batches.
	scratch := rawdb.NewMemoryDatabase()
	defer scratch.Close()
	block, genRoot, dp, err := genesisBlockAndStateRoot(&genesis, state.NewDatabase(rawdb.WrapKeyValueStore(scratch)))
	if err != nil {
		return err
	}
	if genRoot != rawdb.ReadGenesisStateRoot(bc.db) {
		return errors.New("genesis recipe internal state mismatch")
	}
	if err = writeGenesisMaterializedState(scratch, &genesis, block, genRoot, dp); err != nil {
		return err
	}
	if err = rawdb.WriteCanonicalStageProgressWithHash(scratch, 0, block.Hash()); err != nil {
		return err
	}
	attempted := false
	defer func() {
		if attempted && retErr != nil {
			failure := retErr
			bc.commitErr.CompareAndSwap(nil, &failure)
		}
	}()
	attempted = true
	if err = rawdb.WriteStateCommitmentRestoreMarker(bc.db); err != nil {
		return err
	}
	if err = syncKeyValueStore(bc.db); err != nil {
		return err
	}
	oldAsync, oldSuppress := bc.asyncCommit, bc.suppressReplayHooks
	bc.asyncCommit = false
	bc.suppressReplayHooks = true
	defer func() { bc.asyncCommit = oldAsync; bc.suppressReplayHooks = oldSuppress }()
	bc.buffer.Discard()
	if err = rawdb.ResetMutableStateKeepingRestoreMarker(bc.db); err != nil {
		return err
	}
	batch := bc.db.NewBatch()
	defer batch.Close()
	it := scratch.NewIterator(nil, nil)
	for it.Next() {
		if err = batch.Put(it.Key(), it.Value()); err != nil {
			it.Release()
			return err
		}
	}
	err = it.Error()
	it.Release()
	if err != nil {
		return err
	}
	if err = batch.Write(); err != nil {
		return err
	}
	if err = bc.resetRuntimeStateLocked(block, genRoot); err != nil {
		return err
	}
	// These caches describe the discarded future, not the replayed prefix.
	bc.proposalCache.reset()
	bc.versionPassCache.Reset()
	for start := uint64(1); start <= target.Number(); {
		end := start + restartSyncReplayBatchSize - 1
		if end < start || end > target.Number() {
			end = target.Number()
		}
		blocks := make([]*types.Block, 0, end-start+1)
		for n := start; n <= end; n++ {
			b, err := readRestartSyncBlock(bc.chaindb, n, fmt.Sprintf("fork replay body %d missing", n))
			if err != nil {
				return err
			}
			blocks = append(blocks, b)
		}
		if err = bc.insertBlocksLockedMode(blocks, true); err != nil {
			return err
		}
		start = end + 1
	}
	bc.WaitForFlushSettled()
	if p := bc.flushErr.Load(); p != nil {
		return *p
	}
	if err = bc.buffer.Flush(bc.db); err != nil {
		return err
	}
	h, present, err := rawdb.ReadStateCommitmentHead(bc.db)
	if err != nil {
		return err
	}
	if !present || h.BlockHash != target.Hash() || h.Root != expectedRoot {
		return errors.New("NoHistory fork replay target cut mismatch")
	}
	if err = syncKeyValueStore(bc.db); err != nil {
		return err
	}
	if err = rawdb.DeleteStateCommitmentRestoreMarker(bc.db); err != nil {
		return err
	}
	if err = syncKeyValueStore(bc.db); err != nil {
		return err
	}
	return nil
}
