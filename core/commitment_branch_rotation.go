package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

var (
	commitmentBranchRotationStartCounter    = metrics.NewRegisteredCounter("state/commitment/branch_rotation/starts", nil)
	commitmentBranchRotationResumeCounter   = metrics.NewRegisteredCounter("state/commitment/branch_rotation/resumes", nil)
	commitmentBranchRotationDeferredCounter = metrics.NewRegisteredCounter("state/commitment/branch_rotation/deferred", nil)
	commitmentBranchRotationRejectCounter   = metrics.NewRegisteredCounter("state/commitment/branch_rotation/rejected", nil)
	commitmentBranchRotationCompleteCounter = metrics.NewRegisteredCounter("state/commitment/branch_rotation/completed", nil)
)

// BeginCommitmentBranchRotation freezes the complete legacy branch table at
// the current canonical head and redirects subsequent branch writes into a
// generation-specific delta. The short chain barrier drains both asynchronous
// workers and makes the marker durable before import resumes.
func (bc *BlockChain) BeginCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error) {
	return bc.beginCommitmentBranchRotation(false)
}

// BeginInitialCommitmentBranchRotation only creates or resumes generation one
// before any immutable base has been accepted. It never starts a successor.
func (bc *BlockChain) BeginInitialCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error) {
	return bc.beginCommitmentBranchRotation(true)
}

func (bc *BlockChain) beginCommitmentBranchRotation(initialOnly bool) (rawdb.CommitmentBranchRotation, bool, error) {
	if bc == nil || bc.db == nil {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: nil blockchain or database")
	}
	// Writer-before-chainmu is the load-bearing lock order: existing sessions
	// retain the read side until their reusable scope has closed, and Finish
	// needs chainmu to do that. Once this returns, no live session can still
	// target a layer that the rotation's Discard will detach.
	bc.insertSessionGate.Lock()
	defer bc.insertSessionGate.Unlock()
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("begin commitment branch rotation: async commit failed: %w", *errPtr)
	}
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("begin commitment branch rotation: async flush failed: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("begin commitment branch rotation: flush buffer: %w", err)
	}
	base, based, err := rawdb.ReadCommitmentBranchBase(bc.db)
	if err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	}
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(bc.db)
	if err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	}
	if initialOnly {
		if based && rotating || rotating && rotation.Generation != 1 {
			return rawdb.CommitmentBranchRotation{}, false, errors.New("core: initial commitment base cannot resume a successor rotation")
		}
		if based {
			return rawdb.CommitmentBranchRotation{}, false, nil
		}
	}
	if rotating {
		if based && (rotation.Generation != base.Generation+1 || rotation.Generation == 0) {
			return rawdb.CommitmentBranchRotation{}, false,
				fmt.Errorf("core: rotation generation %d does not follow base %d", rotation.Generation, base.Generation)
		}
		bc.buffer.Discard()
		bc.invalidateOrderedCommitPipeline()
		commitmentBranchRotationResumeCounter.Inc(1)
		return rotation, true, nil
	}
	if based {
		// Idempotently finish the second crash window. Once the base marker is
		// durable, legacy is never authoritative again and may be reclaimed on
		// any later lifecycle pass. Avoid adding an empty range tombstone on
		// every later periodic rotation once that cleanup is complete.
		legacyRows, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(bc.db)
		if err != nil {
			return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("inspect commitment branch base cleanup: %w", err)
		}
		if legacyRows {
			if err := rawdb.DeleteCommitmentBranches(bc.db); err != nil {
				return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("resume commitment branch base cleanup: %w", err)
			}
			if err := syncKeyValueStore(bc.db); err != nil {
				return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("sync resumed commitment branch base cleanup: %w", err)
			}
		}
		if err := rawdb.DeleteCommitmentBranchDeltaGenerationsExcept(bc.db, base.Generation); err != nil {
			return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("resume commitment branch delta cleanup: %w", err)
		}
		if base.Generation == ^uint64(0) {
			return rawdb.CommitmentBranchRotation{}, false, errors.New("core: commitment branch generation exhausted")
		}
	}

	head := bc.CurrentBlock()
	if head == nil {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: cannot rotate commitment branches without a current head")
	}
	txNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(bc.db, head.Number())
	if err != nil {
		return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("begin commitment branch rotation: resolve head tx: %w", err)
	}
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(bc.db)
	if err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	}
	if !ok {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: cannot rotate missing latest commitment root")
	}
	if root == (common.Hash{}) {
		bc.buffer.Discard()
		return rawdb.CommitmentBranchRotation{}, false, nil
	}
	if based {
		store, err := statedomains.NewStagedCommitmentStoreWithRepair(bc.db, statedomains.CommitmentSnapshotRepair{Source: bc.stateCommitmentColdHistory}, false)
		if err != nil {
			return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("core: open commitment base before rotation: %w", err)
		}
		present, presentErr := store.RootNodePresent(root)
		closeErr := statedomains.CloseLatestCommitmentStore(store)
		if presentErr != nil {
			return rawdb.CommitmentBranchRotation{}, false, presentErr
		}
		if closeErr != nil {
			return rawdb.CommitmentBranchRotation{}, false, closeErr
		}
		if !present {
			return rawdb.CommitmentBranchRotation{}, false, errors.New("core: cannot rotate inconsistent commitment base and delta")
		}
	} else if _, rootBranchOK, err := rawdb.ReadCommitmentBranchNoCopy(bc.db, nil); err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	} else if !rootBranchOK {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: cannot rotate missing commitment root branch")
	}
	generation := uint64(1)
	if based {
		generation = base.Generation + 1
	}
	rotation = rawdb.CommitmentBranchRotation{
		Generation:    generation,
		SnapshotTxNum: txNum,
		Root:          root,
		BlockNum:      head.Number(),
		BlockHash:     head.Hash(),
	}
	batch := bc.db.NewBatch()
	if err := rawdb.WriteCommitmentBranchRotation(batch, rotation); err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	}
	if err := batch.Write(); err != nil {
		return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("begin commitment branch rotation: write marker: %w", err)
	}
	// The marker is visible after batch.Write even if the following durability
	// sync reports an error. Switch all in-process readers before that call so a
	// recoverable I/O failure cannot resume legacy writes into the frozen table.
	bc.buffer.Discard()
	bc.invalidateOrderedCommitPipeline()
	if err := syncKeyValueStore(bc.db); err != nil {
		return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("begin commitment branch rotation: sync marker: %w", err)
	}
	commitmentBranchRotationStartCounter.Inc(1)
	log.Info("Commitment branch rotation started", "generation", rotation.Generation,
		"block", rotation.BlockNum, "tx", rotation.SnapshotTxNum, "root", rotation.Root)
	return rotation, true, nil
}

// CleanupAcceptedInitialCommitmentBranchBase closes the post-accept crash
// window without rotating again. The exact accepted snapshot must be supplied;
// neither a visible but unsynced marker nor a missing base authorizes deletion.
// This entry point is deliberately restricted to the first-generation layout.
func (bc *BlockChain) CleanupAcceptedInitialCommitmentBranchBase(ctx context.Context, proof *snapshots.VerifiedCommitmentBranchBase) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := proof.Recheck(proof.Rotation()); err != nil {
		return false, err
	}
	if err := verifyCommitmentBranchRotationSnapshot(proof.Manager(), proof.Rotation()); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if bc == nil || bc.db == nil {
		return false, errors.New("core: nil blockchain or database")
	}
	bc.insertSessionGate.Lock()
	defer bc.insertSessionGate.Unlock()
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return false, fmt.Errorf("cleanup initial commitment base: async commit failed: %w", *errPtr)
	}
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return false, fmt.Errorf("cleanup initial commitment base: async flush failed: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return false, fmt.Errorf("cleanup initial commitment base: flush buffer: %w", err)
	}
	base, based, err := rawdb.ReadCommitmentBranchBase(bc.db)
	if err != nil {
		return false, err
	}
	_, rotating, err := rawdb.ReadCommitmentBranchRotation(bc.db)
	if err != nil {
		return false, err
	}
	if !based || base.Generation != 1 || rotating {
		return false, errors.New("core: initial commitment base cleanup requires an accepted generation-one base")
	}
	legacyRows, err := rawdb.LegacyCommitmentBranchKeyspace().HasRows(bc.db)
	if err != nil || !legacyRows {
		return false, err
	}
	rotation := rawdb.CommitmentBranchRotation{
		Generation: base.Generation, SnapshotTxNum: base.SnapshotTxNum, Root: base.Root,
		BlockNum: base.BlockNum, BlockHash: base.BlockHash,
	}
	if err := proof.Recheck(rotation); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return false, fmt.Errorf("cleanup initial commitment base: sync accepted marker: %w", err)
	}
	if err := rawdb.DeleteCommitmentBranches(bc.db); err != nil {
		return false, fmt.Errorf("cleanup initial commitment base: remove legacy branches: %w", err)
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return false, fmt.Errorf("cleanup initial commitment base: sync legacy cleanup: %w", err)
	}
	log.Info("Accepted initial commitment branch base cleanup completed", "block", base.BlockNum, "tx", base.SnapshotTxNum)
	return true, nil
}

// CompleteCommitmentBranchRotation atomically promotes a verified immutable
// snapshot, then removes the now-covered legacy table or prior delta. A crash
// before the marker swap resumes current delta -> frozen delta/legacy -> base;
// a crash after it resumes current delta -> snapshot, whether or not cleanup
// had finished.
func (bc *BlockChain) CompleteCommitmentBranchRotation(rotation rawdb.CommitmentBranchRotation, mgr *snapshots.Manager) error {
	return bc.completeCommitmentBranchRotation(context.Background(), rotation, mgr, nil)
}

func (bc *BlockChain) CompleteInitialCommitmentBranchRotation(ctx context.Context, rotation rawdb.CommitmentBranchRotation, proof *snapshots.VerifiedCommitmentBranchBase) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rotation.Generation != 1 {
		return errors.New("core: initial commitment base requires generation one")
	}
	if err := proof.Recheck(rotation); err != nil {
		return err
	}
	if err := verifyCommitmentBranchRotationSnapshot(proof.Manager(), rotation); err != nil {
		return err
	}
	return bc.completeCommitmentBranchRotation(ctx, rotation, proof.Manager(), proof)
}

func (bc *BlockChain) completeCommitmentBranchRotation(ctx context.Context, rotation rawdb.CommitmentBranchRotation, mgr *snapshots.Manager, proof *snapshots.VerifiedCommitmentBranchBase) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if bc == nil || bc.db == nil {
		return errors.New("core: nil blockchain or database")
	}
	bc.insertSessionGate.Lock()
	defer bc.insertSessionGate.Unlock()
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if errPtr := bc.commitErr.Load(); errPtr != nil {
		return fmt.Errorf("complete commitment branch rotation: async commit failed: %w", *errPtr)
	}
	if errPtr := bc.flushErr.Load(); errPtr != nil {
		return fmt.Errorf("complete commitment branch rotation: async flush failed: %w", *errPtr)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		return fmt.Errorf("complete commitment branch rotation: flush buffer: %w", err)
	}
	stored, ok, err := rawdb.ReadCommitmentBranchRotation(bc.db)
	if err != nil {
		return err
	}
	if !ok || stored != rotation {
		return errors.New("core: commitment branch rotation marker changed before completion")
	}
	priorBase, hadBase, err := rawdb.ReadCommitmentBranchBase(bc.db)
	if err != nil {
		return err
	}
	if proof != nil {
		if hadBase {
			return errors.New("core: initial commitment base cannot replace an accepted base")
		}
		if err := proof.Recheck(rotation); err != nil {
			return err
		}
	}
	if hadBase && (rotation.Generation != priorBase.Generation+1 || rotation.Generation == 0) {
		return fmt.Errorf("core: rotation generation %d does not follow base %d", rotation.Generation, priorBase.Generation)
	}
	if solidified := bc.cachedDynProps().LatestSolidifiedBlockNum(); solidified < 0 || uint64(solidified) < rotation.BlockNum {
		commitmentBranchRotationDeferredCounter.Inc(1)
		return snapshots.ErrCommitmentBranchRotationNotSolidified
	}
	canonical, canonicalOK, err := rawdb.ReadBlockHashByNumberStrict(bc.chaindb, rotation.BlockNum)
	if err != nil {
		return fmt.Errorf("complete commitment branch rotation: canonical boundary: %w", err)
	}
	if !canonicalOK || canonical != rotation.BlockHash {
		if err := ctx.Err(); err != nil {
			return err
		}
		commitmentBranchRotationRejectCounter.Inc(1)
		if rebuildErr := bc.rebuildCommitmentBranchesAfterRejectedRotation(); rebuildErr != nil {
			return fmt.Errorf("core: rotation boundary is no longer canonical; rebuild failed: %w", rebuildErr)
		}
		return errors.New("core: commitment branch rotation boundary is no longer canonical; rebuilt hot branches")
	}
	if proof == nil {
		if err := verifyCommitmentBranchRotationSnapshot(mgr, rotation); err != nil {
			return err
		}
	} else if err := proof.Recheck(rotation); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	batch := bc.db.NewBatch()
	if err := rawdb.WriteCommitmentBranchBase(batch, rawdb.CommitmentBranchBase{
		Generation: rotation.Generation, SnapshotTxNum: rotation.SnapshotTxNum, Root: rotation.Root,
		BlockNum: rotation.BlockNum, BlockHash: rotation.BlockHash,
	}); err != nil {
		return err
	}
	if err := rawdb.DeleteCommitmentBranchRotation(batch); err != nil {
		return err
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("complete commitment branch rotation: publish base marker: %w", err)
	}
	// As at begin, change in-process routing immediately after the atomic marker
	// swap; durability errors must not leave a live pipeline writing the retired
	// rotation fallback.
	bc.buffer.Discard()
	bc.invalidateOrderedCommitPipeline()
	if err := syncKeyValueStore(bc.db); err != nil {
		return fmt.Errorf("complete commitment branch rotation: sync base marker: %w", err)
	}

	if hadBase {
		if err := rawdb.DeleteCommitmentBranchDeltaGenerationsExcept(bc.db, rotation.Generation); err != nil {
			return fmt.Errorf("complete commitment branch rotation: remove covered deltas: %w", err)
		}
	} else if err := rawdb.DeleteCommitmentBranches(bc.db); err != nil {
		return fmt.Errorf("complete commitment branch rotation: remove legacy branches: %w", err)
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return fmt.Errorf("complete commitment branch rotation: sync covered branch cleanup: %w", err)
	}
	commitmentBranchRotationCompleteCounter.Inc(1)
	log.Info("Commitment branch rotation completed", "generation", rotation.Generation,
		"block", rotation.BlockNum, "tx", rotation.SnapshotTxNum, "root", rotation.Root)
	return nil
}

func verifyCommitmentBranchRotationSnapshot(mgr *snapshots.Manager, rotation rawdb.CommitmentBranchRotation) error {
	if mgr == nil {
		return errors.New("core: nil snapshot manager for commitment branch rotation")
	}
	root, ok, err := mgr.GetCommitmentRoot(rotation.SnapshotTxNum)
	if err != nil {
		return fmt.Errorf("core: read rotation commitment root: %w", err)
	}
	if !ok || root != rotation.Root {
		return fmt.Errorf("core: rotation commitment root mismatch at tx %d", rotation.SnapshotTxNum)
	}
	view, ok, err := mgr.OpenCommitmentBranchSnapshot(rotation.SnapshotTxNum)
	if err != nil {
		return fmt.Errorf("core: open rotation commitment branches: %w", err)
	}
	if !ok || view == nil {
		return fmt.Errorf("core: rotation commitment branches missing at tx %d", rotation.SnapshotTxNum)
	}
	defer view.Close()
	if view.SnapshotTxNum() != rotation.SnapshotTxNum {
		return fmt.Errorf("core: rotation branch tx mismatch: marker %d snapshot %d", rotation.SnapshotTxNum, view.SnapshotTxNum())
	}
	if err := statedomains.VerifyCommitmentBranchSnapshotRoot(view, rotation.Root); err != nil {
		return fmt.Errorf("core: verify rotation branch root: %w", err)
	}
	return nil
}

func (bc *BlockChain) rebuildCommitmentBranchesAfterRejectedRotation() error {
	store := statedomains.NewStagedCommitmentStore(bc.db)
	_, rebuildErr := store.Rebuild()
	closeErr := statedomains.CloseLatestCommitmentStore(store)
	if rebuildErr != nil {
		return rebuildErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return err
	}
	bc.buffer.Discard()
	bc.invalidateOrderedCommitPipeline()
	return nil
}
