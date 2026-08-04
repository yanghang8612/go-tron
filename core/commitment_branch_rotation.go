package core

import (
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
	if bc == nil || bc.db == nil {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: nil blockchain or database")
	}
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
	_, based, err := rawdb.ReadCommitmentBranchBase(bc.db)
	if err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	}
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(bc.db)
	if err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	}
	if based && rotating {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: commitment branch base and rotation markers both present")
	}
	if rotating {
		bc.buffer.Discard()
		bc.invalidateOrderedCommitPipeline()
		commitmentBranchRotationResumeCounter.Inc(1)
		return rotation, true, nil
	}
	if based {
		// Idempotently finish the second crash window. Once the base marker is
		// durable, legacy is never authoritative again and may be reclaimed on
		// any later lifecycle pass.
		if err := rawdb.DeleteCommitmentBranches(bc.db); err != nil {
			return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("resume commitment branch base cleanup: %w", err)
		}
		if err := syncKeyValueStore(bc.db); err != nil {
			return rawdb.CommitmentBranchRotation{}, false, fmt.Errorf("sync resumed commitment branch base cleanup: %w", err)
		}
		// Periodic base+delta merge is the next rotation generation.
		bc.buffer.Discard()
		bc.invalidateOrderedCommitPipeline()
		return rawdb.CommitmentBranchRotation{}, false, nil
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
	if _, rootBranchOK, err := rawdb.ReadCommitmentBranchNoCopy(bc.db, nil); err != nil {
		return rawdb.CommitmentBranchRotation{}, false, err
	} else if !rootBranchOK {
		return rawdb.CommitmentBranchRotation{}, false, errors.New("core: cannot rotate missing commitment root branch")
	}
	rotation = rawdb.CommitmentBranchRotation{
		Generation:    1,
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

// CompleteCommitmentBranchRotation atomically promotes a verified immutable
// snapshot, then removes the now-redundant legacy table. A crash before the
// marker swap resumes delta->legacy; a crash after it resumes delta->snapshot,
// whether or not legacy cleanup had finished.
func (bc *BlockChain) CompleteCommitmentBranchRotation(rotation rawdb.CommitmentBranchRotation, mgr *snapshots.Manager) error {
	if bc == nil || bc.db == nil {
		return errors.New("core: nil blockchain or database")
	}
	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

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
	if solidified := bc.cachedDynProps().LatestSolidifiedBlockNum(); solidified < 0 || uint64(solidified) < rotation.BlockNum {
		commitmentBranchRotationDeferredCounter.Inc(1)
		return snapshots.ErrCommitmentBranchRotationNotSolidified
	}
	canonical, canonicalOK, err := rawdb.ReadBlockHashByNumberStrict(bc.chaindb, rotation.BlockNum)
	if err != nil {
		return fmt.Errorf("complete commitment branch rotation: canonical boundary: %w", err)
	}
	if !canonicalOK || canonical != rotation.BlockHash {
		commitmentBranchRotationRejectCounter.Inc(1)
		if rebuildErr := bc.rebuildCommitmentBranchesAfterRejectedRotation(); rebuildErr != nil {
			return fmt.Errorf("core: rotation boundary is no longer canonical; rebuild failed: %w", rebuildErr)
		}
		return errors.New("core: commitment branch rotation boundary is no longer canonical; rebuilt hot branches")
	}
	if err := verifyCommitmentBranchRotationSnapshot(mgr, rotation); err != nil {
		return err
	}

	batch := bc.db.NewBatch()
	if err := rawdb.WriteCommitmentBranchBase(batch, rawdb.CommitmentBranchBase{
		Generation: rotation.Generation, SnapshotTxNum: rotation.SnapshotTxNum, Root: rotation.Root,
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

	if err := rawdb.DeleteCommitmentBranches(bc.db); err != nil {
		return fmt.Errorf("complete commitment branch rotation: remove legacy branches: %w", err)
	}
	if err := syncKeyValueStore(bc.db); err != nil {
		return fmt.Errorf("complete commitment branch rotation: sync legacy cleanup: %w", err)
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
	store, err := statedomains.NewStagedCommitmentStoreWithRepair(bc.db, statedomains.CommitmentSnapshotRepair{}, false)
	if err != nil {
		return err
	}
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
