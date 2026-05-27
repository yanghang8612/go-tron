package domains

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var ErrNilCommitmentStore = errors.New("domains: nil commitment store")

type CommitmentDB interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

// LatestCommitmentStore is the engine-agnostic persistence seam the latest-domain
// commitment orchestrator drives. Both the legacy incremental binary-radix store
// and the Erigon-style staged store implement it; callers select between them via
// the constructors below and feed the chosen store to the With-Store apply
// entries.
type LatestCommitmentStore interface {
	ReadRoot() (common.Hash, bool, error)
	WriteRoot(root common.Hash) error
	RootNodePresent(root common.Hash) (bool, error)
	RestoreRootFromNodes() (common.Hash, bool, error)
	RestoreNodesFromSnapshot(source CommitmentSnapshotSource, txNum uint64, expectedRoot common.Hash) (bool, error)
	Rebuild() (common.Hash, error)
	Update(updates []rawdb.StateCommitmentUpdate) (common.Hash, error)
	ReadLatestCheckpoint() (*rawdb.StateCommitmentCheckpoint, bool, error)
	IterateCheckpoints(fn func(*rawdb.StateCommitmentCheckpoint) (bool, error)) error
}

type CommitmentSnapshotSource interface {
	GetCommitmentRoot(txNum uint64) (common.Hash, bool, error)
	IterateCommitmentNodes(logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error
}

// CommitmentBranchSnapshotSource is the staged engine's snapshot restore seam.
// It pairs the snapshot root with an iterator over the snapshotted
// state-commitment-branch-v1- rows (hex-trie prefix -> encoded BranchData),
// which the legacy CommitmentSnapshotSource (tree/node/ logical keys) cannot
// express. A source may satisfy both interfaces — the production snapshot
// Manager does — so stagedCommitmentStore.RestoreNodesFromSnapshot type-asserts
// to this shape and declines gracefully when it is not implemented, leaving the
// LatestCommitmentStore interface and CommitmentSnapshotRepair unchanged.
type CommitmentBranchSnapshotSource interface {
	GetCommitmentRoot(txNum uint64) (common.Hash, bool, error)
	IterateCommitmentBranches(txNum uint64, fn func(prefix, encoded []byte) (bool, error)) error
}

type CommitmentSnapshotRepair struct {
	Source CommitmentSnapshotSource
	TxNum  uint64
}

type rawDBLatestCommitmentStore struct {
	db CommitmentDB
}

// NewRawDBLatestCommitmentStore builds the legacy incremental binary-radix
// LatestCommitmentStore over db. This is the gate=false path.
func NewRawDBLatestCommitmentStore(db CommitmentDB) LatestCommitmentStore {
	return rawDBLatestCommitmentStore{db: db}
}

func (s rawDBLatestCommitmentStore) ReadRoot() (common.Hash, bool, error) {
	return rawdb.ReadLatestDomainCommitmentRoot(s.db)
}

func (s rawDBLatestCommitmentStore) WriteRoot(root common.Hash) error {
	return rawdb.WriteLatestDomainCommitmentRoot(s.db, root)
}

func (s rawDBLatestCommitmentStore) RootNodePresent(root common.Hash) (bool, error) {
	return rawdb.LatestDomainCommitmentRootNodePresent(s.db, root)
}

func (s rawDBLatestCommitmentStore) RestoreRootFromNodes() (common.Hash, bool, error) {
	return rawdb.RestoreLatestDomainCommitmentRootFromNodes(s.db)
}

func (s rawDBLatestCommitmentStore) RestoreNodesFromSnapshot(source CommitmentSnapshotSource, txNum uint64, expectedRoot common.Hash) (bool, error) {
	if source == nil || expectedRoot == (common.Hash{}) {
		return false, nil
	}
	snapshotRoot, ok, err := source.GetCommitmentRoot(txNum)
	if err != nil || !ok || snapshotRoot != expectedRoot {
		return false, err
	}
	restored := 0
	if err := source.IterateCommitmentNodes(rawdb.LatestDomainCommitmentNodeLogicalPrefix(), txNum, func(logicalKey, value []byte) (bool, error) {
		if !rawdb.IsLatestDomainCommitmentNodeLogicalKey(logicalKey) {
			return false, fmt.Errorf("domains: snapshot commitment node has unexpected key %x", logicalKey)
		}
		if len(value) != common.HashLength {
			return false, fmt.Errorf("domains: snapshot commitment node %x has bad hash length %d", logicalKey, len(value))
		}
		if err := rawdb.WriteStateCommitmentDomain(s.db, logicalKey, value); err != nil {
			return false, err
		}
		restored++
		return true, nil
	}); err != nil {
		return false, err
	}
	if restored == 0 {
		return false, nil
	}
	return s.RootNodePresent(expectedRoot)
}

func (s rawDBLatestCommitmentStore) Rebuild() (common.Hash, error) {
	return rawdb.RebuildLatestDomainCommitment(s.db)
}

func (s rawDBLatestCommitmentStore) Update(updates []rawdb.StateCommitmentUpdate) (common.Hash, error) {
	return rawdb.UpdateLatestDomainCommitment(s.db, updates)
}

func (s rawDBLatestCommitmentStore) ReadLatestCheckpoint() (*rawdb.StateCommitmentCheckpoint, bool, error) {
	return rawdb.ReadLatestStateCommitmentCheckpoint(s.db)
}

func (s rawDBLatestCommitmentStore) IterateCheckpoints(fn func(*rawdb.StateCommitmentCheckpoint) (bool, error)) error {
	return rawdb.IterateStateCommitmentCheckpoints(s.db, fn)
}

func ApplyLatestCommitment(db CommitmentDB, updates []rawdb.StateCommitmentUpdate) (common.Hash, error) {
	if db == nil {
		return common.Hash{}, ErrNilCommitmentStore
	}
	return ApplyLatestCommitmentWithStore(NewRawDBLatestCommitmentStore(db), updates)
}

func ApplyLatestCommitmentWithSnapshotRepair(db CommitmentDB, updates []rawdb.StateCommitmentUpdate, repair CommitmentSnapshotRepair) (common.Hash, error) {
	if db == nil {
		return common.Hash{}, ErrNilCommitmentStore
	}
	return ApplyLatestCommitmentWithStoreAndRepair(NewRawDBLatestCommitmentStore(db), updates, repair)
}

// ApplyLatestCommitmentWithStore drives the engine-agnostic orchestrator over an
// explicitly-chosen LatestCommitmentStore. Callers pick the store implementation
// (legacy vs staged) and pass it in.
func ApplyLatestCommitmentWithStore(store LatestCommitmentStore, updates []rawdb.StateCommitmentUpdate) (common.Hash, error) {
	return ApplyLatestCommitmentWithStoreAndRepair(store, updates, CommitmentSnapshotRepair{})
}

// ApplyLatestCommitmentWithStoreAndRepair is ApplyLatestCommitmentWithStore plus
// a snapshot-repair source for restoring pruned branch state before the update.
func ApplyLatestCommitmentWithStoreAndRepair(store LatestCommitmentStore, updates []rawdb.StateCommitmentUpdate, repair CommitmentSnapshotRepair) (common.Hash, error) {
	if store == nil {
		return common.Hash{}, ErrNilCommitmentStore
	}
	return applyLatestCommitmentWithRepair(store, updates, repair)
}

func applyLatestCommitment(store LatestCommitmentStore, updates []rawdb.StateCommitmentUpdate) (common.Hash, error) {
	return applyLatestCommitmentWithRepair(store, updates, CommitmentSnapshotRepair{})
}

func applyLatestCommitmentWithRepair(store LatestCommitmentStore, updates []rawdb.StateCommitmentUpdate, repair CommitmentSnapshotRepair) (common.Hash, error) {
	if store == nil {
		return common.Hash{}, ErrNilCommitmentStore
	}
	updates = rawdb.CoalesceStateCommitmentUpdates(updates)
	root, rootOK, err := store.ReadRoot()
	if err != nil {
		return common.Hash{}, err
	}
	rootPersisted := rootOK
	if len(updates) == 0 {
		if rootOK {
			return root, nil
		}
		if root, ok, err := store.RestoreRootFromNodes(); err != nil {
			return common.Hash{}, err
		} else if ok {
			return root, nil
		}
		if checkpoint, ok, err := store.ReadLatestCheckpoint(); err != nil {
			return common.Hash{}, err
		} else if ok {
			if err := store.WriteRoot(checkpoint.Root); err != nil {
				return common.Hash{}, err
			}
			return checkpoint.Root, nil
		}
		return store.Rebuild()
	}

	branchReady := false
	if rootOK {
		if ok, err := store.RootNodePresent(root); err != nil {
			return common.Hash{}, err
		} else {
			branchReady = ok
		}
	} else if root, rootOK, err = store.RestoreRootFromNodes(); err != nil {
		return common.Hash{}, err
	} else {
		branchReady = rootOK
		rootPersisted = rootOK
	}
	if !branchReady && repair.Source != nil {
		if !rootOK {
			checkpoint, ok, err := store.ReadLatestCheckpoint()
			if err != nil {
				return common.Hash{}, err
			}
			if ok {
				root = checkpoint.Root
				rootOK = true
			}
		}
		if rootOK {
			ok, err := store.RestoreNodesFromSnapshot(repair.Source, repair.TxNum, root)
			if err != nil {
				return common.Hash{}, err
			}
			if ok {
				branchReady = true
				if !rootPersisted {
					if err := store.WriteRoot(root); err != nil {
						return common.Hash{}, err
					}
					rootPersisted = true
				}
			}
		}
	}
	if !branchReady {
		if _, err := store.Rebuild(); err != nil {
			return common.Hash{}, err
		}
	}
	return store.Update(updates)
}

func SeekLatestCommitment(db CommitmentDB, txNumAtBlockEnd func(blockNum uint64) (uint64, error)) (uint64, uint64, error) {
	if db == nil {
		return 0, 0, ErrNilCommitmentStore
	}
	return seekLatestCommitment(NewRawDBLatestCommitmentStore(db), txNumAtBlockEnd)
}

// SeekLatestCommitmentWithStore is the store-injecting variant of
// SeekLatestCommitment, so callers can select the commitment engine.
func SeekLatestCommitmentWithStore(store LatestCommitmentStore, txNumAtBlockEnd func(blockNum uint64) (uint64, error)) (uint64, uint64, error) {
	return seekLatestCommitment(store, txNumAtBlockEnd)
}

func seekLatestCommitment(store LatestCommitmentStore, txNumAtBlockEnd func(blockNum uint64) (uint64, error)) (uint64, uint64, error) {
	if store == nil {
		return 0, 0, ErrNilCommitmentStore
	}
	if latest, ok, err := store.ReadLatestCheckpoint(); err != nil {
		return 0, 0, err
	} else if ok {
		if txNumAtBlockEnd == nil {
			return 0, latest.BlockNum, nil
		}
		txNum, err := txNumAtBlockEnd(latest.BlockNum)
		if err != nil {
			return 0, 0, err
		}
		return txNum, latest.BlockNum, nil
	}
	var latest *rawdb.StateCommitmentCheckpoint
	if err := store.IterateCheckpoints(func(checkpoint *rawdb.StateCommitmentCheckpoint) (bool, error) {
		if latest == nil || checkpoint.BlockNum > latest.BlockNum {
			cp := *checkpoint
			latest = &cp
		}
		return true, nil
	}); err != nil {
		return 0, 0, err
	}
	if latest == nil {
		if _, ok, err := store.ReadRoot(); err != nil || !ok {
			return 0, 0, err
		}
		return 0, 0, nil
	}
	if txNumAtBlockEnd == nil {
		return 0, latest.BlockNum, nil
	}
	txNum, err := txNumAtBlockEnd(latest.BlockNum)
	if err != nil {
		return 0, 0, err
	}
	return txNum, latest.BlockNum, nil
}
