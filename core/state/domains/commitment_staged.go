package domains

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// rawdbBranchStore is a branchStore backed by the rawdb commitment-branch
// keyspace (prefix state-commitment-branch-v1-). Branch nodes are encoded with
// BranchData.Encode and decoded with DecodeBranchData; the prefix is the
// hex-trie nibble path (one byte per nibble, nil for the root).
type rawdbBranchStore struct {
	db CommitmentDB
}

func newRawdbBranchStore(db CommitmentDB) *rawdbBranchStore {
	return &rawdbBranchStore{db: db}
}

func (s *rawdbBranchStore) GetBranch(prefix []byte) (BranchData, bool, error) {
	encoded, ok, err := rawdb.ReadCommitmentBranch(s.db, prefix)
	if err != nil || !ok {
		return BranchData{}, ok, err
	}
	b, err := DecodeBranchData(encoded)
	if err != nil {
		return BranchData{}, false, err
	}
	return b, true, nil
}

func (s *rawdbBranchStore) PutBranch(prefix []byte, b BranchData) error {
	return rawdb.WriteCommitmentBranch(s.db, prefix, b.Encode())
}

func (s *rawdbBranchStore) DelBranch(prefix []byte) error {
	return rawdb.DeleteCommitmentBranch(s.db, prefix)
}

// clear removes every persisted branch row in the commitment-branch keyspace.
// Rebuild calls this before re-folding so a full latest-domain scan produces a
// root that reflects exactly the current source rows, with no contribution from
// branches left over from an earlier (e.g. pre-rewind) tip. Mirrors the legacy
// engine's clearLatestDomainCommitmentNodes.
func (s *rawdbBranchStore) clear() error {
	var prefixes [][]byte
	if err := rawdb.IterateCommitmentBranches(s.db, func(prefix, _ []byte) (bool, error) {
		prefixes = append(prefixes, append([]byte(nil), prefix...))
		return true, nil
	}); err != nil {
		return err
	}
	for _, prefix := range prefixes {
		if err := rawdb.DeleteCommitmentBranch(s.db, prefix); err != nil {
			return err
		}
	}
	return nil
}

// stagedCommitmentStore is the LatestCommitmentStore implementation backed by the
// Erigon-style staged engine: a hex-patricia commitmentTrie over prefix-keyed
// BranchData rows in the rawdb commitment-branch keyspace. The root row and
// checkpoints reuse the same rawdb accessors as the legacy store, so the
// engine-agnostic orchestrator (applyLatestCommitmentWithRepair) drives it
// unchanged.
type stagedCommitmentStore struct {
	db    CommitmentDB
	store *rawdbBranchStore
	trie  *commitmentTrie

	// bootstrapCount counts Rebuild invocations (full latest-domain scans). It
	// lets tests prove that normal incremental commits do not trigger a bootstrap
	// scan once branch state is persisted.
	bootstrapCount int
}

// NewStagedCommitmentStore builds a staged LatestCommitmentStore over db.
func NewStagedCommitmentStore(db CommitmentDB) LatestCommitmentStore {
	return newStagedCommitmentStore(db)
}

func newStagedCommitmentStore(db CommitmentDB) *stagedCommitmentStore {
	branchStore := newRawdbBranchStore(db)
	return &stagedCommitmentStore{
		db:    db,
		store: branchStore,
		trie:  newCommitmentTrie(branchStore),
	}
}

func (s *stagedCommitmentStore) ReadRoot() (common.Hash, bool, error) {
	return rawdb.ReadLatestDomainCommitmentRoot(s.db)
}

func (s *stagedCommitmentStore) WriteRoot(root common.Hash) error {
	return rawdb.WriteLatestDomainCommitmentRoot(s.db, root)
}

// RootNodePresent reports whether the persisted branch state re-derives to root.
// Fold(nil) reads branches only (no latest-domain scan), so this never triggers
// a bootstrap. The zero root is treated as always present (empty trie).
func (s *stagedCommitmentStore) RootNodePresent(root common.Hash) (bool, error) {
	if root == (common.Hash{}) {
		return true, nil
	}
	current, err := s.trie.Fold(nil)
	if err != nil {
		return false, err
	}
	return current == root, nil
}

// RestoreRootFromNodes re-derives the root from persisted branch state and, when
// a root branch exists, writes the latest-root row. Distinguishing "no branches"
// from "empty trie" requires the explicit root-branch presence check, since
// Fold(nil) returns the zero hash in both cases.
func (s *stagedCommitmentStore) RestoreRootFromNodes() (common.Hash, bool, error) {
	_, hasRoot, err := s.store.GetBranch(nil)
	if err != nil {
		return common.Hash{}, false, err
	}
	if !hasRoot {
		return common.Hash{}, false, nil
	}
	root, err := s.trie.Fold(nil)
	if err != nil {
		return common.Hash{}, false, err
	}
	if err := s.WriteRoot(root); err != nil {
		return common.Hash{}, false, err
	}
	return root, true, nil
}

// RestoreNodesFromSnapshot is a no-op for the staged engine: branch snapshots
// are a later task. The orchestrator falls through to Rebuild when branch state
// is missing and no snapshot restore succeeds.
func (s *stagedCommitmentStore) RestoreNodesFromSnapshot(source CommitmentSnapshotSource, txNum uint64, expectedRoot common.Hash) (bool, error) {
	return false, nil
}

// Rebuild bootstraps the full staged trie from every latest-domain source row,
// writes the root row, and returns the root. This is the one-time fallback used
// when no branch state is present; it must not run on a normal incremental
// commit.
func (s *stagedCommitmentStore) Rebuild() (common.Hash, error) {
	s.bootstrapCount++
	// Fold MERGES into existing branches, so a rebuild must start from a clean
	// branch keyspace; otherwise rows from an earlier (e.g. pre-rewind) tip would
	// contribute to the rebuilt root. Mirrors the legacy engine, which clears its
	// tree/node/ rows in RebuildLatestDomainCommitment before re-applying.
	if err := s.store.clear(); err != nil {
		return common.Hash{}, err
	}
	var updates []Update
	if err := rawdb.IterateLatestDomainCommitmentSources(s.db, func(key, value []byte) (bool, error) {
		updates = append(updates, Update{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), value...),
		})
		return true, nil
	}); err != nil {
		return common.Hash{}, err
	}
	root, err := s.trie.Fold(updates)
	if err != nil {
		return common.Hash{}, err
	}
	if err := s.WriteRoot(root); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// Update applies the incremental commitment updates through the fold engine
// using persisted branch state and writes the resulting root row.
func (s *stagedCommitmentStore) Update(updates []rawdb.StateCommitmentUpdate) (common.Hash, error) {
	foldUpdates := make([]Update, 0, len(updates))
	for _, u := range updates {
		foldUpdates = append(foldUpdates, Update{Key: u.Key, Value: u.Value, Delete: u.Delete})
	}
	root, err := s.trie.Fold(foldUpdates)
	if err != nil {
		return common.Hash{}, err
	}
	if err := s.WriteRoot(root); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

func (s *stagedCommitmentStore) ReadLatestCheckpoint() (*rawdb.StateCommitmentCheckpoint, bool, error) {
	return rawdb.ReadLatestStateCommitmentCheckpoint(s.db)
}

func (s *stagedCommitmentStore) IterateCheckpoints(fn func(*rawdb.StateCommitmentCheckpoint) (bool, error)) error {
	return rawdb.IterateStateCommitmentCheckpoints(s.db, fn)
}
