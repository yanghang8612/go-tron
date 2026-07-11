package domains

import (
	"fmt"

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

func (*rawdbBranchStore) branchStoreParallelSafe() {}

func (s *rawdbBranchStore) GetBranch(prefix []byte) (BranchData, bool, error) {
	// NoCopy avoids the per-Get defensive copy; DecodeBranchData consumes the
	// bytes immediately and copies the leafKey field, so the aliased slice is
	// not retained past this function.
	encoded, ok, err := rawdb.ReadCommitmentBranchNoCopy(s.db, prefix)
	if err != nil || !ok {
		return BranchData{}, ok, err
	}
	b, err := DecodeBranchData(encoded)
	if err != nil {
		return BranchData{}, false, err
	}
	return b, true, nil
}

// GetBranchInto is GetBranch but writes into *dst instead of returning the
// value. The bulk-sync fold uses this with a pool-borrowed *BranchData to keep
// the ~1.5 KB struct off the heap; see branchPool in commitment_tree.go.
func (s *rawdbBranchStore) GetBranchInto(prefix []byte, dst *BranchData) (bool, error) {
	encoded, ok, err := rawdb.ReadCommitmentBranchNoCopy(s.db, prefix)
	if err != nil || !ok {
		return ok, err
	}
	if err := DecodeBranchDataInto(encoded, dst); err != nil {
		return false, err
	}
	return true, nil
}

func (s *rawdbBranchStore) PutBranch(prefix []byte, b BranchData) error {
	// Reuse a pooled encode buffer. The KV writer (pebble batch or direct Put)
	// copies the value into its own arena during the call, so the buffer is
	// safe to return as soon as WriteCommitmentBranch returns.
	bp := borrowEncodeBuf()
	defer returnEncodeBuf(bp)
	*bp = b.EncodeTo((*bp)[:0])
	return rawdb.WriteCommitmentBranch(s.db, prefix, *bp)
}

func (s *rawdbBranchStore) DelBranch(prefix []byte) error {
	return rawdb.DeleteCommitmentBranch(s.db, prefix)
}

// clear removes every persisted branch row in the commitment-branch keyspace.
// Rebuild calls this before re-folding so a full latest-domain scan produces a
// root that reflects exactly the current source rows, with no contribution from
// branches left over from an earlier (e.g. pre-rewind) tip.
func (s *rawdbBranchStore) clear() error {
	return rawdb.DeleteCommitmentBranches(s.db)
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
	trie := newCommitmentTrie(branchStore)
	// Opt into the parallel root fold for production commits. The keccak-bound
	// fold runs single-threaded otherwise; splitting the 16 first-nibble subtries
	// across cores recovers idle CPU on the sync hot path. ParallelFoldMinOps is
	// the threshold/kill switch; both paths yield identical roots and branch rows.
	trie.parallelMinOps = ParallelFoldMinOps
	return &stagedCommitmentStore{
		db:    db,
		store: branchStore,
		trie:  trie,
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

// RestoreNodesFromSnapshot restores the staged engine's branch rows from a cold
// snapshot so a pruned-then-restored store re-derives expectedRoot WITHOUT a full
// latest-domain Rebuild scan.
//
// The supplied source is the engine-agnostic CommitmentSnapshotSource the
// orchestrator carries; the staged engine needs the branch-row iterator, so we
// type-assert to CommitmentBranchSnapshotSource and decline gracefully (false,
// nil) when it is absent, letting the orchestrator fall through to Rebuild. The
// restore is self-verifying: it confirms the snapshot root matches expectedRoot,
// writes the branch rows back via WriteCommitmentBranch, and returns true only
// when re-folding the restored branches (Fold(nil), no latest-domain scan)
// reproduces expectedRoot. On any mismatch or empty snapshot it returns (false,
// nil); the orchestrator's Rebuild then clears the branch keyspace before
// re-folding, so partially-written rows from a failed restore cannot survive.
func (s *stagedCommitmentStore) RestoreNodesFromSnapshot(source CommitmentSnapshotSource, txNum uint64, expectedRoot common.Hash) (bool, error) {
	if source == nil || expectedRoot == (common.Hash{}) {
		return false, nil
	}
	branchSource, ok := source.(CommitmentBranchSnapshotSource)
	if !ok {
		return false, nil
	}
	snapshotRoot, ok, err := branchSource.GetCommitmentRoot(txNum)
	if err != nil || !ok || snapshotRoot != expectedRoot {
		return false, err
	}
	restored := 0
	if err := branchSource.IterateCommitmentBranches(txNum, func(prefix, encoded []byte) (bool, error) {
		// Validate the encoded value decodes to a BranchData before persisting,
		// so a corrupt snapshot is rejected rather than poisoning the keyspace.
		if _, decodeErr := DecodeBranchData(encoded); decodeErr != nil {
			return false, fmt.Errorf("domains: snapshot branch %x: %w", prefix, decodeErr)
		}
		if err := rawdb.WriteCommitmentBranch(s.db, prefix, encoded); err != nil {
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
	rederived, err := s.trie.Fold(nil)
	if err != nil {
		return false, err
	}
	if rederived != expectedRoot {
		return false, nil
	}
	return true, nil
}

// rebuildSpyHook, when non-nil, fires at the start of Rebuild. It is nil in
// production (zero overhead) and set by tests to assert whether the full-scan
// rebuild path was taken.
var rebuildSpyHook func()

const (
	// rebuildFoldMaxUpdates bounds the resolved-op and parallel branch-buffer
	// working sets for one staged rebuild fold. The latest-domain source can
	// contain tens of millions of rows, so Rebuild must never collect it all.
	rebuildFoldMaxUpdates = 64 * 1024
	// rebuildFoldMaxBytes bounds copied iterator keys/values before a fold. One
	// oversized source row is still processed on its own, which is unavoidable
	// because hashing that value requires it to be read from the database.
	rebuildFoldMaxBytes = 32 * 1024 * 1024
	// Account for the Update header and allocator slack as well as key/value
	// bytes so many tiny rows cannot bypass the byte budget.
	rebuildFoldUpdateOverhead = 64
)

// SetRebuildSpyHook installs fn as the rebuild spy for tests. Pass nil to clear.
// This is the only exported interface to rebuildSpyHook; production code never
// calls it.
func SetRebuildSpyHook(fn func()) { rebuildSpyHook = fn }

// Rebuild bootstraps the full staged trie from every latest-domain source row,
// writes the root row, and returns the root. This is the one-time fallback used
// when no branch state is present; it must not run on a normal incremental
// commit.
func (s *stagedCommitmentStore) Rebuild() (common.Hash, error) {
	if rebuildSpyHook != nil {
		rebuildSpyHook()
	}
	s.bootstrapCount++
	// Fold MERGES into existing branches, so a rebuild must start from a clean
	// branch keyspace; otherwise rows from an earlier (e.g. pre-rewind) tip would
	// contribute to the rebuilt root.
	if err := s.store.clear(); err != nil {
		return common.Hash{}, err
	}
	root, err := s.rebuildLatestDomainSources(rebuildFoldMaxUpdates, rebuildFoldMaxBytes)
	if err != nil {
		return common.Hash{}, err
	}
	if err := s.WriteRoot(root); err != nil {
		return common.Hash{}, err
	}
	return root, nil
}

// rebuildLatestDomainSources folds current latest-domain rows in bounded input
// windows. Fold is incremental by construction: its persisted branch rows are
// the next window's base state, and the fold equivalence tests prove that this
// produces the same root and branch set as one resident all-rows fold.
func (s *stagedCommitmentStore) rebuildLatestDomainSources(maxUpdates, maxBytes int) (common.Hash, error) {
	if maxUpdates <= 0 || maxBytes <= 0 {
		return common.Hash{}, fmt.Errorf("domains: invalid staged rebuild limits updates=%d bytes=%d", maxUpdates, maxBytes)
	}
	initialCap := maxUpdates
	if initialCap > 4096 {
		initialCap = 4096
	}
	updates := make([]Update, 0, initialCap)
	var root common.Hash
	var updateBytes int
	folded := false
	flush := func() error {
		if len(updates) == 0 {
			return nil
		}
		var err error
		root, err = s.trie.Fold(updates)
		if err != nil {
			return err
		}
		for i := range updates {
			updates[i] = Update{}
		}
		updates = updates[:0]
		updateBytes = 0
		folded = true
		return nil
	}
	if err := rawdb.IterateLatestDomainCommitmentSources(s.db, func(key, value []byte) (bool, error) {
		entryBytes := len(key) + len(value) + rebuildFoldUpdateOverhead
		if len(updates) > 0 && (len(updates) >= maxUpdates || entryBytes > maxBytes-updateBytes) {
			if err := flush(); err != nil {
				return false, err
			}
		}
		updates = append(updates, Update{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), value...),
		})
		updateBytes += entryBytes
		return true, nil
	}); err != nil {
		return common.Hash{}, err
	}
	if err := flush(); err != nil {
		return common.Hash{}, err
	}
	if folded {
		return root, nil
	}
	return s.trie.Fold(nil)
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
