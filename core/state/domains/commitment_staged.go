package domains

import (
	"fmt"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// rawdbBranchStore is a branchStore backed by the rawdb commitment-branch
// keyspace (prefix state-commitment-branch-v1-). Branch nodes are encoded with
// BranchData.Encode and decoded with DecodeBranchData; the prefix is the
// hex-trie nibble path (one byte per nibble, nil for the root).
type rawdbBranchStore struct {
	db                 CommitmentDB
	ownedValue         bool
	readParentBranches bool
}

// branchDecodeView owns the callback passed through rawdb/blockbuffer's
// callback-scoped value API. Keeping the bound method once on a pooled context
// avoids allocating a fresh capturing closure for every deep-trie branch read.
// The callback is strictly synchronous, so dst can be cleared and the context
// returned immediately after the view call.
type branchDecodeView struct {
	dst     *BranchData
	consume func(encoded []byte, stable bool) error
}

var branchDecodeViewPool = sync.Pool{
	New: func() any {
		view := new(branchDecodeView)
		view.consume = view.decode
		return view
	},
}

func (v *branchDecodeView) decode(encoded []byte, stable bool) error {
	if stable {
		// Immutable overlay/cache values (and generic owned Get results) live
		// for the full fold descent, so leaf keys may alias them directly.
		return decodeBranchDataIntoNoCopy(encoded, v.dst)
	}
	// A cold Pebble value is valid only inside this callback. Copy only its
	// leaf keys into BranchData instead of copying the complete encoded branch
	// (which is dominated by fixed child hashes) before decoding.
	return DecodeBranchDataInto(encoded, v.dst)
}

var branchEncodingSlicesPool = sync.Pool{
	New: func() any {
		values := make([][]byte, 0, 256)
		return &values
	},
}

type branchEncodingPlan struct {
	branch *BranchData
	mask   uint16
	size   int
}

var branchEncodingPlansPool = sync.Pool{
	New: func() any {
		plans := make([]branchEncodingPlan, 0, 256)
		return &plans
	},
}

func borrowBranchEncodingSlices(size int) *[][]byte {
	valuesPtr := branchEncodingSlicesPool.Get().(*[][]byte)
	if cap(*valuesPtr) < size {
		*valuesPtr = make([][]byte, size)
	} else {
		*valuesPtr = (*valuesPtr)[:size]
	}
	return valuesPtr
}

func returnBranchEncodingSlices(valuesPtr *[][]byte) {
	values := *valuesPtr
	clear(values)
	if cap(values) <= 4096 {
		*valuesPtr = values[:0]
		branchEncodingSlicesPool.Put(valuesPtr)
	}
}

func borrowBranchEncodingPlans(size int) *[]branchEncodingPlan {
	plansPtr := branchEncodingPlansPool.Get().(*[]branchEncodingPlan)
	if cap(*plansPtr) < size {
		*plansPtr = make([]branchEncodingPlan, size)
	} else {
		*plansPtr = (*plansPtr)[:size]
	}
	return plansPtr
}

func returnBranchEncodingPlans(plansPtr *[]branchEncodingPlan) {
	plans := *plansPtr
	clear(plans)
	if cap(plans) <= 4096 {
		*plansPtr = plans[:0]
		branchEncodingPlansPool.Put(plansPtr)
	}
}

func newRawdbBranchStore(db CommitmentDB) *rawdbBranchStore {
	return &rawdbBranchStore{db: db, ownedValue: rawdb.SupportsCommitmentBranchOwnedValue(db)}
}

// concurrentSiblingFlushSafe opts in only when the underlying CommitmentDB
// explicitly advertises concurrent reads and writes. The steady-state sync path
// uses blockbuffer.Buffer/LayerView, which provide the marker; direct Pebble,
// memorydb, and custom stores keep serial flushes unless separately audited.
func (s *rawdbBranchStore) concurrentSiblingFlushSafe() bool {
	_, ok := s.db.(interface{ ConcurrentReadWriteSafe() })
	return ok
}

func (s *rawdbBranchStore) GetBranch(prefix []byte) (BranchData, bool, error) {
	// NoCopy avoids the per-Get defensive copy. The returned BranchData may
	// borrow leaf-key slices from the owned/immutable encoded value; callers use
	// it only within the synchronous fold or encode it before returning.
	encoded, ok, err := rawdb.ReadCommitmentBranchNoCopy(s.db, prefix)
	if err != nil || !ok {
		return BranchData{}, ok, err
	}
	var b BranchData
	if err := decodeBranchDataIntoNoCopy(encoded, &b); err != nil {
		return BranchData{}, false, err
	}
	return b, true, nil
}

// GetBranchInto is GetBranch but writes into *dst instead of returning the
// value. The bulk-sync fold uses this with a pool-borrowed *BranchData to keep
// the ~1 KiB struct off the heap; see branchPool in commitment_tree.go.
func (s *rawdbBranchStore) GetBranchInto(prefix []byte, dst *BranchData) (bool, error) {
	view := rawdb.ViewCommitmentBranchNoCopy
	if s.readParentBranches {
		view = rawdb.ViewCommitmentParentBranchNoCopy
	}
	decodeView := branchDecodeViewPool.Get().(*branchDecodeView)
	decodeView.dst = dst
	found, err := view(s.db, prefix, decodeView.consume)
	decodeView.dst = nil
	branchDecodeViewPool.Put(decodeView)
	return found, err
}

func (s *rawdbBranchStore) PutBranch(prefix []byte, b BranchData) error {
	// A blockbuffer layer can retain a freshly allocated encoding directly.
	// Encode into that final immutable slice and transfer it, avoiding the
	// scratch-to-layer copy on every branch flushed by the commitment fold.
	if s.ownedValue {
		return rawdb.WriteCommitmentBranchOwned(s.db, prefix, b.Encode())
	}
	// Reuse a pooled encode buffer. The KV writer (pebble batch or direct Put)
	// copies the value into its own arena during the call, so the buffer is
	// safe to return as soon as WriteCommitmentBranch returns.
	bp := borrowEncodeBuf()
	defer returnEncodeBuf(bp)
	*bp = b.EncodeTo((*bp)[:0])
	return rawdb.WriteCommitmentBranch(s.db, prefix, *bp)
}

// putBranchesSorted encodes one sibling fold's final branches into a single
// immutable arena before transferring its disjoint slices to blockbuffer. The
// layer retains those slices until commit/drop, so a scratch buffer cannot be
// reused; sharing one exact-sized arena removes the per-branch heap object while
// preserving the owned-value lifetime. keys must be sorted by the caller.
func (s *rawdbBranchStore) putBranchesSorted(keys []string, branches map[string]*BranchData, batchCount int) error {
	if !s.ownedValue {
		for _, key := range keys {
			if err := s.PutBranch([]byte(key), *branches[key]); err != nil {
				return err
			}
		}
		return nil
	}
	plansPtr := borrowBranchEncodingPlans(len(keys))
	defer returnBranchEncodingPlans(plansPtr)
	plans := *plansPtr
	totalSize := 0
	for i, key := range keys {
		branch := branches[key]
		mask, size := branch.encodingLayout()
		plans[i] = branchEncodingPlan{branch: branch, mask: mask, size: size}
		totalSize += size
	}
	arena := make([]byte, 0, totalSize)
	valuesPtr := borrowBranchEncodingSlices(len(keys))
	defer returnBranchEncodingSlices(valuesPtr)
	values := *valuesPtr
	for i, plan := range plans {
		start := len(arena)
		arena = plan.branch.encodeToLayout(arena, plan.mask, plan.size)
		values[i] = arena[start:len(arena):len(arena)]
	}
	return rawdb.WriteCommitmentBranchesOwnedStringsWithBatchCount(s.db, keys, values, batchCount)
}

func (s *rawdbBranchStore) DelBranch(prefix []byte) error {
	return rawdb.DeleteCommitmentBranch(s.db, prefix)
}

// clear removes every persisted branch row in the commitment-branch keyspace.
// Rebuild calls this before re-folding so a full latest-domain scan produces a
// root that reflects exactly the current source rows, with no contribution from
// branches left over from an earlier (e.g. pre-rewind) tip.
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

	// asyncParentBranches enables the commit worker's one-shot parent-state
	// branch reader. branchStateWritten disables it after a rebuild/snapshot
	// restore or first update, preserving read-your-own-writes if a store is
	// reused while keeping the normal constructors fully unchanged.
	asyncParentBranches bool
	branchStateWritten  bool

	// bootstrapCount counts Rebuild invocations (full latest-domain scans). It
	// lets tests prove that normal incremental commits do not trigger a bootstrap
	// scan once branch state is persisted.
	bootstrapCount int
}

// NewStagedCommitmentStore builds a staged LatestCommitmentStore over db.
func NewStagedCommitmentStore(db CommitmentDB) LatestCommitmentStore {
	return newStagedCommitmentStore(db)
}

// NewStagedCommitmentStoreForAsyncFold builds the one-shot store used by the
// serial async commit worker. Its first incremental Update may read commitment
// child branches from the parent state without probing the committing layer;
// rebuild and snapshot-repair paths automatically retain ordinary visibility.
func NewStagedCommitmentStoreForAsyncFold(db CommitmentDB) LatestCommitmentStore {
	store := newStagedCommitmentStore(db)
	store.asyncParentBranches = true
	return store
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
	s.branchStateWritten = true
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
	s.branchStateWritten = true
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
	s.store.readParentBranches = s.asyncParentBranches && !s.branchStateWritten
	root, err := s.trie.Fold(foldUpdates)
	s.store.readParentBranches = false
	if err != nil {
		return common.Hash{}, err
	}
	s.branchStateWritten = true
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
