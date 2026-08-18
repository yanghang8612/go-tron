package domains

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// rawdbBranchStore is a branchStore backed by the rawdb commitment-branch
// keyspace (prefix state-commitment-branch-v1-). Branch nodes are encoded with
// BranchData.Encode and decoded with DecodeBranchData; the prefix is the
// hex-trie nibble path (one byte per nibble, nil for the root).
type rawdbBranchStore struct {
	db                         CommitmentDB
	keyspace                   rawdb.CommitmentBranchKeyspace
	cold                       pointread.CommitmentBranchSnapshotView
	coldOwned                  bool
	legacyFallback             bool
	frozenKeyspace             rawdb.CommitmentBranchKeyspace
	hasFrozenKeyspace          bool
	ownedValue                 bool
	readParentBranches         bool
	parentView                 pointread.CommitmentParentView
	parentSession              pointread.CommitmentParentSession
	parentPrefetchBase         int
	parentFallbackPrefetchBase int
	// Each persistent prefetch lane exclusively owns one path scratch region.
	// Keeping it on the fold store prevents the interface call from forcing a
	// fresh pathLen-byte heap object for every block/lane plan.
	parentPrefetchPaths [maxFoldNibbles][pathLen]byte
	// One fold-scoped leaf-key arena per exclusive parent reader. Decoder output
	// may be value-copied into sibling buffers, so arenas remain immutable until
	// all parallel work and flushes finish and closeParentRead returns them.
	leafKeyArenas [maxFoldNibbles + 1]*[]byte
}

// branchDecodeView owns the callback passed through rawdb/blockbuffer's
// callback-scoped value API. Keeping the bound method once on a pooled context
// avoids allocating a fresh capturing closure for every deep-trie branch read.
// The callback is strictly synchronous, so dst can be cleared and the context
// returned immediately after the view call.
type branchDecodeView struct {
	dst            *BranchData
	arena          *[]byte
	tombstone      bool
	allowTombstone bool
	consume        func(encoded []byte, stable bool) error
}

var branchDecodeViewPool = sync.Pool{
	New: func() any {
		view := new(branchDecodeView)
		view.consume = view.decode
		return view
	},
}

func (v *branchDecodeView) decode(encoded []byte, stable bool) error {
	if len(encoded) == 0 && v.allowTombstone {
		*v.dst = BranchData{}
		v.tombstone = true
		return nil
	}
	if stable {
		// Immutable overlay values and generic owned Get results live for the
		// full fold descent, so leaf keys may alias them directly.
		return decodeBranchDataIntoNoCopy(encoded, v.dst)
	}
	// A cold Pebble value is valid only inside this callback. Copy only its
	// leaf keys into BranchData instead of copying the complete encoded branch
	// (which is dominated by fixed child hashes) before decoding.
	return decodeBranchDataIntoArena(encoded, v.dst, v.arena)
}

const maxPooledBranchLeafKeyArena = 64 << 10

var branchLeafKeyArenaPool = sync.Pool{
	New: func() any { return new([]byte) },
}

func (s *rawdbBranchStore) borrowLeafKeyArenas() {
	for i := range s.leafKeyArenas {
		arena := branchLeafKeyArenaPool.Get().(*[]byte)
		*arena = (*arena)[:0]
		s.leafKeyArenas[i] = arena
	}
}

func (s *rawdbBranchStore) returnLeafKeyArenas() {
	for i, arena := range s.leafKeyArenas {
		if arena == nil {
			continue
		}
		if cap(*arena) > maxPooledBranchLeafKeyArena {
			*arena = nil
		} else {
			*arena = (*arena)[:0]
		}
		branchLeafKeyArenaPool.Put(arena)
		s.leafKeyArenas[i] = nil
	}
}

var branchEncodingSlicesPool = sync.Pool{
	New: func() any {
		values := make([][]byte, 0, 256)
		return &values
	},
}

type branchEncodingPlan struct {
	branch    *BranchData
	childBits uint32
	size      int
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
	return newRawdbBranchStoreInKeyspace(db, rawdb.LegacyCommitmentBranchKeyspace(), nil, false)
}

func newRawdbBranchStoreInKeyspace(db CommitmentDB, keyspace rawdb.CommitmentBranchKeyspace, cold pointread.CommitmentBranchSnapshotView, coldOwned bool) *rawdbBranchStore {
	return &rawdbBranchStore{
		db:         db,
		keyspace:   keyspace,
		cold:       cold,
		coldOwned:  coldOwned,
		ownedValue: rawdb.SupportsCommitmentBranchOwnedValue(db),
	}
}

func newRawdbBranchStoreWithRepair(db CommitmentDB, repair CommitmentSnapshotRepair) (*rawdbBranchStore, error) {
	base, ok, err := rawdb.ReadCommitmentBranchBase(db)
	if err != nil {
		return nil, err
	}
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(db)
	if err != nil {
		return nil, err
	}
	if rotating {
		keyspace, err := rawdb.NewCommitmentBranchDeltaKeyspace(rotation.Generation)
		if err != nil {
			return nil, err
		}
		if ok {
			if rotation.Generation != base.Generation+1 || rotation.Generation == 0 {
				return nil, fmt.Errorf("domains: commitment branch rotation generation %d does not follow base %d", rotation.Generation, base.Generation)
			}
			store, err := openRawdbBranchBase(db, repair, base)
			if err != nil {
				return nil, err
			}
			store.frozenKeyspace = store.keyspace
			store.hasFrozenKeyspace = true
			store.keyspace = keyspace
			commitmentBranchRotationOpenCounter.Inc(1)
			return store, nil
		}
		store := newRawdbBranchStoreInKeyspace(db, keyspace, nil, false)
		store.legacyFallback = true
		commitmentBranchRotationOpenCounter.Inc(1)
		return store, nil
	}
	if !ok {
		return newRawdbBranchStore(db), nil
	}
	return openRawdbBranchBase(db, repair, base)
}

func openRawdbBranchBase(db CommitmentDB, repair CommitmentSnapshotRepair, base rawdb.CommitmentBranchBase) (*rawdbBranchStore, error) {
	if repair.Source == nil {
		return nil, errors.New("domains: commitment branch base requires snapshot source")
	}
	snapshotRoot, rootOK, err := repair.Source.GetCommitmentRoot(base.SnapshotTxNum)
	if err != nil {
		return nil, fmt.Errorf("domains: read commitment branch base root: %w", err)
	}
	if !rootOK || snapshotRoot != base.Root {
		return nil, fmt.Errorf("domains: commitment branch base root mismatch at tx %d", base.SnapshotTxNum)
	}
	viewer, ok := repair.Source.(pointread.CommitmentBranchSnapshotViewer)
	if !ok {
		return nil, errors.New("domains: commitment branch base requires indexed snapshot source")
	}
	view, ok, err := viewer.OpenCommitmentBranchSnapshot(base.SnapshotTxNum)
	if err != nil {
		return nil, fmt.Errorf("domains: open commitment branch base at tx %d: %w", base.SnapshotTxNum, err)
	}
	if !ok || view == nil {
		return nil, fmt.Errorf("domains: commitment branch base missing at tx %d", base.SnapshotTxNum)
	}
	if view.SnapshotTxNum() != base.SnapshotTxNum {
		_ = view.Close()
		return nil, fmt.Errorf("domains: commitment branch base tx mismatch: marker %d snapshot %d", base.SnapshotTxNum, view.SnapshotTxNum())
	}
	keyspace, err := rawdb.NewCommitmentBranchDeltaKeyspace(base.Generation)
	if err != nil {
		_ = view.Close()
		return nil, err
	}
	commitmentBranchBaseOpenCounter.Inc(1)
	return newRawdbBranchStoreInKeyspace(db, keyspace, view, true), nil
}

func (s *rawdbBranchStore) hasBaseline() bool {
	return s.cold != nil || s.legacyFallback || s.hasFrozenKeyspace
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
	encoded, ok, err := s.keyspace.ReadNoCopy(s.db, prefix)
	if err != nil {
		return BranchData{}, false, err
	}
	if ok {
		if len(encoded) == 0 && s.hasBaseline() {
			commitmentBranchTombstoneCounter.Inc(1)
			return BranchData{}, false, nil
		}
		if s.hasBaseline() {
			commitmentBranchDeltaHitCounter.Inc(1)
		}
		var b BranchData
		if err := decodeBranchDataIntoNoCopy(encoded, &b); err != nil {
			return BranchData{}, false, err
		}
		return b, true, nil
	}
	if !s.hasBaseline() {
		return BranchData{}, false, nil
	}
	if s.hasFrozenKeyspace {
		encoded, ok, err = s.frozenKeyspace.ReadNoCopy(s.db, prefix)
		if err != nil {
			return BranchData{}, false, err
		}
		if ok {
			if len(encoded) == 0 {
				commitmentBranchFrozenTombstoneCounter.Inc(1)
				return BranchData{}, false, nil
			}
			commitmentBranchFrozenDeltaHitCounter.Inc(1)
			var b BranchData
			if err := decodeBranchDataIntoNoCopy(encoded, &b); err != nil {
				return BranchData{}, false, err
			}
			return b, true, nil
		}
		commitmentBranchFrozenDeltaMissCounter.Inc(1)
	}
	if s.legacyFallback {
		encoded, ok, err = rawdb.LegacyCommitmentBranchKeyspace().ReadNoCopy(s.db, prefix)
		if err != nil || !ok {
			if err == nil {
				commitmentBranchLegacyMissCounter.Inc(1)
			}
			return BranchData{}, ok, err
		}
		commitmentBranchLegacyHitCounter.Inc(1)
		var b BranchData
		if err := decodeBranchDataIntoNoCopy(encoded, &b); err != nil {
			return BranchData{}, false, err
		}
		return b, true, nil
	}
	encoded, ok, err = s.cold.Get(prefix)
	if err != nil || !ok {
		if err == nil {
			commitmentBranchColdMissCounter.Inc(1)
		}
		return BranchData{}, ok, err
	}
	commitmentBranchColdHitCounter.Inc(1)
	var b BranchData
	if err := DecodeBranchDataInto(encoded, &b); err != nil {
		return BranchData{}, false, err
	}
	return b, true, nil
}

// GetBranchInto is GetBranch but writes into *dst instead of returning the
// value. The bulk-sync fold uses this with a pool-borrowed *BranchData to keep
// the ~800-byte struct off the heap; see branchPool in commitment_tree.go.
func (s *rawdbBranchStore) GetBranchInto(prefix []byte, dst *BranchData) (bool, error) {
	decodeView := branchDecodeViewPool.Get().(*branchDecodeView)
	decodeView.dst = dst
	reader := maxFoldNibbles // root branch has no first-nibble owner
	if len(prefix) > 0 && prefix[0] < maxFoldNibbles {
		reader = int(prefix[0])
	}
	decodeView.arena = s.leafKeyArenas[reader]
	decodeView.tombstone = false
	decodeView.allowTombstone = s.hasBaseline()
	var found bool
	var err error
	if s.parentSession != nil {
		found, err = s.keyspace.ViewParentInSession(s.parentSession, reader, prefix, decodeView.consume)
	} else if s.parentView != nil {
		found, err = s.keyspace.ViewParentInView(s.parentView, prefix, decodeView.consume)
	} else if s.readParentBranches {
		found, err = s.keyspace.ViewParentNoCopy(s.db, prefix, decodeView.consume)
	} else {
		found, err = s.keyspace.ViewNoCopy(s.db, prefix, decodeView.consume)
	}
	tombstone := decodeView.tombstone
	decodeView.dst = nil
	decodeView.arena = nil
	decodeView.tombstone = false
	decodeView.allowTombstone = false
	branchDecodeViewPool.Put(decodeView)
	if err != nil {
		return false, err
	}
	if found {
		if tombstone {
			commitmentBranchTombstoneCounter.Inc(1)
		} else if s.hasBaseline() {
			commitmentBranchDeltaHitCounter.Inc(1)
		}
		return !tombstone, nil
	}
	if !s.hasBaseline() {
		*dst = BranchData{}
		return false, nil
	}
	if s.hasFrozenKeyspace {
		decodeView := branchDecodeViewPool.Get().(*branchDecodeView)
		decodeView.dst = dst
		decodeView.arena = s.leafKeyArenas[reader]
		decodeView.tombstone = false
		decodeView.allowTombstone = true
		fallbackReader := reader + maxFoldNibbles + 1
		var found bool
		var err error
		if s.parentSession != nil {
			found, err = s.frozenKeyspace.ViewParentInSession(s.parentSession, fallbackReader, prefix, decodeView.consume)
		} else if s.parentView != nil {
			found, err = s.frozenKeyspace.ViewParentInView(s.parentView, prefix, decodeView.consume)
		} else if s.readParentBranches {
			found, err = s.frozenKeyspace.ViewParentNoCopy(s.db, prefix, decodeView.consume)
		} else {
			found, err = s.frozenKeyspace.ViewNoCopy(s.db, prefix, decodeView.consume)
		}
		tombstone := decodeView.tombstone
		decodeView.dst = nil
		decodeView.arena = nil
		decodeView.tombstone = false
		decodeView.allowTombstone = false
		branchDecodeViewPool.Put(decodeView)
		if err != nil {
			return false, err
		}
		if found {
			if tombstone {
				commitmentBranchFrozenTombstoneCounter.Inc(1)
				return false, nil
			}
			commitmentBranchFrozenDeltaHitCounter.Inc(1)
			return true, nil
		}
		commitmentBranchFrozenDeltaMissCounter.Inc(1)
	}
	if s.legacyFallback {
		decodeView := branchDecodeViewPool.Get().(*branchDecodeView)
		decodeView.dst = dst
		decodeView.arena = s.leafKeyArenas[reader]
		decodeView.tombstone = false
		decodeView.allowTombstone = false
		fallbackReader := reader + maxFoldNibbles + 1
		var found bool
		var err error
		legacy := rawdb.LegacyCommitmentBranchKeyspace()
		if s.parentSession != nil {
			found, err = legacy.ViewParentInSession(s.parentSession, fallbackReader, prefix, decodeView.consume)
		} else if s.parentView != nil {
			found, err = legacy.ViewParentInView(s.parentView, prefix, decodeView.consume)
		} else if s.readParentBranches {
			found, err = legacy.ViewParentNoCopy(s.db, prefix, decodeView.consume)
		} else {
			found, err = legacy.ViewNoCopy(s.db, prefix, decodeView.consume)
		}
		decodeView.dst = nil
		decodeView.arena = nil
		decodeView.allowTombstone = false
		branchDecodeViewPool.Put(decodeView)
		if err != nil || !found {
			if err == nil {
				commitmentBranchLegacyMissCounter.Inc(1)
			}
			*dst = BranchData{}
			return found, err
		}
		commitmentBranchLegacyHitCounter.Inc(1)
		return true, nil
	}
	encoded, ok, err := s.cold.Get(prefix)
	if err != nil || !ok {
		if err == nil {
			commitmentBranchColdMissCounter.Inc(1)
		}
		*dst = BranchData{}
		return ok, err
	}
	commitmentBranchColdHitCounter.Inc(1)
	if err := decodeBranchDataIntoArena(encoded, dst, s.leafKeyArenas[reader]); err != nil {
		return false, err
	}
	return true, nil
}

// prefetchParentLane predicts the first branch outside the fixed commitment
// cache trunk for every distinct path in one ordered lane. Ops are already
// sorted by full hash, so equal prefixes are contiguous and deduplicate without
// a map or heap allocation. The read-ahead does not decode values: the normal
// fold remains the sole authority for branch kinds and mutations.
func (s *rawdbBranchStore) prefetchParentLane(nb uint8, ops []op, depth int) error {
	_, _, err := s.prefetchParentLaneLimited(nb, ops, depth, 0)
	return err
}

// prefetchParentLaneLimited is the bounded planner used by child-level
// lookahead. A non-positive limit preserves the unbounded critical-level
// behavior. The count is in logical prefixes; legacy/frozen fallback probes do
// not consume a second unit.
func (s *rawdbBranchStore) prefetchParentLaneLimited(nb uint8, ops []op, depth, limit int) (int, bool, error) {
	prefetch, ok := s.parentSession.(pointread.CommitmentParentPrefetchSession)
	if !ok || len(ops) == 0 || depth <= 0 {
		return 0, false, nil
	}
	if depth > pathLen {
		depth = pathLen
	}
	current := &s.parentPrefetchPaths[nb]
	var previous [pathLen]byte
	havePrevious := false
	planned := 0
	for i := range ops {
		if pathNibble(ops[i].path, 0) != nb {
			return planned, false, fmt.Errorf("domains: commitment prefetch lane %d received path in lane %d", nb, pathNibble(ops[i].path, 0))
		}
		for d := 0; d < depth; d++ {
			current[d] = pathNibble(ops[i].path, d)
		}
		same := havePrevious
		for d := 0; same && d < depth; d++ {
			same = current[d] == previous[d]
		}
		if same {
			continue
		}
		if limit > 0 && planned >= limit {
			return planned, true, nil
		}
		copy(previous[:depth], current[:depth])
		havePrevious = true
		planned++

		found, err := s.keyspace.PrefetchParentInSession(prefetch, s.parentPrefetchBase+int(nb), current[:depth])
		if err != nil {
			return planned, false, err
		}
		if found || s.parentFallbackPrefetchBase < 0 {
			continue
		}
		fallback := rawdb.LegacyCommitmentBranchKeyspace()
		if s.hasFrozenKeyspace {
			fallback = s.frozenKeyspace
		}
		if _, err := fallback.PrefetchParentInSession(prefetch, s.parentFallbackPrefetchBase+int(nb), current[:depth]); err != nil {
			return planned, false, err
		}
	}
	return planned, false, nil
}

func (s *rawdbBranchStore) supportsParentPrefetch() bool {
	if s == nil {
		return false
	}
	_, ok := s.parentSession.(pointread.CommitmentParentPrefetchSession)
	return ok
}

func (s *rawdbBranchStore) beginParentRead() error {
	const ordinaryReaders = maxFoldNibbles + 1
	hasFallback := s.legacyFallback || s.hasFrozenKeyspace
	readers := ordinaryReaders
	if hasFallback {
		readers *= 2
	}
	// The 16 optional read-ahead cursors are disjoint from the foreground lane
	// cursors. A future block may prefetch while the same lane is still folding
	// its predecessor, and Pebble cursors are intentionally single-owner.
	s.parentPrefetchBase = readers
	readers += maxFoldNibbles
	s.parentFallbackPrefetchBase = -1
	if hasFallback {
		s.parentFallbackPrefetchBase = readers
		readers += maxFoldNibbles
	}
	session, err := rawdb.NewCommitmentParentReadSession(s.db, readers)
	if err != nil {
		if session != nil {
			_ = session.Close()
		}
		return err
	}
	if session != nil {
		s.parentSession = session
		s.borrowLeafKeyArenas()
		return nil
	}
	if err := s.beginParentView(); err != nil {
		return err
	}
	s.borrowLeafKeyArenas()
	return nil
}

func (s *rawdbBranchStore) closeParentRead() error {
	defer s.returnLeafKeyArenas()
	if s.parentSession != nil {
		err := s.parentSession.Close()
		s.parentSession = nil
		return err
	}
	return s.closeParentView()
}

func (s *rawdbBranchStore) beginParentView() error {
	view, err := rawdb.NewCommitmentParentView(s.db)
	if err != nil {
		if view != nil {
			_ = view.Close()
		}
		return err
	}
	s.parentView = view
	return nil
}

func (s *rawdbBranchStore) closeParentView() error {
	if s.parentView == nil {
		return nil
	}
	err := s.parentView.Close()
	s.parentView = nil
	return err
}

func (s *rawdbBranchStore) PutBranch(prefix []byte, b BranchData) error {
	// A blockbuffer layer can retain a freshly allocated encoding directly.
	// Encode into that final immutable slice and transfer it, avoiding the
	// scratch-to-layer copy on every branch flushed by the commitment fold.
	if s.ownedValue {
		return s.keyspace.WriteOwned(s.db, prefix, b.Encode())
	}
	// Reuse a pooled encode buffer. The KV writer (pebble batch or direct Put)
	// copies the value into its own arena during the call, so the buffer is
	// safe to return as soon as WriteCommitmentBranch returns.
	bp := borrowEncodeBuf()
	defer returnEncodeBuf(bp)
	*bp = b.EncodeTo((*bp)[:0])
	return s.keyspace.Write(s.db, prefix, *bp)
}

// putBranches encodes one sibling fold's final branches into a single
// immutable arena before transferring its disjoint slices to blockbuffer. The
// layer retains those slices until commit/drop, so a scratch buffer cannot be
// reused; sharing one exact-sized arena removes the per-branch heap object while
// preserving the owned-value lifetime. Key order is intentionally immaterial:
// blockbuffer stores a map and its durable flush sorts the globally coalesced
// operations after every sibling has published.
func (s *rawdbBranchStore) putBranches(keys []string, branches map[string]*BranchData, batchCount int) error {
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
		childBits, size := branch.encodingLayout()
		plans[i] = branchEncodingPlan{branch: branch, childBits: childBits, size: size}
		totalSize += size
	}
	arena := make([]byte, 0, totalSize)
	valuesPtr := borrowBranchEncodingSlices(len(keys))
	defer returnBranchEncodingSlices(valuesPtr)
	values := *valuesPtr
	for i, plan := range plans {
		start := len(arena)
		arena = plan.branch.encodeToLayout(arena, plan.childBits, plan.size)
		values[i] = arena[start:len(arena):len(arena)]
	}
	return s.keyspace.WriteOwnedStringsWithBatchCount(s.db, keys, values, batchCount)
}

func (s *rawdbBranchStore) DelBranch(prefix []byte) error {
	if s.hasBaseline() {
		// An explicit empty row shadows a branch that may still exist in the
		// immutable baseline. Physical absence means "consult cold".
		return s.keyspace.Write(s.db, prefix, []byte{})
	}
	return s.keyspace.Delete(s.db, prefix)
}

// clear removes every persisted branch row in the commitment-branch keyspace.
// Rebuild calls this before re-folding so a full latest-domain scan produces a
// root that reflects exactly the current source rows, with no contribution from
// branches left over from an earlier (e.g. pre-rewind) tip.
func (s *rawdbBranchStore) clear() error {
	base, based, err := rawdb.ReadCommitmentBranchBase(s.db)
	if err != nil {
		return err
	}
	rotation, rotating, err := rawdb.ReadCommitmentBranchRotation(s.db)
	if err != nil {
		return err
	}
	// Invalidate the marker before dropping its delta or closing the immutable
	// view. A crash can therefore only leave an inactive, rebuildable legacy
	// table; it can never expose a partial delta as a complete branch tree.
	if err := rawdb.DeleteCommitmentBranchBase(s.db); err != nil {
		return err
	}
	if err := rawdb.DeleteCommitmentBranchRotation(s.db); err != nil {
		return err
	}
	if s.hasBaseline() {
		if err := s.keyspace.DeleteAll(s.db); err != nil {
			return err
		}
		if err := s.close(); err != nil {
			return err
		}
		s.keyspace = rawdb.LegacyCommitmentBranchKeyspace()
		s.legacyFallback = false
		s.frozenKeyspace = rawdb.CommitmentBranchKeyspace{}
		s.hasFrozenKeyspace = false
	}
	// A legacy constructor may be used by an explicit administrative Rebuild.
	// It cannot read an immutable base without a source, but it must still erase
	// that marker's active delta before rebuilding the complete legacy table.
	var generations [2]uint64
	if based {
		generations[0] = base.Generation
	}
	if rotating {
		generations[1] = rotation.Generation
	}
	for _, generation := range generations {
		if generation == 0 {
			continue
		}
		delta, err := rawdb.NewCommitmentBranchDeltaKeyspace(generation)
		if err != nil {
			return err
		}
		if err := delta.DeleteAll(s.db); err != nil {
			return err
		}
	}
	return s.keyspace.DeleteAll(s.db)
}

func (s *rawdbBranchStore) close() error {
	if s == nil || s.cold == nil {
		return nil
	}
	var err error
	if s.coldOwned {
		err = s.cold.Close()
	}
	s.cold = nil
	s.coldOwned = false
	return err
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
	// initErr prevents legacy constructors that lack a snapshot source from
	// silently updating an active immutable-base database. Rebuild remains
	// available as the explicit destructive recovery path.
	initErr error

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

// NewStagedCommitmentStoreWithRepair opens a generation-specific hot delta
// over its hash-bound immutable branch snapshot when a base marker is present.
// Without a marker it is byte-for-byte equivalent to the legacy complete hot
// table. The returned store owns any opened snapshot view and must be closed.
func NewStagedCommitmentStoreWithRepair(db CommitmentDB, repair CommitmentSnapshotRepair, asyncParentBranches bool) (LatestCommitmentStore, error) {
	branchStore, err := newRawdbBranchStoreWithRepair(db, repair)
	if err != nil {
		return nil, err
	}
	store := newStagedCommitmentStoreWithBranchStore(db, branchStore)
	store.asyncParentBranches = asyncParentBranches
	return store, nil
}

// CloseLatestCommitmentStore releases resources owned by stores constructed
// with NewStagedCommitmentStoreWithRepair. Other implementations are a no-op.
func CloseLatestCommitmentStore(store LatestCommitmentStore) error {
	if closer, ok := store.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

// VerifyCommitmentBranchSnapshotRoot proves that an immutable branch snapshot's
// root row derives to expected. Segment checksums and indexes protect bytes and
// lookup structure; this binds those bytes to the independently snapshotted
// commitment root before a live rotation can make the snapshot authoritative.
func VerifyCommitmentBranchSnapshotRoot(view pointread.CommitmentBranchSnapshotView, expected common.Hash) error {
	if view == nil {
		return errors.New("domains: nil commitment branch snapshot view")
	}
	encoded, ok, err := view.Get(nil)
	if err != nil {
		return err
	}
	if expected == (common.Hash{}) {
		if ok {
			return errors.New("domains: zero commitment root has a snapshot root branch")
		}
		return nil
	}
	if !ok {
		return errors.New("domains: commitment snapshot root branch missing")
	}
	var root BranchData
	if err := DecodeBranchDataInto(encoded, &root); err != nil {
		return err
	}
	if derived := rootHash(&root); derived != expected {
		return fmt.Errorf("domains: commitment snapshot root mismatch: derived %x expected %x", derived, expected)
	}
	return nil
}

func newStagedCommitmentStore(db CommitmentDB) *stagedCommitmentStore {
	branchStore, err := newRawdbBranchStoreWithRepair(db, CommitmentSnapshotRepair{})
	if err == nil {
		return newStagedCommitmentStoreWithBranchStore(db, branchStore)
	}
	store := newStagedCommitmentStoreWithBranchStore(db, newRawdbBranchStore(db))
	store.initErr = err
	return store
}

func newStagedCommitmentStoreWithBranchStore(db CommitmentDB, branchStore *rawdbBranchStore) *stagedCommitmentStore {
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

func (s *stagedCommitmentStore) Close() error {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.close()
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
	if s.initErr != nil {
		return false, s.initErr
	}
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
	if s.initErr != nil {
		return common.Hash{}, false, s.initErr
	}
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
	if s.initErr != nil {
		return false, s.initErr
	}
	if source == nil || expectedRoot == (common.Hash{}) {
		return false, nil
	}
	// A marker-aware store already reads this indexed snapshot in place. If its
	// hash-bound view failed root validation, decline the copying repair and let
	// the orchestrator rebuild from authoritative latest rows. Copying into the
	// ignored legacy namespace could neither repair the active view nor be made
	// crash-atomic without a separate generation switch.
	if s.store.cold != nil {
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
	var batch ethdb.Batch
	if batcher, ok := s.db.(ethdb.Batcher); ok {
		batch = batcher.NewBatch()
	}
	flush := func() error {
		if batch == nil || batch.ValueSize() == 0 {
			return nil
		}
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
		return nil
	}
	if err := branchSource.IterateCommitmentBranches(txNum, func(prefix, encoded []byte) (bool, error) {
		// Validate the encoded value decodes to a BranchData before persisting,
		// so a corrupt snapshot is rejected rather than poisoning the keyspace.
		if _, decodeErr := DecodeBranchData(encoded); decodeErr != nil {
			return false, fmt.Errorf("domains: snapshot branch %x: %w", prefix, decodeErr)
		}
		writer := ethdb.KeyValueWriter(s.db)
		if batch != nil {
			writer = batch
		}
		if err := rawdb.WriteCommitmentBranch(writer, prefix, encoded); err != nil {
			return false, err
		}
		restored++
		if batch != nil && batch.ValueSize() >= ethdb.IdealBatchSize {
			if err := flush(); err != nil {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return false, err
	}
	if err := flush(); err != nil {
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
	if s.initErr != nil {
		return common.Hash{}, s.initErr
	}
	s.store.readParentBranches = s.asyncParentBranches && !s.branchStateWritten
	if s.store.readParentBranches {
		if err := s.store.beginParentRead(); err != nil {
			s.store.readParentBranches = false
			return common.Hash{}, err
		}
	}
	root, err := s.trie.Fold(updates)
	closeErr := s.store.closeParentRead()
	s.store.readParentBranches = false
	if err != nil {
		return common.Hash{}, err
	}
	if closeErr != nil {
		return common.Hash{}, closeErr
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
