package snapshots

import (
	"slices"
	"sort"
	"sync"
)

// Only Manager-owned immutable manifests and function-local read views carry
// this index. Public loaders and clones return mutable, unindexed manifests.
// Indexes contain row numbers, not duplicate SegmentRefs or owned path strings.
// The two eager uint32 arrays plus a lazy event-row array retain at most
// 12 MiB at the reference limit. Larger catalogs retain the scan path instead
// of growing an unbounded metadata cache.
const manifestLookupMaxReferences = 1 << 20

type manifestLookup struct {
	paths       []uint32
	freezer     []uint32 // same descending range order as chainFreezerRefs
	historyOnce sync.Once
	history     *historySourceTable
	eventOnce   sync.Once
	events      []uint32 // ascending, non-overlapping event source ranges
}

// manifestLookupView borrows m.Segments. The caller must keep that slice
// immutable for the view's lifetime. Managers already own a detached manifest;
// bulk operations use this only within one synchronous read-only call. Never
// attach an index to a caller's externally mutable Manifest in place.
func manifestLookupView(m *Manifest) *Manifest {
	if m == nil || m.lookup != nil || len(m.Segments) > manifestLookupMaxReferences {
		return m
	}
	index := &manifestLookup{paths: make([]uint32, len(m.Segments))}
	freezers := 0
	for i, ref := range m.Segments {
		index.paths[i] = uint32(i)
		if ref.Kind == SegmentChainFreezer && ref.normalizedDataset() == SegmentDatasetChainFreezer {
			freezers++
		}
	}
	slices.SortFunc(index.paths, func(a, b uint32) int {
		if m.Segments[a].Path < m.Segments[b].Path {
			return -1
		}
		if m.Segments[a].Path > m.Segments[b].Path {
			return 1
		}
		// A function-local unvalidated input may contain duplicate paths. Keep
		// its original first-matching-row behavior rather than hiding a row.
		return int(a) - int(b)
	})
	index.freezer = make([]uint32, 0, freezers)
	for i, ref := range m.Segments {
		if ref.Kind == SegmentChainFreezer && ref.normalizedDataset() == SegmentDatasetChainFreezer {
			index.freezer = append(index.freezer, uint32(i))
		}
	}
	sort.SliceStable(index.freezer, func(i, j int) bool {
		return chainFreezerRefLess(m.Segments[index.freezer[i]], m.Segments[index.freezer[j]])
	})
	out := *m
	out.lookup = index
	return &out
}

func findManifestRef(m *Manifest, path string, matches func(SegmentRef) bool) (SegmentRef, bool) {
	if m == nil || path == "" {
		return SegmentRef{}, false
	}
	if m.lookup != nil {
		rows := m.lookup.paths
		first := sort.Search(len(rows), func(i int) bool { return m.Segments[rows[i]].Path >= path })
		for _, row := range rows[first:] {
			ref := m.Segments[row]
			if ref.Path != path {
				break
			}
			if matches(ref) {
				return ref, true
			}
		}
		return SegmentRef{}, false
	}
	for _, ref := range m.Segments {
		if ref.Path == path && matches(ref) {
			return ref, true
		}
	}
	return SegmentRef{}, false
}

// visitEventSourceRefs examines only the intersecting source refs in immutable
// manager views. The lazily built row-number index avoids adding a full catalog
// copy/sort to every narrow RPC. Unindexed diagnostic inputs retain a no-copy
// scan without imposing a new cache above the existing reference limit.
func visitEventSourceRefs(m *Manifest, from, to uint64, visit func(SegmentRef) (bool, error)) (bool, error) {
	if m == nil {
		return true, nil
	}
	if m.lookup == nil {
		for _, ref := range m.Segments {
			if ref.Kind != SegmentEventLog || ref.normalizedDataset() != SegmentDatasetEventLog || ref.ToTxNum < from || ref.FromTxNum > to {
				continue
			}
			if ok, err := visit(ref); err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	index := m.lookup
	index.eventOnce.Do(func() {
		count := 0
		for _, ref := range m.Segments {
			if ref.Kind == SegmentEventLog && ref.normalizedDataset() == SegmentDatasetEventLog {
				count++
			}
		}
		index.events = make([]uint32, 0, count)
		for i, ref := range m.Segments {
			if ref.Kind == SegmentEventLog && ref.normalizedDataset() == SegmentDatasetEventLog {
				index.events = append(index.events, uint32(i))
			}
		}
		slices.SortFunc(index.events, func(a, b uint32) int {
			left, right := m.Segments[a], m.Segments[b]
			if left.FromTxNum < right.FromTxNum {
				return -1
			}
			if left.FromTxNum > right.FromTxNum {
				return 1
			}
			return int(a) - int(b)
		})
	})
	first := sort.Search(len(index.events), func(i int) bool { return m.Segments[index.events[i]].ToTxNum >= from })
	for _, row := range index.events[first:] {
		ref := m.Segments[row]
		if ref.FromTxNum > to {
			break
		}
		if ok, err := visit(ref); err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}
