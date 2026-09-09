package snapshots

import (
	"slices"
	"sort"
)

// Only Manager-owned immutable manifests and function-local read views carry
// this index. Public loaders and clones return mutable, unindexed manifests.
// Indexes contain row numbers, not duplicate SegmentRefs or owned path strings.
// At most two uint32 arrays are retained: 8 MiB at the reference limit. Larger
// catalogs retain the scan path instead of growing an unbounded second cache.
const manifestLookupMaxReferences = 1 << 20

type manifestLookup struct {
	paths   []uint32
	freezer []uint32 // same descending range order as chainFreezerRefs
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
