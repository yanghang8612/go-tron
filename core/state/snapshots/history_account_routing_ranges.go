package snapshots

import "sort"

// Immutable manifest-owned row numbers and subtree upper bounds. At most
// 12 bytes per delta, independently capped at 3 MiB; no absence proof or file
// bytes live here. This avoids a catalog scan for every canonical source.
type accountDeltaRangeIndex struct {
	rows  []uint32
	maxTo []uint64
}

func manifestAccountDeltaRanges(m *Manifest) *accountDeltaRangeIndex {
	if m == nil || m.lookup == nil {
		return nil
	}
	m.lookup.accountDeltaOnce.Do(func() {
		n := 0
		for _, ref := range m.Segments {
			if ref.Kind == SegmentAccountHistoryRoutingDelta {
				n++
			}
		}
		if n > historyCandidateMaxSources {
			return
		}
		x := &accountDeltaRangeIndex{rows: make([]uint32, 0, n), maxTo: make([]uint64, n)}
		for i, ref := range m.Segments {
			if ref.Kind == SegmentAccountHistoryRoutingDelta {
				x.rows = append(x.rows, uint32(i))
			}
		}
		sort.Slice(x.rows, func(i, j int) bool {
			a, b := m.Segments[x.rows[i]], m.Segments[x.rows[j]]
			if a.FromTxNum != b.FromTxNum {
				return a.FromTxNum < b.FromTxNum
			}
			if a.ToTxNum != b.ToTxNum {
				return a.ToTxNum < b.ToTxNum
			}
			return x.rows[i] < x.rows[j]
		})
		var build func(int, int) uint64
		build = func(lo, hi int) uint64 {
			if lo == hi {
				return 0
			}
			mid := lo + (hi-lo)/2
			x.maxTo[mid] = max(m.Segments[x.rows[mid]].ToTxNum, build(lo, mid), build(mid+1, hi))
			return x.maxTo[mid]
		}
		build(0, n)
		m.lookup.accountDelta = x
	})
	return m.lookup.accountDelta
}

// Only enclosing ranges can carry a proof for a whole exact canonical pair.
// The callback still authenticates metadata and matches that pair ID; an
// envelope match alone never grants absence. Return true to stop walking.
func (x *accountDeltaRangeIndex) visit(m *Manifest, history SegmentRef, fn func(uint32, SegmentRef) bool) {
	if x == nil {
		return
	}
	var walk func(int, int) bool
	walk = func(lo, hi int) bool {
		if lo == hi {
			return false
		}
		mid := lo + (hi-lo)/2
		if x.maxTo[mid] < history.ToTxNum || m.Segments[x.rows[lo]].FromTxNum > history.FromTxNum {
			return false
		}
		if walk(lo, mid) {
			return true
		}
		ref := m.Segments[x.rows[mid]]
		if ref.FromTxNum <= history.FromTxNum && ref.ToTxNum >= history.ToTxNum && fn(x.rows[mid], ref) {
			return true
		}
		return walk(mid+1, hi)
	}
	walk(0, len(x.rows))
}
