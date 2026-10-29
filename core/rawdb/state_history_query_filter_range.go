package rawdb

import (
	"context"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const historyTailFilterRepairBytes = 1 << 20

// Exact-range negative proof and raw positive-sequence repairs, owned by one
// pinned request. It does not imply a complete owner-row cache.
type historyTailFilterRange struct {
	coveredTo uint64
	seen      bool
	envelopes map[uint64]bool
	from, to  uint64
	known     uint8
	valid     bool
	filters   [historyQueryKeyGroups][]byte
	repairs   []*StateDomainChange
	bytes     int
}

func newHistoryTailFilterRange(from, to uint64) *historyTailFilterRange {
	r := &historyTailFilterRange{from: from, to: to, known: 7, valid: true}
	for g, n := range historyQueryFilterBytes {
		r.filters[g] = make([]byte, n)
	}
	return r
}
func (r *historyTailFilterRange) add(block uint64, known uint8, filters [historyQueryKeyGroups][]byte) {
	if r == nil || !r.valid {
		return
	}
	if (!r.seen && block != r.from) || (r.seen && block != r.coveredTo+1) {
		r.valid = false
		return
	}
	r.seen = true
	r.coveredTo = block
	for hash, negative := range r.envelopes {
		if negative && (known&1 == 0 || historyQueryFilterContains(filters[0], hash)) {
			r.envelopes[hash] = false
		}
	}
	r.known &= known
	for g, filter := range filters {
		if known&(1<<g) == 0 {
			continue
		}
		if len(filter) != len(r.filters[g]) {
			r.valid = false
			return
		}
		for i, b := range filter {
			r.filters[g][i] |= b
		}
	}
}
func (r *historyTailFilterRange) addRepair(row *StateDomainChange) {
	if r == nil || !r.valid {
		return
	}
	n := 256 + len(row.Key) + len(row.Prev) + len(row.Next)
	if n > historyTailFilterRepairBytes-r.bytes {
		r.valid = false
		r.repairs = nil
		return
	}
	r.repairs = append(r.repairs, cloneStateDomainChange(row))
	r.bytes += n
}
func (q *historyTailKeyQuery) observeFilters(block uint64, known uint8, filters [historyQueryKeyGroups][]byte) {
	q.summary.add(block, known, filters)
}
func (q *historyTailKeyQuery) noFilterProof() {
	if q.summary != nil {
		q.summary.valid = false
		q.summary.repairs = nil
	}
}
func (v *stateHistoryQueryTailView) cachedFilterRange(from, to uint64, q *historyTailKeyQuery) *historyTailFilterRange {
	v.mu.Lock()
	r := v.filterRange
	v.mu.Unlock()
	if r != nil && r.from == from && r.to == to && !r.mayMatch(q) {
		return r
	}
	return nil
}
func (r *historyTailFilterRange) iterateRepairs(ctx context.Context, owner common.Address, fn func(*StateDomainChange) (bool, error)) error {
	for _, row := range r.repairs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if row.Owner != owner {
			continue
		}
		cont, err := fn(cloneStateDomainChange(row))
		if err != nil || !cont {
			return err
		}
	}
	return ctx.Err()
}

func (r *historyTailFilterRange) planEnvelopes(owners map[common.Address]bool, owner common.Address) {
	r.envelopes = make(map[uint64]bool, 2*(len(owners)+1))
	add := func(o common.Address) {
		for _, flat := range []StateFlatDomain{StateFlatDomainAccountLatest, StateFlatDomainKVGeneration} {
			key, ok := stateDomainChangeLatestKeyByKey(flat, o, 0, kvdomains.KVDomain(0), nil)
			if ok {
				r.envelopes[historyQueryKeyHash(key)] = true
			}
		}
	}
	for o := range owners {
		add(o)
	}
	add(owner)
	// Probe the omitted caller too; this reads no canonical caller rows.
	add(common.Address{})
}
func (r *historyTailFilterRange) mayMatch(q *historyTailKeyQuery) bool {
	if q.group == 0 {
		negative := true
		for name := range q.wanted {
			if !r.envelopes[q.hashes[name]] {
				negative = false
				break
			}
		}
		if negative {
			return false
		}
	}
	return q.filtersMayMatch(r.known, r.filters)
}
