package rawdb

import (
	"bytes"

	"golang.org/x/crypto/blake2b"
)

// Request-owned, immutable source-bound metadata only. It cannot publish a
// complete owner-row cache, nor survive the pinned history view that owns it.
const historyTailPackFilterCacheBytes = 4 << 20

type historyTailPackFilters struct {
	digest  [32]byte
	known   uint8
	filters [historyQueryKeyGroups][]byte
}

func (v *stateHistoryQueryTailView) cachedPackFilters(block uint64, pack []byte) *historyTailPackFilters {
	v.mu.Lock()
	r := v.packFilters[block]
	v.mu.Unlock()
	if r != nil && blake2b.Sum256(pack) == r.digest {
		return r
	}
	return nil
}

func (v *stateHistoryQueryTailView) rememberPackFilters(block uint64, r *historyOwnerSpans) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.packFilters[block] != nil {
		return
	}
	size := 128 // Includes entry and map charge; payload lengths are fixed.
	for _, filter := range r.queryFilters {
		size += len(filter)
	}
	if size > historyTailPackFilterCacheBytes-v.packFilterBytes {
		return
	}
	next := &historyTailPackFilters{digest: r.digest, known: r.queryKnown}
	for group, filter := range r.queryFilters {
		next.filters[group] = bytes.Clone(filter) // Decoder storage is pooled.
	}
	if v.packFilters == nil {
		v.packFilters = make(map[uint64]*historyTailPackFilters)
	}
	v.packFilters[block] = next
	v.packFilterBytes += size
}

func (q *historyTailKeyQuery) filtersMayMatch(known uint8, filters [historyQueryKeyGroups][]byte) bool {
	if known&(1<<q.group) == 0 {
		return true
	}
	for name := range q.wanted {
		if historyQueryFilterContains(filters[q.group], q.hashes[name]) {
			return true
		}
	}
	return false
}
