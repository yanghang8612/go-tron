package downloader

import "github.com/tronprotocol/go-tron/core/types"

// BlockFilter reports whether a candidate block ID is still eligible for the
// local fetch queue. The caller owns side effects such as path reservation.
type BlockFilter func(types.BlockID) bool

// PopFetchBatch filters candidates in order, returns up to max eligible block
// IDs for the next FETCH_INV_DATA request, and keeps later eligible IDs in
// remaining. Ineligible IDs are dropped.
//
// remaining may reuse candidates' backing array; callers should replace their
// queue with the returned slice.
func PopFetchBatch(candidates []types.BlockID, max int, accept BlockFilter) (batch []types.BlockID, remaining []types.BlockID) {
	if len(candidates) == 0 || max <= 0 {
		return nil, candidates
	}
	batch = make([]types.BlockID, 0, max)
	remaining = candidates[:0]
	for _, bid := range candidates {
		if accept != nil && !accept(bid) {
			continue
		}
		if len(batch) < max {
			batch = append(batch, bid)
			continue
		}
		remaining = append(remaining, bid)
	}
	if len(remaining) == 0 {
		remaining = nil
	}
	return batch, remaining
}
