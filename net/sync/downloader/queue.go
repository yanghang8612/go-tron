package downloader

import "github.com/tronprotocol/go-tron/core/types"

// BlockFilter reports whether a candidate block ID is still eligible for the
// local fetch queue. The caller owns side effects such as path reservation.
type BlockFilter func(types.BlockID) bool

// RetryDecision tells AssignRetryCandidates what to do with one retry entry.
type RetryDecision uint8

const (
	// RetryDrop removes the entry from the retry list without assigning it.
	RetryDrop RetryDecision = iota
	// RetryKeep leaves the entry in the retry list for a later peer/session.
	RetryKeep
	// RetryAssign appends the entry to the target peer's fetch queue.
	RetryAssign
)

// RetryClassifier classifies one retry entry. Callers own any side effects
// needed before assignment, such as reserving a block path.
type RetryClassifier func(types.BlockID) RetryDecision

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

// AssignRetryCandidates partitions a retry list into entries assigned to the
// current peer and entries that should remain retryable. Dropped entries are
// intentionally omitted from both results.
//
// keep may reuse retries' backing array; callers should replace their retry
// list with the returned slice.
func AssignRetryCandidates(retries []types.BlockID, classify RetryClassifier) (assigned []types.BlockID, keep []types.BlockID) {
	if len(retries) == 0 {
		return nil, nil
	}
	keep = retries[:0]
	for _, bid := range retries {
		decision := RetryDrop
		if classify != nil {
			decision = classify(bid)
		}
		switch decision {
		case RetryAssign:
			assigned = append(assigned, bid)
		case RetryKeep:
			keep = append(keep, bid)
		default:
			// RetryDrop or unknown decisions remove the stale entry.
		}
	}
	if len(keep) == 0 {
		keep = nil
	}
	return assigned, keep
}
