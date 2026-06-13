package downloader

// RetryCandidateFacts are the side-effect-free facts needed to decide how a
// retry-list block ID should move through the downloader queue.
type RetryCandidateFacts struct {
	KnownOrRequested bool
	InWindow         bool
	PeerRequested    bool
	ReservedPath     bool
}

// ClassifyRetryCandidate maps one retry-list block ID to the next retry-list
// operation. Callers own side effects such as path reservation and should
// gather facts in the same order they previously used inline.
func ClassifyRetryCandidate(f RetryCandidateFacts) RetryDecision {
	if f.KnownOrRequested {
		return RetryDrop
	}
	if !f.InWindow || f.PeerRequested {
		return RetryKeep
	}
	if !f.ReservedPath {
		return RetryDrop
	}
	return RetryAssign
}

// FetchCandidateFacts are the side-effect-free facts needed to decide whether
// a peer-local fetch-list block ID is eligible for the next wire batch.
type FetchCandidateFacts struct {
	KnownOrRequested bool
	ReservedPath     bool
	PeerRequested    bool
}

// AcceptFetchCandidate reports whether a fetch-list block ID should be emitted
// in the next FETCH_INV_DATA request. Ineligible entries are dropped from the
// peer-local fetch queue by PopFetchBatch.
func AcceptFetchCandidate(f FetchCandidateFacts) bool {
	return !f.KnownOrRequested && f.ReservedPath && !f.PeerRequested
}
