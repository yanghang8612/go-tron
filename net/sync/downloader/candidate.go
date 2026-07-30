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
	// Deferred keeps the candidate queued without attempting path reservation.
	// The service uses this for temporary local backpressure such as the
	// buffered-runahead budget.
	Deferred         bool
	KnownOrRequested bool
	ReservedPath     bool
	PeerRequested    bool
}

// FetchCandidateDecision records why a peer-local fetch-list block ID was
// accepted or discarded before the next wire batch.
type FetchCandidateDecision uint8

const (
	FetchCandidateAccepted FetchCandidateDecision = iota
	FetchCandidateDeferred
	FetchCandidateKnownOrRequested
	FetchCandidatePathConflict
	FetchCandidatePeerDuplicate
)

// ClassifyFetchCandidate maps one fetch-list block ID to an explicit
// downloader decision. The service owns fact gathering and path reservation;
// the downloader owns the policy ordering.
func ClassifyFetchCandidate(f FetchCandidateFacts) FetchCandidateDecision {
	if f.Deferred {
		return FetchCandidateDeferred
	}
	if f.KnownOrRequested {
		return FetchCandidateKnownOrRequested
	}
	if !f.ReservedPath {
		return FetchCandidatePathConflict
	}
	if f.PeerRequested {
		return FetchCandidatePeerDuplicate
	}
	return FetchCandidateAccepted
}

// AcceptFetchCandidate reports whether a fetch-list block ID should be emitted
// in the next FETCH_INV_DATA request. Ineligible entries are dropped from the
// peer-local fetch queue by PopFetchBatch.
func AcceptFetchCandidate(f FetchCandidateFacts) bool {
	return ClassifyFetchCandidate(f) == FetchCandidateAccepted
}

// InventoryCandidateFacts are the side-effect-free facts needed to decide
// whether a CHAIN_INVENTORY block ID should enter a peer-local fetch queue.
type InventoryCandidateFacts struct {
	KnownOrRequested bool
	PeerRequested    bool
	ReservedPath     bool
}

// AcceptInventoryCandidate reports whether an advertised block ID should be
// queued for future fetch. Callers own the chain/cache lookups and path
// reservation side effects.
func AcceptInventoryCandidate(f InventoryCandidateFacts) bool {
	return !f.KnownOrRequested && !f.PeerRequested && f.ReservedPath
}
