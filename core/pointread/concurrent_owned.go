package pointread

// ConcurrentOwnedKeyValueView is an explicit, opt-in contract for speculative
// history authentication. Get and Has may run concurrently with each other and
// with independently owned iterators on the same pinned sequence. Every Get
// returns caller-owned bytes. Iterators themselves are single-caller objects.
// All operations must finish before Close. Neither pinning nor ownership alone
// implies this capability; wrappers may expose it only after their own audit.
// This does not audit GetWithPresence. The history pipeline explicitly rejects
// adapters exposing that separate operation instead of changing its semantics.
type ConcurrentOwnedKeyValueView interface {
	PinnedKeyValueView
	OwnedKeyValueReader
	ConcurrentOwnedHistoryReads() bool
}
