package sync

import "time"

// Tunables shared across the sync sub-packages. Moved verbatim from the
// constants block in net/sync.go; values must stay byte-for-byte
// identical to preserve wire-protocol behaviour. Tests may shrink
// StatsReportInterval directly; for the fetch timeout they override the
// per-instance SyncService.fetchTimeout (seeded from SyncFetchTimeout)
// instead of this global. The `const` entries must not be adjusted.
const (
	// MaxChainInventorySize bounds the number of block IDs returned in a
	// single CHAIN_INVENTORY response. Matches java-tron's
	// SyncService.MAX_BLOCK_FETCH_PER_PEER.
	MaxChainInventorySize = 2000
	// MaxFetchBatch bounds the number of block hashes requested in a
	// single FETCH_INV_DATA from one peer.
	MaxFetchBatch = 100
	// MaxParallelSyncPeers caps how many peers participate in a single
	// sync session at once.
	MaxParallelSyncPeers = 8
)

// MinFetchRequestInterval stays just below java-tron's 3/s FETCH_INV_DATA
// limiter while preserving a one-request-at-a-time contract per peer.
const MinFetchRequestInterval = 350 * time.Millisecond

// SyncFetchTimeout is how long to wait for a block response before failing
// over to another peer. It seeds SyncService.fetchTimeout at construction;
// tests shrink the per-instance field rather than this global so the
// fetch-timer goroutine never races a test's restore.
var SyncFetchTimeout = 30 * time.Second

// StatsReportInterval is the cadence at which sync emits "Imported chain
// segment" summary lines. Exposed as a var so tests can shrink it. Mirrors
// geth's blockchain_insert.go:statsReportLimit.
var StatsReportInterval = 8 * time.Second
