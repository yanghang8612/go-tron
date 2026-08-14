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
	// MaxImportBatch is the default local staged import range. It is
	// intentionally smaller than MaxFetchBatch so normal sync uses
	// lower-memory execution/commit chunks.
	MaxImportBatch = 32
	// MaxStagedImportBatch caps an operator-selected local execution range. It
	// is deliberately independent of MaxFetchBatch: FETCH_INV_DATA remains
	// wire-compatible at 100 blocks while a ready body backlog from multiple
	// peers or restart recovery can be committed as one larger staged range.
	MaxStagedImportBatch = 1024
	// MaxParallelSyncPeers caps how many peers participate in a single
	// sync session at once.
	MaxParallelSyncPeers = 8

	// MaxBufferedRunaheadBlocks bounds how far fetch scheduling may run ahead
	// of the locally applied tip, regardless of how many peer windows overlap.
	MaxBufferedRunaheadBlocks = 20000

	// AlwaysFetchRunaheadBlocks leaves a near-tip strip available after the
	// byte budget is reached so a missing contiguous block cannot starve drain.
	AlwaysFetchRunaheadBlocks = 2 * MaxFetchBatch
)

// MaxBufferedRunaheadBytes caps raw bodies held in the in-memory sync buffer
// before far-ahead requests are parked.
const MaxBufferedRunaheadBytes = 512 << 20

// ResumeBufferedRunaheadBytes is the low-water mark for global fetch
// backpressure. Keeping it below MaxBufferedRunaheadBytes prevents every small
// drain from reopening all peer fetch slots and immediately refilling the heap.
const ResumeBufferedRunaheadBytes = 256 << 20

// MinFetchRequestInterval stays just below java-tron's 3/s FETCH_INV_DATA
// limiter while preserving a one-request-at-a-time contract per peer.
const MinFetchRequestInterval = 400 * time.Millisecond

// SyncFetchTimeout is how long to wait for a block response before failing
// over to another peer. It seeds SyncService.fetchTimeout at construction;
// tests shrink the per-instance field rather than this global so the
// fetch-timer goroutine never races a test's restore.
var SyncFetchTimeout = 30 * time.Second

// StatsReportInterval is the cadence at which sync emits compact operational
// progress. Detailed per-window execution diagnostics remain available at
// debug level. Exposed as a var so tests can shrink it.
var StatsReportInterval = 30 * time.Second
