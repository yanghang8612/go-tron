package forks

import "sync"

// VersionPassCache memoizes the set of SR software-fork versions already
// observed to PASS (ceil-aligned HardForkTime gate + vote-rate quorum met) on
// the canonical chain, so the per-transaction fork gate during block execution
// can skip the fork-stats store read + vote tally for a version that has
// already activated. On a synced / near-head node every relevant version
// (energy-bill, multi-sig-v2, cpu-time-guard, …) passed millions of blocks ago,
// yet the uncached gate re-reads and re-tallies its bitmap once per
// transaction — measured at ~3.6% of total sync CPU on a Nile node
// (2026-06-14). This mirrors the proposalScanCache terminal-skip pattern
// (core/proposal_scan_cache.go).
//
// Correctness rests on one invariant, identical to java-tron's
// ForkController.latestVersion freeze: on a linear chain a version's "passed"
// state is monotonic. PassVersionFromStore answers true only once both the
// ceil-aligned HardForkTime gate (monotonic in the strictly-increasing latest
// block time) and the vote-rate quorum hold; java-tron then calls
// saveLatestVersion + upgrade(), freezing the bitmap all-upgrade so the gate
// never re-opens, and go-tron's ForkController.Reset preserves passed versions
// across maintenance boundaries. So once Pass(V) is true it stays true for
// every later block — EXCEPT across a reorg that rewinds below V's activation
// block, which is exactly why switchFork (and a failed apply) call Reset.
//
// Passed TRUE results are memoized across blocks. A block-scoped view created
// by BlockScope additionally memoizes FALSE results for that one block: fork
// votes and the previous-head timestamp are immutable while processBlock runs,
// so every transaction in the block must receive the same answer. The next
// block gets a fresh view and therefore re-reads pending versions, allowing a
// quorum transition to become visible at the exact same boundary as the
// uncached implementation.
//
// All access happens on the serial block-execution thread (processBlock via
// the actuator Context, switchFork, the applyBlockWithPlan error path); the
// async commit worker never touches it. The mutex only guards against an
// incidental diagnostic reader and mirrors proposalScanCache.
type VersionPassCache struct {
	mu     sync.Mutex
	passed map[int32]struct{}

	// parent is non-nil only for a block-scoped view. Cross-block TRUE results
	// live in the root cache; pending contains FALSE results observed by this
	// block only and is discarded with the view after processBlock returns.
	parent  *VersionPassCache
	pending map[int32]struct{}
}

// NewVersionPassCache returns an empty cache ready for the block-execution path.
func NewVersionPassCache() *VersionPassCache {
	return &VersionPassCache{passed: make(map[int32]struct{})}
}

// BlockScope returns an isolated per-block view of c. The view shares c's
// monotonic passed-version set but owns a fresh pending-version set, allowing
// repeated FALSE gates within one block to avoid the rooted fork-stats read.
// Callers must not reuse the returned view for another block. Nil is preserved
// so optional-cache call sites retain their existing behaviour.
func (c *VersionPassCache) BlockScope() *VersionPassCache {
	if c == nil {
		return nil
	}
	return &VersionPassCache{
		parent:  c.root(),
		pending: make(map[int32]struct{}),
	}
}

// Pass is PassVersionFromStore(store, version, latestBlockTime,
// maintenanceIntervalMs) with two short-circuits: a version observed to pass
// returns true across all later blocks, while a pending version observed by a
// BlockScope returns false for the rest of that block. A nil cache degrades to
// the plain uncached call. Calling Pass on the root cache preserves the legacy
// true-only behaviour.
func (c *VersionPassCache) Pass(store ForkStatsReader, version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	if c == nil {
		return PassVersionFromStore(store, version, latestBlockTime, maintenanceIntervalMs)
	}
	root := c.root()
	root.mu.Lock()
	_, ok := root.passed[version]
	root.mu.Unlock()
	if ok {
		return true
	}
	if c != root {
		c.mu.Lock()
		_, ok = c.pending[version]
		c.mu.Unlock()
		if ok {
			return false
		}
	}
	passed := PassVersionFromStore(store, version, latestBlockTime, maintenanceIntervalMs)
	if passed {
		root.mu.Lock()
		if root.passed == nil {
			root.passed = make(map[int32]struct{})
		}
		root.passed[version] = struct{}{}
		root.mu.Unlock()
	} else if c != root {
		c.mu.Lock()
		c.pending[version] = struct{}{}
		c.mu.Unlock()
	}
	return passed
}

// IsPassed reports whether version is currently memoized as passed. For tests
// and diagnostics; nil-safe.
func (c *VersionPassCache) IsPassed(version int32) bool {
	if c == nil {
		return false
	}
	root := c.root()
	root.mu.Lock()
	defer root.mu.Unlock()
	_, ok := root.passed[version]
	return ok
}

// Reset drops every memoized version. Called on reorg (switchFork) and failed
// apply, where a rewind below a version's activation block could revert it to
// pending; the next call then re-reads live state. nil-safe.
func (c *VersionPassCache) Reset() {
	if c == nil {
		return
	}
	root := c.root()
	root.mu.Lock()
	root.passed = nil
	root.mu.Unlock()
	if c != root {
		c.mu.Lock()
		clear(c.pending)
		c.mu.Unlock()
	}
}

func (c *VersionPassCache) root() *VersionPassCache {
	if c.parent != nil {
		return c.parent
	}
	return c
}
