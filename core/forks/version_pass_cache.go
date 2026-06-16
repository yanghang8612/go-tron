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
// Only the TRUE result is memoized: a still-pending version's bitmap is still
// accumulating votes, so it is re-read live on every call and its answer can
// still flip to true the moment quorum is reached. The cache therefore never
// changes an answer — it only elides redundant reads — and is safe to leave
// nil (every method is nil-receiver safe), which the single-block producer
// path does (one block has nothing to amortize).
//
// All access happens on the serial block-execution thread (processBlock via
// the actuator Context, switchFork, the applyBlockWithPlan error path); the
// async commit worker never touches it. The mutex only guards against an
// incidental diagnostic reader and mirrors proposalScanCache.
type VersionPassCache struct {
	mu     sync.Mutex
	passed map[int32]struct{}
}

// NewVersionPassCache returns an empty cache ready for the block-execution path.
func NewVersionPassCache() *VersionPassCache {
	return &VersionPassCache{passed: make(map[int32]struct{})}
}

// Pass is PassVersionFromStore(store, version, latestBlockTime,
// maintenanceIntervalMs) with a monotonic once-true short-circuit: a version
// previously observed to pass returns true without re-reading the store. A nil
// cache degrades to the plain uncached call. Only true is memoized — a pending
// version is always re-read live.
func (c *VersionPassCache) Pass(store ForkStatsReader, version int32, latestBlockTime, maintenanceIntervalMs int64) bool {
	if c == nil {
		return PassVersionFromStore(store, version, latestBlockTime, maintenanceIntervalMs)
	}
	c.mu.Lock()
	_, ok := c.passed[version]
	c.mu.Unlock()
	if ok {
		return true
	}
	passed := PassVersionFromStore(store, version, latestBlockTime, maintenanceIntervalMs)
	if passed {
		c.mu.Lock()
		if c.passed == nil {
			c.passed = make(map[int32]struct{})
		}
		c.passed[version] = struct{}{}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.passed[version]
	return ok
}

// Reset drops every memoized version. Called on reorg (switchFork) and failed
// apply, where a rewind below a version's activation block could revert it to
// pending; the next call then re-reads live state. nil-safe.
func (c *VersionPassCache) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.passed = nil
	c.mu.Unlock()
}
