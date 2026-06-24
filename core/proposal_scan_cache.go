package core

import "sync"

// proposalScanCache remembers the set of proposal IDs that have reached a
// terminal (APPROVED/CANCELED) state on the canonical chain, so the
// per-maintenance ProcessProposals scan can skip re-reading and re-decoding
// them. On a long fast-sync the proposal index grows without bound while only a
// handful of proposals are ever PENDING at a boundary; without this cache every
// maintenance boundary re-reads + JSON-decodes every terminal proposal it has
// already resolved (measured at ~8% of total sync CPU on a Nile node, 2026-06-11).
//
// Correctness rests on a single invariant: on a linear chain a proposal's
// terminal state is permanent. ProcessProposals is the only writer that moves a
// proposal PENDING→terminal, and the ProposalApprove/ProposalDelete actuators
// reject already-terminal proposals (and never resurrect one to PENDING), so a
// cached ID is guaranteed to still read as terminal — meaning the read it skips
// would only have produced a `continue`. The single event that can un-terminal a
// proposal is a reorg that rewinds past its resolution; switchFork calls reset()
// so the cache only ever reflects committed canonical state. A failed block
// apply likewise reset()s (the marks it made may belong to abandoned state).
//
// The cache is node-local and never part of consensus state: it changes which
// proposals get re-read, never the outcome of any proposal. It is therefore
// safe to leave nil (every method is nil-receiver safe), which the block
// producer path does — the cache only pays off when replaying many boundaries.
type proposalScanCache struct {
	mu       sync.Mutex
	terminal map[int64]struct{}
}

func newProposalScanCache() *proposalScanCache {
	return &proposalScanCache{terminal: make(map[int64]struct{})}
}

// isTerminal reports whether id was recorded as terminal. A nil cache always
// reports false, so callers degrade to the full (uncached) scan.
func (c *proposalScanCache) isTerminal(id int64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.terminal[id]
	return ok
}

// markTerminal records that id has reached a terminal state. No-op on nil.
func (c *proposalScanCache) markTerminal(id int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal == nil {
		c.terminal = make(map[int64]struct{})
	}
	c.terminal[id] = struct{}{}
}

// reset drops every recorded id. Called whenever cached marks may no longer
// reflect committed canonical state (reorg rewind, failed block apply). The
// next boundary re-reads from state and lazily re-populates. No-op on nil.
func (c *proposalScanCache) reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.terminal = nil
}
