package main

import rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"

// A sync planner must not wait on the write lock held across direct V2 segment
// construction. Unknown readiness defers the pass; it never removes the event
// coverage cap or changes the freezer's required receipt-coverage checks.
func makeSyncEventLogTargetBlock(store *rawdbfreezer.Freezer, segmentBlocks uint64, promotionAllowed func() bool) func() (uint64, bool, bool) {
	return func() (target uint64, valid, deferred bool) {
		if store == nil || segmentBlocks == 0 {
			return 0, false, false
		}
		coverage, direct, observed := store.TryDirectV2AppendStatus()
		if !observed {
			return 0, false, true
		}
		if !direct || (promotionAllowed != nil && !promotionAllowed()) {
			return 0, false, false
		}
		if coverage > ^uint64(0)-segmentBlocks {
			return ^uint64(0), true, false
		}
		return coverage + segmentBlocks - 1, true, false
	}
}
