package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// seedProposalScanState builds a fresh StateDB seeded with the given proposals
// (and an index covering them), returning it alongside an empty DynamicProperties.
func seedProposalScanState(t *testing.T, proposals []*rawdb.Proposal) (*state.StateDB, *state.DynamicProperties, ethdb.Database) {
	t.Helper()
	db := ethrawdb.NewMemoryDatabase()
	statedb, err := state.New(tcommon.Hash{}, state.NewDatabase(db))
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	ids := make([]int64, 0, len(proposals))
	for _, p := range proposals {
		if err := statedb.WriteProposal(p.ID, p); err != nil {
			t.Fatalf("WriteProposal %d: %v", p.ID, err)
		}
		ids = append(ids, p.ID)
	}
	if err := statedb.WriteProposalIndex(ids); err != nil {
		t.Fatalf("WriteProposalIndex: %v", err)
	}
	return statedb, state.NewDynamicProperties(), db
}

// TestProcessProposalsScanCacheMarksTerminalSkipsPending locks the cache
// bookkeeping: a maintenance scan records every proposal that reached a
// terminal (APPROVED/CANCELED) state and never records one that stays PENDING.
func TestProcessProposalsScanCacheMarksTerminalSkipsPending(t *testing.T) {
	w1 := tcommon.Address{0x41, 0x01}
	w2 := tcommon.Address{0x41, 0x02}
	active := []tcommon.Address{w1, w2} // threshold = 2*7/10 = 1

	const maint = int64(1000)
	approved := &rawdb.Proposal{ID: 1, Parameters: map[int64]int64{9: 1}, ExpirationTime: maint - 1, Approvals: []tcommon.Address{w1}, State: rawdb.ProposalStatePending}
	canceled := &rawdb.Proposal{ID: 2, Parameters: map[int64]int64{13: 50}, ExpirationTime: maint - 1, Approvals: nil, State: rawdb.ProposalStatePending}
	pending := &rawdb.Proposal{ID: 3, Parameters: map[int64]int64{14: 1}, ExpirationTime: maint + 100, Approvals: []tcommon.Address{w1}, State: rawdb.ProposalStatePending}

	statedb, dp, db := seedProposalScanState(t, []*rawdb.Proposal{approved, canceled, pending})

	cache := newProposalScanCache()
	if err := ProcessProposals(db, statedb, dp, active, maint, nil, cache); err != nil {
		t.Fatalf("ProcessProposals: %v", err)
	}

	if !cache.isTerminal(1) {
		t.Error("approved proposal 1 should be cached as terminal")
	}
	if !cache.isTerminal(2) {
		t.Error("canceled proposal 2 should be cached as terminal")
	}
	if cache.isTerminal(3) {
		t.Error("still-pending proposal 3 must NOT be cached as terminal")
	}
}

// runTwoBoundaryScenario drives a state through two maintenance boundaries with
// the supplied cache (nil = uncached), creating a fresh proposal between them so
// the second boundary must re-scan while skipping the now-terminal first batch.
// Returns the final proposal states and the touched dynamic-property values.
func runTwoBoundaryScenario(t *testing.T, cache *proposalScanCache) (map[int64]int32, map[string]int64) {
	t.Helper()
	w1 := tcommon.Address{0x41, 0x01}
	w2 := tcommon.Address{0x41, 0x02}
	active := []tcommon.Address{w1, w2} // threshold = 2*7/10 = 1

	// Boundary 1 at t=1000: P1 approved (sets #9), P2 canceled (0 approvals),
	// P3 not yet expired (stays pending).
	p1 := &rawdb.Proposal{ID: 1, Parameters: map[int64]int64{9: 1}, ExpirationTime: 999, Approvals: []tcommon.Address{w1}, State: rawdb.ProposalStatePending}
	p2 := &rawdb.Proposal{ID: 2, Parameters: map[int64]int64{13: 50}, ExpirationTime: 999, Approvals: nil, State: rawdb.ProposalStatePending}
	p3 := &rawdb.Proposal{ID: 3, Parameters: map[int64]int64{14: 1}, ExpirationTime: 2000, Approvals: []tcommon.Address{w1}, State: rawdb.ProposalStatePending}
	statedb, dp, db := seedProposalScanState(t, []*rawdb.Proposal{p1, p2, p3})

	if err := ProcessProposals(db, statedb, dp, active, 1000, nil, cache); err != nil {
		t.Fatalf("boundary 1: %v", err)
	}

	// Create P4 after boundary 1 (sets #16 once approved at boundary 2).
	p4 := &rawdb.Proposal{ID: 4, Parameters: map[int64]int64{16: 1}, ExpirationTime: 1999, Approvals: []tcommon.Address{w2}, State: rawdb.ProposalStatePending}
	if err := statedb.WriteProposal(p4.ID, p4); err != nil {
		t.Fatalf("WriteProposal 4: %v", err)
	}
	if err := statedb.WriteProposalIndex([]int64{1, 2, 3, 4}); err != nil {
		t.Fatalf("WriteProposalIndex: %v", err)
	}

	// Boundary 2 at t=2000: P3 + P4 now expired and approved; P1/P2 already
	// terminal (the warm cache skips re-reading them).
	if err := ProcessProposals(db, statedb, dp, active, 2000, nil, cache); err != nil {
		t.Fatalf("boundary 2: %v", err)
	}

	states := make(map[int64]int32)
	for _, id := range []int64{1, 2, 3, 4} {
		p := statedb.ReadProposal(id)
		if p == nil {
			t.Fatalf("proposal %d missing after scenario", id)
		}
		states[id] = p.State
	}
	dpv := make(map[string]int64)
	// allow_* keys default to 0, so == 1 proves the approved proposal applied;
	// max_cpu_time_of_one_tx defaults to 50 (java parity) and belongs to the
	// canceled P2 — tracked only to assert cached/uncached equivalence on it.
	for _, name := range []string{"allow_creation_of_contracts", "max_cpu_time_of_one_tx", "allow_update_account_name", "allow_delegate_resource"} {
		v, _ := dp.Get(name)
		dpv[name] = v
	}
	return states, dpv
}

// TestProcessProposalsScanCacheEquivalentToUncached is the safety property: a
// warm scan cache must produce byte-identical proposal states and DP values to
// the uncached full-scan path across multiple maintenance boundaries.
func TestProcessProposalsScanCacheEquivalentToUncached(t *testing.T) {
	uncachedStates, uncachedDP := runTwoBoundaryScenario(t, nil)
	cachedStates, cachedDP := runTwoBoundaryScenario(t, newProposalScanCache())

	wantStates := map[int64]int32{
		1: rawdb.ProposalStateApproved,
		2: rawdb.ProposalStateCanceled,
		3: rawdb.ProposalStateApproved,
		4: rawdb.ProposalStateApproved,
	}
	for id, want := range wantStates {
		if uncachedStates[id] != want {
			t.Errorf("uncached proposal %d state = %d, want %d", id, uncachedStates[id], want)
		}
		if cachedStates[id] != uncachedStates[id] {
			t.Errorf("cached proposal %d state = %d diverges from uncached %d", id, cachedStates[id], uncachedStates[id])
		}
	}
	// Approved proposals applied their params (these keys default to 0).
	wantApplied := map[string]int64{
		"allow_creation_of_contracts": 1, // P1
		"allow_update_account_name":   1, // P3
		"allow_delegate_resource":     1, // P4
	}
	for name, want := range wantApplied {
		if uncachedDP[name] != want {
			t.Errorf("uncached DP %q = %d, want %d", name, uncachedDP[name], want)
		}
	}
	// The core safety property: every touched DP key is identical with and
	// without the cache (including max_cpu_time_of_one_tx, whose canceled P2
	// must NOT have moved it off the default under either path).
	for name := range uncachedDP {
		if cachedDP[name] != uncachedDP[name] {
			t.Errorf("cached DP %q = %d diverges from uncached %d", name, cachedDP[name], uncachedDP[name])
		}
	}
}

// TestProcessProposalsScanCacheResetReprocessesAfterRewind proves reset() is
// load-bearing for reorg safety: a proposal cached terminal on one branch must
// be re-processed from scratch after a rewind re-presents it as PENDING, which
// only happens if the cache is cleared (as switchFork / a failed apply do).
func TestProcessProposalsScanCacheResetReprocessesAfterRewind(t *testing.T) {
	w1 := tcommon.Address{0x41, 0x01}
	w2 := tcommon.Address{0x41, 0x02}
	active := []tcommon.Address{w1, w2} // threshold = 1

	// Each call presents a fresh PENDING P1 (an independent branch's state) and
	// runs one maintenance boundary with the given cache, returning P1's state.
	runOneBoundary := func(cache *proposalScanCache) int32 {
		p1 := &rawdb.Proposal{ID: 1, Parameters: map[int64]int64{9: 1}, ExpirationTime: 999, Approvals: []tcommon.Address{w1}, State: rawdb.ProposalStatePending}
		statedb, dp, db := seedProposalScanState(t, []*rawdb.Proposal{p1})
		if err := ProcessProposals(db, statedb, dp, active, 1000, nil, cache); err != nil {
			t.Fatalf("ProcessProposals: %v", err)
		}
		return statedb.ReadProposal(1).State
	}

	cache := newProposalScanCache()
	// Branch A resolves P1 → terminal, warming the cache.
	if got := runOneBoundary(cache); got != rawdb.ProposalStateApproved {
		t.Fatalf("branch A: P1 state = %d, want APPROVED", got)
	}
	if !cache.isTerminal(1) {
		t.Fatal("cache should be warm with P1 terminal after branch A")
	}

	// A rewind re-presents P1 as PENDING. Without reset the warm cache wrongly
	// skips it, leaving it PENDING — the hazard reset() exists to prevent.
	if got := runOneBoundary(cache); got != rawdb.ProposalStatePending {
		t.Fatalf("stale warm cache should skip P1 (leaving it PENDING); got state %d", got)
	}

	// reset() is exactly what switchFork / failed-apply call; after it the
	// rewound proposal is processed correctly again.
	cache.reset()
	if got := runOneBoundary(cache); got != rawdb.ProposalStateApproved {
		t.Fatalf("after reset P1 should be re-approved; got state %d", got)
	}
}
