package forks

import "testing"

// statsWithUpvotes returns a witness bitmap of `total` slots with the first
// `up` voting VoteUpgrade and the remainder VoteDowngrade.
func statsWithUpvotes(total, up int) []byte {
	s := make([]byte, total)
	for i := 0; i < up && i < total; i++ {
		s[i] = VoteUpgrade
	}
	return s
}

// blockTimeBeforeFork returns a timestamp one millisecond before version's
// ceil-aligned hard-fork time, so the time gate blocks regardless of votes.
func blockTimeBeforeFork(version int32) int64 {
	vp, _ := lookupVersion(version)
	aligned := ((vp.HardForkTime-1)/maintenanceInterval + 1) * maintenanceInterval
	return aligned - 1
}

// TestVersionPassCache_NilDelegatesToUncached locks the nil-receiver contract:
// a nil cache must be safe and answer exactly like the plain uncached call, so
// the single-block producer path can pass nil with zero behaviour change.
func TestVersionPassCache_NilDelegatesToUncached(t *testing.T) {
	var c *VersionPassCache // nil
	store := newCountingForkStore()

	store.data[35] = statsWithUpvotes(27, 10) // below quorum → false
	if got, want := c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval),
		PassVersionFromStore(store, 35, blockTimeAfterFork(35), maintenanceInterval); got != want {
		t.Fatalf("nil cache (pending): got %v want %v", got, want)
	}
	store.data[35] = statsWithUpvotes(27, 19) // at quorum → true
	if got, want := c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval),
		PassVersionFromStore(store, 35, blockTimeAfterFork(35), maintenanceInterval); got != want {
		t.Fatalf("nil cache (passed): got %v want %v", got, want)
	}
}

// runVersionPassBoundaryScenario drives version 35 from pending to passed —
// time-gated, then rate-gated, then crossing the quorum, then a later block —
// answering each step through the supplied cache (nil = uncached).
func runVersionPassBoundaryScenario(cache *VersionPassCache) []bool {
	store := newCountingForkStore()
	var out []bool
	// 1) full votes but before the aligned hard-fork time → false (time gate).
	store.data[35] = statsWithUpvotes(27, 27)
	out = append(out, cache.Pass(store, 35, blockTimeBeforeFork(35), maintenanceInterval))
	// 2) past the time gate, just below the 70% quorum → false (rate gate).
	store.data[35] = statsWithUpvotes(27, 18)
	out = append(out, cache.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval))
	// 3) past the time gate, at quorum (ceil(0.70*27)=19) → true (records it).
	store.data[35] = statsWithUpvotes(27, 19)
	out = append(out, cache.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval))
	// 4) a strictly later block with even more votes → still true.
	store.data[35] = statsWithUpvotes(27, 25)
	out = append(out, cache.Pass(store, 35, blockTimeAfterFork(35)+maintenanceInterval, maintenanceInterval))
	return out
}

// TestVersionPassCache_EquivalentToUncachedAcrossForkBoundary is the core
// byte-identity property: a warm cache must answer exactly like the uncached
// store tally at every step of a fork-activation crossing.
func TestVersionPassCache_EquivalentToUncachedAcrossForkBoundary(t *testing.T) {
	uncached := runVersionPassBoundaryScenario(nil)
	cached := runVersionPassBoundaryScenario(NewVersionPassCache())

	want := []bool{false, false, true, true}
	for i := range want {
		if uncached[i] != want[i] {
			t.Errorf("step %d uncached = %v, want %v", i, uncached[i], want[i])
		}
		if cached[i] != uncached[i] {
			t.Errorf("step %d cached = %v diverges from uncached %v", i, cached[i], uncached[i])
		}
	}
}

// TestVersionPassCache_ShortCircuitsStoreAfterPass gives the optimization
// teeth: once a version passes, a later call must answer true WITHOUT touching
// the store again — even if the underlying bitmap is later corrupted below
// quorum (which on a real chain only a reorg can do, and that calls Reset).
func TestVersionPassCache_ShortCircuitsStoreAfterPass(t *testing.T) {
	c := NewVersionPassCache()
	store := newCountingForkStore()

	store.data[35] = statsWithUpvotes(27, 19)
	if !c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("v35 at quorum should pass")
	}

	// Sanity: an uncached read of the now-corrupted bitmap is false.
	store.data[35] = statsWithUpvotes(27, 0)
	if PassVersionFromStore(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("uncached sanity: corrupted bitmap must read false")
	}

	readsBefore := store.reads
	if !c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("cache must short-circuit a passed version to true")
	}
	if store.reads != readsBefore {
		t.Errorf("cache must not read the store after pass: reads %d → %d", readsBefore, store.reads)
	}
}

// TestVersionPassCache_ResetRevertsToLiveRead proves Reset is load-bearing for
// reorg safety: a version cached as passed on one branch must be re-evaluated
// from live state after a rewind, which only happens if Reset cleared it.
func TestVersionPassCache_ResetRevertsToLiveRead(t *testing.T) {
	c := NewVersionPassCache()
	store := newCountingForkStore()

	store.data[35] = statsWithUpvotes(27, 19)
	if !c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("v35 should pass and be cached")
	}

	// A reorg rewinds to a branch where v35 never reached quorum.
	c.Reset()
	store.data[35] = statsWithUpvotes(27, 3)
	if c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("after Reset the cache must re-read live state (false on the rewound branch)")
	}
}

// TestVersionPassCache_PendingResultNotCached proves only TRUE is memoized: a
// still-pending bitmap is re-read every call, so the gate opens the moment
// quorum is reached.
func TestVersionPassCache_PendingResultNotCached(t *testing.T) {
	c := NewVersionPassCache()
	store := newCountingForkStore()

	store.data[35] = statsWithUpvotes(27, 3) // below quorum → false
	if c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("below quorum must be pending/false")
	}
	// Re-checking while STILL below quorum must stay false: memoizing a pending
	// result would wrongly latch the gate open before quorum is actually met.
	if c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("a still-pending version must never be memoized as passed")
	}

	// Votes climb to quorum on a later block; the earlier false must not stick.
	store.data[35] = statsWithUpvotes(27, 19)
	if !c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("once quorum is reached Pass must return true (a pending false is never cached)")
	}
}

// TestVersionPassCache_PerVersionIndependent proves the cache keys on version:
// memoizing v27 must not leak a true answer to a still-pending v35.
func TestVersionPassCache_PerVersionIndependent(t *testing.T) {
	c := NewVersionPassCache()
	store := newCountingForkStore()

	store.data[27] = statsWithUpvotes(27, 27) // well past the 80% quorum
	store.data[35] = statsWithUpvotes(27, 2)  // nowhere near the 70% quorum
	if !c.Pass(store, 27, blockTimeAfterFork(27), maintenanceInterval) {
		t.Fatal("v27 should pass")
	}
	if c.Pass(store, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("v35 must stay pending even though v27 is cached (per-version keying)")
	}
	if !c.IsPassed(27) {
		t.Fatal("IsPassed(27) should report the memoized version")
	}
	if c.IsPassed(35) {
		t.Fatal("IsPassed(35) must be false for a pending version")
	}
}
