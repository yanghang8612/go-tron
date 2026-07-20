package forks

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

const maintenanceInterval = 21_600_000 // 6h, mainnet genesis default

// blockTimeAfterFork returns a timestamp comfortably past the ceiling-aligned
// hardForkTime for the given version — used when a test needs to exercise the
// quorum path without the time-gate getting in the way.
func blockTimeAfterFork(version int32) int64 {
	vp, _ := lookupVersion(version)
	if vp.HardForkTime == 0 {
		return 0
	}
	aligned := ((vp.HardForkTime-1)/maintenanceInterval + 1) * maintenanceInterval
	return aligned + maintenanceInterval
}

func TestForkController_UpdateSetsSlotBit(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)

	fc.Update(35, 0, 27)

	stats := fc.Stats(35)
	if len(stats) != 27 {
		t.Fatalf("stats len: got %d, want 27", len(stats))
	}
	if stats[0] != VoteUpgrade {
		t.Errorf("slot 0: got %d, want VoteUpgrade", stats[0])
	}
	for i := 1; i < 27; i++ {
		if stats[i] != 0 {
			t.Errorf("slot %d should be 0, got %d", i, stats[i])
		}
	}
}

func TestForkController_UpdateMarksLowerVersionsUpgradeHigherDowngrade(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)

	// SR produces block claiming version 32 (VERSION_4_8_0).
	fc.Update(32, 5, 27)

	// Expect: versions 6..32 slot 5 = upgrade; 33,34,35 slot 5 = downgrade.
	for _, vp := range KnownVersions {
		got := fc.Stats(vp.Value)[5]
		if vp.Value <= 32 {
			if got != VoteUpgrade {
				t.Errorf("v%d slot 5: got %d, want upgrade", vp.Value, got)
			}
		} else {
			if got != VoteDowngrade {
				t.Errorf("v%d slot 5: got %d, want downgrade", vp.Value, got)
			}
		}
	}
}

func TestForkController_Pass_TimeGateBlocksEarly(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// Fill all 27 slots with upgrade votes for v35.
	for i := 0; i < 27; i++ {
		fc.Update(35, i, 27)
	}
	earlyBlockTime := int64(1000)
	if fc.Pass(35, earlyBlockTime, maintenanceInterval) {
		t.Error("Pass(35) must be false before hardForkTime")
	}
}

func TestForkController_Pass_RateGateRequiresQuorum(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// v35 (VERSION_4_8_1_1) requires 70%: ceil(0.70*27)=19. 18 is just under.
	for i := 0; i < 18; i++ {
		fc.Update(35, i, 27)
	}
	now := blockTimeAfterFork(35)
	if fc.Pass(35, now, maintenanceInterval) {
		t.Error("Pass(35) with 18/27 votes (< 19 required) must be false")
	}
}

func TestForkController_Pass_AtThreshold(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// 19 of 27 = ceil(70% * 27) = 19 (v35 VERSION_4_8_1_1 rate).
	for i := 0; i < 19; i++ {
		fc.Update(35, i, 27)
	}
	now := blockTimeAfterFork(35)
	if !fc.Pass(35, now, maintenanceInterval) {
		t.Error("Pass(35) with 19/27 votes must be true (== ceil(70% * 27))")
	}
}

func TestForkController_Pass_UnknownVersionReturnsFalse(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	if fc.Pass(9999, 1<<50, maintenanceInterval) {
		t.Error("Pass for unknown version must be false")
	}
}

func TestForkController_Pass_EmptyStatsReturnsFalse(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	if fc.Pass(35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Error("Pass with no updates must be false")
	}
}

func TestForkController_Pass_LegacyVersionRequiresAllSlots(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// 26 of 27 upgrade — legacy (v6..16) requires strict 27/27.
	for i := 0; i < 26; i++ {
		fc.Update(6, i, 27)
	}
	if fc.Pass(6, 0, maintenanceInterval) {
		t.Error("legacy v6 with 26/27 must NOT pass")
	}
	fc.Update(6, 26, 27)
	if !fc.Pass(6, 0, maintenanceInterval) {
		t.Error("legacy v6 with 27/27 must pass")
	}
}

func TestForkController_Reset_ClearsUnactivatedVersions(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// Activate v32 (22 upgrade votes, time past).
	for i := 0; i < 22; i++ {
		fc.Update(32, i, 27)
	}
	now := blockTimeAfterFork(32)
	if !fc.Pass(32, now, maintenanceInterval) {
		t.Fatal("v32 must be active pre-reset")
	}

	// Also record some votes for v35 which shouldn't have passed.
	for i := 0; i < 5; i++ {
		fc.Update(35, i, 27)
	}

	fc.Reset(now, maintenanceInterval, 27)

	// v32 stays (activated).
	v32Stats := fc.Stats(32)
	upvotes := 0
	for _, b := range v32Stats {
		if b == VoteUpgrade {
			upvotes++
		}
	}
	if upvotes < 22 {
		t.Errorf("v32 upvotes after reset: got %d, want >= 22", upvotes)
	}

	// v35 got zeroed.
	v35Stats := fc.Stats(35)
	for i, b := range v35Stats {
		if b != 0 {
			t.Errorf("v35 slot %d after reset: got %d, want 0", i, b)
		}
	}
}

func TestForkController_Update_IgnoresOutOfRangeSlot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	fc.Update(35, 27, 27) // slot == witnessCount is out of bounds
	if fc.Stats(35) != nil {
		t.Errorf("out-of-range slot must not write stats, got %v", fc.Stats(35))
	}
	fc.Update(35, -1, 27)
	if fc.Stats(35) != nil {
		t.Errorf("negative slot must not write stats, got %v", fc.Stats(35))
	}
}

func TestForkController_Update_RebuildsOnWitnessCountChange(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	fc.Update(35, 3, 5) // small committee
	if len(fc.Stats(35)) != 5 {
		t.Fatalf("expected len 5, got %d", len(fc.Stats(35)))
	}
	fc.Update(35, 3, 27) // committee grew — bitmap resized
	stats := fc.Stats(35)
	if len(stats) != 27 {
		t.Fatalf("expected len 27 after rebuild, got %d", len(stats))
	}
	if stats[3] != VoteUpgrade {
		t.Errorf("slot 3 after rebuild: got %d, want upgrade", stats[3])
	}
}

func TestForkController_UpdateJavaTouchesCurrentAndExistingFutureOnly(t *testing.T) {
	store := newCountingForkStore()
	fc := NewForkControllerFromStore(store)
	future := make([]byte, 27)
	future[3] = VoteUpgrade
	store.WriteForkStats(36, future)
	store.writes = 0

	latest, advanced := fc.UpdateJava(35, 3, 27, 0, blockTimeAfterFork(35), maintenanceInterval)
	if advanced || latest != 0 {
		t.Fatalf("latest=%d advanced=%t, want 0 false", latest, advanced)
	}
	if got := fc.Stats(35); len(got) != 27 || got[3] != VoteUpgrade {
		t.Fatalf("current stats = %v", got)
	}
	if got := fc.Stats(36); got[3] != VoteDowngrade {
		t.Fatalf("future slot was not downgraded: %v", got)
	}
	if got := fc.Stats(34); got != nil {
		t.Fatalf("lower version was materialized before current passed: %v", got)
	}
}

func TestForkController_UpdateJavaAdvancesVersionOnBlockAfterQuorum(t *testing.T) {
	store := newCountingForkStore()
	fc := NewForkControllerFromStore(store)
	stats := make([]byte, 27)
	for i := 0; i < 19; i++ { // v35 threshold is ceil(70% * 27)
		stats[i] = VoteUpgrade
	}
	store.WriteForkStats(35, stats)

	latest, advanced := fc.UpdateJava(35, 19, 27, 0, blockTimeAfterFork(35), maintenanceInterval)
	if !advanced || latest != 35 {
		t.Fatalf("latest=%d advanced=%t, want 35 true", latest, advanced)
	}
	lower := fc.Stats(34)
	if len(lower) != 27 {
		t.Fatalf("lower stats len=%d, want 27", len(lower))
	}
	for i, vote := range lower {
		if vote != VoteUpgrade {
			t.Fatalf("lower slot %d=%d, want upgrade", i, vote)
		}
	}
	// The activation block does not add its own slot; java returns immediately.
	if got := fc.Stats(35); got[19] != VoteDowngrade {
		t.Fatalf("activation block unexpectedly recorded current slot: %v", got)
	}
}

func TestForkController_UpdateJavaSkipsAtOrBelowLatestVersion(t *testing.T) {
	store := newCountingForkStore()
	fc := NewForkControllerFromStore(store)
	latest, advanced := fc.UpdateJava(35, 0, 27, 35, blockTimeAfterFork(35), maintenanceInterval)
	if advanced || latest != 35 || store.writes != 0 {
		t.Fatalf("latest=%d advanced=%t writes=%d", latest, advanced, store.writes)
	}
}

type countingForkStore struct {
	data       map[int32][]byte
	reads      int
	batchReads int
	writes     int
}

func newCountingForkStore() *countingForkStore {
	return &countingForkStore{data: make(map[int32][]byte)}
}

func (s *countingForkStore) ReadForkStats(version int32) []byte {
	s.reads++
	if v := s.data[version]; v != nil {
		return append([]byte(nil), v...)
	}
	return nil
}

func (s *countingForkStore) ReadForkStatsBatch(versions []int32) map[int32][]byte {
	s.batchReads++
	out := make(map[int32][]byte, len(versions))
	for _, version := range versions {
		if v := s.data[version]; v != nil {
			out[version] = append([]byte(nil), v...)
		}
	}
	return out
}

func (s *countingForkStore) WriteForkStats(version int32, stats []byte) {
	s.writes++
	s.data[version] = append([]byte(nil), stats...)
}

func TestForkController_UpdateBatchesReadsAndSkipsNoopWrites(t *testing.T) {
	store := newCountingForkStore()
	fc := NewForkControllerFromStore(store)

	fc.Update(35, 0, 27)
	if store.batchReads != 1 {
		t.Fatalf("first update batch reads = %d, want 1", store.batchReads)
	}
	if store.writes != len(KnownVersions) {
		t.Fatalf("first update writes = %d, want %d", store.writes, len(KnownVersions))
	}

	store.writes = 0
	fc.Update(35, 0, 27)
	if store.batchReads != 2 {
		t.Fatalf("second update batch reads = %d, want 2", store.batchReads)
	}
	if store.writes != 0 {
		t.Fatalf("repeat update writes = %d, want 0", store.writes)
	}
}

// TestForkController_Reset_BoundaryTimestampSensitivity pins the fix for the
// reset-timestamp source: java Manager calls forkController.reset() BEFORE
// updateDynamicProperties, so reset's pass() reads the PREVIOUS block's
// timestamp. At the maintenance boundary that first crosses a version's aligned
// hard-fork time, a rate-met bitmap must be CLEARED (pass(prevTs<aligned)=false),
// not KEPT — otherwise gtron reports pass(version)=true ~1 cycle before java.
func TestForkController_Reset_BoundaryTimestampSensitivity(t *testing.T) {
	vp, _ := lookupVersion(35)
	aligned := ((vp.HardForkTime-1)/maintenanceInterval + 1) * maintenanceInterval
	const required = 19 // ceil(70% * 27) for VERSION_4_8_1_1

	// prevTs (aligned-1): java's reset timestamp at the crossing boundary → CLEAR.
	dbPrev := rawdb.NewMemoryDatabase()
	fcPrev := NewForkController(dbPrev)
	for i := 0; i < required; i++ {
		fcPrev.Update(35, i, 27)
	}
	fcPrev.Reset(aligned-1, maintenanceInterval, 27)
	if fcPrev.Pass(35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("Reset at prev-block ts (< aligned) must CLEAR the rate-met bitmap (java parity)")
	}

	// currentTs (aligned): the old gtron behavior kept the bitmap.
	dbCur := rawdb.NewMemoryDatabase()
	fcCur := NewForkController(dbCur)
	for i := 0; i < required; i++ {
		fcCur.Update(35, i, 27)
	}
	fcCur.Reset(aligned, maintenanceInterval, 27)
	if !fcCur.Pass(35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("sanity: Reset at >= aligned with rate met keeps the bitmap")
	}
}
