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
	// 21 of 27 slots vote upgrade — just under 80% (21.6).
	for i := 0; i < 21; i++ {
		fc.Update(35, i, 27)
	}
	now := blockTimeAfterFork(35)
	if fc.Pass(35, now, maintenanceInterval) {
		t.Error("Pass(35) with 21/27 votes (< 22 required) must be false")
	}
}

func TestForkController_Pass_AtThreshold(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// 22 of 27 = ceil(80% * 27) = 22.
	for i := 0; i < 22; i++ {
		fc.Update(35, i, 27)
	}
	now := blockTimeAfterFork(35)
	if !fc.Pass(35, now, maintenanceInterval) {
		t.Error("Pass(35) with 22/27 votes must be true (== ceil(80% * 27))")
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
