package forks

import (
	"testing"

	gerawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// TestPassVersion mirrors the controller_test.go quorum/time-gate cases through
// the stateless reader entry point so callers without a *ForkController get the
// exact same behaviour.

func TestPassVersion_EmptyStatsReturnsFalse(t *testing.T) {
	db := gerawdb.NewMemoryDatabase()
	if PassVersion(db, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("PassVersion on empty stats should be false")
	}
}

func TestPassVersion_UnknownVersionReturnsFalse(t *testing.T) {
	db := gerawdb.NewMemoryDatabase()
	rawdb.WriteForkStats(db, 999, []byte{VoteUpgrade, VoteUpgrade, VoteUpgrade})
	if PassVersion(db, 999, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("unknown version must return false even with full upgrade votes")
	}
}

func TestPassVersion_TimeGateBlocksEarly(t *testing.T) {
	db := gerawdb.NewMemoryDatabase()
	stats := make([]byte, 27)
	for i := range stats {
		stats[i] = VoteUpgrade
	}
	rawdb.WriteForkStats(db, 35, stats)

	// Block time before HardForkTime → false even at 100% upvotes.
	vp, _ := lookupVersion(35)
	if PassVersion(db, 35, vp.HardForkTime-1, maintenanceInterval) {
		t.Fatal("PassVersion must respect HardForkTime ceiling")
	}
}

func TestPassVersion_RateGateRequiresQuorum(t *testing.T) {
	db := gerawdb.NewMemoryDatabase()
	// 27 witnesses, v35 (VERSION_4_8_1_1) requires 70%. 18 upgrade votes is just
	// under ceil(0.70 * 27) = 19 → must return false.
	stats := make([]byte, 27)
	for i := 0; i < 18; i++ {
		stats[i] = VoteUpgrade
	}
	rawdb.WriteForkStats(db, 35, stats)
	if PassVersion(db, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("18/27 upgrade votes must not pass v35 (70% quorum)")
	}
}

func TestPassVersion_AtThreshold(t *testing.T) {
	db := gerawdb.NewMemoryDatabase()
	// 19/27 = 70.4%, just clears the 70% ceil (ceil(0.70*27)=19) → should pass.
	stats := make([]byte, 27)
	for i := 0; i < 19; i++ {
		stats[i] = VoteUpgrade
	}
	rawdb.WriteForkStats(db, 35, stats)
	if !PassVersion(db, 35, blockTimeAfterFork(35), maintenanceInterval) {
		t.Fatal("19/27 upgrade votes must pass v35 (70% quorum)")
	}
}

func TestPassVersion_V4_8_0_1_LowerRate(t *testing.T) {
	// VERSION_4_8_0_1 (v33) requires 70%; 19/27 = 70.4% should pass.
	db := gerawdb.NewMemoryDatabase()
	stats := make([]byte, 27)
	for i := 0; i < 19; i++ {
		stats[i] = VoteUpgrade
	}
	rawdb.WriteForkStats(db, 33, stats)
	if !PassVersion(db, 33, blockTimeAfterFork(33), maintenanceInterval) {
		t.Fatal("19/27 upgrade votes must pass v33 (70% quorum)")
	}
}

func TestPassVersion_LegacyVersionStrictAllSlots(t *testing.T) {
	// Version <= Version4_0 (16) requires every slot voting upgrade.
	db := gerawdb.NewMemoryDatabase()
	stats := make([]byte, 5)
	for i := range stats {
		stats[i] = VoteUpgrade
	}
	rawdb.WriteForkStats(db, 16, stats)
	if !PassVersion(db, 16, 0, maintenanceInterval) {
		t.Fatal("legacy v16 with all slots upgraded must pass")
	}
	// Flip one slot → must fail strictly.
	stats[2] = VoteDowngrade
	rawdb.WriteForkStats(db, 16, stats)
	if PassVersion(db, 16, 0, maintenanceInterval) {
		t.Fatal("legacy v16 must require ALL slots upgraded")
	}
}

func TestPassVersion_AgreesWithForkController(t *testing.T) {
	// Sanity: PassVersion and ForkController.Pass over the same db must
	// answer identically across a representative parameter sweep.
	db := gerawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	cases := []struct {
		version    int32
		upgraded   int
		witnessNum int
		blockTime  int64
	}{
		{35, 22, 27, blockTimeAfterFork(35)},
		{35, 21, 27, blockTimeAfterFork(35)},
		{33, 19, 27, blockTimeAfterFork(33)},
		{34, 22, 27, blockTimeAfterFork(34)},
		{34, 22, 27, 0},
	}
	for _, tc := range cases {
		stats := make([]byte, tc.witnessNum)
		for i := 0; i < tc.upgraded; i++ {
			stats[i] = VoteUpgrade
		}
		rawdb.WriteForkStats(db, tc.version, stats)
		got := PassVersion(db, tc.version, tc.blockTime, maintenanceInterval)
		want := fc.Pass(tc.version, tc.blockTime, maintenanceInterval)
		if got != want {
			t.Errorf("v=%d up=%d/%d t=%d: PassVersion=%v ForkController.Pass=%v",
				tc.version, tc.upgraded, tc.witnessNum, tc.blockTime, got, want)
		}
	}
}
