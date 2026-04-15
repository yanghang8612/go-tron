package forks

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestFC_IsActive_OffWhenSoftFlagZero(t *testing.T) {
	fc := NewForkController(rawdb.NewMemoryDatabase())
	dp := state.NewDynamicProperties()
	// allow_same_token_name default is 0; soft flag off → IsActive must be false.
	if fc.IsActive(AllowSameTokenName, dp, 1<<50) {
		t.Error("IsActive must be false when soft flag is 0")
	}
}

func TestFC_IsActive_OnWhenSoftFlagSetAndNoVersionGate(t *testing.T) {
	fc := NewForkController(rawdb.NewMemoryDatabase())
	dp := state.NewDynamicProperties()
	dp.SetAllowSameTokenName(true) // no requiredVersion entry
	if !fc.IsActive(AllowSameTokenName, dp, 1<<50) {
		t.Error("IsActive must be true when soft flag is set and no version gate exists")
	}
}

func TestFC_IsActive_VersionGateBlocksEvenIfSoftFlagOn(t *testing.T) {
	fc := NewForkController(rawdb.NewMemoryDatabase())
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(true) // soft flag on
	// AllowAdaptiveEnergy requires VERSION_3_6_5 (9); no votes yet.
	if fc.IsActive(AllowAdaptiveEnergy, dp, 0) {
		t.Error("IsActive must be false when version gate has no votes")
	}
}

func TestFC_IsActive_VersionGatePassesWhenQuorumMet(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	fc := NewForkController(db)
	// Activate VERSION_3_6_5 (legacy: requires 100% of slots).
	for i := 0; i < 27; i++ {
		fc.Update(9, i, 27)
	}
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(true)
	if !fc.IsActive(AllowAdaptiveEnergy, dp, 0) {
		t.Error("IsActive must be true when both soft flag and version quorum pass")
	}
}

func TestFC_IsActive_UnknownFlagReturnsFalse(t *testing.T) {
	fc := NewForkController(rawdb.NewMemoryDatabase())
	dp := state.NewDynamicProperties()
	if fc.IsActive(AllowFlag(9999), dp, 1<<50) {
		t.Error("IsActive for unknown flag must be false")
	}
}

func TestFC_IsActive_NilDPReturnsFalse(t *testing.T) {
	fc := NewForkController(rawdb.NewMemoryDatabase())
	if fc.IsActive(AllowSameTokenName, nil, 1<<50) {
		t.Error("IsActive with nil dp must be false")
	}
}
