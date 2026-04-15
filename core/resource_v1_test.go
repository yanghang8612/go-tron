package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// newEnergyDP builds a DP with only energy-side fields set — keeps tests
// independent from the bandwidth helpers.
func newEnergyDP(totalEnergyLimit, totalEnergyWeight int64) *state.DynamicProperties {
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyLimit(totalEnergyLimit)
	dp.SetTotalEnergyWeight(totalEnergyWeight)
	return dp
}

func TestAvailableAccountEnergy_PureV1_FullShare(t *testing.T) {
	acct := types.NewAccount(newAddr(1), corepb.AccountType_Normal)
	acct.AddFrozenEnergy(1_000_000, 0) // 1 TRX in SUN

	dp := newEnergyDP(90_000_000_000, 1)

	got := availableAccountEnergy(acct, dp)
	if got != 90_000_000_000 {
		t.Errorf("pure V1 full share: got %d, want 90_000_000_000", got)
	}
}

func TestAvailableAccountEnergy_PureV2_FullShare(t *testing.T) {
	acct := types.NewAccount(newAddr(2), corepb.AccountType_Normal)
	acct.AddFreezeV2(corepb.ResourceCode_ENERGY, 1_000_000_000) // 1000 TRX

	dp := newEnergyDP(90_000_000_000, 1000)
	dp.Set("unfreeze_delay_days", 14)

	got := availableAccountEnergy(acct, dp)
	if got != 90_000_000_000 {
		t.Errorf("pure V2 full share: got %d, want 90_000_000_000", got)
	}
}

func TestAvailableAccountEnergy_Mixed(t *testing.T) {
	acct := types.NewAccount(newAddr(3), corepb.AccountType_Normal)
	acct.AddFrozenEnergy(500_000_000, 0)
	acct.AddFreezeV2(corepb.ResourceCode_ENERGY, 500_000_000)

	dp := newEnergyDP(90_000_000_000, 1000)
	dp.Set("unfreeze_delay_days", 14)

	got := availableAccountEnergy(acct, dp)
	if got != 90_000_000_000 {
		t.Errorf("mixed full share: got %d, want 90_000_000_000", got)
	}
}

func TestAvailableAccountEnergy_SubTRX_V1RejectsV2Accepts(t *testing.T) {
	acct := types.NewAccount(newAddr(4), corepb.AccountType_Normal)
	acct.AddFrozenEnergy(999_999, 0)

	dpV1 := newEnergyDP(90_000_000_000, 1)
	if got := availableAccountEnergy(acct, dpV1); got != 0 {
		t.Errorf("V1 sub-TRX: got %d, want 0", got)
	}

	dpV2 := newEnergyDP(90_000_000_000, 1)
	dpV2.Set("unfreeze_delay_days", 14)
	if got := availableAccountEnergy(acct, dpV2); got <= 0 || got >= 90_000_000_000 {
		t.Errorf("V2 sub-TRX: got %d, want 0 < x < 90_000_000_000", got)
	}
}

func TestAvailableAccountEnergy_AcquiredDelegation(t *testing.T) {
	// Acquirer receives V1+V2 delegated energy, holds no own freeze.
	acct := types.NewAccount(newAddr(5), corepb.AccountType_Normal)
	acct.SetAcquiredDelegatedFrozenEnergy(500_000_000)
	acct.SetAcquiredDelegatedFrozenV2BalanceForEnergy(500_000_000)

	dp := newEnergyDP(90_000_000_000, 1000)
	dp.Set("unfreeze_delay_days", 14)

	got := availableAccountEnergy(acct, dp)
	if got != 90_000_000_000 {
		t.Errorf("acquired delegation full share: got %d, want 90_000_000_000", got)
	}
}

func TestAvailableAccountEnergy_ZeroWeight(t *testing.T) {
	acct := types.NewAccount(newAddr(6), corepb.AccountType_Normal)
	acct.AddFrozenEnergy(1_000_000_000, 0)

	dp := newEnergyDP(90_000_000_000, 0)
	if got := availableAccountEnergy(acct, dp); got != 0 {
		t.Errorf("zero totalEnergyWeight: got %d, want 0", got)
	}
}

func TestAvailableAccountEnergy_NilAccount(t *testing.T) {
	dp := newEnergyDP(90_000_000_000, 1000)
	if got := availableAccountEnergy(nil, dp); got != 0 {
		t.Errorf("nil acct: got %d, want 0", got)
	}
}
