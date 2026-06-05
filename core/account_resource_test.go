package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// stakedResourceAccount builds an account with non-zero values across every
// resource dimension the getaccountresource view reports.
func stakedResourceAccount() *types.Account {
	return types.NewAccountFromPB(&corepb.Account{
		FreeNetUsage: 111,
		NetUsage:     222,
		Votes: []*corepb.Vote{
			{VoteCount: 30},
			{VoteCount: 70},
		},
		FrozenV2: []*corepb.Account_FreezeV2{
			{Type: corepb.ResourceCode_BANDWIDTH, Amount: 100_000_000},  // 100 TRX
			{Type: corepb.ResourceCode_ENERGY, Amount: 200_000_000},     // 200 TRX
			{Type: corepb.ResourceCode_TRON_POWER, Amount: 500_000_000}, // 500 TRX
		},
		AccountResource: &corepb.Account_AccountResource{
			EnergyUsage:  333,
			StorageLimit: 4444,
			StorageUsage: 555,
		},
	})
}

// resourceViewDP seeds the dynamic-property keys the resource view consumes.
func resourceViewDP() *state.DynamicProperties {
	dp := state.NewDynamicProperties()
	dp.Set("free_net_limit", 600)
	dp.Set("total_net_limit", 43_200_000_000)
	dp.Set("total_net_weight", 1_000)
	dp.Set("total_energy_current_limit", 90_000_000_000)
	dp.Set("total_energy_weight", 2_000)
	dp.Set("total_tron_power_weight", 3_000)
	return dp
}

func TestAccountResourceFromAccount_PopulatesAllResourceFields(t *testing.T) {
	acc := stakedResourceAccount()
	dp := resourceViewDP()

	res := accountResourceFromAccount(acc, dp)

	checks := []struct {
		name string
		got  int64
		want int64
	}{
		// Already populated before the fix.
		{"FreeNetUsed", res.FreeNetUsed, 111},
		{"NetUsed", res.NetUsed, 222},
		{"FreeNetLimit", res.FreeNetLimit, 600},
		{"TotalNetLimit", res.TotalNetLimit, 43_200_000_000},
		{"TotalEnergyLimit", res.TotalEnergyLimit, 90_000_000_000},
		// Left empty (always 0) by the bug — the core of this fix.
		{"EnergyUsed", res.EnergyUsed, 333},
		{"TotalNetWeight", res.TotalNetWeight, 1_000},
		{"TotalEnergyWeight", res.TotalEnergyWeight, 2_000},
		{"TotalTronPowerWeight", res.TotalTronPowerWeight, 3_000},
		{"TronPowerUsed", res.TronPowerUsed, 100}, // 30+70 votes
		{"TronPowerLimit", res.TronPowerLimit, acc.AllTronPower() / trxPrecision},
		{"StorageLimit", res.StorageLimit, 4444},
		{"StorageUsed", res.StorageUsed, 555},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}

	// NetLimit / EnergyLimit are this account's share of the global pools; they
	// must be wired to the same helpers consensus uses. Guard non-zero so a
	// missing wire (field defaulting to 0) fails instead of silently matching.
	wantNet := availableAccountNet(acc, dp)
	wantEnergy := availableAccountEnergy(acc, dp)
	if wantNet == 0 || wantEnergy == 0 {
		t.Fatalf("setup bug: helper limits must be non-zero (net=%d energy=%d)", wantNet, wantEnergy)
	}
	if res.NetLimit != wantNet {
		t.Errorf("NetLimit = %d, want %d", res.NetLimit, wantNet)
	}
	if res.EnergyLimit != wantEnergy {
		t.Errorf("EnergyLimit = %d, want %d", res.EnergyLimit, wantEnergy)
	}
}

// TestAccountResourceDynamicPropertyKeysCoverView guards the archive path: it
// rebuilds dynamic properties from only accountResourceDynamicPropertyKeys (the
// keys dynamicPropertiesAt reconstructs from history). If that list ever drops a
// key the view consumes, the corresponding limit/weight silently reads 0 on
// archive (block-bound) reads while the live path keeps working.
func TestAccountResourceDynamicPropertyKeysCoverView(t *testing.T) {
	dp := state.NewDynamicProperties()
	for i, key := range accountResourceDynamicPropertyKeys {
		dp.Set(key, int64(i+1)*1_000_000) // distinct, non-zero, >= 1 TRX
	}
	res := accountResourceFromAccount(stakedResourceAccount(), dp)

	derived := map[string]int64{
		"FreeNetLimit":         res.FreeNetLimit,
		"TotalNetLimit":        res.TotalNetLimit,
		"TotalNetWeight":       res.TotalNetWeight,
		"TotalTronPowerWeight": res.TotalTronPowerWeight,
		"TotalEnergyLimit":     res.TotalEnergyLimit,
		"TotalEnergyWeight":    res.TotalEnergyWeight,
		"NetLimit":             res.NetLimit,
		"EnergyLimit":          res.EnergyLimit,
	}
	for name, v := range derived {
		if v == 0 {
			t.Errorf("%s = 0: accountResourceDynamicPropertyKeys is missing a key the view needs", name)
		}
	}
}
