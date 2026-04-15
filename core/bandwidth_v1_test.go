package core

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// dp builder helper: identical limit/weight across tests, override via setters.
func newDP(totalNetLimit, totalNetWeight int64) *state.DynamicProperties {
	dp := state.NewDynamicProperties()
	dp.Set("total_net_limit", totalNetLimit)
	dp.SetTotalNetWeight(totalNetWeight)
	return dp
}

func newAddr(b byte) tcommon.Address {
	var a tcommon.Address
	a[0] = b
	return a
}

func TestAvailableAccountNet_PureV1_FullShare(t *testing.T) {
	acct := types.NewAccount(newAddr(1), corepb.AccountType_Normal)
	acct.AddFrozenBandwidth(1_000_000, 0) // 1 TRX

	dp := newDP(43_200_000_000, 1)

	got := availableAccountNet(acct, dp)
	if got != 43_200_000_000 {
		t.Errorf("pure V1 full share: got %d, want 43_200_000_000", got)
	}
}

func TestAvailableAccountNet_PureV2_FullShare(t *testing.T) {
	acct := types.NewAccount(newAddr(2), corepb.AccountType_Normal)
	acct.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 1_000_000_000) // 1000 TRX in SUN

	dp := newDP(43_200_000_000, 1000)
	dp.Set("unfreeze_delay_days", 14) // V2 era

	got := availableAccountNet(acct, dp)
	if got != 43_200_000_000 {
		t.Errorf("pure V2 full share: got %d, want 43_200_000_000", got)
	}
}

func TestAvailableAccountNet_Mixed_SumsV1AndV2(t *testing.T) {
	acct := types.NewAccount(newAddr(3), corepb.AccountType_Normal)
	acct.AddFrozenBandwidth(500_000_000, 0)                             // 500 TRX V1
	acct.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500_000_000)        // 500 TRX V2
	acct.SetAcquiredDelegatedFrozenBandwidth(0)                          // irrelevant
	acct.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(0)

	dp := newDP(43_200_000_000, 1000)
	dp.Set("unfreeze_delay_days", 14)

	// total frozen counted = 1e9, netWeight = 1000, share = 1000/1000 = 100% of limit
	got := availableAccountNet(acct, dp)
	if got != 43_200_000_000 {
		t.Errorf("mixed full share: got %d, want 43_200_000_000", got)
	}
}

func TestAvailableAccountNet_ProportionalShare(t *testing.T) {
	acct := types.NewAccount(newAddr(4), corepb.AccountType_Normal)
	acct.AddFrozenBandwidth(1_000_000, 0) // 1 TRX, netWeight = 1

	dp := newDP(43_200_000_000, 100) // account holds 1% of global stake

	got := availableAccountNet(acct, dp)
	if got != 432_000_000 {
		t.Errorf("proportional share: got %d, want 432_000_000", got)
	}
}

func TestAvailableAccountNet_SubTRX_V1RejectsV2Accepts(t *testing.T) {
	// Same sub-TRX frozen: 999_999 SUN ≈ 0.999999 TRX.
	acct := types.NewAccount(newAddr(5), corepb.AccountType_Normal)
	acct.AddFrozenBandwidth(999_999, 0)

	// V1 era (unfreeze_delay_days == 0): frozeBalance < TRX_PRECISION → 0.
	dpV1 := newDP(43_200_000_000, 1)
	if got := availableAccountNet(acct, dpV1); got != 0 {
		t.Errorf("V1 sub-TRX: got %d, want 0", got)
	}

	// V2 era: uses float math; result rounds down but is > 0 only if share math doesn't truncate.
	// With weight=1 (float: 999_999/1e6 ≈ 0.999999), limit=43.2e9 → 43.2e9 * 0.999999 / 1 ≈ 43199956800.
	dpV2 := newDP(43_200_000_000, 1)
	dpV2.Set("unfreeze_delay_days", 14)
	got := availableAccountNet(acct, dpV2)
	if got <= 0 || got >= 43_200_000_000 {
		t.Errorf("V2 sub-TRX: got %d, want 0 < x < 43_200_000_000", got)
	}
}

func TestAvailableAccountNet_ZeroTotalWeightReturnsZero(t *testing.T) {
	acct := types.NewAccount(newAddr(6), corepb.AccountType_Normal)
	acct.AddFrozenBandwidth(1_000_000_000, 0)

	dp := newDP(43_200_000_000, 0)

	if got := availableAccountNet(acct, dp); got != 0 {
		t.Errorf("zero totalWeight: got %d, want 0", got)
	}
}

func TestAvailableAccountNet_AcquiredDelegationCountsForRecipient(t *testing.T) {
	// Delegator has 1000 TRX frozen and delegates it out; acquirer receives it.
	// The acquirer's available bandwidth should equal its share of the global pool
	// based on the ACQUIRED amount, regardless of its own frozen balance.
	acquirer := types.NewAccount(newAddr(7), corepb.AccountType_Normal)
	acquirer.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(1_000_000_000) // 1000 TRX V2 in
	// Note: no own frozen.

	dp := newDP(43_200_000_000, 1000)
	dp.Set("unfreeze_delay_days", 14)

	got := availableAccountNet(acquirer, dp)
	if got != 43_200_000_000 {
		t.Errorf("acquirer full share via acquired delegation: got %d, want 43_200_000_000", got)
	}
}

func TestAvailableAccountNet_DelegatedOutDoesNotCountForDelegator(t *testing.T) {
	// Delegator has delegated everything out; its own available must be 0.
	delegator := types.NewAccount(newAddr(8), corepb.AccountType_Normal)
	delegator.SetDelegatedFrozenBandwidth(1_000_000_000) // V1 out
	delegator.SetDelegatedFrozenV2BalanceForBandwidth(1_000_000_000)       // V2 out
	// No frozen list, no acquired.

	dp := newDP(43_200_000_000, 2000)
	dp.Set("unfreeze_delay_days", 14)

	if got := availableAccountNet(delegator, dp); got != 0 {
		t.Errorf("delegator with everything out: got %d, want 0", got)
	}
}

func TestAvailableAccountNet_NilAccount(t *testing.T) {
	dp := newDP(43_200_000_000, 1000)
	if got := availableAccountNet(nil, dp); got != 0 {
		t.Errorf("nil acct: got %d, want 0", got)
	}
}
