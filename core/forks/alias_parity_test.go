package forks_test

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// TestAliasFlags_FlipTogether locks in the M1.3 Task 5 decision that
// AllowStakingV2 and AllowNewResourceModel share the proposal-#62 key,
// and AllowTvmShieldedToken shares the proposal-#39 key. If a future
// refactor breaks the aliasing, this test fires.
func TestAliasFlags_FlipTogether(t *testing.T) {
	cases := []struct {
		name  string
		setup func(dp *state.DynamicProperties)
		check func(dp *state.DynamicProperties) bool
	}{
		{
			name:  "proposal-62 activates both AllowStakingV2 and AllowNewResourceModel",
			setup: func(dp *state.DynamicProperties) { dp.Set("allow_new_resource_model", 1) },
			check: func(dp *state.DynamicProperties) bool {
				return dp.AllowStakingV2() && dp.AllowNewResourceModel()
			},
		},
		{
			name:  "proposal-39 activates AllowTvmShieldedToken via ShieldedTrc20",
			setup: func(dp *state.DynamicProperties) { dp.Set("allow_shielded_trc20_transaction", 1) },
			check: func(dp *state.DynamicProperties) bool {
				return dp.AllowTvmShieldedToken() && dp.AllowShieldedTrc20Transaction()
			},
		},
	}
	for _, c := range cases {
		dp := state.NewDynamicProperties()
		c.setup(dp)
		if !c.check(dp) {
			t.Errorf("%s: alias + canonical getters diverged", c.name)
		}
	}
}

// TestAllowStakingV2_ProposalRoute verifies that writing the proposal-51
// DP key (ALLOW_NEW_RESOURCE_MODEL) also flips forks.IsActive on both
// aliased AllowFlag values.
func TestAllowStakingV2_ProposalRoute(t *testing.T) {
	dp := state.NewDynamicProperties()
	key := forks.ProposalParamKey(51)
	if key == "" {
		t.Fatal("proposal 51 must map to a DP key")
	}
	dp.Set(key, 1)
	if !forks.IsActive(forks.AllowStakingV2, 0, dp) {
		t.Error("AllowStakingV2 must read active after proposal-51 DP key set")
	}
	if !forks.IsActive(forks.AllowNewResourceModel, 0, dp) {
		t.Error("AllowNewResourceModel must read active after proposal-51 DP key set")
	}
}
