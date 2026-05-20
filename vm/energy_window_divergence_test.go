package vm

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// These tests document a cross-impl consensus divergence (cross-impl audit
// 2026-05-19, backlog item 7): java-tron stores a PER-ACCOUNT energy recovery
// window (AccountResource.energy_window_size / energy_window_optimized) and
// uses it for usage recovery, while go-tron's energy-bill settle path
// (actuator/energy_bill.go::useEnergyForBill -> recoverEnergyUsageForDP and
// core/resource.go::recoverUsage) hardcodes the GLOBAL params.WindowSizeSlots
// and never reads or writes the per-account field.
//
// The smoking gun is that go-tron is internally INCONSISTENT: the staking
// query precompile here (stakingWindowSizeSlots / recoverStakingUsage, which
// backs resourceUsageBalanceAndRestoreSeconds) already ports java-tron's
// per-account window faithfully — it just takes windowSize as a parameter. The
// recovery formula is byte-identical to the settle path's; only the window
// differs. So the same account, at the same slot, recovers to two different
// usages depending on which go-tron code path observes it.

// TestEnergyWindow_PrecompileReadsPerAccountWindow proves the precompile
// honors a non-default stored window. An account whose energy_window_size was
// set by java-tron (or ingested from mainnet) to half the default reads back
// as 14400 slots, not the global 28800.
func TestEnergyWindow_PrecompileReadsPerAccountWindow(t *testing.T) {
	// energy_window_size is stored in V2 units (slots * WINDOW_SIZE_PRECISION).
	// 14_400_000 / 1000 = 14400 slots = half of the 28800-slot default.
	acc := types.NewAccountFromPB(&corepb.Account{
		AccountResource: &corepb.Account_AccountResource{
			EnergyWindowSize:      14_400_000,
			EnergyWindowOptimized: true,
		},
	})

	got := stakingWindowSizeSlots(acc, corepb.ResourceCode_ENERGY)
	if want := int64(14400); got != want {
		t.Fatalf("stakingWindowSizeSlots = %d, want %d (per-account window must be honored)", got, want)
	}
	if int64(params.WindowSizeSlots) != 28800 {
		t.Fatalf("global WindowSizeSlots = %d, want 28800", params.WindowSizeSlots)
	}
}

// TestEnergyWindow_RecoveryDivergesOnWindow proves that the window choice
// materially changes recovered usage. The recovery formula is the same one the
// settle path uses (recoverEnergyUsageWithHarden in actuator/energy_bill.go and
// recoverUsageWithHarden in core/resource.go); the ONLY difference is the
// window. With the same stored usage / elapsed time, the per-account window
// (14400) and the global window (28800) recover to 500_000 vs 750_000 — a
// 250_000-energy gap that easily flips an OUT_OF_ENERGY boundary on the next tx.
func TestEnergyWindow_RecoveryDivergesOnWindow(t *testing.T) {
	const (
		usage    = int64(1_000_000)
		lastTime = int64(0)
		now      = int64(7_200) // 6h elapsed; well within both windows
	)

	cases := []struct {
		name   string
		harden bool
	}{
		{"legacy", false},
		{"hardened", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			perAccount := recoverStakingUsage(usage, lastTime, now, 14_400, tc.harden)
			global := recoverStakingUsage(usage, lastTime, now, int64(params.WindowSizeSlots), tc.harden)

			if want := int64(500_000); perAccount != want {
				t.Fatalf("per-account-window recovery = %d, want %d", perAccount, want)
			}
			if want := int64(750_000); global != want {
				t.Fatalf("global-window recovery = %d, want %d", global, want)
			}
			if perAccount == global {
				t.Fatalf("expected divergence: per-account=%d global=%d", perAccount, global)
			}
		})
	}
}
