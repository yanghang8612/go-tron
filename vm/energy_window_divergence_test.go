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
// per-account window faithfully — it just takes windowSize as a parameter.
// recoverStakingUsage's recovery arithmetic now matches the settle path's for
// BOTH branches (harden -> core.increaseHardened, non-harden -> core.increase);
// only the window differs. (The non-harden branch originally diverged with a
// plain `oldUsage * remaining / windowSize` truncate — that separate consensus
// bug is pinned by TestRecoverStakingUsage_NonHardenMatchesSettlePath below.)
// So the same account, at the same slot, recovers to two different usages
// depending only on which window the observing go-tron code path uses.

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

	perAccount := recoverStakingUsage(usage, lastTime, now, 14_400)
	global := recoverStakingUsage(usage, lastTime, now, int64(params.WindowSizeSlots))

	if want := int64(500_000); perAccount != want {
		t.Fatalf("per-account-window recovery = %d, want %d", perAccount, want)
	}
	if want := int64(750_000); global != want {
		t.Fatalf("global-window recovery = %d, want %d", global, want)
	}
	if perAccount == global {
		t.Fatalf("expected divergence: per-account=%d global=%d", perAccount, global)
	}
}

// TestRecoverStakingUsage_MatchesSettlePath pins recoverStakingUsage to java-tron's
// precision-averaging recovery (RepositoryImpl.increase / getUsage, precision=
// 1_000_000), which go-tron's settle path already ports as core.increase (see
// core/energy_adaptive.go and core/resource.go). This VM-getter recovery is now
// UNCONDITIONALLY primitive-long (no harden/BigInteger), matching RepositoryImpl
// which — unlike chainbase ResourceProcessor.increase — has no harden gate at all.
// A plain `oldUsage * remaining / windowSize` truncate would drift ~1 unit per
// recovered block (a free-vs-burn bandwidth fork on every resourceUsage /
// checkUnDelegateResource / delegatableResource precompile call).
func TestRecoverStakingUsage_MatchesSettlePath(t *testing.T) {
	cases := []struct {
		name       string
		oldUsage   int64
		lastTime   int64
		now        int64
		windowSize int64
		want       int64
	}{
		// Default mainnet window: plain truncate gave 12; java/settle give 13.
		{"mainnet-window-drift", 13, 0, 1, 28800, 13},
		// Tiny window: plain truncate gave 7; java/settle give 6.
		{"small-window-drift", 21, 0, 2, 3, 6},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := recoverStakingUsage(tc.oldUsage, tc.lastTime, tc.now, tc.windowSize)
			if got != tc.want {
				t.Fatalf("recoverStakingUsage(%d, %d, %d, %d) = %d, want %d (precision-averaging, not plain truncate)",
					tc.oldUsage, tc.lastTime, tc.now, tc.windowSize, got, tc.want)
			}
		})
	}
}
