package core

import (
	"math"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newTestResourceProcessor(t *testing.T) (*ResourceProcessor, *state.StateDB) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	return NewResourceProcessor(sdb), sdb
}

func TestRecoverBandwidth(t *testing.T) {
	rp, sdb := newTestResourceProcessor(t)
	addr := testCoreAddr(1)
	sdb.GetOrCreateAccount(addr)
	acc := sdb.GetAccount(addr)
	acc.SetNetUsage(500)
	acc.SetLatestConsumeTime(0)

	halfWindow := int64(params.WindowSizeSlots / 2)
	rp.RecoverBandwidth(addr, halfWindow)
	acc = sdb.GetAccount(addr)
	if acc.NetUsage() != 250 {
		t.Fatalf("net usage after half window: want 250, got %d", acc.NetUsage())
	}

	rp.RecoverBandwidth(addr, int64(params.WindowSizeSlots))
	acc = sdb.GetAccount(addr)
	if acc.NetUsage() != 0 {
		t.Fatalf("net usage after full window: want 0, got %d", acc.NetUsage())
	}
}

func TestRecoverEnergy(t *testing.T) {
	rp, sdb := newTestResourceProcessor(t)
	addr := testCoreAddr(1)
	sdb.GetOrCreateAccount(addr)
	acc := sdb.GetAccount(addr)
	acc.SetEnergyUsage(1000)
	acc.SetLatestConsumeTimeForEnergy(0)

	rp.RecoverEnergy(addr, int64(params.WindowSizeSlots))
	acc = sdb.GetAccount(addr)
	if acc.EnergyUsage() != 0 {
		t.Fatalf("energy usage after full window: want 0, got %d", acc.EnergyUsage())
	}
}

func TestRecoverBandwidthPartialWindow(t *testing.T) {
	rp, sdb := newTestResourceProcessor(t)
	addr := testCoreAddr(1)
	sdb.GetOrCreateAccount(addr)
	acc := sdb.GetAccount(addr)
	acc.SetFreeNetUsage(600)
	acc.SetLatestConsumeFreeTime(0)

	rp.RecoverFreeBandwidth(addr, int64(params.WindowSizeSlots*3/4))
	acc = sdb.GetAccount(addr)
	if acc.FreeNetUsage() != 150 {
		t.Fatalf("free net usage at 75%% window: want 150, got %d", acc.FreeNetUsage())
	}
}

func TestRecoverUsageHardenedAvoidsOverflow(t *testing.T) {
	got := recoverUsageWithHarden(math.MaxInt64, 0, int64(params.WindowSizeSlots/2), true)
	if got <= 0 {
		t.Fatalf("hardened recovered usage should stay positive, got %d", got)
	}
}

// TestRecoverUsage_PreStake2PrecisionAveraging pins core bandwidth/NET recovery
// against java-tron ResourceProcessor.increase (the global 28800-slot window,
// precision-averaging: divideCeil*1e6 + round(decay) + getUsage), the recover
// arm of BandwidthProcessor.useAccountNet's pre-Stake-2.0 path. go-tron's old
// harden=false branch used a plain truncate oldUsage*(W-delta)/W that drifted ~1
// unit per recovered block — the NET twin of the energy fork at Nile 8,825,873
// (commit 6cfc163). A 299-byte net_usage (a real Nile VoteWitnessContract size)
// recovered one slot later must stay 299 (java); the truncate gave 298, so the
// stored usage decays below java's and the account looks like it has more free
// bandwidth than it does — eventually flipping a free-vs-burn-TRX boundary.
//
// Oracle values are from a python model of java's formula, cross-checked against
// the java-verified energy golden in actuator (852_710_572 @ delta=1 ->
// 852_680_964), which this same formula reproduces byte-for-byte.
func TestRecoverUsage_PreStake2PrecisionAveraging(t *testing.T) {
	const W = int64(params.WindowSizeSlots) // 28800

	// The divergence: 1-slot recovery must not lose a unit. Both arithmetic
	// modes (int64 legacy / big.Int hardened) match java for non-overflow inputs.
	for _, harden := range []bool{false, true} {
		if got := recoverUsageWithHarden(299, 0, 1, harden); got != 299 {
			t.Fatalf("recover(299, delta=1, harden=%v) = %d, want 299 (java increase); old truncate gave 298", harden, got)
		}
		if got := recoverUsageWithHarden(268, 0, 1, harden); got != 268 {
			t.Fatalf("recover(268, delta=1, harden=%v) = %d, want 268 (java); old truncate gave 267", harden, got)
		}
	}

	// Compounding: repeated 1-slot recovery from 299 stays pinned at 299 under
	// java's formula (the average is preserved); the old truncate bled to 294.
	u := int64(299)
	for i := 0; i < 5; i++ {
		u = recoverUsageWithHarden(u, 0, 1, false)
	}
	if u != 299 {
		t.Fatalf("299 recovered 5x at 1 slot = %d, want 299 (no drift); old truncate bled to 294", u)
	}

	// Bind to java byte-for-byte via the cross-checked energy golden value.
	if got := recoverUsageWithHarden(852_710_572, 0, 1, false); got != 852_680_964 {
		t.Fatalf("recover(852710572, delta=1) = %d, want 852680964 (java); old truncate gave 852680963", got)
	}

	// Boundaries: no decay at lastTime==now; full decay once delta>=window.
	if got := recoverUsageWithHarden(299, 5, 5, false); got != 299 {
		t.Fatalf("recover(delta=0) = %d, want 299 (no decay at lastTime==now)", got)
	}
	if got := recoverUsageWithHarden(299, 0, W, false); got != 0 {
		t.Fatalf("recover(delta>=window) = %d, want 0 (fully decayed)", got)
	}
}

func TestCalculateGlobalResourceLimitV2HardenedAvoidsOverflow(t *testing.T) {
	acct := types.NewAccount(testCoreAddr(2), corepb.AccountType_Normal)
	acct.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, math.MaxInt64)
	dp := newDP(1_000_000_000, math.MaxInt64/1_000_000)
	dp.Set("unfreeze_delay_days", 14)
	dp.SetAllowHardenResourceCalculation(true)

	got := availableAccountNet(acct, dp)
	if got != 1_000_000_000 {
		t.Fatalf("hardened V2 resource limit: got %d, want 1000000000", got)
	}
}

func testCoreAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}
