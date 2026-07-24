package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestCaptureOwnerResourceSnapshot pins the per-tx fee-payer diagnostic snapshot
// (non-consensus) captured at execution start: balance, free/staked bandwidth
// remaining after recovery, the two net recovery timestamps, and the frozen
// sums feeding the net/energy limits. resourceTime == last-consume-time makes
// recovery an identity (delta=0), so the "left" values are hand-computable as
// limit - raw usage.
func TestCaptureOwnerResourceSnapshot(t *testing.T) {
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	owner := testCoreAddr(1)
	sdb.GetOrCreateAccount(owner)
	sdb.AddBalance(owner, 5_000_000)
	acc := sdb.GetAccount(owner)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 1_000_000) // 1 TRX -> feeds net limit
	acc.AddFreezeV2(corepb.ResourceCode_ENERGY, 2_000_000)    // energy-only, must not leak into net
	acc.SetNetUsage(300)
	acc.SetLatestConsumeTime(100)
	acc.SetFreeNetUsage(200)
	acc.SetLatestConsumeFreeTime(100)

	dp := newDP(1000, 1) // total_net_limit=1000, total_net_weight=1 -> 1 TRX frozen => 1000 net
	dp.Set("free_net_limit", 600)
	dp.SetUnfreezeDelayDays(14)

	snap := captureOwnerResourceSnapshot(sdb, dp, owner, 100)

	if snap.Balance != 5_000_000 {
		t.Errorf("Balance = %d, want 5000000", snap.Balance)
	}
	if snap.FrozenNetLeft != 700 { // 1000 limit - 300 staked usage
		t.Errorf("FrozenNetLeft = %d, want 700", snap.FrozenNetLeft)
	}
	if snap.FreeNetLeft != 400 { // 600 limit - 200 free usage
		t.Errorf("FreeNetLeft = %d, want 400", snap.FreeNetLeft)
	}
	if snap.NetLastConsumeTime != 100 {
		t.Errorf("NetLastConsumeTime = %d, want 100", snap.NetLastConsumeTime)
	}
	if snap.FreeNetLastConsumeTime != 100 {
		t.Errorf("FreeNetLastConsumeTime = %d, want 100", snap.FreeNetLastConsumeTime)
	}
	if snap.FrozenForNet != 1_000_000 {
		t.Errorf("FrozenForNet = %d, want 1000000", snap.FrozenForNet)
	}
	if snap.FrozenForEnergy != 2_000_000 {
		t.Errorf("FrozenForEnergy = %d, want 2000000", snap.FrozenForEnergy)
	}
}

// TestCaptureOwnerResourceSnapshot_OverusedClampsToZero verifies the "left"
// values clamp at zero when recovered usage exceeds the limit (java reports
// available bandwidth clamped, never negative), and that a missing account
// yields an all-zero snapshot rather than panicking.
func TestCaptureOwnerResourceSnapshot_OverusedClampsToZero(t *testing.T) {
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatal(err)
	}
	owner := testCoreAddr(1)
	sdb.GetOrCreateAccount(owner)
	acc := sdb.GetAccount(owner)
	acc.SetNetUsage(5000) // far above the 1000 limit
	acc.SetLatestConsumeTime(100)
	acc.SetFreeNetUsage(5000) // far above the 600 free limit
	acc.SetLatestConsumeFreeTime(100)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 1_000_000)

	dp := newDP(1000, 1)
	dp.Set("free_net_limit", 600)
	dp.SetUnfreezeDelayDays(14)

	snap := captureOwnerResourceSnapshot(sdb, dp, owner, 100)
	if snap.FrozenNetLeft != 0 {
		t.Errorf("FrozenNetLeft = %d, want 0 (clamped)", snap.FrozenNetLeft)
	}
	if snap.FreeNetLeft != 0 {
		t.Errorf("FreeNetLeft = %d, want 0 (clamped)", snap.FreeNetLeft)
	}

	missing := captureOwnerResourceSnapshot(sdb, dp, testCoreAddr(9), 100)
	if missing != (ownerResourceSnapshot{}) {
		t.Errorf("missing account snapshot = %+v, want zero value", missing)
	}
}

func BenchmarkCaptureOwnerResourceSnapshot(b *testing.B) {
	db := state.NewDatabase(ethrawdb.NewMemoryDatabase())
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		b.Fatal(err)
	}
	owner := testCoreAddr(1)
	sdb.GetOrCreateAccount(owner)
	sdb.AddBalance(owner, 5_000_000)
	sdb.FreezeV1Bandwidth(owner, 1_000_000, 200)
	sdb.FreezeV1Energy(owner, 2_000_000, 200)
	sdb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 3_000_000)
	sdb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 4_000_000)
	for tokenID := int64(1_000_000); tokenID < 1_000_256; tokenID++ {
		sdb.SetTRC10Balance(owner, tokenID, tokenID)
	}
	root, err := sdb.Commit()
	if err != nil {
		b.Fatal(err)
	}
	dp := newDP(10_000, 4)
	dp.Set("free_net_limit", 600)
	dp.SetUnfreezeDelayDays(14)

	b.Run("targeted-resource-read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, err := state.New(root, db)
			if err != nil {
				b.Fatal(err)
			}
			_ = captureOwnerResourceSnapshot(view, dp, owner, 100)
		}
	})
	b.Run("legacy-full-account-read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, err := state.New(root, db)
			if err != nil {
				b.Fatal(err)
			}
			acct := view.GetAccount(owner)
			_ = frozenForNet(acct)
			_ = frozenForEnergy(acct)
		}
	})
}
