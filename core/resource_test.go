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
