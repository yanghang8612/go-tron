package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
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

	halfWindow := int64(params.WindowSizeMs / 2)
	rp.RecoverBandwidth(addr, halfWindow)
	acc = sdb.GetAccount(addr)
	if acc.NetUsage() != 250 {
		t.Fatalf("net usage after half window: want 250, got %d", acc.NetUsage())
	}

	rp.RecoverBandwidth(addr, int64(params.WindowSizeMs))
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

	rp.RecoverEnergy(addr, int64(params.WindowSizeMs))
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

	rp.RecoverFreeBandwidth(addr, int64(params.WindowSizeMs*3/4))
	acc = sdb.GetAccount(addr)
	if acc.FreeNetUsage() != 150 {
		t.Fatalf("free net usage at 75%% window: want 150, got %d", acc.FreeNetUsage())
	}
}

func testCoreAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}
