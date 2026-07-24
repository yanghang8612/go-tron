package vm

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestNewTVMAllocatesNewContractSetLazily(t *testing.T) {
	tvm := NewTVM(nil, nil, tcommon.Address{}, 1, 2, tcommon.Address{}, 3, TVMConfig{})
	if tvm.newContracts != nil {
		t.Fatal("NewTVM eagerly allocated the new-contract set")
	}
	addr := tcommon.Address{0x41, 0x22}
	if tvm.isNewContract(addr) {
		t.Fatal("nil new-contract set reported a contract as new")
	}
	if wasNew := tvm.markNewContract(addr); wasNew {
		t.Fatal("first mark reported an existing new contract")
	}
	if tvm.newContracts == nil || !tvm.isNewContract(addr) {
		t.Fatal("first mark did not lazily allocate and record the contract")
	}
	if wasNew := tvm.markNewContract(addr); !wasNew {
		t.Fatal("second mark did not report the existing new contract")
	}
	tvm.restoreNewContractMark(addr, false)
	if tvm.isNewContract(addr) {
		t.Fatal("restoring an unmarked contract left a stale mark")
	}
}

func TestNewTVMConfig_AllFalseByDefault(t *testing.T) {
	dp := state.NewDynamicProperties()
	cfg := NewTVMConfig(0, dp)
	if cfg.TransferTrc10 || cfg.Constantinople || cfg.Solidity059 || cfg.Istanbul ||
		cfg.Freeze || cfg.ShieldedToken || cfg.Vote || cfg.StakingV2 || cfg.London ||
		cfg.Compatibility || cfg.DynamicEnergy || cfg.EnergyAdjustment || cfg.Shanghai || cfg.Blob || cfg.Cancun ||
		cfg.SelfdestructRestrict || cfg.Prague || cfg.Osaka || cfg.MultiSign || cfg.OptimizedReturnValueOfChainId {
		t.Fatal("expected all VM fork flags false by default")
	}
}

func TestNewTVMConfig_IstanbulEnabled(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowTvmIstanbul(true)
	cfg := NewTVMConfig(0, dp)
	if !cfg.Istanbul {
		t.Fatal("expected Istanbul=true after enabling allow_tvm_istanbul")
	}
	if cfg.Constantinople {
		t.Fatal("Constantinople should remain false")
	}
}

func TestNewTVMConfig_ConstantinopleEnabled(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowTvmConstantinople(true)
	cfg := NewTVMConfig(0, dp)
	if !cfg.Constantinople {
		t.Fatal("expected Constantinople=true")
	}
}

func TestNewTVMConfig_LondonEnabled(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowTvmLondon(true)
	cfg := NewTVMConfig(0, dp)
	if !cfg.London {
		t.Fatal("expected London=true")
	}
}

func TestNewTVMConfig_NilDynProps(t *testing.T) {
	cfg := NewTVMConfig(0, nil)
	if cfg.TransferTrc10 || cfg.Constantinople || cfg.Solidity059 || cfg.Istanbul ||
		cfg.Freeze || cfg.ShieldedToken || cfg.Vote || cfg.StakingV2 || cfg.London ||
		cfg.Compatibility || cfg.DynamicEnergy || cfg.EnergyAdjustment || cfg.Shanghai || cfg.Blob || cfg.Cancun ||
		cfg.SelfdestructRestrict || cfg.Prague || cfg.Osaka || cfg.MultiSign || cfg.OptimizedReturnValueOfChainId {
		t.Fatal("expected all false with nil DynProps")
	}
}

func TestNewTVMConfig_StakingV2FollowsUnfreezeDelay(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowNewResourceModel(true)
	cfg := NewTVMConfig(0, dp)
	if cfg.StakingV2 {
		t.Fatal("StakingV2 TVM gate must stay false until unfreeze_delay_days is set")
	}
	dp.SetUnfreezeDelayDays(14)
	cfg = NewTVMConfig(0, dp)
	if !cfg.StakingV2 {
		t.Fatal("StakingV2 TVM gate should follow supportUnfreezeDelay")
	}
	if !cfg.NewResourceModelPower {
		t.Fatal("NewResourceModelPower should require both new resource model and unfreeze delay")
	}
}

func TestNewTVMConfig_LateForksEnabled(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowEnergyAdjustment(true)
	dp.SetAllowTvmShanghai(true)
	dp.SetAllowTvmSelfdestructRestriction(true)
	dp.SetAllowTvmPrague(true)
	dp.SetAllowTvmOsaka(true)
	cfg := NewTVMConfig(0, dp)
	if !cfg.EnergyAdjustment || !cfg.Shanghai || !cfg.SelfdestructRestrict || !cfg.Prague || !cfg.Osaka {
		t.Fatalf("late VM fork flags not reflected: %+v", cfg)
	}
}
