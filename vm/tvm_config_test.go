package vm

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

func TestNewTVMConfig_AllFalseByDefault(t *testing.T) {
	dp := state.NewDynamicProperties()
	cfg := NewTVMConfig(0, dp)
	if cfg.TransferTrc10 || cfg.Constantinople || cfg.Solidity059 || cfg.Istanbul ||
		cfg.Freeze || cfg.ShieldedToken || cfg.Vote || cfg.London ||
		cfg.Compatibility || cfg.DynamicEnergy || cfg.EnergyAdjustment || cfg.Shanghai || cfg.Blob || cfg.Cancun ||
		cfg.SelfdestructRestrict || cfg.Prague || cfg.Osaka {
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
		cfg.Freeze || cfg.ShieldedToken || cfg.Vote || cfg.London ||
		cfg.Compatibility || cfg.DynamicEnergy || cfg.EnergyAdjustment || cfg.Shanghai || cfg.Blob || cfg.Cancun ||
		cfg.SelfdestructRestrict || cfg.Prague || cfg.Osaka {
		t.Fatal("expected all false with nil DynProps")
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
