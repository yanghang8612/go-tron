package vm

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

func TestNewTVMConfig_AllFalseByDefault(t *testing.T) {
	dp := state.NewDynamicProperties()
	cfg := NewTVMConfig(0, dp)
	if cfg.Constantinople || cfg.Istanbul || cfg.London || cfg.Freeze || cfg.Vote || cfg.Cancun {
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
	if cfg.Constantinople || cfg.Istanbul || cfg.London {
		t.Fatal("expected all false with nil DynProps")
	}
}
