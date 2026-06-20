package vm

import (
	"math/big"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// TestGetChainParameter_UsesWiredDynProps pins that the getChainParameter
// precompile (0x0100000b) reads the wired tvm.DynProps, not the production-empty
// tvm.StateDB.DynamicProperties() (the genesis default from state.New).
//
// Nile 34,563,685: STRXG1.claim() read getChainParameter(5)=UNFREEZE_DELAY_DAYS
// and got 0 (the genesis default) instead of the live 14, diverging its
// rental/round math (expected SUCCESS, got REVERT). Same dp-source class as the
// dynamic-energy fix. Pre-fix this returns 0; post-fix it returns 14.
func TestGetChainParameter_UsesWiredDynProps(t *testing.T) {
	tvm, stateDB, dp := newProductionWiredTVM(t)
	dp.SetUnfreezeDelayDays(14)

	// Production hazard: the StateDB's own dp is the empty genesis default,
	// distinct from the wired live dp.
	if stateDB.DynamicProperties().UnfreezeDelayDays() == 14 {
		t.Fatal("precondition: production StateDB dp must differ from the wired DynProps")
	}

	out, _, err := (&getChainParameter{}).Run(tvm, tcommon.Address{}, int64ToBytes32(5), 50)
	if err != nil {
		t.Fatalf("getChainParameter run: %v", err)
	}
	if got := new(big.Int).SetBytes(out).Int64(); got != 14 {
		t.Fatalf("getChainParameter(5=UNFREEZE_DELAY_DAYS) = %d, want 14 (must read the wired DynProps, not the empty StateDB dp)", got)
	}
}
