package state

import "testing"

func TestAllowFlagDefaultFalse(t *testing.T) {
	dp := NewDynamicProperties()
	flags := []bool{
		dp.AllowSameTokenName(),
		dp.AllowDelegateResource(),
		dp.AllowMultiSign(),
		dp.AllowChangeDelegation(),
		dp.AllowTvmTransferTrc10(),
		dp.AllowTvmConstantinople(),
		dp.AllowTvmSolidity059(),
		dp.AllowTvmIstanbul(),
		dp.AllowMarketTransaction(),
		dp.AllowTvmFreeze(),
		dp.AllowTvmVote(),
		dp.AllowStakingV2(),
		dp.AllowTvmLondon(),
		dp.AllowTvmCompatibility(),
		dp.AllowDynamicEnergy(),
		dp.AllowTvmCancun(),
		dp.AllowEnergyAdjustment(),
		dp.AllowAdaptiveEnergyLimit(),
		dp.AllowTvmShieldedToken(),
		dp.AllowAccountHistory(),
		dp.AllowPbft(),
		dp.AllowTvmBigInteger(),
		dp.AllowTvmBlob(),
		dp.AllowTvmSolidity058(),
	}
	for i, f := range flags {
		if f {
			t.Fatalf("flag[%d] should default to false", i)
		}
	}
}

func TestAllowFlagSetAndGet(t *testing.T) {
	dp := NewDynamicProperties()
	dp.SetAllowMarketTransaction(true)
	if !dp.AllowMarketTransaction() {
		t.Fatal("AllowMarketTransaction should be true after Set(true)")
	}
	dp.SetAllowMarketTransaction(false)
	if dp.AllowMarketTransaction() {
		t.Fatal("AllowMarketTransaction should be false after Set(false)")
	}
}

func TestAllowFlagPersistence(t *testing.T) {
	dp := NewDynamicProperties()
	dp.SetAllowStakingV2(true)
	dp.SetAllowTvmIstanbul(true)
	v1, ok1 := dp.Get("allow_staking_v2")
	v2, ok2 := dp.Get("allow_tvm_istanbul")
	if !ok1 || v1 != 1 {
		t.Fatalf("allow_staking_v2 not persisted correctly: ok=%v v=%v", ok1, v1)
	}
	if !ok2 || v2 != 1 {
		t.Fatalf("allow_tvm_istanbul not persisted correctly: ok=%v v=%v", ok2, v2)
	}
}
