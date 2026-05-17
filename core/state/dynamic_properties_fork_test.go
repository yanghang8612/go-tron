package state

import "testing"

func TestAllowFlagDefaultFalse(t *testing.T) {
	dp := NewDynamicProperties()
	flags := []bool{
		dp.AllowSameTokenName(),
		dp.AllowDelegateResource(),
		dp.AllowMultiSign(),
		dp.ChangeDelegation(),
		dp.AllowTvmTransferTrc10(),
		dp.AllowTvmConstantinople(),
		dp.AllowTvmSolidity059(),
		dp.AllowTvmIstanbul(),
		dp.AllowMarketTransaction(),
		dp.AllowTvmFreeze(),
		dp.AllowTvmVote(),
		dp.AllowStakingV2(),
		dp.AllowTvmLondon(),
		dp.AllowTvmCompatibleEvm(),
		dp.AllowDynamicEnergy(),
		dp.AllowTvmCancun(),
		dp.AllowEnergyAdjustment(),
		dp.AllowAdaptiveEnergy(),
		dp.AllowTvmShieldedToken(),
		dp.AllowPbft(),
		dp.AllowTvmBlob(),
		dp.AllowTvmPrague(),
		dp.AllowHardenResourceCalculation(),
		dp.AllowHardenExchangeCalculation(),
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

func TestLatestExchangeNum(t *testing.T) {
	dp := NewDynamicProperties()
	if dp.LatestExchangeNum() != 0 {
		t.Fatalf("expected default 0, got %d", dp.LatestExchangeNum())
	}
	dp.SetLatestExchangeNum(5)
	if dp.LatestExchangeNum() != 5 {
		t.Fatalf("expected 5, got %d", dp.LatestExchangeNum())
	}
}

func TestExchangeCreateFee(t *testing.T) {
	dp := NewDynamicProperties()
	if dp.ExchangeCreateFee() != 1_024_000_000 {
		t.Fatalf("expected default 1_024_000_000, got %d", dp.ExchangeCreateFee())
	}
	dp.SetExchangeCreateFee(2_000_000_000)
	if dp.ExchangeCreateFee() != 2_000_000_000 {
		t.Fatalf("expected 2_000_000_000, got %d", dp.ExchangeCreateFee())
	}
	if _, ok := dp.dirty["exchange_create_fee"]; !ok {
		t.Fatalf("exchange_create_fee should be dirty after Set")
	}
}

func TestExchangeBalanceLimit(t *testing.T) {
	dp := NewDynamicProperties()
	if dp.ExchangeBalanceLimit() != 1_000_000_000_000_000 {
		t.Fatalf("expected default 1e15, got %d", dp.ExchangeBalanceLimit())
	}
	dp.SetExchangeBalanceLimit(42)
	if dp.ExchangeBalanceLimit() != 42 {
		t.Fatalf("expected 42, got %d", dp.ExchangeBalanceLimit())
	}
	if _, ok := dp.dirty["exchange_balance_limit"]; !ok {
		t.Fatalf("exchange_balance_limit should be dirty after Set")
	}
}

func TestAllowFlagPersistence(t *testing.T) {
	dp := NewDynamicProperties()
	// AllowStakingV2 is an alias for AllowNewResourceModel since M1.3 Task 5
	// — java-tron uses one flag for both state-layer V2 and VM V2 precompiles.
	dp.SetAllowStakingV2(true)
	dp.SetAllowTvmIstanbul(true)
	v1, ok1 := dp.Get("allow_new_resource_model")
	v2, ok2 := dp.Get("allow_tvm_istanbul")
	if !ok1 || v1 != 1 {
		t.Fatalf("allow_new_resource_model not persisted correctly: ok=%v v=%v", ok1, v1)
	}
	if !ok2 || v2 != 1 {
		t.Fatalf("allow_tvm_istanbul not persisted correctly: ok=%v v=%v", ok2, v2)
	}
	if !dp.AllowStakingV2() || !dp.AllowNewResourceModel() {
		t.Fatal("both alias and canonical getters must read true")
	}
}
