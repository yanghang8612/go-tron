package state

import (
	"testing"
)

func TestWeightCounters_Defaults(t *testing.T) {
	dp := NewDynamicProperties()
	if got := dp.TotalNetWeight(); got != 0 {
		t.Errorf("TotalNetWeight default: got %d, want 0", got)
	}
	if got := dp.TotalEnergyWeight(); got != 0 {
		t.Errorf("TotalEnergyWeight default: got %d, want 0", got)
	}
	if got := dp.TotalTronPowerWeight(); got != 0 {
		t.Errorf("TotalTronPowerWeight default: got %d, want 0", got)
	}
}

func TestAddTotalNetWeight_NoClampWithoutNewReward(t *testing.T) {
	dp := NewDynamicProperties()
	dp.SetTotalNetWeight(5)
	dp.AddTotalNetWeight(-10)
	if got := dp.TotalNetWeight(); got != -5 {
		t.Errorf("pre-reward negative: got %d, want -5", got)
	}
}

func TestAddTotalNetWeight_ClampOnceNewRewardActive(t *testing.T) {
	dp := NewDynamicProperties()
	dp.SetAllowNewReward(true)
	dp.SetTotalNetWeight(5)
	dp.AddTotalNetWeight(-10)
	if got := dp.TotalNetWeight(); got != 0 {
		t.Errorf("post-reward clamp: got %d, want 0", got)
	}
	dp.AddTotalNetWeight(7)
	if got := dp.TotalNetWeight(); got != 7 {
		t.Errorf("post-clamp add: got %d, want 7", got)
	}
}

func TestAddTotalEnergyWeight_ZeroDeltaIsNoOp(t *testing.T) {
	dp := NewDynamicProperties()
	dp.SetTotalEnergyWeight(42)

	// Simulate a second dp instance from the fresh DB; the AddTotalEnergyWeight(0)
	// below must not dirty a key and must not change value on next flush.
	dp2 := flushAndReload(t, dp)
	dp2.AddTotalEnergyWeight(0)
	if got := dp2.TotalEnergyWeight(); got != 42 {
		t.Errorf("zero-delta changed value: got %d, want 42", got)
	}
}

func TestAddTotalTronPowerWeight_RoundTripPersisted(t *testing.T) {
	dp := NewDynamicProperties()
	dp.AddTotalTronPowerWeight(1234)

	dp2 := flushAndReload(t, dp)
	if got := dp2.TotalTronPowerWeight(); got != 1234 {
		t.Errorf("round trip: got %d, want 1234", got)
	}
}
