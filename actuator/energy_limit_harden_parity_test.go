package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

// TestCalcAccountEnergyLimit_HardenBigInteger is the parity regression for
// cross-impl audit finding B-1: the in-VM energy-limit duplicate in java-tron,
// RepositoryImpl.calculateGlobalEnergyLimit (actuator module, ~line 967-985),
// has a hardenResourceCalculation() (#97 allow_harden_resource_calculation)
// branch that computes the limit with BigInteger so weight*totalLimit can exceed
// 2^53 without losing precision:
//
//	if (hardenResourceCalculation())
//	  return BigInteger.valueOf(energyWeight)
//	      .multiply(BigInteger.valueOf(totalEnergyLimit))
//	      .divide(BigInteger.valueOf(totalEnergyWeight))
//	      .longValueExact();
//	return (long) (energyWeight * ((double) totalEnergyLimit / totalEnergyWeight));
//
// NOTE the harden branch is plain weight*limit/weight with NO TRX_PRECISION
// factor (TRX_PRECISION appears only in usageToBalance, a different method);
// energyWeight is already frozeBalance / TRX_PRECISION.
//
// go-tron's calcAccountEnergyLimit previously returned the plain-float result
// UNCONDITIONALLY. When weight*totalLimit > 2^53 the float64 multiply loses a
// unit, so the in-VM energy budget diverges by 1 vs java — same class as the
// 8,825,873 stall (an off-by-one in the per-tx energy budget can flip
// OUT_OF_ENERGY <-> SUCCESS).
//
// Golden numbers (java BigInteger, hand-verified):
//
//	2488889 * 180000000000000 / 7000000001 = 64000002847   (harden / exact)
//	float64 path                            = 64000002848   (current go, off by +1)
func TestCalcAccountEnergyLimit_HardenBigInteger(t *testing.T) {
	const (
		// weight = frozen / TRXPrecision = 2_488_889 (whole TRX)
		frozen     = int64(2_488_889_000_000)
		totalLimit = int64(180_000_000_000_000)
		totalWgt   = int64(7_000_000_001)

		wantHarden = int64(64_000_002_847) // java BigInteger longValueExact
		wantFloat  = int64(64_000_002_848) // plain float64 (pre-harden / pre-fix)
	)

	newDP := func(harden bool) *state.DynamicProperties {
		dp := state.NewDynamicProperties()
		dp.SetTotalEnergyWeight(totalWgt)
		dp.SetTotalEnergyCurrentLimit(totalLimit)
		dp.SetAllowHardenResourceCalculation(harden)
		return dp
	}

	sdb := setupStateDB(t)
	addr := makeTestAddr(31)
	seedAccount(sdb, addr, 1)
	sdb.FreezeV1Energy(addr, frozen, 0)
	acct := sdb.GetAccount(addr)

	// harden=true must match java's BigInteger branch (exact, no precision loss).
	if got := calcAccountEnergyLimit(acct, newDP(true)); got != wantHarden {
		t.Errorf("harden=true in-VM energy limit: got %d, want %d (java BigInteger %d*%d/%d)",
			got, wantHarden, frozen/1_000_000, totalLimit, totalWgt)
	}

	// harden=false must stay on the plain-float path (pre-#97 behavior unchanged),
	// which loses one unit at this magnitude — proving the two branches differ and
	// that the float path is preserved for pre-fork blocks.
	if got := calcAccountEnergyLimit(acct, newDP(false)); got != wantFloat {
		t.Errorf("harden=false in-VM energy limit: got %d, want %d (plain float64, pre-#97)",
			got, wantFloat)
	}
}
