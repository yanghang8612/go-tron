package delegation

import (
	"math/big"
	"testing"

	"github.com/tronprotocol/go-tron/params"
)

// TestResourceUsageToFrozenBalance_AlwaysPlainDouble locks the L7 fix: the
// usage->frozen conversion must use the plain (long)(double) formula on every
// path, matching java DelegateResourceActuator.validate /
// DelegateResourceProcessor. java never applies BigInteger / harden here
// (AllowHardenResourceCalculation only switches the recovery averaging in
// ResourceProcessor.increase). Pre-fix go switched to exact big.Int under harden,
// which diverges from java at the rounding boundary.
func TestResourceUsageToFrozenBalance_AlwaysPlainDouble(t *testing.T) {
	// usage=7, totalLimit=7*P, totalWeight=1:
	//   exact big.Int  : 7*P*1 / (7*P) = 1
	//   plain double    : 7*P * (1.0/(7*P)) = 0.999...  -> (long) 0
	const usage, totalWeight = int64(7), int64(1)
	totalLimit := int64(7) * params.TRXPrecision

	got := resourceUsageToFrozenBalance(usage, totalLimit, totalWeight)

	want := int64(float64(usage) * float64(params.TRXPrecision) * (float64(totalWeight) / float64(totalLimit)))
	if got != want {
		t.Fatalf("resourceUsageToFrozenBalance = %d, want plain-double %d", got, want)
	}
	if got != 0 {
		t.Fatalf("plain-double result = %d, want 0 (proves no big.Int rounding)", got)
	}

	// Cross-check: the exact big.Int formula java does NOT use would give 1 here,
	// so the test has teeth against re-introducing the big.Int branch.
	exact := new(big.Int).Mul(big.NewInt(usage), big.NewInt(params.TRXPrecision))
	exact.Mul(exact, big.NewInt(totalWeight))
	exact.Quo(exact, big.NewInt(totalLimit))
	if exact.Int64() == got {
		t.Fatalf("test ineffective: big.Int and double agree (%d); pick a diverging triple", got)
	}
}
