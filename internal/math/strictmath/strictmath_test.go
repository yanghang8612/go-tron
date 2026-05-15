package strictmath

import (
	"math"
	"testing"
)

// TestPow_DelegatesToMathPow locks in the current placeholder behavior:
// strictmath.Pow forwards to math.Pow unchanged. When the fdlibm port
// lands this test will start failing for inputs where Go's math.Pow and
// java-tron's StrictMath.pow disagree — at which point this test should
// be replaced with a Java-oracle vector comparison.
func TestPow_DelegatesToMathPow(t *testing.T) {
	cases := []struct{ a, b float64 }{
		{1.0001, 0.0005},        // exchangeToSupply shape (small exponent)
		{1.0001, 2000.0},        // exchangeFromSupply shape (large exponent)
		{1.5, 10.0},             // simple integer-ish exponent
		{0.9999, 100.0},         // decay shape (CatchUpToCycle)
		{2.0, 0.5},              // sqrt
	}
	for _, c := range cases {
		got := Pow(c.a, c.b)
		want := math.Pow(c.a, c.b)
		if got != want {
			t.Fatalf("Pow(%g, %g) = %g, want %g (placeholder must match math.Pow until fdlibm port lands)", c.a, c.b, got, want)
		}
	}
}
