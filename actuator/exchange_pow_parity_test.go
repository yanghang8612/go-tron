package actuator

import (
	"math"
	"math/big"
	"testing"

	"github.com/tronprotocol/go-tron/internal/math/strictmath"
)

// exchangePowGolden is the verbatim (base bits, x86 result bits) gold standard
// from java-tron arm MathWrapper.powData (EXPONENT == 0.0005 == 1/2000). These
// are the x86 mainnet `Math.pow` values for the pre-#87 (non-strict) era. We
// re-list them independently of exchange_pow_override.go so the test is a true
// external oracle, not a tautology against the table it guards.
var exchangePowGolden = []struct {
	baseBits uint64
	retBits  uint64
}{
	{0x3ff0192278704be3, 0x3ff000033518c576},
	{0x3ff000002fc6a33f, 0x3ff0000000061d86},
	{0x3ff00314b1e73ecf, 0x3ff0000064ea3ef8},
	{0x3ff0068cd52978ae, 0x3ff00000d676966c},
	{0x3ff0032fda05447d, 0x3ff0000068636fe0},
	{0x3ff00051c09cc796, 0x3ff000000a76c20e},
	{0x3ff00bef8115b65d, 0x3ff0000186893de0},
	{0x3ff009b0b2616930, 0x3ff000013d27849e},
	{0x3ff00364ba163146, 0x3ff000006f26a9dc},
	{0x3ff019be4095d6ae, 0x3ff0000348e9f02a},
	{0x3ff0123e52985644, 0x3ff0000254797fd0},
	{0x3ff0126d052860e2, 0x3ff000025a6cde26},
	{0x3ff0001632cccf1b, 0x3ff0000002d76406},
	{0x3ff0000965922b01, 0x3ff000000133e966},
	{0x3ff00005c7692d61, 0x3ff0000000bd5d34},
	{0x3ff015cba20ec276, 0x3ff00002c84cef0e},
	{0x3ff00002f453d343, 0x3ff000000060cf4e},
	{0x3ff006ea73f88946, 0x3ff00000e26d4ea2},
	{0x3ff00a3632db72be, 0x3ff000014e3382a6},
	{0x3ff000c0e8df0274, 0x3ff0000018b0aeb2},
	{0x3ff00015c8f06afe, 0x3ff0000002c9d73e},
	{0x3ff00068def18101, 0x3ff000000d6c3cac},
	{0x3ff01349f3ac164b, 0x3ff000027693328a},
	{0x3ff00e86a7859088, 0x3ff00001db256a52},
	{0x3ff00000c2a51ab7, 0x3ff000000018ea20},
	{0x3ff020fb74e9f170, 0x3ff00004346fbfa2},
	{0x3ff00001ce277ce7, 0x3ff00000003b27dc},
	{0x3ff005468a327822, 0x3ff00000acc20750},
	{0x3ff00006666f30ff, 0x3ff0000000d1b80e},
	{0x3ff000045a0b2035, 0x3ff00000008e98e6},
	{0x3ff00e00380e10d7, 0x3ff00001c9ff83c8},
	{0x3ff00c15de2b0d5e, 0x3ff000018b6eaab6},
	{0x3ff00042afe6956a, 0x3ff0000008892244},
	{0x3ff0005b7357c2d4, 0x3ff000000bb48572},
	{0x3ff00033d5ab51c8, 0x3ff0000006a279c8},
	{0x3ff0000046d74585, 0x3ff0000000091150},
	{0x3ff0010403f34767, 0x3ff0000021472146},
	{0x3ff00496fe59bc98, 0x3ff000009650a4ca},
	{0x3ff0012e43815868, 0x3ff0000026af266e},
	{0x3ff00021f6080e3c, 0x3ff000000458d16a},
	{0x3ff000489c0f28bd, 0x3ff00000094b3072},
	{0x3ff00009d3df2e9c, 0x3ff00000014207b4},
	{0x3ff000def05fa9c8, 0x3ff000001c887cdc},
	{0x3ff0013bca543227, 0x3ff00000286a42d2},
	{0x3ff0021a2f14a0ee, 0x3ff0000044deb040},
	{0x3ff0002cc166be3c, 0x3ff0000005ba841e},
	{0x3ff0000cc84e613f, 0x3ff0000001a2da46},
	{0x3ff000057b83c83f, 0x3ff0000000b3a640},
}

// exchangePowExp is java MathWrapper EXPONENT ("3f40624dd2f1a9fc" == 0.0005).
const exchangePowExpBits = 0x3f40624dd2f1a9fc

// TestExchangePowNonStrictMatchesX86History asserts that the non-strict pow
// path (pre-#87 mainnet era) reproduces the x86 `Math.pow` gold standard for
// all 48 historical inputs java patches in MathWrapper.powData. This is the
// A-1 regression: plain math.Pow / plain strictmath.Pow miss these by 1 ULP.
func TestExchangePowNonStrictMatchesX86History(t *testing.T) {
	exp := math.Float64frombits(exchangePowExpBits)
	p := newExchangeProcessor(false) // non-strict (pre-#87)
	for _, g := range exchangePowGolden {
		base := math.Float64frombits(g.baseBits)
		got := math.Float64bits(p.pow(base, exp))
		if got != g.retBits {
			t.Errorf("non-strict pow(base=%016x, 0.0005): got bits %016x, want x86 gold %016x",
				g.baseBits, got, g.retBits)
		}
	}
}

// TestExchangePowOverrideTableMatchesGolden guards the override table itself
// against the same external gold list — catching any transcription drift in
// exchange_pow_override.go independent of the pow() wiring.
func TestExchangePowOverrideTableMatchesGolden(t *testing.T) {
	if len(exchangePowOverrideTable) != len(exchangePowGolden) {
		t.Fatalf("override table size %d, want %d (java has 48 entries; distinct bases)",
			len(exchangePowOverrideTable), len(exchangePowGolden))
	}
	exp := math.Float64frombits(exchangePowExpBits)
	for _, g := range exchangePowGolden {
		base := math.Float64frombits(g.baseBits)
		ret, ok := exchangePowOverride(base, exp)
		if !ok {
			t.Errorf("override missing entry for base %016x", g.baseBits)
			continue
		}
		if math.Float64bits(ret) != g.retBits {
			t.Errorf("override[base=%016x]: got %016x, want %016x",
				g.baseBits, math.Float64bits(ret), g.retBits)
		}
	}
}

// TestExchangePowStrictDoesNotConsultOverride asserts the strict path (#87+)
// is pure StrictMath.pow with NO table lookup — mirroring java
// Maths.pow(a, b, true) == StrictMathWrapper.pow (which never touches
// MathWrapper.powData). For these inputs strict.Pow != the x86 override value,
// so equality with the override would prove an illegal table consult.
func TestExchangePowStrictDoesNotConsultOverride(t *testing.T) {
	exp := math.Float64frombits(exchangePowExpBits)
	p := newExchangeProcessor(true) // strict (#87+)
	checkedDivergence := false
	for _, g := range exchangePowGolden {
		base := math.Float64frombits(g.baseBits)
		got := math.Float64bits(p.pow(base, exp))
		wantStrict := math.Float64bits(strictmath.Pow(base, exp))
		if got != wantStrict {
			t.Errorf("strict pow(base=%016x): got %016x, want pure strictmath %016x",
				g.baseBits, got, wantStrict)
		}
		// At least one historical input must have strictmath != x86 override,
		// otherwise this test proves nothing.
		if wantStrict != g.retBits {
			checkedDivergence = true
			if got == g.retBits {
				t.Errorf("strict path returned override value %016x for base %016x — must not consult table",
					g.retBits, g.baseBits)
			}
		}
	}
	if !checkedDivergence {
		t.Fatal("no strictmath!=x86 divergence in golden set; strict-path test is vacuous")
	}
}

// TestExchangePowNonStrictOffTableUsesStrict asserts a base NOT in the override
// table falls through to strictmath.Pow on the non-strict path (java
// getOrDefault default branch).
func TestExchangePowNonStrictOffTableUsesStrict(t *testing.T) {
	exp := math.Float64frombits(exchangePowExpBits)
	// A base adjacent to a real entry but +1 ULP, guaranteed off-table.
	offBase := math.Float64frombits(0x3ff0192278704be3 + 1)
	if _, ok := exchangePowOverride(offBase, exp); ok {
		t.Fatalf("test setup: off-table base unexpectedly in override table")
	}
	p := newExchangeProcessor(false)
	got := math.Float64bits(p.pow(offBase, exp))
	want := math.Float64bits(strictmath.Pow(offBase, exp))
	if got != want {
		t.Errorf("non-strict off-table pow: got %016x, want strictmath %016x", got, want)
	}
}

// TestRatToFloat64CorrectRoundingMatchesJava is the A-2 regression. java
// SafeExchangeProcessor uses BigDecimal.doubleValue() (a single, correctly
// rounded conversion). The old ratToFloat64 used big.Float.SetPrec(64) first,
// double-rounding (64-bit then 53-bit) and diverging by 1 ULP on some inputs.
// This case (balance=100000000000, quant=101611) reproduces the divergence:
// the correct round-to-nearest-even result is 0x3ff0000110c279ff, while the
// double-rounded path yields 0x3ff0000110c27a00.
func TestRatToFloat64CorrectRoundingMatchesJava(t *testing.T) {
	const balance, quant int64 = 100_000_000_000, 101_611
	newBalance := balance + quant
	div := roundRatScaleHalfUp(big.NewRat(quant, newBalance), 18)
	base := new(big.Rat).Add(big.NewRat(1, 1), div)

	// Gold standard = java BigDecimal.doubleValue(), which equals IEEE-754
	// round-to-nearest-even of the exact rational. big.Rat.Float64 yields
	// exactly that.
	const wantBits uint64 = 0x3ff0000110c279ff
	const doubleRoundedBits uint64 = 0x3ff0000110c27a00

	// Sanity: confirm this fixture actually exercises the double-rounding bug,
	// i.e. the two conversions genuinely disagree. Otherwise the test is moot.
	dr, _ := new(big.Float).SetPrec(64).SetRat(base).Float64()
	if math.Float64bits(dr) != doubleRoundedBits {
		t.Fatalf("fixture drift: SetPrec(64) path = %016x, expected %016x",
			math.Float64bits(dr), doubleRoundedBits)
	}
	cr, _ := base.Float64()
	if math.Float64bits(cr) != wantBits {
		t.Fatalf("fixture drift: correctly-rounded = %016x, expected %016x",
			math.Float64bits(cr), wantBits)
	}
	if doubleRoundedBits == wantBits {
		t.Fatal("fixture does not trigger double rounding; pick another base")
	}

	got := math.Float64bits(ratToFloat64(base))
	if got != wantBits {
		t.Errorf("ratToFloat64(base): got %016x, want correctly-rounded (java doubleValue) %016x",
			got, wantBits)
	}
}
