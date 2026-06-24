package actuator

import "math"

// exchangePowOverride mirrors the hard-coded `powData` table in java-tron's
// arm-platform MathWrapper:
//
//	platform/src/main/java/arm/org/tron/common/math/MathWrapper.java
//
// Background: before proposal #87 (`allow_strict_math`), java-tron's exchange
// pricing called `MathWrapper.pow`, which on x86 mainnet evaluated the native
// `Math.pow` (the consensus gold standard for the non-strict era). On arm
// hardware `Math.pow` does not reproduce x86 bit-for-bit, so java-tron's arm
// build computes `StrictMath.pow` and then consults this 48-entry table to
// patch the inputs where `StrictMath.pow != x86 Math.pow`. Every entry uses
// EXPONENT = 0.0005 (the `exchangeToSupply` connector weight 1/2000); these
// are the only historical mainnet inputs at which the two diverge.
//
// go-tron's non-strict path therefore mirrors the arm build: `strictmath.Pow`
// (our fdlibm port == Java `StrictMath.pow`) plus this override table. The
// resulting value equals the x86 `Math.pow` that produced the canonical chain.
//
// The strict path (proposal #87 onward, `useStrictMath == true`) maps to
// java `Maths.pow(..., true)` == `StrictMathWrapper.pow`, which never consults
// MathWrapper / this table — so the override is gated to the non-strict path.
//
// java `PowData` keys on the composite (a, b); we key on the same pair so an
// input with a matching base but a different exponent does not hit the table.
type exchangePowKey struct {
	base float64
	exp  float64
}

// exchangePowExponent is java MathWrapper's EXPONENT constant
// ("3f40624dd2f1a9fc" == 1/2000 == 0.0005).
var exchangePowExponent = math.Float64frombits(0x3f40624dd2f1a9fc)

// exchangePowOverrideTable is the verbatim port of MathWrapper.powData. Each
// entry's trailing comment is the mainnet block number(s) from the java source.
var exchangePowOverrideTable = map[exchangePowKey]float64{
	powKey(0x3ff0192278704be3): math.Float64frombits(0x3ff000033518c576), // 4137160(block)
	powKey(0x3ff000002fc6a33f): math.Float64frombits(0x3ff0000000061d86), // 4065476
	powKey(0x3ff00314b1e73ecf): math.Float64frombits(0x3ff0000064ea3ef8), // 4071538
	powKey(0x3ff0068cd52978ae): math.Float64frombits(0x3ff00000d676966c), // 4109544
	powKey(0x3ff0032fda05447d): math.Float64frombits(0x3ff0000068636fe0), // 4123826
	powKey(0x3ff00051c09cc796): math.Float64frombits(0x3ff000000a76c20e), // 4166806
	powKey(0x3ff00bef8115b65d): math.Float64frombits(0x3ff0000186893de0), // 4225778
	powKey(0x3ff009b0b2616930): math.Float64frombits(0x3ff000013d27849e), // 4251796
	powKey(0x3ff00364ba163146): math.Float64frombits(0x3ff000006f26a9dc), // 4257157
	powKey(0x3ff019be4095d6ae): math.Float64frombits(0x3ff0000348e9f02a), // 4260583
	powKey(0x3ff0123e52985644): math.Float64frombits(0x3ff0000254797fd0), // 4367125
	powKey(0x3ff0126d052860e2): math.Float64frombits(0x3ff000025a6cde26), // 4402197
	powKey(0x3ff0001632cccf1b): math.Float64frombits(0x3ff0000002d76406), // 4405788
	powKey(0x3ff0000965922b01): math.Float64frombits(0x3ff000000133e966), // 4490332
	powKey(0x3ff00005c7692d61): math.Float64frombits(0x3ff0000000bd5d34), // 4499056
	powKey(0x3ff015cba20ec276): math.Float64frombits(0x3ff00002c84cef0e), // 4518035
	powKey(0x3ff00002f453d343): math.Float64frombits(0x3ff000000060cf4e), // 4533215
	powKey(0x3ff006ea73f88946): math.Float64frombits(0x3ff00000e26d4ea2), // 4647814
	powKey(0x3ff00a3632db72be): math.Float64frombits(0x3ff000014e3382a6), // 4766695
	powKey(0x3ff000c0e8df0274): math.Float64frombits(0x3ff0000018b0aeb2), // 4771494
	powKey(0x3ff00015c8f06afe): math.Float64frombits(0x3ff0000002c9d73e), // 4793587
	powKey(0x3ff00068def18101): math.Float64frombits(0x3ff000000d6c3cac), // 4801947
	powKey(0x3ff01349f3ac164b): math.Float64frombits(0x3ff000027693328a), // 4916843
	powKey(0x3ff00e86a7859088): math.Float64frombits(0x3ff00001db256a52), // 4924111
	powKey(0x3ff00000c2a51ab7): math.Float64frombits(0x3ff000000018ea20), // 5098864
	powKey(0x3ff020fb74e9f170): math.Float64frombits(0x3ff00004346fbfa2), // 5133963
	powKey(0x3ff00001ce277ce7): math.Float64frombits(0x3ff00000003b27dc), // 5139389
	powKey(0x3ff005468a327822): math.Float64frombits(0x3ff00000acc20750), // 5151258
	powKey(0x3ff00006666f30ff): math.Float64frombits(0x3ff0000000d1b80e), // 5185021
	powKey(0x3ff000045a0b2035): math.Float64frombits(0x3ff00000008e98e6), // 5295829
	powKey(0x3ff00e00380e10d7): math.Float64frombits(0x3ff00001c9ff83c8), // 5380897
	powKey(0x3ff00c15de2b0d5e): math.Float64frombits(0x3ff000018b6eaab6), // 5400886
	powKey(0x3ff00042afe6956a): math.Float64frombits(0x3ff0000008892244), // 5864127
	powKey(0x3ff0005b7357c2d4): math.Float64frombits(0x3ff000000bb48572), // 6167339
	powKey(0x3ff00033d5ab51c8): math.Float64frombits(0x3ff0000006a279c8), // 6240974
	powKey(0x3ff0000046d74585): math.Float64frombits(0x3ff0000000091150), // 6279093
	powKey(0x3ff0010403f34767): math.Float64frombits(0x3ff0000021472146), // 6428736
	powKey(0x3ff00496fe59bc98): math.Float64frombits(0x3ff000009650a4ca), // 6432355,6493373
	powKey(0x3ff0012e43815868): math.Float64frombits(0x3ff0000026af266e), // 6555029
	powKey(0x3ff00021f6080e3c): math.Float64frombits(0x3ff000000458d16a), // 7092933
	powKey(0x3ff000489c0f28bd): math.Float64frombits(0x3ff00000094b3072), // 7112412
	powKey(0x3ff00009d3df2e9c): math.Float64frombits(0x3ff00000014207b4), // 7675535
	powKey(0x3ff000def05fa9c8): math.Float64frombits(0x3ff000001c887cdc), // 7860324
	powKey(0x3ff0013bca543227): math.Float64frombits(0x3ff00000286a42d2), // 8292427
	powKey(0x3ff0021a2f14a0ee): math.Float64frombits(0x3ff0000044deb040), // 8517311
	powKey(0x3ff0002cc166be3c): math.Float64frombits(0x3ff0000005ba841e), // 8763101
	powKey(0x3ff0000cc84e613f): math.Float64frombits(0x3ff0000001a2da46), // 9269124
	powKey(0x3ff000057b83c83f): math.Float64frombits(0x3ff0000000b3a640), // 9631452
}

// powKey builds the lookup key for a base whose IEEE-754 bits are baseBits,
// paired with the fixed 0.0005 exponent (java MathWrapper EXPONENT).
func powKey(baseBits uint64) exchangePowKey {
	return exchangePowKey{base: math.Float64frombits(baseBits), exp: exchangePowExponent}
}

// exchangePowOverride returns the x86-mainnet `Math.pow(a, b)` value for the
// historical inputs java patches in MathWrapper.powData, mirroring
// `powData.getOrDefault(new PowData(a, b), ...)`. ok is false for any input
// outside the table (the caller then falls back to strictmath.Pow).
func exchangePowOverride(a, b float64) (float64, bool) {
	r, ok := exchangePowOverrideTable[exchangePowKey{base: a, exp: b}]
	return r, ok
}
