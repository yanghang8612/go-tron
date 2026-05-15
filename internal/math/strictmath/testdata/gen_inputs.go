// gen_inputs is a helper that produces a curated input vector for the
// fdlibm `pow` oracle. Outputs one `<a> <b>` pair per line using Go's
// `strconv.FormatFloat(..., 'x', ...)` (which Java's `Double.parseDouble`
// accepts as hex-float) so every mantissa bit is preserved exactly.
//
// Run via testdata/gen.sh — not as a normal Go test or build target.
//go:build ignore
// +build ignore

package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

func fmtFloat(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'x', -1, 64)
}

type pair struct{ a, b float64 }

func main() {
	pairs := []pair{
		// === Exchange shape: Bancor formula in exchange_processor.go ===
		// exchangeToSupply: small fractional exponent
		{1.0001, 0.0005},
		{1.000001, 0.0005},
		{1.5, 0.0005},
		{2.0, 0.0005},
		{1.0 + 1e-9, 0.0005},
		// exchangeFromSupply: large exponent
		{1.0001, 2000.0},
		{1.000001, 2000.0},
		{1.0000001, 2000.0},
		{0.9999, 2000.0},
		{1.0 + 1e-9, 2000.0},

		// === CatchUpToCycle shape: dynamic-energy decay ===
		// base in (0, 1); integer-ish exponent up to thousands
		{0.9999, 1.0},
		{0.9999, 10.0},
		{0.9999, 100.0},
		{0.9999, 1000.0},
		{0.9999, 10000.0},
		{0.99, 100.0},
		{0.9, 100.0},
		{0.5, 1000.0},
		{0.999999, 17280.0}, // ~ one maintenance period worth of slots

		// === Special-case coverage (cases 1-19 from the algorithm spec) ===
		// 1. (anything) ** 0
		{42.0, 0.0},
		{math.NaN(), 0.0},
		{math.Inf(1), 0.0},
		{math.Inf(-1), 0.0},
		{0.0, 0.0},
		{math.Copysign(0, -1), 0.0},
		// 2. (anything) ** 1
		{42.0, 1.0},
		{math.Copysign(0, -1), 1.0},
		// 3. (anything) ** NaN
		{1.0, math.NaN()},
		{0.0, math.NaN()},
		{math.NaN(), math.NaN()},
		// 4. NaN ** (anything except 0)
		{math.NaN(), 1.5},
		{math.NaN(), math.Inf(1)},
		// 5/6. (|x|>1) ** +-INF
		{2.0, math.Inf(1)},
		{2.0, math.Inf(-1)},
		{-2.0, math.Inf(1)},
		{-2.0, math.Inf(-1)},
		// 7/8. (|x|<1) ** +-INF
		{0.5, math.Inf(1)},
		{0.5, math.Inf(-1)},
		{-0.5, math.Inf(1)},
		// 9. +-1 ** +-INF
		{1.0, math.Inf(1)},
		{-1.0, math.Inf(1)},
		{1.0, math.Inf(-1)},
		// 10-14. signed zero bases with various exponent flavors
		{0.0, 3.0},
		{0.0, -3.0},
		{math.Copysign(0, -1), 3.0},  // -0 ** odd int = -0
		{math.Copysign(0, -1), 4.0},  // -0 ** even int = +0
		{math.Copysign(0, -1), 3.5},  // -0 ** non-int = +0
		{math.Copysign(0, -1), -3.0}, // -0 ** -odd int = -Inf
		{math.Copysign(0, -1), -4.0}, // -0 ** -even int = +Inf
		{0.0, -2.0},
		{0.0, 2.5},
		// 15-17. +-INF as base
		{math.Inf(1), 2.0},
		{math.Inf(1), -2.0},
		{math.Inf(-1), 3.0},  // -inf ** odd = -inf
		{math.Inf(-1), 2.0},  // -inf ** even = +inf
		{math.Inf(-1), -3.0}, // -inf ** -odd = -0
		// 18. (-anything) ** integer
		{-2.0, 3.0},
		{-2.0, 4.0},
		{-2.0, -3.0},
		{-1.5, 7.0},
		// 19. (-anything) ** non-integer is NaN
		{-2.0, 0.5},
		{-2.0, 1.5},
		{-0.5, 0.3},

		// === Easy reference values ===
		{2.0, 0.5},  // sqrt(2)
		{2.0, 10.0}, // 1024
		{2.0, 53.0}, // 2^53
		{2.0, 1023.0},
		{2.0, 1024.0}, // overflow
		{2.0, -1075.0},
		{10.0, 100.0},
		{1.5, 10.0},
		{3.0, 3.0},
		{1.0, 1.0e300},
		{1.0, 1.0e-300},

		// === Boundary: |y| just above/below 2**31 threshold ===
		{1.0 + 1e-12, 2147483648.0},
		{1.0 - 1e-12, 2147483648.0},
		{1.0 + 1e-12, 2147483647.0},

		// === Boundary: x close to 1.0 ===
		{math.Nextafter(1.0, 2.0), 1.0e6},
		{math.Nextafter(1.0, 0.0), 1.0e6},
		{math.Nextafter(1.0, 2.0), -1.0e6},

		// === Boundary: overflow / underflow tail of the algorithm ===
		{2.0, 1024.0}, // exactly 2^1024 — overflow per IEEE
		{0.5, 1075.0}, // underflow
		{0.5, 1074.0},
		{2.0, -1074.0},
	}

	// Uniform sweep across binades for both base and exponent. The point
	// is to stress mantissa-bit handling without explosion in vector size.
	bases := []float64{
		0.5, 0.75, 0.875, 1.125, 1.25, 1.5, 1.75, 2.5, 3.0, 7.0, 13.0,
		0x1.0p-100, 0x1.0p100, 0x1.fffffp-100, 0x1.fffffp100,
		0x1.0000000000001p0, 0x1.fffffffffffffp-1,
	}
	exps := []float64{
		0.25, 0.5, 1.0, 2.0, 3.0, 7.0, 0.1, 0.01, -0.5, -1.5, 0.3333333333333333,
		17.0, 50.0, 200.0, -200.0,
	}
	for _, b := range bases {
		for _, e := range exps {
			pairs = append(pairs, pair{b, e})
		}
	}

	// Subnormal-result regime: 0.5 ** large exponent
	for n := 1; n <= 1050; n += 50 {
		pairs = append(pairs, pair{0.5, float64(n)})
	}

	// Negative-base integer sweep
	for n := -10; n <= 10; n++ {
		pairs = append(pairs, pair{-1.5, float64(n)})
	}

	w := os.Stdout
	for _, p := range pairs {
		fmt.Fprintf(w, "%s %s\n", fmtFloat(p.a), fmtFloat(p.b))
	}
}
