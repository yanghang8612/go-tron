package common

import (
	"math"
	"math/big"
	"testing"
)

func TestBigInt64Exact(t *testing.T) {
	if got := BigInt64Exact(big.NewInt(math.MaxInt64), "test"); got != math.MaxInt64 {
		t.Fatalf("got %d, want MaxInt64", got)
	}
	defer func() {
		recovered := recover()
		if _, ok := ArithmeticOverflowFromPanic(recovered); !ok {
			t.Fatalf("got panic %T, want ArithmeticOverflowError", recovered)
		}
	}()
	BigInt64Exact(new(big.Int).Add(big.NewInt(math.MaxInt64), big.NewInt(1)), "test overflow")
}

func TestJavaDoubleToInt64(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   float64
		want int64
	}{
		{name: "positive", in: 123.9, want: 123},
		{name: "negative", in: -123.9, want: -123},
		{name: "nan", in: math.NaN(), want: 0},
		{name: "positive infinity", in: math.Inf(1), want: math.MaxInt64},
		{name: "negative infinity", in: math.Inf(-1), want: math.MinInt64},
		{name: "positive endpoint", in: math.Ldexp(1, 63), want: math.MaxInt64},
		{name: "negative endpoint", in: -math.Ldexp(1, 63), want: math.MinInt64},
		{name: "largest in range", in: math.Nextafter(math.Ldexp(1, 63), 0), want: math.MaxInt64 - 1023},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := JavaDoubleToInt64(tc.in); got != tc.want {
				t.Fatalf("JavaDoubleToInt64(%v): got %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestJavaMathRoundToInt64(t *testing.T) {
	tests := []struct {
		in   float64
		want int64
	}{
		{math.NaN(), 0},
		{math.Inf(1), math.MaxInt64},
		{math.Inf(-1), math.MinInt64},
		{1.49, 1},
		{1.5, 2},
		{-1.49, -1},
		{-1.5, -1},
		{-1.51, -2},
	}
	for _, tt := range tests {
		if got := JavaMathRoundToInt64(tt.in); got != tt.want {
			t.Fatalf("JavaMathRoundToInt64(%v): got %d, want %d", tt.in, got, tt.want)
		}
	}
}
