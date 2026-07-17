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
