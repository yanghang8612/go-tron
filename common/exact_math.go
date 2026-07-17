package common

import (
	"fmt"
	"math/big"
)

// ArithmeticOverflowError mirrors java.math.BigInteger.longValueExact's
// unchecked ArithmeticException. Consensus callers recover only this concrete
// type at transaction/block boundaries; unrelated panics remain fatal.
type ArithmeticOverflowError struct {
	Operation string
}

func (e *ArithmeticOverflowError) Error() string {
	return fmt.Sprintf("arithmetic overflow in %s: result does not fit int64", e.Operation)
}

// BigInt64Exact returns v as int64 or panics with ArithmeticOverflowError,
// matching BigInteger.longValueExact rather than big.Int.Int64's truncation.
func BigInt64Exact(v *big.Int, operation string) int64 {
	if v == nil || !v.IsInt64() {
		panic(&ArithmeticOverflowError{Operation: operation})
	}
	return v.Int64()
}

// ArithmeticOverflowFromPanic converts only the exact arithmetic panic above.
func ArithmeticOverflowFromPanic(v any) (error, bool) {
	err, ok := v.(*ArithmeticOverflowError)
	return err, ok
}
