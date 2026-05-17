package actuator

import "math"

func validReadableBytes(b []byte, max int) bool {
	if len(b) == 0 || len(b) > max {
		return false
	}
	for _, c := range b {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

func validBytesLen(b []byte, max int, allowEmpty bool) bool {
	if len(b) == 0 {
		return allowEmpty
	}
	return len(b) <= max
}

func isNumericBytes(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func checkedAddInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}
