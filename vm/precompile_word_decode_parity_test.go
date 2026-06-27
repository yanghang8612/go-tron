package vm

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// java PrecompiledContracts.parseLen decodes each ModExp length word via
// `new DataWord(bytes).intValueSafe()` (saturate to Integer.MAX_VALUE when the word
// occupies >4 bytes or its low-32 is negative). gtron read them with big.Int.Uint64()
// (low-64 bits). For the degenerate baseLen==0 && modLen==0 input with an expLen word
// of 2^64 (low-64 == 0), gtron saw expLen=0 and skipped the VERSION_4_8_1_1 OutOfTime
// guard (`expLen > UPPER_BOUND`), cheaply succeeding, where java saturates expLen to
// Integer.MAX_VALUE > 1024 and aborts with OutOfTime + spendAll.
func TestModExpLengthWordSaturatesLikeJava(t *testing.T) {
	c := &bigModExp{cpuTimeGuard: true}

	// baseLen=0, modLen=0, expLen word = 2^64 (byte index 55 set; low-64 bits == 0).
	input := make([]byte, 96)
	input[55] = 0x01

	if _, _, err := c.Run(nil, tcommon.Address{}, input, 100_000_000); !errors.Is(err, ErrAlreadyTimeOut) {
		t.Fatalf("ModExp expLen=2^64 (baseLen=modLen=0): got err=%v, want ErrAlreadyTimeOut (java intValueSafe -> expLen=MAX_INT > 1024; .Uint64() truncated to 0 -> cheap success)", err)
	}

	// Sanity: a genuine small expLen=2048 still trips the same guard.
	small := make([]byte, 96)
	small[32+30] = 0x08 // expLen = 0x0800 = 2048
	if _, _, err := c.Run(nil, tcommon.Address{}, small, 100_000_000); !errors.Is(err, ErrAlreadyTimeOut) {
		t.Fatalf("ModExp expLen=2048 guard: got err=%v, want ErrAlreadyTimeOut", err)
	}

	// Sanity: expLen=1024 (== UPPER_BOUND, not >) is NOT a timeout.
	ok := make([]byte, 96)
	ok[32+30] = 0x04 // expLen = 0x0400 = 1024
	if _, _, err := c.Run(nil, tcommon.Address{}, ok, 100_000_000); err != nil {
		t.Fatalf("ModExp expLen=1024 should succeed (not > UPPER_BOUND): got err=%v", err)
	}
}
