package vm

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// nullEVM is a minimal TVM suitable for testing precompiles that don't need state.
func nullEVM() *TVM {
	return &TVM{}
}

var zeroCaller tcommon.Address

func TestPrecompileECRecover(t *testing.T) {
	p := &ecRecover{}
	// Empty input → should return 32 zero bytes, no error.
	result, cost, err := p.Run(nullEVM(), zeroCaller, nil, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 3000 {
		t.Fatalf("expected cost 3000, got %d", cost)
	}
	if len(result) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(result))
	}
	// All zeros means recovery failed (which is correct for empty input).
	for _, b := range result {
		if b != 0 {
			t.Fatalf("expected zero result for empty input")
		}
	}
}

func TestPrecompileSHA256(t *testing.T) {
	p := &sha256hash{}
	input := []byte("hello world")
	expected := sha256.Sum256(input)

	result, cost, err := p.Run(nullEVM(), zeroCaller, input, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 72 {
		t.Fatalf("expected cost 72, got %d", cost)
	}
	if hex.EncodeToString(result) != hex.EncodeToString(expected[:]) {
		t.Fatalf("sha256 mismatch")
	}
}

// TestPrecompileTronRipemd160 verifies the TRON-specific 0x03 behaviour:
// SHA256(SHA256(input)[0:20]) — NOT standard RIPEMD-160.
func TestPrecompileTronRipemd160(t *testing.T) {
	p := &tronRipemd160{}
	input := []byte("hello world")

	// Expected: SHA256(SHA256(input)[0:20])
	first := sha256.Sum256(input)
	expected := sha256.Sum256(first[:20])

	result, cost, err := p.Run(nullEVM(), zeroCaller, input, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 720 {
		t.Fatalf("expected cost 720, got %d", cost)
	}
	if len(result) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(result))
	}
	if hex.EncodeToString(result) != hex.EncodeToString(expected[:]) {
		t.Fatalf("tronRipemd160 mismatch: got %s want %s",
			hex.EncodeToString(result), hex.EncodeToString(expected[:]))
	}
}

func TestPrecompileDataCopy(t *testing.T) {
	p := &dataCopy{}
	input := []byte{0x01, 0x02, 0x03, 0x04}

	result, cost, err := p.Run(nullEVM(), zeroCaller, input, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 18 {
		t.Fatalf("expected cost 18, got %d", cost)
	}
	if string(result) != string(input) {
		t.Fatalf("data copy mismatch")
	}
}

func TestPrecompileOutOfEnergy(t *testing.T) {
	p := &sha256hash{}
	_, _, err := p.Run(nullEVM(), zeroCaller, []byte("test"), 1)
	if err != ErrOutOfEnergy {
		t.Fatalf("expected ErrOutOfEnergy, got %v", err)
	}
}

func TestAddrFromUint(t *testing.T) {
	// ecRecover at 0x01 should produce 0x41 00…01
	addr := addrFromUint(0x01)
	if addr[0] != 0x41 {
		t.Fatalf("addr[0] should be 0x41, got 0x%02x", addr[0])
	}
	if addr[20] != 0x01 {
		t.Fatalf("addr[20] should be 0x01, got 0x%02x", addr[20])
	}
	for i := 1; i < 20; i++ {
		if addr[i] != 0x00 {
			t.Fatalf("addr[%d] should be 0x00, got 0x%02x", i, addr[i])
		}
	}

	// Voting precompile at 0x01000005
	addr2 := addrFromUint(0x01000005)
	if addr2[0] != 0x41 {
		t.Fatalf("prefix mismatch")
	}
	if addr2[17] != 0x01 || addr2[18] != 0x00 || addr2[19] != 0x00 || addr2[20] != 0x05 {
		t.Fatalf("tron system addr mismatch: %x", addr2[:])
	}

	// TVM compat Blake2F at 0x020009
	addr3 := addrFromUint(0x020009)
	if addr3[18] != 0x02 || addr3[19] != 0x00 || addr3[20] != 0x09 {
		t.Fatalf("compat addr mismatch: %x", addr3[:])
	}
}

func TestBigModExp(t *testing.T) {
	p := &bigModExp{istanbul: true}
	// 2^10 mod 11 = 1024 mod 11 = 1
	input := make([]byte, 96+3)
	input[31] = 1 // baseLen = 1
	input[63] = 1 // expLen = 1
	input[95] = 1 // modLen = 1
	input[96] = 2 // base = 2
	input[97] = 10 // exp = 10
	input[98] = 11 // mod = 11

	result, _, err := p.Run(nullEVM(), zeroCaller, input, 100000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0] != 1 {
		t.Fatalf("expected 2^10 mod 11 = 1, got %v", result)
	}
}

func TestTronAddrFromWord(t *testing.T) {
	word := make([]byte, 32)
	// Simulate Solidity address encoding: address in last 20 bytes
	word[12] = 0x12
	word[31] = 0x34
	addr := tronAddrFromWord(word)
	if addr[0] != 0x41 {
		t.Fatalf("expected 0x41 prefix, got 0x%02x", addr[0])
	}
	if addr[1] != 0x12 {
		t.Fatalf("expected addr[1]=0x12, got 0x%02x", addr[1])
	}
	if addr[20] != 0x34 {
		t.Fatalf("expected addr[20]=0x34, got 0x%02x", addr[20])
	}
}
