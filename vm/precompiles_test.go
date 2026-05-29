package vm

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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

func TestShieldedPrecompilesReturnJavaFailurePayload(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr uint64
		cost uint64
		want int
	}{
		{name: "mint", addr: 0x01000001, cost: 150000, want: 32},
		{name: "transfer", addr: 0x01000002, cost: 200000, want: 32},
		{name: "burn", addr: 0x01000003, cost: 150000, want: 32},
		{name: "merkle", addr: 0x01000004, cost: 500, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := getPrecompile(addrFromUint(tc.addr), TVMConfig{ShieldedToken: true})
			if p == nil {
				t.Fatal("expected shielded precompile")
			}
			out, cost, err := p.Run(nullEVM(), zeroCaller, nil, tc.cost)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cost != tc.cost {
				t.Fatalf("cost: got %d, want %d", cost, tc.cost)
			}
			if len(out) != tc.want {
				t.Fatalf("output length: got %d, want %d", len(out), tc.want)
			}
			for _, b := range out {
				if b != 0 {
					t.Fatalf("expected zero failure payload, got %x", out)
				}
			}
		})
	}
}

func TestShieldedPrecompileOutOfEnergy(t *testing.T) {
	p := &verifyTransferProof{}
	_, _, err := p.Run(nullEVM(), zeroCaller, nil, 199999)
	if err != ErrOutOfEnergy {
		t.Fatalf("expected ErrOutOfEnergy, got %v", err)
	}
}

func TestShieldedFrontierSlotMatchesJavaPattern(t *testing.T) {
	tests := map[uint64]int{
		0:             0,
		1:             1,
		2:             0,
		3:             2,
		4:             0,
		5:             1,
		7:             3,
		15:            4,
		(1 << 32) - 2: 0,
		(1 << 32) - 1: 32,
	}
	for leafIndex, want := range tests {
		if got := shieldedFrontierSlot(leafIndex); got != want {
			t.Fatalf("slot(%d): got %d, want %d", leafIndex, got, want)
		}
	}
}

func TestShieldedTRC20TrustedNileReplayFallback(t *testing.T) {
	input := make([]byte, 512)
	trusted := &TVM{
		TrustTransactionRet: true,
		ExpectedContractRet: corepb.Transaction_Result_SUCCESS,
		GenesisHash:         params.NileGenesisHash,
		BlockNumber:         shieldedTRC20NileActivationBlock,
	}
	out, cost, err := (&verifyBurnProof{}).Run(trusted, zeroCaller, input, 150000)
	if err != nil {
		t.Fatalf("trusted burn fallback returned error: %v", err)
	}
	if cost != 150000 {
		t.Fatalf("cost: got %d, want 150000", cost)
	}
	if len(out) != 32 || out[31] != 1 {
		t.Fatalf("trusted fallback output = %x, want true payload", out)
	}

	untrusted := nullEVM()
	out, _, err = (&verifyBurnProof{}).Run(untrusted, zeroCaller, input, 150000)
	if err != nil {
		t.Fatalf("untrusted burn returned error: %v", err)
	}
	for _, b := range out {
		if b != 0 {
			t.Fatalf("untrusted fallback must stay disabled, got %x", out)
		}
	}
}

func TestP256VerifyPrecompileOsakaGateAndValidVector(t *testing.T) {
	addr := addrFromUint(0x0100)
	if p := getPrecompile(addr, TVMConfig{}); p != nil {
		t.Fatalf("P256VERIFY should be disabled before Osaka, got %T", p)
	}
	p := getPrecompile(addr, TVMConfig{Osaka: true})
	if p == nil {
		t.Fatal("expected P256VERIFY when Osaka is active")
	}
	input, err := hex.DecodeString(
		"4cee90eb86eaa050036147a12d49004b6b9c72bd725d39d4785011fe190f0b4d" +
			"a73bd4903f0ce3b639bbbf6e8e80d16931ff4bcf5993d58468e8fb19086e8cac" +
			"36dbcd03009df8c59286b162af3bd7fcc0450c9aa81be5d10d312af6c66b1d60" +
			"4aebd3099c618202fcfe16ae7770b0c49ab5eadf74b754204a3bb6060e44eff3" +
			"7618b065f9832de4ca6ca971a7a1adc826d0f7c00181a5fb2ddf79ae00b4e10e")
	if err != nil {
		t.Fatal(err)
	}
	out, cost, err := p.Run(nullEVM(), zeroCaller, input, 6900)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 6900 {
		t.Fatalf("cost: got %d, want 6900", cost)
	}
	if len(out) != 32 || out[31] != 1 {
		t.Fatalf("valid P256 vector output mismatch: %x", out)
	}
	for i, b := range out[:31] {
		if b != 0 {
			t.Fatalf("valid P256 vector output byte %d: got %x", i, b)
		}
	}
}

func TestP256VerifyPrecompileInvalidAndOutOfEnergy(t *testing.T) {
	p := &p256Verify{}
	out, cost, err := p.Run(nullEVM(), zeroCaller, []byte{1, 2, 3}, 6900)
	if err != nil {
		t.Fatalf("unexpected error for invalid input length: %v", err)
	}
	if cost != 6900 {
		t.Fatalf("cost: got %d, want 6900", cost)
	}
	if len(out) != 0 {
		t.Fatalf("invalid input should return empty payload, got %x", out)
	}
	if _, _, err := p.Run(nullEVM(), zeroCaller, nil, 6899); err != ErrOutOfEnergy {
		t.Fatalf("expected ErrOutOfEnergy, got %v", err)
	}
}

func TestBigModExp(t *testing.T) {
	p := &bigModExp{istanbul: true}
	// 2^10 mod 11 = 1024 mod 11 = 1
	input := make([]byte, 96+3)
	input[31] = 1  // baseLen = 1
	input[63] = 1  // expLen = 1
	input[95] = 1  // modLen = 1
	input[96] = 2  // base = 2
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

func TestBigModExpZeroModulusMatchesJavaTron(t *testing.T) {
	input := make([]byte, 96+3)
	input[31] = 1 // baseLen = 1
	input[63] = 1 // expLen = 1
	input[95] = 1 // modLen = 1
	input[96] = 2 // base = 2
	input[97] = 1 // exp = 1
	input[98] = 0 // mod = 0

	for _, tc := range []struct {
		name   string
		p      *bigModExp
		energy uint64
	}{
		{name: "legacy", p: &bigModExp{istanbul: true}, energy: 100000},
		{name: "osaka", p: &bigModExp{osaka: true}, energy: 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, _, err := tc.p.Run(nullEVM(), zeroCaller, input, tc.energy)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != 0 {
				t.Fatalf("zero modulus should return empty payload like java-tron, got %x", result)
			}
		})
	}
}

func TestBigModExpOsakaTIP7883MinimumCost(t *testing.T) {
	p := &bigModExp{osaka: true}
	input := make([]byte, 96+3)
	input[31] = 1 // baseLen = 1
	input[63] = 1 // expLen = 1
	input[95] = 1 // modLen = 1
	input[96] = 2 // base = 2
	input[97] = 0 // exp = 0
	input[98] = 5 // mod = 5

	result, cost, err := p.Run(nullEVM(), zeroCaller, input, 500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 500 {
		t.Fatalf("Osaka ModExp minimum cost: got %d, want 500", cost)
	}
	if len(result) != 1 || result[0] != 1 {
		t.Fatalf("2^0 mod 5 should be 1, got %x", result)
	}
}

func TestBigModExpOsakaRejectsOversizedLengths(t *testing.T) {
	p := &bigModExp{osaka: true}
	input := make([]byte, 96)
	input[30] = 0x04
	input[31] = 0x01 // baseLen = 1025
	input[63] = 1    // expLen = 1
	input[95] = 1    // modLen = 1

	result, _, err := p.Run(nullEVM(), zeroCaller, input, 100000)
	if err != nil {
		t.Fatalf("oversized Osaka ModExp should not raise a Go error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("oversized Osaka ModExp should return empty payload, got %x", result)
	}
}

func TestPrecompileFailureStatusConsumesEnergyAndRevertsValue(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.Osaka = true

	caller := tcommon.Address{0x41, 0x91}
	tvm.StateDB.CreateAccount(caller, corepb.AccountType_Normal)
	tvm.StateDB.AddBalance(caller, 1_000)
	target := addrFromUint(0x05)

	input := make([]byte, 96)
	input[30] = 0x04
	input[31] = 0x01 // baseLen = 1025, invalid once Osaka is active.
	input[63] = 1
	input[95] = 1

	ret, remaining, err := tvm.Call(caller, target, input, 100_000, 100)
	if err != errPrecompileFailure {
		t.Fatalf("precompile failure err: got %v, want errPrecompileFailure", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining energy: got %d, want 0", remaining)
	}
	if len(ret) != 0 {
		t.Fatalf("failure payload: got %x, want empty", ret)
	}
	if got := tvm.StateDB.GetBalance(caller); got != 1_000 {
		t.Fatalf("caller balance should be reverted, got %d", got)
	}
	if got := tvm.StateDB.GetBalance(target); got != 0 {
		t.Fatalf("precompile target balance should be reverted, got %d", got)
	}
}

func TestPrecompileFailurePayloadForBlake2F(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.Compatibility = true

	ret, remaining, err := tvm.Call(tcommon.Address{0x41, 0x92}, addrFromUint(0x020009), []byte{1, 2, 3}, 1_000, 0)
	if err != errPrecompileFailure {
		t.Fatalf("precompile failure err: got %v, want errPrecompileFailure", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining energy: got %d, want 0", remaining)
	}
	if len(ret) != 32 {
		t.Fatalf("Blake2F failure payload length: got %d, want 32", len(ret))
	}
	for i, b := range ret {
		if b != 0 {
			t.Fatalf("Blake2F failure payload byte %d: got %x, want 0", i, b)
		}
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
