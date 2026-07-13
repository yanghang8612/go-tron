package vm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto"
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
	// Empty input → recovery fails. java ECRecover returns EMPTY_BYTE_ARRAY (0
	// bytes), not 32 zeros — the CALL returndata/output buffer and RETURNDATASIZE
	// must match. (gtron previously returned 32 zero bytes, diverging from java.)
	result, cost, err := p.Run(nullEVM(), zeroCaller, nil, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 3000 {
		t.Fatalf("expected cost 3000, got %d", cost)
	}
	if len(result) != 0 {
		t.Fatalf("failed recovery must return empty (java), got %d bytes", len(result))
	}
}

// TestPrecompileECRecoverJavaParity pins divergence findings against java's
// ECRecover.validateV + ECKey.validateComponents: a clean v word with v∈{27,28}
// recovers; raw recovery ids 0/1, a dirty high byte in the v word, and any
// failed recovery all return EMPTY (gtron previously accepted 0/1 and dirty
// high bytes, and returned 32 zeros on failure).
func TestPrecompileECRecoverJavaParity(t *testing.T) {
	p := &ecRecover{}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, 32)
	msg[31] = 0x42
	sig, err := crypto.Sign(msg, key) // 65-byte [r|s|v], v == recovery id in {0,1}
	if err != nil {
		t.Fatal(err)
	}
	recid := sig[64]

	vWord := func(v byte) []byte { w := make([]byte, 32); w[31] = v; return w }
	build := func(word []byte) []byte {
		in := make([]byte, 128)
		copy(in[0:32], msg)
		copy(in[32:64], word)
		copy(in[64:96], sig[0:32])
		copy(in[96:128], sig[32:64])
		return in
	}

	// Valid v (27/28) recovers a 32-byte (right-aligned) address.
	out, _, err := p.Run(nullEVM(), zeroCaller, build(vWord(27+recid)), 10000)
	if err != nil {
		t.Fatalf("valid recovery error: %v", err)
	}
	if len(out) != 32 {
		t.Fatalf("valid v∈{27,28} must recover 32 bytes, got %d", len(out))
	}

	// Raw recovery id 0/1: java validateComponents requires v∈{27,28} → EMPTY.
	if out, _, _ := p.Run(nullEVM(), zeroCaller, build(vWord(recid)), 10000); len(out) != 0 {
		t.Fatalf("raw recovery id v=%d must return empty (java validateComponents), got %x", recid, out)
	}

	// Dirty high byte in the v word: java validateV → EMPTY.
	dirty := vWord(27 + recid)
	dirty[0] = 0x01
	if out, _, _ := p.Run(nullEVM(), zeroCaller, build(dirty), 10000); len(out) != 0 {
		t.Fatalf("dirty v-word high byte must return empty (java validateV), got %x", out)
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

// TestKZGPointEvaluationNile55610290 pins the first transaction whose missing
// point-evaluation precompile accumulated the balance drift that surfaced at
// Nile block 55,611,077. The input is the exact 192-byte payload assembled by
// tx 3a3918db... from (versionedHash, z, y, commitment, proof).
func TestKZGPointEvaluationNile55610290(t *testing.T) {
	const inputHex = "01a327088bb2b13151449d8313c281d0006d12e8453e863637b746898b6ad5a6" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000010000000000000000000000000000000000000000000000000000000" +
		"8f26f349339c68b33ce856aa2c05b8f89e7c23db0c00817550679998efcbd8f2464f9e1ea6c3172b0b750603d1e4ea38" +
		"97d8c90897645ac9e31e8017981de0f9d0d5de4cec12899680ee4e810f4f7f56ac765e46a801f2f1046f8f305d33e27c"
	input, err := hex.DecodeString(inputHex)
	if err != nil {
		t.Fatal(err)
	}

	if p := getPrecompile(addrFromUint(0x02000a), TVMConfig{}, params.NileGenesisHash); p != nil {
		t.Fatal("point-evaluation precompile must be disabled before allow_tvm_blob")
	}
	p := getPrecompile(addrFromUint(0x02000a), TVMConfig{Blob: true}, params.NileGenesisHash)
	if p == nil {
		t.Fatal("point-evaluation precompile missing with allow_tvm_blob")
	}
	out, used, err := p.Run(nullEVM(), zeroCaller, input, kzgPointEvaluationCost)
	if err != nil {
		t.Fatalf("valid Nile proof: %v", err)
	}
	if used != kzgPointEvaluationCost {
		t.Fatalf("energy: got %d, want %d", used, kzgPointEvaluationCost)
	}
	want := "0000000000000000000000000000000000000000000000000000000000001000" +
		"73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001"
	if got := hex.EncodeToString(out); got != want {
		t.Fatalf("output: got %s, want %s", got, want)
	}

	// Exercise the production CALL dispatcher too. Before the fix this target
	// was treated as an empty ordinary account: it consumed no 50k precompile
	// energy, returned empty data, and emitted a bogus internal transaction.
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.Blob = true
	tvm.GenesisHash = params.NileGenesisHash
	caller := tcommon.Address{0x41, 0xA4}
	ret, remaining, err := tvm.Call(caller, addrFromUint(0x02000a), input, 60_000, 0)
	if err != nil {
		t.Fatalf("TVM.Call: %v", err)
	}
	if remaining != 10_000 {
		t.Fatalf("remaining energy: got %d, want 10000", remaining)
	}
	if got := hex.EncodeToString(ret); got != want {
		t.Fatalf("TVM.Call output: got %s, want %s", got, want)
	}
	if len(tvm.InternalTransactions) != 0 {
		t.Fatalf("precompile call emitted %d internal transactions, want 0", len(tvm.InternalTransactions))
	}
}

func TestKZGPointEvaluationAddressIsOrdinaryOnMainnet(t *testing.T) {
	addr := addrFromUint(0x02000a)
	mainnetGenesis := tcommon.HexToHash("00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc")
	if p := getPrecompile(addr, TVMConfig{Blob: true}, mainnetGenesis); p != nil {
		t.Fatal("0x02000a must not become a precompile on mainnet")
	}

	// Pin the runtime path too: with allow_tvm_blob enabled on mainnet, code
	// deployed at 0x02000a must execute as an ordinary smart contract.
	tvm, statedb, _ := newTestTVMWithDB(t)
	tvm.cfg.Blob = true
	tvm.GenesisHash = mainnetGenesis
	statedb.CreateAccount(addr, corepb.AccountType_Contract)
	statedb.SetCode(addr, []byte{
		byte(PUSH1), 0x2a,
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	})

	ret, _, err := tvm.Call(tcommon.Address{0x41, 0xa4}, addr, nil, 60_000, 0)
	if err != nil {
		t.Fatalf("ordinary mainnet contract call: %v", err)
	}
	if len(ret) != 32 || ret[31] != 0x2a {
		t.Fatalf("ordinary mainnet contract output: %x", ret)
	}
}

func TestKZGPointEvaluationFailureAndEnergy(t *testing.T) {
	p := &kzgPointEvaluation{}
	if ret, used, err := p.Run(nullEVM(), zeroCaller, make([]byte, kzgPointInputLength), kzgPointEvaluationCost-1); err != ErrOutOfEnergy || used != kzgPointEvaluationCost-1 || ret != nil {
		t.Fatalf("OOE: ret=%x used=%d err=%v", ret, used, err)
	}
	// java failures are Pair.of(false, DataWord.ZERO().getData()); the 32-byte
	// zero payload is memorySaved into the caller's out-region like the
	// shielded verify* failures.
	wantZero := make([]byte, 32)
	if ret, used, success, err := p.RunWithStatus(nullEVM(), zeroCaller, make([]byte, kzgPointInputLength-1), kzgPointEvaluationCost); err != nil || success || used != kzgPointEvaluationCost || !bytes.Equal(ret, wantZero) {
		t.Fatalf("invalid length: ret=%x used=%d success=%v err=%v", ret, used, success, err)
	}
	if ret, used, success, err := p.RunWithStatus(nullEVM(), zeroCaller, make([]byte, kzgPointInputLength), kzgPointEvaluationCost); err != nil || success || used != kzgPointEvaluationCost || !bytes.Equal(ret, wantZero) {
		t.Fatalf("mismatched hash: ret=%x used=%d success=%v err=%v", ret, used, success, err)
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
			p := getPrecompile(addrFromUint(tc.addr), TVMConfig{ShieldedToken: true}, tcommon.Hash{})
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
	if p := getPrecompile(addr, TVMConfig{}, tcommon.Hash{}); p != nil {
		t.Fatalf("P256VERIFY should be disabled before Osaka, got %T", p)
	}
	p := getPrecompile(addr, TVMConfig{Osaka: true}, tcommon.Hash{})
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

	// java PrecompiledContracts.ModExp (lines 711-715): zero modulus returns
	// EMPTY_BYTE_ARRAY pre-Osaka, but new byte[modLen] (modLen zero-bytes) post-Osaka
	// (TIP-7883). modLen == 1 here.
	for _, tc := range []struct {
		name    string
		p       *bigModExp
		energy  uint64
		wantLen int
	}{
		{name: "legacy", p: &bigModExp{istanbul: true}, energy: 100000, wantLen: 0},
		{name: "osaka", p: &bigModExp{osaka: true}, energy: 500, wantLen: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, _, err := tc.p.Run(nullEVM(), zeroCaller, input, tc.energy)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(result) != tc.wantLen {
				t.Fatalf("zero modulus payload length: got %d (%x), want %d", len(result), result, tc.wantLen)
			}
			for i, b := range result {
				if b != 0 {
					t.Fatalf("zero modulus payload must be all zero bytes, byte %d = %#x", i, b)
				}
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
	// Pre-create the precompile's account so the endowment passes java's
	// validateForSmartContract (a missing account would now fail earlier
	// with "transfer failure"); this test pins the EXECUTION-failure path.
	target := addrFromUint(0x05)
	tvm.StateDB.CreateAccount(target, corepb.AccountType_Normal)

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
