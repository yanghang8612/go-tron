package vm

import (
	"crypto/sha256"
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TestValidateMultiSignShortInputRaisesUnknown pins divergence #5: java
// PrecompiledContracts.ValidateMultiSign reads words[0..3] (DataWord.parseArray)
// BEFORE its inner try/catch, so an input shorter than 4 words (128 bytes) throws
// an uncaught ArrayIndexOutOfBoundsException → VM.java spendAllEnergy + rethrow →
// RuntimeImpl.setResultCode falls through to contractResult.UNKNOWN(13). gtron
// previously returned DATA_FALSE+success — a latent fork. The fix surfaces
// ErrPrecompileUnknown (maps to UNKNOWN(13) via contractRetFromError's default and
// propagates from sub-calls so the whole tx fails, mirroring java).
func TestValidateMultiSignShortInputRaisesUnknown(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	for _, n := range []int{0, 1, 32, 64, 96, 127} {
		_, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, make([]byte, n), 1500)
		if !errors.Is(err, ErrPrecompileUnknown) {
			t.Fatalf("len=%d: err = %v, want ErrPrecompileUnknown (java UNKNOWN path)", n, err)
		}
		if success {
			t.Fatalf("len=%d: success = true, want false (uncaught exception)", n)
		}
	}
	// Exactly 4 words (128 bytes) clears the words[0..3] access — must NOT raise.
	if _, _, _, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, make([]byte, 128), 1500); errors.Is(err, ErrPrecompileUnknown) {
		t.Fatalf("len=128 (4 words) must not raise the short-input exception, got %v", err)
	}
	// java fails the whole tx (the AIOOBE is not contained at the CALL boundary).
	if !shouldPropagateCallError(ErrPrecompileUnknown) {
		t.Fatal("ErrPrecompileUnknown must propagate from sub-calls")
	}
}

func validateMultiSignInput(owner tcommon.Address, permID int64, msgData, sig []byte) []byte {
	input := make([]byte, 10*32)
	copy(input[0:32], stakingAddrWord(owner))
	copy(input[32:64], int64ToBytes32(permID))
	copy(input[64:96], msgData)
	copy(input[96:128], int64ToBytes32(5*32))
	copy(input[5*32:6*32], int64ToBytes32(1))
	// Selfdestruct-restriction layout: offset points directly to a fixed
	// 65-byte signature slot; there is no per-element bytes length word.
	copy(input[6*32:7*32], int64ToBytes32(0))
	copy(input[7*32:], sig)
	return input
}

func TestValidateMultiSignSelfdestructRestrictionUsesFixed65SignatureSlots(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	owner := crypto.PubkeyToAddress(&key.PublicKey)
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	tvm.StateDB.SetPermissions(owner, coretypes.MakeDefaultOwnerPermission(owner), nil, nil)

	msgData := make([]byte, 32)
	msgData[31] = 0x7b
	var combine [21 + 4 + 32]byte
	copy(combine[0:21], owner[:])
	copy(combine[25:57], msgData)
	hash := sha256.Sum256(combine[:])
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}

	input := validateMultiSignInput(owner, 0, msgData, sig)
	out, _, err := (&validateMultiSign{}).Run(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("validateMultiSign error: %v", err)
	}
	if len(out) != 32 || out[31] != 1 {
		t.Fatalf("fixed65 signature slot should validate, got %x", out)
	}

	tvm.cfg.SelfdestructRestrict = false
	out, _, err = (&validateMultiSign{}).Run(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("legacy validateMultiSign error: %v", err)
	}
	if len(out) != 32 || out[31] != 0 {
		t.Fatalf("legacy bytes[] parser should reject fixed65 layout, got %x", out)
	}
}

func TestSignaturePrecompilesOsakaRejectInvalidABIShape(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.Osaka = true

	out, _, err := (&validateMultiSign{}).Run(tvm, zeroCaller, make([]byte, 4*32), 0)
	if err != nil {
		t.Fatalf("validateMultiSign invalid ABI error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("validateMultiSign invalid Osaka ABI should return empty payload, got %x", out)
	}

	out, _, err = (&batchValidateSign{}).Run(tvm, zeroCaller, make([]byte, 4*32), 0)
	if err != nil {
		t.Fatalf("batchValidateSign invalid ABI error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("batchValidateSign invalid Osaka ABI should return empty payload, got %x", out)
	}
}
