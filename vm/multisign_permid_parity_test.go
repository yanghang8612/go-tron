package vm

import (
	"testing"

	coretypes "github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// java ValidateMultiSign reads the permission-id word via words[1].intValueSafe()
// (PrecompiledContracts.java:1059) — saturating a >4-byte / negative word to
// Integer.MAX_VALUE — then both hashes ByteArray.fromInt(permissionId) into the
// SHA256 signed-hash and looks up account.getPermissionById(permissionId). gtron read
// it with parseInt64FromWord (raw low-64). For a permID word of 2^64 (low-64 == 0),
// gtron decoded permID=0 — the always-present owner permission — and validated; java
// saturates to MAX_INT, finds no such permission, and returns DATA_FALSE.
func TestValidateMultiSignPermIdSaturatesLikeJava(t *testing.T) {
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
	// Sign over the hash gtron's BUGGY low-64 decode would build (permID == 0).
	hash := hashForMultiSign(owner, 0, msgData)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}

	input := validateMultiSignInputN(owner, 0, msgData, [][]byte{sig})
	// Overwrite word[1] (permID) with 2^64: byte index 23 of the word == input[55].
	// low-64 bits stay 0; intValueSafe saturates to Integer.MAX_VALUE.
	input[55] = 0x01

	out, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500)
	if err != nil {
		t.Fatalf("unexpected vm error: %v", err)
	}
	if !success {
		t.Fatalf("want success=true (DATA_FALSE is a successful precompile return)")
	}
	if len(out) != 32 || out[31] != 0 {
		t.Fatalf("permID word 2^64 must DATA_FALSE (java intValueSafe -> MAX_INT -> no permission); got out=%x (buggy low-64 read 0 -> owner perm -> DATA_ONE)", out)
	}

	// Sanity: a clean permID=0 word still validates against the owner permission.
	clean := validateMultiSignInputN(owner, 0, msgData, [][]byte{sig})
	out2, _, success2, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, clean, 1500)
	if err != nil || !success2 || len(out2) != 32 || out2[31] != 1 {
		t.Fatalf("clean permID=0 must validate (DATA_ONE): success=%v out=%x err=%v", success2, out2, err)
	}
}
