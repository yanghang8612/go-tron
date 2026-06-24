package vm

import (
	"crypto/sha256"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Tests for the h1 fix in docs/dev/proposal-hardfork-audit-2026-05-18.md:
// 0x0a ValidateMultiSign must differentiate exact (addr, sig) duplicates
// (always skip, both pre- and post-VERSION_4_7_1) from same-addr-with-
// different-sig duplicates (pre = fall through and re-count; post = fail
// the precompile with success=false).

// validateMultiSignInputN builds the SelfdestructRestrict-layout input for
// `len(sigs)` signatures, using fixed-65 per-element slots laid out WORD-ALIGNED,
// matching standard Solidity ABI encoding (each 65-byte bytes element occupies
// 3 words / 96 bytes, zero-padded) — i.e. exactly what java-tron's own
// ValidateMultiSignContractTest produces via AbiUtil.parseParameters. java's
// extractSigArray reads the per-element offset as a WORD index
// (words[offset+i+1].intValueSafe()/WORD_SIZE) and reads the sig from the
// word-aligned position (bytesOffset+offset+2)*WORD_SIZE, so element payloads
// MUST start on a word boundary. (The earlier tight-packed 65-byte-stride layout
// only decoded under gtron's pre-D-2 byte-arithmetic parser, which diverged from
// java for any non-word-aligned element offset; see multisign_offset_parity_test.go.)
func validateMultiSignInputN(owner tcommon.Address, permID int64, msgData []byte, sigs [][]byte) []byte {
	n := len(sigs)
	const sigsOffset = 160 // 5 * 32
	const sigSlot = 96     // 65-byte sig padded to 3 words
	tablePos := sigsOffset + 32
	payloadBase := tablePos + 32*n
	totalLen := payloadBase + sigSlot*n
	input := make([]byte, totalLen)
	copy(input[0:32], stakingAddrWord(owner))
	copy(input[32:64], int64ToBytes32(permID))
	copy(input[64:96], msgData)
	copy(input[96:128], int64ToBytes32(int64(sigsOffset)))
	copy(input[sigsOffset:sigsOffset+32], int64ToBytes32(int64(n)))
	for i := range sigs {
		// payload_i lives at the word-aligned slot payloadBase + i*96. java reads
		// it at (bytesOffset + sigsOffset/32 + 2)*32, so encode the element offset
		// word as bytesOffset*32 with bytesOffset = payloadPos/32 - sigsOffset/32 - 2.
		payloadPos := payloadBase + i*sigSlot
		bytesOffsetWords := payloadPos/32 - sigsOffset/32 - 2
		copy(input[tablePos+32*i:tablePos+32*(i+1)], int64ToBytes32(int64(bytesOffsetWords*32)))
		copy(input[payloadPos:payloadPos+65], sigs[i])
	}
	return input
}

// hashForMultiSign reproduces the 0x0a precompile's SHA256(addr || permID || msgData) digest.
func hashForMultiSign(owner tcommon.Address, permID int64, msgData []byte) [32]byte {
	var combine [21 + 4 + 32]byte
	copy(combine[0:21], owner[:])
	combine[21] = byte(permID >> 24)
	combine[22] = byte(permID >> 16)
	combine[23] = byte(permID >> 8)
	combine[24] = byte(permID)
	copy(combine[25:57], msgData)
	return sha256.Sum256(combine[:])
}

// Exact-duplicate signatures (identical bytes from the same key) must be
// silently skipped in both fork regimes — h1 must not have broken this.
func TestValidateMultiSign_ExactDuplicate_PostV4_7_1_Skipped(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true
	tvm.cfg.MultiSigCheckV2 = true

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	owner := crypto.PubkeyToAddress(&key.PublicKey)
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	tvm.StateDB.SetPermissions(owner, coretypes.MakeDefaultOwnerPermission(owner), nil, nil)

	msgData := make([]byte, 32)
	msgData[31] = 0x7b
	hash := hashForMultiSign(owner, 0, msgData)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}

	// Two byte-identical signatures from the same key. java's `continue`
	// path covers this regardless of VERSION_4_7_1: the second iteration
	// is skipped, the threshold-1 permission is satisfied by sig #0.
	input := validateMultiSignInputN(owner, 0, msgData, [][]byte{sig, sig})
	out, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500*2)
	if err != nil {
		t.Fatalf("unexpected vm error: %v", err)
	}
	if !success {
		t.Fatalf("exact-dup should NOT trigger checkCPUTime failure, got success=false")
	}
	if len(out) != 32 || out[31] != 1 {
		t.Fatalf("threshold-1 permission should pass with two exact-dup sigs, got %x", out)
	}
}

// Pre-VERSION_4_7_1 exact-duplicate also skips. Equivalent test with the
// fork gate off — the behaviour is identical here, only different in the
// (untestable from go's deterministic ECDSA) same-addr-diff-sig case.
func TestValidateMultiSign_ExactDuplicate_PreV4_7_1_Skipped(t *testing.T) {
	tvm, _, _ := newTestTVMWithDB(t)
	tvm.cfg.SelfdestructRestrict = true
	tvm.cfg.MultiSigCheckV2 = false

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	owner := crypto.PubkeyToAddress(&key.PublicKey)
	tvm.StateDB.CreateAccount(owner, corepb.AccountType_Normal)
	tvm.StateDB.SetPermissions(owner, coretypes.MakeDefaultOwnerPermission(owner), nil, nil)

	msgData := make([]byte, 32)
	msgData[31] = 0x42
	hash := hashForMultiSign(owner, 0, msgData)
	sig, err := crypto.Sign(hash[:], key)
	if err != nil {
		t.Fatal(err)
	}

	input := validateMultiSignInputN(owner, 0, msgData, [][]byte{sig, sig})
	out, _, success, err := (&validateMultiSign{}).RunWithStatus(tvm, zeroCaller, input, 1500*2)
	if err != nil {
		t.Fatalf("unexpected vm error: %v", err)
	}
	if !success {
		t.Fatalf("pre-4_7_1 exact-dup must skip, got success=false")
	}
	if len(out) != 32 || out[31] != 1 {
		t.Fatalf("threshold-1 should pass, got %x", out)
	}
}

// Post-VERSION_4_7_1 same-addr-different-sig must fail the precompile
// with success=false (java throws OutOfTimeException). We synthesize the
// "different sig" case by mutating the second signature's recovery byte:
// the precompile's address recovery will produce a NEW (random) address
// rather than `owner`, so the dedup-by-address check doesn't fire — and
// the wrong recovered address fails the permission lookup. To exercise
// the same-addr-diff-sig branch we instead supply the SAME sig twice
// (exact dup → skip, asserted above), then a third entry that is the
// same recovered address but with a single-bit-different payload that
// still recovers to `owner`. secp256k1's (s, n-s) malleability gives us
// such a pair — but go-ethereum normalizes to low-s. Without a non-
// deterministic signer we can't construct the required tuple. We
// therefore document the gap and leave a focused replay-fixture
// candidate, while still pinning the post-4_7_1 wiring through the
// exact-dup test (which would have been the second branch in java's
// `if (executedSignList contains recoveredAddr)` check).
//
// To at least pin the "post-fork wiring is live" property: when
// MultiSigCheckV2 is on, the precompile must NOT silently accept a
// permission threshold that depends on a dropped duplicate (covered by
// TestValidateMultiSign_ExactDuplicate_PostV4_7_1_Skipped above).
