package vm

import (
	"testing"

	"github.com/holiman/uint256"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// java V1 freeze/unfreeze/freezeExpireTime decode the resourceType word via
// DataWord.intValue() (Program.parseResourceCode: `switch (resourceType.intValue())`,
// Program.java:2226; freezeExpireTime: `int resourceCode = resourceType.intValue()`)
// — the LOW 32 bits as a signed int (truncating/wrapping, NOT the saturating
// intValueSafe used by the V2 staking opcodes). gtron read `int64(resourceWord.Uint64())`
// (low-64), so a word like 2^32 (low-32 == 0) decoded to a huge value and was rejected
// (push 0, no state change) where java reads BANDWIDTH(0) and freezes (push 1). A
// from-genesis Nile re-sync over the V1-freeze window (allow_tvm_freeze ON,
// FreezeV2/Vote OFF) would diverge.
func TestFreezeV1ResourceTypeTruncatesLowWordLikeJava(t *testing.T) {
	tvm, statedb, _ := newFreezeV1TVM(t)
	owner := freezeAmtAddr(0x07)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100*tvmTRXPrecision)

	// resourceType = 2^32: low-32 == 0 -> java intValue() -> BANDWIDTH(0).
	stack := newStack()
	recv := addressWord(owner)
	stack.push(&recv)
	stack.push(uint256.NewInt(uint64(10 * tvmTRXPrecision))) // amount
	stack.push(pow2(32))                                     // resourceType (popped first)
	contract := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opFreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opFreeze: %v", err)
	}
	ret := stack.pop()
	if ret.Uint64() != 1 {
		t.Fatalf("freeze resourceType 2^32 must decode to BANDWIDTH(0) via java intValue() low-32 and succeed; got %d (go int64(low-64) wrongly rejected)", ret.Uint64())
	}
	if got := statedb.GetBalance(owner); got != 90*tvmTRXPrecision {
		t.Fatalf("BANDWIDTH freeze must reduce balance: got %d, want %d", got, 90*tvmTRXPrecision)
	}

	// A word whose low-32 is a high-bit value (e.g. 0x80000000) is NEGATIVE as a
	// signed int32 -> UNRECOGNIZED -> rejected (matches java's signed intValue()).
	stack2 := newStack()
	recv2 := addressWord(owner)
	stack2.push(&recv2)
	stack2.push(uint256.NewInt(uint64(10 * tvmTRXPrecision)))
	stack2.push(uint256.NewInt(0x80000000)) // low-32 high bit -> negative int32
	contract2 := NewContract(owner, owner, 0, 5_000_000)
	if _, err := opFreeze(nil, tvm.interpreter, contract2, nil, stack2); err != nil {
		t.Fatalf("opFreeze (neg): %v", err)
	}
	ret2 := stack2.pop()
	if ret2.Uint64() != 0 {
		t.Fatalf("resourceType 0x80000000 is negative int32 -> UNRECOGNIZED -> reject; got %d", ret2.Uint64())
	}

	// Sanity: clean ENERGY(1) still freezes.
	if got := callFreeze(t, tvm, owner, owner, uint256.NewInt(uint64(10*tvmTRXPrecision)), 1); got != 1 {
		t.Fatalf("clean ENERGY freeze: got %d, want 1", got)
	}
	_ = corepb.AccountType_Normal
}
