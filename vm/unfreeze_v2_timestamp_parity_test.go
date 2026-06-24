package vm

import (
	"testing"

	"github.com/holiman/uint256"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// F-3: the Stake-2.0 expiry/withdraw opcodes must decide expiry on
// DynProps.LatestBlockHeaderTimestamp() — the java getLatestBlockHeaderTimestamp
// source (WithdrawExpireUnfreezeProcessor / UnfreezeBalanceV2Processor) — NOT on
// tvm.Timestamp (the current block's TIMESTAMP-opcode value). The two differ by
// a block interval; at an expiry boundary the wrong source flips withdraw vs
// keep, a consensus divergence. Each test sets the two timestamps to straddle an
// entry so only the header-timestamp source passes.

// TestWithdrawExpireUnfreezeOpcodeUsesHeaderTimestamp locks WITHDRAWEXPIREUNFREEZE (0xDD).
func TestWithdrawExpireUnfreezeOpcodeUsesHeaderTimestamp(t *testing.T) {
	tvm, statedb, dp := newStakeParityTVM(t)
	owner := stakeAddr(0x41)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 5*tvmTRXPrecision)

	const headerNow = int64(1_000_000)
	dp.SetLatestBlockHeaderTimestamp(headerNow)
	tvm.Timestamp = headerNow + 100_000 // later: a buggy now=tvm.Timestamp treats the entry as expired

	// Entry expires at headerNow+1: unexpired under the header timestamp,
	// "expired" under the (wrong) tvm.Timestamp source.
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision, headerNow+1)

	stack := newStack()
	contract := NewContract(owner, owner, 0, 1_000_000)
	if _, err := opWithdrawExpireUnfreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opWithdrawExpireUnfreeze error: %v", err)
	}
	released := stack.pop()
	if got := released.Uint64(); got != 0 {
		t.Fatalf("released: got %d, want 0 (entry unexpired under header timestamp)", got)
	}
	if got := statedb.GetBalance(owner); got != 5*tvmTRXPrecision {
		t.Fatalf("balance: got %d, want %d (nothing withdrawn)", got, 5*tvmTRXPrecision)
	}
	if got := statedb.UnfreezeV2Count(owner); got != 1 {
		t.Fatalf("unfreeze entry wrongly removed: count %d, want 1", got)
	}
}

// TestUnfreezeBalanceV2OpcodeUsesHeaderTimestamp locks UNFREEZEBALANCEV2 (0xDB):
// both the expired-withdrawal sweep and the new entry's expire time derive from
// the header timestamp.
func TestUnfreezeBalanceV2OpcodeUsesHeaderTimestamp(t *testing.T) {
	tvm, statedb, dp := newStakeParityTVM(t)
	owner := stakeAddr(0x42)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 5*tvmTRXPrecision)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_ENERGY, 300*tvmTRXPrecision)

	const headerNow = int64(1_000_000)
	dp.SetLatestBlockHeaderTimestamp(headerNow)
	tvm.Timestamp = headerNow + 100_000

	// Pre-existing entry expires at headerNow+1: unexpired under header, "expired" under tvm.Timestamp.
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 100*tvmTRXPrecision, headerNow+1)

	stack := newStack()
	contract := NewContract(owner, owner, 0, 1_000_000)
	stack.push(uint256.NewInt(uint64(200 * tvmTRXPrecision)))      // amount (bottom)
	stack.push(uint256.NewInt(uint64(corepb.ResourceCode_ENERGY))) // resourceType (top): java pops resourceType first
	if _, err := opUnfreezeBalanceV2(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opUnfreezeBalanceV2 error: %v", err)
	}
	ret := stack.pop()
	if got := ret.Uint64(); got != 1 {
		t.Fatalf("opcode return: got %d, want 1 (success)", got)
	}
	// Pre-existing entry NOT withdrawn under the header timestamp: balance unchanged.
	if got := statedb.GetBalance(owner); got != 5*tvmTRXPrecision {
		t.Fatalf("balance: got %d, want %d (nothing expired under header)", got, 5*tvmTRXPrecision)
	}
	// The new 200-TRX unfreeze entry's expire time must be headerNow+14d, NOT tvm.Timestamp+14d.
	const delayMs = int64(14) * 86_400_000
	wantExpire := headerNow + delayMs
	var found bool
	for _, u := range statedb.GetAccount(owner).UnfrozenV2() {
		if u.UnfreezeAmount == 200*tvmTRXPrecision {
			found = true
			if u.UnfreezeExpireTime != wantExpire {
				t.Fatalf("new entry expire: got %d, want %d (headerNow+14d; tvm.Timestamp+14d would be %d)",
					u.UnfreezeExpireTime, wantExpire, tvm.Timestamp+delayMs)
			}
		}
	}
	if !found {
		t.Fatal("new 200-TRX unfreeze entry not found")
	}
}
