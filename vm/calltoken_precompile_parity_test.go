package vm

// Companion to nile_precompile_transfer_test.go (Nile block 18,112,819), but
// for the TRC10 leg of CALLTOKEN instead of the TRX leg.
//
// java-tron dispatches a value-bearing precompile call through
// Program.callToPrecompiledAddress. For a TRC10 endowment (isTokenTransfer &&
// endowment > 0 && sender != context) it runs
// VMUtils.validateForSmartContract(deposit, sender, context, tokenId, amount)
// (Program.java:1710-1719). That validator rejects a destination whose account
// does not exist ("Validate InternalTransfer error, no ToAccount. And not
// allowed to create account in smart contract.", VMUtils.java:241-243) by
// throwing ContractValidateException, which callToPrecompiledAddress re-throws
// as BytecodeExecutionException("transfer failure"). A BytecodeExecutionException
// is NOT a TransferException, so VM.play spendAllEnergy → the receipt records
// UNKNOWN(13) and burns the full energy budget.
//
// Precompile addresses normally have no account and are never auto-created on
// this path, so a value-bearing CALLTOKEN into one must surface
// ErrPrecompileTransferFailure (which shouldPropagateCallError propagates →
// spend-all → UNKNOWN), exactly like the TRX leg fixed in d46b3cdb. Returning
// ErrInsufficientBalance instead (the pre-fix behavior) is swallowed by
// opCallToken, which pushes 0 and lets the tx continue as SUCCESS.

import (
	"errors"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// tokenLegParityConfig mirrors the active fork flags at Nile 18,112,819 used by
// the sibling TRX-leg replay, so getPrecompile resolves the stock precompiles.
func tokenLegParityConfig() TVMConfig {
	return TVMConfig{
		TransferTrc10:  true,
		Constantinople: true,
		Solidity059:    true,
		ShieldedToken:  true,
		Istanbul:       true,
		MultiSign:      true,
	}
}

// TestNilePrecompileTokenEndowmentBurnsAllEnergy is the TRC10 twin of
// TestNilePrecompileEndowmentBurnsAllEnergy: a CALLTOKEN that carries a TRC10
// value into a precompile address with no account must burn all energy and
// surface ErrPrecompileTransferFailure (java "transfer failure" → UNKNOWN).
func TestNilePrecompileTokenEndowmentBurnsAllEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, tokenLegParityConfig())
	evm.BlockNumber = 18112819
	evm.Timestamp = 1627457319000

	caller := hexAddr(t, "41de9934ba6c9063ac0771adb0255800a405220ec3")
	evm.StateDB.CreateAccount(caller, corepb.AccountType_Normal)

	const tokenID = int64(1000001)
	const tokenValue = int64(300)
	// Fund the caller's TRC10 balance so the insufficient-balance gate
	// (GetTRC10Balance < tokenValue) passes and execution reaches the
	// destination-account check — the branch under test.
	evm.StateDB.SetTRC10Balance(caller, tokenID, 1_000)

	// sha256 precompile at 0x02; deliberately NOT created, so it has no account.
	sha256Addr := hexAddr(t, "410000000000000000000000000000000000000002")
	if evm.StateDB.AccountExists(sha256Addr) {
		t.Fatalf("precondition: precompile address must have no account")
	}

	const limit = 28_571_428
	ret, left, err := evm.CallToken(caller, sha256Addr, []byte{0x01}, limit, 0 /*TRX value*/, tokenID, tokenValue)

	if !errors.Is(err, ErrPrecompileTransferFailure) {
		t.Errorf("result: got err=%v want ErrPrecompileTransferFailure (java callToPrecompiledAddress token leg → BytecodeExecutionException \"transfer failure\" → UNKNOWN)", err)
	}
	if used := uint64(limit) - left; used != limit {
		t.Errorf("energy: got %d want %d (java spendAllEnergy burns the full budget)", used, limit)
	}
	if len(ret) != 0 {
		t.Errorf("return data: got %x want empty", ret)
	}
	// The TRC10 debit/credit must have been rolled back (snapshot revert).
	if got := evm.StateDB.GetTRC10Balance(caller, tokenID); got != 1_000 {
		t.Errorf("caller TRC10 balance: got %d want 1000 (transfer reverted)", got)
	}
	if got := evm.StateDB.GetTRC10Balance(sha256Addr, tokenID); got != 0 {
		t.Errorf("precompile TRC10 balance: got %d want 0 (no credit on transfer failure)", got)
	}
}

// TestPrecompileTokenEndowmentWithExistingAccountSucceeds pins the other side of
// validateForSmartContract: when the precompile address DOES have an account,
// the TRC10 transfer passes and the precompile executes normally — symmetric to
// TestPrecompileEndowmentWithExistingAccountSucceeds for the TRX leg.
func TestPrecompileTokenEndowmentWithExistingAccountSucceeds(t *testing.T) {
	evm := newTestEVMWithConfig(t, tokenLegParityConfig())

	caller := hexAddr(t, "41de9934ba6c9063ac0771adb0255800a405220ec3")
	evm.StateDB.CreateAccount(caller, corepb.AccountType_Normal)

	const tokenID = int64(1000001)
	const tokenValue = int64(300)
	evm.StateDB.SetTRC10Balance(caller, tokenID, 1_000)

	sha256Addr := hexAddr(t, "410000000000000000000000000000000000000002")
	evm.StateDB.CreateAccount(sha256Addr, corepb.AccountType_Normal)

	ret, _, err := evm.CallToken(caller, sha256Addr, []byte{0x01}, 100_000, 0 /*TRX value*/, tokenID, tokenValue)
	if err != nil {
		t.Fatalf("TRC10 endowment to an EXISTING precompile account must succeed (java validateForSmartContract passes), got %v", err)
	}
	if len(ret) != 32 {
		t.Fatalf("sha256 precompile output: got %d bytes want 32", len(ret))
	}
	if got := evm.StateDB.GetTRC10Balance(sha256Addr, tokenID); got != tokenValue {
		t.Fatalf("precompile TRC10 balance: got %d want %d", got, tokenValue)
	}
	if got := evm.StateDB.GetTRC10Balance(caller, tokenID); got != 700 {
		t.Fatalf("caller TRC10 balance: got %d want 700 (debited %d)", got, tokenValue)
	}
}
