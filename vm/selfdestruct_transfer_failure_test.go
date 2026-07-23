package vm

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const mainnet5196383EnergyLimit = uint64(98_971_525)

func runMainnet5196383SelfDestruct(t *testing.T, cfg TVMConfig, dpInit func(*state.DynamicProperties)) (*TVM, *state.StateDB, uint64, error) {
	t.Helper()

	tvm, sdb, _ := newTestTVMForCreate(t, cfg, dpInit)
	owner := tcommon.BytesToAddress(tcommon.FromHex("41eb44510a44517ebd7a0a1b99a035e10ef4f00fad"))
	contractAddr := tcommon.BytesToAddress(tcommon.FromHex("414957e150e7d37c21522a9cabe1fbe4f6cf4f827a"))
	beneficiary := tcommon.BytesToAddress(tcommon.FromHex("41b2265cb9c12ab8b5cf6054226e5bd41bdd04f841"))

	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.AddBalance(owner, 20_000_000)

	// Runtime body of tron:A.go(): PUSH20 beneficiary; SELFDESTRUCT.
	code := []byte{byte(PUSH20)}
	code = append(code, beneficiary[1:]...)
	code = append(code, byte(SELFDESTRUCT))
	sdb.SetCode(contractAddr, code)

	_, left, err := tvm.Call(owner, contractAddr, nil, mainnet5196383EnergyLimit, 10_000_000)
	return tvm, sdb, left, err
}

// TestMainnet5196383SelfDestructMissingBeneficiary pins tx
// 4d880490a2d2e5c83c737909cd3c015eb4ec8c9315a16ee01d8f391b0f3ff5fe.
// Before ALLOW_TVM_CONSTANTINOPLE and ALLOW_TVM_SOLIDITY_059, Program.suicide
// attempted to transfer the contract balance to an accountless beneficiary.
// MUtil.transfer rejected it and VM.play recorded UNKNOWN / "transfer failure"
// after consuming the full transaction energy.
func TestMainnet5196383SelfDestructMissingBeneficiary(t *testing.T) {
	tvm, sdb, left, err := runMainnet5196383SelfDestruct(t, TVMConfig{}, nil)
	beneficiary := tcommon.BytesToAddress(tcommon.FromHex("41b2265cb9c12ab8b5cf6054226e5bd41bdd04f841"))
	contractAddr := tcommon.BytesToAddress(tcommon.FromHex("414957e150e7d37c21522a9cabe1fbe4f6cf4f827a"))
	owner := tcommon.BytesToAddress(tcommon.FromHex("41eb44510a44517ebd7a0a1b99a035e10ef4f00fad"))

	if !errors.Is(err, ErrSelfDestructTransferFailure) {
		t.Fatalf("Call error: got %v, want ErrSelfDestructTransferFailure", err)
	}
	if got := err.Error(); got != "transfer failure" {
		t.Fatalf("runtime message: got %q, want %q", got, "transfer failure")
	}
	if left != 0 {
		t.Fatalf("remaining energy: got %d, want 0", left)
	}
	if sdb.AccountExists(beneficiary) {
		t.Fatal("failed legacy SELFDESTRUCT must not create the beneficiary")
	}
	if tvm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("failed legacy SELFDESTRUCT must not delete the contract")
	}
	if got := sdb.GetBalance(owner); got != 20_000_000 {
		t.Fatalf("owner balance after failed trigger: got %d, want 20000000", got)
	}
}

func TestSelfDestructMissingBeneficiaryProposalTransitions(t *testing.T) {
	beneficiary := tcommon.BytesToAddress(tcommon.FromHex("41b2265cb9c12ab8b5cf6054226e5bd41bdd04f841"))
	contractAddr := tcommon.BytesToAddress(tcommon.FromHex("414957e150e7d37c21522a9cabe1fbe4f6cf4f827a"))

	t.Run("constantinople-before-solidity059", func(t *testing.T) {
		tvm, sdb, left, err := runMainnet5196383SelfDestruct(t, TVMConfig{Constantinople: true}, nil)
		if !errors.Is(err, ErrTransferFailed) {
			t.Fatalf("Call error: got %v, want TRANSFER_FAILED classification", err)
		}
		const wantMessage = "transfer all token or transfer all trx failed in suicide: Validate InternalTransfer error, no ToAccount. And not allowed to create an account in a smartContract."
		if got := err.Error(); got != wantMessage {
			t.Fatalf("runtime message: got %q, want %q", got, wantMessage)
		}
		if left == 0 {
			t.Fatal("Constantinople TransferException must preserve remaining energy")
		}
		if sdb.AccountExists(beneficiary) {
			t.Fatal("Constantinople alone must not create the beneficiary")
		}
		if tvm.StateDB.HasSelfDestructed(contractAddr) {
			t.Fatal("failed SELFDESTRUCT must not delete the contract")
		}
	})

	t.Run("solidity059", func(t *testing.T) {
		tvm, sdb, _, err := runMainnet5196383SelfDestruct(t, TVMConfig{
			Constantinople: true,
			Solidity059:    true,
			MultiSign:      true,
		}, func(dp *state.DynamicProperties) {
			dp.SetLatestBlockHeaderTimestamp(1_700_000_000_000)
			dp.SetAllowMultiSign(true)
		})
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if !sdb.AccountExists(beneficiary) {
			t.Fatal("Solidity059 SELFDESTRUCT must create the beneficiary")
		}
		if got := sdb.GetBalance(beneficiary); got != 10_000_000 {
			t.Fatalf("beneficiary balance: got %d, want 10000000", got)
		}
		if !tvm.StateDB.HasSelfDestructed(contractAddr) {
			t.Fatal("successful legacy SELFDESTRUCT must delete the contract")
		}
	})
}
