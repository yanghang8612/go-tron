package vm

// Replay of the Nile block 18,112,819 stall (2021-07-28):
// tx 5ca838a345df23b8132fe69b48e9d0ef9733d9cedd23441bac5675f9608488aa —
// contract "Test".test(address(2)) with call_value 300, i.e. a TRX
// endowment CALL into the SHA256 precompile address. java-tron dispatches
// value-bearing precompile calls through callToPrecompiledAddress, whose
// MUtil.transfer -> validateForSmartContract rejects the credit because the
// precompile address has no account ("Validate InternalTransfer error, no
// ToAccount...") -> BytecodeExecutionException("transfer failure"). That is
// NOT a TransferException, so VM.play spends ALL remaining energy and the
// receipt records UNKNOWN with resMessage "transfer failure"
// (energy_usage_total 28,571,428 == the full fee_limit/140 budget).
// gtron instead credited the precompile address directly and returned
// SUCCESS — flipping the canonical UNKNOWN.

import (
	"encoding/hex"
	"errors"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestNilePrecompileEndowmentBurnsAllEnergy(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{
		TransferTrc10:  true,
		Constantinople: true,
		Solidity059:    true,
		ShieldedToken:  true,
		Istanbul:       true,
	})
	evm.BlockNumber = 18112819
	evm.Timestamp = 1627457319000

	contractAddr := hexAddr(t, "41b337741b688e9f0dc117c1379c81c5c0d1508a20")
	code := mustHexFile(t, "testdata/nile_precompile_transfer18112819.hex")
	evm.StateDB.CreateAccount(contractAddr, corepb.AccountType_Contract)
	evm.StateDB.SetCode(contractAddr, code)

	caller := hexAddr(t, "41de9934ba6c9063ac0771adb0255800a405220ec3")
	evm.StateDB.CreateAccount(caller, corepb.AccountType_Normal)
	evm.StateDB.AddBalance(caller, 1_000_000)

	// test(address(2)) with 300 sun call_value; fee_limit 4000 TRX at 140
	// sun/energy == 28,571,428 energy, all of which java burned.
	calldata, _ := hex.DecodeString("bb29998e0000000000000000000000000000000000000000000000000000000000000002")
	const limit = 28_571_428
	ret, left, err := evm.Call(caller, contractAddr, calldata, limit, 300)

	if !errors.Is(err, ErrPrecompileTransferFailure) {
		t.Errorf("result: got err=%v want ErrPrecompileTransferFailure (java contractRet UNKNOWN, resMessage \"transfer failure\")", err)
	}
	if used := uint64(limit) - left; used != limit {
		t.Errorf("energy: got %d want %d (java spendAllEnergy burns the full budget)", used, limit)
	}
	if len(ret) != 0 {
		t.Errorf("return data: got %x want empty", ret)
	}
}

// TestPrecompileEndowmentWithExistingAccountSucceeds pins the other side of
// java's validateForSmartContract: when the precompile address DOES have an
// account, MUtil.transfer passes and the call executes normally.
func TestPrecompileEndowmentWithExistingAccountSucceeds(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true, Istanbul: true})

	caller := hexAddr(t, "41de9934ba6c9063ac0771adb0255800a405220ec3")
	evm.StateDB.CreateAccount(caller, corepb.AccountType_Normal)
	evm.StateDB.AddBalance(caller, 1_000)

	sha256Addr := hexAddr(t, "410000000000000000000000000000000000000002")
	evm.StateDB.CreateAccount(sha256Addr, corepb.AccountType_Normal)

	ret, _, err := evm.Call(caller, sha256Addr, []byte{0x01}, 100_000, 300)
	if err != nil {
		t.Fatalf("endowment to an EXISTING precompile account must succeed (java MUtil.transfer passes), got %v", err)
	}
	if len(ret) != 32 {
		t.Fatalf("sha256 precompile output: got %d bytes want 32", len(ret))
	}
	if got := evm.StateDB.GetBalance(sha256Addr); got != 300 {
		t.Fatalf("precompile account balance: got %d want 300", got)
	}
}
