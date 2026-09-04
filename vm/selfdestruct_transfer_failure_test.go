package vm

import (
	"errors"
	"math"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

const mainnet5196383EnergyLimit = uint64(98_971_525)

// Program.suicide withdraws reward before creating the legacy SELFDESTRUCT
// record. Its checked balance-plus-allowance overflow therefore raises
// BytecodeExecutionException before the nonce/record, transfer or deletion; the
// enclosing call discards the child repository. Program.suicide2 has a different
// ordering locked separately below.
func TestSelfDestructBalanceAllowanceOverflowReverts(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Vote: true}, nil)
	origin := tcommon.Address{0x41, 0x31}
	contractAddr := tcommon.Address{0x41, 0x32}
	beneficiary := tcommon.Address{0x41, 0x33}
	sdb.CreateAccount(origin, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.CreateAccount(beneficiary, corepb.AccountType_Normal)
	sdb.AddBalance(contractAddr, math.MaxInt64)
	sdb.SetAllowance(contractAddr, 1)
	// Skip reward computation so the fixture isolates balance+allowance.
	if err := sdb.WriteBeginCycle(contractAddr.Bytes(), 1); err != nil {
		t.Fatal(err)
	}
	code := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
	code = append(code, byte(SELFDESTRUCT))
	sdb.SetCode(contractAddr, code)

	_, left, err := tvm.Call(origin, contractAddr, nil, 1_000_000, 0)
	if !errors.Is(err, ErrSelfDestructBalanceAllowanceOverflow) {
		t.Fatalf("Call error: got %v, want ErrSelfDestructBalanceAllowanceOverflow", err)
	}
	if got := err.Error(); got != "Suicide: balance and allowance out of long range." {
		t.Fatalf("runtime message: got %q", got)
	}
	if left != 0 {
		t.Fatalf("remaining energy: got %d, want 0", left)
	}
	if got := sdb.GetBalance(contractAddr); got != math.MaxInt64 {
		t.Fatalf("contract balance after failed suicide: got %d", got)
	}
	if got := sdb.GetAllowance(contractAddr); got != 1 {
		t.Fatalf("contract allowance after failed suicide: got %d", got)
	}
	if tvm.Nonce != 0 {
		t.Fatalf("SELFDESTRUCT nonce advanced before overflow: got %d", tvm.Nonce)
	}
	if tvm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("failed SELFDESTRUCT deleted the contract")
	}
	if got := sdb.GetBalance(beneficiary); got != 0 {
		t.Fatalf("beneficiary balance after failed suicide: got %d", got)
	}
}

func TestRestrictedSelfDestructInternalHashUsesPreRewardBalance(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{
		Vote:                 true,
		SelfdestructRestrict: true,
	}, nil)
	root := tcommon.HexToHash("010203")
	tvm.SetRootTransactionID(root)
	origin := tcommon.Address{0x41, 0x51}
	contractAddr := tcommon.Address{0x41, 0x52}
	beneficiary := tcommon.Address{0x41, 0x53}
	sdb.CreateAccount(origin, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.CreateAccount(beneficiary, corepb.AccountType_Normal)
	sdb.AddBalance(contractAddr, 100)
	sdb.SetAllowance(contractAddr, 5)
	if err := sdb.WriteBeginCycle(contractAddr.Bytes(), 1); err != nil {
		t.Fatal(err)
	}
	code := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
	code = append(code, byte(SELFDESTRUCT))
	sdb.SetCode(contractAddr, code)

	if _, _, err := tvm.Call(origin, contractAddr, nil, 1_000_000, 0); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if len(tvm.InternalTransactions) != 1 {
		t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
	}
	it := tvm.InternalTransactions[0]
	if got := it.CallValueInfo[0].CallValue; got != 105 {
		t.Fatalf("published suicide value: got %d, want post-reward 105", got)
	}
	// Program.suicide2 cached the identity at construction, when the contract
	// held only 100. setValue(105) after reward settlement must not rehash it.
	wantHash := expectedInternalTxHash(root, beneficiary.Bytes(), nil, 100, 1)
	if got := tcommon.BytesToHash(it.Hash); got != wantHash {
		t.Fatalf("internal hash: got %s, want pre-reward %s", got.Hex(), wantHash.Hex())
	}
	if got := sdb.GetBalance(beneficiary); got != 105 {
		t.Fatalf("beneficiary balance: got %d, want 105", got)
	}
	if got := sdb.GetAllowance(contractAddr); got != 0 {
		t.Fatalf("contract allowance: got %d, want 0", got)
	}
	if tvm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("restricted pre-existing contract must not be deleted")
	}
}

func TestSelfDestructInternalTransactionRetainsZeroTokenEntries(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	origin := tcommon.Address{0x41, 0x41}
	contractAddr := tcommon.Address{0x41, 0x42}
	beneficiary := tcommon.Address{0x41, 0x43}
	sdb.CreateAccount(origin, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.CreateAccount(beneficiary, corepb.AccountType_Normal)
	sdb.SetTRC10Balance(contractAddr, 1_000_001, 0)
	code := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
	code = append(code, byte(SELFDESTRUCT))
	sdb.SetCode(contractAddr, code)

	if _, _, err := tvm.Call(origin, contractAddr, nil, 1_000_000, 0); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if len(tvm.InternalTransactions) != 1 {
		t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
	}
	it := tvm.InternalTransactions[0]
	if string(it.Note) != "suicide" {
		t.Fatalf("note: got %q, want suicide", it.Note)
	}
	if len(it.CallValueInfo) != 2 {
		t.Fatalf("call value entries: got %d, want base plus zero token", len(it.CallValueInfo))
	}
	token := it.CallValueInfo[1]
	if token.TokenId != "1000001" || token.CallValue != 0 {
		t.Fatalf("zero token entry: got %+v", token)
	}
}

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

func TestSelfDestructZeroBalanceSolidity059StillCreatesBeneficiary(t *testing.T) {
	const createTime = int64(1_700_000_123_000)
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: true}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(createTime)
	})
	origin := tcommon.Address{0x41, 0x61}
	contractAddr := tcommon.Address{0x41, 0x62}
	beneficiary := tcommon.Address{0x41, 0x63}
	sdb.CreateAccount(origin, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	code := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
	code = append(code, byte(SELFDESTRUCT))
	sdb.SetCode(contractAddr, code)

	if _, _, err := tvm.Call(origin, contractAddr, nil, 1_000_000, 0); err != nil {
		t.Fatalf("Call: %v", err)
	}
	account := sdb.GetAccount(beneficiary)
	if account == nil {
		t.Fatal("Solidity059 must create a zero-balance SELFDESTRUCT beneficiary")
	}
	if got := account.Proto().GetCreateTime(); got != createTime {
		t.Fatalf("beneficiary create_time: got %d, want %d", got, createTime)
	}
}

func TestSelfDestructZeroBalanceMissingTRC10BeneficiaryIsUnknown(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	origin := tcommon.Address{0x41, 0x71}
	contractAddr := tcommon.Address{0x41, 0x72}
	beneficiary := tcommon.Address{0x41, 0x73}
	sdb.CreateAccount(origin, corepb.AccountType_Normal)
	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	code := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
	code = append(code, byte(SELFDESTRUCT))
	sdb.SetCode(contractAddr, code)

	_, left, err := tvm.Call(origin, contractAddr, nil, 1_000_000, 0)
	if !errors.Is(err, errSelfDestructMissingTokenBeneficiary) {
		t.Fatalf("Call error: got %v, want normalised SELFDESTRUCT NPE", err)
	}
	if got := err.Error(); got != "Unknown Exception" {
		t.Fatalf("runtime message: got %q, want Unknown Exception", got)
	}
	if left != 0 {
		t.Fatalf("remaining energy: got %d, want 0", left)
	}
	if sdb.AccountExists(beneficiary) || tvm.StateDB.HasSelfDestructed(contractAddr) {
		t.Fatal("failed SELFDESTRUCT must roll back beneficiary creation and deletion")
	}
}

func TestSelfDestructNegativeBalanceUsesJavaTransferValidation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		constantinople bool
		wantError      error
		wantMessage    string
		wantEnergyLeft bool
	}{
		{
			name:        "legacy",
			wantError:   ErrSelfDestructTransferFailure,
			wantMessage: "transfer failure",
		},
		{
			name:           "constantinople",
			constantinople: true,
			wantError:      ErrTransferFailed,
			wantMessage:    "transfer all token or transfer all trx failed in suicide: Amount must be greater than or equals 0.",
			wantEnergyLeft: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: tc.constantinople}, nil)
			origin := tcommon.Address{0x41, 0x81}
			contractAddr := tcommon.Address{0x41, 0x82}
			beneficiary := tcommon.Address{0x41, 0x83}
			sdb.CreateAccount(origin, corepb.AccountType_Normal)
			sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
			sdb.CreateAccount(beneficiary, corepb.AccountType_Normal)
			sdb.AddBalance(contractAddr, -1)
			code := append([]byte{byte(PUSH20)}, beneficiary[1:]...)
			code = append(code, byte(SELFDESTRUCT))
			sdb.SetCode(contractAddr, code)

			_, left, err := tvm.Call(origin, contractAddr, nil, 1_000_000, 0)
			if !errors.Is(err, tc.wantError) {
				t.Fatalf("Call error: got %v, want %v", err, tc.wantError)
			}
			if got := err.Error(); got != tc.wantMessage {
				t.Fatalf("runtime message: got %q, want %q", got, tc.wantMessage)
			}
			if (left > 0) != tc.wantEnergyLeft {
				t.Fatalf("remaining energy: got %d, want positive=%v", left, tc.wantEnergyLeft)
			}
			if got := sdb.GetBalance(contractAddr); got != -1 {
				t.Fatalf("contract balance after rollback: got %d, want -1", got)
			}
			if tvm.StateDB.HasSelfDestructed(contractAddr) {
				t.Fatal("failed negative-balance SELFDESTRUCT deleted contract")
			}
		})
	}
}
