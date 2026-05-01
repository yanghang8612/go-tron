package vm

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

// newTestTVMForCreate spins up a TVM with a real DynamicProperties so the
// CALL-with-value auto-create path (slice 2c) can be exercised. cfg lets each
// test toggle Solidity059. dpInit lets the caller stamp DP fields (timestamp,
// AllowMultiSign) before constructing the TVM.
func newTestTVMForCreate(t *testing.T, cfg TVMConfig, dpInit func(*state.DynamicProperties)) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	stateDB, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	stateDB.SetDynamicProperties(dp)
	if dpInit != nil {
		dpInit(dp)
	}
	tvm := NewTVM(stateDB, dp, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, cfg)
	return tvm, stateDB, dp
}

// TestCreateAccountWithTime_FromCALLWithValue verifies that VM CALL with TRX
// value to a non-existent address auto-creates the destination account with
// `Account.create_time = dp.LatestBlockHeaderTimestamp()` and (when
// AllowMultiSign is on) default Owner/Active permissions — mirroring
// java-tron `Program.callToAddress` (Program.java:1083) →
// `RepositoryImpl.createNormalAccount` (RepositoryImpl.java:1103-1114).
func TestCreateAccountWithTime_FromCALLWithValue(t *testing.T) {
	const fixedTS = int64(1_700_000_000_000)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: true}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(fixedTS)
		dp.SetAllowMultiSign(true)
		// NewDynamicProperties seeds active_default_operations with the
		// java-tron default bitmap (0x7fff1fc0033e...), so no explicit set
		// is required for the Active[0] Operations bitmap to be populated.
	})

	caller := tcommon.Address{0x41, 0x01}
	dest := tcommon.Address{0x41, 0xAA, 0xBB, 0xCC}
	sdb.AddBalance(caller, 100_000_000)

	if sdb.AccountExists(dest) {
		t.Fatal("precondition: dest must not exist")
	}

	_, _, err := tvm.Call(caller, dest, nil, 1_000_000, 50_000_000)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}

	acc := sdb.GetAccount(dest)
	if acc == nil {
		t.Fatal("dest account should exist after CALL-with-value")
	}
	if acc.CreateTime() != fixedTS {
		t.Fatalf("create_time: got %d, want %d", acc.CreateTime(), fixedTS)
	}
	if sdb.GetBalance(dest) != 50_000_000 {
		t.Fatalf("balance: got %d, want 50000000", sdb.GetBalance(dest))
	}
	if acc.OwnerPermission() == nil {
		t.Fatal("Owner permission should be installed when AllowMultiSign is on")
	}
	if !bytes.Equal(acc.OwnerPermission().Keys[0].Address, dest[:]) {
		t.Fatalf("Owner key address: got %x, want %x", acc.OwnerPermission().Keys[0].Address, dest[:])
	}
	if len(acc.ActivePermission()) != 1 {
		t.Fatalf("Active permission count: got %d, want 1", len(acc.ActivePermission()))
	}
	if len(acc.ActivePermission()[0].Operations) == 0 {
		t.Fatal("Active[0] Operations bitmap should be populated from ActiveDefaultOperations")
	}
}

// TestCreateAccountWithTime_FromCALL_NoMultiSign locks the independent
// gating: with Solidity059 ON but AllowMultiSign OFF, the new account still
// gets create_time stamped (mirrors java's unconditional ts in the 5-arg
// AccountCapsule constructor) but NO default permissions.
func TestCreateAccountWithTime_FromCALL_NoMultiSign(t *testing.T) {
	const fixedTS = int64(1_700_000_000_000)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: true}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(fixedTS)
		// AllowMultiSign deliberately left at default (false).
	})

	caller := tcommon.Address{0x41, 0x01}
	dest := tcommon.Address{0x41, 0xAA, 0xBB, 0xCD}
	sdb.AddBalance(caller, 100_000_000)

	if _, _, err := tvm.Call(caller, dest, nil, 1_000_000, 50_000_000); err != nil {
		t.Fatalf("Call: %v", err)
	}

	acc := sdb.GetAccount(dest)
	if acc == nil {
		t.Fatal("dest account should exist after CALL-with-value")
	}
	if acc.CreateTime() != fixedTS {
		t.Fatalf("create_time: got %d, want %d (must be unconditional on AllowMultiSign)", acc.CreateTime(), fixedTS)
	}
	if acc.OwnerPermission() != nil {
		t.Fatal("Owner permission must NOT be installed when AllowMultiSign is off")
	}
	if len(acc.ActivePermission()) != 0 {
		t.Fatalf("Active permission must be empty when AllowMultiSign is off, got %d", len(acc.ActivePermission()))
	}
}

// TestCreateAccountWithTime_FromCALL_PreSolidity059 locks the fork gate:
// before Solidity059 activation, CALL-with-value to a non-existent address
// still credits balance (java's pre-solidity059 behavior at
// Program.java:1875-1881 skips the create-with-time branch entirely) — but
// the auto-created account leaves create_time at 0 and has no default
// permissions, matching java's pre-fork wire format.
func TestCreateAccountWithTime_FromCALL_PreSolidity059(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: false}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(1_700_000_000_000)
		dp.SetAllowMultiSign(true)
	})

	caller := tcommon.Address{0x41, 0x01}
	dest := tcommon.Address{0x41, 0xAA, 0xBB, 0xCE}
	sdb.AddBalance(caller, 100_000_000)

	if _, _, err := tvm.Call(caller, dest, nil, 1_000_000, 50_000_000); err != nil {
		t.Fatalf("Call: %v", err)
	}

	acc := sdb.GetAccount(dest)
	if acc == nil {
		t.Fatal("dest account should exist (auto-created via AddBalance)")
	}
	if acc.CreateTime() != 0 {
		t.Fatalf("create_time pre-Solidity059: got %d, want 0 (fork gate must be off)", acc.CreateTime())
	}
	if acc.OwnerPermission() != nil {
		t.Fatal("Owner permission must NOT be installed pre-Solidity059")
	}
	if len(acc.ActivePermission()) != 0 {
		t.Fatal("Active permission must be empty pre-Solidity059")
	}
}

// TestCreateAccountWithTime_FromCALLToken_TokenOnly locks the
// `endowment > 0` gate from java Program.callToAddress: a pure TRC-10 token
// transfer (TRX value == 0) MUST NOT trigger the auto-create-with-time path.
// Going broader than java would inject create_time on accounts java leaves
// with create_time=0 — wire divergence.
func TestCreateAccountWithTime_FromCALLToken_TokenOnly(t *testing.T) {
	const fixedTS = int64(1_700_000_000_000)
	const tokenID = int64(1_000_001)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: true, TransferTrc10: true}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(fixedTS)
		dp.SetAllowMultiSign(true)
	})

	caller := tcommon.Address{0x41, 0x01}
	dest := tcommon.Address{0x41, 0xAA, 0xBB, 0xCF}
	sdb.AddBalance(caller, 100_000_000)
	sdb.AddTRC10Balance(caller, tokenID, 1_000_000)

	// Pure token transfer: value = 0, tokenValue > 0.
	if _, _, err := tvm.CallToken(caller, dest, nil, 1_000_000, 0, tokenID, 100); err != nil {
		t.Fatalf("CallToken: %v", err)
	}

	acc := sdb.GetAccount(dest)
	if acc == nil {
		t.Fatal("dest account should exist (auto-created via AddTRC10Balance)")
	}
	if acc.CreateTime() != 0 {
		t.Fatalf("create_time on token-only transfer: got %d, want 0 (must mirror java's endowment>0 gate)", acc.CreateTime())
	}
	if acc.OwnerPermission() != nil {
		t.Fatal("Owner permission must NOT be installed on token-only transfer")
	}
}

// TestCreateAccountWithTime_FromCALL_PrecompileAddrUntouched locks the
// precompile-dispatch parity: java-tron routes precompile-targeted CALLs to
// `callToPrecompiledAddress` (OperationActions.java:1034-1042) BEFORE
// entering `callToAddress`, so precompile addresses never reach
// `createAccountIfNotExist` and never get an AccountCapsule materialized.
// go-tron must skip the create-with-time helper when the target is a
// precompile, even with TRX value > 0 and Solidity059 + AllowMultiSign on.
func TestCreateAccountWithTime_FromCALL_PrecompileAddrUntouched(t *testing.T) {
	const fixedTS = int64(1_700_000_000_000)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: true}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(fixedTS)
		dp.SetAllowMultiSign(true)
	})

	caller := tcommon.Address{0x41, 0x01}
	// addrFromUint(0x02) → SHA256 precompile (always-active, no fork gate).
	precompileAddr := addrFromUint(0x02)
	sdb.AddBalance(caller, 100_000_000)

	if sdb.AccountExists(precompileAddr) {
		t.Fatal("precondition: precompile address must not pre-exist")
	}

	// CALL with value > 0 to the precompile.
	if _, _, err := tvm.Call(caller, precompileAddr, nil, 1_000_000, 50_000_000); err != nil {
		t.Fatalf("Call: %v", err)
	}

	// AddBalance auto-creates the precompile account via GetOrCreateAccount,
	// but the slice-2c create-with-time path MUST NOT fire — create_time
	// stays at 0 and no default permissions are installed (matching java's
	// behavior where callToPrecompiledAddress doesn't call
	// createAccountIfNotExist).
	acc := sdb.GetAccount(precompileAddr)
	if acc != nil && acc.CreateTime() != 0 {
		t.Fatalf("create_time on precompile addr: got %d, want 0 (java skips createAccountIfNotExist for precompiles)", acc.CreateTime())
	}
	if acc != nil && acc.OwnerPermission() != nil {
		t.Fatal("Owner permission must NOT be installed on precompile addr (slice-2c path must skip precompiles)")
	}
	if acc != nil && len(acc.ActivePermission()) != 0 {
		t.Fatal("Active permission must be empty on precompile addr")
	}
}

// TestCreateAccountWithTime_FromSUICIDE verifies the SELFDESTRUCT path: when
// a contract self-destructs to a non-existent obtainer with positive balance,
// the obtainer is auto-created with create_time stamped — mirroring
// java-tron `Program.suicide` (Program.java:483) /
// `Program.suicide2` (555) which both call createAccountIfNotExist before
// the balance transfer.
func TestCreateAccountWithTime_FromSUICIDE(t *testing.T) {
	const fixedTS = int64(1_700_000_000_000)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Solidity059: true}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(fixedTS)
		dp.SetAllowMultiSign(true)
	})

	contractAddr := tcommon.Address{0x41, 0xCC}
	obtainer := tcommon.Address{0x41, 0xDD, 0x01, 0x02}

	// Seed contract with balance, then SELFDESTRUCT(obtainer).
	sdb.AddBalance(contractAddr, 7_777)

	// PUSH20 <obtainer-without-0x41-prefix>; SELFDESTRUCT.
	// uint256ToAddress restores the 0x41 prefix in opSelfDestruct, so we
	// only push the 20-byte EVM-form address tail (matches java-tron stack
	// representation: addresses on the stack are 20 bytes).
	code := []byte{0x73}
	code = append(code, obtainer[1:]...)
	code = append(code, 0xFF) // SELFDESTRUCT
	sdb.SetCode(contractAddr, code)

	if sdb.AccountExists(obtainer) {
		t.Fatal("precondition: obtainer must not exist")
	}

	caller := tcommon.Address{0x41, 0x01}
	if _, _, err := tvm.Call(caller, contractAddr, nil, 100_000, 0); err != nil {
		t.Fatalf("Call: %v", err)
	}

	acc := sdb.GetAccount(obtainer)
	if acc == nil {
		t.Fatal("obtainer account should exist after SELFDESTRUCT-with-balance")
	}
	if acc.CreateTime() != fixedTS {
		t.Fatalf("create_time on SUICIDE auto-create: got %d, want %d", acc.CreateTime(), fixedTS)
	}
	if acc.OwnerPermission() == nil {
		t.Fatal("Owner permission should be installed on SUICIDE auto-create when AllowMultiSign is on")
	}
	if sdb.GetBalance(obtainer) != 7_777 {
		t.Fatalf("obtainer balance: got %d, want 7777", sdb.GetBalance(obtainer))
	}
}
