package vm

import (
	"bytes"
	"encoding/binary"
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

func expectedInternalTxHash(parent tcommon.Hash, receiveAddress, data []byte, value int64, nonce uint64) tcommon.Hash {
	var valueBytes [8]byte
	binary.BigEndian.PutUint64(valueBytes[:], uint64(value))
	raw := make([]byte, 0, len(parent)+len(receiveAddress)+len(data)+len(valueBytes)+8)
	raw = append(raw, parent[:]...)
	raw = append(raw, receiveAddress...)
	raw = append(raw, data...)
	raw = append(raw, valueBytes[:]...)
	var nonceBytes [8]byte
	binary.BigEndian.PutUint64(nonceBytes[:], nonce)
	raw = append(raw, nonceBytes[:]...)
	return tcommon.Keccak256(raw)
}

func TestInternalTransactionRecordedForNestedCallToEmptyAccount(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	root := tcommon.HexToHash("010203")
	tvm.SetRootTransactionID(root)
	tvm.Depth = 1
	tvm.internalTxHashStack = append(tvm.internalTxHashStack, root)

	caller := tcommon.Address{0x41, 0x01}
	target := tcommon.Address{0x41, 0x02}
	input := []byte{0xaa, 0xbb}
	sdb.CreateAccount(caller, 0)
	sdb.CreateAccount(target, 0)
	sdb.AddBalance(caller, 100)

	if _, _, err := tvm.Call(caller, target, input, 1_000_000, 7); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if len(tvm.InternalTransactions) != 1 {
		t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
	}
	it := tvm.InternalTransactions[0]
	wantHash := expectedInternalTxHash(root, target.Bytes(), input, 7, 1)
	if string(it.Hash) != string(wantHash.Bytes()) {
		t.Fatalf("internal tx hash: got %x, want %x", it.Hash, wantHash.Bytes())
	}
	if string(it.CallerAddress) != string(caller.Bytes()) {
		t.Fatalf("caller: got %x, want %x", it.CallerAddress, caller.Bytes())
	}
	if string(it.TransferToAddress) != string(target.Bytes()) {
		t.Fatalf("transferTo: got %x, want %x", it.TransferToAddress, target.Bytes())
	}
	if len(it.CallValueInfo) != 1 || it.CallValueInfo[0].CallValue != 7 {
		t.Fatalf("callValueInfo: got %+v, want one TRX value 7", it.CallValueInfo)
	}
	if string(it.Note) != "call" {
		t.Fatalf("note: got %q, want call", it.Note)
	}
	if it.Rejected {
		t.Fatal("internal transaction should not be rejected")
	}
}

func TestInternalTransactionRejectedWhenNestedCallReverts(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	root := tcommon.HexToHash("0a0b0c")
	tvm.SetRootTransactionID(root)
	tvm.Depth = 1
	tvm.internalTxHashStack = append(tvm.internalTxHashStack, root)

	caller := tcommon.Address{0x41, 0x01}
	target := tcommon.Address{0x41, 0x02}
	sdb.CreateAccount(caller, 0)
	sdb.CreateAccount(target, 0)
	sdb.SetCode(target, []byte{0x60, 0x00, 0x60, 0x00, 0xfd}) // REVERT(0, 0)

	if _, _, err := tvm.Call(caller, target, nil, 1_000_000, 0); err != ErrExecutionReverted {
		t.Fatalf("Call error: got %v, want ErrExecutionReverted", err)
	}
	if len(tvm.InternalTransactions) != 1 {
		t.Fatalf("internal transactions: got %d, want 1", len(tvm.InternalTransactions))
	}
	if !tvm.InternalTransactions[0].Rejected {
		t.Fatal("internal transaction should be rejected")
	}
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

// TestCreateAccountWithTime_FromCALLToken_TokenOnly locks java
// Program.callToAddress: TRC-10 transfer uses tokenValue as the message
// endowment, so Solidity059 creates the destination account before token
// validation even when TRX value is zero.
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
	if acc.CreateTime() != fixedTS {
		t.Fatalf("create_time on token-only transfer: got %d, want %d", acc.CreateTime(), fixedTS)
	}
	if acc.OwnerPermission() == nil {
		t.Fatal("Owner permission should be installed when AllowMultiSign is on")
	}
}

func TestCreateAtWithToken_TransfersAndExposesMessageToken(t *testing.T) {
	const (
		tokenID    = int64(1_000_002)
		tokenValue = int64(7)
		callValue  = int64(5)
	)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	sdb.AddBalance(caller, 1_000_000)
	sdb.AddTRC10Balance(caller, tokenID, 100)

	code := []byte{
		byte(CALLTOKENID), byte(PUSH1), 0x00, byte(SSTORE),
		byte(CALLTOKENVALUE), byte(PUSH1), 0x01, byte(SSTORE),
		byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(RETURN),
	}
	_, addr, _, err := tvm.CreateAtWithToken(caller, contractAddr, code, 1_000_000, callValue, tokenID, tokenValue)
	if err != nil {
		t.Fatalf("CreateAtWithToken: %v", err)
	}
	if addr != contractAddr {
		t.Fatalf("contract address: got %x, want %x", addr, contractAddr)
	}
	if got := sdb.GetBalance(caller); got != 1_000_000-callValue {
		t.Fatalf("caller balance: got %d", got)
	}
	if got := sdb.GetBalance(contractAddr); got != callValue {
		t.Fatalf("contract balance: got %d", got)
	}
	if got := sdb.GetTRC10Balance(caller, tokenID); got != 100-tokenValue {
		t.Fatalf("caller token balance: got %d", got)
	}
	if got := sdb.GetTRC10Balance(contractAddr, tokenID); got != tokenValue {
		t.Fatalf("contract token balance: got %d", got)
	}
	if got := sdb.GetState(contractAddr, hashFromUint64(0)); got != hashFromUint64(uint64(tokenID)) {
		t.Fatalf("slot0 token id: got %x", got)
	}
	if got := sdb.GetState(contractAddr, hashFromUint64(1)); got != hashFromUint64(uint64(tokenValue)) {
		t.Fatalf("slot1 token value: got %x", got)
	}
}

func TestCreateAtWithToken_AllowsRuntimeCodeLargerThanEIP170(t *testing.T) {
	const runtimeLen = 24_577
	const runtimeLenHi = byte(runtimeLen >> 8)
	const runtimeLenLo = byte(runtimeLen & 0xff)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true}, nil)
	caller := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	runtime := bytes.Repeat([]byte{byte(STOP)}, runtimeLen)
	initCode := []byte{
		byte(PUSH2), runtimeLenHi, runtimeLenLo,
		byte(PUSH1), 14,
		byte(PUSH1), 0,
		byte(CODECOPY),
		byte(PUSH2), runtimeLenHi, runtimeLenLo,
		byte(PUSH1), 0,
		byte(RETURN),
	}
	bytecode := append(initCode, runtime...)

	ret, addr, _, err := tvm.CreateAtWithToken(caller, contractAddr, bytecode, 10_000_000, 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateAtWithToken: %v", err)
	}
	if addr != contractAddr {
		t.Fatalf("contract address: got %x, want %x", addr, contractAddr)
	}
	if len(ret) != runtimeLen {
		t.Fatalf("return code length: got %d, want %d", len(ret), runtimeLen)
	}
	if got := len(sdb.GetCode(contractAddr)); got != runtimeLen {
		t.Fatalf("stored code length: got %d, want %d", got, runtimeLen)
	}
}

func TestCreateAtWithToken_PreConstantinopleStoresLegacyPrecompiledCode(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: false}, nil)
	caller := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x03}
	legacyCode := []byte{byte(PUSH1), 0x2A, byte(PUSH1), 0x00, byte(SSTORE), byte(STOP)}

	bytecode := []byte{
		byte(PUSH1), 0,
		byte(PUSH1), 0,
		byte(RETURN),
		byte(STOP),
	}
	bytecode = append(bytecode, legacyCode...)

	ret, addr, _, err := tvm.CreateAtWithToken(caller, contractAddr, bytecode, 1_000_000, 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateAtWithToken: %v", err)
	}
	if addr != contractAddr {
		t.Fatalf("contract address: got %x, want %x", addr, contractAddr)
	}
	if len(ret) != 0 {
		t.Fatalf("constructor return length: got %d, want 0", len(ret))
	}
	if got := sdb.GetCode(contractAddr); !bytes.Equal(got, legacyCode) {
		t.Fatalf("stored code: got %x, want %x", got, legacyCode)
	}
}

func TestCreateAtWithToken_LondonRejectsEFPrefixRuntimeCode(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true, London: true}, nil)
	caller := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x04}

	bytecode := []byte{
		byte(PUSH1), 1,
		byte(PUSH1), 12,
		byte(PUSH1), 0,
		byte(CODECOPY),
		byte(PUSH1), 1,
		byte(PUSH1), 0,
		byte(RETURN),
		0xEF,
	}

	ret, _, _, err := tvm.CreateAtWithToken(caller, contractAddr, bytecode, 1_000_000, 0, 0, 0)
	if err != ErrInvalidCode {
		t.Fatalf("CreateAtWithToken error: got %v, want %v", err, ErrInvalidCode)
	}
	if string(ret) != string([]byte{0xEF}) {
		t.Fatalf("contract result: got %x, want ef", ret)
	}
	if got := sdb.GetCode(contractAddr); len(got) != 0 {
		t.Fatalf("invalid EF-prefixed runtime code should not be stored, got %x", got)
	}
}

func TestCallTokenToExistingNoCodeChargesJavaNetCost(t *testing.T) {
	const tokenID = int64(1_000_002)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	dest := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(caller)
	sdb.GetOrCreateAccount(dest)
	sdb.AddTRC10Balance(caller, tokenID, 10)

	code := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH3), 0x0f, 0x42, 0x42, // tokenId = 1000002
		byte(PUSH1), 0x01, // tokenValue
		byte(PUSH20),
	}
	code = append(code, dest[1:]...)
	code = append(code,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALLTOKEN),
		byte(STOP),
	)
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := sdb.GetTRC10Balance(caller, tokenID); got != 9 {
		t.Fatalf("caller token balance: got %d", got)
	}
	if got := sdb.GetTRC10Balance(dest, tokenID); got != 1 {
		t.Fatalf("dest token balance: got %d", got)
	}
	if got, want := uint64(100_000)-contract.Energy, uint64(6764); got != want {
		t.Fatalf("energy used: got %d, want %d", got, want)
	}
}

func TestCallTokenToExistingCodeSkipsJavaSurcharge(t *testing.T) {
	const tokenID = int64(1_000_002)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	dest := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(caller)
	sdb.GetOrCreateAccount(dest)
	sdb.AddTRC10Balance(caller, tokenID, 10)
	sdb.SetCode(dest, []byte{byte(STOP)})

	code := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH3), 0x0f, 0x42, 0x42, // tokenId = 1000002
		byte(PUSH1), 0x01, // tokenValue
		byte(PUSH20),
	}
	code = append(code, dest[1:]...)
	code = append(code,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALLTOKEN),
		byte(STOP),
	)
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := sdb.GetTRC10Balance(caller, tokenID); got != 9 {
		t.Fatalf("caller token balance: got %d", got)
	}
	if got := sdb.GetTRC10Balance(dest, tokenID); got != 1 {
		t.Fatalf("dest token balance: got %d", got)
	}
	if got, want := uint64(100_000)-contract.Energy, uint64(6764); got != want {
		t.Fatalf("energy used: got %d, want %d", got, want)
	}
}

func TestCallTokenToExistingCodeInsufficientBalanceSkipsJavaSurcharge(t *testing.T) {
	const tokenID = int64(1_000_002)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	dest := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(caller)
	sdb.GetOrCreateAccount(dest)
	sdb.SetCode(dest, []byte{byte(STOP)})

	code := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH3), 0x0f, 0x42, 0x42, // tokenId = 1000002
		byte(PUSH1), 0x01, // tokenValue
		byte(PUSH20),
	}
	code = append(code, dest[1:]...)
	code = append(code,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALLTOKEN),
		byte(STOP),
	)
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := sdb.GetTRC10Balance(dest, tokenID); got != 0 {
		t.Fatalf("dest token balance: got %d", got)
	}
	if got, want := uint64(100_000)-contract.Energy, uint64(6764); got != want {
		t.Fatalf("energy used: got %d, want %d", got, want)
	}
}

func TestCallTokenToSelfReturnsTransferFailed(t *testing.T) {
	const tokenID = int64(1_000_002)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	sdb.GetOrCreateAccount(caller)
	sdb.AddTRC10Balance(caller, tokenID, 10)

	code := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH3), 0x0f, 0x42, 0x42, // tokenId = 1000002
		byte(PUSH1), 0x01, // tokenValue
		byte(PUSH20),
	}
	code = append(code, caller[1:]...)
	code = append(code,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALLTOKEN),
		byte(STOP),
	)
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != ErrTokenTransferFailed {
		t.Fatalf("Run error: got %v, want %v", err, ErrTokenTransferFailed)
	}
	if got := sdb.GetTRC10Balance(caller, tokenID); got != 10 {
		t.Fatalf("caller token balance changed: got %d", got)
	}
}

func TestCallValueToSelfReturnsTransferFailed(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := tcommon.Address{0x41, 0x11}
	sdb.GetOrCreateAccount(caller)
	sdb.AddBalance(caller, 10)

	if _, _, err := tvm.Call(caller, caller, nil, 100_000, 1); err != ErrTransferFailed {
		t.Fatalf("Call error: got %v, want %v", err, ErrTransferFailed)
	}
}

func TestCallTokenValueOutOfLongRangeReturnsTransferFailed(t *testing.T) {
	const tokenID = int64(1_000_002)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	dest := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(caller)
	sdb.AddTRC10Balance(caller, tokenID, 10)

	code := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH3), 0x0f, 0x42, 0x42, // tokenId = 1000002
		byte(PUSH8), 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Long.MAX_VALUE + 1
		byte(PUSH20),
	}
	code = append(code, dest[1:]...)
	code = append(code,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALLTOKEN),
		byte(STOP),
	)
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != ErrEndowmentOutOfRange {
		t.Fatalf("Run error: got %v, want %v", err, ErrEndowmentOutOfRange)
	}
}

func TestCreateValueOutOfLongRangeReturnsTransferFailed(t *testing.T) {
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := tcommon.Address{0x41, 0x11}

	code := []byte{
		byte(PUSH1), 0x00, // size
		byte(PUSH1), 0x00, // offset
		byte(PUSH8), 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Long.MAX_VALUE + 1
		byte(CREATE),
		byte(STOP),
	}
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != ErrEndowmentOutOfRange {
		t.Fatalf("Run error: got %v, want %v", err, ErrEndowmentOutOfRange)
	}
	if tvm.Nonce != 0 {
		t.Fatalf("nonce after failed CREATE: got %d, want 0", tvm.Nonce)
	}
}

func TestCreate2ValueOutOfLongRangeReturnsTransferFailed(t *testing.T) {
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true}, nil)
	caller := tcommon.Address{0x41, 0x11}

	code := []byte{
		byte(PUSH1), 0x00, // salt
		byte(PUSH1), 0x00, // size
		byte(PUSH1), 0x00, // offset
		byte(PUSH8), 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Long.MAX_VALUE + 1
		byte(CREATE2),
		byte(STOP),
	}
	contract := NewContract(caller, caller, 0, 100_000)
	contract.SetCode(caller, code)

	if _, err := tvm.interpreter.Run(contract); err != ErrEndowmentOutOfRange {
		t.Fatalf("Run error: got %v, want %v", err, ErrEndowmentOutOfRange)
	}
	if tvm.Nonce != 0 {
		t.Fatalf("nonce after failed CREATE2: got %d, want 0", tvm.Nonce)
	}
}

func TestCallTokenToSelfInsufficientBalanceReturnsCallFailure(t *testing.T) {
	const tokenID = int64(1_000_002)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	sdb.GetOrCreateAccount(caller)

	if _, _, err := tvm.CallToken(caller, caller, nil, 100_000, 0, tokenID, 1); err != ErrInsufficientBalance {
		t.Fatalf("CallToken error: got %v, want %v", err, ErrInsufficientBalance)
	}
}

func TestCallValueToSelfInsufficientBalanceReturnsCallFailure(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := tcommon.Address{0x41, 0x11}
	sdb.GetOrCreateAccount(caller)

	if _, _, err := tvm.Call(caller, caller, nil, 100_000, 1); err != ErrInsufficientBalance {
		t.Fatalf("Call error: got %v, want %v", err, ErrInsufficientBalance)
	}
}

func TestCallTokenValueExposesNegativeMessageValue(t *testing.T) {
	const tokenID = int64(1_001_127)
	tokenValue := int64(-1000)

	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{TransferTrc10: true}, nil)
	caller := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x22}
	sdb.GetOrCreateAccount(caller)
	sdb.SetCode(contractAddr, []byte{
		byte(CALLTOKENVALUE),
		byte(PUSH1), 0x00,
		byte(SSTORE),
		byte(STOP),
	})

	if _, _, err := tvm.CallToken(caller, contractAddr, nil, 100_000, 0, tokenID, tokenValue); err != nil {
		t.Fatalf("CallToken: %v", err)
	}
	if got, want := sdb.GetState(contractAddr, hashFromUint64(0)), hashFromUint64(uint64(tokenValue)); got != want {
		t.Fatalf("slot0 CALLTOKENVALUE: got %x, want %x", got, want)
	}
}

func TestDelegateCallUsesCurrentContractBalanceForNestedTransfer(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{}, nil)

	owner := tcommon.Address{0x41, 0x01}
	proxy := tcommon.Address{0x41, 0x02}
	impl := tcommon.Address{0x41, 0x03}
	user := tcommon.Address{0x41, 0x04}

	sdb.GetOrCreateAccount(owner)
	sdb.GetOrCreateAccount(user)
	sdb.AddBalance(owner, 100)
	sdb.AddBalance(proxy, 1000)

	implCode := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH1), 0x01, // value
		byte(PUSH20),
	}
	implCode = append(implCode, user[1:]...)
	implCode = append(implCode,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALL),
		byte(STOP),
	)
	sdb.SetCode(impl, implCode)

	proxyCode := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH20),
	}
	proxyCode = append(proxyCode, impl[1:]...)
	proxyCode = append(proxyCode,
		byte(PUSH2), 0x75, 0x30, // gas
		byte(DELEGATECALL),
		byte(STOP),
	)

	tvm.RootTxID = tcommon.BytesToHash([]byte("root"))
	contract := NewContract(owner, proxy, 2, 100_000)
	contract.InternalTxHash = tvm.RootTxID
	contract.SetCode(proxy, proxyCode)
	if _, err := tvm.runContract(contract); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := sdb.GetBalance(owner); got != 100 {
		t.Fatalf("owner balance: got %d, want 100", got)
	}
	if got := sdb.GetBalance(proxy); got != 999 {
		t.Fatalf("proxy balance: got %d, want 999", got)
	}
	if got := sdb.GetBalance(user); got != 1 {
		t.Fatalf("user balance: got %d, want 1", got)
	}
	if len(tvm.InternalTransactions) < 1 {
		t.Fatal("missing delegate internal transaction")
	}
	if got := tvm.InternalTransactions[0].CallValueInfo[0].CallValue; got != 0 {
		t.Fatalf("delegate internal transaction value: got %d, want 0", got)
	}
}

func hashFromUint64(n uint64) tcommon.Hash {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return tcommon.BytesToHash(b[:])
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
