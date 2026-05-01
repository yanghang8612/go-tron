package vm

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"golang.org/x/crypto/sha3"
)

func TestIntegrationDeployAndCall(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	owner := tcommon.Address{0x41, 0x01}
	sdb.AddBalance(owner, 1_000_000_000_000)

	// Simple runtime code:
	// If calldatasize >= 32:
	//   CALLDATALOAD(0) → PUSH1 0 → SSTORE  (store calldata to slot 0)
	//   STOP
	// Else:
	//   PUSH1 0 → SLOAD → PUSH1 0 → MSTORE → PUSH1 32 → PUSH1 0 → RETURN
	runtime := []byte{
		byte(PUSH1), 0x20,   // 0x00-0x01: push 32
		byte(CALLDATASIZE),  // 0x02: push calldatasize
		byte(LT),            // 0x03: calldatasize < 32 ?
		byte(PUSH1), 0x0f,   // 0x04-0x05: jump target for GET
		byte(JUMPI),         // 0x06
		// SET path (calldatasize >= 32)
		byte(PUSH1), 0x00,   // 0x07-0x08
		byte(CALLDATALOAD),  // 0x09
		byte(PUSH1), 0x00,   // 0x0a-0x0b
		byte(SSTORE),        // 0x0c
		byte(STOP),          // 0x0d
		byte(STOP),          // 0x0e: padding
		// GET path at 0x0f
		byte(JUMPDEST),      // 0x0f
		byte(PUSH1), 0x00,   // 0x10-0x11
		byte(SLOAD),         // 0x12
		byte(PUSH1), 0x00,   // 0x13-0x14
		byte(MSTORE),        // 0x15
		byte(PUSH1), 0x20,   // 0x16-0x17
		byte(PUSH1), 0x00,   // 0x18-0x19
		byte(RETURN),        // 0x1a
	}

	// Init code: CODECOPY runtime to memory, RETURN it
	runtimeLen := len(runtime)
	initCode := []byte{
		byte(PUSH1), byte(runtimeLen), // size
		byte(DUP1),                    // dup for RETURN
		byte(PUSH1), 0x00,             // placeholder: codeOffset (= len(initCode))
		byte(PUSH1), 0x00,             // memOffset
		byte(CODECOPY),                // copy runtime to memory
		byte(PUSH1), byte(runtimeLen), // size for RETURN
		byte(PUSH1), 0x00,             // offset for RETURN
		byte(RETURN),
	}
	initCode[4] = byte(len(initCode)) // fix the CODECOPY source offset

	deployCode := append(initCode, runtime...)

	evm := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, TVMConfig{})

	// Deploy
	_, contractAddr, energyLeft, err := evm.Create(owner, deployCode, 1000000, 0)
	if err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	t.Logf("Contract deployed at %x, energy remaining: %d", contractAddr[:6], energyLeft)

	code := sdb.GetCode(contractAddr)
	if len(code) == 0 {
		t.Fatal("no code stored at contract address")
	}
	if len(code) != runtimeLen {
		t.Fatalf("stored code length %d != runtime length %d", len(code), runtimeLen)
	}

	// Call SET: store value 42
	var setInput [32]byte
	binary.BigEndian.PutUint64(setInput[24:], 42)
	_, _, err = evm.Call(owner, contractAddr, setInput[:], 1000000, 0)
	if err != nil {
		t.Fatalf("SET call failed: %v", err)
	}

	// Call GET: should return 42
	ret, _, err := evm.Call(owner, contractAddr, nil, 1000000, 0)
	if err != nil {
		t.Fatalf("GET call failed: %v", err)
	}
	if len(ret) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(ret))
	}
	val := binary.BigEndian.Uint64(ret[24:])
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
	t.Logf("GET returned %d", val)
}

func TestIntegrationStaticCall(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	owner := tcommon.Address{0x41, 0x01}
	sdb.AddBalance(owner, 1_000_000_000_000)
	contract := tcommon.Address{0x41, 0x02}

	// Simple code: PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		byte(PUSH1), 0x42,
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	}
	sdb.SetCode(contract, code)

	evm := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, TVMConfig{})
	ret, _, err := evm.StaticCall(owner, contract, nil, 1000000)
	if err != nil {
		t.Fatalf("static call failed: %v", err)
	}
	if len(ret) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(ret))
	}
	if ret[31] != 0x42 {
		t.Fatalf("expected 0x42, got 0x%x", ret[31])
	}
}

func TestIntegrationSHA3(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}

	// Keccak256 of empty data, store to memory, return
	code := []byte{
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(SHA3),
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	}
	sdb.SetCode(contractAddr, code)

	evm := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, TVMConfig{})
	ret, _, err := evm.StaticCall(owner, contractAddr, nil, 1000000)
	if err != nil {
		t.Fatalf("sha3 call failed: %v", err)
	}

	h := sha3.NewLegacyKeccak256()
	expected := h.Sum(nil)
	if string(ret) != string(expected) {
		t.Fatalf("sha3 mismatch:\n  got:  %x\n  want: %x", ret, expected)
	}
}

func TestIntegrationValueTransfer(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	sdb.AddBalance(owner, 1_000_000)

	// Simple code: STOP (accept value)
	sdb.SetCode(contractAddr, []byte{byte(STOP)})

	evm := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, TVMConfig{})
	_, _, err = evm.Call(owner, contractAddr, nil, 100000, 500)
	if err != nil {
		t.Fatalf("value transfer failed: %v", err)
	}

	if sdb.GetBalance(contractAddr) != 500 {
		t.Fatalf("contract should have 500, got %d", sdb.GetBalance(contractAddr))
	}
	if sdb.GetBalance(owner) != 999500 {
		t.Fatalf("owner should have 999500, got %d", sdb.GetBalance(owner))
	}
}

// TestSolidityStorageContract tests the Solidity 0.8.19 Storage contract bytecode
// that is used in the system test F8 flows (F8/1 deploy, F8/2 trigger, F8/3 constant).
// This test exercises the full deploy→set→get lifecycle through TVM.
func TestSolidityStorageContract(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	owner := tcommon.Address{0x41, 0x01}
	sdb.AddBalance(owner, 1_000_000_000_000)

	// Solidity 0.8.19 compiled Storage contract (set/get uint256).
	// 368 bytes total: 32-byte init code + 336-byte runtime.
	deployHex := "608060405234801561001057600080fd5b50610150806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c806360fe47b11461003b5780636d4ce63c14610057575b600080fd5b610055600480360381019061005091906100c3565b610075565b005b61005f61007f565b60405161006c91906100ff565b60405180910390f35b8060008190555050565b60008054905090565b600080fd5b6000819050919050565b6100a08161008d565b81146100ab57600080fd5b50565b6000813590506100bd81610097565b92915050565b6000602082840312156100d9576100d8610088565b5b60006100e7848285016100ae565b91505092915050565b6100f98161008d565b82525050565b600060208201905061011460008301846100f0565b9291505056fea2646970667358221220abcdef1234567890fedcba9876543210abcdef0011223344556677889900aabbcc64736f6c63430008130033"
	deployCode, err := hex.DecodeString(deployHex)
	if err != nil {
		t.Fatalf("decode bytecode: %v", err)
	}

	cfg := TVMConfig{Constantinople: true}
	evm := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, cfg)

	_, contractAddr, _, vmErr := evm.Create(owner, deployCode, 10_000_000, 0)
	if vmErr != nil {
		t.Fatalf("deploy failed: %v", vmErr)
	}
	t.Logf("deployed at %x", contractAddr)

	code := sdb.GetCode(contractAddr)
	if len(code) != 336 {
		t.Errorf("runtime code length: got %d, want 336", len(code))
	}

	// set(42): selector keccak256("set(uint256)")[:4] = 0x60fe47b1
	setInput := make([]byte, 36)
	setInput[0], setInput[1], setInput[2], setInput[3] = 0x60, 0xfe, 0x47, 0xb1
	setInput[35] = 42
	evm2 := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, cfg)
	_, _, vmErr = evm2.Call(owner, contractAddr, setInput, 10_000_000, 0)
	if vmErr != nil {
		t.Fatalf("set(42) failed: %v", vmErr)
	}

	// get(): selector keccak256("get()")[:4] = 0x6d4ce63c
	getInput := []byte{0x6d, 0x4c, 0xe6, 0x3c}
	evm3 := NewTVM(sdb, nil, owner, 1, 1000, tcommon.Address{}, 1, cfg)
	ret, _, vmErr := evm3.Call(owner, contractAddr, getInput, 10_000_000, 0)
	if vmErr != nil {
		t.Fatalf("get() failed: %v", vmErr)
	}
	if len(ret) < 32 {
		t.Fatalf("get() returned %d bytes, want 32", len(ret))
	}
	if ret[31] != 42 {
		t.Fatalf("get() returned %d, want 42", ret[31])
	}
}
