package vm

import (
	"bytes"
	"encoding/hex"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEnvironmentAddressOpcodesUseTwentyByteWords(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	origin := tcommon.Address{0x41, 0x10, 0x11, 0x12}
	caller := tcommon.Address{0x41, 0x20, 0x21, 0x22}
	contractAddr := tcommon.Address{0x41, 0x30, 0x31, 0x32}
	code := []byte{
		byte(ADDRESS), byte(PUSH1), 0x00, byte(MSTORE),
		byte(CALLER), byte(PUSH1), 0x20, byte(MSTORE),
		byte(ORIGIN), byte(PUSH1), 0x40, byte(MSTORE),
		byte(PUSH1), 0x60, byte(PUSH1), 0x00, byte(RETURN),
	}
	sdb.SetCode(contractAddr, code)

	tvm := NewTVM(sdb, nil, origin, 1, 1000, tcommon.Address{}, 1, TVMConfig{})
	ret, _, err := tvm.StaticCall(caller, contractAddr, nil, 1_000_000)
	if err != nil {
		t.Fatalf("StaticCall: %v", err)
	}
	assertAddressWord(t, ret[0:32], contractAddr)
	assertAddressWord(t, ret[32:64], caller)
	assertAddressWord(t, ret[64:96], origin)
}

func TestCreateReturnsTwentyByteAddressWord(t *testing.T) {
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	caller := tcommon.Address{0x41, 0x01}
	parent := tcommon.Address{0x41, 0x02}

	mem := newMemory()
	initCode := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(RETURN)}
	mem.set(0, uint64(len(initCode)), initCode)

	stack := newStack()
	stack.push(uint256.NewInt(uint64(len(initCode))))
	stack.push(uint256.NewInt(0))
	stack.push(uint256.NewInt(0))
	contract := NewContract(caller, parent, 0, 1_000_000)

	if _, err := opCreate(nil, tvm.interpreter, contract, mem, stack); err != nil {
		t.Fatalf("opCreate: %v", err)
	}
	result := stack.pop()
	addr := uint256ToAddress(&result)
	want := addressToUint256(addr)
	if result.Cmp(&want) != 0 {
		t.Fatalf("CREATE stack result contains TRON prefix: got %064x want %064x", result.Bytes32(), want.Bytes32())
	}
}

func TestCreateAddressMatchesJavaRootTransactionNonce(t *testing.T) {
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{}, nil)
	tvm.SetRootTransactionID(tcommon.HexToHash("07b254b84ec06839e28295f1679f1e22bd96d82ae83e2e5df6df6c69c859f2da"))
	caller := tcommon.Address{0x41, 0x02}
	code := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(RETURN)}

	_, addr, _, err := tvm.Create(caller, code, 1_000_000, 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	want := mustAddressFromHex(t, "4164c373cdf8bd8170e45c1081807ad2ecb20810b2")
	if addr != want {
		t.Fatalf("CREATE address: got %s want %s", addr.Hex(), want.Hex())
	}
	if tvm.Nonce != 1 {
		t.Fatalf("nonce after CREATE: got %d want 1", tvm.Nonce)
	}
}

func TestCreate2AddressMatchesJavaFormula(t *testing.T) {
	tvm, _, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true}, nil)
	caller := mustAddressFromHex(t, "410102030405060708090a0b0c0d0e0f1011121314")
	code := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(RETURN)}
	var salt [32]byte
	salt[30], salt[31] = 0x12, 0x32

	_, addr, _, err := tvm.Create2(caller, code, 1_000_000, 0, salt)
	if err != nil {
		t.Fatalf("Create2: %v", err)
	}
	want := mustAddressFromHex(t, "418c8494edfa05ddfebe8e7b11cede2610dfbb3efc")
	if addr != want {
		t.Fatalf("CREATE2 address: got %s want %s", addr.Hex(), want.Hex())
	}
}

func TestCreate2CollisionConsumesChildEnergyAndReturnsZero(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true}, nil)
	caller := mustAddressFromHex(t, "410102030405060708090a0b0c0d0e0f1011121314")
	code := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(RETURN)}
	var salt [32]byte
	salt[30], salt[31] = 0x12, 0x32
	existing := mustAddressFromHex(t, "418c8494edfa05ddfebe8e7b11cede2610dfbb3efc")
	existingCode := []byte{byte(STOP)}
	sdb.SetCode(existing, existingCode)

	ret, addr, remaining, err := tvm.Create2(caller, code, 1_000_000, 0, salt)
	if err != ErrContractAlreadyExists {
		t.Fatalf("Create2 error: got %v, want %v", err, ErrContractAlreadyExists)
	}
	if ret != nil {
		t.Fatalf("return data: got %x, want nil", ret)
	}
	if addr != (tcommon.Address{}) {
		t.Fatalf("created address: got %s, want zero", addr.Hex())
	}
	if remaining != 0 {
		t.Fatalf("remaining energy: got %d, want 0", remaining)
	}
	if tvm.Nonce != 1 {
		t.Fatalf("nonce after CREATE2 collision: got %d, want 1", tvm.Nonce)
	}
	if got := sdb.GetCode(existing); !bytes.Equal(got, existingCode) {
		t.Fatalf("existing code changed: got %x want %x", got, existingCode)
	}
}

func TestCreate2StoresInternalContractMetadata(t *testing.T) {
	tvm, sdb, _ := newTestTVMForCreate(t, TVMConfig{Constantinople: true, Compatibility: true}, nil)
	rootTxID := tcommon.HexToHash("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	tvm.SetRootTransactionID(rootTxID)
	caller := mustAddressFromHex(t, "410102030405060708090a0b0c0d0e0f1011121314")
	code := []byte{byte(PUSH1), 0x01, byte(PUSH1), 0x1f, byte(RETURN)}
	var salt [32]byte
	salt[30], salt[31] = 0x12, 0x32

	_, addr, _, err := tvm.Create2(caller, code, 1_000_000, 0, salt)
	if err != nil {
		t.Fatalf("Create2: %v", err)
	}
	acc := sdb.GetAccount(addr)
	if acc == nil {
		t.Fatal("created account missing")
	}
	if acc.Type() != corepb.AccountType_Contract {
		t.Fatalf("created account type: got %v want %v", acc.Type(), corepb.AccountType_Contract)
	}
	if acc.AccountName() != "CreatedByContract" {
		t.Fatalf("created account name: got %q want CreatedByContract", acc.AccountName())
	}

	meta := sdb.GetContract(addr)
	if meta == nil {
		t.Fatal("created contract metadata missing")
	}
	if !bytes.Equal(meta.ContractAddress, addr.Bytes()) {
		t.Fatalf("contract address metadata: got %x want %x", meta.ContractAddress, addr.Bytes())
	}
	if !bytes.Equal(meta.OriginAddress, caller.Bytes()) {
		t.Fatalf("origin address metadata: got %x want %x", meta.OriginAddress, caller.Bytes())
	}
	if meta.ConsumeUserResourcePercent != 100 {
		t.Fatalf("consume user resource percent: got %d want 100", meta.ConsumeUserResourcePercent)
	}
	if meta.Version != 1 {
		t.Fatalf("contract version: got %d want 1", meta.Version)
	}
	if !bytes.Equal(meta.TrxHash, rootTxID.Bytes()) {
		t.Fatalf("CREATE2 trx_hash: got %x want %x", meta.TrxHash, rootTxID.Bytes())
	}
}

func assertAddressWord(t *testing.T, word []byte, addr tcommon.Address) {
	t.Helper()
	want := make([]byte, 32)
	copy(want[12:], addr[1:])
	if !bytes.Equal(word, want) {
		t.Fatalf("address word mismatch:\n got  %x\n want %x", word, want)
	}
}

func mustAddressFromHex(t *testing.T, s string) tcommon.Address {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return tcommon.BytesToAddress(b)
}
