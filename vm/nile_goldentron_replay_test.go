package vm

// Faithful replay of the Nile block 11,359,658 stall (2020-12-03):
// tx 4b48c007ae6eba93e6d69737012a54466e1b0f3a8cba0988a8da3661383b32f4
// ("GoldenTron".injectUserRegiste(address[9])) — java-tron canonical result
// REVERT with energy_usage_total=390,378 of the 500,000 fee-limit budget
// (fee_limit 20 TRX at 40 sun/energy, zero stake coverage), 66 internal
// self-calls all rejected. The contract was deployed 63 blocks earlier
// (block 11,359,595, tx 58b23b926d1a0f6375116e8f9e029a9ef880a84fdf63b35dd6faafd02fa27637,
// SUCCESS, energy_usage_total=1,624,169 of the 2,500,000 budget) with no
// other transactions touching it in between, so constructor output is the
// complete pre-state of the failing call.
//
// The contract's findUpline helper recurses via external self-STATICCALLs.
// java-tron terminates the recursion at Program.MAX_DEPTH = 64 (push 0 +
// refund), after which the contract unwinds to a top-level REVERT. gtron's
// geth-derived maxCallDepth of 1024 let the recursion run ~10× deeper; with
// no 63/64 reservation each frame forwards everything, so the bottom OOE
// cascaded up and flipped the result to OUT_OF_ENERGY. The java oracle
// values pin the fix: 2 findFreeReferrer calls + exactly 64 recursion
// frames = 66 internal transactions, REVERT at 390,378.
//
// Era flags (Nile 2020-12-03): TransferTrc10/Constantinople/Solidity059/
// ShieldedTRC20/Istanbul active (AllowTvmIstanbul approved 2020-10-28 via
// proposal 5023); Freeze/Vote/London and later inactive.

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func mustHexFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func hexAddr(t *testing.T, s string) tcommon.Address {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return tcommon.BytesToAddress(b)
}

func TestNileGoldenTronInjectUserRegisteReplay(t *testing.T) {
	owner := hexAddr(t, "4182956e1b8f23c7da3a0b9e88c9626147b9118c6f")
	contractAddr := hexAddr(t, "41f8a43db88d9e0d6748d9129d9aa37c4eea558655")

	cfg := TVMConfig{
		TransferTrc10:  true,
		Constantinople: true,
		Solidity059:    true,
		ShieldedToken:  true,
		Istanbul:       true,
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}

	// --- Tx 1: CreateSmartContract @ block 11,359,595 ---
	creation := mustHexFile(t, "testdata/nile_goldentron_create.hex")
	deployTVM := NewTVM(sdb, nil, owner, 11359595, 1606992009000,
		hexAddr(t, "410c4c64201f66a32719cf9ab4e6f4aed6330b48bd"), 1, cfg)
	deployTVM.SetRootTransactionID(tcommon.HexToHash("58b23b926d1a0f6375116e8f9e029a9ef880a84fdf63b35dd6faafd02fa27637"))

	meta := &contractpb.SmartContract{
		OriginAddress:              owner.Bytes(),
		ContractAddress:            contractAddr.Bytes(),
		ConsumeUserResourcePercent: 100,
		OriginEnergyLimit:          1000000,
		Name:                       "GoldenTron",
	}
	const deployLimit = 2_500_000 // fee_limit 100 TRX / 40 sun-per-energy
	_, _, deployLeft, err := deployTVM.CreateAtWithTokenAndContract(owner, contractAddr, creation, deployLimit, 0, 0, 0, meta)
	if err != nil {
		t.Fatalf("deploy failed: %v", err)
	}
	if deployUsed := uint64(deployLimit) - deployLeft; deployUsed != 1624169 {
		t.Errorf("deploy energy mismatch: got %d want 1624169 (java receipt)", deployUsed)
	}

	// --- Tx 2: injectUserRegiste(address[9]) @ block 11,359,658 ---
	calldata := mustHexFile(t, "testdata/nile_goldentron_inject_calldata.hex")
	callTVM := NewTVM(sdb, nil, owner, 11359658, 1606992198000,
		hexAddr(t, "4121f8ff8bfda1b50ea50a75bf91c000eb32c89da8"), 1, cfg)
	callTVM.SetRootTransactionID(tcommon.HexToHash("4b48c007ae6eba93e6d69737012a54466e1b0f3a8cba0988a8da3661383b32f4"))

	const callLimit = 500_000 // fee_limit 20 TRX / 40 sun-per-energy
	ret, callLeft, err := callTVM.Call(owner, contractAddr, calldata, callLimit, 0)

	if err != ErrExecutionReverted {
		t.Errorf("result mismatch: got err=%v want ErrExecutionReverted (java contractRet REVERT)", err)
	}
	if callUsed := uint64(callLimit) - callLeft; callUsed != 390378 {
		t.Errorf("energy mismatch: got %d want 390378 (java receipt energy_usage_total)", callUsed)
	}
	if len(ret) != 0 {
		t.Errorf("revert data mismatch: got %d bytes want 0 (java contractResult empty)", len(ret))
	}
	if n := len(callTVM.InternalTransactions); n != 66 {
		t.Errorf("internal tx count mismatch: got %d want 66 (java internal_transactions)", n)
	}
	for i, it := range callTVM.InternalTransactions {
		if !it.Rejected {
			t.Errorf("internal tx %d not rejected; java marks all 66 rejected", i)
		}
	}
}

// TestCallDepthLimitMatchesJavaTron pins the TVM call-depth boundary in
// isolation: java-tron Program.MAX_DEPTH = 64 refuses a spawn when the
// current frame's deep == 64, so an unbounded self-recursion creates exactly
// 64 child frames; the refused 65th CALL pushes 0, refunds the forwarded
// energy, and execution unwinds successfully.
func TestCallDepthLimitMatchesJavaTron(t *testing.T) {
	evm := newTestEVM(t)
	caller := hexAddr(t, "41000000000000000000000000000000000000aaaa")
	addr := hexAddr(t, "41000000000000000000000000000000000000bbbb")

	// PUSH1 0 (retsz, retoff, insz, inoff, value), ADDRESS, GAS, CALL, STOP —
	// every frame forwards all remaining energy to a fresh self-call.
	code := []byte{
		0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00,
		0x30, 0x5a, 0xf1, 0x00,
	}
	evm.StateDB.CreateAccount(addr, corepb.AccountType_Contract)
	evm.StateDB.SetCode(addr, code)

	_, _, err := evm.Call(caller, addr, nil, 1_000_000, 0)
	if err != nil {
		t.Fatalf("recursion should unwind successfully, got %v", err)
	}
	if n := len(evm.InternalTransactions); n != 64 {
		t.Errorf("child frame count: got %d want 64 (java Program.MAX_DEPTH)", n)
	}
}
