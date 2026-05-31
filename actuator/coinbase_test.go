package actuator

import (
	"bytes"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TestVMActuatorCoinbaseReturnsBlockWitness guards the COINBASE opcode against
// regressing to zero. java-tron's COINBASE returns the block's witness address
// (ProgramInvokeFactory derives it from block.witnessAddress); gtron must thread
// ctx.Coinbase = block.WitnessAddress() into the TVM. A zero coinbase silently
// corrupts block.coinbase-based randomness (e.g. Nile block 7,799,482 CoinFlip:
// keccak(coinbase, difficulty, timestamp) → wrong flip → REVERT vs SUCCESS).
func TestVMActuatorCoinbaseReturnsBlockWitness(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	witness := tcommon.Address{0x41, 0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03}

	// COINBASE; PUSH1 0; MSTORE; PUSH1 32; PUSH1 0; RETURN
	code := []byte{0x41, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}

	tsc := &contractpb.TriggerSmartContract{OwnerAddress: owner[:], ContractAddress: contractAddr[:]}
	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 10_000_000)
	enableVM(ctx)
	ctx.Coinbase = witness // the block producer, as threaded from block.WitnessAddress()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, 100_000_000)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner[:],
		ContractAddress: contractAddr[:],
	})
	ctx.State.SetCode(contractAddr, code)

	result, err := (&VMActuator{}).Execute(ctx)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var want [32]byte
	copy(want[32-len(witness):], witness[:]) // witness right-aligned in a word
	if !bytes.Equal(result.ContractResult, want[:]) {
		t.Fatalf("COINBASE = %x, want block witness %x (all-zero = the pre-fix bug)", result.ContractResult, want[:])
	}
}
