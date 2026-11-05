package vm

import (
	"bytes"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// Mainnet H58,435,297 tx 29f9367a... catches a failed delegated frame after
// that frame has successfully called a pool and token contracts. The outer
// transaction is SUCCESS, so only reverting the entry CALL cannot undo the
// failed delegate's writes, transfers and logs. Program.callToAddress gives
// CALLCODE/DELEGATECALL a child Repository and commits it only on success.
func TestCaughtDelegatedFrameRollsBackEffects(t *testing.T) {
	for _, opcode := range []OpCode{DELEGATECALL, CALLCODE} {
		for _, ending := range []string{"revert", "invalid", "out-of-energy", "success"} {
			for _, traced := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/traced=%t", opcode, ending, traced), func(t *testing.T) {
					cfg := TVMConfig{Constantinople: true, Solidity059: true}
					if traced {
						cfg.Tracer = &recorderTracer{}
					}
					evm := newTestEVMWithConfig(t, cfg)
					caller := tcommon.Address{0x41, 0x81}
					context := tcommon.Address{0x41, 0x82}
					implementation := tcommon.Address{0x41, 0x83}
					leaf := tcommon.Address{0x41, 0x84}
					evm.StateDB.GetOrCreateAccount(caller)
					evm.StateDB.GetOrCreateAccount(context)
					evm.StateDB.GetOrCreateAccount(leaf)
					evm.StateDB.AddBalance(context, 100)
					evm.StateDB.AddBalance(leaf, 10)

					// Parent's own write/log must survive a caught delegated failure.
					parent := []byte{byte(PUSH1), 0x11, byte(PUSH1), 0, byte(SSTORE)}
					parent = append(parent, delegatedRollbackLog(0xa1)...)
					parent = append(parent, delegatedRollbackInvoke(opcode, implementation, 0, 200_000, 32)...)
					parent = append(parent, byte(PUSH1), 1, byte(SSTORE)) // status
					parent = append(parent, byte(RETURNDATASIZE), byte(PUSH1), 2, byte(SSTORE))
					parent = append(parent, delegatedRollbackLog(0xa2)...)
					parent = append(parent, byte(PUSH1), 32, byte(PUSH1), 0, byte(RETURN))
					evm.StateDB.SetCode(context, parent)

					child := []byte{byte(PUSH1), 0x22, byte(PUSH1), 0, byte(SSTORE)}
					child = append(child, delegatedRollbackLog(0xb1)...)
					child = append(child, delegatedRollbackInvoke(CALL, leaf, 5, 50_000, 0)...)
					child = append(child, byte(POP))
					switch ending {
					case "revert", "success":
						child = append(child, byte(PUSH2), 'o', 'k', byte(PUSH1), 0, byte(MSTORE))
						child = append(child, byte(PUSH1), 2, byte(PUSH1), 30)
						if ending == "revert" {
							child = append(child, byte(REVERT))
						} else {
							child = append(child, byte(RETURN))
						}
					case "invalid":
						child = append(child, 0xfe)
					case "out-of-energy":
						loop := len(child)
						child = append(child, byte(JUMPDEST), byte(PUSH2), byte(loop>>8), byte(loop), byte(JUMP))
					}
					evm.StateDB.SetCode(implementation, child)
					leafCode := []byte{byte(PUSH1), 0x33, byte(PUSH1), 0, byte(SSTORE)}
					leafCode = append(leafCode, delegatedRollbackLog(0xc1)...)
					leafCode = append(leafCode, byte(STOP))
					evm.StateDB.SetCode(leaf, leafCode)

					ret, _, err := evm.Call(caller, context, nil, 1_000_000, 0)
					if err != nil {
						t.Fatalf("outer call must catch delegated failure: %v", err)
					}
					success := ending == "success"
					wantParent, wantLeaf := byte(0x11), byte(0)
					wantContextBalance, wantLeafBalance := int64(100), int64(10)
					wantTopics := []byte{0xa1, 0xa2}
					if success {
						wantParent, wantLeaf = 0x22, 0x33
						wantContextBalance, wantLeafBalance = 95, 15
						wantTopics = []byte{0xa1, 0xb1, 0xc1, 0xa2}
					}
					if got := evm.StateDB.GetState(context, tcommon.Hash{}); got != (tcommon.Hash{31: wantParent}) {
						t.Errorf("context storage: got %x, want %x", got[:], wantParent)
					}
					if got := evm.StateDB.GetState(leaf, tcommon.Hash{}); got != (tcommon.Hash{31: wantLeaf}) {
						t.Errorf("successful descendant storage escaped failed delegate: %x", got[:])
					}
					if evm.StateDB.GetBalance(context) != wantContextBalance || evm.StateDB.GetBalance(leaf) != wantLeafBalance {
						t.Errorf("descendant transfer: context=%d leaf=%d", evm.StateDB.GetBalance(context), evm.StateDB.GetBalance(leaf))
					}
					wantStatus := tcommon.Hash{}
					if success {
						wantStatus[31] = 1
					}
					if got := evm.StateDB.GetState(context, tcommon.Hash{31: 1}); got != wantStatus {
						t.Errorf("delegate status: got %x, want %x", got[:], wantStatus[:])
					}
					wantReturnSize := byte(0)
					wantReturn := make([]byte, 32)
					if ending == "revert" || success {
						wantReturnSize = 2
						copy(wantReturn, "ok")
					}
					if got := evm.StateDB.GetState(context, tcommon.Hash{31: 2}); got != (tcommon.Hash{31: wantReturnSize}) {
						t.Errorf("return-data size: %x", got[:])
					}
					if !bytes.Equal(ret, wantReturn) {
						t.Errorf("return data: got %x, want %x", ret, wantReturn)
					}
					if len(evm.Logs) != len(wantTopics) {
						t.Errorf("logs: got %d, want %d", len(evm.Logs), len(wantTopics))
					} else {
						for i, topic := range wantTopics {
							got := evm.Logs[i].Topic(0)
							want := tcommon.Hash{31: topic}
							if !bytes.Equal(got, want[:]) {
								t.Errorf("log[%d] topic: %x", i, got[:])
							}
						}
					}
					for _, internal := range evm.InternalTransactions {
						if internal.Rejected == success {
							t.Errorf("internal transaction rejected=%t, success=%t", internal.Rejected, success)
						}
					}
					if err := evm.StateDB.Error(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func delegatedRollbackLog(topic byte) []byte {
	return []byte{byte(PUSH1), topic, byte(PUSH1), 0, byte(PUSH1), 0, byte(LOG1)}
}

func delegatedRollbackInvoke(op OpCode, target tcommon.Address, value byte, energy uint32, returnSize byte) []byte {
	code := []byte{byte(PUSH1), returnSize, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0}
	if op != DELEGATECALL {
		code = append(code, byte(PUSH1), value)
	}
	code = append(code, byte(PUSH21))
	code = append(code, target[:]...)
	return append(code, byte(PUSH3), byte(energy>>16), byte(energy>>8), byte(energy), byte(op))
}
