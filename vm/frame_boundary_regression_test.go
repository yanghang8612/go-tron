package vm

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// These regressions assert java-tron's child Repository publication and
// message-endowment rules for caught frame failures.
func auditMessageCall(op OpCode, target tcommon.Address, value byte, energy uint32) []byte {
	code := []byte{byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0, byte(PUSH1), 0}
	if op == CALL || op == CALLCODE {
		code = append(code, byte(PUSH1), value)
	}
	code = append(code, byte(PUSH20))
	code = append(code, target[1:]...)
	return append(code, byte(PUSH3), byte(energy>>16), byte(energy>>8), byte(energy), byte(op))
}

func TestFailedStaticCallDynamicState(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		for _, traced := range []bool{false, true} {
			for _, ending := range []string{"revert", "invalid", "write-protection", "out-of-energy", "success"} {
				t.Run(fmt.Sprintf("dynamic=%v/trace=%v/%s", dynamic, traced, ending), func(t *testing.T) {
					cfg := TVMConfig{DynamicEnergy: dynamic}
					if traced {
						cfg.Tracer = &recorderTracer{}
					}
					tvm, st, _ := newTestTVMForCreate(t, cfg, func(dp *state.DynamicProperties) {
						dp.SetCurrentCycleNumber(2)
						dp.SetDynamicEnergyThreshold(1_000_000)
						dp.SetDynamicEnergyIncreaseFactor(2000)
						dp.SetDynamicEnergyMaxFactor(34000)
					})
					caller, parent, child, leaf := tcommon.Address{0x41, 0xc0}, tcommon.Address{0x41, 0xc1}, tcommon.Address{0x41, 0xc2}, tcommon.Address{0x41, 0xc3}
					for _, addr := range []tcommon.Address{caller, parent, child, leaf} {
						st.CreateAccount(addr, corepb.AccountType_Contract)
					}
					before := make(map[tcommon.Address][]byte)
					for _, addr := range []tcommon.Address{child, leaf} {
						cs := types.NewContractState(1)
						cs.AddEnergyUsage(17)
						if err := st.WriteContractState(addr, cs); err != nil {
							t.Fatal(err)
						}
						before[addr], _ = cs.Bytes()
					}
					st.SetCode(leaf, []byte{byte(PUSH1), 1, byte(POP), byte(STOP)})
					code := append(auditMessageCall(STATICCALL, leaf, 0, 10_000), byte(POP))
					switch ending {
					case "revert":
						code = append(code, byte(PUSH1), 0, byte(PUSH1), 0, byte(REVERT))
					case "invalid":
						code = append(code, 0xfe)
					case "write-protection":
						code = append(code, byte(PUSH1), 1, byte(PUSH1), 0, byte(SSTORE))
					case "out-of-energy":
						loop := byte(len(code))
						code = append(code, byte(JUMPDEST), byte(PUSH1), loop, byte(JUMP))
					case "success":
						code = append(code, byte(STOP))
					}
					st.SetCode(child, code)
					code = auditMessageCall(STATICCALL, child, 0, 100_000)
					st.SetCode(parent, append(code, byte(PUSH1), 0, byte(SSTORE), byte(STOP)))
					if _, _, err := tvm.Call(caller, parent, nil, 1_000_000, 0); err != nil {
						t.Fatalf("parent must catch static failure: %v", err)
					}
					wantStatus := tcommon.Hash{}
					if ending == "success" {
						wantStatus[31] = 1
					}
					if got := st.GetState(parent, tcommon.Hash{}); got != wantStatus {
						t.Fatalf("call status %x, want %x", got, wantStatus)
					}
					for _, addr := range []tcommon.Address{child, leaf} {
						cs := st.ReadContractState(addr)
						after, err := cs.Bytes()
						if err != nil {
							t.Fatal(err)
						}
						wantChange := dynamic && ending == "success"
						if gotChange := !bytes.Equal(before[addr], after); gotChange != wantChange {
							t.Errorf("static child %s state escaped failure: before=%x after=%x cycle=%d usage=%d wantChange=%v", addr.Hex(), before[addr], after, cs.UpdateCycle(), cs.EnergyUsage(), wantChange)
						}
					}
					if err := st.Error(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestCallCodeChecksEndowmentBalance(t *testing.T) {
	for _, precompile := range []bool{false, true} {
		for _, traced := range []bool{false, true} {
			for _, tc := range []struct {
				balance int64
				value   byte
			}{{0, 1}, {1, 2}, {1, 1}, {0, 0}} {
				t.Run(fmt.Sprintf("precompile=%v/trace=%v/balance=%d/value=%d", precompile, traced, tc.balance, tc.value), func(t *testing.T) {
					cfg := TVMConfig{}
					if traced {
						cfg.Tracer = &recorderTracer{}
					}
					tvm := newTestEVMWithConfig(t, cfg)
					caller, parent, target := tcommon.Address{0x41, 0xd0}, tcommon.Address{0x41, 0xd1}, tcommon.Address{0x41, 0xd2}
					for _, addr := range []tcommon.Address{caller, parent, target} {
						tvm.StateDB.CreateAccount(addr, corepb.AccountType_Contract)
					}
					tvm.StateDB.AddBalance(parent, tc.balance)
					tvm.StateDB.SetCode(target, []byte{byte(PUSH1), 0x44, byte(PUSH1), 0, byte(SSTORE), byte(STOP)})
					if precompile {
						target = addrFromUint(4)
					}
					code := auditMessageCall(CALLCODE, target, tc.value, 100_000)
					tvm.StateDB.SetCode(parent, append(code, byte(PUSH1), 1, byte(SSTORE), byte(STOP)))
					if _, _, err := tvm.Call(caller, parent, nil, 1_000_000, 0); err != nil {
						t.Fatal(err)
					}
					success := tc.balance >= int64(tc.value)
					wantStatus, wantWrite := tcommon.Hash{}, tcommon.Hash{}
					if success {
						wantStatus[31] = 1
						if !precompile {
							wantWrite[31] = 0x44
						}
					}
					if got := tvm.StateDB.GetState(parent, tcommon.Hash{31: 1}); got != wantStatus {
						t.Errorf("CALLCODE status=%d, want %d for balance=%d value=%d", got[31], wantStatus[31], tc.balance, tc.value)
					}
					if got := tvm.StateDB.GetState(parent, tcommon.Hash{}); got != wantWrite {
						t.Errorf("CALLCODE ran code despite insufficient balance: storage=%x want=%x", got[:], wantWrite[:])
					}
					if !success && len(tvm.InternalTransactions) != 0 {
						t.Errorf("insufficient-balance call created %d internal records; java exits before nonce/record creation", len(tvm.InternalTransactions))
					}
				})
			}
		}
	}
}

func TestCreateRevertEFExceptionOrdering(t *testing.T) {
	for _, kind := range []string{"CREATE", "CREATE2", "top-level"} {
		for _, london := range []bool{false, true} {
			for _, traced := range []bool{false, true} {
				for _, prefix := range []byte{0xee, 0xef} {
					for _, revert := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/london=%v/trace=%v/prefix=%x/revert=%v", kind, london, traced, prefix, revert), func(t *testing.T) {
							cfg := TVMConfig{Constantinople: true, London: london, MultiSign: true}
							if traced {
								cfg.Tracer = &recorderTracer{}
							}
							tvm := newTestEVMWithConfig(t, cfg)
							caller := tcommon.Address{0x41, 0xe0}
							tvm.StateDB.CreateAccount(caller, corepb.AccountType_Contract)
							code := []byte{byte(PUSH1), prefix, byte(PUSH1), 0, byte(MSTORE8), byte(PUSH1), 1, byte(PUSH1), 0, byte(RETURN)}
							if revert {
								code[len(code)-1] = byte(REVERT)
							}
							created := tvm.createAddress(0)
							if kind == "CREATE2" {
								created = create2Address(caller, code, [32]byte{1})
							} else if kind == "top-level" {
								created = tcommon.Address{0x41, 0xe1}
							}
							var remaining uint64
							var ret []byte
							var err error
							switch kind {
							case "CREATE":
								ret, _, remaining, err = tvm.Create(caller, code, 100_000, 0)
							case "CREATE2":
								ret, _, remaining, err = tvm.Create2(caller, code, 100_000, 0, [32]byte{1})
							case "top-level":
								ret, _, remaining, err = tvm.CreateAt(caller, tcommon.Address{0x41, 0xe1}, code, 100_000, 0)
							}
							var want error
							if revert {
								want = ErrExecutionReverted
							}
							if london && prefix == 0xef && (!revert || kind != "top-level") {
								want = ErrInvalidCode
							}
							if !errors.Is(err, want) {
								t.Errorf("constructor result=%v remaining=%d; java expects %v", err, remaining, want)
							}
							if want == ErrInvalidCode && remaining != 0 {
								t.Errorf("exceptional constructor refunded %d energy; java skips refund", remaining)
							}
							if want == ErrExecutionReverted && remaining == 0 {
								t.Error("ordinary REVERT must refund unused constructor energy")
							}
							if want == ErrExecutionReverted && !bytes.Equal(ret, []byte{prefix}) {
								t.Errorf("ordinary REVERT lost data: %x", ret)
							}
							if kind != "top-level" && revert && want == ErrInvalidCode && len(ret) != 0 {
								t.Errorf("exceptional internal creation exposed revert data: %x", ret)
							}
							if want != nil {
								if tvm.StateDB.AccountExists(created) || len(tvm.StateDB.GetCode(created)) != 0 || tvm.StateDB.GetContract(created) != nil {
									t.Error("failed constructor retained its account, code or metadata")
								}
							} else if !tvm.StateDB.AccountExists(created) || !bytes.Equal(tvm.StateDB.GetCode(created), []byte{prefix}) {
								t.Error("successful constructor lost its deployed account or code")
							}
							for _, tx := range tvm.InternalTransactions {
								if tx.Rejected != (want != nil) {
									t.Errorf("create internal transaction rejected=%v, want %v", tx.Rejected, want != nil)
								}
							}
						})
					}
				}
			}
		}
	}
}
