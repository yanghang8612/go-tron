package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

// freshJumpdestState returns an empty in-memory StateDB for TVM wiring tests.
func freshJumpdestState(t *testing.T) *state.StateDB {
	t.Helper()
	sdb, err := state.New(tcommon.Hash{}, state.NewDatabase(ethrawdb.NewMemoryDatabase()))
	if err != nil {
		t.Fatalf("new state: %v", err)
	}
	return sdb
}

// TestTVMCallPopulatesJumpdestCache proves the production wiring end to end:
// executing a contract through TVM.Call / TVM.StaticCall must thread the StateDB
// code hash into the call frame so the JUMPDEST analysis lands in the process-wide
// cache keyed by Keccak256(code). Without the tvm.go CodeHash wiring the entry
// would be absent (the frame would fall back to an uncached direct analysis).
func TestTVMCallPopulatesJumpdestCache(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}

	cases := []struct {
		name   string
		addr   tcommon.Address
		code   []byte
		invoke func(evm *TVM, addr tcommon.Address) error
	}{
		{
			name: "Call",
			addr: tcommon.Address{0x41, 0xC1},
			// PUSH1 3; JUMP; JUMPDEST; STOP — a real JUMP to a valid dest.
			code: []byte{byte(PUSH1), 0x03, byte(JUMP), byte(JUMPDEST), byte(STOP)},
			invoke: func(evm *TVM, addr tcommon.Address) error {
				_, _, err := evm.Call(owner, addr, nil, 1_000_000, 0)
				return err
			},
		},
		{
			name: "StaticCall",
			addr: tcommon.Address{0x41, 0xC2},
			// Distinct bytecode (trailing JUMPDEST) ⇒ distinct code hash.
			code: []byte{byte(PUSH1), 0x03, byte(JUMP), byte(JUMPDEST), byte(STOP), byte(JUMPDEST)},
			invoke: func(evm *TVM, addr tcommon.Address) error {
				_, _, err := evm.StaticCall(owner, addr, nil, 1_000_000)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sdb := freshJumpdestState(t)
			sdb.AddBalance(owner, 1_000_000_000)
			sdb.SetCode(tc.addr, tc.code)
			h := tcommon.Keccak256(tc.code)

			evm := NewTVM(sdb, nil, owner, 1, 1, tcommon.Address{}, 1, TVMConfig{})
			if err := tc.invoke(evm, tc.addr); err != nil {
				t.Fatalf("%s: execution failed: %v", tc.name, err)
			}
			if !globalJumpdestCache.cache.Contains(h) {
				t.Fatalf("%s: jumpdest cache missing entry for executed code hash %v", tc.name, h)
			}
		})
	}
}
