package vm

// Focused opcode-level coverage for the per-call-frame return-data buffer,
// the root cause of the Nile block 14,151,095 sync stall (full chain replay
// in nile_tusd_replay_test.go). java-tron gives every Program frame its own
// returnDataBuffer (null at entry, EIP-211), so RETURNDATASIZE reads 0 until
// the current frame itself completes a sub-call. gtron reuses one Interpreter
// across frames, so the buffer must be reset on entry and restored on exit.

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func deployTestCode(t *testing.T, evm *TVM, suffix byte, code []byte) tcommon.Address {
	t.Helper()
	var addr tcommon.Address
	addr[0] = 0x41
	addr[len(addr)-1] = suffix
	evm.StateDB.CreateAccount(addr, corepb.AccountType_Contract)
	evm.StateDB.SetCode(addr, code)
	return addr
}

// callSeq emits a 0-value CALL to a 20-byte address suffix, forwarding all
// energy and writing retSize bytes of output to memory offset 0.
func callSeq(addrSuffix byte, retSize byte) []byte {
	addr20 := make([]byte, 20)
	addr20[19] = addrSuffix
	seq := []byte{
		byte(PUSH1), retSize, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH1), 0x00, // value
		byte(PUSH20),
	}
	seq = append(seq, addr20...)
	seq = append(seq, byte(GAS), byte(CALL), byte(POP))
	return seq
}

// returns 32 nonzero bytes, seeding the caller's return buffer.
func returnNonzero32() []byte {
	return []byte{
		byte(PUSH1), 0xff, byte(PUSH1), 0x00, byte(MSTORE),
		byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN),
	}
}

// retU256 interprets a 32-byte return value as a number.
func retU256(b []byte) *uint256.Int {
	return new(uint256.Int).SetBytes(b)
}

// TestChildFrameSeesEmptyReturnData proves a freshly entered call frame reads
// RETURNDATASIZE == 0 even when the caller already holds return data from an
// earlier sub-call. Without the per-frame reset, the child observed the
// caller's 32-byte buffer — the exact defect that shifted the proxy
// fallback's calldatacopy(ptr, returndatasize(), calldatasize()) in the
// stalling block.
func TestChildFrameSeesEmptyReturnData(t *testing.T) {
	evm := newTestEVM(t)

	deployTestCode(t, evm, 0xDD, returnNonzero32())
	// C echoes the RETURNDATASIZE it sees at its own entry.
	deployTestCode(t, evm, 0xCC, []byte{
		byte(RETURNDATASIZE),
		byte(PUSH1), 0x00, byte(MSTORE),
		byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN),
	})

	// A: CALL D (fills return buffer), then CALL C (whose 32-byte output
	// lands in mem[0:32]), then RETURN that output.
	var a []byte
	a = append(a, callSeq(0xDD, 0x00)...)
	a = append(a, callSeq(0xCC, 0x20)...)
	a = append(a, byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN))
	aAddr := deployTestCode(t, evm, 0xAA, a)

	caller := tcommon.Address{0x41, 0x11}
	ret, _, err := evm.Call(caller, aAddr, nil, 1_000_000, 0)
	if err != nil {
		t.Fatalf("call A: %v", err)
	}
	if got := retU256(ret); !got.IsZero() {
		t.Errorf("child entry RETURNDATASIZE = %s, want 0 (java per-frame buffer)", got)
	}
}

// TestCreateSuccessClearsReturnData proves a successful CREATE leaves the
// return buffer empty (java Program.createContract resets it unconditionally
// before the call; the deployed runtime code is never visible to RETURNDATA*).
func TestCreateSuccessClearsReturnData(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true})

	deployTestCode(t, evm, 0xDD, returnNonzero32())

	// Child init code returns a 1-byte runtime (0x01): a successful deploy.
	initCode := []byte{
		byte(PUSH1), 0x01, byte(PUSH1), 0x00, byte(MSTORE),
		byte(PUSH1), 0x01, byte(PUSH1), 0x1f, byte(RETURN),
	}
	// E: CALL D to seed the buffer, place initCode in memory, CREATE,
	// then RETURN the post-create RETURNDATASIZE.
	var e []byte
	e = append(e, callSeq(0xDD, 0x00)...)
	// PUSH<len> initCode; PUSH1 0; MSTORE → initCode right-aligned at mem[32-len:32].
	e = append(e, byte(int(PUSH1)+len(initCode)-1))
	e = append(e, initCode...)
	e = append(e, byte(PUSH1), 0x00, byte(MSTORE))
	off := byte(32 - len(initCode))
	e = append(e,
		byte(PUSH1), byte(len(initCode)), // size
		byte(PUSH1), off, // offset
		byte(PUSH1), 0x00, // value
		byte(CREATE), byte(POP),
		byte(RETURNDATASIZE), byte(PUSH1), 0x00, byte(MSTORE),
		byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN),
	)
	eAddr := deployTestCode(t, evm, 0xEE, e)

	caller := tcommon.Address{0x41, 0x22}
	ret, _, err := evm.Call(caller, eAddr, nil, 2_000_000, 0)
	if err != nil {
		t.Fatalf("call E: %v", err)
	}
	if got := retU256(ret); !got.IsZero() {
		t.Errorf("post-CREATE RETURNDATASIZE = %s, want 0 (java clears buffer on success)", got)
	}
}

// TestCreate2SuccessKeepsResidualReturnData proves the CREATE/CREATE2
// asymmetry: pre-Osaka, java Program.createContract2 does NOT reset the
// return buffer (the reset is allowTvmOsaka-gated), so a successful CREATE2
// preserves the caller's prior CALL output instead of clearing it. All TRON
// networks are pre-Osaka for Nile history and the current head.
func TestCreate2SuccessKeepsResidualReturnData(t *testing.T) {
	evm := newTestEVMWithConfig(t, TVMConfig{Constantinople: true, Istanbul: true})

	// D seeds the caller's return buffer with 32 nonzero bytes.
	deployTestCode(t, evm, 0xDD, returnNonzero32())

	initCode := []byte{
		byte(PUSH1), 0x01, byte(PUSH1), 0x00, byte(MSTORE),
		byte(PUSH1), 0x01, byte(PUSH1), 0x1f, byte(RETURN),
	}
	// E: CALL D (buffer=32B), place initCode, CREATE2, then RETURN the
	// post-create RETURNDATASIZE. CREATE2 stack is [value, offset, size, salt].
	var e []byte
	e = append(e, callSeq(0xDD, 0x00)...)
	e = append(e, byte(int(PUSH1)+len(initCode)-1))
	e = append(e, initCode...)
	e = append(e, byte(PUSH1), 0x00, byte(MSTORE))
	off := byte(32 - len(initCode))
	e = append(e,
		byte(PUSH1), 0x00, // salt (stack bottom)
		byte(PUSH1), byte(len(initCode)), // size
		byte(PUSH1), off, // offset
		byte(PUSH1), 0x00, // value (stack top)
		byte(CREATE2), byte(POP),
		byte(RETURNDATASIZE), byte(PUSH1), 0x00, byte(MSTORE),
		byte(PUSH1), 0x20, byte(PUSH1), 0x00, byte(RETURN),
	)
	eAddr := deployTestCode(t, evm, 0xEE, e)

	caller := tcommon.Address{0x41, 0x33}
	ret, _, err := evm.Call(caller, eAddr, nil, 2_000_000, 0)
	if err != nil {
		t.Fatalf("call E: %v", err)
	}
	if got := retU256(ret); got.Uint64() != 32 {
		t.Errorf("post-CREATE2 RETURNDATASIZE = %s, want 32 (pre-Osaka keeps residual)", got)
	}
}
