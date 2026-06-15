package vm

import (
	"errors"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// Nile block 23,077,310 tx a5580051… stalled with "expected REVERT actual
// TRANSFER_FAILED". A GovernanceProxy → Governance.execute(target, data) chain
// forwarded a call whose nested StakeManager frame issued a CALL with an
// endowment of 123e18 (> int64). java-tron throws a TransferException at that
// inner frame, but its VM.play outer handler stores the exception in the child
// result and the *caller's* CALL opcode pushes 0 (Program.java:1157-1168) — a
// soft failure. The outermost Governance.execute then does require(ok,
// "Update failed") and the transaction ends in REVERT.
//
// go-tron instead propagated the transfer-failure all the way to the top
// (shouldPropagateCallError → isTransferFailure at every boundary), surfacing
// TRANSFER_FAILED. These tests pin java's "a transfer-failure aborts only the
// frame that raised it; its parent catches it as a 0-push; only the entry
// frame turns it into TRANSFER_FAILED" semantics.

// callee B: performs a CALL with a value that does not fit in int64, which
// raises ErrEndowmentOutOfRange inside B's own frame.
func endowmentOverflowCallee() []byte {
	return []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH32), // value (2^256-1, far beyond int64)
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		byte(PUSH1), 0x99, // address (non-existent)
		byte(PUSH2), 0x03, 0xe8, // gas
		byte(CALL),
		byte(STOP),
	}
}

func TestNestedTransferFailureSurfacesAsRevert(t *testing.T) {
	evm := newTestEVM(t)

	addrA := tcommon.Address{0x41, 0x0A}
	addrB := tcommon.Address{0x41, 0x0B}

	// caller A: CALL B with value 0; if B fails, REVERT (mirrors
	// Governance.execute's require(ok, "Update failed")).
	aCode := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH1), 0x00, // value 0
		byte(PUSH20), // address of B -> low 20 bytes become addr[1:], 0x41 prepended
		0x0B, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		byte(PUSH3), 0x0f, 0x42, 0x40, // gas 1_000_000
		byte(CALL),
		byte(ISZERO),
		byte(PUSH1), 0x29, // revert dest = 41
		byte(JUMPI),
		byte(STOP),     // [40] success path (not taken)
		byte(JUMPDEST), // [41]
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(REVERT),
	}

	evm.StateDB.SetCode(addrA, aCode)
	evm.StateDB.SetCode(addrB, endowmentOverflowCallee())

	caller := tcommon.Address{0x41, 0x01}
	_, _, err := evm.Call(caller, addrA, nil, 5_000_000, 0)
	if !errors.Is(err, ErrExecutionReverted) {
		t.Fatalf("nested transfer-failure must be caught and surface as REVERT, got %v", err)
	}
	if isTransferFailure(err) {
		t.Fatalf("nested transfer-failure must not propagate as TRANSFER_FAILED, got %v", err)
	}
}

func TestEntryFrameTransferFailureStaysTransferFailed(t *testing.T) {
	evm := newTestEVM(t)

	addrB := tcommon.Address{0x41, 0x0B}
	evm.StateDB.SetCode(addrB, endowmentOverflowCallee())

	caller := tcommon.Address{0x41, 0x01}
	_, _, err := evm.Call(caller, addrB, nil, 5_000_000, 0)
	if !errors.Is(err, ErrEndowmentOutOfRange) {
		t.Fatalf("entry-frame transfer-failure must stay TRANSFER_FAILED, got %v", err)
	}
}
