package vm

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Word-decode parity: java reads the SOURCE offset of the base-EVM copy/load
// opcodes through saturating accessors (CALLDATALOAD: ProgramInvokeImpl.getDataValue
// full-256 value() vs MAX_MSG_DATA=Integer.MAX_VALUE -> zero; CALLDATACOPY:
// getDataCopy offsetData.intValueSafe(); CODECOPY/EXTCODECOPY:
// stackPop().intValueSafe() then `if (codeOffset < fullCode.length)`). A 256-bit
// offset word whose span exceeds 4 bytes (or, for CALLDATALOAD, exceeds
// Integer.MAX_VALUE) is therefore treated out-of-range and java reads ZEROS. gtron
// took the raw low-64 bits via .Uint64(); for a word like 2^64+k (low-64 = small k
// < len) it copied the REAL source bytes, diverging from java's zeros and flipping
// any downstream SHA3/branch/return. Same root class as the VOTEWITNESS intValueSafe
// fix, applied to the base-EVM source offsets.

func newWordDecodeTVM(t *testing.T) (*TVM, *state.StateDB) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	statedb.SetDynamicProperties(dp)
	tvm := NewTVM(statedb, dp, tcommon.Address{}, 1, 1_000_000, tcommon.Address{}, 1, TVMConfig{})
	tvm.SetDB(diskdb)
	return tvm, statedb
}

func wordDecodeAddr(last byte) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	a[20] = last
	return a
}

// highByteOffset returns 2^64 + low: a 256-bit word whose low-64 bits == low but
// whose 9th byte is set (bytesOccupied == 9 > 4), so java intValueSafe/value()
// treats it as out-of-range while a raw .Uint64() truncates it to `low`.
func highByteOffset(low uint64) *uint256.Int {
	return new(uint256.Int).Add(pow2(64), uint256.NewInt(low))
}

func TestCallDataLoadHighByteOffsetReadsZero(t *testing.T) {
	tvm, _ := newWordDecodeTVM(t)
	owner := wordDecodeAddr(0x01)
	contract := NewContract(owner, owner, 0, 1_000_000)
	contract.SetInput(bytes.Repeat([]byte{0xAB}, 64))

	stack := newStack()
	stack.push(highByteOffset(5)) // offset 2^64+5; .Uint64()==5 would read input[5:]
	if _, err := opCallDataLoad(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opCallDataLoad: %v", err)
	}
	got := stack.pop()
	if !got.IsZero() {
		t.Fatalf("CALLDATALOAD offset 2^64+5: got %x, want 0 (java getDataValue full-256 > MAX_MSG_DATA -> zero word)", got.Bytes())
	}

	// Sanity: a normal small offset still reads the real word.
	stack.push(uint256.NewInt(0))
	if _, err := opCallDataLoad(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("opCallDataLoad normal: %v", err)
	}
	if got := stack.pop(); got.IsZero() {
		t.Fatal("CALLDATALOAD offset 0 should read the real (non-zero) calldata word")
	}
}

func TestCallDataCopyHighByteOffsetCopiesZero(t *testing.T) {
	tvm, _ := newWordDecodeTVM(t)
	owner := wordDecodeAddr(0x02)
	contract := NewContract(owner, owner, 0, 1_000_000)
	contract.SetInput(bytes.Repeat([]byte{0xAB}, 64))

	mem := newMemory()
	stack := newStack()
	stack.push(uint256.NewInt(32)) // length (bottom)
	stack.push(highByteOffset(5))  // dataOffset 2^64+5
	stack.push(uint256.NewInt(0))  // memOffset (top)
	if _, err := opCallDataCopy(nil, tvm.interpreter, contract, mem, stack); err != nil {
		t.Fatalf("opCallDataCopy: %v", err)
	}
	if got := mem.getCopy(0, 32); !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("CALLDATACOPY dataOffset 2^64+5: copied %x, want 32 zeros (java getDataCopy intValueSafe -> offset>len -> zeros)", got)
	}
}

func TestCodeCopyHighByteOffsetCopiesZero(t *testing.T) {
	tvm, _ := newWordDecodeTVM(t)
	owner := wordDecodeAddr(0x03)
	contract := NewContract(owner, owner, 0, 1_000_000)
	contract.SetCode(owner, bytes.Repeat([]byte{0xCC}, 64))

	mem := newMemory()
	stack := newStack()
	stack.push(uint256.NewInt(32)) // length
	stack.push(highByteOffset(5))  // codeOffset 2^64+5
	stack.push(uint256.NewInt(0))  // memOffset
	if _, err := opCodeCopy(nil, tvm.interpreter, contract, mem, stack); err != nil {
		t.Fatalf("opCodeCopy: %v", err)
	}
	if got := mem.getCopy(0, 32); !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("CODECOPY codeOffset 2^64+5: copied %x, want 32 zeros (java codeCopyAction intValueSafe -> codeOffset>=len -> zeros)", got)
	}
}

func TestExtCodeCopyHighByteOffsetCopiesZero(t *testing.T) {
	tvm, statedb := newWordDecodeTVM(t)
	owner := wordDecodeAddr(0x04)
	target := wordDecodeAddr(0x05)
	statedb.CreateAccount(target, corepb.AccountType_Contract)
	statedb.SetCode(target, bytes.Repeat([]byte{0xDD}, 64))
	contract := NewContract(owner, owner, 0, 1_000_000)

	mem := newMemory()
	stack := newStack()
	tw := addressWord(target)
	stack.push(uint256.NewInt(32)) // length (bottom)
	stack.push(highByteOffset(5))  // codeOffset 2^64+5
	stack.push(uint256.NewInt(0))  // memOffset
	stack.push(&tw)                // address (top)
	if _, err := opExtCodeCopy(nil, tvm.interpreter, contract, mem, stack); err != nil {
		t.Fatalf("opExtCodeCopy: %v", err)
	}
	if got := mem.getCopy(0, 32); !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("EXTCODECOPY codeOffset 2^64+5: copied %x, want 32 zeros (java extCodeCopyAction intValueSafe -> codeOffset>=len -> zeros)", got)
	}
}
