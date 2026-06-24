package vm

// Coverage for the rawdb.BlockHashReader capability on TVM.DB: BLOCKHASH
// (and CHAINID's genesis read) must resolve through it when present, so the
// chain can serve hashes for blocks whose hot rows the freezer has pruned —
// the Nile 16,745,722 JustLink VRF stall (blockhash(head-211) returned 0).

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// hashReaderKV is a bare memdb plus a canned number->hash table, standing in
// for the chain's ancient-aware store. The memdb intentionally holds no
// block rows, proving the opcode prefers the reader over raw KV.
type hashReaderKV struct {
	ethdb.KeyValueStore
	hashes map[uint64]tcommon.Hash
	calls  []uint64
}

func (f *hashReaderKV) BlockHashByNumber(number uint64) (tcommon.Hash, bool) {
	f.calls = append(f.calls, number)
	h, ok := f.hashes[number]
	return h, ok
}

func TestBlockHashOpcodePrefersBlockHashReader(t *testing.T) {
	pruned := tcommon.HexToHash("0000000000ff842737eddf96785498c824c9752d49254b49887160f4ea17c7b6")
	db := &hashReaderKV{
		KeyValueStore: ethrawdb.NewMemoryDatabase(),
		hashes:        map[uint64]tcommon.Hash{16745511: pruned},
	}
	tvm := NewTVM(nil, nil, tcommon.Address{}, 16745722, 0, tcommon.Address{}, 1, TVMConfig{})
	tvm.SetDB(db)

	// In-window block resolvable only through the reader (no KV row).
	got := runBlockHashOpcode(t, tvm, uint256.NewInt(16745511))
	if !bytes.Equal(got[:], pruned.Bytes()) {
		t.Fatalf("BLOCKHASH via reader: got %x want %x", got, pruned.Bytes())
	}
	// In-window block the reader cannot resolve -> zero.
	if got := runBlockHashOpcode(t, tvm, uint256.NewInt(16745600)); got != ([32]byte{}) {
		t.Fatalf("unresolvable in-window block: got %x want zero", got)
	}
	// Out-of-window blocks must short-circuit before consulting the reader.
	before := len(db.calls)
	if got := runBlockHashOpcode(t, tvm, uint256.NewInt(16745722-257)); got != ([32]byte{}) {
		t.Fatalf("out-of-window block: got %x want zero", got)
	}
	if got := runBlockHashOpcode(t, tvm, uint256.NewInt(16745722)); got != ([32]byte{}) {
		t.Fatalf("current block: got %x want zero", got)
	}
	if len(db.calls) != before {
		t.Fatalf("window check must precede the reader; got extra lookups %v", db.calls[before:])
	}
}

func TestChainIDOpcodeUsesBlockHashReaderForGenesis(t *testing.T) {
	genesisID := tcommon.HexToHash("0000000000000000d698d4192c56cb6be724a558448e2684802de4d6cd8690dc")
	db := &hashReaderKV{
		KeyValueStore: ethrawdb.NewMemoryDatabase(),
		hashes:        map[uint64]tcommon.Hash{0: genesisID},
	}
	tvm := NewTVM(nil, nil, tcommon.Address{}, 100, 0, tcommon.Address{}, 1, TVMConfig{Istanbul: true})
	tvm.SetDB(db)

	stack := newStack()
	if _, err := opChainID(nil, tvm.interpreter, nil, nil, stack); err != nil {
		t.Fatalf("opChainID: %v", err)
	}
	top := stack.pop()
	got := top.Bytes32()
	if !bytes.Equal(got[:], genesisID.Bytes()) {
		t.Fatalf("CHAINID via reader: got %x want %x (genesis blockID)", got, genesisID.Bytes())
	}
}
