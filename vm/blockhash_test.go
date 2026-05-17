package vm

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func blockHashTestBlock(number uint64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(number * 3000),
			},
		},
	})
}

func runBlockHashOpcode(t *testing.T, tvm *TVM, index *uint256.Int) [32]byte {
	t.Helper()
	stack := newStack()
	stack.push(index)
	if _, err := opBlockHash(nil, tvm.interpreter, nil, nil, stack); err != nil {
		t.Fatalf("opBlockHash error: %v", err)
	}
	got := stack.pop()
	return got.Bytes32()
}

func TestBlockHashOpcodeReadsJavaHistoryWindow(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block44 := blockHashTestBlock(44)
	block299 := blockHashTestBlock(299)
	if err := rawdb.WriteBlock(db, block44); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteBlock(db, block299); err != nil {
		t.Fatal(err)
	}
	tvm := NewTVM(nil, nil, tcommon.Address{}, 300, 0, tcommon.Address{}, 1, TVMConfig{})
	tvm.SetDB(db)

	for _, tc := range []struct {
		name  string
		index uint64
		want  []byte
	}{
		{name: "latest previous block", index: 299, want: block299.Hash().Bytes()},
		{name: "oldest window block", index: 44, want: block44.Hash().Bytes()},
		{name: "too old", index: 43, want: nil},
		{name: "current block excluded", index: 300, want: nil},
		{name: "future block excluded", index: 301, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := runBlockHashOpcode(t, tvm, uint256.NewInt(tc.index))
			if tc.want == nil {
				if got != ([32]byte{}) {
					t.Fatalf("expected zero, got %x", got)
				}
				return
			}
			if !bytes.Equal(got[:], tc.want) {
				t.Fatalf("block hash: got %x, want %x", got, tc.want)
			}
		})
	}
}

func TestBlockHashOpcodeWindowBeforeBlock256StartsAtZero(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block0 := blockHashTestBlock(0)
	block9 := blockHashTestBlock(9)
	if err := rawdb.WriteBlock(db, block0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteBlock(db, block9); err != nil {
		t.Fatal(err)
	}
	tvm := NewTVM(nil, nil, tcommon.Address{}, 10, 0, tcommon.Address{}, 1, TVMConfig{})
	tvm.SetDB(db)

	got0 := runBlockHashOpcode(t, tvm, uint256.NewInt(0))
	if !bytes.Equal(got0[:], block0.Hash().Bytes()) {
		t.Fatalf("block 0 hash: got %x, want %x", got0, block0.Hash().Bytes())
	}
	got9 := runBlockHashOpcode(t, tvm, uint256.NewInt(9))
	if !bytes.Equal(got9[:], block9.Hash().Bytes()) {
		t.Fatalf("block 9 hash: got %x, want %x", got9, block9.Hash().Bytes())
	}
	got10 := runBlockHashOpcode(t, tvm, uint256.NewInt(10))
	if got10 != ([32]byte{}) {
		t.Fatalf("current block should be zero, got %x", got10)
	}
}

func TestBlockHashOpcodeUsesJavaIntValueSafe(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	block := blockHashTestBlock(javaMaxInt)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatal(err)
	}
	tvm := NewTVM(nil, nil, tcommon.Address{}, javaMaxInt+1, 0, tcommon.Address{}, 1, TVMConfig{})
	tvm.SetDB(db)

	index := uint256.NewInt(javaMaxInt + 123)
	got := runBlockHashOpcode(t, tvm, index)
	if !bytes.Equal(got[:], block.Hash().Bytes()) {
		t.Fatalf("saturated block hash: got %x, want %x", got, block.Hash().Bytes())
	}
}
