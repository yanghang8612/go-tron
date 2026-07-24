package vm

import (
	"encoding/binary"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var internalTransactionBenchmarkSink *corepb.InternalTransaction
var tvmConstructionBenchmarkSink *TVM

func BenchmarkNewTVMNoCreate(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		tvm := NewTVM(nil, nil, tcommon.Address{}, 1, 2, tcommon.Address{}, 3, TVMConfig{})
		tvmConstructionBenchmarkSink = tvm
		ReleaseTVM(tvm)
	}
}

// addInternalTransactionLegacy preserves the pre-optimization construction
// shape so the benchmark measures the removed concatenation, field-copy and
// slice-growth allocations rather than an unrelated synthetic hash loop.
func addInternalTransactionLegacy(tvm *TVM, caller, transferTo tcommon.Address, value int64, data []byte, note string) *corepb.InternalTransaction {
	parentHash := tvm.currentInternalTxHash()
	receiveAddress := transferTo.Bytes()
	if note == "create" {
		receiveAddress = nil
	}
	var valueBytes [8]byte
	binary.BigEndian.PutUint64(valueBytes[:], uint64(value))
	raw := make([]byte, 0, len(parentHash)+len(receiveAddress)+len(data)+len(valueBytes))
	raw = append(raw, parentHash[:]...)
	raw = append(raw, receiveAddress...)
	raw = append(raw, data...)
	raw = append(raw, valueBytes[:]...)
	var nonceBytes [8]byte
	binary.BigEndian.PutUint64(nonceBytes[:], tvm.Nonce)
	hash := tcommon.Keccak256(append(raw, nonceBytes[:]...))
	it := &corepb.InternalTransaction{
		Hash:              hash.Bytes(),
		CallerAddress:     caller.Bytes(),
		TransferToAddress: transferTo.Bytes(),
		CallValueInfo: []*corepb.InternalTransaction_CallValueInfo{{
			CallValue: value,
		}},
		Note: []byte(note),
	}
	tvm.InternalTransactions = append(tvm.InternalTransactions, it)
	return it
}

func BenchmarkInternalTransactionConstruction(b *testing.B) {
	root := tcommon.HexToHash("010203040506070809")
	caller := tcommon.BytesToAddress([]byte{0x41, 0x11})
	target := tcommon.BytesToAddress([]byte{0x41, 0x22})
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	b.Run("Legacy8", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(8 * len(data)))
		tvm := &TVM{RootTxID: root}
		for b.Loop() {
			tvm.InternalTransactions = nil
			for i := range 8 {
				tvm.Nonce = uint64(i + 1)
				internalTransactionBenchmarkSink = addInternalTransactionLegacy(tvm, caller, target, 7, data, "call")
			}
		}
	})

	b.Run("PooledStreaming8", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(8 * len(data)))
		tvm := &TVM{RootTxID: root}
		for b.Loop() {
			tvm.InternalTransactions = nil
			for i := range 8 {
				tvm.Nonce = uint64(i + 1)
				internalTransactionBenchmarkSink = tvm.addInternalTransaction(caller, target, 7, data, "call", 0, 0)
			}
		}
	})
}
