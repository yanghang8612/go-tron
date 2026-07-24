package vm

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

var logBenchmarkSink Log

func BenchmarkLogOpcode(b *testing.B) {
	for _, topicCount := range []int{0, 1, 4} {
		b.Run(fmt.Sprintf("topics_%d", topicCount), func(b *testing.B) {
			tvm := new(TVM)
			interpreter := NewInterpreter(tvm, TVMConfig{})
			contract := NewContract(tcommon.Address{0x41, 1}, tcommon.Address{0x41, 2}, 0, 1_000_000)
			memory := newMemory()
			memory.resize(64)
			for i := range memory.store {
				memory.store[i] = byte(i)
			}
			stack := newStack()
			execute := makeLog(topicCount)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tvm.Logs = nil
				stack.data = stack.data[:0]
				for topic := 0; topic < topicCount; topic++ {
					stack.push(uint256.NewInt(uint64(topic + 1)))
				}
				stack.push(uint256.NewInt(64))
				stack.push(uint256.NewInt(0))
				contract.Energy = 1_000_000
				var pc uint64
				if _, err := execute(&pc, interpreter, contract, memory, stack); err != nil {
					b.Fatal(err)
				}
				logBenchmarkSink = tvm.Logs[0]
			}
		})
	}
}

func TestLogSnapshotRevert(t *testing.T) {
	evm := &TVM{}

	evm.Logs = append(evm.Logs, Log{
		Address: tcommon.Address{0x41, 0x01},
		Topics:  [][]byte{{0x01}},
		Data:    []byte{0xAA},
	})
	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(evm.Logs))
	}

	snap := evm.LogSnapshot()
	if snap != 1 {
		t.Fatalf("expected snapshot 1, got %d", snap)
	}

	evm.Logs = append(evm.Logs, Log{
		Address: tcommon.Address{0x41, 0x02},
		Topics:  [][]byte{{0x02}},
		Data:    []byte{0xBB},
	})
	if len(evm.Logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(evm.Logs))
	}

	evm.RevertLogs(snap)
	if len(evm.Logs) != 1 {
		t.Fatalf("expected 1 log after revert, got %d", len(evm.Logs))
	}
	if evm.Logs[0].Data[0] != 0xAA {
		t.Fatal("wrong log after revert")
	}
}
