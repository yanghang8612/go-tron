package vm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

var logBenchmarkSink Log
var executionLogArenaSink []byte

func TestLogTopicAccess(t *testing.T) {
	legacy := &Log{Topics: [][]byte{{0x01}, {0x02}}}
	if legacy.TopicCount() != 2 || legacy.Topic(1)[0] != 0x02 {
		t.Fatalf("legacy topics not preserved: %#v", legacy.Topics)
	}

	payload := make([]byte, 64)
	payload[0], payload[32] = 0x03, 0x04
	compact := &Log{compactTopics: payload}
	if compact.TopicCount() != 2 || compact.Topic(0)[0] != 0x03 || compact.Topic(1)[0] != 0x04 {
		t.Fatalf("compact topics not preserved: %x %x", compact.Topic(0), compact.Topic(1))
	}
	if cap(compact.Topic(0)) != 32 {
		t.Fatalf("topic capacity = %d, want 32", cap(compact.Topic(0)))
	}
}

func TestExecutionLogArenaReusesPayloadWithoutExposingSpareCapacity(t *testing.T) {
	arena := new(ExecutionLogArena)
	first := arena.acquire(96)
	for i := range first {
		first[i] = byte(i)
	}
	firstStorage := &first[0]
	if cap(first) != len(first) {
		t.Fatalf("payload capacity = %d, want length %d", cap(first), len(first))
	}

	arena.Reset()
	second := arena.acquire(96)
	if &second[0] != firstStorage {
		t.Fatal("arena did not reuse payload backing storage")
	}
	if !bytes.Equal(second, first) {
		t.Fatal("arena reset unexpectedly changed payload bytes before overwrite")
	}

	arena.Reset()
	if allocs := testing.AllocsPerRun(1000, func() {
		executionLogArenaSink = arena.acquire(96)
		arena.Reset()
	}); allocs != 0 {
		t.Fatalf("warmed arena allocated %.2f objects, want 0", allocs)
	}
}

func TestExecutionLogArenaDropsPathologicalHighWater(t *testing.T) {
	arena := new(ExecutionLogArena)
	arena.acquire(maxRetainedExecutionLogPayloadBytes + 1)
	arena.Reset()
	if arena.payload != nil {
		t.Fatalf("oversized arena retained capacity %d", cap(arena.payload))
	}
}

func TestLogOpcodeUsesExecutionLogArena(t *testing.T) {
	tvm := new(TVM)
	arena := new(ExecutionLogArena)
	tvm.SetExecutionLogArena(arena)
	interpreter := NewInterpreter(tvm, TVMConfig{})
	contract := NewContract(tcommon.Address{0x41, 1}, tcommon.Address{0x41, 2}, 0, 1_000_000)
	memory := newMemory()
	memory.resize(64)
	for i := range memory.store {
		memory.store[i] = byte(i)
	}
	execute := makeLog(1)

	run := func() *byte {
		stack := newStack()
		stack.push(uint256.NewInt(7))
		stack.push(uint256.NewInt(64))
		stack.push(uint256.NewInt(0))
		contract.Energy = 1_000_000
		var pc uint64
		if _, err := execute(&pc, interpreter, contract, memory, stack); err != nil {
			t.Fatal(err)
		}
		log := &tvm.Logs[0]
		if log.TopicCount() != 1 || log.Topic(0)[31] != 7 || !bytes.Equal(log.Data, memory.store) {
			t.Fatalf("arena-backed log = topic %x data %x", log.Topic(0), log.Data)
		}
		return &log.compactTopics[0]
	}

	firstStorage := run()
	ReleaseExecutionLogs(tvm.Logs)
	tvm.Logs = nil
	arena.Reset()
	if secondStorage := run(); secondStorage != firstStorage {
		t.Fatal("opcode did not reuse the execution-log arena")
	}
	ReleaseExecutionLogs(tvm.Logs)
}

func BenchmarkLogOpcode(b *testing.B) {
	for _, topicCount := range []int{0, 1, 2, 3, 4} {
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
				if tvm.Logs != nil {
					ReleaseExecutionLogs(tvm.Logs)
					tvm.Logs = nil
				}
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
			ReleaseExecutionLogs(tvm.Logs)
			tvm.Logs = nil
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
