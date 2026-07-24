package vm

import (
	"testing"

	"github.com/holiman/uint256"
)

func TestMemorySetGet(t *testing.T) {
	m := newMemory()
	data := []byte{0x01, 0x02, 0x03}
	m.set(0, uint64(len(data)), data)

	got := m.getCopy(0, int64(len(data)))
	if string(got) != string(data) {
		t.Fatalf("got %x, want %x", got, data)
	}
}

func TestMemorySetPadded(t *testing.T) {
	m := newMemory()
	m.resize(16)
	for i := range m.store {
		m.store[i] = 0xaa
	}
	source := []byte{1, 2, 3, 4, 5}
	m.setPadded(4, 6, source, 2)
	want := []byte{3, 4, 5, 0, 0, 0}
	if got := m.store[4:10]; string(got) != string(want) {
		t.Fatalf("partial padded copy = %x, want %x", got, want)
	}
	if m.store[3] != 0xaa || m.store[10] != 0xaa {
		t.Fatalf("padded copy changed surrounding bytes: %x", m.store)
	}

	m.setPadded(5, 4, source, uint64(len(source)))
	if got := m.store[5:9]; string(got) != string([]byte{0, 0, 0, 0}) {
		t.Fatalf("out-of-range padded copy = %x, want zeros", got)
	}

	m.setPadded(16, 3, source, 1)
	if got := m.store[16:19]; string(got) != string([]byte{2, 3, 4}) {
		t.Fatalf("growing padded copy = %x, want 020304", got)
	}

	overlap := newMemory()
	overlap.store = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	overlap.setPadded(2, 6, overlap.store, 0)
	if got := overlap.store; string(got) != string([]byte{1, 2, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("overlapping padded copy = %x, want 0102010203040506", got)
	}
}

func TestMemorySet32(t *testing.T) {
	m := newMemory()
	m.resize(32)
	v := uint256.NewInt(0xFF)
	m.set32(0, v)

	if m.store[31] != 0xFF {
		t.Fatalf("expected 0xFF at byte 31, got %x", m.store[31])
	}
}

func TestMemoryResize(t *testing.T) {
	m := newMemory()
	m.resize(64)
	if m.len() != 64 {
		t.Fatalf("expected len 64, got %d", m.len())
	}

	m.resize(32)
	if m.len() != 64 {
		t.Fatalf("should not shrink, got %d", m.len())
	}

	m.resize(128)
	if m.len() != 128 {
		t.Fatalf("expected 128, got %d", m.len())
	}
}

func TestMemoryGetPtr(t *testing.T) {
	m := newMemory()
	m.resize(32)
	m.store[0] = 0xAA
	m.store[1] = 0xBB

	ptr := m.getPtr(0, 2)
	if ptr[0] != 0xAA || ptr[1] != 0xBB {
		t.Fatalf("getPtr mismatch: %x", ptr)
	}
}

func TestCallFrameInputOwnership(t *testing.T) {
	memory := newMemory()
	memory.resize(32)
	memory.store[4] = 0xaa

	borrowed := callFrameInput(&Interpreter{}, memory, 4, 1, false)
	memory.store[4] = 0xbb
	if borrowed[0] != 0xbb {
		t.Fatalf("ordinary frame input was copied: got %x", borrowed)
	}

	precompileInput := callFrameInput(&Interpreter{}, memory, 4, 1, true)
	memory.store[4] = 0xcc
	if precompileInput[0] != 0xbb {
		t.Fatalf("precompile input aliases caller memory: got %x", precompileInput)
	}

	traced := callFrameInput(&Interpreter{tvmConfig: TVMConfig{Tracer: &recorderTracer{}}}, memory, 4, 1, false)
	memory.store[4] = 0xdd
	if traced[0] != 0xcc {
		t.Fatalf("traced frame input aliases caller memory: got %x", traced)
	}
}

var callFrameInputSink []byte
var memoryPaddedCopySink byte

func setPaddedLegacy(m *Memory, offset, size uint64, source []byte, sourceOffset uint64) {
	data := make([]byte, size)
	if sourceOffset < uint64(len(source)) {
		copy(data, source[sourceOffset:])
	}
	m.set(offset, size, data)
}

func BenchmarkMemoryPaddedCopy(b *testing.B) {
	const size = 2048
	source := make([]byte, 1536)
	for i := range source {
		source[i] = byte(i)
	}
	memory := newMemory()
	memory.resize(size)
	for _, test := range []struct {
		name string
		copy func(*Memory, uint64, uint64, []byte, uint64)
	}{
		{name: "temporary-slice", copy: setPaddedLegacy},
		{name: "direct-memory", copy: (*Memory).setPadded},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(size)
			for b.Loop() {
				test.copy(memory, 0, size, source, 256)
				memoryPaddedCopySink = memory.store[size-1]
			}
		})
	}
}

func BenchmarkCallFrameInput(b *testing.B) {
	memory := newMemory()
	memory.resize(1024)
	interpreter := new(Interpreter)

	b.Run("borrowed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			callFrameInputSink = callFrameInput(interpreter, memory, 0, 1024, false)
		}
	})
	b.Run("retained", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			callFrameInputSink = callFrameInput(interpreter, memory, 0, 1024, true)
		}
	})
}

func BenchmarkMemoryIncrementalResize(b *testing.B) {
	const words = 1024
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		memory := newMemory()
		for word := 1; word <= words; word++ {
			memory.resize(uint64(word * 32))
		}
		if memory.len() != words*32 {
			b.Fatal(memory.len())
		}
	}
}
