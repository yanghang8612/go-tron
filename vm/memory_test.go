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
