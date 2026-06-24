package vm

import "github.com/holiman/uint256"

// Memory is byte-addressable, word-aligned expandable memory.
type Memory struct {
	store []byte
}

func newMemory() *Memory {
	return &Memory{}
}

// set copies value into memory at [offset, offset+size).
func (m *Memory) set(offset, size uint64, value []byte) {
	if size == 0 {
		return
	}
	if offset+size > uint64(len(m.store)) {
		m.resize(offset + size)
	}
	copy(m.store[offset:offset+size], value)
}

// set32 writes a 32-byte big-endian uint256 at offset.
func (m *Memory) set32(offset uint64, val *uint256.Int) {
	if offset+32 > uint64(len(m.store)) {
		m.resize(offset + 32)
	}
	b32 := val.Bytes32()
	copy(m.store[offset:offset+32], b32[:])
}

// getCopy returns a copy of the memory range [offset, offset+size).
func (m *Memory) getCopy(offset, size int64) []byte {
	if size == 0 {
		return nil
	}
	cpy := make([]byte, size)
	copy(cpy, m.store[offset:offset+size])
	return cpy
}

// getPtr returns a direct slice into memory (no copy).
func (m *Memory) getPtr(offset, size int64) []byte {
	if size == 0 {
		return nil
	}
	return m.store[offset : offset+size]
}

// len returns the current memory size in bytes.
func (m *Memory) len() int {
	return len(m.store)
}

// Data returns the backing memory bytes (no copy). Tracers read it to record
// the memory image at a step; callers must not mutate the returned slice.
func (m *Memory) Data() []byte {
	return m.store
}

// resize grows memory to at least size bytes (never shrinks).
func (m *Memory) resize(size uint64) {
	if uint64(len(m.store)) >= size {
		return
	}
	newStore := make([]byte, size)
	copy(newStore, m.store)
	m.store = newStore
}

func resizeMemory(m *Memory, offset, size uint64) {
	if size == 0 {
		return
	}
	m.resize(offset + size)
}
