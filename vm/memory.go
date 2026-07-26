package vm

import (
	"sync"

	"github.com/holiman/uint256"
)

// Memory is byte-addressable, word-aligned expandable memory.
type Memory struct {
	store []byte
}

const maxPooledMemoryCapacity = 256 << 10

// ABI return values on the sync path are overwhelmingly one to six words.
// Keep the observed common range inline and retain only modest larger values;
// an adversarial/exceptional large RETURN keeps its historical one-shot
// ownership instead of pinning max-sized VM memory in the TVM pool.
const (
	inlineNestedReturnCapacity   = 192
	maxReusableNestedReturnBytes = 4 << 10
)

type nestedReturnBuffers struct {
	inline   [maxCallDepth + 2][inlineNestedReturnCapacity]byte
	overflow [maxCallDepth + 2][]byte
}

var executionMemoryPool = sync.Pool{
	New: func() any { return newMemory() },
}

func acquireExecutionMemory() *Memory {
	memory := executionMemoryPool.Get().(*Memory)
	memory.store = memory.store[:0]
	return memory
}

func releaseExecutionMemory(memory *Memory) {
	if memory == nil {
		return
	}
	if cap(memory.store) > maxPooledMemoryCapacity {
		memory.store = nil
	} else {
		// A new EVM frame observes zero-filled memory. Clear before pooling so a
		// later frame can grow into the retained capacity without seeing bytes
		// from its predecessor.
		clear(memory.store)
		memory.store = memory.store[:0]
	}
	executionMemoryPool.Put(memory)
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

// setPadded copies source[sourceOffset:] into [offset, offset+size), truncating
// to size and zero-filling every byte not covered by the source. The COPY
// opcodes consume the result immediately, so writing into final VM memory
// avoids allocating a same-sized zero-padded temporary slice.
func (m *Memory) setPadded(offset, size uint64, source []byte, sourceOffset uint64) {
	if size == 0 {
		return
	}
	if offset+size > uint64(len(m.store)) {
		m.resize(offset + size)
	}
	dst := m.store[offset : offset+size]
	copied := 0
	if sourceOffset < uint64(len(source)) {
		copied = copy(dst, source[sourceOffset:])
	}
	clear(dst[copied:])
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

// copyFrameReturn gives ordinary nested CALL-family frames a result buffer
// owned by their TVM and call depth. The parent is suspended while the child
// runs and every such call replaces the parent's prior return-data buffer, so
// the same depth can be reused by the next call. Top-level and CREATE results
// retain independent ownership through getCopy.
func (in *Interpreter) copyFrameReturn(contract *Contract, memory *Memory, offset, size int64) []byte {
	if size == 0 {
		return nil
	}
	tvm := in.tvm
	if contract == nil || !contract.reuseReturnBuffer || tvm == nil ||
		tvm.nestedReturns == nil || tvm.Depth <= 1 || tvm.Depth >= len(tvm.nestedReturns.inline) {
		return memory.getCopy(offset, size)
	}

	n := int(size)
	var dst []byte
	if n <= inlineNestedReturnCapacity {
		dst = tvm.nestedReturns.inline[tvm.Depth][:n:n]
	} else if n <= maxReusableNestedReturnBytes {
		dst = tvm.nestedReturns.overflow[tvm.Depth]
		if cap(dst) < n {
			dst = make([]byte, n)
		} else {
			dst = dst[:n]
		}
		tvm.nestedReturns.overflow[tvm.Depth] = dst
		dst = dst[:n:n]
	} else {
		return memory.getCopy(offset, size)
	}
	copy(dst, memory.store[offset:offset+size])
	return dst
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
	if size <= uint64(cap(m.store)) {
		oldLen := len(m.store)
		m.store = m.store[:size]
		clear(m.store[oldLen:])
		return
	}
	// Keep the logical length exact (energy accounting and MSIZE observe len),
	// but grow backing capacity geometrically. Exact-capacity allocation made
	// incremental MSTORE/MSTORE8 expansion reallocate and copy the entire memory
	// on every word, turning a linear bytecode walk into quadratic work.
	newCap := cap(m.store) * 2
	if newCap < 64 {
		newCap = 64
	}
	if uint64(newCap) < size {
		newCap = int(size)
	}
	newStore := make([]byte, int(size), newCap)
	copy(newStore, m.store)
	m.store = newStore
}

func resizeMemory(m *Memory, offset, size uint64) {
	if size == 0 {
		return
	}
	m.resize(offset + size)
}
