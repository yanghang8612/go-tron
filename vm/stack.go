package vm

import (
	"sync"

	"github.com/holiman/uint256"
)

const stackLimit = 1024

// Stack is the TVM operand stack.
type Stack struct {
	data []uint256.Int
}

// executionStackPool reuses call-frame operand stacks. A contract call can
// allocate several nested frames and every frame previously allocated both a
// Stack and its initial 16-word backing slice. Stacks contain no pointers and
// never exceed stackLimit, so retaining their high-water capacity is bounded.
var executionStackPool = sync.Pool{
	New: func() any { return newStack() },
}

func acquireExecutionStack() *Stack {
	stack := executionStackPool.Get().(*Stack)
	stack.data = stack.data[:0]
	return stack
}

func releaseExecutionStack(stack *Stack) {
	if stack == nil {
		return
	}
	stack.data = stack.data[:0]
	executionStackPool.Put(stack)
}

func newStack() *Stack {
	return &Stack{data: make([]uint256.Int, 0, 16)}
}

func (s *Stack) push(v *uint256.Int) {
	s.data = append(s.data, *v)
}

func (s *Stack) pop() uint256.Int {
	ret := s.data[len(s.data)-1]
	s.data = s.data[:len(s.data)-1]
	return ret
}

func (s *Stack) peek() *uint256.Int {
	return &s.data[len(s.data)-1]
}

// back returns a pointer to the nth element from the top (0 = top).
func (s *Stack) back(n int) *uint256.Int {
	return &s.data[len(s.data)-1-n]
}

func (s *Stack) swap(n int) {
	top := len(s.data) - 1
	s.data[top], s.data[top-n] = s.data[top-n], s.data[top]
}

func (s *Stack) dup(n int) {
	v := s.data[len(s.data)-n]
	s.data = append(s.data, v)
}

func (s *Stack) len() int {
	return len(s.data)
}

// Data returns the underlying operand slice (bottom..top, top last). Tracers
// read it to record per-opcode operands; callers must not mutate it.
func (s *Stack) Data() []uint256.Int {
	return s.data
}
