package vm

import "github.com/holiman/uint256"

const stackLimit = 1024

// Stack is the EVM operand stack.
type Stack struct {
	data []uint256.Int
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
