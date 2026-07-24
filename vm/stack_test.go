package vm

import (
	"testing"

	"github.com/holiman/uint256"
)

func TestStackPushPop(t *testing.T) {
	s := newStack()
	v := uint256.NewInt(42)
	s.push(v)

	if s.len() != 1 {
		t.Fatalf("expected len 1, got %d", s.len())
	}

	got := s.pop()
	if !got.Eq(v) {
		t.Fatalf("expected 42, got %s", got.String())
	}
	if s.len() != 0 {
		t.Fatalf("expected len 0, got %d", s.len())
	}
}

func TestStackPushBytesMatchesUint256(t *testing.T) {
	s := newStack()
	for size := 0; size <= 32; size++ {
		input := make([]byte, size)
		for i := range input {
			input[i] = byte(size + i + 1)
		}
		var want uint256.Int
		want.SetBytes(input)
		s.pushBytes(input)
		if got := s.pop(); !got.Eq(&want) {
			t.Fatalf("pushBytes size %d = %s, want %s", size, got.String(), want.String())
		}
	}
}

func TestStackPeek(t *testing.T) {
	s := newStack()
	s.push(uint256.NewInt(10))
	s.push(uint256.NewInt(20))

	top := s.peek()
	if !top.Eq(uint256.NewInt(20)) {
		t.Fatalf("expected 20, got %s", top.String())
	}
	if s.len() != 2 {
		t.Fatal("peek should not remove element")
	}
}

func TestStackBack(t *testing.T) {
	s := newStack()
	s.push(uint256.NewInt(1))
	s.push(uint256.NewInt(2))
	s.push(uint256.NewInt(3))

	if !s.back(0).Eq(uint256.NewInt(3)) {
		t.Fatal("back(0) should be top")
	}
	if !s.back(1).Eq(uint256.NewInt(2)) {
		t.Fatal("back(1) should be second from top")
	}
	if !s.back(2).Eq(uint256.NewInt(1)) {
		t.Fatal("back(2) should be bottom")
	}
}

func TestStackSwap(t *testing.T) {
	s := newStack()
	s.push(uint256.NewInt(1))
	s.push(uint256.NewInt(2))
	s.push(uint256.NewInt(3))

	s.swap(2)
	if !s.peek().Eq(uint256.NewInt(1)) {
		t.Fatalf("after swap, top should be 1, got %s", s.peek().String())
	}
	if !s.back(2).Eq(uint256.NewInt(3)) {
		t.Fatalf("after swap, bottom should be 3, got %s", s.back(2).String())
	}
}

func TestStackDup(t *testing.T) {
	s := newStack()
	s.push(uint256.NewInt(10))
	s.push(uint256.NewInt(20))

	s.dup(2)
	if s.len() != 3 {
		t.Fatalf("expected len 3, got %d", s.len())
	}
	if !s.peek().Eq(uint256.NewInt(10)) {
		t.Fatalf("dup should push copy of 2nd element, got %s", s.peek().String())
	}
}
