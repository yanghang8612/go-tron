package common

import (
	"encoding/hex"
	"testing"
)

func TestAddressSize(t *testing.T) {
	var addr Address
	if len(addr) != AddressLength {
		t.Fatalf("expected %d, got %d", AddressLength, len(addr))
	}
}

func TestBytesToAddress(t *testing.T) {
	b, _ := hex.DecodeString("41a614f803b6fd780986a42c78ec9c7f77e6ded13c")
	addr := BytesToAddress(b)
	if addr[0] != 0x41 {
		t.Fatalf("expected prefix 0x41, got 0x%x", addr[0])
	}
	if hex.EncodeToString(addr[:]) != "41a614f803b6fd780986a42c78ec9c7f77e6ded13c" {
		t.Fatalf("unexpected address: %x", addr)
	}
}

func TestAddressHex(t *testing.T) {
	b, _ := hex.DecodeString("41a614f803b6fd780986a42c78ec9c7f77e6ded13c")
	addr := BytesToAddress(b)
	if addr.Hex() != "41a614f803b6fd780986a42c78ec9c7f77e6ded13c" {
		t.Fatalf("expected 41a614f803b6fd780986a42c78ec9c7f77e6ded13c, got %s", addr.Hex())
	}
}

func TestEmptyAddress(t *testing.T) {
	var addr Address
	if !addr.IsEmpty() {
		t.Fatal("zero address should be empty")
	}
	b, _ := hex.DecodeString("41a614f803b6fd780986a42c78ec9c7f77e6ded13c")
	addr = BytesToAddress(b)
	if addr.IsEmpty() {
		t.Fatal("non-zero address should not be empty")
	}
}
