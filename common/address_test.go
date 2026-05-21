package common

import (
	"bytes"
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

func TestAddressAccountID(t *testing.T) {
	var addr Address
	addr[0] = AddressPrefixMainnet
	for i := 1; i < AddressLength; i++ {
		addr[i] = byte(i)
	}
	id := addr.AccountID()
	if len(id.Bytes()) != AccountIDLength {
		t.Fatalf("AccountID len = %d, want %d", len(id.Bytes()), AccountIDLength)
	}
	if !bytes.Equal(id.Bytes(), addr[1:]) {
		t.Fatalf("AccountID = %x, want %x", id.Bytes(), addr[1:])
	}
}

func TestAccountIDRoundTrip(t *testing.T) {
	var addr Address
	addr[0] = AddressPrefixMainnet
	for i := 1; i < AddressLength; i++ {
		addr[i] = byte(0xF0 + i)
	}
	got := addr.AccountID().Address(AddressPrefixMainnet)
	if got != addr {
		t.Fatalf("round-trip = %x, want %x", got.Bytes(), addr.Bytes())
	}
}

func TestAccountIDIgnoresPrefix(t *testing.T) {
	// Same 20-byte identity, different network prefix -> identical AccountID.
	var a, b Address
	a[0], b[0] = AddressPrefixMainnet, AddressPrefixTestnet
	for i := 1; i < AddressLength; i++ {
		a[i], b[i] = byte(i), byte(i)
	}
	if a.AccountID() != b.AccountID() {
		t.Fatal("AccountID must ignore the network prefix byte")
	}
}
