package common

import (
	"encoding/hex"
	"testing"
)

func TestHashSize(t *testing.T) {
	var h Hash
	if len(h) != HashLength {
		t.Fatalf("expected %d, got %d", HashLength, len(h))
	}
}

func TestBytesToHash(t *testing.T) {
	b := make([]byte, 32)
	b[31] = 0xff
	h := BytesToHash(b)
	if h[31] != 0xff {
		t.Fatal("expected last byte 0xff")
	}
}

func TestHashHex(t *testing.T) {
	b := make([]byte, 32)
	b[0] = 0xab
	h := BytesToHash(b)
	if h.Hex()[:2] != "ab" {
		t.Fatalf("expected prefix ab, got %s", h.Hex()[:2])
	}
}

func TestHexToHash(t *testing.T) {
	hexStr := "e58f33f9baf9305dc6f82b9f1934ea8f0ade2defb951258d50167028c780351f"
	h := HexToHash(hexStr)
	if hex.EncodeToString(h[:]) != hexStr {
		t.Fatal("round trip failed")
	}
}
