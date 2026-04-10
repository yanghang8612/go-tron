package vm

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"golang.org/x/crypto/ripemd160"
)

func TestPrecompileSHA256(t *testing.T) {
	p := &sha256hash{}
	input := []byte("hello world")
	expected := sha256.Sum256(input)

	result, cost, err := p.Run(input, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 72 {
		t.Fatalf("expected cost 72, got %d", cost)
	}
	if hex.EncodeToString(result) != hex.EncodeToString(expected[:]) {
		t.Fatalf("hash mismatch")
	}
}

func TestPrecompileRIPEMD160(t *testing.T) {
	p := &ripemd160hash{}
	input := []byte("hello world")

	h := ripemd160.New()
	h.Write(input)
	expectedDigest := h.Sum(nil)

	result, cost, err := p.Run(input, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 720 {
		t.Fatalf("expected cost 720, got %d", cost)
	}
	if len(result) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(result))
	}
	actualDigest := result[32-len(expectedDigest):]
	if hex.EncodeToString(actualDigest) != hex.EncodeToString(expectedDigest) {
		t.Fatalf("ripemd160 mismatch")
	}
}

func TestPrecompileDataCopy(t *testing.T) {
	p := &dataCopy{}
	input := []byte{0x01, 0x02, 0x03, 0x04}

	result, cost, err := p.Run(input, 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cost != 18 {
		t.Fatalf("expected cost 18, got %d", cost)
	}
	if string(result) != string(input) {
		t.Fatalf("data copy mismatch")
	}
}

func TestPrecompileOutOfEnergy(t *testing.T) {
	p := &sha256hash{}
	_, _, err := p.Run([]byte("test"), 1)
	if err != ErrOutOfEnergy {
		t.Fatalf("expected ErrOutOfEnergy, got %v", err)
	}
}
