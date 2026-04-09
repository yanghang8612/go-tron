package crypto

import (
	"encoding/hex"
	"testing"
)

func TestGenerateKey(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	privBytes := PrivateKeyToBytes(key)
	if len(privBytes) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(privBytes))
	}
}

func TestSignAndRecover(t *testing.T) {
	key, _ := GenerateKey()
	msg := Keccak256([]byte("test message"))
	sig, err := Sign(msg, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(sig) != 65 {
		t.Fatalf("expected 65 byte sig, got %d", len(sig))
	}
	pub, err := SigToPub(msg, sig)
	if err != nil {
		t.Fatal(err)
	}
	expectedPub := PubkeyToBytes(&key.PublicKey)
	recoveredPub := PubkeyToBytes(pub)
	if hex.EncodeToString(expectedPub) != hex.EncodeToString(recoveredPub) {
		t.Fatal("recovered pubkey does not match")
	}
}

func TestKeccak256(t *testing.T) {
	hash := Keccak256([]byte(""))
	expected := "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
	if hex.EncodeToString(hash) != expected {
		t.Fatalf("expected %s, got %s", expected, hex.EncodeToString(hash))
	}
}
