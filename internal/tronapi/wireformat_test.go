package tronapi

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/crypto"
)

func TestParseAddress_Hex(t *testing.T) {
	got, err := parseAddress("4101", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// BytesToAddress left-pads short input to 21 bytes — matches the
	// historical common.FromHex + BytesToAddress chain that test fixtures
	// rely on.
	want := common.BytesToAddress([]byte{0x41, 0x01})
	if got != want {
		t.Fatalf("hex parse: got %x, want %x", got, want)
	}
}

func TestParseAddress_HexWith0xPrefix(t *testing.T) {
	got, err := parseAddress("0x4101", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := common.BytesToAddress([]byte{0x41, 0x01})
	if got != want {
		t.Fatalf("0x-prefix parse: got %x, want %x", got, want)
	}
}

func TestParseAddress_HexRejectsNonHex(t *testing.T) {
	// The audit-flagged silent-swallow bug: common.FromHex used to drop
	// hex.DecodeString's error and return nil, which BytesToAddress then
	// promoted into addr(0). Money sent to that "address" was lost.
	if _, err := parseAddress("not-hex-zzz", false); err == nil {
		t.Fatal("expected error for non-hex address, got nil")
	}
}

func TestParseAddress_HexRejectsEmpty(t *testing.T) {
	if _, err := parseAddress("", false); err == nil {
		t.Fatal("expected error for empty address, got nil")
	}
}

func TestParseAddress_Base58Visible(t *testing.T) {
	// Round-trip: encode a known address as Base58Check then parse it.
	want := common.Address{0x41, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13,
		0x14, 0x15}
	encoded := crypto.AddressToBase58(want)
	got, err := parseAddress(encoded, true)
	if err != nil {
		t.Fatalf("base58 parse: %v", err)
	}
	if got != want {
		t.Fatalf("base58 round-trip: got %x, want %x", got, want)
	}
}

func TestParseAddress_Base58RejectsInvalidChecksum(t *testing.T) {
	// Flip the last char so the Base58Check verify fails. The
	// alternative — Base58 decoding succeeding but yielding a different
	// address — would silently route to the wrong account, the same
	// failure class as the hex-swallow bug.
	addr := common.Address{0x41, 0x02, 0x03}
	encoded := crypto.AddressToBase58(addr)
	last := encoded[len(encoded)-1]
	swap := byte('A')
	if last == 'A' {
		swap = 'B'
	}
	corrupted := encoded[:len(encoded)-1] + string(swap)
	if _, err := parseAddress(corrupted, true); err == nil {
		t.Fatal("expected base58 checksum failure, got nil error")
	}
}

func TestParseBytes_HexOddLengthPadded(t *testing.T) {
	got, err := parseBytes("123", false)
	if err != nil {
		t.Fatalf("odd-length hex: %v", err)
	}
	// java-tron Hex.toBytes left-pads "123" → "0123" → [0x01, 0x23].
	want := []byte{0x01, 0x23}
	if string(got) != string(want) {
		t.Fatalf("odd-length: got %x, want %x", got, want)
	}
}

func TestParseBytes_HexRejectsNonHex(t *testing.T) {
	if _, err := parseBytes("https://x.example.com", false); err == nil {
		t.Fatal("expected non-hex bytes to be rejected (audit-flagged silent swallow)")
	}
}

func TestParseBytes_VisibleUtf8PassThrough(t *testing.T) {
	got, err := parseBytes("https://x.example.com", true)
	if err != nil {
		t.Fatalf("visible utf8: %v", err)
	}
	if string(got) != "https://x.example.com" {
		t.Fatalf("visible utf8: got %s, want passthrough", string(got))
	}
}
