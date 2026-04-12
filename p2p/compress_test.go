package p2p

import (
	"bytes"
	"testing"
)

func TestCompressRoundtripUncompressed(t *testing.T) {
	payload := []byte("hello world")
	wrapped, err := WrapPostHandshake(0x20, payload)
	if err != nil {
		t.Fatal(err)
	}
	code, got, err := UnwrapPostHandshake(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0x20 {
		t.Fatalf("code: got %#x want 0x20", code)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload: got %q want %q", got, payload)
	}
}

func TestCompressRoundtripHighlyCompressible(t *testing.T) {
	// Snappy should win on this — all zeros.
	payload := make([]byte, 10000)
	wrapped, err := WrapPostHandshake(0x08, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(wrapped) >= len(payload) {
		t.Fatalf("expected wrapped < payload for compressible input; got wrapped=%d payload=%d",
			len(wrapped), len(payload))
	}
	code, got, err := UnwrapPostHandshake(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0x08 {
		t.Fatalf("code: got %#x want 0x08", code)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestCompressRoundtripEmptyPayload(t *testing.T) {
	wrapped, err := WrapPostHandshake(0xFF, nil)
	if err != nil {
		t.Fatal(err)
	}
	code, got, err := UnwrapPostHandshake(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0xFF {
		t.Fatalf("code: got %#x want 0xFF", code)
	}
	if len(got) != 0 {
		t.Fatalf("payload: got %d bytes, want 0", len(got))
	}
}
