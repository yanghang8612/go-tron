package p2p

import (
	"bytes"
	"testing"
)

func TestVarint32Roundtrip(t *testing.T) {
	cases := []uint32{0, 1, 127, 128, 16383, 16384, 1 << 21, 1 << 28, (1 << 31) - 1}
	for _, v := range cases {
		var buf bytes.Buffer
		if err := WriteVarint32(&buf, v); err != nil {
			t.Fatalf("write %d: %v", v, err)
		}
		got, err := ReadVarint32(&buf)
		if err != nil {
			t.Fatalf("read %d: %v", v, err)
		}
		if got != v {
			t.Fatalf("roundtrip %d → %d", v, got)
		}
	}
}

// Google protobuf docs: 300 = 0xAC 0x02 in varint.
func TestVarint32KnownVectors(t *testing.T) {
	var buf bytes.Buffer
	WriteVarint32(&buf, 300)
	want := []byte{0xAC, 0x02}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("300: got %x, want %x", buf.Bytes(), want)
	}
}

func TestVarint32MaxSize(t *testing.T) {
	var buf bytes.Buffer
	WriteVarint32(&buf, (1<<31)-1)
	if buf.Len() != 5 {
		t.Fatalf("uint32 max encoded in %d bytes, want 5", buf.Len())
	}
}

func TestWriteReadMsgFraming(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte("hello")
	if err := WriteMsg(&buf, 0x20, payload); err != nil {
		t.Fatal(err)
	}
	// varint(6) = 0x06, then type 0x20, then 'hello'
	want := []byte{0x06, 0x20, 'h', 'e', 'l', 'l', 'o'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("framing: got %x, want %x", buf.Bytes(), want)
	}

	code, got, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0x20 || !bytes.Equal(got, payload) {
		t.Fatalf("read: code=%#x payload=%x", code, got)
	}
}

func TestReadMsgRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	// Write varint for 6 MB — exceeds 5 MB max.
	WriteVarint32(&buf, 6*1024*1024)
	_, _, err := ReadMsg(&buf)
	if err == nil {
		t.Fatal("expected error for oversize frame")
	}
}

func TestReadMsgRejectsEmpty(t *testing.T) {
	var buf bytes.Buffer
	WriteVarint32(&buf, 0)
	_, _, err := ReadMsg(&buf)
	if err == nil {
		t.Fatal("expected error for empty frame")
	}
}
