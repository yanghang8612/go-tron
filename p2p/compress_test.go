package p2p

import (
	"bytes"
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

var unwrapPayloadBenchmarkSink []byte

func BenchmarkUnwrapPostHandshakeSnappy(b *testing.B) {
	for _, size := range []int{1024, 64 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			payload := make([]byte, size)
			wrapped, err := WrapPostHandshake(MsgBlock, payload)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				_, got, err := UnwrapPostHandshake(wrapped)
				if err != nil {
					b.Fatal(err)
				}
				unwrapPayloadBenchmarkSink = got
			}
		})
	}
}

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

func TestUnwrapPayloadOwnsFrameBytes(t *testing.T) {
	want := []byte("payload retained after the frame scratch is reused")
	wrapped, err := WrapPostHandshake(0x20, want)
	if err != nil {
		t.Fatal(err)
	}
	code, got, err := UnwrapPostHandshake(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	clear(wrapped)
	if code != 0x20 || !bytes.Equal(got, want) {
		t.Fatalf("payload changed after frame reuse: code=%#x got=%q want=%q", code, got, want)
	}
}

func TestCanonicalSnappyFrameFastPathAndFallback(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, 10_000)
	wrapped, err := WrapPostHandshake(MsgBlock, want)
	if err != nil {
		t.Fatal(err)
	}
	compressed, ok := canonicalSnappyFrameData(wrapped)
	if !ok {
		t.Fatal("normal snappy wrapper missed the canonical fast path")
	}

	// Unknown fields are valid protobuf and must use the generated-decoder
	// fallback rather than being silently accepted by the narrow fast path.
	extended := protowire.AppendTag(append([]byte(nil), wrapped...), 100, protowire.VarintType)
	extended = protowire.AppendVarint(extended, 7)
	if _, ok := canonicalSnappyFrameData(extended); ok {
		t.Fatal("extended wrapper incorrectly accepted as canonical")
	}
	code, got, err := UnwrapPostHandshake(extended)
	if err != nil || code != MsgBlock || !bytes.Equal(got, want) {
		t.Fatalf("fallback wrapper decode = code %#x payload %d err %v", code, len(got), err)
	}

	// A valid field-order permutation likewise falls back and retains protobuf
	// compatibility. compressed aliases wrapped only while this test builds it.
	reordered := protowire.AppendTag(nil, 2, protowire.BytesType)
	reordered = protowire.AppendBytes(reordered, compressed)
	reordered = protowire.AppendTag(reordered, 1, protowire.VarintType)
	reordered = protowire.AppendVarint(reordered, 1)
	if _, ok := canonicalSnappyFrameData(reordered); ok {
		t.Fatal("reordered wrapper incorrectly accepted as canonical")
	}
	code, got, err = UnwrapPostHandshake(reordered)
	if err != nil || code != MsgBlock || !bytes.Equal(got, want) {
		t.Fatalf("reordered wrapper decode = code %#x payload %d err %v", code, len(got), err)
	}
}
