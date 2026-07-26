package p2p

import (
	"bytes"
	"fmt"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var frameBodyBenchmarkSink []byte

func BenchmarkReadFrameBody(b *testing.B) {
	for _, size := range []int{64, 16 << 10, 128 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			payload := make([]byte, size)
			var framed bytes.Buffer
			if err := WriteFrameBody(&framed, payload); err != nil {
				b.Fatal(err)
			}
			frame := framed.Bytes()
			var reader bytes.Reader
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				reader.Reset(frame)
				body, err := ReadFrameBody(&reader)
				if err != nil {
					b.Fatal(err)
				}
				frameBodyBenchmarkSink = body
			}
		})
	}
}

func BenchmarkReadFrameBodyReuse(b *testing.B) {
	for _, size := range []int{64, 16 << 10, 128 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			payload := make([]byte, size)
			var framed bytes.Buffer
			if err := WriteFrameBody(&framed, payload); err != nil {
				b.Fatal(err)
			}
			frame := framed.Bytes()
			var reader bytes.Reader
			bodyBuf := make([]byte, 0, size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				reader.Reset(frame)
				body, err := readFrameBodyInto(&reader, bodyBuf)
				if err != nil {
					b.Fatal(err)
				}
				frameBodyBenchmarkSink = body
				bodyBuf = body[:0]
			}
		})
	}
}

func TestEncodeDecodeMessage(t *testing.T) {
	inv := &corepb.Inventory{
		Type: corepb.Inventory_BLOCK,
		Ids:  [][]byte{{1, 2, 3}},
	}
	data, _ := proto.Marshal(inv)

	var buf bytes.Buffer
	err := WriteMsg(&buf, MsgInventory, data)
	if err != nil {
		t.Fatal(err)
	}

	code, payload, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != MsgInventory {
		t.Fatalf("code: want 0x%02x, got 0x%02x", MsgInventory, code)
	}
	if !bytes.Equal(payload, data) {
		t.Fatal("payload mismatch")
	}
}

func TestReadMsgTooLarge(t *testing.T) {
	var buf bytes.Buffer
	// Write a varint frame claiming 20 MB — exceeds MaxMessageSize (5 MB).
	WriteVarint32(&buf, 20*1024*1024)
	_, _, err := ReadMsg(&buf)
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
}

func TestPingPongEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMsg(&buf, MsgPing, nil)
	if err != nil {
		t.Fatal(err)
	}
	code, payload, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != MsgPing {
		t.Fatalf("code: want 0x%02x, got 0x%02x", MsgPing, code)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(payload))
	}
}
