package discover

import (
	"bytes"
	"testing"

	discoverpb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestEncodeDecodePing(t *testing.T) {
	from := &discoverpb.Endpoint{
		Address: []byte("127.0.0.1"), // ASCII bytes
		Port:    18888,
		NodeId:  bytes.Repeat([]byte{0xAB}, 64),
	}
	to := &discoverpb.Endpoint{
		Address: []byte("192.168.1.1"),
		Port:    18888,
	}
	ping := &discoverpb.PingMessage{From: from, To: to, Version: 1, Timestamp: 1000}

	data, err := EncodeMessage(MsgPing, ping)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != MsgPing {
		t.Fatalf("type byte: got %#x, want %#x", data[0], MsgPing)
	}
	// Plain wire: byte 0 is type, rest is proto payload.
	expected, _ := proto.Marshal(ping)
	if !bytes.Equal(data[1:], expected) {
		t.Fatalf("payload mismatch")
	}

	msgType, payload, err := DecodeMessage(data)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgPing {
		t.Fatalf("type: %d", msgType)
	}
	var got discoverpb.PingMessage
	if err := proto.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.From.Address) != "127.0.0.1" {
		t.Fatalf("address: got %q, want 127.0.0.1", got.From.Address)
	}
	if !bytes.Equal(got.From.NodeId, from.NodeId) {
		t.Fatalf("nodeId mismatch")
	}
	if got.Version != 1 || got.Timestamp != 1000 {
		t.Fatalf("ping fields: %+v", &got)
	}
}

func TestDecodeMessageTooShort(t *testing.T) {
	_, _, err := DecodeMessage([]byte{0x01})
	if err == nil {
		t.Fatal("expected error for 1-byte packet")
	}
}
