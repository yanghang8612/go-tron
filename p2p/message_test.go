package p2p

import (
	"bytes"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

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
