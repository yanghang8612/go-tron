package p2p_test

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
	"google.golang.org/protobuf/proto"
)

func TestConnectHelloRoundtrip(t *testing.T) {
	h := &p2ppb.HelloMessage{
		From:      &corepb.Endpoint{NodeId: []byte("nodeid"), Port: 18888},
		NetworkId: 1,
		Version:   1,
		Code:      0,
		Timestamp: 1000,
	}
	data, err := proto.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	var got p2ppb.HelloMessage
	if err := proto.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.NetworkId != 1 || got.Version != 1 || got.Timestamp != 1000 {
		t.Fatalf("roundtrip mismatch: %+v", &got)
	}
}

func TestDisconnectReasonValues(t *testing.T) {
	if p2ppb.DisconnectReason_DIFFERENT_VERSION != 4 {
		t.Fatalf("DIFFERENT_VERSION should be 4, got %d", p2ppb.DisconnectReason_DIFFERENT_VERSION)
	}
	if p2ppb.DisconnectReason_UNKNOWN != 255 {
		t.Fatalf("UNKNOWN should be 255, got %d", p2ppb.DisconnectReason_UNKNOWN)
	}
}
