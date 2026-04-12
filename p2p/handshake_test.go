package p2p

import (
	"testing"
	"time"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
)

func makeEndpoint(nodeID []byte) *corepb.Endpoint {
	return &corepb.Endpoint{
		Address: []byte("127.0.0.1"),
		Port:    18888,
		NodeId:  nodeID,
	}
}

func nodeID64() []byte {
	id := make([]byte, 64)
	for i := range id {
		id[i] = byte(i)
	}
	return id
}

func TestHelloRoundtrip(t *testing.T) {
	ep := makeEndpoint(nodeID64())
	msg := BuildHelloMessage(ep, 1, 0)
	payload, err := EncodeHello(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseHello(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.NetworkId != 1 || got.Version != Libp2pProtocolVersion {
		t.Fatalf("hello roundtrip mismatch: %+v", got)
	}
	if len(got.From.NodeId) != 64 {
		t.Fatalf("nodeId length: %d", len(got.From.NodeId))
	}
}

func TestValidateHelloNetworkMismatch(t *testing.T) {
	ep := makeEndpoint(nodeID64())
	msg := BuildHelloMessage(ep, 2 /* peer's network */, 0)
	reason := ValidateHello(msg, 1 /* ours */, Libp2pProtocolVersion, time.Now())
	if reason != p2ppb.DisconnectReason_DIFFERENT_VERSION {
		t.Fatalf("expected DIFFERENT_VERSION, got %v", reason)
	}
}

func TestValidateHelloVersionMismatch(t *testing.T) {
	ep := makeEndpoint(nodeID64())
	msg := BuildHelloMessage(ep, 1, 0)
	msg.Version = 99
	reason := ValidateHello(msg, 1, 1, time.Now())
	if reason != p2ppb.DisconnectReason_DIFFERENT_VERSION {
		t.Fatalf("expected DIFFERENT_VERSION, got %v", reason)
	}
}

func TestValidateHelloClockSkew(t *testing.T) {
	ep := makeEndpoint(nodeID64())
	msg := BuildHelloMessage(ep, 1, 0)
	// Peer timestamp is 10 seconds ahead of our time
	msg.Timestamp = time.Now().Add(10 * time.Second).UnixMilli()
	reason := ValidateHello(msg, 1, 1, time.Now())
	// Expect a time-related rejection (either PING_TIMEOUT or whatever we picked)
	if reason == p2ppb.DisconnectReason_PEER_QUITING {
		t.Fatalf("expected rejection due to clock skew, got accept")
	}
}

func TestValidateHelloAccept(t *testing.T) {
	ep := makeEndpoint(nodeID64())
	msg := BuildHelloMessage(ep, 1, 0)
	reason := ValidateHello(msg, 1, 1, time.Now())
	if reason != p2ppb.DisconnectReason_PEER_QUITING { // 0 = accept
		t.Fatalf("expected accept (PEER_QUITING=0), got %v", reason)
	}
}

func TestValidateHelloBadNodeID(t *testing.T) {
	ep := &corepb.Endpoint{Address: []byte("127.0.0.1"), Port: 18888, NodeId: []byte{0x01}} // too short
	msg := BuildHelloMessage(ep, 1, 0)
	reason := ValidateHello(msg, 1, 1, time.Now())
	if reason != p2ppb.DisconnectReason_BAD_MESSAGE {
		t.Fatalf("expected BAD_MESSAGE, got %v", reason)
	}
}

func TestKeepAliveRoundtrip(t *testing.T) {
	msg := BuildKeepAlive()
	payload, err := EncodeKeepAlive(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseKeepAlive(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Timestamp != msg.Timestamp {
		t.Fatalf("timestamp mismatch: %d vs %d", got.Timestamp, msg.Timestamp)
	}
}

func TestDisconnectRoundtrip(t *testing.T) {
	msg := BuildDisconnect(p2ppb.DisconnectReason_DIFFERENT_VERSION)
	payload, err := EncodeDisconnect(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseDisconnect(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.Reason != p2ppb.DisconnectReason_DIFFERENT_VERSION {
		t.Fatalf("reason mismatch: %v", got.Reason)
	}
}

func TestStatusRoundtrip(t *testing.T) {
	ep := makeEndpoint(nodeID64())
	msg := BuildStatusMessage(ep, 1, 50, 5)
	payload, err := EncodeStatus(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseStatus(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxConnections != 50 || got.CurrentConnections != 5 {
		t.Fatalf("status mismatch: %+v", got)
	}
}
