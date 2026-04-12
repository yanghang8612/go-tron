package discover

import (
	"bytes"
	"net"
	"testing"
)

func TestGenerateNodeID(t *testing.T) {
	id1 := GenerateNodeID()
	id2 := GenerateNodeID()
	if len(id1) != NodeIDLen {
		t.Fatalf("expected %d bytes, got %d", NodeIDLen, len(id1))
	}
	if bytes.Equal(id1, id2) {
		t.Fatalf("expected distinct random IDs")
	}
}

func TestNodeEndpointStringAddress(t *testing.T) {
	n := &Node{
		IP:   net.ParseIP("192.168.1.1"),
		Port: 18888,
		ID:   make([]byte, NodeIDLen),
	}
	ep := n.Endpoint()
	if string(ep.Address) != "192.168.1.1" {
		t.Fatalf("address should be ASCII string, got %q", string(ep.Address))
	}
	if ep.Port != 18888 {
		t.Fatalf("port: %d", ep.Port)
	}
}

func TestEndpointToNodeRoundtrip(t *testing.T) {
	orig := &Node{
		IP:   net.ParseIP("10.0.0.5"),
		Port: 18888,
		ID:   bytes.Repeat([]byte{0xAB}, NodeIDLen),
	}
	ep := orig.Endpoint()
	got := EndpointToNode(ep)
	if !got.IP.Equal(orig.IP) {
		t.Fatalf("IP mismatch: got %v, want %v", got.IP, orig.IP)
	}
	if got.Port != orig.Port {
		t.Fatalf("port mismatch: got %d, want %d", got.Port, orig.Port)
	}
	if !bytes.Equal(got.ID, orig.ID) {
		t.Fatalf("ID mismatch")
	}
}

func TestNodeDistance(t *testing.T) {
	id1 := make([]byte, 64)
	id2 := make([]byte, 64)
	id2[0] = 0xFF
	d := LogDist(id1, id2)
	if d < 500 {
		t.Fatalf("expected large distance, got %d", d)
	}
}

func TestLogDistZero(t *testing.T) {
	id := make([]byte, 64)
	d := LogDist(id, id)
	if d != 0 {
		t.Fatalf("expected distance 0 for identical IDs, got %d", d)
	}
}
