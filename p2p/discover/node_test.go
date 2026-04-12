package discover

import (
	"net"
	"testing"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

func TestNodeIDFromKey(t *testing.T) {
	key, err := ethcrypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	id := PubKeyToNodeID(key.PublicKey)
	if len(id) != 64 {
		t.Fatalf("expected 64-byte node ID, got %d", len(id))
	}
}

func TestNodeEndpoint(t *testing.T) {
	n := &Node{
		IP:   net.ParseIP("192.168.1.1"),
		Port: 18888,
		ID:   make([]byte, 64),
	}
	ep := n.Endpoint()
	if ep.Port != 18888 {
		t.Fatalf("wrong port: %d", ep.Port)
	}
	if len(ep.Address) != 4 {
		t.Fatalf("expected 4-byte IPv4, got %d bytes", len(ep.Address))
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

func TestEndpointToNode(t *testing.T) {
	key, _ := ethcrypto.GenerateKey()
	id := PubKeyToNodeID(key.PublicKey)

	n := &Node{
		IP:   net.ParseIP("10.0.0.1"),
		Port: 18888,
		ID:   id,
	}
	ep := n.Endpoint()
	n2 := EndpointToNode(ep)
	if n2.Port != n.Port {
		t.Fatalf("port mismatch: %d vs %d", n2.Port, n.Port)
	}
	if string(n2.ID) != string(n.ID) {
		t.Fatal("ID mismatch after round-trip")
	}
}
