package discover

import (
	"crypto/rand"
	"math/bits"
	"net"

	discoverpb "github.com/tronprotocol/go-tron/proto/core"
)

// NodeIDLen is java-tron libp2p's Constant.NODE_ID_LEN.
const NodeIDLen = 64

// Node represents a discovered TRON peer.
type Node struct {
	ID   []byte // 64 random bytes (not a cryptographic key)
	IP   net.IP
	Port int
}

// GenerateNodeID returns 64 random bytes from crypto/rand, for use as a TRON
// node ID. Matches libp2p NetUtil.getNodeId().
func GenerateNodeID() []byte {
	id := make([]byte, NodeIDLen)
	if _, err := rand.Read(id); err != nil {
		panic(err) // crypto/rand should not fail
	}
	return id
}

// Endpoint builds a proto Endpoint for this node.
// The Address field holds IP as ASCII string bytes — matches libp2p
// KadMessage.getEndpointFromNode which does ByteArray.fromString(host).
func (n *Node) Endpoint() *discoverpb.Endpoint {
	ep := &discoverpb.Endpoint{NodeId: n.ID, Port: int32(n.Port)}
	if n.IP == nil {
		return ep
	}
	if ip4 := n.IP.To4(); ip4 != nil {
		ep.Address = []byte(ip4.String())
	} else {
		ep.AddressIpv6 = []byte(n.IP.String())
	}
	return ep
}

// EndpointToNode converts a proto Endpoint back to a Node.
// Parses the ASCII string address back into a net.IP.
func EndpointToNode(ep *discoverpb.Endpoint) *Node {
	n := &Node{ID: ep.NodeId, Port: int(ep.Port)}
	if len(ep.Address) > 0 {
		n.IP = net.ParseIP(string(ep.Address))
	} else if len(ep.AddressIpv6) > 0 {
		n.IP = net.ParseIP(string(ep.AddressIpv6))
	}
	return n
}

// LogDist returns the XOR log-distance between two 64-byte node IDs.
// Used for bucket selection only — does not affect wire compatibility.
func LogDist(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		x := a[i] ^ b[i]
		if x != 0 {
			return (n-i-1)*8 + bits.Len8(x)
		}
	}
	return 0
}
