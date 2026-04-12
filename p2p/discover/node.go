package discover

import (
	"crypto/ecdsa"
	"math/bits"
	"net"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	discoverpb "github.com/tronprotocol/go-tron/proto/core"
)

// Node represents a discovered TRON network node.
type Node struct {
	ID   []byte // 64-byte secp256k1 public key (uncompressed, no 0x04 prefix)
	IP   net.IP
	Port int
}

// PubKeyToNodeID returns the 64-byte node ID from an ECDSA public key.
func PubKeyToNodeID(pub ecdsa.PublicKey) []byte {
	b := ethcrypto.FromECDSAPub(&pub)
	// b[0] is 0x04 (uncompressed prefix) — skip it
	id := make([]byte, 64)
	copy(id, b[1:])
	return id
}

// Endpoint builds a proto Endpoint for this node.
func (n *Node) Endpoint() *discoverpb.Endpoint {
	ep := &discoverpb.Endpoint{
		NodeId: n.ID,
		Port:   int32(n.Port),
	}
	if ip4 := n.IP.To4(); ip4 != nil {
		ep.Address = ip4
	} else if n.IP != nil {
		ep.AddressIpv6 = n.IP.To16()
	}
	return ep
}

// EndpointToNode converts a proto Endpoint to a Node.
func EndpointToNode(ep *discoverpb.Endpoint) *Node {
	n := &Node{
		ID:   ep.NodeId,
		Port: int(ep.Port),
	}
	if len(ep.Address) == 4 {
		n.IP = net.IP(ep.Address)
	} else if len(ep.AddressIpv6) == 16 {
		n.IP = net.IP(ep.AddressIpv6)
	}
	return n
}

// LogDist returns the XOR log-distance between two node IDs.
// Each ID should be 64 bytes (512 bits); result is in range [0, 512].
func LogDist(a, b []byte) int {
	lca := len(a)
	if len(b) < lca {
		lca = len(b)
	}
	for i := 0; i < lca; i++ {
		x := a[i] ^ b[i]
		if x != 0 {
			return (lca-i-1)*8 + bits.Len8(x)
		}
	}
	return 0
}
