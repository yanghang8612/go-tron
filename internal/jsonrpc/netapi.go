package jsonrpc

import "fmt"

// NetAPI implements the "net" JSON-RPC namespace on the reflection-based
// internal/rpc framework. It is the migration target for the net_* arms of
// api.go's dispatch switch (jsonrpc-reflection). Method names map by the
// framework's reflection rule (first letter lowered): Version -> net_version,
// Listening -> net_listening, PeerCount -> net_peerCount.
//
// Each method mirrors the legacy handler in api.go byte-for-byte so the cutover
// is zero-diff against the frozen jsonrpc-corpus.
type NetAPI struct {
	backend Backend
}

// NewNetAPI builds a NetAPI over the given backend.
func NewNetAPI(backend Backend) *NetAPI { return &NetAPI{backend: backend} }

// Version serves net_version: the chain id rendered as a decimal string.
func (n *NetAPI) Version() string { return fmt.Sprintf("%d", n.backend.ChainID()) }

// Listening serves net_listening: whether the node has at least one peer.
func (n *NetAPI) Listening() bool { return n.backend.PeerCount() >= 1 }

// PeerCount serves net_peerCount: the connected peer count as 0x-hex.
func (n *NetAPI) PeerCount() string { return hexUint64(uint64(n.backend.PeerCount())) }
