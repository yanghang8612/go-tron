package p2p

const (
	// Message type codes — match java-tron
	MsgTx             byte = 0x01
	MsgBlock          byte = 0x02
	MsgInventory      byte = 0x06
	MsgFetchInvData   byte = 0x07
	MsgSyncBlockChain byte = 0x08
	MsgChainInventory byte = 0x09
	MsgHello          byte = 0x20
	MsgDisconnect     byte = 0x21
	MsgPing           byte = 0x22
	MsgPong           byte = 0x23

	// MaxMessageSize is the maximum allowed message payload (10 MB).
	MaxMessageSize = 10 * 1024 * 1024

	// ProtocolVersion is the P2P protocol version for handshake.
	ProtocolVersion int32 = 1
)

// Handler processes messages from a connected peer.
type Handler interface {
	OnPeerConnected(p *Peer)
	OnPeerDisconnected(p *Peer)
	OnMessage(p *Peer, code byte, payload []byte)
}
