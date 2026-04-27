package p2p

import "time"

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
	MsgPbftCommitMsg  byte = 0x14 // PBFT_COMMIT_MSG — pre-aggregated commit result
	MsgPbftMsg        byte = 0x34 // PBFT_MSG — three-phase PBFT protocol message

	// libp2p control messages (TCP layer) — match io.github.tronprotocol/libp2p's
	// connection/message/MessageType.java enum.
	MsgLibp2pHello         byte = 0xFD
	MsgLibp2pStatus        byte = 0xFC
	MsgLibp2pDisconnect    byte = 0xFB
	MsgLibp2pKeepAlivePing byte = 0xFF
	MsgLibp2pKeepAlivePong byte = 0xFE

	// MaxMessageSize is the maximum allowed frame size (5 MB).
	// Matches java-tron libp2p's Parameter.MAX_MESSAGE_LENGTH used by
	// P2pProtobufVarint32FrameDecoder.
	MaxMessageSize = 5 * 1024 * 1024

	// ProtocolVersion is the P2P protocol version for handshake.
	ProtocolVersion int32 = 1
)

// Network-timing constants (libp2p Parameter.java)
const (
	// KeepAliveTimeout is the max interval between successive KEEP_ALIVE_PINGs
	// before the peer is considered dead. Matches libp2p KEEP_ALIVE_TIMEOUT.
	KeepAliveTimeout = 20 * time.Second

	// NetworkTimeDiff is the max tolerated clock skew between peers during
	// handshake. Matches libp2p NETWORK_TIME_DIFF.
	NetworkTimeDiff = 1 * time.Second

	// Libp2pProtocolVersion is the value sent in HelloMessage.version. Matches
	// libp2p Parameter.version.
	Libp2pProtocolVersion int32 = 1
)

// Handler processes messages from a connected peer.
type Handler interface {
	OnPeerConnected(p *Peer)
	OnPeerDisconnected(p *Peer)
	OnMessage(p *Peer, code byte, payload []byte)
}
