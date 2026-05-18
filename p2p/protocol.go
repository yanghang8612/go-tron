package p2p

import "time"

const (
	// Message type codes — match java-tron
	MsgTx             byte = 0x01
	MsgBlock          byte = 0x02
	// MsgTrxs (TRXS, 0x03) carries a `protocol.Transactions` proto (a
	// repeated Transaction list). java-tron's P2pEventHandlerImpl only
	// dispatches TRXS to the transactions handler — single-TRX (0x01)
	// messages from peers fall through to NO_SUCH_MESSAGE and trigger
	// disconnect. Always wrap outbound tx data in TRXS even when the
	// payload is a single transaction.
	MsgTrxs           byte = 0x03
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

// MsgName returns the protocol-message name for a 1-byte type code. Falls back
// to an empty string for unknown codes; callers should include the raw byte
// separately.
func MsgName(code byte) string {
	switch code {
	case MsgTx:
		return "TX"
	case MsgBlock:
		return "BLOCK"
	case MsgTrxs:
		return "TRXS"
	case MsgInventory:
		return "INVENTORY"
	case MsgFetchInvData:
		return "FETCH_INV_DATA"
	case MsgSyncBlockChain:
		return "SYNC_BLOCK_CHAIN"
	case MsgChainInventory:
		return "CHAIN_INVENTORY"
	case MsgHello:
		return "HELLO"
	case MsgDisconnect:
		return "DISCONNECT"
	case MsgPing:
		return "PING"
	case MsgPong:
		return "PONG"
	case MsgPbftCommitMsg:
		return "PBFT_COMMIT_MSG"
	case MsgPbftMsg:
		return "PBFT_MSG"
	case MsgLibp2pHello:
		return "P2P_HELLO"
	case MsgLibp2pStatus:
		return "P2P_STATUS"
	case MsgLibp2pDisconnect:
		return "P2P_DISCONNECT"
	case MsgLibp2pKeepAlivePing:
		return "P2P_KEEPALIVE_PING"
	case MsgLibp2pKeepAlivePong:
		return "P2P_KEEPALIVE_PONG"
	default:
		return ""
	}
}
