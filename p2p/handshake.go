package p2p

import (
	"fmt"
	"time"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
	"google.golang.org/protobuf/proto"
)

// ── HelloMessage (HANDSHAKE_HELLO, 0xFD) ──────────────────────────────────────

// BuildHelloMessage constructs a libp2p HANDSHAKE_HELLO payload.
// `code` is the DisconnectReason on rejection; use 0 (PEER_QUITING) on accept.
func BuildHelloMessage(localEP *corepb.Endpoint, networkID int32, code int32) *p2ppb.HelloMessage {
	return &p2ppb.HelloMessage{
		From:      localEP,
		NetworkId: networkID,
		Version:   Libp2pProtocolVersion,
		Code:      code,
		Timestamp: time.Now().UnixMilli(),
	}
}

// EncodeHello marshals a HelloMessage to bytes (payload only — caller frames
// with WriteMsg(w, MsgLibp2pHello, payload)).
func EncodeHello(msg *p2ppb.HelloMessage) ([]byte, error) {
	return proto.Marshal(msg)
}

// ParseHello deserializes a HelloMessage payload.
func ParseHello(payload []byte) (*p2ppb.HelloMessage, error) {
	var msg p2ppb.HelloMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("parse hello: %w", err)
	}
	return &msg, nil
}

// ValidateHello checks the peer's hello message for compatibility.
// Returns DisconnectReason_PEER_QUITING (value 0) on accept — this is the zero
// value of the enum and means "no problem". Any other value indicates rejection.
//
// NOTE on timestamp: java-tron libp2p `HelloMessage.valid()` only validates
// the From endpoint, NOT the timestamp (libp2p:HelloMessage.java:58-61). The
// `NetworkTimeDiff` skew check belongs to keepalive Ping/Pong validation
// (libp2p:PingMessage.java:31). Earlier versions of this function rejected
// peers whose Hello timestamp was >1s behind ours, which was wrong on two
// counts: (1) java-tron sets Hello.timestamp to channel.getStartTime() —
// the TCP-accept time — so a busy seed's Hello can legitimately carry a
// "stale" timestamp by several seconds; (2) java-tron itself never gates
// on this field. Mainnet sync against the public seed list would 100%
// fail on the first hello round-trip until we removed the check.
func ValidateHello(peer *p2ppb.HelloMessage, ourNetworkID, ourVersion int32, now time.Time) p2ppb.DisconnectReason {
	if peer == nil || peer.From == nil || len(peer.From.NodeId) != 64 {
		return p2ppb.DisconnectReason_BAD_MESSAGE
	}
	if peer.NetworkId != ourNetworkID {
		return p2ppb.DisconnectReason_DIFFERENT_VERSION
	}
	if peer.Version != ourVersion {
		return p2ppb.DisconnectReason_DIFFERENT_VERSION
	}
	return p2ppb.DisconnectReason_PEER_QUITING // "0" = accept
}

// ── StatusMessage (STATUS, 0xFC) ──────────────────────────────────────────────

func BuildStatusMessage(localEP *corepb.Endpoint, networkID, maxConn, curConn int32) *p2ppb.StatusMessage {
	return &p2ppb.StatusMessage{
		From:               localEP,
		Version:            Libp2pProtocolVersion,
		NetworkId:          networkID,
		MaxConnections:     maxConn,
		CurrentConnections: curConn,
		Timestamp:          time.Now().UnixMilli(),
	}
}

func EncodeStatus(msg *p2ppb.StatusMessage) ([]byte, error) { return proto.Marshal(msg) }

func ParseStatus(payload []byte) (*p2ppb.StatusMessage, error) {
	var msg p2ppb.StatusMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("parse status: %w", err)
	}
	return &msg, nil
}

// ── KeepAliveMessage (KEEP_ALIVE_PING 0xFF / _PONG 0xFE) ──────────────────────

func BuildKeepAlive() *p2ppb.KeepAliveMessage {
	return &p2ppb.KeepAliveMessage{Timestamp: time.Now().UnixMilli()}
}

func EncodeKeepAlive(msg *p2ppb.KeepAliveMessage) ([]byte, error) { return proto.Marshal(msg) }

func ParseKeepAlive(payload []byte) (*p2ppb.KeepAliveMessage, error) {
	var msg p2ppb.KeepAliveMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("parse keepalive: %w", err)
	}
	return &msg, nil
}

// ── DisconnectMessage (DISCONNECT, 0xFB) ──────────────────────────────────────

func BuildDisconnect(reason p2ppb.DisconnectReason) *p2ppb.P2PDisconnectMessage {
	return &p2ppb.P2PDisconnectMessage{Reason: reason}
}

func EncodeDisconnect(msg *p2ppb.P2PDisconnectMessage) ([]byte, error) {
	return proto.Marshal(msg)
}

func ParseDisconnect(payload []byte) (*p2ppb.P2PDisconnectMessage, error) {
	var msg p2ppb.P2PDisconnectMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("parse disconnect: %w", err)
	}
	return &msg, nil
}
