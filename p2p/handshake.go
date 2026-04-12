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
	peerTime := time.UnixMilli(peer.Timestamp)
	skew := now.Sub(peerTime)
	if skew < 0 {
		skew = -skew
	}
	if skew > NetworkTimeDiff {
		return p2ppb.DisconnectReason_PING_TIMEOUT // libp2p uses TIME_OUT; our enum has PING_TIMEOUT matching this semantic
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
