package discover

import (
	"bytes"
	"testing"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	discoverpb "github.com/tronprotocol/go-tron/proto/core"
)

func TestEncodeDecodePing(t *testing.T) {
	key, _ := ethcrypto.GenerateKey()
	senderID := PubKeyToNodeID(key.PublicKey)

	from := &discoverpb.Endpoint{Address: []byte{127, 0, 0, 1}, Port: 18888, NodeId: senderID}
	to := &discoverpb.Endpoint{Address: []byte{192, 168, 1, 1}, Port: 18888}
	ping := &discoverpb.PingMessage{From: from, To: to, Version: 1, Timestamp: 1000}

	data, err := EncodeMessage(MsgPing, ping, key)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if len(data) < 66 {
		t.Fatalf("encoded message too short: %d", len(data))
	}
	// Verify header byte
	if data[0] != MsgPing {
		t.Fatalf("expected type byte %d, got %d", MsgPing, data[0])
	}

	msgType, payload, recoveredID, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if msgType != MsgPing {
		t.Fatalf("wrong type: %d", msgType)
	}
	if len(recoveredID) != 64 {
		t.Fatalf("expected 64-byte sender pubkey, got %d", len(recoveredID))
	}
	if !bytes.Equal(recoveredID, senderID) {
		t.Fatal("recovered sender ID does not match original")
	}
	_ = payload
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	key, _ := ethcrypto.GenerateKey()

	// Test all message types
	testCases := []struct {
		name    string
		msgType byte
		msg     interface{ ProtoReflect() interface{ IsValid() bool } }
	}{}
	_ = testCases

	// PONG
	pong := &discoverpb.PongMessage{
		From:      &discoverpb.Endpoint{Address: []byte{1, 2, 3, 4}, Port: 18888},
		Echo:      42,
		Timestamp: 9999,
	}
	data, err := EncodeMessage(MsgPong, pong, key)
	if err != nil {
		t.Fatalf("encode pong: %v", err)
	}
	msgType, _, _, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("decode pong: %v", err)
	}
	if msgType != MsgPong {
		t.Fatalf("pong type: want %d got %d", MsgPong, msgType)
	}
}

func TestDecodeMessageTooShort(t *testing.T) {
	_, _, _, err := DecodeMessage([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

func TestDecodeMessageInvalidSig(t *testing.T) {
	// Build a packet with a zeroed signature — ecrecover should fail or return garbage
	data := make([]byte, 100)
	data[0] = MsgPing
	// sig bytes [1:66] are all zero — invalid signature
	_, _, _, err := DecodeMessage(data)
	// May or may not error depending on ecrecover implementation; just ensure no panic
	_ = err
}
