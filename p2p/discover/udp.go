package discover

import (
	"crypto/ecdsa"
	"errors"
	"net"
	"time"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	discoverpb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Message type codes (matches java-tron NodeManager constants).
const (
	MsgPing       byte = 1
	MsgPong       byte = 2
	MsgFindNode   byte = 3
	MsgNeighbours byte = 4
)

// EncodeMessage serializes a discovery message with type prefix and ECDSA signature.
// Wire format: [type(1)][sig(65)][proto payload]
// Signature is over Keccak256(payload).
func EncodeMessage(msgType byte, msg proto.Message, privKey *ecdsa.PrivateKey) ([]byte, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	hash := ethcrypto.Keccak256(payload)
	sig, err := ethcrypto.Sign(hash, privKey)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 1+65+len(payload))
	buf[0] = msgType
	copy(buf[1:66], sig)
	copy(buf[66:], payload)
	return buf, nil
}

// DecodeMessage parses a raw UDP datagram.
// Returns (msgType, proto payload bytes, sender nodeID (64B pubkey), error).
func DecodeMessage(data []byte) (byte, []byte, []byte, error) {
	if len(data) < 66 {
		return 0, nil, nil, errors.New("discover: message too short")
	}
	msgType := data[0]
	sig := data[1:66]
	payload := data[66:]

	hash := ethcrypto.Keccak256(payload)
	pubBytes, err := ethcrypto.Ecrecover(hash, sig)
	if err != nil {
		return 0, nil, nil, err
	}
	// pubBytes is 65-byte (0x04 prefix + 64-byte key); strip prefix
	senderID := make([]byte, 64)
	copy(senderID, pubBytes[1:])
	return msgType, payload, senderID, nil
}

// Conn is a UDP connection for discovery.
type Conn struct {
	conn    *net.UDPConn
	privKey *ecdsa.PrivateKey
	localID []byte
}

// NewConn creates a UDP discovery connection bound to listenAddr.
func NewConn(listenAddr string, privKey *ecdsa.PrivateKey) (*Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{
		conn:    conn,
		privKey: privKey,
		localID: PubKeyToNodeID(privKey.PublicKey),
	}, nil
}

// SendPing sends a PingMessage to the target.
func (c *Conn) SendPing(target *net.UDPAddr, localEP, remoteEP *discoverpb.Endpoint) error {
	msg := &discoverpb.PingMessage{
		From:      localEP,
		To:        remoteEP,
		Version:   1,
		Timestamp: time.Now().UnixMilli(),
	}
	data, err := EncodeMessage(MsgPing, msg, c.privKey)
	if err != nil {
		return err
	}
	_, err = c.conn.WriteToUDP(data, target)
	return err
}

// SendPong replies to a ping.
func (c *Conn) SendPong(target *net.UDPAddr, localEP *discoverpb.Endpoint, echo int32) error {
	msg := &discoverpb.PongMessage{
		From:      localEP,
		Echo:      echo,
		Timestamp: time.Now().UnixMilli(),
	}
	data, err := EncodeMessage(MsgPong, msg, c.privKey)
	if err != nil {
		return err
	}
	_, err = c.conn.WriteToUDP(data, target)
	return err
}

// SendFindNode sends a FindNeighbours request for targetID.
func (c *Conn) SendFindNode(target *net.UDPAddr, localEP *discoverpb.Endpoint, targetID []byte) error {
	msg := &discoverpb.FindNeighbours{
		From:      localEP,
		TargetId:  targetID,
		Timestamp: time.Now().UnixMilli(),
	}
	data, err := EncodeMessage(MsgFindNode, msg, c.privKey)
	if err != nil {
		return err
	}
	_, err = c.conn.WriteToUDP(data, target)
	return err
}

// SendNeighbours responds with a list of neighbours.
func (c *Conn) SendNeighbours(target *net.UDPAddr, localEP *discoverpb.Endpoint, neighbours []*discoverpb.Endpoint) error {
	msg := &discoverpb.Neighbours{
		From:       localEP,
		Neighbours: neighbours,
		Timestamp:  time.Now().UnixMilli(),
	}
	data, err := EncodeMessage(MsgNeighbours, msg, c.privKey)
	if err != nil {
		return err
	}
	_, err = c.conn.WriteToUDP(data, target)
	return err
}

// ReadFrom reads the next UDP datagram. Blocks until data arrives or close.
func (c *Conn) ReadFrom(buf []byte) (int, *net.UDPAddr, error) {
	return c.conn.ReadFromUDP(buf)
}

// Close shuts down the UDP connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}
