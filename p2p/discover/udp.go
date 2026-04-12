package discover

import (
	"errors"
	"net"
	"time"

	discoverpb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Message type codes — match libp2p MessageType.java.
const (
	MsgPing       byte = 0x01
	MsgPong       byte = 0x02
	MsgFindNode   byte = 0x03
	MsgNeighbours byte = 0x04
)

// EncodeMessage serializes a discovery message as [type(1)][proto payload].
// No signature — sender identity comes from proto fields (e.g. PingMessage.From.NodeId).
func EncodeMessage(msgType byte, msg proto.Message) ([]byte, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 1+len(payload))
	buf[0] = msgType
	copy(buf[1:], payload)
	return buf, nil
}

// DecodeMessage parses [type(1)][proto payload]. Returns (msgType, payload).
func DecodeMessage(data []byte) (byte, []byte, error) {
	if len(data) < 2 {
		return 0, nil, errors.New("discover: message too short")
	}
	return data[0], data[1:], nil
}

// Conn is a UDP connection for discovery.
type Conn struct {
	conn *net.UDPConn
}

// NewConn opens a UDP discovery socket bound to listenAddr.
func NewConn(listenAddr string) (*Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

// SendPing sends a PingMessage to target.
func (c *Conn) SendPing(target *net.UDPAddr, localEP, remoteEP *discoverpb.Endpoint, networkID int32) error {
	msg := &discoverpb.PingMessage{
		From:      localEP,
		To:        remoteEP,
		Version:   networkID, // libp2p repurposes Version as networkId
		Timestamp: time.Now().UnixMilli(),
	}
	return c.send(target, MsgPing, msg)
}

// SendPong replies to a ping. `echo` is the sender's networkId (from their ping).
func (c *Conn) SendPong(target *net.UDPAddr, localEP *discoverpb.Endpoint, echo int32) error {
	msg := &discoverpb.PongMessage{
		From:      localEP,
		Echo:      echo,
		Timestamp: time.Now().UnixMilli(),
	}
	return c.send(target, MsgPong, msg)
}

// SendFindNode asks target for neighbours of targetID.
func (c *Conn) SendFindNode(target *net.UDPAddr, localEP *discoverpb.Endpoint, targetID []byte) error {
	msg := &discoverpb.FindNeighbours{
		From:      localEP,
		TargetId:  targetID,
		Timestamp: time.Now().UnixMilli(),
	}
	return c.send(target, MsgFindNode, msg)
}

// SendNeighbours replies with a list of neighbour endpoints.
func (c *Conn) SendNeighbours(target *net.UDPAddr, localEP *discoverpb.Endpoint, neighbours []*discoverpb.Endpoint) error {
	msg := &discoverpb.Neighbours{
		From:       localEP,
		Neighbours: neighbours,
		Timestamp:  time.Now().UnixMilli(),
	}
	return c.send(target, MsgNeighbours, msg)
}

func (c *Conn) send(target *net.UDPAddr, msgType byte, msg proto.Message) error {
	data, err := EncodeMessage(msgType, msg)
	if err != nil {
		return err
	}
	_, err = c.conn.WriteToUDP(data, target)
	return err
}

// ReadFrom reads the next UDP packet (blocks until data arrives or close).
func (c *Conn) ReadFrom(buf []byte) (int, *net.UDPAddr, error) {
	return c.conn.ReadFromUDP(buf)
}

// Close shuts down the socket.
func (c *Conn) Close() error {
	return c.conn.Close()
}
