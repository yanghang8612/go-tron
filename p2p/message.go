package p2p

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteMsg writes a length-prefixed message: [4B length][1B type][payload].
// Length includes the type byte.
func WriteMsg(w io.Writer, code byte, payload []byte) error {
	length := uint32(1 + len(payload)) // type byte + payload
	var header [5]byte
	binary.BigEndian.PutUint32(header[:4], length)
	header[4] = code
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadMsg reads a length-prefixed message, returning (type, payload, error).
func ReadMsg(r io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:4]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length == 0 {
		return 0, nil, fmt.Errorf("empty message frame")
	}
	if length > MaxMessageSize {
		return 0, nil, fmt.Errorf("message too large: %d bytes (max %d)", length, MaxMessageSize)
	}
	// Read type byte
	if _, err := io.ReadFull(r, header[4:5]); err != nil {
		return 0, nil, err
	}
	code := header[4]
	// Read payload (length - 1 because type byte already consumed)
	payloadLen := length - 1
	if payloadLen == 0 {
		return code, nil, nil
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return code, payload, nil
}
