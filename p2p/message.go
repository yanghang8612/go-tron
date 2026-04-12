package p2p

import (
	"fmt"
	"io"
)

// WriteMsg writes one framed message: varint32(1+len(payload)) || code || payload.
// `length` covers the type byte and the payload. Max total frame = MaxMessageSize (5 MB).
func WriteMsg(w io.Writer, code byte, payload []byte) error {
	total := uint32(1 + len(payload))
	if total > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes (max %d)", total, MaxMessageSize)
	}
	if err := WriteVarint32(w, total); err != nil {
		return err
	}
	if _, err := w.Write([]byte{code}); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadMsg reads one framed message, returning (code, payload, error).
// Rejects frames where length == 0 or length > MaxMessageSize.
func ReadMsg(r io.Reader) (byte, []byte, error) {
	length, err := ReadVarint32(r)
	if err != nil {
		return 0, nil, err
	}
	if length == 0 {
		return 0, nil, fmt.Errorf("empty frame")
	}
	if length > MaxMessageSize {
		return 0, nil, fmt.Errorf("frame too large: %d bytes", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return buf[0], buf[1:], nil
}
