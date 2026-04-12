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
// Pre-handshake usage: first byte of the frame body is the message code.
func ReadMsg(r io.Reader) (byte, []byte, error) {
	body, err := ReadFrameBody(r)
	if err != nil {
		return 0, nil, err
	}
	if len(body) == 0 {
		return 0, nil, fmt.Errorf("empty frame")
	}
	return body[0], body[1:], nil
}

// ReadFrameBody reads one varint32-framed message and returns the raw body
// bytes (without any per-frame parsing). Used post-handshake, where the
// body is a CompressMessage proto rather than [code][payload].
func ReadFrameBody(r io.Reader) ([]byte, error) {
	length, err := ReadVarint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, fmt.Errorf("empty frame")
	}
	if length > MaxMessageSize {
		return nil, fmt.Errorf("frame too large: %d bytes", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// WriteFrameBody writes a varint32-framed message with the given body bytes.
// Used post-handshake, where the body is a serialized CompressMessage.
func WriteFrameBody(w io.Writer, body []byte) error {
	if uint32(len(body)) > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes (max %d)", len(body), MaxMessageSize)
	}
	if err := WriteVarint32(w, uint32(len(body))); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}
