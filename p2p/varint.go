package p2p

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteVarint32 writes v as a protobuf-style varint (1-5 bytes, LSB first).
func WriteVarint32(w io.Writer, v uint32) error {
	var buf [5]byte
	n := binary.PutUvarint(buf[:], uint64(v))
	_, err := w.Write(buf[:n])
	return err
}

// ReadVarint32 reads a protobuf-style varint from r. Reads at most 5 bytes.
// Returns an error on overflow (>32 bits) or unterminated sequences.
func ReadVarint32(r io.Reader) (uint32, error) {
	var b [1]byte
	var result uint64
	for i := 0; i < 5; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		result |= uint64(b[0]&0x7F) << (7 * i)
		if b[0]&0x80 == 0 {
			if result > 0xFFFFFFFF {
				return 0, fmt.Errorf("varint32 overflow")
			}
			return uint32(result), nil
		}
	}
	return 0, fmt.Errorf("varint32 too long (>5 bytes)")
}
