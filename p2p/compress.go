package p2p

import (
	"fmt"

	"github.com/golang/snappy"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
	"google.golang.org/protobuf/proto"
)

// WrapPostHandshake builds a CompressMessage wrapping [code || payload].
// Matches libp2p's ProtoUtil.compressMessage: tries snappy, keeps whichever
// is smaller. The inner bytes are always [1-byte type][payload].
//
// Returns the proto-marshaled CompressMessage — ready to be framed by WriteMsg
// (which prepends the varint length). The CompressMessage byte stream itself
// is what the peer expects to read as the post-handshake frame body.
func WrapPostHandshake(code byte, payload []byte) ([]byte, error) {
	inner := make([]byte, 1+len(payload))
	inner[0] = code
	copy(inner[1:], payload)

	compressType := p2ppb.CompressMessage_uncompress
	outerData := inner
	compressed := snappy.Encode(nil, inner)
	if len(compressed) < len(inner) {
		compressType = p2ppb.CompressMessage_snappy
		outerData = compressed
	}

	wrap := &p2ppb.CompressMessage{
		Type: compressType,
		Data: outerData,
	}
	return proto.Marshal(wrap)
}

// UnwrapPostHandshake is the inverse of WrapPostHandshake. It parses a
// CompressMessage, decompresses if necessary, and returns (code, payload).
// The CompressMessage.data contains [type_byte][payload_bytes].
//
// `frame` is the complete post-handshake frame body (after the varint length
// prefix is stripped — i.e., what ReadMsg currently returns as (code, payload)
// concatenated). Pass `append([]byte{code}, payload...)` from a ReadMsg result.
func UnwrapPostHandshake(frame []byte) (byte, []byte, error) {
	var msg p2ppb.CompressMessage
	if err := proto.Unmarshal(frame, &msg); err != nil {
		return 0, nil, fmt.Errorf("unwrap: %w", err)
	}
	inner := msg.Data
	if msg.Type == p2ppb.CompressMessage_snappy {
		decoded, err := snappy.Decode(nil, inner)
		if err != nil {
			return 0, nil, fmt.Errorf("snappy decode: %w", err)
		}
		inner = decoded
	}
	if len(inner) == 0 {
		return 0, nil, fmt.Errorf("unwrap: empty inner payload")
	}
	return inner[0], inner[1:], nil
}
