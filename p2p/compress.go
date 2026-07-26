package p2p

import (
	"fmt"

	"github.com/golang/snappy"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
	"google.golang.org/protobuf/encoding/protowire"
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
	// Normal Go and java-tron send the wrapper in canonical field order. For a
	// snappy frame the compressed bytes are consumed synchronously, so decode
	// directly from the frame instead of making proto.Unmarshal's redundant
	// copy first. Any non-canonical or extended wire form keeps the generated
	// protobuf decoder below, preserving its compatibility and error behavior.
	if compressed, ok := canonicalSnappyFrameData(frame); ok {
		inner, err := snappy.Decode(nil, compressed)
		if err != nil {
			return 0, nil, fmt.Errorf("snappy decode: %w", err)
		}
		if len(inner) == 0 {
			return 0, nil, fmt.Errorf("unwrap: empty inner payload")
		}
		return inner[0], inner[1:], nil
	}

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

// canonicalSnappyFrameData recognizes exactly the normal protobuf encoding:
// field 1 (type=snappy), then field 2 (data), with canonical length encoding
// and no trailing fields. Restricting the fast path this tightly means unusual
// but valid protobuf forms continue through proto.Unmarshal.
func canonicalSnappyFrameData(frame []byte) ([]byte, bool) {
	// 0x08 = field 1/varint, 0x01 = snappy, 0x12 = field 2/bytes.
	if len(frame) < 4 || frame[0] != 0x08 || frame[1] != 0x01 || frame[2] != 0x12 {
		return nil, false
	}
	data, size := protowire.ConsumeBytes(frame[3:])
	if size < 0 || 3+size != len(frame) || size != protowire.SizeBytes(len(data)) {
		return nil, false
	}
	return data, true
}
