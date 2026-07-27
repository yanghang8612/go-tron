package state

import (
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func unmarshalExchange(data []byte) (*corepb.Exchange, error) {
	decoded := new(corepb.Exchange)
	var (
		creatorAddress []byte
		firstTokenID   []byte
		secondTokenID  []byte
		unknown        []byte
	)

	for rest := data; len(rest) > 0; {
		number, wireType, tagSize := protowire.ConsumeTag(rest)
		if tagSize < 0 {
			return nil, protowire.ParseError(tagSize)
		}
		if !number.IsValid() || wireType == protowire.EndGroupType {
			return unmarshalExchangeFallback(data, decoded)
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, rest[tagSize:])
		if valueSize < 0 {
			return nil, protowire.ParseError(valueSize)
		}
		fieldSize := tagSize + valueSize
		switch number {
		case 1, 3, 7, 9:
			if wireType != protowire.VarintType {
				return unmarshalExchangeFallback(data, decoded)
			}
			value, size := protowire.ConsumeVarint(rest[tagSize:])
			if size < 0 {
				return nil, protowire.ParseError(size)
			}
			switch number {
			case 1:
				decoded.ExchangeId = int64(value)
			case 3:
				decoded.CreateTime = int64(value)
			case 7:
				decoded.FirstTokenBalance = int64(value)
			case 9:
				decoded.SecondTokenBalance = int64(value)
			}
		case 2, 6, 8:
			if wireType != protowire.BytesType {
				return unmarshalExchangeFallback(data, decoded)
			}
			value, size := protowire.ConsumeBytes(rest[tagSize:])
			if size < 0 {
				return nil, protowire.ParseError(size)
			}
			// Singular fields use their last wire occurrence.
			switch number {
			case 2:
				creatorAddress = value
			case 6:
				firstTokenID = value
			case 8:
				secondTokenID = value
			}
		default:
			// Preserve unknown fields with protobuf-go's normalization so a
			// read-modify-write remains compatible with future schema versions.
			// The generic decoder canonicalizes a non-minimal tag varint while
			// retaining the encoded value bytes. Unknown groups require recursive
			// tag canonicalization, so leave that uncommon case to the generic path.
			if wireType == protowire.StartGroupType {
				return unmarshalExchangeFallback(data, decoded)
			}
			unknown = protowire.AppendTag(unknown, number, wireType)
			unknown = append(unknown, rest[tagSize:fieldSize]...)
		}
		rest = rest[fieldSize:]
	}

	totalFields := len(creatorAddress) + len(firstTokenID) + len(secondTokenID)
	if totalFields > 0 {
		fields := make([]byte, totalFields)
		offset := 0
		decoded.CreatorAddress = copyExchangeField(fields, &offset, creatorAddress)
		decoded.FirstTokenId = copyExchangeField(fields, &offset, firstTokenID)
		decoded.SecondTokenId = copyExchangeField(fields, &offset, secondTokenID)
	}
	if len(unknown) > 0 {
		decoded.ProtoReflect().SetUnknown(unknown)
	}
	return decoded, nil
}

func copyExchangeField(arena []byte, offset *int, value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	start := *offset
	end := start + len(value)
	copy(arena[start:end], value)
	*offset = end
	return arena[start:end:end]
}

func unmarshalExchangeFallback(data []byte, decoded *corepb.Exchange) (*corepb.Exchange, error) {
	*decoded = corepb.Exchange{}
	if err := proto.Unmarshal(data, decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
