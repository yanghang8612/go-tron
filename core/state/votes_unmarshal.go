package state

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var votesDecodeLayoutOK = verifyVotesDecodeLayout()

func verifyVotesDecodeLayout() bool {
	votesFields := (&corepb.Votes{}).ProtoReflect().Descriptor().Fields()
	voteFields := (&corepb.Vote{}).ProtoReflect().Descriptor().Fields()
	return votesFields.Len() == 3 &&
		voteProtoFieldShape(votesFields, 1, protoreflect.BytesKind, false) &&
		voteProtoFieldShape(votesFields, 2, protoreflect.MessageKind, true) &&
		voteProtoFieldShape(votesFields, 3, protoreflect.MessageKind, true) &&
		voteFields.Len() == 2 &&
		voteProtoFieldShape(voteFields, 1, protoreflect.BytesKind, false) &&
		voteProtoFieldShape(voteFields, 2, protoreflect.Int64Kind, false)
}

func voteProtoFieldShape(fields protoreflect.FieldDescriptors, number protoreflect.FieldNumber, kind protoreflect.Kind, list bool) bool {
	field := fields.ByNumber(number)
	return field != nil && field.Kind() == kind && field.IsList() == list && !field.IsMap()
}

type decodedVotes struct {
	votes   corepb.Votes
	address [common.AddressLength]byte
}

type votesWireLayout struct {
	address      []byte
	oldCount     int
	newCount     int
	addressBytes int
}

// unmarshalVotesOwned handles the stable canonical Votes/Vote schema with
// coalesced ownership. Unknown fields and unusual wire types use protobuf-go's
// generic decoder so forward compatibility and malformed-input errors remain
// exactly aligned with generated code.
func unmarshalVotesOwned(data []byte) (*corepb.Votes, error) {
	if votesDecodeLayoutOK {
		if layout, ok := scanVotesWire(data); ok {
			return decodeVotesWire(data, layout), nil
		}
	}
	votes := new(corepb.Votes)
	if err := proto.Unmarshal(data, votes); err != nil {
		return nil, err
	}
	return votes, nil
}

func scanVotesWire(data []byte) (votesWireLayout, bool) {
	var layout votesWireLayout
	for rest := data; len(rest) > 0; {
		number, wireType, tagSize := protowire.ConsumeTag(rest)
		if tagSize < 0 || !number.IsValid() {
			return votesWireLayout{}, false
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, rest[tagSize:])
		if valueSize < 0 || wireType != protowire.BytesType || (number != 1 && number != 2 && number != 3) {
			return votesWireLayout{}, false
		}
		value, size := protowire.ConsumeBytes(rest[tagSize:])
		if size < 0 {
			return votesWireLayout{}, false
		}
		switch number {
		case 1:
			layout.address = value
		case 2, 3:
			voteAddress, ok := scanVoteWire(value)
			if !ok {
				return votesWireLayout{}, false
			}
			if number == 2 {
				layout.oldCount++
			} else {
				layout.newCount++
			}
			layout.addressBytes += len(voteAddress)
		}
		rest = rest[tagSize+valueSize:]
	}
	return layout, true
}

func scanVoteWire(data []byte) ([]byte, bool) {
	var address []byte
	for rest := data; len(rest) > 0; {
		number, wireType, tagSize := protowire.ConsumeTag(rest)
		if tagSize < 0 || !number.IsValid() {
			return nil, false
		}
		valueSize := protowire.ConsumeFieldValue(number, wireType, rest[tagSize:])
		if valueSize < 0 {
			return nil, false
		}
		switch number {
		case 1:
			if wireType != protowire.BytesType {
				return nil, false
			}
			value, size := protowire.ConsumeBytes(rest[tagSize:])
			if size < 0 {
				return nil, false
			}
			address = value
		case 2:
			if wireType != protowire.VarintType {
				return nil, false
			}
			if _, size := protowire.ConsumeVarint(rest[tagSize:]); size < 0 {
				return nil, false
			}
		default:
			return nil, false
		}
		rest = rest[tagSize+valueSize:]
	}
	return address, true
}

func decodeVotesWire(data []byte, layout votesWireLayout) *corepb.Votes {
	decoded := new(decodedVotes)
	if len(layout.address) <= len(decoded.address) {
		if len(layout.address) > 0 {
			copy(decoded.address[:], layout.address)
			decoded.votes.Address = decoded.address[:len(layout.address):len(layout.address)]
		}
	} else {
		decoded.votes.Address = append([]byte(nil), layout.address...)
	}

	totalVotes := layout.oldCount + layout.newCount
	if totalVotes == 0 {
		return &decoded.votes
	}
	voteStorage := make([]corepb.Vote, totalVotes)
	votePointers := make([]*corepb.Vote, totalVotes)
	addressArena := make([]byte, layout.addressBytes)
	for index := range voteStorage {
		votePointers[index] = &voteStorage[index]
	}
	decoded.votes.OldVotes = votePointers[:layout.oldCount:layout.oldCount]
	decoded.votes.NewVotes = votePointers[layout.oldCount:totalVotes:totalVotes]

	oldIndex := 0
	newIndex := layout.oldCount
	addressOffset := 0
	for rest := data; len(rest) > 0; {
		number, _, tagSize := protowire.ConsumeTag(rest)
		valueSize := protowire.ConsumeFieldValue(number, protowire.BytesType, rest[tagSize:])
		if number == 2 || number == 3 {
			value, _ := protowire.ConsumeBytes(rest[tagSize:])
			index := oldIndex
			if number == 2 {
				oldIndex++
			} else {
				index = newIndex
				newIndex++
			}
			decodeVoteWire(value, &voteStorage[index], addressArena, &addressOffset)
		}
		rest = rest[tagSize+valueSize:]
	}
	return &decoded.votes
}

func decodeVoteWire(data []byte, vote *corepb.Vote, addressArena []byte, addressOffset *int) {
	var address []byte
	for rest := data; len(rest) > 0; {
		number, wireType, tagSize := protowire.ConsumeTag(rest)
		valueSize := protowire.ConsumeFieldValue(number, wireType, rest[tagSize:])
		if number == 1 {
			address, _ = protowire.ConsumeBytes(rest[tagSize:])
		} else {
			value, _ := protowire.ConsumeVarint(rest[tagSize:])
			vote.VoteCount = int64(value)
		}
		rest = rest[tagSize+valueSize:]
	}
	if len(address) == 0 {
		return
	}
	start := *addressOffset
	end := start + len(address)
	copy(addressArena[start:end], address)
	vote.VoteAddress = addressArena[start:end:end]
	*addressOffset = end
}
