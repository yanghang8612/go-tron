package statecodec

import (
	"encoding/binary"
	"errors"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MarshalRewardVotes encodes the V4 reward snapshot's complete ordered Vote
// list. It deliberately stores no other Account fields. A present empty list
// has a valid native Account value; row presence must be carried separately.
// Per-Vote unknown fields, duplicate entries, order and signed counts survive.
func MarshalRewardVotes(votes []*corepb.Vote) ([]byte, error) {
	// Protobuf's repeated-message API treats a nil item as an empty message.
	// Match that logical input without mutating a caller-owned slice.
	for _, vote := range votes {
		if vote == nil {
			owned := append([]*corepb.Vote(nil), votes...)
			for i, value := range owned {
				if value == nil {
					owned[i] = new(corepb.Vote)
				}
			}
			votes = owned
			break
		}
	}
	return Marshal(&corepb.Account{Votes: votes})
}

// UnmarshalRewardVotes rejects legacy complete Account snapshots rather than
// silently discarding fields. The new V4 commitment/snapshot identity is also
// required at the database boundary; this is not an old-state conversion API.
func UnmarshalRewardVotes(raw []byte) ([]*corepb.Vote, error) {
	if err := ValidateRewardVotes(raw); err != nil {
		return nil, err
	}
	var account corepb.Account
	if err := Unmarshal(raw, &account); err != nil {
		return nil, err
	}
	return account.Votes, nil
}

// ValidateRewardVotes is the allocation-free source-boundary validator. It
// checks canonical native framing and the complete V4 Account/Vote schema
// without constructing generated protobuf objects. Vote unknown bytes remain
// opaque, exactly as in the generic native codec; Account unknowns are rejected.
func ValidateRewardVotes(raw []byte) error {
	if !IsNative(raw) {
		return errors.New("reward votes snapshot is not native")
	}
	count, rest, err := consumeUvarint(raw[len(magic):])
	if err != nil {
		return err
	}
	switch count {
	case 0:
	case 1:
		var number uint64
		number, rest, err = consumeUvarint(rest)
		if err != nil {
			return err
		}
		if number != 5 {
			return errors.New("reward votes snapshot contains non-Votes Account fields")
		}
		if len(rest) == 0 || rest[0] != shapeList|byte(protoreflect.MessageKind) {
			return errors.New("reward votes snapshot has invalid Votes type")
		}
		var payload []byte
		payload, rest, err = consumeBytes(rest[1:])
		if err != nil {
			return err
		}
		if err := validateRewardVotesList(payload); err != nil {
			return err
		}
	default:
		return errors.New("reward votes snapshot contains non-Votes Account fields")
	}
	unknown, tail, err := consumeBytes(rest)
	if err != nil {
		return err
	}
	if len(unknown) != 0 {
		return errors.New("reward votes snapshot contains Account-level unknown fields")
	}
	if len(tail) != 0 {
		return errors.New("reward votes snapshot has trailing data")
	}
	return nil
}

func validateRewardVotesList(payload []byte) error {
	count, rest, err := consumeUvarint(payload)
	if err != nil {
		return err
	}
	if count == 0 || count > uint64(len(rest)) {
		return errors.New("reward votes snapshot has noncanonical list count")
	}
	for i := uint64(0); i < count; i++ {
		var vote []byte
		vote, rest, err = consumeBytes(rest)
		if err != nil {
			return err
		}
		if err := validateRewardVote(vote); err != nil {
			return err
		}
	}
	if len(rest) != 0 {
		return errors.New("reward votes snapshot has trailing list data")
	}
	return nil
}

func validateRewardVote(raw []byte) error {
	count, rest, err := consumeUvarint(raw)
	if err != nil {
		return err
	}
	if count > 2 {
		return errors.New("reward vote has unregistered fields")
	}
	var previous uint64
	for i := uint64(0); i < count; i++ {
		var number uint64
		number, rest, err = consumeUvarint(rest)
		if err != nil {
			return err
		}
		if number <= previous || number > 2 || len(rest) == 0 {
			return errors.New("reward vote has invalid field order")
		}
		previous = number
		kind := rest[0]
		var value []byte
		value, rest, err = consumeBytes(rest[1:])
		if err != nil {
			return err
		}
		switch number {
		case 1:
			if kind != byte(protoreflect.BytesKind) || len(value) == 0 {
				return errors.New("reward vote has invalid address encoding")
			}
		case 2:
			if kind != byte(protoreflect.Int64Kind) || len(value) != 8 || binary.BigEndian.Uint64(value) == 0 {
				return errors.New("reward vote has invalid count encoding")
			}
		}
	}
	_, tail, err := consumeBytes(rest)
	if err != nil {
		return err
	}
	if len(tail) != 0 {
		return errors.New("reward vote has trailing data")
	}
	return nil
}
