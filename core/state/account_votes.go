package state

import (
	"encoding/binary"
	"fmt"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func accountVoteKey(index uint32) []byte {
	var key [4]byte
	binary.BigEndian.PutUint32(key[:], index)
	return key[:]
}

func decodeAccountVoteRow(key, value []byte) (uint32, *corepb.Vote, error) {
	if len(key) != 4 {
		return 0, nil, fmt.Errorf("account vote key length %d, want 4", len(key))
	}
	vote, err := unmarshalVoteOwned(value)
	if err != nil {
		return 0, nil, fmt.Errorf("decode account vote %x: %w", key, err)
	}
	return binary.BigEndian.Uint32(key), vote, nil
}

func clearAccountVotesProto(pb *corepb.Account) {
	if pb != nil {
		pb.Votes = nil
	}
}

func (s *StateDB) materializeAccountVotes(obj *stateObject) error {
	if obj == nil || obj.account == nil || obj.accountVotesLoaded {
		return nil
	}
	votes := make([]*corepb.Vote, 0, params.MaxVoteNumber)
	// VoteWitnessContract is consensus-limited to MaxVoteNumber entries and
	// writeAccountVotes persists each entry in its bounded numeric slot. Point
	// reads avoid constructing a blockbuffer prefix iterator, whose overlay walk
	// scans every write in every live layer even though there can be at most 30
	// relevant rows. Check every slot rather than stopping at the first miss so
	// older sparse rows remain readable.
	for index := uint32(0); index < uint32(params.MaxVoteNumber); index++ {
		key := s.accountUint32Key(index)
		value, exists, err := s.getAccountKVForDecoding(obj.address, kvdomains.AccountVotesAux, key)
		if err != nil {
			clearAccountVotesProto(obj.account.Proto())
			return err
		}
		if !exists {
			continue
		}
		_, vote, err := decodeAccountVoteRow(key, value)
		if err != nil {
			clearAccountVotesProto(obj.account.Proto())
			return err
		}
		votes = append(votes, vote)
	}
	pb := obj.account.Proto()
	clearAccountVotesProto(pb)
	pb.Votes = append(pb.Votes, votes...)
	obj.accountVotesLoaded = true
	return nil
}

func (s *StateDB) writeAccountVotes(obj *stateObject, votes []*corepb.Vote) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	// The protocol admits at most MaxVoteNumber rows. Delete those bounded slots
	// directly instead of opening a prefix iterator over the whole block overlay.
	for index := uint32(0); index < uint32(params.MaxVoteNumber); index++ {
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountVotesAux, s.accountUint32Key(index)); err != nil {
			return err
		}
	}
	for index, vote := range votes {
		if vote == nil {
			continue
		}
		// SetAccountKV synchronously copies value into the StateDB's immutable
		// block arena. Reuse StateDB-owned scratch for the transient protobuf
		// encoding rather than allocating a second owned byte slice for every
		// vote row. MarshalAppend preserves the generated codec's exact wire
		// behavior, including unknown fields and negative int64 values; unusually
		// large messages simply grow beyond the common 64-byte scratch capacity.
		value, err := proto.MarshalOptions{Deterministic: true}.MarshalAppend(s.accountVoteMarshalScratch[:0], vote)
		if err != nil {
			return err
		}
		if err := s.SetAccountKV(obj.address, kvdomains.AccountVotesAux, s.accountUint32Key(uint32(index)), value); err != nil {
			return err
		}
	}
	clearAccountVotesProto(obj.account.Proto())
	obj.accountVotesLoaded = false
	return nil
}
