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
	var vote corepb.Vote
	if err := proto.Unmarshal(value, &vote); err != nil {
		return 0, nil, fmt.Errorf("decode account vote %x: %w", key, err)
	}
	return binary.BigEndian.Uint32(key), &vote, nil
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
		key := accountVoteKey(index)
		value, exists, err := s.GetAccountKV(obj.address, kvdomains.AccountVotesAux, key)
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
		if err := s.DeleteAccountKV(obj.address, kvdomains.AccountVotesAux, accountVoteKey(index)); err != nil {
			return err
		}
	}
	for index, vote := range votes {
		if vote == nil {
			continue
		}
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(vote)
		if err != nil {
			return err
		}
		if err := s.SetAccountKV(obj.address, kvdomains.AccountVotesAux, accountVoteKey(uint32(index)), value); err != nil {
			return err
		}
	}
	clearAccountVotesProto(obj.account.Proto())
	obj.accountVotesLoaded = false
	return nil
}
