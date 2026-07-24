package state

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
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
	type indexedVote struct {
		index uint32
		vote  *corepb.Vote
	}
	rows := make([]indexedVote, 0)
	if err := s.IterateAccountKV(obj.address, kvdomains.AccountVotesAux, nil, func(key, value []byte) (bool, error) {
		index, vote, err := decodeAccountVoteRow(key, value)
		if err != nil {
			return false, err
		}
		rows = append(rows, indexedVote{index: index, vote: vote})
		return true, nil
	}); err != nil {
		clearAccountVotesProto(obj.account.Proto())
		return err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].index < rows[j].index })
	pb := obj.account.Proto()
	clearAccountVotesProto(pb)
	for _, row := range rows {
		pb.Votes = append(pb.Votes, row.vote)
	}
	obj.accountVotesLoaded = true
	return nil
}

func (s *StateDB) writeAccountVotes(obj *stateObject, votes []*corepb.Vote) error {
	if obj == nil || obj.account == nil {
		return nil
	}
	if err := s.DeleteAccountKVPrefix(obj.address, kvdomains.AccountVotesAux, nil); err != nil {
		return err
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
