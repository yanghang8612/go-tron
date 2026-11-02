package statecodec

import (
	"bytes"
	"math"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestRewardVotesRoundTripOrderUnknownAndEmpty(t *testing.T) {
	votes := []*corepb.Vote{
		{VoteAddress: []byte{0x41, 3}, VoteCount: math.MaxInt64},
		{VoteAddress: []byte{0x41, 3}, VoteCount: -1},
		{},
	}
	votes[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	for _, test := range [][]*corepb.Vote{nil, {}, votes, {nil}} {
		raw, err := MarshalRewardVotes(test)
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalRewardVotes(raw)
		if err != nil || !proto.Equal(&corepb.Account{Votes: test}, &corepb.Account{Votes: got}) {
			t.Fatalf("Votes mismatch: got=%v want=%v err=%v", got, test, err)
		}
		again, err := MarshalRewardVotes(got)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatalf("noncanonical reward encoding: %v", err)
		}
		for end := 0; end < len(raw); end++ {
			if _, err := UnmarshalRewardVotes(raw[:end]); err == nil {
				t.Fatalf("truncation %d accepted", end)
			}
		}
	}
}

func TestRewardVotesRejectsFullAccountAndUnknownAccountFields(t *testing.T) {
	for _, account := range []*corepb.Account{
		{Address: []byte{0x41, 1}},
		{Allowance: 7},
		{AssetV2: map[string]int64{"1000001": 2}},
		{OwnerPermission: &corepb.Permission{}},
		{Votes: []*corepb.Vote{{VoteCount: 1}}, Balance: 3},
	} {
		raw, err := Marshal(account)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := UnmarshalRewardVotes(raw); err == nil {
			t.Fatalf("legacy complete Account field accepted: %v", account)
		}
	}
	a := &corepb.Account{Votes: []*corepb.Vote{{VoteCount: 2}}}
	a.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	raw, err := Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalRewardVotes(raw); err == nil {
		t.Fatal("Account-level unknown fields accepted")
	}
	wire, _ := proto.Marshal(&corepb.Account{Votes: a.Votes})
	if _, err := UnmarshalRewardVotes(wire); err == nil {
		t.Fatal("old protobuf value accepted")
	}
}

func TestValidateRewardVotesDoesNotAllocateAndMatchesNativeDecoder(t *testing.T) {
	votes := []*corepb.Vote{{VoteAddress: []byte{0x41, 1}, VoteCount: 7}, {VoteCount: -1}, {}}
	votes[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	raw, err := MarshalRewardVotes(votes)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := ValidateRewardVotes(raw); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("source validator allocated %.2f objects", allocs)
	}
	check := func(data []byte) {
		t.Helper()
		fastErr := ValidateRewardVotes(data)
		var decoded corepb.Account
		referenceErr := Unmarshal(data, &decoded)
		referenceValid := referenceErr == nil && len(decoded.ProtoReflect().GetUnknown()) == 0
		decoded.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			if fd.Number() != 5 {
				referenceValid = false
			}
			return true
		})
		if (fastErr == nil) != referenceValid {
			t.Fatalf("leaf validator disagrees with generic native decoder: raw=%x fast=%v generic=%v account=%v", data, fastErr, referenceErr, &decoded)
		}
	}
	for end := 0; end <= len(raw); end++ {
		check(raw[:end])
	}
	for pos := range raw {
		for bit := byte(1); bit != 0; bit <<= 1 {
			mutated := append([]byte(nil), raw...)
			mutated[pos] ^= bit
			check(mutated)
		}
	}
}

func BenchmarkValidateRewardVotes(b *testing.B) {
	votes := make([]*corepb.Vote, 30)
	for i := range votes {
		votes[i] = &corepb.Vote{VoteAddress: append(make([]byte, 20), byte(i+1)), VoteCount: int64(i + 1)}
	}
	raw, err := MarshalRewardVotes(votes)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if err := ValidateRewardVotes(raw); err != nil {
			b.Fatal(err)
		}
	}
}
