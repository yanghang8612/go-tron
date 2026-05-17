package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func testVotesAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestWriteReadVotesAndIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	voter := testVotesAddr(1)
	witness := testVotesAddr(2)
	votes := &corepb.Votes{
		Address: voter.Bytes(),
		OldVotes: []*corepb.Vote{
			{VoteAddress: witness.Bytes(), VoteCount: 10},
		},
		NewVotes: []*corepb.Vote{
			{VoteAddress: witness.Bytes(), VoteCount: 20},
		},
	}
	if err := WriteVotes(db, voter, votes); err != nil {
		t.Fatal(err)
	}
	got := ReadVotes(db, voter)
	if got == nil || len(got.OldVotes) != 1 || len(got.NewVotes) != 1 {
		t.Fatalf("votes not round-tripped: %+v", got)
	}
	if got.NewVotes[0].VoteCount != 20 {
		t.Fatalf("new vote count: got %d, want 20", got.NewVotes[0].VoteCount)
	}
	if idx := ReadVotesIndex(db); len(idx) != 1 || idx[0] != voter {
		t.Fatalf("votes index = %v, want [%s]", idx, voter.Hex())
	}

	if err := WriteVotes(db, voter, votes); err != nil {
		t.Fatal(err)
	}
	if idx := ReadVotesIndex(db); len(idx) != 1 {
		t.Fatalf("duplicate index entry written: %v", idx)
	}
}
