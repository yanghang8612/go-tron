package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type votesIndexReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// WriteVotes stores a pending vote change keyed by voter address, matching
// java-tron's VotesStore. The value contains the voter's old vote list for
// this epoch and the latest new vote list.
func WriteVotes(db votesIndexReadWriter, addr common.Address, votes *corepb.Votes) error {
	if votes == nil {
		return db.Delete(votesKey(addr.Bytes()))
	}
	if len(votes.Address) == 0 {
		votes.Address = addr.Bytes()
	}
	data, err := proto.Marshal(votes)
	if err != nil {
		return err
	}
	if err := db.Put(votesKey(addr.Bytes()), data); err != nil {
		return err
	}
	AppendVotesIndex(db, addr)
	return nil
}

// ReadVotes returns the pending vote change for a voter, or nil if absent.
func ReadVotes(db ethdb.KeyValueReader, addr common.Address) *corepb.Votes {
	data, err := db.Get(votesKey(addr.Bytes()))
	if err != nil || len(data) == 0 {
		return nil
	}
	var votes corepb.Votes
	if err := proto.Unmarshal(data, &votes); err != nil {
		return nil
	}
	return &votes
}

func DeleteVotes(db ethdb.KeyValueWriter, addr common.Address) error {
	return db.Delete(votesKey(addr.Bytes()))
}

func WriteVotesIndex(db ethdb.KeyValueWriter, voters []common.Address) {
	buf := make([]byte, 4+len(voters)*common.AddressLength)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(voters)))
	for i, voter := range voters {
		copy(buf[4+i*common.AddressLength:], voter.Bytes())
	}
	_ = db.Put(votesIndexKey, buf)
}

func ReadVotesIndex(db ethdb.KeyValueReader) []common.Address {
	data, err := db.Get(votesIndexKey)
	if err != nil || len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+count*common.AddressLength {
		return nil
	}
	voters := make([]common.Address, count)
	for i := range voters {
		voters[i] = common.BytesToAddress(data[4+i*common.AddressLength : 4+(i+1)*common.AddressLength])
	}
	return voters
}

func AppendVotesIndex(db votesIndexReadWriter, addr common.Address) {
	existing := ReadVotesIndex(db)
	for _, voter := range existing {
		if voter == addr {
			return
		}
	}
	existing = append(existing, addr)
	WriteVotesIndex(db, existing)
}
