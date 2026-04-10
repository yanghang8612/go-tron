package rawdb

import (
	"encoding/binary"
	"encoding/json"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const (
	ProposalStatePending  = 0
	ProposalStateApproved = 1
	ProposalStateCanceled = 2
)

type Proposal struct {
	ID             int64            `json:"id"`
	Proposer       common.Address   `json:"proposer"`
	Parameters     map[int64]int64  `json:"parameters"`
	CreateTime     int64            `json:"create_time"`
	ExpirationTime int64            `json:"expiration_time"`
	Approvals      []common.Address `json:"approvals"`
	State          int32            `json:"state"`
}

func WriteProposal(db ethdb.KeyValueWriter, id int64, p *Proposal) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return db.Put(proposalKey(id), data)
}

func ReadProposal(db ethdb.KeyValueReader, id int64) *Proposal {
	data, err := db.Get(proposalKey(id))
	if err != nil || len(data) == 0 {
		return nil
	}
	p := &Proposal{}
	if err := json.Unmarshal(data, p); err != nil {
		return nil
	}
	return p
}

func WriteProposalIndex(db ethdb.KeyValueWriter, ids []int64) error {
	buf := make([]byte, 8*len(ids))
	for i, id := range ids {
		binary.BigEndian.PutUint64(buf[i*8:], uint64(id))
	}
	return db.Put(proposalIndexKey, buf)
}

func ReadProposalIndex(db ethdb.KeyValueReader) []int64 {
	data, err := db.Get(proposalIndexKey)
	if err != nil || len(data) == 0 {
		return nil
	}
	ids := make([]int64, len(data)/8)
	for i := range ids {
		ids[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	return ids
}
