package rawdb

import (
	"github.com/tronprotocol/go-tron/common"
)

const (
	ProposalStatePending  = 0
	ProposalStateApproved = 1
	ProposalStateCanceled = 2
)

// Proposal is the on-disk record for a TRON governance proposal. The records
// and their index are rooted into the SystemProposal account-KV domain (see
// core/state/proposal_store.go); this struct stays here as the shared wire
// type read by the actuators, ProcessProposals, and the API backend.
type Proposal struct {
	ID             int64            `json:"id"`
	Proposer       common.Address   `json:"proposer"`
	Parameters     map[int64]int64  `json:"parameters"`
	CreateTime     int64            `json:"create_time"`
	ExpirationTime int64            `json:"expiration_time"`
	Approvals      []common.Address `json:"approvals"`
	State          int32            `json:"state"`
}
