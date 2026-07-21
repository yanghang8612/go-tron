package rawdb

import (
	"github.com/tronprotocol/go-tron/common"
)

const (
	// Keep the historical internal APPROVED value stable: proposal records are
	// JSON-encoded in rooted state and existing databases already contain 1.
	// DISAPPROVED and CANCELED must nevertheless remain distinct, matching
	// java-tron's Proposal.State semantics.
	ProposalStatePending     = 0
	ProposalStateApproved    = 1
	ProposalStateDisapproved = 2
	ProposalStateCanceled    = 3
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
