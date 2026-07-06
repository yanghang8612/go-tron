package state

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// Phase 3d roots the proposal governance store into the reserved system
// account's SystemProposal KV so it rewinds with the full state root. Two
// logical keys live in the domain, mirroring java-tron's ProposalStore
// (a TronStoreWithRevoking, hence rewindable):
//
//   - proposalStoreKey(id): one JSON-encoded rawdb.Proposal per id, the
//     per-proposal record written by ProposalCreate/Approve/Delete and the
//     maintenance settlement (ProcessProposals).
//   - proposalStoreIndexKey: the enumeration of every proposal id, grown by
//     ProposalCreateActuator and iterated at each maintenance boundary.
//
// LatestProposalNum stays in dynamic properties (rooted in Phase 3b); only the
// proposal records and their index move here.
//
// The value encoding reuses the existing on-disk wire format verbatim — the
// per-proposal record is json.Marshal of rawdb.Proposal (the same bytes the
// old flat `p-` accessor wrote), and the index is 8-byte big-endian ids (the
// same bytes the old `propi` accessor wrote) — so no new encoding lineage is
// introduced.
var proposalStoreIndexKey = []byte("ProposalIndex")

// proposalStoreKey maps a proposal id to its logical key within the
// SystemProposal domain: a fixed "p" tag followed by the 8-byte big-endian id.
func proposalStoreKey(id int64) []byte {
	k := make([]byte, 1+8)
	k[0] = 'p'
	binary.BigEndian.PutUint64(k[1:], uint64(id))
	return k
}

// encodeProposalIndex packs ids as N×8 big-endian bytes (drop-in for the prior
// rawdb writer's format).
func encodeProposalIndex(ids []int64) []byte {
	buf := make([]byte, 8*len(ids))
	for i, id := range ids {
		binary.BigEndian.PutUint64(buf[i*8:], uint64(id))
	}
	return buf
}

// decodeProposalIndex reverses encodeProposalIndex. Empty data → nil, matching
// the prior rawdb reader.
func decodeProposalIndex(data []byte) []int64 {
	if len(data) == 0 {
		return nil
	}
	if len(data)%8 != 0 {
		return nil
	}
	ids := make([]int64, len(data)/8)
	for i := range ids {
		ids[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	return ids
}

func decodeProposalIndexStrict(data []byte) ([]int64, error) {
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("decode proposal index: length %d is not a multiple of 8", len(data))
	}
	return decodeProposalIndex(data), nil
}

// ReadProposal resolves a proposal record from the rooted system-KV (nil if
// absent or on a decode/KV error, matching the prior rawdb reader's defensive
// behavior).
func (s *StateDB) ReadProposal(id int64) *rawdb.Proposal {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemProposal, proposalStoreKey(id))
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	p := &rawdb.Proposal{}
	if err := json.Unmarshal(raw, p); err != nil {
		return nil
	}
	return p
}

// ReadProposalStrict resolves a proposal record from the rooted system-KV and
// surfaces storage/corruption errors. Missing rows return (nil, false, nil).
func (s *StateDB) ReadProposalStrict(id int64) (*rawdb.Proposal, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemProposal, proposalStoreKey(id))
	if err != nil || !ok {
		return nil, ok, err
	}
	p := &rawdb.Proposal{}
	if err := json.Unmarshal(raw, p); err != nil {
		return nil, true, fmt.Errorf("decode proposal %d: %w", id, err)
	}
	return p, true, nil
}

// ProposalAt reconstructs a rooted proposal record at the end of blockNum.
func (r *PersistentHistoryReader) ProposalAt(id int64, blockNum uint64) (*rawdb.Proposal, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemProposal, proposalStoreKey(id), blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return nil, err
	}
	p := &rawdb.Proposal{}
	if err := json.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("decode proposal %d: %w", id, err)
	}
	return p, nil
}

// WriteProposal stages a proposal record into the system-KV. The error is
// non-nil only for a marshal failure or an unregistered domain (a programmer
// error), since SystemProposal is registered at init.
func (s *StateDB) WriteProposal(id int64, p *rawdb.Proposal) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemProposal, proposalStoreKey(id), data)
}

// ReadProposalIndex returns the rooted proposal index (nil if unset). KV error
// swallowed to nil — drop-in for the prior rawdb reader's consumers.
func (s *StateDB) ReadProposalIndex() []int64 {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemProposal, proposalStoreIndexKey)
	if err != nil || !ok {
		return nil
	}
	return decodeProposalIndex(raw)
}

// ReadProposalIndexStrict returns the rooted proposal id index and surfaces
// storage/corruption errors. Missing rows return (nil, false, nil).
func (s *StateDB) ReadProposalIndexStrict() ([]int64, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemProposal, proposalStoreIndexKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	ids, err := decodeProposalIndexStrict(raw)
	if err != nil {
		return nil, true, err
	}
	return ids, true, nil
}

// ProposalIndexAt reconstructs the rooted proposal id index at the end of blockNum.
func (r *PersistentHistoryReader) ProposalIndexAt(blockNum uint64) ([]int64, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemProposal, proposalStoreIndexKey, blockNum)
	if err != nil || !ok {
		return nil, err
	}
	return decodeProposalIndexStrict(raw)
}

// WriteProposalIndex stages the full proposal index into the system-KV.
func (s *StateDB) WriteProposalIndex(ids []int64) error {
	return s.SystemKVPut(kvdomains.SystemProposal, proposalStoreIndexKey, encodeProposalIndex(ids))
}

// AppendProposalIndex adds id to the proposal index. The read error is
// propagated (not swallowed) so a transient trie failure aborts the append
// instead of overwriting the index with a truncated list. Proposal ids are
// strictly increasing (LatestProposalNum pre-increment), so no dedup is needed.
func (s *StateDB) AppendProposalIndex(id int64) error {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemProposal, proposalStoreIndexKey)
	if err != nil {
		return err
	}
	var existing []int64
	if ok {
		existing, err = decodeProposalIndexStrict(raw)
		if err != nil {
			return err
		}
	}
	return s.WriteProposalIndex(append(existing, id))
}
