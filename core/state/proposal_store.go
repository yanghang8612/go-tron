package state

import (
	"encoding/binary"
	"encoding/json"

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
	ids := make([]int64, len(data)/8)
	for i := range ids {
		ids[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	return ids
}

// ReadProposal resolves a proposal record from the rooted system-KV (nil if
// absent or on a decode/KV error, matching the prior rawdb reader's defensive
// behavior).
func (s *StateDB) ReadProposal(id int64) *rawdb.Proposal {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemProposal, proposalStoreKey(id))
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	p := &rawdb.Proposal{}
	if err := json.Unmarshal(raw, p); err != nil {
		return nil
	}
	return p
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
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemProposal, proposalStoreIndexKey)
	if err != nil || !ok {
		return nil
	}
	return decodeProposalIndex(raw)
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
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemProposal, proposalStoreIndexKey)
	if err != nil {
		return err
	}
	var existing []int64
	if ok {
		existing = decodeProposalIndex(raw)
	}
	return s.WriteProposalIndex(append(existing, id))
}
