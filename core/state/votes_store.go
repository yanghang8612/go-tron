package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// This file roots java-tron's VotesStore into the reserved system account's
// WitnessVoteState KV so the pending-vote ledger rewinds with the full state
// root. The store is self-evicting: actuators (Vote / Unfreeze) and the TVM
// opVote write a per-voter delta record across an epoch, and the maintenance
// drain (core.applyPendingVotes) reads every record at the cycle boundary,
// folds the old/new deltas into the witness vote counts, then clears the whole
// store. Two logical keys live in the domain:
//
//   - votesStoreKey(voter): one proto-encoded corepb.Votes per voter — the
//     same bytes the old flat `v-` accessor wrote.
//   - votesStoreIndexKey: the enumeration of every voter that wrote a record
//     this epoch, iterated by the drain — encoded as the shared address-list
//     format (4-byte BE count || N×AddressLength), bit-for-bit identical to the
//     old flat `v-index` accessor.
//
// No new encoding lineage is introduced: the record codec is corepb.Votes and
// the index codec is encodeAddressList/decodeAddressList (shared with the
// witness schedule).
var votesStoreIndexKey = []byte("VotesIndex")

// votesStoreKey maps a voter address to its logical key within the
// WitnessVoteState domain: a fixed "v" tag followed by the 21-byte address.
func votesStoreKey(addr tcommon.Address) []byte {
	k := make([]byte, 1+tcommon.AddressLength)
	k[0] = 'v'
	copy(k[1:], addr.Bytes())
	return k
}

// ReadVotes resolves a voter's pending vote record from the rooted system-KV
// (nil if absent or on a decode/KV error, matching the prior rawdb reader's
// defensive behavior).
func (s *StateDB) ReadVotes(addr tcommon.Address) *corepb.Votes {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.WitnessVoteState, votesStoreKey(addr))
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	votes := &corepb.Votes{}
	if err := proto.Unmarshal(raw, votes); err != nil {
		return nil
	}
	return votes
}

// WriteVotes stages a voter's pending vote record into the system-KV and adds
// the voter to the index, mirroring the flat rawdb.WriteVotes coupling so the
// actuator / TVM callers (which never append the index explicitly) keep working
// unchanged. A nil record deletes the voter (without touching the index, as the
// flat accessor did). The error is non-nil only for a marshal failure or an
// unregistered domain (a programmer error), since WitnessVoteState is
// registered at init.
func (s *StateDB) WriteVotes(addr tcommon.Address, votes *corepb.Votes) error {
	if votes == nil {
		return s.DeleteVotes(addr)
	}
	if len(votes.Address) == 0 {
		votes.Address = addr.Bytes()
	}
	data, err := proto.Marshal(votes)
	if err != nil {
		return err
	}
	if err := s.SystemKVPut(kvdomains.WitnessVoteState, votesStoreKey(addr), data); err != nil {
		return err
	}
	return s.AppendVotesIndex(addr)
}

// DeleteVotes removes a voter's pending vote record from the system-KV. The
// index is left untouched (the drain clears it wholesale via WriteVotesIndex).
func (s *StateDB) DeleteVotes(addr tcommon.Address) error {
	return s.SystemKVDelete(kvdomains.WitnessVoteState, votesStoreKey(addr))
}

// ReadVotesIndex returns the rooted voter index (nil if unset). KV error
// swallowed to nil — drop-in for the prior rawdb reader's consumers.
func (s *StateDB) ReadVotesIndex() []tcommon.Address {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.WitnessVoteState, votesStoreIndexKey)
	if err != nil || !ok {
		return nil
	}
	return decodeAddressList(raw)
}

// WriteVotesIndex stages the full voter index into the system-KV. A nil/empty
// list clears it (the drain's reset path), which encodeAddressList renders as a
// 4-byte zero count that decodeAddressList maps back to nil.
func (s *StateDB) WriteVotesIndex(voters []tcommon.Address) error {
	return s.SystemKVPut(kvdomains.WitnessVoteState, votesStoreIndexKey, encodeAddressList(voters))
}

// AppendVotesIndex adds addr to the voter index if absent. The read error is
// propagated (not swallowed) so a transient trie failure aborts the append
// instead of overwriting the index with a truncated list. The read goes through
// the WitnessVoteState domain directly (not the witness-schedule-scoped
// readAddressList helper) so the round-trip stays within this store's domain.
func (s *StateDB) AppendVotesIndex(addr tcommon.Address) error {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.WitnessVoteState, votesStoreIndexKey)
	if err != nil {
		return err
	}
	var existing []tcommon.Address
	if ok {
		existing = decodeAddressList(raw)
	}
	for _, a := range existing {
		if a == addr {
			return nil
		}
	}
	return s.WriteVotesIndex(append(existing, addr))
}
