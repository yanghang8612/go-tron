package state

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/tronprotocol/go-tron/core/rawdb"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// This file roots java-tron's VotesStore into the reserved system account's
// WitnessVoteState KV so the pending-vote ledger rewinds with the full state
// root. The store is self-evicting: actuators (Vote / Unfreeze) and the TVM
// opVote write a per-voter delta record across an epoch, and the maintenance
// drain (core.applyPendingVotes) reads every record at the cycle boundary,
// folds the old/new deltas into the witness vote counts, then clears the whole
// store. The following logical rows live in the domain:
//
//   - votesStoreKey(voter): one versioned native corepb.Votes-shaped record per
//     voter. Legacy flat/rooted protobuf bytes remain read-compatible.
//   - count, bounded ordered pages, and membership rows for this epoch.
//
// All index rows use the same journal/history/MuHash lifecycle as vote records.
const votesIndexPageEntries = 128

var votesStoreIndexKey = rawdb.VotesIndexMetaStateKey()

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
	votes, _, err := s.ReadVotesStrict(addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read votes %s", addr.Hex()), err)
		return nil
	}
	return votes
}

// ReadVotesStrict resolves a voter's pending vote record and distinguishes
// missing rows from unreadable or malformed rooted data.
func (s *StateDB) ReadVotesStrict(addr tcommon.Address) (*corepb.Votes, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.WitnessVoteState, votesStoreKey(addr))
	if err != nil || !ok {
		return nil, ok, err
	}
	if len(raw) == 0 {
		return nil, true, fmt.Errorf("decode votes: empty value")
	}
	votes := &corepb.Votes{}
	if err := statecodec.Unmarshal(raw, votes); err != nil {
		return nil, true, fmt.Errorf("decode votes: %w", err)
	}
	return votes, true, nil
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
	data, err := statecodec.Marshal(votes)
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
	voters, _, err := s.ReadVotesIndexStrict()
	if err != nil {
		s.recordStateError("read votes index", err)
		return nil
	}
	return voters
}

// ReadVotesIndexStrict returns the rooted voter index and distinguishes an
// absent row from unreadable or malformed rooted data.
func (s *StateDB) votesIndexCount() (uint64, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.WitnessVoteState, votesStoreIndexKey)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(raw) != 8 {
		return 0, true, fmt.Errorf("votes index: invalid count length %d", len(raw))
	}
	n := binary.BigEndian.Uint64(raw)
	if n > uint64(math.MaxInt/tcommon.AddressLength) {
		return 0, true, fmt.Errorf("votes index: count overflow")
	}
	return n, true, nil
}

func (s *StateDB) votesIndexPage(page, expected uint64) ([]tcommon.Address, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.WitnessVoteState, rawdb.VotesIndexPageStateKey(page))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("votes index: missing page %d", page)
	}
	if expected == 0 || expected > votesIndexPageEntries || uint64(len(raw)) != 4+expected*tcommon.AddressLength {
		return nil, fmt.Errorf("votes index: page %d length", page)
	}
	entries, err := decodeAddressListStrict("votes index page", raw)
	if err != nil || uint64(len(entries)) != expected {
		return nil, fmt.Errorf("votes index: page %d count (%v)", page, err)
	}
	return entries, nil
}

func (s *StateDB) ReadVotesIndexStrict() ([]tcommon.Address, bool, error) {
	n, ok, err := s.votesIndexCount()
	if err != nil || !ok {
		return nil, ok, err
	}
	var all []tcommon.Address
	for page, offset := uint64(0), uint64(0); offset < n; page, offset = page+1, offset+votesIndexPageEntries {
		entries, err := s.votesIndexPage(page, min(n-offset, votesIndexPageEntries))
		if err != nil {
			return nil, true, err
		}
		all = append(all, entries...)
	}
	return all, true, nil
}

// WriteVotesIndex preserves explicit list order/duplicates. Empty is present,
// with no pages or memberships. Replacement uses normal journaled row deletes.
func (s *StateDB) WriteVotesIndex(voters []tcommon.Address) error {
	for _, prefix := range [][]byte{rawdb.VotesIndexPageStatePrefix(), rawdb.VotesIndexMemberStatePrefix()} {
		if err := s.DeleteAccountKVPrefix(tcommon.SystemAccountAddress, kvdomains.WitnessVoteState, prefix); err != nil {
			return err
		}
	}
	for offset := 0; offset < len(voters); offset += votesIndexPageEntries {
		entries := voters[offset:min(len(voters), offset+votesIndexPageEntries)]
		if err := s.SystemKVPut(kvdomains.WitnessVoteState, rawdb.VotesIndexPageStateKey(uint64(offset/votesIndexPageEntries)), encodeAddressList(entries)); err != nil {
			return err
		}
	}
	for _, addr := range voters {
		if err := s.SystemKVPut(kvdomains.WitnessVoteState, rawdb.VotesIndexMemberStateKey(addr), []byte{1}); err != nil {
			return err
		}
	}
	return s.SystemKVPut(kvdomains.WitnessVoteState, votesStoreIndexKey, binary.BigEndian.AppendUint64(nil, uint64(len(voters))))
}

// AppendVotesIndex performs bounded point reads and only rewrites the tail page.
func (s *StateDB) AppendVotesIndex(addr tcommon.Address) error {
	member, exists, err := s.SystemKVGet(kvdomains.WitnessVoteState, rawdb.VotesIndexMemberStateKey(addr))
	if err != nil {
		return err
	}
	if exists {
		if len(member) != 1 || member[0] != 1 {
			return fmt.Errorf("votes index: malformed membership")
		}
		return nil
	}
	n, _, err := s.votesIndexCount()
	if err != nil {
		return err
	}
	if n >= uint64(math.MaxInt/tcommon.AddressLength) {
		return fmt.Errorf("votes index: count overflow")
	}
	var entries []tcommon.Address
	if n%votesIndexPageEntries != 0 {
		entries, err = s.votesIndexPage(n/votesIndexPageEntries, n%votesIndexPageEntries)
		if err != nil {
			return err
		}
	}
	entries = append(entries, addr)
	if err = s.SystemKVPut(kvdomains.WitnessVoteState, rawdb.VotesIndexPageStateKey(n/votesIndexPageEntries), encodeAddressList(entries)); err != nil {
		return err
	}
	if err = s.SystemKVPut(kvdomains.WitnessVoteState, rawdb.VotesIndexMemberStateKey(addr), []byte{1}); err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.WitnessVoteState, votesStoreIndexKey, binary.BigEndian.AppendUint64(nil, n+1))
}
