package state

import (
	"bytes"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/params"
)

// Account name/ID uniqueness indexes are rooted into the reserved system
// account's SystemAccountIndex KV so they rewind with the full state root.
// They mirror java-tron's two reverse-lookup stores:
//
//   - AccountIndexStore (db "account-index"): account_name bytes -> 21-byte
//     owner address. Written by AccountUpdateContract and genesis; read by the
//     name-uniqueness precheck and the TVM Blackhole-address lookup.
//   - AccountIdIndexStore (db "accountid-index"): account_id (a user-chosen
//     UTF-8 string, lower-cased before insert) -> 21-byte owner address.
//     Written by SetAccountIdContract; read by its uniqueness check and the
//     getaccountbyid RPC.
//
// Both share one domain (SystemAccountIndex) but address disjoint key-spaces.
// A single-byte tag disambiguates the two so a name can never collide with an
// id of the same bytes:
//
//	accountNameIndexTag || name        (name index)
//	accountIdIndexTag    || lower(id)   (id index)
//
// The value is the raw 21-byte owner address, identical to the prior flat
// `ani-`/`aid-` on-disk format, so no new value encoding is introduced.
const (
	accountNameIndexTag byte = 0x01
	accountIdIndexTag   byte = 0x02
)

var blackholeAccountNameIndexKey = [...]byte{
	accountNameIndexTag, 'B', 'l', 'a', 'c', 'k', 'h', 'o', 'l', 'e',
}

// accountNameIndexKVKey is the SystemAccountIndex logical key for a name entry.
func accountNameIndexKVKey(accountName []byte) []byte {
	out := make([]byte, 1+len(accountName))
	out[0] = accountNameIndexTag
	copy(out[1:], accountName)
	return out
}

// accountIdIndexKVKey is the SystemAccountIndex logical key for an id entry.
// The id is lower-cased here (the java-tron AccountIdIndexStore boundary) so
// callers never have to, matching the prior flat accessor's behavior.
func accountIdIndexKVKey(accountID []byte) []byte {
	lower := bytes.ToLower(accountID)
	out := make([]byte, 1+len(lower))
	out[0] = accountIdIndexTag
	copy(out[1:], lower)
	return out
}

// ReadAccountNameIndex returns the owner address registered for accountName, or
// nil if none. A KV error is swallowed to nil, matching the prior rawdb reader.
func (s *StateDB) ReadAccountNameIndex(accountName []byte) []byte {
	raw, ok, err := s.ReadAccountNameIndexStrict(accountName)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read account name index %q", string(accountName)), err)
		return nil
	}
	if !ok {
		return nil
	}
	return raw
}

// BlackholeAddress resolves the chain-specific genesis Blackhole account.
// Nile and mainnet use different addresses; the rooted genesis name index is
// the same source already used by TVM execution. The mainnet constant remains
// a compatibility fallback for unit tests and legacy databases without the
// index row.
func (s *StateDB) BlackholeAddress() tcommon.Address {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemAccountIndex, blackholeAccountNameIndexKey[:])
	if err != nil {
		return params.BlackholeAddress
	}
	if ok && len(raw) == tcommon.AddressLength {
		return tcommon.BytesToAddress(raw)
	}
	if ok {
		s.recordStateError("decode blackhole account name index", fmt.Errorf("length %d, want %d", len(raw), tcommon.AddressLength))
	}
	return params.BlackholeAddress
}

// HasAccountNameIndex reports whether accountName is registered. Mirrors
// java-tron AccountIndexStore.has, used by AccountUpdateActuator's uniqueness
// precheck.
func (s *StateDB) HasAccountNameIndex(accountName []byte) bool {
	ok, err := s.HasAccountNameIndexStrict(accountName)
	if err != nil {
		s.recordStateError(fmt.Sprintf("check account name index %q", string(accountName)), err)
		return false
	}
	return ok
}

// WriteAccountNameIndex stages a name->owner mapping. The error is non-nil only
// for an unregistered domain (a programmer error), since SystemAccountIndex is
// registered at init.
func (s *StateDB) WriteAccountNameIndex(accountName []byte, owner tcommon.Address) error {
	return s.SystemKVPut(kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName), owner.Bytes())
}

// DeleteAccountNameIndex removes a name mapping.
func (s *StateDB) DeleteAccountNameIndex(accountName []byte) error {
	return s.SystemKVDelete(kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
}

// ReadAccountIdIndex returns the owner address registered for accountID
// (case-insensitive), or nil if none. KV error swallowed to nil.
func (s *StateDB) ReadAccountIdIndex(accountID []byte) []byte {
	raw, ok, err := s.ReadAccountIdIndexStrict(accountID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read account id index %q", string(accountID)), err)
		return nil
	}
	if !ok {
		return nil
	}
	return raw
}

// HasAccountIdIndex reports whether accountID is registered (case-insensitive).
// Mirrors java-tron AccountIdIndexStore.has, used by SetAccountIdActuator's
// uniqueness precheck.
func (s *StateDB) HasAccountIdIndex(accountID []byte) bool {
	ok, err := s.HasAccountIdIndexStrict(accountID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("check account id index %q", string(accountID)), err)
		return false
	}
	return ok
}

// WriteAccountIdIndex stages an id->owner mapping (id lower-cased internally).
func (s *StateDB) WriteAccountIdIndex(accountID []byte, owner tcommon.Address) error {
	return s.SystemKVPut(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID), owner.Bytes())
}

// DeleteAccountIdIndex removes an id mapping (case-insensitive).
func (s *StateDB) DeleteAccountIdIndex(accountID []byte) error {
	return s.SystemKVDelete(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
}
