package state

import (
	"bytes"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
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
	raw, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
	if err != nil || !ok {
		return nil
	}
	return raw
}

// HasAccountNameIndex reports whether accountName is registered. Mirrors
// java-tron AccountIndexStore.has, used by AccountUpdateActuator's uniqueness
// precheck.
func (s *StateDB) HasAccountNameIndex(accountName []byte) bool {
	_, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
	return err == nil && ok
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
	raw, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
	if err != nil || !ok {
		return nil
	}
	return raw
}

// HasAccountIdIndex reports whether accountID is registered (case-insensitive).
// Mirrors java-tron AccountIdIndexStore.has, used by SetAccountIdActuator's
// uniqueness precheck.
func (s *StateDB) HasAccountIdIndex(accountID []byte) bool {
	_, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
	return err == nil && ok
}

// WriteAccountIdIndex stages an id->owner mapping (id lower-cased internally).
func (s *StateDB) WriteAccountIdIndex(accountID []byte, owner tcommon.Address) error {
	return s.SystemKVPut(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID), owner.Bytes())
}

// DeleteAccountIdIndex removes an id mapping (case-insensitive).
func (s *StateDB) DeleteAccountIdIndex(accountID []byte) error {
	return s.SystemKVDelete(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
}
