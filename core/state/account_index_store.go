package state

import (
	"bytes"
	"fmt"

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

// AccountNameIndexPrefetchKey returns the latest account name -> owner row.
func AccountNameIndexPrefetchKey(accountName []byte) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
}

// AccountIDIndexPrefetchKey returns the latest lower-cased account id -> owner row.
func AccountIDIndexPrefetchKey(accountID []byte) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
}

func validateAccountIndexAddress(label string, raw []byte) ([]byte, error) {
	if len(raw) != tcommon.AddressLength {
		return nil, fmt.Errorf("%s has malformed length %d, want %d", label, len(raw), tcommon.AddressLength)
	}
	return raw, nil
}

func (s *StateDB) readAccountIndexAddressStrict(key []byte, label string) ([]byte, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	raw, err = validateAccountIndexAddress(label, raw)
	if err != nil {
		return nil, true, err
	}
	return raw, true, nil
}

// ReadAccountNameIndex returns the owner address registered for accountName, or
// nil if none. A KV error is swallowed to nil, matching the prior rawdb reader.
func (s *StateDB) ReadAccountNameIndex(accountName []byte) []byte {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
	if err != nil || !ok {
		return nil
	}
	if raw, err = validateAccountIndexAddress("account name index", raw); err != nil {
		return nil
	}
	return raw
}

func (s *StateDB) ReadAccountNameIndexStrict(accountName []byte) ([]byte, bool, error) {
	return s.readAccountIndexAddressStrict(accountNameIndexKVKey(accountName), "account name index")
}

// HasAccountNameIndex reports whether accountName is registered. Mirrors
// java-tron AccountIndexStore.has, used by AccountUpdateActuator's uniqueness
// precheck.
func (s *StateDB) HasAccountNameIndex(accountName []byte) bool {
	_, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
	return err == nil && ok
}

func (s *StateDB) HasAccountNameIndexStrict(accountName []byte) (bool, error) {
	_, ok, err := s.ReadAccountNameIndexStrict(accountName)
	if err != nil {
		return false, err
	}
	return ok, nil
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
	if raw, err = validateAccountIndexAddress("account id index", raw); err != nil {
		return nil
	}
	return raw
}

func (s *StateDB) ReadAccountIdIndexStrict(accountID []byte) ([]byte, bool, error) {
	return s.readAccountIndexAddressStrict(accountIdIndexKVKey(accountID), "account id index")
}

// AccountIdIndexAt returns the owner address registered for accountID at the
// end of blockNum, using temporal SystemAccountIndex history.
func (r *PersistentHistoryReader) AccountIdIndexAt(accountID []byte, blockNum uint64) ([]byte, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID), blockNum)
	if err != nil || !ok {
		return nil, ok, err
	}
	raw, err = validateAccountIndexAddress(fmt.Sprintf("account id index at block %d", blockNum), raw)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// HasAccountIdIndex reports whether accountID is registered (case-insensitive).
// Mirrors java-tron AccountIdIndexStore.has, used by SetAccountIdActuator's
// uniqueness precheck.
func (s *StateDB) HasAccountIdIndex(accountID []byte) bool {
	_, ok, err := s.SystemKVGet(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
	return err == nil && ok
}

func (s *StateDB) HasAccountIdIndexStrict(accountID []byte) (bool, error) {
	_, ok, err := s.ReadAccountIdIndexStrict(accountID)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// WriteAccountIdIndex stages an id->owner mapping (id lower-cased internally).
func (s *StateDB) WriteAccountIdIndex(accountID []byte, owner tcommon.Address) error {
	return s.SystemKVPut(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID), owner.Bytes())
}

// DeleteAccountIdIndex removes an id mapping (case-insensitive).
func (s *StateDB) DeleteAccountIdIndex(accountID []byte) error {
	return s.SystemKVDelete(kvdomains.SystemAccountIndex, accountIdIndexKVKey(accountID))
}
