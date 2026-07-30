package state

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func AccountNameIndexPrefetchKey(accountName []byte) PrefetchKey {
	return AccountKVPrefetchKey(tcommon.SystemAccountAddress, kvdomains.SystemAccountIndex, accountNameIndexKVKey(accountName))
}

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

func (s *StateDB) ReadAccountNameIndexStrict(accountName []byte) ([]byte, bool, error) {
	return s.readAccountIndexAddressStrict(accountNameIndexKVKey(accountName), "account name index")
}

func (s *StateDB) HasAccountNameIndexStrict(accountName []byte) (bool, error) {
	_, ok, err := s.ReadAccountNameIndexStrict(accountName)
	if err != nil {
		return false, err
	}
	return ok, err
}

func (s *StateDB) ReadAccountIdIndexStrict(accountID []byte) ([]byte, bool, error) {
	return s.readAccountIndexAddressStrict(accountIdIndexKVKey(accountID), "account id index")
}

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

func (s *StateDB) HasAccountIdIndexStrict(accountID []byte) (bool, error) {
	_, ok, err := s.ReadAccountIdIndexStrict(accountID)
	if err != nil {
		return false, err
	}
	return ok, err
}
