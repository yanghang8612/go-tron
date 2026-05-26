package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// RootedStore is a test-only rawdb-compatible view over StateDB. It exists to
// verify old rawdb key mappings against flat account-KV state after production
// code stopped wrapping execution DBs with this adapter.
type RootedStore struct {
	state    *StateDB
	fallback interface {
		ethdb.KeyValueReader
		ethdb.KeyValueWriter
	}
}

// NewRootedStore wraps fallback with a StateDB-backed view. Passing nil state
// or an already-rooted store is tolerated by callers that run outside block
// execution.
func NewRootedStore(state *StateDB, fallback interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}) *RootedStore {
	if r, ok := fallback.(*RootedStore); ok {
		return r
	}
	return &RootedStore{state: state, fallback: fallback}
}

// IsRootedStore reports whether db is already a StateDB-backed legacy view.
func IsRootedStore(db interface{}) bool {
	_, ok := db.(*RootedStore)
	return ok
}

func (s *RootedStore) Has(key []byte) (bool, error) {
	if s != nil && s.state != nil {
		if owner, ok := rawdb.ParseLegacyCodeKey(key); ok {
			return len(s.state.GetCode(owner)) > 0, nil
		}
		if rk, ok := rawdb.LookupRootedStateKey(key); ok {
			_, exists, err := s.state.GetAccountKV(rk.Owner, rk.Domain, rk.Key)
			return exists, err
		}
	}
	if s == nil || s.fallback == nil {
		return false, nil
	}
	return s.fallback.Has(key)
}

func (s *RootedStore) Get(key []byte) ([]byte, error) {
	if s != nil && s.state != nil {
		if owner, ok := rawdb.ParseLegacyCodeKey(key); ok {
			if code := s.state.GetCode(owner); len(code) > 0 {
				return append([]byte(nil), code...), nil
			}
			return nil, errors.New("rooted store: not found")
		}
		if rk, ok := rawdb.LookupRootedStateKey(key); ok {
			value, exists, err := s.state.GetAccountKV(rk.Owner, rk.Domain, rk.Key)
			if err != nil {
				return nil, err
			}
			if exists {
				return value, nil
			}
			return nil, errors.New("rooted store: not found")
		}
	}
	if s == nil || s.fallback == nil {
		return nil, errors.New("rooted store: not found")
	}
	return s.fallback.Get(key)
}

func (s *RootedStore) Put(key, value []byte) error {
	if s != nil && s.state != nil {
		if owner, ok := rawdb.ParseLegacyCodeKey(key); ok {
			s.state.SetCode(owner, value)
		}
		if rk, ok := rawdb.LookupRootedStateKey(key); ok {
			if err := s.state.SetAccountKV(rk.Owner, rk.Domain, rk.Key, value); err != nil {
				return err
			}
		}
	}
	if s == nil || s.fallback == nil {
		return nil
	}
	return s.fallback.Put(key, value)
}

func (s *RootedStore) Delete(key []byte) error {
	if s != nil && s.state != nil {
		if owner, ok := rawdb.ParseLegacyCodeKey(key); ok {
			s.state.SetCode(owner, nil)
		}
		if rk, ok := rawdb.LookupRootedStateKey(key); ok {
			if err := s.state.DeleteAccountKV(rk.Owner, rk.Domain, rk.Key); err != nil {
				return err
			}
		}
	}
	if s == nil || s.fallback == nil {
		return nil
	}
	return s.fallback.Delete(key)
}
