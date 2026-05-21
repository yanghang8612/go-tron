package state

import (
	"encoding/binary"
	"fmt"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// kvPresencePrefix wraps persisted KV values so an empty-but-present value
// stays distinct from an absent key (go-ethereum tries treat an empty value as
// a delete). Internal to the generic KV trie; never java-tron-visible.
const kvPresencePrefix = 0x01

// kvEntry is one pending account-KV write in the dirty overlay. deleted=true is
// a tombstone; deleted=false means val is present (val may be empty but != nil).
type kvEntry struct {
	val     []byte
	deleted bool
}

// kvCompositeKey is the pre-hash logical key: domain (big-endian u16) || key.
func kvCompositeKey(domain kvdomains.KVDomain, key []byte) []byte {
	out := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(out, uint16(domain))
	copy(out[2:], key)
	return out
}

// kvTrieKey is the per-account KV trie key: Keccak256(domain || key).
func kvTrieKey(composite []byte) []byte {
	return crypto.Keccak256(composite)
}

// GetAccountKV reads a generic-KV value for owner. Returns (value, exists, err).
func (s *StateDB) GetAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil, false, nil
	}
	comp := kvCompositeKey(domain, key)
	if e, ok := obj.kvDirty[string(comp)]; ok {
		if e.deleted {
			return nil, false, nil
		}
		return append([]byte{}, e.val...), true, nil
	}
	tr, err := s.db.OpenTrie(ethcommon.Hash(obj.accountKVRoot))
	if err != nil {
		return nil, false, err
	}
	raw, err := tr.Get(kvTrieKey(comp))
	if err != nil {
		return nil, false, err
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	return append([]byte{}, raw[1:]...), true, nil // strip presence prefix
}

// SetAccountKV stages a generic-KV write for owner (creating the account if absent).
func (s *StateDB) SetAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.GetOrCreateAccount(owner)
	mk := string(kvCompositeKey(domain, key))
	prev, had := obj.kvDirty[mk]
	s.journal.append(kvChange{address: owner, mapKey: mk, hadEntry: had, prevEntry: prev})
	obj.kvDirty[mk] = kvEntry{val: append([]byte{}, value...), deleted: false}
	obj.markDirty()
	return nil
}

// DeleteAccountKV stages a tombstone for owner's (domain,key).
func (s *StateDB) DeleteAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil
	}
	mk := string(kvCompositeKey(domain, key))
	prev, had := obj.kvDirty[mk]
	s.journal.append(kvChange{address: owner, mapKey: mk, hadEntry: had, prevEntry: prev})
	obj.kvDirty[mk] = kvEntry{val: nil, deleted: true}
	obj.markDirty()
	return nil
}

// ResetAccountKV discards owner's entire generic-KV namespace: the KV root is
// reset to empty and the generation is bumped. Old keys become unreachable from
// the new generation without an O(N) prefix delete (Erigon-incarnation style).
func (s *StateDB) ResetAccountKV(owner tcommon.Address) error {
	obj := s.getStateObject(owner)
	if obj == nil {
		return nil
	}
	prevDirty := make(map[string]kvEntry, len(obj.kvDirty))
	for k, v := range obj.kvDirty {
		prevDirty[k] = v
	}
	s.journal.append(kvResetChange{
		address:        owner,
		prevRoot:       obj.accountKVRoot,
		prevGeneration: obj.accountKVGeneration,
		prevDirty:      prevDirty,
	})
	obj.kvDirty = make(map[string]kvEntry)
	obj.accountKVRoot = EmptyKVRoot
	obj.accountKVGeneration++
	obj.markDirty()
	return nil
}

// commitAccountKV applies obj's dirty KV overlay to its KV trie, persists the
// trie nodes, and returns the new AccountKVRoot. Call only when len(obj.kvDirty) > 0.
func (s *StateDB) commitAccountKV(obj *stateObject) (tcommon.Hash, error) {
	base := ethcommon.Hash(obj.accountKVRoot)
	tr, err := s.db.OpenTrie(base)
	if err != nil {
		return tcommon.Hash{}, err
	}
	for mk, e := range obj.kvDirty {
		tk := kvTrieKey([]byte(mk))
		if e.deleted {
			if err := tr.Delete(tk); err != nil {
				return tcommon.Hash{}, err
			}
			continue
		}
		wrapped := make([]byte, 1+len(e.val))
		wrapped[0] = kvPresencePrefix
		copy(wrapped[1:], e.val)
		if err := tr.Update(tk, wrapped); err != nil {
			return tcommon.Hash{}, err
		}
	}
	root, nodes := tr.Commit(false)
	if nodes != nil {
		if err := s.db.TrieDB().Update(root, base, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}
	return tcommon.Hash(root), nil
}
