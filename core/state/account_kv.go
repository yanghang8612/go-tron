package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

type accountKVIndexStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
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

func splitKVCompositeKey(composite []byte) (kvdomains.KVDomain, []byte, bool) {
	if len(composite) < 2 {
		return 0, nil, false
	}
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(composite[:2]))
	return domain, append([]byte(nil), composite[2:]...), true
}

func (s *StateDB) accountKVIndex() accountKVIndexStore {
	if s.accountKVIndexStore != nil {
		return s.accountKVIndexStore
	}
	return s.db.DiskDB()
}

func (s *StateDB) nextAccountKVGeneration(owner tcommon.Address, prev *stateObject) uint64 {
	if prev != nil {
		return prev.accountKVGeneration + 1
	}
	gen, ok, err := rawdb.ReadStateKVGeneration(s.accountKVIndex(), owner)
	if err == nil && ok {
		return gen + 1
	}
	return 0
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

// GetAccountKVBatch opens owner's KV trie ONCE and resolves every key in one
// domain, returning name->value for present keys (presence prefix stripped).
// The dirty overlay is consulted first per key, matching GetAccountKV; a
// freshly-opened StateDB (the dynamic-properties Load case) has an empty
// overlay, so this is effectively N trie Gets over a single OpenTrie — versus
// N OpenTrie+Get pairs if the caller looped GetAccountKV.
func (s *StateDB) GetAccountKVBatch(owner tcommon.Address, domain kvdomains.KVDomain, keys [][]byte) (map[string][]byte, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	out := make(map[string][]byte, len(keys))
	obj := s.getStateObject(owner)
	if obj == nil {
		return out, nil
	}
	tr, err := s.db.OpenTrie(ethcommon.Hash(obj.accountKVRoot))
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		comp := kvCompositeKey(domain, key)
		if e, ok := obj.kvDirty[string(comp)]; ok {
			if !e.deleted {
				out[string(key)] = append([]byte{}, e.val...)
			}
			continue
		}
		raw, err := tr.Get(kvTrieKey(comp))
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		out[string(key)] = append([]byte{}, raw[1:]...) // strip presence prefix
	}
	return out, nil
}

// IterateAccountKV iterates the current StateDB view for owner's domain and
// logical prefix. The physical latest-state index supplies the committed rows;
// the in-memory dirty overlay is merged on top so callers see uncommitted puts
// and tombstones consistently with GetAccountKV.
func (s *StateDB) IterateAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil
	}
	entries := make(map[string][]byte)
	if err := rawdb.IterateStateKVLatest(s.accountKVIndex(), owner, obj.accountKVGeneration, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	}); err != nil {
		return err
	}
	for mapKey, entry := range obj.kvDirty {
		d, logicalKey, ok := splitKVCompositeKey([]byte(mapKey))
		if !ok || d != domain || !bytes.HasPrefix(logicalKey, prefix) {
			continue
		}
		if entry.deleted {
			delete(entries, string(logicalKey))
			continue
		}
		entries[string(logicalKey)] = append([]byte(nil), entry.val...)
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		cont, err := fn([]byte(key), append([]byte(nil), entries[key]...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
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

// DeleteAccountKVPrefix stages tombstones for every visible key under
// owner/domain/prefix. It uses the physical latest-state index for committed
// rows and the dirty overlay for same-block writes.
func (s *StateDB) DeleteAccountKVPrefix(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte) error {
	var keys [][]byte
	if err := s.IterateAccountKV(owner, domain, prefix, func(key, _ []byte) (bool, error) {
		keys = append(keys, append([]byte(nil), key...))
		return true, nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.DeleteAccountKV(owner, domain, key); err != nil {
			return err
		}
	}
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

func (s *StateDB) commitAccountKVLatest(obj *stateObject) error {
	index := s.accountKVIndex()
	keys := make([]string, 0, len(obj.kvDirty))
	for mapKey := range obj.kvDirty {
		keys = append(keys, mapKey)
	}
	sort.Strings(keys)
	for _, mapKey := range keys {
		entry := obj.kvDirty[mapKey]
		domain, logicalKey, ok := splitKVCompositeKey([]byte(mapKey))
		if !ok {
			return fmt.Errorf("account kv: malformed composite key for %s", obj.address.Hex())
		}
		if err := s.writeDomainChange(index, obj, domain, logicalKey, entry); err != nil {
			return err
		}
		if entry.deleted {
			if err := rawdb.DeleteStateKVLatest(index, obj.address, obj.accountKVGeneration, domain, logicalKey); err != nil {
				return err
			}
			continue
		}
		if err := rawdb.WriteStateKVLatest(index, obj.address, obj.accountKVGeneration, domain, logicalKey, entry.val); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateDB) writeDomainChange(index accountKVIndexStore, obj *stateObject, domain kvdomains.KVDomain, logicalKey []byte, entry kvEntry) error {
	if !s.changeSet.enabled {
		return nil
	}
	prev, prevExists, err := rawdb.ReadStateKVLatest(index, obj.address, obj.accountKVGeneration, domain, logicalKey)
	if err != nil {
		return err
	}
	nextExists := !entry.deleted
	var next []byte
	if nextExists {
		next = append([]byte(nil), entry.val...)
	}
	s.changeSet.seq++
	return rawdb.WriteStateDomainChange(s.changeSet.writer, &rawdb.StateDomainChange{
		BlockNum:   s.changeSet.blockNum,
		BlockHash:  s.changeSet.blockHash,
		TxNum:      s.changeSet.txNum,
		Seq:        s.changeSet.seq,
		Owner:      obj.address,
		Generation: obj.accountKVGeneration,
		Domain:     domain,
		Key:        append([]byte(nil), logicalKey...),
		PrevExists: prevExists,
		Prev:       prev,
		NextExists: nextExists,
		Next:       next,
	})
}

func (s *StateDB) writeAccountKVGeneration(obj *stateObject) error {
	return rawdb.WriteStateKVGeneration(s.accountKVIndex(), obj.address, obj.accountKVGeneration)
}
