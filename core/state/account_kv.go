package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
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
	val        []byte
	wrapped    []byte
	prev       []byte
	deleted    bool
	trieKey    ethcommon.Hash
	prevExists bool
	prevLoaded bool
}

type kvCommitItem struct {
	logicalKey []byte
	entry      kvEntry
	domain     kvdomains.KVDomain
}

type accountKVCommitPlan struct {
	items        []kvCommitItem
	noopItems    int
	noopByDomain [kvDomainStatCount]int
}

type accountKVLatestBatch struct {
	writer ethdb.KeyValueWriter
	batch  ethdb.Batch
	ops    int
}

type accountKVCommitResult struct {
	root        tcommon.Hash
	trieChanged bool
	nodes       *trienode.NodeSet
}

type accountKVIndexStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

func newAccountKVLatestBatch(index accountKVIndexStore) *accountKVLatestBatch {
	w := &accountKVLatestBatch{writer: index}
	if batcher, ok := index.(ethdb.Batcher); ok {
		w.batch = batcher.NewBatch()
		w.writer = w.batch
	}
	return w
}

func (w *accountKVLatestBatch) put(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, encodedValue []byte) error {
	if err := rawdb.WriteStateKVLatestEncoded(w.writer, owner, generation, domain, logicalKey, encodedValue); err != nil {
		return err
	}
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) delete(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error {
	if err := rawdb.DeleteStateKVLatest(w.writer, owner, generation, domain, logicalKey); err != nil {
		return err
	}
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) maybeFlush() error {
	if w.batch == nil {
		return nil
	}
	w.ops++
	if w.batch.ValueSize() < ethdb.IdealBatchSize && w.ops < 4096 {
		return nil
	}
	if err := w.batch.Write(); err != nil {
		return err
	}
	w.batch.Reset()
	w.ops = 0
	return nil
}

func (w *accountKVLatestBatch) flush() error {
	if w.batch == nil || w.ops == 0 {
		return nil
	}
	if err := w.batch.Write(); err != nil {
		return err
	}
	w.batch.Reset()
	w.ops = 0
	return nil
}

type trieNodeBatchWriter struct {
	db           *Database
	batch        ethdb.Batch
	pendingCache []trieNodeCacheEntry
}

type trieNodeCacheEntry struct {
	hash ethcommon.Hash
	blob []byte
}

func newTrieNodeBatchWriter(db *Database) *trieNodeBatchWriter {
	return &trieNodeBatchWriter{
		db:    db,
		batch: db.newTrieNodeBatch(),
	}
}

func (w *trieNodeBatchWriter) release() {
	if w == nil || w.batch == nil {
		return
	}
	w.pendingCache = nil
	w.db.releaseTrieNodeBatch(w.batch)
	w.batch = nil
}

func (w *trieNodeBatchWriter) write(hash ethcommon.Hash, blob []byte) error {
	ethrawdb.WriteLegacyTrieNode(w.batch, hash, blob)
	if w.db.trieNodeCache != nil && len(blob) > 0 {
		w.pendingCache = append(w.pendingCache, trieNodeCacheEntry{hash: hash, blob: blob})
	}
	if w.batch.ValueSize() < ethdb.IdealBatchSize {
		return nil
	}
	return w.flush()
}

func (w *trieNodeBatchWriter) writeNodeSet(nodes *trienode.NodeSet) error {
	if nodes == nil {
		return nil
	}
	var err error
	nodes.ForEachWithOrder(func(_ string, n *trienode.Node) {
		if err != nil || n.IsDeleted() {
			return
		}
		err = w.write(n.Hash, n.Blob)
	})
	return err
}

func (w *trieNodeBatchWriter) flush() error {
	if w.batch.ValueSize() == 0 {
		return nil
	}
	if err := w.batch.Write(); err != nil {
		return err
	}
	for _, entry := range w.pendingCache {
		w.db.cacheTrieNode(entry.hash, entry.blob)
	}
	w.pendingCache = w.pendingCache[:0]
	w.batch.Reset()
	return nil
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

func kvTrieHash(composite []byte) ethcommon.Hash {
	return ethcommon.BytesToHash(kvTrieKey(composite))
}

func newKVEntry(composite, value []byte, deleted bool) kvEntry {
	e := kvEntry{deleted: deleted, trieKey: kvTrieHash(composite)}
	if !deleted {
		e.wrapped = make([]byte, 1+len(value))
		e.wrapped[0] = kvPresencePrefix
		copy(e.wrapped[1:], value)
		e.val = e.wrapped[1:]
	}
	return e
}

func (e kvEntry) encodedValue() []byte {
	if len(e.wrapped) > 0 {
		return e.wrapped
	}
	wrapped := make([]byte, 1+len(e.val))
	wrapped[0] = kvPresencePrefix
	copy(wrapped[1:], e.val)
	return wrapped
}

func (e *kvEntry) setPrev(value []byte, exists bool) {
	e.prevLoaded = true
	e.prevExists = exists
	if exists {
		e.prev = append(e.prev[:0], value...)
		return
	}
	e.prev = nil
}

func (e *kvEntry) inheritPrev(prev kvEntry) {
	if !prev.prevLoaded {
		return
	}
	e.prevLoaded = true
	e.prevExists = prev.prevExists
	if prev.prevExists {
		e.prev = append([]byte(nil), prev.prev...)
	}
}

func (e kvEntry) latestNoop() (bool, bool) {
	if !e.prevLoaded {
		return false, false
	}
	if e.deleted {
		return !e.prevExists, true
	}
	return e.prevExists && bytes.Equal(e.prev, e.val), true
}

func splitKVCompositeKey(composite []byte) (kvdomains.KVDomain, []byte, bool) {
	if len(composite) < 2 {
		return 0, nil, false
	}
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(composite[:2]))
	return domain, append([]byte(nil), composite[2:]...), true
}

func splitKVCompositeKeyView(composite []byte) (kvdomains.KVDomain, []byte, bool) {
	if len(composite) < 2 {
		return 0, nil, false
	}
	domain := kvdomains.KVDomain(binary.BigEndian.Uint16(composite[:2]))
	return domain, composite[2:], true
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

func (s *StateDB) accountKVTrie(obj *stateObject) (*trie.Trie, error) {
	if obj == nil {
		return nil, nil
	}
	if s.accountKVTries == nil {
		s.accountKVTries = make(map[tcommon.Address]*trie.Trie)
	}
	if tr := s.accountKVTries[obj.address]; tr != nil {
		return tr, nil
	}
	tr, err := s.db.OpenTrie(ethcommon.Hash(obj.accountKVRoot))
	if err != nil {
		return nil, err
	}
	s.accountKVTries[obj.address] = tr
	return tr, nil
}

func (s *StateDB) invalidateAccountKVTrie(owner tcommon.Address) {
	if s.accountKVTries != nil {
		delete(s.accountKVTries, owner)
	}
}

func (s *StateDB) clearAccountKVTrieCache() {
	if len(s.accountKVTries) > 0 {
		s.accountKVTries = nil
	}
}

func (s *StateDB) accountKVLatestReadEnabled() bool {
	return s.accountKVIndexReads && s.accountKVIndexStore != nil
}

// GetAccountKV reads a generic-KV value for owner. Returns (value, exists, err).
func (s *StateDB) GetAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	comp := kvCompositeKey(domain, key)
	if e, ok := obj.kvDirty[string(comp)]; ok {
		if e.deleted {
			return nil, false, nil
		}
		return append([]byte{}, e.val...), true, nil
	}
	if s.accountKVLatestReadEnabled() {
		return rawdb.ReadStateKVLatest(s.accountKVIndex(), owner, obj.accountKVGeneration, domain, key)
	}
	tr, err := s.accountKVTrie(obj)
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

// GetAccountKVAsOf reconstructs owner's domain key at the end of targetBlock
// using the Phase-5 domain change-set history. This first typed bridge resolves
// history within the account's current KV generation; account-incarnation
// history is handled by later commitment/history phases.
func (s *StateDB) GetAccountKVAsOf(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	return rawdb.ReadStateKVAsOf(s.accountKVIndex(), owner, obj.accountKVGeneration, domain, key, targetBlock, headBlock)
}

// IterateAccountKVAsOf reconstructs a domain prefix at the end of targetBlock
// using the Phase-5 domain change-set history. Like GetAccountKVAsOf, this
// first bridge resolves history within the account's current KV generation.
func (s *StateDB) IterateAccountKVAsOf(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, targetBlock, headBlock uint64, fn func(key, value []byte) (bool, error)) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil
	}
	return rawdb.IterateStateKVAsOfPrefix(s.accountKVIndex(), owner, obj.accountKVGeneration, domain, prefix, targetBlock, headBlock, fn)
}

// GetAccountKVBatch resolves many keys in one owner/domain, returning
// name->value for present keys. The dirty overlay is consulted first per key,
// matching GetAccountKV. Live block application may opt into the flat latest
// index; historical readers fall back to one opened KV trie plus N trie Gets.
func (s *StateDB) GetAccountKVBatch(owner tcommon.Address, domain kvdomains.KVDomain, keys [][]byte) (map[string][]byte, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return out, nil
	}
	if s.accountKVLatestReadEnabled() {
		index := s.accountKVIndex()
		for _, key := range keys {
			comp := kvCompositeKey(domain, key)
			if e, ok := obj.kvDirty[string(comp)]; ok {
				if !e.deleted {
					out[string(key)] = append([]byte{}, e.val...)
				}
				continue
			}
			value, ok, err := rawdb.ReadStateKVLatest(index, owner, obj.accountKVGeneration, domain, key)
			if err != nil {
				return nil, err
			}
			if ok {
				out[string(key)] = value
			}
		}
		return out, nil
	}
	tr, err := s.accountKVTrie(obj)
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
	return s.setAccountKV(owner, domain, key, value, true)
}

// SetAccountKVFinal stages a block-final generic-KV write without appending a
// transaction-snapshot journal entry. Use only after transaction execution is
// complete; ordinary actuator/VM paths must keep using SetAccountKV.
func (s *StateDB) SetAccountKVFinal(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte) error {
	return s.setAccountKV(owner, domain, key, value, false)
}

func (s *StateDB) setAccountKV(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte, journal bool) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.GetOrCreateAccount(owner)
	comp := kvCompositeKey(domain, key)
	mk := string(comp)
	var (
		prevValue  []byte
		prevExists bool
		prevLoaded bool
	)
	_, dirty := obj.kvDirty[mk]
	if !dirty && s.accountKVLatestReadEnabled() {
		current, exists, err := rawdb.ReadStateKVLatest(s.accountKVIndex(), owner, obj.accountKVGeneration, domain, key)
		if err != nil {
			return err
		}
		prevValue = current
		prevExists = exists
		prevLoaded = true
	}
	return s.setAccountKVWithPrev(owner, domain, key, value, journal, prevValue, prevExists, prevLoaded)
}

func (s *StateDB) setAccountKVFinalWithPrev(owner tcommon.Address, domain kvdomains.KVDomain, key, prev, value []byte, prevExists bool) error {
	return s.setAccountKVWithPrev(owner, domain, key, value, false, prev, prevExists, true)
}

// setAccountKVFinalNoRead stages block-final bookkeeping writes whose caller
// already knows the value changed. History-enabled commits still read the
// previous value later when writing the domain change-set.
func (s *StateDB) setAccountKVFinalNoRead(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte) error {
	return s.setAccountKVWithPrev(owner, domain, key, value, false, nil, false, false)
}

func (s *StateDB) setAccountKVWithPrev(owner tcommon.Address, domain kvdomains.KVDomain, key, value []byte, journal bool, prevValue []byte, prevExists, prevLoaded bool) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.GetOrCreateAccount(owner)
	comp := kvCompositeKey(domain, key)
	mk := string(comp)
	prevDirty, dirty := obj.kvDirty[mk]
	if dirty {
		if !prevDirty.deleted && bytes.Equal(prevDirty.val, value) {
			return nil
		}
	} else if prevLoaded && prevExists && bytes.Equal(prevValue, value) {
		return nil
	}
	if journal {
		s.journal.append(kvChange{address: owner, mapKey: mk, hadEntry: dirty, prevEntry: prevDirty})
	}
	entry := newKVEntry(comp, value, false)
	if dirty {
		entry.inheritPrev(prevDirty)
	} else if prevLoaded {
		entry.setPrev(prevValue, prevExists)
	}
	obj.kvDirty[mk] = entry
	obj.markDirty()
	return nil
}

func (s *StateDB) stageAccountKVCommit(obj *stateObject, domain kvdomains.KVDomain, key, value []byte, deleted bool) (bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	if obj == nil {
		return false, nil
	}
	comp := kvCompositeKey(domain, key)
	mk := string(comp)
	prevDirty, dirty := obj.kvDirty[mk]
	entry := newKVEntry(comp, value, deleted)
	if dirty {
		entry.inheritPrev(prevDirty)
	} else if s.accountKVLatestReadEnabled() {
		current, exists, err := rawdb.ReadStateKVLatest(s.accountKVIndex(), obj.address, obj.accountKVGeneration, domain, key)
		if err != nil {
			return false, err
		}
		entry.setPrev(current, exists)
		if noop, known := entry.latestNoop(); known && noop {
			return false, nil
		}
	}
	obj.kvDirty[mk] = entry
	return true, nil
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
	comp := kvCompositeKey(domain, key)
	mk := string(comp)
	prevDirty, dirty := obj.kvDirty[mk]
	var (
		prevValue  []byte
		prevExists bool
		prevLoaded bool
	)
	if !dirty && s.accountKVLatestReadEnabled() {
		current, exists, err := rawdb.ReadStateKVLatest(s.accountKVIndex(), owner, obj.accountKVGeneration, domain, key)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		prevValue = current
		prevExists = true
		prevLoaded = true
	}
	s.journal.append(kvChange{address: owner, mapKey: mk, hadEntry: dirty, prevEntry: prevDirty})
	entry := newKVEntry(comp, nil, true)
	if dirty {
		entry.inheritPrev(prevDirty)
	} else if prevLoaded {
		entry.setPrev(prevValue, prevExists)
	}
	obj.kvDirty[mk] = entry
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
		address:             owner,
		prevRoot:            obj.accountKVRoot,
		prevGeneration:      obj.accountKVGeneration,
		prevGenerationDirty: obj.accountKVGenerationDirty,
		prevDirty:           prevDirty,
	})
	obj.kvDirty = make(map[string]kvEntry)
	obj.accountKVRoot = EmptyKVRoot
	obj.accountKVGeneration++
	obj.accountKVGenerationDirty = true
	obj.markDirty()
	s.invalidateAccountKVTrie(owner)
	return nil
}

func (s *StateDB) prepareAccountKVCommitPlan(obj *stateObject) (*accountKVCommitPlan, error) {
	plan := &accountKVCommitPlan{items: make([]kvCommitItem, 0, len(obj.kvDirty))}
	for mk, e := range obj.kvDirty {
		composite := []byte(mk)
		domain, logicalKey, ok := splitKVCompositeKeyView(composite)
		if !ok {
			return nil, fmt.Errorf("account kv: malformed composite key for %s", obj.address.Hex())
		}
		if noop, known := e.latestNoop(); known && noop {
			plan.noopItems++
			if idx, ok := kvDomainStatIndex(domain); ok {
				plan.noopByDomain[idx]++
			}
			continue
		}
		if e.trieKey == (ethcommon.Hash{}) {
			e.trieKey = kvTrieHash([]byte(mk))
			obj.kvDirty[mk] = e
		}
		plan.items = append(plan.items, kvCommitItem{
			logicalKey: logicalKey,
			entry:      e,
			domain:     domain,
		})
	}
	sort.Slice(plan.items, func(i, j int) bool {
		return bytes.Compare(plan.items[i].entry.trieKey[:], plan.items[j].entry.trieKey[:]) < 0
	})
	return plan, nil
}

// computeAccountKVCommitment applies obj's dirty KV overlay to an isolated
// per-account KV trie and returns the new root plus dirty node set. It
// deliberately bypasses StateDB.accountKVTries so this CPU-heavy commitment
// step can be workerized without mutating shared trie caches.
func (s *StateDB) computeAccountKVCommitment(obj *stateObject, plan *accountKVCommitPlan) (accountKVCommitResult, error) {
	result := accountKVCommitResult{root: obj.accountKVRoot}
	if plan == nil || len(plan.items) == 0 {
		return result, nil
	}
	tr, err := s.db.OpenTrie(ethcommon.Hash(obj.accountKVRoot))
	if err != nil {
		return accountKVCommitResult{}, err
	}
	for _, item := range plan.items {
		e := item.entry
		tk := e.trieKey.Bytes()
		if e.deleted {
			if err := tr.Delete(tk); err != nil {
				return accountKVCommitResult{}, err
			}
			continue
		}
		if err := tr.Update(tk, e.encodedValue()); err != nil {
			return accountKVCommitResult{}, err
		}
	}
	root, nodes := tr.Commit(false)
	result.root = tcommon.Hash(root)
	result.trieChanged = true
	result.nodes = nodes
	return result, nil
}

func (s *StateDB) accountKVCommitPlans(plans []*accountCommitPlan) []*accountCommitPlan {
	out := make([]*accountCommitPlan, 0, len(plans))
	for _, plan := range plans {
		if plan.deleteAccount || !plan.hadKVDirty {
			continue
		}
		if plan.kvPlan == nil || len(plan.kvPlan.items) == 0 {
			continue
		}
		out = append(out, plan)
	}
	return out
}

func accountKVCommitWorkers(tasks int) int {
	if tasks <= 1 {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > 8 {
		workers = 8
	}
	if workers > tasks {
		workers = tasks
	}
	return workers
}

func (s *StateDB) computeAccountKVCommitments(plans []*accountCommitPlan) ([]accountKVCommitResult, error) {
	results := make([]accountKVCommitResult, len(plans))
	if len(plans) == 0 {
		return results, nil
	}
	if len(plans) == 1 {
		result, err := s.computeAccountKVCommitment(plans[0].obj, plans[0].kvPlan)
		if err != nil {
			return nil, err
		}
		results[0] = result
		return results, nil
	}

	workers := accountKVCommitWorkers(len(plans))
	jobs := make(chan int)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				result, err := s.computeAccountKVCommitment(plans[idx].obj, plans[idx].kvPlan)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				results[idx] = result
			}
		}()
	}
	for idx := range plans {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func (s *StateDB) commitAccountKVPlans(plans []*accountCommitPlan, nodeWriter *trieNodeBatchWriter, stats *CommitStats) error {
	kvPlans := s.accountKVCommitPlans(plans)
	if stats != nil {
		stats.KVAccounts += len(kvPlans)
		for _, plan := range kvPlans {
			stats.KVItems += len(plan.kvPlan.items)
		}
	}

	start := time.Now()
	results, err := s.computeAccountKVCommitments(kvPlans)
	if stats != nil {
		stats.KVCompute += time.Since(start)
	}
	if err != nil {
		return err
	}

	start = time.Now()
	for i, plan := range kvPlans {
		result := results[i]
		if err := nodeWriter.writeNodeSet(result.nodes); err != nil {
			return err
		}
		plan.kvTrieChanged = result.trieChanged
		plan.obj.accountKVRoot = result.root
		s.invalidateAccountKVTrie(plan.addr)
	}
	if stats != nil {
		stats.KVNodeWrite += time.Since(start)
	}
	return nil
}

func (s *StateDB) commitAccountKVLatest(obj *stateObject, plan *accountKVCommitPlan, index accountKVIndexStore, latest *accountKVLatestBatch) (bool, error) {
	if plan == nil || len(plan.items) == 0 {
		return false, nil
	}
	wrote := false
	for _, item := range plan.items {
		entry := item.entry
		changed, err := s.writeDomainChange(index, obj, item.domain, item.logicalKey, entry)
		if err != nil {
			return false, err
		}
		if !changed {
			continue
		}
		wrote = true
		if entry.deleted {
			if err := latest.delete(obj.address, obj.accountKVGeneration, item.domain, item.logicalKey); err != nil {
				return false, err
			}
			continue
		}
		if err := latest.put(obj.address, obj.accountKVGeneration, item.domain, item.logicalKey, entry.encodedValue()); err != nil {
			return false, err
		}
	}
	return wrote, nil
}

func (s *StateDB) writeDomainChange(index accountKVIndexStore, obj *stateObject, domain kvdomains.KVDomain, logicalKey []byte, entry kvEntry) (bool, error) {
	prev := entry.prev
	prevExists := entry.prevExists
	if !entry.prevLoaded {
		if !s.changeSet.enabled {
			return true, nil
		}
		var err error
		prev, prevExists, err = rawdb.ReadStateKVLatest(index, obj.address, obj.accountKVGeneration, domain, logicalKey)
		if err != nil {
			return false, err
		}
	}
	nextExists := !entry.deleted
	nextValue := entry.val
	if prevExists == nextExists && (!nextExists || bytes.Equal(prev, nextValue)) {
		return false, nil
	}
	if !s.changeSet.enabled {
		return true, nil
	}
	var next []byte
	if nextExists {
		next = append([]byte(nil), nextValue...)
	}
	s.changeSet.seq++
	return true, rawdb.WriteStateDomainChange(s.changeSet.writer, &rawdb.StateDomainChange{
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
	if err := rawdb.WriteStateKVGeneration(s.accountKVIndex(), obj.address, obj.accountKVGeneration); err != nil {
		return err
	}
	obj.accountKVGenerationDirty = false
	return nil
}
