package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// kvPresencePrefix wraps persisted KV values so an empty-but-present value
// stays distinct from an absent key.
const kvPresencePrefix = 0x01

// kvEntry is one pending account-KV write in the dirty overlay. deleted=true is
// a tombstone; deleted=false means val is present (val may be empty but != nil).
type kvEntry struct {
	val        []byte
	wrapped    []byte
	prev       []byte
	deleted    bool
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
	index             accountKVIndexStore
	writer            ethdb.KeyValueWriter
	latestStore       accountKVPhysicalLatestStore
	batch             ethdb.Batch
	ops               int
	pending           map[string]accountKVLatestPending
	accountPending    map[string]accountLatestPending
	generationPending map[string]kvGenerationPending
	generation        func(tcommon.Address) (uint64, error)
	changeSet         *domainChangeSetCapture
	record            func(rawdb.StateCommitmentUpdate)
}

type accountKVLatestPending struct {
	owner      tcommon.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        []byte
	value      []byte
	deleted    bool
}

type accountLatestPending struct {
	value   []byte
	deleted bool
}

type kvGenerationPending struct {
	generation uint64
	deleted    bool
}

type accountKVIndexStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

type accountKVLayerBatch interface {
	WriteUpTo(cutoff uint64) (remaining int, err error)
	WriteCommitted(dropStale bool) (remaining int, err error)
}

type accountKVLatestGenerationReader interface {
	KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
}

type accountKVLatestGenerationIterator interface {
	KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error
}

type accountKVPhysicalLatestStore interface {
	ReadAccountLatest(owner tcommon.Address) ([]byte, bool, error)
	WriteAccountLatest(owner tcommon.Address, value []byte) error
	DeleteAccountLatest(owner tcommon.Address) error
	ReadKVGeneration(owner tcommon.Address) (uint64, bool, error)
	WriteKVGeneration(owner tcommon.Address, generation uint64) error
	DeleteKVGeneration(owner tcommon.Address) error
	ReadKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error)
	WriteKVLatestEncoded(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, encodedValue []byte) error
	DeleteKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error
	IterateKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error
}

func newAccountKVLatestBatch(index accountKVIndexStore, record func(rawdb.StateCommitmentUpdate)) *accountKVLatestBatch {
	w := &accountKVLatestBatch{index: index, writer: index, record: record}
	if batcher, ok := index.(ethdb.Batcher); ok {
		w.batch = batcher.NewBatch()
		w.writer = w.batch
	}
	w.latestStore = newRawdbAccountKVPhysicalLatestStore(index, w.writer)
	return w
}

func newAccountKVLatestDomainBatch(index accountKVIndexStore, generation func(tcommon.Address) (uint64, error), changeSet *domainChangeSetCapture, record func(rawdb.StateCommitmentUpdate)) *accountKVLatestBatch {
	w := newAccountKVLatestBatch(index, record)
	w.generation = generation
	w.changeSet = changeSet
	return w
}

var _ statedomains.Writer = (*accountKVLatestBatch)(nil)

func (w *accountKVLatestBatch) DomainPut(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey, value []byte) error {
	generation, err := w.resolveGeneration(owner)
	if err != nil {
		return err
	}
	if err := w.writeDomainChange(owner, generation, domain, logicalKey, true, value); err != nil {
		return err
	}
	return w.put(owner, generation, domain, logicalKey, rawdb.EncodeStateKVLatestValue(value))
}

func (w *accountKVLatestBatch) DomainDel(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey []byte) error {
	generation, err := w.resolveGeneration(owner)
	if err != nil {
		return err
	}
	if err := w.writeDomainChange(owner, generation, domain, logicalKey, false, nil); err != nil {
		return err
	}
	return w.delete(owner, generation, domain, logicalKey)
}

func (w *accountKVLatestBatch) DomainDelPrefix(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte) error {
	if w == nil || w.index == nil {
		return fmt.Errorf("account kv latest domain writer: nil index")
	}
	generation, err := w.resolveGeneration(owner)
	if err != nil {
		return err
	}
	var keys [][]byte
	if err := w.iterateLatestPrefix(owner, generation, domain, prefix, func(key, _ []byte) (bool, error) {
		keys = append(keys, append([]byte(nil), key...))
		return true, nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := w.writeDomainChange(owner, generation, domain, key, false, nil); err != nil {
			return err
		}
		if err := w.delete(owner, generation, domain, key); err != nil {
			return err
		}
	}
	return nil
}

func (w *accountKVLatestBatch) resolveGeneration(owner tcommon.Address) (uint64, error) {
	if w == nil || w.generation == nil {
		return 0, fmt.Errorf("account kv latest domain writer: missing generation resolver for %s", owner.Hex())
	}
	return w.generation(owner)
}

func (w *accountKVLatestBatch) writeDomainChange(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte, nextExists bool, nextValue []byte) error {
	if w == nil || w.changeSet == nil || !w.changeSet.enabled || !w.changeSet.captureAtCommit {
		return nil
	}
	prev, prevExists, err := w.readLatest(owner, generation, domain, logicalKey)
	if err != nil {
		return err
	}
	if prevExists == nextExists && (!nextExists || bytes.Equal(prev, nextValue)) {
		return nil
	}
	var next []byte
	if nextExists {
		next = append([]byte(nil), nextValue...)
	}
	return w.changeSet.publishCommitDomainChange(&rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: generation,
		Domain:     domain,
		Key:        append([]byte(nil), logicalKey...),
		PrevExists: prevExists,
		Prev:       prev,
		NextExists: nextExists,
		Next:       next,
	})
}

func (w *accountKVLatestBatch) put(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, encodedValue []byte) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.WriteKVLatestEncoded(owner, generation, domain, logicalKey, encodedValue); err != nil {
		return err
	}
	value, err := rawdb.DecodeStateKVLatestValue(encodedValue)
	if err != nil {
		return err
	}
	w.rememberPut(owner, generation, domain, logicalKey, value)
	if w.record != nil {
		w.record(rawdb.NewStateCommitmentPut(
			rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logicalKey),
			encodedValue,
		))
	}
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) delete(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.DeleteKVLatest(owner, generation, domain, logicalKey); err != nil {
		return err
	}
	w.rememberDelete(owner, generation, domain, logicalKey)
	if w.record != nil {
		w.record(rawdb.NewStateCommitmentDelete(rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logicalKey)))
	}
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) maybeFlush() error {
	if w.batch == nil {
		return nil
	}
	w.ops++
	if _, ok := w.batch.(accountKVLayerBatch); ok {
		return nil
	}
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
	if w.batch == nil {
		w.clearPending()
		return nil
	}
	if w.ops == 0 {
		return nil
	}
	if err := w.batch.Write(); err != nil {
		return err
	}
	w.batch.Reset()
	w.ops = 0
	w.clearPending()
	return nil
}

func (w *accountKVLatestBatch) flushUpTo(cutoff uint64) error {
	if w == nil || w.batch == nil || w.ops == 0 {
		return nil
	}
	layerBatch, ok := w.batch.(accountKVLayerBatch)
	if !ok {
		return w.flush()
	}
	remaining, err := layerBatch.WriteUpTo(cutoff)
	if err != nil {
		return err
	}
	w.ops = remaining
	if remaining == 0 {
		w.clearPending()
	}
	return nil
}

func (w *accountKVLatestBatch) flushCommitted(dropStale bool) error {
	if w == nil || w.batch == nil || w.ops == 0 {
		return nil
	}
	layerBatch, ok := w.batch.(accountKVLayerBatch)
	if !ok {
		return w.flush()
	}
	remaining, err := layerBatch.WriteCommitted(dropStale)
	if err != nil {
		return err
	}
	w.ops = remaining
	if remaining == 0 {
		w.clearPending()
	}
	return nil
}

func (w *accountKVLatestBatch) reset() {
	if w == nil {
		return
	}
	if w.batch != nil {
		w.batch.Reset()
	}
	w.ops = 0
	w.clearPending()
}

func (w *accountKVLatestBatch) clearPending() {
	if w == nil {
		return
	}
	w.pending = nil
	w.accountPending = nil
	w.generationPending = nil
}

func (w *accountKVLatestBatch) writeAccountLatest(owner tcommon.Address, value []byte) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.WriteAccountLatest(owner, value); err != nil {
		return err
	}
	w.rememberAccountLatestPut(owner, value)
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) deleteAccountLatest(owner tcommon.Address) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.DeleteAccountLatest(owner); err != nil {
		return err
	}
	w.rememberAccountLatestDelete(owner)
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) readAccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if w != nil {
		if pending, ok := w.accountPending[string(rawdb.StateAccountLatestCommitmentKey(owner))]; ok {
			if pending.deleted {
				return nil, false, nil
			}
			return append([]byte(nil), pending.value...), true, nil
		}
	}
	if w == nil || w.latestStore == nil {
		return nil, false, nil
	}
	return w.latestStore.ReadAccountLatest(owner)
}

func (w *accountKVLatestBatch) writeKVGeneration(owner tcommon.Address, generation uint64) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.WriteKVGeneration(owner, generation); err != nil {
		return err
	}
	w.rememberKVGenerationPut(owner, generation)
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) deleteKVGeneration(owner tcommon.Address) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.DeleteKVGeneration(owner); err != nil {
		return err
	}
	w.rememberKVGenerationDelete(owner)
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) readKVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if w != nil {
		if pending, ok := w.generationPending[string(rawdb.StateKVGenerationCommitmentKey(owner))]; ok {
			if pending.deleted {
				return 0, false, nil
			}
			return pending.generation, true, nil
		}
	}
	if w == nil || w.latestStore == nil {
		return 0, false, nil
	}
	return w.latestStore.ReadKVGeneration(owner)
}

func (w *accountKVLatestBatch) readLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	if w != nil {
		if pending, ok := w.pending[string(accountKVLatestPendingKey(owner, generation, domain, logicalKey))]; ok {
			if pending.deleted {
				return nil, false, nil
			}
			return append([]byte(nil), pending.value...), true, nil
		}
	}
	if w == nil || w.latestStore == nil {
		return nil, false, nil
	}
	return w.latestStore.ReadKVLatest(owner, generation, domain, logicalKey)
}

func (w *accountKVLatestBatch) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	return w.readAccountLatest(owner)
}

func (w *accountKVLatestBatch) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	return w.readKVGeneration(owner)
}

func (w *accountKVLatestBatch) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	return w.readLatest(owner, generation, domain, key)
}

func (w *accountKVLatestBatch) iterateLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(logicalKey, value []byte) (bool, error)) error {
	if w == nil || w.latestStore == nil {
		return nil
	}
	entries := make(map[string][]byte)
	if err := w.latestStore.IterateKVLatest(owner, generation, domain, prefix, func(key, value []byte) (bool, error) {
		entries[string(key)] = append([]byte(nil), value...)
		return true, nil
	}); err != nil {
		return err
	}
	for _, pending := range w.pending {
		if pending.owner != owner || pending.generation != generation || pending.domain != domain || !bytes.HasPrefix(pending.key, prefix) {
			continue
		}
		mapKey := string(pending.key)
		if pending.deleted {
			delete(entries, mapKey)
			continue
		}
		entries[mapKey] = append([]byte(nil), pending.value...)
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

func (w *accountKVLatestBatch) rememberPut(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, value []byte) {
	if w == nil {
		return
	}
	if w.pending == nil {
		w.pending = make(map[string]accountKVLatestPending)
	}
	w.pending[string(accountKVLatestPendingKey(owner, generation, domain, logicalKey))] = accountKVLatestPending{
		owner:      owner,
		generation: generation,
		domain:     domain,
		key:        append([]byte(nil), logicalKey...),
		value:      append([]byte(nil), value...),
	}
}

func (w *accountKVLatestBatch) rememberAccountLatestPut(owner tcommon.Address, value []byte) {
	if w == nil {
		return
	}
	if w.accountPending == nil {
		w.accountPending = make(map[string]accountLatestPending)
	}
	w.accountPending[string(rawdb.StateAccountLatestCommitmentKey(owner))] = accountLatestPending{
		value: append([]byte(nil), value...),
	}
}

func (w *accountKVLatestBatch) rememberAccountLatestDelete(owner tcommon.Address) {
	if w == nil {
		return
	}
	if w.accountPending == nil {
		w.accountPending = make(map[string]accountLatestPending)
	}
	w.accountPending[string(rawdb.StateAccountLatestCommitmentKey(owner))] = accountLatestPending{deleted: true}
}

func (w *accountKVLatestBatch) rememberKVGenerationPut(owner tcommon.Address, generation uint64) {
	if w == nil {
		return
	}
	if w.generationPending == nil {
		w.generationPending = make(map[string]kvGenerationPending)
	}
	w.generationPending[string(rawdb.StateKVGenerationCommitmentKey(owner))] = kvGenerationPending{generation: generation}
}

func (w *accountKVLatestBatch) rememberKVGenerationDelete(owner tcommon.Address) {
	if w == nil {
		return
	}
	if w.generationPending == nil {
		w.generationPending = make(map[string]kvGenerationPending)
	}
	w.generationPending[string(rawdb.StateKVGenerationCommitmentKey(owner))] = kvGenerationPending{deleted: true}
}

func (w *accountKVLatestBatch) rememberDelete(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) {
	if w == nil {
		return
	}
	if w.pending == nil {
		w.pending = make(map[string]accountKVLatestPending)
	}
	w.pending[string(accountKVLatestPendingKey(owner, generation, domain, logicalKey))] = accountKVLatestPending{
		owner:      owner,
		generation: generation,
		domain:     domain,
		key:        append([]byte(nil), logicalKey...),
		deleted:    true,
	}
}

func accountKVLatestPendingKey(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) []byte {
	return rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logicalKey)
}

// kvCompositeKey is the pre-hash logical key: domain (big-endian u16) || key.
func kvCompositeKey(domain kvdomains.KVDomain, key []byte) []byte {
	out := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(out, uint16(domain))
	copy(out[2:], key)
	return out
}

func newKVEntry(composite, value []byte, deleted bool) kvEntry {
	e := kvEntry{deleted: deleted}
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

func wrapKVValue(value []byte) []byte {
	wrapped := make([]byte, 1+len(value))
	wrapped[0] = kvPresencePrefix
	copy(wrapped[1:], value)
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

func (s *StateDB) accountKVPhysicalLatestStore() accountKVPhysicalLatestStore {
	index := s.accountKVIndex()
	return newRawdbAccountKVPhysicalLatestStore(index, index)
}

func (s *StateDB) setAccountKVLatestView(reader statedomains.LatestReader, iterator statedomains.Iterator) {
	if s == nil {
		return
	}
	s.accountKVLatestReader = reader
	s.accountKVLatestIterator = iterator
}

func (s *StateDB) readAccountKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if s != nil && s.accountKVLatestReader != nil {
		if reader, ok := s.accountKVLatestReader.(accountKVLatestGenerationReader); ok {
			return reader.KVLatest(owner, generation, domain, key)
		}
		return s.accountKVLatestReader.GetLatest(owner, domain, key)
	}
	return s.accountKVPhysicalLatestStore().ReadKVLatest(owner, generation, domain, key)
}

func (s *StateDB) iterateAccountKVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if s != nil && s.accountKVLatestIterator != nil {
		if iterator, ok := s.accountKVLatestIterator.(accountKVLatestGenerationIterator); ok {
			return iterator.KVLatestPrefix(owner, generation, domain, prefix, fn)
		}
		return s.accountKVLatestIterator.DomainIterate(owner, domain, prefix, fn)
	}
	return s.accountKVPhysicalLatestStore().IterateKVLatest(owner, generation, domain, prefix, fn)
}

func (s *StateDB) readStateAccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if s != nil && s.flatLatestReader != nil {
		return s.flatLatestReader.AccountLatest(owner)
	}
	return s.accountKVPhysicalLatestStore().ReadAccountLatest(owner)
}

func (s *StateDB) readStateKVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if s != nil && s.flatLatestReader != nil {
		return s.flatLatestReader.KVGeneration(owner)
	}
	return s.accountKVPhysicalLatestStore().ReadKVGeneration(owner)
}

func (s *StateDB) nextAccountKVGeneration(owner tcommon.Address, prev *stateObject) uint64 {
	if prev != nil {
		return prev.accountKVGeneration + 1
	}
	gen, ok, err := s.readStateKVGeneration(owner)
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
	return s.readAccountKVLatest(owner, obj.accountKVGeneration, domain, key)
}

// GetAccountKVAsOf reconstructs owner's domain key at the end of targetBlock
// using the flat-domain change-set history. Blocks are mapped to their stored
// end txNum when a StateTxRange row is available.
func (s *StateDB) GetAccountKVAsOf(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	targetTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(s.accountKVIndex(), targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(s.accountKVIndex(), headBlock)
	if err != nil {
		return nil, false, err
	}
	return s.GetAccountKVAsOfTxNum(owner, domain, key, targetTxNum, headTxNum)
}

// GetAccountKVAsOfTxNum reconstructs owner's domain key at the end of
// targetTxNum. This is the txNum-native archive read path used by
// Erigon-style history.
func (s *StateDB) GetAccountKVAsOfTxNum(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	cfg, err := stateDomainHistoryConfig()
	if err != nil {
		return nil, false, err
	}
	if cfg.ReadHotAccountKVAsOf == nil {
		return nil, false, ErrStateDomainHistoryUnavailable
	}
	return cfg.ReadHotAccountKVAsOf(s.accountKVIndex(), owner, domain, key, targetTxNum, headTxNum)
}

// IterateAccountKVAsOf reconstructs a domain prefix at the end of targetBlock
// using the flat-domain change-set history.
func (s *StateDB) IterateAccountKVAsOf(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, targetBlock, headBlock uint64, fn func(key, value []byte) (bool, error)) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil
	}
	targetTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(s.accountKVIndex(), targetBlock)
	if err != nil {
		return err
	}
	headTxNum, err := snapshots.StateDomainHistoryTxNumAtBlockEnd(s.accountKVIndex(), headBlock)
	if err != nil {
		return err
	}
	return s.IterateAccountKVAsOfTxNum(owner, domain, prefix, targetTxNum, headTxNum, fn)
}

// IterateAccountKVAsOfTxNum reconstructs a domain prefix at the end of
// targetTxNum using txNum-native flat-domain history.
func (s *StateDB) IterateAccountKVAsOfTxNum(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, targetTxNum, headTxNum uint64, fn func(key, value []byte) (bool, error)) error {
	if !kvdomains.IsRegistered(domain) {
		return fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil
	}
	cfg, err := stateDomainHistoryConfig()
	if err != nil {
		return err
	}
	if cfg.IterateHotAccountKVPrefixAsOf == nil {
		return ErrStateDomainHistoryUnavailable
	}
	return cfg.IterateHotAccountKVPrefixAsOf(s.accountKVIndex(), owner, domain, prefix, targetTxNum, headTxNum, fn)
}

// GetAccountKVBatch resolves many keys in one owner/domain, returning
// name->value for present keys. The dirty overlay is consulted first per key,
// matching GetAccountKV.
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
	for _, key := range keys {
		comp := kvCompositeKey(domain, key)
		if e, ok := obj.kvDirty[string(comp)]; ok {
			if !e.deleted {
				out[string(key)] = append([]byte{}, e.val...)
			}
			continue
		}
		value, ok, err := s.readAccountKVLatest(owner, obj.accountKVGeneration, domain, key)
		if err != nil {
			return nil, err
		}
		if ok {
			out[string(key)] = value
		}
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
	if err := s.iterateAccountKVLatest(owner, obj.accountKVGeneration, domain, prefix, func(key, value []byte) (bool, error) {
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
	if !dirty {
		current, exists, err := s.readAccountKVLatest(owner, obj.accountKVGeneration, domain, key)
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
	} else if s.changeSet.enabled && !s.changeSet.captureAtCommit {
		s.domainChangeNoJournal = append(s.domainChangeNoJournal, kvChange{address: owner, mapKey: mk, hadEntry: dirty, prevEntry: prevDirty})
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
	} else {
		current, exists, err := s.readAccountKVLatest(obj.address, obj.accountKVGeneration, domain, key)
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
	if !dirty {
		current, exists, err := s.readAccountKVLatest(owner, obj.accountKVGeneration, domain, key)
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
	s.journalAccount(owner, obj)
	prevDirty := make(map[string]kvEntry, len(obj.kvDirty))
	for k, v := range obj.kvDirty {
		prevDirty[k] = v
	}
	prevGenerationExists := obj.accountKVGenerationDirty
	if !prevGenerationExists {
		_, prevGenerationExists, _ = s.readStateKVGeneration(owner)
	}
	s.journal.append(kvResetChange{
		address:              owner,
		prevRoot:             obj.accountKVRoot,
		prevGeneration:       obj.accountKVGeneration,
		prevGenerationExists: prevGenerationExists,
		prevGenerationDirty:  obj.accountKVGenerationDirty,
		prevDirty:            prevDirty,
	})
	obj.kvDirty = make(map[string]kvEntry)
	obj.accountKVRoot = EmptyKVRoot
	obj.accountKVGeneration++
	obj.accountKVGenerationDirty = true
	obj.markDirty()
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
		plan.items = append(plan.items, kvCommitItem{
			logicalKey: logicalKey,
			entry:      e,
			domain:     domain,
		})
	}
	sort.Slice(plan.items, func(i, j int) bool {
		if plan.items[i].domain != plan.items[j].domain {
			return plan.items[i].domain < plan.items[j].domain
		}
		return bytes.Compare(plan.items[i].logicalKey, plan.items[j].logicalKey) < 0
	})
	return plan, nil
}

func (s *StateDB) commitAccountKVLatest(obj *stateObject, plan *accountKVCommitPlan, latest statedomains.Writer) (bool, error) {
	if plan == nil || len(plan.items) == 0 {
		return false, nil
	}
	wrote := false
	for _, item := range plan.items {
		entry := item.entry
		wrote = true
		if entry.deleted {
			if err := latest.DomainDel(obj.address, item.domain, item.logicalKey); err != nil {
				return false, err
			}
			continue
		}
		if err := latest.DomainPut(obj.address, item.domain, item.logicalKey, entry.val); err != nil {
			return false, err
		}
	}
	return wrote, nil
}

func (s *StateDB) writeAccountKVGeneration(obj *stateObject, commitment *DomainCommitmentState, latestWriter *accountKVLatestBatch) error {
	if s.changeSet.enabled && s.changeSet.captureAtCommit {
		prev, prevExists, err := s.readStateKVGeneration(obj.address)
		if err != nil {
			return err
		}
		if !prevExists || prev != obj.accountKVGeneration {
			var prevValue []byte
			if prevExists {
				prevValue = rawdb.EncodeStateKVGenerationValue(prev)
			}
			if err := s.changeSet.publishCommitDomainChange(&rawdb.StateDomainChange{
				FlatDomain: rawdb.StateFlatDomainKVGeneration,
				Owner:      obj.address,
				PrevExists: prevExists,
				Prev:       prevValue,
				NextExists: true,
				Next:       rawdb.EncodeStateKVGenerationValue(obj.accountKVGeneration),
			}); err != nil {
				return err
			}
		}
	}
	if latestWriter == nil {
		return fmt.Errorf("account kv generation writer unavailable")
	}
	if err := latestWriter.writeKVGeneration(obj.address, obj.accountKVGeneration); err != nil {
		return err
	}
	commitment.recordKVGenerationTouch(obj.address)
	obj.accountKVGenerationDirty = false
	return nil
}
