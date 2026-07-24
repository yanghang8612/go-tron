package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

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
	pooledItems  *[]kvCommitItem
	pooledPlan   bool
}

// Account-KV commit plans live only until the current block's flat writes have
// been flushed. Reuse their item backing arrays across blocks to avoid making
// every dirty account allocate a short-lived sort/work slice. The cap prevents
// one pathological contract from pinning a very large array in sync.Pool.
const maxPooledKVCommitItems = 1024

var accountKVCommitItemsPool = sync.Pool{
	New: func() any {
		items := make([]kvCommitItem, 0, 8)
		return &items
	},
}

var accountKVCommitPlanPool = sync.Pool{
	New: func() any { return new(accountKVCommitPlan) },
}

func borrowAccountKVCommitItems(size int) ([]kvCommitItem, *[]kvCommitItem) {
	if size > maxPooledKVCommitItems {
		return make([]kvCommitItem, 0, size), nil
	}
	itemsPtr := accountKVCommitItemsPool.Get().(*[]kvCommitItem)
	if cap(*itemsPtr) < size {
		*itemsPtr = make([]kvCommitItem, 0, size)
	}
	return (*itemsPtr)[:0], itemsPtr
}

func releaseAccountKVCommitPlan(plan *accountKVCommitPlan) {
	if plan == nil {
		return
	}
	itemsPtr := plan.pooledItems
	if itemsPtr != nil {
		clear(plan.items)
		*itemsPtr = plan.items[:0]
		accountKVCommitItemsPool.Put(itemsPtr)
	}
	pooledPlan := plan.pooledPlan
	*plan = accountKVCommitPlan{}
	if pooledPlan {
		accountKVCommitPlanPool.Put(plan)
	}
}

func releaseAccountKVCommitPlans(plans []*accountCommitPlan) {
	for _, plan := range plans {
		if plan != nil {
			releaseAccountKVCommitPlan(plan.kvPlan)
		}
	}
}

type accountKVLatestBatch struct {
	index             accountKVIndexStore
	writer            ethdb.KeyValueWriter
	latestStore       accountKVPhysicalLatestStore
	batch             ethdb.Batch
	ops               int
	pending           map[accountKVLatestPendingMapKey]accountKVLatestPending
	accountPending    map[tcommon.AccountID]accountLatestPending
	generationPending map[tcommon.AccountID]kvGenerationPending
	generation        func(tcommon.Address) (uint64, error)
	changeSet         *domainChangeSetCapture
	record            func(rawdb.StateCommitmentUpdate)
	// commitBlock is the block number whose commit is currently writing through
	// this batch. Every overlay put made while it is set is tagged with it, so a
	// later partial flush can prune the entries whose puts are now durable in the
	// buffer's committed layers (read-your-writes overlay pruning). It SHOULD equal
	// the buffer active-layer number that the bufferBatch op binds to — both are
	// block.Number(), and BeginBlock(block.Number()) precedes the block's commit
	// puts. Threaded per block via CommitOptions.BlockNumber.
	commitBlock uint64
	// deepAsync is set on the deep async-commit pipeline (GTRON_ASYNC_COMMIT_DEPTH
	// > 2). On that path prunePending verifies each candidate is actually durable
	// in the underlying store before dropping it, instead of trusting the
	// commitBlock tag — a defence against any case where the commitBlock==op-layer
	// invariant above breaks and the overlay entry would otherwise be pruned while
	// its buffer op has not yet been applied/flushed (the read-your-writes lost
	// write). At depth 2 (deepAsync=false) the invariant holds and the fast
	// tag-based prune is kept, preserving the byte-identical/synchronous path.
	deepAsync bool
}

type accountKVLatestPending struct {
	value   []byte
	deleted bool
	// number is the commit block that produced this entry, overwritten to the
	// latest put's block on re-put of the same key (matching the map's
	// overwrite-by-key semantics). Entries are pruned once a flush makes their
	// number durable in latestStore.
	number uint64
}

// accountKVLatestPendingMapKey indexes the read-your-writes overlay by its
// logical identity instead of serializing a full physical Pebble key for every
// lookup. AccountID deliberately strips the network prefix, matching the
// rooted-state key schema. logicalKey owns an immutable string copy on insert;
// map lookups can convert a caller's transient []byte without allocating.
type accountKVLatestPendingMapKey struct {
	owner      tcommon.AccountID
	generation uint64
	domain     kvdomains.KVDomain
	logicalKey string
}

type accountLatestPending struct {
	value   []byte
	deleted bool
	number  uint64
}

type kvGenerationPending struct {
	generation uint64
	deleted    bool
	number     uint64
}

type accountKVIndexStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

type accountKVLayerBatch interface {
	WriteUpTo(cutoff uint64) (remaining int, err error)
	WriteCommitted(dropStale bool) (remaining int, err error)
	// NewestCommittedNumber reports the block number of the newest committed
	// (promoted, not-yet-flushed) buffer layer, or (0,false) when none is
	// committed. A flush only makes an op durable once its layer is committed,
	// so this bounds which overlay entries are safe to prune.
	NewestCommittedNumber() (uint64, bool)
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

type accountKVOwnedPhysicalLatestStore interface {
	WriteKVLatestEncodedOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, encodedValue []byte) error
}

type accountLatestNoCopyPhysicalStore interface {
	ReadAccountLatestNoCopy(owner tcommon.Address) ([]byte, bool, error)
}

type accountKVLatestNoCopyPhysicalStore interface {
	ReadKVLatestNoCopy(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error)
}

// accountLatestHydrationBorrower is package-private because its result is only
// valid for immediate RLP decoding. General AccountLatest callers keep their
// defensive-copy contract.
type accountLatestHydrationBorrower interface {
	accountLatestForHydration(owner tcommon.Address) ([]byte, bool, error)
}

// accountKVLatestDecodingBorrower is package-private because its result is
// borrowed only for immediate protobuf or scalar decoding. General KVLatest
// callers retain their defensive-copy contract.
type accountKVLatestDecodingBorrower interface {
	kvLatestForDecoding(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error)
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
var _ statedomains.OwnedWriter = (*accountKVLatestBatch)(nil)
var _ statedomains.EncodedOwnedWriter = (*accountKVLatestBatch)(nil)

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

// DomainPutOwned transfers the immutable raw value through the latest batch.
// The encoded envelope is separately transferred to the physical writer,
// while pending read-your-writes state retains value without decode/copy.
func (w *accountKVLatestBatch) DomainPutOwned(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey, value []byte) error {
	generation, err := w.resolveGeneration(owner)
	if err != nil {
		return err
	}
	if err := w.writeDomainChange(owner, generation, domain, logicalKey, true, value); err != nil {
		return err
	}
	return w.putOwned(owner, generation, domain, logicalKey, value, rawdb.EncodeStateKVLatestValue(value))
}

func (w *accountKVLatestBatch) DomainPutEncodedOwned(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey, value, encodedValue []byte) error {
	generation, err := w.resolveGeneration(owner)
	if err != nil {
		return err
	}
	if err := w.writeDomainChange(owner, generation, domain, logicalKey, true, value); err != nil {
		return err
	}
	return w.putOwned(owner, generation, domain, logicalKey, value, encodedValue)
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

func (w *accountKVLatestBatch) DomainDelOwned(owner tcommon.Address, domain kvdomains.KVDomain, logicalKey []byte) error {
	generation, err := w.resolveGeneration(owner)
	if err != nil {
		return err
	}
	if err := w.writeDomainChange(owner, generation, domain, logicalKey, false, nil); err != nil {
		return err
	}
	return w.deleteOwned(owner, generation, domain, logicalKey)
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
		w.record(rawdb.NewStateCommitmentPutOwned(
			rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logicalKey),
			encodedValue,
		))
	}
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) putOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, value, encodedValue []byte) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if store, ok := w.latestStore.(accountKVOwnedPhysicalLatestStore); ok {
		if err := store.WriteKVLatestEncodedOwned(owner, generation, domain, logicalKey, encodedValue); err != nil {
			return err
		}
	} else if err := w.latestStore.WriteKVLatestEncoded(owner, generation, domain, logicalKey, encodedValue); err != nil {
		return err
	}
	w.rememberPutOwned(owner, generation, domain, logicalKey, value)
	if w.record != nil {
		w.record(rawdb.NewStateCommitmentPutOwned(
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
		w.record(rawdb.NewStateCommitmentDeleteOwned(rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logicalKey)))
	}
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) deleteOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) error {
	if w == nil || w.latestStore == nil {
		return fmt.Errorf("account kv latest domain writer: nil latest store")
	}
	if err := w.latestStore.DeleteKVLatest(owner, generation, domain, logicalKey); err != nil {
		return err
	}
	w.rememberDeleteOwned(owner, generation, domain, logicalKey)
	if w.record != nil {
		w.record(rawdb.NewStateCommitmentDeleteOwned(rawdb.StateKVLatestCommitmentKey(owner, generation, domain, logicalKey)))
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
	// Capture the newest committed block BEFORE the write: WriteUpTo only applies
	// ops whose layer is committed, so the highest block it can make durable is
	// min(cutoff, newest-committed). Production callers already cap cutoff at the
	// newest committed (blockchain.go / async_commit.go), so min == cutoff there;
	// the cap keeps the prune exact even if a caller passes a larger cutoff (it
	// must not prune entries whose still-in-flight layer was not applied). Reading
	// before the write is conservative — a layer that commits during the write is
	// applied but left un-pruned this round (pruned next round), never over-pruned.
	ncn, hasCommitted := layerBatch.NewestCommittedNumber()
	remaining, err := layerBatch.WriteUpTo(cutoff)
	if err != nil {
		return err
	}
	w.ops = remaining
	if remaining == 0 {
		w.clearPending()
		return nil
	}
	if hasCommitted {
		pruneCutoff := cutoff
		if ncn < pruneCutoff {
			pruneCutoff = ncn
		}
		w.prunePending(pruneCutoff)
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
	// WriteCommitted applies every committed layer's ops, so the highest durable
	// block afterwards is the newest committed one. Capture it BEFORE the write
	// (same conservative ordering as flushUpTo).
	ncn, hasCommitted := layerBatch.NewestCommittedNumber()
	remaining, err := layerBatch.WriteCommitted(dropStale)
	if err != nil {
		return err
	}
	w.ops = remaining
	if remaining == 0 {
		w.clearPending()
		return nil
	}
	if hasCommitted {
		w.prunePending(ncn)
	}
	return nil
}

// prunePending drops overlay entries whose tagged block number is <= cutoff.
// Those puts are now durable in latestStore (their buffer layer is committed and
// flushed up to cutoff), so a subsequent readLatest / readAccountLatest /
// readKVGeneration / iterateLatestPrefix falls through to latestStore and returns
// the identical value/existence — the same fall-through clearPending relies on at
// full flush, just triggered per-key as soon as the put is durable. Entries
// tagged > cutoff correspond to ops still queued in the batch (their layer not
// yet flushed) and MUST stay so read-your-writes keeps returning the latest
// value. This bounds the overlay to the in-flight window under deep async commit
// instead of accumulating the whole staged range.
func (w *accountKVLatestBatch) prunePending(cutoff uint64) {
	if w == nil {
		return
	}
	for k, e := range w.pending {
		if e.number > cutoff {
			continue
		}
		if !w.deepAsync || w.kvPendingDurable(k, e) {
			delete(w.pending, k)
		}
	}
	for k, e := range w.accountPending {
		if e.number > cutoff {
			continue
		}
		if !w.deepAsync || w.accountPendingDurable(k, e) {
			delete(w.accountPending, k)
		}
	}
	for k, e := range w.generationPending {
		if e.number > cutoff {
			continue
		}
		if !w.deepAsync || w.generationPendingDurable(k, e) {
			delete(w.generationPending, k)
		}
	}
}

// kvPendingDurable reports whether overlay entry e is now durable in the
// underlying latest store — the buffer's committed layers + disk, which
// latestStore reads through (the write-only batch is bypassed). Only then is it
// safe to drop the read-your-writes overlay entry: a subsequent readLatest falls
// through to latestStore and returns the identical value/existence. If e's buffer
// op has NOT been applied yet (e.g. it bound to a layer above the flush cutoff,
// breaking the commitBlock==op-layer assumption), latestStore still returns the
// OLD value, so this keeps the entry — guarding the deep async-commit lost write.
// On a read error it conservatively keeps the entry. Used only when deepAsync.
func (w *accountKVLatestBatch) kvPendingDurable(key accountKVLatestPendingMapKey, e accountKVLatestPending) bool {
	if w.latestStore == nil {
		return true
	}
	owner := key.owner.Address(tcommon.AddressPrefixMainnet)
	var val []byte
	var exists bool
	var err error
	if reader, ok := w.latestStore.(accountKVLatestNoCopyPhysicalStore); ok {
		val, exists, err = reader.ReadKVLatestNoCopy(owner, key.generation, key.domain, []byte(key.logicalKey))
	} else {
		val, exists, err = w.latestStore.ReadKVLatest(owner, key.generation, key.domain, []byte(key.logicalKey))
	}
	if err != nil {
		return false
	}
	if e.deleted {
		return !exists
	}
	return exists && bytes.Equal(val, e.value)
}

func (w *accountKVLatestBatch) accountPendingDurable(ownerID tcommon.AccountID, e accountLatestPending) bool {
	if w.latestStore == nil {
		return true
	}
	val, exists, err := w.latestStore.ReadAccountLatest(ownerID.Address(tcommon.AddressPrefixMainnet))
	if err != nil {
		return false
	}
	if e.deleted {
		return !exists
	}
	return exists && bytes.Equal(val, e.value)
}

func (w *accountKVLatestBatch) generationPendingDurable(ownerID tcommon.AccountID, e kvGenerationPending) bool {
	if w.latestStore == nil {
		return true
	}
	gen, exists, err := w.latestStore.ReadKVGeneration(ownerID.Address(tcommon.AddressPrefixMainnet))
	if err != nil {
		return false
	}
	if e.deleted {
		return !exists
	}
	return exists && gen == e.generation
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

// writeAccountLatestOwnedByKey accepts a physical latest key from the
// commit-wide key arena and takes ownership of the freshly encoded value.
// Both the layer batch and pending read overlay retain it as immutable data.
func (w *accountKVLatestBatch) writeAccountLatestOwnedByKey(owner tcommon.Address, physicalKey, value []byte) error {
	if w == nil || w.writer == nil {
		return fmt.Errorf("account kv latest domain writer: nil writer")
	}
	if err := rawdb.WriteStateAccountLatestOwnedByKey(w.writer, physicalKey, value); err != nil {
		return err
	}
	w.rememberAccountLatestPutOwned(owner, value)
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

func (w *accountKVLatestBatch) deleteAccountLatestByKey(owner tcommon.Address, physicalKey []byte) error {
	if w == nil || w.writer == nil {
		return fmt.Errorf("account kv latest domain writer: nil writer")
	}
	if err := rawdb.DeleteStateAccountLatestByKey(w.writer, physicalKey); err != nil {
		return err
	}
	w.rememberAccountLatestDelete(owner)
	return w.maybeFlush()
}

func (w *accountKVLatestBatch) readAccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if w != nil {
		if pending, ok := w.accountPending[owner.AccountID()]; ok {
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

// readAccountLatestForCommitment is the synchronous commitment-fold variant
// of readAccountLatest. Pending account values are freshly encoded immutable
// buffers whose ownership was transferred to this batch. The fold only hashes
// them before returning, so borrowing the pending slice avoids a defensive copy
// per touched account without weakening readAccountLatest's public ownership
// contract. The durable-store fallback already returns an owned value.
func (w *accountKVLatestBatch) readAccountLatestForCommitment(owner tcommon.Address) ([]byte, bool, error) {
	if w != nil {
		if pending, ok := w.accountPending[owner.AccountID()]; ok {
			if pending.deleted {
				return nil, false, nil
			}
			return pending.value, true, nil
		}
	}
	if w == nil || w.latestStore == nil {
		return nil, false, nil
	}
	return w.latestStore.ReadAccountLatest(owner)
}

// readAccountLatestForHydration lends immutable bytes only until the caller's
// immediate decode completes. Pending values and blockbuffer cache values are
// already immutable; generic stores fall back to their ordinary owned read.
func (w *accountKVLatestBatch) readAccountLatestForHydration(owner tcommon.Address) ([]byte, bool, error) {
	if w != nil {
		if pending, ok := w.accountPending[owner.AccountID()]; ok {
			if pending.deleted {
				return nil, false, nil
			}
			return pending.value, true, nil
		}
	}
	if w == nil || w.latestStore == nil {
		return nil, false, nil
	}
	if reader, ok := w.latestStore.(accountLatestNoCopyPhysicalStore); ok {
		return reader.ReadAccountLatestNoCopy(owner)
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
		if pending, ok := w.generationPending[owner.AccountID()]; ok {
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
		if pending, ok := w.pending[accountKVLatestPendingKey(owner, generation, domain, logicalKey)]; ok {
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

// readLatestForDecoding lends immutable bytes only until the caller's
// immediate decode completes. Pending values and blockbuffer cache values are
// immutable; generic stores fall back to their ordinary owned read.
func (w *accountKVLatestBatch) readLatestForDecoding(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) ([]byte, bool, error) {
	if w != nil {
		if pending, ok := w.pending[accountKVLatestPendingKey(owner, generation, domain, logicalKey)]; ok {
			if pending.deleted {
				return nil, false, nil
			}
			return pending.value, true, nil
		}
	}
	if w == nil || w.latestStore == nil {
		return nil, false, nil
	}
	if reader, ok := w.latestStore.(accountKVLatestNoCopyPhysicalStore); ok {
		return reader.ReadKVLatestNoCopy(owner, generation, domain, logicalKey)
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
	ownerID := owner.AccountID()
	logicalPrefix := string(prefix)
	for key, pending := range w.pending {
		if key.owner != ownerID || key.generation != generation || key.domain != domain || !strings.HasPrefix(key.logicalKey, logicalPrefix) {
			continue
		}
		mapKey := key.logicalKey
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
	w.rememberPutWithKey(accountKVLatestPendingKey(owner, generation, domain, logicalKey), append([]byte(nil), value...))
}

func (w *accountKVLatestBatch) rememberPutOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey, value []byte) {
	if w == nil {
		return
	}
	w.rememberPutWithKey(accountKVLatestPendingKeyOwned(owner, generation, domain, logicalKey), value)
}

func (w *accountKVLatestBatch) rememberPutWithKey(mapKey accountKVLatestPendingMapKey, value []byte) {
	if w.pending == nil {
		w.pending = make(map[accountKVLatestPendingMapKey]accountKVLatestPending)
	}
	w.pending[mapKey] = accountKVLatestPending{
		value:  value,
		number: w.commitBlock,
	}
}

func (w *accountKVLatestBatch) rememberAccountLatestPut(owner tcommon.Address, value []byte) {
	w.rememberAccountLatestPutOwned(owner, append([]byte(nil), value...))
}

func (w *accountKVLatestBatch) rememberAccountLatestPutOwned(owner tcommon.Address, value []byte) {
	if w == nil {
		return
	}
	if w.accountPending == nil {
		w.accountPending = make(map[tcommon.AccountID]accountLatestPending)
	}
	w.accountPending[owner.AccountID()] = accountLatestPending{
		value:  value,
		number: w.commitBlock,
	}
}

func (w *accountKVLatestBatch) rememberAccountLatestDelete(owner tcommon.Address) {
	if w == nil {
		return
	}
	if w.accountPending == nil {
		w.accountPending = make(map[tcommon.AccountID]accountLatestPending)
	}
	w.accountPending[owner.AccountID()] = accountLatestPending{deleted: true, number: w.commitBlock}
}

func (w *accountKVLatestBatch) rememberKVGenerationPut(owner tcommon.Address, generation uint64) {
	if w == nil {
		return
	}
	if w.generationPending == nil {
		w.generationPending = make(map[tcommon.AccountID]kvGenerationPending)
	}
	w.generationPending[owner.AccountID()] = kvGenerationPending{generation: generation, number: w.commitBlock}
}

func (w *accountKVLatestBatch) rememberKVGenerationDelete(owner tcommon.Address) {
	if w == nil {
		return
	}
	if w.generationPending == nil {
		w.generationPending = make(map[tcommon.AccountID]kvGenerationPending)
	}
	w.generationPending[owner.AccountID()] = kvGenerationPending{deleted: true, number: w.commitBlock}
}

func (w *accountKVLatestBatch) rememberDelete(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) {
	if w == nil {
		return
	}
	if w.pending == nil {
		w.pending = make(map[accountKVLatestPendingMapKey]accountKVLatestPending)
	}
	mapKey := accountKVLatestPendingKey(owner, generation, domain, logicalKey)
	w.pending[mapKey] = accountKVLatestPending{
		deleted: true,
		number:  w.commitBlock,
	}
}

func (w *accountKVLatestBatch) rememberDeleteOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) {
	if w == nil {
		return
	}
	if w.pending == nil {
		w.pending = make(map[accountKVLatestPendingMapKey]accountKVLatestPending)
	}
	mapKey := accountKVLatestPendingKeyOwned(owner, generation, domain, logicalKey)
	w.pending[mapKey] = accountKVLatestPending{
		deleted: true,
		number:  w.commitBlock,
	}
}

func accountKVLatestPendingKey(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) accountKVLatestPendingMapKey {
	return accountKVLatestPendingMapKey{
		owner:      owner.AccountID(),
		generation: generation,
		domain:     domain,
		logicalKey: string(logicalKey),
	}
}

func accountKVLatestPendingKeyOwned(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, logicalKey []byte) accountKVLatestPendingMapKey {
	return accountKVLatestPendingMapKey{
		owner:      owner.AccountID(),
		generation: generation,
		domain:     domain,
		logicalKey: ownedBytesString(logicalKey),
	}
}

// kvCompositeKeyString owns the pre-hash map key domainBE2||key in one
// allocation. The previous []byte construction followed by string conversion
// copied every staged key twice.
func kvCompositeKeyString(domain kvdomains.KVDomain, key []byte) string {
	out := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(out, uint16(domain))
	copy(out[2:], key)
	return ownedBytesString(out)
}

// lookupKVEntry builds the common <=64-byte logical key in stack storage. The
// transient []byte-to-string conversion is allocation-free for map lookup.
func lookupKVEntry(entries map[string]kvEntry, domain kvdomains.KVDomain, key []byte) (kvEntry, bool) {
	var stack [66]byte
	var composite []byte
	if size := 2 + len(key); size <= len(stack) {
		composite = stack[:size]
	} else {
		composite = make([]byte, size)
	}
	binary.BigEndian.PutUint16(composite, uint16(domain))
	copy(composite[2:], key)
	entry, ok := entries[string(composite)]
	return entry, ok
}

func newKVEntry(value []byte, deleted bool) kvEntry {
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

func (s *StateDB) readAccountKVLatestForDecoding(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if s != nil && s.accountKVLatestReader != nil {
		if reader, ok := s.accountKVLatestReader.(accountKVLatestDecodingBorrower); ok {
			return reader.kvLatestForDecoding(owner, generation, domain, key)
		}
		return s.readAccountKVLatest(owner, generation, domain, key)
	}
	store := s.accountKVPhysicalLatestStore()
	if reader, ok := store.(accountKVLatestNoCopyPhysicalStore); ok {
		return reader.ReadKVLatestNoCopy(owner, generation, domain, key)
	}
	return store.ReadKVLatest(owner, generation, domain, key)
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

func (s *StateDB) readStateAccountLatestForHydration(owner tcommon.Address) ([]byte, bool, error) {
	if s != nil && s.flatLatestReader != nil {
		if reader, ok := s.flatLatestReader.(accountLatestHydrationBorrower); ok {
			return reader.accountLatestForHydration(owner)
		}
		return s.flatLatestReader.AccountLatest(owner)
	}
	store := s.accountKVPhysicalLatestStore()
	if reader, ok := store.(accountLatestNoCopyPhysicalStore); ok {
		return reader.ReadAccountLatestNoCopy(owner)
	}
	return store.ReadAccountLatest(owner)
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
	if e, ok := lookupKVEntry(obj.kvDirty, domain, key); ok {
		if e.deleted {
			return nil, false, nil
		}
		return append([]byte{}, e.val...), true, nil
	}
	return s.readAccountKVLatest(owner, obj.accountKVGeneration, domain, key)
}

// getAccountKVForDecoding borrows immutable state bytes for immediate package-
// internal decoding. Callers must neither mutate nor retain the returned slice.
// GetAccountKV remains the owning public API.
func (s *StateDB) getAccountKVForDecoding(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	obj := s.getStateObject(owner)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	if e, ok := lookupKVEntry(obj.kvDirty, domain, key); ok {
		if e.deleted {
			return nil, false, nil
		}
		return e.val, true, nil
	}
	return s.readAccountKVLatestForDecoding(owner, obj.accountKVGeneration, domain, key)
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
		if e, ok := lookupKVEntry(obj.kvDirty, domain, key); ok {
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
		d, logicalKey, ok := splitKVCompositeKey(ownedStringBytes(mapKey))
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
	var (
		prevValue  []byte
		prevExists bool
		prevLoaded bool
	)
	prevDirty, dirty := lookupKVEntry(obj.kvDirty, domain, key)
	if !dirty {
		current, exists, err := s.readAccountKVLatest(owner, obj.accountKVGeneration, domain, key)
		if err != nil {
			return err
		}
		prevValue = current
		prevExists = exists
		prevLoaded = true
	}
	return s.setAccountKVResolved(obj, domain, key, value, journal, prevValue, prevExists, prevLoaded, prevDirty, dirty)
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
	prevDirty, dirty := lookupKVEntry(obj.kvDirty, domain, key)
	return s.setAccountKVResolved(obj, domain, key, value, journal, prevValue, prevExists, prevLoaded, prevDirty, dirty)
}

// setAccountKVResolved receives the allocation-free dirty lookup performed by
// its caller. Delay owning the composite map key until after no-op detection so
// repeated writes of an identical dirty value allocate nothing.
func (s *StateDB) setAccountKVResolved(obj *stateObject, domain kvdomains.KVDomain, key, value []byte, journal bool, prevValue []byte, prevExists, prevLoaded bool, prevDirty kvEntry, dirty bool) error {
	if dirty {
		if !prevDirty.deleted && bytes.Equal(prevDirty.val, value) {
			return nil
		}
	} else if prevLoaded && prevExists && bytes.Equal(prevValue, value) {
		return nil
	}
	mk := kvCompositeKeyString(domain, key)
	if journal {
		s.journal.append(kvChange{address: obj.address, mapKey: mk, hadEntry: dirty, prevEntry: prevDirty})
	} else if s.changeSet.enabled && !s.changeSet.captureAtCommit {
		s.domainChangeNoJournal = append(s.domainChangeNoJournal, kvChange{address: obj.address, mapKey: mk, hadEntry: dirty, prevEntry: prevDirty})
	}
	entry := newKVEntry(value, false)
	if dirty {
		entry.inheritPrev(prevDirty)
	} else if prevLoaded {
		entry.setPrev(prevValue, prevExists)
	}
	obj.setKVDirty(mk, entry)
	invalidateAccountSplitMaterialization(obj, domain)
	obj.markDirty()
	return nil
}

func (s *StateDB) stageAccountKVCommit(obj *stateObject, domain kvdomains.KVDomain, key, value []byte, deleted bool) (bool, error) {
	return s.stageAccountKVCommitWithPrev(obj, domain, key, value, deleted, nil, false, false)
}

// stageAccountKVCommitWithPrev stages a commit-generated account-KV mutation.
// Callers that already loaded the durable pre-image can pass it here to avoid a
// duplicate flat-latest lookup. prevLoaded=true with prevExists=false denotes a
// known absent row.
func (s *StateDB) stageAccountKVCommitWithPrev(obj *stateObject, domain kvdomains.KVDomain, key, value []byte, deleted bool, prevValue []byte, prevExists, prevLoaded bool) (bool, error) {
	if !kvdomains.IsRegistered(domain) {
		return false, fmt.Errorf("account kv: unregistered domain %#04x", uint16(domain))
	}
	if obj == nil {
		return false, nil
	}
	mk := kvCompositeKeyString(domain, key)
	prevDirty, dirty := obj.kvDirty[mk]
	entry := newKVEntry(value, deleted)
	if dirty {
		entry.inheritPrev(prevDirty)
	} else if prevLoaded {
		entry.setPrev(prevValue, prevExists)
		if noop, known := entry.latestNoop(); known && noop {
			return false, nil
		}
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
	obj.setKVDirty(mk, entry)
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
	mk := kvCompositeKeyString(domain, key)
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
	entry := newKVEntry(nil, true)
	if dirty {
		entry.inheritPrev(prevDirty)
	} else if prevLoaded {
		entry.setPrev(prevValue, prevExists)
	}
	obj.setKVDirty(mk, entry)
	invalidateAccountSplitMaterialization(obj, domain)
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
	obj.releaseKVDirty()
	obj.accountKVRoot = EmptyKVRoot
	obj.accountKVGeneration++
	obj.accountKVGenerationDirty = true
	obj.markDirty()
	return nil
}

func (s *StateDB) prepareAccountKVCommitPlan(obj *stateObject) (*accountKVCommitPlan, error) {
	items, pooledItems := borrowAccountKVCommitItems(len(obj.kvDirty))
	plan := accountKVCommitPlanPool.Get().(*accountKVCommitPlan)
	*plan = accountKVCommitPlan{items: items, pooledItems: pooledItems, pooledPlan: true}
	for mk, e := range obj.kvDirty {
		// kvDirty owns immutable composite strings through commit finalization;
		// lend that backing to the plan and downstream ownership pipeline.
		composite := ownedStringBytes(mk)
		domain, logicalKey, ok := splitKVCompositeKeyView(composite)
		if !ok {
			releaseAccountKVCommitPlan(plan)
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
	owned, canTransfer := latest.(statedomains.OwnedWriter)
	encoded, canTransferEncoded := latest.(statedomains.EncodedOwnedWriter)
	for _, item := range plan.items {
		entry := item.entry
		wrote = true
		if entry.deleted {
			if canTransfer {
				// logicalKey is backed by this commit plan and remains immutable
				// until SharedDomainTx.Flush completes below the plan loop.
				if err := owned.DomainDelOwned(obj.address, item.domain, item.logicalKey); err != nil {
					return false, err
				}
				continue
			}
			if err := latest.DomainDel(obj.address, item.domain, item.logicalKey); err != nil {
				return false, err
			}
			continue
		}
		if canTransferEncoded && len(entry.wrapped) != 0 {
			// wrapped owns the persisted presence envelope and entry.val is its
			// semantic suffix. Neither is mutated after this commit plan forms,
			// so latest, pending reads and commitment capture can share it.
			if err := encoded.DomainPutEncodedOwned(obj.address, item.domain, item.logicalKey, entry.val, entry.wrapped); err != nil {
				return false, err
			}
			continue
		}
		if canTransfer {
			// entry.val is owned by obj.kvDirty, which is finalized only after
			// the temporal transaction has synchronously recorded and flushed
			// every mutation. Transfer both slices instead of cloning them into
			// the short-lived overlay.
			if err := owned.DomainPutOwned(obj.address, item.domain, item.logicalKey, entry.val); err != nil {
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
