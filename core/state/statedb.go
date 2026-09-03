package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")

	stateObjectCachePreviousReuseCounter = metrics.NewRegisteredCounter("state/account_cache/reuse/previous", nil)
	stateObjectCacheOlderReuseCounter    = metrics.NewRegisteredCounter("state/account_cache/reuse/older", nil)
	stateObjectCacheOldestReuseCounter   = metrics.NewRegisteredCounter("state/account_cache/reuse/oldest", nil)
	stateObjectCacheAncientReuseCounter  = metrics.NewRegisteredCounter("state/account_cache/reuse/ancient", nil)
	stateObjectCacheHydrationCounter     = metrics.NewRegisteredCounter("state/account_cache/hydrations", nil)
	stateObjectCacheEvictionCounter      = metrics.NewRegisteredCounter("state/account_cache/evictions", nil)
	stateExecutionCopySourceCounter      = metrics.NewRegisteredCounter("state/copy/execution/source_objects", nil)
	stateExecutionCopyDirtyCounter       = metrics.NewRegisteredCounter("state/copy/execution/dirty_objects", nil)
	stateExecutionCopyCachedCodeCounter  = metrics.NewRegisteredCounter("state/copy/execution/cached_code_objects", nil)
	stateExecutionCopyOmittedCounter     = metrics.NewRegisteredCounter("state/copy/execution/omitted_clean_objects", nil)
)

// storageReadKeyPool keeps the computed 32-byte StorageRow key alive across
// interface-based latest-domain reads. Passing a slice of a stack-local hash
// through those reader interfaces makes it escape once per cold SLOAD; readers
// consume request keys synchronously and never retain them, so the buffer can
// be reused immediately after the call.
var storageReadKeyPool = sync.Pool{
	New: func() any { return new(tcommon.Hash) },
}

// maxStateObjectCachedStorageSlots bounds the cross-block SLOAD cache of one
// repeatedly used contract. Large token contracts touch an effectively
// unbounded stream of mapping slots during a historical sync; retaining all of
// them made one hot stateObject pin hundreds of MiB even after older account
// objects were evicted. Slots remain fully cached within a block. At the
// successful commit boundary an oversized clean map is discarded and later
// reads reload through the bounded blockbuffer base-read cache.
const maxStateObjectCachedStorageSlots = 4096

// StateDB manages in-memory account state with Erigon-style flat latest-domain
// commits backed by a CommitmentDomain root.
type StateDB struct {
	db *Database
	// dbErr is the first durable-state read/decode failure observed by this
	// execution view. Getters whose public signatures cannot return an error must
	// never turn corruption into a semantic miss: they record it here, and the
	// transaction/commit boundaries fail closed before publishing mutations.
	dbErr error

	stateObjects map[tcommon.Address]*stateObject
	witnesses    map[tcommon.Address]*types.Witness

	// transactionVersionedReader is installed only on block-scoped speculative
	// views. It overlays canonical post-images published before the retry's
	// frozen transaction boundary without replaying every prefix WriteSet into a
	// private StateDB. Canonical execution never installs this reader.
	transactionVersionedReader   TransactionVersionedValueReader
	transactionVersionedTxIndex  int
	transactionVersionedHydrated map[tcommon.Address]struct{}
	transactionVersionedMissing  map[tcommon.Address]struct{}
	// transactionVersionedStorageChecked distinguishes task-local dirty slots
	// from clean cache entries inherited from a reverted retry template. The
	// latter must be checked once against the immutable canonical prefix before
	// their cached block-start value is trusted.
	transactionVersionedStorageChecked map[transactionVersionedStorageKey]struct{}

	// Small encoded logical keys are borrowed only for the duration of a
	// synchronous account-KV lookup/write. Keeping their backing arrays on the
	// execution-confined StateDB avoids one tiny heap object per TRC10, asset ID,
	// vote index, or frozen-row access. Writers own composite keys before return.
	trc10TokenKeyScratch     [20]byte
	assetIDKeyScratch        [9]byte
	assetBytesKeyScratch     [33]byte
	assetBandwidthKeyScratch [33]byte
	accountUint32KeyScratch  [4]byte
	// Witness capsule keys are address-dependent but the witness set is tiny and
	// stable across a replay range. Cache one immutable fixed key per observed
	// witness so repeated lookups do not allocate a 23-byte temporary that
	// escapes through the generic account-KV reader interface.
	witnessCapsuleKeys map[tcommon.Address]*[rawdb.WitnessCapsuleStateKeyLength]byte
	// Account-local native rows normally fit in 64 bytes. Writers encode into
	// this execution-confined buffer before SetAccountKV copies the bytes into
	// its block arena, avoiding one short-lived allocation per hot row.
	accountRowMarshalScratch [64]byte
	// Standard cycle reward keys are at most 46 bytes (dl- + cycle +
	// separators + a 21-byte address + account-vote). The public APIs still
	// accept arbitrary address slices; append grows beyond this scratch when
	// necessary without changing key bytes.
	delegRewardKeyScratch [64]byte

	// lastStateObject is a single-entry lookup cache for the account map. TVM
	// execution commonly performs long runs of SLOAD/SSTORE and account queries
	// against the current contract, all of which otherwise hash the same address
	// in stateObjects. StateDB is execution-goroutine confined, so no locking is
	// needed. RevertToSnapshot clears the cache because journal replay may delete
	// or replace the mapped object.
	lastStateObject *stateObject
	// touchedStateObjects contains each account first accessed in the current
	// block. The four retained-state slices are the preceding four successful
	// blocks' working sets. At commit the five slices rotate and
	// clean accounts not reused for four complete blocks are evicted. This small
	// generational window captures common alternating-block account reuse without
	// letting a range-reused StateDB retain every account it has ever read. Slice
	// backing alternates in steady state, so rotation itself does not allocate.
	touchedStateObjects          []tcommon.Address
	retainedStateObjects         []tcommon.Address
	olderRetainedStateObjects    []tcommon.Address
	oldestRetainedStateObjects   []tcommon.Address
	ancientRetainedStateObjects  []tcommon.Address
	stateObjectWorkingGeneration uint64

	// Storage-read counters are execution-goroutine confined and flushed to
	// registered metrics once per successful commit. oversizedStorageSamples is
	// a bounded exact probe of whether slots discarded from >4096-entry caches
	// are read again in the next four blocks; it is diagnostic-only state and is
	// intentionally excluded from Copy and consensus serialization.
	storageObservability    storageObservabilityBatch
	oversizedStorageSamples map[tcommon.Address]*oversizedStorageSample

	// loadedAccountProtoObjects tracks objects whose original flat-envelope
	// AccountProto is retained for a possible same-block journal pre-image.
	// Successful commit releases bytes that were never consumed by a mutation,
	// keeping this optimization bounded to one block without scanning the whole
	// range-accumulated stateObjects map.
	loadedAccountProtoObjects []*stateObject

	// dirtyWitnesses tracks addresses whose VoteCount or URL changed in
	// the current block. FlushWitnesses iterates this set instead of the
	// full witnesses map so no-op blocks (the common case — no VoteWitness
	// or WitnessUpdate tx) skip witness persistence entirely.
	//
	// Population: every mutator that changes VoteCount or URL marks dirty
	// (PutWitness, SetWitnessURL, AddWitnessVoteCount). Preload via
	// GetWitness / LoadWitness does NOT mark dirty — it just hydrates the
	// in-memory cache.
	//
	// Revert: the set is deliberately NOT cleared by RevertToSnapshot.
	// Flushing a witness whose net change is zero costs one Read+Write but
	// is correctness-preserving (the stored counters round-trip unchanged).
	// Precise clearing would require walking the journal to undo dirty
	// marks per change — the saved IO doesn't justify the complexity.
	dirtyWitnesses map[tcommon.Address]struct{}

	// standbyWitnessVersion changes only when the membership or vote weights
	// used by the top-127 standby reward calculation change. It is deliberately
	// monotonic across journal reverts: a false-positive cache rebuild is safe,
	// while reusing a set after a reverted speculative mutation is not worth the
	// bookkeeping risk. Production/missed counters and URL-only updates do not
	// affect standby ranking and therefore leave this version unchanged.
	standbyWitnessVersion uint64

	// txFinalizeDirty tracks addresses whose contract storage was written
	// (SetState) or which were self-destructed since the last
	// FinalizeTransaction. FinalizeTransaction iterates this set instead of the
	// full stateObjects map to flip zero-valued storage rows non-existent and
	// delete self-destructed accounts at the transaction boundary.
	//
	// Completeness: a zero-valued slot that needs a present→absent transition can
	// only come from SetState. GetStateWithExist may cache a durable miss as an
	// already-absent clean slot, but it never enters dirtyStorage and needs no
	// transaction-finalization work. Thus every object the old full scan would
	// have changed is in this set. Self-destructs are the only other thing the
	// scan acted on, and SelfDestruct populates the set too.
	//
	// Stale entries (left by a reverted write/self-destruct) are harmless:
	// FinalizeTransaction re-resolves the live object by address via
	// stateObjects, and the v==0 / (selfDestructed && !deleted) guards no-op on
	// a reverted object. It is therefore deliberately NOT cleared by
	// RevertToSnapshot, matching dirtyWitnesses. It IS cleared at the end of
	// every FinalizeTransaction (per-transaction scope).
	txFinalizeDirty map[tcommon.Address]struct{}

	// dirtyObjects tracks the addresses of every stateObject marked dirty since
	// the last commit. dirtyAccountCommitPlans iterates this set instead of
	// scanning the whole stateObjects map (which, on the reused-StateDB sync
	// path, accumulates the entire range's mostly read-only working set).
	//
	// Population: markDirty records s.address through the stateObject's dirtySet
	// back-pointer (set when an object enters the cache via getStateObject), and
	// GetOrCreateAccount records the born-dirty address it creates. Every forward
	// mutator obtains its object via getStateObject / GetOrCreateAccount before
	// calling markDirty, so the back-pointer is always set first; the set is thus
	// a complete superset of {addr : stateObjects[addr].dirty}.
	//
	// Revert: an object re-dirtied during RevertToSnapshot (journal replay) is
	// already in the set from the forward mutation that journaled it, and an
	// address stays in the set until the next commit clears it, so the set is
	// never cleared by RevertToSnapshot — matching dirtyWitnesses. Stale entries
	// (a created-then-reverted address, or one re-dirtied to its original value)
	// are harmless: dirtyAccountCommitPlans re-resolves the live object by
	// address and the obj == nil / !obj.dirty guards skip it.
	//
	// Lifecycle is PER-BLOCK: cleared once after the finalizeAccountCommitPlan
	// loop. dirtyAccountCommitPlans and the finalize loop both run on the
	// committing goroutine (the deferred-fold worker only folds captured data and
	// never touches stateObjects/dirtyObjects), so there is no exec/worker race.
	dirtyObjects map[tcommon.Address]struct{}

	// accountCommitPlans owns the address ordering, stable plan records, and
	// pointer view used by one block's commit planning. The sync path reuses one
	// StateDB across a range, so retaining bounded capacity here removes three
	// otherwise per-block allocations. The storage composite-key arena is
	// intentionally excluded: successful commits may transfer borrowed key bytes
	// into the unsolidified blockbuffer, so those bytes must remain immutable.
	accountCommitPlans accountCommitPlanWorkspace

	// kvEntryArena owns immutable value/pre-image chunks for the current block's
	// account-KV dirty overlay. Successful commit drops only the arena's current
	// reference: slices transferred to blockbuffer retain their backing arrays.
	// Failed commits deliberately keep the arena intact with the dirty entries.
	kvEntryArena kvEntryArena

	journal   *journal
	snapshots []int // journal length at each snapshot

	balanceTrace *balanceTraceRecorder
	// balanceTraceSnapshots mirrors snapshots so balance-history capture rolls
	// back with failed TVM calls and transactions.
	balanceTraceSnapshots []balanceTraceSnapshot
	// accountJournalPos remembers the most recent full or scalar Account journal
	// entry for each address. Account journal helpers use it to coalesce writes
	// within one snapshot/history interval. Positions are validated against the
	// live journal before reuse, so entries truncated by RevertToSnapshot cannot
	// become false hits after the slice grows again.
	accountJournalPos map[tcommon.Address]int

	// transientStorage holds EIP-1153 (Cancun) TLOAD/TSTORE slots for the
	// current transaction, keyed by (contract address, slot) — the same
	// namespacing persistent storage uses. Writes are journaled
	// (transientStorageChange) so RevertToSnapshot rolls them back with the
	// enclosing call frame, mirroring java-tron's per-frame child
	// RepositoryImpl that commits transient storage to its parent only on
	// success and discards it on revert. The whole map is cleared at every
	// FinalizeTransaction — the EIP-1153 end-of-transaction discard. Lazily
	// allocated on first SetTransientState; left nil on a fresh or Copy()'d
	// StateDB so a constant-call sees empty transient storage.
	transientStorage map[transientStorageKey]tcommon.Hash

	// domainChangeNoJournal mirrors block-final writes that intentionally
	// bypass the snapshot/revert journal but still need temporal change rows.
	domainChangeNoJournal []journalChange

	dynProps *DynamicProperties

	// transactionAccess is installed only around canonical transaction
	// execution by the P4 versioned-shadow planner. StateDB remains confined to
	// the execution goroutine, so recording needs no synchronization. Copies and
	// RPC simulations intentionally start with this nil.
	transactionAccess *TransactionAccessRecorder

	// originRoot is the CommitmentDomain root at the time of the last successful
	// Commit (or the root passed to New).
	originRoot ethcommon.Hash

	// deferFold, when set, makes commitWithStatsOptions stop after capturing the
	// commitment-fold inputs instead of folding inline: it stashes them in
	// capturedFold (consumed synchronously by the same goroutine via
	// TakeCapturedFold before the next block runs) and resets the journal so the
	// reused StateDB can proceed. The async commit worker then runs the fold off
	// the chainmu critical path. With deferFold false (the default) the commit
	// path is byte-identical to the synchronous one — the deferFold branch is
	// simply not taken. See FoldLatestCommitment / CapturedCommit.
	deferFold    bool
	capturedFold *CapturedCommit

	// accountKVIndexStore is the physical latest-state index view. It defaults
	// to the disk DB, but block application points it at blockbuffer so latest
	// rows rewind with unsolidified blocks.
	accountKVIndexStore interface {
		ethdb.KeyValueReader
		ethdb.KeyValueWriter
		ethdb.Iteratee
	}
	accountKVLatestReader   statedomains.LatestReader
	accountKVLatestIterator statedomains.Iterator
	flatLatestReader        domainCommitmentLatestReader

	changeSet domainChangeSetCapture

	codeStore       stateCodeStore
	codeColdHistory StateCodeColdHistoryAtOrBefore
	codeColdTxNum   uint64

	commitmentColdHistory statedomains.CommitmentSnapshotSource
	commitmentColdTxNum   uint64

	cycleRewardSink CycleRewardSink
}

// SetTransactionAccessRecorder installs a transaction-scoped observe-only
// access recorder. Passing nil disables capture. The recorder never changes
// state values, journal entries, snapshots, or commit behavior.
func (s *StateDB) SetTransactionAccessRecorder(recorder *TransactionAccessRecorder) {
	if s != nil {
		s.transactionAccess = recorder
		if s.journal != nil {
			s.journal.transactionAccess = recorder
		}
	}
}

// CommitStats breaks StateDB.Commit wall-clock time into the write phases that
// matter during sync. With zero-value CommitOptions it is intentionally
// observational: Commit and CommitWithStats execute the same state transition
// and persist the same keys.
type CommitStats struct {
	Prepare           time.Duration
	FlatWrite         time.Duration
	FlatFlush         time.Duration
	KVCompute         time.Duration
	KVNodeWrite       time.Duration
	AccountTrieUpdate time.Duration
	// AccountTrie* names are retained for metrics compatibility. In the flat
	// layout they measure account-latest envelope update and CommitmentDomain
	// root work, not full-state trie writes.
	AccountTrieMarshal    time.Duration
	AccountTrieGeneration time.Duration
	AccountTrieWrite      time.Duration
	Finalize              time.Duration
	AccountTrieCommit     time.Duration
	TrieNodeWrite         time.Duration
	TrieNodeFlush         time.Duration
	Reopen                time.Duration

	Accounts   int
	KVAccounts int
	KVItems    int
	// DeferredKV* is retained for metrics compatibility and is always zero in
	// the flat-only commit path.
	DeferredKVAccounts int
	DeferredKVItems    int
	// RebuiltKV* is retained for metrics compatibility and is always zero in
	// the flat-only commit path.
	RebuiltKVAccounts int
	RebuiltKVItems    int

	// CommitmentUpdates is the exact number of unique, sorted latest-domain
	// updates handed to the commitment fold after same-block overwrite
	// collapse. It is a workload counter, not part of Total.
	CommitmentUpdates int

	Mutations CommitMutationStats
}

// CommitOptions is intentionally empty after the state commit path was
// collapsed onto flat latest domains plus CommitmentDomain. The type remains as
// an API shim for call sites that still route through CommitWithStatsOptions.
type CommitOptions struct {
	// FlushLatestDomain lets a staged execution plan own the scoped
	// account-KV latest flush. The scoped commitment reader can consume the
	// pending latest batch before it is physically flushed. It is only valid
	// together with CommitWithStatsOptionsInScope.
	FlushLatestDomain func() error
	// BlockNumber is the number of the block being committed. It tags the scoped
	// latest writer's read-your-writes overlay entries so a later partial flush
	// can prune the ones whose puts are durable in the buffer's committed layers
	// (deep async-commit overlay pruning). It must match the buffer active-layer
	// number the latest-domain ops bind to — BeginBlock(block.Number()) precedes
	// the commit, so callers pass block.Number() here. Required on the scoped
	// (CommitWithStatsOptionsInScope) path; ignored on the per-block fresh-writer
	// path, which fully flushes and clears its overlay every commit.
	BlockNumber uint64
	// DeepAsync marks the deep async-commit pipeline (GTRON_ASYNC_COMMIT_DEPTH >
	// 2). It makes the scoped latest writer's prunePending verify each overlay
	// entry is actually durable in the underlying store before dropping it, rather
	// than trusting the BlockNumber tag — the guard against a read-your-writes lost
	// write if the tag ever diverges from the buffer layer the op bound to. At
	// depth 2 it is false and the fast tag-based prune (byte-identical to the
	// synchronous path) is kept.
	DeepAsync bool
}

// CommitScope carries the domain transaction objects reused by a staged range
// import. A scope is tied to one live StateDB instance; each block still emits
// its own state root and history range, but generic account-KV writes flow
// through the same SharedDomainTx/latest writer instead of allocating a fresh
// domain transaction per block.
type CommitScope struct {
	state              *StateDB
	generationResolver statedomains.GenerationResolver
	history            *DomainHistoryState
	commitment         *commitScopeCommitment
	commitmentState    *DomainCommitmentState
	latestWriter       *accountKVLatestBatch
	latestReader       *commitScopeLatestReader
	tx                 *statedomains.SharedDomainTx
	commits            uint64
}

type commitScopeCommitment struct {
	current *DomainCommitmentState
}

// NewCommitScope prepares reusable domain-transaction state for staged range
// execution. The caller must create the scope after the StateDB has been
// configured with its active latest-domain index store.
func (s *StateDB) NewCommitScope() *CommitScope {
	scope := &CommitScope{state: s}
	resolveGeneration := func(owner tcommon.Address) (uint64, error) {
		if scope.generationResolver == nil {
			return 0, fmt.Errorf("state commit scope: missing account kv generation for %s", owner.Hex())
		}
		return scope.generationResolver(owner)
	}
	index := s.accountKVIndex()
	scope.history = NewDomainHistoryState(s, 0)
	scope.commitment = &commitScopeCommitment{}
	scope.commitmentState = NewDomainCommitmentState(s)
	scope.latestWriter = newAccountKVLatestDomainBatch(index, resolveGeneration, &s.changeSet, nil)
	// The scope's read-your-writes overlay can satisfy a storage-cache miss
	// before the request reaches blockbuffer/Pebble. cold_pending_resolved is a
	// strict subset of cold; their difference crossed the scope pending overlay.
	scope.latestWriter.storagePendingResolved = &s.storageObservability.coldPendingResolved
	scope.latestReader = &commitScopeLatestReader{writer: scope.latestWriter, state: s}
	scope.tx = statedomains.NewSharedDomainTx(statedomains.SharedDomainTxConfig{
		Latest:          scope.latestReader,
		Writer:          scope.latestWriter,
		History:         scope.history,
		Commitment:      scope.commitment,
		UnindexedWrites: true,
	})
	s.setAccountKVLatestView(scope.latestReader, scope.latestReader)
	s.flatLatestReader = scope.latestReader
	return scope
}

func (s *StateDB) CommitWithStatsOptionsInScope(scope *CommitScope, opts CommitOptions) (tcommon.Hash, CommitStats, error) {
	return s.commitWithStatsOptions(opts, scope)
}

func (scope *CommitScope) Close() error {
	if scope == nil || scope.tx == nil {
		return nil
	}
	if err := scope.FlushLatest(); err != nil {
		return err
	}
	if err := scope.tx.Close(); err != nil {
		return err
	}
	scope.discard()
	return nil
}

func (scope *CommitScope) FlushLatest() error {
	if scope == nil || scope.latestWriter == nil {
		return nil
	}
	return scope.latestWriter.flush()
}

func (scope *CommitScope) FlushLatestUpTo(cutoff uint64) error {
	if scope == nil || scope.latestWriter == nil {
		return nil
	}
	return scope.latestWriter.flushUpTo(cutoff)
}

func (scope *CommitScope) Abort() error {
	if scope == nil {
		return nil
	}
	err := scope.FlushLatestCommitted()
	if scope.tx != nil {
		if closeErr := scope.tx.Close(); err == nil {
			err = closeErr
		}
	}
	scope.discard()
	return err
}

func (scope *CommitScope) Discard() {
	if scope == nil {
		return
	}
	if scope.tx != nil {
		_ = scope.tx.Close()
	}
	scope.discard()
}

func (scope *CommitScope) FlushLatestCommitted() error {
	if scope == nil || scope.latestWriter == nil {
		return nil
	}
	return scope.latestWriter.flushCommitted(true)
}

func (scope *CommitScope) discard() {
	if scope == nil {
		return
	}
	if scope.tx != nil {
		scope.tx.Discard()
	}
	if scope.latestWriter != nil {
		scope.latestWriter.reset()
	}
	if scope.commitment != nil {
		scope.commitment.current = nil
	}
	if scope.commitmentState != nil {
		scope.commitmentState.release()
	}
	if scope.latestReader != nil {
		scope.latestReader.generation = nil
	}
	scope.detachLatestView()
	scope.generationResolver = nil
}

func (scope *CommitScope) prepare(generation statedomains.GenerationResolver, commitment *DomainCommitmentState, txNum uint64) error {
	if scope == nil || scope.tx == nil || scope.latestWriter == nil || scope.latestReader == nil || scope.history == nil || scope.commitment == nil {
		return errors.New("state commit scope: incomplete scope")
	}
	scope.generationResolver = generation
	scope.latestReader.generation = generation
	if commitment != nil {
		commitment.latestReader = scope.latestReader
	}
	scope.commitment.current = commitment
	scope.history.SetHeadTxNum(txNum)
	scope.tx.SetTxNum(txNum)
	scope.commits++
	return nil
}

type commitScopeLatestReader struct {
	writer     *accountKVLatestBatch
	state      *StateDB
	generation statedomains.GenerationResolver
}

func (r *commitScopeLatestReader) GetLatest(owner tcommon.Address, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, false, nil
	}
	generation, err := r.resolveGeneration(owner)
	if err != nil {
		return nil, false, err
	}
	return r.writer.readLatest(owner, generation, domain, key)
}

func (r *commitScopeLatestReader) resolveGeneration(owner tcommon.Address) (uint64, error) {
	if r == nil {
		return 0, nil
	}
	if r.generation != nil {
		return r.generation(owner)
	}
	if r.state != nil {
		if obj := r.state.getStateObject(owner); obj != nil {
			return obj.accountKVGeneration, nil
		}
		generation, ok, err := r.state.readStateKVGeneration(owner)
		if err != nil || ok {
			return generation, err
		}
	}
	return 0, nil
}

func (r *commitScopeLatestReader) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, false, nil
	}
	return r.writer.readAccountLatest(owner)
}

func (r *commitScopeLatestReader) accountLatestForCommitment(owner tcommon.Address) ([]byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, false, nil
	}
	return r.writer.readAccountLatestForCommitment(owner)
}

func (r *commitScopeLatestReader) accountLatestForHydration(owner tcommon.Address) ([]byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, false, nil
	}
	return r.writer.readAccountLatestForHydration(owner)
}

func (r *commitScopeLatestReader) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	if r == nil || r.writer == nil {
		return 0, false, nil
	}
	return r.writer.readKVGeneration(owner)
}

func (r *commitScopeLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, false, nil
	}
	return r.writer.readLatest(owner, generation, domain, key)
}

func (r *commitScopeLatestReader) kvLatestForDecoding(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	if r == nil || r.writer == nil {
		return nil, false, nil
	}
	return r.writer.readLatestForDecoding(owner, generation, domain, key)
}

func (r *commitScopeLatestReader) KVLatestPrefix(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix []byte, fn func(key, value []byte) (bool, error)) error {
	if r == nil || r.writer == nil {
		return nil
	}
	return r.writer.iterateLatestPrefix(owner, generation, domain, prefix, fn)
}

func (r *commitScopeLatestReader) DomainIterate(owner tcommon.Address, domain kvdomains.KVDomain, prefix []byte, fn statedomains.IterateFunc) error {
	if r == nil || r.writer == nil {
		return nil
	}
	generation, err := r.resolveGeneration(owner)
	if err != nil {
		return err
	}
	return r.writer.iterateLatestPrefix(owner, generation, domain, prefix, fn)
}

func (scope *CommitScope) detachLatestView() {
	if scope == nil || scope.state == nil {
		return
	}
	if scope.state.accountKVLatestReader == scope.latestReader {
		scope.state.accountKVLatestReader = nil
	}
	if scope.state.accountKVLatestIterator == scope.latestReader {
		scope.state.accountKVLatestIterator = nil
	}
	if scope.state.flatLatestReader == scope.latestReader {
		scope.state.flatLatestReader = nil
	}
}

func (c *commitScopeCommitment) RecordCommitmentMutations(ctx context.Context, mutations []statedomains.Mutation) error {
	if c == nil || c.current == nil {
		return nil
	}
	return c.current.RecordCommitmentMutations(ctx, mutations)
}

func (c *commitScopeCommitment) SeekCommitment(ctx context.Context) (uint64, uint64, error) {
	if c == nil || c.current == nil {
		return 0, 0, statedomains.ErrNilCommitmentProcessor
	}
	return c.current.SeekCommitment(ctx)
}

func (c *commitScopeCommitment) ComputeCommitment(ctx context.Context, blockNum, txNum uint64) (tcommon.Hash, error) {
	if c == nil || c.current == nil {
		return tcommon.Hash{}, statedomains.ErrNilCommitmentProcessor
	}
	return c.current.ComputeCommitment(ctx, blockNum, txNum)
}

// Add folds another commit breakdown into this one.
func (s *CommitStats) Add(o CommitStats) {
	s.Prepare += o.Prepare
	s.FlatWrite += o.FlatWrite
	s.FlatFlush += o.FlatFlush
	s.KVCompute += o.KVCompute
	s.KVNodeWrite += o.KVNodeWrite
	s.AccountTrieUpdate += o.AccountTrieUpdate
	s.AccountTrieMarshal += o.AccountTrieMarshal
	s.AccountTrieGeneration += o.AccountTrieGeneration
	s.AccountTrieWrite += o.AccountTrieWrite
	s.Finalize += o.Finalize
	s.AccountTrieCommit += o.AccountTrieCommit
	s.TrieNodeWrite += o.TrieNodeWrite
	s.TrieNodeFlush += o.TrieNodeFlush
	s.Reopen += o.Reopen
	s.Accounts += o.Accounts
	s.KVAccounts += o.KVAccounts
	s.KVItems += o.KVItems
	s.DeferredKVAccounts += o.DeferredKVAccounts
	s.DeferredKVItems += o.DeferredKVItems
	s.RebuiltKVAccounts += o.RebuiltKVAccounts
	s.RebuiltKVItems += o.RebuiltKVItems
	s.CommitmentUpdates += o.CommitmentUpdates
	s.Mutations.Add(o.Mutations)
}

// Total returns the sum of measured Commit subphases.
func (s CommitStats) Total() time.Duration {
	return s.Prepare +
		s.FlatWrite +
		s.FlatFlush +
		s.KVCompute +
		s.KVNodeWrite +
		s.AccountTrieUpdate +
		s.Finalize +
		s.AccountTrieCommit +
		s.TrieNodeWrite +
		s.TrieNodeFlush +
		s.Reopen
}

type domainChangeSetCapture struct {
	writer          ethdb.KeyValueWriter
	blockNum        uint64
	blockHash       tcommon.Hash
	beginTxNum      uint64
	endTxNum        uint64
	txNum           uint64
	seq             uint64
	journalMark     int
	captureAtCommit bool
	enabled         bool
}

// AccountSnapshot is a compact view of the account envelope needed to hydrate
// a state object without re-reading account latest rows. It intentionally
// carries the generic-KV generation and code hash alongside the account proto.
type AccountSnapshot struct {
	Account             *types.Account
	AccountProto        []byte
	AccountKVRoot       tcommon.Hash
	AccountKVGeneration uint64
	CodeHash            tcommon.Hash
}

// New creates a flat-domain StateDB from the given CommitmentDomain root.
func New(root tcommon.Hash, db *Database) (*StateDB, error) {
	return &StateDB{
		db:              db,
		stateObjects:    make(map[tcommon.Address]*stateObject),
		witnesses:       make(map[tcommon.Address]*types.Witness),
		dirtyWitnesses:  make(map[tcommon.Address]struct{}),
		txFinalizeDirty: make(map[tcommon.Address]struct{}),
		dirtyObjects:    make(map[tcommon.Address]struct{}),
		journal:         newJournal(),
		dynProps:        NewDynamicProperties(),
		originRoot:      ethcommon.Hash(root),
		codeStore:       newDefaultStateCodeStore(db),
	}, nil
}

// Error returns the first durable-state access error observed by this view.
// It intentionally survives journal reverts: reverting mutations cannot repair
// a corrupt or unreadable rooted row.
func (s *StateDB) Error() error {
	if s == nil {
		return nil
	}
	return s.dbErr
}

func (s *StateDB) setError(err error) {
	if s != nil && err != nil && s.dbErr == nil {
		s.dbErr = err
	}
}

// recordStateError makes a durable-state read, decode or write failure sticky
// while returning the same contextual error to strict callers. State mutations
// may be journal-reverted, but a corrupt or unreadable rooted row cannot be
// repaired by continuing the transaction with a zero-value substitute.
func (s *StateDB) recordStateError(context string, err error) error {
	if err == nil {
		return nil
	}
	wrapped := fmt.Errorf("%s: %w", context, err)
	s.setError(wrapped)
	return wrapped
}

// NewFlat is an explicit alias for New. New databases have only one state
// layout: flat latest domains plus CommitmentDomain.
func NewFlat(root tcommon.Hash, db *Database) (*StateDB, error) {
	return New(root, db)
}

// SetAccountKVIndexStore overrides the physical latest-state index view used
// by account-KV iteration and commit-time index writes. Block execution passes
// the blockbuffer here so unsolidified latest rows are fork-rewindable.
func (s *StateDB) SetAccountKVIndexStore(store interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}) {
	s.accountKVIndexStore = store
}

// SetAccountKVIndexReads is retained as a no-op shim. Flat latest rows are the
// only account-KV read path.
func (s *StateDB) SetAccountKVIndexReads(on bool) {
}

// SetCodeColdHistory enables content-addressed CodeDomain snapshot fallback
// for live code reads. txNum is the latest state txNum this StateDB may read
// from; code is immutable, so older CodeDomain snapshots at or before txNum
// are valid fallback sources when hot state-code rows have been pruned.
func (s *StateDB) SetCodeColdHistory(source StateCodeColdHistoryAtOrBefore, txNum uint64) {
	s.codeColdHistory = source
	s.codeColdTxNum = txNum
}

// SetCommitmentColdHistory enables CommitmentDomain branch-node snapshot
// repair during commit. txNum identifies the cold latest snapshot that matches
// the hot state root being extended.
func (s *StateDB) SetCommitmentColdHistory(source statedomains.CommitmentSnapshotSource, txNum uint64) {
	s.commitmentColdHistory = source
	s.commitmentColdTxNum = txNum
}

// SetDomainChangeSetWriter enables block-level domain change capture for the
// next Commit. This compatibility helper writes a single block-final txNum.
func (s *StateDB) SetDomainChangeSetWriter(writer ethdb.KeyValueWriter, blockNum uint64, blockHash tcommon.Hash) {
	s.SetDomainChangeSetWriterRange(writer, blockNum, blockHash, blockNum, blockNum)
}

// SetDomainChangeSetWriterRange enables block-level domain change capture for
// the next Commit and records the txNum range reserved for the block. Domain
// changes written by this Commit are still tagged with the block-final txNum;
// the range is present so later per-transaction flushes can fill earlier txNums
// without changing the persisted StateTxRange contract.
func (s *StateDB) SetDomainChangeSetWriterRange(writer ethdb.KeyValueWriter, blockNum uint64, blockHash tcommon.Hash, beginTxNum, endTxNum uint64) {
	if writer == nil {
		s.changeSet = domainChangeSetCapture{}
		return
	}
	s.changeSet = domainChangeSetCapture{
		writer:          writer,
		blockNum:        blockNum,
		blockHash:       blockHash,
		beginTxNum:      beginTxNum,
		endTxNum:        endTxNum,
		txNum:           endTxNum,
		journalMark:     s.journal.length(),
		captureAtCommit: true,
		enabled:         true,
	}
}

// BeginDomainChangeJournalCapture enables Erigon-style temporal capture for
// the current block. Domain changes are emitted from journal deltas at
// transaction/block-final boundaries; Commit only writes latest rows and the
// StateTxRange metadata.
func (s *StateDB) BeginDomainChangeJournalCapture(writer ethdb.KeyValueWriter, blockNum uint64, blockHash tcommon.Hash, beginTxNum, endTxNum uint64) {
	s.SetDomainChangeSetWriterRange(writer, blockNum, blockHash, beginTxNum, endTxNum)
	s.changeSet.captureAtCommit = false
	s.changeSet.journalMark = s.journal.length()
}

// SetDomainChangeTxNum sets the txNum stamped on subsequently flushed
// StateDomainChange rows. If the txNum falls outside the currently reserved
// block range, the range is widened so the persisted StateTxRange still covers
// every change emitted by this StateDB instance.
func (s *StateDB) SetDomainChangeTxNum(txNum uint64) {
	if !s.changeSet.enabled {
		return
	}
	s.changeSet.txNum = txNum
	if txNum < s.changeSet.beginTxNum {
		s.changeSet.beginTxNum = txNum
	}
	if txNum > s.changeSet.endTxNum {
		s.changeSet.endTxNum = txNum
	}
}

func (s *StateDB) DomainChangeTxNumAtOrdinal(ordinal uint64) (uint64, error) {
	if s == nil || !s.changeSet.enabled {
		return rawdb.StateTxNumAt(0, ordinal)
	}
	return rawdb.StateTxNumAt(s.changeSet.beginTxNum, ordinal)
}

func (s *StateDB) DomainChangeJournalMark() int {
	if s == nil {
		return 0
	}
	return s.journal.length()
}

func (s *StateDB) FlushPendingDomainChanges(txNum uint64) error {
	if s == nil || !s.changeSet.enabled || s.changeSet.captureAtCommit {
		return nil
	}
	return s.FlushDomainChangesSince(s.changeSet.journalMark, txNum)
}

// GetAccount returns the account at addr, or nil if not found.
func (s *StateDB) GetAccount(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	if err := s.materializeAccountAux(obj); err != nil {
		return nil
	}
	if err := s.materializeAccountPermissions(obj); err != nil {
		return nil
	}
	if err := s.materializeAccountVotes(obj); err != nil {
		return nil
	}
	if err := s.materializeAccountStakeV1(obj); err != nil {
		return nil
	}
	if err := s.materializeAccountStakeV2(obj); err != nil {
		return nil
	}
	if err := s.materializeAccountFrozenSupply(obj); err != nil {
		return nil
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return nil
	}
	return obj.account
}

// LoadAccount hydrates an account into the in-memory object cache without
// marking it dirty or appending to the journal. The caller must provide an
// account matching this StateDB's origin root.
func (s *StateDB) LoadAccount(acc *types.Account) {
	if acc == nil {
		return
	}
	addr := acc.Address()
	if obj, ok := s.stateObjects[addr]; ok {
		s.touchStateObject(obj)
		return
	}
	if obj := s.getStateObject(addr); obj != nil {
		obj.account = acc.Copy()
		return
	}
	obj := s.newStateObject(addr, acc.Copy())
	obj.dirtySet = s.dirtyObjects
	s.stateObjects[addr] = obj
	s.lastStateObject = obj
	s.touchStateObject(obj)
}

// LoadAccountReference hydrates an account into the in-memory object cache
// without copying it. This is only for per-block hot-path caches that are
// cleared if block processing fails before commit.
func (s *StateDB) LoadAccountReference(acc *types.Account) {
	if acc == nil {
		return
	}
	addr := acc.Address()
	if obj, ok := s.stateObjects[addr]; ok {
		s.touchStateObject(obj)
		return
	}
	if obj := s.getStateObject(addr); obj != nil {
		obj.account = acc
		return
	}
	obj := s.newStateObject(addr, acc)
	obj.dirtySet = s.dirtyObjects
	s.stateObjects[addr] = obj
	s.lastStateObject = obj
	s.touchStateObject(obj)
}

// LoadAccountSnapshotReference hydrates an account envelope into the in-memory
// object cache without copying the account. It is for hot-path block caches
// that are discarded if block processing fails before commit.
func (s *StateDB) LoadAccountSnapshotReference(snapshot *AccountSnapshot) {
	if snapshot == nil || snapshot.Account == nil {
		return
	}
	addr := snapshot.Account.Address()
	if obj, ok := s.stateObjects[addr]; ok {
		s.touchStateObject(obj)
		return
	}
	obj := s.newStateObject(addr, snapshot.Account)
	obj.accountProto = snapshot.AccountProto
	obj.accountKVRoot = snapshot.AccountKVRoot
	obj.accountKVGeneration = snapshot.AccountKVGeneration
	obj.accountKVGenerationDirty = false
	obj.codeHash = snapshot.CodeHash
	obj.dirtySet = s.dirtyObjects
	s.stateObjects[addr] = obj
	s.lastStateObject = obj
	s.touchStateObject(obj)
}

// CopyAccount returns a detached copy of the cached/live account.
func (s *StateDB) CopyAccount(addr tcommon.Address) *types.Account {
	account := s.GetAccount(addr)
	if account == nil {
		return nil
	}
	return account.Copy()
}

// AccountReference returns the cached/live account pointer without copying it.
// Callers must treat the returned account as immutable unless they own the
// StateDB lifecycle and clear any external cache on failure.
func (s *StateDB) AccountReference(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account
}

// AccountResourceReference returns the live account with only its resource row
// materialized. Callers must restrict themselves to resource-bearing fields;
// unlike AccountReference this records a typed resource dependency rather than
// consuming the whole Account envelope version.
func (s *StateDB) AccountResourceReference(addr tcommon.Address) *types.Account {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldFrozenResource)
	if obj == nil || obj.deleted {
		return nil
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return nil
	}
	return obj.account
}

// AccountSnapshotReferenceInto refreshes snapshot with a cacheable view of the
// account envelope without copying the account proto. A nil snapshot allocates
// one; callers that cache the result across successful blocks can pass it back
// to avoid replacing the same small envelope on every block. Mutating the
// returned Account mutates the cached state object, so callers must only keep
// this across block boundaries where the snapshot is refreshed immediately
// after Commit.
func (s *StateDB) AccountSnapshotReferenceInto(addr tcommon.Address, snapshot *AccountSnapshot) *AccountSnapshot {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	if snapshot == nil {
		snapshot = new(AccountSnapshot)
	}
	*snapshot = AccountSnapshot{
		Account:             obj.account,
		AccountProto:        obj.accountProto,
		AccountKVRoot:       obj.accountKVRoot,
		AccountKVGeneration: obj.accountKVGeneration,
		CodeHash:            obj.codeHash,
	}
	return snapshot
}

// AccountSnapshotReference returns a newly allocated cacheable account
// envelope. Long-lived hot-path caches should use AccountSnapshotReferenceInto
// to refresh their existing storage.
func (s *StateDB) AccountSnapshotReference(addr tcommon.Address) *AccountSnapshot {
	return s.AccountSnapshotReferenceInto(addr, nil)
}

// GetOrCreateAccount returns the state object at addr, creating it if it doesn't exist.
// When a new account is created, a nil-prev journal entry is recorded so that
// snapshot revert can delete it. Because this exported method exposes the
// otherwise-private wrapper identity, that wrapper is not reused after cache
// eviction. StateDB's own mutators use getOrCreateAccount instead.
func (s *StateDB) GetOrCreateAccount(addr tcommon.Address) *stateObject {
	obj := s.getOrCreateAccount(addr)
	if obj != nil {
		obj.wrapperEscaped = true
	}
	return obj
}

func (s *StateDB) getOrCreateAccount(addr tcommon.Address) *stateObject {
	return s.getOrCreateAccountLoaded(addr, s.getStateObject(addr))
}

func (s *StateDB) getOrCreateAccountForField(addr tcommon.Address, field TransactionAccountField) *stateObject {
	return s.getOrCreateAccountLoaded(addr, s.getStateObjectForField(addr, field))
}

func (s *StateDB) getOrCreateAccountLoaded(addr tcommon.Address, obj *stateObject) *stateObject {
	if obj != nil && !obj.deleted {
		return obj
	}
	// Journal the pre-create shape so revert can restore either a truly
	// missing account or a pending-delete object from an earlier tx in the
	// same block.
	s.journalAccount(addr, obj)
	nextGeneration := s.nextAccountKVGeneration(addr, obj)
	// Any exact storage sample belongs to the old object/incarnation. Settle it
	// before replacing the stateObjects entry so the recreated account cannot
	// produce false sampled-key reuse.
	s.settleOversizedStorageSample(addr)
	obj = s.newEmptyStateObject(addr)
	obj.accountKVGeneration = nextGeneration
	// A non-zero generation means this is a recreate after SELFDESTRUCT: the
	// counter was bumped past the destroyed incarnation, which is semantically a
	// generation reset (a fresh KV namespace). Record it exactly as
	// ResetAccountKV does so the bump is reflected everywhere archive history is
	// built:
	//   - mark the generation dirty so Commit writes the bumped KVGeneration row
	//     (writeAccountKVGeneration);
	//   - journal a kvResetChange so the journal-capture path emits the
	//     StateFlatDomainKVGeneration change-set entry (collectJournalDomainChanges
	//     only derives generation changes from kvResetChange).
	// Without the change-set entry, row-seeded archive readers
	// (rawdb.ReadStateAccountKVAsOfTxNum, reached via GetAccountKVAsOf) cannot
	// cross the generation boundary and leak the destroyed account's storage into
	// the recreated account. Live execution and StorageAt read the generation
	// from the in-memory object / envelope, so this never affected execution or
	// the java AccountStateRoot. A fresh account (generation 0) records nothing,
	// matching prior behavior.
	if nextGeneration > 0 {
		s.recordAccountKVGenerationWrite(addr)
		s.journal.append(kvResetChange{
			address:              addr,
			prevRoot:             EmptyKVRoot,
			prevGeneration:       nextGeneration - 1,
			prevGenerationExists: true,
			prevGenerationDirty:  false,
			prevDirty:            make(map[string]kvEntry),
		})
		obj.accountKVGenerationDirty = true
	}
	// Recreating an address after SELFDESTRUCT must not resurrect stale code
	// or contract metadata from rawdb. java-tron deletes CodeStore and
	// ContractStore alongside the account; keep that deletion intent on the
	// new in-memory object until Commit removes the raw keys.
	obj.codeDirty = true
	obj.contractMetaDirty = true
	// The new object is born dirty (created/accountDirty); record it directly
	// since this path constructs the object instead of going through markDirty.
	obj.dirtySet = s.dirtyObjects
	s.dirtyObjects[addr] = struct{}{}
	s.stateObjects[addr] = obj
	s.lastStateObject = obj
	s.touchStateObject(obj)
	return obj
}

// GetBalance returns the TRX balance of the account.
func (s *StateDB) GetBalance(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldBalance)
	if obj == nil {
		return 0
	}
	return obj.account.Balance()
}

// AddBalance adds amount to the account's balance.
func (s *StateDB) AddBalance(addr tcommon.Address, amount int64) {
	obj := s.getOrCreateAccountForField(addr, TransactionAccountFieldBalance)
	s.recordAccountFieldWrite(addr, TransactionAccountFieldBalance)
	s.journalAccountScalars(addr, obj)
	obj.account.SetBalance(obj.account.Balance() + amount)
	obj.markDirty()
	s.recordBalanceTraceChange(addr, amount)
}

// AddSettlementBalance performs the same canonical serial mutation as
// AddBalance, but labels the destination-side read-modify-write as a
// commutative protocol settlement. It is intentionally reserved for fees
// credited to the blackhole account; ordinary transfers must use AddBalance so
// their dependency remains versioned normally.
func (s *StateDB) AddSettlementBalance(addr tcommon.Address, amount int64) {
	if s == nil {
		return
	}
	if recorder := s.transactionAccess; recorder != nil {
		key := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}
		recorder.recordCommutativeDelta(key, amount)
		previousKey, previousOpen := recorder.beginCommutativeScope(key)
		s.AddBalance(addr, amount)
		recorder.endCommutativeScope(previousKey, previousOpen)
		return
	}
	s.AddBalance(addr, amount)
}

// SubBalance subtracts amount from the account's balance.
func (s *StateDB) SubBalance(addr tcommon.Address, amount int64) error {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldBalance)
	if obj == nil {
		return ErrInsufficientBalance
	}
	if obj.account.Balance() < amount {
		return ErrInsufficientBalance
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldBalance)
	s.journalAccountScalars(addr, obj)
	obj.account.SetBalance(obj.account.Balance() - amount)
	obj.markDirty()
	s.recordBalanceTraceChange(addr, -amount)
	return nil
}

// GetTRC10Balance returns the split Account.assetV2 value for tokenID.
func (s *StateDB) GetTRC10Balance(addr tcommon.Address, tokenID int64) int64 {
	return s.trc10Balance(addr, kvdomains.AccountAssetV2, s.trc10TokenKey(tokenID))
}

// GetTRC10BalanceByName returns the legacy pre-AllowSameTokenName TRC10
// balance stored in Account.asset keyed by token name.
func (s *StateDB) GetTRC10BalanceByName(addr tcommon.Address, name []byte) int64 {
	return s.trc10Balance(addr, kvdomains.AccountAsset, name)
}

// SetTRC10Balance writes one split Account.assetV2 row.
func (s *StateDB) SetTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	s.setTRC10BalanceKey(addr, kvdomains.AccountAssetV2, s.trc10TokenKey(tokenID), amount)
}

// SetTRC10BalanceByName sets the legacy Account.asset balance keyed by token name.
func (s *StateDB) SetTRC10BalanceByName(addr tcommon.Address, name []byte, amount int64) {
	s.setTRC10BalanceKey(addr, kvdomains.AccountAsset, name, amount)
}

// SetTRC10BalanceLegacyAndV2 mirrors java-tron AccountCapsule.addAssetAmountV2
// before AllowSameTokenName: the legacy Account.asset value is authoritative,
// and Account.assetV2 is kept in lockstep under the token ID.
func (s *StateDB) SetTRC10BalanceLegacyAndV2(addr tcommon.Address, name []byte, tokenID int64, amount int64) {
	s.setTRC10BalanceKey(addr, kvdomains.AccountAsset, name, amount)
	s.setTRC10BalanceKey(addr, kvdomains.AccountAssetV2, s.trc10TokenKey(tokenID), amount)
}

// setTRC10BalanceLegacyAndV2Changed is the arithmetic-mutation counterpart of
// SetTRC10BalanceLegacyAndV2. The legacy balance was just read to compute
// amount, while the pre-AllowSameTokenName V2 row is a required mirror. Avoid
// rereading that mirror only to establish its pre-image; rollback and optional
// history capture retain their exact semantics through the journal path.
func (s *StateDB) setTRC10BalanceLegacyAndV2Changed(addr tcommon.Address, name []byte, tokenID int64, amount int64) {
	s.setTRC10BalanceKey(addr, kvdomains.AccountAsset, name, amount)
	_ = s.setAccountAuxValueUnconditional(addr, kvdomains.AccountAssetV2, s.trc10TokenKey(tokenID), amount)
}

func (s *StateDB) GetTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, allowSameTokenName bool) int64 {
	value, _ := s.GetTRC10BalanceFinalStrict(addr, name, tokenID, allowSameTokenName)
	return value
}

// GetTRC10BalanceFinalStrict is the consensus-path balance reader. Unlike the
// compatibility getter, it surfaces malformed split rows directly while also
// poisoning the StateDB so transaction boundaries cannot continue execution.
func (s *StateDB) GetTRC10BalanceFinalStrict(addr tcommon.Address, name []byte, tokenID int64, allowSameTokenName bool) (int64, error) {
	var domain kvdomains.KVDomain
	var key []byte
	if allowSameTokenName {
		domain = kvdomains.AccountAssetV2
		key = s.trc10TokenKey(tokenID)
	} else {
		domain = kvdomains.AccountAsset
		key = name
	}
	value, _, err := s.accountAuxValue(addr, domain, key)
	return value, err
}

// SetTRC10BalanceFinal writes an already-computed final balance without first
// reading the current balance again. Callers that validated the arithmetic can
// therefore carry the same read through execution. Before
// AllowSameTokenName, the legacy name-keyed value remains authoritative and
// the V2 id-keyed value is updated as java-tron's mandatory migration mirror.
func (s *StateDB) SetTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) {
	_ = s.SetTRC10BalanceFinalStrict(addr, name, tokenID, amount, allowSameTokenName)
}

// SetTRC10BalanceFinalStrict is the error-returning execution counterpart used
// by TransferAsset. Before AllowSameTokenName both authoritative and mirror
// legs remain transaction-journaled, but no write error is discarded.
func (s *StateDB) SetTRC10BalanceFinalStrict(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) error {
	if allowSameTokenName {
		return s.setAccountAuxValue(addr, kvdomains.AccountAssetV2, s.trc10TokenKey(tokenID), amount)
	}
	if err := s.setAccountAuxValue(addr, kvdomains.AccountAsset, name, amount); err != nil {
		return err
	}
	return s.setAccountAuxValueUnconditional(addr, kvdomains.AccountAssetV2, s.trc10TokenKey(tokenID), amount)
}

func (s *StateDB) AddTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) {
	if allowSameTokenName {
		s.AddTRC10Balance(addr, tokenID, amount)
		return
	}
	current := s.GetTRC10BalanceByName(addr, name)
	s.setTRC10BalanceLegacyAndV2Changed(addr, name, tokenID, current+amount)
}

func (s *StateDB) SubTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) error {
	if allowSameTokenName {
		return s.SubTRC10Balance(addr, tokenID, amount)
	}
	current := s.GetTRC10BalanceByName(addr, name)
	if current < amount {
		return ErrInsufficientBalance
	}
	s.setTRC10BalanceLegacyAndV2Changed(addr, name, tokenID, current-amount)
	return nil
}

// SetAssetIssued records the issued TRC10 token's name and ID on the issuer
// account, mirroring java-tron's AssetIssueActuator (accountCapsule
// .setAssetIssuedName / .setAssetIssuedID). These fields are part of the
// persisted account proto, so they must live in state — not be derived at
// read time — or the conformance digest diverges at the issuance block.
func (s *StateDB) SetAssetIssued(addr tcommon.Address, name []byte, id string) {
	obj := s.getOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.AssetIssuedName = name
	pb.AssetIssued_ID = []byte(id)
	obj.markDirty()
}

// AddFrozenSupply appends split frozen-supply rows in java-tron list order.
func (s *StateDB) AddFrozenSupply(addr tcommon.Address, frozen []*corepb.Account_Frozen) {
	if len(frozen) == 0 {
		return
	}
	obj := s.getOrCreateAccount(addr)
	_ = s.addAccountFrozenSupply(obj, frozen)
}

func (s *StateDB) RemoveExpiredFrozenSupply(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	amount, err := s.removeExpiredAccountFrozenSupply(obj, now)
	if err != nil {
		return 0
	}
	return amount
}

// AddTRC10Balance credits amount TRC10 tokens to addr.
func (s *StateDB) AddTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	s.SetTRC10Balance(addr, tokenID, s.GetTRC10Balance(addr, tokenID)+amount)
}

// SubTRC10Balance debits amount TRC10 tokens from addr.
// Returns ErrInsufficientBalance if addr has fewer than amount tokens.
func (s *StateDB) SubTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) error {
	current := s.GetTRC10Balance(addr, tokenID)
	if current < amount {
		return ErrInsufficientBalance
	}
	s.SetTRC10Balance(addr, tokenID, current-amount)
	return nil
}

// TransferAllTRC10Balance moves every AssetV2 token balance from one account
// to another, leaving explicit zero entries on the source account. This
// mirrors java-tron MUtil.transferAllToken, used by SELFDESTRUCT.
func (s *StateDB) TransferAllTRC10Balance(from, to tcommon.Address) {
	fromObj := s.getStateObject(from)
	if fromObj == nil || fromObj.account == nil {
		return
	}
	balances := make(map[string]int64)
	if err := s.IterateAccountKV(from, kvdomains.AccountAssetV2, nil, func(key, value []byte) (bool, error) {
		amount, err := decodeAccountAuxInt64(value)
		if err != nil {
			return false, err
		}
		balances[string(key)] = amount
		return true, nil
	}); err != nil {
		return
	}
	for tokenID, amount := range balances {
		key := ownedStringBytes(tokenID)
		toAmount := s.trc10Balance(to, kvdomains.AccountAssetV2, key)
		s.setTRC10BalanceKey(to, kvdomains.AccountAssetV2, key, toAmount+amount)
		s.setTRC10BalanceKey(from, kvdomains.AccountAssetV2, key, 0)
	}
}

// IsFrozenClaimed returns whether frozen_supply entry at index has been claimed.
func (s *StateDB) IsFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) bool {
	v := s.GetState(addr, trc10FrozenClaimedSlot(tokenID, index))
	return v[31] != 0
}

// SetFrozenClaimed marks frozen_supply entry at index as claimed.
//
// Pre-warms the storage cache via GetState so that SetState's journal entry
// records the real disk pre-value (not zero). Callers in production today
// (UnfreezeSupplyActuator) already pre-warm via IsFrozenClaimed, but the
// pre-warm here defends against future direct callers and keeps this
// function structurally aligned with writeHistoryBlockHash's fix — both
// are direct-SetState paths bypassing the TVM's opSload pre-warm.
func (s *StateDB) SetFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) {
	slot := trc10FrozenClaimedSlot(tokenID, index)
	_ = s.GetState(addr, slot)
	var v tcommon.Hash
	v[31] = 0x01
	s.SetState(addr, slot, v)
}

// AddFreezeV2 adds a freeze entry for the given resource type.
func (s *StateDB) AddFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.addAccountFrozenV2(obj, resourceType, amount)
}

// ClearV2Freeze zeroes an account's Stake-2.0 freeze state in place, mirroring
// java-tron's clearOwnerFreezeV2 (clearFrozenV2 + setNetUsage(0) +
// setNewWindowSize(BANDWIDTH,0) + setEnergyUsage(0) + setNewWindowSize(ENERGY,0)
// + clearUnfrozenV2) used by the SELFDESTRUCT path under allow_tvm_freeze_v2.
// Only the window *size* is zeroed (the optimized flag is left as the preceding
// usage recovery set it, matching java setNewWindowSize); the latest-consume
// times are likewise left untouched here.
func (s *StateDB) ClearV2Freeze(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return
	}
	s.journalAccount(addr, obj)
	_ = s.clearAccountFrozenV2(obj)
	obj.account.SetNetUsage(0)
	obj.account.SetNewNetWindowSize(0)
	obj.account.SetEnergyUsage(0)
	obj.account.SetNewEnergyWindowSize(0)
	_ = s.writeAccountResource(obj)
	_ = s.clearAccountUnfrozenV2(obj)
	obj.markDirty()
}

// --- V1 Stake (Stake 1.0) StateDB methods ---

func (s *StateDB) FreezeV1Bandwidth(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	rows, err := s.accountFrozenBandwidthRows(obj)
	if err != nil {
		return
	}
	var total int64
	for _, row := range rows {
		total += row.entry.FrozenBalance
	}
	_ = s.writeAccountFrozenBandwidthReplacing(obj, rows, []*corepb.Account_Frozen{{
		FrozenBalance: total + amount,
		ExpireTime:    expireTimeMs,
	}})
}

func (s *StateDB) UnfreezeV1Bandwidth(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	refunded, err := s.removeExpiredAccountFrozenBandwidth(obj, blockTimeMs)
	if err != nil {
		return 0
	}
	return refunded
}

func (s *StateDB) FreezeV1Energy(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
		obj.account.AddFrozenEnergy(amount, expireTimeMs)
	})
}

// ClearV1Freeze zeroes an account's V1 frozen bandwidth and energy slots in
// place, mirroring java-tron's clearOwnerFreeze (AccountCapsule
// setFrozenForBandwidth(0,0) / setFrozenForEnergy(0,0)) used by the SELFDESTRUCT
// path under allow_tvm_selfdestruct_restriction. Both slots are left
// present-but-zero to match java's proto encoding.
func (s *StateDB) ClearV1Freeze(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return
	}
	_ = s.setAccountFrozenBandwidth(obj, 0, 0)
	obj.account.SetFrozenEnergy(0, 0)
	_ = s.writeAccountResource(obj)
}

func (s *StateDB) FreezeV1TronPower(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.addAccountTronPower(obj, amount, expireTimeMs)
}

func (s *StateDB) UnfreezeV1TronPower(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	amount, err := s.removeExpiredAccountTronPower(obj, blockTimeMs)
	if err != nil {
		return 0
	}
	return amount
}

func (s *StateDB) UnfreezeV1Energy(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return 0
	}
	if obj.account.FrozenEnergyExpireTime() > blockTimeMs {
		return 0
	}
	amount := obj.account.FrozenEnergyAmount()
	if amount == 0 {
		return 0
	}
	obj.account.ClearFrozenEnergy()
	_ = s.writeAccountResource(obj)
	return amount
}

func (s *StateDB) GetDelegatedFrozenV1Bandwidth(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.DelegatedFrozenBandwidth()
}

func (s *StateDB) GetDelegatedFrozenV1Energy(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return 0
	}
	return obj.account.DelegatedFrozenEnergy()
}

func (s *StateDB) FreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() + amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	recvObj.account.SetAcquiredDelegatedFrozenBandwidth(recvObj.account.AcquiredDelegatedFrozenBandwidth() + amount)
	recvObj.markDirty()
}

func (s *StateDB) UnfreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() - amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	v := recvObj.account.AcquiredDelegatedFrozenBandwidth() - amount
	if v < 0 {
		v = 0
	}
	recvObj.account.SetAcquiredDelegatedFrozenBandwidth(v)
	recvObj.markDirty()
}

func (s *StateDB) FreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	if err := s.mutateAccountResource(ownerObj, func(_ *corepb.Account_AccountResource) {
		ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() + amount)
	}); err != nil {
		return
	}

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	_ = s.mutateAccountResource(recvObj, func(_ *corepb.Account_AccountResource) {
		recvObj.account.SetAcquiredDelegatedFrozenEnergy(recvObj.account.AcquiredDelegatedFrozenEnergy() + amount)
	})
}

func (s *StateDB) UnfreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	if err := s.mutateAccountResource(ownerObj, func(_ *corepb.Account_AccountResource) {
		ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() - amount)
	}); err != nil {
		return
	}

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	_ = s.mutateAccountResource(recvObj, func(_ *corepb.Account_AccountResource) {
		v := recvObj.account.AcquiredDelegatedFrozenEnergy() - amount
		if v < 0 {
			v = 0
		}
		recvObj.account.SetAcquiredDelegatedFrozenEnergy(v)
	})
}

// trxPrecisionState is SUN per TRX, used for V1 delegated-weight rounding.
const trxPrecisionState = 1_000_000

// UnfreezeV1DelegatedOwner decrements ONLY the owner's delegated frozen balance
// for the resource. The receiver's acquired-delegated balance is handled
// separately (see DecrementReceiverAcquired) so the actuator can mirror
// java-tron UnfreezeBalanceActuator's contract-receiver skip: for a Contract
// receiver under allow_tvm_constantinople java never touches the receiver.
func (s *StateDB) UnfreezeV1DelegatedOwner(owner tcommon.Address, amount int64, resource corepb.ResourceCode) {
	obj := s.getStateObject(owner)
	if obj == nil {
		return
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		s.journalAccount(owner, obj)
		obj.account.SetDelegatedFrozenBandwidth(obj.account.DelegatedFrozenBandwidth() - amount)
		obj.markDirty()
	case corepb.ResourceCode_ENERGY:
		_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
			obj.account.SetDelegatedFrozenEnergy(obj.account.DelegatedFrozenEnergy() - amount)
		})
	}
}

// DecrementReceiverAcquired mirrors java-tron UnfreezeBalanceActuator's
// non-contract receiver branch: it decrements the receiver's acquired-delegated
// frozen balance for the resource and returns the exact weight delta
// (newWeight - oldWeight, in TRX). With AllowTvmSolidity059 active the acquired
// balance is clamped to 0 on underflow and oldWeight is taken as amount/TRX
// (java's underflow guard); otherwise it decrements raw (may go negative,
// matching java's pre-Solidity059 addAcquired(-amount)).
func (s *StateDB) DecrementReceiverAcquired(receiver tcommon.Address, amount int64, resource corepb.ResourceCode, solidity059 bool) int64 {
	obj := s.getStateObject(receiver)
	if obj == nil {
		return 0
	}
	var acquired int64
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		s.journalAccount(receiver, obj)
		acquired = obj.account.AcquiredDelegatedFrozenBandwidth()
	case corepb.ResourceCode_ENERGY:
		if err := s.materializeAccountResource(obj); err != nil {
			return 0
		}
		acquired = obj.account.AcquiredDelegatedFrozenEnergy()
	default:
		return 0
	}
	var oldW, newAcquired int64
	if solidity059 && acquired < amount {
		oldW = amount / trxPrecisionState
		newAcquired = 0
	} else {
		oldW = acquired / trxPrecisionState
		newAcquired = acquired - amount
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		obj.account.SetAcquiredDelegatedFrozenBandwidth(newAcquired)
		obj.markDirty()
	case corepb.ResourceCode_ENERGY:
		obj.account.SetAcquiredDelegatedFrozenEnergy(newAcquired)
		_ = s.writeAccountResource(obj)
	}
	return newAcquired/trxPrecisionState - oldW
}

// GetStateObject returns the account for addr (nil if not found). Used by tests and later tasks.
func (s *StateDB) GetStateObject(addr tcommon.Address) *types.Account {
	return s.GetAccount(addr)
}

// GetWitness returns the witness at addr.
func (s *StateDB) GetWitness(addr tcommon.Address) *types.Witness {
	s.recordWitnessRead(addr)
	if w := s.witnesses[addr]; w != nil {
		return w
	}
	w, err := s.readWitnessCapsule(addr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read witness %s", addr.Hex()), err)
		return nil
	}
	if w == nil {
		return nil
	}
	s.witnesses[addr] = w.Copy()
	return s.witnesses[addr]
}

// LoadWitness hydrates the in-memory witness cache from an external record
// without marking the address dirty or appending to the journal. Legacy tests
// and compatibility fallback paths use it to seed records that are not yet in
// the native witness domain.
//
// Stores a deep copy of w so the in-memory map does not alias the
// caller's record (the caller typically discards w after this call, but
// subsequent mutations would otherwise leak back into the rawdb-returned
// pointer).
func (s *StateDB) LoadWitness(w *types.Witness) {
	if w == nil {
		return
	}
	s.witnesses[w.Address()] = w.Copy()
}

// PutWitness stores a witness, journaling the previous state for revert.
// The new record carries only the URL; counters reset to zero. Use
// SetWitnessURL when updating an existing witness so that VoteCount /
// production counters survive the URL change (java-tron parity).
//
// Marks the address dirty so FlushWitnesses persists it. For preload use
// GetWitness (native domain) or LoadWitness (external record) instead.
func (s *StateDB) PutWitness(addr tcommon.Address, url string) {
	s.journalWitness(addr)
	s.witnesses[addr] = types.NewWitness(addr, url)
	s.dirtyWitnesses[addr] = struct{}{}
	s.standbyWitnessVersion++
}

// SetWitnessURL updates the URL on the existing in-memory witness without
// resetting VoteCount / production counters. Mirrors java-tron's
// WitnessCapsule.setUrl semantics where only the URL field is mutated.
//
// Marks the address dirty so FlushWitnesses persists it.
func (s *StateDB) SetWitnessURL(addr tcommon.Address, url string) {
	existing := s.GetWitness(addr)
	if existing == nil {
		// No in-memory record — promote a fresh one. Caller is responsible
		// for ensuring counters are loaded separately if needed.
		s.journalWitness(addr)
		s.witnesses[addr] = types.NewWitness(addr, url)
		s.dirtyWitnesses[addr] = struct{}{}
		s.standbyWitnessVersion++
		return
	}
	s.journalWitness(addr)
	existing.Proto().Url = url
	s.dirtyWitnesses[addr] = struct{}{}
}

// FlushWitnesses persists the in-memory witness deltas (VoteCount, URL) into
// the native witness account-KV domain. Called by applyBlock between
// ProcessBlock and ApplyBlockStatistics so VoteWitness / Unfreeze /
// WitnessUpdate effects on VoteCount and URL survive across blocks.
//
// Mirrors java-tron's pattern where VoteWitnessActuator writes to
// VotesStore and MaintenanceManager.countVote drains it into WitnessStore;
// the per-block merge here keeps the in-memory cache aligned with the rooted
// capsule so the next block sees the updated VoteCount.
//
// Only addresses in s.dirtyWitnesses are flushed: a no-op block (no
// VoteWitness, no WitnessUpdate, no Unfreeze touching votes) does zero
// persistence work. The dirty set is cleared at the end so a
// subsequent applyBlock on the same StateDB instance starts clean.
func (s *StateDB) FlushWitnesses() {
	for addr := range s.dirtyWitnesses {
		w := s.witnesses[addr]
		if w == nil {
			// Witness was created and then reverted within this block.
			// The dirty mark survived (RevertToSnapshot deliberately
			// does not clear the set — see field doc), but there is
			// nothing to write.
			continue
		}
		stored, _ := s.readWitnessCapsule(addr)
		if stored == nil {
			// Witness not yet persisted (e.g. WitnessCreateActuator
			// already wrote it via ctx.DB earlier in this block, OR a
			// new witness materialised purely in memory). Write the
			// in-memory record so its VoteCount/URL land — counters
			// default to 0, which ApplyBlockStatistics will populate
			// when the witness produces or misses.
			_ = s.SetWitnessCapsule(w.Copy())
			continue
		}
		// Merge: only override fields the in-memory record owns.
		// TotalProduced / TotalMissed / LatestBlockNum / LatestSlotNum
		// are written by ApplyBlockStatistics on the same buffer and
		// must not be clobbered.
		stored.SetVoteCount(w.VoteCount())
		stored.Proto().Url = w.URL()
		_ = s.SetWitnessCapsule(stored)
	}
	clear(s.dirtyWitnesses)
}

// DynamicProperties returns the dynamic properties.
func (s *StateDB) DynamicProperties() *DynamicProperties {
	return s.dynProps
}

// SetDynamicProperties sets the dynamic properties (used during genesis setup).
func (s *StateDB) SetDynamicProperties(dp *DynamicProperties) {
	s.dynProps = dp
}

// Snapshot returns a snapshot ID for later revert.
func (s *StateDB) Snapshot() int {
	id := len(s.snapshots)
	s.snapshots = append(s.snapshots, s.journal.length())
	if s.balanceTrace != nil {
		s.balanceTraceSnapshots = append(s.balanceTraceSnapshots, s.balanceTrace.snapshot())
	} else {
		s.balanceTraceSnapshots = append(s.balanceTraceSnapshots, balanceTraceSnapshot{})
	}
	return id
}

// RevertToSnapshot reverts state changes to the given snapshot.
//
// NOTE: s.dirtyWitnesses is deliberately NOT cleared here. A witness mark
// can outlive its mutation when an actuator reverts — FlushWitnesses will
// then do a Read+Write that round-trips the unchanged stored fields. The
// IO cost (~one Pebble read+write per reverted witness, capped at the
// number of witnesses touched in the block) is far cheaper than the
// journal walk a precise undo would require. See the dirtyWitnesses
// field doc for the design rationale.
func (s *StateDB) RevertToSnapshot(id int) {
	if id < 0 || id >= len(s.snapshots) {
		return
	}
	journalLen := s.snapshots[id]
	s.journal.revert(s.stateObjects, s.witnesses, journalLen)
	// accountChange/codeChange/contractMetaChange may delete or replace an
	// object in stateObjects. Never retain a pointer across journal replay.
	s.lastStateObject = nil
	s.snapshots = s.snapshots[:id]
	if id < len(s.balanceTraceSnapshots) {
		snap := s.balanceTraceSnapshots[id]
		if snap.recorder == nil {
			s.balanceTrace = nil
		} else {
			s.balanceTrace = snap.recorder
			s.balanceTrace.revert(snap)
		}
		s.balanceTraceSnapshots = s.balanceTraceSnapshots[:id]
	}
}

// FinalizeTransaction mirrors java-tron's rootRepository.commit() boundary for
// storage-row existence. java-tron keeps a zero StorageRow visible inside the
// executing transaction, then commit() deletes it before the next transaction.
// StateDB commits only once per block, so keep the zero value cached for the
// eventual disk delete but make later SSTORE cost checks see the row as absent.
func (s *StateDB) FinalizeTransaction() {
	for addr := range s.txFinalizeDirty {
		// Re-resolve the live object by address: a reverted create/recreate may
		// have replaced or removed the object since it was recorded, so the
		// stale-pointer it was recorded under must not be used.
		obj := s.stateObjects[addr]
		if obj == nil {
			continue
		}
		// Scope to slots written this block (dirtyStorage), not the whole cached
		// storage map. A zero-valued cached slot can only come from SetState
		// (reads never cache a zero row), which adds it to dirtyStorage, and
		// the cached row-existence bit is NOT reset at commit — so a zero row from
		// an earlier block already reads as absent and need not be re-marked. This
		// is the same authoritative write set the commit path uses and keeps the scan
		// O(writes-this-block) instead of O(accumulated storage cache).
		for k := range obj.dirtyStorage {
			if slot := obj.storage[k]; slot.value == (tcommon.Hash{}) {
				slot.exists = false
				obj.cacheStorageSlot(k, slot)
			}
		}
		if obj.selfDestructed && !obj.deleted {
			s.DeleteAccount(obj.address)
		}
	}
	clear(s.txFinalizeDirty)
	// EIP-1153: transient storage is discarded at the end of every
	// transaction. clear() on a nil map is a no-op, and it preserves the map
	// header so any journal entries still referencing it stay valid.
	clear(s.transientStorage)
}

// transientStorageKey identifies an EIP-1153 transient storage slot by
// (account address, 32-byte slot). Transient storage is namespaced per
// contract address exactly like persistent storage.
type transientStorageKey struct {
	addr tcommon.Address
	key  tcommon.Hash
}

// GetTransientState returns the transient storage value at (addr, key) for the
// current transaction, or the zero hash if unset. EIP-1153 (Cancun).
func (s *StateDB) GetTransientState(addr tcommon.Address, key tcommon.Hash) tcommon.Hash {
	s.recordTransientStorageRead(addr, key)
	return s.transientStorage[transientStorageKey{addr: addr, key: key}]
}

// SetTransientState writes value to the transient storage slot (addr, key) for
// the current transaction. The write is journaled so RevertToSnapshot undoes it
// with the enclosing call frame; the whole map is discarded at
// FinalizeTransaction. A write that does not change the slot is skipped (no
// journal entry), matching go-ethereum. EIP-1153 (Cancun).
func (s *StateDB) SetTransientState(addr tcommon.Address, key, value tcommon.Hash) {
	tk := transientStorageKey{addr: addr, key: key}
	prev := s.transientStorage[tk] // zero hash if absent
	if prev == value {
		return
	}
	if s.transientStorage == nil {
		s.transientStorage = make(map[transientStorageKey]tcommon.Hash)
	}
	s.journal.append(transientStorageChange{storage: s.transientStorage, tk: tk, prev: prev})
	s.transientStorage[tk] = value
}

// AccountExists returns whether an account exists (non-nil and not deleted).
func (s *StateDB) AccountExists(addr tcommon.Address) bool {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	return obj != nil && !obj.deleted
}

// GetAccountType returns the compact account-envelope type and existence bit
// without making callers consume every mutable Account field.
func (s *StateDB) GetAccountType(addr tcommon.Address) (corepb.AccountType, bool) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldAccountType)
	if obj == nil || obj.deleted || obj.account == nil {
		return corepb.AccountType_Normal, false
	}
	return obj.account.Type(), true
}

// SetAccountType updates the typed account-envelope field without turning an
// existing-account mutation into a coarse full-account write. Account creation
// remains the responsibility of CreateAccount/CreateAccountWithTime.
func (s *StateDB) SetAccountType(addr tcommon.Address, accountType corepb.AccountType) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldAccountType)
	if obj == nil || obj.deleted || obj.account == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldAccountType)
	s.journalAccountScalars(addr, obj)
	obj.account.SetAccountType(accountType)
	obj.markDirty()
}

// CreateAccount creates a new account at addr with the given type.
// If the account already exists, it returns the existing account.
//
// NOTE: This entry point leaves Account.create_time at 0. New on-chain
// account-creation paths must use CreateAccountWithTime so the field mirrors
// java-tron's `dynamicStore.getLatestBlockHeaderTimestamp()`. This 2-arg form
// is retained for VM-internal call sites (slice 2c) and tests/genesis paths
// where create_time is irrelevant.
func (s *StateDB) CreateAccount(addr tcommon.Address, accountType corepb.AccountType) *types.Account {
	obj := s.getOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetAccountType(accountType)
	obj.markDirty()
	return obj.account
}

// CreateAccountWithTime creates a new account at addr with the given type and
// stamps Account.create_time = createTime. Mirrors java-tron's AccountCapsule
// 5-arg constructor (AccountCapsule.java:158-180), which sets create_time on
// both the with-default-permission and without-default-permission branches —
// i.e. createTime is unconditional, independent of AllowMultiSign.
//
// Callers should pass `dp.LatestBlockHeaderTimestamp()` so the value matches
// java-tron's `dynamicStore.getLatestBlockHeaderTimestamp()`.
//
// This is the entry point for actuators creating new on-chain accounts
// (Transfer / TransferAsset / CreateAccount / ShieldedTransfer). Like
// CreateAccount, it overwrites type/create_time on an existing account, so
// callers must first gate on !AccountExists(addr) to preserve real stored
// values — every actuator call site already does this.
func (s *StateDB) CreateAccountWithTime(addr tcommon.Address, accountType corepb.AccountType, createTime int64) *types.Account {
	obj := s.getOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetAccountType(accountType)
	obj.account.SetCreateTime(createTime)
	obj.markDirty()
	return obj.account
}

// ClearAcquiredDelegatedResource clears incoming delegated-resource fields.
// java-tron's CREATE2 collision path uses this when an existing account is
// upgraded to a contract account.
func (s *StateDB) ClearAcquiredDelegatedResource(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return
	}
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.AcquiredDelegatedFrozenBalanceForBandwidth = 0
	pb.AcquiredDelegatedFrozenV2BalanceForBandwidth = 0
	if pb.AccountResource != nil {
		pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy = 0
		pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy = 0
	}
	_ = s.writeAccountResource(obj)
	obj.markDirty()
}

// IsWitness returns whether the account is marked as a witness.
func (s *StateDB) IsWitness(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	if obj == nil {
		return false
	}
	return obj.account.IsWitness()
}

// SetIsWitness sets the witness flag on an account.
func (s *StateDB) SetIsWitness(addr tcommon.Address, isWitness bool) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetIsWitness(isWitness)
	obj.markDirty()
}

// GetFrozenV2Amount returns the frozen amount for a specific resource type.
func (s *StateDB) GetFrozenV2Amount(addr tcommon.Address, resourceType corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	amount, _, err := s.accountFrozenV2Amount(obj, resourceType)
	if err != nil {
		return 0
	}
	return amount
}

// ReduceFreezeV2 reduces the frozen amount for a resource type.
func (s *StateDB) ReduceFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.reduceAccountFrozenV2(obj, resourceType, amount)
}

// AddUnfreezeV2 adds a pending unfreeze entry with expiration time.
func (s *StateDB) AddUnfreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount, expireTime int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.addAccountUnfrozenV2(obj, resourceType, amount, expireTime)
}

// GetFreezeV1ExpireTime returns the expire time (ms) of the V1 frozen balance
// for the given resource type (0=BANDWIDTH, 1=ENERGY).
func (s *StateDB) GetFreezeV1ExpireTime(addr tcommon.Address, resourceType int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	switch resourceType {
	case 0: // BANDWIDTH: max expire time across Frozen list
		maxExpire, err := s.accountFrozenBandwidthMaxExpire(obj)
		if err != nil {
			return 0
		}
		return maxExpire
	case 1: // ENERGY
		if err := s.materializeAccountResource(obj); err != nil {
			return 0
		}
		return obj.account.FrozenEnergyExpireTime()
	}
	return 0
}

// frozenV2WeightWithDelegated returns the V2 stake weight (in TRX) for a
// resource on this account, mirroring java AccountCapsule
// getFrozenV2BalanceWithDelegated for BANDWIDTH/ENERGY (frozen + outgoing
// delegated) and getTronPowerFrozenV2Balance for TRON_POWER (no delegated leg).
// Used to compute (newWeight - oldWeight) when refreezing cancelled unstakes.
func (s *StateDB) frozenV2WeightWithDelegated(addr tcommon.Address, resource corepb.ResourceCode) int64 {
	balance := s.GetFrozenV2Amount(addr, resource)
	if resource != corepb.ResourceCode_TRON_POWER {
		balance += s.GetDelegatedFrozenV2(addr, resource)
	}
	return balance / trxPrecisionState
}

// CancelAllUnfreezeV2 cancels the account's pending V2 unfreeze queue, splitting
// each entry on `now` exactly like java CancelAllUnfreezeV2Processor.execute
// (now = getLatestBlockHeaderTimestamp) and go actuator
// CancelAllUnfreezeV2Actuator.Execute:
//   - UnfreezeExpireTime  > now  -> refrozen into FrozenV2; the per-resource
//     total_{net,energy,tron_power}_weight delta (newWeight - oldWeight) is
//     accumulated into the returned map.
//   - UnfreezeExpireTime <= now  -> expired; its amount is accumulated and
//     RETURNED so the caller can add it to balance.
//
// The queue is always cleared. The weight deltas are NOT applied here: the caller
// must apply them to the LIVE DynamicProperties (the StateDB's own dp is the empty
// genesis default in production) through a journaled path so a VM-frame revert
// rolls them back — mirrors how the FREEZE/UNFREEZE opcodes own the weight
// mutation via tvmAddResourceWeight. Returns (total expired, per-resource weight
// delta in TRX units).
func (s *StateDB) CancelAllUnfreezeV2(addr tcommon.Address, now int64) (int64, map[corepb.ResourceCode]int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0, nil
	}
	rows, err := s.accountUnfrozenV2Rows(obj)
	if err != nil || len(rows) == 0 {
		return 0, nil
	}
	var withdrawExpire int64
	weightDeltas := make(map[corepb.ResourceCode]int64, 3)
	for _, row := range rows {
		u := row.entry
		if u.UnfreezeExpireTime > now {
			// Refreeze and record the global resource weight delta for the caller.
			oldWeight := s.frozenV2WeightWithDelegated(addr, u.Type)
			_ = s.addAccountFrozenV2(obj, u.Type, u.UnfreezeAmount)
			newWeight := s.frozenV2WeightWithDelegated(addr, u.Type)
			weightDeltas[u.Type] += newWeight - oldWeight
		} else {
			withdrawExpire += u.UnfreezeAmount
		}
	}
	_ = s.clearAccountUnfrozenV2(obj)
	return withdrawExpire, weightDeltas
}

// applyResourceWeight adds delta to dp's matching total_*_weight, mirroring java
// repo.addTotalNet/Energy/TronPowerWeight. Non-journaled: callers that need the
// delta rolled back on a VM revert use StateDB.AddResourceWeightJournaled.
func applyResourceWeight(dp *DynamicProperties, resource corepb.ResourceCode, delta int64) {
	if delta == 0 || dp == nil {
		return
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		dp.AddTotalNetWeight(delta)
	case corepb.ResourceCode_ENERGY:
		dp.AddTotalEnergyWeight(delta)
	case corepb.ResourceCode_TRON_POWER:
		dp.AddTotalTronPowerWeight(delta)
	}
}

// AddResourceWeightJournaled applies a resource-weight delta to dp AND records a
// journal entry so a later RevertToSnapshot rolls it back. The TVM staking
// opcodes (FREEZE/UNFREEZE) and the selfdestruct resource release must use this:
// java applies these to a discardable Repository whose delta is dropped on
// revert, but gtron mutates the shared DynamicProperties directly and Set is not
// journaled — so a freeze-opcode-then-revert would otherwise leak the weight and
// over-count total_energy_weight. dp is passed explicitly (not s.dynProps)
// because a VM frame may run against a DynamicProperties distinct from the
// StateDB's own (the never-committed simulation path loads a fresh one); journal
// and mutate the exact object the frame uses and commits.
func (s *StateDB) AddResourceWeightJournaled(dp *DynamicProperties, resource corepb.ResourceCode, delta int64) {
	if delta == 0 || dp == nil {
		return
	}
	s.journal.append(resourceWeightChange{dp: dp, resource: resource, delta: delta})
	applyResourceWeight(dp, resource, delta)
}

// UnfreezeV2Count returns the number of pending unfreeze entries.
func (s *StateDB) UnfreezeV2Count(addr tcommon.Address) int {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	rows, err := s.accountUnfrozenV2Rows(obj)
	if err != nil {
		return 0
	}
	return len(rows)
}

// RemoveExpiredUnfreezeV2 removes expired entries and returns the total withdrawn.
func (s *StateDB) RemoveExpiredUnfreezeV2(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	rows, err := s.accountUnfrozenV2Rows(obj)
	if err != nil {
		return 0
	}
	var amount int64
	removed := false
	for _, row := range rows {
		if row.entry.UnfreezeExpireTime > now {
			continue
		}
		if err := s.DeleteAccountKV(addr, kvdomains.AccountUnfrozenV2Aux, row.key); err != nil {
			return 0
		}
		amount += row.entry.UnfreezeAmount
		removed = true
	}
	if removed {
		s.invalidateAccountStakeV2(obj)
	}
	return amount
}

// TotalFrozenV2 returns the total frozen balance across all resource types.
func (s *StateDB) TotalFrozenV2(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	rows, err := s.accountFrozenV2Rows(obj)
	if err != nil {
		return 0
	}
	var total int64
	for _, row := range rows {
		total += row.amount
	}
	return total
}

// GetLegacyTronPower returns the pre-AllowNewResourceModel voting power in drops.
func (s *StateDB) GetLegacyTronPower(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	power, err := s.legacyTronPower(obj)
	if err != nil {
		return 0
	}
	return power
}

// legacyTronPower computes java-tron's getTronPower from the exact resource
// rows it uses. In particular it does not scan the unrelated V2 unfreeze queue
// or materialize the whole StakeV2 account domain.
func (s *StateDB) legacyTronPower(obj *stateObject) (int64, error) {
	bandwidthRows, err := s.accountFrozenBandwidthFastRows(obj)
	if err != nil {
		return 0, err
	}
	var bandwidth int64
	for _, row := range bandwidthRows {
		bandwidth += row.entry.FrozenBalance
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return 0, err
	}
	frozenV2Bandwidth, _, err := s.accountFrozenV2Amount(obj, corepb.ResourceCode_BANDWIDTH)
	if err != nil {
		return 0, err
	}
	frozenV2Energy, _, err := s.accountFrozenV2Amount(obj, corepb.ResourceCode_ENERGY)
	if err != nil {
		return 0, err
	}
	acct := obj.account
	return bandwidth +
		acct.FrozenEnergyAmount() +
		acct.DelegatedFrozenBandwidth() +
		acct.DelegatedFrozenEnergy() +
		frozenV2Bandwidth +
		frozenV2Energy +
		acct.DelegatedFrozenV2BalanceForBandwidth() +
		acct.DelegatedFrozenV2BalanceForEnergy(), nil
}

// GetAllTronPower returns the AllowNewResourceModel voting power in drops.
func (s *StateDB) GetAllTronPower(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	v1TronPower, _, err := s.accountTronPower(obj)
	if err != nil {
		return 0
	}
	var v1Amount int64
	if v1TronPower != nil {
		v1Amount = v1TronPower.FrozenBalance
	}
	v2Amount, _, err := s.accountFrozenV2Amount(obj, corepb.ResourceCode_TRON_POWER)
	if err != nil {
		return 0
	}
	switch oldPower := obj.account.OldTronPower(); {
	case oldPower == -1:
		return v1Amount + v2Amount
	case oldPower == 0:
		legacy, err := s.legacyTronPower(obj)
		if err != nil {
			return 0
		}
		return legacy + v1Amount + v2Amount
	default:
		return oldPower + v1Amount + v2Amount
	}
}

// InitializeOldTronPowerIfNeeded snapshots LegacyTronPower into old_tron_power
// when the field is still uninitialized (== 0). No-op otherwise.
func (s *StateDB) InitializeOldTronPowerIfNeeded(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil || !obj.account.OldTronPowerIsNotInitialized() {
		return
	}
	value, err := s.legacyTronPower(obj)
	if err != nil {
		return
	}
	if value == 0 {
		value = -1
	}
	s.journalAccount(addr, obj)
	obj.account.SetOldTronPower(value)
	obj.markDirty()
}

// InvalidateOldTronPower sets old_tron_power to -1 (invalid), consuming the
// legacy snapshot. No-op if already invalid.
func (s *StateDB) InvalidateOldTronPower(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.account.OldTronPowerIsInvalid() {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.InvalidateOldTronPower()
	obj.markDirty()
}

// GetVotes returns the votes for an account.
func (s *StateDB) GetVotes(addr tcommon.Address) []*corepb.Vote {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	if err := s.materializeAccountVotes(obj); err != nil {
		return nil
	}
	return obj.account.Votes()
}

// SetVotes sets the vote list on an account.
func (s *StateDB) SetVotes(addr tcommon.Address, votes []*corepb.Vote) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.writeAccountVotes(obj, votes)
}

// ClearVotes clears all votes on an account.
func (s *StateDB) ClearVotes(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.writeAccountVotes(obj, nil)
}

// AddWitnessVoteCount adds delta to a witness's vote count. Marks the
// address dirty so FlushWitnesses persists the new VoteCount.
func (s *StateDB) AddWitnessVoteCount(addr tcommon.Address, delta int64) {
	w := s.GetWitness(addr)
	if w == nil {
		return
	}
	s.journalWitness(addr)
	w.SetVoteCount(w.VoteCount() + delta)
	s.dirtyWitnesses[addr] = struct{}{}
	if delta != 0 {
		s.standbyWitnessVersion++
	}
}

// StandbyWitnessVersion returns the execution-view generation of the witness
// membership/vote inputs used by standby reward selection.
func (s *StateDB) StandbyWitnessVersion() uint64 { return s.standbyWitnessVersion }

// GetAllowance returns the witness reward allowance.
func (s *StateDB) GetAllowance(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldAllowance)
	if obj == nil {
		return 0
	}
	return obj.account.Allowance()
}

// SetAllowance sets the witness reward allowance.
func (s *StateDB) SetAllowance(addr tcommon.Address, allowance int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldAllowance)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldAllowance)
	s.journalAccountScalars(addr, obj)
	obj.account.SetAllowance(allowance)
	obj.markDirty()
}

// AddAllowance adds amount to the witness reward allowance.
func (s *StateDB) AddAllowance(addr tcommon.Address, amount int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldAllowance)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldAllowance)
	s.journalAccountScalars(addr, obj)
	obj.account.SetAllowance(obj.account.Allowance() + amount)
	obj.markDirty()
}

// AddAllowanceFinalReward adds a block-final witness reward without legacy SHI
// journaling. Reward payment runs after transaction execution and after java
// account-state-root calculation; flat temporal history captures the rooted
// writes through the domain-change no-journal path.
func (s *StateDB) AddAllowanceFinalReward(addr tcommon.Address, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if s.changeSet.enabled {
		var prevLatest []byte
		if latest, exists, err := encodeAccountLatestObject(obj, true); err == nil && exists {
			prevLatest = latest
		}
		s.domainChangeNoJournal = append(s.domainChangeNoJournal, accountChange{
			address:          addr,
			prevLatest:       prevLatest,
			prevDeleted:      obj.deleted,
			prevCreated:      obj.created,
			prevSelfDestruct: obj.selfDestructed,
		})
	}
	obj.account.SetAllowance(obj.account.Allowance() + amount)
	obj.invalidateAccountProto()
	obj.accountDirty = true
	obj.markDirty()
}

// GetLatestWithdrawTime returns the latest withdraw timestamp.
func (s *StateDB) GetLatestWithdrawTime(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestWithdrawTime)
	if obj == nil {
		return 0
	}
	return obj.account.LatestWithdrawTime()
}

// SetLatestWithdrawTime sets the latest withdraw timestamp.
func (s *StateDB) SetLatestWithdrawTime(addr tcommon.Address, t int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestWithdrawTime)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldLatestWithdrawTime)
	s.journalAccountScalars(addr, obj)
	obj.account.SetLatestWithdrawTime(t)
	obj.markDirty()
}

// SetOldTronPower sets the account's old_tron_power field directly. Mirrors
// java AccountCapsule.setOldTronPower; used by the suicide vote-cancel path.
func (s *StateDB) SetOldTronPower(addr tcommon.Address, v int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetOldTronPower(v)
	obj.markDirty()
}

// GetNetUsage returns the net (bandwidth) usage for an account.
func (s *StateDB) GetNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldNetUsage)
	if obj == nil {
		return 0
	}
	return obj.account.NetUsage()
}

// SetNetUsage sets the net (bandwidth) usage for an account.
func (s *StateDB) SetNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldNetUsage)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldNetUsage)
	s.journalAccountScalars(addr, obj)
	obj.account.SetNetUsage(usage)
	obj.markDirty()
}

// GetLatestOperationTime returns the latest account operation timestamp.
func (s *StateDB) GetLatestOperationTime(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestOperationTime)
	if obj == nil {
		return 0
	}
	return obj.account.LatestOperationTime()
}

// SetLatestOperationTime sets the latest account operation timestamp.
func (s *StateDB) SetLatestOperationTime(addr tcommon.Address, t int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestOperationTime)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldLatestOperationTime)
	s.journalAccountScalars(addr, obj)
	obj.account.SetLatestOperationTime(t)
	obj.markDirty()
}

// GetLatestConsumeTime returns the latest consume time for an account.
func (s *StateDB) GetLatestConsumeTime(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestConsumeTime)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeTime()
}

// SetLatestConsumeTime sets the latest consume time for an account.
func (s *StateDB) SetLatestConsumeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestConsumeTime)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldLatestConsumeTime)
	s.journalAccountScalars(addr, obj)
	obj.account.SetLatestConsumeTime(t)
	obj.markDirty()
}

// GetFreeNetUsage returns the free net (bandwidth) usage for an account.
func (s *StateDB) GetFreeNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldFreeNetUsage)
	if obj == nil {
		return 0
	}
	return obj.account.FreeNetUsage()
}

// SetFreeNetUsage sets the free net (bandwidth) usage for an account.
func (s *StateDB) SetFreeNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldFreeNetUsage)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldFreeNetUsage)
	s.journalAccountScalars(addr, obj)
	obj.account.SetFreeNetUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeFreeTime returns the latest consume free time for an account.
func (s *StateDB) GetLatestConsumeFreeTime(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestConsumeFreeTime)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeFreeTime()
}

// SetLatestConsumeFreeTime sets the latest consume free time for an account.
func (s *StateDB) SetLatestConsumeFreeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldLatestConsumeFreeTime)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldLatestConsumeFreeTime)
	s.journalAccountScalars(addr, obj)
	obj.account.SetLatestConsumeFreeTime(t)
	obj.markDirty()
}

func (s *StateDB) GetFreeAssetNetUsage(addr tcommon.Address, key string) int64 {
	return s.trc10Balance(addr, kvdomains.AccountFreeAssetNetUsage, ownedStringBytes(key))
}

func (s *StateDB) SetFreeAssetNetUsage(addr tcommon.Address, key string, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.setAccountAuxValue(addr, kvdomains.AccountFreeAssetNetUsage, ownedStringBytes(key), usage)
}

func (s *StateDB) GetFreeAssetNetUsageV2(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return s.trc10Balance(addr, kvdomains.AccountFreeAssetNetUsageV2, ownedStringBytes(key))
}

func (s *StateDB) SetFreeAssetNetUsageV2(addr tcommon.Address, key string, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.setAccountAuxValue(addr, kvdomains.AccountFreeAssetNetUsageV2, ownedStringBytes(key), usage)
}

func (s *StateDB) GetLatestAssetOperationTime(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return s.trc10Balance(addr, kvdomains.AccountAssetOperationTime, ownedStringBytes(key))
}

func (s *StateDB) SetLatestAssetOperationTime(addr tcommon.Address, key string, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.setAccountAuxValue(addr, kvdomains.AccountAssetOperationTime, ownedStringBytes(key), t)
}

func (s *StateDB) GetLatestAssetOperationTimeV2(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return s.trc10Balance(addr, kvdomains.AccountAssetOperationTimeV2, ownedStringBytes(key))
}

func (s *StateDB) SetLatestAssetOperationTimeV2(addr tcommon.Address, key string, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.setAccountAuxValue(addr, kvdomains.AccountAssetOperationTimeV2, ownedStringBytes(key), t)
}

// GetEnergyUsage returns the energy usage for an account.
func (s *StateDB) GetEnergyUsage(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return 0
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return 0
	}
	return obj.account.EnergyUsage()
}

// SetEnergyUsage sets the energy usage for an account.
func (s *StateDB) SetEnergyUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return
	}
	_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
		obj.account.SetEnergyUsage(usage)
	})
}

// GetLatestConsumeTimeForEnergy returns the latest energy consume time for an account.
func (s *StateDB) GetLatestConsumeTimeForEnergy(addr tcommon.Address) int64 {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return 0
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return 0
	}
	return obj.account.LatestConsumeTimeForEnergy()
}

// SetLatestConsumeTimeForEnergy sets the latest energy consume time for an account.
func (s *StateDB) SetLatestConsumeTimeForEnergy(addr tcommon.Address, t int64) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return
	}
	_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
		obj.account.SetLatestConsumeTimeForEnergy(t)
	})
}

// SetEnergyUsageAndLatestConsumeTime updates the legacy energy settlement
// fields in one AccountResource write. Energy billing always changes these two
// fields together; serializing the split resource once preserves the same final
// state and journal predecessor while avoiding a redundant protobuf encode and
// KV mutation.
func (s *StateDB) SetEnergyUsageAndLatestConsumeTime(addr tcommon.Address, usage, t int64) {
	s.setEnergySettlement(addr, usage, t, 0, false, false)
}

// SetEnergyUsageWindowAndLatestConsumeTime updates all Stake 2.0 energy
// settlement fields in one AccountResource write.
func (s *StateDB) SetEnergyUsageWindowAndLatestConsumeTime(addr tcommon.Address, usage, rawWindow, t int64, optimized bool) {
	s.setEnergySettlement(addr, usage, t, rawWindow, optimized, true)
}

func (s *StateDB) setEnergySettlement(addr tcommon.Address, usage, t, rawWindow int64, optimized, updateWindow bool) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return
	}
	obj.account.SetEnergyUsage(usage)
	if updateWindow {
		obj.account.SetEnergyWindow(rawWindow, optimized)
	}
	obj.account.SetLatestConsumeTimeForEnergy(t)
	_ = s.writeAccountResource(obj)
}

// SetEnergyWindow sets the per-account energy recovery window (raw field +
// optimized flag) for an account. Mirrors java-tron's
// setNewWindowSize / setNewWindowSizeV2 persistence.
func (s *StateDB) SetEnergyWindow(addr tcommon.Address, raw int64, optimized bool) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return
	}
	_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
		obj.account.SetEnergyWindow(raw, optimized)
	})
}

// SetNetWindow sets the per-account bandwidth recovery window (raw field +
// optimized flag) for an account. Mirrors java-tron's
// setNewWindowSize / setNewWindowSizeV2 persistence for BANDWIDTH.
func (s *StateDB) SetNetWindow(addr tcommon.Address, raw int64, optimized bool) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldNetWindow)
	if obj == nil {
		return
	}
	s.recordAccountFieldWrite(addr, TransactionAccountFieldNetWindow)
	s.journalAccountScalars(addr, obj)
	obj.account.SetNetWindow(raw, optimized)
	obj.markDirty()
}

// GetNetWindow returns the per-account bandwidth recovery window without
// promoting the read to a full Account dependency.
func (s *StateDB) GetNetWindow(addr tcommon.Address) (raw int64, optimized bool) {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldNetWindow)
	if obj == nil {
		return 0, false
	}
	return obj.account.RawNetWindowSize(), obj.account.NetWindowOptimized()
}

// --- Contract support ---

var (
	contractMetaKVKey  = []byte("meta")
	contractABIKVKey   = []byte("abi")
	contractStateKVKey = []byte("state")
)

// GetCode returns the contract bytecode at addr.
func (s *StateDB) GetCode(addr tcommon.Address) []byte {
	s.recordCodeRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return nil
	}
	if obj.code == nil && !obj.codeDirty && obj.codeHash != (tcommon.Hash{}) {
		if code := s.readStateCode(obj.codeHash); len(code) > 0 {
			// stateCodeReader transfers ownership. Bytecode is immutable and the
			// state object retains it for the rest of its cache lifetime, so no
			// second defensive copy is needed here.
			obj.code = code
		} else if s.codeColdHistory != nil {
			if code, ok, err := s.codeColdHistory.GetCodeAtOrBefore(obj.codeHash, s.codeColdTxNum); err == nil && ok && len(code) > 0 {
				obj.code = append([]byte(nil), code...)
				if store := s.getStateCodeStore(); store != nil {
					s.admitStateCode(obj.codeHash, obj.code, store)
				}
			}
		}
	}
	return obj.code
}

// SetCode sets the contract bytecode at addr. Creates the account if needed.
func (s *StateDB) SetCode(addr tcommon.Address, code []byte) {
	obj := s.getOrCreateAccountForField(addr, TransactionAccountFieldExistence)
	prevCode := append([]byte(nil), s.GetCode(addr)...)
	var prevLatest []byte
	if latest, exists, err := encodeAccountLatestObject(obj, true); err == nil && exists {
		prevLatest = latest
	}
	s.journal.append(codeChange{
		address:    addr,
		prevCode:   prevCode,
		prevHash:   obj.codeHash,
		prevLatest: prevLatest,
	})
	obj.setCode(code)
}

// GetCodeSize returns the length of the contract bytecode.
func (s *StateDB) GetCodeSize(addr tcommon.Address) int {
	return len(s.GetCode(addr))
}

// GetCodeHash returns the java-tron EXTCODEHASH value for an existing account:
// Keccak-256(code) for contracts and Keccak-256(empty) for existing accounts
// without contract code. Missing accounts return zero.
func (s *StateDB) GetCodeHash(addr tcommon.Address) tcommon.Hash {
	s.recordCodeRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return tcommon.Hash{}
	}
	if obj.codeHash != (tcommon.Hash{}) {
		return obj.codeHash
	}
	if len(obj.code) > 0 {
		obj.codeHash = tcommon.Keccak256(obj.code)
		return obj.codeHash
	}
	return tcommon.Keccak256(nil)
}

// GetState returns a storage value from a contract.
func (s *StateDB) GetState(addr tcommon.Address, key tcommon.Hash) tcommon.Hash {
	v, _ := s.GetStateWithExist(addr, key)
	return v
}

// GetStateWithExist returns a storage value and whether the java-tron
// StorageRow exists. A present zero row can exist inside the same transaction
// before commit; SSTORE energy accounting distinguishes that from a missing
// row even though both read as zero.
func (s *StateDB) GetStateWithExist(addr tcommon.Address, key tcommon.Hash) (tcommon.Hash, bool) {
	s.recordStorageRead(addr, key)
	s.recordContractMetadataRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		s.storageObservability.accountMissingZero++
		return tcommon.Hash{}, false
	}
	slot, cached := obj.storage[key]
	if cached {
		if s.transactionVersionedReader == nil {
			s.storageObservability.objectCacheHits++
			return slot.value, slot.exists
		}
		// Writes made by this task (including post-images forwarded from an
		// earlier member of its sender chain) always win over the frozen prefix.
		if _, dirty := obj.dirtyStorage[key]; dirty {
			s.storageObservability.objectCacheHits++
			return slot.value, slot.exists
		}
		// A prewarmed retry template was previously executed and reverted. Its
		// clean storage cache can therefore contain a block-start value older
		// than this retry's canonical prefix. Check such a slot once per reader
		// binding before accepting the cache hit.
		versionedKey := transactionVersionedStorageKey{address: addr, key: key}
		if _, checked := s.transactionVersionedStorageChecked[versionedKey]; checked {
			s.storageObservability.objectCacheHits++
			return slot.value, slot.exists
		}
	}
	if obj.created {
		s.storageObservability.createdZero++
		return tcommon.Hash{}, false
	}
	// Async retry workers start from a clean block-start view and consume exact
	// canonical post-images through the shared version floor. Storage has its
	// own typed access key; routing it through the generic account-KV reader
	// would miss the writer recorded by SLOAD/SSTORE dependency tracking.
	if s.transactionVersionedReader != nil {
		versionedKey := transactionVersionedStorageKey{address: addr, key: key}
		s.transactionVersionedStorageChecked[versionedKey] = struct{}{}
		if value, exists, versioned := s.readTransactionVersionedStorage(addr, key); versioned {
			obj.cacheStorageSlot(key, storageSlot{value: value, exists: exists})
			return value, exists
		}
	}
	if cached {
		s.storageObservability.objectCacheHits++
		return slot.value, slot.exists
	}
	// Load from persistent storage on cache miss.
	s.storageObservability.coldReads++
	rowKey := storageReadKeyPool.Get().(*tcommon.Hash)
	*rowKey = s.storageRowKey(addr, key)
	raw, ok, err := s.getAccountKVForDecoding(addr, kvdomains.ContractStorage, rowKey[:])
	storageReadKeyPool.Put(rowKey)
	if err != nil {
		s.storageObservability.coldErrors++
		return tcommon.Hash{}, false
	}
	// Cache durable misses as an explicit non-existent slot. Contracts often
	// probe the same empty mapping/array position repeatedly; without this entry
	// every SLOAD crosses the blockbuffer and reaches Pebble again. Keep errors
	// uncached above because a transient read failure must remain retryable.
	if !ok || len(raw) == 0 {
		s.storageObservability.coldMissing++
		obj.cacheStorageSlot(key, storageSlot{})
		s.recordOversizedStorageReload(obj, key)
		return tcommon.Hash{}, false
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	if h == (tcommon.Hash{}) {
		s.storageObservability.coldMissing++
		obj.cacheStorageSlot(key, storageSlot{})
		s.recordOversizedStorageReload(obj, key)
		return tcommon.Hash{}, false
	}
	s.storageObservability.coldFound++
	obj.cacheStorageSlot(key, storageSlot{value: h, exists: true})
	s.recordOversizedStorageReload(obj, key)
	return h, true
}

// SetState sets a storage value on a contract.
func (s *StateDB) SetState(addr tcommon.Address, key, value tcommon.Hash) {
	s.recordContractMetadataRead(addr)
	obj := s.getOrCreateAccountForField(addr, TransactionAccountFieldExistence)
	prev, prevExists, cached := obj.getStorageWithExist(key)
	if !cached {
		prev, prevExists = s.GetStateWithExist(addr, key)
	}
	if prevExists && prev == value {
		return
	}
	_, prevDirty := obj.dirtyStorage[key]
	if !prevDirty {
		// SetState has already paid for the durable read needed by SSTORE. Keep
		// that pre-image with the dirty slot so commit planning does not issue the
		// same account-KV/Pebble lookup a second time.
		obj.recordDirtyStorageOrigin(key, storageOrigin{value: prev, exists: prevExists, loaded: true})
	}
	s.journal.append(acquireStorageChange(addr, key, prev, prevExists, prevDirty))
	obj.setStorageValue(key, value, true)
	// A write to zero leaves a present-zero row that FinalizeTransaction must
	// flip to non-existent; record the address so the boundary scan can skip
	// untouched objects. (Non-zero writes here are harmless no-ops there.)
	s.txFinalizeDirty[addr] = struct{}{}
}

func (s *StateDB) storageRowKey(addr tcommon.Address, key tcommon.Hash) tcommon.Hash {
	s.recordContractMetadataRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return javaStorageRowKey(addr, key, nil)
	}
	if !obj.storageKeyLayoutCached {
		if meta, ok := s.contractRuntime(obj); ok {
			obj.storageKeyPrefix = meta.storageKeyPrefix
			obj.storageKeyHashSlot = meta.storageKeyHashSlot
		} else {
			obj.storageKeyPrefix, obj.storageKeyHashSlot = javaStorageKeyLayoutFields(addr, nil, 0)
		}
		obj.storageKeyLayoutCached = true
	}
	return storageRowKeyWithLayout(key, obj.storageKeyPrefix, obj.storageKeyHashSlot)
}

// GetContract returns the contract metadata at addr.
func (s *StateDB) GetContract(addr tcommon.Address) *contractpb.SmartContract {
	s.recordContractMetadataRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return nil
	}
	return s.loadContract(obj)
}

func (s *StateDB) loadContract(obj *stateObject) *contractpb.SmartContract {
	if obj.contractMeta == nil && !obj.contractMetaDirty {
		data, ok, err := s.getAccountKVForDecoding(obj.address, kvdomains.ContractMetadata, contractMetaKVKey)
		if err != nil {
			s.recordStateError(fmt.Sprintf("read contract metadata %s", obj.address.Hex()), err)
			return nil
		}
		if ok && len(data) == 0 {
			s.recordStateError(fmt.Sprintf("decode contract metadata %s", obj.address.Hex()), errors.New("empty value"))
			return nil
		}
		if ok {
			var sc contractpb.SmartContract
			if err := statecodec.Unmarshal(data, &sc); err != nil {
				s.recordStateError(fmt.Sprintf("decode contract metadata %s", obj.address.Hex()), err)
				return nil
			}
			obj.contractMeta = &sc
			// Loading metadata changes the derived storage-key layout.
			obj.invalidateStorageKeyLayout()
		}
	}
	return obj.contractMeta
}

// GetContractMetadataBytes returns serialized contract metadata without
// unmarshalling committed data or populating the contract metadata cache.
// Dirty in-memory metadata is marshalled so the result matches GetContract.
func (s *StateDB) GetContractMetadataBytes(addr tcommon.Address) ([]byte, bool, error) {
	s.recordContractMetadataRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return nil, false, nil
	}
	if obj.contractMetaDirty {
		if obj.contractMeta == nil {
			return nil, false, nil
		}
		data, err := proto.Marshal(obj.contractMeta)
		if err != nil {
			return nil, false, err
		}
		return data, true, nil
	}
	if hasFreshAccountKVGeneration(obj) {
		recordFreshAccountKVPointReadsAvoided(1)
		return nil, false, nil
	}
	data, ok, err := s.readAccountKVLatest(addr, obj.accountKVGeneration, kvdomains.ContractMetadata, contractMetaKVKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	var meta contractpb.SmartContract
	if err := statecodec.Unmarshal(data, &meta); err != nil {
		return nil, true, err
	}
	data, err = proto.Marshal(&meta)
	return data, true, err
}

// SetContract stores contract metadata at addr.
func (s *StateDB) SetContract(addr tcommon.Address, contract *contractpb.SmartContract) {
	obj := s.getOrCreateAccountForField(addr, TransactionAccountFieldExistence)
	// Clone prevMeta so the journal holds a snapshot of the pre-mutation state.
	// Callers often mutate the pointer returned by GetContract in-place and then
	// call SetContract with the same pointer; without cloning, prevMeta would
	// already reflect the mutation and RevertToSnapshot would be a no-op.
	var prevMeta *contractpb.SmartContract
	if obj.contractMeta != nil {
		prevMeta = proto.Clone(obj.contractMeta).(*contractpb.SmartContract)
	}
	s.journal.append(contractMetaChange{
		address:  addr,
		prevMeta: prevMeta,
	})
	obj.contractMeta = contract
	obj.contractMetaDirty = true
	obj.invalidateStorageKeyLayout()
	obj.markDirty()
}

// ReadContractState loads the per-contract dynamic-energy runtime state.
func (s *StateDB) ReadContractState(addr tcommon.Address) *types.ContractState {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return nil
	}
	data, ok, err := s.getAccountKVForDecoding(addr, kvdomains.ContractRuntimeState, contractStateKVKey)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read contract runtime state %s", addr.Hex()), err)
		return nil
	}
	if !ok {
		return nil
	}
	if len(data) == 0 {
		s.recordStateError(fmt.Sprintf("decode contract runtime state %s", addr.Hex()), errors.New("empty value"))
		return nil
	}
	pb := new(contractpb.ContractState)
	if err := statecodec.Unmarshal(data, pb); err != nil {
		s.recordStateError(fmt.Sprintf("decode contract runtime state %s", addr.Hex()), err)
		return nil
	}
	return types.NewContractStateFromPB(pb)
}

// WriteContractState stores the per-contract dynamic-energy runtime state.
func (s *StateDB) WriteContractState(addr tcommon.Address, cs *types.ContractState) error {
	if cs == nil {
		return nil
	}
	data, err := statecodec.Marshal(cs.Proto())
	if err != nil {
		return err
	}
	return s.SetAccountKV(addr, kvdomains.ContractRuntimeState, contractStateKVKey, data)
}

// ReadContractABI loads the dedicated ABI store entry for a contract.
func (s *StateDB) ReadContractABI(addr tcommon.Address) *contractpb.SmartContract_ABI {
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil || obj.deleted {
		return nil
	}
	data, ok, err := s.getAccountKVForDecoding(addr, kvdomains.ContractABI, contractABIKVKey)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read contract ABI %s", addr.Hex()), err)
		return nil
	}
	if !ok {
		return nil
	}
	if len(data) == 0 {
		s.recordStateError(fmt.Sprintf("decode contract ABI %s", addr.Hex()), errors.New("empty value"))
		return nil
	}
	var abi contractpb.SmartContract_ABI
	if err := statecodec.Unmarshal(data, &abi); err != nil {
		s.recordStateError(fmt.Sprintf("decode contract ABI %s", addr.Hex()), err)
		return nil
	}
	return &abi
}

// WriteContractABI stores a dedicated ABI entry for a contract.
func (s *StateDB) WriteContractABI(addr tcommon.Address, abi *contractpb.SmartContract_ABI) error {
	if abi == nil {
		return s.DeleteAccountKV(addr, kvdomains.ContractABI, contractABIKVKey)
	}
	data, err := statecodec.Marshal(abi)
	if err != nil {
		return err
	}
	return s.SetAccountKV(addr, kvdomains.ContractABI, contractABIKVKey, data)
}

// IsContract returns whether the address has contract code or metadata.
func (s *StateDB) IsContract(addr tcommon.Address) bool {
	_, hasMetadata := s.ContractRuntime(addr)
	return hasMetadata || len(s.GetCode(addr)) > 0
}

// Exist returns whether an account exists (non-nil and not deleted).
func (s *StateDB) Exist(addr tcommon.Address) bool {
	return s.AccountExists(addr)
}

// Empty returns whether an account is empty (no balance, no code).
func (s *StateDB) Empty(addr tcommon.Address) bool {
	if !s.AccountExists(addr) {
		return true
	}
	return s.GetBalance(addr) == 0 && len(s.GetCode(addr)) == 0
}

// SelfDestruct marks an account as self-destructed.
func (s *StateDB) SelfDestruct(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	prevCode := append([]byte(nil), s.GetCode(addr)...)
	var prevMeta *contractpb.SmartContract
	if meta := s.GetContract(addr); meta != nil {
		prevMeta = proto.Clone(meta).(*contractpb.SmartContract)
	}
	s.journalAccount(addr, obj)
	s.journal.append(codeChange{
		address:  addr,
		prevCode: prevCode,
		prevHash: obj.codeHash,
	})
	s.journal.append(contractMetaChange{
		address:  addr,
		prevMeta: prevMeta,
	})
	s.journal.append(selfDestructChange{
		address: addr,
		prev:    obj.selfDestructed,
	})
	obj.markSelfDestructed()
	// FinalizeTransaction deletes self-destructed accounts at the boundary;
	// record the address so the boundary scan can skip untouched objects.
	s.txFinalizeDirty[addr] = struct{}{}
}

// DeleteAccount removes an account from flat account latest on commit.
func (s *StateDB) DeleteAccount(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.recordBalanceTraceChange(addr, -obj.account.Balance())
	s.touchBalanceTraceAccount(addr)
	prevCode := append([]byte(nil), s.GetCode(addr)...)
	var prevMeta *contractpb.SmartContract
	if meta := s.GetContract(addr); meta != nil {
		prevMeta = proto.Clone(meta).(*contractpb.SmartContract)
	}
	s.journalAccount(addr, obj)
	s.journal.append(codeChange{
		address:  addr,
		prevCode: prevCode,
		prevHash: obj.codeHash,
	})
	s.journal.append(contractMetaChange{
		address:  addr,
		prevMeta: prevMeta,
	})
	obj.code = nil
	obj.codeHash = tcommon.Hash{}
	obj.codeDirty = true
	obj.contractMeta = nil
	obj.contractMetaDirty = true
	obj.deleted = true
	obj.markDirty()
}

// HasSelfDestructed returns whether the account has been self-destructed.
func (s *StateDB) HasSelfDestructed(addr tcommon.Address) bool {
	s.recordSelfDestructRead(addr)
	obj := s.getStateObjectForField(addr, TransactionAccountFieldExistence)
	if obj == nil {
		return false
	}
	return obj.selfDestructed
}

// Copy creates a deep copy of the StateDB for read-only execution.
//
// NOTE: the journal is NOT copied — `cp.journal` is a fresh empty journal.
// Production uses Copy only for read-only VM execution (eth_call /
// debug_traceCall snapshots), where temporal history capture is not invoked.
func (s *StateDB) Copy() (*StateDB, error) {
	// CommitScope can retain several committed-but-not-yet-flushed latest-domain
	// batches. A read-only copy must share those read views; falling back to the
	// physical index would silently execute against an older head. Copy callers
	// run while the parent execution view is stable, and all mutations below stay
	// in copy-owned state objects and journals.
	cp := s.newCopyBase(len(s.stateObjects))
	for addr, obj := range s.stateObjects {
		s.copyStateObjectInto(cp, addr, obj, true)
	}
	return cp, nil
}

// CopyBlockExecutionBase creates an independent execution view while omitting
// clean objects from the source's bounded cross-block account cache. Clean
// state is already authoritative in the shared latest-domain/blockbuffer read
// views and is hydrated lazily if the speculative execution actually touches
// it. Only uncommitted dirty objects need an eager private copy.
//
// This is the Erigon-style block-start copy path used by the OCC observers and
// narrow parallel publisher. The caller must finish every copy-owned execution
// before publishing the source block's latest-domain writes; ordinary Copy
// retains its unrestricted deep-copy semantics for RPC/debug callers.
func (s *StateDB) CopyBlockExecutionBase() (*StateDB, error) {
	cp := s.newCopyBase(len(s.dirtyObjects))
	copied := 0
	for addr := range s.dirtyObjects {
		obj := s.stateObjects[addr]
		if obj == nil || !obj.dirty {
			continue
		}
		// Clean storage cache entries are already authoritative in the stable
		// latest view shared by the execution copy. Preserve only slots changed
		// by this uncommitted block; untouched slots can hydrate lazily without
		// cloning an account's accumulated cross-block read cache.
		s.copyStateObjectInto(cp, addr, obj, false)
		copied++
	}
	// Contract code is immutable, but it is not guaranteed to remain available
	// from the same hot reader for the full lifetime of the cross-block account
	// cache (for example after its content-addressed row moves to cold history).
	// The canonical StateDB can still execute from obj.code in that interval.
	// Preserve the same cached code in speculative block-start views so a clean
	// contract can never degrade into a successful empty-code call merely because
	// execution copies otherwise omit clean objects.
	cachedCode := 0
	for addr, obj := range s.stateObjects {
		if obj == nil || obj.dirty || obj.deleted || len(obj.code) == 0 {
			continue
		}
		if _, exists := cp.stateObjects[addr]; exists {
			continue
		}
		if !s.copyCachedCodeStateObjectInto(cp, addr, obj) {
			continue
		}
		copied++
		cachedCode++
	}
	source := len(s.stateObjects)
	stateExecutionCopySourceCounter.Inc(int64(source))
	stateExecutionCopyDirtyCounter.Inc(int64(copied - cachedCode))
	stateExecutionCopyCachedCodeCounter.Inc(int64(cachedCode))
	stateExecutionCopyOmittedCounter.Inc(int64(source - copied))
	return cp, nil
}

func (s *StateDB) newCopyBase(accountHint int) *StateDB {
	witnesses := make(map[tcommon.Address]*types.Witness, len(s.witnesses))
	for addr, witness := range s.witnesses {
		if witness != nil {
			witnesses[addr] = witness.Copy()
		}
	}
	dirtyWitnesses := make(map[tcommon.Address]struct{}, len(s.dirtyWitnesses))
	for addr := range s.dirtyWitnesses {
		dirtyWitnesses[addr] = struct{}{}
	}
	return &StateDB{
		db:           s.db,
		dbErr:        s.dbErr,
		stateObjects: make(map[tcommon.Address]*stateObject, accountHint),
		// Witness updates are cached outside stateObjects and are flushed only
		// after transaction execution. Copies used by speculative workers and
		// the canonical-boundary oracle must therefore retain the exact in-memory
		// witness view instead of rehydrating an older durable capsule.
		witnesses:               witnesses,
		dirtyWitnesses:          dirtyWitnesses,
		standbyWitnessVersion:   s.standbyWitnessVersion,
		txFinalizeDirty:         make(map[tcommon.Address]struct{}),
		dirtyObjects:            make(map[tcommon.Address]struct{}, accountHint),
		journal:                 newJournal(),
		dynProps:                s.dynProps,
		originRoot:              s.originRoot,
		accountKVIndexStore:     s.accountKVIndexStore,
		accountKVLatestReader:   s.accountKVLatestReader,
		accountKVLatestIterator: s.accountKVLatestIterator,
		flatLatestReader:        s.flatLatestReader,
		codeStore:               s.codeStore,
		codeColdHistory:         s.codeColdHistory,
		codeColdTxNum:           s.codeColdTxNum,
		commitmentColdHistory:   s.commitmentColdHistory,
		commitmentColdTxNum:     s.commitmentColdTxNum,
	}
}

func (s *StateDB) copyStateObjectInto(cp *StateDB, addr tcommon.Address, obj *stateObject, copyCleanStorage bool) {
	if cp == nil || obj == nil {
		return
	}
	var metaCopy *contractpb.SmartContract
	if obj.contractMeta != nil {
		metaCopy = proto.Clone(obj.contractMeta).(*contractpb.SmartContract)
	}
	var kvDirtyCopy map[string]kvEntry
	if len(obj.kvDirty) != 0 {
		kvDirtyCopy = make(map[string]kvEntry, len(obj.kvDirty))
	}
	for k, v := range obj.kvDirty {
		ec := kvEntry{
			deleted:    v.deleted,
			prevExists: v.prevExists,
			prevLoaded: v.prevLoaded,
		}
		if v.val != nil {
			ec.val = append([]byte{}, v.val...)
		}
		if v.prev != nil {
			ec.prev = append([]byte{}, v.prev...)
		}
		kvDirtyCopy[k] = ec
	}
	storageEntries := len(obj.storage)
	if !copyCleanStorage {
		storageEntries = len(obj.dirtyStorage)
	}
	var storageCopy map[tcommon.Hash]storageSlot
	if storageEntries != 0 {
		storageCopy = make(map[tcommon.Hash]storageSlot, storageEntries)
	}
	var dirtyStorageCopy map[tcommon.Hash]storageOrigin
	if len(obj.dirtyStorage) != 0 {
		dirtyStorageCopy = make(map[tcommon.Hash]storageOrigin, len(obj.dirtyStorage))
	}
	newObj := acquireStateObject()
	*newObj = stateObject{
		address:                  addr,
		dirty:                    obj.dirty,
		accountDirty:             obj.accountDirty,
		deleted:                  obj.deleted,
		created:                  obj.created,
		code:                     append([]byte{}, obj.code...),
		codeHash:                 obj.codeHash,
		codeDirty:                obj.codeDirty,
		contractMeta:             metaCopy,
		contractMetaDirty:        obj.contractMetaDirty,
		storage:                  storageCopy,
		storageHighWater:         storageEntries,
		dirtyStorage:             dirtyStorageCopy,
		dirtyStorageHighWater:    len(obj.dirtyStorage),
		selfDestructed:           obj.selfDestructed,
		accountKVRoot:            obj.accountKVRoot,
		accountKVGeneration:      obj.accountKVGeneration,
		accountKVGenerationDirty: obj.accountKVGenerationDirty,
		kvDirty:                  kvDirtyCopy,
		kvDirtyHighWater:         len(obj.kvDirty),
	}
	if obj.account != nil {
		newObj.account = obj.account.Copy()
		if obj.accountProto != nil && types.IsAccountStorageCoreV4(obj.accountProto) {
			newObj.accountProto = bytes.Clone(obj.accountProto)
		} else {
			newObj.accountProto, _ = newObj.account.MarshalStorageCoreV4()
		}
	}
	if copyCleanStorage {
		for k, v := range obj.storage {
			newObj.storage[k] = v
		}
	} else {
		for k := range obj.dirtyStorage {
			if v, ok := obj.storage[k]; ok {
				newObj.storage[k] = v
			}
		}
	}
	for k, origin := range obj.dirtyStorage {
		newObj.dirtyStorage[k] = origin
	}
	newObj.dirtySet = cp.dirtyObjects
	if newObj.dirty {
		cp.dirtyObjects[addr] = struct{}{}
	}
	cp.stateObjects[addr] = newObj
}

// copyCachedCodeStateObjectInto retains the minimum clean account envelope
// needed to execute already-loaded immutable bytecode. Storage and account-KV
// caches remain lazy; copying those potentially large read caches would undo
// the purpose of CopyBlockExecutionBase.
func (s *StateDB) copyCachedCodeStateObjectInto(cp *StateDB, addr tcommon.Address, obj *stateObject) bool {
	if cp == nil || obj == nil || obj.account == nil || len(obj.code) == 0 {
		return false
	}
	var metaCopy *contractpb.SmartContract
	if obj.contractMeta != nil {
		metaCopy = proto.Clone(obj.contractMeta).(*contractpb.SmartContract)
	}
	data, err := obj.account.Marshal()
	if err != nil {
		return false
	}
	account, err := types.UnmarshalAccount(data)
	if err != nil {
		return false
	}
	newObj := acquireStateObject()
	*newObj = stateObject{
		address:                  addr,
		account:                  account,
		code:                     obj.code,
		codeHash:                 obj.codeHash,
		contractMeta:             metaCopy,
		selfDestructed:           obj.selfDestructed,
		accountKVRoot:            obj.accountKVRoot,
		accountKVGeneration:      obj.accountKVGeneration,
		accountKVGenerationDirty: false,
		dirtySet:                 cp.dirtyObjects,
	}
	newObj.accountProto, _ = account.MarshalStorageCoreV4()
	cp.stateObjects[addr] = newObj
	return true
}

type accountCommitPlan struct {
	addr          tcommon.Address
	obj           *stateObject
	deleteAccount bool
	// Storage staging only needs per-kind totals after preparation. Keeping
	// counters here avoids retaining one result object (and a duplicate value)
	// for every dirty slot until mutation statistics are summarized.
	storagePuts        int
	storageDeletes     int
	storageNoops       int
	kvPlan             *accountKVCommitPlan
	hadKVDirty         bool
	accountLatestDirty bool
	// accountProtoSize is measured without marshaling during the first commit
	// pass, then reused to append the protobuf directly into the value arena.
	accountProtoSize int
}

// Keep normal block-sized commit-planning workspaces on the range-reused
// StateDB, but do not pin an anomalously large block forever. A TRON block is
// capped at 2 MB; this bound covers the observed live-sync workload with room
// to spare while limiting retained scratch to well below that size.
const maxRetainedAccountCommitPlans = 4096

type accountCommitPlanWorkspace struct {
	addrs       []tcommon.Address
	planStorage []accountCommitPlan
	plans       []*accountCommitPlan
}

func (s *StateDB) releaseAccountCommitPlans(plans []*accountCommitPlan) {
	releaseAccountKVCommitPlans(plans)

	w := &s.accountCommitPlans
	clear(w.planStorage)
	clear(w.plans)
	if cap(w.addrs) > maxRetainedAccountCommitPlans {
		w.addrs = nil
	} else {
		w.addrs = w.addrs[:0]
	}
	if cap(w.planStorage) > maxRetainedAccountCommitPlans {
		w.planStorage = nil
	} else {
		w.planStorage = w.planStorage[:0]
	}
	if cap(w.plans) > maxRetainedAccountCommitPlans {
		w.plans = nil
	} else {
		w.plans = w.plans[:0]
	}
}

func (s *StateDB) dirtyAccountCommitPlans() ([]*accountCommitPlan, error) {
	// Iterate the incrementally-maintained dirty-address set instead of scanning
	// the whole stateObjects map (which accumulates the range's read-only working
	// set on the reused-StateDB sync path). The set is a complete superset of the
	// dirty objects; the obj == nil / !obj.dirty guards filter stale entries (a
	// created-then-reverted address, or one re-dirtied to its original value),
	// reproducing the old `for addr, obj := range s.stateObjects` filter exactly.
	// The sort keeps the commit order address-deterministic, byte-identical to
	// the full scan.
	w := &s.accountCommitPlans
	if cap(w.addrs) < len(s.dirtyObjects) {
		w.addrs = make([]tcommon.Address, 0, len(s.dirtyObjects))
	} else {
		w.addrs = w.addrs[:0]
	}
	for addr := range s.dirtyObjects {
		obj := s.stateObjects[addr]
		if obj == nil || !obj.dirty {
			continue
		}
		w.addrs = append(w.addrs, addr)
	}
	slices.SortFunc(w.addrs, func(a, b tcommon.Address) int {
		return bytes.Compare(a[:], b[:])
	})

	// Downstream phases keep pointer-based plans, but every plan lives only for
	// this commit. Allocate their stable addresses from one commit-sized arena
	// instead of creating one heap object per dirty account.
	storageCompositeBytes := 0
	for _, addr := range w.addrs {
		storageCompositeBytes += len(s.stateObjects[addr].dirtyStorage) * (2 + tcommon.HashLength)
	}
	// Map keys and OwnedWriter calls may lend slices backed by this arena to the
	// unsolidified blockbuffer. Allocate one immutable arena per commit; retaining
	// and overwriting it on the next block would corrupt those borrowed keys.
	storageKeyArena := make([]byte, 0, storageCompositeBytes)
	if cap(w.planStorage) < len(w.addrs) {
		w.planStorage = make([]accountCommitPlan, len(w.addrs))
	} else {
		w.planStorage = w.planStorage[:len(w.addrs)]
		clear(w.planStorage)
	}
	if cap(w.plans) < len(w.addrs) {
		w.plans = make([]*accountCommitPlan, len(w.addrs))
	} else {
		w.plans = w.plans[:len(w.addrs)]
		clear(w.plans)
	}
	for i, addr := range w.addrs {
		plan := &w.planStorage[i]
		w.plans[i] = plan
		if err := s.prepareAccountCommitPlanWithArena(addr, s.stateObjects[addr], plan, &storageKeyArena); err != nil {
			s.releaseAccountCommitPlans(w.plans[:i+1])
			return nil, err
		}
	}
	return w.plans, nil
}

func (s *StateDB) prepareAccountCommitPlan(addr tcommon.Address, obj *stateObject, plan *accountCommitPlan) error {
	storageKeyArena := make([]byte, 0, len(obj.dirtyStorage)*(2+tcommon.HashLength))
	return s.prepareAccountCommitPlanWithArena(addr, obj, plan, &storageKeyArena)
}

func (s *StateDB) prepareAccountCommitPlanWithArena(addr tcommon.Address, obj *stateObject, plan *accountCommitPlan, storageKeyArena *[]byte) error {
	*plan = accountCommitPlan{
		addr:               addr,
		obj:                obj,
		deleteAccount:      obj.deleted || obj.selfDestructed,
		accountLatestDirty: obj.accountDirty || obj.created || obj.codeDirty || obj.accountKVGenerationDirty,
	}
	if plan.deleteAccount {
		return nil
	}
	if obj.contractMetaDirty {
		if obj.contractMeta == nil {
			if _, err := s.stageAccountKVCommit(obj, kvdomains.ContractMetadata, contractMetaKVKey, nil, true); err != nil {
				return err
			}
			if _, err := s.stageAccountKVCommit(obj, kvdomains.ContractABI, contractABIKVKey, nil, true); err != nil {
				return err
			}
		} else {
			metaBytes, err := statecodec.Marshal(obj.contractMeta)
			if err != nil {
				return fmt.Errorf("marshal contractMeta for %s: %w", addr.Hex(), err)
			}
			if _, err := s.stageAccountKVCommit(obj, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes, false); err != nil {
				return err
			}
		}
	}

	if len(obj.dirtyStorage) > 0 {
		storageKeys := make([]tcommon.Hash, 0, len(obj.dirtyStorage))
		for key := range obj.dirtyStorage {
			storageKeys = append(storageKeys, key)
		}
		slices.SortFunc(storageKeys, func(a, b tcommon.Hash) int {
			return bytes.Compare(a[:], b[:])
		})
		for _, key := range storageKeys {
			value := obj.storage[key].value
			origin := obj.dirtyStorage[key]
			rowKey := s.storageRowKey(addr, key)
			if origin.loaded {
				staged := s.stageStorageCommitWithPrevArena(obj, rowKey, value, value == (tcommon.Hash{}), origin, storageKeyArena)
				if staged {
					if value == (tcommon.Hash{}) {
						plan.storageDeletes++
					} else {
						plan.storagePuts++
					}
				} else {
					plan.storageNoops++
				}
				continue
			}
			if value == (tcommon.Hash{}) {
				staged, err := s.stageAccountKVCommitWithPrev(obj, kvdomains.ContractStorage, rowKey.Bytes(), nil, true, nil, origin.exists, false)
				if err != nil {
					return err
				}
				if staged {
					plan.storageDeletes++
				} else {
					plan.storageNoops++
				}
				continue
			}
			staged, err := s.stageAccountKVCommitWithPrev(obj, kvdomains.ContractStorage, rowKey.Bytes(), value.Bytes(), false, nil, origin.exists, false)
			if err != nil {
				return err
			}
			if staged {
				plan.storagePuts++
			} else {
				plan.storageNoops++
			}
		}
	}

	plan.hadKVDirty = len(obj.kvDirty) > 0
	if plan.hadKVDirty {
		kvPlan, err := s.prepareAccountKVCommitPlan(obj)
		if err != nil {
			return err
		}
		plan.kvPlan = kvPlan
	}
	return nil
}

func (s *StateDB) applyAccountPlanFlat(plan *accountCommitPlan, accountKVIndex accountKVIndexStore, accountKVLatestWriter statedomains.Writer) error {
	obj := plan.obj
	if plan.deleteAccount {
		return nil
	}

	if obj.codeDirty {
		if len(obj.code) != 0 && obj.codeHash != (tcommon.Hash{}) {
			if err := s.writeStateCode(obj.codeHash, obj.code); err != nil {
				return err
			}
		}
	}
	if plan.hadKVDirty {
		if _, err := s.commitAccountKVLatest(obj, plan.kvPlan, accountKVLatestWriter); err != nil {
			return err
		}
	}
	return nil
}

// accountCommitPlanGenerationResolver searches the already address-sorted
// commit plans produced by dirtyAccountCommitPlans. Keeping the resolver on
// that immutable slice avoids rebuilding an address->generation map for every
// block while retaining logarithmic lookups and the same missing-owner check.
func accountCommitPlanGenerationResolver(plans []*accountCommitPlan) statedomains.GenerationResolver {
	return func(owner tcommon.Address) (uint64, error) {
		low, high := 0, len(plans)
		for low < high {
			mid := int(uint(low+high) >> 1)
			plan := plans[mid]
			if plan == nil || plan.obj == nil {
				return 0, errors.New("account kv generation resolver: invalid commit plan")
			}
			switch bytes.Compare(plan.addr[:], owner[:]) {
			case -1:
				low = mid + 1
			case 1:
				high = mid
			default:
				return plan.obj.accountKVGeneration, nil
			}
		}
		return 0, fmt.Errorf("account kv generation for %s not in commit plan", owner.Hex())
	}
}

func encodeAccountLatestObject(obj *stateObject, flatRoot bool) ([]byte, bool, error) {
	return appendAccountLatestObject(nil, obj, flatRoot)
}

func accountLatestObjectEncodedSize(obj *stateObject) (encodedSize, accountProtoSize int, exists bool, err error) {
	if obj == nil || obj.deleted || obj.selfDestructed || obj.account == nil {
		return 0, 0, false, nil
	}
	if obj.accountProto != nil && types.IsAccountStorageCoreV4(obj.accountProto) {
		return stateAccountV2EncodedSize(StateAccountVersion, obj.accountProto, obj.accountKVGeneration), len(obj.accountProto), true, nil
	}
	// A cached non-native core is an internal invariant violation. Re-encode the
	// authoritative in-memory account rather than copying it into rooted state.
	obj.accountProto = nil
	obj.accountProtoLoaded = false
	accountProtoSize, err = obj.account.StorageCoreV4Size()
	if err != nil {
		return 0, 0, false, err
	}
	if accountProtoSize == 1 {
		// RLP encodes a one-byte string differently when the byte is below 0x80.
		// Structured Account protobufs do not normally reach this case; retain the
		// authoritative byte-aware path for synthetic unknown-field values.
		accBytes, marshalErr := obj.deterministicAccountProto()
		if marshalErr != nil {
			return 0, 0, false, marshalErr
		}
		return stateAccountV2EncodedSize(StateAccountVersion, accBytes, obj.accountKVGeneration), len(accBytes), true, nil
	}
	return stateAccountV2EncodedSizeFromProtoSize(StateAccountVersion, accountProtoSize, obj.accountKVGeneration), accountProtoSize, true, nil
}

func appendAccountLatestObject(dst []byte, obj *stateObject, flatRoot bool) ([]byte, bool, error) {
	if obj == nil || obj.deleted || obj.selfDestructed || obj.account == nil {
		return dst, false, nil
	}
	accBytes, err := obj.deterministicAccountProto()
	if err != nil {
		return dst, false, err
	}
	return appendAccountLatestObjectFromProto(dst, obj, accBytes, flatRoot), true, nil
}

// encodeAccountLatestObjectFromProto wraps an already-serialized account in its
// flat latest-domain envelope. Callers that also need the raw account bytes can
// reuse the same deterministic protobuf encoding instead of marshaling maps a
// second time.
func encodeAccountLatestObjectFromProto(obj *stateObject, accBytes []byte, flatRoot bool) ([]byte, error) {
	return appendAccountLatestObjectFromProto(nil, obj, accBytes, flatRoot), nil
}

func appendAccountLatestObjectFromProto(dst []byte, obj *stateObject, accBytes []byte, flatRoot bool) []byte {
	accountKVRoot := obj.accountKVRoot
	if flatRoot {
		accountKVRoot = EmptyKVRoot
	}
	return appendStateAccountV2Fields(dst, StateAccountVersion, accBytes, accountKVRoot, obj.accountKVGeneration, obj.codeHash)
}

// appendAccountLatestObjectPrepared encodes an uncached storage-core protobuf
// directly into the final commit-wide value arena. The protobuf sub-slice also
// becomes the state object's post-image cache, avoiding a second owned copy.
func appendAccountLatestObjectPrepared(dst []byte, obj *stateObject, flatRoot bool, accountProtoSize int) ([]byte, bool, error) {
	if obj == nil || obj.deleted || obj.selfDestructed || obj.account == nil {
		return dst, false, nil
	}
	if obj.accountProto != nil && types.IsAccountStorageCoreV4(obj.accountProto) {
		return appendAccountLatestObjectFromProto(dst, obj, obj.accountProto, flatRoot), true, nil
	}
	obj.accountProto = nil
	obj.accountProtoLoaded = false
	if accountProtoSize == 1 {
		// accountLatestObjectEncodedSize materializes and caches this exceptional
		// value so the byte-aware ordinary path above handles its RLP encoding.
		return dst, false, errors.New("one-byte account proto missing prepared cache")
	}
	accountKVRoot := obj.accountKVRoot
	if flatRoot {
		accountKVRoot = EmptyKVRoot
	}
	dst = appendStateAccountV2StorageCorePrefix(dst, StateAccountVersion, accountProtoSize, obj.accountKVGeneration)
	protoStart := len(dst)
	var err error
	dst, err = obj.account.AppendStorageCoreV4(dst)
	if err != nil {
		return dst, false, err
	}
	protoEnd := len(dst)
	if protoEnd-protoStart != accountProtoSize {
		return dst, false, fmt.Errorf("account storage core size changed during commit: encoded %d, want %d", protoEnd-protoStart, accountProtoSize)
	}
	dst = appendStateAccountV2StorageCoreTrailer(dst, accountKVRoot, obj.accountKVGeneration, obj.codeHash)
	obj.accountProto = dst[protoStart:protoEnd:protoEnd]
	obj.accountProtoLoaded = false
	return dst, true, nil
}

func (s *StateDB) writeFlatAccountLatestWithPlan(plan *accountCommitPlan, commitment *DomainCommitmentState, latestWriter *accountKVLatestBatch, physicalKey, accountLatestData []byte, accountLatestExists bool) error {
	if plan == nil || plan.obj == nil {
		return nil
	}
	obj := plan.obj
	addr := plan.addr
	if plan.deleteAccount {
		if len(physicalKey) == 0 {
			physicalKey = rawdb.StateAccountLatestCommitmentKey(addr)
		}
		if err := s.writeAccountLatestChange(addr, false, nil); err != nil {
			return err
		}
		if latestWriter == nil {
			return fmt.Errorf("account latest writer unavailable")
		}
		if err := latestWriter.deleteAccountLatestByKey(addr, physicalKey); err != nil {
			return err
		}
		commitment.recordAccountLatestTouch(addr)
		if err := s.writeAccountKVGeneration(obj, commitment, latestWriter); err != nil {
			return err
		}
		return nil
	}
	generationDirty := obj.accountKVGenerationDirty
	// Flat latest-domain layout keeps accountKVRoot fixed at EmptyKVRoot.
	// Pure account-KV mutations are represented by KV latest rows, so they do
	// not need to rewrite the account envelope or touch account commitment keys.
	needsAccountLatestUpdate := plan.accountLatestDirty
	if generationDirty {
		if err := s.writeAccountKVGeneration(obj, commitment, latestWriter); err != nil {
			return err
		}
	}
	if !needsAccountLatestUpdate {
		return nil
	}
	if !accountLatestExists {
		return nil
	}
	if len(physicalKey) == 0 {
		physicalKey = rawdb.StateAccountLatestCommitmentKey(addr)
	}
	if err := s.writeAccountLatestChange(addr, true, accountLatestData); err != nil {
		return err
	}
	if latestWriter == nil {
		return fmt.Errorf("account latest writer unavailable")
	}
	if err := latestWriter.writeAccountLatestOwnedByKey(addr, physicalKey, accountLatestData); err != nil {
		return err
	}
	commitment.recordAccountLatestTouch(addr)
	return nil
}

func (s *StateDB) writeAccountLatestChange(addr tcommon.Address, nextExists bool, next []byte) error {
	if s == nil || !s.changeSet.enabled || !s.changeSet.captureAtCommit {
		return nil
	}
	prev, prevExists, err := s.readStateAccountLatest(addr)
	if err != nil {
		return err
	}
	if prevExists == nextExists && (!nextExists || bytes.Equal(prev, next)) {
		return nil
	}
	return s.changeSet.publishCommitDomainChange(&rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      addr,
		PrevExists: prevExists,
		Prev:       prev,
		NextExists: nextExists,
		Next:       next,
	})
}

func (s *StateDB) writeFlatAccountLatestPlans(plans []*accountCommitPlan, flatRoot bool, commitment *DomainCommitmentState, latestWriter *accountKVLatestBatch) error {
	// dirtyAccountCommitPlans already emits address-sorted plans. Reuse that
	// immutable ordering instead of allocating and sorting a second pointer
	// slice during every block commit.
	orderedPlans := plans
	accountLatestWrites := 0
	accountLatestValueBytes := 0
	for _, plan := range orderedPlans {
		if plan != nil && (plan.deleteAccount || plan.accountLatestDirty) {
			accountLatestWrites++
		}
		if plan == nil || plan.deleteAccount || !plan.accountLatestDirty {
			continue
		}
		size, protoSize, exists, err := accountLatestObjectEncodedSize(plan.obj)
		if err != nil {
			return err
		}
		if exists {
			accountLatestValueBytes += size
			plan.accountProtoSize = protoSize
		}
	}
	keyArena := make([]byte, 0, accountLatestWrites*rawdb.StateAccountLatestCommitmentKeySize())
	// The first pass measured every uncached protobuf without materializing it;
	// the append pass writes both envelope and protobuf into this final arena.
	valueArena := make([]byte, 0, accountLatestValueBytes)
	for _, plan := range orderedPlans {
		var physicalKey []byte
		var accountLatestData []byte
		var accountLatestExists bool
		if plan != nil && (plan.deleteAccount || plan.accountLatestDirty) {
			start := len(keyArena)
			keyArena = rawdb.AppendStateAccountLatestCommitmentKey(keyArena, plan.addr)
			physicalKey = keyArena[start:len(keyArena):len(keyArena)]
		}
		if plan != nil && !plan.deleteAccount && plan.accountLatestDirty {
			start := len(valueArena)
			var err error
			valueArena, accountLatestExists, err = appendAccountLatestObjectPrepared(valueArena, plan.obj, flatRoot, plan.accountProtoSize)
			if err != nil {
				return err
			}
			if accountLatestExists {
				accountLatestData = valueArena[start:len(valueArena):len(valueArena)]
			}
		}
		if err := s.writeFlatAccountLatestWithPlan(plan, commitment, latestWriter, physicalKey, accountLatestData, accountLatestExists); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateDB) finalizeAccountCommitPlan(plan *accountCommitPlan) {
	obj := plan.obj
	if plan.deleteAccount {
		// The account incarnation is ending; sampled keys from an earlier
		// oversized-cache release can no longer be reloaded by this object.
		s.settleOversizedStorageSample(obj.address)
		obj.releaseKVDirty()
		obj.deleted = true
		obj.selfDestructed = false
		obj.code = nil
		obj.codeHash = tcommon.Hash{}
		obj.codeDirty = false
		obj.accountDirty = false
		obj.contractMeta = nil
		obj.contractMetaDirty = false
		obj.releaseStorage()
		obj.releaseDirtyStorage()
		obj.dirty = false
		return
	}
	if plan.hadKVDirty {
		obj.releaseKVDirty()
	}
	if obj.codeDirty {
		obj.codeDirty = false
	}
	if obj.contractMetaDirty {
		obj.contractMetaDirty = false
	}
	obj.accountDirty = false
	// Origins are only needed between the first SSTORE and this commit. Detach
	// the map afterward so non-contract accounts stay allocation-free; bounded
	// cleared buckets are available to a later storage-writing account.
	obj.releaseDirtyStorage()
	obj.created = false
	obj.dirty = false
}

// Commit writes all dirty latest-domain rows and returns the new CommitmentDomain root.
func (s *StateDB) Commit() (tcommon.Hash, error) {
	root, _, err := s.CommitWithStats()
	return root, err
}

// CommitWithStats writes all dirty latest-domain rows and returns the new
// CommitmentDomain root plus a phase breakdown for sync/profile diagnostics.
func (s *StateDB) CommitWithStats() (tcommon.Hash, CommitStats, error) {
	return s.CommitWithStatsOptions(CommitOptions{})
}

// CommitWithStatsOptions writes dirty latest-domain rows and updates the
// CommitmentDomain root. The options parameter is retained for API stability;
// there is no longer a rooted trie commit mode.
func (s *StateDB) CommitWithStatsOptions(opts CommitOptions) (tcommon.Hash, CommitStats, error) {
	return s.commitWithStatsOptions(opts, nil)
}

func (s *StateDB) commitWithStatsOptions(opts CommitOptions, scope *CommitScope) (tcommon.Hash, CommitStats, error) {
	var stats CommitStats
	if err := s.Error(); err != nil {
		return tcommon.Hash{}, stats, fmt.Errorf("state commit blocked by prior read failure: %w", err)
	}
	last := time.Now()
	mark := func(phase *time.Duration) {
		now := time.Now()
		*phase += now.Sub(last)
		last = now
	}
	if scope != nil && scope.state != s {
		return tcommon.Hash{}, stats, errors.New("state commit scope belongs to a different StateDB")
	}

	if s.changeSet.enabled {
		if err := defaultStateDomainChangeRunner(s.changeSet.writer).PublishStateTxRange(s.changeSet.blockNum, s.changeSet.blockHash, s.changeSet.beginTxNum, s.changeSet.endTxNum); err != nil {
			return tcommon.Hash{}, stats, err
		}
		defer func() {
			s.changeSet = domainChangeSetCapture{}
		}()
	}
	plans, err := s.dirtyAccountCommitPlans()
	if err != nil {
		return tcommon.Hash{}, stats, err
	}
	defer s.releaseAccountCommitPlans(plans)
	stats.Accounts = len(plans)
	stats.Mutations = summarizeCommitMutations(plans)
	commitmentTouchCapacity := 0
	for _, plan := range plans {
		if plan == nil || plan.obj == nil {
			continue
		}
		if plan.deleteAccount {
			// Account deletion writes both the account-latest tombstone and
			// its generation row.
			commitmentTouchCapacity += 2
		} else {
			if plan.accountLatestDirty {
				commitmentTouchCapacity++
			}
			if plan.obj.accountKVGenerationDirty {
				commitmentTouchCapacity++
			}
		}
		if plan.kvPlan != nil && len(plan.kvPlan.items) > 0 {
			stats.KVAccounts++
			stats.KVItems += len(plan.kvPlan.items)
			commitmentTouchCapacity += len(plan.kvPlan.items)
		}
	}
	mark(&stats.Prepare)

	accountKVIndex := s.accountKVIndex()
	generationResolver := accountCommitPlanGenerationResolver(plans)
	var commitmentState *DomainCommitmentState
	if scope != nil {
		commitmentState = scope.commitmentState
		if commitmentState == nil {
			commitmentState = NewDomainCommitmentState(s)
			scope.commitmentState = commitmentState
		}
		commitmentState.resetForCommit(s, generationResolver)
		defer commitmentState.finishCommit()
	} else {
		commitmentState = NewDomainCommitmentStateWithGenerationResolver(s, generationResolver)
	}
	// The commit plans expose the exact maximum number of distinct commitment
	// touches before any writes begin. Reserve it once instead of growing the
	// per-block mutation map and captured-value slice through multiple sizes.
	commitmentState.reserveTouches(commitmentTouchCapacity)
	var accountKVLatestWriter *accountKVLatestBatch
	var accountKVTemporalTx statedomains.TemporalTx
	if scope != nil {
		if err := scope.prepare(generationResolver, commitmentState, s.changeSet.endTxNum); err != nil {
			return tcommon.Hash{}, stats, err
		}
		accountKVLatestWriter = scope.latestWriter
		accountKVTemporalTx = scope.tx
		defer func() {
			scope.generationResolver = nil
			scope.latestReader.generation = nil
			scope.commitment.current = nil
			commitmentState.latestReader = nil
		}()
	} else {
		accountKVLatestWriter = newAccountKVLatestDomainBatch(accountKVIndex, generationResolver, &s.changeSet, nil)
		tx := statedomains.NewSharedDomainTx(statedomains.SharedDomainTxConfig{
			Latest:          statedomains.NewFlatStoreWithGenerationResolver(accountKVIndex, 0, generationResolver),
			Writer:          accountKVLatestWriter,
			History:         NewDomainHistoryState(s, s.changeSet.endTxNum),
			Commitment:      commitmentState,
			UnindexedWrites: true,
		})
		tx.SetTxNum(s.changeSet.endTxNum)
		defer tx.Close()
		accountKVTemporalTx = tx
	}
	// Tag this block's overlay puts with its number so a later partial flush can
	// prune the entries whose puts become durable, instead of accumulating the
	// whole staged range (the deep async-commit overlay leak). On the fresh-writer
	// path this is harmless: that writer is fully flushed (overlay cleared) below.
	accountKVLatestWriter.commitBlock = opts.BlockNumber
	accountKVLatestWriter.deepAsync = opts.DeepAsync
	for _, plan := range plans {
		if err := s.applyAccountPlanFlat(plan, accountKVIndex, accountKVTemporalTx); err != nil {
			if scope != nil {
				scope.discard()
			}
			return tcommon.Hash{}, stats, err
		}
	}
	mark(&stats.FlatWrite)

	if err := accountKVTemporalTx.Flush(context.Background()); err != nil {
		if scope != nil {
			scope.discard()
		}
		return tcommon.Hash{}, stats, err
	}
	if err := s.writeFlatAccountLatestPlans(plans, true, commitmentState, accountKVLatestWriter); err != nil {
		if scope != nil {
			scope.discard()
		}
		return tcommon.Hash{}, stats, err
	}
	mark(&stats.AccountTrieUpdate)

	if opts.FlushLatestDomain != nil && scope != nil {
		if err := opts.FlushLatestDomain(); err != nil {
			scope.discard()
			return tcommon.Hash{}, stats, err
		}
	} else {
		if err := accountKVLatestWriter.flush(); err != nil {
			if scope != nil {
				scope.discard()
			}
			return tcommon.Hash{}, stats, err
		}
	}
	mark(&stats.FlatFlush)

	for _, plan := range plans {
		s.finalizeAccountCommitPlan(plan)
	}
	// Every dirty object has now been finalized (obj.dirty cleared); drop the
	// per-block dirty-address set in one shot, mirroring dirtyWitnesses /
	// txFinalizeDirty. clear() (not reassignment) keeps every cached object's
	// dirtySet back-pointer valid for the next block. No object is marked dirty
	// between dirtyAccountCommitPlans and here, so nothing is lost.
	clear(s.dirtyObjects)
	mark(&stats.Finalize)

	touchBatch, err := commitmentState.latestUpdateBatchFromTouches()
	if err != nil {
		if scope != nil {
			scope.discard()
		}
		return tcommon.Hash{}, stats, err
	}
	if touchBatch != nil {
		stats.CommitmentUpdates = len(touchBatch.updates)
	}
	if s.deferFold {
		// Async commit: capture the fold inputs and hand the fold to the commit
		// worker. Reset the journal/snapshots now (the fold reads neither, so
		// this is byte-identical to resetting after the fold) so the reused
		// StateDB can begin the next block. The captured slot is read
		// synchronously by the same goroutine (TakeCapturedFold) before the next
		// block runs, so no cross-block race. originRoot is left unchanged — it
		// is write-only (no read path consumes it), and the worker computes the
		// real root via FoldLatestCommitment.
		//
		// deferFold is SINGLE-SHOT: cleared here so it must be re-armed (by
		// CommitStateCapture) for each deferred commit. This is a safety guard —
		// if a block on the reused StateDB ever falls back to the synchronous
		// commit path (e.g. a non-range single-block insert), a leftover sticky
		// deferFold would otherwise make it return a zero root instead of
		// folding. Re-arming per block keeps the async path correct while making
		// any accidental sync commit fold normally.
		s.capturedFold = &CapturedCommit{batch: touchBatch, repair: s.commitmentRepair()}
		s.deferFold = false
		s.resetJournal()
		s.releaseUnusedLoadedAccountProtos()
		s.rotateStateObjectWorkingSet()
		s.kvEntryArena.reset()
		mark(&stats.AccountTrieCommit)
		s.flushStorageObservability()
		return tcommon.Hash{}, stats, nil
	}
	var touchUpdates []rawdb.StateCommitmentUpdate
	if touchBatch != nil {
		touchUpdates = touchBatch.updates
	}
	root, err := s.applyLatestDomainCommitment(touchUpdates)
	touchBatch.release()
	if err != nil {
		if scope != nil {
			scope.discard()
		}
		return tcommon.Hash{}, stats, err
	}
	s.originRoot = ethcommon.Hash(root)
	s.resetJournal()
	s.releaseUnusedLoadedAccountProtos()
	s.rotateStateObjectWorkingSet()
	s.kvEntryArena.reset()
	mark(&stats.AccountTrieCommit)
	s.flushStorageObservability()

	return root, stats, nil
}

// CapturedCommit holds the self-contained inputs to a deferred commitment fold:
// the latest-domain updates produced by a block's commit (packed owned keys and
// immutable borrowed values) and the cold-history repair inputs. It is severable from the
// StateDB — FoldLatestCommitment(index, c.updates, c.repair) reproduces the
// exact root the synchronous path would have computed — so the async commit
// worker can fold it against a buffer LayerView while the foreground proceeds.
type CapturedCommit struct {
	batch  *commitmentUpdateBatch
	repair statedomains.CommitmentSnapshotRepair
}

type OrderedCommitmentPipeline = statedomains.OrderedCommitmentPipeline
type OrderedCommitmentResult = statedomains.OrderedCommitmentResult

func NewOrderedCommitmentPipeline(index statedomains.CommitmentDB) (*OrderedCommitmentPipeline, error) {
	return statedomains.NewOrderedCommitmentPipeline(index)
}

func NewOrderedCommitmentPipelineWithRepair(index statedomains.CommitmentDB, repair statedomains.CommitmentSnapshotRepair) (*OrderedCommitmentPipeline, error) {
	return statedomains.NewOrderedCommitmentPipelineWithRepair(index, repair)
}

// SubmitOrdered hands this captured block to the persistent 16-lane ordered
// commitment scheduler. The CapturedCommit retains its update backing until the
// caller receives the result and calls Release (the core commit worker does so
// as part of the ordered publish tail).
func (c *CapturedCommit) SubmitOrdered(pipeline *OrderedCommitmentPipeline, index statedomains.CommitmentDB) <-chan OrderedCommitmentResult {
	if c == nil || c.batch == nil {
		result := make(chan OrderedCommitmentResult, 1)
		result <- OrderedCommitmentResult{Err: errors.New("state: captured commitment already consumed")}
		close(result)
		return result
	}
	return pipeline.Submit(index, c.batch.updates)
}

// Repair returns the immutable snapshot binding captured with this block. The
// commit worker uses it when opening or reseeding a persistent ordered pipeline.
func (c *CapturedCommit) Repair() statedomains.CommitmentSnapshotRepair {
	if c == nil {
		return statedomains.CommitmentSnapshotRepair{}
	}
	return c.repair
}

// Fold runs the captured commitment fold against index and returns the root.
// A CapturedCommit is single-use; Fold releases its transient update/key
// storage on both success and failure.
func (c *CapturedCommit) Fold(index statedomains.CommitmentDB) (tcommon.Hash, error) {
	if c == nil || c.batch == nil {
		return tcommon.Hash{}, errors.New("state: captured commitment already consumed")
	}
	defer c.Release()
	store, err := statedomains.NewRecoveringStagedCommitmentStoreWithRepair(index, c.repair, true)
	if err != nil {
		return tcommon.Hash{}, err
	}
	root, applyErr := statedomains.ApplyLatestCommitmentWithStoreAndRepair(store, c.batch.updates, c.repair)
	closeErr := statedomains.CloseLatestCommitmentStore(store)
	if applyErr != nil {
		return tcommon.Hash{}, applyErr
	}
	if closeErr != nil {
		return tcommon.Hash{}, closeErr
	}
	return tcommon.Hash(root), nil
}

// Release drops a captured fold without running it. It is idempotent so the
// worker can defer it across early-failure paths while Fold also releases its
// storage immediately after consumption.
func (c *CapturedCommit) Release() {
	if c == nil || c.batch == nil {
		return
	}
	c.batch.release()
	c.batch = nil
}

// SetDeferFold toggles deferred-fold mode (async commit). When set, the next
// commit captures its fold inputs instead of folding inline; the caller must
// consume them with TakeCapturedFold and run the fold itself (the commit
// worker). The default (false) is the synchronous, byte-identical path.
func (s *StateDB) SetDeferFold(defer_ bool) { s.deferFold = defer_ }

// TakeCapturedFold returns and clears the fold inputs captured by the most
// recent deferred commit, or nil if none. Must be called synchronously by the
// committing goroutine before the next block reuses this StateDB.
func (s *StateDB) TakeCapturedFold() *CapturedCommit {
	c := s.capturedFold
	s.capturedFold = nil
	return c
}

func (s *StateDB) applyLatestDomainCommitment(updates []rawdb.StateCommitmentUpdate) (tcommon.Hash, error) {
	return FoldLatestCommitment(s.accountKVIndex(), updates, s.commitmentRepair())
}

// commitmentRepair captures the cold-history snapshot-repair inputs for the
// commitment fold. They are stable for the lifetime of the StateDB (set once at
// open via SetCommitmentColdHistory and never mutated per block), so they can be
// captured at the async-commit handoff and consumed by the commit worker
// without racing the foreground's continued use of the StateDB.
func (s *StateDB) commitmentRepair() statedomains.CommitmentSnapshotRepair {
	return statedomains.CommitmentSnapshotRepair{
		Source: s.commitmentColdHistory,
		TxNum:  s.commitmentColdTxNum,
	}
}

// FoldLatestCommitment runs the latest-domain commitment fold against an
// explicit commitment-branch index and returns the new root. It is the
// severable unit the async commit worker runs on its own goroutine: it touches
// NO StateDB state — only the supplied index, the captured updates, and the
// captured repair inputs — so the foreground may continue mutating the (reused)
// StateDB for the next block while this runs.
//
// The synchronous path calls it inline with s.accountKVIndex() (= bc.buffer's
// single active layer); the async path calls it with a buffer LayerView bound
// to the committing block's in-flight layer, so the fold writes that block's
// commitment-branch rows while the foreground writes the next block's layer.
func FoldLatestCommitment(index statedomains.CommitmentDB, updates []rawdb.StateCommitmentUpdate, repair statedomains.CommitmentSnapshotRepair) (tcommon.Hash, error) {
	store, err := statedomains.NewRecoveringStagedCommitmentStoreWithRepair(index, repair, false)
	if err != nil {
		return tcommon.Hash{}, err
	}
	root, applyErr := statedomains.ApplyLatestCommitmentWithStoreAndRepair(store, updates, repair)
	closeErr := statedomains.CloseLatestCommitmentStore(store)
	if applyErr != nil {
		return tcommon.Hash{}, applyErr
	}
	if closeErr != nil {
		return tcommon.Hash{}, closeErr
	}
	return tcommon.Hash(root), nil
}

// SetAccountName sets the account name.
func (s *StateDB) SetAccountName(addr tcommon.Address, name string) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAccountName(name)
	obj.markDirty()
}

// GetAccountName returns the account name.
func (s *StateDB) GetAccountName(addr tcommon.Address) string {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ""
	}
	return obj.account.AccountName()
}

// SetAccountId sets the account ID.
func (s *StateDB) SetAccountId(addr tcommon.Address, id string) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAccountId(id)
	obj.markDirty()
}

// GetAccountId returns the account ID.
func (s *StateDB) GetAccountId(addr tcommon.Address) string {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ""
	}
	return obj.account.AccountId()
}

// SetPermissions sets all permissions on the account.
func (s *StateDB) SetPermissions(addr tcommon.Address, owner, witness *corepb.Permission, actives []*corepb.Permission) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.writeAccountPermissions(obj, owner, witness, actives)
}

// ApplyDefaultAccountPermissions installs the default Owner permission and a
// default Active[0] permission whose operations bitmap is loaded from
// dp.ActiveDefaultOperations(). Mirrors java-tron AccountCapsule's
// `withDefaultPermission=true` constructor branch (createDefaultOwnerPermission
// + createDefaultActivePermission). The caller is responsible for the
// AllowMultiSign gate. No-op if the account does not exist.
//
// Note: this OVERWRITES any existing Owner / Active permissions; intended use
// is immediately after StateDB.CreateAccount on a freshly-minted account.
func (s *StateDB) ApplyDefaultAccountPermissions(addr tcommon.Address, dp *DynamicProperties) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return
	}
	owner := types.MakeDefaultOwnerPermission(addr)
	active := types.MakeDefaultActivePermission(addr, dp.ActiveDefaultOperations())
	_ = s.writeAccountPermissions(obj, owner, nil, []*corepb.Permission{active})
}

// ApplyWitnessPermissions installs the witness permission on addr and
// back-fills default Owner / Active[0] only if they are missing. Mirrors
// java-tron AccountCapsule.setDefaultWitnessPermission. The caller is
// responsible for the AllowMultiSign gate. No-op if the account does not
// exist.
//
// Conditional semantics (java-tron parity):
//   - Witness permission is ALWAYS set/overwritten (default shape).
//   - Owner permission is only set if account.OwnerPermission() == nil.
//   - Active[0] is only appended if len(account.ActivePermission()) == 0.
//
// This preserves any custom Owner/Active permissions the account installed
// via AccountPermissionUpdate before being upgraded to a witness.
func (s *StateDB) ApplyWitnessPermissions(addr tcommon.Address, dp *DynamicProperties) {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return
	}
	if err := s.materializeAccountPermissions(obj); err != nil {
		return
	}
	owner := obj.account.OwnerPermission()
	actives := obj.account.ActivePermission()
	witness := types.MakeDefaultWitnessPermission(addr)
	// Owner: only fill if missing.
	if owner == nil {
		owner = types.MakeDefaultOwnerPermission(addr)
	}
	// Active: only fill if list is empty.
	if len(actives) == 0 {
		active := types.MakeDefaultActivePermission(addr, dp.ActiveDefaultOperations())
		actives = []*corepb.Permission{active}
	}
	_ = s.writeAccountPermissions(obj, owner, witness, actives)
}

// GetDelegatedFrozenV2 returns the delegated (outgoing) frozen balance for a resource type.
func (s *StateDB) GetDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		return obj.account.DelegatedFrozenV2BalanceForBandwidth()
	}
	if err := s.materializeAccountResource(obj); err != nil {
		return 0
	}
	return obj.account.DelegatedFrozenV2BalanceForEnergy()
}

// AddDelegatedFrozenV2 adds to the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) AddDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		s.journalAccount(addr, obj)
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(obj.account.DelegatedFrozenV2BalanceForBandwidth() + amount)
		obj.markDirty()
	} else {
		_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
			obj.account.SetDelegatedFrozenV2BalanceForEnergy(obj.account.DelegatedFrozenV2BalanceForEnergy() + amount)
		})
	}
}

// SubDelegatedFrozenV2 subtracts from the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) SubDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		s.journalAccount(addr, obj)
		v := obj.account.DelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(v)
		obj.markDirty()
	} else {
		_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
			v := obj.account.DelegatedFrozenV2BalanceForEnergy() - amount
			if v < 0 {
				v = 0
			}
			obj.account.SetDelegatedFrozenV2BalanceForEnergy(v)
		})
	}
}

// AddAcquiredDelegatedFrozenV2 adds to the acquired (incoming) delegated frozen balance.
func (s *StateDB) AddAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		s.journalAccount(addr, obj)
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() + amount)
		obj.markDirty()
	} else {
		_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
			obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() + amount)
		})
	}
}

// SubAcquiredDelegatedFrozenV2 subtracts from the acquired (incoming) delegated frozen balance.
func (s *StateDB) SubAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		s.journalAccount(addr, obj)
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(v)
		obj.markDirty()
	} else {
		_ = s.mutateAccountResource(obj, func(_ *corepb.Account_AccountResource) {
			v := obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() - amount
			if v < 0 {
				v = 0
			}
			obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(v)
		})
	}
}

// ClearUnfrozenV2 removes all pending unfreeze entries.
func (s *StateDB) ClearUnfrozenV2(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	_ = s.clearAccountUnfrozenV2(obj)
}

// getStateObject returns the state object for addr, loading from the flat
// account latest domain.
func (s *StateDB) getStateObject(addr tcommon.Address) *stateObject {
	s.recordAccountRead(addr)
	return s.getStateObjectWithoutAccess(addr)
}

func (s *StateDB) getStateObjectForField(addr tcommon.Address, field TransactionAccountField) *stateObject {
	s.recordAccountFieldRead(addr, field)
	return s.getStateObjectWithoutAccess(addr)
}

func (s *StateDB) getStateObjectWithoutAccess(addr tcommon.Address) *stateObject {
	// Address byte 20 is uniformly distributed for normal TRON addresses. Check
	// it before the full 21-byte equality so alternating-account workloads pay
	// one byte comparison rather than comparing the common 0x41 prefix first.
	if obj := s.lastStateObject; obj != nil && obj.address[20] == addr[20] && obj.address == addr {
		s.touchRetainedStateObject(obj)
		return obj
	}
	if obj, ok := s.stateObjects[addr]; ok {
		if s.transactionVersionedReader != nil {
			if _, hydrated := s.transactionVersionedHydrated[addr]; !hydrated {
				obj = s.hydrateTransactionVersionedAccount(addr, obj)
				s.transactionVersionedHydrated[addr] = struct{}{}
				if obj == nil {
					delete(s.stateObjects, addr)
					s.transactionVersionedMissing[addr] = struct{}{}
					return nil
				}
				s.stateObjects[addr] = obj
			}
		}
		// Heal objects that entered the cache without a back-pointer (Load*
		// hydration, journal-revert re-creation) so their next markDirty records.
		if obj.dirtySet == nil {
			obj.dirtySet = s.dirtyObjects
		}
		s.lastStateObject = obj
		s.touchRetainedStateObject(obj)
		return obj
	}
	if s.transactionVersionedReader != nil {
		if _, missing := s.transactionVersionedMissing[addr]; missing {
			return nil
		}
	}
	data, ok, err := s.readStateAccountLatestForHydration(addr)
	var obj *stateObject
	if err != nil {
		s.setError(fmt.Errorf("read rooted account %s: %w", addr.Hex(), err))
	} else if ok {
		var envelope StateAccountV3
		if decodeErr := decodeStateAccountV3Into(data, &envelope); decodeErr != nil {
			s.setError(fmt.Errorf("decode rooted account envelope %s: %w", addr.Hex(), decodeErr))
		} else if acc, decodeErr := types.UnmarshalAccountStorageCoreV4(envelope.AccountProto); decodeErr != nil {
			s.setError(fmt.Errorf("decode rooted account core %s: %w", addr.Hex(), decodeErr))
		} else if acc.Address() != addr {
			s.setError(fmt.Errorf("rooted account address mismatch: key %s, value %s", addr.Hex(), acc.Address().Hex()))
		} else {
			stateObjectCacheHydrationCounter.Inc(1)
			obj = s.newStateObject(addr, acc)
			// DecodeStateAccountV2 owns AccountProto. Retain those exact durable bytes as
			// a potential journal pre-image instead of immediately re-marshaling the
			// account on its first mutation. A successful block commit releases this
			// copy if the account stayed read-only.
			obj.accountProto = envelope.AccountProto
			obj.accountProtoLoaded = true
			obj.accountKVRoot = envelope.AccountKVRoot
			obj.accountKVGeneration = envelope.AccountKVGeneration
			obj.accountKVGenerationDirty = false
			obj.codeHash = envelope.CodeHash
			obj.dirtySet = s.dirtyObjects
		}
	}
	if s.transactionVersionedReader != nil {
		obj = s.hydrateTransactionVersionedAccount(addr, obj)
		s.transactionVersionedHydrated[addr] = struct{}{}
	}
	if obj == nil {
		if s.transactionVersionedReader != nil {
			s.transactionVersionedMissing[addr] = struct{}{}
		}
		return nil
	}
	s.stateObjects[addr] = obj
	s.lastStateObject = obj
	s.loadedAccountProtoObjects = append(s.loadedAccountProtoObjects, obj)
	s.touchStateObject(obj)
	return obj
}

// touchStateObject records obj once in the current block's account working set.
// The common repeated-account path pays only the already-true branch.
func (s *StateDB) touchStateObject(obj *stateObject) {
	if obj == nil || obj.cacheTouched {
		return
	}
	obj.cacheTouched = true
	obj.cacheGeneration = s.stateObjectWorkingGeneration
	s.touchedStateObjects = append(s.touchedStateObjects, obj.address)
}

// touchRetainedStateObject records the first cross-block reuse before moving
// obj into the current generation. Calls after the first current-block touch
// take the existing cacheTouched fast path and do not update global metrics.
func (s *StateDB) touchRetainedStateObject(obj *stateObject) {
	if obj != nil && !obj.cacheTouched && s.stateObjectWorkingGeneration > obj.cacheGeneration {
		switch s.stateObjectWorkingGeneration - obj.cacheGeneration {
		case 1:
			stateObjectCachePreviousReuseCounter.Inc(1)
		case 2:
			stateObjectCacheOlderReuseCounter.Inc(1)
		case 3:
			stateObjectCacheOldestReuseCounter.Inc(1)
		case 4:
			stateObjectCacheAncientReuseCounter.Inc(1)
		}
	}
	s.touchStateObject(obj)
}

// rotateStateObjectWorkingSet evicts clean accounts whose most recent access
// was four successful blocks ago. All dirty objects have normally been finalized
// before this call. The defensive dirty branch retains an object if a synthetic
// caller violates that lifecycle rather than risking loss of uncommitted state.
func (s *StateDB) rotateStateObjectWorkingSet() {
	expired := s.ancientRetainedStateObjects
	ancient := s.oldestRetainedStateObjects
	oldest := s.olderRetainedStateObjects
	previous := s.retainedStateObjects
	current := s.touchedStateObjects
	expiredGeneration := uint64(0)
	if s.stateObjectWorkingGeneration >= 4 {
		expiredGeneration = s.stateObjectWorkingGeneration - 4
	}
	evictions := int64(0)
	for _, addr := range expired {
		obj := s.stateObjects[addr]
		if obj == nil || obj.cacheGeneration != expiredGeneration {
			continue
		}
		if obj.dirty {
			obj.cacheTouched = true
			obj.cacheGeneration = s.stateObjectWorkingGeneration
			current = append(current, addr)
			continue
		}
		clearAccountFrozenBandwidthCache(obj)
		s.settleOversizedStorageSample(addr)
		delete(s.stateObjects, addr)
		if s.lastStateObject == obj {
			s.lastStateObject = nil
		}
		s.releaseStateObject(obj)
		evictions++
	}
	for _, addr := range current {
		if obj := s.stateObjects[addr]; obj != nil {
			if !obj.dirty && len(obj.storage) > maxStateObjectCachedStorageSlots {
				s.recordOversizedStorageRelease(obj)
				obj.releaseStorage()
			}
			obj.cacheTouched = false
		}
	}
	// Give every sampled key a full four-block window, then settle remaining
	// keys. This also catches object replacements performed indirectly by journal
	// replay without adding work to the hot RevertToSnapshot path.
	s.expireOversizedStorageSamples()
	s.ancientRetainedStateObjects = ancient
	s.oldestRetainedStateObjects = oldest
	s.olderRetainedStateObjects = previous
	s.retainedStateObjects = current
	s.touchedStateObjects = expired[:0]
	s.stateObjectWorkingGeneration++
	stateObjectCacheEvictionCounter.Inc(evictions)
}

// releaseUnusedLoadedAccountProtos drops envelope bytes retained only to avoid
// a same-block pre-image marshal. Mutated accounts clear accountProtoLoaded as
// they journal/invalidate and retain their newly encoded post-image normally.
func (s *StateDB) releaseUnusedLoadedAccountProtos() {
	for _, obj := range s.loadedAccountProtoObjects {
		if obj != nil && obj.accountProtoLoaded {
			obj.accountProto = nil
			obj.accountProtoLoaded = false
		}
	}
	clear(s.loadedAccountProtoObjects)
	s.loadedAccountProtoObjects = s.loadedAccountProtoObjects[:0]
}

// journalAccount records the current state of an account for revert.
func (s *StateDB) journalAccount(addr tcommon.Address, obj *stateObject) {
	s.recordAccountWrite(addr)
	s.journalAccountPreimage(addr, obj)
}

// journalAccountPreimage performs the canonical undo/history work without
// classifying the mutation as a full-account access. Scalar setters use this
// path when temporal history requires accountChange bytes but their exact
// transaction field writes have already been recorded inline.
func (s *StateDB) journalAccountPreimage(addr tcommon.Address, obj *stateObject) {
	if obj != nil && obj.account != nil {
		obj.accountDirty = true
	}
	if boundary, ok := s.accountJournalBoundary(); ok {
		if pos, exists := s.accountJournalPos[addr]; exists && pos >= boundary && pos < s.journal.length() {
			if change, valid := s.journal.entries[pos].(accountChange); valid && change.address == addr {
				obj.invalidateAccountProto()
				return
			}
		}
	}
	var prev []byte
	var prevLatest []byte
	var prevProtoLoaded bool
	if obj != nil && obj.account != nil {
		prevProtoLoaded = obj.accountProtoLoaded
		var err error
		prev, err = obj.deterministicAccountProto()
		if err == nil && !obj.deleted && !obj.selfDestructed {
			latest, err := encodeAccountLatestObjectFromProto(obj, prev, true)
			if err == nil {
				prevLatest = latest
			}
		}
		obj.invalidateAccountProto()
	}
	pos := s.journal.length()
	s.journal.append(accountChange{
		address:          addr,
		prev:             prev,
		prevLatest:       prevLatest,
		prevProtoLoaded:  prevProtoLoaded,
		prevDeleted:      obj != nil && obj.deleted,
		prevCreated:      obj != nil && obj.created,
		prevSelfDestruct: obj != nil && obj.selfDestructed,
	})
	if len(s.snapshots) > 0 {
		if s.accountJournalPos == nil {
			s.accountJournalPos = make(map[tcommon.Address]int)
		}
		s.accountJournalPos[addr] = pos
	}
}

// journalAccountScalars records a structured pre-image for balance/resource
// fields when temporal history is disabled. History capture must retain the
// complete deterministic Account and flat-envelope bytes for every tx, so it
// intentionally falls back to journalAccount unchanged.
func (s *StateDB) journalAccountScalars(addr tcommon.Address, obj *stateObject) {
	if s.changeSet.enabled {
		s.journalAccountPreimage(addr, obj)
		return
	}
	if obj == nil || obj.account == nil {
		return
	}
	obj.accountDirty = true
	if boundary, ok := s.accountJournalBoundary(); ok {
		if pos, exists := s.accountJournalPos[addr]; exists && pos >= boundary && pos < s.journal.length() {
			switch change := s.journal.entries[pos].(type) {
			case accountChange:
				if change.address == addr {
					obj.invalidateAccountProto()
					return
				}
			case *accountScalarChange:
				if change.address == addr {
					obj.invalidateAccountProto()
					return
				}
			}
		}
	}
	pb := obj.account.Proto()
	change := acquireAccountScalarChange()
	change.address = addr
	change.prevProto = obj.accountProto
	change.prevProtoLoaded = obj.accountProtoLoaded
	change.balance = pb.Balance
	change.allowance = pb.Allowance
	change.latestWithdrawTime = pb.LatestWithdrawTime
	change.netUsage = pb.NetUsage
	change.latestOperationTime = pb.LatestOprationTime
	change.latestConsumeTime = pb.LatestConsumeTime
	change.freeNetUsage = pb.FreeNetUsage
	change.latestConsumeFreeTime = pb.LatestConsumeFreeTime
	change.netWindowSize = pb.NetWindowSize
	change.netWindowOptimized = pb.NetWindowOptimized
	pos := s.journal.length()
	s.journal.append(change)
	if len(s.snapshots) > 0 {
		if s.accountJournalPos == nil {
			s.accountJournalPos = make(map[tcommon.Address]int)
		}
		s.accountJournalPos[addr] = pos
	}
	obj.invalidateAccountProto()
}

// accountJournalBoundary returns the earliest journal position whose account
// pre-images still belong to the current rollback/history interval. Snapshot
// boundaries protect nested TVM calls. The temporal-history mark advances
// after each transaction is published, so a later mutation must append a new
// accountChange even if an older live snapshot still covers the address.
func (s *StateDB) accountJournalBoundary() (int, bool) {
	if len(s.snapshots) == 0 {
		return 0, false
	}
	boundary := s.snapshots[len(s.snapshots)-1]
	if s.changeSet.enabled && !s.changeSet.captureAtCommit && s.changeSet.journalMark > boundary {
		boundary = s.changeSet.journalMark
	}
	return boundary, true
}

func (s *StateDB) resetJournal() {
	s.journal.reset()
	s.snapshots = s.snapshots[:0]
	s.balanceTraceSnapshots = s.balanceTraceSnapshots[:0]
	clear(s.accountJournalPos)
}

// journalWitness records the current witness state for revert.
func (s *StateDB) journalWitness(addr tcommon.Address) {
	existing := s.witnesses[addr]
	var prev *types.Witness
	if existing != nil {
		prev = existing.Copy()
	}
	s.journal.append(witnessChange{
		address: addr,
		prev:    prev,
	})
}
