package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
)

// StateDB manages in-memory account state with Erigon-style flat latest-domain
// commits backed by a CommitmentDomain root.
type StateDB struct {
	db *Database

	stateObjects map[tcommon.Address]*stateObject
	witnesses    map[tcommon.Address]*types.Witness

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

	journal   *journal
	snapshots []int // journal length at each snapshot
	// domainChangeNoJournal mirrors block-final writes that intentionally
	// bypass the snapshot/revert journal but still need temporal change rows.
	domainChangeNoJournal []journalChange

	dynProps *DynamicProperties

	// originRoot is the CommitmentDomain root at the time of the last successful
	// Commit (or the root passed to New).
	originRoot ethcommon.Hash

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
	scope.latestWriter = newAccountKVLatestDomainBatch(index, resolveGeneration, &s.changeSet, nil)
	scope.latestReader = &commitScopeLatestReader{writer: scope.latestWriter, state: s}
	scope.tx = statedomains.NewSharedDomainTx(statedomains.SharedDomainTxConfig{
		Latest:     scope.latestReader,
		Writer:     scope.latestWriter,
		History:    scope.history,
		Commitment: scope.commitment,
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

func (scope *CommitScope) FlushLatestUpTo(cutoff uint64, numberOf func(tcommon.Hash) (uint64, bool)) error {
	if scope == nil || scope.latestWriter == nil {
		return nil
	}
	return scope.latestWriter.flushUpTo(cutoff, numberOf)
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
	AccountKVRoot       tcommon.Hash
	AccountKVGeneration uint64
	CodeHash            tcommon.Hash
}

// New creates a flat-domain StateDB from the given CommitmentDomain root.
func New(root tcommon.Hash, db *Database) (*StateDB, error) {
	return &StateDB{
		db:             db,
		stateObjects:   make(map[tcommon.Address]*stateObject),
		witnesses:      make(map[tcommon.Address]*types.Witness),
		dirtyWitnesses: make(map[tcommon.Address]struct{}),
		journal:        newJournal(),
		dynProps:       NewDynamicProperties(),
		originRoot:     ethcommon.Hash(root),
		codeStore:      newDefaultStateCodeStore(db),
	}, nil
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
	if _, ok := s.stateObjects[addr]; ok {
		return
	}
	if obj := s.getStateObject(addr); obj != nil {
		obj.account = acc.Copy()
		return
	}
	s.stateObjects[addr] = newStateObject(addr, acc.Copy())
}

// LoadAccountReference hydrates an account into the in-memory object cache
// without copying it. This is only for per-block hot-path caches that are
// cleared if block processing fails before commit.
func (s *StateDB) LoadAccountReference(acc *types.Account) {
	if acc == nil {
		return
	}
	addr := acc.Address()
	if _, ok := s.stateObjects[addr]; ok {
		return
	}
	if obj := s.getStateObject(addr); obj != nil {
		obj.account = acc
		return
	}
	s.stateObjects[addr] = newStateObject(addr, acc)
}

// LoadAccountSnapshotReference hydrates an account envelope into the in-memory
// object cache without copying the account. It is for hot-path block caches
// that are discarded if block processing fails before commit.
func (s *StateDB) LoadAccountSnapshotReference(snapshot *AccountSnapshot) {
	if snapshot == nil || snapshot.Account == nil {
		return
	}
	addr := snapshot.Account.Address()
	if _, ok := s.stateObjects[addr]; ok {
		return
	}
	obj := newStateObject(addr, snapshot.Account)
	obj.accountKVRoot = snapshot.AccountKVRoot
	obj.accountKVGeneration = snapshot.AccountKVGeneration
	obj.accountKVGenerationDirty = false
	obj.codeHash = snapshot.CodeHash
	s.stateObjects[addr] = obj
}

// CopyAccount returns a detached copy of the cached/live account.
func (s *StateDB) CopyAccount(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account.Copy()
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

// AccountSnapshotReference returns a cacheable view of the account envelope
// without copying the account proto. Mutating the returned Account mutates the
// cached state object, so callers must only keep this across successful block
// boundaries where the cache is updated immediately after Commit.
func (s *StateDB) AccountSnapshotReference(addr tcommon.Address) *AccountSnapshot {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return &AccountSnapshot{
		Account:             obj.account,
		AccountKVRoot:       obj.accountKVRoot,
		AccountKVGeneration: obj.accountKVGeneration,
		CodeHash:            obj.codeHash,
	}
}

// GetOrCreateAccount returns the state object at addr, creating it if it doesn't exist.
// When a new account is created, a nil-prev journal entry is recorded so that
// snapshot revert can delete it.
func (s *StateDB) GetOrCreateAccount(addr tcommon.Address) *stateObject {
	obj := s.getStateObject(addr)
	if obj != nil && !obj.deleted {
		return obj
	}
	// Journal the pre-create shape so revert can restore either a truly
	// missing account or a pending-delete object from an earlier tx in the
	// same block.
	s.journalAccount(addr, obj)
	nextGeneration := s.nextAccountKVGeneration(addr, obj)
	obj = newEmptyStateObject(addr)
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
	s.stateObjects[addr] = obj
	return obj
}

// GetBalance returns the TRX balance of the account.
func (s *StateDB) GetBalance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Balance()
}

// AddBalance adds amount to the account's balance.
func (s *StateDB) AddBalance(addr tcommon.Address, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() + amount)
	obj.markDirty()
}

// SubBalance subtracts amount from the account's balance.
func (s *StateDB) SubBalance(addr tcommon.Address, amount int64) error {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ErrInsufficientBalance
	}
	if obj.account.Balance() < amount {
		return ErrInsufficientBalance
	}
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() - amount)
	obj.markDirty()
	return nil
}

// GetTRC10Balance returns the TRC10 token balance of addr for the given tokenID.
// Balances are stored in the account proto's AssetV2 map (persisted through state commits).
func (s *StateDB) GetTRC10Balance(addr tcommon.Address, tokenID int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Proto().GetAssetV2()[strconv.FormatInt(tokenID, 10)]
}

// GetTRC10BalanceByName returns the legacy pre-AllowSameTokenName TRC10
// balance stored in Account.asset keyed by token name.
func (s *StateDB) GetTRC10BalanceByName(addr tcommon.Address, name []byte) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Proto().GetAsset()[string(name)]
}

// SetTRC10Balance sets the TRC10 token balance in the account proto's AssetV2 map.
func (s *StateDB) SetTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	if pb.AssetV2 == nil {
		pb.AssetV2 = make(map[string]int64)
	}
	pb.AssetV2[strconv.FormatInt(tokenID, 10)] = amount
	obj.markDirty()
}

// SetTRC10BalanceByName sets the legacy Account.asset balance keyed by token name.
func (s *StateDB) SetTRC10BalanceByName(addr tcommon.Address, name []byte, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	if pb.Asset == nil {
		pb.Asset = make(map[string]int64)
	}
	pb.Asset[string(name)] = amount
	obj.markDirty()
}

// SetTRC10BalanceLegacyAndV2 mirrors java-tron AccountCapsule.addAssetAmountV2
// before AllowSameTokenName: the legacy Account.asset value is authoritative,
// and Account.assetV2 is kept in lockstep under the token ID.
func (s *StateDB) SetTRC10BalanceLegacyAndV2(addr tcommon.Address, name []byte, tokenID int64, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	if pb.Asset == nil {
		pb.Asset = make(map[string]int64)
	}
	if pb.AssetV2 == nil {
		pb.AssetV2 = make(map[string]int64)
	}
	pb.Asset[string(name)] = amount
	pb.AssetV2[strconv.FormatInt(tokenID, 10)] = amount
	obj.markDirty()
}

func (s *StateDB) GetTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, allowSameTokenName bool) int64 {
	if allowSameTokenName {
		return s.GetTRC10Balance(addr, tokenID)
	}
	return s.GetTRC10BalanceByName(addr, name)
}

func (s *StateDB) AddTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) {
	if allowSameTokenName {
		s.AddTRC10Balance(addr, tokenID, amount)
		return
	}
	current := s.GetTRC10BalanceByName(addr, name)
	s.SetTRC10BalanceLegacyAndV2(addr, name, tokenID, current+amount)
}

func (s *StateDB) SubTRC10BalanceFinal(addr tcommon.Address, name []byte, tokenID int64, amount int64, allowSameTokenName bool) error {
	if allowSameTokenName {
		return s.SubTRC10Balance(addr, tokenID, amount)
	}
	current := s.GetTRC10BalanceByName(addr, name)
	if current < amount {
		return ErrInsufficientBalance
	}
	s.SetTRC10BalanceLegacyAndV2(addr, name, tokenID, current-amount)
	return nil
}

// SetAssetIssued records the issued TRC10 token's name and ID on the issuer
// account, mirroring java-tron's AssetIssueActuator (accountCapsule
// .setAssetIssuedName / .setAssetIssuedID). These fields are part of the
// persisted account proto, so they must live in state — not be derived at
// read time — or the conformance digest diverges at the issuance block.
func (s *StateDB) SetAssetIssued(addr tcommon.Address, name []byte, id string) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.AssetIssuedName = name
	pb.AssetIssued_ID = []byte(id)
	obj.markDirty()
}

// AddFrozenSupply appends frozen-supply entries to the account proto's
// frozen_supply field. java-tron's AssetIssueActuator writes these onto the
// issuer account when a TRC10 token is issued with a FrozenSupply list.
func (s *StateDB) AddFrozenSupply(addr tcommon.Address, frozen []*corepb.Account_Frozen) {
	if len(frozen) == 0 {
		return
	}
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.FrozenSupply = append(pb.FrozenSupply, frozen...)
	obj.markDirty()
}

func (s *StateDB) RemoveExpiredFrozenSupply(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	pb := obj.account.Proto()
	if len(pb.FrozenSupply) == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	var remaining []*corepb.Account_Frozen
	var amount int64
	for _, frozen := range pb.FrozenSupply {
		if frozen.ExpireTime <= now {
			amount += frozen.FrozenBalance
			continue
		}
		remaining = append(remaining, frozen)
	}
	pb.FrozenSupply = remaining
	obj.markDirty()
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
	fromPB := fromObj.account.Proto()
	if len(fromPB.AssetV2) == 0 {
		return
	}
	toObj := s.GetOrCreateAccount(to)
	s.journalAccount(from, fromObj)
	s.journalAccount(to, toObj)
	toPB := toObj.account.Proto()
	if toPB.AssetV2 == nil {
		toPB.AssetV2 = make(map[string]int64)
	}
	for tokenID, amount := range fromPB.AssetV2 {
		toPB.AssetV2[tokenID] += amount
		fromPB.AssetV2[tokenID] = 0
	}
	fromObj.markDirty()
	toObj.markDirty()
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
	s.journalAccount(addr, obj)
	obj.account.AddFreezeV2(resourceType, amount)
	obj.markDirty()
}

// --- V1 Stake (Stake 1.0) StateDB methods ---

func (s *StateDB) FreezeV1Bandwidth(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFrozenBandwidth(obj.account.TotalFrozenBandwidth()+amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) UnfreezeV1Bandwidth(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	s.journalAccount(addr, obj)
	refunded := obj.account.RemoveExpiredFrozenBandwidth(blockTimeMs)
	obj.markDirty()
	return refunded
}

func (s *StateDB) FreezeV1Energy(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFrozenEnergy(amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) FreezeV1TronPower(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddV1TronPower(amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) UnfreezeV1TronPower(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if obj.account.V1TronPowerExpireTime() > blockTimeMs {
		return 0
	}
	amount := obj.account.V1TronPowerFrozen()
	if amount == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	obj.account.ClearV1TronPower()
	obj.markDirty()
	return amount
}

func (s *StateDB) UnfreezeV1Energy(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if obj.account.FrozenEnergyExpireTime() > blockTimeMs {
		return 0
	}
	amount := obj.account.FrozenEnergyAmount()
	if amount == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	obj.account.ClearFrozenEnergy()
	obj.markDirty()
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
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() + amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	recvObj.account.SetAcquiredDelegatedFrozenEnergy(recvObj.account.AcquiredDelegatedFrozenEnergy() + amount)
	recvObj.markDirty()
}

func (s *StateDB) UnfreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() - amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	v := recvObj.account.AcquiredDelegatedFrozenEnergy() - amount
	if v < 0 {
		v = 0
	}
	recvObj.account.SetAcquiredDelegatedFrozenEnergy(v)
	recvObj.markDirty()
}

// GetStateObject returns the account for addr (nil if not found). Used by tests and later tasks.
func (s *StateDB) GetStateObject(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account
}

// GetWitness returns the witness at addr.
func (s *StateDB) GetWitness(addr tcommon.Address) *types.Witness {
	if w := s.witnesses[addr]; w != nil {
		return w
	}
	w, err := s.readWitnessCapsule(addr)
	if err != nil || w == nil {
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
	if id >= len(s.snapshots) {
		return
	}
	journalLen := s.snapshots[id]
	s.journal.revert(s.stateObjects, s.witnesses, journalLen)
	s.snapshots = s.snapshots[:id]
}

// FinalizeTransaction mirrors java-tron's rootRepository.commit() boundary for
// storage-row existence. java-tron keeps a zero StorageRow visible inside the
// executing transaction, then commit() deletes it before the next transaction.
// StateDB commits only once per block, so keep the zero value cached for the
// eventual disk delete but make later SSTORE cost checks see the row as absent.
func (s *StateDB) FinalizeTransaction() {
	for _, obj := range s.stateObjects {
		for k, v := range obj.storage {
			if v == (tcommon.Hash{}) {
				obj.storageExists[k] = false
			}
		}
		if obj.selfDestructed && !obj.deleted {
			s.DeleteAccount(obj.address)
		}
	}
}

// AccountExists returns whether an account exists (non-nil and not deleted).
func (s *StateDB) AccountExists(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	return obj != nil && !obj.deleted
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
	obj := s.GetOrCreateAccount(addr)
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
	obj := s.GetOrCreateAccount(addr)
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
	s.journalAccount(addr, obj)
	pb := obj.account.Proto()
	pb.AcquiredDelegatedFrozenBalanceForBandwidth = 0
	pb.AcquiredDelegatedFrozenV2BalanceForBandwidth = 0
	if pb.AccountResource != nil {
		pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy = 0
		pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy = 0
	}
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
	return obj.account.GetFrozenV2Amount(resourceType)
}

// ReduceFreezeV2 reduces the frozen amount for a resource type.
func (s *StateDB) ReduceFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ReduceFreezeV2(resourceType, amount)
	obj.markDirty()
}

// AddUnfreezeV2 adds a pending unfreeze entry with expiration time.
func (s *StateDB) AddUnfreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount, expireTime int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddUnfreezeV2(resourceType, amount, expireTime)
	obj.markDirty()
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
		var maxExpire int64
		for _, f := range obj.account.FrozenBandwidthList() {
			if f.ExpireTime > maxExpire {
				maxExpire = f.ExpireTime
			}
		}
		return maxExpire
	case 1: // ENERGY
		return obj.account.FrozenEnergyExpireTime()
	}
	return 0
}

// CancelAllUnfreezeV2 moves all pending V2 unfreeze entries back to frozen
// and returns the total amount cancelled.
func (s *StateDB) CancelAllUnfreezeV2(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	entries := obj.account.UnfrozenV2()
	if len(entries) == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	var total int64
	for _, u := range entries {
		total += u.UnfreezeAmount
		obj.account.AddFreezeV2(u.Type, u.UnfreezeAmount)
	}
	obj.account.ClearUnfrozenV2()
	obj.markDirty()
	return total
}

// UnfreezeV2Count returns the number of pending unfreeze entries.
func (s *StateDB) UnfreezeV2Count(addr tcommon.Address) int {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return len(obj.account.UnfrozenV2())
}

// RemoveExpiredUnfreezeV2 removes expired entries and returns the total withdrawn.
func (s *StateDB) RemoveExpiredUnfreezeV2(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	// Check if any entries would expire before journaling.
	hasExpired := false
	for _, u := range obj.account.UnfrozenV2() {
		if u.UnfreezeExpireTime <= now {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return 0
	}
	s.journalAccount(addr, obj)
	amount := obj.account.RemoveExpiredUnfreezeV2(now)
	obj.markDirty()
	return amount
}

// TotalFrozenV2 returns the total frozen balance across all resource types.
func (s *StateDB) TotalFrozenV2(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.TotalFrozenV2()
}

// GetLegacyTronPower returns the pre-AllowNewResourceModel voting power in drops.
func (s *StateDB) GetLegacyTronPower(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LegacyTronPower()
}

// GetAllTronPower returns the AllowNewResourceModel voting power in drops.
func (s *StateDB) GetAllTronPower(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.AllTronPower()
}

// InitializeOldTronPowerIfNeeded snapshots LegacyTronPower into old_tron_power
// when the field is still uninitialized (== 0). No-op otherwise.
func (s *StateDB) InitializeOldTronPowerIfNeeded(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil || !obj.account.OldTronPowerIsNotInitialized() {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.InitializeOldTronPower()
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
	return obj.account.Votes()
}

// SetVotes sets the vote list on an account.
func (s *StateDB) SetVotes(addr tcommon.Address, votes []*corepb.Vote) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetVotes(votes)
	obj.markDirty()
}

// ClearVotes clears all votes on an account.
func (s *StateDB) ClearVotes(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ClearVotes()
	obj.markDirty()
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
}

// GetAllowance returns the witness reward allowance.
func (s *StateDB) GetAllowance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Allowance()
}

// SetAllowance sets the witness reward allowance.
func (s *StateDB) SetAllowance(addr tcommon.Address, allowance int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAllowance(allowance)
	obj.markDirty()
}

// AddAllowance adds amount to the witness reward allowance.
func (s *StateDB) AddAllowance(addr tcommon.Address, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
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
	obj.accountDirty = true
	obj.markDirty()
}

// GetLatestWithdrawTime returns the latest withdraw timestamp.
func (s *StateDB) GetLatestWithdrawTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestWithdrawTime()
}

// SetLatestWithdrawTime sets the latest withdraw timestamp.
func (s *StateDB) SetLatestWithdrawTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestWithdrawTime(t)
	obj.markDirty()
}

// GetNetUsage returns the net (bandwidth) usage for an account.
func (s *StateDB) GetNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.NetUsage()
}

// SetNetUsage sets the net (bandwidth) usage for an account.
func (s *StateDB) SetNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetNetUsage(usage)
	obj.markDirty()
}

// GetLatestOperationTime returns the latest account operation timestamp.
func (s *StateDB) GetLatestOperationTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestOperationTime()
}

// SetLatestOperationTime sets the latest account operation timestamp.
func (s *StateDB) SetLatestOperationTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestOperationTime(t)
	obj.markDirty()
}

// GetLatestConsumeTime returns the latest consume time for an account.
func (s *StateDB) GetLatestConsumeTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeTime()
}

// SetLatestConsumeTime sets the latest consume time for an account.
func (s *StateDB) SetLatestConsumeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeTime(t)
	obj.markDirty()
}

// GetFreeNetUsage returns the free net (bandwidth) usage for an account.
func (s *StateDB) GetFreeNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeNetUsage()
}

// SetFreeNetUsage sets the free net (bandwidth) usage for an account.
func (s *StateDB) SetFreeNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeNetUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeFreeTime returns the latest consume free time for an account.
func (s *StateDB) GetLatestConsumeFreeTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeFreeTime()
}

// SetLatestConsumeFreeTime sets the latest consume free time for an account.
func (s *StateDB) SetLatestConsumeFreeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeFreeTime(t)
	obj.markDirty()
}

func (s *StateDB) GetFreeAssetNetUsage(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeAssetNetUsage(key)
}

func (s *StateDB) SetFreeAssetNetUsage(addr tcommon.Address, key string, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeAssetNetUsage(key, usage)
	obj.markDirty()
}

func (s *StateDB) GetFreeAssetNetUsageV2(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeAssetNetUsageV2(key)
}

func (s *StateDB) SetFreeAssetNetUsageV2(addr tcommon.Address, key string, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeAssetNetUsageV2(key, usage)
	obj.markDirty()
}

func (s *StateDB) GetLatestAssetOperationTime(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestAssetOperationTime(key)
}

func (s *StateDB) SetLatestAssetOperationTime(addr tcommon.Address, key string, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestAssetOperationTime(key, t)
	obj.markDirty()
}

func (s *StateDB) GetLatestAssetOperationTimeV2(addr tcommon.Address, key string) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestAssetOperationTimeV2(key)
}

func (s *StateDB) SetLatestAssetOperationTimeV2(addr tcommon.Address, key string, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestAssetOperationTimeV2(key, t)
	obj.markDirty()
}

// GetEnergyUsage returns the energy usage for an account.
func (s *StateDB) GetEnergyUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.EnergyUsage()
}

// SetEnergyUsage sets the energy usage for an account.
func (s *StateDB) SetEnergyUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetEnergyUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeTimeForEnergy returns the latest energy consume time for an account.
func (s *StateDB) GetLatestConsumeTimeForEnergy(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeTimeForEnergy()
}

// SetLatestConsumeTimeForEnergy sets the latest energy consume time for an account.
func (s *StateDB) SetLatestConsumeTimeForEnergy(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeTimeForEnergy(t)
	obj.markDirty()
}

// SetEnergyWindow sets the per-account energy recovery window (raw field +
// optimized flag) for an account. Mirrors java-tron's
// setNewWindowSize / setNewWindowSizeV2 persistence.
func (s *StateDB) SetEnergyWindow(addr tcommon.Address, raw int64, optimized bool) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetEnergyWindow(raw, optimized)
	obj.markDirty()
}

// --- Contract support ---

var (
	contractMetaKVKey  = []byte("meta")
	contractABIKVKey   = []byte("abi")
	contractStateKVKey = []byte("state")
)

// GetCode returns the contract bytecode at addr.
func (s *StateDB) GetCode(addr tcommon.Address) []byte {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	if obj.code == nil && !obj.codeDirty && obj.codeHash != (tcommon.Hash{}) {
		if code := s.readStateCode(obj.codeHash); len(code) > 0 {
			obj.code = append([]byte(nil), code...)
		} else if s.codeColdHistory != nil {
			if code, ok, err := s.codeColdHistory.GetCodeAtOrBefore(obj.codeHash, s.codeColdTxNum); err == nil && ok && len(code) > 0 {
				obj.code = append([]byte(nil), code...)
			}
		}
	}
	return obj.code
}

// SetCode sets the contract bytecode at addr. Creates the account if needed.
func (s *StateDB) SetCode(addr tcommon.Address, code []byte) {
	obj := s.GetOrCreateAccount(addr)
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
	obj := s.getStateObject(addr)
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
	obj := s.getStateObject(addr)
	if obj == nil {
		return tcommon.Hash{}, false
	}
	if v, ok := obj.storage[key]; ok {
		return v, obj.storageExists[key]
	}
	if obj.created {
		return tcommon.Hash{}, false
	}
	// Load from persistent storage on cache miss.
	raw, ok, err := s.GetAccountKV(addr, kvdomains.ContractStorage, s.storageRowKey(addr, key).Bytes())
	if err != nil || !ok || len(raw) == 0 {
		return tcommon.Hash{}, false
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	if h == (tcommon.Hash{}) {
		return tcommon.Hash{}, false
	}
	obj.storage[key] = h
	obj.storageExists[key] = true
	return h, true
}

// SetState sets a storage value on a contract.
func (s *StateDB) SetState(addr tcommon.Address, key, value tcommon.Hash) {
	obj := s.GetOrCreateAccount(addr)
	prev, prevExists, _ := obj.getStorageWithExist(key)
	if _, cached := obj.storage[key]; !cached {
		prev, prevExists = s.GetStateWithExist(addr, key)
	}
	if prevExists && prev == value {
		return
	}
	_, prevDirty := obj.dirtyStorage[key]
	s.journal.append(storageChange{
		address:    addr,
		key:        key,
		prev:       prev,
		prevExists: prevExists,
		prevDirty:  prevDirty,
	})
	obj.setStorage(key, value, true)
}

func (s *StateDB) storageRowKey(addr tcommon.Address, key tcommon.Hash) tcommon.Hash {
	return javaStorageRowKey(addr, key, s.GetContract(addr))
}

// GetContract returns the contract metadata at addr.
func (s *StateDB) GetContract(addr tcommon.Address) *contractpb.SmartContract {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	if obj.contractMeta == nil && !obj.contractMetaDirty {
		data, ok, err := s.GetAccountKV(addr, kvdomains.ContractMetadata, contractMetaKVKey)
		if err == nil && ok && len(data) > 0 {
			var sc contractpb.SmartContract
			if err := proto.Unmarshal(data, &sc); err == nil {
				obj.contractMeta = &sc
			}
		}
	}
	return obj.contractMeta
}

// SetContract stores contract metadata at addr.
func (s *StateDB) SetContract(addr tcommon.Address, contract *contractpb.SmartContract) {
	obj := s.GetOrCreateAccount(addr)
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
	obj.markDirty()
}

// ReadContractState loads the per-contract dynamic-energy runtime state.
func (s *StateDB) ReadContractState(addr tcommon.Address) *types.ContractState {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	data, ok, err := s.GetAccountKV(addr, kvdomains.ContractRuntimeState, contractStateKVKey)
	if err != nil || !ok || len(data) == 0 {
		return nil
	}
	cs, err := types.NewContractStateFromBytes(data)
	if err != nil {
		return nil
	}
	return cs
}

// WriteContractState stores the per-contract dynamic-energy runtime state.
func (s *StateDB) WriteContractState(addr tcommon.Address, cs *types.ContractState) error {
	if cs == nil {
		return nil
	}
	data, err := cs.Bytes()
	if err != nil {
		return err
	}
	return s.SetAccountKV(addr, kvdomains.ContractRuntimeState, contractStateKVKey, data)
}

// ReadContractABI loads the dedicated ABI store entry for a contract.
func (s *StateDB) ReadContractABI(addr tcommon.Address) *contractpb.SmartContract_ABI {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	data, ok, err := s.GetAccountKV(addr, kvdomains.ContractABI, contractABIKVKey)
	if err != nil || !ok {
		return nil
	}
	var abi contractpb.SmartContract_ABI
	if err := proto.Unmarshal(data, &abi); err != nil {
		return nil
	}
	return &abi
}

// WriteContractABI stores a dedicated ABI entry for a contract.
func (s *StateDB) WriteContractABI(addr tcommon.Address, abi *contractpb.SmartContract_ABI) error {
	if abi == nil {
		return s.DeleteAccountKV(addr, kvdomains.ContractABI, contractABIKVKey)
	}
	data, err := proto.Marshal(abi)
	if err != nil {
		return err
	}
	return s.SetAccountKV(addr, kvdomains.ContractABI, contractABIKVKey, data)
}

// IsContract returns whether the address has contract code or metadata.
func (s *StateDB) IsContract(addr tcommon.Address) bool {
	return s.GetContract(addr) != nil || len(s.GetCode(addr)) > 0
}

// Exist returns whether an account exists (non-nil and not deleted).
func (s *StateDB) Exist(addr tcommon.Address) bool {
	return s.AccountExists(addr)
}

// Empty returns whether an account is empty (no balance, no code).
func (s *StateDB) Empty(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return true
	}
	return obj.account.Balance() == 0 && len(s.GetCode(addr)) == 0
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
}

// DeleteAccount removes an account from flat account latest on commit.
func (s *StateDB) DeleteAccount(addr tcommon.Address) {
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
	obj := s.getStateObject(addr)
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
	cp := &StateDB{
		db:                    s.db,
		stateObjects:          make(map[tcommon.Address]*stateObject),
		witnesses:             make(map[tcommon.Address]*types.Witness),
		dirtyWitnesses:        make(map[tcommon.Address]struct{}),
		journal:               newJournal(),
		dynProps:              s.dynProps,
		originRoot:            s.originRoot,
		accountKVIndexStore:   s.accountKVIndexStore,
		codeColdHistory:       s.codeColdHistory,
		codeColdTxNum:         s.codeColdTxNum,
		commitmentColdHistory: s.commitmentColdHistory,
		commitmentColdTxNum:   s.commitmentColdTxNum,
	}
	for addr, obj := range s.stateObjects {
		var metaCopy *contractpb.SmartContract
		if obj.contractMeta != nil {
			metaCopy = proto.Clone(obj.contractMeta).(*contractpb.SmartContract)
		}
		kvDirtyCopy := make(map[string]kvEntry, len(obj.kvDirty))
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
		newObj := &stateObject{
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
			storage:                  make(map[tcommon.Hash]tcommon.Hash),
			storageExists:            make(map[tcommon.Hash]bool),
			dirtyStorage:             make(map[tcommon.Hash]struct{}, len(obj.dirtyStorage)),
			selfDestructed:           obj.selfDestructed,
			accountKVRoot:            obj.accountKVRoot,
			accountKVGeneration:      obj.accountKVGeneration,
			accountKVGenerationDirty: obj.accountKVGenerationDirty,
			kvDirty:                  kvDirtyCopy,
		}
		if obj.account != nil {
			data, _ := obj.account.Marshal()
			acc, _ := types.UnmarshalAccount(data)
			newObj.account = acc
		}
		for k, v := range obj.storage {
			newObj.storage[k] = v
			newObj.storageExists[k] = obj.storageExists[k]
		}
		for k := range obj.dirtyStorage {
			newObj.dirtyStorage[k] = struct{}{}
		}
		cp.stateObjects[addr] = newObj
	}
	return cp, nil
}

type storageCommitOp struct {
	rowKey tcommon.Hash
	value  []byte
	delete bool
	staged bool
}

type accountCommitPlan struct {
	addr               tcommon.Address
	obj                *stateObject
	deleteAccount      bool
	storageOps         []storageCommitOp
	kvPlan             *accountKVCommitPlan
	hadKVDirty         bool
	accountLatestDirty bool
}

func (s *StateDB) dirtyAccountCommitPlans() ([]*accountCommitPlan, error) {
	addrs := make([]tcommon.Address, 0, len(s.stateObjects))
	for addr, obj := range s.stateObjects {
		if !obj.dirty {
			continue
		}
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i].Bytes(), addrs[j].Bytes()) < 0
	})

	plans := make([]*accountCommitPlan, 0, len(addrs))
	for _, addr := range addrs {
		plan, err := s.prepareAccountCommitPlan(addr, s.stateObjects[addr])
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (s *StateDB) prepareAccountCommitPlan(addr tcommon.Address, obj *stateObject) (*accountCommitPlan, error) {
	plan := &accountCommitPlan{
		addr:               addr,
		obj:                obj,
		deleteAccount:      obj.deleted || obj.selfDestructed,
		accountLatestDirty: obj.accountDirty || obj.created || obj.codeDirty || obj.accountKVGenerationDirty,
	}
	if plan.deleteAccount {
		return plan, nil
	}
	if obj.contractMetaDirty {
		if obj.contractMeta == nil {
			if _, err := s.stageAccountKVCommit(obj, kvdomains.ContractMetadata, contractMetaKVKey, nil, true); err != nil {
				return nil, err
			}
			if _, err := s.stageAccountKVCommit(obj, kvdomains.ContractABI, contractABIKVKey, nil, true); err != nil {
				return nil, err
			}
		} else {
			metaBytes, err := proto.Marshal(obj.contractMeta)
			if err != nil {
				return nil, fmt.Errorf("marshal contractMeta for %s: %w", addr.Hex(), err)
			}
			if _, err := s.stageAccountKVCommit(obj, kvdomains.ContractMetadata, contractMetaKVKey, metaBytes, false); err != nil {
				return nil, err
			}
		}
	}

	if len(obj.dirtyStorage) > 0 {
		storageKeys := make([]tcommon.Hash, 0, len(obj.dirtyStorage))
		for key := range obj.dirtyStorage {
			storageKeys = append(storageKeys, key)
		}
		sort.Slice(storageKeys, func(i, j int) bool {
			return bytes.Compare(storageKeys[i].Bytes(), storageKeys[j].Bytes()) < 0
		})
		plan.storageOps = make([]storageCommitOp, 0, len(storageKeys))
		for _, key := range storageKeys {
			value := obj.storage[key]
			rowKey := s.storageRowKey(addr, key)
			if value == (tcommon.Hash{}) {
				staged, err := s.stageAccountKVCommit(obj, kvdomains.ContractStorage, rowKey.Bytes(), nil, true)
				if err != nil {
					return nil, err
				}
				plan.storageOps = append(plan.storageOps, storageCommitOp{
					rowKey: rowKey,
					delete: true,
					staged: staged,
				})
				continue
			}
			staged, err := s.stageAccountKVCommit(obj, kvdomains.ContractStorage, rowKey.Bytes(), value.Bytes(), false)
			if err != nil {
				return nil, err
			}
			plan.storageOps = append(plan.storageOps, storageCommitOp{
				rowKey: rowKey,
				value:  append([]byte(nil), value.Bytes()...),
				staged: staged,
			})
		}
	}

	plan.hadKVDirty = len(obj.kvDirty) > 0
	if plan.hadKVDirty {
		kvPlan, err := s.prepareAccountKVCommitPlan(obj)
		if err != nil {
			return nil, err
		}
		plan.kvPlan = kvPlan
	}
	return plan, nil
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

func accountCommitPlansByAddress(plans []*accountCommitPlan) []*accountCommitPlan {
	out := append([]*accountCommitPlan(nil), plans...)
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].addr.Bytes(), out[j].addr.Bytes()) < 0
	})
	return out
}

func accountCommitPlanGenerationResolver(plans []*accountCommitPlan) statedomains.GenerationResolver {
	generations := make(map[tcommon.Address]uint64, len(plans))
	for _, plan := range plans {
		if plan == nil || plan.obj == nil {
			continue
		}
		generations[plan.addr] = plan.obj.accountKVGeneration
	}
	return func(owner tcommon.Address) (uint64, error) {
		generation, ok := generations[owner]
		if !ok {
			return 0, fmt.Errorf("account kv generation for %s not in commit plan", owner.Hex())
		}
		return generation, nil
	}
}

func encodeAccountLatestObject(obj *stateObject, flatRoot bool) ([]byte, bool, error) {
	if obj == nil || obj.deleted || obj.selfDestructed || obj.account == nil {
		return nil, false, nil
	}
	accBytes, err := obj.account.Marshal()
	if err != nil {
		return nil, false, err
	}
	accountKVRoot := obj.accountKVRoot
	if flatRoot {
		accountKVRoot = EmptyKVRoot
	}
	envelope := &StateAccountV2{
		Version:             StateAccountVersion,
		AccountProto:        accBytes,
		AccountKVRoot:       accountKVRoot,
		AccountKVGeneration: obj.accountKVGeneration,
		CodeHash:            obj.codeHash,
	}
	data, err := envelope.Encode()
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (s *StateDB) writeFlatAccountLatestWithPlan(plan *accountCommitPlan, flatRoot bool, commitment *DomainCommitmentState, latestWriter *accountKVLatestBatch) error {
	if plan == nil || plan.obj == nil {
		return nil
	}
	obj := plan.obj
	addr := plan.addr
	if plan.deleteAccount {
		if err := s.writeAccountLatestChange(addr, false, nil); err != nil {
			return err
		}
		if latestWriter == nil {
			return fmt.Errorf("account latest writer unavailable")
		}
		if err := latestWriter.deleteAccountLatest(addr); err != nil {
			return err
		}
		commitment.recordAccountLatestTouch(addr)
		if err := s.writeAccountKVGeneration(obj, commitment, latestWriter); err != nil {
			return err
		}
		return nil
	}
	generationDirty := obj.accountKVGenerationDirty
	needsAccountLatestUpdate := plan.accountLatestDirty || flatRoot
	if generationDirty {
		if err := s.writeAccountKVGeneration(obj, commitment, latestWriter); err != nil {
			return err
		}
	}
	if !needsAccountLatestUpdate {
		return nil
	}
	data, exists, err := encodeAccountLatestObject(obj, flatRoot)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := s.writeAccountLatestChange(addr, exists, data); err != nil {
		return err
	}
	if latestWriter == nil {
		return fmt.Errorf("account latest writer unavailable")
	}
	if err := latestWriter.writeAccountLatest(addr, data); err != nil {
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
	var nextCopy []byte
	if nextExists {
		nextCopy = append([]byte(nil), next...)
	}
	return s.changeSet.publishCommitDomainChange(&rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      addr,
		PrevExists: prevExists,
		Prev:       prev,
		NextExists: nextExists,
		Next:       nextCopy,
	})
}

func (s *StateDB) writeFlatAccountLatestPlans(plans []*accountCommitPlan, flatRoot bool, commitment *DomainCommitmentState, latestWriter *accountKVLatestBatch) error {
	for _, plan := range accountCommitPlansByAddress(plans) {
		if err := s.writeFlatAccountLatestWithPlan(plan, flatRoot, commitment, latestWriter); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateDB) finalizeAccountCommitPlan(plan *accountCommitPlan) {
	obj := plan.obj
	if plan.deleteAccount {
		obj.deleted = true
		obj.selfDestructed = false
		obj.code = nil
		obj.codeHash = tcommon.Hash{}
		obj.codeDirty = false
		obj.accountDirty = false
		obj.contractMeta = nil
		obj.contractMetaDirty = false
		obj.storage = make(map[tcommon.Hash]tcommon.Hash)
		obj.storageExists = make(map[tcommon.Hash]bool)
		obj.dirtyStorage = make(map[tcommon.Hash]struct{})
		obj.dirty = false
		return
	}
	if plan.hadKVDirty {
		obj.kvDirty = make(map[string]kvEntry)
	}
	if obj.codeDirty {
		obj.codeDirty = false
	}
	if obj.contractMetaDirty {
		obj.contractMetaDirty = false
	}
	obj.accountDirty = false
	obj.dirtyStorage = make(map[tcommon.Hash]struct{})
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
	stats.Accounts = len(plans)
	stats.Mutations = summarizeCommitMutations(plans)
	for _, plan := range plans {
		if plan.kvPlan == nil || len(plan.kvPlan.items) == 0 {
			continue
		}
		stats.KVAccounts++
		stats.KVItems += len(plan.kvPlan.items)
	}
	mark(&stats.Prepare)

	accountKVIndex := s.accountKVIndex()
	generationResolver := accountCommitPlanGenerationResolver(plans)
	commitmentState := NewDomainCommitmentStateWithGenerationResolver(s, generationResolver)
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
			Latest:     statedomains.NewFlatStoreWithGenerationResolver(accountKVIndex, 0, generationResolver),
			Writer:     accountKVLatestWriter,
			History:    NewDomainHistoryState(s, s.changeSet.endTxNum),
			Commitment: commitmentState,
		})
		tx.SetTxNum(s.changeSet.endTxNum)
		defer tx.Close()
		accountKVTemporalTx = tx
	}
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
	mark(&stats.Finalize)

	touchUpdates, err := commitmentState.latestUpdatesFromTouches()
	if err != nil {
		if scope != nil {
			scope.discard()
		}
		return tcommon.Hash{}, stats, err
	}
	root, err := s.applyLatestDomainCommitment(touchUpdates)
	if err != nil {
		if scope != nil {
			scope.discard()
		}
		return tcommon.Hash{}, stats, err
	}
	s.originRoot = ethcommon.Hash(root)
	s.journal = newJournal()
	s.snapshots = s.snapshots[:0]
	mark(&stats.AccountTrieCommit)

	return root, stats, nil
}

func (s *StateDB) applyLatestDomainCommitment(updates []rawdb.StateCommitmentUpdate) (tcommon.Hash, error) {
	index := s.accountKVIndex()
	repair := statedomains.CommitmentSnapshotRepair{
		Source: s.commitmentColdHistory,
		TxNum:  s.commitmentColdTxNum,
	}
	store := s.latestCommitmentStore(index)
	root, err := statedomains.ApplyLatestCommitmentWithStoreAndRepair(store, updates, repair)
	return tcommon.Hash(root), err
}

// latestCommitmentStore returns the commitment store for this StateDB's database.
// The Erigon-style staged store is the only engine; the legacy binary-radix store
// has been retired.
func (s *StateDB) latestCommitmentStore(index statedomains.CommitmentDB) statedomains.LatestCommitmentStore {
	return statedomains.NewStagedCommitmentStore(index)
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
	s.journalAccount(addr, obj)
	obj.account.SetOwnerPermission(owner)
	obj.account.SetWitnessPermission(witness)
	obj.account.SetActivePermission(actives)
	obj.markDirty()
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
	s.journalAccount(addr, obj)
	owner := types.MakeDefaultOwnerPermission(addr)
	active := types.MakeDefaultActivePermission(addr, dp.ActiveDefaultOperations())
	obj.account.SetOwnerPermission(owner)
	obj.account.SetActivePermission([]*corepb.Permission{active})
	obj.markDirty()
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
	s.journalAccount(addr, obj)
	// Witness: unconditional (overwrite if any).
	obj.account.SetWitnessPermission(types.MakeDefaultWitnessPermission(addr))
	// Owner: only fill if missing.
	if obj.account.OwnerPermission() == nil {
		obj.account.SetOwnerPermission(types.MakeDefaultOwnerPermission(addr))
	}
	// Active: only fill if list is empty.
	if len(obj.account.ActivePermission()) == 0 {
		active := types.MakeDefaultActivePermission(addr, dp.ActiveDefaultOperations())
		obj.account.SetActivePermission([]*corepb.Permission{active})
	}
	obj.markDirty()
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
	return obj.account.DelegatedFrozenV2BalanceForEnergy()
}

// AddDelegatedFrozenV2 adds to the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) AddDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(obj.account.DelegatedFrozenV2BalanceForBandwidth() + amount)
	} else {
		obj.account.SetDelegatedFrozenV2BalanceForEnergy(obj.account.DelegatedFrozenV2BalanceForEnergy() + amount)
	}
	obj.markDirty()
}

// SubDelegatedFrozenV2 subtracts from the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) SubDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		v := obj.account.DelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(v)
	} else {
		v := obj.account.DelegatedFrozenV2BalanceForEnergy() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForEnergy(v)
	}
	obj.markDirty()
}

// AddAcquiredDelegatedFrozenV2 adds to the acquired (incoming) delegated frozen balance.
func (s *StateDB) AddAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() + amount)
	} else {
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() + amount)
	}
	obj.markDirty()
}

// SubAcquiredDelegatedFrozenV2 subtracts from the acquired (incoming) delegated frozen balance.
func (s *StateDB) SubAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(v)
	} else {
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(v)
	}
	obj.markDirty()
}

// ClearUnfrozenV2 removes all pending unfreeze entries.
func (s *StateDB) ClearUnfrozenV2(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ClearUnfrozenV2()
	obj.markDirty()
}

// getStateObject returns the state object for addr, loading from the flat
// account latest domain.
func (s *StateDB) getStateObject(addr tcommon.Address) *stateObject {
	if obj, ok := s.stateObjects[addr]; ok {
		return obj
	}
	data, ok, err := s.readStateAccountLatest(addr)
	if err != nil || !ok {
		return nil
	}
	envelope, err := DecodeStateAccountV2(data)
	if err != nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(envelope.AccountProto)
	if err != nil {
		return nil
	}
	obj := newStateObject(addr, acc)
	obj.accountKVRoot = envelope.AccountKVRoot
	obj.accountKVGeneration = envelope.AccountKVGeneration
	obj.accountKVGenerationDirty = false
	obj.codeHash = envelope.CodeHash
	s.stateObjects[addr] = obj
	return obj
}

// journalAccount records the current state of an account for revert.
func (s *StateDB) journalAccount(addr tcommon.Address, obj *stateObject) {
	var prev []byte
	var prevLatest []byte
	if obj != nil && obj.account != nil {
		prev, _ = obj.account.Marshal()
		if latest, exists, err := encodeAccountLatestObject(obj, true); err == nil && exists {
			prevLatest = latest
		}
		obj.accountDirty = true
	}
	s.journal.append(accountChange{
		address:          addr,
		prev:             prev,
		prevLatest:       prevLatest,
		prevDeleted:      obj != nil && obj.deleted,
		prevCreated:      obj != nil && obj.created,
		prevSelfDestruct: obj != nil && obj.selfDestructed,
	})
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
