package state

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

var ErrStateDomainHistoryUnavailable = errors.New("flat state domain history unavailable")

func stateDomainHistoryConfig() (snapshots.DomainCfg, error) {
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		return snapshots.DomainCfg{}, ErrStateDomainHistoryUnavailable
	}
	return cfg, nil
}

// HistoryReader is the read-side API for archive-mode state history queries.
// It returns the state of an account / storage slot / contract code as it
// existed AT THE END of `blockNum` — i.e. after block blockNum's transactions
// were applied. This matches the eth_getBalance(addr, N) / java-tron
// getAccountAt(addr, N) JSON-RPC convention.
//
// blockNum == 0 returns the genesis state (no block has yet been applied
// after genesis from a "state at the end of block 0" perspective; for an
// archive node that started from genesis the live state at blockNum=0 IS
// the genesis state, and the inverse-index walk will roll back every block
// in (0, HEAD] that touched the key).
//
// blockNum >= currentHead short-circuits to a live read.
//
// Implementations:
//
//   - LiveStateHistoryReader (this file) — no-op fallback for nodes built
//     with HistoryEnabled=false. It IGNORES blockNum and returns the LIVE
//     state. Kept for internal callers that explicitly want best-effort live
//     reads; production archive RPCs return an explicit error when temporal
//     history is unavailable.
//
//   - PersistentHistoryReader — the archive-mode implementation. Rolls flat
//     latest-domain rows back with StateDomainChange history until the state
//     matches end-of-blockNum.
//
// All methods return (nil, nil) / (zero, nil) for "account/slot doesn't
// exist at that block" — they do NOT return an error for that case. A
// non-nil error means the underlying KV layer failed (DB unreachable,
// corrupted proto, etc.).
type HistoryReader interface {
	// AccountAt returns the Account proto as it stood at the end of blockNum.
	// Returns (nil, nil) if the account did not exist at that point.
	AccountAt(addr tcommon.Address, blockNum uint64) (*types.Account, error)

	// StorageAt returns the value of (addr, slot) at the end of blockNum.
	// Returns the zero hash with nil error for "slot is empty or account
	// doesn't exist at that point".
	StorageAt(addr tcommon.Address, slot tcommon.Hash, blockNum uint64) (tcommon.Hash, error)

	// CodeAt returns the contract bytecode at addr at the end of blockNum.
	// Returns nil with nil error for "account doesn't exist or had no code
	// at that point".
	CodeAt(addr tcommon.Address, blockNum uint64) ([]byte, error)
}

// LiveAccountReader is the narrow capability the history readers need to
// resolve the END-OF-HEAD account snapshot. StateDB satisfies this interface
// directly via GetAccount, resolving its in-memory overlay before falling back
// to flat account latest rows.
//
// Storage cells and contract code are available from StateDB's rooted contract
// domains. Contract code requires a live contract reader because the canonical
// address -> code-hash edge lives in the account envelope, not in the legacy
// flat CodeStore.
type LiveAccountReader interface {
	GetAccount(addr tcommon.Address) *types.Account
}

type LiveContractReader interface {
	GetCode(addr tcommon.Address) []byte
	GetState(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash
}

// LiveStateHistoryReader is the no-op fallback. It IGNORES blockNum and
// returns the live state for every query. Used by RPC handlers on nodes
// that explicitly choose best-effort current-state answers instead of strict
// archive semantics.
//
// Backed by a LiveAccountReader (for accounts) plus optional live contract
// reads. Without a live contract reader, CodeAt degrades to nil because the
// content-addressed code domain is keyed by hash, not by address.
type LiveStateHistoryReader struct {
	db   ethdb.KeyValueReader
	live LiveAccountReader
}

// NewLiveStateHistoryReader wraps a (disk KV reader, live-account reader)
// pair as a HistoryReader that returns live state for any block. `db` serves
// storage from flat latest account-KV when no live contract reader exists;
// `live` resolves accounts and contract code via the current StateDB view.
//
// `live` may be nil — in that case `AccountAt` returns nil (degraded
// "no account exists" semantics) but storage reads continue to function.
// Contract code returns nil without a live contract reader. This is the same
// nil tolerance as PersistentHistoryReader so
// the two readers share an interface contract.
//
// Parameter order is (db, live) to match NewPersistentHistoryReader and
// keep call sites consistent across both reader types.
func NewLiveStateHistoryReader(db ethdb.KeyValueReader, live LiveAccountReader) *LiveStateHistoryReader {
	return &LiveStateHistoryReader{db: db, live: live}
}

// AccountAt returns the live account at addr; blockNum is ignored.
func (r *LiveStateHistoryReader) AccountAt(addr tcommon.Address, _ uint64) (*types.Account, error) {
	if r.live == nil {
		return nil, nil
	}
	return r.live.GetAccount(addr), nil
}

// StorageAt returns the live storage slot value for (addr, slot);
// blockNum is ignored. Slot values are stored as raw bytes with leading
// zeros trimmed by the contract writer — we right-align into a Hash.
func (r *LiveStateHistoryReader) StorageAt(addr tcommon.Address, slot tcommon.Hash, _ uint64) (tcommon.Hash, error) {
	if live, ok := r.live.(LiveContractReader); ok {
		return live.GetState(addr, slot), nil
	}
	return readFlatStorageLatest(r.db, addr, slot)
}

// CodeAt returns the live contract bytecode at addr; blockNum is ignored.
func (r *LiveStateHistoryReader) CodeAt(addr tcommon.Address, _ uint64) ([]byte, error) {
	if live, ok := r.live.(LiveContractReader); ok {
		code := live.GetCode(addr)
		if len(code) == 0 {
			return nil, nil
		}
		return append([]byte(nil), code...), nil
	}
	return nil, nil
}

// Compile-time interface checks.
var (
	_ HistoryReader = (*LiveStateHistoryReader)(nil)
	_ HistoryReader = (*PersistentHistoryReader)(nil)
)

// PersistentHistoryReader reconstructs historical state from flat temporal
// StateDomainChange rows. It rolls physical latest-domain values back from the
// current head to the requested block using StateTxRange txNum windows.
//
// Per-request caching:
//
//	The reader is constructed per RPC handler invocation and memoises
//	reconstructed accounts, code, and slot values inside `cache`. Multi-key
//	reads at the same blockNum share work — important for endpoints like
//	getAccountAt that walk many addresses, and for debug_traceTransactionAt
//	which exercises the same blockNum repeatedly. The cache is dropped when
//	the reader goes out of scope; no expiry or LRU because the lifetime is
//	tied to a single request.
//
// Concurrency:
//
//	Not safe for concurrent use. Each RPC handler should construct its own
//	reader. The cache is a plain map; readers shouldn't be shared.
//
// Buffer awareness:
//
//	The reader takes an ethdb.KeyValueReader that should be the chain's
//	buffer-aware view (bc.buffer), NOT the bare disk store. Temporal rows for
//	recent (unflushed) blocks live only in the in-memory buffer layers until
//	they flush past the solidified line; the buffer's NewIterator merges those
//	layers over disk and masks tombstones, so a buffer-backed reader sees the
//	logically-complete row set and transparently falls through to disk for
//	flushed rows. The production archive-query path
//	(core.TronBackend.historyReaderAt) wires bc.buffer for exactly this reason.
//	Passing the bare disk store would miss recent history.
//
// The headNum parameter is the chain's current head as of reader
// construction. When blockNum >= headNum the reader short-circuits to a
// live read (no inverse-index scan needed because no future blocks have
// modified the key yet from this reader's perspective).
type PersistentHistoryReader struct {
	// db backs both the per-block delta Get() calls and the inverse-index
	// range scans. Tests inject ethrawdb.NewMemoryDatabase(); production
	// uses bc.db (the chain disk store).
	db readerDB

	// live resolves the end-of-HEAD account snapshot. Accounts live in the
	// flat account latest domain plus StateDB's in-memory overlay. live may be
	// nil — in that case the reader's live-account baseline is "no account
	// exists", and any AccountAt(addr, N) is answerable only from history rows.
	live LiveAccountReader

	headNum     uint64
	coldHistory StateDomainChangeColdHistory
	latest      hotStateLatestReader
	cache       map[reqCacheKey]any
}

// readerDB is the KV surface the reader needs: point Get/Has reads plus
// prefix iteration. The chain disk store and memorydb satisfy it; tests
// in this package use the latter.
type readerDB interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
}

type StateDomainChangeColdHistory interface {
	IterateStateDomainChanges(fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error
}

type StateDomainChangeColdTxRange interface {
	StateTxRangeForBlock(blockNum uint64) (*rawdb.StateTxRange, bool, error)
}

type StateDomainChangeColdKeyHistory interface {
	IterateStateDomainChangesByKey(fromTxNum, toTxNum uint64, flatDomain rawdb.StateFlatDomain, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error
}

type StateCodeColdHistory interface {
	GetCode(hash tcommon.Hash, txNum uint64) ([]byte, bool, error)
}

type StateCodeColdHistoryAtOrBefore interface {
	GetCodeAtOrBefore(hash tcommon.Hash, txNum uint64) ([]byte, bool, error)
}

// StateKVColdAsOf exposes an exact historical prefix view. It is deliberately
// distinct from snapshots.Manager.IterateKVLatestPrefix: a latest snapshot is
// a boundary seed and is not by itself an as-of view for every txNum in its
// file range.
type StateKVColdAsOf interface {
	IterateKVAsOfPrefix(domain kvdomains.KVDomain, owner tcommon.Address, generation uint64, logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error
}

// StateKVColdBoundary enumerates keys from an immutable latest snapshot. Its
// values are only boundary seeds; historical values are still reconstructed
// through StateDomainChange rows.
type StateKVColdBoundary interface {
	IterateKVLatestPrefix(domain kvdomains.KVDomain, owner tcommon.Address, generation uint64, logicalPrefix []byte, txNum uint64, fn func(logicalKey, value []byte) (bool, error)) error
}

// reqCacheKey identifies one (kind, addr [, slot], blockNum) cache entry.
type reqCacheKey struct {
	kind     uint8 // 0=account, 1=code, 2=storage
	addr     tcommon.Address
	slot     tcommon.Hash
	blockNum uint64
}

const (
	cacheKindAccount uint8 = 0
	cacheKindCode    uint8 = 1
	cacheKindStorage uint8 = 2
)

// accountCacheEntry holds both the unmarshalled account and its
// associated code so that AccountAt + CodeAt at the same (addr, blockNum)
// share the inverse-index walk. The code field may be nil even when the
// account is non-nil — e.g. an EOA with no code, or a contract whose code
// was deleted at end-of-blockNum.
type accountCacheEntry struct {
	account *types.Account // nil if the account did not exist at end-of-blockNum
	code    []byte         // nil if no code
}

// NewPersistentHistoryReader builds a reader keyed on the supplied disk
// store, live-account resolver, and current chain head. The reader is
// single-use; callers should instantiate one per RPC request so the cache
// doesn't leak across unrelated queries.
//
// `live` is the production state.StateDB or any LiveAccountReader; it
// resolves the end-of-HEAD account snapshot that the inverse-index walk
// rolls back from. Pass nil if no live account view is available (the
// reader will then treat "live" as "no account exists" and reconstruct
// purely from history rows — useful for tests that target the rollback
// path in isolation).
//
// `db` is the disk-side KV store: it serves per-block AccountDelta /
// SlotDelta rows, the inverse-index scan, and the live flat latest domains.
// The chain's *rawdb.Database satisfies the interface directly; tests use
// ethrawdb.NewMemoryDatabase().
func NewPersistentHistoryReader(db readerDB, live LiveAccountReader, headNum uint64) *PersistentHistoryReader {
	return NewPersistentHistoryReaderWithColdHistory(db, live, headNum, nil)
}

func NewPersistentHistoryReaderWithColdHistory(db readerDB, live LiveAccountReader, headNum uint64, coldHistory StateDomainChangeColdHistory) *PersistentHistoryReader {
	return &PersistentHistoryReader{
		db:          db,
		live:        live,
		headNum:     headNum,
		coldHistory: coldHistory,
		latest:      newRegistryHotStateLatestReader(db, snapshots.DefaultDomainRegistry()),
		cache:       make(map[reqCacheKey]any),
	}
}

// AccountAt returns the account at addr at the end of blockNum.
func (r *PersistentHistoryReader) AccountAt(addr tcommon.Address, blockNum uint64) (*types.Account, error) {
	entry, err := r.accountAndCode(addr, blockNum)
	if err != nil {
		return nil, err
	}
	return entry.account, nil
}

// CodeAt returns the contract bytecode at addr at the end of blockNum.
// Shares the inverse-index walk with AccountAt via accountAndCode.
func (r *PersistentHistoryReader) CodeAt(addr tcommon.Address, blockNum uint64) ([]byte, error) {
	// Code reads cache separately from account reads — a caller doing
	// CodeAt -> AccountAt -> CodeAt at the same blockNum still pays only
	// one walk (the account+code walk fills both account and code caches).
	key := reqCacheKey{kind: cacheKindCode, addr: addr, blockNum: blockNum}
	if v, ok := r.cache[key]; ok {
		return v.([]byte), nil
	}
	entry, err := r.accountAndCode(addr, blockNum)
	if err != nil {
		return nil, err
	}
	r.cache[key] = entry.code
	return entry.code, nil
}

// StorageAt returns the storage slot value at (addr, slot) at the end of
// blockNum.
func (r *PersistentHistoryReader) StorageAt(addr tcommon.Address, slot tcommon.Hash, blockNum uint64) (tcommon.Hash, error) {
	key := reqCacheKey{kind: cacheKindStorage, addr: addr, slot: slot, blockNum: blockNum}
	if v, ok := r.cache[key]; ok {
		return v.(tcommon.Hash), nil
	}

	// At or past head: nothing to roll back, return the live value.
	if blockNum >= r.headNum {
		h, err := r.readStorageLive(addr, slot)
		if err != nil {
			return tcommon.Hash{}, err
		}
		r.cache[key] = h
		return h, nil
	}

	if h, ok, err := r.storageFromStateDomain(addr, slot, blockNum); err != nil {
		return tcommon.Hash{}, err
	} else if ok {
		r.cache[key] = h
		return h, nil
	}
	return tcommon.Hash{}, ErrStateDomainHistoryUnavailable
}

// AccountKVAt returns an account-owned domain value at the end of blockNum.
// It is the generic-KV companion to AccountAt/StorageAt for callers that need a
// rooted system-domain value, such as historical DynamicProperties reads.
func (r *PersistentHistoryReader) AccountKVAt(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, blockNum uint64) ([]byte, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	if !kvdomains.IsRegistered(domain) {
		return nil, false, fmt.Errorf("history account kv: unregistered domain %#04x", uint16(domain))
	}
	if blockNum >= r.headNum {
		generation, _, err := r.hotLatest().KVGeneration(owner)
		if err != nil {
			return nil, false, err
		}
		return r.hotLatest().KVLatest(owner, generation, domain, key)
	}
	ok, err := r.stateDomainHistoryAvailable()
	if err != nil || !ok {
		return nil, false, err
	}
	return r.readStateAccountKVAsOf(owner, domain, key, blockNum, r.headNum)
}

// accountAndCode walks the addr inverse index once and reconstructs both
// the account proto and the contract bytecode at end-of-blockNum. Account
// and code are returned together because slice-2 packs the pre-block code
// (when captured) into AccountDelta.CodePre, so a single walk fills both
// answers.
func (r *PersistentHistoryReader) accountAndCode(addr tcommon.Address, blockNum uint64) (accountCacheEntry, error) {
	cacheKey := reqCacheKey{kind: cacheKindAccount, addr: addr, blockNum: blockNum}
	if v, ok := r.cache[cacheKey]; ok {
		return v.(accountCacheEntry), nil
	}

	// At or past head: live read.
	if blockNum >= r.headNum {
		entry := r.readAccountAndCodeLive(addr)
		r.cache[cacheKey] = entry
		return entry, nil
	}

	if entry, ok, err := r.accountAndCodeFromStateDomain(addr, blockNum); err != nil {
		return accountCacheEntry{}, err
	} else if ok {
		r.cache[cacheKey] = entry
		return entry, nil
	}
	return accountCacheEntry{}, ErrStateDomainHistoryUnavailable
}

func (r *PersistentHistoryReader) accountAndCodeFromStateDomain(addr tcommon.Address, blockNum uint64) (accountCacheEntry, bool, error) {
	ok, err := r.stateDomainHistoryAvailable()
	if err != nil || !ok {
		return accountCacheEntry{}, false, err
	}
	data, ok, err := r.readStateAccountLatestAsOf(addr, blockNum, r.headNum)
	if err != nil {
		return accountCacheEntry{}, false, err
	}
	if !ok {
		return accountCacheEntry{}, true, nil
	}
	envelope, err := DecodeStateAccountV2(data)
	if err != nil {
		return accountCacheEntry{}, false, err
	}
	var pb corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &pb); err != nil {
		return accountCacheEntry{}, false, err
	}
	if err := r.materializeHistoricalAccountAux(&pb, addr, envelope.AccountKVGeneration, blockNum); err != nil {
		return accountCacheEntry{}, false, err
	}
	acc := types.NewAccountFromPB(&pb)
	var code []byte
	if envelope.CodeHash != (tcommon.Hash{}) {
		code, err = r.readCodeByHashAtBlock(envelope.CodeHash, blockNum)
		if err != nil {
			return accountCacheEntry{}, false, err
		}
	}
	if len(code) == 0 {
		code = nil
	}
	return accountCacheEntry{account: acc, code: code}, true, nil
}

func (r *PersistentHistoryReader) materializeHistoricalAccountAux(pb *corepb.Account, owner tcommon.Address, generation, blockNum uint64) error {
	if pb == nil {
		return nil
	}
	clearAccountAuxProto(pb)
	targetTxNum, err := r.stateTxNumAtBlockEnd(blockNum)
	if err != nil {
		return err
	}
	headTxNum, err := r.stateTxNumAtBlockEnd(r.headNum)
	if err != nil {
		return err
	}
	candidates := make(map[kvdomains.KVDomain]map[string]struct{}, len(accountSplitDomains))
	for _, domain := range accountSplitDomains {
		candidates[domain] = make(map[string]struct{})
	}
	if r.coldHistory != nil && targetTxNum < headTxNum {
		if err := r.coldHistory.IterateStateDomainChanges(targetTxNum+1, headTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
			if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Owner == owner && isAccountSplitDomain(change.Domain) {
				candidates[change.Domain][string(change.Key)] = struct{}{}
			}
			return true, nil
		}); err != nil {
			return err
		}
	}
	if r.coldHistory != nil {
		currentGeneration, exists, err := r.hotLatest().KVGeneration(owner)
		if err != nil {
			return err
		}
		if exists {
			for _, domain := range accountSplitDomains {
				if err := rawdb.IterateStateKVLatest(r.db, owner, currentGeneration, domain, nil, func(key, _ []byte) (bool, error) {
					candidates[domain][string(key)] = struct{}{}
					return true, nil
				}); err != nil {
					return err
				}
			}
		}
	}
	if boundary, ok := r.coldHistory.(StateKVColdBoundary); ok {
		for _, domain := range accountSplitDomains {
			if err := boundary.IterateKVLatestPrefix(domain, owner, generation, nil, targetTxNum, func(key, _ []byte) (bool, error) {
				candidates[domain][string(key)] = struct{}{}
				return true, nil
			}); err != nil {
				return err
			}
		}
	}
	loadDomain := func(domain kvdomains.KVDomain, load func(key, value []byte) (bool, error)) error {
		if cold, ok := r.coldHistory.(StateKVColdAsOf); ok {
			return cold.IterateKVAsOfPrefix(domain, owner, generation, nil, targetTxNum, load)
		}
		if r.coldHistory != nil {
			for key := range candidates[domain] {
				value, exists, err := r.readStateAccountKVAsOf(owner, domain, []byte(key), blockNum, r.headNum)
				if err != nil {
					return err
				}
				if exists {
					if _, err := load([]byte(key), value); err != nil {
						return err
					}
				}
			}
			return nil
		}
		return rawdb.IterateStateKVAsOfPrefixTxNum(r.db, owner, generation, domain, nil, targetTxNum, headTxNum, load)
	}

	for _, domain := range accountAuxDomains {
		values := accountAuxMap(pb, domain, true)
		load := func(key, value []byte) (bool, error) {
			decoded, err := decodeAccountAuxInt64(value)
			if err != nil {
				return false, err
			}
			values[string(key)] = decoded
			return true, nil
		}
		if err := loadDomain(domain, load); err != nil {
			return err
		}
	}

	clearAccountPermissionProto(pb)
	loadPermission := func(key, value []byte) (bool, error) {
		permission, kind, err := decodeAccountPermissionRow(key, value)
		if err != nil {
			return false, err
		}
		switch kind {
		case accountOwnerPermissionKey[0]:
			pb.OwnerPermission = permission
		case accountWitnessPermissionKey[0]:
			pb.WitnessPermission = permission
		case accountActivePermissionRoot[0]:
			pb.ActivePermission = append(pb.ActivePermission, permission)
		}
		return true, nil
	}
	if err := loadDomain(kvdomains.AccountPermissionAux, loadPermission); err != nil {
		return err
	}
	sort.Slice(pb.ActivePermission, func(i, j int) bool {
		return pb.ActivePermission[i].GetId() < pb.ActivePermission[j].GetId()
	})

	type indexedVote struct {
		index uint32
		vote  *corepb.Vote
	}
	votes := make([]indexedVote, 0)
	loadVote := func(key, value []byte) (bool, error) {
		index, vote, err := decodeAccountVoteRow(key, value)
		if err != nil {
			return false, err
		}
		votes = append(votes, indexedVote{index: index, vote: vote})
		return true, nil
	}
	if err := loadDomain(kvdomains.AccountVotesAux, loadVote); err != nil {
		return err
	}
	sort.Slice(votes, func(i, j int) bool { return votes[i].index < votes[j].index })
	clearAccountVotesProto(pb)
	for _, row := range votes {
		pb.Votes = append(pb.Votes, row.vote)
	}

	frozenBandwidth := make([]accountFrozenBandwidthRow, 0, 2)
	if err := loadDomain(kvdomains.AccountFrozenBandwidthAux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenBandwidthRow(key, value)
		if err != nil {
			return false, err
		}
		frozenBandwidth = append(frozenBandwidth, row)
		return true, nil
	}); err != nil {
		return err
	}
	sort.Slice(frozenBandwidth, func(i, j int) bool { return frozenBandwidth[i].index < frozenBandwidth[j].index })
	clearAccountStakeV1Proto(pb)
	for _, row := range frozenBandwidth {
		pb.Frozen = append(pb.Frozen, row.entry)
	}
	if err := loadDomain(kvdomains.AccountTronPowerAux, func(key, value []byte) (bool, error) {
		tronPower, err := decodeAccountTronPower(key, value)
		if err != nil {
			return false, err
		}
		pb.TronPower = tronPower
		return true, nil
	}); err != nil {
		return err
	}

	frozen := make([]accountFrozenV2Row, 0, 3)
	if err := loadDomain(kvdomains.AccountFrozenV2Aux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenV2Row(key, value)
		if err != nil {
			return false, err
		}
		frozen = append(frozen, row)
		return true, nil
	}); err != nil {
		return err
	}
	unfrozen := make([]accountUnfrozenV2Row, 0, 32)
	if err := loadDomain(kvdomains.AccountUnfrozenV2Aux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountUnfrozenV2Row(key, value)
		if err != nil {
			return false, err
		}
		unfrozen = append(unfrozen, row)
		return true, nil
	}); err != nil {
		return err
	}
	sort.Slice(frozen, func(i, j int) bool { return frozen[i].ordinal < frozen[j].ordinal })
	sort.Slice(unfrozen, func(i, j int) bool { return unfrozen[i].seq < unfrozen[j].seq })
	clearAccountStakeV2Proto(pb)
	for _, row := range frozen {
		pb.FrozenV2 = append(pb.FrozenV2, &corepb.Account_FreezeV2{Type: row.resource, Amount: row.amount})
	}
	for _, row := range unfrozen {
		pb.UnfrozenV2 = append(pb.UnfrozenV2, row.entry)
	}

	frozenSupply := make([]accountFrozenSupplyRow, 0, 10)
	if err := loadDomain(kvdomains.AccountFrozenSupplyAux, func(key, value []byte) (bool, error) {
		row, err := decodeAccountFrozenSupplyRow(key, value)
		if err != nil {
			return false, err
		}
		frozenSupply = append(frozenSupply, row)
		return true, nil
	}); err != nil {
		return err
	}
	sort.Slice(frozenSupply, func(i, j int) bool { return frozenSupply[i].index < frozenSupply[j].index })
	clearAccountFrozenSupplyProto(pb)
	for _, row := range frozenSupply {
		pb.FrozenSupply = append(pb.FrozenSupply, row.entry)
	}
	clearAccountResourceProto(pb)
	if err := loadDomain(kvdomains.AccountResourceAux, func(key, value []byte) (bool, error) {
		resource, err := decodeHistoricalAccountResource(key, value)
		if err != nil {
			return false, err
		}
		pb.AccountResource = resource
		return true, nil
	}); err != nil {
		return err
	}
	return nil
}

func (r *PersistentHistoryReader) storageFromStateDomain(addr tcommon.Address, slot tcommon.Hash, blockNum uint64) (tcommon.Hash, bool, error) {
	ok, err := r.stateDomainHistoryAvailable()
	if err != nil || !ok {
		return tcommon.Hash{}, false, err
	}
	accountData, accountExists, err := r.readStateAccountLatestAsOf(addr, blockNum, r.headNum)
	if err != nil {
		return tcommon.Hash{}, false, err
	}
	if !accountExists {
		return tcommon.Hash{}, true, nil
	}
	envelope, err := DecodeStateAccountV2(accountData)
	if err != nil {
		return tcommon.Hash{}, false, err
	}
	var meta *contractpb.SmartContract
	if data, ok, err := r.readStateAccountKVAsOf(addr, kvdomains.ContractMetadata, contractMetaKVKey, blockNum, r.headNum); err != nil {
		return tcommon.Hash{}, false, err
	} else if ok && len(data) > 0 {
		var sc contractpb.SmartContract
		if err := proto.Unmarshal(data, &sc); err != nil {
			return tcommon.Hash{}, false, err
		}
		meta = &sc
	}
	rowKey := javaStorageRowKey(addr, slot, meta)
	raw, ok, err := r.readStateKVAsOf(addr, envelope.AccountKVGeneration, kvdomains.ContractStorage, rowKey.Bytes(), blockNum, r.headNum)
	if err != nil {
		return tcommon.Hash{}, false, err
	}
	if !ok || len(raw) == 0 {
		return tcommon.Hash{}, true, nil
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	return h, true, nil
}

func (r *PersistentHistoryReader) readStateAccountLatestAsOf(owner tcommon.Address, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := r.stateTxNumAtBlockEnd(targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := r.stateTxNumAtBlockEnd(headBlock)
	if err != nil {
		return nil, false, err
	}
	if r.coldHistory == nil {
		cfg, err := stateDomainHistoryConfig()
		if err != nil {
			return nil, false, err
		}
		if cfg.ReadHotAccountLatestAsOf == nil {
			return nil, false, ErrStateDomainHistoryUnavailable
		}
		return cfg.ReadHotAccountLatestAsOf(r.db, owner, targetTxNum, headTxNum)
	}
	value, exists, err := r.hotLatest().AccountLatest(owner)
	if err != nil {
		return nil, false, err
	}
	if targetTxNum >= headTxNum {
		return append([]byte(nil), value...), exists, nil
	}
	changes, err := r.collectStateDomainChangesByKey(targetTxNum, headTxNum, rawdb.StateFlatDomainAccountLatest, owner, 0, 0, nil)
	if err != nil {
		return nil, false, err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		value, exists = previousStateDomainValue(changes[i])
	}
	return append([]byte(nil), value...), exists, nil
}

func (r *PersistentHistoryReader) readStateKVAsOf(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := r.stateTxNumAtBlockEnd(targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := r.stateTxNumAtBlockEnd(headBlock)
	if err != nil {
		return nil, false, err
	}
	return r.readStateKVAsOfTxNum(owner, generation, domain, key, targetTxNum, headTxNum)
}

func (r *PersistentHistoryReader) readStateKVAsOfTxNum(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte, targetTxNum, headTxNum uint64) ([]byte, bool, error) {
	if r.coldHistory == nil {
		cfg, err := stateDomainHistoryConfig()
		if err != nil {
			return nil, false, err
		}
		if cfg.ReadHotKVLatestAsOf == nil {
			return nil, false, ErrStateDomainHistoryUnavailable
		}
		return cfg.ReadHotKVLatestAsOf(r.db, owner, generation, domain, key, targetTxNum, headTxNum)
	}
	value, exists, err := r.hotLatest().KVLatest(owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	if targetTxNum >= headTxNum {
		return append([]byte(nil), value...), exists, nil
	}
	changes, err := r.collectStateDomainChangesByKey(targetTxNum, headTxNum, rawdb.StateFlatDomainKVLatest, owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		value, exists = previousStateDomainValue(changes[i])
	}
	return append([]byte(nil), value...), exists, nil
}

func (r *PersistentHistoryReader) readCodeByHashAtBlock(hash tcommon.Hash, blockNum uint64) ([]byte, error) {
	if hash == (tcommon.Hash{}) {
		return nil, nil
	}
	if code, ok, err := r.hotLatest().Code(hash); err != nil {
		return nil, err
	} else if ok && len(code) > 0 {
		return code, nil
	}
	cold, ok := r.coldHistory.(StateCodeColdHistory)
	if !ok {
		return nil, nil
	}
	txNum, err := r.stateTxNumAtBlockEnd(blockNum)
	if err != nil {
		return nil, err
	}
	if contentAddressed, ok := r.coldHistory.(StateCodeColdHistoryAtOrBefore); ok {
		code, ok, err := contentAddressed.GetCodeAtOrBefore(hash, txNum)
		if err != nil || !ok || len(code) == 0 {
			return nil, err
		}
		return append([]byte(nil), code...), nil
	}
	code, ok, err := cold.GetCode(hash, txNum)
	if err != nil || !ok || len(code) == 0 {
		return nil, err
	}
	return append([]byte(nil), code...), nil
}

func (r *PersistentHistoryReader) readStateKVGenerationAsOfTxNum(owner tcommon.Address, targetTxNum, headTxNum uint64) (uint64, bool, error) {
	if r.coldHistory == nil {
		cfg, err := stateDomainHistoryConfig()
		if err != nil {
			return 0, false, err
		}
		if cfg.ReadHotKVGenerationAsOf == nil {
			return 0, false, ErrStateDomainHistoryUnavailable
		}
		return cfg.ReadHotKVGenerationAsOf(r.db, owner, targetTxNum, headTxNum)
	}
	generation, exists, err := r.hotLatest().KVGeneration(owner)
	if err != nil {
		return 0, false, err
	}
	if targetTxNum >= headTxNum {
		return generation, exists, nil
	}
	changes, err := r.collectStateDomainChangesByKey(targetTxNum, headTxNum, rawdb.StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return 0, false, err
	}
	for i := len(changes) - 1; i >= 0; i-- {
		change := changes[i]
		if !change.PrevExists {
			generation = 0
			exists = false
			continue
		}
		generation, err = rawdb.DecodeStateKVGenerationValue(change.Prev)
		if err != nil {
			return 0, false, err
		}
		exists = true
	}
	return generation, exists, nil
}

func (r *PersistentHistoryReader) readStateAccountKVAsOf(owner tcommon.Address, domain kvdomains.KVDomain, key []byte, targetBlock, headBlock uint64) ([]byte, bool, error) {
	targetTxNum, err := r.stateTxNumAtBlockEnd(targetBlock)
	if err != nil {
		return nil, false, err
	}
	headTxNum, err := r.stateTxNumAtBlockEnd(headBlock)
	if err != nil {
		return nil, false, err
	}
	if r.coldHistory == nil {
		cfg, err := stateDomainHistoryConfig()
		if err != nil {
			return nil, false, err
		}
		if cfg.ReadHotAccountKVAsOf == nil {
			return nil, false, ErrStateDomainHistoryUnavailable
		}
		return cfg.ReadHotAccountKVAsOf(r.db, owner, domain, key, targetTxNum, headTxNum)
	}
	generation, _, err := r.hotLatest().KVGeneration(owner)
	if err != nil {
		return nil, false, err
	}
	value, exists, err := r.hotLatest().KVLatest(owner, generation, domain, key)
	if err != nil {
		return nil, false, err
	}
	upperTxNum := headTxNum
	for targetTxNum < upperTxNum {
		changes, err := r.collectStateAccountKVChanges(targetTxNum, upperTxNum, owner, generation, domain, key)
		if err != nil {
			return nil, false, err
		}
		if len(changes) == 0 {
			break
		}
		generationChanged := false
		for i := len(changes) - 1; i >= 0; i-- {
			change := changes[i]
			switch change.FlatDomain {
			case rawdb.StateFlatDomainKVLatest:
				value, exists = previousStateDomainValue(change)
			case rawdb.StateFlatDomainKVGeneration:
				generation, _, err = r.readStateKVGenerationAsOfTxNum(owner, previousTxNum(change.TxNum), headTxNum)
				if err != nil {
					return nil, false, err
				}
				value, exists, err = r.hotLatest().KVLatest(owner, generation, domain, key)
				if err != nil {
					return nil, false, err
				}
				upperTxNum = previousTxNum(change.TxNum)
				generationChanged = true
			}
			if generationChanged {
				break
			}
		}
		if !generationChanged {
			break
		}
	}
	return append([]byte(nil), value...), exists, nil
}

func (r *PersistentHistoryReader) collectStateAccountKVChanges(targetTxNum, headTxNum uint64, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]*rawdb.StateDomainChange, error) {
	kvChanges, err := r.collectStateDomainChangesByKey(targetTxNum, headTxNum, rawdb.StateFlatDomainKVLatest, owner, generation, domain, key)
	if err != nil {
		return nil, err
	}
	generationChanges, err := r.collectStateDomainChangesByKey(targetTxNum, headTxNum, rawdb.StateFlatDomainKVGeneration, owner, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	return mergeStateDomainChangeSets(kvChanges, generationChanges), nil
}

func (r *PersistentHistoryReader) collectStateDomainChangesByKey(targetTxNum, headTxNum uint64, flatDomain rawdb.StateFlatDomain, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]*rawdb.StateDomainChange, error) {
	if r == nil || targetTxNum >= headTxNum {
		return nil, nil
	}
	match := func(change *rawdb.StateDomainChange) bool {
		if change.FlatDomain != flatDomain || change.Owner != owner {
			return false
		}
		if flatDomain != rawdb.StateFlatDomainKVLatest {
			return true
		}
		return change.Generation == generation && change.Domain == domain && bytes.Equal(change.Key, key)
	}
	seen := make(map[stateDomainChangeKey]struct{})
	var changes []*rawdb.StateDomainChange
	add := func(change *rawdb.StateDomainChange) error {
		if change == nil || change.TxNum <= targetTxNum || change.TxNum > headTxNum || !match(change) {
			return nil
		}
		key := makeStateDomainChangeKey(change)
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		changes = append(changes, cloneHistoryDomainChange(change))
		return nil
	}
	if err := r.iterateHotStateDomainChangesByKey(targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, func(change *rawdb.StateDomainChange) (bool, error) {
		return true, add(change)
	}); err != nil {
		return nil, err
	}
	if r.coldHistory != nil && targetTxNum != ^uint64(0) {
		if keyed, ok := r.coldHistory.(StateDomainChangeColdKeyHistory); ok {
			if err := keyed.IterateStateDomainChangesByKey(targetTxNum+1, headTxNum, flatDomain, owner, generation, domain, key, func(change *rawdb.StateDomainChange) (bool, error) {
				return true, add(change)
			}); err != nil {
				return nil, err
			}
		} else if err := r.coldHistory.IterateStateDomainChanges(targetTxNum+1, headTxNum, func(change *rawdb.StateDomainChange) (bool, error) {
			return true, add(change)
		}); err != nil {
			return nil, err
		}
	}
	sortStateDomainChanges(changes)
	return changes, nil
}

func (r *PersistentHistoryReader) iterateHotStateDomainChangesByKey(targetTxNum, headTxNum uint64, flatDomain rawdb.StateFlatDomain, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if r == nil || targetTxNum >= headTxNum {
		return nil
	}
	cfg, err := stateDomainHistoryConfig()
	if err != nil {
		return err
	}
	if cfg.IterateHotHistoryChanges == nil {
		return ErrStateDomainHistoryUnavailable
	}
	return cfg.IterateHotHistoryChanges(r.db, targetTxNum, headTxNum, flatDomain, owner, generation, domain, key, fn)
}

func mergeStateDomainChangeSets(sets ...[]*rawdb.StateDomainChange) []*rawdb.StateDomainChange {
	seen := make(map[stateDomainChangeKey]struct{})
	var out []*rawdb.StateDomainChange
	for _, changes := range sets {
		for _, change := range changes {
			if change == nil {
				continue
			}
			key := makeStateDomainChangeKey(change)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, change)
		}
	}
	sortStateDomainChanges(out)
	return out
}

func sortStateDomainChanges(changes []*rawdb.StateDomainChange) {
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].TxNum != changes[j].TxNum {
			return changes[i].TxNum < changes[j].TxNum
		}
		if changes[i].BlockNum != changes[j].BlockNum {
			return changes[i].BlockNum < changes[j].BlockNum
		}
		return changes[i].Seq < changes[j].Seq
	})
}

type stateDomainChangeKey struct {
	blockNum   uint64
	txNum      uint64
	seq        uint64
	flatDomain rawdb.StateFlatDomain
	owner      tcommon.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        string
}

func makeStateDomainChangeKey(change *rawdb.StateDomainChange) stateDomainChangeKey {
	return stateDomainChangeKey{
		blockNum:   change.BlockNum,
		txNum:      change.TxNum,
		seq:        change.Seq,
		flatDomain: change.FlatDomain,
		owner:      change.Owner,
		generation: change.Generation,
		domain:     change.Domain,
		key:        string(change.Key),
	}
}

func previousStateDomainValue(change *rawdb.StateDomainChange) ([]byte, bool) {
	if change == nil || !change.PrevExists {
		return nil, false
	}
	return append([]byte(nil), change.Prev...), true
}

func previousTxNum(txNum uint64) uint64 {
	if txNum == 0 {
		return 0
	}
	return txNum - 1
}

func cloneHistoryDomainChange(in *rawdb.StateDomainChange) *rawdb.StateDomainChange {
	if in == nil {
		return nil
	}
	out := *in
	out.Key = append([]byte(nil), in.Key...)
	out.Prev = append([]byte(nil), in.Prev...)
	out.Next = append([]byte(nil), in.Next...)
	return &out
}

func (r *PersistentHistoryReader) stateDomainHistoryAvailable() (bool, error) {
	if r == nil || r.headNum == 0 {
		return false, nil
	}
	_, ok, err := r.stateTxRangeForBlock(r.headNum)
	return ok, err
}

func (r *PersistentHistoryReader) stateTxNumAtBlockEnd(blockNum uint64) (uint64, error) {
	row, ok, err := r.stateTxRangeForBlock(blockNum)
	if err != nil {
		return 0, err
	}
	if ok {
		if row.EndTxNum < row.BeginTxNum {
			return 0, errors.New("state history: invalid stored state tx range")
		}
		return row.EndTxNum, nil
	}
	return blockNum, nil
}

func (r *PersistentHistoryReader) stateTxRangeForBlock(blockNum uint64) (*rawdb.StateTxRange, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	if row, ok, err := snapshots.StateDomainHistoryTxRangeForBlock(r.db, blockNum); err != nil {
		return nil, false, err
	} else if ok {
		return row, true, nil
	}
	if cold, ok := r.coldHistory.(StateDomainChangeColdTxRange); ok {
		return cold.StateTxRangeForBlock(blockNum)
	}
	return nil, false, nil
}

// readAccountAndCodeLive reads the current account + code for addr from
// the chain's live view. Code resolution goes through the account envelope's
// CodeHash; there is no canonical address-keyed flat code fallback.
func (r *PersistentHistoryReader) readAccountAndCodeLive(addr tcommon.Address) accountCacheEntry {
	if r.live != nil {
		acc := r.live.GetAccount(addr)
		if acc == nil {
			// No live account means "no code either" — contract code is
			// selected by the account envelope and cleared together with
			// SELFDESTRUCT+DeleteAccount in StateDB.Commit().
			return accountCacheEntry{}
		}
		var code []byte
		if live, ok := r.live.(LiveContractReader); ok {
			code = live.GetCode(addr)
		}
		if len(code) == 0 {
			code = nil
		}
		return accountCacheEntry{account: acc, code: code}
	}

	envelope, ok, err := readFlatAccountLatestEnvelopeWithReader(r.hotLatest(), addr)
	if err != nil || !ok {
		return accountCacheEntry{}
	}
	var pb corepb.Account
	if err := proto.Unmarshal(envelope.AccountProto, &pb); err != nil {
		return accountCacheEntry{}
	}
	acc := types.NewAccountFromPB(&pb)
	var code []byte
	if envelope.CodeHash != (tcommon.Hash{}) {
		if hotCode, ok, err := r.hotLatest().Code(envelope.CodeHash); err == nil && ok {
			code = hotCode
		}
	}
	if len(code) == 0 {
		code = nil
	}
	return accountCacheEntry{account: acc, code: code}
}

// readStorageLive reads the current on-disk slot value for (addr, slot).
// Returns the zero hash when the slot is empty.
func (r *PersistentHistoryReader) readStorageLive(addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, error) {
	if live, ok := r.live.(LiveContractReader); ok {
		return live.GetState(addr, slot), nil
	}
	return readFlatStorageLatestWithReader(r.hotLatest(), addr, slot)
}

func readFlatStorageLatest(db ethdb.KeyValueReader, addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, error) {
	return readFlatStorageLatestWithReader(defaultHotLatest(db), addr, slot)
}

func readFlatStorageLatestWithReader(latest hotStateLatestReader, addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, error) {
	envelope, ok, err := readFlatAccountLatestEnvelopeWithReader(latest, addr)
	if err != nil || !ok {
		return tcommon.Hash{}, err
	}
	rowKey, err := storageRowKeyFromFlatLatest(latest, addr, envelope.AccountKVGeneration, slot)
	if err != nil {
		return tcommon.Hash{}, err
	}
	raw, ok, err := latest.KVLatest(addr, envelope.AccountKVGeneration, kvdomains.ContractStorage, rowKey.Bytes())
	if err != nil || !ok || len(raw) == 0 {
		return tcommon.Hash{}, err
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	return h, nil
}

func readFlatAccountLatestEnvelope(db ethdb.KeyValueReader, addr tcommon.Address) (*StateAccountV2, bool, error) {
	return readFlatAccountLatestEnvelopeWithReader(defaultHotLatest(db), addr)
}

func readFlatAccountLatestEnvelopeWithReader(latest hotStateLatestReader, addr tcommon.Address) (*StateAccountV2, bool, error) {
	return decodeHotAccountEnvelope(latest, addr)
}
