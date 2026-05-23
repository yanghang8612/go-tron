package state

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var ErrHistoryDeltaMissing = errors.New("history inverse index references missing forward delta")

func missingHistoryDelta(kind string, blockNum uint64) error {
	return fmt.Errorf("%w: kind=%s block=%d", ErrHistoryDeltaMissing, kind, blockNum)
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
//     state. RPC handlers select this implementation when the node wasn't
//     synced in archive mode; callers get "degraded" historical reads
//     (== live reads) without an error.
//
//   - PersistentHistoryReader — the archive-mode implementation. Walks the
//     sh-a- / sh-s- delta entries newest-first, applying each as a rollback,
//     until the state matches end-of-blockNum. Uses the sh-i-a- / sh-i-s-
//     inverse indexes to skip blocks that didn't touch the queried key.
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
// resolve the END-OF-HEAD account snapshot. gtron persists account state
// in an MPT keyed by Keccak256(addr) (see state.StateDB.Commit), not under
// the flat-state account prefix — so a live account read can NOT be served
// by rawdb.ReadAccount(disk, addr). The chain's *StateDB already knows how
// to resolve "live" accounts (in-memory stateObjects map falling back to
// trie.Get), and satisfies this interface directly via GetAccount.
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
// that weren't synced with HistoryEnabled=true — those nodes don't have
// sh-* rows on disk, so the only available answer is "current state".
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
// storage from flat compatibility paths when no live contract reader exists;
// `live` resolves accounts and contract code via the trie-backed state.
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
	raw := rawdb.ReadStorage(r.db, addr, storageRowKeyFromDB(r.db, addr, slot))
	if len(raw) == 0 {
		return tcommon.Hash{}, nil
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	return h, nil
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

// PersistentHistoryReader reconstructs historical state from sh-* rawdb
// rows. It seeks the inverse index for the queried key (sh-i-a-, sh-i-s-),
// gathers every block M > blockNum at which the key was modified, then
// rolls back each AccountDelta / SlotDelta in newest-first order. The
// resulting candidate is the state at the end of blockNum.
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
//	buffer-aware view (bc.buffer), NOT the bare disk store. sh-* delta rows
//	for recent (unflushed) blocks live only in the in-memory buffer layers
//	until they flush past the solidified line; the buffer's NewIterator
//	merges those layers over disk and masks tombstones, so a buffer-backed
//	reader sees the logically-complete row set and transparently falls
//	through to disk for flushed rows. The production archive-query path
//	(core.TronBackend.historyReaderAt) wires bc.buffer for exactly this
//	reason. Passing the bare disk store would miss recent history.
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
	// MPT (state.StateDB.Commit writes via trie.Update keyed by
	// Keccak256(addr)) so a flat-state Get(accountKey) won't find them.
	// Code and storage stay on flat-state prefixes and are served by `db`
	// directly. live may be nil — in that case the reader's live-account
	// baseline is "no account exists", and any AccountAt(addr, N) is
	// answerable only from history rows.
	live LiveAccountReader

	headNum uint64
	cache   map[reqCacheKey]any
}

// readerDB is the KV surface the reader needs: point Get/Has reads plus
// prefix iteration. The chain disk store and memorydb satisfy it; tests
// in this package use the latter.
type readerDB interface {
	ethdb.KeyValueReader
	ethdb.Iteratee
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
// SlotDelta rows, the inverse-index scan, and the live flat-state code +
// storage. The chain's *rawdb.Database satisfies the interface directly;
// tests use ethrawdb.NewMemoryDatabase().
func NewPersistentHistoryReader(db readerDB, live LiveAccountReader, headNum uint64) *PersistentHistoryReader {
	return &PersistentHistoryReader{
		db:      db,
		live:    live,
		headNum: headNum,
		cache:   make(map[reqCacheKey]any),
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

	// Walk the inverse index forward, collect every block M > blockNum
	// where the slot was modified. memorydb / Pebble iterators are
	// forward-only; we materialise into a slice so we can roll back in
	// newest-first order (apply older pre-values last, so they overwrite
	// the more-recent rollback steps).
	futures, err := r.collectFutureBlocks(rawdb.IterateSlotInverse(r.db, addr, slot), rawdb.SlotInverseBlockNum, blockNum)
	if err != nil {
		return tcommon.Hash{}, err
	}
	if len(futures) == 0 {
		// Slot never modified after blockNum → live state is correct.
		h, err := r.readStorageLive(addr, slot)
		if err != nil {
			return tcommon.Hash{}, err
		}
		r.cache[key] = h
		return h, nil
	}

	// Start with live (end-of-HEAD) and roll back newest-first.
	candidate, err := r.readStorageLive(addr, slot)
	if err != nil {
		return tcommon.Hash{}, err
	}
	for i := len(futures) - 1; i >= 0; i-- {
		preVal, found := rawdb.ReadSlotDelta(r.db, futures[i], addr, slot)
		if !found {
			return tcommon.Hash{}, missingHistoryDelta("slot", futures[i])
		}
		candidate = preVal
	}
	r.cache[key] = candidate
	return candidate, nil
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

	futures, err := r.collectFutureBlocks(rawdb.IterateAddrInverse(r.db, addr), rawdb.AddrInverseBlockNum, blockNum)
	if err != nil {
		return accountCacheEntry{}, err
	}
	if len(futures) == 0 {
		entry := r.readAccountAndCodeLive(addr)
		r.cache[cacheKey] = entry
		return entry, nil
	}

	// Live = end-of-HEAD baseline.
	entry := r.readAccountAndCodeLive(addr)
	acc := entry.account
	code := entry.code

	// Roll back newest-first.
	//
	// Each AccountDelta records the pre-block state at its block M:
	//   - ExistedPre=false → at end-of-(M-1), account didn't exist.
	//   - AccountProtoPre != nil → the full pre-block account proto.
	//   - CodePre != nil → block M's codeChange captured this prevCode.
	//     If CodePre is nil, code at end-of-(M-1) == code at end-of-M (no
	//     codeChange happened at M), so we keep the running candidate.
	for i := len(futures) - 1; i >= 0; i-- {
		delta := rawdb.ReadAccountDelta(r.db, futures[i], addr)
		if delta == nil {
			return accountCacheEntry{}, missingHistoryDelta("account", futures[i])
		}
		if !delta.ExistedPre {
			// Account didn't exist pre-block M. Code likewise gone.
			acc = nil
			code = nil
			continue
		}
		if len(delta.AccountProtoPre) > 0 {
			var pb corepb.Account
			if err := proto.Unmarshal(delta.AccountProtoPre, &pb); err != nil {
				return accountCacheEntry{}, err
			}
			acc = types.NewAccountFromPB(&pb)
		} else {
			// ExistedPre=true but no proto bytes is only possible if slice-2
			// captured no accountChange but did capture a codeChange /
			// contractMetaChange. The pre-block proto stayed equal to the
			// then-current proto from disk; the absence of AccountProtoPre
			// means "no balance/freeze/etc change at block M", so the
			// running candidate account is already correct for end-of-(M-1).
		}
		if delta.CodePre != nil {
			// nil-vs-empty distinction: slice 2 writes nil for "no
			// codeChange captured" and a (possibly empty) slice for "code
			// was changed; pre-block code is these bytes". A non-nil empty
			// slice means "pre-block had no code" — represent as nil here
			// so callers see the same shape as a never-had-code account.
			//
			// NOTE: len(CodePre)==0 && CodePre!=nil is currently unreachable
			// under the slice-2 capture path (history_capture.go only allocates
			// CodePre when len(prevCode)>0, and proto3 bytes round-trips
			// nil/empty identically). Kept as a future-safe defensive branch
			// in case a downstream capture path produces an explicit-empty
			// pre-image.
			if len(delta.CodePre) == 0 {
				code = nil
			} else {
				code = delta.CodePre
			}
		}
	}
	out := accountCacheEntry{account: acc, code: code}
	r.cache[cacheKey] = out
	return out, nil
}

// readAccountAndCodeLive reads the current account + code for addr from
// the chain's live view. Code resolution goes through the account envelope's
// CodeHash; there is no canonical address-keyed flat code fallback.
func (r *PersistentHistoryReader) readAccountAndCodeLive(addr tcommon.Address) accountCacheEntry {
	var acc *types.Account
	if r.live != nil {
		acc = r.live.GetAccount(addr)
	}
	if acc == nil {
		// No live account means "no code either" — contract code is selected
		// by the account envelope and cleared together with SELFDESTRUCT+
		// DeleteAccount in statedb.Commit().
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

// readStorageLive reads the current on-disk slot value for (addr, slot).
// Returns the zero hash when the slot is empty.
func (r *PersistentHistoryReader) readStorageLive(addr tcommon.Address, slot tcommon.Hash) (tcommon.Hash, error) {
	if live, ok := r.live.(LiveContractReader); ok {
		return live.GetState(addr, slot), nil
	}
	raw := rawdb.ReadStorage(r.db, addr, storageRowKeyFromDB(r.db, addr, slot))
	if len(raw) == 0 {
		return tcommon.Hash{}, nil
	}
	var h tcommon.Hash
	copy(h[len(h)-len(raw):], raw)
	return h, nil
}

// collectFutureBlocks walks an inverse-index iterator forward and
// collects every blockNum strictly greater than `target`. `extract` pulls
// the trailing big-endian uint64 blockNum out of each key — pass
// rawdb.AddrInverseBlockNum or rawdb.SlotInverseBlockNum.
//
// The iterator is released by this helper. The returned slice is sorted
// ascending (matching the inverse-index key order); callers iterate it
// in reverse to roll back newest-first.
//
// Defensive clamp: entries with blockNum > headNum are skipped. In normal
// operation the inverse-index doesn't contain such rows because the
// reader was constructed for "history up to headNum"; if it does, another
// writer raced ahead and the reader treats those rows as out-of-view.
func (r *PersistentHistoryReader) collectFutureBlocks(it ethdb.Iterator, extract func([]byte) (uint64, bool), target uint64) ([]uint64, error) {
	defer it.Release()
	var futures []uint64
	for it.Next() {
		m, ok := extract(it.Key())
		if !ok {
			continue
		}
		if m <= target {
			continue
		}
		if m > r.headNum {
			continue
		}
		futures = append(futures, m)
	}
	return futures, it.Error()
}
