package vm

import (
	"github.com/ethereum/go-ethereum/common/lru"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// jumpdestCacheSize bounds the JUMPDEST analysis cache. Each entry is a
// ceil(len(code)/8)-byte bitvec, so a few thousand hot contracts keep the cache
// in the low single-digit MB even on a long sync while still covering the working
// set of contracts that are called repeatedly.
const jumpdestCacheSize = 4096

// jumpdestCache memoizes JUMPDEST analysis keyed by contract code hash. Identical
// bytecode — a proxy's implementation, a CREATE2 redeploy, a hot contract called
// across many transactions in a block — is analyzed once and the resulting bitvec
// reused, instead of re-scanning the whole code on every call frame (the scan was
// ~7% of block-execution CPU on a contract-heavy Nile sync). It mirrors
// go-ethereum's JumpDestCache: an LRU keyed by code hash.
//
// The cached bitvec is immutable once analyzed (only isSet ever reads it), so
// handing the same slice to many concurrent call frames is safe. lru.Cache is
// mutex-guarded, so the serial block-exec thread and read-only eth_call copies may
// consult it concurrently.
type jumpdestCache struct {
	cache *lru.Cache[tcommon.Hash, bitvec]
}

func newJumpdestCache(size int) *jumpdestCache {
	return &jumpdestCache{cache: lru.NewCache[tcommon.Hash, bitvec](size)}
}

// analyze returns the JUMPDEST bitvec for code, reusing a cached result when
// codeHash identifies bytecode already analyzed. codeHash MUST be the VM code
// identity Keccak256(code) (exactly what StateDB.GetCodeHash returns): same code
// ⇒ same hash ⇒ same bitvec by construction, and returning a wrong set would
// require a keccak collision.
//
// A zero code hash (initcode not yet written to state, or any unknown identity)
// or empty code is analyzed directly and never cached — matching go-ethereum,
// which does not cache transient initcode, and keeping one-off keys out of the
// cache.
func (jc *jumpdestCache) analyze(codeHash tcommon.Hash, code []byte) bitvec {
	if codeHash == (tcommon.Hash{}) || len(code) == 0 {
		return analyzeJumpdests(code)
	}
	if bv, ok := jc.cache.Get(codeHash); ok {
		return bv
	}
	bv := analyzeJumpdests(code)
	jc.cache.Add(codeHash, bv)
	return bv
}

// len reports the number of cached analyses. Used by tests to assert reuse and
// the eviction bound.
func (jc *jumpdestCache) len() int { return jc.cache.Len() }

// globalJumpdestCache is the process-wide analysis cache consulted by every
// Contract.SetCode. One shared cache maximizes reuse across transactions and
// blocks; it is bounded (jumpdestCacheSize) so a long sync touching many distinct
// contracts cannot grow it without limit.
var globalJumpdestCache = newJumpdestCache(jumpdestCacheSize)
