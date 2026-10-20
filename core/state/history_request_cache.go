package state

import (
	"context"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Account copied payload/key bytes plus an estimated 256-byte entry overhead.
// The separate entry cap bounds map/row bookkeeping; this is not an RSS limit.
const historyFirstMemoBytes = 4 << 20
const historyFirstMemoEntries = 4096

type historyFirstKey struct {
	target, head uint64
	flat         rawdb.StateFlatDomain
	owner        common.Address
	generation   uint64
	domain       kvdomains.KVDomain
	key          string
}

func firstMemoKey(target, head uint64, q snapshots.StateHistoryQuery) historyFirstKey {
	return historyFirstKey{target, head, q.FlatDomain, q.Owner, q.Generation, q.Domain, string(q.Key)}
}

func (r *PersistentHistoryReader) memoFirst(target, head uint64, q snapshots.StateHistoryQuery) (*rawdb.StateDomainChange, bool) {
	if r == nil || !r.firstMemoPinned || !r.hotHistoryBounded {
		return nil, false
	}
	change, ok := r.firstMemo[firstMemoKey(target, head, q)]
	return cloneHistoryDomainChange(change), ok
}

// The reader is request-owned, like its existing high-level caches. Account,
// generation, empty KV and absent first-change entries have separate identities.
func (r *PersistentHistoryReader) rememberFirst(target, head uint64, q snapshots.StateHistoryQuery, change *rawdb.StateDomainChange) {
	if r == nil || !r.firstMemoPinned || !r.hotHistoryBounded || target >= head || r.contextError() != nil {
		return
	}
	k := firstMemoKey(target, head, q)
	if _, ok := r.firstMemo[k]; ok {
		return
	}
	size := 256 + len(q.Key)
	if change != nil {
		size += len(change.Key) + len(change.Prev) + len(change.Next)
	}
	if len(r.firstMemo) >= historyFirstMemoEntries || size > historyFirstMemoBytes-r.firstMemoBytes {
		return
	}
	if r.firstMemo == nil {
		r.firstMemo = make(map[historyFirstKey]*rawdb.StateDomainChange)
	}
	r.firstMemo[k] = cloneHistoryDomainChange(change)
	r.firstMemoBytes += size
}

type stateDomainChangeColdMixedHistory interface {
	FirstStateDomainChangesContext(context.Context, uint64, uint64, []snapshots.StateHistoryQuery) ([]*rawdb.StateDomainChange, error)
}

// prefetchFirst merges only fully resolved cold/hot batches into this request's
// memo. A failed speculative batch has no effect on later authoritative reads.
func (r *PersistentHistoryReader) prefetchFirst(target, head uint64, queries []snapshots.StateHistoryQuery) error {
	if err := r.contextError(); err != nil {
		return err
	}
	if !r.firstMemoPinned || !r.hotHistoryBounded || target >= head {
		return nil
	}
	cold, ok := r.coldHistory.(stateDomainChangeColdMixedHistory)
	if !ok {
		return nil
	}
	pending := make([]snapshots.StateHistoryQuery, 0, len(queries))
	seen := make(map[historyFirstKey]bool, len(queries))
	for _, q := range queries {
		k := firstMemoKey(target, head, q)
		if _, ok := r.memoFirst(target, head, q); !ok && !seen[k] {
			pending = append(pending, q)
			seen[k] = true
		}
	}
	if len(pending) == 0 {
		return nil
	}
	// Execution prefetch is small. Do not allocate/retain an unbounded plan.
	if len(pending) > 256 {
		return nil
	}
	changes, err := cold.FirstStateDomainChangesContext(r.ctx, target+1, head, pending)
	if err != nil {
		return err
	}
	if len(changes) != len(pending) {
		return ErrStateDomainHistoryUnavailable
	}
	for i, q := range pending {
		if err := r.contextError(); err != nil {
			return err
		}
		first := changes[i]
		if first != nil && (first.TxNum <= target || first.TxNum > head || first.FlatDomain != q.FlatDomain || first.Owner != q.Owner || q.FlatDomain == rawdb.StateFlatDomainKVLatest && (first.Generation != q.Generation || first.Domain != q.Domain || string(first.Key) != string(q.Key))) {
			return ErrStateDomainHistoryUnavailable
		}
		hotHead, hotBlock := head, r.hotHistoryToBlock
		if first != nil {
			hotHead = first.TxNum
			hotBlock = min(hotBlock, first.BlockNum)
		}
		hot, err := r.readFirstHotStateDomainChangeByKeyThroughBlock(target, hotHead, hotBlock, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
		if err != nil {
			return err
		}
		if hot != nil && (first == nil || stateDomainChangeLess(hot, first)) {
			changes[i] = hot
		}
	}
	if err := r.contextError(); err != nil {
		return err
	}
	for i, q := range pending {
		r.rememberFirst(target, head, q, changes[i])
	}
	return nil
}

// PrefetchExecutionAt batches the known read-only constant-call inputs. It is
// advisory: non-cancellation errors are retried by ordinary demand reads, so an
// unused speculative key cannot make an otherwise valid call fail. No errors
// or incomplete batches enter the memo. Account envelopes determine generations
// before planning metadata/system-domain keys; storage slots remain on demand.
func (r *PersistentHistoryReader) PrefetchExecutionAt(block uint64, caller, contract common.Address, versions []int32) error {
	if err := r.contextError(); err != nil {
		return err
	}
	if r == nil || !r.firstMemoPinned || !r.hotHistoryBounded || block >= r.headNum {
		return nil
	}
	if _, ok := r.coldHistory.(stateDomainChangeColdMixedHistory); !ok {
		return nil
	}
	target, err := r.stateTxNumAtBlockEnd(block)
	if err != nil {
		return r.contextError()
	}
	head, err := r.stateTxNumAtBlockEnd(r.headNum)
	if err != nil {
		return r.contextError()
	}
	accounts := []common.Address{common.SystemAccountAddress, caller, contract}
	queries := make([]snapshots.StateHistoryQuery, 0, 3)
	for _, owner := range accounts {
		queries = append(queries, snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner})
	}
	if err := r.prefetchFirst(target, head, queries); err != nil {
		return r.contextError()
	}
	queries = nil
	for _, owner := range []common.Address{common.SystemAccountAddress, contract} {
		raw, exists, err := r.readStateAccountLatestAsOf(owner, block, r.headNum)
		if err != nil {
			return r.contextError()
		}
		if !exists {
			continue
		}
		envelope, err := DecodeStateAccountV2(raw)
		if err != nil {
			continue
		}
		add := func(domain kvdomains.KVDomain, key []byte) {
			queries = append(queries, snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: envelope.AccountKVGeneration, Domain: domain, Key: key})
		}
		if owner == common.SystemAccountAddress {
			for key := range defaultProps {
				if !isDerivedDPKey(key) {
					add(kvdomains.SystemDynamicProperty, []byte(key))
				}
			}
			for key := range defaultStringProps {
				if !isDerivedDPKey(key) {
					add(kvdomains.SystemDynamicProperty, []byte(key))
				}
			}
			for _, version := range versions {
				add(kvdomains.SystemForkVote, rawdb.ForkStatsStateKey(version))
			}
		} else {
			add(kvdomains.ContractMetadata, contractMetaKVKey)
		}
	}
	if err := r.prefetchFirst(target, head, queries); err != nil {
		return r.contextError()
	}
	return r.contextError()
}
