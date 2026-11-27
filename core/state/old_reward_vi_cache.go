package state

import (
	"math/big"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

const maxCachedOldRewardCycle = 100_000

var oldRewardDecimalOfVI = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

type oldRewardWitnessCycles struct {
	loaded []uint64
	deltas []*big.Int
}

func (w *oldRewardWitnessCycles) ensureCycleCapacity(cycle int) {
	needDeltas := cycle + 1
	if needDeltas > len(w.deltas) {
		grow := max(needDeltas, len(w.deltas)*2)
		if grow < 64 {
			grow = 64
		}
		next := make([]*big.Int, grow)
		copy(next, w.deltas)
		w.deltas = next
	}
	needWords := (cycle >> 6) + 1
	if needWords > len(w.loaded) {
		grow := max(needWords, len(w.loaded)*2)
		if grow < 4 {
			grow = 4
		}
		next := make([]uint64, grow)
		copy(next, w.loaded)
		w.loaded = next
	}
}

func (w *oldRewardWitnessCycles) hasCycle(cycle int64) bool {
	if w == nil || cycle < 0 {
		return false
	}
	idx := int(cycle)
	word := idx >> 6
	if word >= len(w.loaded) {
		return false
	}
	return (w.loaded[word] & (uint64(1) << (idx & 63))) != 0
}

func (w *oldRewardWitnessCycles) setCycle(cycle int64, delta *big.Int) {
	idx := int(cycle)
	w.ensureCycleCapacity(idx)
	w.loaded[idx>>6] |= uint64(1) << (idx & 63)
	w.deltas[idx] = delta
}

// OldRewardVICache caches immutable per-witness per-cycle VI deltas
// floor(reward * 1e18 / vote) for deeply settled cycles in the pre-VI reward
// segment so repeated WithdrawBalance queries sum O(cycles) in memory without
// re-reading thousands of historical SystemReward rows from disk.
type OldRewardVICache struct {
	mu             sync.RWMutex
	generation     uint64
	maxLoadedCycle int64
	witnesses      map[tcommon.Address]*oldRewardWitnessCycles
}

// NewOldRewardVICache constructs an empty historical-cycle VI delta cache.
func NewOldRewardVICache() *OldRewardVICache {
	return &OldRewardVICache{
		maxLoadedCycle: -1,
		witnesses:      make(map[tcommon.Address]*oldRewardWitnessCycles),
	}
}

// Reset discards all cached historical-cycle entries.
func (c *OldRewardVICache) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.resetLocked()
	c.mu.Unlock()
}

func (c *OldRewardVICache) resetLocked() {
	c.generation++
	c.maxLoadedCycle = -1
	clear(c.witnesses)
}

// InvalidateCycle resets cached entries if cycle may already be cached.
func (c *OldRewardVICache) InvalidateCycle(cycle int64) {
	if c == nil || cycle < 0 {
		return
	}
	c.mu.RLock()
	maxLoaded := c.maxLoadedCycle
	c.mu.RUnlock()
	if cycle > maxLoaded {
		return
	}
	c.mu.Lock()
	if cycle <= c.maxLoadedCycle {
		c.resetLocked()
	}
	c.mu.Unlock()
}

// SetOldRewardVICache installs the node-local immutable old-cycle reward cache.
func (s *StateDB) SetOldRewardVICache(cache *OldRewardVICache) {
	if s != nil {
		s.oldRewardVICache = cache
	}
}

func (s *StateDB) hasDirtyAccountKVDomain(owner tcommon.Address, domain kvdomains.KVDomain) bool {
	if s == nil {
		return false
	}
	obj := s.stateObjects[owner]
	if obj == nil {
		return false
	}
	if obj.accountKVGenerationDirty || obj.deleted || obj.created {
		return true
	}
	for mk := range obj.kvDirty {
		if dirtyDomain, _, ok := splitKVCompositeKeyView(ownedStringBytes(mk)); ok && dirtyDomain == domain {
			return true
		}
	}
	return false
}

// OldRewardVISum returns the cached cumulative old-segment VI delta
// sum_{c in [begin, end)} floor(reward[c] * 1e18 / vote[c]) for witness when
// every requested cycle is strictly below currentCycle-1 and no unrooted
// SystemReward mutations exist in StateDB. Returns (nil, false) on any guard
// fallback so callers compute directly from SnapshotReader.
func (s *StateDB) OldRewardVISum(witness tcommon.Address, begin, end, currentCycle int64) (*big.Int, bool) {
	if s == nil || s.oldRewardVICache == nil || s.Error() != nil ||
		s.transactionVersionedReader != nil || s.transactionAccess != nil ||
		begin < 0 || end <= begin || end > maxCachedOldRewardCycle ||
		currentCycle <= end+1 ||
		s.hasDirtyAccountKVDomain(tcommon.SystemAccountAddress, kvdomains.SystemReward) {
		return nil, false
	}

	cache := s.oldRewardVICache
	cache.mu.RLock()
	gen := cache.generation
	entry := cache.witnesses[witness]
	var missing []int64
	if entry == nil {
		missing = make([]int64, 0, int(end-begin))
		for cycle := begin; cycle < end; cycle++ {
			missing = append(missing, cycle)
		}
	} else {
		for cycle := begin; cycle < end; cycle++ {
			if !entry.hasCycle(cycle) {
				missing = append(missing, cycle)
			}
		}
		if len(missing) == 0 {
			viSum := sumCachedOldRewardDeltas(entry, begin, end)
			cache.mu.RUnlock()
			return viSum, true
		}
	}
	cache.mu.RUnlock()

	loadedDeltas := make([]*big.Int, len(missing))
	numScratch := new(big.Int)
	denScratch := new(big.Int)
	qScratch := new(big.Int)
	for i, cycle := range missing {
		totalReward := s.ReadCycleReward(cycle, witness[:])
		if s.Error() != nil {
			return nil, false
		}
		if totalReward == 0 {
			continue
		}
		totalVote := s.ReadCycleVote(cycle, witness[:])
		if s.Error() != nil {
			return nil, false
		}
		if totalVote == 0 {
			continue
		}
		numScratch.SetInt64(totalReward)
		numScratch.Mul(numScratch, oldRewardDecimalOfVI)
		denScratch.SetInt64(totalVote)
		qScratch.Quo(numScratch, denScratch)
		if qScratch.Sign() != 0 {
			loadedDeltas[i] = new(big.Int).Set(qScratch)
		}
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.generation != gen {
		return nil, false
	}
	entry = cache.witnesses[witness]
	if entry == nil {
		entry = &oldRewardWitnessCycles{}
		cache.witnesses[witness] = entry
	}
	for i, cycle := range missing {
		if !entry.hasCycle(cycle) {
			entry.setCycle(cycle, loadedDeltas[i])
		}
	}
	if end-1 > cache.maxLoadedCycle {
		cache.maxLoadedCycle = end - 1
	}
	return sumCachedOldRewardDeltas(entry, begin, end), true
}

func sumCachedOldRewardDeltas(entry *oldRewardWitnessCycles, begin, end int64) *big.Int {
	viSum := new(big.Int)
	for cycle := begin; cycle < end; cycle++ {
		if d := entry.deltas[int(cycle)]; d != nil {
			viSum.Add(viSum, d)
		}
	}
	return viSum
}
