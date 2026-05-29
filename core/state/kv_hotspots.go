package state

import (
	"encoding/hex"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// KVHotspotTracker accumulates per-(domain,key) write activity across every
// Commit. It is a long-lived diagnostic surface for finding "which specific
// keys churn the most" — a level of granularity below the existing
// CommitMutationStats.KVDomain[] counters, which only sum by domain type.
//
// Layout: sharded map[domain+key]→*KVHotspotEntry. Sharding keeps the per-Put
// mutex contention proportional to the number of writers (currently 1 — the
// commit serializer on chainmu — but ready for future concurrency).
//
// Memory bound: each shard caps at hotspotMaxKeysPerShard. Once full, the
// lowest-activity entry is evicted on each new insert so the working set
// converges on the genuinely hot keys. Eviction is intentionally simple
// (linear scan over the shard) — the cap is small and the eviction path only
// fires when capacity is full.
//
// Overhead: established negligible in the existing analysis (≤5k writes/s on
// Nile sync × ~100 ns per shard.Lock+map op = 0.05% of one core). The atomic
// `enabled` flag lets an operator disable the tracker at runtime via the
// /debug/state-hotspots?enabled=false handler if a future workload changes
// that.
type KVHotspotTracker struct {
	enabled atomic.Bool
	shards  [hotspotShardCount]hotspotShard
}

const (
	hotspotShardCount      = 16
	hotspotMaxKeysPerShard = 4096
)

type hotspotShard struct {
	mu      sync.Mutex
	entries map[string]*KVHotspotEntry
}

// KVHotspotEntry is a single (domain,key) row of the tracker.
type KVHotspotEntry struct {
	Domain   kvdomains.KVDomain
	Key      []byte // raw key bytes (copied on insert; never mutated post-insert)
	Puts     uint64 // total Put count since tracker start
	Deletes  uint64 // total Delete count
	PutBytes uint64 // cumulative value bytes written via Put
}

// Activity returns the total mutation count (puts + deletes).
func (e KVHotspotEntry) Activity() uint64 { return e.Puts + e.Deletes }

func newKVHotspotTracker() *KVHotspotTracker {
	t := &KVHotspotTracker{}
	for i := range t.shards {
		t.shards[i].entries = make(map[string]*KVHotspotEntry)
	}
	t.enabled.Store(true)
	return t
}

// SetEnabled toggles whether Record updates the tracker. Reads
// (Top / Snapshot) work regardless.
func (t *KVHotspotTracker) SetEnabled(on bool) { t.enabled.Store(on) }

// Enabled reports whether Record is currently active.
func (t *KVHotspotTracker) Enabled() bool { return t.enabled.Load() }

// Record adds one write to the tracker. Called from summarizeCommitMutations
// once per committed KV item. valueLen is the bytes written (0 for deletes).
func (t *KVHotspotTracker) Record(domain kvdomains.KVDomain, key []byte, deleted bool, valueLen int) {
	if !t.enabled.Load() || len(key) == 0 {
		return
	}
	s := &t.shards[shardOf(key)]
	mapKey := makeMapKey(domain, key)

	s.mu.Lock()
	e, ok := s.entries[mapKey]
	if !ok {
		if len(s.entries) >= hotspotMaxKeysPerShard {
			evictLowestActivity(s.entries)
		}
		e = &KVHotspotEntry{
			Domain: domain,
			Key:    append([]byte(nil), key...),
		}
		s.entries[mapKey] = e
	}
	if deleted {
		e.Deletes++
	} else {
		e.Puts++
		e.PutBytes += uint64(valueLen)
	}
	s.mu.Unlock()
}

// Snapshot returns a copy of every tracked entry. Safe to call concurrently
// with Record; each shard is briefly locked while its entries are appended
// to the output.
func (t *KVHotspotTracker) Snapshot() []KVHotspotEntry {
	var out []KVHotspotEntry
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		if len(s.entries) > 0 && out == nil {
			out = make([]KVHotspotEntry, 0, len(s.entries)*hotspotShardCount)
		}
		for _, e := range s.entries {
			out = append(out, *e)
		}
		s.mu.Unlock()
	}
	return out
}

// HotspotSortKey selects the metric for Top's ordering.
type HotspotSortKey int

const (
	HotspotSortByActivity HotspotSortKey = iota // puts + deletes (default)
	HotspotSortByPuts
	HotspotSortByDeletes
	HotspotSortByBytes
)

// Top returns up to limit entries ordered by sortBy (descending). If
// domainFilter is non-empty, only entries with a matching domain name are
// included. domainFilter is matched against kvdomains.Name(entry.Domain).
func (t *KVHotspotTracker) Top(limit int, sortBy HotspotSortKey, domainFilter string) []KVHotspotEntry {
	all := t.Snapshot()
	if domainFilter != "" {
		filtered := all[:0]
		for _, e := range all {
			if kvdomains.Name(e.Domain) == domainFilter {
				filtered = append(filtered, e)
			}
		}
		all = filtered
	}
	sort.Slice(all, func(i, j int) bool {
		var li, lj uint64
		switch sortBy {
		case HotspotSortByPuts:
			li, lj = all[i].Puts, all[j].Puts
		case HotspotSortByDeletes:
			li, lj = all[i].Deletes, all[j].Deletes
		case HotspotSortByBytes:
			li, lj = all[i].PutBytes, all[j].PutBytes
		default:
			li, lj = all[i].Activity(), all[j].Activity()
		}
		if li != lj {
			return li > lj
		}
		return hex.EncodeToString(all[i].Key) < hex.EncodeToString(all[j].Key)
	})
	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}
	return all
}

// Reset clears every shard's entries. The enabled flag is preserved.
func (t *KVHotspotTracker) Reset() {
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.entries = make(map[string]*KVHotspotEntry)
		s.mu.Unlock()
	}
}

// shardOf computes the shard index for a key via FNV-1a over the first
// 8 bytes (or fewer, if the key is shorter). FNV-1a is allocation-free and
// well-distributed for small inputs.
func shardOf(key []byte) uint8 {
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	h := offset64
	n := len(key)
	if n > 8 {
		n = 8
	}
	for i := 0; i < n; i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return uint8(h % hotspotShardCount)
}

// makeMapKey builds the per-shard map key. Domain is prepended so two
// identical raw keys in different domains stay separate.
func makeMapKey(domain kvdomains.KVDomain, key []byte) string {
	var hdr [1]byte
	hdr[0] = byte(domain)
	// Single string concat to keep this inexpensive; the alloc here is the
	// per-Record cost we already budgeted for.
	return string(hdr[:]) + string(key)
}

func evictLowestActivity(m map[string]*KVHotspotEntry) {
	var lowestKey string
	var lowestActivity uint64 = ^uint64(0)
	for k, e := range m {
		act := e.Activity()
		if act < lowestActivity {
			lowestActivity = act
			lowestKey = k
		}
	}
	if lowestKey != "" {
		delete(m, lowestKey)
	}
}

// defaultKVHotspotTracker is the process-wide singleton. Tracker is
// diagnostic-only and never participates in consensus, so a singleton
// avoids threading another dependency through state.StateDB / BlockChain /
// debugapi.NewServer.
var defaultKVHotspotTracker = newKVHotspotTracker()

// DefaultKVHotspotTracker returns the process singleton.
func DefaultKVHotspotTracker() *KVHotspotTracker { return defaultKVHotspotTracker }
