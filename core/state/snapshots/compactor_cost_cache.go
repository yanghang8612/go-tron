package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strings"
)

const (
	historyCostCacheBytes       = 64 << 20
	historyCostCacheEntryCharge = uint64(512)
	historyCostCacheEntries     = historyCostCacheBytes / int(historyCostCacheEntryCharge)
)

// Runner.passMu protects this volatile selection-hint cache. No hit authorizes
// source use: only selected fresh metadata and complete codec authentication do.
// Keys/values own no refs, FileInfo or manifest string slabs. Charge is a bounded
// conservative map allowance, not a process RSS promise.
type historyCompactionCostCache struct {
	dir        string
	directory  os.FileInfo
	epoch      uint64
	entries    map[historyCompactionCostKey]historyCompactionInputCost
	bytes      uint64
	maxEntries int
	maxBytes   uint64
	stat       func(string) (os.FileInfo, error)
	stats      historyCompactionCostCacheStats
}
type historyCompactionCostKey [sha256.Size]byte
type historyCompactionInputRefs [3]SegmentRef

type historyCompactionCostCacheStats struct {
	Hits, Misses, MetadataReads, FreshReads, EvictedRetired, CapacityRejects, Resets uint64
	Entries, ChargedBytes                                                            uint64
}

func (c *historyCompactionCostCache) snapshot() historyCompactionCostCacheStats {
	if c == nil {
		return historyCompactionCostCacheStats{}
	}
	s := c.stats
	s.Entries = uint64(len(c.entries))
	s.ChargedBytes = c.bytes
	return s
}
func newHistoryCompactionCostCache() *historyCompactionCostCache {
	return &historyCompactionCostCache{maxEntries: historyCostCacheEntries, maxBytes: historyCostCacheBytes}
}
func (c *historyCompactionCostCache) clear() {
	c.entries = make(map[historyCompactionCostKey]historyCompactionInputCost)
	c.bytes = 0
	c.stats.Resets++
}
func (c *historyCompactionCostCache) statFile(path string) (os.FileInfo, error) {
	if c.stat != nil {
		return c.stat(path)
	}
	return os.Stat(path)
}
func (c *historyCompactionCostCache) bind(ctx context.Context, dir string, epoch uint64) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	abs = strings.Clone(abs)
	info, err := c.statFile(abs)
	if err != nil {
		c.clear()
		return err
	}
	if !info.IsDir() {
		c.clear()
		return fmt.Errorf("snapshots: compaction directory %q is not a directory", dir)
	}
	if c.directory == nil || c.dir != abs || c.epoch != epoch || !os.SameFile(c.directory, info) {
		c.clear()
		c.dir, c.directory, c.epoch = abs, info, epoch
	}
	return contextError(ctx)
}

// Canonical slots remove companion-order dependence, not raw ref identity.
func historyCompactionCacheRefs(candidate historyCompactionCandidate) (historyCompactionInputRefs, bool) {
	var refs historyCompactionInputRefs
	if candidate.history.Dataset != SegmentDatasetStateDomainChange || candidate.history.Kind != SegmentHistory || len(candidate.companions) != 2 {
		return refs, false
	}
	refs[0] = candidate.history
	var inverted, accessor bool
	for _, ref := range candidate.companions {
		switch ref.Kind {
		case SegmentInverted:
			if inverted {
				return refs, false
			}
			refs[1] = ref
			inverted = true
		case SegmentAccessor:
			if accessor {
				return refs, false
			}
			refs[2] = ref
			accessor = true
		default:
			return refs, false
		}
	}
	if !inverted || !accessor {
		return refs, false
	}
	for _, ref := range refs {
		if ref.Size == 0 || ref.Checksum == "" {
			return refs, false
		}
	}
	return refs, true
}
func historyCompactionCacheKey(candidate historyCompactionCandidate) (historyCompactionCostKey, bool) {
	refs, ok := historyCompactionCacheRefs(candidate)
	if !ok {
		return historyCompactionCostKey{}, false
	}
	return digestHistoryCompactionRefs(refs), true
}

// Encode every raw SegmentRef field with lengths/fixed-width numbers and a
// dedicated domain. Do not reuse staging IDs (different identity/ownership).
// The field-inventory test deliberately fails when SegmentRef gains a field.
func digestHistoryCompactionRefs(refs historyCompactionInputRefs) historyCompactionCostKey {
	h := sha256.New()
	_, _ = h.Write([]byte("go-tron/history-compaction-cost-hint/v3\x00"))
	var number [8]byte
	writeNumber := func(n uint64) { binary.LittleEndian.PutUint64(number[:], n); _, _ = h.Write(number[:]) }
	for _, ref := range refs {
		writeNumber(ref.Rows)
		writeHistoryCostHintString(h, string(ref.Dataset), number[:])
		writeNumber(uint64(ref.Domain))
		writeHistoryCostHintString(h, string(ref.Kind), number[:])
		writeNumber(ref.FromTxNum)
		writeNumber(ref.ToTxNum)
		writeNumber(ref.AggregationSteps)
		writeHistoryCostHintString(h, ref.Path, number[:])
		writeNumber(ref.Size)
		writeHistoryCostHintString(h, ref.Checksum, number[:])
		writeHistoryCostHintString(h, ref.LookupRoot, number[:])
	}
	var key historyCompactionCostKey
	h.Sum(key[:0])
	return key
}
func writeHistoryCostHintString(h hash.Hash, s string, number []byte) {
	binary.LittleEndian.PutUint64(number, uint64(len(s)))
	_, _ = h.Write(number)
	_, _ = h.Write([]byte(s))
}

func statHistoryCompactionInputs(ctx context.Context, dir string, refs historyCompactionInputRefs) ([3]os.FileInfo, error) {
	return statHistoryCompactionInputsWith(ctx, dir, refs, os.Stat)
}
func statHistoryCompactionInputsWith(ctx context.Context, dir string, refs historyCompactionInputRefs, stat func(string) (os.FileInfo, error)) ([3]os.FileInfo, error) {
	var files [3]os.FileInfo
	for i, ref := range refs {
		if err := contextError(ctx); err != nil {
			return files, err
		}
		info, err := stat(filepath.Join(dir, ref.Path))
		if err != nil {
			return files, err
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != ref.Size {
			return files, fmt.Errorf("snapshots: compaction input %q size/type changed: size %d, want %d", ref.Path, info.Size(), ref.Size)
		}
		files[i] = info
	}
	return files, contextError(ctx)
}
func sameHistoryCompactionFiles(a, b [3]os.FileInfo) bool {
	for i := range a {
		if a[i] == nil || b[i] == nil || !os.SameFile(a[i], b[i]) || a[i].Size() != b[i].Size() || a[i].Mode() != b[i].Mode() || !a[i].ModTime().Equal(b[i].ModTime()) {
			return false
		}
	}
	return true
}
func (c *historyCompactionCostCache) remove(key historyCompactionCostKey) bool {
	if _, ok := c.entries[key]; ok {
		delete(c.entries, key)
		c.bytes -= historyCostCacheEntryCharge
		return true
	}
	return false
}
func (c *historyCompactionCostCache) remember(key historyCompactionCostKey, cost historyCompactionInputCost) {
	c.remove(key)
	if c.maxEntries <= 0 || len(c.entries) >= c.maxEntries || historyCostCacheEntryCharge > c.maxBytes || c.bytes > c.maxBytes-historyCostCacheEntryCharge {
		c.stats.CapacityRejects++
		return
	}
	if c.entries == nil {
		c.entries = make(map[historyCompactionCostKey]historyCompactionInputCost)
	}
	c.entries[key] = cost
	c.bytes += historyCostCacheEntryCharge
}
func (c *historyCompactionCostCache) forgetRetired(candidates []historyCompactionCandidate) {
	for _, candidate := range candidates {
		if key, ok := historyCompactionCacheKey(candidate); ok && c.remove(key) {
			c.stats.EvictedRetired++
		}
	}
}

// A hit is only a budget hint and intentionally performs no file observation.
// Same-ref corruption/deletion of an unselected source may be reported only when
// it is selected (or after hint reset). Overestimates can defer it indefinitely.
func (c *historyCompactionCostCache) read(ctx context.Context, candidate historyCompactionCandidate, read func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error)) (historyCompactionInputCost, error) {
	if err := contextError(ctx); err != nil {
		return historyCompactionInputCost{}, err
	}
	key, ok := historyCompactionCacheKey(candidate)
	if !ok {
		c.stats.MetadataReads++
		return read(ctx, c.dir, candidate)
	}
	if ok {
		if cost, hit := c.entries[key]; hit {
			if err := contextError(ctx); err != nil {
				return historyCompactionInputCost{}, err
			}
			c.stats.Hits++
			return cost, nil
		}
		c.stats.Misses++
	}
	c.stats.MetadataReads++
	cost, err := read(ctx, c.dir, candidate)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	if err := contextError(ctx); err != nil {
		return historyCompactionInputCost{}, err
	}
	if ok {
		c.remember(key, cost)
	}
	return cost, nil
}

// Selected inputs keep their original before/reader/after identity checks. The
// mandatory reader's own path Stats, FD Stats and metadata checks are unchanged.
func (c *historyCompactionCostCache) readFresh(ctx context.Context, candidate historyCompactionCandidate, read func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error)) (historyCompactionInputCost, error) {
	key, cacheable := historyCompactionCacheKey(candidate)
	if !cacheable {
		c.stats.FreshReads++
		c.stats.MetadataReads++
		return read(ctx, c.dir, candidate)
	}
	c.remove(key)
	refs, _ := historyCompactionCacheRefs(candidate)
	before, err := statHistoryCompactionInputsWith(ctx, c.dir, refs, c.statFile)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	c.stats.FreshReads++
	c.stats.MetadataReads++
	cost, err := read(ctx, c.dir, candidate)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	after, err := statHistoryCompactionInputsWith(ctx, c.dir, refs, c.statFile)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	if !sameHistoryCompactionFiles(before, after) {
		return historyCompactionInputCost{}, errors.New("snapshots: compaction input changed while reading admission cost")
	}
	if err := contextError(ctx); err != nil {
		return historyCompactionInputCost{}, err
	}
	c.remember(key, cost)
	return cost, nil
}

// Re-evaluate every chosen group before claims, payload reads or publication.
func revalidateHistoryCompactionCosts(ctx context.Context, cache *historyCompactionCostCache, selection historyCompactionSelection, cfg CompactionConfig, previous map[SegmentRef]historyCompactionInputCost) (bool, error) {
	var total historyCompactionInputCost
	changed := false
	for _, candidate := range selection.candidates {
		cost, err := cache.readFresh(ctx, candidate, readHistoryCompactionInputCost)
		if err != nil {
			return false, err
		}
		changed = changed || cost != previous[candidate.history]
		if cost.bytes > ^uint64(0)-total.bytes || cost.logicalBytes > ^uint64(0)-total.logicalBytes || cost.records > ^uint64(0)-total.records {
			return false, errors.New("snapshots: compaction input cost overflows during revalidation")
		}
		total.bytes += cost.bytes
		total.logicalBytes += cost.logicalBytes
		total.records += cost.records
	}
	return !changed && historyCompactionWithinBudget(cfg, total.bytes, total.logicalBytes, total.records, uint64(len(selection.candidates))), nil
}
