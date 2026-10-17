package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	historyCostCacheEntries = 32768
	historyCostCacheBytes   = 64 << 20
)

// This admission-only cache belongs to one Runner, under its passMu. It never
// replaces source authentication. Its conservative retained-metadata charge
// bounds keys, file observations and map overhead, not process RSS.
type historyCompactionCostCache struct {
	dir        string
	directory  os.FileInfo
	epoch      uint64
	entries    map[historyCompactionCostKey]historyCompactionCachedCost
	bytes      uint64
	maxEntries int
	maxBytes   uint64
	// Private per-instance filesystem seam; production uses os.Stat. Tests count
	// the cache's actual extra Stats without changing process-global readers.
	stat func(string) (os.FileInfo, error)
}

// Values, rather than a borrowed companion slice, preserve the complete trio
// identity even when the caller reuses its manifest or candidate storage.
type historyCompactionCostKey [3]SegmentRef

type historyCompactionCachedCost struct {
	files  [3]os.FileInfo
	cost   historyCompactionInputCost
	charge uint64
}

func newHistoryCompactionCostCache() *historyCompactionCostCache {
	return &historyCompactionCostCache{maxEntries: historyCostCacheEntries, maxBytes: historyCostCacheBytes}
}

func (c *historyCompactionCostCache) clear() {
	c.entries = make(map[historyCompactionCostKey]historyCompactionCachedCost)
	c.bytes = 0
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

func historyCompactionCacheKey(candidate historyCompactionCandidate) (historyCompactionCostKey, bool) {
	var key historyCompactionCostKey
	if candidate.history.Dataset != SegmentDatasetStateDomainChange || len(candidate.companions) != 2 {
		return key, false
	}
	key[0] = candidate.history
	copy(key[1:], candidate.companions)
	accessor, inverted := false, false
	for _, ref := range key {
		if ref.Size == 0 || ref.Checksum == "" {
			return key, false
		}
		if ref.Kind == SegmentAccessor {
			accessor = true
		}
		if ref.Kind == SegmentInverted {
			inverted = true
		}
	}
	return key, accessor && inverted
}

func statHistoryCompactionInputs(ctx context.Context, dir string, key historyCompactionCostKey) ([3]os.FileInfo, error) {
	return statHistoryCompactionInputsWith(ctx, dir, key, os.Stat)
}

func (c *historyCompactionCostCache) statFile(path string) (os.FileInfo, error) {
	if c.stat != nil {
		return c.stat(path)
	}
	return os.Stat(path)
}

func statHistoryCompactionInputsWith(ctx context.Context, dir string, key historyCompactionCostKey, stat func(string) (os.FileInfo, error)) ([3]os.FileInfo, error) {
	var files [3]os.FileInfo
	for i, ref := range key {
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

func (c *historyCompactionCostCache) remove(key historyCompactionCostKey) {
	if entry, ok := c.entries[key]; ok {
		c.bytes -= entry.charge
		delete(c.entries, key)
	}
}

func (c *historyCompactionCostCache) remember(key historyCompactionCostKey, files [3]os.FileInfo, cost historyCompactionInputCost) {
	c.remove(key)
	charge, ok := historyCompactionCostCacheCharge(c.dir, key)
	if !ok {
		return
	}
	// Keep the stable historical prefix at capacity instead of flushing it on
	// each full catalog scan. The uncached suffix still takes the original path.
	if c.maxEntries <= 0 || len(c.entries) >= c.maxEntries || charge > c.maxBytes || c.bytes > c.maxBytes-charge {
		return
	}
	for i, ref := range key {
		ref.Dataset = SegmentDataset(strings.Clone(string(ref.Dataset)))
		ref.Kind = SegmentKind(strings.Clone(string(ref.Kind)))
		ref.Path = strings.Clone(ref.Path)
		ref.Checksum = strings.Clone(ref.Checksum)
		key[i] = ref
	}
	if c.entries == nil {
		c.entries = make(map[historyCompactionCostKey]historyCompactionCachedCost)
	}
	c.entries[key] = historyCompactionCachedCost{files: files, cost: cost, charge: charge}
	c.bytes += charge
}

// Fixed charge covers value/map/FileInfo observations. Dynamic ownership has
// 2x headroom for allocator size classes, including complete Stat path backing.
// The one cache-bound directory string is separately owned once per Runner;
// neither this accounting nor the volatile entry cap promises a process RSS cap.
func historyCompactionCostCacheCharge(dir string, key historyCompactionCostKey) (uint64, bool) {
	charge := uint64(2048)
	for _, ref := range key {
		for _, n := range []int{len(ref.Path), len(ref.Checksum), len(ref.Dataset), len(ref.Kind), len(dir), 1, len(ref.Path)} {
			if uint64(n) > (^uint64(0)-charge)/2 {
				return 0, false
			}
			charge += 2 * uint64(n)
		}
	}
	return charge, true
}

// Every lookup still Stats each file. Unknown immutable identities take the
// original reader. A hit skips only the compressed footer/accessor metadata.
func (c *historyCompactionCostCache) read(ctx context.Context, candidate historyCompactionCandidate, read func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error)) (historyCompactionInputCost, error) {
	if err := contextError(ctx); err != nil {
		return historyCompactionInputCost{}, err
	}
	key, cacheable := historyCompactionCacheKey(candidate)
	if !cacheable {
		return read(ctx, c.dir, candidate)
	}
	files, err := statHistoryCompactionInputsWith(ctx, c.dir, key, c.statFile)
	if err != nil {
		c.remove(key)
		return historyCompactionInputCost{}, err
	}
	if entry, ok := c.entries[key]; ok && sameHistoryCompactionFiles(entry.files, files) {
		if err := contextError(ctx); err != nil {
			return historyCompactionInputCost{}, err
		}
		return entry.cost, nil
	}
	c.remove(key)
	return c.readObserved(ctx, candidate, key, files, read)
}

// Selected inputs always take this path, including cache hits. Only a stable,
// successful observation may be retained; errors and cancellation are not cached.
func (c *historyCompactionCostCache) readFresh(ctx context.Context, candidate historyCompactionCandidate, read func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error)) (historyCompactionInputCost, error) {
	key, cacheable := historyCompactionCacheKey(candidate)
	if !cacheable {
		return read(ctx, c.dir, candidate)
	}
	c.remove(key)
	before, err := statHistoryCompactionInputsWith(ctx, c.dir, key, c.statFile)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	return c.readObserved(ctx, candidate, key, before, read)
}

func (c *historyCompactionCostCache) readObserved(ctx context.Context, candidate historyCompactionCandidate, key historyCompactionCostKey, before [3]os.FileInfo, read func(context.Context, string, historyCompactionCandidate) (historyCompactionInputCost, error)) (historyCompactionInputCost, error) {
	cost, err := read(ctx, c.dir, candidate)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	after, err := statHistoryCompactionInputsWith(ctx, c.dir, key, c.statFile)
	if err != nil {
		return historyCompactionInputCost{}, err
	}
	if !sameHistoryCompactionFiles(before, after) {
		return historyCompactionInputCost{}, errors.New("snapshots: compaction input changed while reading admission cost")
	}
	if err := contextError(ctx); err != nil {
		return historyCompactionInputCost{}, err
	}
	c.remember(key, after, cost)
	return cost, nil
}

// Re-evaluate the chosen group before claims, payload reads or publication.
// A changed cost retries selection next pass, with the updated observations;
// it cannot silently compact a group chosen using obsolete admission costs.
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
