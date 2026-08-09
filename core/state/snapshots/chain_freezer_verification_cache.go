package snapshots

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	chainFreezerVerificationCacheVersion    = 1
	chainFreezerVerificationCacheFile       = ".chain-freezer-verification-v1.json"
	maxChainFreezerVerificationCacheBytes   = 64 << 20
	maxChainFreezerVerificationCacheEntries = 1_000_000
	chainFreezerVerificationMetricsPrefix   = "state/snapshot/chain_freezer/verification/"
)

type chainFreezerFileIdentity struct {
	size        int64
	modUnixNano int64
}

type chainFreezerVerificationKey struct {
	freezer      SegmentRef
	index        SegmentRef
	accessor     SegmentRef
	hasAccessor  bool
	freezerFile  chainFreezerFileIdentity
	indexFile    chainFreezerFileIdentity
	accessorFile chainFreezerFileIdentity
}

// chainFreezerVerificationRecord deliberately omits timestamps. A durable
// proof may replace semantic replay only after every exact content-addressed
// object has been re-hashed following restart.
type chainFreezerVerificationRecord struct {
	Freezer     SegmentRef `json:"freezer"`
	Index       SegmentRef `json:"index"`
	Accessor    SegmentRef `json:"accessor,omitempty"`
	HasAccessor bool       `json:"hasAccessor,omitempty"`
}

// eventLogVerificationRecord binds one event-log lookup sidecar to the exact
// ordered source segments from which its address/topic postings were proven.
type eventLogVerificationRecord struct {
	Index  SegmentRef   `json:"index"`
	Events []SegmentRef `json:"events"`
}

type eventLogVerificationKey struct {
	recordKey   string
	identityKey string
}

type chainFreezerVerificationDisk struct {
	Version      uint32                           `json:"version"`
	Entries      []chainFreezerVerificationRecord `json:"entries"`
	EventEntries []eventLogVerificationRecord     `json:"eventEntries,omitempty"`
}

type ChainFreezerVerificationCacheStats struct {
	MemoryHits           uint64
	PersistentHits       uint64
	FullVerified         uint64
	Entries              uint64
	LoadErrors           uint64
	EventMemoryHits      uint64
	EventPersistentHits  uint64
	EventFullVerified    uint64
	EventEntries         uint64
	TrustedRecorded      uint64
	EventTrustedRecorded uint64
}

type chainFreezerVerificationRoute uint8

const (
	chainFreezerVerificationFull chainFreezerVerificationRoute = iota + 1
	chainFreezerVerificationMemory
	chainFreezerVerificationPersistent
)

type chainFreezerVerificationMetrics struct {
	memoryHits           *metrics.Gauge
	persistentHits       *metrics.Gauge
	fullVerified         *metrics.Gauge
	entries              *metrics.Gauge
	loadErrors           *metrics.Gauge
	eventMemoryHits      *metrics.Gauge
	eventPersistentHits  *metrics.Gauge
	eventFullVerified    *metrics.Gauge
	eventEntries         *metrics.Gauge
	trustedRecorded      *metrics.Gauge
	eventTrustedRecorded *metrics.Gauge
}

// ChainFreezerVerificationCache reuses semantic proofs for exact immutable
// freezer/index/accessor triples and event-log/index companion sets. A
// same-process hit also requires unchanged size and mtime. A restart hit
// re-hashes every object before it may replace the complete protobuf/lookup
// replay.
type ChainFreezerVerificationCache struct {
	mu              sync.Mutex
	dir             string
	verified        map[chainFreezerVerificationKey]struct{}
	persistent      map[chainFreezerVerificationRecord]struct{}
	eventVerified   map[eventLogVerificationKey]struct{}
	eventPersistent map[string]eventLogVerificationRecord
	dirty           bool
	loadErr         error
	stats           ChainFreezerVerificationCacheStats
	metrics         chainFreezerVerificationMetrics
}

func NewChainFreezerVerificationCache(dir string) *ChainFreezerVerificationCache {
	c := &ChainFreezerVerificationCache{
		dir:             strings.TrimSpace(dir),
		verified:        make(map[chainFreezerVerificationKey]struct{}),
		persistent:      make(map[chainFreezerVerificationRecord]struct{}),
		eventVerified:   make(map[eventLogVerificationKey]struct{}),
		eventPersistent: make(map[string]eventLogVerificationRecord),
		metrics: chainFreezerVerificationMetrics{
			memoryHits:           metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"memory_hits", nil),
			persistentHits:       metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"persistent_hits", nil),
			fullVerified:         metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"full", nil),
			entries:              metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"cache_entries", nil),
			loadErrors:           metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"load_errors", nil),
			eventMemoryHits:      metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"event_log/memory_hits", nil),
			eventPersistentHits:  metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"event_log/persistent_hits", nil),
			eventFullVerified:    metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"event_log/full", nil),
			eventEntries:         metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"event_log/cache_entries", nil),
			trustedRecorded:      metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"trusted_recorded", nil),
			eventTrustedRecorded: metrics.GetOrRegisterGauge(chainFreezerVerificationMetricsPrefix+"event_log/trusted_recorded", nil),
		},
	}
	if c.dir != "" {
		if c.loadErr = c.load(); c.loadErr != nil {
			// The cache is advisory. A malformed or partial file falls back to
			// exhaustive verification and is replaced only after a complete pass.
			c.persistent = make(map[chainFreezerVerificationRecord]struct{})
			c.eventPersistent = make(map[string]eventLogVerificationRecord)
			c.dirty = true
			c.stats.LoadErrors++
		}
	}
	c.updateMetricsLocked()
	return c
}

func (c *ChainFreezerVerificationCache) LoadError() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loadErr
}

func (c *ChainFreezerVerificationCache) Stats() ChainFreezerVerificationCacheStats {
	if c == nil {
		return ChainFreezerVerificationCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	stats.Entries = uint64(len(c.persistent))
	stats.EventEntries = uint64(len(c.eventPersistent))
	return stats
}

func (c *ChainFreezerVerificationCache) verify(dir string, freezerRef, indexRef, accessorRef SegmentRef, hasAccessor bool) (chainFreezerVerificationKey, chainFreezerVerificationRoute, error) {
	if c == nil {
		if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir, freezerRef, indexRef, accessorRef, hasAccessor); err != nil {
			return chainFreezerVerificationKey{}, 0, err
		}
		return chainFreezerVerificationKey{}, chainFreezerVerificationFull, nil
	}
	record := normalizeChainFreezerVerificationRecord(chainFreezerVerificationRecord{
		Freezer:     freezerRef,
		Index:       indexRef,
		Accessor:    accessorRef,
		HasAccessor: hasAccessor,
	})
	if err := validateChainFreezerVerificationRecord(record); err != nil {
		// Legacy local manifests may omit strong file metadata. Preserve their
		// exhaustive verification behavior, but never persist an identity that
		// cannot be re-authenticated after restart.
		if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir, freezerRef, indexRef, accessorRef, hasAccessor); err != nil {
			return chainFreezerVerificationKey{}, 0, err
		}
		c.mu.Lock()
		c.stats.FullVerified++
		c.mu.Unlock()
		return chainFreezerVerificationKey{}, chainFreezerVerificationFull, nil
	}
	key, err := chainFreezerVerificationKeyFor(dir, freezerRef, indexRef, accessorRef, hasAccessor)
	if err != nil {
		return chainFreezerVerificationKey{}, 0, err
	}

	c.mu.Lock()
	_, memoryHit := c.verified[key]
	if memoryHit {
		c.stats.MemoryHits++
		c.mu.Unlock()
		return key, chainFreezerVerificationMemory, nil
	}
	c.mu.Unlock()

	record = chainFreezerVerificationRecordFor(key)
	c.mu.Lock()
	_, persistentHit := c.persistent[record]
	c.mu.Unlock()
	if persistentHit {
		if err := verifyChainFreezerCompanionChecksums(dir, freezerRef, indexRef, accessorRef, hasAccessor); err != nil {
			return chainFreezerVerificationKey{}, 0, err
		}
	} else if err := verifyChainFreezerSnapshotCompanionsSinglePass(dir, freezerRef, indexRef, accessorRef, hasAccessor); err != nil {
		return chainFreezerVerificationKey{}, 0, err
	}
	got, err := chainFreezerVerificationKeyFor(dir, freezerRef, indexRef, accessorRef, hasAccessor)
	if err != nil {
		return chainFreezerVerificationKey{}, 0, err
	}
	if got != key {
		return chainFreezerVerificationKey{}, 0, fmt.Errorf("snapshots: chain-freezer companion triple %q changed while being verified", freezerRef.Path)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.verified[key] = struct{}{}
	if persistentHit {
		c.stats.PersistentHits++
		return key, chainFreezerVerificationPersistent, nil
	}
	if len(c.persistent) >= maxChainFreezerVerificationCacheEntries {
		return chainFreezerVerificationKey{}, 0, fmt.Errorf("snapshots: chain-freezer verification cache entries exceed limit %d", maxChainFreezerVerificationCacheEntries)
	}
	c.persistent[record] = struct{}{}
	c.dirty = true
	c.stats.FullVerified++
	return key, chainFreezerVerificationFull, nil
}

// commit persists all newly proven records once after a complete contiguous
// prefix. Batching avoids one JSON rewrite and directory fsync per segment on
// the first run over a large mainnet manifest.
func (c *ChainFreezerVerificationCache) commit(active map[chainFreezerVerificationKey]struct{}) error {
	if c == nil {
		return nil
	}
	activeRecords := make(map[chainFreezerVerificationRecord]struct{}, len(active))
	for key := range active {
		activeRecords[chainFreezerVerificationRecordFor(key)] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.verified {
		if _, ok := active[key]; !ok {
			delete(c.verified, key)
		}
	}
	for record := range c.persistent {
		if _, ok := activeRecords[record]; !ok {
			delete(c.persistent, record)
			c.dirty = true
		}
	}
	if !c.dirty {
		c.updateMetricsLocked()
		return nil
	}
	if err := c.persistLocked(); err != nil {
		return err
	}
	c.dirty = false
	c.updateMetricsLocked()
	return nil
}

func (c *ChainFreezerVerificationCache) verifyEventLogIndex(dir string, indexRef SegmentRef, eventRefs []SegmentRef) (eventLogVerificationKey, chainFreezerVerificationRoute, error) {
	if c == nil {
		if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, eventRefs); err != nil {
			return eventLogVerificationKey{}, 0, err
		}
		return eventLogVerificationKey{}, chainFreezerVerificationFull, nil
	}
	record, err := eventLogVerificationRecordFor(indexRef, eventRefs)
	if err != nil {
		return eventLogVerificationKey{}, 0, err
	}
	if err := validateEventLogVerificationRecord(record); err != nil {
		// As with chain-freezer triples, legacy refs without strong metadata
		// remain fully verified but are intentionally not persisted.
		if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, eventRefs); err != nil {
			return eventLogVerificationKey{}, 0, err
		}
		c.mu.Lock()
		c.stats.EventFullVerified++
		c.mu.Unlock()
		return eventLogVerificationKey{}, chainFreezerVerificationFull, nil
	}
	key, err := eventLogVerificationKeyFor(dir, record)
	if err != nil {
		return eventLogVerificationKey{}, 0, err
	}
	c.mu.Lock()
	_, memoryHit := c.eventVerified[key]
	if memoryHit {
		c.stats.EventMemoryHits++
		c.mu.Unlock()
		return key, chainFreezerVerificationMemory, nil
	}
	c.mu.Unlock()

	c.mu.Lock()
	_, persistentHit := c.eventPersistent[key.recordKey]
	c.mu.Unlock()
	if persistentHit {
		if err := verifyEventLogCompanionChecksums(dir, record); err != nil {
			return eventLogVerificationKey{}, 0, err
		}
	} else if err := verifyEventLogIndexSegmentAgainstEventLogs(dir, indexRef, eventRefs); err != nil {
		return eventLogVerificationKey{}, 0, err
	}
	got, err := eventLogVerificationKeyFor(dir, record)
	if err != nil {
		return eventLogVerificationKey{}, 0, err
	}
	if got != key {
		return eventLogVerificationKey{}, 0, fmt.Errorf("snapshots: event-log companion set %q changed while being verified", indexRef.Path)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventVerified[key] = struct{}{}
	if persistentHit {
		c.stats.EventPersistentHits++
		return key, chainFreezerVerificationPersistent, nil
	}
	if len(c.eventPersistent) >= maxChainFreezerVerificationCacheEntries {
		return eventLogVerificationKey{}, 0, fmt.Errorf("snapshots: event-log verification cache entries exceed limit %d", maxChainFreezerVerificationCacheEntries)
	}
	c.eventPersistent[key.recordKey] = record
	c.dirty = true
	c.stats.EventFullVerified++
	return key, chainFreezerVerificationFull, nil
}

func (c *ChainFreezerVerificationCache) commitEventLogs(active map[eventLogVerificationKey]struct{}) error {
	if c == nil {
		return nil
	}
	activeRecords := make(map[string]struct{}, len(active))
	for key := range active {
		activeRecords[key.recordKey] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.eventVerified {
		if _, ok := active[key]; !ok {
			delete(c.eventVerified, key)
		}
	}
	for recordKey := range c.eventPersistent {
		if _, ok := activeRecords[recordKey]; !ok {
			delete(c.eventPersistent, recordKey)
			c.dirty = true
		}
	}
	if !c.dirty {
		c.updateMetricsLocked()
		return nil
	}
	if err := c.persistLocked(); err != nil {
		return err
	}
	c.dirty = false
	c.updateMetricsLocked()
	return nil
}

func (c *ChainFreezerVerificationCache) recordTrustedChain(dir string, freezerRef, indexRef, accessorRef SegmentRef, hasAccessor bool) error {
	if c == nil {
		return nil
	}
	key, err := chainFreezerVerificationKeyFor(dir, freezerRef, indexRef, accessorRef, hasAccessor)
	if err != nil {
		return err
	}
	record := chainFreezerVerificationRecordFor(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.verified[key] = struct{}{}
	if _, ok := c.persistent[record]; !ok {
		if len(c.persistent) >= maxChainFreezerVerificationCacheEntries {
			return fmt.Errorf("snapshots: chain-freezer verification cache entries exceed limit %d", maxChainFreezerVerificationCacheEntries)
		}
		c.persistent[record] = struct{}{}
		c.dirty = true
	}
	c.stats.TrustedRecorded++
	return c.persistPendingLocked()
}

func (c *ChainFreezerVerificationCache) recordTrustedEventLogs(dir string, indexRef SegmentRef, eventRefs []SegmentRef) error {
	if c == nil {
		return nil
	}
	record, err := eventLogVerificationRecordFor(indexRef, eventRefs)
	if err != nil {
		return err
	}
	key, err := eventLogVerificationKeyFor(dir, record)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventVerified[key] = struct{}{}
	if _, ok := c.eventPersistent[key.recordKey]; !ok {
		if len(c.eventPersistent) >= maxChainFreezerVerificationCacheEntries {
			return fmt.Errorf("snapshots: event-log verification cache entries exceed limit %d", maxChainFreezerVerificationCacheEntries)
		}
		c.eventPersistent[key.recordKey] = record
		c.dirty = true
	}
	c.stats.EventTrustedRecorded++
	return c.persistPendingLocked()
}

func (c *ChainFreezerVerificationCache) persistPendingLocked() error {
	if c.dirty {
		if err := c.persistLocked(); err != nil {
			return err
		}
		c.dirty = false
	}
	c.updateMetricsLocked()
	return nil
}

func (c *ChainFreezerVerificationCache) load() error {
	path := filepath.Join(c.dir, chainFreezerVerificationCacheFile)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Size() < 0 || info.Size() > maxChainFreezerVerificationCacheBytes {
		return fmt.Errorf("snapshots: chain-freezer verification cache size %d exceeds limit %d", info.Size(), maxChainFreezerVerificationCacheBytes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var disk chainFreezerVerificationDisk
	if err := decoder.Decode(&disk); err != nil {
		return fmt.Errorf("snapshots: decode chain-freezer verification cache: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("snapshots: chain-freezer verification cache has trailing JSON")
	}
	if disk.Version != chainFreezerVerificationCacheVersion {
		return fmt.Errorf("snapshots: unsupported chain-freezer verification cache version %d", disk.Version)
	}
	if len(disk.Entries) > maxChainFreezerVerificationCacheEntries {
		return fmt.Errorf("snapshots: chain-freezer verification cache entries %d exceeds limit %d", len(disk.Entries), maxChainFreezerVerificationCacheEntries)
	}
	for _, record := range disk.Entries {
		record = normalizeChainFreezerVerificationRecord(record)
		if err := validateChainFreezerVerificationRecord(record); err != nil {
			return fmt.Errorf("snapshots: invalid chain-freezer verification cache entry: %w", err)
		}
		c.persistent[record] = struct{}{}
	}
	if len(disk.EventEntries) > maxChainFreezerVerificationCacheEntries {
		return fmt.Errorf("snapshots: event-log verification cache entries %d exceeds limit %d", len(disk.EventEntries), maxChainFreezerVerificationCacheEntries)
	}
	for _, record := range disk.EventEntries {
		record = normalizeEventLogVerificationRecord(record)
		if err := validateEventLogVerificationRecord(record); err != nil {
			return fmt.Errorf("snapshots: invalid event-log verification cache entry: %w", err)
		}
		key, err := eventLogVerificationRecordKey(record)
		if err != nil {
			return err
		}
		c.eventPersistent[key] = record
	}
	return nil
}

func (c *ChainFreezerVerificationCache) persistLocked() error {
	if c.dir == "" {
		return nil
	}
	entries := make([]chainFreezerVerificationRecord, 0, len(c.persistent))
	for record := range c.persistent {
		entries = append(entries, record)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Freezer.Path != entries[j].Freezer.Path {
			return entries[i].Freezer.Path < entries[j].Freezer.Path
		}
		if entries[i].Index.Path != entries[j].Index.Path {
			return entries[i].Index.Path < entries[j].Index.Path
		}
		return entries[i].Accessor.Path < entries[j].Accessor.Path
	})
	eventEntries := make([]eventLogVerificationRecord, 0, len(c.eventPersistent))
	for _, record := range c.eventPersistent {
		eventEntries = append(eventEntries, record)
	}
	sort.Slice(eventEntries, func(i, j int) bool {
		return eventEntries[i].Index.Path < eventEntries[j].Index.Path
	})
	data, err := json.MarshalIndent(chainFreezerVerificationDisk{
		Version:      chainFreezerVerificationCacheVersion,
		Entries:      entries,
		EventEntries: eventEntries,
	}, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > maxChainFreezerVerificationCacheBytes {
		return fmt.Errorf("snapshots: encoded chain-freezer verification cache size %d exceeds limit %d", len(data), maxChainFreezerVerificationCacheBytes)
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.dir, ".chain-freezer-verification-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(c.dir, chainFreezerVerificationCacheFile)); err != nil {
		return err
	}
	return syncSnapshotDir(c.dir)
}

func (c *ChainFreezerVerificationCache) updateMetricsLocked() {
	if c == nil {
		return
	}
	c.metrics.memoryHits.Update(coldSnapshotUintGauge(c.stats.MemoryHits))
	c.metrics.persistentHits.Update(coldSnapshotUintGauge(c.stats.PersistentHits))
	c.metrics.fullVerified.Update(coldSnapshotUintGauge(c.stats.FullVerified))
	c.metrics.entries.Update(coldSnapshotUintGauge(uint64(len(c.persistent))))
	c.metrics.loadErrors.Update(coldSnapshotUintGauge(c.stats.LoadErrors))
	c.metrics.eventMemoryHits.Update(coldSnapshotUintGauge(c.stats.EventMemoryHits))
	c.metrics.eventPersistentHits.Update(coldSnapshotUintGauge(c.stats.EventPersistentHits))
	c.metrics.eventFullVerified.Update(coldSnapshotUintGauge(c.stats.EventFullVerified))
	c.metrics.eventEntries.Update(coldSnapshotUintGauge(uint64(len(c.eventPersistent))))
	c.metrics.trustedRecorded.Update(coldSnapshotUintGauge(c.stats.TrustedRecorded))
	c.metrics.eventTrustedRecorded.Update(coldSnapshotUintGauge(c.stats.EventTrustedRecorded))
}

func chainFreezerVerificationRecordFor(key chainFreezerVerificationKey) chainFreezerVerificationRecord {
	return normalizeChainFreezerVerificationRecord(chainFreezerVerificationRecord{
		Freezer:     key.freezer,
		Index:       key.index,
		Accessor:    key.accessor,
		HasAccessor: key.hasAccessor,
	})
}

func normalizeChainFreezerVerificationRecord(record chainFreezerVerificationRecord) chainFreezerVerificationRecord {
	record.Freezer.Checksum = strings.ToLower(strings.TrimSpace(record.Freezer.Checksum))
	record.Index.Checksum = strings.ToLower(strings.TrimSpace(record.Index.Checksum))
	record.Accessor.Checksum = strings.ToLower(strings.TrimSpace(record.Accessor.Checksum))
	return record
}

func validateChainFreezerVerificationRecord(record chainFreezerVerificationRecord) error {
	refs := []struct {
		name string
		ref  SegmentRef
		kind SegmentKind
	}{
		{name: "freezer", ref: record.Freezer, kind: SegmentChainFreezer},
		{name: "index", ref: record.Index, kind: SegmentChainIndex},
	}
	if record.HasAccessor {
		refs = append(refs, struct {
			name string
			ref  SegmentRef
			kind SegmentKind
		}{name: "accessor", ref: record.Accessor, kind: SegmentChainFreezerAccessor})
	} else if record.Accessor != (SegmentRef{}) {
		return errors.New("accessor ref is set while hasAccessor is false")
	}
	for _, item := range refs {
		if err := validateSegmentRef(item.ref); err != nil {
			return fmt.Errorf("%s: %w", item.name, err)
		}
		if item.ref.Kind != item.kind || item.ref.NormalizedDataset() != SegmentDatasetChainFreezer {
			return fmt.Errorf("%s %q is %s/%s", item.name, item.ref.Path, item.ref.NormalizedDataset(), item.ref.Kind)
		}
		if item.ref.FromTxNum != record.Freezer.FromTxNum || item.ref.ToTxNum != record.Freezer.ToTxNum || item.ref.Domain != record.Freezer.Domain {
			return fmt.Errorf("%s %q does not match freezer range/domain", item.name, item.ref.Path)
		}
		if item.ref.Size == 0 {
			return fmt.Errorf("%s %q has no size", item.name, item.ref.Path)
		}
		if _, err := latestBinaryChecksumBytes(item.ref.Checksum); err != nil {
			return fmt.Errorf("%s %q has invalid checksum: %w", item.name, item.ref.Path, err)
		}
	}
	return nil
}

func chainFreezerVerificationKeyFor(dir string, freezerRef, indexRef, accessorRef SegmentRef, hasAccessor bool) (chainFreezerVerificationKey, error) {
	record := normalizeChainFreezerVerificationRecord(chainFreezerVerificationRecord{
		Freezer:     freezerRef,
		Index:       indexRef,
		Accessor:    accessorRef,
		HasAccessor: hasAccessor,
	})
	if err := validateChainFreezerVerificationRecord(record); err != nil {
		return chainFreezerVerificationKey{}, err
	}
	key := chainFreezerVerificationKey{
		freezer:     record.Freezer,
		index:       record.Index,
		accessor:    record.Accessor,
		hasAccessor: record.HasAccessor,
	}
	var err error
	if key.freezerFile, err = chainFreezerVerificationFileIdentity(dir, key.freezer); err != nil {
		return chainFreezerVerificationKey{}, err
	}
	if key.indexFile, err = chainFreezerVerificationFileIdentity(dir, key.index); err != nil {
		return chainFreezerVerificationKey{}, err
	}
	if hasAccessor {
		if key.accessorFile, err = chainFreezerVerificationFileIdentity(dir, key.accessor); err != nil {
			return chainFreezerVerificationKey{}, err
		}
	}
	return key, nil
}

func chainFreezerVerificationFileIdentity(dir string, ref SegmentRef) (chainFreezerFileIdentity, error) {
	info, err := os.Stat(filepath.Join(dir, ref.Path))
	if err != nil {
		return chainFreezerFileIdentity{}, err
	}
	if info.IsDir() {
		return chainFreezerFileIdentity{}, fmt.Errorf("snapshots: segment %q is a directory", ref.Path)
	}
	if uint64(info.Size()) != ref.Size {
		return chainFreezerFileIdentity{}, fmt.Errorf("snapshots: segment %q size %d, want %d", ref.Path, info.Size(), ref.Size)
	}
	return chainFreezerFileIdentity{size: info.Size(), modUnixNano: info.ModTime().UnixNano()}, nil
}

func verifyChainFreezerCompanionChecksums(dir string, freezerRef, indexRef, accessorRef SegmentRef, hasAccessor bool) error {
	for _, ref := range []SegmentRef{freezerRef, indexRef} {
		if err := checkSegmentFileMetadata(dir, ref, true); err != nil {
			return err
		}
	}
	if hasAccessor {
		return checkSegmentFileMetadata(dir, accessorRef, true)
	}
	return nil
}

func eventLogVerificationRecordFor(indexRef SegmentRef, eventRefs []SegmentRef) (eventLogVerificationRecord, error) {
	sortedRefs := append([]SegmentRef(nil), eventRefs...)
	sortSegments(sortedRefs)
	if !eventLogRangeCoveredByRefs(sortedRefs, indexRef.FromTxNum, indexRef.ToTxNum) {
		return eventLogVerificationRecord{}, fmt.Errorf("snapshots: event-log-index segment %q has no continuous event-log coverage for block range [%d,%d]",
			indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
	}
	record := eventLogVerificationRecord{Index: indexRef}
	for _, ref := range sortedRefs {
		if ref.ToTxNum < indexRef.FromTxNum || ref.FromTxNum > indexRef.ToTxNum {
			continue
		}
		if ref.FromTxNum < indexRef.FromTxNum || ref.ToTxNum > indexRef.ToTxNum {
			return eventLogVerificationRecord{}, fmt.Errorf("snapshots: event-log segment %q range [%d,%d] crosses event-log-index %q range [%d,%d]",
				ref.Path, ref.FromTxNum, ref.ToTxNum, indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
		}
		record.Events = append(record.Events, ref)
	}
	return normalizeEventLogVerificationRecord(record), nil
}

func normalizeEventLogVerificationRecord(record eventLogVerificationRecord) eventLogVerificationRecord {
	record.Index.Checksum = strings.ToLower(strings.TrimSpace(record.Index.Checksum))
	record.Events = append([]SegmentRef(nil), record.Events...)
	for i := range record.Events {
		record.Events[i].Checksum = strings.ToLower(strings.TrimSpace(record.Events[i].Checksum))
	}
	sortSegments(record.Events)
	return record
}

func validateEventLogVerificationRecord(record eventLogVerificationRecord) error {
	if err := validateEventLogIndexRef(record.Index); err != nil {
		return err
	}
	if record.Index.Size == 0 {
		return fmt.Errorf("event-log index %q has no size", record.Index.Path)
	}
	if _, err := latestBinaryChecksumBytes(record.Index.Checksum); err != nil {
		return fmt.Errorf("event-log index %q has invalid checksum: %w", record.Index.Path, err)
	}
	if len(record.Events) == 0 {
		return fmt.Errorf("event-log index %q has no source segments", record.Index.Path)
	}
	for _, ref := range record.Events {
		if err := validateEventLogRef(ref); err != nil {
			return err
		}
		if ref.FromTxNum < record.Index.FromTxNum || ref.ToTxNum > record.Index.ToTxNum {
			return fmt.Errorf("event-log segment %q lies outside index %q", ref.Path, record.Index.Path)
		}
		if ref.Size == 0 {
			return fmt.Errorf("event-log segment %q has no size", ref.Path)
		}
		if _, err := latestBinaryChecksumBytes(ref.Checksum); err != nil {
			return fmt.Errorf("event-log segment %q has invalid checksum: %w", ref.Path, err)
		}
	}
	if !eventLogRangeCoveredByRefs(record.Events, record.Index.FromTxNum, record.Index.ToTxNum) {
		return fmt.Errorf("event-log index %q source coverage is incomplete", record.Index.Path)
	}
	return nil
}

func eventLogVerificationRecordKey(record eventLogVerificationRecord) (string, error) {
	data, err := json.Marshal(normalizeEventLogVerificationRecord(record))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func eventLogVerificationKeyFor(dir string, record eventLogVerificationRecord) (eventLogVerificationKey, error) {
	record = normalizeEventLogVerificationRecord(record)
	if err := validateEventLogVerificationRecord(record); err != nil {
		return eventLogVerificationKey{}, err
	}
	recordKey, err := eventLogVerificationRecordKey(record)
	if err != nil {
		return eventLogVerificationKey{}, err
	}
	var identity strings.Builder
	refs := make([]SegmentRef, 0, len(record.Events)+1)
	refs = append(refs, record.Index)
	refs = append(refs, record.Events...)
	for _, ref := range refs {
		file, err := chainFreezerVerificationFileIdentity(dir, ref)
		if err != nil {
			return eventLogVerificationKey{}, err
		}
		_, _ = fmt.Fprintf(&identity, "%d:%d;", file.size, file.modUnixNano)
	}
	return eventLogVerificationKey{recordKey: recordKey, identityKey: identity.String()}, nil
}

func verifyEventLogCompanionChecksums(dir string, record eventLogVerificationRecord) error {
	if err := checkSegmentFileMetadata(dir, record.Index, true); err != nil {
		return err
	}
	for _, ref := range record.Events {
		if err := checkSegmentFileMetadata(dir, ref, true); err != nil {
			return err
		}
	}
	return nil
}
