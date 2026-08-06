package pruning

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

const (
	snapshotCoverageVerificationCacheVersion = 1
	snapshotCoverageVerificationCacheFile    = ".state-domain-history-verification-v1.json"
	maxSnapshotVerificationCacheBytes        = 64 << 20
	maxSnapshotVerificationCacheEntries      = 1_000_000
)

type snapshotFileIdentity struct {
	size        int64
	modUnixNano int64
}

type snapshotHistoryVerificationKey struct {
	history      snapshots.SegmentRef
	index        snapshots.SegmentRef
	accessor     snapshots.SegmentRef
	historyFile  snapshotFileIdentity
	indexFile    snapshotFileIdentity
	accessorFile snapshotFileIdentity
}

// snapshotHistoryVerificationRecord is the content-addressed part of an
// in-process verification key. File timestamps deliberately stay out of the
// durable format: after restart every durable hit must re-hash the immutable
// objects before it may stand in for the previous semantic audit.
type snapshotHistoryVerificationRecord struct {
	History  snapshots.SegmentRef `json:"history"`
	Index    snapshots.SegmentRef `json:"index"`
	Accessor snapshots.SegmentRef `json:"accessor"`
}

type snapshotCoverageVerificationDisk struct {
	Version uint32                              `json:"version"`
	Entries []snapshotHistoryVerificationRecord `json:"entries"`
}

type snapshotCoverageVerificationCacheStats struct {
	MemoryHits      uint64
	PersistentHits  uint64
	FullVerified    uint64
	TrustedRecorded uint64
	Entries         uint64
}

type snapshotCoverageVerificationCache struct {
	mu         sync.Mutex
	dir        string
	verified   map[snapshotHistoryVerificationKey]struct{}
	persistent map[snapshotHistoryVerificationRecord]struct{}
	loadErr    error
	stats      snapshotCoverageVerificationCacheStats
}

func newSnapshotCoverageVerificationCache(dir string) *snapshotCoverageVerificationCache {
	cache := &snapshotCoverageVerificationCache{
		dir:        strings.TrimSpace(dir),
		verified:   make(map[snapshotHistoryVerificationKey]struct{}),
		persistent: make(map[snapshotHistoryVerificationRecord]struct{}),
	}
	if cache.dir != "" {
		if cache.loadErr = cache.load(); cache.loadErr != nil {
			// A partially decoded file is no more trustworthy than a wholly
			// malformed one. Fall back to exhaustive verification for every ref.
			cache.persistent = make(map[snapshotHistoryVerificationRecord]struct{})
		}
	}
	return cache
}

func (c *snapshotCoverageVerificationCache) LoadError() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loadErr
}

func (c *snapshotCoverageVerificationCache) contains(key snapshotHistoryVerificationKey) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.verified[key]
	if ok {
		c.stats.MemoryHits++
	}
	return ok
}

func (c *snapshotCoverageVerificationCache) persistentCandidate(key snapshotHistoryVerificationKey) bool {
	if c == nil {
		return false
	}
	record := snapshotHistoryVerificationRecordFor(key)
	c.mu.Lock()
	_, ok := c.persistent[record]
	c.mu.Unlock()
	return ok
}

func (c *snapshotCoverageVerificationCache) promotePersistent(key snapshotHistoryVerificationKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.verified[key] = struct{}{}
	c.stats.PersistentHits++
	c.mu.Unlock()
}

func (c *snapshotCoverageVerificationCache) addFull(key snapshotHistoryVerificationKey) error {
	return c.add(key, false)
}

func (c *snapshotCoverageVerificationCache) addTrusted(key snapshotHistoryVerificationKey) error {
	return c.add(key, true)
}

func (c *snapshotCoverageVerificationCache) add(key snapshotHistoryVerificationKey, trusted bool) error {
	if c == nil {
		return nil
	}
	record := snapshotHistoryVerificationRecordFor(key)
	if err := validateSnapshotHistoryVerificationRecord(record); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.verified[key] = struct{}{}
	if trusted {
		c.stats.TrustedRecorded++
	} else {
		c.stats.FullVerified++
	}
	if _, ok := c.persistent[record]; ok {
		return nil
	}
	c.persistent[record] = struct{}{}
	if err := c.persistLocked(); err != nil {
		delete(c.persistent, record)
		return err
	}
	return nil
}

func (c *snapshotCoverageVerificationCache) retain(active map[snapshotHistoryVerificationKey]struct{}) error {
	if c == nil {
		return nil
	}
	activeRecords := make(map[snapshotHistoryVerificationRecord]struct{}, len(active))
	for key := range active {
		activeRecords[snapshotHistoryVerificationRecordFor(key)] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.verified {
		if _, ok := active[key]; !ok {
			delete(c.verified, key)
		}
	}
	dirty := false
	for record := range c.persistent {
		if _, ok := activeRecords[record]; !ok {
			delete(c.persistent, record)
			dirty = true
		}
	}
	if !dirty {
		return nil
	}
	return c.persistLocked()
}

func (c *snapshotCoverageVerificationCache) Stats() snapshotCoverageVerificationCacheStats {
	if c == nil {
		return snapshotCoverageVerificationCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	stats.Entries = uint64(len(c.persistent))
	return stats
}

func (c *snapshotCoverageVerificationCache) load() error {
	path := filepath.Join(c.dir, snapshotCoverageVerificationCacheFile)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Size() < 0 || info.Size() > maxSnapshotVerificationCacheBytes {
		return fmt.Errorf("pruning: snapshot verification cache size %d exceeds limit %d", info.Size(), maxSnapshotVerificationCacheBytes)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var disk snapshotCoverageVerificationDisk
	if err := decoder.Decode(&disk); err != nil {
		return fmt.Errorf("pruning: decode snapshot verification cache: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("pruning: snapshot verification cache has trailing JSON")
	}
	if disk.Version != snapshotCoverageVerificationCacheVersion {
		return fmt.Errorf("pruning: unsupported snapshot verification cache version %d", disk.Version)
	}
	if len(disk.Entries) > maxSnapshotVerificationCacheEntries {
		return fmt.Errorf("pruning: snapshot verification cache entries %d exceeds limit %d", len(disk.Entries), maxSnapshotVerificationCacheEntries)
	}
	for _, record := range disk.Entries {
		record = normalizeSnapshotHistoryVerificationRecord(record)
		if err := validateSnapshotHistoryVerificationRecord(record); err != nil {
			return fmt.Errorf("pruning: invalid snapshot verification cache entry: %w", err)
		}
		c.persistent[record] = struct{}{}
	}
	return nil
}

func (c *snapshotCoverageVerificationCache) persistLocked() error {
	if c.dir == "" {
		return nil
	}
	entries := make([]snapshotHistoryVerificationRecord, 0, len(c.persistent))
	for record := range c.persistent {
		entries = append(entries, record)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].History.Path != entries[j].History.Path {
			return entries[i].History.Path < entries[j].History.Path
		}
		if entries[i].Index.Path != entries[j].Index.Path {
			return entries[i].Index.Path < entries[j].Index.Path
		}
		return entries[i].Accessor.Path < entries[j].Accessor.Path
	})
	data, err := json.MarshalIndent(snapshotCoverageVerificationDisk{
		Version: snapshotCoverageVerificationCacheVersion,
		Entries: entries,
	}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.dir, ".state-domain-history-verification-*.tmp")
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
	if err := os.Rename(tmpName, filepath.Join(c.dir, snapshotCoverageVerificationCacheFile)); err != nil {
		return err
	}
	dir, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func snapshotHistoryVerificationRecordFor(key snapshotHistoryVerificationKey) snapshotHistoryVerificationRecord {
	return normalizeSnapshotHistoryVerificationRecord(snapshotHistoryVerificationRecord{
		History:  key.history,
		Index:    key.index,
		Accessor: key.accessor,
	})
}

func normalizeSnapshotHistoryVerificationRecord(record snapshotHistoryVerificationRecord) snapshotHistoryVerificationRecord {
	record.History.Checksum = strings.ToLower(strings.TrimSpace(record.History.Checksum))
	record.Index.Checksum = strings.ToLower(strings.TrimSpace(record.Index.Checksum))
	record.Accessor.Checksum = strings.ToLower(strings.TrimSpace(record.Accessor.Checksum))
	return record
}

func validateSnapshotHistoryVerificationRecord(record snapshotHistoryVerificationRecord) error {
	refs := []struct {
		name string
		ref  snapshots.SegmentRef
		kind snapshots.SegmentKind
	}{
		{name: "history", ref: record.History, kind: snapshots.SegmentHistory},
		{name: "index", ref: record.Index, kind: snapshots.SegmentInverted},
		{name: "accessor", ref: record.Accessor, kind: snapshots.SegmentAccessor},
	}
	for _, item := range refs {
		ref := item.ref
		if ref.NormalizedDataset() != snapshots.SegmentDatasetStateDomainChange || ref.Kind != item.kind {
			return fmt.Errorf("%s %q is %s/%s", item.name, ref.Path, ref.NormalizedDataset(), ref.Kind)
		}
		if ref.Path == "" || filepath.IsAbs(ref.Path) || filepath.Clean(ref.Path) != ref.Path || ref.Path == "." || strings.HasPrefix(ref.Path, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%s has invalid path %q", item.name, ref.Path)
		}
		if ref.Size == 0 {
			return fmt.Errorf("%s %q has no size", item.name, ref.Path)
		}
		if !validSnapshotVerificationChecksum(ref.Checksum) {
			return fmt.Errorf("%s %q has invalid checksum %q", item.name, ref.Path, ref.Checksum)
		}
		if ref.FromTxNum != record.History.FromTxNum || ref.ToTxNum != record.History.ToTxNum || ref.AggregationSteps != record.History.AggregationSteps {
			return fmt.Errorf("%s %q does not match history range/steps", item.name, ref.Path)
		}
	}
	return nil
}

func validSnapshotVerificationChecksum(checksum string) bool {
	const prefix = "sha256:"
	if !strings.HasPrefix(checksum, prefix) || len(checksum) != len(prefix)+64 {
		return false
	}
	_, err := hex.DecodeString(checksum[len(prefix):])
	return err == nil
}

func snapshotHistoryVerificationKeyFor(dir string, manifest *snapshots.Manifest, history snapshots.SegmentRef) (snapshotHistoryVerificationKey, error) {
	key := snapshotHistoryVerificationKey{history: history}
	var err error
	if key.historyFile, err = snapshotVerificationFileIdentity(dir, history); err != nil {
		return snapshotHistoryVerificationKey{}, fmt.Errorf("history segment %q: %w", history.Path, err)
	}
	cfg, ok := snapshots.DefaultDomainRegistry().ConfigForRef(history)
	if !ok || !cfg.IsHistoryBinarySegmentPath(history.Path) {
		return key, nil
	}
	if cfg.HasHistoryInvertedIndex {
		var found bool
		key.index, found = cfg.HistoryIndexRef(manifest, history)
		if !found {
			return snapshotHistoryVerificationKey{}, errors.New("missing history index companion")
		}
		if key.indexFile, err = snapshotVerificationFileIdentity(dir, key.index); err != nil {
			return snapshotHistoryVerificationKey{}, fmt.Errorf("history index %q: %w", key.index.Path, err)
		}
	}
	if cfg.HasHistoryAccessor {
		var found bool
		key.accessor, found = cfg.HistoryAccessorRef(manifest, history)
		if !found {
			return snapshotHistoryVerificationKey{}, errors.New("missing history accessor companion")
		}
		if key.accessorFile, err = snapshotVerificationFileIdentity(dir, key.accessor); err != nil {
			return snapshotHistoryVerificationKey{}, fmt.Errorf("history accessor %q: %w", key.accessor.Path, err)
		}
	}
	return key, nil
}

func snapshotVerificationFileIdentity(dir string, ref snapshots.SegmentRef) (snapshotFileIdentity, error) {
	info, err := os.Stat(filepath.Join(dir, ref.Path))
	if err != nil {
		return snapshotFileIdentity{}, err
	}
	if ref.Size != 0 && uint64(info.Size()) != ref.Size {
		return snapshotFileIdentity{}, fmt.Errorf("segment %q size %d, want %d", ref.Path, info.Size(), ref.Size)
	}
	return snapshotFileIdentity{size: info.Size(), modUnixNano: info.ModTime().UnixNano()}, nil
}
