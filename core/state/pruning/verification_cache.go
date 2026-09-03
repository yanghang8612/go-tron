package pruning

import (
	"bytes"
	"context"
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
	"time"

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
	MemoryHits             uint64
	PersistentHits         uint64
	FullVerified           uint64
	TrustedRecorded        uint64
	Entries                uint64
	ActiveEntries          uint64
	ActiveBytes            uint64
	ChecksumInFlight       uint64
	ChecksumStarted        uint64
	ChecksumCompleted      uint64
	ChecksumFailed         uint64
	ChecksumCanceled       uint64
	ChecksumBytesInFlight  uint64
	ChecksumBytesStarted   uint64
	ChecksumBytesCompleted uint64
}

type snapshotHistoryVerificationRoute uint8

const (
	snapshotHistoryVerificationFull snapshotHistoryVerificationRoute = iota + 1
	snapshotHistoryVerificationMemory
	snapshotHistoryVerificationPersistent
)

type snapshotCoverageVerificationCache struct {
	mu            sync.Mutex
	publishMu     sync.Mutex
	dir           string
	verified      map[snapshotHistoryVerificationKey]struct{}
	persistent    map[snapshotHistoryVerificationRecord]struct{}
	loadErr       error
	stats         snapshotCoverageVerificationCacheStats
	statsObserver func(snapshotCoverageVerificationCacheStats)
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
	_, ok := c.verified[key]
	if ok {
		c.stats.MemoryHits++
	}
	c.mu.Unlock()
	if ok {
		c.publishStats()
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
	c.publishStats()
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
	c.verified[key] = struct{}{}
	if trusted {
		c.stats.TrustedRecorded++
	} else {
		c.stats.FullVerified++
	}
	if _, ok := c.persistent[record]; ok {
		c.mu.Unlock()
		c.publishStats()
		return nil
	}
	c.persistent[record] = struct{}{}
	if err := c.persistLocked(); err != nil {
		delete(c.persistent, record)
		c.mu.Unlock()
		c.publishStats()
		return err
	}
	c.mu.Unlock()
	c.publishStats()
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
		c.mu.Unlock()
		c.publishStats()
		return nil
	}
	err := c.persistLocked()
	c.mu.Unlock()
	c.publishStats()
	return err
}

func (c *snapshotCoverageVerificationCache) Stats() snapshotCoverageVerificationCacheStats {
	if c == nil {
		return snapshotCoverageVerificationCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats, _ := c.statsSnapshotLocked()
	return stats
}

func (c *snapshotCoverageVerificationCache) setStatsObserver(observer func(snapshotCoverageVerificationCacheStats)) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.statsObserver = observer
	c.mu.Unlock()
	c.publishStats()
}

func (c *snapshotCoverageVerificationCache) statsSnapshotLocked() (snapshotCoverageVerificationCacheStats, func(snapshotCoverageVerificationCacheStats)) {
	stats := c.stats
	stats.Entries = uint64(len(c.persistent))
	return stats, c.statsObserver
}

// publishStats serializes observer callbacks and snapshots state only after it
// owns the publication sequence. A delayed publisher therefore emits the most
// recent state instead of overwriting newer gauges with the stale snapshot it
// originally triggered.
func (c *snapshotCoverageVerificationCache) publishStats() {
	if c == nil {
		return
	}
	c.publishMu.Lock()
	defer c.publishMu.Unlock()
	c.mu.Lock()
	stats, observer := c.statsSnapshotLocked()
	c.mu.Unlock()
	if observer != nil {
		observer(stats)
	}
}

func (c *snapshotCoverageVerificationCache) beginChecksum(bytes uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.stats.ChecksumInFlight++
	c.stats.ChecksumStarted++
	c.stats.ChecksumBytesInFlight += bytes
	c.stats.ChecksumBytesStarted += bytes
	c.mu.Unlock()
	c.publishStats()
}

func (c *snapshotCoverageVerificationCache) setActiveManifest(active map[snapshotHistoryVerificationKey]struct{}) {
	if c == nil {
		return
	}
	var bytes uint64
	for key := range active {
		size := snapshotHistoryVerificationKeyBytes(key)
		if ^uint64(0)-bytes < size {
			bytes = ^uint64(0)
			break
		}
		bytes += size
	}
	c.mu.Lock()
	c.stats.ActiveEntries = uint64(len(active))
	c.stats.ActiveBytes = bytes
	c.mu.Unlock()
	c.publishStats()
}

func (c *snapshotCoverageVerificationCache) finishChecksum(bytes uint64, err error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.stats.ChecksumInFlight > 0 {
		c.stats.ChecksumInFlight--
	}
	if bytes >= c.stats.ChecksumBytesInFlight {
		c.stats.ChecksumBytesInFlight = 0
	} else {
		c.stats.ChecksumBytesInFlight -= bytes
	}
	if err == nil {
		c.stats.ChecksumCompleted++
		c.stats.ChecksumBytesCompleted += bytes
	} else {
		c.stats.ChecksumFailed++
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			c.stats.ChecksumCanceled++
		}
	}
	c.mu.Unlock()
	c.publishStats()
}

// verifyHistory authenticates one immutable history/index/accessor triple and
// records the exact file identity used by the in-process fast path. Destructive
// callers set reauthenticateMemory so even a same-process trusted build is
// checksum-bound immediately before retired fallback files are removed.
func (c *snapshotCoverageVerificationCache) verifyHistory(ctx context.Context, dir string, manifest *snapshots.Manifest, ref snapshots.SegmentRef, reauthenticateMemory bool, operation string) (snapshotHistoryVerificationKey, snapshotHistoryVerificationRoute, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return snapshotHistoryVerificationKey{}, 0, err
	}
	if operation == "" {
		operation = "snapshot verification"
	}
	key, err := snapshotHistoryVerificationKeyFor(dir, manifest, ref)
	if err != nil {
		return snapshotHistoryVerificationKey{}, 0, fmt.Errorf("%s: identify state-domain history %q: %w", operation, ref.Path, err)
	}
	if c != nil && c.contains(key) {
		if reauthenticateMemory {
			err := c.observeChecksumVerification(key, operation, ref, snapshotHistoryVerificationMemory, func() error {
				if err := snapshots.VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, ref); err != nil {
					return err
				}
				return verifySnapshotHistoryIdentityUnchanged(dir, manifest, ref, key, operation)
			})
			if err != nil {
				return snapshotHistoryVerificationKey{}, 0, fmt.Errorf("%s: recheck in-memory state-domain history %q: %w", operation, ref.Path, err)
			}
		}
		return key, snapshotHistoryVerificationMemory, nil
	}

	persisted := c != nil && c.persistentCandidate(key)
	if persisted {
		err := c.observeChecksumVerification(key, operation, ref, snapshotHistoryVerificationPersistent, func() error {
			if err := snapshots.VerifyHistorySegmentCompanionChecksumsContext(ctx, dir, manifest, ref); err != nil {
				return err
			}
			return verifySnapshotHistoryIdentityUnchanged(dir, manifest, ref, key, operation)
		})
		if err != nil {
			return snapshotHistoryVerificationKey{}, 0, fmt.Errorf("%s: recheck cached state-domain history %q: %w", operation, ref.Path, err)
		}
	} else {
		err := c.observeChecksumVerification(key, operation, ref, snapshotHistoryVerificationFull, func() error {
			if err := snapshots.VerifyHistorySegmentWithCompanionsContext(ctx, dir, manifest, ref); err != nil {
				return err
			}
			return verifySnapshotHistoryIdentityUnchanged(dir, manifest, ref, key, operation)
		})
		if err != nil {
			return snapshotHistoryVerificationKey{}, 0, fmt.Errorf("%s: verify state-domain history %q: %w", operation, ref.Path, err)
		}
	}
	if c == nil {
		return key, snapshotHistoryVerificationFull, nil
	}
	if persisted {
		c.promotePersistent(key)
		return key, snapshotHistoryVerificationPersistent, nil
	}
	if err := c.addFull(key); err != nil {
		return snapshotHistoryVerificationKey{}, 0, fmt.Errorf("%s: persist state-domain history verification %q: %w", operation, ref.Path, err)
	}
	return key, snapshotHistoryVerificationFull, nil
}

func (c *snapshotCoverageVerificationCache) observeChecksumVerification(key snapshotHistoryVerificationKey, operation string, ref snapshots.SegmentRef, route snapshotHistoryVerificationRoute, verify func() error) (err error) {
	bytes := snapshotHistoryVerificationKeyBytes(key)
	started := time.Now()
	c.beginChecksum(bytes)
	log.Info("Verifying domain snapshot history",
		"operation", operation,
		"route", snapshotHistoryVerificationRouteName(route),
		"path", ref.Path,
		"checksumBytes", bytes)
	defer func() {
		c.finishChecksum(bytes, err)
		if err != nil {
			log.Warn("Domain snapshot history verification failed",
				"operation", operation,
				"route", snapshotHistoryVerificationRouteName(route),
				"path", ref.Path,
				"checksumBytes", bytes,
				"elapsed", time.Since(started),
				"err", err)
			return
		}
		log.Info("Verified domain snapshot history",
			"operation", operation,
			"route", snapshotHistoryVerificationRouteName(route),
			"path", ref.Path,
			"checksumBytes", bytes,
			"elapsed", time.Since(started))
	}()
	return verify()
}

func snapshotHistoryVerificationRouteName(route snapshotHistoryVerificationRoute) string {
	switch route {
	case snapshotHistoryVerificationMemory:
		return "memory-reauth"
	case snapshotHistoryVerificationPersistent:
		return "persistent"
	case snapshotHistoryVerificationFull:
		return "full"
	default:
		return "unknown"
	}
}

func snapshotHistoryVerificationKeyBytes(key snapshotHistoryVerificationKey) uint64 {
	var total uint64
	for _, identity := range []snapshotFileIdentity{key.historyFile, key.indexFile, key.accessorFile} {
		if identity.size <= 0 {
			continue
		}
		size := uint64(identity.size)
		if ^uint64(0)-total < size {
			return ^uint64(0)
		}
		total += size
	}
	return total
}

func verifySnapshotHistoryIdentityUnchanged(dir string, manifest *snapshots.Manifest, ref snapshots.SegmentRef, want snapshotHistoryVerificationKey, operation string) error {
	got, err := snapshotHistoryVerificationKeyFor(dir, manifest, ref)
	if err != nil {
		return fmt.Errorf("%s: re-identify state-domain history %q: %w", operation, ref.Path, err)
	}
	if got != want {
		return fmt.Errorf("%s: state-domain history %q changed while being verified", operation, ref.Path)
	}
	return nil
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
