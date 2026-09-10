package snapshots

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	manifestCacheDefaultBytes   = 256 << 20
	manifestCacheConfigMaxBytes = 1 << 30
	manifestCacheBudgetEnv      = "GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES"
)

var (
	loadedManifestCache              manifestDecodeCache
	manifestCacheHits                = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/hits", nil)
	manifestCacheMisses              = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/misses", nil)
	manifestCacheBypasses            = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/bypasses", nil)
	manifestCacheResident            = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"manifest_cache/resident_bytes", nil)
	manifestCacheBudget              = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"manifest_cache/budget_bytes", nil)
	manifestCacheCandidate           = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"manifest_cache/candidate_charge_bytes", nil)
	manifestCacheHeadroom            = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"manifest_cache/headroom_bytes", nil)
	manifestCacheRejections          = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/rejections/budget", nil)
	manifestCacheRejectedCharge      = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"manifest_cache/last_rejection_charge_bytes", nil)
	manifestCachePublicationSeeds    = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/publication_seeds", nil)
	manifestCachePublicationHits     = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/publication_hits", nil)
	manifestCachePublicationBypasses = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/publication_bypasses", nil)
)

// The cache stores metadata validation only. Segment-file authentication and
// destructive verification gates remain independent and are never cached here.
type manifestDecodeCache struct {
	mu            sync.Mutex
	data          []byte
	manifest      *Manifest
	productionErr error
	charge        uint64
	budget        uint64
	publication   bool
}

// A budget of zero disables the cache. Parse both settings even when disabled,
// so a malformed memory limit cannot silently take effect at a later enable.
func manifestCacheConfiguredBudget() (uint64, error) {
	enabled := true
	switch value := strings.TrimSpace(os.Getenv("GTRON_SNAPSHOT_MANIFEST_CACHE")); value {
	case "", "1":
	case "0":
		enabled = false
	default:
		return 0, fmt.Errorf("snapshots: invalid GTRON_SNAPSHOT_MANIFEST_CACHE %q (want 0 or 1)", value)
	}
	budget := uint64(manifestCacheDefaultBytes)
	if value := strings.TrimSpace(os.Getenv(manifestCacheBudgetEnv)); value != "" {
		// Accept decimal bytes only, not signs, units, exponent or base prefixes.
		for _, digit := range value {
			if digit < '0' || digit > '9' {
				return 0, fmt.Errorf("snapshots: invalid %s %q (want decimal bytes, 0 disables)", manifestCacheBudgetEnv, value)
			}
		}
		parsed, err := strconv.ParseUint(value, 10, 63)
		if err != nil || parsed > manifestCacheConfigMaxBytes {
			return 0, fmt.Errorf("snapshots: invalid %s %q (want 0..%d decimal bytes)", manifestCacheBudgetEnv, value, manifestCacheConfigMaxBytes)
		}
		budget = parsed
	}
	if !enabled {
		budget = 0
	}
	return budget, nil
}

func loadManifestFile(dir string, production bool) (*Manifest, error) {
	budget, err := manifestCacheConfiguredBudget()
	if err != nil {
		return nil, err
	}
	// A lowered limit or disable releases its old resident even if ReadFile
	// subsequently fails. This is configuration, never a stale-read fallback.
	loadedManifestCache.mu.Lock()
	loadedManifestCache.resizeLocked(budget)
	loadedManifestCache.mu.Unlock()
	// Always read the current file. Equal size, timestamp, generation, inode,
	// or checksum is not enough to reuse a previously validated object.
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	if budget == 0 {
		manifestCacheBypasses.Inc(1)
		if production {
			return decodeProductionManifest(data)
		}
		return decodeManifest(data)
	}
	return loadedManifestCache.decodeOwned(data, production, budget)
}

// decodeOwned takes ownership of data, which comes from a private ReadFile
// buffer. Returned manifests never share mutable containers with the cache.
func (c *manifestDecodeCache) decodeOwned(data []byte, production bool, budget uint64) (*Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeLocked(budget)
	if c.manifest != nil && bytes.Equal(data, c.data) {
		manifestCacheHits.Inc(1)
		if c.publication {
			manifestCachePublicationHits.Inc(1)
		}
		manifestCacheCandidate.Update(int64(c.charge))
		if production && c.productionErr != nil {
			return nil, c.productionErr
		}
		return cloneDecodedManifest(c.manifest), nil
	}
	manifestCacheMisses.Inc(1)
	// An old entry is no longer useful for these bytes. In particular, do not
	// retain it indefinitely when the growing replacement exceeds the budget.
	c.clearLocked()
	manifestCacheCandidate.Update(0)
	m, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}
	productionErr := m.ValidateProduction()
	c.retainDecodedLocked(data, m, productionErr, false)
	if production && productionErr != nil {
		return nil, productionErr
	}
	return m, nil
}

// A successful local publisher already normalized, sorted and validated m and
// encoded these exact bytes. Remember that result so the first subsequent load
// need not decode and validate it again. Reads still authenticate the complete
// current file bytes; another process may replace the file at any time.
//
// Cache configuration cannot turn a completed durable publication into an
// error. Invalid settings bypass this optimization; loaders retain their normal
// configuration error. A JSON string with invalid UTF-8 is also left to the
// decoder because encoding/json replaces it, potentially changing validation.
func rememberPublishedManifest(data []byte, m *Manifest) {
	budget, err := manifestCacheConfiguredBudget()
	if err != nil {
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	loadedManifestCache.retainPublished(data, m, budget)
}

func (c *manifestDecodeCache) retainPublished(data []byte, m *Manifest, budget uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeLocked(budget)
	// An idempotent progress publication, or a reader that won the race after
	// rename, may already have populated these exact bytes. Keep its detached
	// entry rather than cloning the whole catalog again.
	if c.manifest != nil && bytes.Equal(data, c.data) {
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	c.clearLocked()
	manifestCacheCandidate.Update(0)
	if budget == 0 || !manifestStringsValidUTF8(m) {
		manifestCachePublicationBypasses.Inc(1)
		return
	}
	// PublishManifest permits base-valid legacy JSON history. Keep its distinct
	// production validation error exactly as a normal decode-cache miss does.
	if c.retainDecodedLocked(data, m, m.ValidateProduction(), true) {
		manifestCachePublicationSeeds.Inc(1)
	} else {
		manifestCachePublicationBypasses.Inc(1)
	}
}

func (c *manifestDecodeCache) retainDecodedLocked(data []byte, m *Manifest, productionErr error, publication bool) bool {
	// The retained clone allocates exactly len elements, not the spare capacity
	// of encoding/json's growable slices. Calculate that exact container charge
	// before allocating the private copy; the caller keeps its decoded view.
	compact := *m
	compact.Segments = compact.Segments[:len(compact.Segments):len(compact.Segments)]
	compact.Retired = compact.Retired[:len(compact.Retired):len(compact.Retired)]
	charge := manifestCacheCharge(data, &compact)
	manifestCacheCandidate.Update(manifestCacheMetricBytes(charge))
	if charge <= c.budget {
		owned := cloneDecodedManifest(m)
		if publication {
			// The public publisher retains its mutable containers. Strings are
			// immutable but may be small substrings of arbitrarily large caller
			// buffers: clone them too so the resident charge bounds what we own.
			clonePublishedManifestStrings(owned)
			// Retired is omitempty, unlike Segments. Its empty-array input is
			// absent in the encoded JSON and decodes back to nil.
			if len(owned.Retired) == 0 {
				owned.Retired = nil
			}
		}
		c.data, c.manifest, c.productionErr, c.charge = data, owned, productionErr, charge
		c.publication = publication
		manifestCacheResident.Update(int64(charge))
		manifestCacheHeadroom.Update(int64(c.budget - charge))
		return true
	} else {
		manifestCacheBypasses.Inc(1)
		manifestCacheRejections.Inc(1)
		manifestCacheRejectedCharge.Update(manifestCacheMetricBytes(charge))
	}
	return false
}

func manifestStringsValidUTF8(m *Manifest) bool {
	if m.Chain != nil && (!utf8.ValidString(m.Chain.GenesisHash) || !utf8.ValidString(m.Chain.ForkConfigHash)) {
		return false
	}
	for _, refs := range [][]SegmentRef{m.Segments, m.Retired} {
		for _, ref := range refs {
			if !utf8.ValidString(string(ref.Dataset)) || !utf8.ValidString(string(ref.Kind)) ||
				!utf8.ValidString(ref.Path) || !utf8.ValidString(ref.Checksum) {
				return false
			}
		}
	}
	return true
}

func clonePublishedManifestStrings(m *Manifest) {
	if m.Chain != nil {
		m.Chain.GenesisHash = strings.Clone(m.Chain.GenesisHash)
		m.Chain.ForkConfigHash = strings.Clone(m.Chain.ForkConfigHash)
	}
	for _, refs := range [][]SegmentRef{m.Segments, m.Retired} {
		for i := range refs {
			ref := &refs[i]
			ref.Dataset = SegmentDataset(strings.Clone(string(ref.Dataset)))
			ref.Kind = SegmentKind(strings.Clone(string(ref.Kind)))
			ref.Path = strings.Clone(ref.Path)
			ref.Checksum = strings.Clone(ref.Checksum)
		}
	}
}

func (c *manifestDecodeCache) resizeLocked(budget uint64) {
	c.budget = budget
	manifestCacheBudget.Update(manifestCacheMetricBytes(budget))
	if budget == 0 || c.charge > budget {
		c.clearLocked()
	}
	manifestCacheHeadroom.Update(manifestCacheMetricBytes(budget - c.charge))
	if budget == 0 {
		manifestCacheCandidate.Update(0)
	}
}

func manifestCacheMetricBytes(value uint64) int64 {
	return int64(min(value, uint64(math.MaxInt64)))
}

func (c *manifestDecodeCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearLocked()
}

func (c *manifestDecodeCache) clearLocked() {
	c.data, c.manifest, c.productionErr, c.charge = nil, nil, nil, 0
	c.publication = false
	manifestCacheResident.Update(0)
	manifestCacheHeadroom.Update(manifestCacheMetricBytes(c.budget))
}

func cloneDecodedManifest(m *Manifest) *Manifest {
	out := *m
	out.lookup = nil
	if m.Chain != nil {
		chain := *m.Chain
		out.Chain = &chain
	}
	if m.Progress != nil {
		progress := *m.Progress
		out.Progress = &progress
	}
	// make(len) deliberately yields cap==len, while retaining [] versus null.
	if m.Segments != nil {
		out.Segments = make([]SegmentRef, len(m.Segments))
		copy(out.Segments, m.Segments)
	}
	if m.Retired != nil {
		out.Retired = make([]SegmentRef, len(m.Retired))
		copy(out.Retired, m.Retired)
	}
	return &out
}

// Charge every owned capacity and string, then double it for allocator
// rounding/metadata. Shared immutable strings are deliberately over-counted.
// Caller copies and transient decoding work are outside this resident budget.
func manifestCacheCharge(data []byte, m *Manifest) uint64 {
	n := uint64(cap(data)) + uint64(unsafe.Sizeof(*m))
	n += uint64(cap(m.Segments)+cap(m.Retired)) * uint64(unsafe.Sizeof(SegmentRef{}))
	for _, refs := range [][]SegmentRef{m.Segments, m.Retired} {
		for _, ref := range refs {
			n += uint64(len(ref.Dataset) + len(ref.Kind) + len(ref.Path) + len(ref.Checksum))
		}
	}
	if m.Chain != nil {
		n += uint64(unsafe.Sizeof(*m.Chain)) + uint64(len(m.Chain.GenesisHash)+len(m.Chain.ForkConfigHash))
	}
	if m.Progress != nil {
		n += uint64(unsafe.Sizeof(*m.Progress))
	}
	return n*2 + 4096
}
