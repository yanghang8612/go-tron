package snapshots

import (
	"bytes"
	"fmt"
	"io"
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
	// Below this size, the extra read calls cost more than the saved ReadFile
	// allocation in complete warm LoadProductionManifest benchmarks.
	manifestCacheStreamMinBytes = 64 << 10
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
	stringSlack   uint64 // owned publication-block capacity beyond string lengths
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
	return loadManifestFileMode(dir, production, false)
}

// loadProductionManifestReadOnly may borrow the cache's immutable generation.
// Keep it package-private: callers must neither mutate this object/its slices
// nor hand it to any public API that may do so. A publication replaces the
// resident pointer, never its contents, so an older borrowed view stays valid.
// Every load still authenticates all current file bytes, including cache hits.
func loadProductionManifestReadOnly(dir string) (*Manifest, error) {
	return loadManifestFileMode(dir, true, true)
}

func loadManifestFileMode(dir string, production, readOnly bool) (*Manifest, error) {
	budget, err := manifestCacheConfiguredBudget()
	if err != nil {
		return nil, err
	}
	// A lowered limit or disable releases its old resident even if ReadFile
	// subsequently fails. This is configuration, never a stale-read fallback.
	loadedManifestCache.mu.Lock()
	loadedManifestCache.resizeLocked(budget)
	cachedData, cachedManifest := loadedManifestCache.data, loadedManifestCache.manifest
	loadedManifestCache.mu.Unlock()
	// Always read the current file. Equal size, timestamp, generation, inode,
	// or checksum is not enough to reuse a previously validated object.
	path := filepath.Join(dir, ManifestFile)
	var data []byte
	if cachedManifest != nil && len(cachedData) >= manifestCacheStreamMinBytes {
		// A byte-for-byte streaming comparison avoids allocating the entire JSON
		// file on a cache hit. A changed file is assembled from the bytes actually
		// read and still takes the ordinary decode and validation path.
		var equal bool
		data, equal, err = readManifestAgainstCache(path, cachedData)
		if err != nil {
			return nil, err
		}
		if equal {
			loadedManifestCache.mu.Lock()
			if loadedManifestCache.manifest == cachedManifest {
				m, hitErr := loadedManifestCache.hitLocked(production, readOnly)
				loadedManifestCache.mu.Unlock()
				return m, hitErr
			}
			loadedManifestCache.mu.Unlock()
			// Publication changed the resident while the file was read. Retain the
			// exact bytes observed by this load, rather than reopening the path.
			data = bytes.Clone(cachedData)
		}
	} else {
		data, err = os.ReadFile(path)
	}
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
	return loadedManifestCache.decodeMode(data, production, budget, readOnly)
}

// readManifestAgainstCache reads through EOF even on a mismatch, including
// extra and truncated bytes. It holds no cache lock during I/O. The cached
// input is immutable for its lifetime, including after another reader or a
// publisher replaces the resident entry.
func readManifestAgainstCache(path string, cached []byte) ([]byte, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	return compareManifestReader(f, cached)
}

func compareManifestReader(reader io.Reader, cached []byte) ([]byte, bool, error) {
	// A hit on a small manifest must not allocate a fixed 64 KiB scratch.
	// The extra byte also detects a file that grew beyond the cached length.
	chunk := make([]byte, min(64<<10, len(cached)+1))
	var data []byte
	read, emptyReads, equal := 0, 0, true
	for {
		n, readErr := reader.Read(chunk)
		if n != 0 {
			emptyReads = 0
			if equal && (read+n > len(cached) || !bytes.Equal(chunk[:n], cached[read:read+n])) {
				equal = false
				// All earlier chunks matched, so the already-observed prefix is
				// exactly the cached prefix. Stat is a capacity hint only: it
				// cannot establish identity or bypass reading the rest of the
				// actual file. Reserve once to avoid repeated growth on misses.
				capacity := read + n
				if file, ok := reader.(*os.File); ok {
					if info, statErr := file.Stat(); statErr == nil && info.Mode().IsRegular() && info.Size() >= 0 && info.Size() < int64(^uint(0)>>1) {
						capacity = max(capacity, int(info.Size())+1)
					}
				}
				data = make([]byte, read, capacity)
				copy(data, cached[:read])
			}
			if !equal {
				data = append(data, chunk[:n]...)
			}
			read += n
		}
		if readErr == io.EOF {
			if equal && read == len(cached) {
				return nil, true, nil
			}
			if equal {
				data = bytes.Clone(cached[:read])
			}
			return data, false, nil
		}
		if readErr != nil {
			return nil, false, readErr
		}
		if n == 0 {
			emptyReads++
			if emptyReads >= 100 {
				return nil, false, io.ErrNoProgress
			}
		}
		if !equal && n == len(chunk) && len(chunk) < 64<<10 {
			// A small cached file grew. Keep a large sequential read buffer
			// only for the remainder of this changed-file load.
			chunk = make([]byte, 64<<10)
		}
	}
}

// decodeOwned takes ownership of data, which comes from a private ReadFile
// buffer. Returned manifests never share mutable containers with the cache.
func (c *manifestDecodeCache) decodeOwned(data []byte, production bool, budget uint64) (*Manifest, error) {
	return c.decodeMode(data, production, budget, false)
}

func (c *manifestDecodeCache) decodeMode(data []byte, production bool, budget uint64, readOnly bool) (*Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resizeLocked(budget)
	if c.manifest != nil && bytes.Equal(data, c.data) {
		return c.hitLocked(production, readOnly)
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
	if readOnly {
		// The decoder owns this generation exclusively. Unlike the public load
		// path, no mutable result escapes, so transferring it avoids a catalog
		// clone. Charge the actual growable decoder slice capacities.
		c.retainReadOnlyDecodedLocked(data, m, productionErr)
	} else {
		c.retainDecodedLocked(data, m, productionErr, false)
	}
	if production && productionErr != nil {
		return nil, productionErr
	}
	return m, nil
}

func (c *manifestDecodeCache) hitLocked(production, readOnly bool) (*Manifest, error) {
	manifestCacheHits.Inc(1)
	if c.publication {
		manifestCachePublicationHits.Inc(1)
	}
	manifestCacheCandidate.Update(int64(c.charge))
	if production && c.productionErr != nil {
		return nil, c.productionErr
	}
	if readOnly {
		return c.manifest, nil
	}
	return cloneDecodedManifest(c.manifest), nil
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
		var stringSlack uint64
		if publication {
			// The public publisher retains its mutable containers. Strings are
			// immutable but may be small substrings of arbitrarily large caller
			// buffers: clone them too so the resident charge bounds what we own.
			stringSlack = clonePublishedManifestStrings(owned)
			// Retired is omitempty, unlike Segments. Its empty-array input is
			// absent in the encoded JSON and decodes back to nil.
			if len(owned.Retired) == 0 {
				owned.Retired = nil
			}
		}
		charge = manifestCacheChargeWithStringSlack(data, owned, stringSlack)
		manifestCacheCandidate.Update(manifestCacheMetricBytes(charge))
		if charge > c.budget {
			manifestCacheBypasses.Inc(1)
			manifestCacheRejections.Inc(1)
			manifestCacheRejectedCharge.Update(manifestCacheMetricBytes(charge))
			return false
		}
		c.data, c.manifest, c.productionErr, c.charge = data, owned, productionErr, charge
		c.publication = publication
		c.stringSlack = stringSlack
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

// Called only with a freshly decoded, exclusively owned read-only generation.
// Rejected generations remain local to the read-only caller, never cached.
func (c *manifestDecodeCache) retainReadOnlyDecodedLocked(data []byte, m *Manifest, productionErr error) {
	charge := manifestCacheCharge(data, m)
	manifestCacheCandidate.Update(manifestCacheMetricBytes(charge))
	if charge > c.budget {
		manifestCacheBypasses.Inc(1)
		manifestCacheRejections.Inc(1)
		manifestCacheRejectedCharge.Update(manifestCacheMetricBytes(charge))
		return
	}
	c.data, c.manifest, c.productionErr, c.charge = data, m, productionErr, charge
	c.publication = false
	c.stringSlack = 0
	manifestCacheResident.Update(int64(charge))
	manifestCacheHeadroom.Update(int64(c.budget - charge))
}

func manifestStringsValidUTF8(m *Manifest) bool {
	if m.Chain != nil && (!utf8.ValidString(m.Chain.GenesisHash) || !utf8.ValidString(m.Chain.ForkConfigHash)) {
		return false
	}
	for _, refs := range [][]SegmentRef{m.Segments, m.Retired} {
		for _, ref := range refs {
			if !utf8.ValidString(string(ref.Dataset)) || !utf8.ValidString(string(ref.Kind)) ||
				!utf8.ValidString(ref.Path) || !utf8.ValidString(ref.Checksum) || !utf8.ValidString(ref.LookupRoot) {
				return false
			}
		}
	}
	return true
}

func clonePublishedManifestStrings(m *Manifest) uint64 {
	if m.Chain != nil {
		m.Chain.GenesisHash = strings.Clone(m.Chain.GenesisHash)
		m.Chain.ForkConfigHash = strings.Clone(m.Chain.ForkConfigHash)
	}
	return clonePublishedRefStrings(m.Segments) + clonePublishedRefStrings(m.Retired)
}

// Pack private strings in bounded blocks instead of separate allocations per ref.
// Finish each block before exposing any of its substrings. A caller retaining
// one field can keep at most a 64 KiB block alive; an oversized field gets its
// own allocation. The resident charge still counts every string byte and
// doubles it for allocation rounding/metadata, as it does for decoded strings.
func clonePublishedRefStrings(refs []SegmentRef) uint64 {
	const maxBlock = 64 << 10
	const fieldsPerRef = 5
	var slack uint64
	field := func(index int) string {
		ref := &refs[index/fieldsPerRef]
		switch index % fieldsPerRef {
		case 0:
			return string(ref.Dataset)
		case 1:
			return string(ref.Kind)
		case 2:
			return ref.Path
		case 3:
			return ref.Checksum
		default:
			return ref.LookupRoot
		}
	}
	for first := 0; first < len(refs)*fieldsPerRef; {
		last, size := first, 0
		for last < len(refs)*fieldsPerRef {
			n := len(field(last))
			if last > first && size+n > maxBlock {
				break
			}
			size += n
			last++
			if size >= maxBlock {
				break
			}
		}
		var block strings.Builder
		block.Grow(size)
		for i := first; i < last; i++ {
			block.WriteString(field(i))
		}
		// Count actual capacity in addition to all logical string bytes, before
		// applying the ordinary allocation-rounding and metadata allowance.
		slack += uint64(block.Cap() - size)
		owned := block.String()
		offset := 0
		for i := first; i < last; i++ {
			n := len(field(i))
			value := owned[offset : offset+n]
			ref := &refs[i/fieldsPerRef]
			switch i % fieldsPerRef {
			case 0:
				ref.Dataset = SegmentDataset(value)
			case 1:
				ref.Kind = SegmentKind(value)
			case 2:
				ref.Path = value
			case 3:
				ref.Checksum = value
			default:
				ref.LookupRoot = value
			}
			offset += n
		}
		first = last
	}
	return slack
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
	c.stringSlack = 0
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
			n += uint64(len(ref.Dataset) + len(ref.Kind) + len(ref.Path) + len(ref.Checksum) + len(ref.LookupRoot))
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

func manifestCacheChargeWithStringSlack(data []byte, m *Manifest, slack uint64) uint64 {
	charge := manifestCacheCharge(data, m)
	if slack > (math.MaxUint64-charge)/2 {
		return math.MaxUint64
	}
	return charge + slack*2
}
