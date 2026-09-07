package snapshots

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/ethereum/go-ethereum/metrics"
)

const manifestCacheMaxBytes = 64 << 20

var (
	loadedManifestCache   manifestDecodeCache
	manifestCacheHits     = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/hits", nil)
	manifestCacheMisses   = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/misses", nil)
	manifestCacheBypasses = metrics.GetOrRegisterCounter(defaultColdSnapshotMetrics+"manifest_cache/bypasses", nil)
	manifestCacheResident = metrics.GetOrRegisterGauge(defaultColdSnapshotMetrics+"manifest_cache/resident_bytes", nil)
)

// The cache stores metadata validation only. Segment-file authentication and
// destructive verification gates remain independent and are never cached here.
type manifestDecodeCache struct {
	mu            sync.Mutex
	data          []byte
	manifest      *Manifest
	productionErr error
	charge        uint64
}

func loadManifestFile(dir string, production bool) (*Manifest, error) {
	enabled := true
	switch value := strings.TrimSpace(os.Getenv("GTRON_SNAPSHOT_MANIFEST_CACHE")); value {
	case "", "1":
	case "0":
		enabled = false
	default:
		return nil, fmt.Errorf("snapshots: invalid GTRON_SNAPSHOT_MANIFEST_CACHE %q (want 0 or 1)", value)
	}
	if !enabled {
		loadedManifestCache.clear()
	}
	// Always read the current file. Equal size, timestamp, generation, inode,
	// or checksum is not enough to reuse a previously validated object.
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	if !enabled || uint64(cap(data))*2 > manifestCacheMaxBytes {
		manifestCacheBypasses.Inc(1)
		if production {
			return decodeProductionManifest(data)
		}
		return decodeManifest(data)
	}
	return loadedManifestCache.decodeOwned(data, production, manifestCacheMaxBytes)
}

// decodeOwned takes ownership of data, which comes from a private ReadFile
// buffer. Returned manifests never share mutable containers with the cache.
func (c *manifestDecodeCache) decodeOwned(data []byte, production bool, budget uint64) (*Manifest, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.charge > budget {
		c.clearLocked()
	}
	if c.manifest != nil && bytes.Equal(data, c.data) {
		manifestCacheHits.Inc(1)
		if production && c.productionErr != nil {
			return nil, c.productionErr
		}
		return cloneDecodedManifest(c.manifest), nil
	}
	manifestCacheMisses.Inc(1)
	m, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}
	productionErr := m.ValidateProduction()
	// Check admission before allocating a private view. The decoded slice
	// capacities conservatively include any spare capacity removed by cloning.
	charge := manifestCacheCharge(data, m)
	if charge <= budget {
		owned := cloneDecodedManifest(m)
		c.data, c.manifest, c.productionErr, c.charge = data, owned, productionErr, charge
		manifestCacheResident.Update(int64(charge))
	} else {
		manifestCacheBypasses.Inc(1)
	}
	if production && productionErr != nil {
		return nil, productionErr
	}
	return m, nil
}

func (c *manifestDecodeCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearLocked()
}

func (c *manifestDecodeCache) clearLocked() {
	c.data, c.manifest, c.productionErr, c.charge = nil, nil, nil, 0
	manifestCacheResident.Update(0)
}

func cloneDecodedManifest(m *Manifest) *Manifest {
	out := cloneManifest(m)
	// Preserve JSON null versus [] even for empty decoded arrays.
	if m.Segments != nil && out.Segments == nil {
		out.Segments = slices.Clone(m.Segments)
	}
	if m.Retired != nil && out.Retired == nil {
		out.Retired = slices.Clone(m.Retired)
	}
	return out
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
