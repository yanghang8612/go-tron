package blockbuffer

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ethereum/go-ethereum/metrics"
)

const (
	baseReadCacheReuseSlots       = 128
	baseReadCacheReuseKeyBytes    = 48
	baseReadCacheReuseSampleShift = 6
	baseReadCacheReuseCohorts     = 2
	baseReadCacheReuseSources     = 4
	baseReadCacheReuseMemoryLimit = 512 << 10
)

const (
	baseReadCacheReuseWindowFlush = iota
	baseReadCacheReuseFlushAdmission
)

const (
	baseReadCacheReuseCollision = iota
	baseReadCacheReuseReplaced
	baseReadCacheReuseDeleted
	baseReadCacheReuseOversized
	baseReadCacheReuseCleared
	baseReadCacheReuseCensorReasons
)

// This optional observer records two cohorts whose enrollment has no foreground
// or prefetch reference: flush-only window promotion and probation-triggered
// canonical flush admission. Probation is a fingerprint observation, not proof
// that the complete physical key was previously read. Later source bits describe
// reference() callers,
// including durable publication races; they are not exact hits, saved I/O, or
// first-hit times. It never changes cache admission, credit, or returned values.
//
// All mutable state belongs to the cache's existing shard writer lock. Slots
// retain complete inline physical keys only, never entry/value/string pointers.
// There is no hook on ordinary hits. The bounded live scan runs only with the
// existing low-frequency occupancy snapshot under each shard's RLock.
type baseReadCacheReuseObserver struct {
	shards [baseReadCacheShardCount]baseReadCacheReuseShard
}

type baseReadCacheReuseSlot struct {
	key           [baseReadCacheReuseKeyBytes]byte
	initialCharge uint64
	keyLen        uint8 // zero means unoccupied
	depth         uint8 // zero-based diagnostic depth (6 or 7)
	cohort        uint8
}

type baseReadCacheReuseTotals struct {
	eligible               uint64
	samples                uint64
	completed              uint64
	censored               [baseReadCacheReuseCensorReasons]uint64
	completedSources       [baseReadCacheReuseSources]uint64
	sampledInitialCharge   uint64
	completedInitialCharge uint64
	censoredInitialCharge  uint64
}

type baseReadCacheReuseShard struct {
	slots  [baseReadCacheReuseSlots]baseReadCacheReuseSlot
	totals [baseReadCacheDiagnosticDepths][baseReadCacheReuseCohorts]baseReadCacheReuseTotals
	live   uint16
}

type baseReadCacheReuseCohortStats struct {
	baseReadCacheReuseTotals
	live              uint64
	liveSources       [baseReadCacheReuseSources]uint64
	liveInitialCharge uint64
	liveCurrentCharge uint64
	// Missing live entries indicate a lifecycle-hook gap. Keep them live and
	// unclassified instead of silently reporting no subsequent reference.
	liveMissing uint64
}

type baseReadCacheReuseStats struct {
	ownerID uint64
	enabled bool
	cohorts [baseReadCacheDiagnosticDepths][baseReadCacheReuseCohorts]baseReadCacheReuseCohortStats
}

var baseReadCacheReuseOwnerSequence atomic.Uint64

func newBaseReadCacheReuseObserver() *baseReadCacheReuseObserver {
	// Read the opt-in once per owner, never on an event or cache hit. Enabling
	// it requires a new cache owner (normally a process restart).
	if os.Getenv("GTRON_BASE_CACHE_REUSE_OBSERVER") != "1" || baseReadCacheReuseMetadataBytes() > baseReadCacheReuseMemoryLimit {
		return nil
	}
	return new(baseReadCacheReuseObserver)
}

func baseReadCacheReuseMetadataBytes() uintptr {
	return unsafe.Sizeof(baseReadCacheReuseObserver{}) + unsafe.Sizeof(baseReadCacheReuseStats{}) +
		(baseReadCacheShardCount+1)*unsafe.Sizeof((*baseReadCacheReuseObserver)(nil)) + unsafe.Sizeof(uint64(0))
}

// Use a fixed full-key fingerprint, then independent bits for the 1/64 gate and
// 128-slot index. The avalanche separates this sampler from probation indexing
// and the shard selector. Complete inline key comparison resolves collisions;
// generation-qualified delta keys are never merged with another generation.
func baseReadCacheReuseIndex(key string) (uint32, bool) {
	if len(key) == 0 || len(key) > baseReadCacheReuseKeyBytes {
		return 0, false
	}
	hash := baseReadCacheAdmissionFingerprintString(key) ^ 0xd6e8feb86659fd93
	hash ^= hash >> 33
	hash *= 0xff51afd7ed558ccd
	hash ^= hash >> 33
	hash *= 0xc4ceb9fe1a85ec53
	hash ^= hash >> 33
	return uint32(hash>>baseReadCacheReuseSampleShift) & (baseReadCacheReuseSlots - 1),
		hash&((1<<baseReadCacheReuseSampleShift)-1) == 0
}

func (slot *baseReadCacheReuseSlot) matches(key string) bool {
	return int(slot.keyLen) == len(key) && string(slot.key[:slot.keyLen]) == key
}

// observeReuseAdmission runs after the real queue transition and before any
// capacity eviction can recycle the entry. The caller owns s.mu for writing.
func (s *baseReadCacheShard) observeReuseAdmission(entry *baseReadCacheEntry, cohort uint8) {
	if s.reuse == nil {
		return
	}
	references := entry.references.Load()
	depth := (references & baseReadCacheDiagnosticDepthMask) >> baseReadCacheDiagnosticDepthShift
	if depth == 0 || depth > baseReadCacheDiagnosticDepths || cohort == baseReadCacheReuseWindowFlush && references&baseReadCacheReferenceSourceMask != baseReadCacheReferenceFlush {
		return
	}
	totals := &s.reuse.totals[depth-1][cohort]
	totals.eligible++
	index, sampled := baseReadCacheReuseIndex(entry.key)
	if !sampled {
		return
	}
	slot := &s.reuse.slots[index]
	if slot.keyLen != 0 {
		reason := baseReadCacheReuseCollision
		if slot.matches(entry.key) {
			reason = baseReadCacheReuseReplaced
		}
		s.reuse.censorSlot(slot, reason)
	}
	copy(slot.key[:], entry.key)
	slot.keyLen = uint8(len(entry.key))
	slot.depth = uint8(depth - 1)
	slot.cohort = cohort
	slot.initialCharge = uint64(entry.charge)
	s.reuse.live++
	totals.samples++
	totals.sampledInitialCharge += slot.initialCharge
}

func (observer *baseReadCacheReuseShard) censorSlot(slot *baseReadCacheReuseSlot, reason int) {
	totals := &observer.totals[slot.depth][slot.cohort]
	totals.censored[reason]++
	totals.censoredInitialCharge += slot.initialCharge
	*slot = baseReadCacheReuseSlot{}
	observer.live--
}

func (s *baseReadCacheShard) censorReuseKey(key string, reason int) {
	if s.reuse == nil || s.reuse.live == 0 {
		return
	}
	index, sampled := baseReadCacheReuseIndex(key)
	if !sampled {
		return
	}
	slot := &s.reuse.slots[index]
	if slot.keyLen != 0 && slot.matches(key) {
		s.reuse.censorSlot(slot, reason)
	}
}

// completeReuseCapacity is called only after the real CLOCK decides to remove
// a live entry. Second chances and stale-token recycling never complete samples.
func (s *baseReadCacheShard) completeReuseCapacity(entry *baseReadCacheEntry) {
	if s.reuse == nil || s.reuse.live == 0 || entry.references.Load()&baseReadCacheDiagnosticDepthMask == 0 {
		return
	}
	index, sampled := baseReadCacheReuseIndex(entry.key)
	if !sampled {
		return
	}
	slot := &s.reuse.slots[index]
	if slot.keyLen == 0 || !slot.matches(entry.key) {
		return
	}
	totals := &s.reuse.totals[slot.depth][slot.cohort]
	totals.completed++
	totals.completedInitialCharge += slot.initialCharge
	sources := entry.references.Load() & (baseReadCacheReferenceForeground | baseReadCacheReferencePrefetch)
	totals.completedSources[sources>>baseReadCacheReferenceSourceShift]++
	*slot = baseReadCacheReuseSlot{}
	s.reuse.live--
}

func (s *baseReadCacheShard) clearReuseSamples() {
	if s.reuse == nil {
		return
	}
	for i := range s.reuse.slots {
		slot := &s.reuse.slots[i]
		if slot.keyLen != 0 {
			s.reuse.censorSlot(slot, baseReadCacheReuseCleared)
		}
	}
}

func addBaseReadCacheReuseShardStats(stats *baseReadCacheReuseStats, shard *baseReadCacheShard) {
	if shard.reuse == nil {
		return
	}
	for depth := range shard.reuse.totals {
		for cohort, totals := range shard.reuse.totals[depth] {
			dst := &stats.cohorts[depth][cohort]
			dst.eligible += totals.eligible
			dst.samples += totals.samples
			dst.completed += totals.completed
			dst.sampledInitialCharge += totals.sampledInitialCharge
			dst.completedInitialCharge += totals.completedInitialCharge
			dst.censoredInitialCharge += totals.censoredInitialCharge
			for reason, count := range totals.censored {
				dst.censored[reason] += count
			}
			for source, count := range totals.completedSources {
				dst.completedSources[source] += count
			}
		}
	}
	for i := range shard.reuse.slots {
		slot := &shard.reuse.slots[i]
		if slot.keyLen == 0 {
			continue
		}
		dst := &stats.cohorts[slot.depth][slot.cohort]
		dst.live++
		dst.liveInitialCharge += slot.initialCharge
		entry := shard.entries[string(slot.key[:slot.keyLen])]
		if entry == nil || !entry.live {
			dst.liveMissing++
			continue
		}
		dst.liveCurrentCharge += uint64(entry.charge)
		sources := entry.references.Load() & (baseReadCacheReferenceForeground | baseReadCacheReferencePrefetch)
		dst.liveSources[sources>>baseReadCacheReferenceSourceShift]++
	}
}

type baseReadCacheReuseCohortGauges struct {
	eligible, samples, completed, censored              *metrics.Gauge
	sampledCharge, completedCharge, censoredCharge      *metrics.Gauge
	live, liveInitialCharge, liveCurrentCharge, missing *metrics.Gauge
	completedSources                                    [baseReadCacheReuseSources]*metrics.Gauge
	liveSources                                         [baseReadCacheReuseSources]*metrics.Gauge
	censorReasons                                       [baseReadCacheReuseCensorReasons]*metrics.Gauge
}

var (
	baseReadCacheReusePublishMu     sync.Mutex
	baseReadCacheReuseOwnerGauge    = metrics.NewRegisteredGauge("blockbuffer/base_cache/reuse/owner_id", nil)
	baseReadCacheReuseEnabledGauge  = metrics.NewRegisteredGauge("blockbuffer/base_cache/reuse/enabled", nil)
	baseReadCacheReuseMetadataGauge = metrics.NewRegisteredGauge("blockbuffer/base_cache/reuse/metadata_bytes", nil)
	baseReadCacheReuseGauges        = newBaseReadCacheReuseGauges()
)

func newBaseReadCacheReuseGauges() [baseReadCacheDiagnosticDepths][baseReadCacheReuseCohorts]baseReadCacheReuseCohortGauges {
	var gauges [baseReadCacheDiagnosticDepths][baseReadCacheReuseCohorts]baseReadCacheReuseCohortGauges
	for depth, depthName := range baseReadCacheDiagnosticDepthNames {
		for cohort, cohortName := range [...]string{"window_flush_only", "flush_probation_admission"} {
			prefix := "blockbuffer/base_cache/reuse/" + depthName + "/" + cohortName + "/"
			gauge := func(suffix string) *metrics.Gauge { return metrics.NewRegisteredGauge(prefix+suffix, nil) }
			dst := &gauges[depth][cohort]
			dst.eligible, dst.samples = gauge("eligible"), gauge("samples")
			dst.completed, dst.censored = gauge("capacity_completed"), gauge("censored")
			dst.sampledCharge = gauge("initial_charge/sampled")
			dst.completedCharge = gauge("initial_charge/capacity_completed")
			dst.censoredCharge = gauge("initial_charge/censored")
			dst.live, dst.liveInitialCharge = gauge("live"), gauge("initial_charge/live")
			dst.liveCurrentCharge, dst.missing = gauge("live_current_charge"), gauge("live_missing")
			for source, sourceName := range [...]string{"none", "foreground", "prefetch", "foreground_prefetch"} {
				dst.completedSources[source] = gauge("capacity_completed/reference_sources/" + sourceName)
				dst.liveSources[source] = gauge("live/reference_sources/" + sourceName)
			}
			for reason, reasonName := range [...]string{"slot_collision", "same_key_episode", "delete", "oversized_refresh", "clear"} {
				dst.censorReasons[reason] = gauge("censored/" + reasonName)
			}
		}
	}
	return gauges
}

// Gauges are one owner's lifetime totals, not process-global accumulating
// counters. Owner identity must match before differencing samples; HTTP metric
// collection and the cross-shard snapshot remain eventually consistent.
func publishBaseReadCacheReuseMetrics(stats baseReadCacheReuseStats) {
	// Different owners have separate ordinary publication locks. Serialize this
	// small metric family so their owner identity and cohort writes do not interleave.
	baseReadCacheReusePublishMu.Lock()
	defer baseReadCacheReusePublishMu.Unlock()
	enabled, metadata := int64(0), int64(0)
	if stats.enabled {
		enabled, metadata = 1, int64(baseReadCacheReuseMetadataBytes())
	}
	baseReadCacheReuseEnabledGauge.Update(enabled)
	baseReadCacheReuseMetadataGauge.Update(metadata)
	for depth := range stats.cohorts {
		for cohort, values := range stats.cohorts[depth] {
			dst := &baseReadCacheReuseGauges[depth][cohort]
			dst.eligible.Update(int64(values.eligible))
			dst.samples.Update(int64(values.samples))
			dst.completed.Update(int64(values.completed))
			dst.sampledCharge.Update(int64(values.sampledInitialCharge))
			dst.completedCharge.Update(int64(values.completedInitialCharge))
			dst.censoredCharge.Update(int64(values.censoredInitialCharge))
			dst.live.Update(int64(values.live))
			dst.liveInitialCharge.Update(int64(values.liveInitialCharge))
			dst.liveCurrentCharge.Update(int64(values.liveCurrentCharge))
			dst.missing.Update(int64(values.liveMissing))
			var censored uint64
			for reason, count := range values.censored {
				censored += count
				dst.censorReasons[reason].Update(int64(count))
			}
			dst.censored.Update(int64(censored))
			for source, count := range values.completedSources {
				dst.completedSources[source].Update(int64(count))
				dst.liveSources[source].Update(int64(values.liveSources[source]))
			}
		}
	}
	baseReadCacheReuseOwnerGauge.Update(int64(stats.ownerID))
}
