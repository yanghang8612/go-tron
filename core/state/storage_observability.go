package state

import (
	"bytes"
	"slices"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
)

const (
	// oversizedStorageSampleKeyLimit is deliberately small enough that the
	// diagnostic cannot become a second storage cache. A sample remembers exact
	// keys (not hashes or a bloom filter), so every reported reuse is real.
	oversizedStorageSampleKeyLimit      = 128
	oversizedStorageSampleContractLimit = 64
	oversizedStorageSampleMaxAge        = 4
)

var (
	storageReadCallsCounter              = metrics.NewRegisteredCounter("state/storage/read/calls", nil)
	storageReadObjectCacheHitCounter     = metrics.NewRegisteredCounter("state/storage/read/object_cache_hit", nil)
	storageReadCreatedZeroCounter        = metrics.NewRegisteredCounter("state/storage/read/created_zero", nil)
	storageReadAccountMissingZeroCounter = metrics.NewRegisteredCounter("state/storage/read/account_missing_zero", nil)
	storageReadColdCounter               = metrics.NewRegisteredCounter("state/storage/read/cold", nil)
	storageReadColdFoundCounter          = metrics.NewRegisteredCounter("state/storage/read/cold_found", nil)
	storageReadColdMissingCounter        = metrics.NewRegisteredCounter("state/storage/read/cold_missing", nil)
	storageReadColdErrorCounter          = metrics.NewRegisteredCounter("state/storage/read/cold_error", nil)
	storageReadColdPendingCounter        = metrics.NewRegisteredCounter("state/storage/read/cold_pending_resolved", nil)

	storageOversizedReleaseCounter       = metrics.NewRegisteredCounter("state/storage/cache/oversized/releases", nil)
	storageOversizedReleasedSlotsCounter = metrics.NewRegisteredCounter("state/storage/cache/oversized/released_slots", nil)
	storageOversizedReleaseMaxGauge      = metrics.NewRegisteredGauge("state/storage/cache/oversized/max_slots", nil)
	storageOversizedSampledCounter       = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/sampled", nil)
	storageOversizedSampleSkippedCounter = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/skipped_contracts", nil)
	storageOversizedReloadedCounter      = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/reloaded", nil)
	storageOversizedReloadedAge1Counter  = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/reloaded_age1", nil)
	storageOversizedReloadedAge2Counter  = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/reloaded_age2", nil)
	storageOversizedReloadedAge3Counter  = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/reloaded_age3", nil)
	storageOversizedReloadedAge4Counter  = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/reloaded_age4", nil)
	storageOversizedUnreloadedCounter    = metrics.NewRegisteredCounter("state/storage/cache/oversized_sample/unreloaded", nil)
)

// storageObservabilityBatch is execution-goroutine confined. GetState's hot
// cache-hit path performs one ordinary increment; all registered metrics are
// updated once at a successful block commit rather than atomically per SLOAD.
// Calls is derived at flush time from the mutually-exclusive first-level
// outcomes, avoiding a second increment on every read.
type storageObservabilityBatch struct {
	objectCacheHits     int64
	createdZero         int64
	accountMissingZero  int64
	coldReads           int64
	coldFound           int64
	coldMissing         int64
	coldErrors          int64
	coldPendingResolved int64

	oversizedReleases      int64
	oversizedReleasedSlots int64
	oversizedReleaseMax    int64
	oversizedSampled       int64
	oversizedSampleSkipped int64
	oversizedReloaded      int64
	oversizedReloadedByAge [oversizedStorageSampleMaxAge]int64
	oversizedUnreloaded    int64
}

func (m storageObservabilityBatch) calls() int64 {
	return m.objectCacheHits + m.createdZero + m.accountMissingZero + m.coldReads
}

// oversizedStorageSample is an exact, fixed-capacity sample. At the configured
// maximum it retains 64*128*32 = 256 KiB of key bytes; the 64 small records and
// address map keep the entire diagnostic comfortably below 0.5 MiB.
type oversizedStorageSample struct {
	object            *stateObject
	accountGeneration uint64
	releaseGeneration uint64
	keys              [oversizedStorageSampleKeyLimit]tcommon.Hash
	keyCount          uint8
}

func incStorageMetric(counter *metrics.Counter, value int64) {
	if value != 0 {
		counter.Inc(value)
	}
}

// flushStorageObservability publishes one execution-confined batch. It is
// called only after a successful Commit, so failed/retried execution does not
// reset or partially publish a block's measurements.
func (s *StateDB) flushStorageObservability() {
	if s == nil {
		return
	}
	m := s.storageObservability
	incStorageMetric(storageReadCallsCounter, m.calls())
	incStorageMetric(storageReadObjectCacheHitCounter, m.objectCacheHits)
	incStorageMetric(storageReadCreatedZeroCounter, m.createdZero)
	incStorageMetric(storageReadAccountMissingZeroCounter, m.accountMissingZero)
	incStorageMetric(storageReadColdCounter, m.coldReads)
	incStorageMetric(storageReadColdFoundCounter, m.coldFound)
	incStorageMetric(storageReadColdMissingCounter, m.coldMissing)
	incStorageMetric(storageReadColdErrorCounter, m.coldErrors)
	incStorageMetric(storageReadColdPendingCounter, m.coldPendingResolved)
	incStorageMetric(storageOversizedReleaseCounter, m.oversizedReleases)
	incStorageMetric(storageOversizedReleasedSlotsCounter, m.oversizedReleasedSlots)
	if m.oversizedReleaseMax != 0 {
		storageOversizedReleaseMaxGauge.UpdateIfGt(m.oversizedReleaseMax)
	}
	incStorageMetric(storageOversizedSampledCounter, m.oversizedSampled)
	incStorageMetric(storageOversizedSampleSkippedCounter, m.oversizedSampleSkipped)
	incStorageMetric(storageOversizedReloadedCounter, m.oversizedReloaded)
	incStorageMetric(storageOversizedReloadedAge1Counter, m.oversizedReloadedByAge[0])
	incStorageMetric(storageOversizedReloadedAge2Counter, m.oversizedReloadedByAge[1])
	incStorageMetric(storageOversizedReloadedAge3Counter, m.oversizedReloadedByAge[2])
	incStorageMetric(storageOversizedReloadedAge4Counter, m.oversizedReloadedByAge[3])
	incStorageMetric(storageOversizedUnreloadedCounter, m.oversizedUnreloaded)
	s.storageObservability = storageObservabilityBatch{}
}

// recordOversizedStorageRelease records the threshold-driven cache discard and
// installs an exact sample of at most 128 keys. Map iteration gives a cheap,
// non-prefix-biased subset without allocating an intermediate key slice.
func (s *StateDB) recordOversizedStorageRelease(obj *stateObject) {
	if s == nil || obj == nil || len(obj.storage) <= maxStateObjectCachedStorageSlots {
		return
	}
	slots := int64(len(obj.storage))
	s.storageObservability.oversizedReleases++
	s.storageObservability.oversizedReleasedSlots += slots
	if slots > s.storageObservability.oversizedReleaseMax {
		s.storageObservability.oversizedReleaseMax = slots
	}

	// A second oversized release supersedes the old observation for this
	// account. Settle its remaining exact keys before installing the new sample.
	s.settleOversizedStorageSample(obj.address)
	if len(s.oversizedStorageSamples) >= oversizedStorageSampleContractLimit {
		s.storageObservability.oversizedSampleSkipped++
		return
	}
	if s.oversizedStorageSamples == nil {
		s.oversizedStorageSamples = make(map[tcommon.Address]*oversizedStorageSample, oversizedStorageSampleContractLimit)
	}
	sample := new(oversizedStorageSample)
	sample.object = obj
	sample.accountGeneration = obj.accountKVGeneration
	sample.releaseGeneration = s.stateObjectWorkingGeneration
	for key := range obj.storage {
		sample.keys[sample.keyCount] = key
		sample.keyCount++
		if int(sample.keyCount) == oversizedStorageSampleKeyLimit {
			break
		}
	}
	if sample.keyCount == 0 {
		return
	}
	slices.SortFunc(sample.keys[:sample.keyCount], func(a, b tcommon.Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	s.oversizedStorageSamples[obj.address] = sample
	s.storageObservability.oversizedSampled += int64(sample.keyCount)
}

// recordOversizedStorageReload is called only after a cold storage read
// succeeds (found or missing). Errors remain retryable and therefore retain
// the sampled key. Removing a match makes each reported reload exact and
// one-shot.
func (s *StateDB) recordOversizedStorageReload(obj *stateObject, key tcommon.Hash) {
	if s == nil || obj == nil || len(s.oversizedStorageSamples) == 0 {
		return
	}
	sample := s.oversizedStorageSamples[obj.address]
	if sample == nil {
		return
	}
	if sample.object != obj || sample.accountGeneration != obj.accountKVGeneration {
		s.settleOversizedStorageSample(obj.address)
		return
	}
	if s.stateObjectWorkingGeneration < sample.releaseGeneration {
		return
	}
	age := s.stateObjectWorkingGeneration - sample.releaseGeneration
	if age == 0 {
		return
	}
	if age > oversizedStorageSampleMaxAge {
		s.settleOversizedStorageSample(obj.address)
		return
	}
	keys := sample.keys[:sample.keyCount]
	i, found := slices.BinarySearchFunc(keys, key, func(a, b tcommon.Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	if !found {
		return
	}
	copy(keys[i:], keys[i+1:])
	keys[len(keys)-1] = tcommon.Hash{}
	sample.keyCount--
	s.storageObservability.oversizedReloaded++
	s.storageObservability.oversizedReloadedByAge[age-1]++
	if sample.keyCount == 0 {
		delete(s.oversizedStorageSamples, obj.address)
	}
}

// settleOversizedStorageSample accounts every exact key that was not reloaded
// before its observation ends (expiry, account eviction/replacement, or a new
// oversized release) and removes the record so stale account identities cannot
// grow the bounded map or create false reuse hits.
func (s *StateDB) settleOversizedStorageSample(addr tcommon.Address) {
	if s == nil || len(s.oversizedStorageSamples) == 0 {
		return
	}
	sample := s.oversizedStorageSamples[addr]
	if sample == nil {
		return
	}
	s.storageObservability.oversizedUnreloaded += int64(sample.keyCount)
	delete(s.oversizedStorageSamples, addr)
}

// expireOversizedStorageSamples runs once per successful block. Age four is
// settled after that block's execution, so reloads from each of ages 1..4 have
// a full opportunity to be observed. Identity/generation checks also settle
// objects replaced indirectly by journal replay.
func (s *StateDB) expireOversizedStorageSamples() {
	for addr, sample := range s.oversizedStorageSamples {
		obj := s.stateObjects[addr]
		if obj != sample.object || obj == nil || obj.accountKVGeneration != sample.accountGeneration {
			s.settleOversizedStorageSample(addr)
			continue
		}
		if s.stateObjectWorkingGeneration >= sample.releaseGeneration && s.stateObjectWorkingGeneration-sample.releaseGeneration >= oversizedStorageSampleMaxAge {
			s.settleOversizedStorageSample(addr)
		}
	}
}
