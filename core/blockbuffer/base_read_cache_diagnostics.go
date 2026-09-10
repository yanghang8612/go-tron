package blockbuffer

import "github.com/ethereum/go-ethereum/metrics"

const (
	baseReadCacheReferenceCreditMask  uint32 = 3
	baseReadCacheReferenceSourceShift        = 28
	baseReadCacheReferenceForeground  uint32 = 1 << baseReadCacheReferenceSourceShift
	baseReadCacheReferencePrefetch    uint32 = 2 << baseReadCacheReferenceSourceShift
	baseReadCacheReferenceFlush       uint32 = 4 << baseReadCacheReferenceSourceShift
	baseReadCacheReferenceSourceMask         = baseReadCacheReferenceForeground | baseReadCacheReferencePrefetch | baseReadCacheReferenceFlush

	baseReadCacheDiagnosticDepthShift        = 26
	baseReadCacheDiagnosticDepthMask  uint32 = 3 << baseReadCacheDiagnosticDepthShift
	baseReadCacheDiagnosticDepths            = 2
	baseReadCacheDiagnosticSources           = 8
)

const (
	baseReadCacheDiagnosticProbationFirst = iota
	baseReadCacheDiagnosticProbationPassed
	baseReadCacheDiagnosticProbationRejected
	baseReadCacheDiagnosticWindowBypassed
	baseReadCacheDiagnosticEpochRejected
	baseReadCacheDiagnosticWindowCapacityRejected
	baseReadCacheDiagnosticFlushAdmission
	baseReadCacheDiagnosticReasons
)

const (
	baseReadCacheDiagnosticWindowPromoted = iota
	baseReadCacheDiagnosticWindowEvicted
	baseReadCacheDiagnosticTailEvicted
	baseReadCacheDiagnosticOutcomes
)

// These totals describe existing branches, not a reconstruction of which
// absent key was previously evicted. Probation counts fingerprint observations;
// a first observation may still be admitted to the window. Epoch rejection can
// include conservative slot collisions. Oversize values rejected before the
// shard lock are excluded so diagnostics do not acquire an extra lock.
type baseReadCacheDepthDiagnostics struct {
	reasons  [baseReadCacheDiagnosticReasons]uint64
	outcomes [baseReadCacheDiagnosticOutcomes][baseReadCacheDiagnosticSources]uint64
}

// Bucket zero is untracked; the stored one-based bucket uses unused atomic bits.
// Callers use the cache's schema-aware parser, including delta generation bytes.
func baseReadCacheDiagnosticDepth(depth int, commitment bool) uint8 {
	if commitment && depth >= 6 && depth <= 7 {
		return uint8(depth - 5)
	}
	return 0
}

func (s *baseReadCacheShard) recordDiagnosticReason(depth uint8, reason int) {
	if depth != 0 {
		s.diagnostics[depth-1].reasons[reason]++
	}
}

// s.mu is already held by admission. No extra key hash or table lookup occurs.
func (s *baseReadCacheShard) admitWithDiagnostics(key []byte, other bool, depth uint8) bool {
	admitted := s.admit(key, other)
	reason := baseReadCacheDiagnosticProbationFirst
	if admitted {
		reason = baseReadCacheDiagnosticProbationPassed
	}
	s.recordDiagnosticReason(depth, reason)
	return admitted
}

// Source combinations are mutually exclusive sets of reference() callers seen
// since admission. They are not independent hit counts or ownership of the
// remaining CLOCK credits. Initial admission sets no source: in particular a
// first-read window must not count its mandatory first read as foreground reuse.
// Flush refresh retains the residency history; retire/recycle clears it.
func (s *baseReadCacheShard) recordDiagnosticOutcome(entry *baseReadCacheEntry, outcome int) {
	state := entry.references.Load()
	depth := (state & baseReadCacheDiagnosticDepthMask) >> baseReadCacheDiagnosticDepthShift
	if depth != 0 {
		sources := (state & baseReadCacheReferenceSourceMask) >> baseReadCacheReferenceSourceShift
		s.diagnostics[depth-1].outcomes[outcome][sources]++
	}
}

func addBaseReadCacheDiagnostics(dst, src *[baseReadCacheDiagnosticDepths]baseReadCacheDepthDiagnostics) {
	for depth := range dst {
		for reason := range dst[depth].reasons {
			dst[depth].reasons[reason] += src[depth].reasons[reason]
		}
		for outcome := range dst[depth].outcomes {
			for sources := range dst[depth].outcomes[outcome] {
				dst[depth].outcomes[outcome][sources] += src[depth].outcomes[outcome][sources]
			}
		}
	}
}

var (
	baseReadCacheDiagnosticDepthNames  = [baseReadCacheDiagnosticDepths]string{"depth_6", "depth_7"}
	baseReadCacheDiagnosticSourceNames = [baseReadCacheDiagnosticSources]string{
		"none", "foreground", "prefetch", "foreground_prefetch", "flush", "foreground_flush", "prefetch_flush", "foreground_prefetch_flush",
	}
	baseReadCacheDiagnosticReasonNames = [baseReadCacheDiagnosticReasons]string{
		"probation/first_observation", "probation/passed", "fill/rejected_probation", "fill/rejected_window_sampling",
		"fill/rejected_epoch", "fill/rejected_window_capacity", "flush/observed_admission",
	}
	baseReadCacheDiagnosticOutcomeNames    = [baseReadCacheDiagnosticOutcomes]string{"window/promoted", "window/capacity_evicted", "tail/capacity_evicted"}
	baseReadCacheDiagnosticReasonGauges    = newBaseReadCacheDiagnosticReasonGauges()
	baseReadCacheDiagnosticOutcomeGauges   = newBaseReadCacheDiagnosticOutcomeGauges()
	commitmentParentDeepNoResidentCounters = newCommitmentParentDeepNoResidentCounters()
)

func newBaseReadCacheDiagnosticReasonGauges() [baseReadCacheDiagnosticDepths][baseReadCacheDiagnosticReasons]*metrics.Gauge {
	var gauges [baseReadCacheDiagnosticDepths][baseReadCacheDiagnosticReasons]*metrics.Gauge
	for depth, depthName := range baseReadCacheDiagnosticDepthNames {
		for reason, name := range baseReadCacheDiagnosticReasonNames {
			gauges[depth][reason] = metrics.NewRegisteredGauge("blockbuffer/base_cache/"+depthName+"/"+name, nil)
		}
	}
	return gauges
}

func newBaseReadCacheDiagnosticOutcomeGauges() [baseReadCacheDiagnosticDepths][baseReadCacheDiagnosticOutcomes][baseReadCacheDiagnosticSources]*metrics.Gauge {
	var gauges [baseReadCacheDiagnosticDepths][baseReadCacheDiagnosticOutcomes][baseReadCacheDiagnosticSources]*metrics.Gauge
	for depth, depthName := range baseReadCacheDiagnosticDepthNames {
		for outcome, name := range baseReadCacheDiagnosticOutcomeNames {
			for sources, sourceName := range baseReadCacheDiagnosticSourceNames {
				gauges[depth][outcome][sources] = metrics.NewRegisteredGauge("blockbuffer/base_cache/"+depthName+"/"+name+"/reference_sources/"+sourceName, nil)
			}
		}
	}
	return gauges
}

func newCommitmentParentDeepNoResidentCounters() [baseReadCacheDiagnosticDepths][2]*metrics.Counter {
	var counters [baseReadCacheDiagnosticDepths][2]*metrics.Counter
	for depth, depthName := range baseReadCacheDiagnosticDepthNames {
		for source, name := range []string{"foreground", "prefetch"} {
			counters[depth][source] = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/cache/miss/no_resident/"+depthName+"/"+name, nil)
		}
	}
	return counters
}

// Publish cache-owner lifetime totals with the existing low-frequency,
// eventually consistent occupancy snapshot. clear does not reset these totals.
func publishBaseReadCacheDiagnostics(stats [baseReadCacheDiagnosticDepths]baseReadCacheDepthDiagnostics) {
	for depth := range stats {
		for reason, count := range stats[depth].reasons {
			baseReadCacheDiagnosticReasonGauges[depth][reason].Update(int64(count))
		}
		for outcome := range stats[depth].outcomes {
			for sources, count := range stats[depth].outcomes[outcome] {
				baseReadCacheDiagnosticOutcomeGauges[depth][outcome][sources].Update(int64(count))
			}
		}
	}
}
