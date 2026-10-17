package snapshots

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

type historyCompactionOuterPhase int

const (
	historyCompactionOuterIdle historyCompactionOuterPhase = iota
	historyCompactionOuterPrepare
	historyCompactionOuterManifest
	historyCompactionOuterCandidates
	historyCompactionOuterSelect
	historyCompactionOuterRevalidate
	historyCompactionOuterClaim
	historyCompactionOuterInner
	historyCompactionOuterIntegrate
	historyCompactionOuterCleanup
	historyCompactionOuterPhaseCount
)

var historyCompactionOuterPhaseNames = [historyCompactionOuterPhaseCount]string{"idle", "prepare", "manifest", "candidates", "select", "revalidate", "claim", "inner", "integrate", "cleanup"}

// Outer scopes are individual CompactHistoryDomain calls, excluding lease wait
// and Runner recovery/drain bookkeeping. Inner codec time is nested, never added
// again. Cache stats count logical metadata reader invocations, not IO syscalls.
// Registers once per Runner using its actual namespace; standalone calls can
// leave the observer nil. No ticker, source IO, or per-candidate registration.
type historyCompactionOuterMetrics struct {
	active, phase, started, phaseStarted, lastDuration, maxDuration                   *metrics.Gauge
	attempts, totalDuration                                                           *metrics.Counter
	last, max                                                                         [historyCompactionOuterPhaseCount]*metrics.Gauge
	total, entered                                                                    [historyCompactionOuterPhaseCount]*metrics.Counter
	hintHits, hintMisses, hintReads, hintFresh, hintRetired, hintCapacity, hintResets *metrics.Counter
	hintEntries, hintBytes, hintUsed, candidates                                      *metrics.Gauge
}

func newHistoryCompactionOuterMetrics(namespace string) *historyCompactionOuterMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace)
	outer := namespace + "compaction/outer/"
	hint := namespace + "compaction/input_cost_hints/"
	gauge := func(name string) *metrics.Gauge { return metrics.GetOrRegisterGauge(name, nil) }
	counter := func(name string) *metrics.Counter { return metrics.GetOrRegisterCounter(name, nil) }
	m := &historyCompactionOuterMetrics{active: gauge(outer + "current/active"), phase: gauge(outer + "current/phase"), started: gauge(outer + "current/outer_started_unix_nano"), phaseStarted: gauge(outer + "current/phase_started_unix_nano"), lastDuration: gauge(outer + "last/duration"), maxDuration: gauge(outer + "max/duration"), totalDuration: counter(outer + "total/duration"), attempts: counter(outer + "attempts"), hintHits: counter(hint + "hits"), hintMisses: counter(hint + "misses"), hintReads: counter(hint + "metadata_reader_calls"), hintFresh: counter(hint + "fresh_reader_calls"), hintRetired: counter(hint + "evicted_retired"), hintCapacity: counter(hint + "capacity_reject"), hintResets: counter(hint + "resets"), hintEntries: gauge(hint + "entries"), hintBytes: gauge(hint + "charged_bytes"), hintUsed: gauge(outer + "last/hints_used"), candidates: gauge(outer + "last/candidates")}
	for p := historyCompactionOuterPrepare; p < historyCompactionOuterPhaseCount; p++ {
		name := historyCompactionOuterPhaseNames[p]
		m.last[p] = gauge(outer + "last/" + name + "/duration")
		m.max[p] = gauge(outer + "max/" + name + "/duration")
		m.total[p] = counter(outer + "total/" + name + "/duration")
		m.entered[p] = counter(outer + "entered/" + name)
	}
	return m
}

type historyCompactionOuterObservation struct {
	metrics               *historyCompactionOuterMetrics
	started, phaseStarted time.Time
	phase                 historyCompactionOuterPhase
	elapsed               [historyCompactionOuterPhaseCount]time.Duration
	costs                 *historyCompactionCostCache
	published             historyCompactionCostCacheStats
}

func (m *historyCompactionOuterMetrics) begin() *historyCompactionOuterObservation {
	if m == nil {
		return nil
	}
	now := time.Now()
	p := &historyCompactionOuterObservation{metrics: m, started: now, phaseStarted: now, phase: historyCompactionOuterPrepare}
	m.attempts.Inc(1)
	m.active.Update(1)
	m.phase.Update(int64(p.phase))
	m.started.Update(now.UnixNano())
	m.phaseStarted.Update(now.UnixNano())
	m.hintUsed.Update(0)
	m.candidates.Update(0)
	for i := historyCompactionOuterPrepare; i < historyCompactionOuterPhaseCount; i++ {
		m.last[i].Update(0)
	}
	m.entered[p.phase].Inc(1)
	return p
}
func (p *historyCompactionOuterObservation) observeCosts(c *historyCompactionCostCache) {
	if p == nil || c == nil {
		return
	}
	p.costs = c
	p.published = c.snapshot()
	p.metrics.hintUsed.Update(1)
}
func (p *historyCompactionOuterObservation) publishCosts() {
	if p == nil || p.costs == nil {
		return
	}
	s := p.costs.snapshot()
	old := p.published
	m := p.metrics
	for _, v := range []struct {
		now, old uint64
		counter  *metrics.Counter
	}{{s.Hits, old.Hits, m.hintHits}, {s.Misses, old.Misses, m.hintMisses}, {s.MetadataReads, old.MetadataReads, m.hintReads}, {s.FreshReads, old.FreshReads, m.hintFresh}, {s.EvictedRetired, old.EvictedRetired, m.hintRetired}, {s.CapacityRejects, old.CapacityRejects, m.hintCapacity}, {s.Resets, old.Resets, m.hintResets}} {
		if v.now >= v.old {
			v.counter.Inc(coldSnapshotUintGauge(v.now - v.old))
		}
	}
	m.hintEntries.Update(coldSnapshotUintGauge(s.Entries))
	m.hintBytes.Update(coldSnapshotUintGauge(s.ChargedBytes))
	p.published = s
}
func (p *historyCompactionOuterObservation) setPhase(phase historyCompactionOuterPhase) {
	if p == nil || phase == p.phase {
		return
	}
	now := time.Now()
	duration := now.Sub(p.phaseStarted)
	p.elapsed[p.phase] += duration
	p.metrics.total[p.phase].Inc(duration.Nanoseconds())
	p.metrics.last[p.phase].Update(p.elapsed[p.phase].Nanoseconds())
	p.metrics.max[p.phase].UpdateIfGt(p.elapsed[p.phase].Nanoseconds())
	p.publishCosts()
	p.phase = phase
	p.phaseStarted = now
	p.metrics.phase.Update(int64(phase))
	p.metrics.phaseStarted.Update(now.UnixNano())
	p.metrics.entered[phase].Inc(1)
}
func (p *historyCompactionOuterObservation) setCandidates(n int) {
	if p != nil {
		p.metrics.candidates.Update(int64(n))
	}
}
func (p *historyCompactionOuterObservation) finish() {
	if p == nil {
		return
	}
	now := time.Now()
	duration := now.Sub(p.phaseStarted)
	p.elapsed[p.phase] += duration
	p.metrics.total[p.phase].Inc(duration.Nanoseconds())
	p.metrics.last[p.phase].Update(p.elapsed[p.phase].Nanoseconds())
	p.metrics.max[p.phase].UpdateIfGt(p.elapsed[p.phase].Nanoseconds())
	p.publishCosts()
	elapsed := now.Sub(p.started).Nanoseconds()
	p.metrics.lastDuration.Update(elapsed)
	p.metrics.totalDuration.Inc(elapsed)
	p.metrics.maxDuration.UpdateIfGt(elapsed)
	p.metrics.started.Update(0)
	p.metrics.active.Update(0)
	p.metrics.phase.Update(int64(historyCompactionOuterIdle))
	p.metrics.phaseStarted.Update(0)
}
