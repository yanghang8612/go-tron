package snapshots

import (
	"time"

	"github.com/ethereum/go-ethereum/metrics"
)

// All eligibility and counters are owned by Runner.passMu. This lane is
// independent of good/debtModerate/cpuBurst and never grants parallel capacity.
type historyModerateL0State struct {
	samples                                  int
	threshold, stop                          int
	selected, attempts, published, completed *metrics.Counter
}

func (s *historyLoadState) initModerateL0Metrics(namespace string) {
	s.moderateL0.selected = metrics.GetOrRegisterCounter(namespace+"moderate_l0/selected", nil)
	s.moderateL0.attempts = metrics.GetOrRegisterCounter(namespace+"moderate_l0/attempts", nil)
	s.moderateL0.published = metrics.GetOrRegisterCounter(namespace+"moderate_l0/publications", nil)
	s.moderateL0.completed = metrics.GetOrRegisterCounter(namespace+"moderate_l0/completed", nil)
}

func (s *historyLoadState) moderateL0Evidence(now time.Time) bool {
	p := s.sample
	if s.hard || !p.Available || !freshHistoryLoad(p.SampledAt, now) || p.HardLimitReached(now) ||
		!p.DeviceAvailable || !freshHistoryLoad(p.DeviceSampledAt, now) || p.DeviceAwait < 0 || p.DeviceAwait > 2*time.Millisecond ||
		p.L0CompactionThreshold <= 0 || p.L0StopWritesThreshold <= 0 || p.L0Sublevels < 0 {
		return false
	}
	// Healthy points below T count too, so T-1/T oscillation cannot create a
	// second starvation boundary. Selection itself still requires L0>=T.
	return p.L0Sublevels < p.L0CompactionThreshold || p.L0Sublevels-p.L0CompactionThreshold < p.L0CompactionThreshold
}

func (s *historyLoadState) updateModerateL0(now time.Time, newSample, comparable, reset, newStall, devicePressure, severe bool) {
	m := &s.moderateL0
	p := s.sample
	sameThresholds := m.threshold == p.L0CompactionThreshold && m.stop == p.L0StopWritesThreshold
	if reset || newStall || devicePressure || severe || !s.moderateL0Evidence(now) {
		m.samples = 0
		return
	}
	if !sameThresholds {
		m.samples = 0
	}
	if newSample {
		if !comparable {
			m.samples = 0
		}
		m.threshold, m.stop = p.L0CompactionThreshold, p.L0StopWritesThreshold
		m.samples = min(2, m.samples+1)
	}
}

func (s *historyLoadState) moderateL0Ready(now time.Time) bool {
	return s.moderateL0.samples >= 2 && s.moderateL0Evidence(now) &&
		s.moderateL0.threshold == s.sample.L0CompactionThreshold && s.moderateL0.stop == s.sample.L0StopWritesThreshold &&
		s.sample.L0Sublevels >= s.sample.L0CompactionThreshold
}

func (s *historyLoadState) revokeExpiredModerateL0(now time.Time) {
	if !s.moderateL0Evidence(now) {
		s.moderateL0.samples = 0
		s.metric("moderate_l0_samples", 0)
		s.metric("moderate_l0_ready", 0)
	}
}

// Revalidate only an already selected tier, after range/source planning and
// before acquiring its lease. A still-legal selection remains latched even if
// the global controller has since recovered to a faster classification.
func (r *Runner) revalidateModerateL0Pass(result *PassResult) bool {
	if !result.HistoryModerateL0 {
		return true
	}
	r.historyLoad.revokeExpiredModerateL0(time.Now())
	ready := r.busyHistoryObservationEnabled() && r.historyLoad.moderateL0Ready(time.Now())
	if ready {
		resourceReady := r.cfg.BusyHistoryBuildReady()
		r.historyLoad.revokeExpiredModerateL0(time.Now())
		ready = resourceReady && r.historyLoad.moderateL0Ready(time.Now())
	}
	if ready {
		return true
	}
	result.HistoryModerateL0 = false
	if !result.HistoryBusyResourceReady {
		// Above-busy or pressure admission already proved the old fallback.
		return true
	}
	result.HistoryBusyResourceReady, result.HistoryForcedBusy = false, false
	result.HistoryDeferred = true
	r.armBusyHistoryObservation(result, time.Now())
	return false
}

func (r *Runner) moderateL0Recovery(work time.Duration, failed bool) time.Duration {
	work = max(time.Duration(0), work)
	// ceil(2*work/3) = work-floor(work/3), without overflowing 2*work.
	recovery := max(work-work/3, 3*time.Second, r.cfg.CatchupHeavyWorkCooldown)
	if failed {
		recovery = max(recovery, time.Minute, r.cfg.CatchupBuildMinInterval)
	}
	return recovery
}

func (s *historyLoadState) recordModerateL0Pass(result PassResult) {
	m := &s.moderateL0
	if !result.HistoryModerateL0 {
		return
	}
	if m.selected != nil {
		m.selected.Inc(1)
	}
	if !result.HistoryBuildAttempted {
		return
	}
	if m.attempts != nil {
		m.attempts.Inc(1)
	}
	if result.Built && m.published != nil {
		m.published.Inc(1)
	}
}
