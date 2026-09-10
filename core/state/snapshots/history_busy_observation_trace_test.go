package snapshots

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

type busyHistoryPressurePoint struct {
	at     time.Time
	sample maintenance.StoragePressure
}

type busyHistoryTraceState struct {
	elapsed            time.Duration
	level, good, rises int
	hard, cpuBurst     bool
	reasons            historyLoadReason
	sequence           uint64
	acceptedAt         time.Time
}

func makeBusyHistoryPressureTrace(until time.Duration, change func(time.Duration, *maintenance.StoragePressure)) []busyHistoryPressurePoint {
	start := time.Unix(1_700_000_000, 123)
	var trace []busyHistoryPressurePoint
	for elapsed := time.Duration(0); elapsed <= until; elapsed += 5 * time.Second {
		at := start.Add(elapsed)
		p := healthyHistoryLoad(at)
		change(elapsed, &p)
		trace = append(trace, busyHistoryPressurePoint{at: at, sample: p})
	}
	return trace
}

// Both consumers receive samples from the same immutable producer trace. A
// slower consumer observes only the newest available point; it does not replay
// the intervening samples or manufacture independent observations by polling.
func consumeBusyHistoryPressureTrace(t *testing.T, trace []busyHistoryPressurePoint, interval time.Duration) []busyHistoryTraceState {
	t.Helper()
	r := historyLoadRegressionRunner(t)
	r.historyLoad.level = 0
	var current maintenance.StoragePressure
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return current }
	var observed []busyHistoryTraceState
	for _, point := range trace {
		elapsed := point.at.Sub(trace[0].at)
		if elapsed%interval != 0 {
			continue
		}
		current = point.sample
		r.refreshHistoryLoad(point.at)
		s := &r.historyLoad
		observed = append(observed, busyHistoryTraceState{
			elapsed: elapsed, level: s.level, good: s.good, rises: s.debtRises,
			hard: s.hard, cpuBurst: s.cpuBurstReady(point.at), reasons: s.reasons,
			sequence: s.acceptedSequence, acceptedAt: s.lastAccepted,
		})
	}
	return observed
}

func firstBusyHistoryTraceState(t *testing.T, observed []busyHistoryTraceState, match func(busyHistoryTraceState) bool) busyHistoryTraceState {
	t.Helper()
	for _, state := range observed {
		if match(state) {
			return state
		}
	}
	t.Fatal("expected budget transition did not occur")
	return busyHistoryTraceState{}
}

func TestHistoryBusyObservationTraceRecoveryCadence(t *testing.T) {
	trace := makeBusyHistoryPressureTrace(3*time.Minute, func(elapsed time.Duration, p *maintenance.StoragePressure) {
		p.WriteStalled = elapsed == 0
		p.StallCount = 1 // Clearing the active stall does not reset its counter.
	})
	for _, tc := range []struct {
		name                          string
		interval, recovering, healthy time.Duration
	}{
		{"five second consumer", 5 * time.Second, 10 * time.Second, 15 * time.Second},
		{"minute consumer", time.Minute, 2 * time.Minute, 3 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			states := consumeBusyHistoryPressureTrace(t, trace, tc.interval)
			if states[0].level != 1 || !states[0].hard || states[0].good != 0 || states[0].sequence != 1 {
				t.Fatalf("initial pressure baseline: %+v", states[0])
			}
			if states[1].level != 1 || states[1].hard || states[1].good != 1 || states[1].sequence != 2 || states[1].reasons != historyLoadReasonRecoverySamples|historyLoadReasonPressureHysteresis {
				t.Fatalf("one fresh good sample bypassed hysteresis: %+v", states[1])
			}
			recovering := firstBusyHistoryTraceState(t, states, func(s busyHistoryTraceState) bool { return s.level == 2 })
			if recovering.elapsed != tc.recovering || recovering.good != 2 || recovering.sequence != 3 {
				t.Fatalf("recovering transition: %+v, want elapsed %s with two good observations", recovering, tc.recovering)
			}
			healthy := firstBusyHistoryTraceState(t, states, func(s busyHistoryTraceState) bool { return s.level == 3 })
			if healthy.elapsed != tc.healthy || healthy.good != 3 || healthy.sequence != 4 {
				t.Fatalf("healthy transition: %+v, want elapsed %s with three good observations", healthy, tc.healthy)
			}
		})
	}
}

func TestHistoryBusyObservationTraceDebtGrowthCadence(t *testing.T) {
	trace := makeBusyHistoryPressureTrace(2*time.Minute, func(elapsed time.Duration, p *maintenance.StoragePressure) {
		p.CompactionDebt = 3<<30 + uint64(elapsed/(5*time.Second))*(1<<20)
	})
	for _, tc := range []struct {
		name               string
		interval, pressure time.Duration
	}{
		{"five second consumer", 5 * time.Second, 10 * time.Second},
		{"minute consumer", time.Minute, 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			states := consumeBusyHistoryPressureTrace(t, trace, tc.interval)
			if states[1].level != 2 || states[1].rises != 1 || states[1].good != 0 || states[1].reasons&historyLoadReasonDebtGrowth != 0 {
				t.Fatalf("one observed debt rise became sustained pressure: %+v", states[1])
			}
			pressure := firstBusyHistoryTraceState(t, states, func(s busyHistoryTraceState) bool {
				return s.reasons&historyLoadReasonDebtGrowth != 0
			})
			if pressure.elapsed != tc.pressure || pressure.level != 1 || pressure.hard || pressure.rises != 2 || pressure.good != 0 || pressure.sequence != 3 {
				t.Fatalf("debt pressure transition: %+v, want elapsed %s after two observed rises", pressure, tc.pressure)
			}
		})
	}
}

func TestHistoryBusyObservationTraceAcceptanceSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   string
		change func(time.Duration, *maintenance.StoragePressure)
	}{
		{"unavailable engine", "unavailable", func(_ time.Duration, p *maintenance.StoragePressure) { p.Available = false }},
		{"stale engine", "stale", func(_ time.Duration, p *maintenance.StoragePressure) {
			p.SampledAt = p.SampledAt.Add(-16 * time.Second)
		}},
		{"future engine", "stale", func(_ time.Duration, p *maintenance.StoragePressure) { p.SampledAt = p.SampledAt.Add(2 * time.Second) }},
		{"repeated engine with fresh device", "repeated", func(elapsed time.Duration, p *maintenance.StoragePressure) { p.SampledAt = p.SampledAt.Add(-elapsed) }},
		{"unavailable after accepted baseline", "outage", func(elapsed time.Duration, p *maintenance.StoragePressure) { p.Available = elapsed == 0 }},
		{"unknown device", "device", func(_ time.Duration, p *maintenance.StoragePressure) { p.DeviceAvailable = false }},
		{"stale device", "device", func(_ time.Duration, p *maintenance.StoragePressure) {
			p.DeviceSampledAt = p.DeviceSampledAt.Add(-16 * time.Second)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trace := makeBusyHistoryPressureTrace(3*time.Minute, tc.change)
			for _, interval := range []time.Duration{5 * time.Second, time.Minute} {
				t.Run(interval.String(), func(t *testing.T) {
					states := consumeBusyHistoryPressureTrace(t, trace, interval)
					for index, state := range states {
						if state.hard || state.rises != 0 {
							t.Fatalf("invalid observation invented pressure: %+v", state)
						}
						wantLevel, wantGood := 0, 0
						var wantSequence uint64
						var wantAccepted time.Time
						var wantReasons historyLoadReason
						switch tc.kind {
						case "unavailable":
							wantReasons = historyLoadReasonEngineUnavailable
						case "stale":
							wantReasons = historyLoadReasonEngineStale
						case "repeated", "outage":
							wantSequence, wantAccepted = 1, trace[0].at
							fresh := state.elapsed == 0 || tc.kind == "repeated" && state.elapsed <= 15*time.Second
							if fresh {
								wantLevel, wantGood, wantReasons = 2, 1, historyLoadReasonRecoverySamples
							} else if tc.kind == "outage" {
								wantReasons = historyLoadReasonEngineUnavailable
							} else {
								wantReasons = historyLoadReasonEngineStale
							}
						case "device":
							wantLevel, wantGood = 2, min(3, index+1)
							wantSequence, wantAccepted = uint64(index+1), trace[0].at.Add(state.elapsed)
							wantReasons = historyLoadReasonDeviceUnknown
							if wantGood < 3 {
								wantReasons |= historyLoadReasonRecoverySamples
							}
							if state.cpuBurst {
								t.Fatalf("unobserved device admitted CPU burst: %+v", state)
							}
						}
						if state.level != wantLevel || state.good != wantGood || state.sequence != wantSequence || !state.acceptedAt.Equal(wantAccepted) || state.reasons != wantReasons {
							t.Fatalf("acceptance changed at %s: %+v, want level/good/sequence=%d/%d/%d accepted=%s reasons=%d", state.elapsed, state, wantLevel, wantGood, wantSequence, wantAccepted, wantReasons)
						}
						if state.level == 0 && state.cpuBurst {
							t.Fatalf("invalid engine sample admitted CPU burst: %+v", state)
						}
					}
				})
			}
		})
	}
}
