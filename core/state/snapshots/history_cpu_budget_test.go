package snapshots

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryCPUBudgetRequiresFreshLowLatencyDevice(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*historyLoadState)
	}{
		{"missing engine", func(s *historyLoadState) { s.sample.Available = false }},
		{"stale engine", func(s *historyLoadState) { s.sample.SampledAt = time.Now().Add(-16 * time.Second) }},
		{"missing device", func(s *historyLoadState) { s.sample.DeviceAvailable = false }},
		{"stale device", func(s *historyLoadState) { s.sample.DeviceSampledAt = time.Now().Add(-16 * time.Second) }},
		{"future device", func(s *historyLoadState) { s.sample.DeviceSampledAt = time.Now().Add(2 * time.Second) }},
		{"latency", func(s *historyLoadState) { s.sample.DeviceAwait = 3 * time.Millisecond }},
		{"invalid latency", func(s *historyLoadState) { s.sample.DeviceAwait = -time.Nanosecond }},
		{"soft pressure", func(s *historyLoadState) { s.level = 1 }},
		{"hard pressure", func(s *historyLoadState) { s.hard = true }},
		{"fresh write stall", func(s *historyLoadState) { s.sample.WriteStalled = true }},
		{"fresh memtable pressure", func(s *historyLoadState) { s.sample.MemTableCount = 3 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := throughputFixture(t)
			r.historyLoad.level = 3
			r.historyLoad.sample = healthyHistoryLoad(time.Now())
			tc.change(&r.historyLoad)
			if r.historyLoad.cpuBurstReady(time.Now()) || r.historyLoad.dutyPPM() > 600_000 {
				t.Fatal("unobserved or pressured storage admitted CPU burst")
			}
			if got := r.throughputRecovery(time.Millisecond, false); got < 3*time.Second {
				t.Fatalf("conservative floor lost: %s", got)
			}
			if got := r.historyLeaseCooldown(); got != 3*time.Second {
				t.Fatalf("gate pause reduced without capacity: %s", got)
			}
		})
	}
}

func TestHistoryCPUBudgetKeepsRealWorkFailureAndExplicitPause(t *testing.T) {
	r := throughputFixture(t)
	r.historyLoad.sample = healthyHistoryLoad(time.Now())
	r.historyLoad.level = 2
	// A four-second complete batch used to receive eight seconds of rest.
	// The same measured work now receives one second while storage is healthy.
	if got := r.throughputRecovery(4*time.Second, false); got != time.Second {
		t.Fatalf("recovering storage throughput: %s", got)
	}
	r.historyLoad.level = 3
	if got := r.throughputRecovery(9*time.Second, false); got != time.Second {
		t.Fatalf("healthy storage throughput: %s", got)
	}
	if got := r.throughputRecovery(time.Millisecond, false); got != historyCPUBurstRecovery {
		t.Fatalf("parallel small job retains obsolete floor: %s", got)
	}
	if got := r.throughputRecovery(9*time.Hour, false); got != time.Hour {
		t.Fatalf("oversized complete block lost measured cost: %s", got)
	}
	if got := r.throughputRecovery(time.Millisecond, true); got < time.Minute {
		t.Fatalf("failure lost backoff: %s", got)
	}
	r.cfg.CatchupHeavyWorkCooldown = 10 * time.Second
	if r.historyLeaseCooldown() != 10*time.Second || r.throughputRecovery(time.Millisecond, false) != 10*time.Second {
		t.Fatal("explicit long operator pause was shortened")
	}
	r.cfg.HistoryCatchupMode = HistoryCatchupBalanced
	r.cfg.CatchupHeavyWorkCooldown = 3 * time.Second
	if r.historyLeaseCooldown() != 3*time.Second {
		t.Fatal("balanced mode gate policy changed")
	}
}

func TestHistoryCPUBudgetRealPublicationKeepsDeadlineAndGate(t *testing.T) {
	r := throughputFixture(t)
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGateWithCooldown(15 * time.Second)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return healthyHistoryLoad(time.Now()) }
	r.historyLoad.level, r.historyLoad.good = 2, 2
	r.historyLoad.lastAccepted = time.Now().Add(-6 * time.Second)
	result, err := r.OnePass()
	if err != nil || !result.Built || !r.historyLoad.cpuBurstReady(time.Now()) {
		t.Fatalf("real history publication: %+v %v", result, err)
	}
	if result.HistoryMinRecovery < historyCPUBurstRecovery || result.HistoryMinRecovery >= 3*time.Second {
		t.Fatalf("small successful batch did not use short recovery: %s", result.HistoryMinRecovery)
	}
	if remaining := r.cfg.HeavyWorkGate.CooldownRemaining(); remaining <= 0 || remaining > historyCPUBurstRecovery {
		t.Fatalf("lease did not use matching short cooldown: %s", remaining)
	}
	deadline := r.historyNotBefore.Load()
	next, err := r.OnePass()
	if err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited || r.historyNotBefore.Load() != deadline {
		t.Fatalf("short deadline was bypassed or extended by retry: %+v %v", next, err)
	}
}
