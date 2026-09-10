package snapshots

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/core/maintenance"
)

func healthyHistoryLoad(now time.Time) maintenance.StoragePressure {
	return maintenance.StoragePressure{Available: true, SampledAt: now,
		L0Sublevels: 2, L0CompactionThreshold: 8, L0StopWritesThreshold: 192,
		MemTableCount: 1, MemTableStopWritesThreshold: 4,
		DeviceAvailable: true, DeviceSampledAt: now, DeviceBusyPPM: 500_000,
		DeviceQueueMilli: 500, DeviceAwait: time.Millisecond}
}

func TestHistoryLoadHysteresisAndFreshness(t *testing.T) {
	r := throughputFixture(t)
	now := time.Now()
	p := healthyHistoryLoad(now)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	step := func() { p.SampledAt, p.DeviceSampledAt = now, now; r.refreshHistoryLoad(now) }
	step()
	for i := 0; i < 20; i++ {
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.level != 2 || r.historyLoad.good != 1 {
		t.Fatal("reused sample raised budget", r.historyLoad)
	}
	for i := 0; i < 2; i++ {
		now = now.Add(5 * time.Second)
		step()
	}
	if r.historyLoad.level != 3 {
		t.Fatal("fresh healthy samples did not recover", r.historyLoad)
	}
	p.WriteStalled = true
	step()
	if !r.historyLoad.hard || r.historyLoad.level != 1 || r.historyLoad.good != 0 {
		t.Fatal("stall did not reduce budget", r.historyLoad)
	}
	p.WriteStalled = false
	now = now.Add(5 * time.Second)
	step()
	if r.historyLoad.level != 1 {
		t.Fatal("one good sample erased pressure", r.historyLoad)
	}
	now = now.Add(5 * time.Second)
	step()
	if r.historyLoad.level != 2 {
		t.Fatal("second good sample did not begin recovery", r.historyLoad)
	}
	now = now.Add(5 * time.Second)
	step()
	if r.historyLoad.level != 3 {
		t.Fatal("third good sample did not recover", r.historyLoad)
	}
	r.refreshHistoryLoad(now.Add(16 * time.Second))
	if r.historyLoad.level != 0 || r.historyLoad.hard {
		t.Fatal("stale sample treated as live", r.historyLoad)
	}
	now = now.Add(20 * time.Second)
	p.DeviceAvailable = false
	step()
	for i := 0; i < 4; i++ {
		now = now.Add(5 * time.Second)
		step()
	}
	if r.historyLoad.level != 2 {
		t.Fatal("unknown shared device admitted maximum budget", r.historyLoad)
	}
}

func TestHistoryLoadDebtAndSharedDevicePressure(t *testing.T) {
	r := throughputFixture(t)
	now := time.Now()
	p := healthyHistoryLoad(now)
	p.CompactionDebt = 3 << 30
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	r.refreshHistoryLoad(now)
	for i := 0; i < 2; i++ {
		now = now.Add(5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = now, now
		p.CompactionDebt += 1 << 20
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.level != 1 || r.historyLoad.hard {
		t.Fatal("growing debt was not soft pressure", r.historyLoad)
	}
	p.CompactionDebt = 0
	p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 999_000, 4000, 10*time.Millisecond
	now = now.Add(5 * time.Second)
	p.SampledAt, p.DeviceSampledAt = now, now
	r.refreshHistoryLoad(now)
	if r.historyLoad.level != 1 || r.historyLoad.hard {
		t.Fatal("shared queue signal", r.historyLoad)
	}
	// Busy alone does not imply the physical device's bandwidth limit.
	p.DeviceQueueMilli, p.DeviceAwait = 500, time.Millisecond
	for i := 0; i < 3; i++ {
		now = now.Add(5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = now, now
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.level != 3 {
		t.Fatal("busy alone starved cold work", r.historyLoad)
	}
}

func TestHistoryLoadDensityJumpShrinksBothBounds(t *testing.T) {
	r := throughputFixture(t)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return maintenance.StoragePressure{} }
	r.historyLoad.level = 3
	r.historyLoad.history = historyWorkSample{blocks: 1000, txnums: 100_000, work: 4 * time.Second, bytes: 64 << 20}
	b, tx := r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 1250 || tx != 125000 {
		t.Fatalf("growth exceeds 25%%: %d %d", b, tx)
	}
	// Model a density transition: the same block/tx count takes 100 times
	// longer and produces 100 times more bytes. No height-based heuristic.
	r.historyLoad.history.work *= 100
	r.historyLoad.history.bytes *= 100
	b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 20 || tx != 2000 {
		t.Fatalf("dense batch not reduced: %d %d", b, tx)
	}
	r.historyLoad.level = 1
	b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 5 || tx != 500 {
		t.Fatalf("pressure did not further reduce dense range: %d %d", b, tx)
	}
	r.historyLoad.history.work = time.Duration(math.MaxInt64)
	r.historyLoad.history.bytes = math.MaxUint64
	b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
	if b != 1 || tx != 1 {
		t.Fatalf("complete-block floor/overflow: %d %d", b, tx)
	}
	if r.adaptiveEventBatchLimit(65536) != 2 {
		t.Fatal("independent event gap started with full freezer segment")
	}
}

func TestHistoryHardPressureRetainsPruneOpportunity(t *testing.T) {
	r := throughputFixture(t)
	p := healthyHistoryLoad(time.Now())
	p.WriteStalled = true
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { p.SampledAt = time.Now(); return p }
	called := false
	result, err := r.OnePassWithMaintenanceContext(context.Background(), func(_ context.Context, result PassResult) error {
		called = true
		if !result.HistoryLoadDeferred || result.HistoryBuildAttempted {
			t.Fatal("load gate ran after expensive build")
		}
		return nil
	})
	if err != nil || !called || result.HistoryBuildAttempted || !result.HistoryLoadDeferred || result.Compaction.Merged || result.HistoryRetryAfter <= 0 {
		t.Fatalf("hard pressure/callback: %+v %v", result, err)
	}
	p.WriteStalled = false
	result, err = r.OnePass()
	if err != nil || !result.Built || result.HistoryBatchBlocks != 2 {
		t.Fatalf("recovery did not use a bounded initial slice: %+v %v", result, err)
	}
}

func TestHistoryRecoveryExcludesLongMergeButRecordsWholeMaintenance(t *testing.T) {
	r := throughputFixture(t)
	r.historyLoad.level = 3
	start := time.Now()
	result := PassResult{Built: true, HistoryBuildAttempted: true, HistoryForcedBusy: true, CompactionDuration: 15 * time.Minute}
	r.applyThroughputRecovery(&result, start, start.Add(15*time.Minute+6*time.Second), nil)
	if result.HistoryMaintenanceDuration != 15*time.Minute+6*time.Second || result.HistoryRecoveryCost != 6*time.Second || result.HistoryMinRecovery != 4*time.Second {
		t.Fatalf("merge poisoned history budget: %+v", result)
	}
	r.historyLoad.level = 0
	if got := r.throughputRecovery(120*time.Second, false); got != 480*time.Second {
		t.Fatal("oversized ordinary block lost its real recovery cost", got)
	}
	for _, level := range []int{0, 1, 2, 3} {
		r.historyLoad.level = level
		if got := r.throughputRecovery(time.Duration(math.MaxInt64), false); got <= time.Hour {
			t.Fatal("overflowed recovery", got)
		}
		if got := r.throughputRecovery(time.Millisecond, true); got < time.Minute {
			t.Fatal("failure did not back off", got)
		}
	}
}

func historyLoadMetricsFixture(t *testing.T, p *maintenance.StoragePressure) *Runner {
	t.Helper()
	r := &Runner{cfg: Config{MetricsNamespace: "test/" + t.Name(),
		HistoryLoadProbe: func() maintenance.StoragePressure { return *p }}}
	r.initHistoryLoadMetrics()
	prefix := r.cfg.MetricsNamespace + "/history/budget/"
	t.Cleanup(func() {
		for name := range r.historyLoad.metrics {
			metrics.Unregister(prefix + name)
		}
		for _, reason := range historyLoadReasonMetrics {
			metrics.Unregister(prefix + "reason/" + reason.name + "/entered")
		}
	})
	return r
}

func assertHistoryLoadReasonEntries(t *testing.T, r *Runner, want map[string]int64) {
	t.Helper()
	for _, reason := range historyLoadReasonMetrics {
		name := r.cfg.MetricsNamespace + "/history/budget/reason/" + reason.name + "/entered"
		counter, ok := metrics.DefaultRegistry.Get(name).(*metrics.Counter)
		if !ok {
			t.Fatalf("missing counter %s", name)
		}
		if got := counter.Snapshot().Count(); got != want[reason.name] {
			t.Errorf("counter %s = %d, want %d", name, got, want[reason.name])
		}
	}
}

func TestHistoryLoadReasonMetricsPressureThresholds(t *testing.T) {
	// A historical instant keeps dutyPPM's wall-clock CPU-burst check disabled;
	// the scheduler's existing 20/33/60 percent targets are checked below.
	now := time.Unix(1_700_000_000, 123)
	for _, tc := range []struct {
		name   string
		change func(*maintenance.StoragePressure)
		bits   int64
		level  int
		hard   bool
		good   int
	}{
		{"write stalled", func(p *maintenance.StoragePressure) { p.WriteStalled = true }, 1, 1, true, 0},
		{"memtable hard boundary", func(p *maintenance.StoragePressure) { p.MemTableCount = 3 }, 1, 1, true, 0},
		{"memtable below boundary", func(p *maintenance.StoragePressure) { p.MemTableCount = 2 }, 128, 2, false, 1},
		{"L0 hard boundary", func(p *maintenance.StoragePressure) { p.L0Sublevels = 144 }, 1 | 8, 1, true, 0},
		{"L0 below hard boundary", func(p *maintenance.StoragePressure) { p.L0Sublevels = 143 }, 8, 1, false, 0},
		{"L0 soft boundary", func(p *maintenance.StoragePressure) { p.L0Sublevels = 16 }, 8, 1, false, 0},
		{"L0 below soft boundary", func(p *maintenance.StoragePressure) { p.L0Sublevels = 15 }, 128, 2, false, 0},
		{"L0 low boundary", func(p *maintenance.StoragePressure) { p.L0Sublevels = 8 }, 128, 2, false, 0},
		{"L0 below low boundary", func(p *maintenance.StoragePressure) { p.L0Sublevels = 7 }, 128, 2, false, 1},
		{"L0 threshold overflow", func(p *maintenance.StoragePressure) {
			p.L0Sublevels, p.L0CompactionThreshold, p.L0StopWritesThreshold = math.MaxInt, math.MaxInt, 0
		}, 128, 2, false, 0},
		{"device pressure boundary", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 950_000, 2_000, 5*time.Millisecond
		}, 4, 1, false, 0},
		{"device below busy boundary", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 949_999, 2_000, 5*time.Millisecond
		}, 128, 2, false, 1},
		{"device below queue boundary", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 950_000, 1_999, 5*time.Millisecond
		}, 128, 2, false, 1},
		{"device below await boundary", func(p *maintenance.StoragePressure) {
			p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 950_000, 2_000, 5*time.Millisecond-time.Nanosecond
		}, 128, 2, false, 1},
		{"device unavailable", func(p *maintenance.StoragePressure) { p.DeviceAvailable = false }, 128 | 512, 2, false, 1},
		{"device stale", func(p *maintenance.StoragePressure) { p.DeviceSampledAt = now.Add(-16 * time.Second) }, 128 | 512, 2, false, 1},
		{"engine unavailable", func(p *maintenance.StoragePressure) { p.Available = false; p.WriteStalled = true }, 32, 0, false, 0},
		{"engine timestamp missing", func(p *maintenance.StoragePressure) { p.SampledAt = time.Time{} }, 64, 0, false, 0},
		{"engine stale", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(-16 * time.Second) }, 64, 0, false, 0},
		{"engine future", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(time.Second + time.Nanosecond) }, 64, 0, false, 0},
		{"engine freshness boundary", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(-15 * time.Second) }, 128, 2, false, 1},
		{"engine future boundary", func(p *maintenance.StoragePressure) { p.SampledAt = now.Add(time.Second) }, 128, 2, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := healthyHistoryLoad(now)
			tc.change(&p)
			r := historyLoadMetricsFixture(t, &p)
			r.refreshHistoryLoad(now)
			s := &r.historyLoad
			if s.level != tc.level || s.hard != tc.hard || s.good != tc.good {
				t.Fatalf("decision level/hard/good = %d/%t/%d, want %d/%t/%d", s.level, s.hard, s.good, tc.level, tc.hard, tc.good)
			}
			wantDuty := int64(200_000)
			if tc.level == 2 {
				wantDuty = 333_333
			}
			wantAccepted, wantAt := int64(0), int64(0)
			if tc.level != 0 {
				wantAccepted, wantAt = 1, p.SampledAt.UnixNano()
			}
			for name, want := range map[string]int64{
				"reason_bits": tc.bits, "level": int64(tc.level), "hard": boolGauge(tc.hard), "duty_ppm": wantDuty,
				"l0_sublevels": int64(p.L0Sublevels), "l0_compaction_threshold": int64(p.L0CompactionThreshold),
				"l0_stop_writes_threshold": int64(p.L0StopWritesThreshold), "debt_rises": 0, "good": int64(tc.good),
				"sample_accepted": wantAccepted, "accepted_sequence": wantAccepted, "accepted_sample_unix_nano": wantAt,
			} {
				assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/"+name, want)
			}
			entries := make(map[string]int64)
			for _, reason := range historyLoadReasonMetrics {
				if tc.bits&int64(reason.bit) != 0 {
					entries[reason.name] = 1
				}
			}
			assertHistoryLoadReasonEntries(t, r, entries)
			// A repeated observation cannot create an independent good point or
			// inflate reason activation counters, including instantaneous pressure.
			for range 4 {
				r.refreshHistoryLoad(now)
			}
			assertHistoryLoadReasonEntries(t, r, entries)
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/sample_accepted", 0)
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/accepted_sequence", wantAccepted)
		})
	}
}

func TestHistoryLoadReasonMetricsRecoveryAndAcceptedObservations(t *testing.T) {
	now := time.Unix(1_700_000_000, 123)
	p := healthyHistoryLoad(now)
	r := historyLoadMetricsFixture(t, &p)
	step := func(label string, advance time.Duration, stamp bool, level, good, debtRises int, bits int64, sequence uint64, accepted bool) {
		t.Helper()
		now = now.Add(advance)
		if stamp {
			p.SampledAt, p.DeviceSampledAt = now, now
		}
		previousAt := r.historyLoad.lastAccepted
		r.refreshHistoryLoad(now)
		s := &r.historyLoad
		if s.level != level || s.good != good || s.debtRises != debtRises || s.acceptedSequence != sequence {
			t.Fatalf("%s: level/good/rises/sequence = %d/%d/%d/%d, want %d/%d/%d/%d", label,
				s.level, s.good, s.debtRises, s.acceptedSequence, level, good, debtRises, sequence)
		}
		wantAt := previousAt
		if accepted {
			wantAt = p.SampledAt
		}
		for name, want := range map[string]int64{
			"reason_bits": bits, "good": int64(good), "debt_rises": int64(debtRises),
			"sample_accepted": boolGauge(accepted), "accepted_sequence": int64(sequence),
			"accepted_sample_unix_nano": wantAt.UnixNano(),
		} {
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/"+name, want)
		}
	}

	step("first independent sample", 0, true, 2, 1, 0, 128, 1, true)
	step("same sample", 0, false, 2, 1, 0, 128, 1, false)
	step("new timestamp below acceptance interval", time.Second, true, 2, 1, 0, 128, 1, false)
	step("second independent sample", 4*time.Second, true, 2, 2, 0, 128, 2, true)
	step("healthy", 5*time.Second, true, 3, 3, 0, 0, 3, true)
	if r.historyLoad.dutyPPM() != 600_000 {
		t.Fatal("healthy duty changed")
	}

	// A completed stall between accepted samples must be visible immediately.
	p.StallCount++
	step("new stall on reused timestamp", 0, false, 1, 0, 0, 2, 3, false)
	step("same stall", 0, false, 1, 0, 0, 2, 3, false)
	step("accept stall baseline", 5*time.Second, true, 1, 0, 0, 2, 4, true)
	step("pressure cleared but no independent good point", 0, false, 1, 0, 0, 128|256, 4, false)
	step("one good point remains pressured", 5*time.Second, true, 1, 1, 0, 128|256, 5, true)
	step("two good points recover", 5*time.Second, true, 2, 2, 0, 128, 6, true)
	p.DeviceAvailable = false
	step("three good points without device", 5*time.Second, true, 2, 3, 0, 512, 7, true)
	p.DeviceAvailable = true
	step("device recovered on reused engine sample", 0, false, 3, 3, 0, 0, 7, false)
	if r.historyLoad.dutyPPM() != 600_000 {
		t.Fatal("recovered duty changed")
	}
	assertHistoryLoadReasonEntries(t, r, map[string]int64{"recovery_samples": 2, "new_stall": 1, "pressure_hysteresis": 1, "device_unknown": 1})

	// Stale/unavailable observations retain the accepted baseline. A backwards
	// observation or counter reset accepts a fresh baseline without rewinding
	// the diagnostic sequence, and still grants only one good point.
	step("stale engine", 16*time.Second, false, 0, 0, 0, 64, 7, false)
	p.Available = false
	step("unavailable engine", 0, false, 0, 0, 0, 32, 7, false)
	p.Available = true
	step("fresh after outage", 0, true, 2, 1, 0, 128, 8, true)
	p.SampledAt = p.SampledAt.Add(-time.Second)
	step("timestamp reset", 0, false, 2, 1, 0, 128, 9, true)
	p.StallCount = 0
	step("counter reset on reused timestamp", 0, false, 2, 1, 0, 128, 10, true)
	step("same reset baseline", 0, false, 2, 1, 0, 128, 10, false)
	assertHistoryLoadReasonEntries(t, r, map[string]int64{
		"recovery_samples": 3, "new_stall": 1, "pressure_hysteresis": 1, "device_unknown": 1,
		"engine_stale": 1, "engine_unavailable": 1,
	})
}

func TestHistoryLoadReasonMetricsDebtAndSimultaneousPressure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		finalDebt uint64
		wantBits  int64
		wantLevel int
	}{
		{"below debt boundary", (2 << 30) - 1, 128, 2},
		{"at debt boundary", 2 << 30, 16, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 123)
			p := healthyHistoryLoad(now)
			p.CompactionDebt = tc.finalDebt - 2
			r := historyLoadMetricsFixture(t, &p)
			r.refreshHistoryLoad(now)
			for i := 1; i <= 2; i++ {
				now = now.Add(5 * time.Second)
				p.SampledAt, p.DeviceSampledAt = now, now
				p.CompactionDebt++
				r.refreshHistoryLoad(now)
				assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_rises", int64(i))
			}
			if r.historyLoad.level != tc.wantLevel || r.historyLoad.good != 0 || r.historyLoad.hard {
				t.Fatalf("debt decision = %+v", r.historyLoad)
			}
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/reason_bits", tc.wantBits)
			if tc.wantLevel != 1 {
				return
			}
			// Every active trigger is retained; hard pressure must not mask the
			// simultaneous stall, L0, device and debt-growth reasons.
			p.WriteStalled, p.StallCount, p.L0Sublevels = true, 1, 16
			p.DeviceBusyPPM, p.DeviceQueueMilli, p.DeviceAwait = 950_000, 2_000, 5*time.Millisecond
			for range 3 {
				r.refreshHistoryLoad(now)
			}
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/reason_bits", 1|2|4|8|16)
			assertHistoryLoadReasonEntries(t, r, map[string]int64{
				"recovery_samples": 1, "hard": 1, "new_stall": 1, "device_pressure": 1, "l0_pressure": 1, "debt_growth": 1,
			})
			now = now.Add(5 * time.Second)
			p = healthyHistoryLoad(now)
			p.StallCount = 1
			r.refreshHistoryLoad(now)
			// The completed stall remains pressure until this observation has
			// accepted its counter as the new baseline.
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/reason_bits", 2)
			r.refreshHistoryLoad(now)
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/reason_bits", 128|256)
			assertColdRunnerGauge(t, r.cfg.MetricsNamespace+"/history/budget/debt_rises", 0)
			p.WriteStalled = true
			r.refreshHistoryLoad(now)
			assertHistoryLoadReasonEntries(t, r, map[string]int64{
				"recovery_samples": 2, "pressure_hysteresis": 1, "hard": 2, "new_stall": 1, "device_pressure": 1, "l0_pressure": 1, "debt_growth": 1,
			})
		})
	}
}
