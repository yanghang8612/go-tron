package snapshots

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func admitMiddleBatchFixture(t *testing.T, r *Runner) (*maintenance.StoragePressure, time.Time) {
	t.Helper()
	now := time.Now()
	p := healthyHistoryLoad(now)
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	for i := 0; i < 7; i++ {
		at := now.Add(time.Duration(i-6) * 5 * time.Second)
		p.SampledAt, p.DeviceSampledAt = at, at
		p.CompactionDebt = 3<<30 + uint64(i)*(128<<20)
		r.refreshHistoryLoad(at)
	}
	if !r.historyLoad.debtModerateReady(now) {
		t.Fatal("did not establish middle tier")
	}
	return &p, now
}

func TestHistoryMiddleBatchResourceTargetsAndUseTimeFreshness(t *testing.T) {
	r := throughputFixture(t)
	p, now := admitMiddleBatchFixture(t, r)
	calls := 0
	r.cfg.BusyHistoryBuildReady = func() bool { calls++; return true }
	assert := func(at time.Time, wantWork time.Duration, wantBytes uint64) {
		t.Helper()
		work, nbytes := r.historyBatchTargets(at)
		if work != wantWork || nbytes != wantBytes {
			t.Fatalf("target %s/%d want %s/%d", work, nbytes, wantWork, wantBytes)
		}
	}
	assert(now, 4*time.Second, 64<<20)
	if calls != 1 {
		t.Fatal("resource probe not checked once")
	}
	assert(now.Add(15*time.Second), 4*time.Second, 64<<20)
	calls = 0
	assert(now.Add(15*time.Second+time.Nanosecond), 2*time.Second, 32<<20)
	if calls != 0 {
		t.Fatal("stale storage called resource probe")
	}
	for _, tc := range []struct {
		name   string
		change func()
	}{
		{"unknown resource", func() { r.cfg.BusyHistoryBuildReady = nil }},
		{"low resource", func() { r.cfg.BusyHistoryBuildReady = func() bool { return false } }},
		{"no shared gate", func() { r.cfg.HeavyWorkGate = nil }},
		{"stale device", func() { p.DeviceSampledAt = now.Add(-16 * time.Second) }},
		{"stale engine", func() { p.SampledAt = now.Add(-16 * time.Second) }},
		{"future engine", func() { p.SampledAt = now.Add(time.Second + time.Nanosecond) }},
		{"future device", func() { p.DeviceSampledAt = now.Add(time.Second + time.Nanosecond) }},
		{"unknown engine", func() { p.Available = false }},
		{"unknown device", func() { p.DeviceAvailable = false }},
		{"slow device", func() { p.DeviceAwait = 2*time.Millisecond + time.Nanosecond }},
		{"l0 equality", func() { p.L0Sublevels = p.L0CompactionThreshold }},
		{"unknown l0", func() { p.L0CompactionThreshold = 0 }},
		{"hard engine", func() { p.WriteStalled = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved, original, probe, gate := r.historyLoad, *p, r.cfg.BusyHistoryBuildReady, r.cfg.HeavyWorkGate
			tc.change()
			r.historyLoad.sample = *p
			assert(now, 2*time.Second, 32<<20)
			r.historyLoad = saved
			*p = original
			r.cfg.BusyHistoryBuildReady, r.cfg.HeavyWorkGate = probe, gate
		})
	}
}

func TestHistoryMiddleBatchClampsAndOneResourceObservation(t *testing.T) {
	r := throughputFixture(t)
	_, _ = admitMiddleBatchFixture(t, r)
	calls := 0
	r.cfg.BusyHistoryBuildReady = func() bool { calls++; return calls == 1 }
	r.historyLoad.history = historyWorkSample{blocks: 100, txnums: 1000, work: 2560 * time.Millisecond, bytes: 16 << 20}
	blocks, txs := r.adaptiveHistoryBatchLimits(1000, 10000)
	if blocks != 125 || txs != 1250 || calls != 1 {
		t.Fatalf("inconsistent observation: %d/%d calls=%d", blocks, txs, calls)
	}
	r.cfg.BusyHistoryBuildReady = func() bool { return true }
	blocks, txs = r.adaptiveHistoryBatchLimits(80, 800)
	if blocks != 80 || txs != 800 {
		t.Fatal("configured cap bypassed")
	}
	r.historyLoad.historyFailureBlocks, r.historyLoad.historyFailureTxNums = 3, 17
	blocks, txs = r.adaptiveHistoryBatchLimits(1000, 10000)
	if blocks != 3 || txs != 17 {
		t.Fatal("failure backoff bypassed")
	}
	r.historyLoad.historyFailureBlocks, r.historyLoad.historyFailureTxNums = 0, 0
	r.historyLoad.history.bytes = 128 << 20
	blocks, txs = r.adaptiveHistoryBatchLimits(1000, 10000)
	if blocks != 50 || txs != 500 {
		t.Fatal("output density cap bypassed", blocks, txs)
	}
	r.historyLoad.event = historyWorkSample{blocks: 100, work: 2560 * time.Millisecond, bytes: 16 << 20}
	r.cfg.BatchBlocks = 1000
	r.historyLoad.eventFailureBlocks = 2
	if got := r.adaptiveEventBatchLimit(1000); got != 2 {
		t.Fatal("event failure cap bypassed", got)
	}
	r.historyLoad.eventFailureBlocks = 0
	r.cfg.BusyHistoryBuildReady = nil
	if got := r.adaptiveEventBatchLimit(1000); got != 78 {
		t.Fatal("unknown resource lost original event target", got)
	}
}

func TestHistoryMiddleBatchRecoveryAndProtectionRevoke(t *testing.T) {
	for _, fault := range []string{"recovery", "reset", "severe", "stall"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := throughputFixture(t)
				p, now := admitMiddleBatchFixture(t, r)
				r.cfg.BusyHistoryBuildReady = func() bool { return true }
				time.Sleep(5 * time.Second)
				next := time.Now()
				p.SampledAt, p.DeviceSampledAt = next, next
				switch fault {
				case "recovery":
					p.CompactionDebt = 3 << 30
				case "reset":
					p.SampledAt = now.Add(-time.Second)
				case "severe":
					p.CompactionDebt = 6 << 30
				case "stall":
					p.StallCount++
				}
				r.refreshHistoryLoad(next)
				work, nbytes := r.historyBatchTargets(next)
				if fault == "recovery" {
					if work != 4*time.Second || nbytes != 64<<20 {
						t.Fatal("fresh admitted carryover lost target")
					}
				} else if work != 2*time.Second || nbytes != 32<<20 {
					t.Fatal("protection retained expanded target")
				}
			})
		})
	}
}

func TestHistoryMiddleBatchIndependentEventFullDrain(t *testing.T) {
	for _, resource := range []bool{false, true} {
		name := "resource-denied"
		if resource {
			name = "resource-ready"
		}
		t.Run(name, func(t *testing.T) {
			r, _, address, topic := historyEventBudgetFixture(t)
			_, _ = admitMiddleBatchFixture(t, r)
			r.cfg.BusyHistoryBuildReady = func() bool { return resource }
			r.cfg.CatchupHeavyWorkCooldown = 0
			passes, blocks := 0, uint64(0)
			for blocks < 4 {
				r.historyLoad.event = historyWorkSample{blocks: 4, work: 6 * time.Second, bytes: 1 << 20}
				r.historyNotBefore.Store(0)
				result, err := r.OnePass()
				if err != nil || !result.HistoryEventAttempted || !result.EventLogBuilt || result.Built || result.HistoryBuildAttempted {
					t.Fatalf("event pass %+v %v", result, err)
				}
				want := uint64(1)
				if resource {
					want = 2
				}
				if result.historyEventBatchBlocks != want {
					t.Fatalf("event range %d want %d", result.historyEventBatchBlocks, want)
				}
				if result.HistoryRecoveryCost <= 0 || result.HistoryMinRecovery <= 0 {
					t.Fatal("event completion lost recovery")
				}
				blocks += result.historyEventBatchBlocks
				passes++
			}
			wantPasses := 4
			if resource {
				wantPasses = 2
			}
			if passes != wantPasses {
				t.Fatal("event debt not drained", passes)
			}
			assertHistoryEventCounters(t, r, uint64(passes))
			assertHistoryEventBudgetQuery(t, r, address, topic)
			manager, err := OpenManager(r.cfg.Dir)
			if err != nil {
				t.Fatal(err)
			}
			covered, err := manager.EventLogIndexedRangeCovered(1, 4)
			if err != nil || !covered {
				t.Fatal("full event coverage remained", covered, err)
			}
			seen := uint64(0)
			err = manager.IterateEventLogs(1, 4, EventLogFilter{}, func(row EventLog) (bool, error) {
				seen++
				if row.BlockNum != seen || row.TxIndex != 0 || row.LogIndex != 0 || len(row.Log.GetData()) != 1 || row.Log.GetData()[0] != byte(seen) {
					t.Fatalf("event row %d: %+v", seen, row)
				}
				return true, nil
			})
			if err != nil || seen != 4 {
				t.Fatal("incomplete event oracle", seen, err)
			}
			stage, ok, err := rawdb.ReadStageProgress(r.chain.DB(), rawdb.StageSnapshotEventLogBuild)
			if err != nil || !ok || stage != 4 || r.pendingMaintenanceID != 0 {
				t.Fatal("event watermark/pending work", stage, ok, err)
			}
		})
	}
}

func TestHistoryMiddleBatchSlowResourceCallbackCannotReuseFreshness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := throughputFixture(t)
		_, now := admitMiddleBatchFixture(t, r)
		r.cfg.BusyHistoryBuildReady = func() bool { time.Sleep(16 * time.Second); return true }
		work, nbytes := r.historyBatchTargets(now)
		if work != 2*time.Second || nbytes != 32<<20 {
			t.Fatal("callback outlived storage evidence")
		}
	})
}

func TestHistoryMiddleBatchSelectionCountersDescribeDecisions(t *testing.T) {
	r := throughputFixture(t)
	_, now := admitMiddleBatchFixture(t, r)
	r.cfg.BusyHistoryBuildReady = func() bool { return true }
	r.historyLoad.history = historyWorkSample{blocks: 100, txnums: 1000, work: 2560 * time.Millisecond, bytes: 1 << 20}
	r.historyLoad.event = historyWorkSample{blocks: 100, work: 2560 * time.Millisecond, bytes: 1 << 20}
	r.cfg.BatchBlocks = 1000
	history, event := r.historyLoad.middleHistoryTargets, r.historyLoad.middleEventTargets
	if history == nil || event == nil {
		t.Fatal("missing range selection counters")
	}
	beforeHistory, beforeEvent := history.Snapshot().Count(), event.Snapshot().Count()
	r.historyBatchTargets(now)
	if history.Snapshot().Count() != beforeHistory || event.Snapshot().Count() != beforeEvent {
		t.Fatal("eligibility query counted as range decision")
	}
	r.adaptiveHistoryBatchLimits(1000, 10000)
	if history.Snapshot().Count() != beforeHistory+1 || event.Snapshot().Count() != beforeEvent {
		t.Fatal("block/txnum double counted")
	}
	r.adaptiveEventBatchLimit(1000)
	if event.Snapshot().Count() != beforeEvent+1 {
		t.Fatal("event range selection missing")
	}
	r.cfg.BusyHistoryBuildReady = func() bool { return false }
	r.adaptiveHistoryBatchLimits(1000, 10000)
	r.adaptiveEventBatchLimit(1000)
	if history.Snapshot().Count() != beforeHistory+1 || event.Snapshot().Count() != beforeEvent+1 {
		t.Fatal("resource rejection counted as expanded selection")
	}
}

func TestHistoryMiddleBatchNonMiddleTargetsRemainOriginal(t *testing.T) {
	r := throughputFixture(t)
	_, now := admitMiddleBatchFixture(t, r)
	r.cfg.BusyHistoryBuildReady = func() bool { t.Fatal("nonmiddle called new resource probe"); return false }
	r.historyLoad.debtModerate = false
	for _, level := range []int{0, 1, 2, 3} {
		r.historyLoad.level = level
		gotWork, gotBytes := r.historyBatchTargets(now)
		wantWork, wantBytes := r.historyLoad.targets()
		if gotWork != wantWork || gotBytes != wantBytes {
			t.Fatal("nonmiddle target changed", level)
		}
	}
}

func TestHistoryMiddleBatchLowResourceKeepsLargeLagLiveness(t *testing.T) {
	for _, ready := range []bool{false, true} {
		name := "low-resource"
		if ready {
			name = "ready"
		}
		t.Run(name, func(t *testing.T) {
			r := throughputFixture(t)
			_, _ = admitMiddleBatchFixture(t, r)
			r.cfg.BusyHistoryBuildReady = func() bool { return ready }
			r.historyLoad.history = historyWorkSample{blocks: 4, txnums: 4, work: 6 * time.Second, bytes: 1 << 20}
			result, err := r.OnePass()
			want := uint64(1)
			if ready {
				want = 2
			}
			if err != nil || !result.Built || !result.HistoryForcedBusy || result.HistoryBusyResourceReady || result.HistoryBatchBlocks != want {
				t.Fatalf("large-lag liveness altered: %+v %v", result, err)
			}
		})
	}
}
