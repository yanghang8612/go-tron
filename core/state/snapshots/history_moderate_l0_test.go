package snapshots

import (
	"context"
	"errors"
	"math"
	"reflect"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"google.golang.org/protobuf/proto"
)

func admitModerateL0(t *testing.T, r *Runner) *maintenance.StoragePressure {
	t.Helper()
	now := time.Now()
	p := healthyHistoryLoad(now)
	p.L0Sublevels = 10
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
	r.historyLoad.level = 1
	for _, at := range []time.Time{now.Add(-5 * time.Second), now} {
		p.SampledAt, p.DeviceSampledAt = at, at
		r.refreshHistoryLoad(at)
	}
	if !r.historyLoad.moderateL0Ready(now) || r.historyLoad.good != 0 || r.historyLoad.cpuBurstReady(now) {
		t.Fatal("independent moderate fixture", r.historyLoad)
	}
	return &p
}

func TestHistoryModerateL0IndependentSamplesAndBoundaryOscillation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		levels []int
		ready  []bool
	}{{"7-8", []int{7, 8}, []bool{false, true}}, {"8-7-8", []int{8, 7, 8}, []bool{false, false, true}}, {"15-16-8-8", []int{15, 16, 8, 8}, []bool{false, false, false, true}}} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().Add(-15 * time.Second)
			p := healthyHistoryLoad(now)
			r := historyLoadMetricsFixture(t, &p)
			r.historyLoad.level = 1
			for i, l0 := range tc.levels {
				at := now.Add(time.Duration(i) * 5 * time.Second)
				p.SampledAt, p.DeviceSampledAt, p.L0Sublevels = at, at, l0
				r.refreshHistoryLoad(at)
				if r.historyLoad.moderateL0Ready(at) != tc.ready[i] {
					t.Fatalf("step %d L0%d state=%+v", i, l0, r.historyLoad.moderateL0)
				}
				samples, sequence := r.historyLoad.moderateL0.samples, r.historyLoad.acceptedSequence
				for range 10 {
					r.refreshHistoryLoad(at)
				}
				if r.historyLoad.moderateL0.samples != samples || r.historyLoad.acceptedSequence != sequence {
					t.Fatal("reused observation promoted independent pair")
				}
			}
		})
	}
}

func TestHistoryModerateL0RevocationAndThresholdEpoch(t *testing.T) {
	changes := map[string]func(*maintenance.StoragePressure){
		"unknown-engine": func(p *maintenance.StoragePressure) { p.Available = false },
		"unknown-device": func(p *maintenance.StoragePressure) { p.DeviceAvailable = false },
		"stale-device":   func(p *maintenance.StoragePressure) { p.DeviceSampledAt = p.SampledAt.Add(-16 * time.Second) },
		"future-device":  func(p *maintenance.StoragePressure) { p.DeviceSampledAt = p.SampledAt.Add(2 * time.Second) },
		"negative-await": func(p *maintenance.StoragePressure) { p.DeviceAwait = -1 },
		"slow-await":     func(p *maintenance.StoragePressure) { p.DeviceAwait = 2*time.Millisecond + 1 },
		"unknown-T":      func(p *maintenance.StoragePressure) { p.L0CompactionThreshold = 0 },
		"unknown-stop":   func(p *maintenance.StoragePressure) { p.L0StopWritesThreshold = 0 },
		"negative-L0":    func(p *maintenance.StoragePressure) { p.L0Sublevels = -1 },
		"double-T":       func(p *maintenance.StoragePressure) { p.L0Sublevels = 16 },
		"hard-L0":        func(p *maintenance.StoragePressure) { p.L0Sublevels = 144 },
		"overflow-int": func(p *maintenance.StoragePressure) {
			p.L0CompactionThreshold = math.MaxInt
			p.L0StopWritesThreshold = math.MaxInt
			p.L0Sublevels = math.MaxInt
		},
		"hard-memtable": func(p *maintenance.StoragePressure) { p.MemTableCount = 3 },
		"active-stall":  func(p *maintenance.StoragePressure) { p.WriteStalled = true },
		"new-stall":     func(p *maintenance.StoragePressure) { p.StallCount++ },
		"severe-debt": func(p *maintenance.StoragePressure) {
			p.CompactionDebt = 8 << 30
			p.SampledAt = p.SampledAt.Add(5 * time.Second)
			p.DeviceSampledAt = p.SampledAt
		},
		"reset-time": func(p *maintenance.StoragePressure) { p.SampledAt = p.SampledAt.Add(-time.Second) },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			r := busyResourceFixture(t)
			p := admitModerateL0(t, r)
			original := *p
			change(p)
			now := original.SampledAt
			if name == "severe-debt" {
				now = p.SampledAt
			}
			r.refreshHistoryLoad(now)
			if r.historyLoad.moderateL0Ready(now) || r.historyLoad.moderateL0.samples != 0 {
				t.Fatal("bad evidence retained lane", r.historyLoad.moderateL0)
			}
			*p = original
			r.refreshHistoryLoad(now)
			if r.historyLoad.moderateL0Ready(now) {
				t.Fatal("reused repaired sample reacquired eligibility")
			}
		})
	}
	for _, field := range []string{"T", "stop"} {
		t.Run("epoch-"+field, func(t *testing.T) {
			r := busyResourceFixture(t)
			p := admitModerateL0(t, r)
			now := p.SampledAt.Add(5 * time.Second)
			p.SampledAt, p.DeviceSampledAt = now, now
			if field == "T" {
				p.L0CompactionThreshold = 7
			} else {
				p.L0StopWritesThreshold = 128
			}
			r.refreshHistoryLoad(now)
			if r.historyLoad.moderateL0.samples != 1 || r.historyLoad.moderateL0Ready(now) {
				t.Fatal("changed thresholds inherited pair")
			}
			r.refreshHistoryLoad(now)
			if r.historyLoad.moderateL0Ready(now) {
				t.Fatal("reused threshold change promoted pair")
			}
			now = now.Add(5 * time.Second)
			p.SampledAt, p.DeviceSampledAt = now, now
			r.refreshHistoryLoad(now)
			if !r.historyLoad.moderateL0Ready(now) {
				t.Fatal("new epoch pair never recovered")
			}
		})
	}
}

func TestHistoryModerateL0BusyWatermarksAndOriginalFallback(t *testing.T) {
	for _, above := range []bool{false, true} {
		for _, resource := range []string{"ready", "false", "unknown"} {
			t.Run(resource+map[bool]string{false: "/within", true: "/above"}[above], func(t *testing.T) {
				r := busyResourceFixture(t)
				admitModerateL0(t, r)
				if above {
					r.cfg.MaxBusyDeferredHistoryBlocks = 4
				}
				if resource == "false" {
					r.cfg.BusyHistoryBuildReady = func() bool { return false }
				}
				if resource == "unknown" {
					r.cfg.BusyHistoryBuildReady = nil
				}
				pass, err := r.OnePass()
				want := above || resource == "ready"
				if err != nil || pass.Built != want || pass.HistoryModerateL0 != (resource == "ready") {
					t.Fatalf("admission/fallback %+v %v", pass, err)
				}
				if resource != "ready" && above && (!pass.HistoryForcedBusy || pass.HistoryBusyResourceDeferred) {
					t.Fatal("new lane vetoed old liveness")
				}
				if pass.HistoryModerateL0 && (pass.HistoryEventParallel || pass.HistorySharedReadWorkers != 0 || pass.HistorySharedChunkCache || pass.HistoryMinRecovery < 3*time.Second) {
					t.Fatal("bounded lane inherited fast topology/recovery")
				}
			})
		}
	}
}

func TestHistoryModerateL0CallbackFreshnessAndNoLeaseCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := busyResourceFixture(t)
		p := admitModerateL0(t, r)
		r.cfg.BusyHistoryBuildReady = func() bool { time.Sleep(16 * time.Second); return true }
		if ready, lane := r.busyHistoryAdmission(time.Now()); ready || lane {
			t.Fatal("slow callback retained expired evidence")
		}
		if r.historyLoad.moderateL0.samples != 0 {
			t.Fatal("consumed stale evidence retained qualified epoch")
		}
		p.SampledAt, p.DeviceSampledAt = time.Now(), time.Now()
		r.refreshHistoryLoad(time.Now())
		if r.historyLoad.moderateL0.samples != 1 || r.historyLoad.moderateL0Ready(time.Now()) {
			t.Fatal("one fresh sample reacquired an expired epoch")
		}
	})
	for _, kind := range []string{"lease", "gate", "space", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			r := busyResourceFixture(t)
			admitModerateL0(t, r)
			ctx := context.Background()
			switch kind {
			case "lease":
				release, ok := r.cfg.HeavyWorkGate.TryAcquire()
				if !ok {
					t.Fatal("setup lease")
				}
				defer release()
			case "gate":
				r.cfg.HeavyWorkGate.SetAdmissionCheck(func() bool { return false })
			case "space":
				r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
					return HistoryPressure{FreeBytesAvailable: true, FreeBytes: 1}, nil
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			beforeAttempts := r.historyLoad.moderateL0.attempts.Snapshot().Count()
			beforePublished := r.historyLoad.moderateL0.published.Snapshot().Count()
			beforeCompleted := r.historyLoad.moderateL0.completed.Snapshot().Count()
			pass, err := r.OnePassContext(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			m := r.historyLoad.moderateL0
			if pass.HistoryBuildAttempted || pass.Built || m.attempts.Snapshot().Count() != beforeAttempts || m.published.Snapshot().Count() != beforePublished || m.completed.Snapshot().Count() != beforeCompleted {
				t.Fatal("unattempted lane claimed work/completion", pass)
			}
		})
	}
}

func TestHistoryModerateL0KeepsExistingBurstOpportunity(t *testing.T) {
	r := busyResourceFixture(t)
	admitModerateL0(t, r)
	r.historyLoad.level = 2
	if !r.historyLoad.cpuBurstReady(time.Now()) || !r.historyLoad.moderateL0Ready(time.Now()) {
		t.Fatal("fixture must qualify both storage paths")
	}
	if ready, lane := r.busyHistoryAdmission(time.Now()); !ready || lane {
		t.Fatal("new lane replaced existing legal burst")
	}
}

func TestHistoryModerateL0LockedFullOuterRecovery(t *testing.T) {
	r := throughputFixture(t)
	now := time.Now()
	r.historyLoad.level = 3
	r.historyLoad.sample = healthyHistoryLoad(now)
	for _, work := range []time.Duration{0, 1, 3 * time.Second, 30*time.Second + 1, time.Duration(math.MaxInt64)} {
		want := max(3*time.Second, work-work/3)
		if got := r.moderateL0Recovery(work, false); got != want {
			t.Fatalf("ceil/full cost %d got%d want%d", work, got, want)
		}
	}
	r.cfg.CatchupHeavyWorkCooldown = 11 * time.Second
	if r.moderateL0Recovery(time.Nanosecond, false) != 11*time.Second {
		t.Fatal("explicit cooldown shortened")
	}
	r.cfg.CatchupBuildMinInterval = 2 * time.Minute
	if r.moderateL0Recovery(time.Nanosecond, true) < 2*time.Minute {
		t.Fatal("failure backoff shortened")
	}
	r.cfg.CatchupHeavyWorkCooldown = 3 * time.Second
	pass := PassResult{HistoryBuildAttempted: true, HistoryForcedBusy: true, HistoryModerateL0: true, CompactionDuration: 20 * time.Second}
	r.applyThroughputRecovery(&pass, now, now.Add(60*time.Second), nil)
	if pass.HistoryRecoveryCost != 60*time.Second || pass.HistoryMinRecovery != 40*time.Second {
		t.Fatal("merge omitted or tier inherited global burst", pass)
	}
	later := now.Add(300 * time.Second)
	r.extendHistoryNotBefore(later)
	r.applyThroughputRecovery(&pass, now, now.Add(90*time.Second), nil)
	if pass.HistoryMinRecovery != 60*time.Second || r.historyNotBefore.Load() != later.UnixNano() {
		t.Fatal("later deadline shortened", pass)
	}
}

func TestHistoryModerateL0RealFilesSerialAndFullClockRecovery(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(oldProcs)
	r := newBusyResourceEquivalenceRunner(t, false, 3, 2, 20)
	p := admitModerateL0(t, r)
	// A later healthy classification cannot change the already selected pass's
	// topology/budget. The independent lease check runs after lane selection.
	r.cfg.HeavyWorkGate.SetAdmissionCheck(func() bool { r.historyLoad.level = 3; return true })
	sharedCalls, pairCalls := 0, 0
	r.cfg.HistorySharedChunkCache = true
	r.cfg.HistorySharedReadWorkers = 4
	r.cfg.HistoryReadResourceProbe = func() HistoryReadResources {
		sharedCalls++
		return HistoryReadResources{Available: true, SampledAt: time.Now(), IdleCoresMilli: 16000, MemoryAvailableBytes: 8 << 30}
	}
	r.cfg.ParallelHistoryEventReady = func() bool { pairCalls++; return true }
	first, err := r.OnePass()
	if err != nil || !first.Built || !first.HistoryModerateL0 {
		t.Fatal("first real publication", first, err)
	}
	output := readBusyResourceColdOutput(t, r, first, 3)
	if len(output.files) == 0 || first.HistoryEventParallel || first.HistorySharedReadWorkers != 0 || first.HistorySharedChunkCache || sharedCalls != 0 || pairCalls != 0 {
		t.Fatal("lane selected enhanced topology")
	}
	deadline := r.historyNotBefore.Load()
	if deadline <= time.Now().UnixNano() {
		t.Fatal("no complete-work recovery")
	}
	next, err := r.OnePass()
	if err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited || r.historyNotBefore.Load() != deadline {
		t.Fatal("early pass bypassed deadline", next, err)
	}
	time.Sleep(time.Until(time.Unix(0, deadline))) // Real recovery, no deadline clearing.
	p.SampledAt, p.DeviceSampledAt = time.Now(), time.Now()
	r.historyLoad.level = 1
	second, err := r.OnePass()
	if err != nil || !second.Built || !second.HistoryModerateL0 || second.FromBlock != first.ToBlock+1 {
		t.Fatal("second real publication", second, err)
	}
	cumulative := second
	cumulative.FromBlock, cumulative.FromTxNum = 1, 1
	readBusyResourceColdOutput(t, r, cumulative, 3)
	if r.pendingMaintenanceID != 0 || r.cfg.HeavyWorkGate.Observation().Owner != maintenance.HeavyWorkUnspecified || r.historyLoad.moderateL0.completed.Snapshot().Count() != 2 {
		t.Fatal("pending maintenance/lease or false completion count")
	}
}

func TestHistoryModerateL0Immutable408SecondTraceAndObservationWake(t *testing.T) {
	// Consume one immutable five-second producer trace. Existing storage burst
	// stays false in this band; bounded eligibility must not depend on good.
	trace := makeBusyHistoryPressureTrace(410*time.Second, func(elapsed time.Duration, p *maintenance.StoragePressure) {
		p.L0Sublevels = []int{8, 10, 12, 9}[int(elapsed/(5*time.Second))%4]
		p.CompactionDebt = 3 << 30
	})
	p := trace[0].sample
	r := historyLoadMetricsFixture(t, &p)
	r.historyLoad.level = 1
	oldOpportunities, newOpportunities := 0, 0
	for i, point := range trace {
		p = point.sample
		r.refreshHistoryLoad(point.at)
		if r.historyLoad.cpuBurstReady(point.at) {
			oldOpportunities++
		}
		if r.historyLoad.moderateL0Ready(point.at) {
			newOpportunities++
		}
		if i > 0 && !r.historyLoad.moderateL0Ready(point.at) {
			t.Fatal("moderate platform starved", i)
		}
	}
	if oldOpportunities != 0 || newOpportunities != len(trace)-1 {
		t.Fatal("trace opportunity counts", oldOpportunities, newOpportunities)
	}
	t.Logf("same 410s trace: old opportunities=%d bounded=%d; producer interval=5s", oldOpportunities, newOpportunities)
	synctest.Test(t, func(t *testing.T) {
		rr := busyResourceFixture(t)
		at := time.Now()
		pp := healthyHistoryLoad(at)
		pp.L0Sublevels = 10
		rr.historyLoad.level = 1
		rr.cfg.HistoryLoadProbe = func() maintenance.StoragePressure {
			pp.SampledAt, pp.DeviceSampledAt = time.Now(), time.Now()
			return pp
		}
		first, err := rr.OnePass()
		if err != nil || !first.HistoryBusyResourceDeferred || first.HistoryBuildAttempted {
			t.Fatal("first sample admitted prematurely", first, err)
		}
		time.Sleep(BusyHistoryObservationInterval)
		wake, _ := rr.ObserveBusyHistoryResources(context.Background())
		if !wake {
			t.Fatal("new independent second sample never woke")
		}
		if wake, _ := rr.ObserveBusyHistoryResources(context.Background()); wake {
			t.Fatal("wake not consumed")
		}
		built, err := rr.OnePass()
		if err != nil || !built.Built || !built.HistoryModerateL0 || !built.HistoryBuildAttempted {
			t.Fatal("normal full pass failed after bounded wake", built, err)
		}
	})
}

type moderateL0PlanningDB struct {
	AggregatorDB
	plan func()
}

func (d *moderateL0PlanningDB) RequireHotRange(uint64, uint64) error { d.plan(); return nil }

type moderateL0PlanningChain struct {
	*coldBuilderChain
	plan func()
}

func (c *moderateL0PlanningChain) AcquireStateHistorySourceView(context.Context) (AggregatorDB, func() error, error) {
	return &moderateL0PlanningDB{AggregatorDB: c.db, plan: c.plan}, func() error { return nil }, nil
}

func TestHistoryModerateL0PreleaseRevalidationAndCancellation(t *testing.T) {
	for _, above := range []bool{false, true} {
		for _, kind := range []string{"expired", "runtime-loss", "callback-cancel"} {
			t.Run(kind+map[bool]string{false: "/within", true: "/above"}[above], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					r := busyResourceFixture(t)
					admitModerateL0(t, r)
					if above {
						r.cfg.MaxBusyDeferredHistoryBlocks = 4
					}
					runtimeReady := true
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					calls := 0
					r.cfg.BusyHistoryBuildReady = func() bool {
						calls++
						if kind == "callback-cancel" && calls == 2 {
							cancel()
							return false
						}
						return runtimeReady
					}
					planned := 0
					r.chain = &moderateL0PlanningChain{coldBuilderChain: r.chain.(*coldBuilderChain), plan: func() {
						planned++
						if kind == "expired" {
							time.Sleep(16 * time.Second)
						}
						if kind == "runtime-loss" {
							runtimeReady = false
						}
					}}
					before := r.cfg.HeavyWorkGate.Observation().Acquisitions
					pass, err := r.OnePassContext(ctx)
					if kind == "callback-cancel" {
						if !errors.Is(err, context.Canceled) || pass.HistoryBuildAttempted || r.cfg.HeavyWorkGate.Observation().Acquisitions != before {
							t.Fatal("cancel continued into lease/work", pass, err)
						}
						return
					}
					if err != nil || planned != 1 || pass.HistoryModerateL0 {
						t.Fatal("stale lane still selected", pass, err, planned)
					}
					if above {
						if !pass.Built || !pass.HistoryForcedBusy {
							t.Fatal("old above-busy fallback blocked", pass)
						}
					} else if pass.HistoryBuildAttempted || !pass.HistoryBusyResourceDeferred || r.cfg.HeavyWorkGate.Observation().Acquisitions != before {
						t.Fatal("stale below-busy opportunity acquired lease/work", pass)
					}
				})
			})
		}
	}
}

func TestHistoryModerateL0SameColdFilesAndPublicQueries(t *testing.T) {
	control := newBusyResourceEquivalenceRunner(t, false, 3, 2, 20)
	admitModerateL0(t, control)
	control.cfg.MaxBusyDeferredHistoryBlocks = 2
	control.cfg.BusyHistoryBuildReady = func() bool { return false }
	original, err := control.OnePass()
	if err != nil || !original.Built || original.HistoryModerateL0 {
		t.Fatal("old liveness control", original, err)
	}
	want := readBusyResourceColdOutput(t, control, original, 3)
	candidate := newBusyResourceEquivalenceRunner(t, false, 3, 2, 20)
	admitModerateL0(t, candidate)
	pass, err := candidate.OnePass()
	if err != nil || !pass.Built || !pass.HistoryModerateL0 {
		t.Fatal("new bounded output", pass, err)
	}
	got := readBusyResourceColdOutput(t, candidate, pass, 3)
	if !reflect.DeepEqual(got.refs, want.refs) || !reflect.DeepEqual(got.files, want.files) || !reflect.DeepEqual(got.changes, want.changes) || len(got.logs) != len(want.logs) {
		t.Fatal("lane changed refs/bytes/history queries")
	}
	for i, row := range got.logs {
		old := want.logs[i]
		if row.BlockNum != old.BlockNum || row.TxIndex != old.TxIndex || row.LogIndex != old.LogIndex || row.TxHash != old.TxHash || row.BlockHash != old.BlockHash || row.Address != old.Address || !proto.Equal(row.Log, old.Log) {
			t.Fatal("event query identity changed", i)
		}
	}
}

func TestHistoryModerateL0ObservationGapStartsNewPair(t *testing.T) {
	r := busyResourceFixture(t)
	p := admitModerateL0(t, r)
	now := p.SampledAt.Add(historyDebtTrendMaxGap + time.Second)
	p.SampledAt, p.DeviceSampledAt = now, now
	r.refreshHistoryLoad(now)
	if r.historyLoad.moderateL0.samples != 1 || r.historyLoad.moderateL0Ready(now) {
		t.Fatal("observation gap inherited qualified epoch")
	}
	for range 10 {
		r.refreshHistoryLoad(now)
	}
	if r.historyLoad.moderateL0.samples != 1 {
		t.Fatal("gap reused observation promoted epoch")
	}
	now = now.Add(5 * time.Second)
	p.SampledAt, p.DeviceSampledAt = now, now
	r.refreshHistoryLoad(now)
	if !r.historyLoad.moderateL0Ready(now) {
		t.Fatal("new comparable pair never recovered")
	}
}
