package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/node"
)

type historyResourceFixture struct {
	resources           *runtimeHistoryResources
	cpuReads, diskReads atomic.Int64
	cpuError, diskError atomic.Bool
	stalled             atomic.Bool
}

func newHistoryResourceFixture() *historyResourceFixture {
	f := new(historyResourceFixture)
	start := time.Now()
	device := historyDeviceID{259, 3}
	f.resources = &runtimeHistoryResources{
		load: &runtimeHistoryLoadProbe{
			now: time.Now,
			engine: historyLoadEngineFunc(func() maintenance.StoragePressure {
				return maintenance.StoragePressure{Available: true, SampledAt: time.Now(), WriteStalled: f.stalled.Load()}
			}),
			locate: func(string) (historyDeviceID, error) { return device, nil },
			read: func() ([]byte, error) {
				f.diskReads.Add(1)
				if f.diskError.Load() {
					return nil, errors.New("disk read failed")
				}
				tick := uint64(time.Since(start)/time.Millisecond) + 100
				return historyTestDiskstats(device, historyDiskCounters{
					reads: tick, writes: tick, readMillis: tick, writeMillis: tick,
					busyMillis: tick / 2, weightedMillis: tick / 2,
				}), nil
			},
		},
		parallel: &runtimeHistoryParallelProbe{
			now: time.Now, gomax: func() int { return 16 }, numCPU: func() int { return 16 },
			read: func() (historyParallelObservation, error) {
				f.cpuReads.Add(1)
				if f.cpuError.Load() {
					return historyParallelObservation{}, errors.New("cpu read failed")
				}
				return historyParallelTestObservation(uint64(time.Since(start)/time.Millisecond) + 100), nil
			},
		},
	}
	return f
}

func startHistoryResources(t *testing.T, p *runtimeHistoryResources) {
	t.Helper()
	if err := p.Start(); err != nil {
		t.Fatalf("start resource sampler: %v", err)
	}
}

func stopHistoryResources(t *testing.T, p *runtimeHistoryResources) {
	t.Helper()
	if err := p.Stop(); err != nil {
		t.Errorf("stop resource sampler: %v", err)
	}
}

func TestHistoryResourcesRefreshWithoutAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newHistoryResourceFixture()
		p := f.resources
		if p.parallelReady() || p.sampleLoad().DeviceAvailable || f.cpuReads.Load() != 0 || f.diskReads.Load() != 0 {
			t.Fatal("admission sampled resources before node startup")
		}
		startHistoryResources(t, p)
		defer stopHistoryResources(t, p)
		synctest.Wait()
		if p.parallelReady() || p.sampleLoad().DeviceAvailable {
			t.Fatal("first point invented capacity")
		}
		// No readiness calls occur during this window. This covers history builds
		// and recovery that exceed the probes' maximum 30-second delta span, as
		// well as storage checks that short-circuit before CPU readiness.
		time.Sleep(65*time.Second + time.Millisecond)
		synctest.Wait()
		if f.cpuReads.Load() != 14 || f.diskReads.Load() != 14 {
			t.Fatalf("background observations cpu=%d disk=%d, want 14 each", f.cpuReads.Load(), f.diskReads.Load())
		}
		if !p.parallelReady() || !p.sampleLoad().DeviceAvailable {
			t.Fatal("long gap between admissions discarded continuously fresh evidence")
		}
		f.stalled.Store(true)
		if !p.sampleLoad().HardLimitReached(time.Now()) || f.diskReads.Load() != 14 {
			t.Fatal("cached device sample suppressed an instantaneous engine write stall")
		}
	})
}

func TestHistoryResourcesWaitAfterObservationCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newHistoryResourceFixture()
		p := f.resources
		read := p.parallel.read
		p.parallel.read = func() (historyParallelObservation, error) {
			if f.cpuReads.Load() == 0 {
				time.Sleep(400 * time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
			return read()
		}
		startHistoryResources(t, p)
		defer stopHistoryResources(t, p)
		time.Sleep(5600 * time.Millisecond)
		synctest.Wait()
		if f.cpuReads.Load() != 2 || !p.parallelReady() {
			t.Fatalf("read-time jitter lost the second sample: reads=%d ready=%v", f.cpuReads.Load(), p.parallelReady())
		}
	})
}

func TestHistoryResourcesFailureRequiresFreshPair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newHistoryResourceFixture()
		p := f.resources
		startHistoryResources(t, p)
		defer stopHistoryResources(t, p)
		synctest.Wait()
		advance := func() { time.Sleep(5 * time.Second); synctest.Wait() }
		advance()
		if !p.parallelReady() || !p.sampleLoad().DeviceAvailable {
			t.Fatal("fixture did not become ready")
		}
		f.cpuError.Store(true)
		f.diskError.Store(true)
		advance()
		if p.parallelReady() || p.sampleLoad().DeviceAvailable {
			t.Fatal("background read failure retained old capacity")
		}
		f.cpuError.Store(false)
		f.diskError.Store(false)
		advance()
		if p.parallelReady() || p.sampleLoad().DeviceAvailable {
			t.Fatal("first point after failed read reused an invalid baseline")
		}
		advance()
		if !p.parallelReady() || !p.sampleLoad().DeviceAvailable {
			t.Fatal("second successful point did not restore fresh evidence")
		}
	})
}

func TestHistoryResourcesStopAndRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newHistoryResourceFixture()
		p := f.resources
		startHistoryResources(t, p)
		startHistoryResources(t, p) // Repeated startup must not double the sample loop.
		synctest.Wait()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if !p.parallelReady() || !p.sampleLoad().DeviceAvailable || f.cpuReads.Load() != 2 || f.diskReads.Load() != 2 {
			t.Fatal("initial lifecycle did not collect one fresh pair")
		}
		stopHistoryResources(t, p)
		stopHistoryResources(t, p)
		f.stalled.Store(true)
		time.Sleep(time.Minute)
		if p.parallelReady() || p.sampleLoad().DeviceAvailable || !p.sampleLoad().HardLimitReached(time.Now()) || f.cpuReads.Load() != 2 || f.diskReads.Load() != 2 {
			t.Fatal("stopped sampler read proc or exposed capacity, or hid engine pressure")
		}
		startHistoryResources(t, p)
		defer stopHistoryResources(t, p)
		synctest.Wait()
		if p.parallelReady() || p.sampleLoad().DeviceAvailable || f.cpuReads.Load() != 3 || f.diskReads.Load() != 3 {
			t.Fatal("restarted sampler retained a previous lifecycle's baseline")
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if !p.parallelReady() || !p.sampleLoad().DeviceAvailable {
			t.Fatal("restarted sampler did not recover after a fresh pair")
		}
	})
}

func TestHistoryResourcesStopJoinsReadAndClosesAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newHistoryResourceFixture()
		p := f.resources
		entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
		read := p.parallel.read
		p.parallel.read = func() (historyParallelObservation, error) {
			close(entered)
			<-release
			return read()
		}
		startHistoryResources(t, p)
		<-entered
		go func() { stopHistoryResources(t, p); close(stopped) }()
		synctest.Wait()
		if p.running.Load() || p.parallelReady() || p.sampleLoad().DeviceAvailable {
			t.Fatal("stopping sampler still admitted optional work")
		}
		select {
		case <-stopped:
			t.Fatal("Stop returned before its in-flight resource read")
		default:
		}
		close(release)
		<-stopped
		time.Sleep(time.Minute)
		if f.cpuReads.Load() != 1 || f.diskReads.Load() != 1 || p.parallel.known || p.parallel.previous != nil || p.load.previous != nil {
			t.Fatal("Stop failed to join the sampler and discard its baselines")
		}
	})
}

type historyResourceLifecycle struct {
	start func() error
	stop  func() error
}

func (l historyResourceLifecycle) Start() error { return l.start() }
func (l historyResourceLifecycle) Stop() error  { return l.stop() }

func TestHistoryResourcesNodeLifecycleOrder(t *testing.T) {
	for _, failStartup := range []bool{false, true} {
		name := "normal stop"
		if failStartup {
			name = "startup failure"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newHistoryResourceFixture()
				p := f.resources
				stack, err := node.New(&node.Config{})
				if err != nil {
					t.Fatal(err)
				}
				stack.RegisterLifecycle(p)
				consumerStops := 0
				stack.RegisterLifecycle(historyResourceLifecycle{
					start: func() error { synctest.Wait(); return nil },
					stop: func() error {
						consumerStops++
						if !p.running.Load() {
							t.Error("sampler stopped before its consumer")
						}
						return nil
					},
				})
				if failStartup {
					stack.RegisterLifecycle(historyResourceLifecycle{
						start: func() error { return errors.New("later startup failed") },
						stop:  func() error { t.Error("unstarted lifecycle was stopped"); return nil },
					})
				}
				err = stack.Start()
				if (err != nil) != failStartup {
					t.Fatalf("node startup error: %v", err)
				}
				stack.Stop()
				time.Sleep(time.Minute)
				if consumerStops != 1 || p.running.Load() || p.parallelReady() || p.sampleLoad().DeviceAvailable || f.cpuReads.Load() != 1 || f.diskReads.Load() != 1 {
					t.Fatal("node shutdown/startup unwind left the sampler alive or exposed stale capacity")
				}
			})
		})
	}
}
