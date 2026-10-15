package main

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// runtimeHistoryResources keeps the CPU and shared-device baselines fresh even
// across a long history build/recovery or an admission check that short-circuits
// before CPU readiness. It observes resources only; it cannot start maintenance
// or relax its CPU, cgroup, memory, disk, or shared-heavy-work admission rules.
type runtimeHistoryResources struct {
	load     *runtimeHistoryLoadProbe
	parallel *runtimeHistoryParallelProbe

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running atomic.Bool
}

func newRuntimeHistoryResources(db any, path string, etlBuffer int) *runtimeHistoryResources {
	p := &runtimeHistoryResources{
		load: newRuntimeHistoryLoadProbe(db, path), parallel: newRuntimeHistoryParallelProbe(),
	}
	p.parallel.busyMinimumMemory = historyBusyMemoryTarget(etlBuffer)
	return p
}

// Two accessor collectors may each retain a full ETL run. Add 512 MiB for
// metadata, identity/replay caches, codec/sort scratch and GC overlap. This is
// conservative opportunity headroom, not a hard bound on a large block or RSS.
func historyBusyMemoryTarget(etlBuffer int) uint64 {
	if etlBuffer <= 0 {
		return historyBusyMinimumMemory
	}
	const reserve = uint64(512 << 20)
	if uint64(etlBuffer) > (math.MaxUint64-reserve)/2 {
		return math.MaxUint64
	}
	return max(uint64(historyBusyMinimumMemory), 2*uint64(etlBuffer)+reserve)
}

func waitHistoryResourceSample(ctx context.Context, after time.Duration) bool {
	timer := time.NewTimer(after)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func (p *runtimeHistoryResources) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel, p.done = cancel, make(chan struct{})
	p.running.Store(true)
	go p.loop(ctx, p.done)
	return nil
}

func (p *runtimeHistoryResources) loop(ctx context.Context, done chan struct{}) {
	defer close(done)
	for ctx.Err() == nil {
		p.load.sample()
		if ctx.Err() != nil {
			return
		}
		p.parallel.ready()
		// Wait after both bounded reads finish, rather than using a ticker:
		// jitter must not shorten the >=5s interval required by either probe.
		if !waitHistoryResourceSample(ctx, historyParallelSampleInterval) {
			return
		}
	}
}

func (p *runtimeHistoryResources) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running.Store(false)
	if p.cancel != nil {
		p.cancel()
		// Proc/cgroup reads are bounded synchronous operations. Join any read
		// already in flight; never leave it accessing the engine after Stop.
		<-p.done
		p.cancel, p.done = nil, nil
	}
	p.load.reset()
	p.parallel.reset()
	return nil
}

func (p *runtimeHistoryResources) sampleLoad() maintenance.StoragePressure {
	if p.running.Load() {
		out := p.load.sample()
		if p.running.Load() {
			return out
		}
	}
	// Preserve instantaneous Pebble hard-pressure checks while refusing to
	// expose device capacity before Start or after cancellation. Do not perform
	// proc/device reads or rewarm a stopped sampler through an admission call.
	if p.load.engine != nil {
		out := p.load.engine.MaintenancePressure()
		out.DeviceAvailable, out.DeviceSampledAt = false, time.Time{}
		out.DeviceBusyPPM, out.DeviceQueueMilli, out.DeviceAwait = 0, 0, 0
		return out
	}
	return maintenance.StoragePressure{}
}

func (p *runtimeHistoryResources) parallelReady() bool {
	return p.running.Load() && p.parallel.ready() && p.running.Load()
}

func (p *runtimeHistoryResources) sharedReadResources() snapshots.HistoryReadResources {
	if p.running.Load() {
		observed := p.parallel.observeCapacity().reads
		if p.running.Load() {
			return observed
		}
	}
	return snapshots.HistoryReadResources{}
}

// The Runner separately retains storage admission, lease and complete-work
// recovery. Stopped/failed samplers never open this opportunity.
func (p *runtimeHistoryResources) busyBuildReady() bool {
	return p.running.Load() && p.parallel.observeCapacity().busyReady && p.running.Load()
}
