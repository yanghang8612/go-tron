package main

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestHistoryBusyCapacityMemoryAndCPU(t *testing.T) {
	for _, tc := range []struct {
		name           string
		memory         uint64
		idle           uint64
		gomax          int
		quota          uint64
		busy, parallel bool
	}{
		{"below serial", (1 << 30) - 1, 50, 16, math.MaxUint64, false, false},
		{"serial boundary", 1 << 30, 50, 16, math.MaxUint64, true, false},
		{"observed low memory", 1500 << 20, 50, 16, math.MaxUint64, true, false},
		{"below parallel", (2 << 30) - 1, 50, 16, math.MaxUint64, true, false},
		{"parallel boundary", 2 << 30, 50, 16, math.MaxUint64, true, true},
		{"too little idle", 2 << 30, 24, 16, math.MaxUint64, false, false},
		{"Go capacity busy", 2 << 30, 50, 8, math.MaxUint64, false, false},
		{"finite quota", 2 << 30, 50, 16, 16000, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0)
			calls := uint64(0)
			p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return tc.gomax }, numCPU: func() int { return 16 }, read: func() (historyParallelObservation, error) {
				calls++
				o := historyParallelTestObservation(calls * 100)
				for i := range o.cpus {
					o.cpus[i].ticks[0] = calls * (100 - tc.idle)
					o.cpus[i].ticks[3] = calls * tc.idle
				}
				o.memoryAvailable = tc.memory
				o.cpuLimitMilli = tc.quota
				o.memoryOOMKnown = true
				o.memoryOOMEvents = []uint64{0}
				return o, nil
			}}
			if p.observeCapacity().busyReady {
				t.Fatal("first observation admitted busy build")
			}
			now = now.Add(5 * time.Second)
			got := p.observeCapacity()
			if got.busyReady != tc.busy || got.ready != tc.parallel {
				t.Fatalf("busy=%v parallel=%v want %v/%v", got.busyReady, got.ready, tc.busy, tc.parallel)
			}
		})
	}
}

func TestHistoryBusyCapacityEvidenceInvalidation(t *testing.T) {
	for _, mode := range []string{"read failure", "stale", "backwards", "scope", "CPU rollback", "unknown OOM", "under OOM", "OOM growth", "OOM reset", "OOM hierarchy changed"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Unix(1000, 0)
			calls := uint64(0)
			bad := false
			p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return 16 }, numCPU: func() int { return 16 }, read: func() (historyParallelObservation, error) {
				calls++
				o := historyParallelTestObservation(calls * 100)
				o.memoryAvailable = 1500 << 20
				o.memoryOOMKnown = true
				o.memoryOOMEvents = []uint64{5}
				if bad {
					switch mode {
					case "read failure":
						return o, errors.New("injected")
					case "scope":
						o.scope = "moved"
					case "CPU rollback":
						o.cpus[0].ticks[0] = 0
					case "unknown OOM":
						o.memoryOOMKnown = false
					case "under OOM":
						o.memoryUnderOOM = true
					case "OOM growth":
						o.memoryOOMEvents[0]++
					case "OOM reset":
						o.memoryOOMEvents[0] = 0
					case "OOM hierarchy changed":
						o.memoryOOMEvents = append(o.memoryOOMEvents, 0)
					}
				}
				return o, nil
			}}
			p.observeCapacity()
			now = now.Add(5 * time.Second)
			if !p.observeCapacity().busyReady {
				t.Fatal("healthy fixture not ready")
			}
			bad = true
			switch mode {
			case "stale":
				now = now.Add(31 * time.Second)
			case "backwards":
				now = now.Add(-time.Second)
			default:
				now = now.Add(5 * time.Second)
			}
			if p.observeCapacity().busyReady {
				t.Fatal("invalid evidence admitted busy build")
			}
		})
	}
}

func TestHistoryBusyCapacityCgroupCreditAndOOM(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "v1", true: "v2"}[v2], func(t *testing.T) {
			files := historyParallelMemoryCreditFixture(v2)
			for path, raw := range files {
				if strings.HasSuffix(path, "/memory.stat") {
					files[path] = strings.ReplaceAll(raw, "8589934592", "3221225472") // 1.5 GiB credit, below parallel target.
				}
			}
			now := time.Unix(1000, 0)
			tick := uint64(0)
			p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return 16 }, numCPU: func() int { return 16 }, read: func() (historyParallelObservation, error) {
				o, e := collectHistoryParallel(historyParallelFixtureReader(files))
				tick += 100
				cpu := historyParallelTestObservation(tick)
				o.cpus = cpu.cpus
				return o, e
			}}
			first := p.observeCapacity()
			if first.busyReady || first.reads.MemoryAvailableBytes != 0 {
				t.Fatal("first point borrowed clean-file credit")
			}
			now = now.Add(5 * time.Second)
			if got := p.observeCapacity(); !got.busyReady || got.ready {
				t.Fatal("stable 1.5 GiB credit must admit only bounded busy work")
			}
			if v2 {
				files["/sys/fs/cgroup/system.slice/gtron.service/memory.events"] = "oom 1\noom_kill 0\n"
			} else {
				files["/sys/fs/cgroup/memory/system.slice/gtron.service/memory.oom_control"] = "under_oom 1\n"
			}
			now = now.Add(5 * time.Second)
			if p.observeCapacity().busyReady {
				t.Fatal("cgroup OOM evidence retained opportunity")
			}
		})
	}
}

func TestHistoryBusyMemoryTarget(t *testing.T) {
	for _, tc := range []struct {
		buffer int
		want   uint64
	}{{0, 1 << 30}, {12 << 20, 1 << 30}, {256 << 20, 1 << 30}, {512 << 20, 1536 << 20}, {int(^uint(0) >> 1), math.MaxUint64}} {
		if got := historyBusyMemoryTarget(tc.buffer); got != tc.want {
			t.Fatalf("buffer%d target%d want%d", tc.buffer, got, tc.want)
		}
	}
}

func TestHistoryResourcesBusyAdmissionLifecycle(t *testing.T) {
	f := newHistoryResourceFixture()
	p := f.resources
	read := p.parallel.read
	now := time.Unix(1000, 0)
	p.parallel.now = func() time.Time { return now }
	p.parallel.read = func() (historyParallelObservation, error) {
		o, e := read()
		o.cpus = historyParallelTestObservation(uint64(f.cpuReads.Load()) * 100).cpus
		o.memoryAvailable = 1500 << 20
		o.memoryOOMKnown = true
		o.memoryOOMEvents = []uint64{0}
		return o, e
	}
	// A stopped sampler cannot perform proc reads or expose cached capacity.
	if p.busyBuildReady() || f.cpuReads.Load() != 0 {
		t.Fatal("stopped sampler admitted or read")
	}
	p.running.Store(true)
	p.parallel.observeCapacity()
	now = now.Add(5 * time.Second)
	if !p.busyBuildReady() || p.parallelReady() {
		t.Fatal("serial opportunity must be independent of optional parallel work")
	}
	p.running.Store(false)
	calls := f.cpuReads.Load()
	if p.busyBuildReady() || f.cpuReads.Load() != calls {
		t.Fatal("stopped sampler retained admission")
	}
}
