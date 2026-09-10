package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func historyParallelTestObservation(tick uint64) historyParallelObservation {
	cpus := make([]historyParallelCPU, 16)
	for i := range cpus {
		cpus[i].id, cpus[i].ticks[0], cpus[i].ticks[3] = i, tick, tick
	}
	return historyParallelObservation{cpus: cpus, memoryAvailable: 8 << 30, cpuLimitMilli: math.MaxUint64, scope: "host"}
}

func TestHistoryParallelProbeLazyFreshnessAndCapacity(t *testing.T) {
	now := time.Unix(1000, 0)
	reads, tick, gomax := 0, uint64(100), 16
	memory, quota := uint64(8<<30), uint64(math.MaxUint64)
	var readErr error
	p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return gomax }, numCPU: func() int { return 16 },
		read: func() (historyParallelObservation, error) {
			reads++
			out := historyParallelTestObservation(tick)
			out.memoryAvailable, out.cpuLimitMilli = memory, quota
			return out, readErr
		}}
	if p.ready() || reads != 1 || p.known {
		t.Fatal("first observation invented idle evidence")
	}
	now = now.Add(4 * time.Second)
	if p.ready() || reads != 1 {
		t.Fatal("lazy probe reread too early")
	}
	now, tick = now.Add(time.Second), tick+100
	if !p.ready() || reads != 2 || p.idlePPM != 500_000 {
		t.Fatalf("healthy sample rejected: %+v", p)
	}
	gomax = 8
	if p.ready() || reads != 2 {
		t.Fatal("16 allowed CPUs at 50% idle do not prove spare slots under GOMAXPROCS=8")
	}
	gomax = 4
	if p.ready() || reads != 2 {
		t.Fatal("cached evidence ignored reduced GOMAXPROCS")
	}
	gomax = 16
	now, tick, quota = now.Add(5*time.Second), tick+100, 3000
	if p.ready() || !p.known {
		t.Fatal("CPU quota did not cap 1.5 idle cores")
	}
	now, tick, quota = now.Add(5*time.Second), tick+100, 16_000
	if p.ready() || !p.known {
		t.Fatal("host idle evidence cannot prove unused finite cgroup CPU quota")
	}
	now, tick, quota, memory = now.Add(5*time.Second), tick+100, math.MaxUint64, (2<<30)-1
	if p.ready() || !p.known {
		t.Fatal("insufficient memory admitted parallel work")
	}
	now, tick, memory = now.Add(5*time.Second), tick+100, 2<<30
	if !p.ready() {
		t.Fatal("exact memory boundary rejected")
	}
	now, tick = now.Add(31*time.Second), tick+100
	if p.ready() || p.known {
		t.Fatal("long average was mistaken for fresh idle")
	}
	now, tick = now.Add(5*time.Second), tick+100
	if !p.ready() {
		t.Fatal("new short sample did not restore evidence")
	}
	now, readErr = now.Add(5*time.Second), errors.New("injected read failure")
	if p.ready() || p.previous != nil || p.known {
		t.Fatal("failed read retained evidence")
	}
	now, tick, readErr = now.Add(5*time.Second), tick+100, nil
	if p.ready() {
		t.Fatal("first point after failure reused old baseline")
	}
	now, tick = now.Add(5*time.Second), tick+100
	if !p.ready() {
		t.Fatal("second point after failure should be ready")
	}
	now = now.Add(-time.Second)
	if p.ready() || p.known {
		t.Fatal("backwards clock retained readiness")
	}
}

func TestHistoryParallelProbeScopeRollbackAndSlowRead(t *testing.T) {
	for _, mode := range []string{"counter", "affinity", "cgroup", "slow"} {
		t.Run(mode, func(t *testing.T) {
			now, calls := time.Unix(1000, 0), 0
			p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return 16 }, numCPU: func() int { return 16 },
				read: func() (historyParallelObservation, error) {
					calls++
					out := historyParallelTestObservation(uint64(calls * 100))
					if calls == 2 {
						switch mode {
						case "counter":
							out.cpus[0].ticks[3] = 0
						case "affinity":
							out.cpus[0].id = 20
						case "cgroup":
							out.scope = "moved"
						case "slow":
							now = now.Add(2 * time.Second)
						}
					}
					return out, nil
				}}
			if p.ready() {
				t.Fatal("first point ready")
			}
			now = now.Add(5 * time.Second)
			if p.ready() || p.known {
				t.Fatal("changed/invalid sample was accepted")
			}
		})
	}
}

func TestHistoryParallelProbeCachesFromReadCompletion(t *testing.T) {
	now, reads := time.Unix(1000, 0), 0
	p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return 16 }, numCPU: func() int { return 16 },
		read: func() (historyParallelObservation, error) {
			reads++
			if reads == 1 {
				now = now.Add(400 * time.Millisecond)
			}
			return historyParallelTestObservation(uint64(reads * 100)), nil
		}}
	p.ready()
	now = now.Add(4600 * time.Millisecond) // 5s since start, only 4.6s since completion.
	if p.ready() || reads != 1 {
		t.Fatal("read began too early to form a valid pair with the previous completion")
	}
	now = now.Add(400 * time.Millisecond)
	if !p.ready() || reads != 2 {
		t.Fatal("read-time jitter discarded a valid five-second pair")
	}
}

func TestHistoryParallelProbeConcurrentReads(t *testing.T) {
	now, calls := time.Unix(1000, 0), 0
	p := &runtimeHistoryParallelProbe{now: func() time.Time { return now }, gomax: func() int { return 16 }, numCPU: func() int { return 16 },
		read: func() (historyParallelObservation, error) {
			calls++
			return historyParallelTestObservation(uint64(calls * 100)), nil
		}}
	p.ready()
	now = now.Add(5 * time.Second)
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			if !p.ready() {
				t.Error("concurrent fresh readiness rejected")
			}
		})
	}
	wg.Wait()
	if calls != 2 {
		t.Fatalf("concurrent callers reread proc %d times", calls)
	}
}

func TestHistoryParallelMinimumIdleEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		allowed, gomax, numCPU int
		idle                   uint64
		want                   bool
	}{
		{"two cores and quarter idle", 8, 8, 8, 25, true},
		{"below quarter idle", 16, 16, 16, 24, false},
		{"Go capacity fully used", 16, 8, 16, 50, false},
		{"Go capacity with proven room", 16, 8, 16, 75, true},
		{"runtime CPU cap fully used", 16, 16, 8, 50, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, reads := time.Unix(1000, 0), 0
			p := &runtimeHistoryParallelProbe{now: func() time.Time { return now },
				gomax: func() int { return tc.gomax }, numCPU: func() int { return tc.numCPU },
				read: func() (historyParallelObservation, error) {
					reads++
					cpus := make([]historyParallelCPU, tc.allowed)
					for i := range cpus {
						cpus[i].id = i
						cpus[i].ticks[0], cpus[i].ticks[3] = uint64(reads)*(100-tc.idle), uint64(reads)*tc.idle
					}
					return historyParallelObservation{cpus: cpus, memoryAvailable: 2 << 30, cpuLimitMilli: math.MaxUint64}, nil
				}}
			if p.ready() {
				t.Fatal("first observation ready")
			}
			now = now.Add(5 * time.Second)
			if got := p.ready(); got != tc.want {
				t.Fatalf("ready=%v want %v", got, tc.want)
			}
		})
	}
}

func TestHistoryParallelCPUAccounting(t *testing.T) {
	ids, err := historyParallelAllowedCPUs([]byte("Name:\tgtron\nCpus_allowed_list:\t0-1,4,6-7\n"))
	if err != nil || !reflect.DeepEqual(ids, []int{0, 1, 4, 6, 7}) {
		t.Fatalf("affinity: %v %v", ids, err)
	}
	for _, value := range []string{"", "0-4096", "1-0", "1,1", "2,1", "0,,1", "+1", "1-2-3"} {
		if _, err := historyParallelAllowedCPUs([]byte("Cpus_allowed_list: " + value)); err == nil {
			t.Fatalf("accepted affinity %q", value)
		}
	}
	raw := []byte("cpu 100 0 0 100 0 0 0 0 100 0\ncpu0 100 0 0 100 200 0 0 100 100 0\ncpu1 1000 0 0 0 0 0 0 0 0 0\n")
	after, err := historyParallelCPUCounters(raw, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	before := []historyParallelCPU{{id: 0}}
	ppm, known := historyParallelIdle(before, after)
	if !known || ppm != 200_000 {
		t.Fatalf("guest/iowait/steal miscounted as idle: %d %v", ppm, known)
	}
	after[0].ticks = [8]uint64{300, 0, 0, 100}
	if ppm, known = historyParallelIdle(before, after); !known || ppm != 250_000 {
		t.Fatalf("exact idle boundary: %d %v", ppm, known)
	}
	for _, raw := range []string{"cpu0 1 2", "cpu0 1 2 3 4 5 6 7 bad", "cpu1 0 0 0 0 0 0 0 0", "cpu0 0 0 0 0 0 0 0 0\ncpu0 0 0 0 0 0 0 0 0"} {
		if _, err := historyParallelCPUCounters([]byte(raw), []int{0}); err == nil {
			t.Fatalf("accepted malformed CPU counters %q", raw)
		}
	}
	after[0].ticks = [8]uint64{math.MaxUint64, 0, 0, 1}
	if _, known := historyParallelIdle(before, after); known {
		t.Fatal("overflow manufactured idle")
	}
	if _, known := historyParallelIdle(before, before); known {
		t.Fatal("unchanged counters manufactured idle")
	}
}

func historyParallelFixture(v2 bool) map[string]string {
	files := map[string]string{
		"/proc/self/status": "Cpus_allowed_list:\t0-15\n",
		"/proc/meminfo":     "MemTotal: 67108864 kB\nMemAvailable: 25165824 kB\n",
	}
	var stat strings.Builder
	stat.WriteString("cpu 0 0 0 0 0 0 0 0 0 0\n")
	for i := range 16 {
		fmt.Fprintf(&stat, "cpu%d 100 0 0 100 0 0 0 0 100 0\n", i)
	}
	files["/proc/stat"] = stat.String()
	if v2 {
		files["/proc/self/cgroup"] = "0::/system.slice/gtron.service\n"
		files["/proc/self/mountinfo"] = "30 20 0:28 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"
		for _, suffix := range []string{"/system.slice/gtron.service", "/system.slice", ""} {
			prefix := "/sys/fs/cgroup" + suffix + "/"
			files[prefix+"memory.max"], files[prefix+"memory.high"], files[prefix+"memory.current"] = "max", "max", "0"
			files[prefix+"cpu.max"] = "max 100000"
		}
		files["/sys/fs/cgroup/system.slice/gtron.service/memory.max"] = "42949672960"
		files["/sys/fs/cgroup/system.slice/gtron.service/memory.current"] = "22827925504"
	} else {
		files["/proc/self/cgroup"] = "9:memory:/system.slice/gtron.service\n8:cpuacct,cpu:/system.slice/gtron.service\n"
		files["/proc/self/mountinfo"] = "30 20 0:28 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n31 20 0:29 / /sys/fs/cgroup/cpu,cpuacct rw - cgroup cgroup rw,cpuacct,cpu\n"
		for _, suffix := range []string{"/system.slice/gtron.service", "/system.slice", ""} {
			prefix := "/sys/fs/cgroup/memory" + suffix + "/"
			files[prefix+"memory.limit_in_bytes"], files[prefix+"memory.soft_limit_in_bytes"], files[prefix+"memory.usage_in_bytes"] = "9223372036854771712", "9223372036854771712", "0"
			prefix = "/sys/fs/cgroup/cpu,cpuacct" + suffix + "/"
			files[prefix+"cpu.cfs_quota_us"], files[prefix+"cpu.cfs_period_us"] = "-1", "100000"
		}
		files["/sys/fs/cgroup/memory/system.slice/gtron.service/memory.limit_in_bytes"] = "42949672960"
		files["/sys/fs/cgroup/memory/system.slice/gtron.service/memory.usage_in_bytes"] = "22827925504"
	}
	return files
}

func historyParallelFixtureReader(files map[string]string) historyParallelReadFile {
	return func(path string, limit int) ([]byte, error) {
		value, ok := files[path]
		if !ok {
			return nil, &os.PathError{Op: "read", Path: path, Err: os.ErrNotExist}
		}
		if len(value) > limit {
			return nil, errors.New("fixture exceeds bound")
		}
		return []byte(value), nil
	}
}

func TestHistoryParallelCgroupHeadroomAndAncestors(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		t.Run(fmt.Sprint("v2=", v2), func(t *testing.T) {
			files := historyParallelFixture(v2)
			out, err := collectHistoryParallel(historyParallelFixtureReader(files))
			if err != nil || out.memoryAvailable != 42949672960-22827925504 || out.cpuLimitMilli != math.MaxUint64 || len(out.cpus) != 16 {
				t.Fatalf("host memory or quota assumed: %+v %v", out, err)
			}
			if v2 {
				files["/sys/fs/cgroup/system.slice/memory.max"], files["/sys/fs/cgroup/system.slice/memory.current"] = "10737418240", "9663676416"
				files["/sys/fs/cgroup/system.slice/cpu.max"] = "250000 100000"
			} else {
				files["/sys/fs/cgroup/memory/system.slice/memory.limit_in_bytes"], files["/sys/fs/cgroup/memory/system.slice/memory.usage_in_bytes"] = "10737418240", "9663676416"
				files["/sys/fs/cgroup/cpu,cpuacct/system.slice/cpu.cfs_quota_us"] = "250000"
			}
			out, err = collectHistoryParallel(historyParallelFixtureReader(files))
			if err != nil || out.memoryAvailable != 1<<30 || out.cpuLimitMilli != 2500 {
				t.Fatalf("ancestor limit bypassed: %+v %v", out, err)
			}
			if v2 {
				files["/sys/fs/cgroup/system.slice/memory.high"] = "10200547328"
			} else {
				files["/sys/fs/cgroup/memory/system.slice/memory.soft_limit_in_bytes"] = "10200547328"
			}
			out, err = collectHistoryParallel(historyParallelFixtureReader(files))
			if err != nil || out.memoryAvailable != 512<<20 {
				t.Fatalf("ancestor soft/high limit bypassed: %+v %v", out, err)
			}
			if v2 {
				files["/sys/fs/cgroup/system.slice/memory.current"] = "10737418241"
			} else {
				files["/sys/fs/cgroup/memory/system.slice/memory.usage_in_bytes"] = "10737418241"
			}
			out, err = collectHistoryParallel(historyParallelFixtureReader(files))
			if err != nil || out.memoryAvailable != 0 {
				t.Fatalf("usage above limit wrapped headroom: %+v %v", out, err)
			}
		})
	}
}

func TestHistoryParallelV2RootAndFiniteQuotaSaturation(t *testing.T) {
	files := historyParallelFixture(true)
	// The real v2 hierarchy root has no resource-control limit interfaces.
	// The process member and every non-root ancestor still require evidence.
	for _, name := range []string{"memory.max", "memory.high", "memory.current", "cpu.max"} {
		delete(files, "/sys/fs/cgroup/"+name)
	}
	out, err := collectHistoryParallel(historyParallelFixtureReader(files))
	if err != nil || out.memoryAvailable != 42949672960-22827925504 {
		t.Fatalf("ordinary v2 root rejected: %+v %v", out, err)
	}
	files["/sys/fs/cgroup/system.slice/cpu.max"] = "18446744073709551615 1"
	out, err = collectHistoryParallel(historyParallelFixtureReader(files))
	if err != nil || out.cpuLimitMilli == math.MaxUint64 {
		t.Fatalf("finite quota became unlimited: %+v %v", out, err)
	}
	delete(files, "/sys/fs/cgroup/system.slice/memory.max")
	if _, err := collectHistoryParallel(historyParallelFixtureReader(files)); err == nil {
		t.Fatal("missing non-root controller treated as unlimited")
	}
}

func TestHistoryParallelUnknownInputsReject(t *testing.T) {
	for _, tc := range []struct{ name, path, value string }{
		{"membership missing", "/proc/self/cgroup", ""},
		{"controller missing", "/proc/self/cgroup", "8:cpuacct,cpu:/system.slice/gtron.service"},
		{"traversal", "/proc/self/cgroup", "9:memory:/system.slice/../gtron.service\n8:cpu:/system.slice/gtron.service"},
		{"mount missing", "/proc/self/mountinfo", ""},
		{"hidden ancestor", "/proc/self/mountinfo", "30 20 0:28 /system.slice /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory"},
		{"available missing", "/proc/meminfo", "MemTotal: 67108864 kB"},
		{"available units", "/proc/meminfo", "MemTotal: 67108864 kB\nMemAvailable: 1024 MB"},
		{"available overflow", "/proc/meminfo", "MemTotal: 18446744073709551615 kB\nMemAvailable: 1 kB"},
		{"usage unknown", "/sys/fs/cgroup/memory/system.slice/gtron.service/memory.usage_in_bytes", "bad"},
		{"quota malformed", "/sys/fs/cgroup/cpu,cpuacct/system.slice/gtron.service/cpu.cfs_quota_us", "-2"},
		{"period zero", "/sys/fs/cgroup/cpu,cpuacct/system.slice/gtron.service/cpu.cfs_period_us", "0"},
		{"oversized proc", "/proc/stat", strings.Repeat("x", historyParallelProcLimit+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := historyParallelFixture(false)
			files[tc.path] = tc.value
			if _, err := collectHistoryParallel(historyParallelFixtureReader(files)); err == nil {
				t.Fatal("unknown capacity was accepted")
			}
		})
	}
	files := historyParallelFixture(false)
	delete(files, "/sys/fs/cgroup/memory/system.slice/gtron.service/memory.limit_in_bytes")
	if _, err := collectHistoryParallel(historyParallelFixtureReader(files)); err == nil {
		t.Fatal("missing cgroup data fell back to host memory")
	}
}

func TestHistoryParallelBoundedReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proc")
	if err := os.WriteFile(path, []byte("12345"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readHistoryParallelFile(path, 4); err == nil {
		t.Fatal("truncated read accepted")
	}
	if raw, err := readHistoryParallelFile(path, 5); err != nil || string(raw) != "12345" {
		t.Fatalf("exact bound rejected: %q %v", raw, err)
	}
}
